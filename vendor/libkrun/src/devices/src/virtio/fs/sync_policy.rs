//! Which FUSE requests may run synchronously on the vCPU that submitted them.
//!
//! The guest's synchronous submission path (KAT_FS_DRAIN) processes requests
//! on the calling vCPU. Anything that can block for long (an fsync, a large
//! copy, an open that must download a file from iCloud, a request on a
//! network volume) must instead go to the worker pool, which completes it
//! with an interrupt. Decisions are made per request from its opcode and the
//! flags of the node it targets; node flags are decided once, when the node
//! is looked up.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use super::fuse::Opcode;

/// Largest READ or WRITE payload handled synchronously: 128 guest pages, the most one
/// request carries. A sequential read comes in 256 KiB requests, and sending those to the worker
/// pool (a wakeup and an interrupt each) cost a quarter of the throughput.
pub const MAX_SYNC_IO: u32 = 512 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route {
    /// Bounded by nature: run on the calling thread.
    Sync,
    /// An open: run with a deadline, retried on the worker pool on timeout.
    BoundedOpen,
    /// May block: always run on the worker pool.
    Async,
}

/// Route by opcode alone. `io_size` is the payload size of a READ or WRITE.
pub fn classify(opcode: u32, io_size: u32) -> Route {
    use Opcode::*;
    const fn op(o: Opcode) -> u32 {
        o as u32
    }
    match opcode {
        x if x == op(Read) || x == op(Write) => {
            if io_size <= MAX_SYNC_IO {
                Route::Sync
            } else {
                Route::Async
            }
        }
        x if x == op(Open) || x == op(Opendir) || x == op(Create) => Route::BoundedOpen,
        x if [
            Lookup,
            Forget,
            BatchForget,
            Getattr,
            Setattr,
            Readlink,
            Symlink,
            Mknod,
            Mkdir,
            Unlink,
            Rmdir,
            Rename,
            Rename2,
            Link,
            Statfs,
            Release,
            Releasedir,
            Getxattr,
            Listxattr,
            Flush,
            Init,
            Readdir,
            Readdirplus,
            Access,
            Interrupt,
            Destroy,
            Lseek,
            // Every server answers at once (unsupported, or a table update);
            // the guest sends one for each isatty() on a shared file.
            Ioctl,
            // F_BARRIERFSYNC on local APFS takes a fraction of a millisecond, less than
            // the pool's wakeup and completion interrupt add to it, so it runs on
            // the vCPU. Nodes on volumes that can block still go to the pool.
            Fsync,
            Fsyncdir,
        ]
        .into_iter()
        .any(|o| op(o) == x) =>
        {
            Route::Sync
        }
        // Fallocate, Setxattr, Removexattr, CopyFileRange,
        // Setlk(w), Getlk, Poll, Bmap, mappings and unknown opcodes.
        _ => Route::Async,
    }
}

/// Where a node sits relative to places that can block.
pub trait Boundaries: Send + Sync {
    /// The volume with this device id is local APFS/HFS and safe to touch
    /// synchronously.
    fn is_safe_dev(&self, dev: u64) -> bool;
    /// The directory (dev, ino) contains mountpoints of unsafe volumes.
    fn is_boundary_parent(&self, dev: u64, ino: u64) -> bool;
    /// `name` in the directory (dev, ino) is the mountpoint of an unsafe volume.
    fn is_unsafe_mountpoint(&self, dev: u64, ino: u64, name: &[u8]) -> bool;
}

/// Every volume is safe; used on hosts without a mount table and in tests.
pub struct NoBoundaries;

impl Boundaries for NoBoundaries {
    fn is_safe_dev(&self, _dev: u64) -> bool {
        true
    }
    fn is_boundary_parent(&self, _dev: u64, _ino: u64) -> bool {
        false
    }
    fn is_unsafe_mountpoint(&self, _dev: u64, _ino: u64, _name: &[u8]) -> bool {
        false
    }
}

/// Everything under the node goes to the worker pool.
const ASYNC_TREE: u8 = 1;
/// A directory holding unsafe mountpoints: lookups of those names and
/// READDIRPLUS (which stats every entry) go to the worker pool.
const BOUNDARY: u8 = 2;
/// A directory named `Library`, whose iCloud and File Provider children are
/// async-only.
const LIBRARY: u8 = 4;

/// Children of `~/Library` that can download or call into File Provider.
const LIBRARY_ASYNC_NAMES: [&[u8]; 3] = [b"Mobile Documents", b"Group Containers", b"CloudStorage"];

#[derive(Clone, Copy)]
struct NodeInfo {
    flags: u8,
    dev: u64,
    ino: u64,
}

/// Node flags for one filesystem. Only flagged nodes have an entry, so the
/// common case is a single read of an empty map.
pub struct SyncPolicy {
    boundaries: Arc<dyn Boundaries>,
    nodes: RwLock<HashMap<u64, NodeInfo>>,
    any: AtomicBool,
}

/// What `route` needs to know about a request before running it.
pub struct Peek<'a> {
    pub opcode: u32,
    pub nodeid: u64,
    pub io_size: u32,
    /// The name of a LOOKUP, when the caller read it.
    pub name: Option<&'a [u8]>,
}

impl SyncPolicy {
    pub fn new(boundaries: Arc<dyn Boundaries>) -> Self {
        Self {
            boundaries,
            nodes: RwLock::new(HashMap::new()),
            any: AtomicBool::new(false),
        }
    }

    fn get(&self, nodeid: u64) -> Option<NodeInfo> {
        if !self.any.load(Ordering::Relaxed) {
            return None;
        }
        self.nodes.read().unwrap().get(&nodeid).copied()
    }

    fn set(&self, nodeid: u64, info: NodeInfo) {
        let mut nodes = self.nodes.write().unwrap();
        let e = nodes.entry(nodeid).or_insert(NodeInfo { flags: 0, ..info });
        e.flags |= info.flags;
        e.dev = info.dev;
        e.ino = info.ino;
        self.any.store(true, Ordering::Relaxed);
    }

    /// The node is async-only from now on (a bounded open on it timed out).
    pub fn mark_async(&self, nodeid: u64) {
        let info = self.get(nodeid).unwrap_or(NodeInfo {
            flags: 0,
            dev: 0,
            ino: 0,
        });
        self.set(
            nodeid,
            NodeInfo {
                flags: ASYNC_TREE,
                ..info
            },
        );
    }

    /// Whether `route` needs the LOOKUP name for this node.
    pub fn wants_name(&self, nodeid: u64) -> bool {
        self.get(nodeid).is_some_and(|i| i.flags & BOUNDARY != 0)
    }

    pub fn route(&self, req: &Peek) -> Route {
        let base = classify(req.opcode, req.io_size);
        if base == Route::Async {
            return base;
        }
        let Some(info) = self.get(req.nodeid) else {
            return base;
        };
        if info.flags & ASYNC_TREE != 0 {
            return Route::Async;
        }
        if info.flags & BOUNDARY != 0 {
            if req.opcode == Opcode::Readdirplus as u32 {
                return Route::Async;
            }
            if req.opcode == Opcode::Lookup as u32 {
                let unsafe_name = match req.name {
                    Some(name) => self
                        .boundaries
                        .is_unsafe_mountpoint(info.dev, info.ino, name),
                    None => true,
                };
                if unsafe_name {
                    return Route::Async;
                }
            }
        }
        base
    }

    /// Decide the flags of the shared root (FUSE node 1).
    pub fn note_root(&self, nodeid: u64, dev: u64, ino: u64) {
        self.note(None, nodeid, b"", dev, ino, true);
    }

    /// Decide the flags of a node returned by LOOKUP, READDIRPLUS, CREATE,
    /// MKDIR, MKNOD, SYMLINK or LINK under `parent`.
    pub fn note_entry(&self, parent: u64, name: &[u8], nodeid: u64, dev: u64, ino: u64, dir: bool) {
        self.note(Some(parent), nodeid, name, dev, ino, dir);
    }

    fn note(&self, parent: Option<u64>, nodeid: u64, name: &[u8], dev: u64, ino: u64, dir: bool) {
        let pflags = parent.and_then(|p| self.get(p)).map_or(0, |i| i.flags);
        let mut flags = 0;
        if pflags & ASYNC_TREE != 0
            || !self.boundaries.is_safe_dev(dev)
            || (pflags & LIBRARY != 0 && LIBRARY_ASYNC_NAMES.contains(&name))
        {
            flags |= ASYNC_TREE;
        } else if dir {
            if self.boundaries.is_boundary_parent(dev, ino) {
                flags |= BOUNDARY;
            }
            if name == b"Library" {
                flags |= LIBRARY;
            }
        }
        if flags != 0 {
            self.set(nodeid, NodeInfo { flags, dev, ino });
        }
    }
}

impl Default for SyncPolicy {
    fn default() -> Self {
        Self::new(Arc::new(NoBoundaries))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classification_table() {
        use Opcode::*;
        for o in [
            Lookup,
            Getattr,
            Readdir,
            Readdirplus,
            Forget,
            BatchForget,
            Release,
            Releasedir,
            Statfs,
            Mkdir,
            Unlink,
            Rename,
            Getxattr,
            Flush,
            Ioctl,
            Fsync,
            Fsyncdir,
        ] {
            assert_eq!(classify(o as u32, 0), Route::Sync, "{o:?}");
        }
        for o in [Open, Opendir, Create] {
            assert_eq!(classify(o as u32, 0), Route::BoundedOpen, "{o:?}");
        }
        for o in [
            Fallocate,
            Setxattr,
            Removexattr,
            CopyFileRange,
            Setlkw,
        ] {
            assert_eq!(classify(o as u32, 0), Route::Async, "{o:?}");
        }
        assert_eq!(classify(Read as u32, MAX_SYNC_IO), Route::Sync);
        assert_eq!(classify(Write as u32, 4096), Route::Sync);
        assert_eq!(classify(Read as u32, MAX_SYNC_IO + 1), Route::Async);
        assert_eq!(classify(Write as u32, 1 << 20), Route::Async);
        assert_eq!(classify(9999, 0), Route::Async);
    }

    struct Fake;
    impl Boundaries for Fake {
        fn is_safe_dev(&self, dev: u64) -> bool {
            dev != 66
        }
        fn is_boundary_parent(&self, dev: u64, ino: u64) -> bool {
            (dev, ino) == (1, 500)
        }
        fn is_unsafe_mountpoint(&self, dev: u64, ino: u64, name: &[u8]) -> bool {
            (dev, ino) == (1, 500) && name == b"nas"
        }
    }

    fn peek(opcode: Opcode, nodeid: u64) -> Peek<'static> {
        Peek {
            opcode: opcode as u32,
            nodeid,
            io_size: 0,
            name: None,
        }
    }

    #[test]
    fn node_flags_route_requests() {
        let p = SyncPolicy::new(Arc::new(Fake));
        p.note_root(1, 1, 2);
        assert_eq!(p.route(&peek(Opcode::Getattr, 1)), Route::Sync);

        // A volume that is not local APFS: the node and everything below it.
        p.note_entry(1, b"net", 10, 66, 3, true);
        assert_eq!(p.route(&peek(Opcode::Getattr, 10)), Route::Async);
        p.note_entry(10, b"file", 11, 66, 4, false);
        assert_eq!(p.route(&peek(Opcode::Read, 11)), Route::Async);

        // iCloud Drive and File Provider below ~/Library.
        p.note_entry(1, b"Library", 20, 1, 21, true);
        assert_eq!(p.route(&peek(Opcode::Lookup, 20)), Route::Sync);
        p.note_entry(20, b"Mobile Documents", 22, 1, 23, true);
        p.note_entry(22, b"doc.pages", 24, 1, 25, false);
        p.note_entry(20, b"Preferences", 26, 1, 27, true);
        assert_eq!(p.route(&peek(Opcode::Open, 24)), Route::Async);
        assert_eq!(p.route(&peek(Opcode::Open, 26)), Route::BoundedOpen);
        p.note_entry(1, b"Mobile Documents", 28, 1, 29, true);
        assert_eq!(p.route(&peek(Opcode::Open, 28)), Route::BoundedOpen);

        // A directory holding a network mountpoint.
        p.note_entry(1, b"home", 30, 1, 500, true);
        let lookup = |name: &'static [u8]| Peek {
            opcode: Opcode::Lookup as u32,
            nodeid: 30,
            io_size: 0,
            name: Some(name),
        };
        assert!(p.wants_name(30));
        assert_eq!(p.route(&lookup(b"nas")), Route::Async);
        assert_eq!(p.route(&lookup(b"src")), Route::Sync);
        assert_eq!(p.route(&peek(Opcode::Readdirplus, 30)), Route::Async);
        assert_eq!(p.route(&peek(Opcode::Readdir, 30)), Route::Sync);
        p.note_entry(30, b"src", 31, 1, 501, true);
        assert_eq!(p.route(&peek(Opcode::Lookup, 31)), Route::Sync);

        // A timed-out open makes the node async-only.
        assert_eq!(p.route(&peek(Opcode::Open, 31)), Route::BoundedOpen);
        p.mark_async(31);
        assert_eq!(p.route(&peek(Opcode::Open, 31)), Route::Async);
        assert_eq!(p.route(&peek(Opcode::Getattr, 31)), Route::Async);
        // Opcodes that always block stay async on any node.
        assert_eq!(p.route(&peek(Opcode::Setxattr, 1)), Route::Async);
        assert_eq!(p.route(&peek(Opcode::Fsync, 31)), Route::Async);
    }
}
