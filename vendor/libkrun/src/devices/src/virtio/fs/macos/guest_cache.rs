//! Which host files the guest holds: the inodes it has looked up and not yet forgotten. A host
//! watcher checks changed files against this, so that the guest can cache entries and attributes
//! for long and is told to drop only what it actually caches.

use std::ffi::CString;
use std::fmt;
use std::sync::{Arc, Mutex, RwLock, Weak};

use super::super::multikey::MultikeyBTreeMap;
use super::passthrough::{InodeAltKey, InodeData};

pub(super) type InodeTable = RwLock<MultikeyBTreeMap<u64, InodeAltKey, Arc<InodeData>>>;

/// A handle on a share's inode table; empty until the device is active and after the file
/// system is gone.
#[derive(Clone, Default)]
pub struct GuestCache(Arc<Mutex<Weak<InodeTable>>>);

impl fmt::Debug for GuestCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("GuestCache")
    }
}

/// An inode the guest holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CachedNode {
    /// The FUSE node id the guest knows it by.
    pub nodeid: u64,
    pub dir: bool,
    /// The parent node and name it was last found by.
    pub anchor: Option<(u64, CString)>,
}

impl CachedNode {
    fn of(data: &InodeData) -> CachedNode {
        CachedNode {
            nodeid: data.inode,
            dir: u32::from(data.kind) == libc::S_IFDIR as u32,
            anchor: data.anchor.lock().unwrap().clone(),
        }
    }
}

impl GuestCache {
    pub(super) fn attach(&self, table: &Arc<InodeTable>) {
        *self.0.lock().unwrap() = Arc::downgrade(table);
    }

    /// The table, while the file system lives. Take one per batch of lookups.
    pub fn view(&self) -> Option<GuestCacheView> {
        self.0.lock().unwrap().upgrade().map(GuestCacheView)
    }
}

pub struct GuestCacheView(Arc<InodeTable>);

impl GuestCacheView {
    /// The inode with host identity (`dev`, `ino`), if the guest holds it.
    pub fn get(&self, dev: i32, ino: u64) -> Option<CachedNode> {
        let key = InodeAltKey { ino, dev };
        self.0
            .read()
            .unwrap()
            .get_alt(&key)
            .map(|d| CachedNode::of(d))
    }

    /// Every inode the guest holds.
    pub fn all(&self) -> Vec<CachedNode> {
        self.0
            .read()
            .unwrap()
            .values()
            .map(|d| CachedNode::of(d))
            .collect()
    }

    pub fn len(&self) -> usize {
        self.0.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::os::unix::fs::MetadataExt;
    use std::time::Duration;

    use super::super::super::filesystem::{Context, FileSystem, FsOptions};
    use super::super::super::fuse::ROOT_ID;
    use super::super::super::inode_alloc::InodeAllocator;
    use super::super::passthrough::{Config, PassthroughFs};
    use super::super::test_util::TempDir;
    use super::*;

    const CTX: Context = Context {
        uid: 0,
        gid: 0,
        pid: 0,
    };

    fn share(dir: &TempDir, cache: &GuestCache) -> PassthroughFs {
        let cfg = Config {
            root_dir: dir.0.to_str().unwrap().into(),
            guest_cache: cache.clone(),
            negative_timeout: Duration::from_secs(60),
            ..Default::default()
        };
        let fs = PassthroughFs::new(cfg, Arc::new(InodeAllocator::new())).unwrap();
        fs.init(FsOptions::empty()).unwrap();
        fs
    }

    #[test]
    fn shows_what_the_guest_holds_until_it_forgets() {
        let dir = TempDir::new();
        std::fs::create_dir(dir.0.join("d")).unwrap();
        std::fs::write(dir.0.join("d/f"), "x").unwrap();
        let cache = GuestCache::default();
        assert!(cache.view().is_none());
        let fs = share(&dir, &cache);
        let view = cache.view().unwrap();
        assert_eq!(view.len(), 1, "the root");

        let d = fs
            .lookup(CTX, ROOT_ID, &CString::new("d").unwrap())
            .unwrap();
        let f = fs
            .lookup(CTX, d.inode, &CString::new("f").unwrap())
            .unwrap();
        let m = std::fs::symlink_metadata(dir.0.join("d/f")).unwrap();
        let node = view.get(m.dev() as i32, m.ino()).unwrap();
        assert_eq!(
            node,
            CachedNode {
                nodeid: f.inode,
                dir: false,
                anchor: Some((d.inode, CString::new("f").unwrap())),
            }
        );
        let dm = std::fs::symlink_metadata(dir.0.join("d")).unwrap();
        assert!(view.get(dm.dev() as i32, dm.ino()).unwrap().dir);
        assert_eq!(view.all().len(), 3);

        fs.forget(CTX, f.inode, 1);
        assert!(view.get(m.dev() as i32, m.ino()).is_none());

        drop(view);
        drop(fs);
        assert!(
            cache.view().is_none(),
            "the table goes with the file system"
        );
    }

    #[test]
    fn opening_a_node_replaced_on_the_host_is_stale() {
        let dir = TempDir::new();
        std::fs::write(dir.0.join("f"), "old").unwrap();
        let fs = share(&dir, &GuestCache::default());
        let f = fs
            .lookup(CTX, ROOT_ID, &CString::new("f").unwrap())
            .unwrap();
        // An editor's save on the host: a new file renamed over the old one.
        std::fs::write(dir.0.join("f.tmp"), "new").unwrap();
        std::fs::rename(dir.0.join("f.tmp"), dir.0.join("f")).unwrap();
        let Err(err) = fs.open(CTX, f.inode, false, libc::O_RDONLY as u32) else {
            panic!("opened a replaced node");
        };
        assert_eq!(err.raw_os_error(), Some(116), "Linux ESTALE");
    }

    #[test]
    fn missing_names_may_be_cached() {
        let dir = TempDir::new();
        let fs = share(&dir, &GuestCache::default());
        assert_eq!(fs.negative_timeout(), Duration::from_secs(60));
        let Err(err) = fs.lookup(CTX, ROOT_ID, &CString::new("nope").unwrap()) else {
            panic!("found a missing name");
        };
        assert_eq!(err.raw_os_error(), Some(libc::ENOENT));
    }
}
