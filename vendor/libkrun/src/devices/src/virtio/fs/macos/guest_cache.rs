//! Which host files the guest holds: the inodes it has looked up and not yet forgotten. A host
//! watcher checks changed files against this, so that the guest can cache entries and attributes
//! for long and is told to drop only what it actually caches.

use std::ffi::CString;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};

use super::super::multikey::MultikeyBTreeMap;
use super::passthrough::{InodeAltKey, InodeData};

pub(super) type InodeTable = RwLock<MultikeyBTreeMap<u64, InodeAltKey, Arc<InodeData>>>;

type WakeCallback = Arc<dyn Fn() + Send + Sync>;

struct State {
    table: Mutex<Weak<InodeTable>>,
    /// Incremented whenever the guest looks up, lists, or caches entries in the share.
    activity: AtomicU64,
    /// Incremented whenever the set of cached directories or a directory's anchor changes.
    dir_seq: AtomicU64,
    activity_armed: AtomicBool,
    dir_armed: AtomicBool,
    on_wake: Mutex<Option<WakeCallback>>,
}

impl Default for State {
    fn default() -> Self {
        State {
            table: Mutex::new(Weak::new()),
            activity: AtomicU64::new(0),
            dir_seq: AtomicU64::new(0),
            activity_armed: AtomicBool::new(false),
            dir_armed: AtomicBool::new(false),
            on_wake: Mutex::new(None),
        }
    }
}

/// A handle on a share's inode table; empty until the device is active and after the file
/// system is gone.
#[derive(Clone, Default)]
pub struct GuestCache(Arc<State>);

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
        *self.0.table.lock().unwrap_or_else(|e| e.into_inner()) = Arc::downgrade(table);
    }

    /// Register a non-blocking callback invoked when an armed activity or directory-change
    /// notification triggers.
    pub fn set_notifier(&self, cb: impl Fn() + Send + Sync + 'static) {
        *self.0.on_wake.lock().unwrap_or_else(|e| e.into_inner()) = Some(Arc::new(cb));
    }

    /// Arm a one-shot wake on the next cache activity. Callers must re-check `activity()`
    /// afterwards so an update racing with arming is never missed.
    pub fn arm_activity(&self) {
        self.0.activity_armed.store(true, Ordering::SeqCst);
    }

    pub fn disarm_activity(&self) {
        self.0.activity_armed.store(false, Ordering::Release);
    }

    /// Arm a one-shot wake on the next cached-directory change. Callers must re-check `dir_seq()`
    /// afterwards so an update racing with arming is never missed.
    pub fn arm_dir_change(&self) {
        self.0.dir_armed.store(true, Ordering::SeqCst);
    }

    pub fn disarm_dir_change(&self) {
        self.0.dir_armed.store(false, Ordering::Release);
    }

    pub fn activity(&self) -> u64 {
        self.0.activity.load(Ordering::SeqCst)
    }

    pub fn dir_seq(&self) -> u64 {
        self.0.dir_seq.load(Ordering::SeqCst)
    }

    #[inline]
    pub(super) fn note_activity(&self) {
        self.0.activity.fetch_add(1, Ordering::SeqCst);
        if self.0.activity_armed.load(Ordering::SeqCst)
            && self.0.activity_armed.swap(false, Ordering::AcqRel)
        {
            self.wake();
        }
    }

    #[inline]
    pub(super) fn note_dir_change(&self) {
        self.0.dir_seq.fetch_add(1, Ordering::SeqCst);
        if self.0.dir_armed.load(Ordering::SeqCst) && self.0.dir_armed.swap(false, Ordering::AcqRel)
        {
            self.wake();
        }
    }

    fn wake(&self) {
        let cb = self
            .0
            .on_wake
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(cb) = cb {
            cb();
        }
    }

    /// The table, while the file system lives. Take one per batch of lookups.
    pub fn view(&self) -> Option<GuestCacheView> {
        self.0
            .table
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .upgrade()
            .map(|table| GuestCacheView {
                table,
                state: Arc::clone(&self.0),
            })
    }
}

pub struct GuestCacheView {
    table: Arc<InodeTable>,
    state: Arc<State>,
}

impl GuestCacheView {
    /// The inode with host identity (`dev`, `ino`), if the guest holds it.
    pub fn get(&self, dev: i32, ino: u64) -> Option<CachedNode> {
        let key = InodeAltKey { ino, dev };
        self.table
            .read()
            .unwrap()
            .get_alt(&key)
            .map(|d| CachedNode::of(d))
    }

    /// Every inode the guest holds.
    pub fn all(&self) -> Vec<CachedNode> {
        self.table
            .read()
            .unwrap()
            .values()
            .map(|d| CachedNode::of(d))
            .collect()
    }

    /// Every directory inode the guest holds.
    pub fn dirs(&self) -> Vec<CachedNode> {
        self.table
            .read()
            .unwrap()
            .values()
            .filter(|d| u32::from(d.kind) == libc::S_IFDIR as u32)
            .map(|d| CachedNode::of(d))
            .collect()
    }

    pub fn len(&self) -> usize {
        self.table.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn activity(&self) -> u64 {
        self.state.activity.load(Ordering::SeqCst)
    }

    pub fn dir_seq(&self) -> u64 {
        self.state.dir_seq.load(Ordering::SeqCst)
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

    #[test]
    fn tracks_activity_dirs_and_one_shot_notifications() {
        use std::sync::atomic::AtomicUsize;

        let dir = TempDir::new();
        std::fs::create_dir(dir.0.join("d")).unwrap();
        std::fs::write(dir.0.join("d/f"), "x").unwrap();

        let cache = GuestCache::default();
        let wakes = Arc::new(AtomicUsize::new(0));
        let wakes_cb = Arc::clone(&wakes);
        cache.set_notifier(move || {
            wakes_cb.fetch_add(1, Ordering::SeqCst);
        });

        let fs = share(&dir, &cache);
        let view = cache.view().unwrap();
        // init registers ROOT_ID and bumps dir_seq, but leaves activity at 0 so idle shares stay dormant.
        assert_eq!(view.activity(), 0);
        assert!(view.dir_seq() >= 1);
        assert_eq!(view.dirs().len(), 1);
        assert_eq!(view.dirs()[0].nodeid, ROOT_ID);

        // Armed activity fires once on negative lookup, then stays disarmed until re-armed.
        cache.arm_activity();
        let _ = fs.lookup(CTX, ROOT_ID, &CString::new("missing").unwrap());
        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        assert_eq!(view.activity(), 1);

        let d = fs
            .lookup(CTX, ROOT_ID, &CString::new("d").unwrap())
            .unwrap();
        assert_eq!(wakes.load(Ordering::SeqCst), 1, "still disarmed");
        assert_eq!(view.dirs().len(), 2);

        // Armed dir_change ignores regular-file lookup, fires on directory forget.
        let dir_seq_before = view.dir_seq();
        cache.arm_dir_change();
        let f = fs
            .lookup(CTX, d.inode, &CString::new("f").unwrap())
            .unwrap();
        assert_eq!(view.dir_seq(), dir_seq_before);
        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        assert_eq!(view.dirs().len(), 2, "only directories returned");

        fs.forget(CTX, f.inode, 1);
        assert_eq!(view.dir_seq(), dir_seq_before);
        assert_eq!(wakes.load(Ordering::SeqCst), 1);

        fs.forget(CTX, d.inode, 1);
        assert!(view.dir_seq() > dir_seq_before);
        assert_eq!(wakes.load(Ordering::SeqCst), 2);
        assert_eq!(view.dirs().len(), 1);
    }
}
