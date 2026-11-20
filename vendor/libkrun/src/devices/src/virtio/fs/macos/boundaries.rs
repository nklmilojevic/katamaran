//! Mount boundaries: which volumes are safe to touch on a vCPU thread.
//!
//! Only local APFS and HFS volumes are safe. Network filesystems, FUSE
//! (macFUSE, fuse-t), autofs and other exotic volumes can block for as long
//! as their server takes, so nodes on them are async-only, and lookups of
//! their mountpoints are too. The table comes from `getfsstat` and is rebuilt
//! whenever the kernel reports a mount change (kqueue `EVFILT_FS`), or when a
//! lookup meets a device that is not in it.

use std::collections::HashMap;
use std::ffi::CStr;
use std::io;
use std::mem::MaybeUninit;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use super::super::sync_policy::Boundaries;

/// One mounted volume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mount {
    /// `f_fsid.val[0]`, which is the `st_dev` of files on the volume.
    pub dev: u64,
    pub fstype: String,
    pub path: Vec<u8>,
    pub local: bool,
}

impl Mount {
    pub fn is_safe(&self) -> bool {
        self.local && matches!(self.fstype.as_str(), "apfs" | "hfs")
    }
}

/// Safe devices plus, for each directory that holds unsafe mountpoints, the
/// names of those mountpoints.
#[derive(Debug, Default)]
pub struct MountTable {
    safe: HashMap<u64, bool>,
    parents: HashMap<(u64, u64), Vec<Vec<u8>>>,
}

impl MountTable {
    /// `stat_dir` returns (dev, ino) of a directory path.
    pub fn build(mounts: &[Mount], stat_dir: impl Fn(&[u8]) -> Option<(u64, u64)>) -> Self {
        let mut t = MountTable::default();
        for m in mounts {
            // A device mounted twice is only safe if every instance is.
            let safe = t.safe.entry(m.dev).or_insert(true);
            *safe &= m.is_safe();
        }
        for m in mounts.iter().filter(|m| !m.is_safe()) {
            let path = Path::new(std::ffi::OsStr::from_bytes(&m.path));
            let (Some(parent), Some(name)) = (path.parent(), path.file_name()) else {
                continue;
            };
            if let Some(key) = stat_dir(parent.as_os_str().as_bytes()) {
                t.parents
                    .entry(key)
                    .or_default()
                    .push(name.as_bytes().to_vec());
            }
        }
        t
    }

    /// `None` when the device is not in the table.
    pub fn dev_safe(&self, dev: u64) -> Option<bool> {
        self.safe.get(&dev).copied()
    }

    pub fn is_boundary_parent(&self, dev: u64, ino: u64) -> bool {
        self.parents.contains_key(&(dev, ino))
    }

    pub fn is_unsafe_mountpoint(&self, dev: u64, ino: u64, name: &[u8]) -> bool {
        self.parents
            .get(&(dev, ino))
            .is_some_and(|names| names.iter().any(|n| n == name))
    }
}

fn cstr_field(buf: &[libc::c_char]) -> &[u8] {
    // SAFETY: getfsstat NUL-terminates these fixed-size fields.
    unsafe { CStr::from_ptr(buf.as_ptr()) }.to_bytes()
}

/// `f_fsid.val[0]`; libc keeps the field private.
fn fsid_dev(fsid: &libc::fsid_t) -> u64 {
    // SAFETY: fsid_t is two int32_t on macOS.
    let val: [i32; 2] = unsafe { std::mem::transmute_copy(fsid) };
    val[0] as u32 as u64
}

/// The current mounts, without waiting for unresponsive volumes.
pub fn enumerate() -> io::Result<Vec<Mount>> {
    loop {
        let n = unsafe { libc::getfsstat(std::ptr::null_mut(), 0, libc::MNT_NOWAIT) };
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        let cap = n as usize + 4;
        let mut buf: Vec<libc::statfs> = Vec::with_capacity(cap);
        let size = (cap * std::mem::size_of::<libc::statfs>()) as libc::c_int;
        let got = unsafe { libc::getfsstat(buf.as_mut_ptr(), size, libc::MNT_NOWAIT) };
        if got < 0 {
            return Err(io::Error::last_os_error());
        }
        if got as usize == cap {
            continue;
        }
        unsafe { buf.set_len(got as usize) };
        return Ok(buf
            .iter()
            .map(|s| Mount {
                dev: fsid_dev(&s.f_fsid),
                fstype: String::from_utf8_lossy(cstr_field(&s.f_fstypename)).into_owned(),
                path: cstr_field(&s.f_mntonname).to_vec(),
                local: s.f_flags & libc::MNT_LOCAL as u32 != 0,
            })
            .collect());
    }
}

fn lstat_dir(path: &[u8]) -> Option<(u64, u64)> {
    let c = std::ffi::CString::new(path).ok()?;
    let mut st = MaybeUninit::<libc::stat>::zeroed();
    if unsafe { libc::lstat(c.as_ptr(), st.as_mut_ptr()) } != 0 {
        return None;
    }
    let st = unsafe { st.assume_init() };
    Some((st.st_dev as u32 as u64, st.st_ino))
}

/// The host's mount boundaries, kept current by a watcher thread.
pub struct MountBoundaries {
    table: RwLock<Arc<MountTable>>,
    last_rebuild: Mutex<Instant>,
    /// Fast path for the most recently seen safe device; cleared on rebuild.
    safe_dev_fast: AtomicU64,
}

/// Unknown devices trigger at most one rebuild per this interval.
const REBUILD_INTERVAL: Duration = Duration::from_secs(1);

impl MountBoundaries {
    /// The process-wide instance; the first call builds the table and starts
    /// the watcher.
    pub fn shared() -> Arc<MountBoundaries> {
        static B: OnceLock<Arc<MountBoundaries>> = OnceLock::new();
        B.get_or_init(|| {
            let b = Arc::new(MountBoundaries {
                table: RwLock::new(Arc::new(MountTable::default())),
                last_rebuild: Mutex::new(Instant::now()),
                safe_dev_fast: AtomicU64::new(0),
            });
            b.rebuild();
            let w = b.clone();
            let _ = thread::Builder::new()
                .name("fs mounts".into())
                .spawn(move || w.watch());
            b
        })
        .clone()
    }

    fn rebuild(&self) {
        match enumerate() {
            Ok(mounts) => {
                let t = MountTable::build(&mounts, lstat_dir);
                debug!(
                    "rebuilt unsafe mount boundaries: {} volumes, {} boundary dirs",
                    t.safe.len(),
                    t.parents.len()
                );
                *self.table.write().unwrap() = Arc::new(t);
                self.safe_dev_fast.store(0, Ordering::Release);
            }
            Err(e) => warn!("failed to enumerate mounts: {e}"),
        }
        *self.last_rebuild.lock().unwrap() = Instant::now();
    }

    fn watch(&self) {
        let kq = unsafe { libc::kqueue() };
        if kq < 0 {
            warn!("mount watcher: kqueue: {}", io::Error::last_os_error());
            return;
        }
        let mut ev: libc::kevent = unsafe { std::mem::zeroed() };
        ev.filter = libc::EVFILT_FS;
        ev.flags = libc::EV_ADD | libc::EV_CLEAR;
        if unsafe { libc::kevent(kq, &ev, 1, std::ptr::null_mut(), 0, std::ptr::null()) } < 0 {
            warn!("mount watcher: EVFILT_FS: {}", io::Error::last_os_error());
            return;
        }
        loop {
            let mut out: libc::kevent = unsafe { std::mem::zeroed() };
            let n = unsafe { libc::kevent(kq, std::ptr::null(), 0, &mut out, 1, std::ptr::null()) };
            if n > 0 {
                // Mounts often come in bursts (a disk image with several volumes).
                thread::sleep(Duration::from_millis(50));
                self.rebuild();
            }
        }
    }
}

impl Boundaries for MountBoundaries {
    fn is_safe_dev(&self, dev: u64) -> bool {
        if dev != 0 && self.safe_dev_fast.load(Ordering::Acquire) == dev {
            return true;
        }
        if let Some(safe) = self.table.read().unwrap().dev_safe(dev) {
            if safe && dev != 0 {
                self.safe_dev_fast.store(dev, Ordering::Relaxed);
            }
            return safe;
        }
        if self.last_rebuild.lock().unwrap().elapsed() >= REBUILD_INTERVAL {
            self.rebuild();
        }
        let safe = self.table.read().unwrap().dev_safe(dev).unwrap_or(false);
        if safe && dev != 0 {
            self.safe_dev_fast.store(dev, Ordering::Relaxed);
        }
        safe
    }

    fn is_boundary_parent(&self, dev: u64, ino: u64) -> bool {
        self.table.read().unwrap().is_boundary_parent(dev, ino)
    }

    fn is_unsafe_mountpoint(&self, dev: u64, ino: u64, name: &[u8]) -> bool {
        self.table
            .read()
            .unwrap()
            .is_unsafe_mountpoint(dev, ino, name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(dev: u64, fstype: &str, path: &str, local: bool) -> Mount {
        Mount {
            dev,
            fstype: fstype.into(),
            path: path.as_bytes().to_vec(),
            local,
        }
    }

    #[test]
    fn safe_volumes() {
        assert!(m(1, "apfs", "/", true).is_safe());
        assert!(m(2, "hfs", "/Volumes/Old", true).is_safe());
        assert!(!m(3, "nfs", "/Users/me/vmshare", false).is_safe());
        assert!(!m(4, "smbfs", "/Volumes/share", false).is_safe());
        assert!(!m(5, "macfuse", "/Volumes/sshfs", true).is_safe());
        assert!(!m(6, "autofs", "/System/Volumes/Data/home", true).is_safe());
        assert!(!m(7, "apfs", "/Volumes/remote", false).is_safe());
    }

    #[test]
    fn boundary_decisions() {
        let mounts = vec![
            m(10, "apfs", "/", true),
            m(11, "apfs", "/System/Volumes/Data", true),
            m(12, "nfs", "/Users/me/vmshare", false),
            m(13, "smbfs", "/Users/me/mnt/nas", false),
            m(14, "macfuse", "/Users/me/mnt/sshfs", true),
            m(15, "devfs", "/dev", true),
        ];
        let dirs: HashMap<&[u8], (u64, u64)> = [
            (&b"/Users/me"[..], (11, 100)),
            (&b"/Users/me/mnt"[..], (11, 200)),
        ]
        .into();
        let t = MountTable::build(&mounts, |p| dirs.get(p).copied());
        assert_eq!(t.dev_safe(10), Some(true));
        assert_eq!(t.dev_safe(11), Some(true));
        assert_eq!(t.dev_safe(12), Some(false));
        assert_eq!(t.dev_safe(14), Some(false));
        assert_eq!(t.dev_safe(15), Some(false));
        assert_eq!(t.dev_safe(99), None);

        assert!(t.is_boundary_parent(11, 100));
        assert!(t.is_boundary_parent(11, 200));
        assert!(!t.is_boundary_parent(11, 300));
        assert!(t.is_unsafe_mountpoint(11, 100, b"vmshare"));
        assert!(!t.is_unsafe_mountpoint(11, 100, b"dev"));
        assert!(t.is_unsafe_mountpoint(11, 200, b"nas"));
        assert!(t.is_unsafe_mountpoint(11, 200, b"sshfs"));
        // devfs at /dev: the parent "/" was not resolvable in this fake.
        assert!(!t.is_boundary_parent(10, 2));
    }

    #[test]
    fn same_device_mounted_twice_is_unsafe_if_any_is() {
        let t = MountTable::build(
            &[m(20, "apfs", "/a", true), m(20, "nullfs", "/b", true)],
            |_| None,
        );
        assert_eq!(t.dev_safe(20), Some(false));
    }

    #[test]
    fn host_mount_table_matches_stat() {
        let mounts = enumerate().unwrap();
        let data = mounts
            .iter()
            .find(|m| m.path == b"/System/Volumes/Data")
            .expect("data volume");
        assert!(data.is_safe(), "{data:?}");
        // st_dev of a file equals the fsid of its volume.
        let tmp = std::env::temp_dir();
        let (tdev, _) = lstat_dir(tmp.as_os_str().as_bytes()).unwrap();
        assert_eq!(tdev, data.dev);
        let b = MountBoundaries::shared();
        assert!(b.is_safe_dev(tdev));
        assert!(!b.is_safe_dev(0xdead_beef));
    }
}
