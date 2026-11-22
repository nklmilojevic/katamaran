//! The guest's files on the Mac: the NFS export of the guest's `kat-nfs`
//! server, mounted at `~/Katamaran` with the Mac's own NFS client.
//!
//! Transport: libkrun listens on a free TCP port of 127.0.0.1, writes its
//! number to `<state>/run/nfs/nfs.port`, and relays each connection to the
//! guest's vsock port 2049. MOUNT and NFS share the port, so there is no
//! portmapper. The mount is made by the user, without a password: macOS
//! lets a user mount on a directory they own, forcing `nosuid,nodev`.
//!
//! The mount is made with `mount(2)` and XDR arguments of our own, naming
//! the server `Katamaran`, so Finder shows "Katamaran" rather than an
//! address. It cannot be a Unix socket: the kernel always names such a
//! mount `<socket path>:/`, and Finder shows that server as "<".
//! `/sbin/mount_nfs` is the fallback; it can only name the server by its
//! address.
//!
//! Access: libkrun accepts only connections whose socket the kernel owns
//! (pid 0 in the kernel's TCP table), which is the NFS client's; any
//! process, root's included, is refused, so no local program can read the
//! guest's files past the mount's permission checks.
//!
//! A dead NFS mount can hang every process that touches it, Finder
//! included, so nothing here stats the mount point while a mount may be on
//! it: the kernel's mount table (`getfsstat` with `MNT_NOWAIT`) is read
//! instead.

use std::collections::HashMap;
use std::ffi::CString;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, LazyLock, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, bail};

const MOUNT_TIMEOUT: Duration = Duration::from_secs(20);

/// The server's name, which Finder shows.
pub const SERVER: &str = "Katamaran";
const ADDRESS: &str = "127.0.0.1";

/// The source of our mount, as the kernel lists it.
pub fn source() -> String {
    format!("{SERVER}:/")
}

/// The mount options, and why:
/// - `vers=3`: stateless; nothing to recover after a VM restart but handles.
/// - `soft,intr,timeo=10,retrans=3`: when the VM is gone a call fails after
///   a few retries (about 10 s) instead of hanging forever, and a signal
///   interrupts a waiting process.
/// - `deadtimeout=30`: still unresponsive 30 s later, the kernel marks the
///   mount dead and unmounts it by itself. A crashed daemon therefore never
///   leaves a mount that freezes Finder for good.
/// - `locallocks`: locks work (on the Mac only); the guest has no lock
///   manager.
/// - `nfc`: names go to Linux in NFC, as Linux programs write them.
/// - `actimeo=1`: attributes and directory contents are reused for at most
///   1 s, so a change made in a container shows up within a second. Opening
///   a file always revalidates it (close-to-open consistency).
/// - `rsize/wsize=1M, readahead=16, dsize=64K`: big transfers and up to
///   16 MiB of sequential read-ahead over loopback.
/// - `noatime`: reads from the Mac do not turn into access-time updates. It
///   saves no guest writes: the guest's Docker data disk is already mounted
///   `noatime`.
///
/// These are for `mount_nfs`; [`mount_args`] encodes the same.
pub fn options(port: u16) -> String {
    format!(
        "vers=3,proto=tcp,port={port},mountport={port},soft,intr,timeo=10,retrans=3,\
         deadtimeout=30,locallocks,nfc,actimeo=1,rsize=1048576,wsize=1048576,readahead=16,\
         dsize=65536,noatime"
    )
}

// The XDR mount arguments of the Mac's NFS client: `<nfs/nfs.h>`, and
// `assemble_mount_args` in Apple's `mount_nfs.c`.
const NFS_ARGSVERSION_XDR: u32 = 88;
const NFS_XDRARGS_VERSION_0: u32 = 0;
const NFS_MATTR_FLAGS: u32 = 0;
const NFS_MATTR_NFS_VERSION: u32 = 1;
const NFS_MATTR_READ_SIZE: u32 = 3;
const NFS_MATTR_WRITE_SIZE: u32 = 4;
const NFS_MATTR_READDIR_SIZE: u32 = 5;
const NFS_MATTR_READAHEAD: u32 = 6;
const NFS_MATTR_ATTRCACHE_REG_MIN: u32 = 7;
const NFS_MATTR_ATTRCACHE_REG_MAX: u32 = 8;
const NFS_MATTR_ATTRCACHE_DIR_MIN: u32 = 9;
const NFS_MATTR_ATTRCACHE_DIR_MAX: u32 = 10;
const NFS_MATTR_LOCK_MODE: u32 = 11;
const NFS_MATTR_SOCKET_TYPE: u32 = 14;
const NFS_MATTR_NFS_PORT: u32 = 15;
const NFS_MATTR_MOUNT_PORT: u32 = 16;
const NFS_MATTR_REQUEST_TIMEOUT: u32 = 17;
const NFS_MATTR_SOFT_RETRY_COUNT: u32 = 18;
const NFS_MATTR_DEAD_TIMEOUT: u32 = 19;
const NFS_MATTR_FS_LOCATIONS: u32 = 21;
const NFS_MATTR_MNTFLAGS: u32 = 22;
const NFS_MATTR_MNTFROM: u32 = 23;
const NFS_MATTR_ATTRCACHE_ROOTDIR_MIN: u32 = 33;
const NFS_MATTR_ATTRCACHE_ROOTDIR_MAX: u32 = 34;
const NFS_MFLAG_SOFT: u32 = 0;
const NFS_MFLAG_INTR: u32 = 1;
const NFS_MFLAG_NFC: u32 = 14;
const NFS_LOCK_MODE_LOCAL: u32 = 2;

/// The flags `mount(2)` gets. A user's mount gets `nodev,nosuid` from the
/// kernel anyway; asking for them keeps a mount by root the same.
const MNT_FLAGS: libc::c_int = libc::MNT_NOATIME | libc::MNT_NODEV | libc::MNT_NOSUID;

#[derive(Default)]
struct Xdr(Vec<u8>);

impl Xdr {
    fn u32(&mut self, v: u32) {
        self.0.extend_from_slice(&v.to_be_bytes());
    }
    fn time(&mut self, secs: u32) {
        self.u32(secs);
        self.u32(0);
    }
    fn string(&mut self, s: &str) {
        self.u32(s.len() as u32);
        self.0.extend_from_slice(s.as_bytes());
        self.0.resize(self.0.len().next_multiple_of(4), 0);
    }
    fn bitmap(&mut self, words: &[u32]) {
        self.u32(words.len() as u32);
        for &w in words {
            self.u32(w);
        }
    }
    fn patch(&mut self, at: usize, v: u32) {
        self.0[at..at + 4].copy_from_slice(&v.to_be_bytes());
    }
}

fn bits<const N: usize>(set: &[u32]) -> [u32; N] {
    let mut b = [0; N];
    for &i in set {
        b[(i / 32) as usize] |= 1 << (i % 32);
    }
    b
}

/// The XDR arguments `mount(2)` takes for our NFS mount: what `mount_nfs`
/// builds from [`options`], but with the server named [`SERVER`] at
/// 127.0.0.1 and [`source`] as the mount's source. Attributes follow the
/// order of their bits, as the kernel reads them.
pub fn mount_args(port: u16) -> Vec<u8> {
    let attrs = [
        NFS_MATTR_FLAGS,
        NFS_MATTR_NFS_VERSION,
        NFS_MATTR_READ_SIZE,
        NFS_MATTR_WRITE_SIZE,
        NFS_MATTR_READDIR_SIZE,
        NFS_MATTR_READAHEAD,
        NFS_MATTR_ATTRCACHE_REG_MIN,
        NFS_MATTR_ATTRCACHE_REG_MAX,
        NFS_MATTR_ATTRCACHE_DIR_MIN,
        NFS_MATTR_ATTRCACHE_DIR_MAX,
        NFS_MATTR_LOCK_MODE,
        NFS_MATTR_SOCKET_TYPE,
        NFS_MATTR_NFS_PORT,
        NFS_MATTR_MOUNT_PORT,
        NFS_MATTR_REQUEST_TIMEOUT,
        NFS_MATTR_SOFT_RETRY_COUNT,
        NFS_MATTR_DEAD_TIMEOUT,
        NFS_MATTR_FS_LOCATIONS,
        NFS_MATTR_MNTFLAGS,
        NFS_MATTR_MNTFROM,
        NFS_MATTR_ATTRCACHE_ROOTDIR_MIN,
        NFS_MATTR_ATTRCACHE_ROOTDIR_MAX,
    ];
    let flags = bits::<1>(&[NFS_MFLAG_SOFT, NFS_MFLAG_INTR, NFS_MFLAG_NFC]);
    let mut x = Xdr::default();
    x.u32(NFS_ARGSVERSION_XDR);
    let args_len = x.0.len();
    x.u32(0);
    x.u32(NFS_XDRARGS_VERSION_0);
    x.bitmap(&bits::<2>(&attrs));
    let attrs_len = x.0.len();
    x.u32(0);
    x.bitmap(&flags); // which flags are given
    x.bitmap(&flags); // and their values
    x.u32(3); // vers=3
    x.u32(1 << 20); // rsize
    x.u32(1 << 20); // wsize
    x.u32(64 << 10); // dsize
    x.u32(16); // readahead
    for _ in 0..4 {
        x.time(1); // actimeo=1: regular files and directories, min and max
    }
    x.u32(NFS_LOCK_MODE_LOCAL);
    x.string("tcp");
    x.u32(port.into()); // NFS
    x.u32(port.into()); // MOUNT
    x.time(1); // timeo=10, in tenths of a second
    x.u32(3); // retrans
    x.time(30); // deadtimeout
    // One location: one server with one address, and the export's root.
    x.u32(1);
    x.u32(1);
    x.string(SERVER);
    x.u32(1);
    x.string(ADDRESS);
    x.u32(0); // server info
    x.u32(0); // path components
    x.u32(0); // location info
    x.u32(MNT_FLAGS as u32);
    x.string(&source());
    x.time(1); // actimeo=1 for the root directory, min and max
    x.time(1);
    let end = x.0.len();
    x.patch(args_len, (end - args_len + 4) as u32);
    x.patch(attrs_len, (end - attrs_len - 4) as u32);
    x.0
}

/// `<state>/run/nfs/nfs.port`, where libkrun writes its port, in a
/// directory only this user can enter.
pub fn port_file(run_dir: &Path) -> anyhow::Result<PathBuf> {
    let dir = run_dir.join("nfs");
    std::fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
    let meta =
        std::fs::symlink_metadata(&dir).with_context(|| format!("inspect {}", dir.display()))?;
    if !meta.is_dir() {
        bail!("{} is not a directory", dir.display());
    }
    if meta.uid() != unsafe { libc::getuid() } {
        bail!("{} belongs to another user", dir.display());
    }
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))?;
    Ok(dir.join("nfs.port"))
}

fn read_port(port_file: &Path) -> anyhow::Result<u16> {
    let s = std::fs::read_to_string(port_file)
        .with_context(|| format!("read {}", port_file.display()))?;
    s.trim()
        .parse()
        .with_context(|| format!("bad port in {}", port_file.display()))
}

/// Where to mount: `--mount`, then `$KATAMARAN_MOUNT`, then `~/Katamaran`
/// for the default state directory. An instance with its own
/// `KATAMARAN_HOME` (a test or a second copy) mounts nothing unless told
/// where, so it never takes `~/Katamaran` from the main one.
pub fn mount_point(explicit: Option<&Path>) -> Option<PathBuf> {
    if let Some(p) = explicit {
        return Some(p.to_path_buf());
    }
    if let Some(p) = std::env::var_os("KATAMARAN_MOUNT") {
        return Some(PathBuf::from(p));
    }
    if std::env::var_os("KATAMARAN_HOME").is_some() {
        return None;
    }
    std::env::var_os("HOME").map(|h| PathBuf::from(h).join("Katamaran"))
}

/// A mount, as the kernel lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mounted {
    pub fstype: String,
    pub from: String,
}

impl Mounted {
    /// Ours, on a mount point that is ours: NFS from `Katamaran:/`, from
    /// `127.0.0.1:/` (the `mount_nfs` fallback), or from the Unix socket
    /// next to `port_file` that earlier versions mounted (`<socket>:/`), so
    /// that their mount is cleaned up too. A TCP mount's source does not
    /// say which instance it belongs to; each instance has a mount point of
    /// its own.
    pub fn is_ours(&self, port_file: &Path) -> bool {
        let old = port_file.with_file_name("nfs.sock");
        self.fstype == "nfs"
            && (self.from == source()
                || self.from == format!("{ADDRESS}:/")
                || self.from == format!("<{}>:/", old.display()))
    }
}

/// Every mount: (mounted on, what). Never blocks on a dead server.
pub fn mounts() -> Vec<(String, Mounted)> {
    fn text(c: &[libc::c_char]) -> String {
        let b: Vec<u8> = c
            .iter()
            .take_while(|&&x| x != 0)
            .map(|&x| x as u8)
            .collect();
        String::from_utf8_lossy(&b).into_owned()
    }
    for _ in 0..4 {
        let n = unsafe { libc::getfsstat(std::ptr::null_mut(), 0, libc::MNT_NOWAIT) };
        if n < 0 {
            return Vec::new();
        }
        let cap = n as usize + 8;
        let mut buf: Vec<libc::statfs> = vec![unsafe { std::mem::zeroed() }; cap];
        let size = (cap * std::mem::size_of::<libc::statfs>()) as libc::c_int;
        let got = unsafe { libc::getfsstat(buf.as_mut_ptr(), size, libc::MNT_NOWAIT) };
        if got < 0 {
            return Vec::new();
        }
        if (got as usize) < cap {
            buf.truncate(got as usize);
            return buf
                .iter()
                .map(|s| {
                    let m = Mounted {
                        fstype: text(&s.f_fstypename),
                        from: text(&s.f_mntfromname),
                    };
                    (text(&s.f_mntonname), m)
                })
                .collect();
        }
    }
    Vec::new()
}

/// The kernel lists a mount point by its real path; resolve the parent
/// only, never the mount point itself.
fn table_path(dir: &Path) -> PathBuf {
    match (
        dir.parent().and_then(|p| p.canonicalize().ok()),
        dir.file_name(),
    ) {
        (Some(p), Some(n)) => p.join(n),
        _ => dir.to_path_buf(),
    }
}

/// What is mounted on `dir`, if anything. Names compare without case, as
/// the Mac's disks do by default.
pub fn mounted_at(dir: &Path) -> Option<Mounted> {
    let want = table_path(dir);
    let want = want.to_string_lossy();
    let want = want.trim_end_matches('/');
    mounts()
        .into_iter()
        .find(|(on, _)| on.trim_end_matches('/').eq_ignore_ascii_case(want))
        .map(|(_, m)| m)
}

/// Remove a mount of ours left on `dir` by a daemon that crashed: forced,
/// since its server is gone. Anything else mounted there is left alone and
/// is an error.
pub fn recover(dir: &Path, port_file: &Path) -> anyhow::Result<bool> {
    match mounted_at(dir) {
        None => Ok(false),
        Some(m) if m.is_ours(port_file) => {
            unmount_now(dir, true)?;
            Ok(true)
        }
        Some(m) => bail!(
            "{} already has {} mounted on it ({}), leaving it alone",
            dir.display(),
            m.from,
            m.fstype
        ),
    }
}

/// Mount the export on `dir`, creating it if needed. Refuses a directory
/// with files of its own in it (Finder's litter aside): `~/katamaran` is the
/// same folder on a case-insensitive disk.
#[allow(dead_code)]
pub fn mount(dir: &Path, port_file: &Path) -> anyhow::Result<Duration> {
    mount_cancellable(dir, port_file, &AtomicBool::new(false))
}

/// Coordinates mount attempts per mount point so a new attempt waits until
/// any earlier abandoned `mount(2)` finishes and cleans up its own mount.
static IN_FLIGHT: LazyLock<Mutex<HashMap<String, Arc<InFlightMount>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

struct InFlightMount {
    done: Mutex<bool>,
    cv: Condvar,
}

impl InFlightMount {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            done: Mutex::new(false),
            cv: Condvar::new(),
        })
    }

    fn mark_done(&self) {
        let mut d = self.done.lock().unwrap_or_else(|e| e.into_inner());
        *d = true;
        self.cv.notify_all();
    }

    fn wait(
        &self,
        cancel: &AtomicBool,
        deadline: Instant,
        timeout: Duration,
    ) -> Result<(), MountSyscallErr> {
        let mut d = self.done.lock().unwrap_or_else(|e| e.into_inner());
        while !*d {
            if cancel.load(Ordering::Acquire) {
                return Err(MountSyscallErr::Cancelled);
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(MountSyscallErr::TimedOut(anyhow::anyhow!(
                    "previous mount(2) still in flight after {}s",
                    timeout.as_secs()
                )));
            }
            let (guard, _) = self
                .cv
                .wait_timeout(d, left.min(Duration::from_millis(50)))
                .unwrap_or_else(|e| e.into_inner());
            d = guard;
        }
        Ok(())
    }
}

struct MountSlotGuard {
    key: String,
    slot: Arc<InFlightMount>,
}

impl Drop for MountSlotGuard {
    fn drop(&mut self) {
        {
            let mut map = IN_FLIGHT.lock().unwrap_or_else(|e| e.into_inner());
            if map
                .get(&self.key)
                .is_some_and(|cur| Arc::ptr_eq(cur, &self.slot))
            {
                map.remove(&self.key);
            }
        }
        self.slot.mark_done();
    }
}

/// Resolve `dir` by canonicalizing the longest existing ancestor strictly
/// above `dir` (never stating `dir` itself, which may hold a dead NFS mount),
/// so the key stays identical before and after missing parent directories are
/// created.
fn stable_mount_path(dir: &Path) -> PathBuf {
    let abs = std::path::absolute(dir).unwrap_or_else(|_| dir.to_path_buf());
    let Some(name) = abs.file_name().map(std::ffi::OsStr::to_os_string) else {
        return abs;
    };
    let mut tail = vec![name];
    let mut cur = match abs.parent() {
        Some(p) => p.to_path_buf(),
        None => return abs,
    };
    loop {
        if let Ok(canon) = cur.canonicalize() {
            let mut out = canon;
            for part in tail.into_iter().rev() {
                out.push(part);
            }
            return out;
        }
        match cur.file_name() {
            Some(n) => {
                tail.push(n.to_os_string());
                if !cur.pop() {
                    return abs;
                }
            }
            None => return abs,
        }
    }
}

fn mount_slot_key(dir: &Path) -> String {
    stable_mount_path(dir)
        .to_string_lossy()
        .trim_end_matches('/')
        .to_ascii_lowercase()
}

fn acquire_mount_slot(
    dir: &Path,
    cancel: &AtomicBool,
    deadline: Instant,
    timeout: Duration,
) -> Result<Arc<MountSlotGuard>, MountSyscallErr> {
    let key = mount_slot_key(dir);
    loop {
        if cancel.load(Ordering::Acquire) {
            return Err(MountSyscallErr::Cancelled);
        }
        let existing = {
            let mut map = IN_FLIGHT.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(prev) = map.get(&key) {
                Some(prev.clone())
            } else {
                let slot = InFlightMount::new();
                map.insert(key.clone(), slot.clone());
                return Ok(Arc::new(MountSlotGuard { key, slot }));
            }
        };
        if let Some(prev) = existing {
            prev.wait(cancel, deadline, timeout)?;
        }
    }
}

/// Like [`mount`], but aborts promptly if `cancel` becomes `true`.
pub fn mount_cancellable(
    dir: &Path,
    port_file: &Path,
    cancel: &AtomicBool,
) -> anyhow::Result<Duration> {
    let t = Instant::now();
    let deadline = t + MOUNT_TIMEOUT;
    if cancel.load(Ordering::Acquire) {
        bail!("mount cancelled");
    }
    let slot = match acquire_mount_slot(dir, cancel, deadline, MOUNT_TIMEOUT) {
        Ok(s) => s,
        Err(MountSyscallErr::TimedOut(e)) => return Err(e),
        Err(MountSyscallErr::Cancelled) => bail!("mount cancelled"),
        Err(MountSyscallErr::Os(e)) => return Err(e),
    };
    recover(dir, port_file)?;
    check_folder(dir)?;
    std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
    let port = read_port(port_file)?;
    if cancel.load(Ordering::Acquire) {
        bail!("mount cancelled");
    }
    match mount_syscall(dir, port_file, port, cancel, slot.clone(), deadline) {
        Ok(()) => {}
        Err(MountSyscallErr::Os(e)) => {
            eprintln!(
                "katamaran: mount(2) of {} failed, trying mount_nfs: {e:#}",
                dir.display()
            );
            mount_nfs(dir, port, cancel)?;
        }
        Err(MountSyscallErr::TimedOut(e)) => return Err(e),
        Err(MountSyscallErr::Cancelled) => bail!("mount cancelled"),
    }
    if !mounted_at(dir).is_some_and(|m| m.is_ours(port_file)) {
        bail!("mounted but {} is not ours", dir.display());
    }
    Ok(t.elapsed())
}

#[derive(Debug)]
enum MountSyscallErr {
    Os(anyhow::Error),
    TimedOut(anyhow::Error),
    Cancelled,
}

enum MountOutcome {
    Pending,
    Abandoned,
    Finished(std::io::Result<()>),
}

/// Mount with `mount(2)`, which, unlike `mount_nfs`, can name the source.
/// It runs on its own thread so that a server that never answers costs
/// [`MOUNT_TIMEOUT`], not the daemon.
fn mount_syscall(
    dir: &Path,
    port_file: &Path,
    port: u16,
    cancel: &AtomicBool,
    slot: Arc<MountSlotGuard>,
    deadline: Instant,
) -> Result<(), MountSyscallErr> {
    let on_path = dir
        .canonicalize()
        .with_context(|| format!("resolve {}", dir.display()))
        .map_err(MountSyscallErr::Os)?;
    let on = CString::new(on_path.clone().into_os_string().into_encoded_bytes())
        .map_err(|e| MountSyscallErr::Os(e.into()))?;
    let args = mount_args(port);
    let port_file_owned = port_file.to_path_buf();
    let on_path_cleanup = on_path.clone();
    mount_syscall_with(
        slot,
        cancel,
        deadline,
        MOUNT_TIMEOUT,
        move || {
            let r = unsafe {
                libc::mount(
                    c"nfs".as_ptr(),
                    on.as_ptr(),
                    MNT_FLAGS,
                    args.as_ptr() as *mut libc::c_void,
                )
            };
            if r == 0 {
                Ok(())
            } else {
                Err(std::io::Error::last_os_error())
            }
        },
        move || {
            let _ = unmount_force(&on_path_cleanup, &port_file_owned);
        },
    )
}

fn mount_syscall_with<M, U>(
    slot: Arc<MountSlotGuard>,
    cancel: &AtomicBool,
    deadline: Instant,
    timeout: Duration,
    mount_fn: M,
    unmount_fn: U,
) -> Result<(), MountSyscallErr>
where
    M: FnOnce() -> std::io::Result<()> + Send + 'static,
    U: Fn() + Send + Sync + 'static,
{
    let state = Arc::new((Mutex::new(MountOutcome::Pending), Condvar::new()));
    let state_bg = state.clone();
    let slot_bg = slot.clone();
    let unmount = Arc::new(unmount_fn);
    let unmount_bg = unmount.clone();
    std::thread::Builder::new()
        .name("nfs-mount".into())
        .spawn(move || {
            // Keep `slot_bg` alive until `mount_fn` and any abandoned cleanup
            // have completed so no newer mount attempt can run concurrently.
            let _slot = slot_bg;
            let res = mount_fn();
            let succeeded = res.is_ok();
            let (lock, cv) = &*state_bg;
            let was_abandoned = {
                let mut outcome = lock.lock().unwrap_or_else(|e| e.into_inner());
                match &*outcome {
                    MountOutcome::Abandoned => true,
                    _ => {
                        *outcome = MountOutcome::Finished(res);
                        cv.notify_all();
                        false
                    }
                }
            };
            if was_abandoned && succeeded {
                unmount_bg();
            }
        })
        .map_err(|e| MountSyscallErr::Os(e.into()))?;

    let (lock, cv) = &*state;
    let mut outcome = lock.lock().unwrap_or_else(|e| e.into_inner());
    loop {
        match std::mem::replace(&mut *outcome, MountOutcome::Pending) {
            MountOutcome::Finished(Ok(())) => {
                if cancel.load(Ordering::Acquire) {
                    drop(outcome);
                    unmount();
                    return Err(MountSyscallErr::Cancelled);
                }
                return Ok(());
            }
            MountOutcome::Finished(Err(e)) => return Err(MountSyscallErr::Os(e.into())),
            MountOutcome::Abandoned => return Err(MountSyscallErr::Cancelled),
            MountOutcome::Pending => {}
        }
        if cancel.load(Ordering::Acquire) {
            *outcome = MountOutcome::Abandoned;
            return Err(MountSyscallErr::Cancelled);
        }
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            *outcome = MountOutcome::Abandoned;
            return Err(MountSyscallErr::TimedOut(anyhow::anyhow!(
                "mount(2) did not finish in {}s",
                timeout.as_secs()
            )));
        }
        let (guard, _) = cv
            .wait_timeout(outcome, left.min(Duration::from_millis(50)))
            .unwrap_or_else(|e| e.into_inner());
        outcome = guard;
    }
}

fn mount_nfs(dir: &Path, port: u16, cancel: &AtomicBool) -> anyhow::Result<()> {
    let mut child = Command::new("/sbin/mount_nfs")
        .arg("-o")
        .arg(options(port))
        .arg(format!("{ADDRESS}:/"))
        .arg(dir)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .context("run /sbin/mount_nfs")?;
    let deadline = Instant::now() + MOUNT_TIMEOUT;
    let status = loop {
        if let Some(s) = child.try_wait()? {
            break s;
        }
        if cancel.load(Ordering::Acquire) {
            child.kill().ok();
            child.wait().ok();
            bail!("mount_nfs cancelled");
        }
        if Instant::now() >= deadline {
            child.kill().ok();
            child.wait().ok();
            bail!("mount_nfs did not finish in {}s", MOUNT_TIMEOUT.as_secs());
        }
        std::thread::sleep(Duration::from_millis(2));
    };
    if !status.success() {
        let mut err = String::new();
        if let Some(mut e) = child.stderr.take() {
            std::io::Read::read_to_string(&mut e, &mut err).ok();
        }
        bail!("mount_nfs failed ({status}): {}", err.trim());
    }
    Ok(())
}

fn check_folder(dir: &Path) -> anyhow::Result<()> {
    let meta = match std::fs::symlink_metadata(dir) {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e).with_context(|| format!("inspect {}", dir.display())),
    };
    if !meta.is_dir() {
        bail!("{} is not a directory", dir.display());
    }
    for e in std::fs::read_dir(dir)? {
        let name = e?.file_name();
        if name != ".DS_Store" && name != ".localized" {
            bail!(
                "{} is not empty ({} is in it); not mounting over it",
                dir.display(),
                name.to_string_lossy()
            );
        }
    }
    Ok(())
}

/// Unmount our mount on `dir`, if it is there: politely first, forced if
/// something (a shell's cwd, an open Finder window) keeps it busy, because
/// the server is about to go away. Returns whether a mount was removed.
pub fn unmount(dir: &Path, port_file: &Path) -> anyhow::Result<bool> {
    unmount_impl(dir, port_file, false)
}

/// Force-unmount our mount on `dir` immediately (for when the VM has already
/// died and a polite unmount would stall against a dead server).
pub fn unmount_force(dir: &Path, port_file: &Path) -> anyhow::Result<bool> {
    unmount_impl(dir, port_file, true)
}

fn unmount_impl(dir: &Path, port_file: &Path, force: bool) -> anyhow::Result<bool> {
    if !mounted_at(dir).is_some_and(|m| m.is_ours(port_file)) {
        return Ok(false);
    }
    if force || unmount_now(dir, false).is_err() {
        unmount_now(dir, true)?;
    }
    Ok(true)
}

fn unmount_now(dir: &Path, force: bool) -> anyhow::Result<()> {
    let p = CString::new(table_path(dir).into_os_string().into_encoded_bytes())?;
    let flags = if force { libc::MNT_FORCE } else { 0 };
    if unsafe { libc::unmount(p.as_ptr(), flags) } != 0 {
        let e = std::io::Error::last_os_error();
        bail!("unmount {}: {e}", dir.display());
    }
    if mounted_at(dir).is_some() {
        bail!("{} is still mounted", dir.display());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn options_name_the_port_for_both_programs() {
        let o = options(51234);
        assert!(o.contains("proto=tcp,port=51234,mountport=51234"));
        for want in [
            "vers=3",
            "soft",
            "intr",
            "deadtimeout=30",
            "locallocks",
            "actimeo=1",
            "noatime",
        ] {
            assert!(o.split(',').any(|x| x == want), "{want} missing from {o}");
        }
    }

    /// Reads back what [`Xdr`] writes.
    struct Reader<'a>(&'a [u8]);

    impl Reader<'_> {
        fn u32(&mut self) -> u32 {
            let (w, rest) = self.0.split_at(4);
            self.0 = rest;
            u32::from_be_bytes(w.try_into().unwrap())
        }
        fn time(&mut self) -> (u32, u32) {
            (self.u32(), self.u32())
        }
        fn string(&mut self) -> String {
            let n = self.u32() as usize;
            let (s, rest) = self.0.split_at(n.next_multiple_of(4));
            assert!(s[n..].iter().all(|&b| b == 0), "padding is zeros");
            self.0 = rest;
            String::from_utf8(s[..n].to_vec()).unwrap()
        }
        fn bitmap(&mut self) -> Vec<u32> {
            let n = self.u32();
            (0..n).map(|_| self.u32()).collect()
        }
    }

    #[test]
    fn xdr_strings_are_padded_to_words() {
        let mut x = Xdr::default();
        x.string("tcp");
        x.string("");
        x.string("abcd");
        assert_eq!(
            x.0,
            [
                0, 0, 0, 3, b't', b'c', b'p', 0, 0, 0, 0, 0, 0, 0, 0, 4, b'a', b'b', b'c', b'd'
            ]
        );
        assert_eq!(bits::<2>(&[0, 1, 31, 33]), [0x8000_0003, 0b10]);
    }

    #[test]
    fn mount_args_carry_every_option_in_bit_order() {
        let a = mount_args(51234);
        assert_eq!(a.len() % 4, 0);
        let mut r = Reader(&a);
        assert_eq!(r.u32(), NFS_ARGSVERSION_XDR);
        assert_eq!(
            r.u32() as usize,
            a.len(),
            "args length covers the whole buffer"
        );
        assert_eq!(r.u32(), NFS_XDRARGS_VERSION_0);
        let attrs = r.bitmap();
        assert_eq!(attrs.len(), 2);
        let set = |i: u32| attrs[(i / 32) as usize] & (1 << (i % 32)) != 0;
        let want = [
            0, 1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 14, 15, 16, 17, 18, 19, 21, 22, 23, 33, 34,
        ];
        for i in 0..64 {
            assert_eq!(set(i), want.contains(&i), "attribute {i}");
        }
        assert_eq!(r.u32() as usize, r.0.len(), "attrs length covers the rest");
        let flags = 1 | 1 << 1 | 1 << 14; // soft, intr, nfc
        assert_eq!(r.bitmap(), [flags]);
        assert_eq!(r.bitmap(), [flags]);
        assert_eq!(r.u32(), 3);
        assert_eq!(r.u32(), 1048576);
        assert_eq!(r.u32(), 1048576);
        assert_eq!(r.u32(), 65536);
        assert_eq!(r.u32(), 16);
        for _ in 0..4 {
            assert_eq!(r.time(), (1, 0));
        }
        assert_eq!(r.u32(), NFS_LOCK_MODE_LOCAL);
        assert_eq!(r.string(), "tcp");
        assert_eq!(r.u32(), 51234);
        assert_eq!(r.u32(), 51234);
        assert_eq!(r.time(), (1, 0), "timeo=10 is one second");
        assert_eq!(r.u32(), 3);
        assert_eq!(r.time(), (30, 0));
        assert_eq!(r.u32(), 1, "locations");
        assert_eq!(r.u32(), 1, "servers");
        assert_eq!(r.string(), "Katamaran");
        assert_eq!(r.u32(), 1, "addresses");
        assert_eq!(r.string(), "127.0.0.1");
        assert_eq!(r.u32(), 0, "server info");
        assert_eq!(r.u32(), 0, "path components");
        assert_eq!(r.u32(), 0, "location info");
        assert_eq!(
            r.u32() as i32,
            libc::MNT_NOATIME | libc::MNT_NODEV | libc::MNT_NOSUID
        );
        assert_eq!(r.string(), "Katamaran:/");
        assert_eq!(r.time(), (1, 0));
        assert_eq!(r.time(), (1, 0));
        assert!(r.0.is_empty());
    }

    #[test]
    fn knows_its_own_mount() {
        let port_file = Path::new("/Users/a/.katamaran/run/nfs/nfs.port");
        let m = |t: &str, f: &str| Mounted {
            fstype: t.into(),
            from: f.into(),
        };
        assert!(m("nfs", "Katamaran:/").is_ours(port_file));
        assert!(m("nfs", "127.0.0.1:/").is_ours(port_file));
        assert!(m("nfs", "</Users/a/.katamaran/run/nfs/nfs.sock>:/").is_ours(port_file));
        assert!(!m("nfs", "</Users/a/.other/run/nfs/nfs.sock>:/").is_ours(port_file));
        assert!(!m("nfs", "Other:/Other").is_ours(port_file));
        assert!(!m("nfs", "Katamaran:/x").is_ours(port_file));
        assert!(!m("apfs", "Katamaran:/").is_ours(port_file));
    }

    #[test]
    fn reads_the_mount_table() {
        assert!(mounts().iter().any(|(on, _)| on == "/"));
        assert!(mounted_at(Path::new("/")).is_some());
        let d = std::env::temp_dir().join(format!("kat-nfs-mt-{}", std::process::id()));
        assert_eq!(mounted_at(&d), None);
        assert!(!recover(&d, Path::new("/x/nfs.port")).unwrap());
        assert!(!unmount(&d, Path::new("/x/nfs.port")).unwrap());
        // Something else mounted there is not ours to remove.
        assert!(recover(Path::new("/"), Path::new("/x/nfs.port")).is_err());
    }

    #[test]
    fn refuses_folders_with_files_in_them() {
        let d = std::env::temp_dir().join(format!("kat-nfs-folder-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        check_folder(&d).unwrap();
        std::fs::create_dir_all(&d).unwrap();
        std::fs::write(d.join(".DS_Store"), b"").unwrap();
        check_folder(&d).unwrap();
        std::fs::write(d.join("notes.txt"), b"").unwrap();
        assert!(check_folder(&d).is_err());
        std::fs::remove_dir_all(&d).unwrap();
        std::fs::write(&d, b"").unwrap();
        assert!(check_folder(&d).is_err());
        std::fs::remove_file(&d).unwrap();
    }

    #[test]
    fn port_file_lives_in_a_private_directory() {
        let d = std::env::temp_dir().join(format!("kat-nfs-port-{}", std::process::id()));
        let p = port_file(&d).unwrap();
        assert_eq!(p, d.join("nfs/nfs.port"));
        let mode = std::fs::metadata(d.join("nfs"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o700);
        assert!(read_port(&p).is_err());
        std::fs::write(&p, "51234\n").unwrap();
        assert_eq!(read_port(&p).unwrap(), 51234);
        std::fs::remove_dir_all(&d).unwrap();
    }

    #[test]
    fn port_file_rejects_symlink_directory() {
        let d = std::env::temp_dir().join(format!("kat-nfs-sym-{}", std::process::id()));
        let target = std::env::temp_dir().join(format!("kat-nfs-sym-t-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        let _ = std::fs::remove_dir_all(&target);
        std::fs::create_dir_all(&d).unwrap();
        std::fs::create_dir_all(&target).unwrap();
        std::os::unix::fs::symlink(&target, d.join("nfs")).unwrap();
        assert!(port_file(&d).is_err());
        std::fs::remove_dir_all(&d).unwrap();
        std::fs::remove_dir_all(&target).unwrap();
    }

    #[test]
    fn abandoned_mount_cleans_up_before_newer_attempt_proceeds() {
        let dir = std::env::temp_dir().join(format!("kat-nfs-inflight-{}", std::process::id()));
        let cancel = AtomicBool::new(false);
        let events = Arc::new(Mutex::new(Vec::<&'static str>::new()));

        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel::<()>();

        // Attempt 1: times out while its mount syscall is still blocked.
        let t1 = Duration::from_millis(50);
        let d1 = Instant::now() + t1;
        let slot1 = acquire_mount_slot(&dir, &cancel, d1, t1).unwrap();
        let ev1_m = events.clone();
        let ev1_u = events.clone();
        let res1 = mount_syscall_with(
            slot1,
            &cancel,
            d1,
            t1,
            move || {
                let _ = entered_tx.send(());
                let _ = release_rx.recv();
                ev1_m.lock().unwrap().push("mount1_done");
                Ok(())
            },
            move || {
                ev1_u.lock().unwrap().push("unmount1_done");
            },
        );
        assert!(matches!(res1, Err(MountSyscallErr::TimedOut(_))));
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();

        // Attempt 2 starts on the same mount point while Attempt 1 is still in
        // flight. It must wait until Attempt 1 finishes and cleans up.
        let dir2 = dir.clone();
        let ev2_m = events.clone();
        let ev2_u = events.clone();
        let attempt2 = std::thread::spawn(move || {
            let cancel2 = AtomicBool::new(false);
            let t2 = Duration::from_secs(5);
            let d2 = Instant::now() + t2;
            let slot2 = acquire_mount_slot(&dir2, &cancel2, d2, t2).unwrap();
            mount_syscall_with(
                slot2,
                &cancel2,
                d2,
                t2,
                move || {
                    ev2_m.lock().unwrap().push("mount2_done");
                    Ok(())
                },
                move || {
                    ev2_u.lock().unwrap().push("unmount2_done");
                },
            )
            .is_ok()
        });

        // Give attempt 2 time to block on the in-flight slot; it must not have
        // executed its mount yet.
        std::thread::sleep(Duration::from_millis(50));
        assert!(events.lock().unwrap().is_empty());

        // Now Attempt 1's late mount syscall succeeds. It must clean up its own
        // mount before Attempt 2 runs, and Attempt 2's mount must stay intact.
        release_tx.send(()).unwrap();
        assert!(attempt2.join().unwrap());
        assert_eq!(
            *events.lock().unwrap(),
            vec!["mount1_done", "unmount1_done", "mount2_done"]
        );
    }

    #[test]
    fn mount_slot_key_is_stable_before_and_after_parent_creation() {
        let base = std::env::temp_dir().join(format!("kat-nfs-key-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        let real = base.join("real");
        let link = base.join("link");
        std::fs::create_dir_all(&real).unwrap();
        std::os::unix::fs::symlink(&real, &link).unwrap();

        let mount_via_link = link.join("missing/nested/Katamaran");
        let mount_via_real = real.join("missing/nested/Katamaran");
        let key_before = mount_slot_key(&mount_via_link);
        assert_eq!(key_before, mount_slot_key(&mount_via_real));

        std::fs::create_dir_all(&mount_via_link).unwrap();
        let key_after = mount_slot_key(&mount_via_link);
        assert_eq!(key_before, key_after);
        assert_eq!(key_after, mount_slot_key(&mount_via_real));

        std::fs::remove_dir_all(&base).unwrap();
    }
}
