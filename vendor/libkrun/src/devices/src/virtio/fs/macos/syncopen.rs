//! Bounded opens for requests processed on a vCPU thread.
//!
//! An open can block for seconds: a dataless iCloud file is downloaded first,
//! a FIFO waits for a writer, a stalled network volume waits for the server.
//! While a request is processed in a bounded scope, opens run on a helper
//! thread and the caller waits at most [`DEADLINE`]. On timeout the open is
//! abandoned (the helper closes the descriptor when it finally returns), the
//! request is retried on the worker pool and the node becomes async-only.
//!
//! Most opens cannot block, and handing them to a helper costs more than the
//! open itself. So the calling thread first tries the open with materializing
//! of dataless files turned off for itself and without waiting for a FIFO's
//! peer; only what that attempt cannot open goes to a helper.

use std::cell::Cell;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::{self, Thread};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender, TryRecvError, unbounded};
use nix::errno::Errno;

pub const DEADLINE: Duration = Duration::from_millis(20);
/// Spin instead of parking while this little time is left, or for this long
/// after starting: most opens finish in a few microseconds, and a park costs
/// a wakeup.
const SPIN: Duration = Duration::from_micros(50);
/// A helper polls for the next job this long before blocking.
const HELPER_SPIN: Duration = Duration::from_micros(250);
/// Helpers stuck in abandoned opens are not reused; stop adding more past this.
const MAX_HELPERS: usize = 64;

#[derive(Clone, Copy)]
struct LastStat {
    fd: c_int,
    mtime: u64,
    dir_stamp: u64,
    hardlinked: bool,
}

thread_local! {
    static BOUNDED: Cell<bool> = const { Cell::new(false) };
    static TIMED_OUT: Cell<bool> = const { Cell::new(false) };
    /// Opens this thread handed to a helper, for tests.
    static HELPER_OPENS: Cell<u64> = const { Cell::new(0) };
    /// Metadata from this thread's last inline open, which already stats the fd.
    static LAST_STAT: Cell<Option<LastStat>> = const { Cell::new(None) };
}

/// The mtime in nanoseconds of `fd` if this thread's last inline open returned it; clears it.
pub fn take_mtime(fd: c_int) -> Option<u64> {
    LAST_STAT
        .with(|m| m.take())
        .and_then(|s| (s.fd == fd).then_some(s.mtime))
}

/// The directory stamp (mtime + ctime + nlink) of `fd` if this thread's last inline open returned it; clears it.
pub fn take_dir_stamp(fd: c_int) -> Option<u64> {
    LAST_STAT
        .with(|m| m.take())
        .and_then(|s| (s.fd == fd).then_some(s.dir_stamp))
}

/// Whether `fd` had `st_nlink > 1` when this thread's last inline open statted it.
pub fn peek_hardlinked(fd: c_int) -> Option<bool> {
    LAST_STAT
        .with(|m| m.get())
        .and_then(|s| (s.fd == fd).then_some(s.hardlinked))
}

/// Forgets the last inline open's stat, before an open that may not go inline.
pub fn clear_mtime() {
    LAST_STAT.with(|m| m.set(None));
}

pub fn mtime_ns(st: &libc::stat) -> u64 {
    (st.st_mtime as u64)
        .wrapping_mul(1_000_000_000)
        .wrapping_add(st.st_mtime_nsec as u64)
}

/// Fingerprint of a directory's contents from its stat: mtime, ctime and entry count (st_nlink).
pub fn dir_stamp(st: &libc::stat) -> u64 {
    let mtime = mtime_ns(st);
    let ctime = (st.st_ctime as u64)
        .wrapping_mul(1_000_000_000)
        .wrapping_add(st.st_ctime_nsec as u64);
    let mut h = mtime
        ^ ctime.rotate_left(21)
        ^ (u64::from(st.st_nlink)).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    h ^= h >> 30;
    h = h.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    h ^= h >> 27;
    if h == 0 { 1 } else { h }
}

// <sys/resource.h>
const IOPOL_TYPE_VFS_MATERIALIZE_DATALESS_FILES: c_int = 3;
const IOPOL_SCOPE_THREAD: c_int = 1;
const IOPOL_MATERIALIZE_DATALESS_FILES_DEFAULT: c_int = 0;
const IOPOL_MATERIALIZE_DATALESS_FILES_OFF: c_int = 1;

unsafe extern "C" {
    fn setiopolicy_np(iotype: c_int, scope: c_int, policy: c_int) -> c_int;
}

/// While alive, opens on this thread go through [`run_bounded`].
pub struct BoundedScope(());

impl Drop for BoundedScope {
    fn drop(&mut self) {
        BOUNDED.with(|b| b.set(false));
    }
}

/// Whether this thread is serving the synchronous path, where opens are bounded.
pub fn in_bounded_scope() -> bool {
    BOUNDED.with(|b| b.get())
}

pub fn bounded_scope() -> BoundedScope {
    BOUNDED.with(|b| b.set(true));
    TIMED_OUT.with(|t| t.set(false));
    BoundedScope(())
}

/// Whether an open in the current scope timed out; clears the flag.
pub fn take_timed_out() -> bool {
    TIMED_OUT.with(|t| t.replace(false))
}

enum State<T> {
    Pending,
    Done(T),
    Abandoned,
}

struct Slot<T> {
    state: Mutex<State<T>>,
    waiter: Thread,
}

type Job = Box<dyn FnOnce() + Send>;

struct Helpers {
    tx: Sender<Job>,
    rx: Receiver<Job>,
    idle: AtomicUsize,
    total: AtomicUsize,
}

fn helpers() -> &'static Helpers {
    static H: OnceLock<Helpers> = OnceLock::new();
    H.get_or_init(|| {
        let (tx, rx) = unbounded();
        Helpers {
            tx,
            rx,
            idle: AtomicUsize::new(0),
            total: AtomicUsize::new(0),
        }
    })
}

fn helper_loop(h: &'static Helpers) {
    super::super::worker::set_interactive_qos();
    loop {
        let spin_until = Instant::now() + HELPER_SPIN;
        let job = loop {
            match h.rx.try_recv() {
                Ok(job) => break Some(job),
                Err(TryRecvError::Disconnected) => return,
                Err(TryRecvError::Empty) if Instant::now() < spin_until => std::hint::spin_loop(),
                Err(TryRecvError::Empty) => break None,
            }
        };
        let job = match job {
            Some(job) => job,
            None => match h.rx.recv() {
                Ok(job) => job,
                Err(_) => return,
            },
        };
        h.idle.fetch_sub(1, Ordering::AcqRel);
        job();
        h.idle.fetch_add(1, Ordering::AcqRel);
    }
}

/// Make sure a helper will pick up the next job. Returns false when too many
/// helpers are stuck in abandoned work.
fn reserve_helper(h: &'static Helpers) -> bool {
    if h.idle.load(Ordering::Acquire) > h.rx.len() {
        return true;
    }
    if h.total.fetch_add(1, Ordering::AcqRel) >= MAX_HELPERS {
        h.total.fetch_sub(1, Ordering::AcqRel);
        return false;
    }
    h.idle.fetch_add(1, Ordering::AcqRel);
    thread::Builder::new()
        .name("fs open".into())
        .spawn(move || helper_loop(h))
        .is_ok()
}

/// Run `work` on a helper thread and wait for it until `deadline`. On
/// timeout returns `None`; `abandon` then receives the result whenever
/// `work` finishes.
pub fn run_bounded<T: Send + 'static>(
    deadline: Duration,
    work: impl FnOnce() -> T + Send + 'static,
    abandon: impl FnOnce(T) + Send + 'static,
) -> Option<T> {
    let h = helpers();
    if !reserve_helper(h) {
        return None;
    }
    let started = Instant::now();
    let end = started + deadline;
    let slot = Arc::new(Slot {
        state: Mutex::new(State::Pending),
        waiter: thread::current(),
    });
    let theirs = slot.clone();
    let job: Job = Box::new(move || {
        let out = work();
        let mut state = theirs.state.lock().unwrap();
        match *state {
            State::Abandoned => {
                drop(state);
                abandon(out);
            }
            _ => {
                *state = State::Done(out);
                drop(state);
                theirs.waiter.unpark();
            }
        }
    });
    if h.tx.send(job).is_err() {
        return None;
    }
    loop {
        {
            let mut state = slot.state.lock().unwrap();
            if let State::Done(_) = *state {
                let State::Done(out) = std::mem::replace(&mut *state, State::Abandoned) else {
                    unreachable!()
                };
                return Some(out);
            }
            let now = Instant::now();
            if now >= end {
                *state = State::Abandoned;
                return None;
            }
        }
        let now = Instant::now();
        let left = end - now;
        if now - started < SPIN || left <= SPIN {
            std::hint::spin_loop();
        } else {
            thread::park_timeout(left - SPIN);
        }
    }
}

/// `open(2)`, bounded by [`DEADLINE`] inside a [`bounded_scope`].
///
/// # Safety
/// `path` must point to a NUL-terminated string.
pub unsafe fn open(path: *const c_char, flags: c_int, mode: c_int) -> c_int {
    if !BOUNDED.with(|b| b.get()) {
        return unsafe { libc::open(path, flags, mode) };
    }
    if let Some(fd) = unsafe { open_inline(path, flags, mode) } {
        return fd;
    }
    HELPER_OPENS.with(|n| n.set(n.get() + 1));
    let owned = unsafe { CStr::from_ptr(path) }.to_owned();
    match open_with_deadline(owned, flags, mode, DEADLINE) {
        Ok(fd) => fd,
        Err(errno) => {
            Errno::set_raw(errno);
            -1
        }
    }
}

/// Open on this thread if that cannot block: a dataless file fails instead of
/// downloading, a FIFO opens without waiting for its peer and is closed again.
/// `None` when the open failed or found something other than a regular file or
/// a directory; the caller then opens it on a helper, as it would block.
///
/// # Safety
/// `path` must point to a NUL-terminated string.
unsafe fn open_inline(path: *const c_char, flags: c_int, mode: c_int) -> Option<c_int> {
    unsafe {
        open_inline_with(flags & libc::O_DIRECTORY != 0, || {
            libc::open(path, flags | libc::O_NONBLOCK, mode)
        })
    }
    .map(|(fd, _)| fd)
}

/// [`open_inline`] of `name` in the directory `dirfd` (a pinned parent): resolving one name
/// in a held directory costs less than resolving `/.vol/<dev>/<ino>`. Returns the descriptor
/// and its stat, for the caller to check that it is still the file it meant.
///
/// # Safety
/// `name` must point to a NUL-terminated string.
pub unsafe fn open_inline_at(
    dirfd: c_int,
    name: *const c_char,
    flags: c_int,
) -> Option<(c_int, libc::stat)> {
    unsafe {
        open_inline_with(flags & libc::O_DIRECTORY != 0, || {
            libc::openat(
                dirfd,
                name,
                flags | libc::O_NONBLOCK | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        })
    }
}

unsafe fn open_inline_with(
    is_dir: bool,
    open: impl FnOnce() -> c_int,
) -> Option<(c_int, libc::stat)> {
    let policy = |p| unsafe {
        setiopolicy_np(
            IOPOL_TYPE_VFS_MATERIALIZE_DATALESS_FILES,
            IOPOL_SCOPE_THREAD,
            p,
        )
    };
    // Directories are never dataless files, and O_DIRECTORY rejects non-directories.
    if !is_dir && policy(IOPOL_MATERIALIZE_DATALESS_FILES_OFF) != 0 {
        return None;
    }
    let fd = open();
    if !is_dir {
        policy(IOPOL_MATERIALIZE_DATALESS_FILES_DEFAULT);
    }
    if fd < 0 {
        return None;
    }
    let mut st = std::mem::MaybeUninit::<libc::stat>::uninit();
    let plain = unsafe { libc::fstat(fd, st.as_mut_ptr()) } == 0
        && matches!(
            unsafe { st.assume_init() }.st_mode & libc::S_IFMT,
            libc::S_IFREG | libc::S_IFDIR
        );
    // O_NONBLOCK does nothing on a regular file or a directory, so it can stay set.
    if plain {
        let st = unsafe { st.assume_init() };
        LAST_STAT.with(|m| {
            m.set(Some(LastStat {
                fd,
                mtime: mtime_ns(&st),
                dir_stamp: dir_stamp(&st),
                hardlinked: (st.st_mode & libc::S_IFMT) != libc::S_IFDIR && st.st_nlink > 1,
            }))
        });
        Some((fd, st))
    } else {
        unsafe { libc::close(fd) };
        None
    }
}

fn open_with_deadline(
    path: CString,
    flags: c_int,
    mode: c_int,
    deadline: Duration,
) -> Result<c_int, c_int> {
    let res = run_bounded(
        deadline,
        move || {
            let fd = unsafe { libc::open(path.as_ptr(), flags, mode) };
            if fd < 0 {
                Err(Errno::last_raw())
            } else {
                Ok(fd)
            }
        },
        |res| {
            if let Ok(fd) = res {
                unsafe { libc::close(fd) };
                debug!("abandoned sync open completed");
            }
        },
    );
    match res {
        Some(r) => r,
        None => {
            debug!("sync open timed out: require async");
            TIMED_OUT.with(|t| t.set(true));
            Err(libc::EAGAIN)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::virtio::fs::macos::test_util::TempDir;
    use std::sync::atomic::AtomicBool;

    fn fifo() -> (TempDir, CString) {
        let dir = TempDir::new();
        let path = CString::new(dir.0.join("fifo").to_str().unwrap()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
        (dir, path)
    }

    #[test]
    fn fast_open_completes_inline() {
        let dir = TempDir::new();
        let path = CString::new(dir.0.join("f").to_str().unwrap()).unwrap();
        let _scope = bounded_scope();
        let fd = unsafe { open(path.as_ptr(), libc::O_CREAT | libc::O_RDWR, 0o600) };
        assert!(fd >= 0);
        assert!(!take_timed_out());
        unsafe { libc::close(fd) };
    }

    fn helper_opens() -> u64 {
        HELPER_OPENS.with(|n| n.get())
    }

    #[test]
    fn regular_files_and_directories_open_without_a_helper() {
        let dir = TempDir::new();
        let path = CString::new(dir.0.join("f").to_str().unwrap()).unwrap();
        let dpath = CString::new(dir.0.to_str().unwrap()).unwrap();
        let _scope = bounded_scope();
        let before = helper_opens();
        let fd = unsafe { open(path.as_ptr(), libc::O_CREAT | libc::O_RDWR, 0o600) };
        assert!(fd >= 0);
        // The inline open stats the file, so its mtime comes along.
        assert!(take_mtime(fd).is_some());
        assert_eq!(take_mtime(fd), None);
        unsafe { libc::close(fd) };
        let fd = unsafe { open(dpath.as_ptr(), libc::O_RDONLY | libc::O_DIRECTORY, 0) };
        assert!(fd >= 0);
        unsafe { libc::close(fd) };
        assert_eq!(helper_opens(), before);
        // Asked for, it stays.
        let fd = unsafe { open(path.as_ptr(), libc::O_RDONLY | libc::O_NONBLOCK, 0) };
        assert_ne!(
            unsafe { libc::fcntl(fd, libc::F_GETFL) } & libc::O_NONBLOCK,
            0
        );
        unsafe { libc::close(fd) };
    }

    #[test]
    fn the_materialization_policy_is_restored() {
        let dir = TempDir::new();
        let path = CString::new(dir.0.join("f").to_str().unwrap()).unwrap();
        let _scope = bounded_scope();
        let fd = unsafe { open(path.as_ptr(), libc::O_CREAT | libc::O_RDWR, 0o600) };
        assert!(fd >= 0);
        unsafe { libc::close(fd) };
        unsafe extern "C" {
            fn getiopolicy_np(iotype: c_int, scope: c_int) -> c_int;
        }
        let p = unsafe {
            getiopolicy_np(
                IOPOL_TYPE_VFS_MATERIALIZE_DATALESS_FILES,
                IOPOL_SCOPE_THREAD,
            )
        };
        assert_eq!(p, IOPOL_MATERIALIZE_DATALESS_FILES_DEFAULT);
    }

    #[test]
    fn a_fifo_goes_to_a_helper_and_times_out() {
        let (_dir, path) = fifo();
        let _scope = bounded_scope();
        let before = helper_opens();
        let started = Instant::now();
        let fd = unsafe { open(path.as_ptr(), libc::O_RDONLY, 0) };
        assert_eq!(fd, -1);
        assert_eq!(Errno::last_raw(), libc::EAGAIN);
        assert!(take_timed_out());
        assert!(started.elapsed() >= DEADLINE);
        assert_eq!(helper_opens(), before + 1);
        let w = unsafe { libc::open(path.as_ptr(), libc::O_WRONLY) };
        assert!(w >= 0);
        unsafe { libc::close(w) };
    }

    #[test]
    fn errors_pass_through() {
        let _scope = bounded_scope();
        let fd = unsafe { open(c"/nonexistent/x".as_ptr(), libc::O_RDONLY, 0) };
        assert_eq!(fd, -1);
        assert_eq!(Errno::last_raw(), libc::ENOENT);
        assert!(!take_timed_out());
    }

    #[test]
    fn stalled_open_times_out_and_is_closed_later() {
        let (_dir, path) = fifo();
        let started = Instant::now();
        // A FIFO opened for reading blocks until a writer appears.
        let res = open_with_deadline(path.clone(), libc::O_RDONLY, 0, DEADLINE);
        let waited = started.elapsed();
        assert_eq!(res, Err(libc::EAGAIN));
        assert!(TIMED_OUT.with(|t| t.replace(false)));
        assert!(waited >= DEADLINE, "{waited:?}");
        assert!(waited < DEADLINE * 10, "{waited:?}");

        // A writer releases the helper, which closes the abandoned descriptor.
        let w = unsafe { libc::open(path.as_ptr(), libc::O_WRONLY) };
        assert!(w >= 0);
        unsafe { libc::close(w) };
    }

    #[test]
    fn unbounded_scope_opens_directly() {
        let (_dir, path) = fifo();
        let fd = unsafe { open(path.as_ptr(), libc::O_RDONLY | libc::O_NONBLOCK, 0) };
        assert!(fd >= 0);
        unsafe { libc::close(fd) };
    }

    #[test]
    fn abandon_receives_the_late_result() {
        let done = Arc::new(AtomicBool::new(false));
        let d = done.clone();
        let res = run_bounded(
            Duration::from_millis(5),
            || {
                thread::sleep(Duration::from_millis(30));
                7
            },
            move |v| {
                assert_eq!(v, 7);
                d.store(true, Ordering::SeqCst);
            },
        );
        assert_eq!(res, None);
        for _ in 0..100 {
            if done.load(Ordering::SeqCst) {
                return;
            }
            thread::sleep(Duration::from_millis(5));
        }
        panic!("abandon callback never ran");
    }

    #[test]
    fn quick_work_returns_its_result() {
        for i in 0..200 {
            assert_eq!(run_bounded(DEADLINE, move || i * 2, |_| {}), Some(i * 2));
        }
    }
}
