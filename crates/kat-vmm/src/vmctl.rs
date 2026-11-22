//! Lifecycle controls of the VM process that are not part of its config:
//! how freed guest memory goes back to macOS, and pausing the vCPUs.
//!
//! The daemon pauses and resumes the VM with signals ([`PAUSE_SIGNAL`],
//! [`RESUME_SIGNAL`]), so it needs no extra socket into the VM process. A
//! process-wide `sigaction` handler writes the signal byte into a non-blocking
//! self-pipe, so GCD worker threads (which do not inherit the main thread's
//! signal mask on macOS) can never be killed by `SIG_DFL`; one thread reads the
//! pipe and asks libkrun's event loop to pause or resume.

use std::io;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::thread;
use std::time::Instant;

use krun::{FreePageReportingMode, VmmHandle};

/// Freeze every vCPU (libkrun holds the guest's virtual counter while paused).
pub const PAUSE_SIGNAL: i32 = libc::SIGUSR1;
/// Run the vCPUs again.
pub const RESUME_SIGNAL: i32 = libc::SIGUSR2;

static SIGNAL_WRITE_FD: AtomicI32 = AtomicI32::new(-1);
static SIGNAL_READ_FD: AtomicI32 = AtomicI32::new(-1);
/// Latest requested control signal (`0`, `PAUSE_SIGNAL`, or `RESUME_SIGNAL`).
/// Updated atomically in the signal handler even if the self-pipe is full so a
/// burst of signals can never discard the final resume/pause request.
static DESIRED_SIGNAL: AtomicI32 = AtomicI32::new(0);
/// Published only after both signal handlers and the self-pipe are initialized.
static HANDLERS_INSTALLED: AtomicBool = AtomicBool::new(false);
static INIT_LOCK: Mutex<()> = Mutex::new(());

#[cfg(target_os = "linux")]
unsafe fn errno() -> *mut libc::c_int {
    unsafe { libc::__errno_location() }
}

#[cfg(not(target_os = "linux"))]
unsafe fn errno() -> *mut libc::c_int {
    unsafe { libc::__error() }
}

extern "C" fn on_signal(sig: libc::c_int) {
    // SAFETY: save and restore thread-local errno across async-signal-safe write().
    let saved = unsafe { *errno() };
    if sig == PAUSE_SIGNAL || sig == RESUME_SIGNAL {
        DESIRED_SIGNAL.store(sig, Ordering::SeqCst);
        let fd = SIGNAL_WRITE_FD.load(Ordering::Acquire);
        if fd >= 0 {
            let byte = sig as u8;
            // SAFETY: write() is async-signal-safe and `byte` is on the stack.
            // The pipe acts as a wakeup; EAGAIN is safe because `DESIRED_SIGNAL`
            // already holds the latest request and unread pipe bytes will wake
            // the reader to reconcile it.
            unsafe { libc::write(fd, (&byte as *const u8).cast(), 1) };
        }
    }
    unsafe { *errno() = saved };
}

fn take_desired_signal() -> Option<i32> {
    let sig = DESIRED_SIGNAL.swap(0, Ordering::SeqCst);
    (sig == PAUSE_SIGNAL || sig == RESUME_SIGNAL).then_some(sig)
}

/// How free page reports go back to macOS: `replace` (default) has libkrun
/// map fresh memory over reported 64 KiB blocks, which releases them even
/// after the guest wrote to them; `madvise` only madvise()s them, which on
/// macOS releases nothing the guest touched; `off` attaches no balloon.
pub fn balloon_mode() -> Option<FreePageReportingMode> {
    parse_balloon_mode(std::env::var("KATAMARAN_BALLOON").ok().as_deref())
}

fn parse_balloon_mode(v: Option<&str>) -> Option<FreePageReportingMode> {
    match v.map(str::trim) {
        Some("off" | "0" | "none") => None,
        Some("madvise" | "default") => Some(FreePageReportingMode::Default),
        _ => Some(FreePageReportingMode::Replace),
    }
}

fn make_action() -> libc::sigaction {
    // SAFETY: sigaction struct is zeroed and initialized with our async-signal-safe handler.
    unsafe {
        let mut sa: libc::sigaction = std::mem::zeroed();
        sa.sa_sigaction = on_signal as *const () as libc::sighandler_t;
        sa.sa_flags = libc::SA_RESTART;
        libc::sigemptyset(&mut sa.sa_mask);
        sa
    }
}

fn sys_sigaction(
    sig: libc::c_int,
    act: &libc::sigaction,
    old: Option<&mut libc::sigaction>,
) -> io::Result<()> {
    let old_ptr = old.map_or(std::ptr::null_mut(), |o| o as *mut libc::sigaction);
    // SAFETY: `act` is a valid initialized sigaction, `old_ptr` is either null or valid.
    if unsafe { libc::sigaction(sig, act, old_ptr) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn init_signals_with(
    installed: &AtomicBool,
    read_slot: &AtomicI32,
    write_slot: &AtomicI32,
    set_action: impl Fn(libc::c_int, &libc::sigaction, Option<&mut libc::sigaction>) -> io::Result<()>,
) -> io::Result<()> {
    if installed.load(Ordering::Acquire) {
        return Ok(());
    }
    let mut fds = [0; 2];
    // SAFETY: pipe() fills the two descriptors.
    if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let [read_fd, write_fd] = fds;
    let close_pipe = || unsafe {
        libc::close(read_fd);
        libc::close(write_fd);
    };
    for fd in fds {
        // SAFETY: the descriptors were just created.
        if unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) } != 0 {
            let e = io::Error::last_os_error();
            close_pipe();
            return Err(e);
        }
    }
    // SAFETY: the write descriptor was just created; non-blocking avoids
    // deadlocking inside the signal handler if the pipe is ever full.
    if unsafe { libc::fcntl(write_fd, libc::F_SETFL, libc::O_NONBLOCK) } != 0 {
        let e = io::Error::last_os_error();
        close_pipe();
        return Err(e);
    }

    let sa = make_action();
    // SAFETY: zeroed storage populated by sigaction's oldact out-parameter.
    let mut old_pause: libc::sigaction = unsafe { std::mem::zeroed() };
    if let Err(e) = set_action(PAUSE_SIGNAL, &sa, Some(&mut old_pause)) {
        close_pipe();
        return Err(e);
    }
    if let Err(e) = set_action(RESUME_SIGNAL, &sa, None) {
        let _ = set_action(PAUSE_SIGNAL, &old_pause, None);
        close_pipe();
        return Err(e);
    }

    // Publish pipe descriptors and ready flag only after both handlers installed.
    read_slot.store(read_fd, Ordering::Release);
    write_slot.store(write_fd, Ordering::Release);
    installed.store(true, Ordering::Release);
    // If a signal arrived while handlers were being installed, ensure the pipe has a wakeup byte.
    if DESIRED_SIGNAL.load(Ordering::SeqCst) != 0 {
        let byte = 1u8;
        // SAFETY: write_fd is valid and non-blocking.
        unsafe { libc::write(write_fd, (&byte as *const u8).cast(), 1) };
    }
    Ok(())
}

/// Install the async-signal-safe self-pipe handlers for the control signals so
/// no thread (including GCD worker threads) can ever terminate on `SIG_DFL`.
/// Must run before libkrun or FSEvents starts its threads.
pub fn block_signals() -> io::Result<()> {
    if HANDLERS_INSTALLED.load(Ordering::Acquire) {
        return Ok(());
    }
    let _guard = INIT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    init_signals_with(
        &HANDLERS_INSTALLED,
        &SIGNAL_READ_FD,
        &SIGNAL_WRITE_FD,
        sys_sigaction,
    )
}

fn serve_loop(read_fd: libc::c_int, mut apply: impl FnMut(i32)) {
    let mut buf = [0u8; 256];
    loop {
        while let Some(sig) = take_desired_signal() {
            apply(sig);
        }
        // SAFETY: reads into a local stack buffer of that length.
        let n = unsafe { libc::read(read_fd, buf.as_mut_ptr().cast(), buf.len()) };
        if n < 0 {
            if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                continue;
            }
            break;
        }
        if n == 0 {
            break;
        }
    }
    // Reconcile any last signal recorded right before EOF/exit.
    while let Some(sig) = take_desired_signal() {
        apply(sig);
    }
    unsafe { libc::close(read_fd) };
}

/// Take the control signals on a thread of their own for the life of the
/// process.
pub fn serve_signals(handle: VmmHandle) -> io::Result<()> {
    block_signals()?;
    let read_fd = SIGNAL_READ_FD.swap(-1, Ordering::AcqRel);
    if read_fd < 0 {
        return Err(io::Error::other("control signal pipe already served"));
    }
    if let Err(e) = thread::Builder::new().name("vmctl".into()).spawn(move || {
        serve_loop(read_fd, |sig| {
            let t = Instant::now();
            let (what, r) = if sig == PAUSE_SIGNAL {
                ("pause", handle.pause())
            } else {
                ("resume", handle.resume())
            };
            match r {
                Ok(()) => eprintln!(
                    "katamaran-vm: {what} requested ({:.2} ms)",
                    t.elapsed().as_secs_f64() * 1e3
                ),
                Err(e) => eprintln!("katamaran-vm: {what} failed: {e}"),
            }
        });
    }) {
        SIGNAL_READ_FD.store(read_fd, Ordering::Release);
        return Err(e);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn balloon_defaults_to_replace() {
        assert_eq!(
            parse_balloon_mode(None),
            Some(FreePageReportingMode::Replace)
        );
        assert_eq!(
            parse_balloon_mode(Some("replace")),
            Some(FreePageReportingMode::Replace)
        );
        assert_eq!(
            parse_balloon_mode(Some("madvise")),
            Some(FreePageReportingMode::Default)
        );
        assert_eq!(parse_balloon_mode(Some("off")), None);
    }

    #[test]
    fn failed_second_handler_rolls_back_and_does_not_publish() {
        let installed = AtomicBool::new(false);
        let read_slot = AtomicI32::new(-1);
        let write_slot = AtomicI32::new(-1);
        let calls = Mutex::new(Vec::<i32>::new());

        let err = init_signals_with(&installed, &read_slot, &write_slot, |sig, _act, old| {
            let mut c = calls.lock().unwrap();
            c.push(sig);
            if sig == RESUME_SIGNAL {
                return Err(io::Error::from_raw_os_error(libc::EINVAL));
            }
            if let Some(o) = old {
                *o = unsafe { std::mem::zeroed() };
            }
            Ok(())
        })
        .unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::EINVAL));
        assert!(!installed.load(Ordering::Acquire));
        assert_eq!(read_slot.load(Ordering::Acquire), -1);
        assert_eq!(write_slot.load(Ordering::Acquire), -1);
        // Installed PAUSE_SIGNAL, failed on RESUME_SIGNAL, then rolled back PAUSE_SIGNAL.
        assert_eq!(
            *calls.lock().unwrap(),
            [PAUSE_SIGNAL, RESUME_SIGNAL, PAUSE_SIGNAL]
        );

        // A subsequent call retries and succeeds once sigaction works.
        init_signals_with(&installed, &read_slot, &write_slot, |_sig, _act, _old| {
            Ok(())
        })
        .unwrap();
        assert!(installed.load(Ordering::Acquire));
        let rfd = read_slot.swap(-1, Ordering::AcqRel);
        let wfd = write_slot.swap(-1, Ordering::AcqRel);
        assert!(rfd >= 0 && wfd >= 0);
        unsafe {
            libc::close(rfd);
            libc::close(wfd);
        }
    }

    #[test]
    fn signal_handler_writes_pipe_preserves_errno_and_survives_full_pipe() {
        block_signals().unwrap();
        let read_fd = SIGNAL_READ_FD.load(Ordering::Acquire);
        let write_fd = SIGNAL_WRITE_FD.load(Ordering::Acquire);
        assert!(read_fd >= 0 && write_fd >= 0);

        unsafe { *errno() = libc::EAGAIN };
        // Raise both signals to verify the process stays alive, preserves errno,
        // and records the latest desired signal.
        unsafe {
            libc::raise(PAUSE_SIGNAL);
            libc::raise(RESUME_SIGNAL);
        }
        assert_eq!(unsafe { *errno() }, libc::EAGAIN);
        assert_eq!(take_desired_signal(), Some(RESUME_SIGNAL));

        // Fill the non-blocking pipe completely with PAUSE bytes until EAGAIN,
        // then deliver RESUME_SIGNAL: even though write() fails with EAGAIN,
        // the latest desired state (RESUME_SIGNAL) must be preserved and errno intact.
        let chunk = [PAUSE_SIGNAL as u8; 4096];
        loop {
            let n = unsafe { libc::write(write_fd, chunk.as_ptr().cast(), chunk.len()) };
            if n < 0 {
                assert_eq!(io::Error::last_os_error().kind(), io::ErrorKind::WouldBlock);
                break;
            }
        }
        unsafe { *errno() = libc::ENOENT };
        unsafe { libc::raise(RESUME_SIGNAL) };
        assert_eq!(unsafe { *errno() }, libc::ENOENT);
        assert_eq!(take_desired_signal(), Some(RESUME_SIGNAL));

        // Drain the pipe via serve_loop (using a dup so SIGNAL_READ_FD stays open
        // for any other test) and verify stale PAUSE bytes never re-trigger PAUSE.
        let mut applied = Vec::new();
        // Put a fresh pipe into serve_loop with full-pipe + EOF semantics.
        let mut fds = [0; 2];
        assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
        let [tr, tw] = fds;
        // Write stale PAUSE bytes into test pipe and set DESIRED_SIGNAL to RESUME.
        let stale = [PAUSE_SIGNAL as u8; 8];
        assert_eq!(
            unsafe { libc::write(tw, stale.as_ptr().cast(), stale.len()) },
            stale.len() as isize
        );
        DESIRED_SIGNAL.store(RESUME_SIGNAL, Ordering::SeqCst);
        unsafe { libc::close(tw) };
        serve_loop(tr, |sig| applied.push(sig));
        assert_eq!(applied, [RESUME_SIGNAL]);

        // Drain the real pipe so nothing stays buffered in it.
        unsafe { libc::fcntl(read_fd, libc::F_SETFL, libc::O_NONBLOCK) };
        let mut drain = [0u8; 4096];
        while unsafe { libc::read(read_fd, drain.as_mut_ptr().cast(), drain.len()) } > 0 {}
        unsafe { libc::fcntl(read_fd, libc::F_SETFL, 0) };
    }
}
