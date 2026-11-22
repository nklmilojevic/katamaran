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
use std::sync::atomic::{AtomicI32, Ordering};
use std::thread;
use std::time::Instant;

use krun::{FreePageReportingMode, VmmHandle};

/// Freeze every vCPU (libkrun holds the guest's virtual counter while paused).
pub const PAUSE_SIGNAL: i32 = libc::SIGUSR1;
/// Run the vCPUs again.
pub const RESUME_SIGNAL: i32 = libc::SIGUSR2;

static SIGNAL_WRITE_FD: AtomicI32 = AtomicI32::new(-1);
static SIGNAL_READ_FD: AtomicI32 = AtomicI32::new(-1);

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
    let fd = SIGNAL_WRITE_FD.load(Ordering::Acquire);
    if fd >= 0 {
        let byte = sig as u8;
        // SAFETY: write() is async-signal-safe and `byte` is on the stack.
        unsafe { libc::write(fd, (&byte as *const u8).cast(), 1) };
    }
    unsafe { *errno() = saved };
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

fn install_handler(sig: libc::c_int) -> io::Result<()> {
    // SAFETY: sigaction struct is zeroed and initialized with our async-signal-safe handler.
    unsafe {
        let mut sa: libc::sigaction = std::mem::zeroed();
        sa.sa_sigaction = on_signal as *const () as libc::sighandler_t;
        sa.sa_flags = libc::SA_RESTART;
        libc::sigemptyset(&mut sa.sa_mask);
        if libc::sigaction(sig, &sa, std::ptr::null_mut()) != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

/// Install the async-signal-safe self-pipe handlers for the control signals so
/// no thread (including GCD worker threads) can ever terminate on `SIG_DFL`.
/// Must run before libkrun or FSEvents starts its threads.
pub fn block_signals() -> io::Result<()> {
    if SIGNAL_WRITE_FD.load(Ordering::Acquire) >= 0 {
        return Ok(());
    }
    let mut fds = [0; 2];
    // SAFETY: pipe() fills the two descriptors.
    if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let [read_fd, write_fd] = fds;
    for fd in fds {
        // SAFETY: the descriptors were just created.
        if unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) } != 0 {
            let e = io::Error::last_os_error();
            unsafe {
                libc::close(read_fd);
                libc::close(write_fd);
            }
            return Err(e);
        }
    }
    // SAFETY: the write descriptor was just created; non-blocking avoids
    // deadlocking inside the signal handler if the pipe is ever full.
    if unsafe { libc::fcntl(write_fd, libc::F_SETFL, libc::O_NONBLOCK) } != 0 {
        let e = io::Error::last_os_error();
        unsafe {
            libc::close(read_fd);
            libc::close(write_fd);
        }
        return Err(e);
    }
    SIGNAL_READ_FD.store(read_fd, Ordering::Release);
    SIGNAL_WRITE_FD.store(write_fd, Ordering::Release);
    install_handler(PAUSE_SIGNAL)?;
    install_handler(RESUME_SIGNAL)?;
    Ok(())
}

/// Take the control signals on a thread of their own for the life of the
/// process.
pub fn serve_signals(handle: VmmHandle) -> io::Result<()> {
    block_signals()?;
    let read_fd = SIGNAL_READ_FD.swap(-1, Ordering::AcqRel);
    if read_fd < 0 {
        return Err(io::Error::other("control signal pipe already served"));
    }
    thread::Builder::new().name("vmctl".into()).spawn(move || {
        let mut buf = [0u8; 64];
        loop {
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
            for &b in &buf[..n as usize] {
                let sig = i32::from(b);
                if sig != PAUSE_SIGNAL && sig != RESUME_SIGNAL {
                    continue;
                }
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
            }
        }
        unsafe { libc::close(read_fd) };
    })?;
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
    fn signal_handler_writes_pipe_and_preserves_errno() {
        block_signals().unwrap();
        let read_fd = SIGNAL_READ_FD.load(Ordering::Acquire);
        assert!(read_fd >= 0);

        unsafe { *errno() = libc::EAGAIN };
        // Raise both signals to verify the process stays alive and writes to the pipe.
        unsafe {
            libc::raise(PAUSE_SIGNAL);
            libc::raise(RESUME_SIGNAL);
        }
        assert_eq!(unsafe { *errno() }, libc::EAGAIN);

        let mut got = [0u8; 2];
        let mut filled = 0;
        while filled < 2 {
            let n = unsafe {
                libc::read(
                    read_fd,
                    got[filled..].as_mut_ptr().cast(),
                    got.len() - filled,
                )
            };
            assert!(n > 0);
            filled += n as usize;
        }
        assert_eq!(got, [PAUSE_SIGNAL as u8, RESUME_SIGNAL as u8]);
    }
}
