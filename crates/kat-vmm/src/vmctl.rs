//! Lifecycle controls of the VM process that are not part of its config:
//! how freed guest memory goes back to macOS, and pausing the vCPUs.
//!
//! The daemon pauses and resumes the VM with signals ([`PAUSE_SIGNAL`],
//! [`RESUME_SIGNAL`]), so it needs no extra socket into the VM process. The
//! signals are blocked in every thread and taken by one thread with
//! `sigwait`, which then asks libkrun's event loop to pause or resume.

use std::io;
use std::thread;
use std::time::Instant;

use krun::{FreePageReportingMode, VmmHandle};

/// Freeze every vCPU (libkrun holds the guest's virtual counter while paused).
pub const PAUSE_SIGNAL: i32 = libc::SIGUSR1;
/// Run the vCPUs again.
pub const RESUME_SIGNAL: i32 = libc::SIGUSR2;

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

fn signal_set() -> libc::sigset_t {
    // SAFETY: sigemptyset/sigaddset initialise and fill a local set.
    unsafe {
        let mut set: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut set);
        libc::sigaddset(&mut set, PAUSE_SIGNAL);
        libc::sigaddset(&mut set, RESUME_SIGNAL);
        set
    }
}

/// Block the control signals in this thread and every thread it spawns
/// later. Must run before libkrun starts its threads.
pub fn block_signals() -> io::Result<()> {
    let set = signal_set();
    // SAFETY: the set is initialised; the old mask is not needed.
    if unsafe { libc::pthread_sigmask(libc::SIG_BLOCK, &set, std::ptr::null_mut()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Take the control signals on a thread of their own for the life of the
/// process.
pub fn serve_signals(handle: VmmHandle) -> io::Result<()> {
    thread::Builder::new().name("vmctl".into()).spawn(move || {
        let set = signal_set();
        loop {
            let mut sig = 0;
            // SAFETY: the set is initialised and blocked in every thread.
            if unsafe { libc::sigwait(&set, &mut sig) } != 0 {
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
}
