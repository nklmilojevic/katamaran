//! Running helper programs (lxc-*, ip, iptables, btrfs, tar) under PID 1's
//! reaper: its `waitpid(-1)` loop would steal the exit status from
//! `Command::output`, so every child goes through [`Reaper::spawn`].

use std::fs::File;
use std::io::{self, Read};
use std::os::fd::{FromRawFd, OwnedFd};
use std::process::{Command, Stdio};
use std::sync::mpsc::RecvTimeoutError;
use std::time::Duration;

use super::super::service::{Reaper, describe_status};

pub const PATH: &str = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";

pub fn pipe() -> io::Result<(OwnedFd, OwnedFd)> {
    let mut fds = [0; 2];
    if unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) })
}

/// Run `cmd` to completion (killing it after `timeout`) and return its
/// combined stdout and stderr. A non-zero exit is an error carrying the output.
pub fn run(reaper: &Reaper, cmd: &mut Command, timeout: Duration) -> io::Result<String> {
    let (status, out) = run_status(reaper, cmd, timeout)?;
    if status != 0 {
        let what = format!("{:?}", cmd.get_program());
        return Err(io::Error::other(format!(
            "{what} {}: {}",
            describe_status(status),
            out.trim()
        )));
    }
    Ok(out)
}

/// Like [`run`], but a non-zero exit is returned, not an error.
pub fn run_status(
    reaper: &Reaper,
    cmd: &mut Command,
    timeout: Duration,
) -> io::Result<(i32, String)> {
    let (r, w) = pipe()?;
    cmd.env("PATH", PATH)
        .stdin(Stdio::null())
        .stdout(Stdio::from(w.try_clone()?))
        .stderr(Stdio::from(w));
    let (pid, exited) = reaper.spawn(cmd)?;
    // The Command keeps copies of the write end until it is replaced.
    cmd.stdout(Stdio::null()).stderr(Stdio::null());
    let reader = std::thread::spawn(move || {
        let mut out = Vec::new();
        let _ = File::from(r).take(1 << 20).read_to_end(&mut out);
        String::from_utf8_lossy(&out).into_owned()
    });
    let status = match exited.recv_timeout(timeout) {
        Ok(s) => s,
        Err(RecvTimeoutError::Timeout) => {
            unsafe { libc::kill(pid, libc::SIGKILL) };
            let _ = exited.recv();
            let _ = reader.join();
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("{:?} timed out", cmd.get_program()),
            ));
        }
        Err(RecvTimeoutError::Disconnected) => -1,
    };
    let out = reader.join().unwrap_or_default();
    let code = if libc::WIFEXITED(status) {
        libc::WEXITSTATUS(status)
    } else {
        128 + libc::WTERMSIG(status)
    };
    Ok((code, out))
}
