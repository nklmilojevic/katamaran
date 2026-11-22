//! Running helper programs (lxc-*, ip, iptables, btrfs, tar) under PID 1's
//! reaper: its `waitpid(-1)` loop would steal the exit status from
//! `Command::output`, so every child goes through [`Reaper::spawn`].

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::sync::mpsc::RecvTimeoutError;
use std::thread;
use std::time::{Duration, Instant};

use super::super::service::{Reaper, describe_status};

pub const PATH: &str = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";
const MAX_OUTPUT: usize = 1 << 20;
const POST_EXIT_DRAIN: Duration = Duration::from_millis(500);

pub fn pipe() -> io::Result<(OwnedFd, OwnedFd)> {
    let mut fds = [0; 2];
    if unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) })
}

fn set_nonblocking(fd: &OwnedFd) -> io::Result<()> {
    let flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Drain `r` up to `max_bytes`. Once `wake_r` closes (signaling child exit),
/// wait at most `post_exit` more for remaining output so a leaked fd in a
/// grandchild can never hang the caller forever.
fn read_bounded_after_exit(
    r: OwnedFd,
    wake_r: OwnedFd,
    max_bytes: usize,
    post_exit: Duration,
) -> String {
    let _ = set_nonblocking(&r);
    let mut out = Vec::new();
    let mut buf = [0u8; 8192];
    let mut deadline: Option<Instant> = None;
    let mut wake_open = true;

    while out.len() < max_bytes {
        let timeout_ms = match deadline {
            None => -1,
            Some(dl) => {
                let rem = dl.saturating_duration_since(Instant::now());
                if rem.is_zero() {
                    break;
                }
                rem.as_millis().min(i32::MAX as u128) as i32
            }
        };
        let mut pfds = [
            libc::pollfd {
                fd: r.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: wake_r.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        let nfds = if wake_open { 2 } else { 1 };
        let rc = unsafe { libc::poll(pfds.as_mut_ptr(), nfds, timeout_ms) };
        if rc < 0 {
            if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                continue;
            }
            break;
        }
        if rc == 0 {
            break;
        }
        if wake_open && pfds[1].revents != 0 {
            wake_open = false;
            deadline = Some(Instant::now() + post_exit);
        }
        if pfds[0].revents != 0 {
            loop {
                let cap = (max_bytes - out.len()).min(buf.len());
                if cap == 0 {
                    break;
                }
                let n = unsafe { libc::read(r.as_raw_fd(), buf.as_mut_ptr().cast(), cap) };
                if n > 0 {
                    out.extend_from_slice(&buf[..n as usize]);
                } else if n == 0 {
                    return String::from_utf8_lossy(&out).into_owned();
                } else {
                    let e = io::Error::last_os_error();
                    match e.kind() {
                        io::ErrorKind::Interrupted => continue,
                        io::ErrorKind::WouldBlock => break,
                        _ => return String::from_utf8_lossy(&out).into_owned(),
                    }
                }
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
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
    let (wake_r, wake_w) = pipe()?;
    cmd.env("PATH", PATH)
        .stdin(Stdio::null())
        .stdout(Stdio::from(w.try_clone()?))
        .stderr(Stdio::from(w));
    // Put the helper in its own process group so timeout kills grandchildren too.
    // SAFETY: setpgid is async-signal-safe.
    unsafe {
        cmd.pre_exec(|| {
            if libc::setpgid(0, 0) < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let (pid, exited) = reaper.spawn(cmd)?;
    // The Command keeps copies of the write end until it is replaced.
    cmd.stdout(Stdio::null()).stderr(Stdio::null());
    let reader = match thread::Builder::new()
        .name("cmd-read".into())
        .spawn(move || read_bounded_after_exit(r, wake_r, MAX_OUTPUT, POST_EXIT_DRAIN))
    {
        Ok(h) => h,
        Err(e) => {
            unsafe { libc::kill(-pid, libc::SIGKILL) };
            let _ = exited.recv_timeout(Duration::from_secs(2));
            return Err(e);
        }
    };
    let status = match exited.recv_timeout(timeout) {
        Ok(s) => s,
        Err(RecvTimeoutError::Timeout) => {
            unsafe { libc::kill(-pid, libc::SIGKILL) };
            let _ = exited.recv_timeout(Duration::from_secs(2));
            drop(wake_w);
            let _ = reader.join();
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("{:?} timed out", cmd.get_program()),
            ));
        }
        Err(RecvTimeoutError::Disconnected) => -1,
    };
    drop(wake_w);
    let out = reader.join().unwrap_or_default();
    let code = if libc::WIFEXITED(status) {
        libc::WEXITSTATUS(status)
    } else {
        128 + libc::WTERMSIG(status)
    };
    Ok((code, out))
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;

    use super::*;

    #[test]
    fn leaked_grandchild_fd_does_not_hang_reader() {
        let (r, w) = pipe().unwrap();
        let (wake_r, wake_w) = pipe().unwrap();
        let mut wf = File::from(w);
        wf.write_all(b"hello from helper").unwrap();
        // Keep `wf` open (simulating a grandchild holding the pipe) and close `wake_w`.
        drop(wake_w);
        let t = Instant::now();
        let out = read_bounded_after_exit(r, wake_r, MAX_OUTPUT, Duration::from_millis(80));
        assert_eq!(out, "hello from helper");
        assert!(t.elapsed() < Duration::from_secs(1));
        drop(wf);
    }
}
