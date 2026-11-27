//! Deadline-aware Unix domain stream socket connect.
//!
//! `std::os::unix::net::UnixStream::connect` performs a blocking `connect(2)`
//! before the caller can install socket timeouts. When a local daemon's Unix
//! listener backlog is full (returning `EAGAIN` on Linux or waiting in
//! `EINPROGRESS`), a blocking dial can stall indefinitely while holding locks
//! such as `k8s::IMAGE_SYNC_LOCK` or during guest shutdown.

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

/// Connect to a Unix domain stream socket at `path` without blocking past
/// `deadline`.
///
/// Handles `EINTR`, `EAGAIN` (full Linux `AF_UNIX` listener backlog), and
/// `EINPROGRESS` (`poll(POLLOUT)` + `SO_ERROR`), then clears `O_NONBLOCK` so
/// subsequent reads and writes honor `SO_RCVTIMEO` / `SO_SNDTIMEO`.
pub fn connect_unix_until(path: &Path, deadline: Instant) -> io::Result<UnixStream> {
    let bytes = path.as_os_str().as_bytes();
    let mut addr: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    if bytes.is_empty() || bytes.contains(&0) || bytes.len() >= addr.sun_path.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid unix socket path: {}", path.display()),
        ));
    }
    if deadline
        .checked_duration_since(Instant::now())
        .is_none_or(|d| d.is_zero())
    {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!("timed out connecting to {}", path.display()),
        ));
    }

    addr.sun_family = libc::AF_UNIX as libc::sa_family_t;
    for (dst, &src) in addr.sun_path.iter_mut().zip(bytes) {
        *dst = src as libc::c_char;
    }
    let addr_len =
        (std::mem::offset_of!(libc::sockaddr_un, sun_path) + bytes.len() + 1) as libc::socklen_t;
    #[cfg(target_os = "macos")]
    {
        addr.sun_len = addr_len as u8;
    }

    let fd = open_nonblocking_unix_socket()?;

    loop {
        let Some(rem) = deadline.checked_duration_since(Instant::now()) else {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("timed out connecting to {}", path.display()),
            ));
        };
        if rem.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("timed out connecting to {}", path.display()),
            ));
        }

        let rc = unsafe {
            libc::connect(
                fd.as_raw_fd(),
                (&addr as *const libc::sockaddr_un).cast(),
                addr_len,
            )
        };
        if rc == 0 {
            break;
        }

        let err = io::Error::last_os_error();
        match err.raw_os_error() {
            Some(libc::EINTR) => continue,
            Some(libc::EAGAIN) => {
                // On Linux, non-blocking AF_UNIX connect returns EAGAIN when
                // the listener backlog is full; retry until deadline.
                thread::sleep(rem.min(Duration::from_millis(10)));
            }
            Some(libc::EINPROGRESS) => {
                wait_connect_writable(&fd, path, deadline)?;
                let mut so_err: libc::c_int = 0;
                let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
                let grc = unsafe {
                    libc::getsockopt(
                        fd.as_raw_fd(),
                        libc::SOL_SOCKET,
                        libc::SO_ERROR,
                        (&mut so_err as *mut libc::c_int).cast(),
                        &mut len,
                    )
                };
                if grc < 0 {
                    return Err(io::Error::last_os_error());
                }
                if so_err == libc::EAGAIN {
                    let Some(rem) = deadline.checked_duration_since(Instant::now()) else {
                        return Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            format!("timed out connecting to {}", path.display()),
                        ));
                    };
                    thread::sleep(rem.min(Duration::from_millis(10)));
                    continue;
                }
                if so_err != 0 {
                    return Err(io::Error::from_raw_os_error(so_err));
                }
                break;
            }
            _ => return Err(err),
        }
    }

    // Clear O_NONBLOCK so subsequent write/read calls honor SO_SNDTIMEO/SO_RCVTIMEO.
    let flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL) };
    if flags < 0
        || unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, flags & !libc::O_NONBLOCK) } < 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(UnixStream::from(fd))
}

fn open_nonblocking_unix_socket() -> io::Result<OwnedFd> {
    #[cfg(target_os = "linux")]
    {
        let fd = unsafe {
            libc::socket(
                libc::AF_UNIX,
                libc::SOCK_STREAM | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK,
                0,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }
    #[cfg(not(target_os = "linux"))]
    {
        let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        let fd_flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFD) };
        if fd_flags < 0
            || unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, fd_flags | libc::FD_CLOEXEC) }
                < 0
        {
            return Err(io::Error::last_os_error());
        }
        let fl_flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL) };
        if fl_flags < 0
            || unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, fl_flags | libc::O_NONBLOCK) }
                < 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(fd)
    }
}

fn wait_connect_writable(fd: &OwnedFd, path: &Path, deadline: Instant) -> io::Result<()> {
    loop {
        let Some(rem) = deadline.checked_duration_since(Instant::now()) else {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("timed out connecting to {}", path.display()),
            ));
        };
        if rem.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("timed out connecting to {}", path.display()),
            ));
        }
        let mut pfd = libc::pollfd {
            fd: fd.as_raw_fd(),
            events: libc::POLLOUT,
            revents: 0,
        };
        let timeout_ms = rem.as_millis().clamp(1, i32::MAX as u128) as i32;
        let prc = unsafe { libc::poll(&mut pfd, 1, timeout_ms) };
        if prc < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err);
        }
        if prc == 0 {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("timed out connecting to {}", path.display()),
            ));
        }
        return Ok(());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::net::UnixListener;

    #[test]
    fn connects_and_honors_expired_deadline_and_invalid_path() {
        let path = std::env::temp_dir().join(format!("kat-unix-conn-{}.sock", std::process::id()));
        let _ = fs::remove_file(&path);
        let listener = UnixListener::bind(&path).unwrap();

        let err = connect_unix_until(&path, Instant::now()).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);

        let invalid =
            connect_unix_until(Path::new(""), Instant::now() + Duration::from_secs(1)).unwrap_err();
        assert_eq!(invalid.kind(), io::ErrorKind::InvalidInput);

        let stream = connect_unix_until(&path, Instant::now() + Duration::from_secs(1)).unwrap();
        drop(stream);
        drop(listener);
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn times_out_when_listener_backlog_is_full() {
        let path =
            std::env::temp_dir().join(format!("kat-unix-backlog-{}.sock", std::process::id()));
        let _ = fs::remove_file(&path);

        // Bind a Unix listener with backlog=1 and never accept() so filling the
        // queue forces subsequent connects to stall on the full backlog.
        let bytes = path.as_os_str().as_bytes();
        let mut addr: libc::sockaddr_un = unsafe { std::mem::zeroed() };
        addr.sun_family = libc::AF_UNIX as libc::sa_family_t;
        for (dst, &src) in addr.sun_path.iter_mut().zip(bytes) {
            *dst = src as libc::c_char;
        }
        let addr_len = (std::mem::offset_of!(libc::sockaddr_un, sun_path) + bytes.len() + 1)
            as libc::socklen_t;
        #[cfg(target_os = "macos")]
        {
            addr.sun_len = addr_len as u8;
        }

        let listen_fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
        assert!(listen_fd >= 0);
        let listen_fd = unsafe { OwnedFd::from_raw_fd(listen_fd) };
        let brc = unsafe {
            libc::bind(
                listen_fd.as_raw_fd(),
                (&addr as *const libc::sockaddr_un).cast(),
                addr_len,
            )
        };
        assert_eq!(brc, 0, "bind failed: {}", io::Error::last_os_error());
        assert_eq!(unsafe { libc::listen(listen_fd.as_raw_fd(), 1) }, 0);

        let mut held = Vec::new();
        let mut hit_timeout = false;
        for _ in 0..64 {
            let start = Instant::now();
            match connect_unix_until(&path, start + Duration::from_millis(60)) {
                Ok(stream) => held.push(stream),
                Err(e) if e.kind() == io::ErrorKind::TimedOut => {
                    assert!(
                        start.elapsed() < Duration::from_millis(500),
                        "full-backlog connect took too long: {:?}",
                        start.elapsed()
                    );
                    hit_timeout = true;
                    break;
                }
                Err(e) if e.raw_os_error() == Some(libc::ECONNREFUSED) => {
                    // BSD/macOS may immediately reject overflowed AF_UNIX
                    // backlog with ECONNREFUSED rather than EAGAIN; confirm it
                    // fails promptly without blocking.
                    assert!(start.elapsed() < Duration::from_millis(500));
                    hit_timeout = true;
                    break;
                }
                Err(e) => panic!("unexpected connect error on full backlog: {e}"),
            }
        }
        assert!(hit_timeout, "expected full-backlog connect to bound wait");

        drop(held);
        drop(listen_fd);
        let _ = fs::remove_file(&path);
    }
}
