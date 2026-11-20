//! Host listeners that only the host kernel may connect to.
//!
//! Some host listeners serve a client inside the host kernel, such as the
//! macOS NFS client. A socket the kernel connects by itself has no owning
//! process: macOS reports peer pid 0 (`kernel_task`), and so does Linux. Any
//! user process has a pid of its own, root's included, so a pid of 0 admits
//! the kernel and nobody else.
//!
//! Such a listener is a Unix socket, or a TCP port on 127.0.0.1 whose number
//! is written to the listener's path. A TCP peer has no peer credentials,
//! but over loopback macOS reports the pid owning the other end with the
//! private `TCP_PEER_PID` option.

#[cfg(target_os = "macos")]
use std::net::Ipv4Addr;
use std::os::unix::io::RawFd;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

static KERNEL_ONLY: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());
static LOOPBACK_TCP: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

/// Admit only the kernel on the listener bound at `path`.
pub fn restrict_to_kernel(path: &Path) {
    let mut paths = KERNEL_ONLY.lock().unwrap();
    if !paths.iter().any(|p| p == path) {
        paths.push(path.to_path_buf());
    }
}

/// Listen on a TCP port of 127.0.0.1 instead of a Unix socket at `path`,
/// and write the port's number to `path`.
pub fn listen_on_loopback_tcp(path: &Path) {
    let mut paths = LOOPBACK_TCP.lock().unwrap();
    if !paths.iter().any(|p| p == path) {
        paths.push(path.to_path_buf());
    }
}

pub fn is_loopback_tcp(path: &Path) -> bool {
    LOOPBACK_TCP.lock().unwrap().iter().any(|p| p == path)
}

/// Whether a connection accepted on the listener at `path` may proceed.
pub fn admits(path: &Path, fd: RawFd) -> bool {
    let restricted = KERNEL_ONLY.lock().unwrap().iter().any(|p| p == path);
    if !restricted {
        return true;
    }
    if is_loopback_tcp(path) {
        tcp_peer_pid(fd) == Some(0)
    } else {
        peer_pid(fd) == Some(0)
    }
}

/// `TCP_PEER_PID` in xnu's `tcp_private.h`.
#[cfg(target_os = "macos")]
const TCP_PEER_PID: libc::c_int = 0x204;

#[cfg(target_os = "macos")]
fn is_loopback(fd: RawFd, peer: bool) -> bool {
    let mut sa: libc::sockaddr_in = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;
    let sap = &mut sa as *mut libc::sockaddr_in as *mut libc::sockaddr;
    // SAFETY: the call writes at most `len` bytes into `sa`.
    let rc = unsafe {
        if peer {
            libc::getpeername(fd, sap, &mut len)
        } else {
            libc::getsockname(fd, sap, &mut len)
        }
    };
    rc == 0
        && sa.sin_family as i32 == libc::AF_INET
        && u32::from_be(sa.sin_addr.s_addr) == u32::from(Ipv4Addr::LOCALHOST)
}

/// The pid owning the other end of a TCP connection over 127.0.0.1.
#[cfg(target_os = "macos")]
fn tcp_peer_pid(fd: RawFd) -> Option<libc::pid_t> {
    if !is_loopback(fd, false) || !is_loopback(fd, true) {
        return None;
    }
    let mut pid: libc::pid_t = -1;
    let mut len = std::mem::size_of::<libc::pid_t>() as libc::socklen_t;
    // SAFETY: getsockopt writes at most `len` bytes into `pid`.
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::IPPROTO_TCP,
            TCP_PEER_PID,
            &mut pid as *mut libc::pid_t as *mut libc::c_void,
            &mut len,
        )
    };
    (rc == 0).then_some(pid)
}

#[cfg(not(target_os = "macos"))]
fn tcp_peer_pid(_fd: RawFd) -> Option<libc::pid_t> {
    None
}

#[cfg(target_os = "macos")]
fn peer_pid(fd: RawFd) -> Option<libc::pid_t> {
    let mut pid: libc::pid_t = -1;
    let mut len = std::mem::size_of::<libc::pid_t>() as libc::socklen_t;
    // SAFETY: getsockopt writes at most `len` bytes into `pid`.
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_LOCAL,
            libc::LOCAL_PEERPID,
            &mut pid as *mut libc::pid_t as *mut libc::c_void,
            &mut len,
        )
    };
    (rc == 0).then_some(pid)
}

#[cfg(target_os = "linux")]
fn peer_pid(fd: RawFd) -> Option<libc::pid_t> {
    let mut cred: libc::ucred = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    // SAFETY: getsockopt writes at most `len` bytes into `cred`.
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            &mut cred as *mut libc::ucred as *mut libc::c_void,
            &mut len,
        )
    };
    (rc == 0).then_some(cred.pid)
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn peer_pid(_fd: RawFd) -> Option<libc::pid_t> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(target_os = "macos")]
    use std::net::{TcpListener, TcpStream};
    use std::os::unix::io::AsRawFd;
    use std::os::unix::net::{UnixListener, UnixStream};

    #[test]
    fn user_processes_are_refused_on_restricted_listeners_only() {
        let dir = std::env::temp_dir().join(format!("krun-kpeer-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("s");
        let _ = std::fs::remove_file(&path);
        let listener = UnixListener::bind(&path).unwrap();
        let _client = UnixStream::connect(&path).unwrap();
        let (conn, _) = listener.accept().unwrap();
        assert_eq!(
            peer_pid(conn.as_raw_fd()),
            Some(std::process::id() as libc::pid_t)
        );
        assert!(admits(&path, conn.as_raw_fd()));
        restrict_to_kernel(&path);
        assert!(!admits(&path, conn.as_raw_fd()));
        assert!(admits(&dir.join("other"), conn.as_raw_fd()));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn a_loopback_tcp_peer_is_a_user_process() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let _client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (conn, _) = listener.accept().unwrap();
        assert_eq!(
            tcp_peer_pid(conn.as_raw_fd()),
            Some(std::process::id() as libc::pid_t)
        );
        let path = std::env::temp_dir().join(format!("krun-kpeer-tcp-{}", std::process::id()));
        listen_on_loopback_tcp(&path);
        restrict_to_kernel(&path);
        assert!(!admits(&path, conn.as_raw_fd()));
    }
}
