//! `quickack` on the routes of container network namespaces (rules in
//! ../quickack.rs). The namespaces are the ones auto-forwarding watches:
//! Docker's (/var/run/docker/netns) and CNI's (/run/netns), which covers
//! Kubernetes pods. A namespace gets its routes from Docker or the CNI plugin
//! shortly after it appears, so it is checked every tick while new and then
//! every [`SETTLED_INTERVAL`]; settled checks only dump the routes.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

use super::autofwd::{NETNS_DIRS, in_netns, is_nsfs};
use crate::quickack::{self, End};

const TICK: Duration = Duration::from_secs(1);
const SETTLING: Duration = Duration::from_secs(30);
const SETTLED_INTERVAL: Duration = Duration::from_secs(10);

/// Off with `kat.quickack=0` on the kernel command line.
pub fn enabled(cmdline: &str) -> bool {
    !cmdline.split_whitespace().any(|a| a == "kat.quickack=0")
}

struct Netns {
    /// An rtnetlink socket created inside the namespace; it acts there.
    sock: OwnedFd,
    key: u64,
    added: Instant,
    checked: Instant,
    /// Errors are logged once per namespace.
    logged: bool,
}

/// Watch container namespaces on a thread of its own.
pub fn start() {
    let spawned = thread::Builder::new().name("quickack".into()).spawn(|| {
        let mut w = Watcher {
            by_path: BTreeMap::new(),
            seq: 0,
            buf: vec![0; 32 << 10],
        };
        loop {
            w.tick();
            thread::sleep(TICK);
        }
    });
    if let Err(e) = spawned {
        eprintln!("kat-init: quickack: spawn: {e}");
    }
}

struct Watcher {
    by_path: BTreeMap<PathBuf, Netns>,
    seq: u32,
    buf: Vec<u8>,
}

impl Watcher {
    fn tick(&mut self) {
        self.refresh();
        let now = Instant::now();
        for (path, ns) in &mut self.by_path {
            let due = now.duration_since(ns.added) < SETTLING
                || now.duration_since(ns.checked) >= SETTLED_INTERVAL;
            if !due {
                continue;
            }
            ns.checked = now;
            if let Err(e) = apply(&ns.sock, &mut self.seq, &mut self.buf)
                && !ns.logged
            {
                ns.logged = true;
                eprintln!("kat-init: quickack on {}: {e}", path.display());
            }
        }
    }

    /// Open new namespaces and forget vanished ones, dropping their sockets
    /// so they do not keep the namespace alive.
    fn refresh(&mut self) {
        let mut paths = BTreeSet::new();
        for dir in NETNS_DIRS {
            if let Ok(entries) = fs::read_dir(dir) {
                paths.extend(entries.flatten().map(|e| e.path()));
            }
        }
        self.by_path.retain(|p, ns| {
            paths.contains(p) && fs::metadata(p).ok().map(|m| m.ino()) == Some(ns.key)
        });
        for path in paths {
            if self.by_path.contains_key(&path) {
                continue;
            }
            let Ok(file) = File::open(&path) else {
                continue;
            };
            // Not a namespace yet: Docker creates the file, then mounts it.
            let Ok(meta) = file.metadata() else { continue };
            if !is_nsfs(&file) {
                continue;
            }
            let key = meta.ino();
            if self.by_path.values().any(|n| n.key == key) {
                continue;
            }
            let Ok(sock) = in_netns(&OwnedFd::from(file), route_socket) else {
                continue;
            };
            let now = Instant::now();
            self.by_path.insert(
                path,
                Netns {
                    sock,
                    key,
                    added: now,
                    checked: now - SETTLED_INTERVAL,
                    logged: false,
                },
            );
        }
    }
}

fn route_socket() -> io::Result<OwnedFd> {
    let fd = unsafe {
        libc::socket(
            libc::AF_NETLINK,
            libc::SOCK_RAW | libc::SOCK_CLOEXEC,
            libc::NETLINK_ROUTE,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let fd = unsafe { OwnedFd::from_raw_fd(fd) };
    // A dead namespace's socket must not block the watcher.
    let tv = libc::timeval {
        tv_sec: 1,
        tv_usec: 0,
    };
    unsafe {
        libc::setsockopt(
            fd.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_RCVTIMEO,
            (&tv as *const libc::timeval).cast(),
            std::mem::size_of::<libc::timeval>() as libc::socklen_t,
        )
    };
    Ok(fd)
}

/// Give every route of `sock`'s namespace that lacks it the metric.
fn apply(sock: &OwnedFd, seq: &mut u32, buf: &mut [u8]) -> io::Result<()> {
    let mut routes = Vec::new();
    for family in [quickack::AF_INET, quickack::AF_INET6] {
        *seq = seq.wrapping_add(1);
        let expected = *seq;
        send(sock, &quickack::dump_request(family, expected))?;
        loop {
            let n = recv(sock, buf)?;
            match quickack::parse_dump(&buf[..n], expected, &mut routes) {
                End::More => continue,
                End::Done => break,
                // No IPv6 in this namespace.
                End::Error(e) if e == libc::EAFNOSUPPORT => break,
                End::Error(e) => return Err(io::Error::from_raw_os_error(e)),
            }
        }
    }
    for route in routes {
        *seq = seq.wrapping_add(1);
        let expected = *seq;
        send(sock, &quickack::replace_request(&route, expected))?;
        loop {
            let n = recv(sock, buf)?;
            match quickack::parse_ack(&buf[..n], expected) {
                End::More => continue,
                End::Done => break,
                // Removed since the dump: nothing to do.
                End::Error(e) if e == libc::ENOENT || e == libc::ESRCH => break,
                End::Error(e) => return Err(io::Error::from_raw_os_error(e)),
            }
        }
    }
    Ok(())
}

fn send(sock: &OwnedFd, msg: &[u8]) -> io::Result<()> {
    loop {
        let n = unsafe {
            libc::send(
                sock.as_raw_fd(),
                msg.as_ptr().cast(),
                msg.len(),
                libc::MSG_NOSIGNAL,
            )
        };
        if n >= 0 {
            return Ok(());
        }
        let e = io::Error::last_os_error();
        if e.kind() != io::ErrorKind::Interrupted {
            return Err(e);
        }
    }
}

fn recv(sock: &OwnedFd, buf: &mut [u8]) -> io::Result<usize> {
    loop {
        let n = unsafe { libc::recv(sock.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len(), 0) };
        if n > 0 {
            return Ok(n as usize);
        }
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "rtnetlink closed",
            ));
        }
        let e = io::Error::last_os_error();
        if e.kind() != io::ErrorKind::Interrupted {
            return Err(e);
        }
    }
}
