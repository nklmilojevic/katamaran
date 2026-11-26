//! `quickack` on the routes of container network namespaces and local
//! container bridges (rules in ../quickack.rs).
//!
//! Container namespaces are the ones auto-forwarding watches: Docker's
//! (/var/run/docker/netns) and CNI's (/run/netns), which covers Kubernetes
//! pods. A namespace gets its routes from Docker or the CNI plugin shortly
//! after it appears, so it is checked every [`TICK`] until routes appear (for
//! up to [`FAST_SETTLING`]), then every [`SETTLING_INTERVAL`] during
//! [`SETTLING`], and every [`SETTLED_INTERVAL`] after that; settled checks
//! only dump the routes.
//!
//! The guest root namespace is excluded from container-namespace discovery by
//! its `(dev, ino)` identity (Docker bind-mounts it as
//! `/var/run/docker/netns/default`). Instead, the root namespace is handled
//! separately with an interface filter so only routes on local container
//! bridges (`cni0`, `docker0`, `br-*`, `katbr0`) are updated and `eth0` (to
//! gvproxy) stays untouched.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::CStr;
use std::fs::{self, File};
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use super::autofwd::{NETNS_DIRS, in_netns, is_nsfs};
use crate::quickack::{self, Dump, End};

const TICK: Duration = Duration::from_millis(200);
const FAST_SETTLING: Duration = Duration::from_secs(5);
const SETTLING: Duration = Duration::from_secs(30);
const SETTLING_INTERVAL: Duration = Duration::from_secs(1);
const SETTLED_INTERVAL: Duration = Duration::from_secs(10);
const ROOT_INTERVAL: Duration = Duration::from_secs(2);

pub use crate::quickack::enabled;

type NsId = (u64, u64);

fn ns_id(meta: &fs::Metadata) -> NsId {
    (meta.dev(), meta.ino())
}

fn should_skip_ns(id: NsId, root_id: NsId, known: impl IntoIterator<Item = NsId>) -> bool {
    id == root_id || known.into_iter().any(|k| k == id)
}

struct Netns {
    /// An rtnetlink socket created inside the namespace; it acts there.
    sock: OwnedFd,
    id: NsId,
    added: Instant,
    checked: Option<Instant>,
    /// True once a dump observed at least one main-table unicast route.
    has_routes: bool,
    /// Errors are logged once per namespace.
    logged: bool,
}

impl Netns {
    fn due(&self, now: Instant) -> bool {
        match self.checked {
            None => true,
            Some(_) if !self.has_routes && now.duration_since(self.added) < FAST_SETTLING => true,
            Some(last) if now.duration_since(self.added) < SETTLING => {
                now.duration_since(last) >= SETTLING_INTERVAL
            }
            Some(last) => now.duration_since(last) >= SETTLED_INTERVAL,
        }
    }
}

/// Watch container namespaces and local container bridges on a thread of its own.
pub fn start() {
    let root_id = match fs::metadata("/proc/self/ns/net") {
        Ok(m) => ns_id(&m),
        Err(e) => {
            eprintln!("kat-init: quickack: stat /proc/self/ns/net: {e}");
            return;
        }
    };
    let spawned = thread::Builder::new()
        .name("quickack".into())
        .spawn(move || {
            let mut w = Watcher {
                by_path: BTreeMap::new(),
                root_id,
                root_sock: route_socket().ok(),
                root_checked: None,
                root_logged: false,
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
    /// `(dev, ino)` of the root network namespace (`/proc/self/ns/net`),
    /// excluded in `refresh()` because Docker bind-mounts it as
    /// `/var/run/docker/netns/default`.
    root_id: NsId,
    root_sock: Option<OwnedFd>,
    root_checked: Option<Instant>,
    root_logged: bool,
    seq: u32,
    buf: Vec<u8>,
}

impl Watcher {
    fn tick(&mut self) {
        self.refresh();
        let now = Instant::now();
        self.tick_root(now);
        for (path, ns) in &mut self.by_path {
            if !ns.due(now) {
                continue;
            }
            ns.checked = Some(now);
            match apply(&ns.sock, &mut self.seq, &mut self.buf, None) {
                Ok(out) => {
                    if out.seen > 0 {
                        ns.has_routes = true;
                    }
                    if let Some(e) = out.route_err
                        && !ns.logged
                    {
                        ns.logged = true;
                        eprintln!("kat-init: quickack on {}: {e}", path.display());
                    }
                }
                Err(e) => {
                    if !ns.logged {
                        ns.logged = true;
                        eprintln!("kat-init: quickack on {}: {e}", path.display());
                    }
                    if let Ok(sock) = open_route_socket(path, self.root_id) {
                        ns.sock = sock;
                    }
                }
            }
        }
    }

    fn tick_root(&mut self, now: Instant) {
        if self
            .root_checked
            .is_some_and(|last| now.duration_since(last) < ROOT_INTERVAL)
        {
            return;
        }
        self.root_checked = Some(now);
        if self.root_sock.is_none() {
            self.root_sock = route_socket().ok();
        }
        let Some(sock) = self.root_sock.as_ref() else {
            return;
        };
        match apply(sock, &mut self.seq, &mut self.buf, Some(&is_local_bridge)) {
            Ok(out) => {
                if let Some(e) = out.route_err
                    && !self.root_logged
                {
                    self.root_logged = true;
                    eprintln!("kat-init: quickack on root bridges: {e}");
                }
            }
            Err(e) => {
                if !self.root_logged {
                    self.root_logged = true;
                    eprintln!("kat-init: quickack on root bridges: {e}");
                }
                self.root_sock = None;
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
            paths.contains(p) && fs::metadata(p).ok().map(|m| ns_id(&m)) == Some(ns.id)
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
            let id = ns_id(&meta);
            if should_skip_ns(id, self.root_id, self.by_path.values().map(|n| n.id)) {
                continue;
            }
            let Ok(sock) = in_netns(&OwnedFd::from(file), route_socket) else {
                continue;
            };
            self.by_path.insert(
                path,
                Netns {
                    sock,
                    id,
                    added: Instant::now(),
                    checked: None,
                    has_routes: false,
                    logged: false,
                },
            );
        }
    }
}

fn open_route_socket(path: &Path, root_id: NsId) -> io::Result<OwnedFd> {
    let file = File::open(path)?;
    let meta = file.metadata()?;
    if !is_nsfs(&file) || ns_id(&meta) == root_id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "not a container netns",
        ));
    }
    in_netns(&OwnedFd::from(file), route_socket)
}

fn is_local_bridge(ifindex: u32) -> bool {
    let mut buf = [0 as libc::c_char; libc::IF_NAMESIZE];
    let p = unsafe { libc::if_indextoname(ifindex, buf.as_mut_ptr()) };
    if p.is_null() {
        return false;
    }
    let name = unsafe { CStr::from_ptr(p) }.to_bytes();
    quickack::is_local_bridge_name(name)
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

struct ApplyOutcome {
    seen: usize,
    route_err: Option<io::Error>,
}

/// Give every unambiguous matching route of `sock`'s namespace that lacks it
/// the metric.
///
/// Transport errors return `Err` (the caller should recreate `sock`).
/// Per-route netlink errors are recorded in `ApplyOutcome::route_err` without
/// skipping the remaining routes. An interrupted dump (`NLM_F_DUMP_INTR`)
/// aborts the pass cleanly so the next tick retries on a consistent snapshot.
fn apply(
    sock: &OwnedFd,
    seq: &mut u32,
    buf: &mut [u8],
    allow_oif: Option<&dyn Fn(u32) -> bool>,
) -> io::Result<ApplyOutcome> {
    let mut dump = Dump::default();
    let mut route_err = None;
    for family in [quickack::AF_INET, quickack::AF_INET6] {
        *seq = seq.wrapping_add(1);
        let expected = *seq;
        send(sock, &quickack::dump_request(family, expected))?;
        loop {
            let n = recv(sock, buf)?;
            match quickack::parse_dump(&buf[..n], expected, allow_oif, &mut dump) {
                End::More => continue,
                End::Done => break,
                // No IPv6 in this namespace.
                End::Error(e) if e == libc::EAFNOSUPPORT => break,
                // Concurrent route update invalidated the dump snapshot; retry
                // on the next tick without applying partial updates.
                End::Error(e) if e == quickack::EINTR => {
                    return Ok(ApplyOutcome {
                        seen: 0,
                        route_err: None,
                    });
                }
                End::Error(e) if e == quickack::EPROTO => {
                    return Err(io::Error::from_raw_os_error(e));
                }
                End::Error(e) => {
                    route_err.get_or_insert_with(|| io::Error::from_raw_os_error(e));
                    break;
                }
            }
        }
    }
    let seen = dump.seen;
    for route in dump.into_updates() {
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
                End::Error(e) if e == quickack::EPROTO => {
                    return Err(io::Error::from_raw_os_error(e));
                }
                End::Error(e) => {
                    route_err.get_or_insert_with(|| io::Error::from_raw_os_error(e));
                    break;
                }
            }
        }
    }
    Ok(ApplyOutcome { seen, route_err })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skips_root_namespace_alias_by_dev_and_ino() {
        let root_meta = fs::metadata("/proc/self/ns/net").expect("stat /proc/self/ns/net");
        let root_id = ns_id(&root_meta);

        // An alias pointing to the same root namespace (such as Docker's
        // /var/run/docker/netns/default or /proc/<pid>/ns/net) has identical
        // (dev, ino) and must be excluded before in_netns/route_socket.
        let alias_path = format!("/proc/{}/ns/net", std::process::id());
        let alias_meta = fs::metadata(&alias_path).expect("stat own /proc/<pid>/ns/net");
        assert!(should_skip_ns(ns_id(&alias_meta), root_id, []));
        assert!(open_route_socket(Path::new(&alias_path), root_id).is_err());

        // Distinct container namespace identities are accepted once and
        // deduplicated thereafter.
        let container_id = (root_id.0, root_id.1.wrapping_add(1));
        assert!(!should_skip_ns(container_id, root_id, []));
        assert!(should_skip_ns(container_id, root_id, [container_id]));
    }
}
