//! Auto-forwarding of unpublished container ports (rules in ../autofwd.rs).
//!
//! Container network namespaces are the files Docker (/var/run/docker/netns)
//! and `ip netns`/CNI (/run/netns) bind-mount. Each gets a sock_diag netlink
//! socket created inside it, so one dump per address family lists its TCP
//! listeners without entering it again; a settled namespace is scanned
//! less often. The host forwards the Mac's
//! 127.0.0.1:<port> over vsock to [`AUTO_FORWARD_PORT`]; each connection
//! names its port, and a thread joins the owner's namespace and dials it.
//! No address in the VM's root namespace is used, so a later `docker run -p`
//! never finds its port taken, and nothing depends on IP routes.
//!
//! Ports Docker published on a loopback address of the VM
//! (`-p 127.0.0.1:8080:80`) take the same path: their route stays in the
//! root namespace and dials docker-proxy's loopback address there. Only the
//! Mac reaches the relay (over vsock), so such a port is never exposed on the
//! VM's interfaces. This needs Docker's userland proxy, which the guest image
//! leaves on: without it, a loopback `-p` is an iptables DNAT that only
//! `route_localnet` traffic in the VM reaches and nothing listens to find.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::net::{IpAddr, Shutdown, SocketAddr, TcpStream};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::fs::MetadataExt;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use kat_proto::{AUTO_FORWARD_PORT, Msg, PortProto};

use super::sys;
use crate::autofwd::{self, DiagEnd, Listener, Namespace, ROOT_NS, Route};

const NETNS_DIRS: [&str; 2] = ["/var/run/docker/netns", "/run/netns"];
const DIAL_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_CONNS: usize = 256;
const ACCEPT_ERR_BACKOFF: Duration = Duration::from_millis(50);
/// A namespace is scanned on every tick while it is new or its listeners
/// just changed (servers start right after their container), then every
/// [`SETTLED_INTERVAL`]: each scan costs two netlink round trips, which adds
/// up with many idle containers.
const SETTLING: Duration = Duration::from_secs(20);
const SETTLED_INTERVAL: Duration = Duration::from_secs(2);

/// Off with `kat.autoforward=0` on the kernel command line.
pub fn enabled(cmdline: &str) -> bool {
    !cmdline.split_whitespace().any(|a| a == "kat.autoforward=0")
}

struct Netns {
    file: Arc<OwnedFd>,
    diag: OwnedFd,
    key: u64,
    seen: u64,
    addrs: Vec<IpAddr>,
    listeners: Vec<Listener>,
    changed_at: Instant,
    scanned_at: Instant,
}

impl Netns {
    fn due(&self, now: Instant) -> bool {
        now.duration_since(self.changed_at) < SETTLING
            || now.duration_since(self.scanned_at) >= SETTLED_INTERVAL
    }
}

/// Where the relay dials for one port.
#[derive(Clone)]
struct Dial {
    /// The namespace to dial in; `None` for the root namespace, where the
    /// relay already runs.
    ns: Option<Arc<OwnedFd>>,
    target: SocketAddr,
    /// For a loopback `-p` port: docker-proxy's listening socket, which must
    /// still be the one on `target` when a connection comes in.
    listener: Option<u32>,
}

type Routes = Arc<Mutex<HashMap<u16, Dial>>>;

/// The running docker-proxy processes' TCP addresses.
#[derive(Default)]
struct Proxies {
    /// `-container-ip`/`-container-port`.
    targets: BTreeSet<(IpAddr, u16)>,
    /// Every socket a proxy with a loopback `-host-ip` holds, as a
    /// listener on its `-host-ip`/`-host-port`: the one that is really
    /// listening there is the proxy's.
    loopback: BTreeSet<Listener>,
}

pub struct AutoForward {
    /// Scan container namespaces (`auto_forward`); loopback `-p` ports are
    /// forwarded either way.
    containers: bool,
    by_path: BTreeMap<PathBuf, Netns>,
    routes: BTreeMap<u16, Route>,
    shared: Routes,
    proxies: Proxies,
    read_proxies: fn() -> Proxies,
    last_root: BTreeSet<u16>,
    loopback: Vec<Listener>,
    /// Loopback listeners no docker-proxy held yet, with when they appeared.
    unmatched: BTreeMap<Listener, Instant>,
    /// How long new loopback listeners are matched against docker-proxy.
    settling: Duration,
    next_seen: u64,
    seq: u32,
    buf: Vec<u8>,
}

/// What a tick changed, for the caller to send around the root watcher's own
/// changes: removals before, additions after.
#[derive(Default)]
pub struct Changes {
    pub removed: Vec<u16>,
    pub added: Vec<u16>,
}

impl AutoForward {
    /// Start the relay. Ports are planned by [`Self::tick`]; container ports
    /// only with `containers`.
    pub fn start(containers: bool) -> io::Result<AutoForward> {
        let shared: Routes = Arc::default();
        serve(shared.clone())?;
        Ok(Self::new(shared, containers, docker_proxies))
    }

    fn new(shared: Routes, containers: bool, read_proxies: fn() -> Proxies) -> AutoForward {
        AutoForward {
            containers,
            by_path: BTreeMap::new(),
            routes: BTreeMap::new(),
            shared,
            proxies: Proxies::default(),
            read_proxies,
            last_root: BTreeSet::new(),
            loopback: Vec::new(),
            unmatched: BTreeMap::new(),
            settling: SETTLING,
            next_seen: 0,
            seq: 0,
            buf: vec![0; 32 << 10],
        }
    }

    /// Rescan, given the root namespace's ports listening on every address
    /// and its loopback listeners.
    pub fn tick(&mut self, root: &BTreeSet<u16>, loopback: &[Listener]) -> Changes {
        let namespaces_changed = self.containers && self.refresh_namespaces();
        let mut listeners_changed = false;
        let t = Instant::now();
        for ns in self.by_path.values_mut().filter(|ns| ns.due(t)) {
            ns.scanned_at = t;
            let now = match listeners(&ns.diag, &mut self.seq, &mut self.buf) {
                Ok(now) => now,
                Err(_) => {
                    // Preserve prior listeners and recreate the netlink socket.
                    if let Ok(diag) = in_netns(&ns.file, diag_socket) {
                        ns.diag = diag;
                    }
                    continue;
                }
            };
            if now != ns.listeners {
                listeners_changed = true;
                ns.changed_at = t;
                ns.listeners = now;
                // Docker joins the interfaces after creating the namespace,
                // so its addresses are read once something listens.
                if let Ok(a) = in_netns(&ns.file, addresses) {
                    ns.addrs = a;
                }
            }
        }
        // A listener's inode is part of it, so a loopback port that docker-proxy
        // let go and another VM process bound within one poll is a change too.
        let loopback_changed = loopback != self.loopback.as_slice();
        if loopback_changed {
            let old: BTreeSet<&Listener> = self.loopback.iter().collect();
            let fresh: Vec<Listener> = loopback
                .iter()
                .filter(|l| !old.contains(l))
                .copied()
                .collect();
            self.unmatched.retain(|l, _| loopback.contains(l));
            self.unmatched.extend(fresh.into_iter().map(|l| (l, t)));
            self.loopback = loopback.to_vec();
        }
        // dockerd binds a `-p` port itself and only then starts docker-proxy
        // (`-use-listen-fd`), so a new loopback listener may have no proxy
        // yet: look again on every tick for a while, but only for listeners
        // that appeared lately. The VM's own loopback services never get a
        // proxy, so they stop being looked at after SETTLING.
        let settling = self.settling;
        self.unmatched
            .retain(|_, seen| t.duration_since(*seen) < settling);
        let proxy_pending = !self.unmatched.is_empty();
        if !namespaces_changed
            && !listeners_changed
            && !loopback_changed
            && !proxy_pending
            && *root == self.last_root
        {
            return Changes::default();
        }
        // The proxies are read on every change, by which time a container
        // that listens has its proxy running.
        self.proxies = (self.read_proxies)();
        self.unmatched
            .retain(|l, _| !self.proxies.loopback.contains(l));
        self.last_root = root.clone();

        let mut order: Vec<&Netns> = self.by_path.values().collect();
        order.sort_by_key(|n| n.seen);
        let input: Vec<Namespace> = order
            .iter()
            .map(|n| Namespace {
                key: n.key,
                addrs: n.addrs.clone(),
                listeners: n.listeners.clone(),
            })
            .collect();
        let loopback = autofwd::published_loopback(root, &self.loopback, &self.proxies.loopback);
        let next = autofwd::plan(root, &self.proxies.targets, &loopback, &input, &self.routes);
        let (removed, added) = autofwd::diff(&self.routes, &next);
        let files: HashMap<u64, Arc<OwnedFd>> =
            order.iter().map(|n| (n.key, n.file.clone())).collect();
        *self.shared.lock().unwrap() = next
            .iter()
            .filter_map(|(p, r)| {
                let dial = match r.ns {
                    ROOT_NS => Dial {
                        ns: None,
                        target: r.target,
                        listener: Some(
                            self.loopback
                                .iter()
                                .find(|l| SocketAddr::new(l.addr, l.port) == r.target)?
                                .inode,
                        ),
                    },
                    key => Dial {
                        ns: Some(files.get(&key)?.clone()),
                        target: r.target,
                        listener: None,
                    },
                };
                Some((*p, dial))
            })
            .collect();
        self.routes = next;
        Changes { removed, added }
    }

    /// Open new namespaces and forget vanished ones. True if any changed.
    fn refresh_namespaces(&mut self) -> bool {
        let mut paths = BTreeSet::new();
        for dir in NETNS_DIRS {
            if let Ok(entries) = fs::read_dir(dir) {
                paths.extend(entries.flatten().map(|e| e.path()));
            }
        }
        let before = self.by_path.len();
        self.by_path.retain(|p, ns| {
            paths.contains(p) && fs::metadata(p).ok().map(|m| m.ino()) == Some(ns.key)
        });
        let mut changed = before != self.by_path.len();
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
            let file = Arc::new(OwnedFd::from(file));
            let Ok(diag) = in_netns(&file, diag_socket) else {
                continue;
            };
            self.next_seen += 1;
            self.by_path.insert(
                path,
                Netns {
                    file,
                    diag,
                    key,
                    seen: self.next_seen,
                    addrs: Vec::new(),
                    listeners: Vec::new(),
                    changed_at: Instant::now(),
                    scanned_at: Instant::now(),
                },
            );
            changed = true;
        }
        changed
    }
}

fn is_nsfs(file: &File) -> bool {
    const NSFS_MAGIC: i64 = 0x6e73_6673;
    let mut st: libc::statfs = unsafe { std::mem::zeroed() };
    unsafe { libc::fstatfs(file.as_raw_fd(), &mut st) == 0 && st.f_type as i64 == NSFS_MAGIC }
}

/// Run `f` on a short-lived thread that joined `netns`, so no long-lived
/// thread is ever left in the wrong namespace.
fn in_netns<T: Send>(netns: &OwnedFd, f: impl FnOnce() -> io::Result<T> + Send) -> io::Result<T> {
    thread::scope(|s| {
        thread::Builder::new()
            .name("autofwd-netns".into())
            .spawn_scoped(s, || {
                setns(netns)?;
                f()
            })?
            .join()
            .unwrap_or_else(|_| Err(io::Error::other("netns thread panicked")))
    })
}

fn setns(netns: &OwnedFd) -> io::Result<()> {
    if unsafe { libc::setns(netns.as_raw_fd(), libc::CLONE_NEWNET) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

pub(super) fn diag_socket() -> io::Result<OwnedFd> {
    let fd = unsafe {
        libc::socket(
            libc::AF_NETLINK,
            libc::SOCK_DGRAM | libc::SOCK_CLOEXEC,
            libc::NETLINK_SOCK_DIAG,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let fd = unsafe { OwnedFd::from_raw_fd(fd) };
    // A dead namespace's socket must not block the scan.
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

/// TCP listeners of the namespace `diag` was created in, sorted.
pub(super) fn listeners(
    diag: &OwnedFd,
    seq: &mut u32,
    buf: &mut [u8],
) -> io::Result<Vec<Listener>> {
    let mut out = Vec::new();
    for family in [autofwd::AF_INET, autofwd::AF_INET6] {
        *seq = seq.wrapping_add(1);
        let expected = *seq;
        let req = autofwd::diag_request(
            family,
            autofwd::IPPROTO_TCP,
            1 << autofwd::TCP_LISTEN,
            expected,
        );
        loop {
            let sent = unsafe {
                libc::send(
                    diag.as_raw_fd(),
                    req.as_ptr().cast(),
                    req.len(),
                    libc::MSG_NOSIGNAL,
                )
            };
            if sent >= 0 {
                break;
            }
            let e = io::Error::last_os_error();
            if e.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(e);
        }
        loop {
            let n = unsafe { libc::recv(diag.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len(), 0) };
            if n < 0 {
                let e = io::Error::last_os_error();
                if e.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(e);
            }
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "netlink sock_diag closed",
                ));
            }
            match autofwd::parse_diag(&buf[..n as usize], expected, &mut out) {
                DiagEnd::More => continue,
                DiagEnd::Done => break,
                DiagEnd::Error(errno) if errno == libc::EAFNOSUPPORT || errno == libc::ENOENT => {
                    break;
                }
                DiagEnd::Error(errno) => return Err(io::Error::from_raw_os_error(errno)),
            }
        }
    }
    out.sort();
    out.dedup();
    Ok(out)
}

/// The current thread's namespace's interface addresses.
fn addresses() -> io::Result<Vec<IpAddr>> {
    let mut head: *mut libc::ifaddrs = std::ptr::null_mut();
    if unsafe { libc::getifaddrs(&mut head) } < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut out = Vec::new();
    let mut cur = head;
    while !cur.is_null() {
        let ifa = unsafe { &*cur };
        if !ifa.ifa_addr.is_null() {
            match unsafe { (*ifa.ifa_addr).sa_family } as i32 {
                libc::AF_INET => {
                    let sin = unsafe { &*(ifa.ifa_addr as *const libc::sockaddr_in) };
                    out.push(IpAddr::from(
                        u32::from_be(sin.sin_addr.s_addr).to_be_bytes(),
                    ));
                }
                libc::AF_INET6 => {
                    let sin6 = unsafe { &*(ifa.ifa_addr as *const libc::sockaddr_in6) };
                    out.push(IpAddr::from(sin6.sin6_addr.s6_addr));
                }
                _ => {}
            }
        }
        cur = ifa.ifa_next;
    }
    unsafe { libc::freeifaddrs(head) };
    Ok(out)
}

/// The running docker-proxy processes' TCP addresses.
fn docker_proxies() -> Proxies {
    let mut out = Proxies::default();
    let Ok(procs) = fs::read_dir("/proc") else {
        return out;
    };
    for p in procs.flatten() {
        let dir = p.path();
        let is_proxy = fs::read(dir.join("comm")).is_ok_and(|c| c.trim_ascii() == b"docker-proxy");
        if !is_proxy {
            continue;
        }
        let Ok(cmdline) = fs::read(dir.join("cmdline")) else {
            continue;
        };
        if let Some((proto, ip, port)) = autofwd::docker_proxy_target(&cmdline)
            && proto == "tcp"
        {
            out.targets.insert((ip, port));
        }
        if let Some((proto, addr, port)) = autofwd::docker_proxy_host(&cmdline)
            && proto == "tcp"
            && addr.is_loopback()
        {
            // The sockets it holds: the listener it was handed
            // (`-use-listen-fd`) is one of them.
            let Ok(fds) = fs::read_dir(dir.join("fd")) else {
                continue;
            };
            for fd in fds.flatten() {
                if let Some(inode) = fs::read_link(fd.path())
                    .ok()
                    .and_then(|l| autofwd::socket_inode(l.as_os_str().as_encoded_bytes()))
                {
                    out.loopback.insert(Listener { addr, port, inode });
                }
            }
        }
    }
    out
}

/// The inode of the socket listening on `addr` in this thread's namespace.
fn listener_inode(addr: SocketAddr) -> io::Result<Option<u32>> {
    let diag = diag_socket()?;
    let mut buf = vec![0u8; 32 << 10];
    let found = listeners(&diag, &mut 0, &mut buf)?
        .into_iter()
        .find(|l| l.addr == addr.ip() && l.port == addr.port());
    Ok(found.map(|l| l.inode))
}

struct ConnGuard(Arc<AtomicUsize>);

impl ConnGuard {
    fn try_acquire(active: &Arc<AtomicUsize>) -> Option<Self> {
        active
            .try_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < MAX_CONNS).then_some(n + 1)
            })
            .ok()
            .map(|_| Self(active.clone()))
    }
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

fn serve(routes: Routes) -> io::Result<()> {
    let listener = sys::vsock_listen(AUTO_FORWARD_PORT)?;
    let active = Arc::new(AtomicUsize::new(0));
    thread::Builder::new()
        .name("autofwd".into())
        .spawn(move || {
            loop {
                match sys::accept(&listener) {
                    Ok(conn) => {
                        let Some(guard) = ConnGuard::try_acquire(&active) else {
                            eprintln!(
                                "kat-init: auto-forward: connection limit ({MAX_CONNS}) reached"
                            );
                            continue;
                        };
                        let routes = routes.clone();
                        let spawned =
                            thread::Builder::new()
                                .name("autofwd-conn".into())
                                .spawn(move || {
                                    let _guard = guard;
                                    relay(conn, &routes);
                                });
                        if let Err(e) = spawned {
                            eprintln!("kat-init: auto-forward: spawn: {e}");
                        }
                    }
                    Err(e) => {
                        eprintln!("kat-init: accept on vsock {AUTO_FORWARD_PORT}: {e}");
                        thread::sleep(ACCEPT_ERR_BACKOFF);
                    }
                }
            }
        })?;
    Ok(())
}

/// One forwarded connection. This thread joins the owner's namespace for
/// the dial and ends with the connection, so it never runs anything else.
/// A root namespace route dials from where the thread already is.
fn relay(conn: OwnedFd, routes: &Routes) {
    let mut client = UnixStream::from(conn);
    let mut header = [0u8; 2];
    if client.read_exact(&mut header).is_err() {
        return;
    }
    let port = u16::from_le_bytes(header);
    let Some(Dial {
        ns: netns,
        target,
        listener,
    }) = routes.lock().unwrap().get(&port).cloned()
    else {
        return;
    };
    // docker-proxy may have let the port go since the last poll, and a VM
    // service bound it: never hand that one to the Mac.
    if let Some(inode) = listener {
        match listener_inode(target) {
            Ok(Some(now)) if now == inode => {}
            Ok(_) => {
                eprintln!("kat-init: auto-forward {port} -> {target}: docker-proxy is gone");
                return;
            }
            Err(e) => {
                eprintln!("kat-init: auto-forward {port} -> {target}: {e}");
                return;
            }
        }
    }
    let joined = netns.as_deref().map_or(Ok(()), setns);
    let upstream = match joined.and_then(|()| TcpStream::connect_timeout(&target, DIAL_TIMEOUT)) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("kat-init: auto-forward {port} -> {target}: {e}");
            return;
        }
    };
    drop(netns);
    let _ = upstream.set_nodelay(true);
    let (Ok(mut c_read), Ok(mut u_write)) = (client.try_clone(), upstream.try_clone()) else {
        return;
    };
    let up = match thread::Builder::new()
        .name("autofwd-up".into())
        .spawn(move || {
            let _ = copy(&mut c_read, &mut u_write);
            let _ = u_write.shutdown(Shutdown::Write);
        }) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("kat-init: auto-forward {port}: spawn: {e}");
            return;
        }
    };
    let mut u_read = upstream;
    let _ = copy(&mut u_read, &mut client);
    let _ = client.shutdown(Shutdown::Write);
    let _ = up.join();
}

/// The messages for a tick's changes.
pub fn messages(ports: &[u16], add: bool) -> impl Iterator<Item = Msg> + '_ {
    ports.iter().map(move |&port| {
        let proto = PortProto::Tcp;
        if add {
            Msg::AutoForwardAdd { proto, port }
        } else {
            Msg::AutoForwardRemove { proto, port }
        }
    })
}

/// `io::copy` with a 64 KiB buffer: larger than std's 8 KiB default to cut
/// syscall overhead without pinning multi-MiB buffers per idle connection.
fn copy(r: &mut impl Read, w: &mut impl Write) -> io::Result<u64> {
    let mut buf = vec![0u8; 64 << 10];
    let mut total = 0;
    loop {
        let n = match r.read(&mut buf) {
            Ok(0) => return Ok(total),
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        w.write_all(&buf[..n])?;
        total += n as u64;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::{Cell, RefCell};
    use std::net::TcpListener;

    thread_local! {
        static PROXY_SOCKETS: RefCell<BTreeSet<Listener>> = RefCell::default();
        static SCANS: Cell<usize> = const { Cell::new(0) };
    }

    fn fake_proxies() -> Proxies {
        SCANS.with(|n| n.set(n.get() + 1));
        Proxies {
            targets: BTreeSet::new(),
            loopback: PROXY_SOCKETS.with(|p| p.borrow().clone()),
        }
    }

    fn set_proxies(sockets: &[Listener]) {
        PROXY_SOCKETS.with(|p| *p.borrow_mut() = sockets.iter().copied().collect());
    }

    fn scans() -> usize {
        SCANS.with(Cell::get)
    }

    fn lo(addr: &str, port: u16, inode: u32) -> Listener {
        Listener {
            addr: addr.parse().unwrap(),
            port,
            inode,
        }
    }

    /// (dials in the root namespace, target, docker-proxy's socket).
    fn route(shared: &Routes, port: u16) -> Option<(bool, SocketAddr, Option<u32>)> {
        let routes = shared.lock().unwrap();
        routes
            .get(&port)
            .map(|d| (d.ns.is_none(), d.target, d.listener))
    }

    fn quiet(c: &Changes) -> bool {
        c.added.is_empty() && c.removed.is_empty()
    }

    #[test]
    fn loopback_publish_comes_and_goes_with_its_proxy() {
        let shared = Routes::default();
        let mut fwd = AutoForward::new(shared.clone(), false, fake_proxies);
        let root = BTreeSet::new();

        // dockerd bound 127.0.0.1:18098 but has not started docker-proxy.
        let bound = [lo("127.0.0.1", 18098, 1)];
        set_proxies(&[]);
        assert!(quiet(&fwd.tick(&root, &bound)));

        // The proxy starts with the socket: the next tick finds it although
        // nothing else changed, and the relay dials the loopback address in
        // the root namespace, checking it is still that socket.
        set_proxies(&[lo("127.0.0.1", 18098, 1)]);
        let c = fwd.tick(&root, &bound);
        assert_eq!((c.added, c.removed), (vec![18098], vec![]));
        assert_eq!(
            route(&shared, 18098),
            Some((true, "127.0.0.1:18098".parse().unwrap(), Some(1)))
        );
        assert!(quiet(&fwd.tick(&root, &bound)));

        // The container stops and a VM service binds the same port before
        // the next poll: a new socket, so the proxy is looked up again and
        // the forward goes.
        set_proxies(&[]);
        let c = fwd.tick(&root, &[lo("127.0.0.1", 18098, 2)]);
        assert_eq!((c.added, c.removed), (vec![], vec![18098]));
        assert_eq!(route(&shared, 18098), None);
        assert!(quiet(&fwd.tick(&root, &[lo("127.0.0.1", 18098, 2)])));
    }

    #[test]
    fn vm_loopback_services_are_never_forwarded() {
        let shared = Routes::default();
        let mut fwd = AutoForward::new(shared.clone(), false, fake_proxies);
        // k3s and kubelet on loopback. Something named docker-proxy claims
        // kubelet's address, but holds another socket.
        set_proxies(&[lo("127.0.0.1", 10248, 99)]);
        let listeners = [lo("127.0.0.1", 6444, 1), lo("::1", 10248, 2)];
        for _ in 0..3 {
            assert!(quiet(&fwd.tick(&BTreeSet::new(), &listeners)));
        }
        assert!(shared.lock().unwrap().is_empty());
    }

    #[test]
    fn old_unmatched_listeners_stop_proxy_scans() {
        let shared = Routes::default();
        let mut fwd = AutoForward::new(shared.clone(), false, fake_proxies);
        fwd.settling = Duration::ZERO;
        set_proxies(&[]);
        let listeners = [lo("127.0.0.1", 6444, 1)];
        let before = scans();
        fwd.tick(&BTreeSet::new(), &listeners);
        assert_eq!(scans(), before + 1, "a new listener is looked up");
        for _ in 0..3 {
            fwd.tick(&BTreeSet::new(), &listeners);
        }
        assert_eq!(scans(), before + 1, "and then left alone");

        // Within the settling time only new, unmatched listeners keep the
        // lookups going: once matched, nothing is looked up again.
        fwd.settling = SETTLING;
        let published = lo("127.0.0.1", 18098, 2);
        set_proxies(&[published]);
        let both = [listeners[0], published];
        assert_eq!(fwd.tick(&BTreeSet::new(), &both).added, [18098]);
        let matched = scans();
        for _ in 0..3 {
            assert!(quiet(&fwd.tick(&BTreeSet::new(), &both)));
        }
        assert_eq!(scans(), matched);
    }

    #[test]
    fn wildcard_publish_of_the_same_port_takes_over() {
        let shared = Routes::default();
        let mut fwd = AutoForward::new(shared.clone(), false, fake_proxies);
        let listeners = [lo("::1", 8080, 1)];
        set_proxies(&listeners);
        assert_eq!(fwd.tick(&BTreeSet::new(), &listeners).added, [8080]);
        assert_eq!(
            route(&shared, 8080),
            Some((true, "[::1]:8080".parse().unwrap(), Some(1)))
        );
        // The published forward (gvproxy) has it now.
        let c = fwd.tick(&BTreeSet::from([8080]), &listeners);
        assert_eq!((c.added, c.removed), (vec![], vec![8080]));
        assert_eq!(route(&shared, 8080), None);
    }

    /// An echo server on loopback: its address and socket inode.
    fn echo_server() -> (SocketAddr, u32) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        assert_eq!(unsafe { libc::fstat(listener.as_raw_fd(), &mut st) }, 0);
        thread::spawn(move || {
            for conn in listener.incoming() {
                let mut conn = conn.unwrap();
                thread::spawn(move || {
                    let mut r = conn.try_clone().unwrap();
                    let _ = copy(&mut r, &mut conn);
                    let _ = conn.shutdown(Shutdown::Write);
                });
            }
        });
        (addr, st.st_ino as u32)
    }

    fn root_dial(target: SocketAddr, listener: Option<u32>) -> Dial {
        Dial {
            ns: None,
            target,
            listener,
        }
    }

    /// A connection from the Mac for `port`, relayed on its own thread.
    fn connect(routes: &Routes, port: u16) -> (UnixStream, thread::JoinHandle<()>) {
        let (mut mac, guest) = UnixStream::pair().unwrap();
        mac.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let routes = routes.clone();
        let relay = thread::spawn(move || relay(OwnedFd::from(guest), &routes));
        mac.write_all(&port.to_le_bytes()).unwrap();
        (mac, relay)
    }

    fn assert_closed(routes: &Routes, port: u16) {
        let (mut mac, relay) = connect(routes, port);
        let mut buf = [0u8; 1];
        assert_eq!(mac.read(&mut buf).unwrap(), 0);
        relay.join().unwrap();
    }

    #[test]
    fn relays_to_docker_proxys_socket_in_the_root_namespace() {
        let routes = Routes::default();
        let (addr, inode) = echo_server();
        routes
            .lock()
            .unwrap()
            .insert(18098, root_dial(addr, Some(inode)));
        let (mut mac, relay) = connect(&routes, 18098);
        mac.write_all(b"hello").unwrap();
        mac.shutdown(Shutdown::Write).unwrap();
        let mut out = String::new();
        mac.read_to_string(&mut out).unwrap();
        assert_eq!(out, "hello");
        relay.join().unwrap();
    }

    #[test]
    fn refuses_another_socket_on_the_proxys_address() {
        // docker-proxy let the port go and a VM service bound it before the
        // next poll: the relay must not reach it.
        let routes = Routes::default();
        let (addr, inode) = echo_server();
        routes
            .lock()
            .unwrap()
            .insert(18098, root_dial(addr, Some(inode.wrapping_add(1))));
        assert_closed(&routes, 18098);
    }

    #[test]
    fn closes_connections_with_no_route_or_no_listener() {
        let routes = Routes::default();
        // Not forwarded (any more).
        assert_closed(&routes, 18098);
        // The container stopped between the forward and this connection.
        // Port 1 is never handed out as an ephemeral port.
        let nothing = "127.0.0.1:1".parse().unwrap();
        routes
            .lock()
            .unwrap()
            .insert(18098, root_dial(nothing, None));
        assert_closed(&routes, 18098);
        routes
            .lock()
            .unwrap()
            .insert(18098, root_dial(nothing, Some(1)));
        assert_closed(&routes, 18098);
    }
}
