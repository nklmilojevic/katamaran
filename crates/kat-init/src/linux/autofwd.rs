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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::net::{IpAddr, Shutdown, SocketAddr, TcpStream};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::fs::MetadataExt;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use kat_proto::{AUTO_FORWARD_PORT, Msg, PortProto};

use super::sys;
use crate::autofwd::{self, DiagEnd, Listener, Namespace, Route};

const NETNS_DIRS: [&str; 2] = ["/var/run/docker/netns", "/run/netns"];
const DIAL_TIMEOUT: Duration = Duration::from_secs(5);
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

type Routes = Arc<Mutex<HashMap<u16, (Arc<OwnedFd>, SocketAddr)>>>;

pub struct AutoForward {
    by_path: BTreeMap<PathBuf, Netns>,
    routes: BTreeMap<u16, Route>,
    shared: Routes,
    published: BTreeSet<(IpAddr, u16)>,
    last_root: BTreeSet<u16>,
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
    /// Start the relay. Ports are planned by [`Self::tick`].
    pub fn start() -> io::Result<AutoForward> {
        let shared: Routes = Arc::default();
        serve(shared.clone())?;
        Ok(AutoForward {
            by_path: BTreeMap::new(),
            routes: BTreeMap::new(),
            shared,
            published: BTreeSet::new(),
            last_root: BTreeSet::new(),
            next_seen: 0,
            seq: 0,
            buf: vec![0; 32 << 10],
        })
    }

    /// Rescan, given the root namespace's listening ports.
    pub fn tick(&mut self, root: &BTreeSet<u16>) -> Changes {
        let namespaces_changed = self.refresh_namespaces();
        let mut listeners_changed = false;
        let t = Instant::now();
        for ns in self.by_path.values_mut().filter(|ns| ns.due(t)) {
            let now = listeners(&ns.diag, &mut self.seq, &mut self.buf);
            ns.scanned_at = t;
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
        if !namespaces_changed && !listeners_changed && *root == self.last_root {
            return Changes::default();
        }
        // dockerd binds a `-p` port itself and only then starts docker-proxy
        // (`-use-listen-fd`), so the proxies are read on every change, by
        // which time a container that listens has its proxy running.
        self.published = published();
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
        let next = autofwd::plan(root, &self.published, &input, &self.routes);
        let (removed, added) = autofwd::diff(&self.routes, &next);
        let files: HashMap<u64, Arc<OwnedFd>> =
            order.iter().map(|n| (n.key, n.file.clone())).collect();
        *self.shared.lock().unwrap() = next
            .iter()
            .filter_map(|(p, r)| Some((*p, (files.get(&r.ns)?.clone(), r.target))))
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
        self.by_path.retain(|p, _| paths.contains(p));
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
        s.spawn(|| {
            setns(netns)?;
            f()
        })
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
pub(super) fn listeners(diag: &OwnedFd, seq: &mut u32, buf: &mut [u8]) -> Vec<Listener> {
    let mut out = Vec::new();
    for family in [autofwd::AF_INET, autofwd::AF_INET6] {
        *seq = seq.wrapping_add(1);
        let req =
            autofwd::diag_request(family, autofwd::IPPROTO_TCP, 1 << autofwd::TCP_LISTEN, *seq);
        let sent = unsafe {
            libc::send(
                diag.as_raw_fd(),
                req.as_ptr().cast(),
                req.len(),
                libc::MSG_NOSIGNAL,
            )
        };
        if sent < 0 {
            continue;
        }
        loop {
            let n = unsafe { libc::recv(diag.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len(), 0) };
            if n <= 0 {
                break;
            }
            if autofwd::parse_diag(&buf[..n as usize], &mut out) != DiagEnd::More {
                break;
            }
        }
    }
    out.sort();
    out.dedup();
    out
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

/// `-p` targets of the running docker-proxy processes.
fn published() -> BTreeSet<(IpAddr, u16)> {
    let mut out = BTreeSet::new();
    let Ok(procs) = fs::read_dir("/proc") else {
        return out;
    };
    for p in procs.flatten() {
        let dir = p.path();
        let is_proxy = fs::read(dir.join("comm")).is_ok_and(|c| c.trim_ascii() == b"docker-proxy");
        if !is_proxy {
            continue;
        }
        if let Some((proto, ip, port)) = fs::read(dir.join("cmdline"))
            .ok()
            .and_then(|c| autofwd::docker_proxy_target(&c))
            && proto == "tcp"
        {
            out.insert((ip, port));
        }
    }
    out
}

fn serve(routes: Routes) -> io::Result<()> {
    let listener = sys::vsock_listen(AUTO_FORWARD_PORT)?;
    thread::Builder::new()
        .name("autofwd".into())
        .spawn(move || {
            loop {
                match sys::accept(&listener) {
                    Ok(conn) => {
                        let routes = routes.clone();
                        let _ = thread::Builder::new()
                            .name("autofwd-conn".into())
                            .spawn(move || relay(conn, &routes));
                    }
                    Err(e) => {
                        eprintln!("kat-init: accept on vsock {AUTO_FORWARD_PORT}: {e}");
                        return;
                    }
                }
            }
        })?;
    Ok(())
}

/// One forwarded connection. This thread joins the owner's namespace for
/// the dial and ends with the connection, so it never runs anything else.
fn relay(conn: OwnedFd, routes: &Routes) {
    let mut client = UnixStream::from(conn);
    let mut header = [0u8; 2];
    if client.read_exact(&mut header).is_err() {
        return;
    }
    let port = u16::from_le_bytes(header);
    let Some((netns, target)) = routes.lock().unwrap().get(&port).cloned() else {
        return;
    };
    let upstream =
        match setns(&netns).and_then(|()| TcpStream::connect_timeout(&target, DIAL_TIMEOUT)) {
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
    let up = thread::spawn(move || {
        let _ = copy(&mut c_read, &mut u_write);
        let _ = u_write.shutdown(Shutdown::Write);
    });
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

/// `io::copy` with a large buffer: its 8 KiB one costs a syscall pair per
/// 8 KiB, which caps a bulk transfer well below what the link carries.
fn copy(r: &mut impl Read, w: &mut impl Write) -> io::Result<u64> {
    let mut buf = vec![0u8; 1 << 20];
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
