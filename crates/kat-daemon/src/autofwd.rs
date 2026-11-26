//! The Mac side of forwarded ports that the daemon listens for itself: a
//! listener on `<addr>:<port>` for each forward address (`127.0.0.1` by
//! default) whose connections go to an [`Upstream`]:
//!
//! - auto-forwarded container ports go over vsock (a Unix socket libkrun
//!   connects to the guest's `AUTO_FORWARD_PORT`), each prefixed with the port
//!   so the guest agent knows which container to dial. This path uses no IP
//!   routes at all, so a VPN that takes over the Mac's routes cannot break it.
//! - published ports, when forward addresses other than `127.0.0.1` are set,
//!   go to a Unix socket gvproxy exposes for the guest's port (gvproxy alone
//!   cannot serve several addresses on one privileged port).

use std::collections::BTreeMap;
use std::io::{self, Read, Write};
use std::net::{Ipv4Addr, Shutdown, TcpStream};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::net::{LoopbackTcpListener, addrs_label};

pub const MAX_CONNS: usize = 256;
/// Maximum connections accepted from a single listening socket per poll wake,
/// so a busy address cannot starve another address bound to the same port.
const MAX_ACCEPT_BATCH: usize = 16;
const COPY_BUF_SIZE: usize = 64 << 10;
const ACCEPT_ERR_BACKOFF: Duration = Duration::from_millis(50);

/// Connection budget policy for forwarded listeners.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnLimit {
    /// One shared cap across all forwarded ports (used for automatic container
    /// forwards so unpublished containers cannot exhaust daemon resources).
    Shared(usize),
    /// An independent cap per forwarded port (used for published ports so one
    /// busy service cannot starve unrelated ports).
    PerPort(usize),
}

/// Where a forwarded connection goes.
#[derive(Clone, Debug)]
pub enum Upstream {
    /// libkrun's Unix socket for the guest's `AUTO_FORWARD_PORT`; each
    /// connection starts with the guest port.
    AutoForward(PathBuf),
    /// A Unix socket per port in this directory, `fwd-<port>.sock`, which
    /// gvproxy serves (see [`Upstream::socket`]).
    PerPort(PathBuf),
}

impl Upstream {
    /// The socket gvproxy listens on for `port`, for [`Upstream::PerPort`].
    pub fn socket(dir: &Path, port: u16) -> PathBuf {
        dir.join(format!("fwd-{port}.sock"))
    }

    fn connect(&self, port: u16) -> io::Result<UnixStream> {
        match self {
            Upstream::AutoForward(sock) => {
                let mut s = UnixStream::connect(sock)?;
                s.write_all(&port.to_le_bytes())?;
                Ok(s)
            }
            Upstream::PerPort(dir) => UnixStream::connect(Upstream::socket(dir, port)),
        }
    }
}

struct Forward {
    addrs: Vec<Ipv4Addr>,
    stop: Arc<AtomicBool>,
    wake_w: OwnedFd,
    thread: JoinHandle<()>,
}

/// RAII guard for an active relayed connection slot.
struct ConnGuard(Arc<AtomicUsize>);

impl ConnGuard {
    fn try_acquire(counter: &Arc<AtomicUsize>, max: usize) -> Option<Self> {
        let mut cur = counter.load(Ordering::Acquire);
        loop {
            if cur >= max {
                return None;
            }
            match counter.compare_exchange_weak(cur, cur + 1, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return Some(ConnGuard(counter.clone())),
                Err(next) => cur = next,
            }
        }
    }
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Release);
    }
}

/// Create a non-blocking, `O_CLOEXEC` wakeup pipe.
fn wakeup_pipe() -> io::Result<(OwnedFd, OwnedFd)> {
    let mut fds = [-1; 2];
    if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `pipe` succeeded and returned two valid open file descriptors.
    let r = unsafe { OwnedFd::from_raw_fd(fds[0]) };
    let w = unsafe { OwnedFd::from_raw_fd(fds[1]) };
    for fd in [r.as_raw_fd(), w.as_raw_fd()] {
        let fd_flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        if fd_flags < 0
            || unsafe { libc::fcntl(fd, libc::F_SETFD, fd_flags | libc::FD_CLOEXEC) } < 0
        {
            return Err(io::Error::last_os_error());
        }
        let status_flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        if status_flags < 0
            || unsafe { libc::fcntl(fd, libc::F_SETFL, status_flags | libc::O_NONBLOCK) } < 0
        {
            return Err(io::Error::last_os_error());
        }
    }
    Ok((r, w))
}

/// The forwards in place.
pub struct AutoForwards {
    upstream: Upstream,
    addrs: Vec<Ipv4Addr>,
    active: BTreeMap<u16, Forward>,
    active_conns: Arc<AtomicUsize>,
    conn_limit: ConnLimit,
}

impl AutoForwards {
    /// `guest` is the Unix socket libkrun relays to the guest's
    /// `AUTO_FORWARD_PORT`.
    #[cfg(test)]
    fn new(guest: &Path) -> AutoForwards {
        Self::with(
            Upstream::AutoForward(guest.to_path_buf()),
            &[Ipv4Addr::LOCALHOST],
        )
    }

    /// Forwards from `addrs` to `upstream`.
    pub fn with(upstream: Upstream, addrs: &[Ipv4Addr]) -> AutoForwards {
        let limit = match upstream {
            Upstream::AutoForward(_) => ConnLimit::Shared(MAX_CONNS),
            Upstream::PerPort(_) => ConnLimit::PerPort(MAX_CONNS),
        };
        Self::with_limit(upstream, addrs, limit)
    }

    /// Forwards from `addrs` to `upstream` with an explicit connection policy.
    pub fn with_limit(
        upstream: Upstream,
        addrs: &[Ipv4Addr],
        conn_limit: ConnLimit,
    ) -> AutoForwards {
        AutoForwards {
            upstream,
            addrs: addrs.to_vec(),
            active: BTreeMap::new(),
            active_conns: Arc::new(AtomicUsize::new(0)),
            conn_limit,
        }
    }

    pub fn addrs(&self) -> &[Ipv4Addr] {
        &self.addrs
    }

    /// The addresses `port` is actually listening on, when active.
    pub fn bound_addrs(&self, port: u16) -> Option<&[Ipv4Addr]> {
        self.active.get(&port).map(|f| f.addrs.as_slice())
    }

    /// The addresses listened on, for log lines.
    pub fn label(&self) -> String {
        addrs_label(&self.addrs)
    }

    /// The addresses `port` is actually listening on, or [`Self::label`] when
    /// it is not active.
    pub fn bound_label(&self, port: u16) -> String {
        self.bound_addrs(port)
            .map(addrs_label)
            .unwrap_or_else(|| self.label())
    }

    pub fn contains(&self, port: u16) -> bool {
        self.active.contains_key(&port)
    }

    pub fn ports(&self) -> Vec<u16> {
        self.active.keys().copied().collect()
    }

    /// Listen on `port` of each forward address. Fails if the Mac already
    /// uses it; the guest reports the port again only after it stops and
    /// starts again.
    pub fn add(&mut self, port: u16) -> io::Result<bool> {
        if self.active.contains_key(&port) {
            return Ok(false);
        }
        let listener = LoopbackTcpListener::bind_on(&self.addrs, port)?;
        let bound = listener.addrs().to_vec();
        listener.set_nonblocking(true)?;
        let (wake_r, wake_w) = wakeup_pipe()?;
        let stop = Arc::new(AtomicBool::new(false));
        let (conns, max_conns) = match self.conn_limit {
            ConnLimit::Shared(max) => (self.active_conns.clone(), max),
            ConnLimit::PerPort(max) => (Arc::new(AtomicUsize::new(0)), max),
        };
        let ctx = AcceptCtx {
            upstream: self.upstream.clone(),
            label: addrs_label(&bound),
            port,
            stop: stop.clone(),
            conns,
            max_conns,
            accept_batch: MAX_ACCEPT_BATCH,
        };
        let thread = thread::Builder::new()
            .name(format!("autofwd-{port}"))
            .spawn(move || accept_loop(&listener, &wake_r, &ctx))?;
        self.active.insert(
            port,
            Forward {
                addrs: bound,
                stop,
                wake_w,
                thread,
            },
        );
        Ok(true)
    }

    /// Stop listening on `port`. Open connections run to their end.
    pub fn remove(&mut self, port: u16) -> bool {
        let Some(f) = self.active.remove(&port) else {
            return false;
        };
        f.stop.store(true, Ordering::SeqCst);
        // Wake the poll loop via the non-blocking pipe; dropping `wake_w`
        // also signals POLLHUP even if the pipe buffer were full.
        let b = [1u8; 1];
        unsafe { libc::write(f.wake_w.as_raw_fd(), b.as_ptr().cast(), 1) };
        drop(f.wake_w);
        let _ = f.thread.join();
        true
    }

    pub fn clear(&mut self) {
        let ports: Vec<u16> = self.active.keys().copied().collect();
        for p in ports {
            self.remove(p);
        }
    }
}

impl Drop for AutoForwards {
    fn drop(&mut self) {
        self.clear();
    }
}

struct AcceptCtx {
    upstream: Upstream,
    label: String,
    port: u16,
    stop: Arc<AtomicBool>,
    conns: Arc<AtomicUsize>,
    max_conns: usize,
    accept_batch: usize,
}

fn accept_loop(listener: &LoopbackTcpListener, wake_r: &OwnedFd, ctx: &AcceptCtx) {
    let stop = &*ctx.stop;
    // The listening sockets, then the wakeup pipe.
    let mut pfds: Vec<libc::pollfd> = listener
        .raw_fds()
        .into_iter()
        .chain(std::iter::once(wake_r.as_raw_fd()))
        .map(|fd| libc::pollfd {
            fd,
            events: libc::POLLIN,
            revents: 0,
        })
        .collect();
    let wake = pfds.len() - 1;
    loop {
        if stop.load(Ordering::SeqCst) {
            return;
        }
        for p in &mut pfds {
            p.revents = 0;
        }
        let rc = unsafe { libc::poll(pfds.as_mut_ptr(), pfds.len() as libc::nfds_t, -1) };
        if rc < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            thread::sleep(ACCEPT_ERR_BACKOFF);
            continue;
        }
        if stop.load(Ordering::SeqCst) || pfds[wake].revents != 0 {
            return;
        }
        for (i, pfd) in pfds[..wake].iter().enumerate() {
            let revents = pfd.revents;
            if revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0 {
                return;
            }
            if revents & libc::POLLIN == 0 {
                continue;
            }
            if !accept_ready_batch(listener, i, ctx) {
                return;
            }
        }
    }
}

/// Accept at most `ctx.accept_batch` connections from `listener`'s `i`th socket,
/// returning `false` if shutdown was signalled. Bounding each socket prevents a
/// busy address from starving another ready address on the same port.
fn accept_ready_batch(listener: &LoopbackTcpListener, i: usize, ctx: &AcceptCtx) -> bool {
    let (port, label) = (ctx.port, &ctx.label);
    for _ in 0..ctx.accept_batch {
        if ctx.stop.load(Ordering::SeqCst) {
            return false;
        }
        match listener.accept_on(i) {
            Ok((conn, _)) => {
                if conn.set_nonblocking(false).is_err() {
                    continue;
                }
                let Some(guard) = ConnGuard::try_acquire(&ctx.conns, ctx.max_conns) else {
                    eprintln!(
                        "katamaran: forward {label}:{port}: connection limit ({}) reached",
                        ctx.max_conns
                    );
                    continue;
                };
                let (upstream, label2) = (ctx.upstream.clone(), label.clone());
                if let Err(e) = thread::Builder::new()
                    .name(format!("autofwd-{port}-conn"))
                    .spawn(move || {
                        let _guard = guard;
                        if let Err(e) = relay(conn, &upstream, port) {
                            eprintln!("katamaran: forward {label2}:{port}: {e}");
                        }
                    })
                {
                    eprintln!("katamaran: forward {label}:{port}: spawn: {e}");
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => {
                eprintln!("katamaran: forward {label}:{port}: accept: {e}");
                thread::sleep(ACCEPT_ERR_BACKOFF);
                break;
            }
        }
    }
    true
}

fn relay(client: TcpStream, upstream: &Upstream, port: u16) -> io::Result<()> {
    let _ = client.set_nodelay(true);
    let mut upstream = upstream.connect(port)?;
    let (mut c_read, mut u_write) = (client.try_clone()?, upstream.try_clone()?);
    let up = thread::Builder::new()
        .name(format!("autofwd-{port}-up"))
        .spawn(move || {
            let _ = copy(&mut c_read, &mut u_write);
            let _ = u_write.shutdown(Shutdown::Write);
        })?;
    let mut client = client;
    let _ = copy(&mut upstream, &mut client);
    let _ = client.shutdown(Shutdown::Write);
    let _ = up.join();
    Ok(())
}

/// `io::copy` with a 64 KiB buffer: avoids the syscall overhead of 8 KiB
/// without allocating megabytes per idle connection.
fn copy(r: &mut impl Read, w: &mut impl Write) -> io::Result<u64> {
    let mut buf = vec![0u8; COPY_BUF_SIZE];
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
    use std::io::Read;
    use std::net::TcpListener;
    use std::os::unix::net::UnixListener;
    use std::time::Duration;

    /// A fake guest: reads the port header, answers "<port>:" and echoes.
    fn fake_guest(dir: &Path) -> PathBuf {
        let sock = dir.join("autofwd.sock");
        let listener = UnixListener::bind(&sock).unwrap();
        thread::spawn(move || {
            for conn in listener.incoming() {
                let mut conn = conn.unwrap();
                thread::spawn(move || {
                    let mut port = [0u8; 2];
                    conn.read_exact(&mut port).unwrap();
                    write!(conn, "{}:", u16::from_le_bytes(port)).unwrap();
                    let mut rest = conn.try_clone().unwrap();
                    copy(&mut rest, &mut conn).unwrap();
                    conn.shutdown(Shutdown::Write).unwrap();
                });
            }
        });
        sock
    }

    /// Bind a fresh port outside macOS's ephemeral range (49152..=65535)
    /// directly via `fwd.add`, avoiding a throwaway probe listener whose fd
    /// could briefly be inherited by a parallel test subprocess on macOS.
    fn add_free_port(fwd: &mut AutoForwards) -> u16 {
        use std::sync::atomic::AtomicU16;
        static NEXT: AtomicU16 = AtomicU16::new(0);
        let base = (std::process::id() % 8_000) as u16;
        for _ in 0..256 {
            let off = NEXT.fetch_add(1, Ordering::Relaxed);
            let port = 36_000u16 + (base.wrapping_add(off) % 10_000);
            if matches!(fwd.add(port), Ok(true)) {
                return port;
            }
        }
        panic!("could not find a free test port");
    }

    fn round_trip(port: u16, msg: &str) -> String {
        let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        // Written while the echo is read, or both directions' buffers fill.
        let mut w = s.try_clone().unwrap();
        let msg = msg.to_string();
        let writer = thread::spawn(move || {
            w.write_all(msg.as_bytes()).unwrap();
            w.shutdown(Shutdown::Write).unwrap();
        });
        let mut out = String::new();
        s.read_to_string(&mut out).unwrap();
        writer.join().unwrap();
        out
    }

    #[test]
    fn forwards_connections_with_their_port_until_removed() {
        let dir = std::env::temp_dir().join(format!("kat-autofwd-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut fwd = AutoForwards::new(&fake_guest(&dir));
        let a = add_free_port(&mut fwd);
        assert!(!fwd.add(a).unwrap(), "added twice");
        let b = add_free_port(&mut fwd);
        assert_eq!(round_trip(a, "hello"), format!("{a}:hello"));
        // Large enough to need many reads in both directions.
        let big = "x".repeat(4 << 20);
        assert_eq!(round_trip(b, &big), format!("{b}:{big}"));

        // A relayed connection outlives the forward's removal.
        let mut open = TcpStream::connect(("127.0.0.1", a)).unwrap();
        open.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        open.write_all(b"x").unwrap();
        let mut head = vec![0u8; format!("{a}:x").len()];
        open.read_exact(&mut head).unwrap();
        assert_eq!(head, format!("{a}:x").as_bytes());
        assert!(fwd.remove(a));
        assert!(!fwd.remove(a));
        // Poll briefly in case a parallel test subprocess (gvproxy) inherited
        // the listener during macOS's non-atomic socket()+FIOCLEX window.
        let assert_closed = |port: u16| {
            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            while TcpStream::connect(("127.0.0.1", port)).is_ok() {
                assert!(
                    std::time::Instant::now() < deadline,
                    "still listening on {port}"
                );
                thread::sleep(Duration::from_millis(10));
            }
        };
        assert_closed(a);
        open.write_all(b"y").unwrap();
        let mut tail = [0u8; 1];
        open.read_exact(&mut tail).unwrap();
        assert_eq!(&tail, b"y");

        // The Mac's port is busy: refused, nothing kept.
        let busy = TcpListener::bind("127.0.0.1:0").unwrap();
        let busy_port = busy.local_addr().unwrap().port();
        assert!(fwd.add(busy_port).is_err());
        assert!(!fwd.remove(busy_port));

        fwd.clear();
        assert_closed(b);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn remove_shuts_down_promptly_and_enforces_concurrency_cap() {
        let dir = std::env::temp_dir().join(format!("kat-autofwd-cap-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut fwd = AutoForwards::with_limit(
            Upstream::AutoForward(fake_guest(&dir)),
            &[Ipv4Addr::LOCALHOST],
            ConnLimit::Shared(2),
        );
        let port = add_free_port(&mut fwd);

        // Hold two open relayed connections (fills the cap of 2).
        let mut c1 = TcpStream::connect(("127.0.0.1", port)).unwrap();
        c1.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        c1.write_all(b"a").unwrap();
        let mut buf = vec![0u8; format!("{port}:a").len()];
        c1.read_exact(&mut buf).unwrap();

        let mut c2 = TcpStream::connect(("127.0.0.1", port)).unwrap();
        c2.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        c2.write_all(b"b").unwrap();
        c2.read_exact(&mut buf).unwrap();

        // A third concurrent connection is rejected immediately (EOF / reset),
        // never silently queued until the read timeout expires.
        let mut c3 = TcpStream::connect(("127.0.0.1", port)).unwrap();
        c3.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let mut one = [0u8; 1];
        match c3.read(&mut one) {
            Ok(0) => {}
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::ConnectionReset
                        | io::ErrorKind::ConnectionAborted
                        | io::ErrorKind::BrokenPipe
                        | io::ErrorKind::UnexpectedEof
                ) => {}
            other => panic!("expected cap rejection (EOF or reset), got {other:?}"),
        }
        drop(c3);

        // Removing the forward returns promptly via the wakeup pipe even while
        // existing connections remain open.
        let t = std::time::Instant::now();
        assert!(fwd.remove(port));
        assert!(
            t.elapsed() < Duration::from_secs(1),
            "remove took {:?}",
            t.elapsed()
        );
        // The listening socket is closed once remove() returns. Poll briefly on
        // AddrInUse in case a parallel test subprocess briefly inherited the fd
        // during macOS's non-atomic socket()+FIOCLEX window.
        let bind_deadline = std::time::Instant::now() + Duration::from_millis(500);
        loop {
            match TcpListener::bind(("127.0.0.1", port)) {
                Ok(rebound) => {
                    drop(rebound);
                    break;
                }
                Err(e)
                    if e.kind() == io::ErrorKind::AddrInUse
                        && std::time::Instant::now() < bind_deadline =>
                {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(e) => panic!("still listening on {port}: {e:?}"),
            }
        }

        // Releasing slots restores capacity when re-added.
        drop(c1);
        drop(c2);
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while fwd.active_conns.load(Ordering::Acquire) != 0 && std::time::Instant::now() < deadline
        {
            thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(fwd.active_conns.load(Ordering::Acquire), 0);
        assert!(fwd.add(port).unwrap());
        assert_eq!(round_trip(port, "ok"), format!("{port}:ok"));
        fwd.clear();
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn per_port_upstream_relays_to_the_ports_socket() {
        let dir = std::env::temp_dir().join(format!("kat-autofwd-pp-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut fwd = AutoForwards::with(Upstream::PerPort(dir.clone()), &[Ipv4Addr::LOCALHOST]);
        let port = add_free_port(&mut fwd);
        // What gvproxy serves for the port: here, an echo with a marker.
        let listener = UnixListener::bind(Upstream::socket(&dir, port)).unwrap();
        let server = thread::spawn(move || {
            let (mut conn, _) = listener.accept().unwrap();
            conn.write_all(b"gv:").unwrap();
            let mut rest = conn.try_clone().unwrap();
            copy(&mut rest, &mut conn).unwrap();
            conn.shutdown(Shutdown::Write).unwrap();
        });
        assert_eq!(round_trip(port, "hello"), "gv:hello");
        server.join().unwrap();
        assert_eq!(fwd.label(), "127.0.0.1");
        fwd.clear();
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn per_port_limit_isolates_busy_port_from_unrelated_port() {
        let dir = std::env::temp_dir().join(format!("kat-autofwd-iso-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut fwd = AutoForwards::with_limit(
            Upstream::AutoForward(fake_guest(&dir)),
            &[Ipv4Addr::LOCALHOST],
            ConnLimit::PerPort(2),
        );
        let a = add_free_port(&mut fwd);
        let b = add_free_port(&mut fwd);

        // Fill port `a`'s full 2-connection budget.
        let mut hold = Vec::new();
        for tag in [b"1", b"2"] {
            let mut s = TcpStream::connect(("127.0.0.1", a)).unwrap();
            s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            s.write_all(tag).unwrap();
            let mut buf = vec![0u8; format!("{a}:1").len()];
            s.read_exact(&mut buf).unwrap();
            hold.push(s);
        }

        // A third connection on `a` hits `a`'s cap and is closed immediately.
        let mut rejected = TcpStream::connect(("127.0.0.1", a)).unwrap();
        rejected
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut one = [0u8; 1];
        match rejected.read(&mut one) {
            Ok(0) => {}
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::ConnectionReset
                        | io::ErrorKind::ConnectionAborted
                        | io::ErrorKind::BrokenPipe
                        | io::ErrorKind::UnexpectedEof
                ) => {}
            other => panic!("expected cap rejection on busy port {a}, got {other:?}"),
        }

        // Unrelated port `b` has its own budget and still relays cleanly.
        assert_eq!(round_trip(b, "isolated"), format!("{b}:isolated"));

        drop(hold);
        fwd.clear();
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn accept_batch_is_bounded_and_second_socket_is_not_starved() {
        // 1. Deterministic batch bound: queue 5 connections on one socket and
        // confirm accept_ready_batch drains only `accept_batch` (2), leaving 3.
        let raw = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = raw.local_addr().unwrap().port();
        raw.set_nonblocking(true).unwrap();
        let listener = LoopbackTcpListener::from_listeners_for_test(vec![raw]);

        let clients: Vec<TcpStream> = (0..5)
            .map(|_| TcpStream::connect(("127.0.0.1", port)).unwrap())
            .collect();
        let ctx = AcceptCtx {
            upstream: Upstream::AutoForward(PathBuf::from("/nonexistent")),
            label: "127.0.0.1".into(),
            port,
            stop: Arc::new(AtomicBool::new(false)),
            conns: Arc::new(AtomicUsize::new(0)),
            max_conns: 0,
            accept_batch: 2,
        };
        assert!(accept_ready_batch(&listener, 0, &ctx));
        for remaining in 0..3 {
            assert!(
                listener.accept_on(0).is_ok(),
                "expected queued connection #{remaining} to remain after batch of 2"
            );
        }
        assert_eq!(
            listener.accept_on(0).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        drop(clients);

        // 2. Fairness across two sockets: continuously flood socket 0 while
        // socket 1 completes round trips through accept_loop.
        let dir = std::env::temp_dir().join(format!("kat-autofwd-fair-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let l0 = TcpListener::bind("127.0.0.1:0").unwrap();
        let l1 = TcpListener::bind("127.0.0.1:0").unwrap();
        let p0 = l0.local_addr().unwrap().port();
        let p1 = l1.local_addr().unwrap().port();
        let multi = LoopbackTcpListener::from_listeners_for_test(vec![l0, l1]);
        multi.set_nonblocking(true).unwrap();

        let (wake_r, wake_w) = wakeup_pipe().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let loop_ctx = AcceptCtx {
            upstream: Upstream::AutoForward(fake_guest(&dir)),
            label: "127.0.0.1".into(),
            port: p1,
            stop: stop.clone(),
            conns: Arc::new(AtomicUsize::new(0)),
            max_conns: 16,
            accept_batch: 2,
        };
        let worker = thread::spawn(move || accept_loop(&multi, &wake_r, &loop_ctx));

        let flood_stop = Arc::new(AtomicBool::new(false));
        let flooder = {
            let flood_stop = flood_stop.clone();
            thread::spawn(move || {
                while !flood_stop.load(Ordering::Relaxed) {
                    if let Ok(s) = TcpStream::connect(("127.0.0.1", p0)) {
                        let _ = s.shutdown(Shutdown::Both);
                    }
                }
            })
        };

        for _ in 0..3 {
            assert_eq!(round_trip(p1, "fair"), format!("{p1}:fair"));
        }

        flood_stop.store(true, Ordering::SeqCst);
        flooder.join().unwrap();
        stop.store(true, Ordering::SeqCst);
        let b = [1u8; 1];
        unsafe { libc::write(wake_w.as_raw_fd(), b.as_ptr().cast(), 1) };
        drop(wake_w);
        worker.join().unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn forwards_privileged_low_port() {
        let dir = std::env::temp_dir().join(format!("kat-autofwd-low-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut fwd = AutoForwards::new(&fake_guest(&dir));
        let mut bound = None;
        for port in (900u16..=1023).rev() {
            if matches!(fwd.add(port), Ok(true)) {
                bound = Some(port);
                break;
            }
        }
        let port = bound.expect("auto-forward a free privileged port < 1024");
        assert_eq!(round_trip(port, "hi"), format!("{port}:hi"));
        assert!(fwd.remove(port));
        fwd.clear();
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
