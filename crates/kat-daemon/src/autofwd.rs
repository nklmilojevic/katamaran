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
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::net::{AcceptOne, LoopbackTcpListener, addrs_label};

pub const MAX_CONNS: usize = 256;
/// Default per-port connection ceiling for published ports (isolates a busy
/// service from starving unrelated ports).
pub const PUBLISHED_MAX_CONNS_PER_PORT: usize = 128;
/// Default aggregate connection ceiling for published ports.
pub const PUBLISHED_MAX_CONNS_TOTAL: usize = 512;
/// Upper bound on process-wide concurrent relays across both automatic and
/// published forwarders, further clamped by `RLIMIT_NOFILE`.
const PROCESS_MAX_RELAY_CONNS: usize = 512;
/// Each live relay holds two TCP fds (cloned) and two Unix fds (cloned).
const FDS_PER_RELAY: usize = 4;
/// Maximum connections accepted from a single listening socket per poll wake,
/// so a busy address cannot starve another address bound to the same port.
const MAX_ACCEPT_BATCH: usize = 16;
const COPY_BUF_SIZE: usize = 64 << 10;
const ACCEPT_ERR_BACKOFF: Duration = Duration::from_millis(50);

/// Daemon-wide relay connection budget derived from `RLIMIT_NOFILE` and shared
/// across automatic and published forwarders, so combined relay traffic leaves
/// file descriptors and threads for the daemon's RPC control plane and
/// listening sockets.
#[derive(Clone, Debug)]
pub struct RelayBudget {
    active: Arc<AtomicUsize>,
    max: usize,
}

impl RelayBudget {
    /// Size a shared relay budget from this process's soft `RLIMIT_NOFILE`.
    pub fn from_process_limits() -> Self {
        Self::new(process_relay_ceiling(current_nofile_limit()))
    }

    pub fn new(max: usize) -> Self {
        Self {
            active: Arc::new(AtomicUsize::new(0)),
            max,
        }
    }

    pub fn max(&self) -> usize {
        self.max
    }

    #[cfg(test)]
    pub fn active(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }

    fn try_acquire(&self) -> Option<ConnGuard> {
        ConnGuard::try_acquire(&self.active, self.max)
    }
}

/// Connection budget policy for forwarded listeners (applied alongside the
/// shared [`RelayBudget`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnLimit {
    /// One shared cap across all forwarded ports in this forwarder (used for
    /// automatic container forwards).
    Shared(usize),
    /// Both an independent per-port cap (so one busy service cannot starve
    /// unrelated ports) and a class aggregate cap across all published ports.
    PerPort { per_port: usize, total: usize },
}

/// Default connection policy for published ports given a process relay budget.
pub fn published_conn_limit_for_budget(budget_max: usize) -> ConnLimit {
    let total = PUBLISHED_MAX_CONNS_TOTAL.min(budget_max.max(1));
    let per_port = PUBLISHED_MAX_CONNS_PER_PORT.min((total / 2).max(1));
    ConnLimit::PerPort { per_port, total }
}

/// Default connection policy for automatic container forwards given a process
/// relay budget. When published TCP ports also share daemon relays (i.e. custom
/// `forward_addresses`), cap automatic forwards to at most half the shared
/// budget so unpublished containers cannot starve published Docker or
/// Kubernetes services.
pub fn auto_conn_limit_for_budget(budget_max: usize, uses_published_relays: bool) -> ConnLimit {
    let cap = if uses_published_relays {
        MAX_CONNS.min((budget_max / 2).max(1))
    } else {
        MAX_CONNS.min(budget_max.max(1))
    };
    ConnLimit::Shared(cap)
}

fn current_nofile_limit() -> Option<u64> {
    let mut lim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) } != 0 {
        return None;
    }
    if lim.rlim_cur == libc::RLIM_INFINITY {
        None
    } else {
        Some(lim.rlim_cur)
    }
}

fn process_relay_ceiling(nofile: Option<u64>) -> usize {
    let fd_cap = match nofile {
        Some(n) => {
            let n = usize::try_from(n).unwrap_or(usize::MAX);
            // Leave at least half the soft limit when it is small (e.g.
            // launchd's 256) and at least 512 descriptors when higher so the
            // daemon RPC socket, listeners, and wakeup pipes stay responsive.
            let usable = if n < 1024 {
                n / 2
            } else {
                n.saturating_sub((n / 2).max(512))
            };
            (usable / FDS_PER_RELAY).max(1)
        }
        None => PROCESS_MAX_RELAY_CONNS,
    };
    PROCESS_MAX_RELAY_CONNS.min(fd_cap)
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

const RELAY_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Default)]
struct LiveRelaysState {
    closed: bool,
    active_relays: usize,
    next_id: u64,
    /// Open descriptors of live relay sockets, removed before each socket
    /// closes so teardown's `shutdown(fd, SHUT_RDWR)` never touches a reused fd.
    sockets: BTreeMap<u64, RawFd>,
}

/// Tracks live relay tasks and their open sockets for a VM session so tearing
/// down the session can shut down lingering clients (e.g. a client that left
/// its write side open across a VM restart) and drain their permits promptly.
/// Ordinary single-port removals leave established relays running.
#[derive(Clone, Default)]
struct LiveRelays {
    inner: Arc<(Mutex<LiveRelaysState>, Condvar)>,
}

impl LiveRelays {
    fn enter(&self) -> Option<RelayScope> {
        let mut st = self.inner.0.lock().unwrap_or_else(|e| e.into_inner());
        if st.closed {
            return None;
        }
        st.active_relays += 1;
        Some(RelayScope {
            relays: self.clone(),
        })
    }

    fn track_socket<S: AsRawFd>(&self, socket: S) -> Option<TrackedFd<S>> {
        let mut st = self.inner.0.lock().unwrap_or_else(|e| e.into_inner());
        if st.closed {
            unsafe { libc::shutdown(socket.as_raw_fd(), libc::SHUT_RDWR) };
            return None;
        }
        st.next_id += 1;
        let id = st.next_id;
        st.sockets.insert(id, socket.as_raw_fd());
        Some(TrackedFd {
            id,
            relays: self.clone(),
            socket,
        })
    }

    fn unregister(&self, id: u64) {
        let mut st = self.inner.0.lock().unwrap_or_else(|e| e.into_inner());
        st.sockets.remove(&id);
    }

    /// Shut down every active relay socket and wait for the relay threads to
    /// unwind and return their [`RelayBudget`] permits.
    fn shutdown_and_drain(&self) {
        let (lock, cvar) = &*self.inner;
        let mut st = lock.lock().unwrap_or_else(|e| e.into_inner());
        st.closed = true;
        for &fd in st.sockets.values() {
            unsafe { libc::shutdown(fd, libc::SHUT_RDWR) };
        }
        let deadline = Instant::now() + RELAY_DRAIN_TIMEOUT;
        while st.active_relays > 0 {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                break;
            }
            let (next, _) = cvar
                .wait_timeout(st, left)
                .unwrap_or_else(|e| e.into_inner());
            st = next;
        }
    }
}

struct RelayScope {
    relays: LiveRelays,
}

impl Drop for RelayScope {
    fn drop(&mut self) {
        let (lock, cvar) = &*self.relays.inner;
        let mut st = lock.lock().unwrap_or_else(|e| e.into_inner());
        st.active_relays = st.active_relays.saturating_sub(1);
        if st.active_relays == 0 {
            cvar.notify_all();
        }
    }
}

/// Registers `socket`'s fd in [`LiveRelays`] and unregisters it inside
/// `Drop::drop` BEFORE `socket` itself is dropped and closed.
struct TrackedFd<S: AsRawFd> {
    id: u64,
    relays: LiveRelays,
    socket: S,
}

impl<S: AsRawFd> Drop for TrackedFd<S> {
    fn drop(&mut self) {
        self.relays.unregister(self.id);
    }
}

/// The forwards in place.
pub struct AutoForwards {
    upstream: Upstream,
    addrs: Vec<Ipv4Addr>,
    active: BTreeMap<u16, Forward>,
    active_conns: Arc<AtomicUsize>,
    conn_limit: ConnLimit,
    budget: RelayBudget,
    relays: LiveRelays,
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

    /// Forwards from `addrs` to `upstream` using a fresh process-derived budget.
    #[cfg(test)]
    pub fn with(upstream: Upstream, addrs: &[Ipv4Addr]) -> AutoForwards {
        Self::with_budget(upstream, addrs, RelayBudget::from_process_limits())
    }

    /// Forwards from `addrs` to `upstream`, sharing `budget` with other
    /// forwarders in the same daemon process.
    pub fn with_budget(
        upstream: Upstream,
        addrs: &[Ipv4Addr],
        budget: RelayBudget,
    ) -> AutoForwards {
        let limit = match upstream {
            Upstream::AutoForward(_) => {
                auto_conn_limit_for_budget(budget.max(), addrs != [Ipv4Addr::LOCALHOST])
            }
            Upstream::PerPort(_) => published_conn_limit_for_budget(budget.max()),
        };
        Self::with_limit_and_budget(upstream, addrs, limit, budget)
    }

    /// Forwards from `addrs` to `upstream` with an explicit connection policy
    /// and a process-derived shared budget.
    #[cfg(test)]
    pub fn with_limit(
        upstream: Upstream,
        addrs: &[Ipv4Addr],
        conn_limit: ConnLimit,
    ) -> AutoForwards {
        Self::with_limit_and_budget(
            upstream,
            addrs,
            conn_limit,
            RelayBudget::from_process_limits(),
        )
    }

    /// Forwards from `addrs` to `upstream` with an explicit subordinate policy
    /// and shared process budget.
    pub fn with_limit_and_budget(
        upstream: Upstream,
        addrs: &[Ipv4Addr],
        conn_limit: ConnLimit,
        budget: RelayBudget,
    ) -> AutoForwards {
        AutoForwards {
            upstream,
            addrs: addrs.to_vec(),
            active: BTreeMap::new(),
            active_conns: Arc::new(AtomicUsize::new(0)),
            conn_limit,
            budget,
            relays: LiveRelays::default(),
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
        let (max_total_conns, port_conns, max_port_conns) = match self.conn_limit {
            ConnLimit::Shared(max) => (max, None, None),
            ConnLimit::PerPort { per_port, total } => {
                (total, Some(Arc::new(AtomicUsize::new(0))), Some(per_port))
            }
        };
        let ctx = AcceptCtx {
            upstream: self.upstream.clone(),
            label: addrs_label(&bound),
            port,
            stop: stop.clone(),
            process_budget: self.budget.clone(),
            class_conns: self.active_conns.clone(),
            max_class_conns: max_total_conns,
            port_conns,
            max_port_conns,
            accept_batch: MAX_ACCEPT_BATCH,
            relays: self.relays.clone(),
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
        self.relays.shutdown_and_drain();
    }
}

/// RAII permit holding the process-wide shared relay slot, this forwarder's
/// class slot, and (for per-port policies) the port's own isolation slot.
/// Dropping on spawn error or relay exit releases every acquired guard.
struct RelayPermit {
    _process: ConnGuard,
    _class: ConnGuard,
    _port: Option<ConnGuard>,
}

impl RelayPermit {
    fn try_acquire(ctx: &AcceptCtx) -> Result<Self, (&'static str, usize)> {
        let port = match (&ctx.port_conns, ctx.max_port_conns) {
            (Some(counter), Some(max)) => {
                Some(ConnGuard::try_acquire(counter, max).ok_or(("per-port", max))?)
            }
            _ => None,
        };
        let class = ConnGuard::try_acquire(&ctx.class_conns, ctx.max_class_conns)
            .ok_or(("class", ctx.max_class_conns))?;
        let process = ctx
            .process_budget
            .try_acquire()
            .ok_or(("shared", ctx.process_budget.max()))?;
        Ok(RelayPermit {
            _process: process,
            _class: class,
            _port: port,
        })
    }
}

struct AcceptCtx {
    upstream: Upstream,
    label: String,
    port: u16,
    stop: Arc<AtomicBool>,
    process_budget: RelayBudget,
    class_conns: Arc<AtomicUsize>,
    max_class_conns: usize,
    port_conns: Option<Arc<AtomicUsize>>,
    max_port_conns: Option<usize>,
    accept_batch: usize,
    relays: LiveRelays,
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

/// Accept at most `ctx.accept_batch` connections (including rejected peers)
/// from `listener`'s `i`th socket, returning `false` if shutdown was signalled.
/// Bounding every underlying accept prevents a busy or rejected-peer flood on
/// one address from starving another address on the same port or delaying
/// shutdown.
fn accept_ready_batch(listener: &LoopbackTcpListener, i: usize, ctx: &AcceptCtx) -> bool {
    let (port, label) = (ctx.port, &ctx.label);
    for _ in 0..ctx.accept_batch {
        if ctx.stop.load(Ordering::SeqCst) {
            return false;
        }
        match listener.accept_one_on(i) {
            Ok(AcceptOne::Accepted(conn, _)) => {
                if conn.set_nonblocking(false).is_err() {
                    continue;
                }
                let permit = match RelayPermit::try_acquire(ctx) {
                    Ok(p) => p,
                    Err((kind, max)) => {
                        eprintln!(
                            "katamaran: forward {label}:{port}: {kind} connection limit ({max}) reached"
                        );
                        continue;
                    }
                };
                let Some(scope) = ctx.relays.enter() else {
                    return false;
                };
                let (upstream, label2, relays) =
                    (ctx.upstream.clone(), label.clone(), ctx.relays.clone());
                if let Err(e) = thread::Builder::new()
                    .name(format!("autofwd-{port}-conn"))
                    .spawn(move || {
                        // Drop order: `_permit` releases budget slots BEFORE
                        // `_scope` notifies `shutdown_and_drain` that this
                        // relay has fully unwound.
                        let _scope = scope;
                        let _permit = permit;
                        if let Err(e) = relay(conn, &upstream, port, &relays) {
                            eprintln!("katamaran: forward {label2}:{port}: {e}");
                        }
                    })
                {
                    eprintln!("katamaran: forward {label}:{port}: spawn: {e}");
                }
            }
            Ok(AcceptOne::Rejected) => continue,
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

fn relay(client: TcpStream, upstream: &Upstream, port: u16, relays: &LiveRelays) -> io::Result<()> {
    let _ = client.set_nodelay(true);
    let Some(mut client) = relays.track_socket(client) else {
        return Ok(());
    };
    let Some(mut upstream) = relays.track_socket(upstream.connect(port)?) else {
        return Ok(());
    };
    let (mut c_read, mut u_write) = (client.socket.try_clone()?, upstream.socket.try_clone()?);
    let up = thread::Builder::new()
        .name(format!("autofwd-{port}-up"))
        .spawn(move || {
            let _ = copy(&mut c_read, &mut u_write);
            let _ = u_write.shutdown(Shutdown::Write);
        })?;
    let _ = copy(&mut upstream.socket, &mut client.socket);
    let _ = client.socket.shutdown(Shutdown::Write);
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
    fn published_conn_limit_scales_with_nofile_headroom() {
        assert_eq!(process_relay_ceiling(Some(256)), 32);
        assert_eq!(
            published_conn_limit_for_budget(32),
            ConnLimit::PerPort {
                per_port: 16,
                total: 32
            }
        );
        assert_eq!(process_relay_ceiling(Some(1024)), 128);
        assert_eq!(
            published_conn_limit_for_budget(128),
            ConnLimit::PerPort {
                per_port: 64,
                total: 128
            }
        );
        assert_eq!(process_relay_ceiling(Some(2560)), 320);
        assert_eq!(
            published_conn_limit_for_budget(320),
            ConnLimit::PerPort {
                per_port: 128,
                total: 320
            }
        );
        assert_eq!(process_relay_ceiling(Some(10240)), PROCESS_MAX_RELAY_CONNS);
        assert_eq!(
            published_conn_limit_for_budget(PROCESS_MAX_RELAY_CONNS),
            ConnLimit::PerPort {
                per_port: PUBLISHED_MAX_CONNS_PER_PORT,
                total: PUBLISHED_MAX_CONNS_TOTAL
            }
        );
        assert_eq!(process_relay_ceiling(None), PROCESS_MAX_RELAY_CONNS);

        // Automatic forwards reserve half the shared budget when published TCP
        // also uses daemon relays, and can use the full budget (up to MAX_CONNS)
        // when published ports go straight through gvproxy.
        assert_eq!(auto_conn_limit_for_budget(32, true), ConnLimit::Shared(16));
        assert_eq!(auto_conn_limit_for_budget(32, false), ConnLimit::Shared(32));
        assert_eq!(auto_conn_limit_for_budget(128, true), ConnLimit::Shared(64));
        assert_eq!(
            auto_conn_limit_for_budget(512, true),
            ConnLimit::Shared(MAX_CONNS)
        );
    }

    #[test]
    fn automatic_cap_reserves_shared_capacity_for_published_ports() {
        let dir = std::env::temp_dir().join(format!("kat-autofwd-reserve-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Shared budget of 4 with published relays enabled: automatic forwards
        // are capped at 2, leaving 2 slots reserved for published services.
        let shared = RelayBudget::new(4);
        let guest_sock = fake_guest(&dir);
        let mut auto = AutoForwards::with_limit_and_budget(
            Upstream::AutoForward(guest_sock),
            &[Ipv4Addr::LOCALHOST],
            auto_conn_limit_for_budget(shared.max(), true),
            shared.clone(),
        );
        let mut published = AutoForwards::with_limit_and_budget(
            Upstream::PerPort(dir.clone()),
            &[Ipv4Addr::LOCALHOST],
            published_conn_limit_for_budget(shared.max()),
            shared.clone(),
        );
        let auto_port = add_free_port(&mut auto);
        let pub_port = add_free_port(&mut published);

        let pub_listener = UnixListener::bind(Upstream::socket(&dir, pub_port)).unwrap();
        let pub_server = thread::spawn(move || {
            let (mut c, _) = pub_listener.accept().unwrap();
            c.write_all(b"gv:").unwrap();
            let mut rest = c.try_clone().unwrap();
            let _ = copy(&mut rest, &mut c);
            let _ = c.shutdown(Shutdown::Write);
        });

        let open_auto = |tag: &[u8]| -> TcpStream {
            let mut s = TcpStream::connect(("127.0.0.1", auto_port)).unwrap();
            s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            s.write_all(tag).unwrap();
            let mut buf = vec![0u8; format!("{auto_port}:").len() + tag.len()];
            s.read_exact(&mut buf).unwrap();
            s
        };

        // Saturate the automatic forwarder's entire subordinate ceiling (2/2).
        let a1 = open_auto(b"1");
        let a2 = open_auto(b"2");
        assert_conn_rejected(auto_port);
        assert_eq!(shared.active(), 2);

        // Published port still succeeds using the reserved shared capacity.
        assert_eq!(round_trip(pub_port, "reserved"), "gv:reserved");

        pub_server.join().unwrap();
        drop(a1);
        drop(a2);
        drop(auto);
        drop(published);
        assert_eq!(shared.active(), 0);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn session_teardown_drains_lingering_client_and_returns_shared_budget() {
        let dir = std::env::temp_dir().join(format!("kat-autofwd-drain-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let shared = RelayBudget::new(2);
        let guest_sock = fake_guest(&dir);

        // Session 1: a Mac client connects and keeps its write half open.
        let mut session1 = AutoForwards::with_limit_and_budget(
            Upstream::AutoForward(guest_sock.clone()),
            &[Ipv4Addr::LOCALHOST],
            ConnLimit::Shared(2),
            shared.clone(),
        );
        let port1 = add_free_port(&mut session1);
        let mut lingering = TcpStream::connect(("127.0.0.1", port1)).unwrap();
        lingering
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        lingering.write_all(b"hi").unwrap();
        let mut head = vec![0u8; format!("{port1}:hi").len()];
        lingering.read_exact(&mut head).unwrap();
        assert_eq!(shared.active(), 1);

        // Simulate VM restart: drop session 1 while `lingering` stays open on
        // the client side (write half NOT closed). Teardown must shut down the
        // relay sockets and reclaim the shared budget slot immediately.
        drop(session1);
        assert_eq!(
            shared.active(),
            0,
            "session teardown must drain lingering relays and return budget permits"
        );
        // The lingering client observes EOF / reset rather than hanging forever.
        let mut one = [0u8; 1];
        match lingering.read(&mut one) {
            Ok(0) => {}
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::ConnectionReset
                        | io::ErrorKind::ConnectionAborted
                        | io::ErrorKind::BrokenPipe
                        | io::ErrorKind::UnexpectedEof
                ) => {}
            other => {
                panic!("expected EOF/reset for lingering client after teardown, got {other:?}")
            }
        }

        // Session 2 reuses the same daemon-lifetime budget with full capacity.
        let mut session2 = AutoForwards::with_limit_and_budget(
            Upstream::AutoForward(guest_sock),
            &[Ipv4Addr::LOCALHOST],
            ConnLimit::Shared(2),
            shared.clone(),
        );
        let port2 = add_free_port(&mut session2);
        assert_eq!(round_trip(port2, "restarted"), format!("{port2}:restarted"));
        drop(session2);
        assert_eq!(shared.active(), 0);
        drop(lingering);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn shared_budget_caps_combined_published_and_automatic_forwards() {
        let dir = std::env::temp_dir().join(format!("kat-autofwd-shared-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let shared = RelayBudget::new(3);
        let guest_sock = fake_guest(&dir);
        let mut auto = AutoForwards::with_limit_and_budget(
            Upstream::AutoForward(guest_sock),
            &[Ipv4Addr::LOCALHOST],
            ConnLimit::Shared(10),
            shared.clone(),
        );
        let mut published = AutoForwards::with_limit_and_budget(
            Upstream::PerPort(dir.clone()),
            &[Ipv4Addr::LOCALHOST],
            ConnLimit::PerPort {
                per_port: 10,
                total: 10,
            },
            shared.clone(),
        );

        let auto_port = add_free_port(&mut auto);
        let pub_port = add_free_port(&mut published);

        // Echo server behind the PerPort Unix socket for `pub_port`.
        let pub_listener = UnixListener::bind(Upstream::socket(&dir, pub_port)).unwrap();
        pub_listener.set_nonblocking(true).unwrap();
        let pub_stop = Arc::new(AtomicBool::new(false));
        let pub_stop2 = pub_stop.clone();
        let pub_server = thread::spawn(move || {
            let mut handlers = Vec::new();
            while !pub_stop2.load(Ordering::SeqCst) {
                let mut pfd = libc::pollfd {
                    fd: pub_listener.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                };
                if unsafe { libc::poll(&mut pfd, 1, 20) } <= 0 {
                    continue;
                }
                if let Ok((mut c, _)) = pub_listener.accept() {
                    let _ = c.set_nonblocking(false);
                    handlers.push(thread::spawn(move || {
                        let _ = c.write_all(b"gv:");
                        if let Ok(mut rest) = c.try_clone() {
                            let _ = copy(&mut rest, &mut c);
                        }
                        let _ = c.shutdown(Shutdown::Write);
                    }));
                }
            }
            for h in handlers {
                let _ = h.join();
            }
        });

        let open_echo = |port: u16, prefix: &str, tag: &[u8]| -> TcpStream {
            let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
            s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            s.write_all(tag).unwrap();
            let mut buf = vec![0u8; prefix.len() + tag.len()];
            s.read_exact(&mut buf).unwrap();
            assert_eq!(&buf[..prefix.len()], prefix.as_bytes());
            s
        };

        // 2 automatic relays + 1 published relay fills the shared budget of 3.
        let a1 = open_echo(auto_port, &format!("{auto_port}:"), b"1");
        let a2 = open_echo(auto_port, &format!("{auto_port}:"), b"2");
        let p1 = open_echo(pub_port, "gv:", b"3");
        assert_eq!(shared.active(), 3);

        // Both forwarders are now capped by the shared budget even though
        // neither subordinate limit (10) has been reached.
        assert_conn_rejected(auto_port);
        assert_conn_rejected(pub_port);

        // Dropping an automatic connection frees shared capacity for a
        // published connection to succeed.
        drop(a1);
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while shared.active() >= 3 && std::time::Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(round_trip(pub_port, "shared-ok"), "gv:shared-ok");

        drop(a2);
        drop(p1);
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while shared.active() != 0 && std::time::Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(shared.active(), 0);

        auto.clear();
        published.clear();
        pub_stop.store(true, Ordering::SeqCst);
        pub_server.join().unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
    }

    fn assert_conn_rejected(port: u16) {
        let mut rejected = TcpStream::connect(("127.0.0.1", port)).unwrap();
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
            other => panic!("expected cap rejection on port {port}, got {other:?}"),
        }
    }

    #[test]
    fn per_port_limit_isolates_busy_port_and_enforces_aggregate_cap() {
        let dir = std::env::temp_dir().join(format!("kat-autofwd-iso-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut fwd = AutoForwards::with_limit(
            Upstream::AutoForward(fake_guest(&dir)),
            &[Ipv4Addr::LOCALHOST],
            ConnLimit::PerPort {
                per_port: 2,
                total: 3,
            },
        );
        let a = add_free_port(&mut fwd);
        let b = add_free_port(&mut fwd);
        let c = add_free_port(&mut fwd);

        let open_active = |port: u16, tag: &[u8]| -> TcpStream {
            let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
            s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            s.write_all(tag).unwrap();
            let mut buf = vec![0u8; format!("{port}:").len() + tag.len()];
            s.read_exact(&mut buf).unwrap();
            s
        };

        // Fill port `a`'s per-port quota of 2 (total = 2/3).
        let a1 = open_active(a, b"1");
        let a2 = open_active(a, b"2");
        assert_conn_rejected(a);

        // Unrelated port `b` still gets a slot (per-port isolation; total = 3/3).
        let b1 = open_active(b, b"3");

        // Now the shared aggregate cap (3) is full across `a` + `b`, so both
        // `b` (under its own per-port cap of 2) and `c` (0 open) are rejected.
        assert_conn_rejected(b);
        assert_conn_rejected(c);

        // Releasing one connection on `a` frees aggregate capacity so `c` works.
        drop(a1);
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while fwd.active_conns.load(Ordering::Acquire) >= 3 && std::time::Instant::now() < deadline
        {
            thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(round_trip(c, "freed"), format!("{c}:freed"));

        drop(a2);
        drop(b1);
        fwd.clear();
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn accept_batch_is_bounded_and_second_socket_is_not_starved() {
        let wait_readable = |fd: std::os::fd::RawFd| {
            let mut pfd = libc::pollfd {
                fd,
                events: libc::POLLIN,
                revents: 0,
            };
            assert_eq!(unsafe { libc::poll(&mut pfd, 1, 1000) }, 1);
        };

        // 1. Deterministic batch bound: queue 5 connections on one socket and
        // confirm accept_ready_batch drains only `accept_batch` (2), leaving 3.
        let raw = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = raw.local_addr().unwrap().port();
        raw.set_nonblocking(true).unwrap();
        let listener = LoopbackTcpListener::from_listeners_for_test(vec![raw]);

        let clients: Vec<TcpStream> = (0..5)
            .map(|_| TcpStream::connect(("127.0.0.1", port)).unwrap())
            .collect();
        wait_readable(listener.raw_fds()[0]);
        let ctx = AcceptCtx {
            upstream: Upstream::AutoForward(PathBuf::from("/nonexistent")),
            label: "127.0.0.1".into(),
            port,
            stop: Arc::new(AtomicBool::new(false)),
            process_budget: RelayBudget::new(16),
            class_conns: Arc::new(AtomicUsize::new(0)),
            max_class_conns: 0,
            port_conns: None,
            max_port_conns: None,
            accept_batch: 2,
            relays: LiveRelays::default(),
        };
        assert!(accept_ready_batch(&listener, 0, &ctx));
        for remaining in 0..3 {
            wait_readable(listener.raw_fds()[0]);
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

        // 2. Rejected peers also count against `accept_batch` and respect stop,
        // so a flood of rejected connections cannot spin inside accept_on().
        let filt_raw = TcpListener::bind("127.0.0.1:0").unwrap();
        let filt_port = filt_raw.local_addr().unwrap().port();
        filt_raw.set_nonblocking(true).unwrap();
        // Allowlist does not include 127.0.0.1, so every connection is rejected.
        let filtered = LoopbackTcpListener::from_listeners_filtered_for_test(
            vec![filt_raw],
            vec![Ipv4Addr::new(172, 16, 1, 200)],
            vec![true],
        );
        let rej_clients: Vec<TcpStream> = (0..5)
            .map(|_| TcpStream::connect(("127.0.0.1", filt_port)).unwrap())
            .collect();
        wait_readable(filtered.raw_fds()[0]);
        let rej_ctx = AcceptCtx {
            upstream: Upstream::AutoForward(PathBuf::from("/nonexistent")),
            label: "172.16.1.200".into(),
            port: filt_port,
            stop: Arc::new(AtomicBool::new(false)),
            process_budget: RelayBudget::new(16),
            class_conns: Arc::new(AtomicUsize::new(0)),
            max_class_conns: 16,
            port_conns: None,
            max_port_conns: None,
            accept_batch: 2,
            relays: LiveRelays::default(),
        };
        assert!(accept_ready_batch(&filtered, 0, &rej_ctx));
        for remaining in 0..3 {
            wait_readable(filtered.raw_fds()[0]);
            assert!(
                matches!(filtered.accept_one_on(0), Ok(AcceptOne::Rejected)),
                "expected queued rejected connection #{remaining} to remain after batch of 2"
            );
        }
        assert_eq!(
            filtered.accept_one_on(0).err().map(|e| e.kind()),
            Some(io::ErrorKind::WouldBlock)
        );
        // Stop flag aborts immediately even with pending rejected connections.
        let _extra = TcpStream::connect(("127.0.0.1", filt_port)).unwrap();
        wait_readable(filtered.raw_fds()[0]);
        rej_ctx.stop.store(true, Ordering::SeqCst);
        assert!(!accept_ready_batch(&filtered, 0, &rej_ctx));
        assert!(matches!(filtered.accept_one_on(0), Ok(AcceptOne::Rejected)));
        drop(rej_clients);

        // 3. Fairness across two sockets: continuously flood socket 0 (with
        // rejected connections!) while socket 1 completes round trips through
        // accept_loop and shuts down promptly.
        let dir = std::env::temp_dir().join(format!("kat-autofwd-fair-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let l0 = TcpListener::bind("127.0.0.1:0").unwrap();
        let l1 = TcpListener::bind("127.0.0.1:0").unwrap();
        let p0 = l0.local_addr().unwrap().port();
        let p1 = l1.local_addr().unwrap().port();
        // Socket 0 rejects 127.0.0.1 peers; socket 1 accepts unfiltered.
        let multi = LoopbackTcpListener::from_listeners_filtered_for_test(
            vec![l0, l1],
            vec![Ipv4Addr::new(172, 16, 1, 200)],
            vec![true, false],
        );
        multi.set_nonblocking(true).unwrap();

        let (wake_r, wake_w) = wakeup_pipe().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let loop_ctx = AcceptCtx {
            upstream: Upstream::AutoForward(fake_guest(&dir)),
            label: "127.0.0.1".into(),
            port: p1,
            stop: stop.clone(),
            process_budget: RelayBudget::new(16),
            class_conns: Arc::new(AtomicUsize::new(0)),
            max_class_conns: 16,
            port_conns: None,
            max_port_conns: None,
            accept_batch: 2,
            relays: LiveRelays::default(),
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

        stop.store(true, Ordering::SeqCst);
        let b = [1u8; 1];
        unsafe { libc::write(wake_w.as_raw_fd(), b.as_ptr().cast(), 1) };
        drop(wake_w);
        // Shutdown completes promptly even while flooder is still pounding socket 0.
        worker.join().unwrap();
        flood_stop.store(true, Ordering::SeqCst);
        flooder.join().unwrap();
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
