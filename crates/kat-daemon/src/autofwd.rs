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

use crate::net::{AcceptOne, BindError, LoopbackTcpListener, addrs_label};

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
/// Each active forwarded port holds a wakeup pipe (`wake_r`, `wake_w`) in
/// addition to one TCP listening socket per bound address.
const WAKEUP_PIPE_FDS: usize = 2;
/// Minimum descriptors for a single-address forwarded port (1 listener + pipe).
const MIN_LISTENER_FDS: usize = 1 + WAKEUP_PIPE_FDS;
/// Upper bound on process-wide listening sockets + wakeup pipes across both
/// automatic and published forwarders, further clamped by `RLIMIT_NOFILE`.
const PROCESS_MAX_LISTENER_FDS: usize = 1024;
/// Upper bound on total daemon descriptors used by forwarding (strictly
/// partitioned between listeners/wakeup pipes and active relays), further
/// clamped by `RLIMIT_NOFILE`.
const PROCESS_MAX_FORWARD_FDS: usize =
    PROCESS_MAX_LISTENER_FDS + PROCESS_MAX_RELAY_CONNS * FDS_PER_RELAY;
/// Maximum connections accepted from a single listening socket per poll wake,
/// so a busy address cannot starve another address bound to the same port.
const MAX_ACCEPT_BATCH: usize = 16;
const COPY_BUF_SIZE: usize = 64 << 10;
const ACCEPT_ERR_BACKOFF: Duration = Duration::from_millis(50);

/// Why [`AutoForwards::add`] failed to start listening on a port.
#[derive(Debug)]
pub enum AddError {
    /// The daemon's listener descriptor budget is currently full; callers
    /// should retain the port and retry when another listener closes.
    CapacityExhausted(String),
    /// Binding or setup failed on the host (e.g. port already in use or no
    /// configured address assigned to `lo0`).
    Io(io::Error),
}

impl std::fmt::Display for AddError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AddError::CapacityExhausted(msg) => f.write_str(msg),
            AddError::Io(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for AddError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            AddError::CapacityExhausted(_) => None,
            AddError::Io(e) => Some(e),
        }
    }
}

impl From<io::Error> for AddError {
    fn from(e: io::Error) -> Self {
        AddError::Io(e)
    }
}

type ReleaseCallback = Arc<dyn Fn() + Send + Sync>;

#[derive(Default)]
struct ReleaseSubscribers {
    next_id: u64,
    callbacks: BTreeMap<u64, ReleaseCallback>,
}

/// RAII handle removing a [`RelayBudget::subscribe_listener_release`] callback
/// when dropped.
pub struct CapacitySubscription {
    id: u64,
    subs: Arc<Mutex<ReleaseSubscribers>>,
}

impl Drop for CapacitySubscription {
    fn drop(&mut self) {
        let mut st = self.subs.lock().unwrap_or_else(|e| e.into_inner());
        st.callbacks.remove(&self.id);
    }
}

/// Daemon-wide forwarding descriptor and relay budget derived from
/// `RLIMIT_NOFILE` and shared across automatic and published forwarders (and
/// across VM restarts).
///
/// Strictly partitions forwarding descriptors into two non-overlapping pools
/// whose sum stays within the daemon's usable `RLIMIT_NOFILE` headroom:
/// - `listener_fds` (`<= max_listener_fds`): per-port TCP listening sockets
///   plus wakeup pipes, preventing many idle ports from exhausting daemon fds.
/// - `active_relays` (`<= max_relays`, `FDS_PER_RELAY` fds each): active client
///   relays, preventing traffic bursts from starving either control-plane RPCs
///   or newly published port bindings.
#[derive(Clone)]
pub struct RelayBudget {
    listener_fds: Arc<AtomicUsize>,
    max_listener_fds: usize,
    active_relays: Arc<AtomicUsize>,
    max_relays: usize,
    /// Serializes local loopback listener binds across forwarders so temporary
    /// partial reservations from one `add()` cannot transiently starve another.
    listener_bind_lock: Arc<Mutex<()>>,
    release_subs: Arc<Mutex<ReleaseSubscribers>>,
}

impl std::fmt::Debug for RelayBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RelayBudget")
            .field("listener_fds", &self.listener_fds.load(Ordering::Relaxed))
            .field("max_listener_fds", &self.max_listener_fds)
            .field("active_relays", &self.active_relays.load(Ordering::Relaxed))
            .field("max_relays", &self.max_relays)
            .finish()
    }
}

/// Soft `RLIMIT_NOFILE` floor requested when sizing the process-wide forwarding
/// budget. macOS `launchd` starts `LaunchAgent` processes with a 256-fd soft
/// limit and an unlimited hard limit; raising the soft limit to `10240`
/// (`OPEN_MAX`) gives both default automatic forwarding and custom-alias
/// published forwarding their full default headroom.
const TARGET_PROCESS_NOFILE: libc::rlim_t = 10_240;

impl RelayBudget {
    /// Raise the process's soft `RLIMIT_NOFILE` up to [`TARGET_PROCESS_NOFILE`]
    /// (bounded by `rlim_max`) and size a shared budget from the resulting limit.
    pub fn from_process_limits() -> Self {
        let (max_listener_fds, max_relays) = process_fd_budget(raise_nofile_limit());
        Self::with_limits(max_listener_fds, max_relays)
    }

    pub fn with_limits(max_listener_fds: usize, max_relays: usize) -> Self {
        Self {
            listener_fds: Arc::new(AtomicUsize::new(0)),
            max_listener_fds,
            active_relays: Arc::new(AtomicUsize::new(0)),
            max_relays,
            listener_bind_lock: Arc::new(Mutex::new(())),
            release_subs: Arc::new(Mutex::new(ReleaseSubscribers::default())),
        }
    }

    #[cfg(test)]
    pub fn new(max_relays: usize) -> Self {
        // Reserve generous listener headroom in relay-count unit tests while
        // keeping `max_relays` exact.
        Self::with_limits(64, max_relays)
    }

    pub fn max(&self) -> usize {
        self.max_relays
    }

    pub fn max_listener_fds(&self) -> usize {
        self.max_listener_fds
    }

    /// Maximum combined descriptors (listeners + wakeup pipes + relays) this
    /// budget permits across all forwarders.
    pub fn max_fds(&self) -> usize {
        self.max_listener_fds
            .saturating_add(self.max_relays.saturating_mul(FDS_PER_RELAY))
    }

    #[cfg(test)]
    pub fn active(&self) -> usize {
        self.active_relays.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub fn listener_fds(&self) -> usize {
        self.listener_fds.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub fn active_fds(&self) -> usize {
        self.listener_fds()
            .saturating_add(self.active().saturating_mul(FDS_PER_RELAY))
    }

    /// Register a non-blocking notification fired whenever an active listener
    /// closes and returns descriptors to this shared budget.
    pub fn subscribe_listener_release(
        &self,
        cb: impl Fn() + Send + Sync + 'static,
    ) -> CapacitySubscription {
        let mut st = self.release_subs.lock().unwrap_or_else(|e| e.into_inner());
        st.next_id += 1;
        let id = st.next_id;
        st.callbacks.insert(id, Arc::new(cb));
        CapacitySubscription {
            id,
            subs: self.release_subs.clone(),
        }
    }

    fn notify_listener_released(&self) {
        let cbs: Vec<ReleaseCallback> = {
            let st = self.release_subs.lock().unwrap_or_else(|e| e.into_inner());
            st.callbacks.values().cloned().collect()
        };
        for cb in cbs {
            cb();
        }
    }

    fn lock_listener_bind(&self) -> std::sync::MutexGuard<'_, ()> {
        self.listener_bind_lock
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

    fn has_listener_headroom(
        &self,
        fds: usize,
        class_counter: &AtomicUsize,
        class_max: Option<usize>,
    ) -> bool {
        let within_class = class_max
            .is_none_or(|max| class_counter.load(Ordering::Acquire).saturating_add(fds) <= max);
        within_class
            && self
                .listener_fds
                .load(Ordering::Acquire)
                .saturating_add(fds)
                <= self.max_listener_fds
    }

    fn try_acquire_listener(
        &self,
        fds: usize,
        class_counter: &Arc<AtomicUsize>,
        class_max: Option<usize>,
    ) -> Option<ListenerPermit> {
        let class_listener_fds = match class_max {
            Some(max) => Some(UnitsGuard::try_acquire(class_counter, fds, max)?),
            None => None,
        };
        let listener_fds = UnitsGuard::try_acquire(&self.listener_fds, fds, self.max_listener_fds)?;
        Some(ListenerPermit {
            _listener_fds: listener_fds,
            _class_listener_fds: class_listener_fds,
        })
    }

    fn try_acquire_relay(&self) -> Option<UnitsGuard> {
        UnitsGuard::try_acquire(&self.active_relays, 1, self.max_relays)
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
pub fn published_conn_limit_for_budget(budget: &RelayBudget) -> ConnLimit {
    let total = PUBLISHED_MAX_CONNS_TOTAL.min(budget.max().max(1));
    let per_port = PUBLISHED_MAX_CONNS_PER_PORT.min((total / 2).max(1));
    ConnLimit::PerPort { per_port, total }
}

/// Default connection policy for automatic container forwards given a process
/// relay budget. When published TCP ports also share daemon relays (i.e. custom
/// `forward_addresses`), cap automatic forwards to at most half the shared
/// relay ceiling so unpublished containers cannot starve published Docker or
/// Kubernetes services.
pub fn auto_conn_limit_for_budget(budget: &RelayBudget, uses_published_relays: bool) -> ConnLimit {
    let cap = if uses_published_relays {
        MAX_CONNS.min((budget.max() / 2).max(1))
    } else {
        MAX_CONNS.min(budget.max().max(1))
    };
    ConnLimit::Shared(cap)
}

/// Class listener-descriptor ceiling for automatic forwards when published TCP
/// ports also use daemon listeners (reserves at least half the listener
/// descriptor budget for published Docker and Kubernetes services).
pub fn auto_listener_fd_limit_for_budget(
    budget: &RelayBudget,
    uses_published_relays: bool,
) -> Option<usize> {
    uses_published_relays.then(|| budget.max_listener_fds() / 2)
}

fn target_nofile_for_limits(cur: libc::rlim_t, max: libc::rlim_t) -> Option<libc::rlim_t> {
    if cur == libc::RLIM_INFINITY {
        return None;
    }
    let cap = if max == libc::RLIM_INFINITY {
        TARGET_PROCESS_NOFILE
    } else {
        TARGET_PROCESS_NOFILE.min(max)
    };
    (cur < cap).then_some(cap)
}

/// Raise this process's soft `RLIMIT_NOFILE` up to [`TARGET_PROCESS_NOFILE`]
/// (bounded by `rlim_max` and any lower kernel per-process cap) and return the
/// resulting soft limit (`None` if unlimited or unavailable).
pub fn raise_nofile_limit() -> Option<u64> {
    let mut lim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) } != 0 {
        return None;
    }
    if lim.rlim_cur == libc::RLIM_INFINITY {
        return None;
    }
    if let Some(cap) = target_nofile_for_limits(lim.rlim_cur, lim.rlim_max) {
        // Try the target first, then step down if the kernel's per-process
        // file ceiling (`kern.maxfilesperproc`) is lower.
        let mut want = cap;
        while want > lim.rlim_cur {
            let next = libc::rlimit {
                rlim_cur: want,
                rlim_max: lim.rlim_max,
            };
            if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &next) } == 0 {
                lim.rlim_cur = want;
                break;
            }
            want /= 2;
        }
    }
    Some(lim.rlim_cur)
}

#[cfg(test)]
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

fn process_fd_budget(nofile: Option<u64>) -> (usize, usize) {
    let usable_fds = match nofile {
        Some(n) => {
            let n = usize::try_from(n).unwrap_or(usize::MAX);
            // Leave at least half the soft limit when it is small (e.g.
            // launchd's 256) and at least 512 descriptors when higher so the
            // daemon RPC socket, gvproxy, logs, and workers stay responsive.
            if n < 1024 {
                n / 2
            } else {
                n.saturating_sub((n / 2).max(512))
            }
        }
        None => PROCESS_MAX_FORWARD_FDS,
    };
    let cap_fds = PROCESS_MAX_FORWARD_FDS.min(usable_fds.max(MIN_LISTENER_FDS + FDS_PER_RELAY));
    // Allocate 1/3 of forwarding descriptors to listeners + wakeup pipes (up
    // to 1024 fds) and the remaining 2/3 to live relays (up to 512 relays * 4
    // fds = 2048 fds), so the two pools never contend or exceed `cap_fds`.
    let max_listener_fds = PROCESS_MAX_LISTENER_FDS
        .min((cap_fds / 3).max(MIN_LISTENER_FDS))
        .min(cap_fds - FDS_PER_RELAY);
    let max_relays =
        PROCESS_MAX_RELAY_CONNS.min(((cap_fds - max_listener_fds) / FDS_PER_RELAY).max(1));
    (max_listener_fds, max_relays)
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
    /// Released after `wake_w` closes and `thread.join()` drops `listener` and
    /// `wake_r`, so descriptor accounting never undercounts open fds.
    fd_permits: Vec<ListenerPermit>,
}

/// RAII guard for `count` units against an atomic ceiling.
struct UnitsGuard {
    counter: Arc<AtomicUsize>,
    count: usize,
}

impl UnitsGuard {
    fn try_acquire(counter: &Arc<AtomicUsize>, amount: usize, max: usize) -> Option<Self> {
        if amount == 0 {
            return Some(UnitsGuard {
                counter: counter.clone(),
                count: 0,
            });
        }
        let mut cur = counter.load(Ordering::Acquire);
        loop {
            let next = cur.checked_add(amount)?;
            if next > max {
                return None;
            }
            match counter.compare_exchange_weak(cur, next, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => {
                    return Some(UnitsGuard {
                        counter: counter.clone(),
                        count: amount,
                    });
                }
                Err(actual) => cur = actual,
            }
        }
    }
}

impl Drop for UnitsGuard {
    fn drop(&mut self) {
        if self.count > 0 {
            self.counter.fetch_sub(self.count, Ordering::Release);
        }
    }
}

/// RAII permit accounting for a forwarded port's TCP listening sockets or
/// wakeup pipe descriptors against the shared [`RelayBudget`].
struct ListenerPermit {
    _listener_fds: UnitsGuard,
    _class_listener_fds: Option<UnitsGuard>,
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
    class_listener_fds: Arc<AtomicUsize>,
    max_class_listener_fds: Option<usize>,
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
        let (limit, max_class_listener_fds) = match upstream {
            Upstream::AutoForward(_) => {
                let published_relays = addrs != [Ipv4Addr::LOCALHOST];
                (
                    auto_conn_limit_for_budget(&budget, published_relays),
                    auto_listener_fd_limit_for_budget(&budget, published_relays),
                )
            }
            Upstream::PerPort(_) => (published_conn_limit_for_budget(&budget), None),
        };
        Self::with_limits_and_budget(upstream, addrs, limit, max_class_listener_fds, budget)
    }

    /// Forwards from `addrs` to `upstream` with an explicit connection policy
    /// and a process-derived shared budget.
    #[cfg(test)]
    pub fn with_limit(
        upstream: Upstream,
        addrs: &[Ipv4Addr],
        conn_limit: ConnLimit,
    ) -> AutoForwards {
        Self::with_limits_and_budget(
            upstream,
            addrs,
            conn_limit,
            None,
            RelayBudget::from_process_limits(),
        )
    }

    /// Forwards from `addrs` to `upstream` with an explicit subordinate policy,
    /// optional class listener-FD ceiling, and shared process budget.
    pub fn with_limits_and_budget(
        upstream: Upstream,
        addrs: &[Ipv4Addr],
        conn_limit: ConnLimit,
        max_class_listener_fds: Option<usize>,
        budget: RelayBudget,
    ) -> AutoForwards {
        AutoForwards {
            upstream,
            addrs: addrs.to_vec(),
            active: BTreeMap::new(),
            active_conns: Arc::new(AtomicUsize::new(0)),
            conn_limit,
            class_listener_fds: Arc::new(AtomicUsize::new(0)),
            max_class_listener_fds,
            budget,
            relays: LiveRelays::default(),
        }
    }

    #[cfg(test)]
    pub fn with_limit_and_budget(
        upstream: Upstream,
        addrs: &[Ipv4Addr],
        conn_limit: ConnLimit,
        budget: RelayBudget,
    ) -> AutoForwards {
        Self::with_limits_and_budget(upstream, addrs, conn_limit, None, budget)
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

    pub fn budget(&self) -> &RelayBudget {
        &self.budget
    }

    fn listener_limit_msg(&self) -> String {
        format!(
            "forward descriptor limit reached ({} listener / {} total fds)",
            self.budget.max_listener_fds(),
            self.budget.max_fds()
        )
    }

    /// Best-effort preflight check requiring only the minimum descriptors any
    /// valid port bind needs (one socket + wakeup pipe), so a smaller bind
    /// (e.g. skipped unassigned alias or privileged wildcard) is never blocked.
    /// Acquires the shared listener-bind lock so it never observes transient
    /// reservations from a concurrent bind attempt that later fails.
    pub fn check_listener_capacity(&self) -> Result<(), AddError> {
        let _bind_lock = self.budget.lock_listener_bind();
        if self.budget.has_listener_headroom(
            MIN_LISTENER_FDS,
            &self.class_listener_fds,
            self.max_class_listener_fds,
        ) {
            Ok(())
        } else {
            Err(AddError::CapacityExhausted(self.listener_limit_msg()))
        }
    }

    /// Listen on `port` of each forward address. Fails with [`AddError::Io`] if
    /// the Mac already uses it or no address is assigned to `lo0`, or
    /// [`AddError::CapacityExhausted`] if the daemon's listener descriptor
    /// budget is currently full.
    pub fn add(&mut self, port: u16) -> Result<bool, AddError> {
        if self.active.contains_key(&port) {
            return Ok(false);
        }
        let _bind_lock = self.budget.lock_listener_bind();
        // Reserve the wakeup pipe up front, then acquire one listener permit
        // per eligible socket inside `bind_on_reserved` (reclaiming failed
        // binds and discarded specific-address sockets on wildcard fallback).
        // Keep `fd_permits` declared before `listener` and the wakeup pipe so
        // early-return drops close OS descriptors before returning permits.
        let pipe_permit = self
            .budget
            .try_acquire_listener(
                WAKEUP_PIPE_FDS,
                &self.class_listener_fds,
                self.max_class_listener_fds,
            )
            .ok_or_else(|| AddError::CapacityExhausted(self.listener_limit_msg()))?;
        let mut fd_permits = vec![pipe_permit];
        let (listener, socket_permits) =
            LoopbackTcpListener::bind_on_reserved(&self.addrs, port, || {
                self.budget.try_acquire_listener(
                    1,
                    &self.class_listener_fds,
                    self.max_class_listener_fds,
                )
            })
            .map_err(|e| match e {
                BindError::CapacityExhausted => {
                    AddError::CapacityExhausted(self.listener_limit_msg())
                }
                BindError::Io(io_err) => AddError::Io(io_err),
            })?;
        fd_permits.extend(socket_permits);
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
                fd_permits,
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
        // Only return descriptor accounting after `wake_w`, `wake_r`, and all
        // listening sockets owned by `f.thread` have actually closed, then
        // wake any forwarders waiting for listener capacity.
        drop(f.fd_permits);
        self.budget.notify_listener_released();
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
    _process_relays: UnitsGuard,
    _class: UnitsGuard,
    _port: Option<UnitsGuard>,
}

impl RelayPermit {
    fn try_acquire(ctx: &AcceptCtx) -> Result<Self, (&'static str, usize)> {
        let port = match (&ctx.port_conns, ctx.max_port_conns) {
            (Some(counter), Some(max)) => {
                Some(UnitsGuard::try_acquire(counter, 1, max).ok_or(("per-port", max))?)
            }
            _ => None,
        };
        let class = UnitsGuard::try_acquire(&ctx.class_conns, 1, ctx.max_class_conns)
            .ok_or(("class", ctx.max_class_conns))?;
        let process_relays = ctx
            .process_budget
            .try_acquire_relay()
            .ok_or(("shared", ctx.process_budget.max()))?;
        Ok(RelayPermit {
            _process_relays: process_relays,
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
        // Every nofile tier strictly partitions forwarding descriptors into
        // non-overlapping listener + relay pools inside usable headroom.
        assert_eq!(process_fd_budget(Some(256)), (42, 21));
        let b256 = RelayBudget::with_limits(42, 21);
        assert!(b256.max_fds() <= 128);
        assert_eq!(
            published_conn_limit_for_budget(&b256),
            ConnLimit::PerPort {
                per_port: 10,
                total: 21
            }
        );

        assert_eq!(process_fd_budget(Some(1024)), (170, 85));
        let b1024 = RelayBudget::with_limits(170, 85);
        assert!(b1024.max_fds() <= 512);
        assert_eq!(
            published_conn_limit_for_budget(&b1024),
            ConnLimit::PerPort {
                per_port: 42,
                total: 85
            }
        );

        assert_eq!(process_fd_budget(Some(2560)), (426, 213));
        let b2560 = RelayBudget::with_limits(426, 213);
        assert!(b2560.max_fds() <= 1280);
        assert_eq!(
            published_conn_limit_for_budget(&b2560),
            ConnLimit::PerPort {
                per_port: 106,
                total: 213
            }
        );

        assert_eq!(
            process_fd_budget(Some(10240)),
            (PROCESS_MAX_LISTENER_FDS, PROCESS_MAX_RELAY_CONNS)
        );
        let bmax = RelayBudget::with_limits(PROCESS_MAX_LISTENER_FDS, PROCESS_MAX_RELAY_CONNS);
        assert_eq!(
            published_conn_limit_for_budget(&bmax),
            ConnLimit::PerPort {
                per_port: PUBLISHED_MAX_CONNS_PER_PORT,
                total: PUBLISHED_MAX_CONNS_TOTAL
            }
        );
        assert_eq!(
            process_fd_budget(None),
            (PROCESS_MAX_LISTENER_FDS, PROCESS_MAX_RELAY_CONNS)
        );

        // Automatic forwards reserve half of both the shared relay ceiling and
        // the listener descriptor ceiling when published TCP also uses daemon
        // relays, and can use the full budget when published ports go straight
        // through gvproxy.
        assert_eq!(
            auto_conn_limit_for_budget(&b256, true),
            ConnLimit::Shared(10)
        );
        assert_eq!(
            auto_conn_limit_for_budget(&b256, false),
            ConnLimit::Shared(21)
        );
        assert_eq!(auto_listener_fd_limit_for_budget(&b256, true), Some(21));
        assert_eq!(auto_listener_fd_limit_for_budget(&b256, false), None);

        assert_eq!(
            auto_conn_limit_for_budget(&b1024, true),
            ConnLimit::Shared(42)
        );
        assert_eq!(
            auto_conn_limit_for_budget(&bmax, true),
            ConnLimit::Shared(MAX_CONNS)
        );
        assert_eq!(
            auto_listener_fd_limit_for_budget(&bmax, true),
            Some(PROCESS_MAX_LISTENER_FDS / 2)
        );

        // Soft-limit raise calculation: lifts launchd's 256 soft limit to
        // TARGET_PROCESS_NOFILE, respects lower hard caps, and never lowers.
        assert_eq!(
            target_nofile_for_limits(256, libc::RLIM_INFINITY),
            Some(TARGET_PROCESS_NOFILE)
        );
        assert_eq!(target_nofile_for_limits(256, 2048), Some(2048));
        assert_eq!(target_nofile_for_limits(256, 256), None);
        assert_eq!(
            target_nofile_for_limits(TARGET_PROCESS_NOFILE, libc::RLIM_INFINITY),
            None
        );
        assert_eq!(
            target_nofile_for_limits(TARGET_PROCESS_NOFILE * 4, libc::RLIM_INFINITY),
            None
        );
        assert_eq!(
            target_nofile_for_limits(libc::RLIM_INFINITY, libc::RLIM_INFINITY),
            None
        );
    }

    #[test]
    fn raise_nofile_limit_lifts_256_soft_limit_to_full_budget() {
        const CHILD_ENV: &str = "KATAMARAN_TEST_NOFILE_CHILD";
        if std::env::var_os(CHILD_ENV).is_some() {
            // Isolated single-test child process: simulate macOS launchd's
            // 256 soft limit and verify `RelayBudget::from_process_limits()`
            // raises it to the full default forwarding budget.
            let mut lim = libc::rlimit {
                rlim_cur: 0,
                rlim_max: 0,
            };
            assert_eq!(unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) }, 0);
            let hard_allows_target =
                lim.rlim_max == libc::RLIM_INFINITY || lim.rlim_max >= TARGET_PROCESS_NOFILE;
            if !hard_allows_target {
                return;
            }
            let low = libc::rlimit {
                rlim_cur: 256,
                rlim_max: lim.rlim_max,
            };
            assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &low) }, 0);
            assert_eq!(current_nofile_limit(), Some(256));

            let budget = RelayBudget::from_process_limits();
            assert!(
                current_nofile_limit().is_none_or(|n| n >= TARGET_PROCESS_NOFILE),
                "expected soft RLIMIT_NOFILE >= {TARGET_PROCESS_NOFILE}, got {:?}",
                current_nofile_limit()
            );
            assert_eq!(budget.max_listener_fds(), PROCESS_MAX_LISTENER_FDS);
            assert_eq!(budget.max(), PROCESS_MAX_RELAY_CONNS);
            assert_eq!(
                auto_conn_limit_for_budget(&budget, false),
                ConnLimit::Shared(MAX_CONNS)
            );
            assert_eq!(
                published_conn_limit_for_budget(&budget),
                ConnLimit::PerPort {
                    per_port: PUBLISHED_MAX_CONNS_PER_PORT,
                    total: PUBLISHED_MAX_CONNS_TOTAL,
                }
            );
            return;
        }

        let out = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "autofwd::tests::raise_nofile_limit_lifts_256_soft_limit_to_full_budget",
            ])
            .env(CHILD_ENV, "1")
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "nofile child check failed ({}): {}",
            out.status,
            String::from_utf8_lossy(&out.stderr)
        );
    }

    #[test]
    fn listener_descriptors_are_capped_and_reclaimed_without_active_clients() {
        let dir = std::env::temp_dir().join(format!("kat-autofwd-lcap-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // 6 listener fds = 2 single-address ports (1 listener + 2 wakeup pipe
        // fds each), plus a disjoint pool of 4 relays.
        let shared = RelayBudget::with_limits(6, 4);
        let mut fwd = AutoForwards::with_limits_and_budget(
            Upstream::AutoForward(fake_guest(&dir)),
            &[Ipv4Addr::LOCALHOST],
            ConnLimit::Shared(4),
            None,
            shared.clone(),
        );

        let p1 = add_free_port(&mut fwd);
        assert_eq!(shared.listener_fds(), 3);
        assert_eq!(shared.active_fds(), 3);

        let p2 = add_free_port(&mut fwd);
        assert_eq!(shared.listener_fds(), 6);
        assert_eq!(shared.active_fds(), 6);

        // Even with zero active clients, a 3rd idle port is rejected before
        // opening any sockets or wakeup pipes.
        let probe = TcpListener::bind("127.0.0.1:0").unwrap();
        let p3 = probe.local_addr().unwrap().port();
        drop(probe);
        let err = fwd.add(p3).unwrap_err();
        assert!(
            matches!(err, AddError::CapacityExhausted(ref msg) if msg.contains("forward descriptor limit reached")),
            "unexpected error: {err:?}"
        );
        assert_eq!(shared.listener_fds(), 6);
        assert!(!fwd.contains(p3));

        // Live relays still work at 100% listener saturation thanks to the
        // disjoint relay descriptor pool.
        assert_eq!(round_trip(p1, "still-works"), format!("{p1}:still-works"));

        // Removing a port reclaims its 3 descriptors once its listener + pipe
        // have closed, allowing a new port to bind.
        assert!(fwd.remove(p1));
        assert_eq!(shared.listener_fds(), 3);
        assert!(fwd.add(p3).unwrap());
        assert_eq!(shared.listener_fds(), 6);
        assert_eq!(round_trip(p3, "reclaimed"), format!("{p3}:reclaimed"));

        drop(fwd);
        assert_eq!(shared.listener_fds(), 0);
        assert_eq!(shared.active(), 0);
        assert_eq!(shared.active_fds(), 0);
        let _ = p2;
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn listener_budget_charges_only_bound_sockets_with_missing_alias_and_low_port() {
        let missing = Ipv4Addr::new(198, 51, 100, 200);
        assert!(
            !crate::net::is_loopback_interface_addr(missing),
            "test expects {missing} not to be assigned to lo0"
        );
        let dir =
            std::env::temp_dir().join(format!("kat-autofwd-missing-alias-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Only 3 listener fds available (2 wakeup pipe + 1 socket), yet two
        // addresses are configured (localhost + an unassigned alias). Filtering
        // unassigned addresses first lets localhost bind within 3 fds.
        let shared = RelayBudget::with_limits(3, 4);
        let mut fwd = AutoForwards::with_limits_and_budget(
            Upstream::AutoForward(fake_guest(&dir)),
            &[Ipv4Addr::LOCALHOST, missing],
            ConnLimit::Shared(4),
            None,
            shared.clone(),
        );

        let high = add_free_port(&mut fwd);
        assert_eq!(shared.listener_fds(), 3);
        assert_eq!(fwd.bound_addrs(high), Some(&[Ipv4Addr::LOCALHOST][..]));
        assert_eq!(round_trip(high, "hi"), format!("{high}:hi"));
        assert!(fwd.remove(high));
        assert_eq!(shared.listener_fds(), 0);

        // Privileged low port also succeeds in 3 fds.
        let (low, added) = crate::net::try_test_low_port(|p| fwd.add(p));
        assert!(added);
        assert_eq!(shared.listener_fds(), 3);
        assert_eq!(round_trip(low, "low"), format!("{low}:low"));
        assert!(fwd.remove(low));
        assert_eq!(shared.listener_fds(), 0);

        // Direct bind_on_reserved check: when a bind runs out of slots, zero
        // reservations remain leaked.
        let slots = Arc::new(AtomicUsize::new(0));
        let (_low2, (listener, permits)) = crate::net::try_test_low_port(|p| {
            LoopbackTcpListener::bind_on_reserved(&[Ipv4Addr::LOCALHOST, missing], p, || {
                UnitsGuard::try_acquire(&slots, 1, 1)
            })
        });
        assert_eq!(permits.len(), 1);
        assert_eq!(slots.load(Ordering::Acquire), 1);
        drop(listener);
        drop(permits);
        assert_eq!(slots.load(Ordering::Acquire), 0);

        drop(fwd);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn capacity_preflight_serializes_with_failed_concurrent_bind_reservation() {
        let dir =
            std::env::temp_dir().join(format!("kat-autofwd-preflight-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Only 3 shared listener fds available. Simulate an automatic bind
        // holding the bind lock and reserving 2 pipe fds before failing and
        // releasing them without calling `notify_listener_released`: a
        // concurrent published `check_listener_capacity` must block until the
        // failed attempt unwinds and then see all 3 descriptors available.
        let shared = RelayBudget::with_limits(3, 4);
        let published = AutoForwards::with_limits_and_budget(
            Upstream::PerPort(dir.clone()),
            &[Ipv4Addr::LOCALHOST],
            published_conn_limit_for_budget(&shared),
            None,
            shared.clone(),
        );

        let bind_guard = shared.lock_listener_bind();
        let temp_pipe = shared
            .try_acquire_listener(WAKEUP_PIPE_FDS, &Arc::new(AtomicUsize::new(0)), None)
            .unwrap();
        assert_eq!(shared.listener_fds(), WAKEUP_PIPE_FDS);

        let started = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicBool::new(false));
        let s2 = started.clone();
        let d2 = done.clone();
        let checker = thread::spawn(move || {
            s2.store(true, Ordering::SeqCst);
            let res = published.check_listener_capacity();
            d2.store(true, Ordering::SeqCst);
            res
        });

        while !started.load(Ordering::SeqCst) {
            thread::sleep(Duration::from_millis(1));
        }
        thread::sleep(Duration::from_millis(20));
        assert!(
            !done.load(Ordering::SeqCst),
            "preflight must wait for the in-flight bind lock"
        );

        // Failed bind unwinds: drops its transient pipe permits and releases
        // the bind lock without calling notify_listener_released.
        drop(temp_pipe);
        drop(bind_guard);

        checker
            .join()
            .unwrap()
            .expect("preflight must observe released descriptors after failed bind");
        assert_eq!(shared.listener_fds(), 0);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn automatic_listeners_reserve_descriptor_budget_for_published_ports() {
        let dir = std::env::temp_dir().join(format!("kat-autofwd-lreserve-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // 12 shared listener fds (4 single-address ports total): automatic
        // forwards are capped at 6 fds (2 ports), reserving 6 fds (2 ports) for
        // published Docker/Kubernetes services.
        let shared = RelayBudget::with_limits(12, 4);
        let guest_sock = fake_guest(&dir);
        let mut auto = AutoForwards::with_limits_and_budget(
            Upstream::AutoForward(guest_sock),
            &[Ipv4Addr::LOCALHOST],
            auto_conn_limit_for_budget(&shared, true),
            auto_listener_fd_limit_for_budget(&shared, true),
            shared.clone(),
        );
        let mut published = AutoForwards::with_limits_and_budget(
            Upstream::PerPort(dir.clone()),
            &[Ipv4Addr::LOCALHOST],
            published_conn_limit_for_budget(&shared),
            None,
            shared.clone(),
        );

        let _a1 = add_free_port(&mut auto);
        let _a2 = add_free_port(&mut auto);
        assert_eq!(shared.listener_fds(), 6);

        // 3rd automatic port hits its class ceiling (6/6), leaving half free.
        let probe = TcpListener::bind("127.0.0.1:0").unwrap();
        let a3 = probe.local_addr().unwrap().port();
        drop(probe);
        assert!(auto.add(a3).is_err());
        assert_eq!(shared.listener_fds(), 6);

        // Published services still bind 2 ports using their reserved half.
        let p1 = add_free_port(&mut published);
        let _p2 = add_free_port(&mut published);
        assert_eq!(shared.listener_fds(), 12);

        // Published port relays normally while listener budget is 12/12 full.
        let pub_listener = UnixListener::bind(Upstream::socket(&dir, p1)).unwrap();
        let pub_server = thread::spawn(move || {
            let (mut c, _) = pub_listener.accept().unwrap();
            c.write_all(b"gv:").unwrap();
            let mut rest = c.try_clone().unwrap();
            let _ = copy(&mut rest, &mut c);
            let _ = c.shutdown(Shutdown::Write);
        });
        assert_eq!(round_trip(p1, "ok"), "gv:ok");
        pub_server.join().unwrap();

        // 3rd published port hits the shared listener ceiling (12/12).
        let probe2 = TcpListener::bind("127.0.0.1:0").unwrap();
        let p3 = probe2.local_addr().unwrap().port();
        drop(probe2);
        assert!(published.add(p3).is_err());

        drop(auto);
        drop(published);
        assert_eq!(shared.listener_fds(), 0);
        assert_eq!(shared.active_fds(), 0);
        std::fs::remove_dir_all(&dir).unwrap();
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
        let mut auto = AutoForwards::with_limits_and_budget(
            Upstream::AutoForward(guest_sock),
            &[Ipv4Addr::LOCALHOST],
            auto_conn_limit_for_budget(&shared, true),
            auto_listener_fd_limit_for_budget(&shared, true),
            shared.clone(),
        );
        let mut published = AutoForwards::with_limits_and_budget(
            Upstream::PerPort(dir.clone()),
            &[Ipv4Addr::LOCALHOST],
            published_conn_limit_for_budget(&shared),
            None,
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
        let (port, added) = crate::net::try_test_low_port(|p| fwd.add(p));
        assert!(added);
        assert_eq!(round_trip(port, "hi"), format!("{port}:hi"));
        assert!(fwd.remove(port));
        fwd.clear();
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
