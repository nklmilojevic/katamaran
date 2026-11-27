//! VM root shell and exec service, served to the host on
//! [`kat_proto::VM_EXEC_PORT`] (see [`kat_proto::vm_exec`]).
//!
//! Spawns root shells and arbitrary commands directly in PID 1's own
//! namespaces so developers can inspect containerd, dockerd, k3s, sysctls,
//! rlimits, and guest logs without needing Kubernetes or an LXC machine.
//! Idle overhead is a single vsock listener thread.

use std::collections::{HashMap, HashSet, VecDeque};
use std::ffi::CString;
use std::fs;
use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, LazyLock, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use kat_proto::VM_EXEC_PORT;
use kat_proto::vm_exec::{self, CHUNK, ExecSpec, Tty, VmExecMsg};

use super::machines::cmd;
use super::service::{ChildTarget, PgrpTarget, Reaper, UnlockedSpawnSpec};
use super::sys;

const DEFAULT_SHELL: &str = "/bin/sh";
const DEFAULT_HOME: &str = "/root";
const VM_PATH: &str =
    "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/var/lib/katamaran/k3s/bin";
const DEFAULT_PS1: &str = r"\h:\w\$ ";
const MAX_CONNS: usize = 64;
/// Separate cap for short-lived handshake / out-of-band control connections
/// (`Ping`, `SignalSession`) so signals can still be delivered when all
/// [`MAX_CONNS`] exec session slots are occupied.
const MAX_PENDING_HANDSHAKES: usize = 64;
const ACCEPT_ERR_BACKOFF: Duration = Duration::from_millis(50);
const MAX_ERROR_BYTES: usize = 64 << 10;
/// Total wall-clock budget for reading the initial handshake (`Ping` / `Exec`).
const FIRST_FRAME_TIMEOUT: Duration = Duration::from_secs(10);
/// Grace period after `SIGHUP` on host disconnect before escalating to `SIGKILL`.
const DISCONNECT_GRACE: Duration = Duration::from_secs(2);
/// Probe interval while guest stdin is backpressured so host disconnect is
/// still detected even when the host vsock proxy is in `WaitingCreditUpdate`
/// with a full guest receive window.
const BACKPRESSURE_PROBE_INTERVAL: Duration = Duration::from_millis(500);
/// Idle window after the main child exits before cancelling pumps held open by
/// a surviving background descendant.
const POST_EXIT_IDLE: Duration = Duration::from_millis(200);
/// Maximum buffered stdin bytes held in user space while guest stdin is
/// backpressured, keeping memory bounded while still monitoring disconnect.
const STDIN_BUF_CAP: usize = 128 << 10;
/// Maximum queued output frames held between output pumps and the nonblocking
/// guest socket writer so backpressure stays bounded without holding a socket
/// mutex across blocking writes.
const GUEST_OUT_QUEUE_CAP: usize = 2;
/// Maximum queued control-reply frames (`SignalAck`) held when a peer stops
/// reading, preventing unbounded heap growth under in-band `Signal` floods.
const GUEST_CTRL_QUEUE_CAP: usize = 16;
const CGROUP_PARENT: &str = "/sys/fs/cgroup/kat-vm-exec";

type SharedChildHandle = Arc<Mutex<Option<ChildHandle>>>;

static NEXT_CGROUP_ID: AtomicUsize = AtomicUsize::new(1);
static LIVE_CGROUPS: LazyLock<Mutex<HashSet<usize>>> = LazyLock::new(|| Mutex::new(HashSet::new()));
static SESSION_SEQ: AtomicU64 = AtomicU64::new(1);
static SESSION_REGISTRY: LazyLock<Mutex<HashMap<[u8; 16], SharedChildHandle>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn random_session_token() -> [u8; 16] {
    let mut token = [0u8; 16];
    let mut filled = 0usize;
    while filled < token.len() {
        let rc = unsafe {
            libc::getrandom(token[filled..].as_mut_ptr().cast(), token.len() - filled, 0)
        };
        if rc > 0 {
            filled += rc as usize;
        } else if rc < 0 && io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
            continue;
        } else {
            let seq = SESSION_SEQ.fetch_add(1, Ordering::Relaxed);
            token[..8].copy_from_slice(&seq.to_le_bytes());
            token[8..12].copy_from_slice(&(std::process::id()).to_le_bytes());
            break;
        }
    }
    token
}

struct SessionRegGuard([u8; 16]);

impl SessionRegGuard {
    fn register(handle: Arc<Mutex<Option<ChildHandle>>>) -> Self {
        let mut reg = SESSION_REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            let token = random_session_token();
            if let std::collections::hash_map::Entry::Vacant(e) = reg.entry(token) {
                e.insert(handle);
                return Self(token);
            }
        }
    }

    fn token(&self) -> [u8; 16] {
        self.0
    }
}

impl Drop for SessionRegGuard {
    fn drop(&mut self) {
        SESSION_REGISTRY
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.0);
    }
}

struct ConnGuard(Arc<AtomicUsize>);

impl ConnGuard {
    fn try_acquire(active: &Arc<AtomicUsize>, limit: usize) -> Option<Self> {
        let mut cur = active.load(Ordering::Acquire);
        loop {
            if cur >= limit {
                return None;
            }
            match active.compare_exchange_weak(cur, cur + 1, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return Some(ConnGuard(Arc::clone(active))),
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

/// Enforces a single wall-clock `deadline` across every read/write syscall on
/// `conn` using `poll` + `MSG_DONTWAIT`, so an idle or trickling peer cannot
/// retain a connection slot past the handshake budget and no persistent socket
/// timeouts remain on the descriptor once the session starts.
struct DeadlineStream<'a> {
    conn: &'a mut UnixStream,
    deadline: Instant,
}

impl DeadlineStream<'_> {
    fn remaining_ms(&self) -> io::Result<i32> {
        let left = self.deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "VM exec handshake timed out",
            ))
        } else {
            Ok(left.as_millis().clamp(1, i32::MAX as u128) as i32)
        }
    }

    fn poll_ready(&self, events: libc::c_short) -> io::Result<()> {
        loop {
            let timeout_ms = self.remaining_ms()?;
            let mut pfd = libc::pollfd {
                fd: self.conn.as_raw_fd(),
                events,
                revents: 0,
            };
            let rc = unsafe { libc::poll(&mut pfd, 1, timeout_ms) };
            if rc > 0 {
                return Ok(());
            }
            if rc == 0 {
                if Instant::now() >= self.deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "VM exec handshake timed out",
                    ));
                }
                continue;
            }
            let e = io::Error::last_os_error();
            if e.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(e);
        }
    }
}

impl Read for DeadlineStream<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            self.remaining_ms()?;
            return Ok(0);
        }
        loop {
            self.poll_ready(libc::POLLIN)?;
            let rc = unsafe {
                libc::recv(
                    self.conn.as_raw_fd(),
                    buf.as_mut_ptr().cast(),
                    buf.len(),
                    libc::MSG_DONTWAIT,
                )
            };
            if rc >= 0 {
                return Ok(rc as usize);
            }
            let e = io::Error::last_os_error();
            if matches!(
                e.kind(),
                io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
            ) {
                continue;
            }
            return Err(e);
        }
    }
}

impl Write for DeadlineStream<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            self.remaining_ms()?;
            return Ok(0);
        }
        loop {
            self.poll_ready(libc::POLLOUT)?;
            let rc = unsafe {
                libc::send(
                    self.conn.as_raw_fd(),
                    buf.as_ptr().cast(),
                    buf.len(),
                    libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL,
                )
            };
            if rc >= 0 {
                return Ok(rc as usize);
            }
            let e = io::Error::last_os_error();
            if matches!(
                e.kind(),
                io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
            ) {
                continue;
            }
            return Err(e);
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.remaining_ms()?;
        Ok(())
    }
}

fn pipe_nonblock() -> io::Result<(OwnedFd, OwnedFd)> {
    let (r, w) = cmd::pipe()?;
    set_nonblocking(&r)?;
    set_nonblocking(&w)?;
    Ok((r, w))
}

fn notify_wake(w: &OwnedFd) {
    let b: u8 = 1;
    unsafe {
        let _ = libc::write(w.as_raw_fd(), (&b as *const u8).cast(), 1);
    }
}

fn drain_wake(r: &OwnedFd) {
    let mut buf = [0u8; 64];
    loop {
        let rc = unsafe { libc::read(r.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
        if rc <= 0 {
            break;
        }
    }
}

/// Nonblocking zero-timeout check for whether all write ends of `fd` have closed.
fn fd_has_hup(fd: RawFd) -> bool {
    let mut pfd = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    let rc = unsafe { libc::poll(&mut pfd, 1, 0) };
    rc > 0 && (pfd.revents & libc::POLLHUP) != 0
}

/// Query exact unread bytes buffered in a Linux pipe without consuming any.
fn pipe_unread_bytes(fd: RawFd) -> Option<usize> {
    let mut avail: libc::c_int = 0;
    if unsafe { libc::ioctl(fd, libc::FIONREAD as _, &mut avail) } == 0 && avail >= 0 {
        Some(avail as usize)
    } else {
        None
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
enum OutputStream {
    Stdout = 0,
    Stderr = 1,
}

impl OutputStream {
    fn encode_frame(self, data: Vec<u8>) -> Vec<u8> {
        match self {
            Self::Stdout => VmExecMsg::Stdout(data).encode(),
            Self::Stderr => VmExecMsg::Stderr(data).encode(),
        }
    }
}

#[derive(Default)]
struct GuestWriterState {
    ctrl: VecDeque<Vec<u8>>,
    queues: [VecDeque<Vec<u8>>; 2],
    /// Remaining per-stream output byte windows (`None` = unmetered, e.g. PTY
    /// or merged stdout/stderr).
    windows: [Option<u64>; 2],
    /// Round-robin selector between stdout (`0`) and stderr (`1`) so neither
    /// stream starves the other inside the guest writer.
    next_stream: usize,
    final_frame: Option<Vec<u8>>,
    /// Coalesced stdin credit (`InputWindow`) not yet sent to the host.
    input_credit: u64,
    probe_pending: bool,
    child_exited: bool,
    post_exit: bool,
    finish: bool,
    closed: bool,
}

/// Nonblocking single-owner writer for all guest-to-host frames (`Stdout`,
/// `Stderr`, `SignalAck`, `Pong` backpressure probes, and terminal `Exit`/`Error`).
///
/// Decoupling socket writes from `pump` threads eliminates blocking I/O under a
/// shared mutex, gives stdout and stderr independent queues and optional
/// per-stream byte windows so a stalled host stdout pipe never blocks stderr,
/// lets `SignalAck` and `Pong` probes preempt queued output without blocking
/// `stdin_worker`, and periodically retries stalled nonblocking sends so Linux
/// `AF_VSOCK` `SEND_SHUTDOWN` (`EPIPE`) is detected even when `vsock_poll()`
/// reports neither `POLLOUT` nor `POLLHUP`.
struct GuestWriter {
    state: Mutex<GuestWriterState>,
    cv: Condvar,
    wake_w: OwnedFd,
    ctrl_space_w: OwnedFd,
}

impl GuestWriter {
    #[cfg(test)]
    fn start(
        stream: UnixStream,
        host_disconnected: Arc<AtomicBool>,
        disconnect_w: OwnedFd,
        backpressure_probe: Duration,
    ) -> io::Result<(Arc<Self>, OwnedFd, thread::JoinHandle<()>)> {
        Self::start_with_window(
            stream,
            host_disconnected,
            disconnect_w,
            backpressure_probe,
            0,
        )
    }

    fn start_with_window(
        stream: UnixStream,
        host_disconnected: Arc<AtomicBool>,
        disconnect_w: OwnedFd,
        backpressure_probe: Duration,
        output_window: u32,
    ) -> io::Result<(Arc<Self>, OwnedFd, thread::JoinHandle<()>)> {
        stream.set_nonblocking(true)?;
        let (wake_r, wake_w) = pipe_nonblock()?;
        let (ctrl_space_r, ctrl_space_w) = pipe_nonblock()?;
        let windows = if output_window > 0 {
            [
                Some(u64::from(output_window)),
                Some(u64::from(output_window)),
            ]
        } else {
            [None, None]
        };
        let writer = Arc::new(Self {
            state: Mutex::new(GuestWriterState {
                windows,
                ..GuestWriterState::default()
            }),
            cv: Condvar::new(),
            wake_w,
            ctrl_space_w,
        });
        let writer_bg = writer.clone();
        let handle = thread::Builder::new()
            .name("vm-exec-tx".into())
            .spawn(move || {
                guest_writer_worker(
                    stream,
                    writer_bg,
                    wake_r,
                    host_disconnected,
                    disconnect_w,
                    backpressure_probe,
                );
            })?;
        Ok((writer, ctrl_space_r, handle))
    }

    /// Wait until `stream` has queue space and (when flow-controlled) at least
    /// 1 byte of window credit. Returns `Ok(Some(allowed_bytes))` with
    /// `1..=max_len`, `Ok(None)` if `child_exited` transitioned or `deadline`
    /// expired, or `Err(BrokenPipe)` if the session closed.
    fn wait_for_send_budget(
        &self,
        stream: OutputStream,
        max_len: usize,
        known_child_exited: bool,
        deadline: Option<Instant>,
    ) -> io::Result<Option<usize>> {
        let idx = stream as usize;
        let mut st = self.state.lock().unwrap();
        loop {
            if st.closed {
                return Err(io::Error::new(io::ErrorKind::BrokenPipe, "exec closed"));
            }
            if !known_child_exited && st.child_exited {
                return Ok(None);
            }
            let queue_ok = st.queues[idx].len() < GUEST_OUT_QUEUE_CAP;
            let win_avail = match st.windows[idx] {
                None => max_len,
                Some(w) => (w as usize).min(max_len),
            };
            if queue_ok && win_avail > 0 {
                return Ok(Some(win_avail));
            }
            match deadline {
                None => {
                    st = self.cv.wait(st).unwrap();
                }
                Some(dl) => {
                    let rem = dl.saturating_duration_since(Instant::now());
                    if rem.is_zero() {
                        return Ok(None);
                    }
                    let (guard, _) = self.cv.wait_timeout(st, rem).unwrap();
                    st = guard;
                }
            }
        }
    }

    fn send_output(
        &self,
        stream: OutputStream,
        frame: Vec<u8>,
        payload_len: usize,
    ) -> io::Result<()> {
        let idx = stream as usize;
        let mut st = self.state.lock().unwrap();
        if st.closed {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "exec closed"));
        }
        if let Some(w) = st.windows[idx].as_mut() {
            *w = w.saturating_sub(payload_len as u64);
        }
        st.queues[idx].push_back(frame);
        drop(st);
        notify_wake(&self.wake_w);
        Ok(())
    }

    fn add_window(&self, stream: u8, bytes: u32) {
        let idx = usize::from(stream);
        if idx >= 2 || bytes == 0 {
            return;
        }
        let mut st = self.state.lock().unwrap();
        if st.closed {
            return;
        }
        if let Some(w) = st.windows[idx].as_mut() {
            *w = w.saturating_add(u64::from(bytes));
            self.cv.notify_all();
        }
    }

    fn ctrl_has_space(&self) -> bool {
        let st = self.state.lock().unwrap();
        !st.closed && st.ctrl.len() < GUEST_CTRL_QUEUE_CAP
    }

    fn send_ctrl(&self, frame: Vec<u8>) {
        let mut st = self.state.lock().unwrap();
        if st.closed || st.ctrl.len() >= GUEST_CTRL_QUEUE_CAP {
            return;
        }
        st.ctrl.push_back(frame);
        drop(st);
        notify_wake(&self.wake_w);
    }

    /// True when the host negotiated flow control (`ExecSpec::output_window`),
    /// which also bounds unacknowledged stdin to [`vm_exec::INPUT_WINDOW`].
    fn windowed(&self) -> bool {
        self.state.lock().unwrap().windows[0].is_some()
    }

    /// Queue (coalesced) stdin credit for the host in windowed sessions. Sent
    /// ahead of queued output so it is never delayed by output backpressure
    /// beyond the frame currently in flight.
    fn add_input_credit(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let mut st = self.state.lock().unwrap();
        if st.closed {
            return;
        }
        st.input_credit = st.input_credit.saturating_add(bytes);
        drop(st);
        notify_wake(&self.wake_w);
    }

    fn request_probe(&self) {
        let mut st = self.state.lock().unwrap();
        if st.closed {
            return;
        }
        st.probe_pending = true;
        drop(st);
        notify_wake(&self.wake_w);
    }

    /// Enable post-exit peer disconnect probes and wake any pump waiting on
    /// window/queue capacity so it transitions to its post-exit drain window.
    fn mark_post_exit(&self) {
        let mut st = self.state.lock().unwrap();
        if st.closed {
            return;
        }
        st.child_exited = true;
        st.post_exit = true;
        self.cv.notify_all();
        drop(st);
        notify_wake(&self.wake_w);
    }

    fn finish_with(&self, final_frame: Option<Vec<u8>>) {
        let mut st = self.state.lock().unwrap();
        if st.closed {
            return;
        }
        if final_frame.is_some() {
            st.final_frame = final_frame;
        }
        st.finish = true;
        drop(st);
        notify_wake(&self.wake_w);
    }

    fn abort(&self) {
        let mut st = self.state.lock().unwrap();
        st.closed = true;
        st.ctrl.clear();
        st.queues[0].clear();
        st.queues[1].clear();
        st.final_frame = None;
        self.cv.notify_all();
        drop(st);
        notify_wake(&self.wake_w);
        notify_wake(&self.ctrl_space_w);
    }
}

fn guest_writer_worker(
    stream: UnixStream,
    writer: Arc<GuestWriter>,
    wake_r: OwnedFd,
    host_disconnected: Arc<AtomicBool>,
    disconnect_w: OwnedFd,
    backpressure_probe: Duration,
) {
    let mut out_buf: Vec<u8> = Vec::new();
    let mut out_off: usize = 0;
    let mut failed = false;

    loop {
        if out_off == out_buf.len() {
            out_buf.clear();
            out_off = 0;
            let mut wake_ctrl_space = false;
            {
                let mut st = writer.state.lock().unwrap();
                if st.closed {
                    return;
                }
                if let Some(frame) = st.ctrl.pop_front() {
                    if st.ctrl.len() < GUEST_CTRL_QUEUE_CAP {
                        wake_ctrl_space = true;
                    }
                    out_buf = frame;
                } else if st.input_credit > 0 {
                    let bytes = st.input_credit.min(u64::from(u32::MAX));
                    st.input_credit -= bytes;
                    out_buf = VmExecMsg::InputWindow {
                        bytes: bytes as u32,
                    }
                    .encode();
                } else {
                    let first = st.next_stream;
                    let second = 1 - first;
                    if let Some(frame) = st.queues[first].pop_front() {
                        st.next_stream = second;
                        out_buf = frame;
                        writer.cv.notify_all();
                    } else if let Some(frame) = st.queues[second].pop_front() {
                        st.next_stream = first;
                        out_buf = frame;
                        writer.cv.notify_all();
                    } else if st.probe_pending {
                        st.probe_pending = false;
                        out_buf = VmExecMsg::Pong.encode();
                    } else if let Some(frame) = st.final_frame.take() {
                        out_buf = frame;
                    } else if st.finish {
                        return;
                    }
                }
            }
            if wake_ctrl_space {
                notify_wake(&writer.ctrl_space_w);
            }
        }

        if out_off < out_buf.len() {
            let mut flush_done = false;
            loop {
                let rc = unsafe {
                    libc::send(
                        stream.as_raw_fd(),
                        out_buf[out_off..].as_ptr().cast(),
                        out_buf.len() - out_off,
                        libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL,
                    )
                };
                if rc > 0 {
                    out_off += rc as usize;
                    if out_off == out_buf.len() {
                        out_buf.clear();
                        out_off = 0;
                        flush_done = true;
                        break;
                    }
                } else if rc == 0 {
                    failed = true;
                    break;
                } else {
                    let e = io::Error::last_os_error();
                    match e.kind() {
                        io::ErrorKind::Interrupted => continue,
                        io::ErrorKind::WouldBlock => break,
                        _ => {
                            failed = true;
                            break;
                        }
                    }
                }
            }
            if failed {
                break;
            }
            if flush_done {
                continue;
            }
        }

        let need_write = out_off < out_buf.len();
        let (post_exit, finishing) = {
            let st = writer.state.lock().unwrap();
            if st.closed {
                return;
            }
            (st.post_exit, st.finish)
        };
        // On Linux AF_VSOCK, peer `VSOCK_FLAGS_SHUTDOWN_RCV` sets
        // `SEND_SHUTDOWN` without raising `POLLOUT` or `POLLHUP` in
        // `vsock_poll()`. Bound `poll` by `backpressure_probe` whenever a send
        // is stalled (or while draining output after leader exit) so `send()`
        // is retried directly and surfaces `EPIPE` promptly.
        let poll_timeout_ms = if need_write || (post_exit && !finishing) {
            backpressure_probe.as_millis().clamp(1, i32::MAX as u128) as i32
        } else {
            -1
        };
        let mut sock_events = if need_write { libc::POLLOUT } else { 0 };
        if post_exit {
            sock_events |= libc::POLLRDHUP;
        }
        let mut pfds = [
            libc::pollfd {
                fd: wake_r.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: stream.as_raw_fd(),
                events: sock_events,
                revents: 0,
            },
        ];
        let rc = unsafe { libc::poll(pfds.as_mut_ptr(), 2, poll_timeout_ms) };
        if rc < 0 {
            if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                continue;
            }
            failed = true;
            break;
        }
        if rc == 0 {
            if !need_write && post_exit && !finishing {
                out_buf = VmExecMsg::Pong.encode();
                out_off = 0;
            }
            continue;
        }
        if pfds[0].revents != 0 {
            drain_wake(&wake_r);
            if writer.state.lock().unwrap().closed {
                return;
            }
        }
        if (post_exit && (pfds[1].revents & libc::POLLRDHUP) != 0)
            || ((pfds[1].revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL)) != 0
                && (pfds[1].revents & libc::POLLOUT) == 0)
        {
            failed = true;
            break;
        }
    }

    if failed {
        host_disconnected.store(true, Ordering::Release);
        let mut st = writer.state.lock().unwrap();
        st.closed = true;
        st.ctrl.clear();
        st.queues[0].clear();
        st.queues[1].clear();
        st.final_frame = None;
        writer.cv.notify_all();
        drop(st);
        notify_wake(&disconnect_w);
    }
}

enum ChildHandle {
    /// PTY session leader spawned with `setsid`, guarded by both `pidfd` and
    /// a token-pinned [`PgrpTarget`] so PID reuse can never be signaled.
    Pty {
        target: ChildTarget,
        pgrp: PgrpTarget,
        /// Dup of the PTY master, used to find the terminal's foreground
        /// process group for out-of-band signals.
        tty: OwnedFd,
    },
    /// Non-PTY process group leader spawned with `setpgid(0, 0)`, guarded by
    /// a token-pinned [`PgrpTarget`] inside [`Reaper`].
    Pipe(PgrpTarget),
}

impl ChildHandle {
    fn signal(&self, reaper: &Reaper, sig: i32) {
        match self {
            ChildHandle::Pty { target, pgrp, tty } => {
                if matches!(sig, libc::SIGHUP | libc::SIGKILL) {
                    reaper.signal_target(target, sig);
                    let _ = reaper.signal_pgrp_target(*pgrp, sig);
                    return;
                }
                // Like a terminal: deliver to the foreground process group
                // (e.g. `sleep` under a non-interactive `bash -c`, or a job
                // an interactive shell put in its own group), never outside
                // this session.
                let fg = unsafe { libc::tcgetpgrp(tty.as_raw_fd()) };
                if fg == pgrp.id() {
                    let _ = reaper.signal_pgrp_target(*pgrp, sig);
                } else if fg > 0 && unsafe { libc::getsid(fg) } == pgrp.id() {
                    unsafe { libc::kill(-fg, sig) };
                } else {
                    reaper.signal_target(target, sig);
                }
            }
            ChildHandle::Pipe(pgrp) => {
                let _ = reaper.signal_pgrp_target(*pgrp, sig);
            }
        }
    }
}

fn signal_handle(reaper: &Reaper, handle: &Mutex<Option<ChildHandle>>, sig: i32) {
    let guard = handle.lock().unwrap();
    if let Some(h) = guard.as_ref() {
        h.signal(reaper, sig);
    }
}

fn cgroup_events_populated(text: &str) -> bool {
    text.lines().any(|line| line.trim() == "populated 1")
}

enum CgroupMode<'a> {
    /// Production mode: require per-session cgroup v2 isolation and fail loudly
    /// rather than silently falling back to leader-only disconnect cleanup.
    Required(&'a Path),
    /// Unprivileged unit-test mode when `/sys/fs/cgroup` is not writable.
    #[cfg(test)]
    BestEffort(&'a Path),
}

/// Per-session cgroup v2 leaf under `/sys/fs/cgroup/kat-vm-exec/s-<n>`.
///
/// On host disconnect, allows finishing `SIGKILL` escalation for surviving
/// descendants even after the process-group leader has exited and been reaped,
/// with zero PID/PGID reuse risk.
struct SessionCgroup {
    id: usize,
    path: PathBuf,
    kill_fd: OwnedFd,
    events_fd: OwnedFd,
}

impl SessionCgroup {
    fn create(parent: &Path) -> io::Result<(Self, OwnedFd)> {
        fs::create_dir_all(parent)?;
        let id = NEXT_CGROUP_ID.fetch_add(1, Ordering::Relaxed);
        {
            let mut live = LIVE_CGROUPS.lock().unwrap_or_else(|e| e.into_inner());
            live.insert(id);
            // Best-effort sweep of empty leftover directories from prior
            // sessions whose detached background processes have since exited,
            // never touching any cgroup currently owned by an active session.
            if let Ok(entries) = fs::read_dir(parent) {
                for entry in entries.flatten() {
                    let name = entry.file_name();
                    let is_live = name
                        .to_str()
                        .and_then(|s| s.strip_prefix("s-"))
                        .and_then(|s| s.parse::<usize>().ok())
                        .is_some_and(|n| live.contains(&n));
                    if !is_live {
                        let _ = fs::remove_dir(entry.path());
                    }
                }
            }
        }
        let release_id = || {
            LIVE_CGROUPS
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&id);
        };
        let path = parent.join(format!("s-{id}"));
        if let Err(e) = fs::create_dir(&path) {
            release_id();
            return Err(e);
        }
        let open_cloexec = |name: &str, flags: libc::c_int| -> io::Result<OwnedFd> {
            let p = path.join(name);
            let c = CString::new(p.as_os_str().as_encoded_bytes())
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL in cgroup path"))?;
            let fd = unsafe { libc::open(c.as_ptr(), flags | libc::O_CLOEXEC) };
            if fd < 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(unsafe { OwnedFd::from_raw_fd(fd) })
            }
        };
        let procs_fd = match open_cloexec("cgroup.procs", libc::O_WRONLY) {
            Ok(fd) => fd,
            Err(e) => {
                let _ = fs::remove_dir(&path);
                release_id();
                return Err(e);
            }
        };
        let kill_fd = match open_cloexec("cgroup.kill", libc::O_WRONLY) {
            Ok(fd) => fd,
            Err(e) => {
                let _ = fs::remove_dir(&path);
                release_id();
                return Err(e);
            }
        };
        let events_fd = match open_cloexec("cgroup.events", libc::O_RDONLY) {
            Ok(fd) => fd,
            Err(e) => {
                let _ = fs::remove_dir(&path);
                release_id();
                return Err(e);
            }
        };
        Ok((
            SessionCgroup {
                id,
                path,
                kill_fd,
                events_fd,
            },
            procs_fd,
        ))
    }

    fn is_populated(&self) -> bool {
        let mut buf = [0u8; 128];
        let n = unsafe {
            libc::pread(
                self.events_fd.as_raw_fd(),
                buf.as_mut_ptr().cast(),
                buf.len(),
                0,
            )
        };
        if n <= 0 {
            return false;
        }
        let text = std::str::from_utf8(&buf[..n as usize]).unwrap_or("");
        cgroup_events_populated(text)
    }

    fn kill(&self) {
        unsafe {
            let _ = libc::pwrite(self.kill_fd.as_raw_fd(), b"1\n".as_ptr().cast(), 2, 0);
        }
    }

    fn wait_empty(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if !self.is_populated() {
                return true;
            }
            let rem = deadline.saturating_duration_since(Instant::now());
            if rem.is_zero() {
                return false;
            }
            let mut pfd = libc::pollfd {
                fd: self.events_fd.as_raw_fd(),
                events: libc::POLLPRI,
                revents: 0,
            };
            let timeout_ms = rem.as_millis().clamp(1, i32::MAX as u128) as i32;
            let _ = unsafe { libc::poll(&mut pfd, 1, timeout_ms) };
        }
    }

    /// Wait up to `grace` after `SIGHUP` for both leader exit (`cancel_r`) and
    /// all session processes in this cgroup to exit; escalate to `SIGKILL` and
    /// `cgroup.kill` if either the leader or any descendant survives past
    /// `grace` (even if the process-group leader itself already exited).
    fn cleanup_after_disconnect(
        &self,
        cancel_r: &OwnedFd,
        grace: Duration,
        reaper: &Reaper,
        handle: &Mutex<Option<ChildHandle>>,
    ) {
        let deadline = Instant::now() + grace;
        let mut leader_exited = false;
        loop {
            if leader_exited && !self.is_populated() {
                return;
            }
            let rem = deadline.saturating_duration_since(Instant::now());
            if rem.is_zero() {
                break;
            }
            let timeout_ms = rem.as_millis().clamp(1, i32::MAX as u128) as i32;
            if !leader_exited {
                // Poll only `cancel_r` while waiting for the leader: kernfs
                // `cgroup.events` reports `POLLPRI|POLLERR` immediately until
                // its first read, which would busy-spin before `leader_exited`.
                let mut pfd = libc::pollfd {
                    fd: cancel_r.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                };
                let rc = unsafe { libc::poll(&mut pfd, 1, timeout_ms) };
                if rc > 0 && pfd.revents != 0 {
                    leader_exited = true;
                }
            } else {
                // `is_populated()` at the top of this loop just read
                // `cgroup.events`, advancing kernfs's event counter so `POLLPRI`
                // sleeps cleanly until the next populated transition.
                let mut pfd = libc::pollfd {
                    fd: self.events_fd.as_raw_fd(),
                    events: libc::POLLPRI,
                    revents: 0,
                };
                let _ = unsafe { libc::poll(&mut pfd, 1, timeout_ms) };
            }
        }
        if !leader_exited {
            signal_handle(reaper, handle, libc::SIGKILL);
        }
        if self.is_populated() {
            self.kill();
            let _ = self.wait_empty(Duration::from_millis(500));
        }
    }
}

impl Drop for SessionCgroup {
    fn drop(&mut self) {
        let _ = fs::remove_dir(&self.path);
        LIVE_CGROUPS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.id);
    }
}

/// Linux signals are `1..=64` (`_NSIG - 1`, i.e. up to `SIGRTMAX`).
fn valid_signal(sig: i32) -> bool {
    (1..=64).contains(&sig)
}

fn truncate_error(s: &str) -> String {
    if s.len() <= MAX_ERROR_BYTES {
        return s.to_string();
    }
    let suffix = "...";
    let mut end = MAX_ERROR_BYTES.saturating_sub(suffix.len());
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    let mut out = String::with_capacity(end + suffix.len());
    out.push_str(&s[..end]);
    out.push_str(suffix);
    out
}

fn winsize(rows: u16, cols: u16) -> libc::winsize {
    libc::winsize {
        ws_row: rows,
        ws_col: cols,
        ws_xpixel: 0,
        ws_ypixel: 0,
    }
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

/// Allocate a PTY master/slave pair with `O_CLOEXEC` set atomically on both
/// descriptors so concurrent forks in other threads can never inherit them.
fn openpty(t: &Tty) -> io::Result<(OwnedFd, OwnedFd)> {
    let flags = libc::O_RDWR | libc::O_NOCTTY | libc::O_CLOEXEC;
    let master_raw = unsafe { libc::open(c"/dev/ptmx".as_ptr(), flags) };
    if master_raw < 0 {
        return Err(io::Error::last_os_error());
    }
    let master = unsafe { OwnedFd::from_raw_fd(master_raw) };

    let unlock: libc::c_int = 0;
    if unsafe { libc::ioctl(master.as_raw_fd(), libc::TIOCSPTLCK as _, &unlock) } < 0 {
        return Err(io::Error::last_os_error());
    }

    let slave_raw = unsafe { libc::ioctl(master.as_raw_fd(), libc::TIOCGPTPEER as _, flags) };
    if slave_raw < 0 {
        return Err(io::Error::last_os_error());
    }
    let slave = unsafe { OwnedFd::from_raw_fd(slave_raw) };

    let ws = winsize(t.rows.max(1), t.cols.max(1));
    if unsafe { libc::ioctl(master.as_raw_fd(), libc::TIOCSWINSZ as _, &ws) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok((master, slave))
}

/// Verify that an accepted vsock peer address is a complete `AF_VSOCK`
/// `sockaddr_vm` originating from the host (`VMADDR_CID_HOST`). Rejects guest
/// loopback (`VMADDR_CID_LOCAL`) and any truncated or non-vsock address.
///
/// This check is the listener's only authentication, and it is sound because
/// no guest workload can appear as a `VMADDR_CID_HOST` peer:
/// - The guest kernel assigns the source address of every vsock connection.
///   Connections made inside the guest (to `VMADDR_CID_LOCAL` or the guest's
///   own CID) use the loopback transport and carry a guest-side source CID, so
///   containers and other guest processes, which share the guest's vsock
///   namespace, are rejected here.
/// - Only the virtio-vsock transport delivers `VMADDR_CID_HOST` peers, and
///   libkrun's muxer creates those connections only when a host process
///   connects to the host-side listener socket for this port
///   (`<home>/run/vm-exec.sock`, inside the daemon's mode-0700 run directory).
/// - A guest cannot loop back through libkrun to reach that listener: the
///   muxer rejects guest TSI connects and proxy creation that target any
///   host-listener port or its Unix socket path (`is_host_listener_id` /
///   `is_protected_unix_path` in `vendor/libkrun/.../vsock/muxer.rs`).
fn peer_is_host(addr: &libc::sockaddr_vm, len: libc::socklen_t) -> bool {
    len as usize == std::mem::size_of::<libc::sockaddr_vm>()
        && addr.svm_family == libc::AF_VSOCK as libc::sa_family_t
        && addr.svm_cid == libc::VMADDR_CID_HOST
}

/// Tracks whether the Linux `n_tty` canonical input buffer holds an uncommitted
/// partial line after applying `termios` input translation (`ISTRIP`, `IGNCR`,
/// `ICRNL`, `INLCR`, `IEXTEN`/`VLNEXT`, `VERASE`, `VKILL`, `VEOF`, `VEOL`,
/// `VEOL2`).
#[derive(Default)]
struct PtyCanonTracker {
    partial_len: usize,
    lnext_pending: bool,
}

impl PtyCanonTracker {
    fn feed(&mut self, termios: &libc::termios, bytes: &[u8]) {
        let istrip = (termios.c_iflag & libc::ISTRIP) != 0;
        let igncr = (termios.c_iflag & libc::IGNCR) != 0;
        let icrnl = (termios.c_iflag & libc::ICRNL) != 0;
        let inlcr = (termios.c_iflag & libc::INLCR) != 0;
        let icanon = (termios.c_lflag & libc::ICANON) != 0;
        let iexten = (termios.c_lflag & libc::IEXTEN) != 0;

        let veof = termios.c_cc[libc::VEOF];
        let veol = termios.c_cc[libc::VEOL];
        let veol2 = termios.c_cc[libc::VEOL2];
        let verase = termios.c_cc[libc::VERASE];
        let vkill = termios.c_cc[libc::VKILL];
        let vlnext = termios.c_cc[libc::VLNEXT];

        for &raw in bytes {
            let mut c = if istrip { raw & 0x7f } else { raw };

            if self.lnext_pending {
                self.lnext_pending = false;
                if icanon {
                    self.partial_len = self.partial_len.saturating_add(1);
                }
                continue;
            }

            if c == b'\r' {
                if igncr {
                    continue;
                }
                if icrnl {
                    c = b'\n';
                }
            } else if c == b'\n' && inlcr {
                c = b'\r';
            }

            if !icanon {
                self.partial_len = 0;
                continue;
            }

            if iexten && vlnext != 0 && c == vlnext {
                self.lnext_pending = true;
                continue;
            }

            if c == b'\n'
                || (veof != 0 && c == veof)
                || (veol != 0 && c == veol)
                || (iexten && veol2 != 0 && c == veol2)
                || (vkill != 0 && c == vkill)
            {
                self.partial_len = 0;
            } else if verase != 0 && c == verase {
                self.partial_len = self.partial_len.saturating_sub(1);
            } else {
                self.partial_len = self.partial_len.saturating_add(1);
            }
        }
    }

    /// Compute the `VEOF` sequence to write to a PTY master on `DataEnd`.
    ///
    /// In noncanonical/raw mode (or when `VEOF` is disabled), returns 0 bytes
    /// so literal control bytes are never injected into raw-mode sessions; raw
    /// PTY readers have no in-band EOF mechanism.
    fn eof_bytes(&self, termios: &libc::termios) -> ([u8; 3], usize) {
        if (termios.c_lflag & libc::ICANON) == 0 {
            return ([0; 3], 0);
        }
        let veof = termios.c_cc[libc::VEOF];
        if veof == 0 {
            return ([0; 3], 0);
        }
        if self.lnext_pending {
            ([veof, veof, veof], 3)
        } else if self.partial_len > 0 {
            ([veof, veof, 0], 2)
        } else {
            ([veof, 0, 0], 1)
        }
    }
}

enum PollFrame {
    Msg(VmExecMsg),
    WouldBlock,
    Eof,
}

/// Incremental nonblocking frame decoder using per-call `MSG_DONTWAIT` so the
/// shared socket file description stays blocking for `ExecWriter` writes.
#[derive(Default)]
struct FrameReader {
    len_buf: [u8; 4],
    len_read: usize,
    body: Vec<u8>,
    body_read: usize,
}

impl FrameReader {
    fn poll_frame(&mut self, fd: RawFd) -> io::Result<PollFrame> {
        while self.len_read < 4 {
            match recv_nonblock(fd, &mut self.len_buf[self.len_read..])? {
                None => return Ok(PollFrame::WouldBlock),
                Some(0) if self.len_read == 0 => return Ok(PollFrame::Eof),
                Some(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "truncated frame length",
                    ));
                }
                Some(n) => self.len_read += n,
            }
            if self.len_read == 4 {
                let len = u32::from_le_bytes(self.len_buf) as usize;
                if len < 2 {
                    return Err(kat_proto::ProtoError::Truncated.into());
                }
                if len > kat_proto::MAX_FRAME {
                    return Err(kat_proto::ProtoError::FrameTooLarge(len).into());
                }
                self.body = vec![0u8; len];
                self.body_read = 0;
            }
        }
        while self.body_read < self.body.len() {
            match recv_nonblock(fd, &mut self.body[self.body_read..])? {
                None => return Ok(PollFrame::WouldBlock),
                Some(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "truncated frame body",
                    ));
                }
                Some(n) => self.body_read += n,
            }
        }
        let k = u16::from_le_bytes([self.body[0], self.body[1]]);
        let msg = VmExecMsg::decode(k, &self.body[2..])?;
        self.len_read = 0;
        self.body.clear();
        self.body_read = 0;
        Ok(PollFrame::Msg(msg))
    }
}

fn recv_nonblock(fd: RawFd, buf: &mut [u8]) -> io::Result<Option<usize>> {
    loop {
        let rc = unsafe { libc::recv(fd, buf.as_mut_ptr().cast(), buf.len(), libc::MSG_DONTWAIT) };
        if rc >= 0 {
            return Ok(Some(rc as usize));
        }
        let e = io::Error::last_os_error();
        match e.kind() {
            io::ErrorKind::Interrupted => continue,
            io::ErrorKind::WouldBlock => return Ok(None),
            _ => return Err(e),
        }
    }
}

fn flush_stdin(
    stdin: &mut Option<OwnedFd>,
    pending: &mut VecDeque<u8>,
    eof_pending: &mut bool,
    master: &Option<OwnedFd>,
    tty: bool,
    canon: &mut PtyCanonTracker,
) {
    loop {
        while let Some(fd) = stdin.as_ref() {
            let slice = pending.as_slices().0;
            if slice.is_empty() {
                break;
            }
            let mut tio: Option<libc::termios> = None;
            if tty && let Some(m) = master.as_ref() {
                let mut cur: libc::termios = unsafe { std::mem::zeroed() };
                if unsafe { libc::tcgetattr(m.as_raw_fd(), &mut cur) } == 0 {
                    tio = Some(cur);
                }
            }
            let rc = unsafe { libc::write(fd.as_raw_fd(), slice.as_ptr().cast(), slice.len()) };
            if rc > 0 {
                let n = rc as usize;
                if let Some(cur) = &tio {
                    canon.feed(cur, &slice[..n]);
                }
                pending.drain(..n);
            } else if rc == 0 {
                break;
            } else {
                let e = io::Error::last_os_error();
                match e.kind() {
                    io::ErrorKind::Interrupted => continue,
                    io::ErrorKind::WouldBlock => return,
                    _ => {
                        *stdin = None;
                        pending.clear();
                        *eof_pending = false;
                        return;
                    }
                }
            }
        }
        if !pending.is_empty() || !*eof_pending {
            return;
        }
        *eof_pending = false;
        if !tty {
            *stdin = None;
            return;
        }
        let Some(m) = master.as_ref() else {
            return;
        };
        if stdin.is_none() {
            return;
        }
        let mut tio: libc::termios = unsafe { std::mem::zeroed() };
        if unsafe { libc::tcgetattr(m.as_raw_fd(), &mut tio) } < 0 {
            return;
        }
        let (bytes, n) = canon.eof_bytes(&tio);
        if n == 0 {
            return;
        }
        pending.extend(&bytes[..n]);
    }
}

/// Wait up to `timeout` for `cancel_r` to become readable/closed (child exit).
/// Returns `true` if `cancel_r` fired before `timeout`.
fn wait_for_cancel(cancel_r: &OwnedFd, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        let rem = deadline.saturating_duration_since(Instant::now());
        if rem.is_zero() {
            return false;
        }
        let mut pfd = libc::pollfd {
            fd: cancel_r.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        let timeout_ms = rem.as_millis().clamp(1, i32::MAX as u128) as i32;
        let rc = unsafe { libc::poll(&mut pfd, 1, timeout_ms) };
        if rc > 0 {
            return true;
        }
        if rc == 0 {
            return false;
        }
        if io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
            return false;
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn stdin_worker(
    reader: UnixStream,
    writer: Arc<GuestWriter>,
    cancel_r: OwnedFd,
    disconnect_r: OwnedFd,
    ctrl_space_r: OwnedFd,
    stdin: Option<OwnedFd>,
    master: Option<OwnedFd>,
    tty: bool,
    reaper: Reaper,
    handle: Arc<Mutex<Option<ChildHandle>>>,
    cgroup: Option<Arc<SessionCgroup>>,
    host_disconnected: Arc<AtomicBool>,
    disconnect_grace: Duration,
    backpressure_probe: Duration,
) {
    let mut stdin: Option<OwnedFd> = stdin;
    let mut master: Option<OwnedFd> = master;
    let mut frames = FrameReader::default();
    let mut pending: VecDeque<u8> = VecDeque::new();
    let mut eof_pending = false;
    let mut canon = PtyCanonTracker::default();
    let mut disconnected = false;
    let mut post_exit = false;
    let mut last_probe: Option<Instant> = None;
    // Windowed sessions: the host never has more than `INPUT_WINDOW`
    // unacknowledged stdin bytes in flight, so `pending` is bounded by the
    // protocol and the connection is read regardless of `pending` (keeping
    // `OutputWindow` credits flowing even while the command ignores stdin).
    let windowed = writer.windowed();
    let mut stdin_received: u64 = 0;
    let mut stdin_credited: u64 = 0;

    'outer: loop {
        if windowed && !post_exit {
            // Credit back every received byte that has left `pending`
            // (written to the command, or discarded after stdin closed).
            let release = stdin_received
                .saturating_sub(stdin_credited)
                .saturating_sub(pending.len() as u64);
            if release > 0
                && (pending.is_empty() || release >= u64::from(vm_exec::INPUT_WINDOW) / 4)
            {
                stdin_credited += release;
                writer.add_input_credit(release);
            }
        }
        let ctrl_ok = post_exit || writer.ctrl_has_space();
        let can_read = post_exit || (ctrl_ok && (windowed || pending.len() < STDIN_BUF_CAP));
        let poll_timeout_ms = if can_read {
            last_probe = None;
            -1
        } else {
            let now = Instant::now();
            let next_probe = match last_probe {
                Some(t) => t + backpressure_probe,
                None => {
                    last_probe = Some(now);
                    now + backpressure_probe
                }
            };
            let rem = next_probe.saturating_duration_since(now);
            if rem.is_zero() {
                writer.request_probe();
                last_probe = Some(now);
                backpressure_probe.as_millis().clamp(1, i32::MAX as u128) as i32
            } else {
                rem.as_millis().clamp(1, i32::MAX as u128) as i32
            }
        };

        let mut pfds = [
            libc::pollfd {
                fd: cancel_r.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: disconnect_r.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: reader.as_raw_fd(),
                events: if can_read {
                    libc::POLLIN | libc::POLLRDHUP
                } else {
                    libc::POLLRDHUP
                },
                revents: 0,
            },
            libc::pollfd {
                fd: if ctrl_ok {
                    -1
                } else {
                    ctrl_space_r.as_raw_fd()
                },
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: stdin.as_ref().map_or(-1, AsRawFd::as_raw_fd),
                events: libc::POLLOUT,
                revents: 0,
            },
        ];
        let nfds = if stdin.is_some() && !pending.is_empty() {
            5
        } else {
            4
        };
        let rc = unsafe { libc::poll(pfds.as_mut_ptr(), nfds, poll_timeout_ms) };
        if rc < 0 {
            if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                continue;
            }
            break;
        }
        if rc == 0 {
            // Backpressure probe interval fired: ask the nonblocking writer
            // thread to send a 6-byte Pong probe so host close is detected even
            // when the host vsock proxy is in WaitingCreditUpdate.
            writer.request_probe();
            last_probe = Some(Instant::now());
            continue;
        }
        // Two-stage cancel pipe:
        // - A byte (`b'e'`) signals the leader exited: close `stdin`/`master`
        //   immediately and stay in `post_exit` mode reading only
        //   `OutputWindow` credits and peer disconnect until output pumps join.
        // - EOF (`rc == 0`) signals all output pumps have finished: exit cleanly.
        if pfds[0].revents != 0 {
            let mut b = [0u8; 16];
            let rc = unsafe { libc::read(cancel_r.as_raw_fd(), b.as_mut_ptr().cast(), b.len()) };
            if rc == 0 {
                break;
            }
            if rc > 0 {
                post_exit = true;
                stdin = None;
                master = None;
                pending.clear();
                eof_pending = false;
                continue;
            }
            if io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
                break;
            }
        }
        // Writer thread hit EPIPE/POLLERR/POLLHUP (even when both vsock
        // directions were backpressured): immediately trigger disconnect cleanup.
        if pfds[1].revents != 0 {
            disconnected = true;
            break;
        }
        if pfds[3].revents != 0 {
            drain_wake(&ctrl_space_r);
        }
        if nfds == 5 && pfds[4].revents != 0 {
            if (pfds[4].revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL)) != 0
                && (pfds[4].revents & libc::POLLOUT) == 0
            {
                stdin = None;
                pending.clear();
                eof_pending = false;
            } else {
                flush_stdin(
                    &mut stdin,
                    &mut pending,
                    &mut eof_pending,
                    &master,
                    tty,
                    &mut canon,
                );
            }
        }
        if pfds[2].revents != 0 {
            if can_read {
                while post_exit
                    || (writer.ctrl_has_space() && (windowed || pending.len() < STDIN_BUF_CAP))
                {
                    match frames.poll_frame(reader.as_raw_fd()) {
                        Ok(PollFrame::Msg(m)) => match m {
                            VmExecMsg::OutputWindow { stream, bytes } => {
                                writer.add_window(stream, bytes);
                            }
                            _ if post_exit => {}
                            VmExecMsg::Data(d) => {
                                if windowed {
                                    stdin_received += d.len() as u64;
                                    if stdin_received - stdin_credited
                                        > u64::from(vm_exec::INPUT_WINDOW)
                                    {
                                        log("host exceeded the stdin flow-control window");
                                        disconnected = true;
                                        break 'outer;
                                    }
                                }
                                if !eof_pending && stdin.is_some() && !d.is_empty() {
                                    pending.extend(&d);
                                    flush_stdin(
                                        &mut stdin,
                                        &mut pending,
                                        &mut eof_pending,
                                        &master,
                                        tty,
                                        &mut canon,
                                    );
                                }
                            }
                            VmExecMsg::DataEnd => {
                                eof_pending = true;
                                flush_stdin(
                                    &mut stdin,
                                    &mut pending,
                                    &mut eof_pending,
                                    &master,
                                    tty,
                                    &mut canon,
                                );
                            }
                            VmExecMsg::Resize { rows, cols } => {
                                if let Some(m) = &master {
                                    let ws = winsize(rows.max(1), cols.max(1));
                                    unsafe { libc::ioctl(m.as_raw_fd(), libc::TIOCSWINSZ, &ws) };
                                }
                            }
                            VmExecMsg::Signal { signal } if valid_signal(signal) => {
                                signal_handle(&reaper, &handle, signal);
                                writer.send_ctrl(VmExecMsg::SignalAck.encode());
                            }
                            _ => {}
                        },
                        Ok(PollFrame::WouldBlock) => break,
                        Ok(PollFrame::Eof) | Err(_) => {
                            disconnected = true;
                            break 'outer;
                        }
                    }
                }
            } else if (pfds[2].revents & (libc::POLLRDHUP | libc::POLLHUP | libc::POLLERR)) != 0 {
                // Input is backpressured, yet the peer closed the connection.
                disconnected = true;
                break 'outer;
            }
        }
    }

    drop(stdin);
    drop(master);

    if disconnected {
        host_disconnected.store(true, Ordering::Release);
        // Unblock writer_thread and any output pumps immediately before
        // running the disconnect grace period.
        writer.abort();
        // If the leader already exited (`post_exit`), `exec_with_timeouts`
        // performs post-exit cgroup descendant cleanup once pumps join.
        if !post_exit {
            // The host went away: hang up the session or process group, then
            // escalate to SIGKILL after a bounded grace period if the leader or
            // any surviving session descendant remains live.
            signal_handle(&reaper, &handle, libc::SIGHUP);
            if let Some(cg) = &cgroup {
                cg.cleanup_after_disconnect(&cancel_r, disconnect_grace, &reaper, &handle);
            } else if !wait_for_cancel(&cancel_r, disconnect_grace) {
                signal_handle(&reaper, &handle, libc::SIGKILL);
            }
        }
    }
    drop(cgroup);
}

/// First stage of session shutdown once the leader is gone: tell
/// `stdin_worker` (via one byte on its cancel pipe) to drop the command's stdin
/// while it keeps reading `OutputWindow` credits and peer disconnects, and
/// switch `writer` to post-exit mode. Closing the cancel pipe afterwards (once
/// every output pump has joined) is the second stage that stops `stdin_worker`.
fn enter_post_exit(stdin_cancel_w: &OwnedFd, writer: &GuestWriter) {
    let exited_byte = *b"e";
    loop {
        let rc = unsafe { libc::write(stdin_cancel_w.as_raw_fd(), exited_byte.as_ptr().cast(), 1) };
        if rc >= 0 || io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
            break;
        }
    }
    writer.mark_post_exit();
}

/// Relay output from `from` (nonblocking pipe or PTY master) until EOF/EIO.
///
/// Honors optional per-stream byte windows on `writer` so a blocked stdout sink
/// pauses only its own pipe reader without filling the shared connection.
/// Once `wake_r` closes (main child exited):
/// - If all write ends have closed (`POLLHUP`), drains every buffered byte
///   across arbitrary host backpressure without discarding output.
/// - If a surviving background descendant still holds the write end open,
///   first drains the backlog already buffered in `from` at the moment the
///   leader exited (the command's own output, however long the host withholds
///   credit), then bounds further descendant output to `post_exit_idle`,
///   observing `wake_r` between reads even under continuous descendant output.
///   At the deadline, rechecks `POLLHUP` so a writer that closed while a send
///   was backpressured still drains completely.
fn pump(
    from: OwnedFd,
    wake_r: OwnedFd,
    writer: Arc<GuestWriter>,
    stream: OutputStream,
    post_exit_idle: Duration,
    host_disconnected: Arc<AtomicBool>,
) -> io::Result<()> {
    let mut buf = vec![0u8; CHUNK];
    let mut child_exited = false;
    let mut idle_deadline: Option<Instant> = None;
    let mut all_writers_closed = false;
    // Bytes that were already buffered in `from` when the leader exited; never
    // subject to the descendant idle deadline.
    let mut exit_backlog = 0usize;
    let on_leader_exit = |exit_backlog: &mut usize| -> Option<Instant> {
        *exit_backlog = pipe_unread_bytes(from.as_raw_fd()).unwrap_or(0);
        Some(Instant::now() + post_exit_idle)
    };

    loop {
        if host_disconnected.load(Ordering::Acquire) {
            return Ok(());
        }
        if fd_has_hup(from.as_raw_fd()) {
            all_writers_closed = true;
            if pipe_unread_bytes(from.as_raw_fd()) == Some(0) {
                return Ok(());
            }
        }

        let budget_deadline = if !child_exited || all_writers_closed || exit_backlog > 0 {
            None
        } else {
            idle_deadline
        };
        let allowed =
            match writer.wait_for_send_budget(stream, buf.len(), child_exited, budget_deadline) {
                Ok(Some(n)) => n,
                Ok(None) => {
                    if !child_exited {
                        child_exited = true;
                        idle_deadline = on_leader_exit(&mut exit_backlog);
                        continue;
                    }
                    if fd_has_hup(from.as_raw_fd()) {
                        all_writers_closed = true;
                        continue;
                    }
                    return Ok(());
                }
                Err(e) => {
                    if host_disconnected.load(Ordering::Acquire) {
                        return Ok(());
                    }
                    return Err(e);
                }
            };

        let timeout_ms = if !child_exited || all_writers_closed || exit_backlog > 0 {
            -1
        } else {
            match idle_deadline {
                None => -1,
                Some(dl) => {
                    let rem = dl.saturating_duration_since(Instant::now());
                    if rem.is_zero() {
                        if fd_has_hup(from.as_raw_fd()) {
                            all_writers_closed = true;
                            -1
                        } else {
                            return Ok(());
                        }
                    } else {
                        rem.as_millis().clamp(1, i32::MAX as u128) as i32
                    }
                }
            }
        };

        let mut pfds = [
            libc::pollfd {
                fd: from.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: wake_r.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        let nfds = if child_exited { 1 } else { 2 };
        let rc = unsafe { libc::poll(pfds.as_mut_ptr(), nfds, timeout_ms) };
        if rc < 0 {
            let e = io::Error::last_os_error();
            if e.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(e);
        }
        if rc == 0 {
            if fd_has_hup(from.as_raw_fd()) {
                all_writers_closed = true;
                continue;
            }
            return Ok(());
        }
        if !child_exited && pfds[1].revents != 0 {
            child_exited = true;
            idle_deadline = on_leader_exit(&mut exit_backlog);
        }
        if (pfds[0].revents & libc::POLLHUP) != 0 {
            all_writers_closed = true;
        }
        if pfds[0].revents != 0 || child_exited {
            // Exactly one budgeted read per outer iteration: the next pass
            // re-checks per-stream queue space and window credit before reading
            // again, and always re-polls `wake_r`/`idle_deadline`, so continuous
            // descendant output can never starve child-exit detection.
            let want = allowed.min(buf.len());
            let n = loop {
                let n = unsafe { libc::read(from.as_raw_fd(), buf.as_mut_ptr().cast(), want) };
                if n >= 0 {
                    break n as usize;
                }
                let e = io::Error::last_os_error();
                match e.kind() {
                    io::ErrorKind::Interrupted => continue,
                    io::ErrorKind::WouldBlock => break usize::MAX,
                    // On Linux PTY master, EIO signals all slave descriptors closed.
                    _ if e.raw_os_error() == Some(libc::EIO) => return Ok(()),
                    _ => return Err(e),
                }
            };
            if n == 0 {
                return Ok(());
            }
            if n != usize::MAX {
                if exit_backlog > 0 {
                    exit_backlog = exit_backlog.saturating_sub(n);
                    if exit_backlog == 0 {
                        // Descendant-only output from here on.
                        idle_deadline = Some(Instant::now() + post_exit_idle);
                    }
                }
                if host_disconnected.load(Ordering::Acquire) {
                    return Ok(());
                }
                if let Err(e) =
                    writer.send_output(stream, stream.encode_frame(buf[..n].to_vec()), n)
                {
                    if host_disconnected.load(Ordering::Acquire) {
                        return Ok(());
                    }
                    return Err(e);
                }
            }
        }
    }
}

fn log(msg: &str) {
    eprintln!("kat-init: vm-exec: {msg}");
}

/// `kat-init vm-exec-probe` (never as PID 1): from inside the guest, try to
/// open an exec session the way a guest workload could — via vsock loopback,
/// the guest's own CID, and the host CID — and exit 0 only if every attempt is
/// refused or never answers a `Ping`. The real-VM integration test runs this
/// as guest root (strictly more capable than any container) to prove that only
/// the host can reach the listener (see [`peer_is_host`]).
pub fn probe_main() -> ! {
    const IOCTL_VM_SOCKETS_GET_LOCAL_CID: libc::c_ulong = 0x7b9;
    let own_cid = fs::File::open("/dev/vsock").ok().and_then(|f| {
        let mut cid: u32 = 0;
        let rc =
            unsafe { libc::ioctl(f.as_raw_fd(), IOCTL_VM_SOCKETS_GET_LOCAL_CID as _, &mut cid) };
        (rc == 0).then_some(cid)
    });
    let mut targets = vec![
        ("loopback", libc::VMADDR_CID_LOCAL),
        ("host", libc::VMADDR_CID_HOST),
    ];
    if let Some(cid) = own_cid {
        targets.push(("own-cid", cid));
    }
    let mut accepted = false;
    for (name, cid) in targets {
        let outcome = match sys::vsock_connect(cid, VM_EXEC_PORT) {
            Err(e) => format!("refused ({e})"),
            Ok(fd) => {
                let mut s = UnixStream::from(fd);
                let _ = s.set_read_timeout(Some(Duration::from_secs(3)));
                let _ = s.set_write_timeout(Some(Duration::from_secs(3)));
                match vm_exec::write(&mut s, &VmExecMsg::Ping).and_then(|()| vm_exec::read(&mut s))
                {
                    Ok(Some(VmExecMsg::Pong)) => {
                        accepted = true;
                        "ACCEPTED (answered Ping)".to_string()
                    }
                    Ok(other) => format!("rejected (closed: {other:?})"),
                    Err(e) => format!("rejected ({e})"),
                }
            }
        };
        println!("vm-exec-probe {name} cid={cid}: {outcome}");
    }
    std::process::exit(i32::from(accepted));
}

/// Start listening on [`VM_EXEC_PORT`] for host VM shell/exec sessions.
pub fn start(reaper: Reaper) -> io::Result<()> {
    let listener = sys::vsock_listen(VM_EXEC_PORT)?;
    let pending = Arc::new(AtomicUsize::new(0));
    let active_sessions = Arc::new(AtomicUsize::new(0));
    thread::Builder::new()
        .name("vm-exec".into())
        .spawn(move || {
            loop {
                match sys::vsock_accept(&listener) {
                    Ok((fd, peer, peer_len)) => {
                        if !peer_is_host(&peer, peer_len) {
                            log(&format!(
                                "rejecting non-host vsock peer (family={}, cid={}, len={peer_len})",
                                peer.svm_family, peer.svm_cid
                            ));
                            drop(fd);
                            continue;
                        }
                        let Some(pending_guard) =
                            ConnGuard::try_acquire(&pending, MAX_PENDING_HANDSHAKES)
                        else {
                            log(&format!(
                                "handshake limit ({MAX_PENDING_HANDSHAKES}) reached on vsock {VM_EXEC_PORT}"
                            ));
                            continue;
                        };
                        let reaper = reaper.clone();
                        let active_sessions = active_sessions.clone();
                        if let Err(e) =
                            thread::Builder::new()
                                .name("vm-exec-conn".into())
                                .spawn(move || {
                                    serve(
                                        &reaper,
                                        UnixStream::from(fd),
                                        pending_guard,
                                        &active_sessions,
                                    );
                                })
                        {
                            log(&format!("spawn vm-exec-conn: {e}"));
                        }
                    }
                    Err(e) => {
                        log(&format!("accept on vsock {VM_EXEC_PORT}: {e}"));
                        thread::sleep(ACCEPT_ERR_BACKOFF);
                    }
                }
            }
        })?;
    Ok(())
}

fn serve(
    reaper: &Reaper,
    conn: UnixStream,
    pending_guard: ConnGuard,
    active_sessions: &Arc<AtomicUsize>,
) {
    serve_with_timeouts(
        reaper,
        conn,
        Some(pending_guard),
        active_sessions,
        FIRST_FRAME_TIMEOUT,
        DISCONNECT_GRACE,
        POST_EXIT_IDLE,
        BACKPRESSURE_PROBE_INTERVAL,
        CgroupMode::Required(Path::new(CGROUP_PARENT)),
    );
}

#[allow(clippy::too_many_arguments)]
fn serve_with_timeouts(
    reaper: &Reaper,
    mut conn: UnixStream,
    pending_guard: Option<ConnGuard>,
    active_sessions: &Arc<AtomicUsize>,
    first_frame_timeout: Duration,
    disconnect_grace: Duration,
    post_exit_idle: Duration,
    backpressure_probe: Duration,
    cgroup_mode: CgroupMode<'_>,
) {
    let deadline = Instant::now() + first_frame_timeout;
    let spec = {
        let mut ds = DeadlineStream {
            conn: &mut conn,
            deadline,
        };
        let mut saw_ping = false;
        loop {
            match vm_exec::read(&mut ds) {
                Ok(Some(VmExecMsg::Ping)) if !saw_ping => {
                    saw_ping = true;
                    if vm_exec::write(&mut ds, &VmExecMsg::Pong).is_err() {
                        return;
                    }
                }
                Ok(Some(VmExecMsg::SignalSession { token, signal })) => {
                    if !valid_signal(signal) {
                        let _ = vm_exec::write(
                            &mut ds,
                            &VmExecMsg::Error(format!("invalid signal {signal}")),
                        );
                        return;
                    }
                    let handle = SESSION_REGISTRY
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .get(&token)
                        .cloned();
                    match handle {
                        Some(h) => {
                            signal_handle(reaper, &h, signal);
                            let _ = vm_exec::write(&mut ds, &VmExecMsg::SignalAck);
                        }
                        None => {
                            let _ = vm_exec::write(
                                &mut ds,
                                &VmExecMsg::Error("unknown session".into()),
                            );
                        }
                    }
                    return;
                }
                Ok(Some(VmExecMsg::Exec(s))) => break s,
                Ok(None) => return,
                Ok(Some(other)) => {
                    let _ = vm_exec::write(
                        &mut ds,
                        &VmExecMsg::Error(truncate_error(&format!("unexpected request {other:?}"))),
                    );
                    return;
                }
                Err(e) if e.kind() == io::ErrorKind::TimedOut => return,
                Err(e) => {
                    let _ =
                        vm_exec::write(&mut ds, &VmExecMsg::Error(truncate_error(&e.to_string())));
                    return;
                }
            }
        }
    };

    let Some(_session_guard) = ConnGuard::try_acquire(active_sessions, MAX_CONNS) else {
        let mut ds = DeadlineStream {
            conn: &mut conn,
            deadline: Instant::now() + first_frame_timeout,
        };
        let _ = vm_exec::write(
            &mut ds,
            &VmExecMsg::Error(format!(
                "connection limit ({MAX_CONNS}) reached on VM shell service"
            )),
        );
        return;
    };
    // Release the handshake slot now that this connection holds a session slot.
    drop(pending_guard);

    // Ensure no socket timeouts remain on the connection for interactive sessions.
    let _ = conn.set_read_timeout(None);
    let _ = conn.set_write_timeout(None);

    if let Err(e) = exec_with_timeouts(
        reaper,
        conn.try_clone(),
        &spec,
        first_frame_timeout,
        disconnect_grace,
        post_exit_idle,
        backpressure_probe,
        cgroup_mode,
    ) {
        let mut ds = DeadlineStream {
            conn: &mut conn,
            deadline: Instant::now() + first_frame_timeout,
        };
        let _ = vm_exec::write(&mut ds, &VmExecMsg::Error(truncate_error(&e)));
    }
}

/// Validate and resolve the session working directory WITHOUT touching the
/// target filesystem: the child enters it with `chdir` after `fork`, while
/// `spawn_unlocked` watches the host connection, so a stalled mount (e.g. the
/// `/Users` share) can never hold a session slot after the host disconnects.
/// Only the default (`/root` on the local rootfs, else `/`) is checked here.
fn resolve_workdir(cwd: &str) -> Result<CString, String> {
    let path = if cwd.is_empty() {
        if Path::new(DEFAULT_HOME).is_dir() {
            DEFAULT_HOME
        } else {
            "/"
        }
    } else {
        cwd
    };
    CString::new(path).map_err(|_| "workdir contains NUL byte".to_string())
}

struct BuiltCommand {
    program: String,
    args: Vec<String>,
    env: Vec<(String, String)>,
    cwd: CString,
}

fn build_command(spec: &ExecSpec) -> Result<BuiltCommand, String> {
    let cwd = resolve_workdir(&spec.cwd)?;
    for (k, v) in &spec.env {
        if !vm_exec::env_name_ok(k) {
            return Err(format!("invalid environment variable name {k:?}"));
        }
        if !vm_exec::value_ok(v) {
            return Err(format!("invalid environment value for {k:?}"));
        }
    }
    if let Some(t) = &spec.tty
        && !vm_exec::value_ok(&t.term)
    {
        return Err("TERM contains NUL byte".into());
    }

    let (program, args, is_shell): (&str, &[String], bool) = if spec.argv.is_empty() {
        (DEFAULT_SHELL, &[], true)
    } else {
        if spec.argv[0].is_empty() {
            return Err("command cannot be empty".into());
        }
        for a in &spec.argv {
            if !vm_exec::value_ok(a) {
                return Err("command argument contains NUL byte".into());
            }
        }
        (&spec.argv[0], &spec.argv[1..], false)
    };

    let mut env = vec![
        ("PATH".into(), VM_PATH.into()),
        ("HOME".into(), DEFAULT_HOME.into()),
        ("USER".into(), "root".into()),
        ("LOGNAME".into(), "root".into()),
        ("SHELL".into(), DEFAULT_SHELL.into()),
        ("CHARSET".into(), "UTF-8".into()),
    ];
    if is_shell {
        env.push(("PS1".into(), DEFAULT_PS1.into()));
    }
    if let Some(term) = spec
        .tty
        .as_ref()
        .map(|t| t.term.as_str())
        .filter(|t| !t.is_empty())
    {
        env.push(("TERM".into(), guest_term(term, TERMINFO_DIRS).into()));
    }
    for (k, v) in &spec.env {
        env.push((k.clone(), v.clone()));
    }
    Ok(BuiltCommand {
        program: program.to_string(),
        args: args.to_vec(),
        env,
        cwd,
    })
}

const TERMINFO_DIRS: &[&str] = &["/etc/terminfo", "/usr/share/terminfo", "/lib/terminfo"];
const FALLBACK_TERM: &str = "xterm-256color";

/// The host's `TERM` when the guest has a terminfo entry for it (or has no
/// terminfo database at all, so nothing can be checked); otherwise
/// [`FALLBACK_TERM`], so a modern host terminal name (e.g. `xterm-ghostty`)
/// unknown to the minimal rootfs does not break line editing or full-screen
/// tools. Only local rootfs directories are consulted.
fn guest_term<'a>(term: &'a str, dirs: &[&str]) -> &'a str {
    let Some(first) = term.chars().next() else {
        return term;
    };
    if term.contains('/') || term.starts_with('.') {
        return FALLBACK_TERM;
    }
    let mut have_db = false;
    for dir in dirs {
        let dir = Path::new(dir);
        if !dir.is_dir() {
            continue;
        }
        have_db = true;
        let hex = format!("{:x}", u32::from(first));
        if dir.join(first.to_string()).join(term).is_file() || dir.join(hex).join(term).is_file() {
            return term;
        }
    }
    if have_db { FALLBACK_TERM } else { term }
}

/// User-facing spawn failure: workdir errors (reported by the child's `chdir`
/// as `workdir "<path>": ...`) stand alone; everything else names the command.
fn spawn_error(program: &str, e: &io::Error) -> String {
    let msg = e.to_string();
    if msg.starts_with("workdir ") {
        msg
    } else {
        format!("exec {program:?}: {msg}")
    }
}

fn exit_code(status: i32) -> i32 {
    if libc::WIFEXITED(status) {
        libc::WEXITSTATUS(status)
    } else if libc::WIFSIGNALED(status) {
        128 + libc::WTERMSIG(status)
    } else {
        1
    }
}

/// Spawn and relay `spec`. Returns `Err` only before `Started` has been sent
/// (so the caller can reply with a single `Error` frame); once `Started` has
/// been sent, all terminal frames (`Exit` or `Error`) are sent through the
/// nonblocking [`GuestWriter`].
#[allow(clippy::too_many_arguments)]
fn exec_with_timeouts(
    reaper: &Reaper,
    conn: io::Result<UnixStream>,
    spec: &ExecSpec,
    first_frame_timeout: Duration,
    disconnect_grace: Duration,
    post_exit_idle: Duration,
    backpressure_probe: Duration,
    cgroup_mode: CgroupMode<'_>,
) -> Result<(), String> {
    let mut conn = conn.map_err(|e| format!("clone stream: {e}"))?;
    let built = build_command(spec)?;
    let program = built.program.clone();

    let reader = conn.try_clone().map_err(|e| format!("clone stream: {e}"))?;

    // Allocate all control pipes, cgroup descriptors, and PTY/pipe descriptors
    // BEFORE spawning the child so no fallible descriptor allocation can occur
    // after spawn.
    let (stdin_cancel_r, stdin_cancel_w) = cmd::pipe().map_err(|e| format!("pipe: {e}"))?;
    let (disconnect_r, disconnect_w) = pipe_nonblock().map_err(|e| format!("pipe: {e}"))?;
    let (cgroup, procs_fd) = match cgroup_mode {
        CgroupMode::Required(parent) => {
            let (cg, pfd) =
                SessionCgroup::create(parent).map_err(|e| format!("create session cgroup: {e}"))?;
            (Some(Arc::new(cg)), Some(pfd))
        }
        #[cfg(test)]
        CgroupMode::BestEffort(parent) => match SessionCgroup::create(parent) {
            Ok((cg, pfd)) => (Some(Arc::new(cg)), Some(pfd)),
            Err(_) => (None, None),
        },
    };

    let (handle, exited, stdin, master, out_specs) = match &spec.tty {
        Some(t) => {
            let (master, slave) = openpty(t).map_err(|e| format!("openpty: {e}"))?;
            set_nonblocking(&master).map_err(|e| format!("pty nonblocking: {e}"))?;
            let out = master
                .try_clone()
                .map_err(|e| format!("pty master dup: {e}"))?;
            let stdin = master
                .try_clone()
                .map_err(|e| format!("pty master dup: {e}"))?;
            let tty = master
                .try_clone()
                .map_err(|e| format!("pty master dup: {e}"))?;
            let (out_wake_r, out_wake_w) = cmd::pipe().map_err(|e| format!("pipe: {e}"))?;
            let spawn_spec = UnlockedSpawnSpec {
                program: built.program,
                args: built.args,
                env: built.env,
                stdin: slave.try_clone().map_err(|e| format!("pty stdin: {e}"))?,
                stdout: slave.try_clone().map_err(|e| format!("pty stdout: {e}"))?,
                stderr: slave,
                cwd: built.cwd,
                cgroup_procs_fd: procs_fd,
                pty: true,
                cancel_fd: Some(conn.as_raw_fd()),
            };
            let (target, pgrp, exited) = match reaper.spawn_unlocked(spawn_spec) {
                Ok(t) => t,
                Err(e) => {
                    if let Some(cg) = &cgroup {
                        cg.kill();
                    }
                    return Err(spawn_error(&program, &e));
                }
            };
            (
                ChildHandle::Pty { target, pgrp, tty },
                exited,
                Some(stdin),
                Some(master),
                vec![(out, out_wake_r, out_wake_w, OutputStream::Stdout)],
            )
        }
        None => {
            let (in_r, in_w) = cmd::pipe().map_err(|e| format!("pipe: {e}"))?;
            let (out_r, out_w) = cmd::pipe().map_err(|e| format!("pipe: {e}"))?;
            let (err_r, err_w) = cmd::pipe().map_err(|e| format!("pipe: {e}"))?;
            set_nonblocking(&in_w).map_err(|e| format!("stdin nonblocking: {e}"))?;
            set_nonblocking(&out_r).map_err(|e| format!("stdout nonblocking: {e}"))?;
            set_nonblocking(&err_r).map_err(|e| format!("stderr nonblocking: {e}"))?;
            let (out_wake_r, out_wake_w) = cmd::pipe().map_err(|e| format!("pipe: {e}"))?;
            let (err_wake_r, err_wake_w) = cmd::pipe().map_err(|e| format!("pipe: {e}"))?;
            let spawn_spec = UnlockedSpawnSpec {
                program: built.program,
                args: built.args,
                env: built.env,
                stdin: in_r,
                stdout: out_w,
                stderr: err_w,
                cwd: built.cwd,
                cgroup_procs_fd: procs_fd,
                pty: false,
                cancel_fd: Some(conn.as_raw_fd()),
            };
            let (_, pgrp, exited) = match reaper.spawn_unlocked(spawn_spec) {
                Ok(t) => t,
                Err(e) => {
                    if let Some(cg) = &cgroup {
                        cg.kill();
                    }
                    return Err(spawn_error(&program, &e));
                }
            };
            (
                ChildHandle::Pipe(pgrp),
                exited,
                Some(in_w),
                None,
                vec![
                    (out_r, out_wake_r, out_wake_w, OutputStream::Stdout),
                    (err_r, err_wake_r, err_wake_w, OutputStream::Stderr),
                ],
            )
        }
    };

    let handle = Arc::new(Mutex::new(Some(handle)));
    let session_reg = SessionRegGuard::register(handle.clone());
    let host_disconnected = Arc::new(AtomicBool::new(false));

    {
        let mut ds = DeadlineStream {
            conn: &mut conn,
            deadline: Instant::now() + first_frame_timeout,
        };
        if vm_exec::write(
            &mut ds,
            &VmExecMsg::Started {
                token: session_reg.token(),
            },
        )
        .is_err()
        {
            signal_handle(reaper, &handle, libc::SIGKILL);
            if let Some(cg) = &cgroup {
                cg.kill();
            }
            *handle.lock().unwrap() = None;
            let _ = exited.recv_timeout(Duration::from_secs(2));
            return Ok(());
        }
    }

    let output_window = if spec.tty.is_none() {
        spec.output_window
    } else {
        0
    };
    let (writer, ctrl_space_r, writer_thread) = match GuestWriter::start_with_window(
        conn,
        host_disconnected.clone(),
        disconnect_w,
        backpressure_probe,
        output_window,
    ) {
        Ok(triple) => triple,
        Err(e) => {
            log(&format!("start writer thread: {e}"));
            signal_handle(reaper, &handle, libc::SIGKILL);
            if let Some(cg) = &cgroup {
                cg.kill();
            }
            *handle.lock().unwrap() = None;
            let _ = exited.recv_timeout(Duration::from_secs(2));
            return Ok(());
        }
    };

    let tty = spec.tty.is_some();
    let handle_in = handle.clone();
    let reaper_in = reaper.clone();
    let disc_in = host_disconnected.clone();
    let w_in = writer.clone();
    let cg_in = cgroup.clone();
    let stdin_thread = match thread::Builder::new()
        .name("vm-exec-in".into())
        .spawn(move || {
            stdin_worker(
                reader,
                w_in,
                stdin_cancel_r,
                disconnect_r,
                ctrl_space_r,
                stdin,
                master,
                tty,
                reaper_in,
                handle_in,
                cg_in,
                disc_in,
                disconnect_grace,
                backpressure_probe,
            );
        }) {
        Ok(h) => h,
        Err(e) => {
            signal_handle(reaper, &handle, libc::SIGKILL);
            if let Some(cg) = &cgroup {
                cg.kill();
            }
            *handle.lock().unwrap() = None;
            writer.finish_with(Some(
                VmExecMsg::Error(truncate_error(&format!("spawn stdin thread: {e}"))).encode(),
            ));
            let _ = writer_thread.join();
            let _ = exited.recv_timeout(Duration::from_secs(2));
            return Ok(());
        }
    };

    let mut outputs = Vec::with_capacity(out_specs.len());
    let mut out_wakes = Vec::with_capacity(out_specs.len());
    for (out, wake_r, wake_w, stream) in out_specs {
        let w2 = writer.clone();
        let disc = host_disconnected.clone();
        match thread::Builder::new()
            .name("vm-exec-out".into())
            .spawn(move || pump(out, wake_r, w2, stream, post_exit_idle, disc))
        {
            Ok(h) => {
                outputs.push(h);
                out_wakes.push(wake_w);
            }
            Err(e) => {
                signal_handle(reaper, &handle, libc::SIGKILL);
                if let Some(cg) = &cgroup {
                    cg.kill();
                }
                *handle.lock().unwrap() = None;
                // Same two-stage shutdown as a normal exit: keep `stdin_worker`
                // reading `OutputWindow` credits (and host disconnects) until
                // the already-started pumps have drained, THEN stop it.
                enter_post_exit(&stdin_cancel_w, &writer);
                drop(out_wakes);
                drop(wake_w);
                for o in outputs {
                    let _ = o.join();
                }
                drop(stdin_cancel_w);
                let _ = stdin_thread.join();
                if host_disconnected.load(Ordering::Acquire) {
                    writer.abort();
                    let _ = writer_thread.join();
                    return Ok(());
                }
                writer.finish_with(Some(
                    VmExecMsg::Error(truncate_error(&format!("spawn output thread: {e}"))).encode(),
                ));
                let _ = writer_thread.join();
                let _ = exited.recv_timeout(Duration::from_secs(2));
                return Ok(());
            }
        }
    }

    let status = exited.recv().unwrap_or(-1);
    // Clear the handle and transition `stdin_thread` into post-exit mode
    // (dropping `stdin`/`master` immediately while continuing to read
    // `OutputWindow` credits and peer disconnects until all output pumps drain).
    *handle.lock().unwrap() = None;
    enter_post_exit(&stdin_cancel_w, &writer);

    // Signal output pumps that the main child has exited, then join every
    // pump so buffered output is preserved across host backpressure while
    // surviving background descendants cannot leak worker threads or descriptors.
    drop(out_wakes);
    let mut output_err: Option<io::Error> = None;
    for o in outputs {
        match o.join() {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                if output_err.is_none() {
                    output_err = Some(e);
                }
            }
            Err(_) => {
                if output_err.is_none() {
                    output_err = Some(io::Error::other("output pump panicked"));
                }
            }
        }
    }
    drop(stdin_cancel_w);
    let _ = stdin_thread.join();

    let cleanup_post_exit_descendants = |cg: &Option<Arc<SessionCgroup>>| {
        if let Some(cg) = cg
            && cg.is_populated()
        {
            cg.kill();
            let _ = cg.wait_empty(Duration::from_millis(500));
        }
    };

    if host_disconnected.load(Ordering::Acquire) {
        writer.abort();
        let _ = writer_thread.join();
        cleanup_post_exit_descendants(&cgroup);
        return Ok(());
    }
    let final_msg = if let Some(e) = output_err {
        VmExecMsg::Error(truncate_error(&format!("output relay failed: {e}")))
    } else {
        VmExecMsg::Exit {
            code: exit_code(status),
        }
    };
    writer.finish_with(Some(final_msg.encode()));
    let _ = writer_thread.join();
    if host_disconnected.load(Ordering::Acquire) {
        cleanup_post_exit_descendants(&cgroup);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::net::Shutdown;

    use super::*;

    #[test]
    fn builds_default_shell_and_exec_commands() {
        let built = build_command(&ExecSpec {
            cwd: "/".into(),
            env: vec![("FOO".into(), "bar".into())],
            argv: vec![],
            tty: Some(Tty {
                term: "xterm-256color".into(),
                rows: 24,
                cols: 80,
            }),
            output_window: 0,
        })
        .unwrap();
        assert_eq!(built.program, DEFAULT_SHELL);
        assert!(built.args.is_empty());
        assert!(
            built
                .env
                .iter()
                .any(|(k, v)| k == "PATH" && v.contains("k3s/bin"))
        );
        assert!(
            built
                .env
                .iter()
                .any(|(k, v)| k == "PS1" && v == DEFAULT_PS1)
        );
        assert!(
            built
                .env
                .iter()
                .any(|(k, v)| k == "TERM" && v == "xterm-256color")
        );
        assert!(built.env.iter().any(|(k, v)| k == "FOO" && v == "bar"));

        let built = build_command(&ExecSpec {
            cwd: "/".into(),
            env: vec![],
            argv: vec!["sysctl".into(), "-n".into(), "kernel.ostype".into()],
            tty: None,
            output_window: 0,
        })
        .unwrap();
        assert_eq!(built.program, "sysctl");
        assert_eq!(built.args, vec!["-n", "kernel.ostype"]);
        assert!(!built.env.iter().any(|(k, _)| k == "PS1"));
    }

    #[test]
    fn unknown_term_falls_back_when_guest_has_terminfo() {
        let base = std::env::temp_dir().join(format!("kat-terminfo-{}", std::process::id()));
        let _ = fs::remove_dir_all(&base);
        fs::create_dir_all(base.join("x")).unwrap();
        fs::write(base.join("x/xterm-256color"), b"").unwrap();
        let dirs = [base.to_str().unwrap()];
        assert_eq!(guest_term("xterm-256color", &dirs), "xterm-256color");
        assert_eq!(guest_term("xterm-ghostty", &dirs), FALLBACK_TERM);
        assert_eq!(guest_term("../../etc/passwd", &dirs), FALLBACK_TERM);
        // No terminfo database at all: nothing to check, keep the host value.
        assert_eq!(
            guest_term("xterm-ghostty", &["/nonexistent-terminfo"]),
            "xterm-ghostty"
        );
        assert!(valid_signal(64) && valid_signal(1) && !valid_signal(0) && !valid_signal(65));
        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn rejects_invalid_spec_inputs() {
        // The workdir is never touched before spawn (the child enters it while
        // the host connection is monitored); a missing directory is reported
        // by `spawn_unlocked` instead.
        let built = build_command(&ExecSpec {
            cwd: "/no/such/dir/katamaran-test".into(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(built.cwd.as_bytes(), b"/no/such/dir/katamaran-test");
        assert!(
            build_command(&ExecSpec {
                cwd: "/\0bad".into(),
                ..Default::default()
            })
            .is_err()
        );
        assert!(
            build_command(&ExecSpec {
                cwd: "/".into(),
                env: vec![("BAD=NAME".into(), "1".into())],
                ..Default::default()
            })
            .is_err()
        );
        assert!(
            build_command(&ExecSpec {
                cwd: "/".into(),
                env: vec![("OK".into(), "a\0b".into())],
                ..Default::default()
            })
            .is_err()
        );
        assert!(
            build_command(&ExecSpec {
                cwd: "/".into(),
                argv: vec!["".into()],
                ..Default::default()
            })
            .is_err()
        );
        assert!(
            build_command(&ExecSpec {
                cwd: "/".into(),
                argv: vec!["echo".into(), "a\0b".into()],
                ..Default::default()
            })
            .is_err()
        );
        assert!(
            build_command(&ExecSpec {
                cwd: "/".into(),
                tty: Some(Tty {
                    term: "xterm\0".into(),
                    rows: 24,
                    cols: 80,
                }),
                ..Default::default()
            })
            .is_err()
        );
    }

    #[test]
    fn validates_signals_and_exit_codes() {
        assert!(valid_signal(libc::SIGINT));
        assert!(valid_signal(libc::SIGTERM));
        assert!(valid_signal(libc::SIGHUP));
        assert!(!valid_signal(0));
        assert!(!valid_signal(-1));
        assert!(valid_signal(64));
        assert!(!valid_signal(65));
        assert_eq!(exit_code(7 << 8), 7);
        assert_eq!(exit_code(libc::SIGKILL), 128 + libc::SIGKILL);
    }

    #[test]
    fn truncates_errors_on_utf8_boundaries() {
        let short = "boom";
        assert_eq!(truncate_error(short), "boom");
        let long = "é".repeat(MAX_ERROR_BYTES);
        let t = truncate_error(&long);
        assert!(t.len() <= MAX_ERROR_BYTES);
        assert!(t.ends_with("..."));
    }

    #[test]
    fn parses_cgroup_events_populated() {
        assert!(cgroup_events_populated("populated 1\nfrozen 0\n"));
        assert!(!cgroup_events_populated("populated 0\nfrozen 0\n"));
        assert!(!cgroup_events_populated(""));
    }

    #[test]
    fn accepts_only_host_vsock_peer() {
        let len = std::mem::size_of::<libc::sockaddr_vm>() as libc::socklen_t;
        let make = |family: i32, cid: u32| {
            let mut addr: libc::sockaddr_vm = unsafe { std::mem::zeroed() };
            addr.svm_family = family as libc::sa_family_t;
            addr.svm_cid = cid;
            addr
        };
        assert!(peer_is_host(
            &make(libc::AF_VSOCK, libc::VMADDR_CID_HOST),
            len
        ));
        assert!(!peer_is_host(
            &make(libc::AF_VSOCK, libc::VMADDR_CID_LOCAL),
            len
        ));
        assert!(!peer_is_host(&make(libc::AF_VSOCK, 3), len));
        assert!(!peer_is_host(
            &make(libc::AF_VSOCK, libc::VMADDR_CID_HYPERVISOR),
            len
        ));
        assert!(!peer_is_host(
            &make(libc::AF_VSOCK, libc::VMADDR_CID_ANY),
            len
        ));
        assert!(!peer_is_host(
            &make(libc::AF_UNIX, libc::VMADDR_CID_HOST),
            len
        ));
        assert!(!peer_is_host(&make(0, libc::VMADDR_CID_HOST), len));
        assert!(!peer_is_host(
            &make(libc::AF_VSOCK, libc::VMADDR_CID_HOST),
            0
        ));
        assert!(!peer_is_host(
            &make(libc::AF_VSOCK, libc::VMADDR_CID_HOST),
            len - 1
        ));
        assert!(!peer_is_host(
            &make(libc::AF_VSOCK, libc::VMADDR_CID_HOST),
            len + 1
        ));
    }

    #[test]
    fn pty_canon_tracker_respects_input_translation_flags() {
        let base_tio = || {
            let mut tio: libc::termios = unsafe { std::mem::zeroed() };
            tio.c_lflag = libc::ICANON | libc::IEXTEN;
            tio.c_iflag = libc::ICRNL;
            tio.c_cc[libc::VEOF] = 0x04;
            tio.c_cc[libc::VERASE] = 0x7f;
            tio.c_cc[libc::VKILL] = 0x15;
            tio.c_cc[libc::VLNEXT] = 0x16;
            tio.c_cc[libc::VEOL] = 0;
            tio.c_cc[libc::VEOL2] = 0x05;
            tio
        };

        // Empty or newline-terminated input needs a single VEOF.
        let tio = base_tio();
        let mut tr = PtyCanonTracker::default();
        assert_eq!(tr.eof_bytes(&tio), ([0x04, 0, 0], 1));
        tr.feed(&tio, b"hello\n");
        assert_eq!(tr.eof_bytes(&tio), ([0x04, 0, 0], 1));

        // Partial line needs two VEOF bytes.
        tr.feed(&tio, b"part");
        assert_eq!(tr.eof_bytes(&tio), ([0x04, 0x04, 0], 2));

        // ICRNL maps CR -> NL (ends line).
        tr.feed(&tio, b"\r");
        assert_eq!(tr.eof_bytes(&tio), ([0x04, 0, 0], 1));

        // IGNCR discards CR even when ICRNL is also set, preserving partial line.
        let mut igncr_tio = base_tio();
        igncr_tio.c_iflag = libc::IGNCR | libc::ICRNL;
        let mut tr = PtyCanonTracker::default();
        tr.feed(&igncr_tio, b"hello\r");
        assert_eq!(tr.eof_bytes(&igncr_tio), ([0x04, 0x04, 0], 2));

        // INLCR maps NL -> CR (does NOT terminate the line).
        let mut inlcr_tio = base_tio();
        inlcr_tio.c_iflag = libc::INLCR;
        let mut tr = PtyCanonTracker::default();
        tr.feed(&inlcr_tio, b"hello\n");
        assert_eq!(tr.eof_bytes(&inlcr_tio), ([0x04, 0x04, 0], 2));

        // ISTRIP strips bit 7 before checking newline.
        let mut strip_tio = base_tio();
        strip_tio.c_iflag = libc::ISTRIP;
        let mut tr = PtyCanonTracker::default();
        tr.feed(&strip_tio, &[b'a', b'\n' | 0x80]);
        assert_eq!(tr.eof_bytes(&strip_tio), ([0x04, 0, 0], 1));

        // VLNEXT escapes newline so line remains partial.
        let mut tr = PtyCanonTracker::default();
        tr.feed(&tio, &[b'a', 0x16, b'\n']);
        assert_eq!(tr.eof_bytes(&tio), ([0x04, 0x04, 0], 2));

        // VEOL2 terminates only when IEXTEN is enabled.
        let mut tr = PtyCanonTracker::default();
        tr.feed(&tio, &[b'a', 0x05]);
        assert_eq!(tr.eof_bytes(&tio), ([0x04, 0, 0], 1));
        let mut no_exten = base_tio();
        no_exten.c_lflag = libc::ICANON;
        let mut tr = PtyCanonTracker::default();
        tr.feed(&no_exten, &[b'a', 0x05]);
        assert_eq!(tr.eof_bytes(&no_exten), ([0x04, 0x04, 0], 2));

        // VERASE backspacing the only char returns to line boundary.
        let mut tr = PtyCanonTracker::default();
        tr.feed(&tio, &[b'a', 0x7f]);
        assert_eq!(tr.eof_bytes(&tio), ([0x04, 0, 0], 1));

        // Noncanonical / disabled VEOF injects nothing.
        let mut raw = base_tio();
        raw.c_lflag = 0;
        assert_eq!(tr.eof_bytes(&raw), ([0, 0, 0], 0));
        let mut no_eof = base_tio();
        no_eof.c_cc[libc::VEOF] = 0;
        assert_eq!(tr.eof_bytes(&no_eof), ([0, 0, 0], 0));
    }

    fn shrink_pair(a: &UnixStream, b: &UnixStream, bytes: libc::c_int) {
        unsafe {
            for fd in [a.as_raw_fd(), b.as_raw_fd()] {
                let _ = libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_SNDBUF,
                    (&bytes as *const libc::c_int).cast(),
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
                let _ = libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_RCVBUF,
                    (&bytes as *const libc::c_int).cast(),
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
            }
        }
    }

    #[test]
    fn idle_first_frame_connection_releases_slot() {
        let pending = Arc::new(AtomicUsize::new(0));
        let active_sessions = Arc::new(AtomicUsize::new(0));
        let reaper = Reaper::shared_for_tests();
        let (_client, server) = UnixStream::pair().unwrap();
        let guard = ConnGuard::try_acquire(&pending, 1).unwrap();
        assert_eq!(pending.load(Ordering::Acquire), 1);

        let t = Instant::now();
        let active_bg = active_sessions.clone();
        let handle = thread::spawn(move || {
            serve_with_timeouts(
                &reaper,
                server,
                Some(guard),
                &active_bg,
                Duration::from_millis(120),
                Duration::from_millis(100),
                Duration::from_millis(50),
                Duration::from_millis(50),
                CgroupMode::BestEffort(Path::new(CGROUP_PARENT)),
            );
        });
        handle.join().unwrap();
        assert!(t.elapsed() < Duration::from_secs(2));
        assert_eq!(pending.load(Ordering::Acquire), 0);
        assert_eq!(active_sessions.load(Ordering::Acquire), 0);
        assert!(ConnGuard::try_acquire(&pending, 1).is_some());
    }

    #[test]
    fn required_cgroup_mode_fails_loudly_when_cgroup_unavailable() {
        let reaper = Reaper::shared_for_tests();
        let active_sessions = Arc::new(AtomicUsize::new(0));
        let (mut host, guest) = UnixStream::pair().unwrap();
        // `/dev/null/kat-vm-exec` cannot be created as a directory; Required
        // mode must return a guest Error instead of silently falling back to
        // leader-only disconnect cleanup.
        let server = thread::spawn(move || {
            serve_with_timeouts(
                &reaper,
                guest,
                None,
                &active_sessions,
                Duration::from_secs(2),
                Duration::from_millis(100),
                Duration::from_millis(50),
                Duration::from_millis(50),
                CgroupMode::Required(Path::new("/dev/null/kat-vm-exec")),
            );
        });

        vm_exec::write(
            &mut host,
            &VmExecMsg::Exec(ExecSpec {
                cwd: "/".into(),
                env: vec![],
                argv: vec!["/bin/true".into()],
                tty: None,
                output_window: 0,
            }),
        )
        .unwrap();
        match vm_exec::read(&mut host).unwrap() {
            Some(VmExecMsg::Error(e)) => {
                assert!(e.contains("cgroup"), "expected cgroup error, got {e:?}");
            }
            other => panic!("expected cgroup Error, got {other:?}"),
        }
        server.join().unwrap();
    }

    #[test]
    fn signal_frame_emits_signal_ack() {
        let reaper = Reaper::shared_for_tests();
        let active_sessions = Arc::new(AtomicUsize::new(0));
        let (mut host, guest) = UnixStream::pair().unwrap();

        let server = thread::spawn(move || {
            serve_with_timeouts(
                &reaper,
                guest,
                None,
                &active_sessions,
                Duration::from_secs(2),
                Duration::from_millis(150),
                Duration::from_millis(50),
                Duration::from_millis(50),
                CgroupMode::BestEffort(Path::new(CGROUP_PARENT)),
            );
        });

        vm_exec::write(
            &mut host,
            &VmExecMsg::Exec(ExecSpec {
                cwd: "/".into(),
                env: vec![],
                argv: vec![
                    "/bin/sh".into(),
                    "-c".into(),
                    "trap 'exit 77' INT; echo READY; while true; do sleep 1; done".into(),
                ],
                tty: None,
                output_window: 0,
            }),
        )
        .unwrap();
        assert!(matches!(
            vm_exec::read(&mut host).unwrap(),
            Some(VmExecMsg::Started { .. })
        ));
        assert_eq!(
            vm_exec::read(&mut host).unwrap(),
            Some(VmExecMsg::Stdout(b"READY\n".to_vec()))
        );

        vm_exec::write(
            &mut host,
            &VmExecMsg::Signal {
                signal: libc::SIGINT,
            },
        )
        .unwrap();

        let mut saw_ack = false;
        let mut exit_status = None;
        while let Ok(Some(msg)) = vm_exec::read(&mut host) {
            match msg {
                VmExecMsg::SignalAck => saw_ack = true,
                VmExecMsg::Exit { code } => {
                    exit_status = Some(code);
                    break;
                }
                VmExecMsg::Stdout(_) | VmExecMsg::Stderr(_) | VmExecMsg::Pong => {}
                other => panic!("unexpected frame {other:?}"),
            }
        }
        assert!(saw_ack, "expected VmExecMsg::SignalAck after Signal");
        assert_eq!(exit_status, Some(77));
        server.join().unwrap();
    }

    #[test]
    fn pump_preserves_buffered_output_across_slow_reader() {
        let (r, w_pipe) = cmd::pipe().unwrap();
        set_nonblocking(&r).unwrap();
        let (wake_r, wake_w) = cmd::pipe().unwrap();
        let (mut host, guest) = UnixStream::pair().unwrap();
        shrink_pair(&host, &guest, 8192);
        let disc = Arc::new(AtomicBool::new(false));
        let (_disc_r, disc_w) = pipe_nonblock().unwrap();
        let (writer, _ctrl_space_r, writer_thread) =
            GuestWriter::start(guest, disc.clone(), disc_w, Duration::from_millis(50)).unwrap();

        let payload = vec![b'A'; 48 << 10];
        let payload_w = payload.clone();
        // Start pump first and write on a worker thread so this test can never
        // deadlock regardless of kernel pipe capacity.
        let writer_clone = writer.clone();
        let pump_handle = thread::spawn(move || {
            pump(
                r,
                wake_r,
                writer_clone,
                OutputStream::Stdout,
                Duration::from_millis(50),
                disc,
            )
        });
        let producer = thread::spawn(move || {
            let mut wf = std::fs::File::from(w_pipe);
            wf.write_all(&payload_w).unwrap();
            drop(wf);
            drop(wake_w);
        });

        // Pause longer than post_exit_idle before draining so any fixed-window
        // cutoff would truncate buffered bytes.
        thread::sleep(Duration::from_millis(200));

        let drain = thread::spawn(move || {
            let mut received = Vec::new();
            while let Ok(Some(msg)) = vm_exec::read(&mut host) {
                match msg {
                    VmExecMsg::Stdout(d) => received.extend_from_slice(&d),
                    VmExecMsg::Exit { code } => return (received, code),
                    VmExecMsg::Pong => {}
                    other => panic!("unexpected frame {other:?}"),
                }
            }
            panic!("EOF before Exit");
        });

        producer.join().unwrap();
        pump_handle.join().unwrap().unwrap();
        writer.finish_with(Some(VmExecMsg::Exit { code: 0 }.encode()));
        writer_thread.join().unwrap();
        let (received, code) = drain.join().unwrap();
        assert_eq!(code, 0);
        assert_eq!(received.len(), payload.len());
        assert_eq!(received, payload);
    }

    #[test]
    fn pump_drains_buffered_output_when_descendant_closes_during_backpressured_send() {
        let (r, w_pipe) = cmd::pipe().unwrap();
        set_nonblocking(&r).unwrap();
        // Room for all 256 KiB, so the producer deterministically finishes and
        // closes the pipe WHILE the host is stalled (instead of racing the
        // descendant idle deadline on a full 64 KiB pipe).
        assert!(
            unsafe { libc::fcntl(w_pipe.as_raw_fd(), libc::F_SETPIPE_SZ, 1 << 20) } >= 256 << 10
        );
        let (wake_r, wake_w) = cmd::pipe().unwrap();
        let (mut host, guest) = UnixStream::pair().unwrap();
        shrink_pair(&host, &guest, 4096);
        let disc = Arc::new(AtomicBool::new(false));
        let (_disc_r, disc_w) = pipe_nonblock().unwrap();
        let (writer, _ctrl_space_r, writer_thread) =
            GuestWriter::start(guest, disc.clone(), disc_w, Duration::from_millis(50)).unwrap();

        let writer_clone = writer.clone();
        let pump_handle = thread::spawn(move || {
            pump(
                r,
                wake_r,
                writer_clone,
                OutputStream::Stdout,
                Duration::from_millis(80),
                disc,
            )
        });

        // Write enough data across multiple chunks to fill the shrunk socket
        // AND GuestWriter's bounded queue while keeping the pipe writer open at
        // leader exit (`wake_w` dropped), then close the pipe writer while the
        // relay is backpressured past `post_exit_idle`, leaving tail bytes
        // buffered in the pipe.
        let chunk_size = 32 << 10;
        let chunks = 8usize;
        let expected = vec![b'B'; chunk_size * chunks];
        let producer = thread::spawn(move || {
            let mut wf = std::fs::File::from(w_pipe);
            let chunk = vec![b'B'; chunk_size];
            // Write first 4 chunks while pipe remains open, then signal leader exit.
            for _ in 0..4 {
                wf.write_all(&chunk).unwrap();
            }
            drop(wake_w);
            // Write remaining chunks while host is stalled past post_exit_idle,
            // then close the last writer end.
            for _ in 4..chunks {
                wf.write_all(&chunk).unwrap();
            }
            drop(wf);
        });

        // Hold host reading past post_exit_idle so `pump`'s deadline expires
        // while sends are backpressured and the descendant has closed the pipe.
        thread::sleep(Duration::from_millis(220));

        let drain = thread::spawn(move || {
            let mut received = Vec::new();
            while let Ok(Some(msg)) = vm_exec::read(&mut host) {
                match msg {
                    VmExecMsg::Stdout(d) => received.extend_from_slice(&d),
                    VmExecMsg::Exit { code } => return (received, code),
                    VmExecMsg::Pong => {}
                    other => panic!("unexpected frame {other:?}"),
                }
            }
            panic!("EOF before Exit");
        });

        producer.join().unwrap();
        pump_handle.join().unwrap().unwrap();
        writer.finish_with(Some(VmExecMsg::Exit { code: 0 }.encode()));
        writer_thread.join().unwrap();
        let (received, code) = drain.join().unwrap();
        assert_eq!(code, 0);
        assert_eq!(received.len(), expected.len());
        assert_eq!(received, expected);
    }

    #[test]
    fn pump_exits_when_quiet_background_descendant_keeps_pipe_open() {
        let (r, w_pipe) = cmd::pipe().unwrap();
        set_nonblocking(&r).unwrap();
        let (wake_r, wake_w) = cmd::pipe().unwrap();
        let (mut host, guest) = UnixStream::pair().unwrap();
        let disc = Arc::new(AtomicBool::new(false));
        let (_disc_r, disc_w) = pipe_nonblock().unwrap();
        let (writer, _ctrl_space_r, writer_thread) =
            GuestWriter::start(guest, disc.clone(), disc_w, Duration::from_millis(50)).unwrap();

        let mut wf = std::fs::File::from(w_pipe);
        wf.write_all(b"hi\n").unwrap();
        drop(wake_w);

        let t = Instant::now();
        let writer_clone = writer.clone();
        let pump_handle = thread::spawn(move || {
            pump(
                r,
                wake_r,
                writer_clone,
                OutputStream::Stdout,
                Duration::from_millis(80),
                disc,
            )
        });

        assert_eq!(
            vm_exec::read(&mut host).unwrap(),
            Some(VmExecMsg::Stdout(b"hi\n".to_vec()))
        );
        pump_handle.join().unwrap().unwrap();
        assert!(t.elapsed() < Duration::from_secs(1));
        writer.finish_with(None);
        writer_thread.join().unwrap();
        drop(wf);
    }

    #[test]
    fn pump_terminates_when_descendant_writes_continuously_after_leader_exits() {
        let (r, w_pipe) = cmd::pipe().unwrap();
        set_nonblocking(&r).unwrap();
        let (wake_r, wake_w) = cmd::pipe().unwrap();
        let (mut host, guest) = UnixStream::pair().unwrap();
        let disc = Arc::new(AtomicBool::new(false));
        let (_disc_r, disc_w) = pipe_nonblock().unwrap();
        let (writer, _ctrl_space_r, writer_thread) =
            GuestWriter::start(guest, disc.clone(), disc_w, Duration::from_millis(50)).unwrap();
        let stop_spam = Arc::new(AtomicBool::new(false));
        let stop_spam_w = stop_spam.clone();

        // Continuously write so the pipe stays readable without hitting EAGAIN.
        let spammer = thread::spawn(move || {
            let mut wf = std::fs::File::from(w_pipe);
            let chunk = vec![b'Z'; 4096];
            while !stop_spam_w.load(Ordering::Relaxed) {
                if wf.write_all(&chunk).is_err() {
                    break;
                }
            }
        });

        let writer_clone = writer.clone();
        let pump_handle = thread::spawn(move || {
            pump(
                r,
                wake_r,
                writer_clone,
                OutputStream::Stdout,
                Duration::from_millis(80),
                disc,
            )
        });

        let drain = thread::spawn(move || {
            while let Ok(Some(msg)) = vm_exec::read(&mut host) {
                if let VmExecMsg::Exit { code } = msg {
                    return code;
                }
            }
            panic!("EOF before Exit");
        });

        // Let continuous output start, then signal main child exit.
        thread::sleep(Duration::from_millis(30));
        let t = Instant::now();
        drop(wake_w);

        pump_handle.join().unwrap().unwrap();
        assert!(
            t.elapsed() < Duration::from_secs(1),
            "pump did not observe wake_r under continuous output ({:?})",
            t.elapsed()
        );
        writer.finish_with(Some(VmExecMsg::Exit { code: 42 }.encode()));
        writer_thread.join().unwrap();
        assert_eq!(drain.join().unwrap(), 42);

        stop_spam.store(true, Ordering::Relaxed);
        let _ = spammer.join();
    }

    #[test]
    fn disconnect_under_stdin_backpressure_detected_even_without_rdhup() {
        let reaper = Reaper::shared_for_tests();
        let active_sessions = Arc::new(AtomicUsize::new(0));
        let (mut host, guest) = UnixStream::pair().unwrap();

        let server = thread::spawn(move || {
            serve_with_timeouts(
                &reaper,
                guest,
                None,
                &active_sessions,
                Duration::from_secs(2),
                Duration::from_millis(150),
                Duration::from_millis(50),
                Duration::from_millis(50),
                CgroupMode::BestEffort(Path::new(CGROUP_PARENT)),
            );
        });

        vm_exec::write(&mut host, &VmExecMsg::Ping).unwrap();
        assert_eq!(vm_exec::read(&mut host).unwrap(), Some(VmExecMsg::Pong));
        vm_exec::write(
            &mut host,
            &VmExecMsg::Exec(ExecSpec {
                cwd: "/".into(),
                env: vec![],
                argv: vec![
                    "/bin/sh".into(),
                    "-c".into(),
                    "trap '' HUP; sleep 60".into(),
                ],
                tty: None,
                output_window: 0,
            }),
        )
        .unwrap();
        assert!(matches!(
            vm_exec::read(&mut host).unwrap(),
            Some(VmExecMsg::Started { .. })
        ));

        // Fill guest stdin pipe + user-space buffer.
        let chunk = vec![b'x'; CHUNK];
        for _ in 0..6 {
            if vm_exec::write(&mut host, &VmExecMsg::Data(chunk.clone())).is_err() {
                break;
            }
        }
        // Shut down ONLY the host read half so the guest does NOT get POLLRDHUP
        // on `reader` (simulating libkrun's WaitingCreditUpdate when the vsock
        // receive window is full); the guest must still detect disconnect via
        // its backpressure keepalive probe and terminate the child.
        let t = Instant::now();
        host.shutdown(Shutdown::Read).unwrap();
        server.join().unwrap();
        assert!(
            t.elapsed() < Duration::from_secs(3),
            "disconnect cleanup under full rx window took {:?}",
            t.elapsed()
        );
        drop(host);
    }

    #[test]
    fn disconnect_detected_when_both_directions_are_backpressured() {
        let reaper = Reaper::shared_for_tests();
        let active_sessions = Arc::new(AtomicUsize::new(0));
        let (mut host, guest) = UnixStream::pair().unwrap();
        shrink_pair(&host, &guest, 8192);

        let server = thread::spawn(move || {
            serve_with_timeouts(
                &reaper,
                guest,
                None,
                &active_sessions,
                Duration::from_secs(2),
                Duration::from_millis(150),
                Duration::from_millis(50),
                Duration::from_millis(50),
                CgroupMode::BestEffort(Path::new(CGROUP_PARENT)),
            );
        });

        vm_exec::write(
            &mut host,
            &VmExecMsg::Exec(ExecSpec {
                cwd: "/".into(),
                env: vec![],
                argv: vec![
                    "/bin/sh".into(),
                    "-c".into(),
                    "trap '' HUP; while true; do printf '0123456789abcdef%.0s' 1 2 3 4 5 6 7 8 9 0; done".into(),
                ],
                tty: None,
                output_window: 0,
            }),
        )
        .unwrap();
        assert!(matches!(
            vm_exec::read(&mut host).unwrap(),
            Some(VmExecMsg::Started { .. })
        ));

        // Stop reading stdout (backpressuring guest->host) AND flood stdin
        // (backpressuring host->guest) so both directions stall simultaneously.
        let chunk = vec![b'x'; CHUNK];
        for _ in 0..6 {
            if vm_exec::write(&mut host, &VmExecMsg::Data(chunk.clone())).is_err() {
                break;
            }
        }
        thread::sleep(Duration::from_millis(80));

        // Shut down ONLY host read (no POLLRDHUP on guest reader); the
        // nonblocking guest writer must detect EPIPE immediately, wake
        // stdin_worker, unblock output pumps, and terminate the child.
        let t = Instant::now();
        host.shutdown(Shutdown::Read).unwrap();
        server.join().unwrap();
        assert!(
            t.elapsed() < Duration::from_secs(3),
            "disconnect cleanup under bidirectional backpressure took {:?}",
            t.elapsed()
        );
        drop(host);
    }

    #[test]
    fn signal_session_oob_delivers_signal_even_under_bidirectional_backpressure() {
        let reaper = Reaper::shared_for_tests();
        let active_sessions = Arc::new(AtomicUsize::new(0));
        let (mut host, guest) = UnixStream::pair().unwrap();
        shrink_pair(&host, &guest, 8192);

        let r1 = reaper.clone();
        let active_bg = active_sessions.clone();
        let server = thread::spawn(move || {
            serve_with_timeouts(
                &r1,
                guest,
                None,
                &active_bg,
                Duration::from_secs(2),
                Duration::from_millis(150),
                Duration::from_millis(50),
                Duration::from_millis(50),
                CgroupMode::BestEffort(Path::new(CGROUP_PARENT)),
            );
        });

        vm_exec::write(
            &mut host,
            &VmExecMsg::Exec(ExecSpec {
                cwd: "/".into(),
                env: vec![],
                argv: vec![
                    "/bin/sh".into(),
                    "-c".into(),
                    "trap 'exit 77' INT; while true; do printf '0123456789abcdef%.0s' 1 2 3 4 5 6 7 8 9 0; done".into(),
                ],
                tty: None,
                output_window: 0,
            }),
        )
        .unwrap();
        let token = match vm_exec::read(&mut host).unwrap() {
            Some(VmExecMsg::Started { token }) => token,
            other => panic!("expected Started, got {other:?}"),
        };

        // Stall BOTH directions (do not read stdout, flood stdin).
        let chunk = vec![b'x'; CHUNK];
        for _ in 0..6 {
            if vm_exec::write(&mut host, &VmExecMsg::Data(chunk.clone())).is_err() {
                break;
            }
        }
        thread::sleep(Duration::from_millis(60));

        // Deliver SIGINT out-of-band even when active_sessions is at MAX_CONNS.
        let full_sessions = Arc::new(AtomicUsize::new(MAX_CONNS));
        let (mut ctrl_host, ctrl_guest) = UnixStream::pair().unwrap();
        let r2 = reaper.clone();
        let ctrl_srv = thread::spawn(move || {
            serve_with_timeouts(
                &r2,
                ctrl_guest,
                None,
                &full_sessions,
                Duration::from_secs(2),
                Duration::from_millis(150),
                Duration::from_millis(50),
                Duration::from_millis(50),
                CgroupMode::BestEffort(Path::new(CGROUP_PARENT)),
            );
        });
        vm_exec::write(
            &mut ctrl_host,
            &VmExecMsg::SignalSession {
                token,
                signal: libc::SIGINT,
            },
        )
        .unwrap();
        assert_eq!(
            vm_exec::read(&mut ctrl_host).unwrap(),
            Some(VmExecMsg::SignalAck)
        );
        ctrl_srv.join().unwrap();

        // Drain stdout until the trapped exit code 77 arrives.
        let mut exit_code_seen = None;
        while let Ok(Some(msg)) = vm_exec::read(&mut host) {
            if let VmExecMsg::Exit { code } = msg {
                exit_code_seen = Some(code);
                break;
            }
        }
        assert_eq!(exit_code_seen, Some(77));
        server.join().unwrap();
    }

    #[test]
    fn post_exit_writer_detects_disconnect_during_drain() {
        let (host, guest) = UnixStream::pair().unwrap();
        let disc = Arc::new(AtomicBool::new(false));
        let (_disc_r, disc_w) = pipe_nonblock().unwrap();
        let (writer, _ctrl_space_r, writer_thread) =
            GuestWriter::start(guest, disc.clone(), disc_w, Duration::from_millis(40)).unwrap();

        // Simulate leader exit handing disconnect monitoring to the writer
        // while output pumps are still open (before finish_with).
        writer.mark_post_exit();
        let t = Instant::now();
        host.shutdown(Shutdown::Read).unwrap();
        writer_thread.join().unwrap();
        assert!(disc.load(Ordering::Acquire));
        assert!(t.elapsed() < Duration::from_secs(2));
        drop(host);
    }

    #[test]
    fn ctrl_queue_is_bounded_when_peer_stops_reading() {
        let (_host, guest) = UnixStream::pair().unwrap();
        let disc = Arc::new(AtomicBool::new(false));
        let (_disc_r, disc_w) = pipe_nonblock().unwrap();
        let (writer, _ctrl_space_r, writer_thread) =
            GuestWriter::start(guest, disc, disc_w, Duration::from_millis(50)).unwrap();

        // Enqueue far more SignalAck frames than GUEST_CTRL_QUEUE_CAP while
        // the peer never reads; the control queue must stay bounded.
        for _ in 0..(GUEST_CTRL_QUEUE_CAP * 8) {
            writer.send_ctrl(VmExecMsg::SignalAck.encode());
        }
        let queued = writer.state.lock().unwrap().ctrl.len();
        assert!(
            queued <= GUEST_CTRL_QUEUE_CAP,
            "ctrl queue grew beyond cap ({queued} > {GUEST_CTRL_QUEUE_CAP})"
        );

        writer.abort();
        writer_thread.join().unwrap();
    }

    #[test]
    fn openpty_sets_cloexec_atomically() {
        let Ok((master, slave)) = openpty(&Tty {
            term: "xterm".into(),
            rows: 24,
            cols: 80,
        }) else {
            return;
        };
        let mf = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFD) };
        let sf = unsafe { libc::fcntl(slave.as_raw_fd(), libc::F_GETFD) };
        assert!(mf >= 0 && (mf & libc::FD_CLOEXEC) != 0);
        assert!(sf >= 0 && (sf & libc::FD_CLOEXEC) != 0);
    }

    #[test]
    fn windowed_session_keeps_reading_credits_while_command_ignores_stdin() {
        let reaper = Reaper::shared_for_tests();
        let active_sessions = Arc::new(AtomicUsize::new(0));
        let (mut host, guest) = UnixStream::pair().unwrap();

        let server = thread::spawn(move || {
            serve_with_timeouts(
                &reaper,
                guest,
                None,
                &active_sessions,
                Duration::from_secs(2),
                Duration::from_millis(150),
                Duration::from_millis(50),
                Duration::from_millis(50),
                CgroupMode::BestEffort(Path::new(CGROUP_PARENT)),
            );
        });

        // The command writes 16x its stdout window BEFORE reading any stdin,
        // while the host has already sent its full stdin window. The guest
        // must keep reading the connection (stdin is bounded by
        // `INPUT_WINDOW`), so `OutputWindow` credits are never stuck behind
        // unread `Data`, then credit stdin back once it is consumed.
        let window = 32u32 << 10;
        let input = vm_exec::INPUT_WINDOW as usize;
        let script =
            format!("dd if=/dev/zero bs=4096 count=128 2>/dev/null; head -c {input} >/dev/null");
        vm_exec::write(
            &mut host,
            &VmExecMsg::Exec(ExecSpec {
                cwd: "/".into(),
                env: vec![],
                argv: vec!["/bin/sh".into(), "-c".into(), script],
                tty: None,
                output_window: window,
            }),
        )
        .unwrap();
        assert!(matches!(
            vm_exec::read(&mut host).unwrap(),
            Some(VmExecMsg::Started { .. })
        ));
        let mut sent = 0usize;
        while sent < input {
            let n = CHUNK.min(input - sent);
            vm_exec::write(&mut host, &VmExecMsg::Data(vec![b'i'; n])).unwrap();
            sent += n;
        }

        host.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut stdout_bytes = 0usize;
        let mut outstanding = 0usize;
        let mut input_credit = 0usize;
        let code = loop {
            match vm_exec::read(&mut host).unwrap() {
                Some(VmExecMsg::Stdout(d)) => {
                    outstanding += d.len();
                    assert!(
                        outstanding <= window as usize,
                        "guest exceeded stdout window"
                    );
                    stdout_bytes += d.len();
                    outstanding -= d.len();
                    vm_exec::write(
                        &mut host,
                        &VmExecMsg::OutputWindow {
                            stream: vm_exec::OUTPUT_STREAM_STDOUT,
                            bytes: d.len() as u32,
                        },
                    )
                    .unwrap();
                }
                Some(VmExecMsg::InputWindow { bytes }) => input_credit += bytes as usize,
                Some(VmExecMsg::Pong) => {}
                Some(VmExecMsg::Exit { code }) => break code,
                other => panic!("unexpected frame: {other:?}"),
            }
        };
        assert_eq!(code, 0);
        assert_eq!(stdout_bytes, 128 * 4096);
        assert_eq!(input_credit, input);
        server.join().unwrap();
    }

    #[test]
    fn leader_exit_backlog_survives_withheld_credit_while_descendant_holds_pipe() {
        let reaper = Reaper::shared_for_tests();
        let active_sessions = Arc::new(AtomicUsize::new(0));
        let (mut host, guest) = UnixStream::pair().unwrap();
        let post_exit_idle = Duration::from_millis(50);

        let server = thread::spawn(move || {
            serve_with_timeouts(
                &reaper,
                guest,
                None,
                &active_sessions,
                Duration::from_secs(2),
                Duration::from_millis(150),
                post_exit_idle,
                Duration::from_millis(50),
                CgroupMode::BestEffort(Path::new(CGROUP_PARENT)),
            );
        });

        // The leader writes 80 KiB and exits while a quiet background
        // descendant keeps stdout open (no POLLHUP). With a 32 KiB window, the
        // remaining ~48 KiB is still in the pipe at leader exit; withholding
        // credit far past `post_exit_idle` must not discard it.
        let window = 32u32 << 10;
        let total = 20 * 4096;
        vm_exec::write(
            &mut host,
            &VmExecMsg::Exec(ExecSpec {
                cwd: "/".into(),
                env: vec![],
                argv: vec![
                    "/bin/sh".into(),
                    "-c".into(),
                    "dd if=/dev/zero bs=4096 count=20 2>/dev/null; sleep 5 & exit 0".into(),
                ],
                tty: None,
                output_window: window,
            }),
        )
        .unwrap();
        assert!(matches!(
            vm_exec::read(&mut host).unwrap(),
            Some(VmExecMsg::Started { .. })
        ));

        host.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut stdout_bytes = 0usize;
        while stdout_bytes < window as usize {
            match vm_exec::read(&mut host).unwrap() {
                Some(VmExecMsg::Stdout(d)) => stdout_bytes += d.len(),
                Some(VmExecMsg::Pong) => {}
                other => panic!("unexpected frame before window exhausted: {other:?}"),
            }
        }
        thread::sleep(post_exit_idle * 10);

        let code = loop {
            vm_exec::write(
                &mut host,
                &VmExecMsg::OutputWindow {
                    stream: vm_exec::OUTPUT_STREAM_STDOUT,
                    bytes: window,
                },
            )
            .unwrap();
            match vm_exec::read(&mut host).unwrap() {
                Some(VmExecMsg::Stdout(d)) => stdout_bytes += d.len(),
                Some(VmExecMsg::Pong) => {}
                Some(VmExecMsg::Exit { code }) => break code,
                other => panic!("unexpected frame: {other:?}"),
            }
        };
        assert_eq!(code, 0);
        assert_eq!(stdout_bytes, total, "leader output lost at exit");
        server.join().unwrap();
    }

    #[test]
    fn pty_signal_reaches_foreground_job_not_just_session_leader() {
        let reaper = Reaper::shared_for_tests();
        let active_sessions = Arc::new(AtomicUsize::new(0));
        let (mut host, guest) = UnixStream::pair().unwrap();
        let server = thread::spawn(move || {
            serve_with_timeouts(
                &reaper,
                guest,
                None,
                &active_sessions,
                Duration::from_secs(2),
                Duration::from_millis(150),
                Duration::from_millis(50),
                Duration::from_millis(50),
                CgroupMode::BestEffort(Path::new(CGROUP_PARENT)),
            );
        });
        // The session leader traps INT; only signalling the foreground job
        // (`sleep`) like a real terminal ends the 30 s sleep promptly.
        vm_exec::write(
            &mut host,
            &VmExecMsg::Exec(ExecSpec {
                cwd: "/".into(),
                env: vec![],
                argv: vec![
                    "/bin/sh".into(),
                    "-c".into(),
                    "trap 'echo TRAPPED' INT; echo READY; sleep 30; echo DONE".into(),
                ],
                tty: Some(Tty {
                    term: "xterm".into(),
                    rows: 24,
                    cols: 80,
                }),
                output_window: 0,
            }),
        )
        .unwrap();
        assert!(matches!(
            vm_exec::read(&mut host).unwrap(),
            Some(VmExecMsg::Started { .. })
        ));
        host.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut out = Vec::new();
        while !out.windows(5).any(|w| w == b"READY") {
            match vm_exec::read(&mut host).unwrap() {
                Some(VmExecMsg::Stdout(d)) => out.extend_from_slice(&d),
                Some(VmExecMsg::Pong) => {}
                other => panic!("unexpected frame: {other:?}"),
            }
        }
        thread::sleep(Duration::from_millis(200));
        let t = Instant::now();
        vm_exec::write(
            &mut host,
            &VmExecMsg::Signal {
                signal: libc::SIGINT,
            },
        )
        .unwrap();
        let code = loop {
            match vm_exec::read(&mut host).unwrap() {
                Some(VmExecMsg::Stdout(d)) => out.extend_from_slice(&d),
                Some(VmExecMsg::Exit { code }) => break code,
                Some(VmExecMsg::SignalAck | VmExecMsg::Pong) => {}
                other => panic!("unexpected frame: {other:?}"),
            }
        };
        assert!(
            t.elapsed() < Duration::from_secs(5),
            "SIGINT never reached sleep"
        );
        let text = String::from_utf8_lossy(&out);
        assert!(text.contains("TRAPPED") && text.contains("DONE"), "{text}");
        assert_eq!(code, 0);
        server.join().unwrap();
    }

    #[test]
    fn output_window_keeps_stderr_moving_when_stdout_window_is_exhausted() {
        let reaper = Reaper::shared_for_tests();
        let active_sessions = Arc::new(AtomicUsize::new(0));
        let (mut host, guest) = UnixStream::pair().unwrap();

        let server = thread::spawn(move || {
            serve_with_timeouts(
                &reaper,
                guest,
                None,
                &active_sessions,
                Duration::from_secs(2),
                Duration::from_millis(150),
                Duration::from_millis(50),
                Duration::from_millis(50),
                CgroupMode::BestEffort(Path::new(CGROUP_PARENT)),
            );
        });

        let window = 32u32 << 10;
        // Write 256 KiB to stdout in the background (8x the 32 KiB window!)
        // and emit a stderr readiness marker independently. Even while the
        // host never replenishes stdout credits, stdout stops at 32 KiB and
        // the stderr marker MUST arrive promptly, followed by the rest of
        // stdout once stdout credits are replenished.
        let script =
            "dd if=/dev/zero bs=4096 count=64 2>/dev/null & printf 'STDERR_READY\\n' >&2; wait";
        vm_exec::write(
            &mut host,
            &VmExecMsg::Exec(ExecSpec {
                cwd: "/".into(),
                env: vec![],
                argv: vec!["/bin/sh".into(), "-c".into(), script.into()],
                tty: None,
                output_window: window,
            }),
        )
        .unwrap();
        assert!(matches!(
            vm_exec::read(&mut host).unwrap(),
            Some(VmExecMsg::Started { .. })
        ));

        host.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let mut stdout_bytes = 0usize;
        let mut stderr_buf = Vec::new();
        while !stderr_buf.windows(12).any(|w| w == b"STDERR_READY") {
            match vm_exec::read(&mut host).unwrap() {
                Some(VmExecMsg::Stdout(d)) => {
                    stdout_bytes += d.len();
                    assert!(
                        stdout_bytes <= window as usize,
                        "guest exceeded stdout window ({stdout_bytes} > {window})"
                    );
                }
                Some(VmExecMsg::Stderr(d)) => stderr_buf.extend_from_slice(&d),
                Some(VmExecMsg::Pong) => {}
                other => panic!("unexpected frame before stderr marker: {other:?}"),
            }
        }
        assert!(stdout_bytes <= window as usize);

        // Now replenish stdout window; all 256 KiB of stdout + Exit(0) arrive.
        vm_exec::write(
            &mut host,
            &VmExecMsg::OutputWindow {
                stream: vm_exec::OUTPUT_STREAM_STDOUT,
                bytes: 512 << 10,
            },
        )
        .unwrap();
        let mut exit_code_seen = None;
        while let Some(msg) = vm_exec::read(&mut host).unwrap() {
            match msg {
                VmExecMsg::Stdout(d) => stdout_bytes += d.len(),
                VmExecMsg::Stderr(d) => stderr_buf.extend_from_slice(&d),
                VmExecMsg::Exit { code } => {
                    exit_code_seen = Some(code);
                    break;
                }
                VmExecMsg::Pong => {}
                other => panic!("unexpected frame during drain: {other:?}"),
            }
        }
        assert_eq!(exit_code_seen, Some(0));
        assert_eq!(stdout_bytes, 64 * 4096);
        server.join().unwrap();
    }
}
