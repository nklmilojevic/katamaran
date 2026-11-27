//! VM root shell and exec service, served to the host on
//! [`kat_proto::VM_EXEC_PORT`] (see [`kat_proto::vm_exec`]).
//!
//! Spawns root shells and arbitrary commands directly in PID 1's own
//! namespaces so developers can inspect containerd, dockerd, k3s, sysctls,
//! rlimits, and guest logs without needing Kubernetes or an LXC machine.
//! Idle overhead is a single vsock listener thread.

use std::collections::{HashSet, VecDeque};
use std::ffi::CString;
use std::fs;
use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, LazyLock, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use kat_proto::VM_EXEC_PORT;
use kat_proto::vm_exec::{self, CHUNK, ExecSpec, Tty, VmExecMsg};

use super::machines::cmd;
use super::service::{ChildTarget, PgrpTarget, Reaper};
use super::sys;

const DEFAULT_SHELL: &str = "/bin/sh";
const DEFAULT_HOME: &str = "/root";
const VM_PATH: &str =
    "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/var/lib/katamaran/k3s/bin";
const DEFAULT_PS1: &str = r"\h:\w\$ ";
const MAX_CONNS: usize = 64;
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
/// Maximum chunks drained per outer poll cycle when writers remain open so
/// `wake_r` (child exit) is always observed promptly during continuous output.
const PUMP_DRAIN_BATCH: usize = 4;
/// Maximum buffered stdin bytes held in user space while guest stdin is
/// backpressured, keeping memory bounded while still monitoring disconnect.
const STDIN_BUF_CAP: usize = 128 << 10;
/// Maximum queued output frames held between output pumps and the nonblocking
/// guest socket writer so backpressure stays bounded without holding a socket
/// mutex across blocking writes.
const GUEST_OUT_QUEUE_CAP: usize = 2;
const CGROUP_PARENT: &str = "/sys/fs/cgroup/kat-vm-exec";

static NEXT_CGROUP_ID: AtomicUsize = AtomicUsize::new(1);
static LIVE_CGROUPS: LazyLock<Mutex<HashSet<usize>>> = LazyLock::new(|| Mutex::new(HashSet::new()));

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

#[derive(Default)]
struct GuestWriterState {
    ctrl: VecDeque<Vec<u8>>,
    queue: VecDeque<Vec<u8>>,
    probe_pending: bool,
    finish: bool,
    closed: bool,
}

/// Nonblocking single-owner writer for all guest-to-host frames (`Stdout`,
/// `Stderr`, `SignalAck`, `Pong` backpressure probes, and terminal `Exit`/`Error`).
///
/// Decoupling socket writes from `pump` threads eliminates blocking I/O under a
/// shared mutex, lets `SignalAck` and `Pong` probes preempt queued output without
/// blocking `stdin_worker`, and detects host disconnect immediately on `POLLOUT` /
/// `EPIPE` even when both vsock directions are simultaneously backpressured.
struct GuestWriter {
    state: Mutex<GuestWriterState>,
    cv: Condvar,
    wake_w: OwnedFd,
}

impl GuestWriter {
    fn start(
        stream: UnixStream,
        host_disconnected: Arc<AtomicBool>,
        disconnect_w: OwnedFd,
    ) -> io::Result<(Arc<Self>, thread::JoinHandle<()>)> {
        stream.set_nonblocking(true)?;
        let (wake_r, wake_w) = pipe_nonblock()?;
        let writer = Arc::new(Self {
            state: Mutex::new(GuestWriterState::default()),
            cv: Condvar::new(),
            wake_w,
        });
        let writer_bg = writer.clone();
        let handle = thread::Builder::new()
            .name("vm-exec-tx".into())
            .spawn(move || {
                guest_writer_worker(stream, writer_bg, wake_r, host_disconnected, disconnect_w);
            })?;
        Ok((writer, handle))
    }

    fn send_output(&self, frame: Vec<u8>) -> io::Result<()> {
        let mut st = self.state.lock().unwrap();
        while !st.closed && st.queue.len() >= GUEST_OUT_QUEUE_CAP {
            st = self.cv.wait(st).unwrap();
        }
        if st.closed {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "exec closed"));
        }
        st.queue.push_back(frame);
        drop(st);
        notify_wake(&self.wake_w);
        Ok(())
    }

    fn send_ctrl(&self, frame: Vec<u8>) {
        let mut st = self.state.lock().unwrap();
        if st.closed {
            return;
        }
        st.ctrl.push_back(frame);
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

    fn finish_with(&self, final_frame: Option<Vec<u8>>) {
        let mut st = self.state.lock().unwrap();
        if st.closed {
            return;
        }
        if let Some(frame) = final_frame {
            st.queue.push_back(frame);
        }
        st.finish = true;
        drop(st);
        notify_wake(&self.wake_w);
    }

    fn abort(&self) {
        let mut st = self.state.lock().unwrap();
        st.closed = true;
        st.ctrl.clear();
        st.queue.clear();
        self.cv.notify_all();
        drop(st);
        notify_wake(&self.wake_w);
    }
}

fn guest_writer_worker(
    stream: UnixStream,
    writer: Arc<GuestWriter>,
    wake_r: OwnedFd,
    host_disconnected: Arc<AtomicBool>,
    disconnect_w: OwnedFd,
) {
    let mut out_buf: Vec<u8> = Vec::new();
    let mut out_off: usize = 0;
    let mut failed = false;

    loop {
        if out_off == out_buf.len() {
            out_buf.clear();
            out_off = 0;
            let mut st = writer.state.lock().unwrap();
            if st.closed {
                return;
            }
            if let Some(frame) = st.ctrl.pop_front() {
                out_buf = frame;
            } else if let Some(frame) = st.queue.pop_front() {
                out_buf = frame;
                writer.cv.notify_all();
            } else if st.probe_pending {
                st.probe_pending = false;
                out_buf = VmExecMsg::Pong.encode();
            } else if st.finish {
                return;
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
        let mut pfds = [
            libc::pollfd {
                fd: wake_r.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: stream.as_raw_fd(),
                events: if need_write { libc::POLLOUT } else { 0 },
                revents: 0,
            },
        ];
        let rc = unsafe { libc::poll(pfds.as_mut_ptr(), 2, -1) };
        if rc < 0 {
            if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                continue;
            }
            failed = true;
            break;
        }
        if pfds[0].revents != 0 {
            drain_wake(&wake_r);
            if writer.state.lock().unwrap().closed {
                return;
            }
        }
        if (pfds[1].revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL)) != 0
            && (pfds[1].revents & libc::POLLOUT) == 0
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
        st.queue.clear();
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
    },
    /// Non-PTY process group leader spawned with `setpgid(0, 0)`, guarded by
    /// a token-pinned [`PgrpTarget`] inside [`Reaper`].
    Pipe(PgrpTarget),
}

impl ChildHandle {
    fn signal(&self, reaper: &Reaper, sig: i32) {
        match self {
            ChildHandle::Pty { target, pgrp } => {
                reaper.signal_target(target, sig);
                if matches!(sig, libc::SIGHUP | libc::SIGKILL) {
                    let _ = reaper.signal_pgrp_target(*pgrp, sig);
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

fn valid_signal(sig: i32) -> bool {
    (1..64).contains(&sig)
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
    mut stdin: Option<OwnedFd>,
    master: Option<OwnedFd>,
    tty: bool,
    reaper: Reaper,
    handle: Arc<Mutex<Option<ChildHandle>>>,
    cgroup: Option<Arc<SessionCgroup>>,
    host_disconnected: Arc<AtomicBool>,
    disconnect_grace: Duration,
    backpressure_probe: Duration,
) {
    let mut frames = FrameReader::default();
    let mut pending: VecDeque<u8> = VecDeque::new();
    let mut eof_pending = false;
    let mut canon = PtyCanonTracker::default();
    let mut disconnected = false;
    let mut last_probe: Option<Instant> = None;

    'outer: loop {
        let can_read = pending.len() < STDIN_BUF_CAP;
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
                fd: stdin.as_ref().map_or(-1, AsRawFd::as_raw_fd),
                events: libc::POLLOUT,
                revents: 0,
            },
        ];
        let nfds = if stdin.is_some() && !pending.is_empty() {
            4
        } else {
            3
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
        // Normal child exit wakes cancel_r: exit cleanly without signaling.
        if pfds[0].revents != 0 {
            break;
        }
        // Writer thread hit EPIPE/POLLERR/POLLHUP (even when both vsock
        // directions were backpressured): immediately trigger disconnect cleanup.
        if pfds[1].revents != 0 {
            disconnected = true;
            break;
        }
        if nfds == 4 && pfds[3].revents != 0 {
            if (pfds[3].revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL)) != 0
                && (pfds[3].revents & libc::POLLOUT) == 0
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
                while pending.len() < STDIN_BUF_CAP {
                    match frames.poll_frame(reader.as_raw_fd()) {
                        Ok(PollFrame::Msg(m)) => match m {
                            VmExecMsg::Data(d) => {
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
    drop(cgroup);
}

/// Relay output from `from` (nonblocking pipe or PTY master) until EOF/EIO.
///
/// Once `wake_r` closes (main child exited):
/// - If all write ends have closed (`POLLHUP`), drains every buffered byte
///   across arbitrary host backpressure without discarding output.
/// - If a surviving background descendant still holds the write end open,
///   bounds the post-exit window to `post_exit_idle` and bounds each read
///   batch so `wake_r` is always observed promptly even under continuous
///   descendant output. At the deadline, rechecks `POLLHUP` so a writer that
///   closed while a send was backpressured still drains completely.
fn pump(
    from: OwnedFd,
    wake_r: OwnedFd,
    writer: Arc<GuestWriter>,
    kind: fn(Vec<u8>) -> VmExecMsg,
    post_exit_idle: Duration,
    host_disconnected: Arc<AtomicBool>,
) -> io::Result<()> {
    let mut buf = vec![0u8; CHUNK];
    let mut child_exited = false;
    let mut idle_deadline: Option<Instant> = None;
    let mut all_writers_closed = false;

    loop {
        if host_disconnected.load(Ordering::Acquire) {
            return Ok(());
        }
        let timeout_ms = if !child_exited || all_writers_closed {
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
            idle_deadline = Some(Instant::now() + post_exit_idle);
        }
        if (pfds[0].revents & libc::POLLHUP) != 0 {
            all_writers_closed = true;
        }
        if pfds[0].revents != 0 || child_exited {
            let mut chunks = 0usize;
            loop {
                if host_disconnected.load(Ordering::Acquire) {
                    return Ok(());
                }
                if !all_writers_closed && chunks >= PUMP_DRAIN_BATCH {
                    // Return to the outer poll so `wake_r` (child exit) and
                    // `idle_deadline` are always observed during continuous
                    // output from a surviving descendant.
                    break;
                }
                let n = unsafe { libc::read(from.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
                if n > 0 {
                    chunks += 1;
                    if let Err(e) = writer.send_output(kind(buf[..n as usize].to_vec()).encode()) {
                        if host_disconnected.load(Ordering::Acquire) {
                            return Ok(());
                        }
                        return Err(e);
                    }
                } else if n == 0 {
                    return Ok(());
                } else {
                    let e = io::Error::last_os_error();
                    match e.kind() {
                        io::ErrorKind::Interrupted => continue,
                        io::ErrorKind::WouldBlock => break,
                        // On Linux PTY master, EIO signals all slave descriptors closed.
                        _ if e.raw_os_error() == Some(libc::EIO) => return Ok(()),
                        _ => return Err(e),
                    }
                }
            }
        }
    }
}

fn log(msg: &str) {
    eprintln!("kat-init: vm-exec: {msg}");
}

/// Start listening on [`VM_EXEC_PORT`] for host VM shell/exec sessions.
pub fn start(reaper: Reaper) -> io::Result<()> {
    let listener = sys::vsock_listen(VM_EXEC_PORT)?;
    let active = Arc::new(AtomicUsize::new(0));
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
                        let Some(guard) = ConnGuard::try_acquire(&active, MAX_CONNS) else {
                            log(&format!(
                                "connection limit ({MAX_CONNS}) reached on vsock {VM_EXEC_PORT}"
                            ));
                            continue;
                        };
                        let reaper = reaper.clone();
                        if let Err(e) =
                            thread::Builder::new()
                                .name("vm-exec-conn".into())
                                .spawn(move || {
                                    let _guard = guard;
                                    serve(&reaper, UnixStream::from(fd));
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

fn serve(reaper: &Reaper, conn: UnixStream) {
    serve_with_timeouts(
        reaper,
        conn,
        FIRST_FRAME_TIMEOUT,
        DISCONNECT_GRACE,
        POST_EXIT_IDLE,
        BACKPRESSURE_PROBE_INTERVAL,
        CgroupMode::Required(Path::new(CGROUP_PARENT)),
    );
}

fn serve_with_timeouts(
    reaper: &Reaper,
    mut conn: UnixStream,
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

fn resolve_workdir(cwd: &str) -> Result<PathBuf, String> {
    if !vm_exec::value_ok(cwd) {
        return Err("workdir contains NUL byte".into());
    }
    if cwd.is_empty() {
        return Ok(if Path::new(DEFAULT_HOME).is_dir() {
            PathBuf::from(DEFAULT_HOME)
        } else {
            PathBuf::from("/")
        });
    }
    match fs::metadata(cwd) {
        Ok(m) if m.is_dir() => Ok(PathBuf::from(cwd)),
        Ok(_) => Err(format!("workdir {cwd:?}: not a directory")),
        Err(e) => Err(format!("workdir {cwd:?}: {e}")),
    }
}

fn build_command(spec: &ExecSpec) -> Result<(Command, String), String> {
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

    let mut c = Command::new(program);
    c.args(args)
        .current_dir(&cwd)
        .env_clear()
        .env("PATH", VM_PATH)
        .env("HOME", DEFAULT_HOME)
        .env("USER", "root")
        .env("LOGNAME", "root")
        .env("SHELL", DEFAULT_SHELL)
        .env("CHARSET", "UTF-8");
    if is_shell {
        c.env("PS1", DEFAULT_PS1);
    }
    if let Some(term) = spec
        .tty
        .as_ref()
        .map(|t| t.term.as_str())
        .filter(|t| !t.is_empty())
    {
        c.env("TERM", term);
    }
    for (k, v) in &spec.env {
        c.env(k, v);
    }
    Ok((c, program.to_string()))
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
    let (mut c, program) = build_command(spec)?;

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
    let procs_raw = procs_fd.as_ref().map(AsRawFd::as_raw_fd);

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
            let (out_wake_r, out_wake_w) = cmd::pipe().map_err(|e| format!("pipe: {e}"))?;
            c.stdin(Stdio::from(
                slave.try_clone().map_err(|e| format!("pty stdin: {e}"))?,
            ))
            .stdout(Stdio::from(
                slave.try_clone().map_err(|e| format!("pty stdout: {e}"))?,
            ))
            .stderr(Stdio::from(slave));
            // SAFETY: write, setsid, and ioctl(TIOCSCTTY) are async-signal-safe.
            unsafe {
                c.pre_exec(move || {
                    if let Some(pfd) = procs_raw {
                        let n = libc::write(pfd, b"0\n".as_ptr().cast(), 2);
                        if n != 2 {
                            return Err(if n < 0 {
                                io::Error::last_os_error()
                            } else {
                                io::Error::new(
                                    io::ErrorKind::WriteZero,
                                    "short write to cgroup.procs",
                                )
                            });
                        }
                    }
                    if libc::setsid() < 0 || libc::ioctl(0, libc::TIOCSCTTY, 0) < 0 {
                        return Err(io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
            let (target, pgrp, exited) = reaper
                .spawn_pidfd_pgrp(&mut c)
                .map_err(|e| format!("exec {program:?}: {e}"))?;
            drop(c);
            drop(procs_fd);
            (
                ChildHandle::Pty { target, pgrp },
                exited,
                Some(stdin),
                Some(master),
                vec![(
                    out,
                    out_wake_r,
                    out_wake_w,
                    VmExecMsg::Stdout as fn(Vec<u8>) -> VmExecMsg,
                )],
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
            c.stdin(Stdio::from(in_r))
                .stdout(Stdio::from(out_w))
                .stderr(Stdio::from(err_w));
            // Put non-PTY commands in their own process group so host
            // disconnect or signals reach descendant processes too.
            // SAFETY: write and setpgid are async-signal-safe.
            unsafe {
                c.pre_exec(move || {
                    if let Some(pfd) = procs_raw {
                        let n = libc::write(pfd, b"0\n".as_ptr().cast(), 2);
                        if n != 2 {
                            return Err(if n < 0 {
                                io::Error::last_os_error()
                            } else {
                                io::Error::new(
                                    io::ErrorKind::WriteZero,
                                    "short write to cgroup.procs",
                                )
                            });
                        }
                    }
                    if libc::setpgid(0, 0) < 0 {
                        return Err(io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
            let (pgrp, exited) = reaper
                .spawn_pgrp(&mut c)
                .map_err(|e| format!("exec {program:?}: {e}"))?;
            drop(c);
            drop(procs_fd);
            (
                ChildHandle::Pipe(pgrp),
                exited,
                Some(in_w),
                None,
                vec![
                    (
                        out_r,
                        out_wake_r,
                        out_wake_w,
                        VmExecMsg::Stdout as fn(Vec<u8>) -> VmExecMsg,
                    ),
                    (err_r, err_wake_r, err_wake_w, VmExecMsg::Stderr),
                ],
            )
        }
    };

    let handle = Arc::new(Mutex::new(Some(handle)));
    let host_disconnected = Arc::new(AtomicBool::new(false));

    {
        let mut ds = DeadlineStream {
            conn: &mut conn,
            deadline: Instant::now() + first_frame_timeout,
        };
        if vm_exec::write(&mut ds, &VmExecMsg::Started).is_err() {
            signal_handle(reaper, &handle, libc::SIGKILL);
            if let Some(cg) = &cgroup {
                cg.kill();
            }
            *handle.lock().unwrap() = None;
            let _ = exited.recv_timeout(Duration::from_secs(2));
            return Ok(());
        }
    }

    let (writer, writer_thread) =
        match GuestWriter::start(conn, host_disconnected.clone(), disconnect_w) {
            Ok(pair) => pair,
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
    for (out, wake_r, wake_w, kind) in out_specs {
        let w2 = writer.clone();
        let disc = host_disconnected.clone();
        match thread::Builder::new()
            .name("vm-exec-out".into())
            .spawn(move || pump(out, wake_r, w2, kind, post_exit_idle, disc))
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
                drop(stdin_cancel_w);
                drop(out_wakes);
                drop(wake_w);
                let _ = stdin_thread.join();
                for o in outputs {
                    let _ = o.join();
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
    // Clear the handle and wake stdin_worker via its dedicated cancel pipe so
    // normal child exit is never confused with host disconnect.
    *handle.lock().unwrap() = None;
    drop(stdin_cancel_w);
    let _ = stdin_thread.join();

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

    if host_disconnected.load(Ordering::Acquire) {
        writer.abort();
        let _ = writer_thread.join();
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
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::net::Shutdown;

    use super::*;

    #[test]
    fn builds_default_shell_and_exec_commands() {
        let (cmd, prog) = build_command(&ExecSpec {
            cwd: "/".into(),
            env: vec![("FOO".into(), "bar".into())],
            argv: vec![],
            tty: Some(Tty {
                term: "xterm-256color".into(),
                rows: 24,
                cols: 80,
            }),
        })
        .unwrap();
        assert_eq!(prog, DEFAULT_SHELL);
        assert_eq!(cmd.get_program(), DEFAULT_SHELL);
        assert_eq!(cmd.get_args().count(), 0);
        let envs: Vec<(String, String)> = cmd
            .get_envs()
            .filter_map(|(k, v)| {
                Some((
                    k.to_string_lossy().into_owned(),
                    v?.to_string_lossy().into_owned(),
                ))
            })
            .collect();
        assert!(
            envs.iter()
                .any(|(k, v)| k == "PATH" && v.contains("k3s/bin"))
        );
        assert!(envs.iter().any(|(k, v)| k == "PS1" && v == DEFAULT_PS1));
        assert!(
            envs.iter()
                .any(|(k, v)| k == "TERM" && v == "xterm-256color")
        );
        assert!(envs.iter().any(|(k, v)| k == "FOO" && v == "bar"));

        let (cmd, prog) = build_command(&ExecSpec {
            cwd: "/".into(),
            env: vec![],
            argv: vec!["sysctl".into(), "-n".into(), "kernel.ostype".into()],
            tty: None,
        })
        .unwrap();
        assert_eq!(prog, "sysctl");
        let args: Vec<_> = cmd
            .get_args()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert_eq!(args, vec!["-n", "kernel.ostype"]);
        assert!(!cmd.get_envs().any(|(k, _)| k == "PS1"));
    }

    #[test]
    fn rejects_invalid_spec_inputs() {
        assert!(
            build_command(&ExecSpec {
                cwd: "/no/such/dir/katamaran-test".into(),
                ..Default::default()
            })
            .is_err()
        );
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
        assert!(!valid_signal(64));
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
        let active = Arc::new(AtomicUsize::new(0));
        let reaper = Reaper::start();
        let (_client, server) = UnixStream::pair().unwrap();
        let guard = ConnGuard::try_acquire(&active, 1).unwrap();
        assert_eq!(active.load(Ordering::Acquire), 1);

        let t = Instant::now();
        let handle = thread::spawn(move || {
            let _guard = guard;
            serve_with_timeouts(
                &reaper,
                server,
                Duration::from_millis(120),
                Duration::from_millis(100),
                Duration::from_millis(50),
                Duration::from_millis(50),
                CgroupMode::BestEffort(Path::new(CGROUP_PARENT)),
            );
        });
        handle.join().unwrap();
        assert!(t.elapsed() < Duration::from_secs(2));
        assert_eq!(active.load(Ordering::Acquire), 0);
        assert!(ConnGuard::try_acquire(&active, 1).is_some());
    }

    #[test]
    fn required_cgroup_mode_fails_loudly_when_cgroup_unavailable() {
        let reaper = Reaper::start();
        let (mut host, guest) = UnixStream::pair().unwrap();
        // `/dev/null/kat-vm-exec` cannot be created as a directory; Required
        // mode must return a guest Error instead of silently falling back to
        // leader-only disconnect cleanup.
        let server = thread::spawn(move || {
            serve_with_timeouts(
                &reaper,
                guest,
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
        let reaper = Reaper::start();
        let (mut host, guest) = UnixStream::pair().unwrap();

        let server = thread::spawn(move || {
            serve_with_timeouts(
                &reaper,
                guest,
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
            }),
        )
        .unwrap();
        assert_eq!(vm_exec::read(&mut host).unwrap(), Some(VmExecMsg::Started));
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
        let (writer, writer_thread) = GuestWriter::start(guest, disc.clone(), disc_w).unwrap();

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
                VmExecMsg::Stdout,
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
        let (wake_r, wake_w) = cmd::pipe().unwrap();
        let (mut host, guest) = UnixStream::pair().unwrap();
        shrink_pair(&host, &guest, 4096);
        let disc = Arc::new(AtomicBool::new(false));
        let (_disc_r, disc_w) = pipe_nonblock().unwrap();
        let (writer, writer_thread) = GuestWriter::start(guest, disc.clone(), disc_w).unwrap();

        let writer_clone = writer.clone();
        let pump_handle = thread::spawn(move || {
            pump(
                r,
                wake_r,
                writer_clone,
                VmExecMsg::Stdout,
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
        let (writer, writer_thread) = GuestWriter::start(guest, disc.clone(), disc_w).unwrap();

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
                VmExecMsg::Stdout,
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
        let (writer, writer_thread) = GuestWriter::start(guest, disc.clone(), disc_w).unwrap();
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
                VmExecMsg::Stdout,
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
        let reaper = Reaper::start();
        let (mut host, guest) = UnixStream::pair().unwrap();

        let server = thread::spawn(move || {
            serve_with_timeouts(
                &reaper,
                guest,
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
            }),
        )
        .unwrap();
        assert_eq!(vm_exec::read(&mut host).unwrap(), Some(VmExecMsg::Started));

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
        let reaper = Reaper::start();
        let (mut host, guest) = UnixStream::pair().unwrap();
        shrink_pair(&host, &guest, 8192);

        let server = thread::spawn(move || {
            serve_with_timeouts(
                &reaper,
                guest,
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
            }),
        )
        .unwrap();
        assert_eq!(vm_exec::read(&mut host).unwrap(), Some(VmExecMsg::Started));

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
}
