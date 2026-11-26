//! VM root shell and exec service, served to the host on
//! [`kat_proto::VM_EXEC_PORT`] (see [`kat_proto::vm_exec`]).
//!
//! Spawns root shells and arbitrary commands directly in PID 1's own
//! namespaces so developers can inspect containerd, dockerd, k3s, sysctls,
//! rlimits, and guest logs without needing Kubernetes or an LXC machine.
//! Idle overhead is a single vsock listener thread.

use std::collections::VecDeque;
use std::fs;
use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
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
/// Idle window after the main child exits before cancelling pumps held open by
/// a quiet background descendant.
const POST_EXIT_IDLE: Duration = Duration::from_millis(200);
/// Maximum bytes accepted from a noisy surviving descendant after the main
/// child exits and `POST_EXIT_IDLE` has elapsed (never applies when all write
/// ends have closed and buffered main-child output is draining).
const MAX_POST_EXIT_DESCENDANT_BYTES: usize = 2 << 20;
/// Maximum buffered stdin bytes held in user space while guest stdin is
/// backpressured, keeping memory bounded while still monitoring disconnect.
const STDIN_BUF_CAP: usize = 128 << 10;

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

struct ExecWriter {
    stream: UnixStream,
    closed: bool,
}

type Writer = Arc<Mutex<ExecWriter>>;

fn send(w: &Writer, m: &VmExecMsg) -> io::Result<()> {
    let mut guard = w.lock().unwrap();
    if guard.closed {
        return Err(io::Error::new(io::ErrorKind::BrokenPipe, "exec closed"));
    }
    vm_exec::write(&mut guard.stream, m)
}

fn send_final(w: &Writer, m: &VmExecMsg) -> io::Result<()> {
    let mut guard = w.lock().unwrap();
    if guard.closed {
        return Ok(());
    }
    guard.closed = true;
    vm_exec::write(&mut guard.stream, m)
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

/// Compute the byte sequence to write to a PTY master on `DataEnd`.
///
/// In canonical mode (`ICANON`), returns the terminal's configured `VEOF`:
/// twice if the previous byte did not terminate a line (the first flushes the
/// partial line, the second delivers EOF on the following read), or once if
/// the canonical buffer is already at a line boundary.
///
/// In noncanonical/raw mode (or when `VEOF` is disabled), returns 0 bytes so
/// literal control bytes are never injected into raw-mode sessions; raw PTY
/// readers have no in-band EOF mechanism.
fn pty_eof_bytes(termios: &libc::termios, last_byte: Option<u8>) -> ([u8; 2], usize) {
    if (termios.c_lflag & libc::ICANON) == 0 {
        return ([0, 0], 0);
    }
    let veof = termios.c_cc[libc::VEOF];
    if veof == 0 {
        return ([0, 0], 0);
    }
    let veol = termios.c_cc[libc::VEOL];
    let veol2 = termios.c_cc[libc::VEOL2];
    let icrnl = (termios.c_iflag & libc::ICRNL) != 0;
    let at_line_boundary = match last_byte {
        None => true,
        Some(b) => {
            b == b'\n'
                || b == veof
                || (veol != 0 && b == veol)
                || (veol2 != 0 && b == veol2)
                || (icrnl && b == b'\r')
        }
    };
    if at_line_boundary {
        ([veof, 0], 1)
    } else {
        ([veof, veof], 2)
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
    last_byte: &mut Option<u8>,
) {
    loop {
        while let Some(fd) = stdin.as_ref() {
            let slice = pending.as_slices().0;
            if slice.is_empty() {
                break;
            }
            let rc = unsafe { libc::write(fd.as_raw_fd(), slice.as_ptr().cast(), slice.len()) };
            if rc > 0 {
                pending.drain(..rc as usize);
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
        let (bytes, n) = pty_eof_bytes(&tio, *last_byte);
        if n == 0 {
            return;
        }
        *last_byte = Some(bytes[0]);
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
    cancel_r: OwnedFd,
    mut stdin: Option<OwnedFd>,
    master: Option<OwnedFd>,
    tty: bool,
    reaper: Reaper,
    handle: Arc<Mutex<Option<ChildHandle>>>,
    host_disconnected: Arc<AtomicBool>,
    disconnect_grace: Duration,
) {
    let mut frames = FrameReader::default();
    let mut pending: VecDeque<u8> = VecDeque::new();
    let mut eof_pending = false;
    let mut last_byte: Option<u8> = None;
    let mut disconnected = false;

    'outer: loop {
        let can_read = pending.len() < STDIN_BUF_CAP;
        let mut pfds = [
            libc::pollfd {
                fd: cancel_r.as_raw_fd(),
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
            3
        } else {
            2
        };
        let rc = unsafe { libc::poll(pfds.as_mut_ptr(), nfds, -1) };
        if rc < 0 {
            if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                continue;
            }
            break;
        }
        // Normal child exit wakes cancel_r: exit cleanly without signaling.
        if pfds[0].revents != 0 {
            break;
        }
        if nfds == 3 && pfds[2].revents != 0 {
            if (pfds[2].revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL)) != 0
                && (pfds[2].revents & libc::POLLOUT) == 0
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
                    &mut last_byte,
                );
            }
        }
        if pfds[1].revents != 0 {
            if can_read {
                while pending.len() < STDIN_BUF_CAP {
                    match frames.poll_frame(reader.as_raw_fd()) {
                        Ok(PollFrame::Msg(m)) => match m {
                            VmExecMsg::Data(d) => {
                                if !eof_pending && stdin.is_some() && !d.is_empty() {
                                    last_byte = d.last().copied();
                                    pending.extend(&d);
                                    flush_stdin(
                                        &mut stdin,
                                        &mut pending,
                                        &mut eof_pending,
                                        &master,
                                        tty,
                                        &mut last_byte,
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
                                    &mut last_byte,
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
            } else if (pfds[1].revents & (libc::POLLRDHUP | libc::POLLHUP | libc::POLLERR)) != 0 {
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
        // The host went away: hang up the session or process group, then
        // escalate to SIGKILL after a bounded grace period if still live.
        signal_handle(&reaper, &handle, libc::SIGHUP);
        if !wait_for_cancel(&cancel_r, disconnect_grace) {
            signal_handle(&reaper, &handle, libc::SIGKILL);
        }
    }
}

/// Relay output from `from` (nonblocking pipe or PTY master) until EOF/EIO.
///
/// Once `wake_r` closes (main child exited):
/// - If all write ends have closed (`POLLHUP`), drains every buffered byte
///   across arbitrary host backpressure without discarding output.
/// - If a surviving background descendant still holds the write end open,
///   bounds the post-exit idle wait to `post_exit_idle` (and caps noisy
///   descendant writes) so the worker always exits and can be joined cleanly.
fn pump(
    from: OwnedFd,
    wake_r: OwnedFd,
    w: Writer,
    kind: fn(Vec<u8>) -> VmExecMsg,
    post_exit_idle: Duration,
    host_disconnected: Arc<AtomicBool>,
) -> io::Result<()> {
    let mut buf = vec![0u8; CHUNK];
    let mut child_exited = false;
    let mut idle_deadline: Option<Instant> = None;
    let mut all_writers_closed = false;
    let mut post_exit_descendant_bytes: usize = 0;

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
                        return Ok(());
                    }
                    rem.as_millis().clamp(1, i32::MAX as u128) as i32
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
            loop {
                if host_disconnected.load(Ordering::Acquire) {
                    return Ok(());
                }
                let n = unsafe { libc::read(from.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
                if n > 0 {
                    let n = n as usize;
                    send(&w, &kind(buf[..n].to_vec()))?;
                    if child_exited && !all_writers_closed {
                        post_exit_descendant_bytes = post_exit_descendant_bytes.saturating_add(n);
                        if post_exit_descendant_bytes >= MAX_POST_EXIT_DESCENDANT_BYTES
                            && idle_deadline.is_some_and(|dl| Instant::now() >= dl)
                        {
                            return Ok(());
                        }
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
    );
}

fn serve_with_timeouts(
    reaper: &Reaper,
    mut conn: UnixStream,
    first_frame_timeout: Duration,
    disconnect_grace: Duration,
    post_exit_idle: Duration,
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

    // Clear handshake timeouts so interactive sessions stay untimed.
    if let Err(e) = conn
        .set_read_timeout(None)
        .and_then(|()| conn.set_write_timeout(None))
    {
        log(&format!("clear socket timeouts: {e}"));
        return;
    }

    if let Err(e) = exec_with_timeouts(
        reaper,
        conn.try_clone(),
        &spec,
        disconnect_grace,
        post_exit_idle,
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
/// been sent, all terminal frames (`Exit` or `Error`) are sent through `w`.
fn exec_with_timeouts(
    reaper: &Reaper,
    conn: io::Result<UnixStream>,
    spec: &ExecSpec,
    disconnect_grace: Duration,
    post_exit_idle: Duration,
) -> Result<(), String> {
    let conn = conn.map_err(|e| format!("clone stream: {e}"))?;
    let (mut c, program) = build_command(spec)?;

    let w: Writer = Arc::new(Mutex::new(ExecWriter {
        stream: conn.try_clone().map_err(|e| format!("clone stream: {e}"))?,
        closed: false,
    }));
    let reader = conn;

    // Allocate all control pipes and PTY/pipe descriptors BEFORE spawning the
    // child so no fallible descriptor allocation can occur after spawn.
    let (stdin_cancel_r, stdin_cancel_w) = cmd::pipe().map_err(|e| format!("pipe: {e}"))?;

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
            // SAFETY: setsid and ioctl(TIOCSCTTY) are async-signal-safe.
            unsafe {
                c.pre_exec(|| {
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
            // SAFETY: setpgid is async-signal-safe.
            unsafe {
                c.pre_exec(|| {
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

    if send(&w, &VmExecMsg::Started).is_err() {
        signal_handle(reaper, &handle, libc::SIGKILL);
        *handle.lock().unwrap() = None;
        let _ = exited.recv_timeout(Duration::from_secs(2));
        return Ok(());
    }

    let tty = spec.tty.is_some();
    let handle_in = handle.clone();
    let reaper_in = reaper.clone();
    let disc_in = host_disconnected.clone();
    let stdin_thread = match thread::Builder::new()
        .name("vm-exec-in".into())
        .spawn(move || {
            stdin_worker(
                reader,
                stdin_cancel_r,
                stdin,
                master,
                tty,
                reaper_in,
                handle_in,
                disc_in,
                disconnect_grace,
            );
        }) {
        Ok(h) => h,
        Err(e) => {
            signal_handle(reaper, &handle, libc::SIGKILL);
            *handle.lock().unwrap() = None;
            let _ = send_final(
                &w,
                &VmExecMsg::Error(truncate_error(&format!("spawn stdin thread: {e}"))),
            );
            let _ = exited.recv_timeout(Duration::from_secs(2));
            return Ok(());
        }
    };

    let mut outputs = Vec::with_capacity(out_specs.len());
    let mut out_wakes = Vec::with_capacity(out_specs.len());
    for (out, wake_r, wake_w, kind) in out_specs {
        let w2 = w.clone();
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
                *handle.lock().unwrap() = None;
                let _ = send_final(
                    &w,
                    &VmExecMsg::Error(truncate_error(&format!("spawn output thread: {e}"))),
                );
                drop(stdin_cancel_w);
                drop(out_wakes);
                drop(wake_w);
                let _ = stdin_thread.join();
                for o in outputs {
                    let _ = o.join();
                }
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
    // quiet background descendants cannot leak worker threads or descriptors.
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
        return Ok(());
    }
    if let Some(e) = output_err {
        let _ = send_final(
            &w,
            &VmExecMsg::Error(truncate_error(&format!("output relay failed: {e}"))),
        );
        return Ok(());
    }
    let _ = send_final(
        &w,
        &VmExecMsg::Exit {
            code: exit_code(status),
        },
    );
    Ok(())
}

#[cfg(test)]
mod tests {
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
        // Loopback, guest, hypervisor, and wildcard CIDs must be refused.
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
        // Wrong family or truncated/oversized length must be refused.
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
    fn pty_eof_bytes_handles_canonical_partial_and_noncanonical() {
        let mut tio: libc::termios = unsafe { std::mem::zeroed() };
        tio.c_lflag = libc::ICANON;
        tio.c_iflag = libc::ICRNL;
        tio.c_cc[libc::VEOF] = 0x04;

        // Empty or line-terminated input needs a single VEOF.
        assert_eq!(pty_eof_bytes(&tio, None), ([0x04, 0], 1));
        assert_eq!(pty_eof_bytes(&tio, Some(b'\n')), ([0x04, 0], 1));
        assert_eq!(pty_eof_bytes(&tio, Some(b'\r')), ([0x04, 0], 1));
        assert_eq!(pty_eof_bytes(&tio, Some(0x04)), ([0x04, 0], 1));

        // Partial line needs two VEOF bytes: flush line, then deliver EOF.
        assert_eq!(pty_eof_bytes(&tio, Some(b'x')), ([0x04, 0x04], 2));

        // Without ICRNL, '\r' is a partial line character.
        tio.c_iflag = 0;
        assert_eq!(pty_eof_bytes(&tio, Some(b'\r')), ([0x04, 0x04], 2));

        // Custom VEOF is respected.
        tio.c_cc[libc::VEOF] = 0x1a;
        assert_eq!(pty_eof_bytes(&tio, Some(b'a')), ([0x1a, 0x1a], 2));

        // Disabled VEOF (0) injects nothing.
        tio.c_cc[libc::VEOF] = 0;
        assert_eq!(pty_eof_bytes(&tio, Some(b'a')), ([0, 0], 0));

        // Noncanonical / raw mode injects nothing.
        tio.c_lflag = 0;
        tio.c_cc[libc::VEOF] = 0x04;
        assert_eq!(pty_eof_bytes(&tio, Some(b'a')), ([0, 0], 0));
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
            );
        });
        handle.join().unwrap();
        assert!(t.elapsed() < Duration::from_secs(2));
        assert_eq!(active.load(Ordering::Acquire), 0);
        assert!(ConnGuard::try_acquire(&active, 1).is_some());
    }

    #[test]
    fn pump_preserves_buffered_output_across_slow_reader() {
        let (r, w_pipe) = cmd::pipe().unwrap();
        set_nonblocking(&r).unwrap();
        let (wake_r, wake_w) = cmd::pipe().unwrap();
        let (mut host, guest) = UnixStream::pair().unwrap();
        let writer: Writer = Arc::new(Mutex::new(ExecWriter {
            stream: guest,
            closed: false,
        }));
        let disc = Arc::new(AtomicBool::new(false));

        let payload = vec![b'A'; 48 << 10];
        let mut wf = std::fs::File::from(w_pipe);
        wf.write_all(&payload).unwrap();
        drop(wf); // Child closed pipe and exited.
        drop(wake_w);

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

        // Pause longer than post_exit_idle before draining so any fixed-window
        // cutoff would truncate buffered bytes.
        thread::sleep(Duration::from_millis(200));

        let mut received = Vec::new();
        let drain = thread::spawn(move || {
            while let Ok(Some(msg)) = vm_exec::read(&mut host) {
                match msg {
                    VmExecMsg::Stdout(d) => received.extend_from_slice(&d),
                    VmExecMsg::Exit { code } => return (received, code),
                    other => panic!("unexpected frame {other:?}"),
                }
            }
            panic!("EOF before Exit");
        });

        pump_handle.join().unwrap().unwrap();
        send_final(&writer, &VmExecMsg::Exit { code: 0 }).unwrap();
        let (received, code) = drain.join().unwrap();
        assert_eq!(code, 0);
        assert_eq!(received.len(), payload.len());
        assert_eq!(received, payload);
    }

    #[test]
    fn pump_exits_when_quiet_background_descendant_keeps_pipe_open() {
        let (r, w_pipe) = cmd::pipe().unwrap();
        set_nonblocking(&r).unwrap();
        let (wake_r, wake_w) = cmd::pipe().unwrap();
        let (mut host, guest) = UnixStream::pair().unwrap();
        let writer: Writer = Arc::new(Mutex::new(ExecWriter {
            stream: guest,
            closed: false,
        }));
        let disc = Arc::new(AtomicBool::new(false));

        let mut wf = std::fs::File::from(w_pipe);
        wf.write_all(b"hi\n").unwrap();
        // Keep `wf` open to simulate a quiet background descendant retaining stdout.
        drop(wake_w);

        let t = Instant::now();
        let pump_handle = thread::spawn(move || {
            pump(
                r,
                wake_r,
                writer,
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
        drop(wf);
    }

    #[test]
    fn disconnect_under_stdin_backpressure_terminates_child() {
        let reaper = Reaper::start();
        let (mut host, guest) = UnixStream::pair().unwrap();

        let server = thread::spawn(move || {
            serve_with_timeouts(
                &reaper,
                guest,
                Duration::from_secs(2),
                Duration::from_millis(150),
                Duration::from_millis(50),
            );
        });

        // Probe readiness, then run a command that ignores SIGHUP and never reads stdin.
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

        // Fill guest stdin pipe + user-space buffer, then disconnect.
        let chunk = vec![b'x'; CHUNK];
        for _ in 0..6 {
            if vm_exec::write(&mut host, &VmExecMsg::Data(chunk.clone())).is_err() {
                break;
            }
        }
        let t = Instant::now();
        drop(host);
        server.join().unwrap();
        assert!(
            t.elapsed() < Duration::from_secs(3),
            "disconnect cleanup under backpressure took {:?}",
            t.elapsed()
        );
    }

    #[test]
    fn openpty_sets_cloexec_atomically() {
        let Ok((master, slave)) = openpty(&Tty {
            term: "xterm".into(),
            rows: 24,
            cols: 80,
        }) else {
            // Skip if /dev/ptmx is unavailable in a minimal test container.
            return;
        };
        let mf = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFD) };
        let sf = unsafe { libc::fcntl(slave.as_raw_fd(), libc::F_GETFD) };
        assert!(mf >= 0 && (mf & libc::FD_CLOEXEC) != 0);
        assert!(sf >= 0 && (sf & libc::FD_CLOEXEC) != 0);
    }
}
