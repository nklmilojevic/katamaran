//! Child process supervision for PID 1: reaping every child (orphans
//! included), and running containerd + dockerd with readiness and exit
//! reporting to the host.

use std::collections::HashMap;
use std::ffi::CString;
use std::fs::File;
use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use kat_proto::Msg;
use kat_proto::backoff::Backoff;

use super::machines::cmd;
use super::sys;

const PATH: &str = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";
pub const CONTAINERD_SOCKET: &str = "/run/containerd/containerd.sock";
pub const DOCKER_SOCKET: &str = "/run/docker.sock";
const READY_TIMEOUT: Duration = Duration::from_secs(120);

struct WatchedChild {
    token: u64,
    tx: Sender<i32>,
}

#[derive(Default)]
struct WatchTable {
    next_token: u64,
    children: HashMap<i32, WatchedChild>,
}

impl WatchTable {
    fn register(&mut self, pid: i32) -> (u64, Receiver<i32>) {
        self.next_token += 1;
        let token = self.next_token;
        let (tx, rx) = mpsc::channel();
        self.children.insert(pid, WatchedChild { token, tx });
        (token, rx)
    }

    fn matches(&self, pid: i32, token: u64) -> bool {
        self.children.get(&pid).is_some_and(|w| w.token == token)
    }
}

/// Reaps all children. PID 1 inherits every orphan, so something must wait
/// for them; processes started through [`Reaper::spawn`] also get their exit
/// status delivered on a channel.
#[derive(Clone)]
pub struct Reaper {
    watched: Arc<Mutex<WatchTable>>,
}

/// Identity-pinned handle for a child spawned in its own process group.
/// `token` uniquely identifies this spawn inside [`Reaper`], so once the
/// leader is reaped a recycled PID can never match this handle.
#[derive(Clone, Copy, Debug)]
pub struct PgrpTarget {
    pid: i32,
    token: u64,
}

/// How to signal a spawned child without racing PID reuse.
#[derive(Debug)]
pub enum ChildTarget {
    /// Kernel pidfd: `pidfd_send_signal` returns `ESRCH` once reaped.
    PidFd(OwnedFd),
    /// Fallback when `pidfd_open` is unavailable on the kernel.
    /// `token` pins this specific spawn so a recycled PID can never match.
    Pid { pid: i32, token: u64 },
    /// The child had already exited and been reaped before `pidfd_open` ran.
    Exited,
}

impl Reaper {
    pub fn start() -> Reaper {
        let r = Reaper {
            watched: Arc::new(Mutex::new(WatchTable::default())),
        };
        let watched = r.watched.clone();
        thread::Builder::new()
            .name("reaper".into())
            .spawn(move || reap_loop(&watched))
            .expect("spawn reaper thread");
        r
    }

    /// Spawn `cmd` and return its pid plus a channel that yields its wait
    /// status. The lock is held across spawn and registration so an
    /// immediate exit cannot be reaped before it is watched.
    pub fn spawn(&self, cmd: &mut Command) -> io::Result<(i32, Receiver<i32>)> {
        let (target, rx) = self.spawn_pgrp(cmd)?;
        Ok((target.pid, rx))
    }
}

/// Specification for [`Reaper::spawn_unlocked`], which spawns a child directly
/// via `fork` + `execve` without holding PID 1's shared reaper lock across
/// `fork`, `fchdir`, `cgroup.procs`, or `execve`, and leaves `reap_loop` as the
/// sole child-reaping owner on both success and failure.
pub struct UnlockedSpawnSpec {
    pub program: String,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    pub stdin: OwnedFd,
    pub stdout: OwnedFd,
    pub stderr: OwnedFd,
    pub cwd_fd: OwnedFd,
    pub cgroup_procs_fd: Option<OwnedFd>,
    /// True for PTY sessions (`setsid` + `TIOCSCTTY` + `pidfd_open`); false
    /// for piped sessions (`setpgid(0, 0)`).
    pub pty: bool,
}

fn last_errno() -> libc::c_int {
    io::Error::last_os_error()
        .raw_os_error()
        .unwrap_or(libc::EIO)
}

unsafe fn fail_child(err_fd: RawFd, errno: libc::c_int) -> ! {
    let bytes = errno.to_ne_bytes();
    let mut off = 0usize;
    while off < bytes.len() {
        let rc = unsafe { libc::write(err_fd, bytes[off..].as_ptr().cast(), bytes.len() - off) };
        if rc > 0 {
            off += rc as usize;
        } else if rc < 0 && last_errno() == libc::EINTR {
            continue;
        } else {
            break;
        }
    }
    unsafe { libc::_exit(127) }
}

unsafe fn dup_ge7(fd: RawFd, err_fd: RawFd) -> RawFd {
    let r = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 7) };
    if r < 0 {
        unsafe { fail_child(err_fd, last_errno()) };
    }
    r
}

unsafe fn dup2_retry(src: RawFd, dst: RawFd, err_fd: RawFd) {
    loop {
        let rc = unsafe { libc::dup2(src, dst) };
        if rc >= 0 {
            return;
        }
        let e = last_errno();
        if e != libc::EINTR {
            unsafe { fail_child(err_fd, e) };
        }
    }
}

unsafe fn dup2_cloexec(src: RawFd, dst: RawFd, err_fd: RawFd) {
    unsafe { dup2_retry(src, dst, err_fd) };
    if unsafe { libc::fcntl(dst, libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
        unsafe { fail_child(err_fd, last_errno()) };
    }
}

unsafe fn close_fds_from(first: u32, max_fd: u32) {
    // Linux generic __NR_close_range (436 since Linux 5.9).
    const SYS_CLOSE_RANGE: libc::c_long = 436;
    let rc = unsafe { libc::syscall(SYS_CLOSE_RANGE, first as libc::c_uint, u32::MAX, 0u32) };
    if rc < 0 {
        for fd in first..max_fd {
            unsafe {
                let _ = libc::close(fd as RawFd);
            }
        }
    }
}

unsafe fn write_all_raw(fd: RawFd, data: &[u8]) -> Result<(), libc::c_int> {
    let mut off = 0usize;
    while off < data.len() {
        let rc = unsafe { libc::write(fd, data[off..].as_ptr().cast(), data.len() - off) };
        if rc > 0 {
            off += rc as usize;
        } else if rc == 0 {
            return Err(libc::EIO);
        } else {
            let e = last_errno();
            if e != libc::EINTR {
                return Err(e);
            }
        }
    }
    Ok(())
}

fn prepare_exec_tables(
    program: &str,
    args: &[String],
    env: &[(String, String)],
) -> io::Result<(Vec<CString>, Vec<CString>, Vec<CString>)> {
    let invalid = || io::Error::new(io::ErrorKind::InvalidInput, "NUL byte in exec spec");
    if program.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "empty executable",
        ));
    }

    // Deduplicate env entries preserving order (last value wins), matching `Command::env`.
    let mut dedup_env: Vec<(&str, &str)> = Vec::with_capacity(env.len());
    for (k, v) in env {
        if let Some(existing) = dedup_env.iter_mut().find(|(ek, _)| *ek == k.as_str()) {
            existing.1 = v.as_str();
        } else {
            dedup_env.push((k.as_str(), v.as_str()));
        }
    }

    let mut candidates = Vec::new();
    if program.contains('/') {
        candidates.push(CString::new(program).map_err(|_| invalid())?);
    } else if let Some((_, path)) = dedup_env.iter().find(|(k, _)| *k == "PATH") {
        for dir in path.split(':') {
            let full = if dir.is_empty() {
                program.to_string()
            } else {
                format!("{dir}/{program}")
            };
            candidates.push(CString::new(full).map_err(|_| invalid())?);
        }
    }
    if candidates.is_empty() {
        candidates.push(CString::new(program).map_err(|_| invalid())?);
    }

    let mut argv = Vec::with_capacity(1 + args.len());
    argv.push(CString::new(program).map_err(|_| invalid())?);
    for a in args {
        argv.push(CString::new(a.as_str()).map_err(|_| invalid())?);
    }

    let mut envp = Vec::with_capacity(dedup_env.len());
    for (k, v) in dedup_env {
        envp.push(CString::new(format!("{k}={v}")).map_err(|_| invalid())?);
    }

    Ok((candidates, argv, envp))
}

impl Reaper {
    /// Spawn a child from `spec` without holding PID 1's shared reaper lock
    /// across `fork`, `fchdir`, `cgroup.procs`, or `execve`.
    ///
    /// Immediately after `fork`, the child packs only its owned descriptors
    /// into `0..=6` and closes all other inherited descriptors (preventing
    /// deadlocks with concurrent `Command::spawn` exec-status sockets) before
    /// pausing on the registration gate. The parent briefly acquires
    /// `self.watched` to register the PID (and open a `pidfd` for PTY
    /// sessions), releases the lock and gate, and reads any exec failure from
    /// an `O_CLOEXEC` status pipe while leaving `reap_loop` as the single
    /// child-reaping owner on both success and failure.
    pub fn spawn_unlocked(
        &self,
        spec: UnlockedSpawnSpec,
    ) -> io::Result<(ChildTarget, PgrpTarget, Receiver<i32>)> {
        self.spawn_unlocked_inner(spec, None)
    }

    fn spawn_unlocked_inner(
        &self,
        spec: UnlockedSpawnSpec,
        before_reg: Option<Box<dyn FnOnce()>>,
    ) -> io::Result<(ChildTarget, PgrpTarget, Receiver<i32>)> {
        let (candidates, argv, envp) = prepare_exec_tables(&spec.program, &spec.args, &spec.env)?;
        let candidate_ptrs: Vec<*const libc::c_char> =
            candidates.iter().map(|c| c.as_ptr()).collect();
        let mut argv_ptrs: Vec<*const libc::c_char> = argv.iter().map(|c| c.as_ptr()).collect();
        argv_ptrs.push(std::ptr::null());
        let mut envp_ptrs: Vec<*const libc::c_char> = envp.iter().map(|c| c.as_ptr()).collect();
        envp_ptrs.push(std::ptr::null());

        let (go_r, go_w) = cmd::pipe()?;
        let (err_r, err_w) = cmd::pipe()?;
        let sc = unsafe { libc::sysconf(libc::_SC_OPEN_MAX) };
        let max_fd: u32 = if sc > 0 {
            (sc as u64).clamp(1024, 65536) as u32
        } else {
            4096
        };

        let stdin_raw = spec.stdin.as_raw_fd();
        let stdout_raw = spec.stdout.as_raw_fd();
        let stderr_raw = spec.stderr.as_raw_fd();
        let go_r_raw = go_r.as_raw_fd();
        let err_w_raw = err_w.as_raw_fd();
        let cwd_raw = spec.cwd_fd.as_raw_fd();
        let procs_raw = spec.cgroup_procs_fd.as_ref().map(AsRawFd::as_raw_fd);
        let pty = spec.pty;

        // SAFETY: The child branch runs strictly async-signal-safe syscalls
        // using pre-allocated slices and exits via `_exit(127)` on any error.
        let pid = unsafe { libc::fork() };
        if pid < 0 {
            return Err(io::Error::last_os_error());
        }
        if pid == 0 {
            unsafe {
                let r_err = dup_ge7(err_w_raw, err_w_raw);
                let r_in = dup_ge7(stdin_raw, r_err);
                let r_out = dup_ge7(stdout_raw, r_err);
                let r_stderr = dup_ge7(stderr_raw, r_err);
                let r_go = dup_ge7(go_r_raw, r_err);
                let r_cwd = dup_ge7(cwd_raw, r_err);
                let r_procs = procs_raw.map(|fd| dup_ge7(fd, r_err));

                dup2_retry(r_in, 0, r_err);
                dup2_retry(r_out, 1, r_err);
                dup2_retry(r_stderr, 2, r_err);
                dup2_cloexec(r_go, 3, r_err);
                dup2_cloexec(r_err, 4, r_err);
                dup2_cloexec(r_cwd, 5, 4);
                let close_from: u32 = if let Some(rp) = r_procs {
                    dup2_cloexec(rp, 6, 4);
                    7
                } else {
                    6
                };

                // Close EVERY unrelated inherited descriptor (including any
                // concurrent `Command::spawn` exec-status socket) BEFORE
                // waiting at the registration gate.
                close_fds_from(close_from, max_fd);

                let mut empty: libc::sigset_t = std::mem::zeroed();
                libc::sigemptyset(&mut empty);
                libc::sigprocmask(libc::SIG_SETMASK, &empty, std::ptr::null_mut());
                libc::signal(libc::SIGPIPE, libc::SIG_DFL);

                // Wait for the parent to register our PID and release the gate.
                let mut gate = [0u8; 1];
                loop {
                    let rc = libc::read(3, gate.as_mut_ptr().cast(), 1);
                    if rc == 1 {
                        if gate[0] != 1 {
                            libc::_exit(127);
                        }
                        break;
                    }
                    if rc == 0 {
                        libc::_exit(127);
                    }
                    let e = last_errno();
                    if e != libc::EINTR {
                        fail_child(4, e);
                    }
                }
                let _ = libc::close(3);

                if r_procs.is_some() {
                    if let Err(e) = write_all_raw(6, b"0\n") {
                        fail_child(4, e);
                    }
                    let _ = libc::close(6);
                }
                if libc::fchdir(5) < 0 {
                    fail_child(4, last_errno());
                }
                let _ = libc::close(5);

                if pty {
                    if libc::setsid() < 0 || libc::ioctl(0, libc::TIOCSCTTY, 0) < 0 {
                        fail_child(4, last_errno());
                    }
                } else if libc::setpgid(0, 0) < 0 {
                    fail_child(4, last_errno());
                }

                let mut saved_eacces: Option<libc::c_int> = None;
                let mut final_err = libc::ENOENT;
                for &cand in &candidate_ptrs {
                    libc::execve(cand, argv_ptrs.as_ptr(), envp_ptrs.as_ptr());
                    let e = last_errno();
                    match e {
                        libc::ENOENT | libc::ENOTDIR => final_err = e,
                        libc::EACCES | libc::EPERM => saved_eacces = Some(e),
                        other => {
                            final_err = other;
                            saved_eacces = None;
                            break;
                        }
                    }
                }
                fail_child(4, saved_eacces.unwrap_or(final_err));
            }
        }

        // Parent: drop child-side descriptors immediately so only the child
        // holds `err_w` and `go_r`.
        drop(go_r);
        drop(err_w);
        drop(spec);

        if let Some(hook) = before_reg {
            hook();
        }

        // Hold `self.watched` ONLY for the microsecond registration + pidfd_open,
        // never across `fchdir`, `cgroup.procs`, or `execve`.
        let (target, pgrp, rx) = {
            let mut table = self.watched.lock().unwrap();
            let (token, rx) = table.register(pid);
            let target = if pty {
                match sys::pidfd_open(pid) {
                    Ok(fd) => ChildTarget::PidFd(fd),
                    Err(e) if e.raw_os_error() == Some(libc::ESRCH) => ChildTarget::Exited,
                    Err(_) => ChildTarget::Pid { pid, token },
                }
            } else {
                ChildTarget::Pid { pid, token }
            };
            (target, PgrpTarget { pid, token }, rx)
        };

        // Release the child with an explicit byte so any unrelated concurrent
        // fork transiently inheriting `go_w` cannot stall the gate.
        let go_byte = [1u8];
        loop {
            let rc = unsafe { libc::write(go_w.as_raw_fd(), go_byte.as_ptr().cast(), 1) };
            if rc >= 0 {
                break;
            }
            if io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
                break;
            }
        }
        drop(go_w);

        let mut err_buf = [0u8; 4];
        let mut err_off = 0usize;
        while err_off < err_buf.len() {
            let rc = unsafe {
                libc::read(
                    err_r.as_raw_fd(),
                    err_buf[err_off..].as_mut_ptr().cast(),
                    err_buf.len() - err_off,
                )
            };
            if rc > 0 {
                err_off += rc as usize;
            } else if rc == 0 {
                break;
            } else if io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
                let e = io::Error::last_os_error();
                self.finish_failed_unlocked_child(pgrp, &target, &rx);
                return Err(e);
            }
        }
        drop(err_r);

        if err_off == 0 {
            return Ok((target, pgrp, rx));
        }

        self.finish_failed_unlocked_child(pgrp, &target, &rx);
        if err_off == 4 {
            Err(io::Error::from_raw_os_error(i32::from_ne_bytes(err_buf)))
        } else {
            Err(io::Error::other("child terminated before completing exec"))
        }
    }

    /// Wait for `reap_loop` (the sole reaping owner) to collect a child that
    /// failed before `execve`, and clear any stale watch entry if the child had
    /// already been reaped before `register` ran.
    fn finish_failed_unlocked_child(
        &self,
        pgrp: PgrpTarget,
        target: &ChildTarget,
        rx: &Receiver<i32>,
    ) {
        if !matches!(target, ChildTarget::Exited) {
            let _ = rx.recv_timeout(Duration::from_secs(2));
        }
        let mut table = self.watched.lock().unwrap();
        if table.matches(pgrp.pid, pgrp.token) {
            table.children.remove(&pgrp.pid);
        }
    }
}

impl Reaper {
    /// Spawn `cmd` and return a token-pinned [`PgrpTarget`] plus a channel
    /// yielding its wait status. The lock is held across spawn and
    /// registration so an immediate exit cannot be reaped before it is watched.
    pub fn spawn_pgrp(&self, cmd: &mut Command) -> io::Result<(PgrpTarget, Receiver<i32>)> {
        let mut table = self.watched.lock().unwrap();
        let child = cmd.spawn()?;
        let pid = child.id() as i32;
        let (token, rx) = table.register(pid);
        Ok((PgrpTarget { pid, token }, rx))
    }

    /// Like [`Reaper::spawn`], also opening a `pidfd` while the watch table
    /// lock is held. Because `reap_loop` waits with `WNOWAIT` and only reaps
    /// under `self.watched`, the child cannot be reaped or its PID recycled
    /// before `pidfd_open` and registration complete.
    pub fn spawn_pidfd(&self, cmd: &mut Command) -> io::Result<(ChildTarget, Receiver<i32>)> {
        let (target, _, rx) = self.spawn_pidfd_pgrp(cmd)?;
        Ok((target, rx))
    }

    /// Like [`Reaper::spawn_pidfd`], also returning a token-pinned
    /// [`PgrpTarget`] for safe process-group signaling of session/pgrp leaders.
    pub fn spawn_pidfd_pgrp(
        &self,
        cmd: &mut Command,
    ) -> io::Result<(ChildTarget, PgrpTarget, Receiver<i32>)> {
        let mut table = self.watched.lock().unwrap();
        let child = cmd.spawn()?;
        let pid = child.id() as i32;
        let (token, rx) = table.register(pid);
        let target = match sys::pidfd_open(pid) {
            Ok(fd) => ChildTarget::PidFd(fd),
            Err(e) if e.raw_os_error() == Some(libc::ESRCH) => ChildTarget::Exited,
            Err(_) => ChildTarget::Pid { pid, token },
        };
        Ok((target, PgrpTarget { pid, token }, rx))
    }

    /// Signal `target` without risking PID reuse. On kernels with `pidfd`,
    /// `pidfd_send_signal` enforces process identity in the kernel; on the
    /// fallback `ChildTarget::Pid` path, holding `self.watched` and matching
    /// `token` prevents `reap_loop` from reaping and freeing the PID across
    /// the liveness check and `kill(2)`.
    pub fn signal_target(&self, target: &ChildTarget, sig: i32) {
        match target {
            ChildTarget::PidFd(fd) => {
                let _ = sys::pidfd_send_signal(fd, sig);
            }
            ChildTarget::Pid { pid, token } => {
                let table = self.watched.lock().unwrap();
                if table.matches(*pid, *token) {
                    unsafe { libc::kill(*pid, sig) };
                }
            }
            ChildTarget::Exited => {}
        }
    }

    /// Signal process group `-pid` only while `pid` is still registered in
    /// `self.watched`. Prefer [`Self::signal_pgrp_target`] for callers that
    /// may signal concurrently with child exit and reaping.
    pub fn signal_pgrp(&self, pid: i32, sig: i32) {
        let table = self.watched.lock().unwrap();
        if table.children.contains_key(&pid) {
            unsafe { libc::kill(-pid, sig) };
        }
    }

    /// Signal process group `-target.pid` only while the exact spawn
    /// identified by `target` is still registered in `self.watched` (not yet
    /// reaped). Because `reap_loop` waits with `WNOWAIT` and only reaps under
    /// `self.watched`, holding the lock guarantees neither the PID nor its
    /// PGID can be reused before `kill(-target.pid, sig)` runs.
    pub fn signal_pgrp_target(&self, target: PgrpTarget, sig: i32) -> bool {
        let table = self.watched.lock().unwrap();
        if table.matches(target.pid, target.token) {
            unsafe { libc::kill(-target.pid, sig) };
            true
        } else {
            false
        }
    }
}

fn reap_loop(watched: &Mutex<WatchTable>) {
    loop {
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        // Wait without reaping (`WNOWAIT`) so zombies stay pinned until we hold
        // `watched`; that guarantees `spawn_pidfd` and token-guarded signaling
        // can never race with reaping and PID reuse.
        let rc = unsafe { libc::waitid(libc::P_ALL, 0, &mut info, libc::WEXITED | libc::WNOWAIT) };
        if rc < 0 {
            if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                continue;
            }
            // ECHILD: nothing to wait for yet.
            thread::sleep(Duration::from_millis(50));
            continue;
        }
        let pid = unsafe { info.si_pid() };
        if pid <= 0 {
            continue;
        }
        let mut status = 0;
        let tx = {
            let mut table = watched.lock().unwrap();
            let reaped = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
            if reaped > 0 {
                table.children.remove(&reaped).map(|w| w.tx)
            } else {
                None
            }
        };
        if let Some(tx) = tx {
            let _ = tx.send(status);
        }
    }
}

pub fn describe_status(status: i32) -> String {
    if libc::WIFEXITED(status) {
        format!("exited with status {}", libc::WEXITSTATUS(status))
    } else if libc::WIFSIGNALED(status) {
        format!("killed by signal {}", libc::WTERMSIG(status))
    } else {
        format!("wait status {status}")
    }
}

/// Sends control messages to the host from any thread.
#[derive(Clone)]
pub struct Notifier(pub Arc<Mutex<File>>);

impl Notifier {
    pub fn send(&self, msg: &Msg) {
        let mut conn = self.0.lock().unwrap();
        if let Err(e) = kat_proto::write_msg(&mut *conn, msg) {
            eprintln!("kat-init: failed to notify host: {e}");
        }
    }

    pub fn failed(&self, name: &str, error: String) {
        self.send(&Msg::ServiceFailed {
            name: name.into(),
            error,
        });
    }
}

/// The two services of the Docker stack, in start order.
const CONTAINERD: usize = 0;
const DOCKERD: usize = 1;
const NAMES: [&str; 2] = ["containerd", "dockerd"];
/// Stop timeouts, after which the service is killed. dockerd's covers
/// containers the shutdown sequence did not stop already.
const STOP_TIMEOUT: [Duration; 2] = [Duration::from_secs(5), Duration::from_secs(10)];
/// Give up after this many restarts without a stable run.
const MAX_RESTARTS: u32 = 10;

fn backoff() -> Backoff {
    Backoff::new(
        Duration::from_millis(500),
        Duration::from_secs(30),
        Duration::from_secs(60),
    )
}

struct ExitState {
    status: Mutex<Option<i32>>,
    notify: Condvar,
}

impl ExitState {
    fn new() -> Self {
        Self {
            status: Mutex::new(None),
            notify: Condvar::new(),
        }
    }

    fn has_exited(&self) -> bool {
        self.status.lock().unwrap().is_some()
    }

    fn wait_timeout(&self, timeout: Duration) -> bool {
        let guard = self.status.lock().unwrap();
        let (guard, _) = self
            .notify
            .wait_timeout_while(guard, timeout, |s| s.is_none())
            .unwrap();
        guard.is_some()
    }
}

/// A running service process.
struct Service {
    name: &'static str,
    reaper: Reaper,
    target: ChildTarget,
    started: Instant,
    exit: Arc<ExitState>,
}

impl Service {
    /// Spawn `program`; its exit is sent on `exits` as `(index, wait status)`.
    fn spawn(
        reaper: &Reaper,
        exits: &Sender<(usize, i32)>,
        index: usize,
        program: &str,
        args: &[&str],
    ) -> io::Result<Arc<Service>> {
        let name = NAMES[index];
        sys::mkdir("/var/log")?;
        // Appended, so a restarted service keeps the log of its crash.
        let log = File::options()
            .create(true)
            .append(true)
            .open(format!("/var/log/{name}.log"))?;
        let mut cmd = Command::new(program);
        cmd.args(args)
            .env("PATH", PATH)
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log);
        let (target, status_rx) = reaper.spawn_pidfd(&mut cmd)?;
        let exit = Arc::new(ExitState::new());
        let (exit2, exits) = (exit.clone(), exits.clone());
        let waiter = thread::Builder::new()
            .name(format!("{name}-wait"))
            .spawn(move || {
                if let Ok(status) = status_rx.recv() {
                    *exit2.status.lock().unwrap() = Some(status);
                    exit2.notify.notify_all();
                    let _ = exits.send((index, status));
                }
            });
        if let Err(e) = waiter {
            // `reaper.signal_target` uses pidfd when available, or holds the
            // reaper watch-table lock across the liveness check and kill(2) so
            // `reap_loop` cannot reap and free the PID in between.
            reaper.signal_target(&target, libc::SIGKILL);
            return Err(e);
        }
        Ok(Arc::new(Service {
            name,
            reaper: reaper.clone(),
            target,
            started: Instant::now(),
            exit,
        }))
    }

    fn has_exited(&self) -> bool {
        self.exit.has_exited()
    }

    /// SIGTERM, wait up to `timeout`, then SIGKILL.
    fn stop(&self, timeout: Duration) {
        if self.has_exited() {
            return;
        }
        let t = Instant::now();
        self.reaper.signal_target(&self.target, libc::SIGTERM);
        if !self.exit.wait_timeout(timeout) {
            eprintln!(
                "kat-init: {} did not stop in {}s, killing it",
                self.name,
                timeout.as_secs()
            );
            if !self.has_exited() {
                self.reaper.signal_target(&self.target, libc::SIGKILL);
            }
            let _ = self.exit.wait_timeout(Duration::from_secs(2));
        }
        eprintln!(
            "kat-init: shutdown: {} stopped in {} ms",
            self.name,
            t.elapsed().as_millis()
        );
    }
}

/// Poll `check` until it succeeds, the process exits or the timeout passes.
fn wait_ready(name: &str, exit: &ExitState, check: impl Fn() -> bool) -> Result<(), String> {
    let deadline = Instant::now() + READY_TIMEOUT;
    loop {
        if exit.has_exited() {
            return Err(format!("{name} exited during startup"));
        }
        if check() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{name} not ready after {}s, see /var/log/{name}.log",
                READY_TIMEOUT.as_secs(),
            ));
        }
        thread::sleep(Duration::from_millis(2));
    }
}

#[derive(Default)]
struct ContainerdConfigState {
    path: Option<String>,
    generation: u64,
    /// True once `containerd` has reached `Ready` with the current `path` and
    /// `generation`. Only an unproven override is rolled back to the stock
    /// config on startup failure; a proven-good override is retained across
    /// later transient containerd restarts so a running k3s server never loses
    /// CRI.
    proven_ready: bool,
    waiter: Option<ReloadWaiter>,
}

struct ReloadWaiter {
    target_generation: u64,
    tx: Sender<Result<(), String>>,
}

#[derive(Default)]
struct Inner {
    services: Mutex<[Option<Arc<Service>>; 2]>,
    stopping: AtomicBool,
    reloading_containerd: AtomicBool,
    containerd_config: Mutex<ContainerdConfigState>,
}

/// containerd and dockerd, started in order on a background thread and
/// restarted with backoff when either exits. Each exit is reported to the
/// host as `ServiceFailed`, and dockerd's return to service as
/// `ServiceReady { name: "docker" }`.
#[derive(Clone, Default)]
pub struct DockerStack {
    inner: Arc<Inner>,
}

impl DockerStack {
    pub fn start(
        reaper: Reaper,
        notifier: Notifier,
        containerd_config: Option<String>,
    ) -> DockerStack {
        let stack = DockerStack::default();
        stack.inner.containerd_config.lock().unwrap().path = containerd_config;
        let inner = stack.inner.clone();
        thread::Builder::new()
            .name("docker-supervisor".into())
            .spawn(move || supervise(&reaper, &notifier, &inner))
            .expect("spawn docker supervisor");
        stack
    }

    /// The runtime config override currently configured for containerd, or
    /// `None` when using the stock `/etc/containerd/config.toml`.
    pub fn containerd_config(&self) -> Option<String> {
        self.inner.containerd_config.lock().unwrap().path.clone()
    }

    fn stop_containerd_for_reload(&self) {
        let service = {
            let services = self.inner.services.lock().unwrap();
            match &services[CONTAINERD] {
                Some(s) if !s.has_exited() => {
                    self.inner
                        .reloading_containerd
                        .store(true, Ordering::SeqCst);
                    Some(s.clone())
                }
                _ => None,
            }
        };
        if let Some(s) = service {
            s.stop(STOP_TIMEOUT[CONTAINERD]);
        }
    }

    /// Switch containerd to `config` (or the stock `/etc/containerd/config.toml`
    /// when `None`), restart it cleanly without reporting a failure or backoff,
    /// and wait until the restarted containerd instance becomes ready or fails.
    pub fn reload_containerd_config(
        &self,
        config: Option<String>,
        timeout: Duration,
    ) -> Result<(), String> {
        if self.inner.stopping.load(Ordering::SeqCst) {
            return Err("docker stack is stopping".into());
        }
        let (tx, rx) = mpsc::channel();
        let target_generation = {
            let mut cfg = self.inner.containerd_config.lock().unwrap();
            cfg.generation += 1;
            cfg.path = config;
            cfg.proven_ready = false;
            cfg.waiter = Some(ReloadWaiter {
                target_generation: cfg.generation,
                tx,
            });
            cfg.generation
        };
        self.stop_containerd_for_reload();
        match rx.recv_timeout(timeout) {
            Ok(res) => res,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let rolled_back_path = {
                    let mut cfg = self.inner.containerd_config.lock().unwrap();
                    if cfg
                        .waiter
                        .as_ref()
                        .is_some_and(|w| w.target_generation == target_generation)
                    {
                        cfg.waiter = None;
                    }
                    if cfg.generation == target_generation
                        && cfg.path.is_some()
                        && !cfg.proven_ready
                    {
                        cfg.generation += 1;
                        cfg.proven_ready = false;
                        cfg.path.take()
                    } else {
                        None
                    }
                };
                if let Some(override_path) = rolled_back_path {
                    let _ = std::fs::remove_file(&override_path);
                    self.stop_containerd_for_reload();
                }
                Err(format!(
                    "timed out waiting for containerd reload after {}s",
                    timeout.as_secs()
                ))
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                Err("docker supervisor exited during containerd reload".into())
            }
        }
    }

    /// Stop in reverse start order: dockerd first, then containerd. No
    /// restart happens after this.
    pub fn stop(&self) {
        self.inner.stopping.store(true, Ordering::SeqCst);
        fail_containerd_waiter(&self.inner, "docker stack is stopping".into());
        let services = { self.inner.services.lock().unwrap().clone() };
        for i in [DOCKERD, CONTAINERD] {
            if let Some(s) = &services[i] {
                s.stop(STOP_TIMEOUT[i]);
            }
        }
    }
}

struct SuperviseExit<'a>(&'a Inner);

impl Drop for SuperviseExit<'_> {
    fn drop(&mut self) {
        fail_containerd_waiter(self.0, "docker supervisor stopped".into());
    }
}

fn mark_containerd_ready(inner: &Inner, generation: u64) {
    let waiter = {
        let mut cfg = inner.containerd_config.lock().unwrap();
        if cfg.generation == generation {
            cfg.proven_ready = true;
        }
        if cfg
            .waiter
            .as_ref()
            .is_some_and(|w| w.target_generation <= generation)
        {
            cfg.waiter.take()
        } else {
            None
        }
    };
    if let Some(w) = waiter {
        let _ = w.tx.send(Ok(()));
    }
}

fn notify_containerd_waiter(inner: &Inner, generation: u64, result: Result<(), String>) {
    let waiter = {
        let mut cfg = inner.containerd_config.lock().unwrap();
        if cfg
            .waiter
            .as_ref()
            .is_some_and(|w| w.target_generation <= generation)
        {
            cfg.waiter.take()
        } else {
            None
        }
    };
    if let Some(w) = waiter {
        let _ = w.tx.send(result);
    }
}

fn fail_containerd_waiter(inner: &Inner, reason: String) {
    let waiter = inner.containerd_config.lock().unwrap().waiter.take();
    if let Some(w) = waiter {
        let _ = w.tx.send(Err(reason));
    }
}

/// Roll back a failed newly activated runtime override config to the stock
/// `/etc/containerd/config.toml` if no newer config change has superseded
/// `failed_generation` and this override has not already been proven ready.
fn rollback_containerd_config(inner: &Inner, failed_generation: u64) -> Option<String> {
    let mut cfg = inner.containerd_config.lock().unwrap();
    if cfg.generation == failed_generation && cfg.path.is_some() && !cfg.proven_ready {
        cfg.generation += 1;
        cfg.proven_ready = false;
        cfg.path.take()
    } else {
        None
    }
}

fn containerd_args(config: Option<&str>) -> Vec<&str> {
    match config {
        Some(path) => vec!["--config", path],
        None => Vec::new(),
    }
}

struct ContainerdRun {
    used_override: bool,
    generation: u64,
}

enum StartOutcome {
    Ready,
    Stopping,
    Failed { error: String, spawned: bool },
}

/// Returns `true` if an unproven override config was rolled back to default.
fn handle_containerd_failure(
    inner: &Inner,
    notifier: &Notifier,
    run: ContainerdRun,
    error: String,
    spawned: bool,
) -> bool {
    let fell_back = if run.used_override {
        if let Some(override_path) = rollback_containerd_config(inner, run.generation) {
            let _ = std::fs::remove_file(&override_path);
            if spawned {
                inner.reloading_containerd.store(true, Ordering::SeqCst);
            }
            true
        } else {
            false
        }
    } else {
        false
    };
    if fell_back {
        notifier.failed(
            NAMES[CONTAINERD],
            format!("{error}; falling back to default config"),
        );
    } else if spawned && !inner.reloading_containerd.load(Ordering::SeqCst) {
        notifier.failed(NAMES[CONTAINERD], error.clone());
    }
    notify_containerd_waiter(inner, run.generation, Err(error));
    fell_back
}

/// Pick which service is due to start at `now`, in dependency order
/// (`CONTAINERD` before `DOCKERD`). If `DOCKERD` is due while `containerd` is
/// not alive, defer `DOCKERD` into `dockerd_pending` so it starts once
/// `containerd` becomes ready again.
fn pick_due_start(
    retry_at: &mut [Option<Instant>; 2],
    dockerd_pending: &mut bool,
    containerd_alive: bool,
    now: Instant,
) -> Option<usize> {
    if retry_at[CONTAINERD].is_some_and(|t| t <= now) {
        retry_at[CONTAINERD] = None;
        return Some(CONTAINERD);
    }
    if retry_at[DOCKERD].is_some_and(|t| t <= now) {
        retry_at[DOCKERD] = None;
        if containerd_alive {
            return Some(DOCKERD);
        }
        *dockerd_pending = true;
    }
    None
}

fn supervise(reaper: &Reaper, notifier: &Notifier, inner: &Inner) {
    let _exit_guard = SuperviseExit(inner);
    let (exits, exited) = mpsc::channel();
    let mut backoff = [backoff(), backoff()];
    let mut retry_at: [Option<Instant>; 2] = [Some(Instant::now()), None];
    let mut dockerd_pending = true;

    let handle_exit =
        |i: usize, status: i32, backoff: &mut [Backoff; 2], retry_at: &mut [Option<Instant>; 2]| {
            if i == CONTAINERD && inner.reloading_containerd.swap(false, Ordering::SeqCst) {
                retry_at[CONTAINERD] = Some(Instant::now());
                return;
            }
            let ran_for = inner.services.lock().unwrap()[i]
                .as_ref()
                .map_or(Duration::ZERO, |s| s.started.elapsed());
            let delay = backoff[i].next(ran_for);
            let name = NAMES[i];
            if backoff[i].failures() > MAX_RESTARTS {
                notifier.failed(
                    name,
                    format!(
                        "{name} {}; gave up after {MAX_RESTARTS} restarts",
                        describe_status(status)
                    ),
                );
                retry_at[i] = None;
                return;
            }
            notifier.failed(
                name,
                format!(
                    "{name} {}; restarting in {} ms",
                    describe_status(status),
                    delay.as_millis()
                ),
            );
            retry_at[i] = Some(Instant::now() + delay);
        };

    loop {
        if inner.stopping.load(Ordering::SeqCst) {
            return;
        }
        // Drain all pending exit notifications before starting or retrying
        // anything so containerd failures are never starved behind dockerd
        // spawn backoff.
        while let Ok((i, status)) = exited.try_recv() {
            if inner.stopping.load(Ordering::SeqCst) {
                return;
            }
            handle_exit(i, status, &mut backoff, &mut retry_at);
        }

        let containerd_alive = inner.services.lock().unwrap()[CONTAINERD]
            .as_ref()
            .is_some_and(|s| !s.has_exited());
        if let Some(i) = pick_due_start(
            &mut retry_at,
            &mut dockerd_pending,
            containerd_alive,
            Instant::now(),
        ) {
            let (outcome, containerd_run) = start_one(reaper, &exits, inner, i);
            match outcome {
                StartOutcome::Ready => {
                    if let Some(run) = containerd_run {
                        mark_containerd_ready(inner, run.generation);
                        if dockerd_pending {
                            dockerd_pending = false;
                            retry_at[DOCKERD] = Some(Instant::now());
                        }
                    } else if i == DOCKERD {
                        notifier.send(&Msg::ServiceReady {
                            name: "docker".into(),
                            at_ns: sys::boottime_ns(),
                        });
                    }
                }
                StartOutcome::Stopping => return,
                StartOutcome::Failed { error, spawned } => {
                    let fell_back = if let Some(run) = containerd_run {
                        handle_containerd_failure(inner, notifier, run, error.clone(), spawned)
                    } else {
                        if spawned {
                            notifier.failed(NAMES[i], error.clone());
                        }
                        false
                    };
                    if !spawned {
                        if fell_back {
                            retry_at[i] = Some(Instant::now());
                        } else {
                            let delay = backoff[i].next(Duration::ZERO);
                            let name = NAMES[i];
                            if backoff[i].failures() > MAX_RESTARTS {
                                notifier.failed(
                                    name,
                                    format!("{error}; gave up after {MAX_RESTARTS} restarts"),
                                );
                                retry_at[i] = None;
                            } else {
                                notifier.failed(
                                    name,
                                    format!("{error}; restarting in {} ms", delay.as_millis()),
                                );
                                retry_at[i] = Some(Instant::now() + delay);
                            }
                        }
                    }
                }
            }
            continue;
        }

        let wait = match retry_at.into_iter().flatten().min() {
            Some(dl) => dl
                .saturating_duration_since(Instant::now())
                .min(Duration::from_millis(20)),
            None => Duration::from_millis(50),
        };
        match exited.recv_timeout(wait) {
            Ok((i, status)) => {
                if inner.stopping.load(Ordering::SeqCst) {
                    return;
                }
                handle_exit(i, status, &mut backoff, &mut retry_at);
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return,
        }
    }
}

/// Start service `i` and wait until it serves.
fn start_one(
    reaper: &Reaper,
    exits: &Sender<(usize, i32)>,
    inner: &Inner,
    i: usize,
) -> (StartOutcome, Option<ContainerdRun>) {
    let dockerd_args = super::net::dockerd_args();
    let mut services = inner.services.lock().unwrap();
    // Checked under the lock, so stop() never misses a service.
    if inner.stopping.load(Ordering::SeqCst) {
        return (StartOutcome::Stopping, None);
    }
    // Read containerd_config under `services` lock so a concurrent
    // reload_containerd_config cannot miss a spawn that picked up a stale config.
    let (containerd_cfg_path, containerd_run) = if i == CONTAINERD {
        let cfg = inner.containerd_config.lock().unwrap();
        let path = cfg.path.clone();
        let run = ContainerdRun {
            used_override: path.is_some(),
            generation: cfg.generation,
        };
        (path, Some(run))
    } else {
        (None, None)
    };
    let (program, args): (&str, Vec<&str>) = match i {
        CONTAINERD => (
            "/usr/bin/containerd",
            containerd_args(containerd_cfg_path.as_deref()),
        ),
        _ => (
            "/usr/bin/dockerd",
            dockerd_args.iter().map(String::as_str).collect(),
        ),
    };
    let service = match Service::spawn(reaper, exits, i, program, &args) {
        Ok(s) => s,
        Err(e) => {
            return (
                StartOutcome::Failed {
                    error: format!("start {}: {e}", NAMES[i]),
                    spawned: false,
                },
                containerd_run,
            );
        }
    };
    services[i] = Some(service.clone());
    drop(services);

    let ready = match i {
        CONTAINERD => wait_ready(NAMES[i], &service.exit, || {
            UnixStream::connect(CONTAINERD_SOCKET).is_ok()
        }),
        _ => wait_ready(NAMES[i], &service.exit, docker_ping),
    };
    let outcome = match ready {
        Ok(()) => StartOutcome::Ready,
        Err(error) => {
            if !service.has_exited() {
                reaper.signal_target(&service.target, libc::SIGKILL);
            }
            StartOutcome::Failed {
                error,
                spawned: true,
            }
        }
    };
    (outcome, containerd_run)
}

/// `GET /_ping` over the Docker socket.
fn docker_ping() -> bool {
    let Ok(mut s) = UnixStream::connect(DOCKER_SOCKET) else {
        return false;
    };
    let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
    if s.write_all(b"GET /_ping HTTP/1.1\r\nHost: docker\r\nConnection: close\r\n\r\n")
        .is_err()
    {
        return false;
    }
    let mut buf = [0u8; 64];
    match s.read(&mut buf) {
        Ok(n) => is_http_ok(&buf[..n]),
        Err(_) => false,
    }
}

fn is_http_ok(resp: &[u8]) -> bool {
    resp.starts_with(b"HTTP/1.1 200") || resp.starts_with(b"HTTP/1.0 200")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognises_http_ok() {
        assert!(is_http_ok(b"HTTP/1.1 200 OK\r\n"));
        assert!(!is_http_ok(b"HTTP/1.1 500 Internal"));
        assert!(!is_http_ok(b""));
    }

    #[test]
    fn describes_wait_status() {
        assert_eq!(describe_status(1 << 8), "exited with status 1");
        assert_eq!(describe_status(9), "killed by signal 9");
    }

    #[test]
    fn due_start_prioritizes_containerd_and_defers_dockerd_when_down() {
        let t0 = Instant::now();
        let mut retry_at = [Some(t0), Some(t0)];
        let mut dockerd_pending = false;
        // Containerd is always picked first when both are due.
        assert_eq!(
            pick_due_start(&mut retry_at, &mut dockerd_pending, false, t0),
            Some(CONTAINERD)
        );
        // Dockerd due while containerd is not alive defers into dockerd_pending.
        assert_eq!(
            pick_due_start(&mut retry_at, &mut dockerd_pending, false, t0),
            None
        );
        assert!(dockerd_pending);
        assert_eq!(retry_at[DOCKERD], None);
        // Once containerd is alive, a due dockerd retry starts immediately.
        retry_at[DOCKERD] = Some(t0);
        assert_eq!(
            pick_due_start(&mut retry_at, &mut dockerd_pending, true, t0),
            Some(DOCKERD)
        );
    }

    #[test]
    fn containerd_args_include_override_only_when_set() {
        assert!(containerd_args(None).is_empty());
        assert_eq!(
            containerd_args(Some("/run/katamaran/containerd-cri.toml")),
            vec!["--config", "/run/katamaran/containerd-cri.toml"]
        );
    }

    #[test]
    fn rollback_clears_matching_generation_only() {
        let inner = Inner::default();
        {
            let mut cfg = inner.containerd_config.lock().unwrap();
            cfg.path = Some("/run/katamaran/containerd-cri.toml".into());
            cfg.generation = 2;
        }
        // Stale generation does not roll back newer config.
        assert_eq!(rollback_containerd_config(&inner, 1), None);
        assert_eq!(
            inner.containerd_config.lock().unwrap().path.as_deref(),
            Some("/run/katamaran/containerd-cri.toml")
        );

        // Matching generation clears path and bumps generation when unproven.
        assert_eq!(
            rollback_containerd_config(&inner, 2).as_deref(),
            Some("/run/katamaran/containerd-cri.toml")
        );
        let cfg = inner.containerd_config.lock().unwrap();
        assert_eq!(cfg.path, None);
        assert_eq!(cfg.generation, 3);
    }

    #[test]
    fn proven_ready_override_is_preserved_after_transient_restart_failure() {
        let inner = Inner::default();
        let (tx, rx) = mpsc::channel();
        {
            let mut cfg = inner.containerd_config.lock().unwrap();
            cfg.path = Some("/run/katamaran/containerd-cri.toml".into());
            cfg.generation = 4;
            cfg.proven_ready = false;
            cfg.waiter = Some(ReloadWaiter {
                target_generation: 4,
                tx,
            });
        }

        // Initial activation reaches Ready: waiter succeeds and override is proven.
        mark_containerd_ready(&inner, 4);
        assert_eq!(rx.try_recv().unwrap(), Ok(()));
        assert!(inner.containerd_config.lock().unwrap().proven_ready);

        // A subsequent transient restart failure must NOT discard the CRI override.
        assert_eq!(rollback_containerd_config(&inner, 4), None);
        let cfg = inner.containerd_config.lock().unwrap();
        assert_eq!(
            cfg.path.as_deref(),
            Some("/run/katamaran/containerd-cri.toml")
        );
        assert_eq!(cfg.generation, 4);
        assert!(cfg.proven_ready);
    }

    #[test]
    fn waiter_ignores_stale_generation_and_delivers_target() {
        let inner = Inner::default();
        let (tx, rx) = mpsc::channel();
        {
            let mut cfg = inner.containerd_config.lock().unwrap();
            cfg.generation = 5;
            cfg.waiter = Some(ReloadWaiter {
                target_generation: 5,
                tx,
            });
        }
        notify_containerd_waiter(&inner, 4, Err("stale".into()));
        assert!(rx.try_recv().is_err());

        notify_containerd_waiter(&inner, 5, Ok(()));
        assert_eq!(rx.try_recv().unwrap(), Ok(()));
    }

    #[test]
    fn watch_table_token_rejects_recycled_pid() {
        let mut table = WatchTable::default();
        let (tok1, _rx1) = table.register(4242);
        assert!(table.matches(4242, tok1));

        // Simulate reaping PID 4242 and reusing the same PID for a new child.
        table.children.remove(&4242);
        assert!(!table.matches(4242, tok1));
        let (tok2, _rx2) = table.register(4242);
        assert_ne!(tok1, tok2);
        assert!(!table.matches(4242, tok1));
        assert!(table.matches(4242, tok2));
    }

    fn open_dir_fd(path: &str) -> OwnedFd {
        let c = CString::new(path).unwrap();
        let fd = unsafe {
            libc::open(
                c.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
            )
        };
        assert!(fd >= 0);
        unsafe { std::os::fd::FromRawFd::from_raw_fd(fd) }
    }

    fn open_devnull_fd() -> OwnedFd {
        let fd = unsafe { libc::open(c"/dev/null".as_ptr(), libc::O_RDWR | libc::O_CLOEXEC) };
        assert!(fd >= 0);
        unsafe { std::os::fd::FromRawFd::from_raw_fd(fd) }
    }

    #[test]
    fn spawn_unlocked_closes_unrelated_fds_before_gate_avoiding_deadlock() {
        let reaper = Reaper::start();
        let (at_gate_tx, at_gate_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        // Keep an extra O_CLOEXEC pipe open in the parent to verify the
        // unlocked child does NOT hold it open while waiting at the gate.
        let (extra_r, extra_w) = cmd::pipe().unwrap();
        let (out_r, out_w) = cmd::pipe().unwrap();

        let r2 = reaper.clone();
        let unlocked = thread::spawn(move || {
            let spec = UnlockedSpawnSpec {
                program: "/bin/sh".into(),
                args: vec!["-c".into(), "echo ok".into()],
                env: vec![("PATH".into(), PATH.into())],
                stdin: open_devnull_fd(),
                stdout: out_w,
                stderr: open_devnull_fd(),
                cwd_fd: open_dir_fd("/"),
                cgroup_procs_fd: None,
                pty: false,
            };
            r2.spawn_unlocked_inner(
                spec,
                Some(Box::new(move || {
                    let _ = at_gate_tx.send(());
                    let _ = release_rx.recv_timeout(Duration::from_secs(5));
                })),
            )
        });

        // Child has forked and is parked at `go_r`; parent has NOT locked
        // `reaper.watched` yet.
        at_gate_rx.recv_timeout(Duration::from_secs(2)).unwrap();

        // Dropping `extra_w` in the parent must immediately yield EOF on
        // `extra_r` because the unlocked child closed unrelated fds before `go_r`.
        drop(extra_w);
        let mut pfd = libc::pollfd {
            fd: extra_r.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        assert!(
            unsafe { libc::poll(&mut pfd, 1, 1000) } > 0,
            "unlocked child retained an unrelated inherited fd while paused at gate"
        );

        // Meanwhile, a concurrent `reaper.spawn` (holding `self.watched`) must
        // complete cleanly without deadlocking against the paused unlocked child.
        let (_, rx_other) = reaper.spawn(&mut Command::new("/bin/true")).unwrap();
        let status_other = rx_other.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(status_other, 0);

        let _ = release_tx.send(());
        let (_, _, rx_unlocked) = unlocked.join().unwrap().unwrap();
        let status_unlocked = rx_unlocked.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(status_unlocked, 0);
        drop(out_r);
    }

    #[test]
    fn spawn_unlocked_exec_failure_reaps_without_panicking_and_reaper_continues() {
        let reaper = Reaper::start();
        for _ in 0..8 {
            let spec = UnlockedSpawnSpec {
                program: "/no/such/binary-kat-test".into(),
                args: vec![],
                env: vec![("PATH".into(), PATH.into())],
                stdin: open_devnull_fd(),
                stdout: open_devnull_fd(),
                stderr: open_devnull_fd(),
                cwd_fd: open_dir_fd("/"),
                cgroup_procs_fd: None,
                pty: false,
            };
            let err = reaper.spawn_unlocked(spec).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::NotFound, "{err}");
        }
        // Reaper continues handling ordinary spawns cleanly after failed execs.
        let (_, rx) = reaper.spawn(&mut Command::new("/bin/true")).unwrap();
        assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), 0);
    }
}
