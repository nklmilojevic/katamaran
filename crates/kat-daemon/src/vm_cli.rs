//! `kat vm shell` and `kat vm exec`: root shell and command execution inside
//! the Linux VM's own namespaces (not an LXC machine), relayed over
//! `<home>/run/vm-exec.sock` (see [`kat_proto::vm_exec`]).

#[cfg(test)]
use std::io::Write;
use std::io::{self, IsTerminal};
use std::net::Shutdown;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use kat_proto::vm_exec::{self, CHUNK, ExecSpec, Tty, VmExecMsg};

use crate::api::{Call, Status};
use crate::state::VmState;
use crate::vm::DeadlineStream;

pub const SOCKET_NAME: &str = "vm-exec.sock";
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const PROBE_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(2);
const VM_START_TIMEOUT: Duration = Duration::from_secs(180);
const HANDSHAKE_RETRY: Duration = Duration::from_millis(50);
/// Maximum time a non-PTY `SIGINT` waits for the guest to acknowledge receipt
/// (`SignalAck`) before falling back to disconnecting the session.
const SIGINT_DELIVERY_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(clap::Subcommand)]
pub enum VmCmd {
    /// Open an interactive root shell in the VM's own namespaces.
    Shell(VmShellArgs),
    /// Run a command as root in the VM's own namespaces.
    Exec(VmExecArgs),
}

#[derive(clap::Args)]
pub struct VmShellArgs {
    /// Working directory inside the VM (default: /root).
    #[arg(short = 'w', long)]
    pub workdir: Option<String>,
    /// Extra environment variables (`KEY=VALUE`, or `KEY` to copy from the host).
    #[arg(short = 'e', long = "env", value_name = "KEY=VALUE")]
    pub env: Vec<String>,
    /// Run this command instead of an interactive shell.
    #[arg(last = true)]
    pub cmd: Vec<String>,
}

#[derive(clap::Args)]
pub struct VmExecArgs {
    /// Allocate a pseudo-terminal.
    #[arg(short = 't', long)]
    pub tty: bool,
    /// Working directory inside the VM (default: /root).
    #[arg(short = 'w', long)]
    pub workdir: Option<String>,
    /// Extra environment variables (`KEY=VALUE`, or `KEY` to copy from the host).
    #[arg(short = 'e', long = "env", value_name = "KEY=VALUE")]
    pub env: Vec<String>,
    /// Command and arguments to run in the VM.
    #[arg(required = true, trailing_var_arg = true, allow_hyphen_values = true)]
    pub cmd: Vec<String>,
}

pub fn socket_path(home: &Path) -> PathBuf {
    home.join("run").join(SOCKET_NAME)
}

pub fn run(cmd: VmCmd) -> anyhow::Result<()> {
    match cmd {
        VmCmd::Shell(args) => shell(args),
        VmCmd::Exec(args) => exec_cmd(args),
    }
}

fn shell(args: VmShellArgs) -> anyhow::Result<()> {
    let allocate_pty = io::stdin().is_terminal() && io::stdout().is_terminal();
    run_spec(args.workdir, &args.env, args.cmd, allocate_pty)
}

fn exec_cmd(args: VmExecArgs) -> anyhow::Result<()> {
    run_spec(args.workdir, &args.env, args.cmd, args.tty)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionExit {
    Code(i32),
    Signal(i32),
}

fn run_spec(
    workdir: Option<String>,
    extra_env: &[String],
    argv: Vec<String>,
    allocate_pty: bool,
) -> anyhow::Result<()> {
    let spec = build_spec(workdir, extra_env, argv, allocate_pty)?;
    let initial_tty_size = spec.tty.as_ref().map(|t| (t.rows, t.cols));
    let (conn, session_token, sock_path) = connect_and_start(&spec)?;
    match run_session(conn, session_token, sock_path, initial_tty_size)? {
        SessionExit::Code(code) => std::process::exit(code),
        SessionExit::Signal(sig) => {
            // Raw mode has already been restored; re-raise with default
            // disposition so parent processes observe the signal termination.
            unsafe {
                libc::signal(sig, libc::SIG_DFL);
                libc::raise(sig);
            }
            std::process::exit(128 + sig);
        }
    }
}

fn build_spec(
    workdir: Option<String>,
    extra_env: &[String],
    argv: Vec<String>,
    allocate_pty: bool,
) -> anyhow::Result<ExecSpec> {
    let cwd = workdir.unwrap_or_default();
    if !vm_exec::value_ok(&cwd) {
        bail!("--workdir cannot contain a NUL byte");
    }
    let env = parse_env(extra_env, |k| std::env::var(k).ok())?;
    let tty = allocate_pty.then(|| {
        let (rows, cols) = window_size();
        Tty {
            term: std::env::var("TERM").unwrap_or_else(|_| "xterm-256color".into()),
            rows,
            cols,
        }
    });
    Ok(ExecSpec {
        cwd,
        env,
        argv,
        tty,
    })
}

fn parse_env(
    extra: &[String],
    host_var: impl Fn(&str) -> Option<String>,
) -> anyhow::Result<Vec<(String, String)>> {
    let mut env: Vec<(String, String)> = ["LANG", "LC_ALL", "COLORTERM"]
        .iter()
        .filter_map(|k| Some(((*k).to_string(), host_var(k)?)))
        .collect();
    for item in extra {
        let (key, value) = match item.split_once('=') {
            Some((k, v)) => (k, Some(v.to_string())),
            None => (item.as_str(), host_var(item)),
        };
        if !vm_exec::env_name_ok(key) {
            bail!("invalid --env name {key:?}: variable name must be non-empty and valid");
        }
        if let Some(val) = value {
            if !vm_exec::value_ok(&val) {
                bail!("invalid --env value for {key:?}: value cannot contain a NUL byte");
            }
            if let Some(existing) = env.iter_mut().find(|(k, _)| k == key) {
                existing.1 = val;
            } else {
                env.push((key.to_string(), val));
            }
        }
    }
    Ok(env)
}

/// Restores the terminal mode when dropped.
struct RawMode {
    fd: RawFd,
    saved: libc::termios,
}

impl RawMode {
    fn enter_fd(fd: RawFd) -> io::Result<RawMode> {
        let mut t: libc::termios = unsafe { std::mem::zeroed() };
        if unsafe { libc::tcgetattr(fd, &mut t) } < 0 {
            return Err(io::Error::last_os_error());
        }
        let saved = t;
        unsafe {
            libc::cfmakeraw(&mut t);
            if libc::tcsetattr(fd, libc::TCSANOW, &t) < 0 {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(RawMode { fd, saved })
    }
}

impl Drop for RawMode {
    fn drop(&mut self) {
        unsafe { libc::tcsetattr(self.fd, libc::TCSANOW, &self.saved) };
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HelperFinish {
    Drained,
    BrokenPipe,
    Cancelled,
}

fn dup_ge3(fd: RawFd) -> io::Result<OwnedFd> {
    let raw = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 3) };
    if raw < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(raw) })
}

struct HelperProc {
    pid: Option<libc::pid_t>,
}

impl HelperProc {
    /// Spawn `/bin/cat` via `posix_spawn` with `stdin_fd -> 0`, `stdout_fd -> 1`,
    /// `/dev/null -> 2`, `POSIX_SPAWN_CLOEXEC_DEFAULT` (so no unrelated fd from
    /// any concurrent thread can leak into the helper), `[SIGINT, SIGQUIT,
    /// SIGHUP, SIGTERM]` blocked (preventing spurious terminal-signal EOF while
    /// `SIGKILL` still terminates immediately), and `SIGPIPE` reset to `SIG_DFL`.
    fn spawn_cat(stdin_fd: RawFd, stdout_fd: RawFd) -> io::Result<Self> {
        // Duplicate both source fds to >= 3 so `dup2(..., 0)` and `dup2(..., 1)`
        // can never clobber each other even if fd 0/1/2 was closed by caller.
        let in_dup = dup_ge3(stdin_fd)?;
        let out_dup = dup_ge3(stdout_fd)?;

        struct ActionsGuard(libc::posix_spawn_file_actions_t);
        impl Drop for ActionsGuard {
            fn drop(&mut self) {
                unsafe { libc::posix_spawn_file_actions_destroy(&mut self.0) };
            }
        }
        struct AttrGuard(libc::posix_spawnattr_t);
        impl Drop for AttrGuard {
            fn drop(&mut self) {
                unsafe { libc::posix_spawnattr_destroy(&mut self.0) };
            }
        }

        let mut actions_raw: libc::posix_spawn_file_actions_t = unsafe { std::mem::zeroed() };
        let rc = unsafe { libc::posix_spawn_file_actions_init(&mut actions_raw) };
        if rc != 0 {
            return Err(io::Error::from_raw_os_error(rc));
        }
        let mut actions = ActionsGuard(actions_raw);

        for rc in [
            unsafe {
                libc::posix_spawn_file_actions_adddup2(&mut actions.0, in_dup.as_raw_fd(), 0)
            },
            unsafe {
                libc::posix_spawn_file_actions_adddup2(&mut actions.0, out_dup.as_raw_fd(), 1)
            },
            unsafe {
                libc::posix_spawn_file_actions_addopen(
                    &mut actions.0,
                    2,
                    c"/dev/null".as_ptr(),
                    libc::O_WRONLY,
                    0,
                )
            },
        ] {
            if rc != 0 {
                return Err(io::Error::from_raw_os_error(rc));
            }
        }

        let mut attr_raw: libc::posix_spawnattr_t = unsafe { std::mem::zeroed() };
        let rc = unsafe { libc::posix_spawnattr_init(&mut attr_raw) };
        if rc != 0 {
            return Err(io::Error::from_raw_os_error(rc));
        }
        let mut attr = AttrGuard(attr_raw);

        let mut sigmask: libc::sigset_t = unsafe { std::mem::zeroed() };
        let mut sigdef: libc::sigset_t = unsafe { std::mem::zeroed() };
        unsafe {
            libc::sigemptyset(&mut sigmask);
            for sig in [libc::SIGINT, libc::SIGQUIT, libc::SIGHUP, libc::SIGTERM] {
                libc::sigaddset(&mut sigmask, sig);
            }
            libc::sigemptyset(&mut sigdef);
            libc::sigaddset(&mut sigdef, libc::SIGPIPE);
        }
        let mut flags: libc::c_short =
            (libc::POSIX_SPAWN_SETSIGMASK | libc::POSIX_SPAWN_SETSIGDEF) as libc::c_short;
        #[cfg(target_os = "macos")]
        {
            flags |= libc::POSIX_SPAWN_CLOEXEC_DEFAULT as libc::c_short;
        }
        for rc in [
            unsafe { libc::posix_spawnattr_setsigmask(&mut attr.0, &sigmask) },
            unsafe { libc::posix_spawnattr_setsigdefault(&mut attr.0, &sigdef) },
            unsafe { libc::posix_spawnattr_setflags(&mut attr.0, flags) },
        ] {
            if rc != 0 {
                return Err(io::Error::from_raw_os_error(rc));
            }
        }

        let mut pid: libc::pid_t = 0;
        let mut argv = [c"/bin/cat".as_ptr().cast_mut(), std::ptr::null_mut()];
        let mut envp: [*mut libc::c_char; 1] = [std::ptr::null_mut()];
        let rc = unsafe {
            libc::posix_spawn(
                &mut pid,
                c"/bin/cat".as_ptr(),
                &actions.0,
                &attr.0,
                argv.as_mut_ptr(),
                envp.as_mut_ptr(),
            )
        };
        if rc != 0 {
            return Err(io::Error::from_raw_os_error(rc));
        }
        Ok(Self { pid: Some(pid) })
    }

    fn try_wait(&mut self) -> io::Result<Option<libc::c_int>> {
        let Some(pid) = self.pid else {
            return Ok(Some(0));
        };
        let mut status: libc::c_int = 0;
        loop {
            let rc = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
            if rc == 0 {
                return Ok(None);
            }
            if rc > 0 {
                self.pid = None;
                return Ok(Some(status));
            }
            let e = io::Error::last_os_error();
            if e.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            self.pid = None;
            return Err(e);
        }
    }

    fn kill_and_wait(&mut self) {
        let Some(pid) = self.pid.take() else {
            return;
        };
        unsafe {
            let _ = libc::kill(pid, libc::SIGKILL);
            let mut status: libc::c_int = 0;
            while libc::waitpid(pid, &mut status, 0) < 0 {
                if io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
                    break;
                }
            }
        }
    }
}

impl Drop for HelperProc {
    fn drop(&mut self) {
        self.kill_and_wait();
    }
}

/// Relays output written to a private nonblocking pipe into an inherited
/// blocking output fd via a dedicated `/bin/cat` helper process so
/// `run_session_io` NEVER sets `O_NONBLOCK` on any inherited descriptor (even
/// when `stdin` and `stdout`/`stderr` share the same open file description,
/// such as an interactive TTY or full-duplex socketpair), yet output writes
/// remain immediately cancellable on termination signals.
struct OutputHelper {
    proc: HelperProc,
    write_end: Option<OwnedFd>,
}

impl OutputHelper {
    fn maybe_spawn(out_fd: RawFd) -> io::Result<Option<Self>> {
        if out_fd < 0 {
            return Ok(None);
        }
        let fl = unsafe { libc::fcntl(out_fd, libc::F_GETFL) };
        if fl < 0 || (fl & libc::O_NONBLOCK) != 0 {
            return Ok(None);
        }
        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        if unsafe { libc::fstat(out_fd, &mut st) } == 0
            && (st.st_mode & libc::S_IFMT) == libc::S_IFREG
        {
            // Regular file writes do not block on pipes/sockets; no helper needed.
            return Ok(None);
        }
        let (read_end, write_end) = pipe_cloexec(false)?;
        unsafe {
            let wfl = libc::fcntl(write_end.as_raw_fd(), libc::F_GETFL);
            if wfl < 0
                || libc::fcntl(write_end.as_raw_fd(), libc::F_SETFL, wfl | libc::O_NONBLOCK) < 0
            {
                return Err(io::Error::last_os_error());
            }
        }
        let proc = HelperProc::spawn_cat(read_end.as_raw_fd(), out_fd)?;
        drop(read_end);
        Ok(Some(Self {
            proc,
            write_end: Some(write_end),
        }))
    }

    fn as_raw_fd(&self) -> RawFd {
        self.write_end
            .as_ref()
            .expect("OutputHelper already closed")
            .as_raw_fd()
    }

    /// Close `write_end` and wait for `/bin/cat` to flush buffered output to
    /// the inherited descriptor, aborting immediately if `term_wake_r` fires.
    fn finish(&mut self, term_wake_r: RawFd) -> HelperFinish {
        self.write_end.take();
        let mut wait_ms = 1i32;
        loop {
            match self.proc.try_wait() {
                Ok(Some(status)) => {
                    return if libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0 {
                        HelperFinish::Drained
                    } else {
                        HelperFinish::BrokenPipe
                    };
                }
                Ok(None) => {}
                Err(_) => return HelperFinish::Drained,
            }
            let mut pfd = libc::pollfd {
                fd: term_wake_r,
                events: libc::POLLIN,
                revents: 0,
            };
            let rc = unsafe { libc::poll(&mut pfd, 1, wait_ms) };
            if rc > 0 && pfd.revents != 0 {
                self.proc.kill_and_wait();
                return HelperFinish::Cancelled;
            }
            wait_ms = (wait_ms * 2).min(20);
        }
    }

    fn abort(&mut self) {
        self.write_end.take();
        self.proc.kill_and_wait();
    }
}

impl Drop for OutputHelper {
    fn drop(&mut self) {
        self.abort();
    }
}

/// Relays an inherited blocking `stdin_fd` into a private nonblocking pipe via
/// a dedicated `/bin/cat` helper process so `run_session_io` never modifies
/// `stdin_fd`'s shared open file description (`O_NONBLOCK`), yet teardown can
/// still terminate and reap a blocked reader promptly even if another process
/// drained the last ready byte.
struct StdinHelper {
    _proc: HelperProc,
    read_end: OwnedFd,
}

impl StdinHelper {
    fn maybe_spawn(stdin_fd: RawFd) -> io::Result<Option<Self>> {
        if stdin_fd < 0 {
            return Ok(None);
        }
        let fl = unsafe { libc::fcntl(stdin_fd, libc::F_GETFL) };
        if fl < 0 || (fl & libc::O_NONBLOCK) != 0 {
            return Ok(None);
        }
        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        if unsafe { libc::fstat(stdin_fd, &mut st) } == 0
            && (st.st_mode & libc::S_IFMT) == libc::S_IFREG
        {
            // Regular file reads never block; no helper needed.
            return Ok(None);
        }
        let (read_end, write_end) = pipe_cloexec(false)?;
        unsafe {
            let rfl = libc::fcntl(read_end.as_raw_fd(), libc::F_GETFL);
            if rfl < 0
                || libc::fcntl(read_end.as_raw_fd(), libc::F_SETFL, rfl | libc::O_NONBLOCK) < 0
            {
                return Err(io::Error::last_os_error());
            }
        }
        let proc = HelperProc::spawn_cat(stdin_fd, write_end.as_raw_fd())?;
        drop(write_end);
        Ok(Some(Self {
            _proc: proc,
            read_end,
        }))
    }

    fn as_raw_fd(&self) -> RawFd {
        self.read_end.as_raw_fd()
    }
}

fn pipe_cloexec(nonblock_both: bool) -> io::Result<(OwnedFd, OwnedFd)> {
    let mut fds = [0; 2];
    if unsafe { libc::pipe(fds.as_mut_ptr()) } < 0 {
        return Err(io::Error::last_os_error());
    }
    let r = unsafe { OwnedFd::from_raw_fd(fds[0]) };
    let w = unsafe { OwnedFd::from_raw_fd(fds[1]) };
    unsafe {
        libc::fcntl(r.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC);
        libc::fcntl(w.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC);
        if nonblock_both {
            for fd in [r.as_raw_fd(), w.as_raw_fd()] {
                let fl = libc::fcntl(fd, libc::F_GETFL);
                if fl >= 0 {
                    libc::fcntl(fd, libc::F_SETFL, fl | libc::O_NONBLOCK);
                }
            }
        }
    }
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

fn window_size() -> (u16, u16) {
    window_size_fds([1, 0])
}

fn window_size_fds(fds: [RawFd; 2]) -> (u16, u16) {
    for fd in fds {
        let mut ws: libc::winsize = unsafe { std::mem::zeroed() };
        if unsafe { libc::ioctl(fd, libc::TIOCGWINSZ, &mut ws) } == 0
            && ws.ws_row > 0
            && ws.ws_col > 0
        {
            return (ws.ws_row, ws.ws_col);
        }
    }
    (24, 80)
}

fn is_tty(fd: RawFd) -> bool {
    fd >= 0 && unsafe { libc::isatty(fd) == 1 }
}

/// Self-pipe write fd used by the async-signal-safe `sigaction` handler.
static SIGNAL_PIPE_FD: AtomicI32 = AtomicI32::new(-1);

extern "C" fn signal_handler(sig: libc::c_int) {
    let fd = SIGNAL_PIPE_FD.load(Ordering::Relaxed);
    if fd >= 0 {
        let b = sig as u8;
        unsafe {
            let _ = libc::write(fd, (&b as *const u8).cast(), 1);
        }
    }
}

struct SignalGuard {
    saved: Vec<(libc::c_int, libc::sigaction)>,
    sig_w: OwnedFd,
    dispatcher: Option<thread::JoinHandle<()>>,
}

impl SignalGuard {
    fn install(signals: &[libc::c_int]) -> io::Result<(Self, OwnedFd)> {
        let (sig_r, sig_w) = pipe_cloexec(false)?;
        unsafe {
            let fl = libc::fcntl(sig_w.as_raw_fd(), libc::F_GETFL);
            if fl >= 0 {
                libc::fcntl(sig_w.as_raw_fd(), libc::F_SETFL, fl | libc::O_NONBLOCK);
            }
        }

        SIGNAL_PIPE_FD.store(sig_w.as_raw_fd(), Ordering::Release);
        let mut guard = SignalGuard {
            saved: Vec::with_capacity(signals.len()),
            sig_w,
            dispatcher: None,
        };

        for &sig in signals {
            let mut old: libc::sigaction = unsafe { std::mem::zeroed() };
            if unsafe { libc::sigaction(sig, std::ptr::null(), &mut old) } < 0 {
                return Err(io::Error::last_os_error());
            }
            // Respect explicitly ignored signals (e.g., SIGINT in background jobs).
            if old.sa_sigaction == libc::SIG_IGN {
                continue;
            }
            let mut act: libc::sigaction = unsafe { std::mem::zeroed() };
            act.sa_sigaction = signal_handler as *const () as usize;
            act.sa_flags = libc::SA_RESTART;
            unsafe { libc::sigemptyset(&mut act.sa_mask) };
            if unsafe { libc::sigaction(sig, &act, std::ptr::null_mut()) } < 0 {
                return Err(io::Error::last_os_error());
            }
            guard.saved.push((sig, old));
        }
        Ok((guard, sig_r))
    }

    fn attach_dispatcher(&mut self, handle: thread::JoinHandle<()>) {
        self.dispatcher = Some(handle);
    }
}

impl Drop for SignalGuard {
    fn drop(&mut self) {
        for &(sig, ref old) in &self.saved {
            unsafe {
                let _ = libc::sigaction(sig, old, std::ptr::null_mut());
            }
        }
        SIGNAL_PIPE_FD.store(-1, Ordering::Release);
        // Wake and join the dispatcher thread before closing `sig_w`.
        let stop: u8 = 0;
        unsafe {
            let _ = libc::write(self.sig_w.as_raw_fd(), (&stop as *const u8).cast(), 1);
        }
        if let Some(h) = self.dispatcher.take() {
            let _ = h.join();
        }
    }
}

fn fd_ready(fd: RawFd) -> bool {
    let mut pfd = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    let rc = unsafe { libc::poll(&mut pfd, 1, 0) };
    rc > 0 && pfd.revents != 0
}

/// Connect to a Unix domain stream socket while honoring an absolute `deadline`.
fn connect_with_deadline(path: &Path, deadline: Instant) -> io::Result<UnixStream> {
    connect_with_deadline_cancellable(path, deadline, None)
}

/// Connect to a Unix domain stream socket while honoring an absolute `deadline`
/// and waking immediately if `stop_r` becomes readable/closed.
fn connect_with_deadline_cancellable(
    path: &Path,
    deadline: Instant,
    stop_r: Option<RawFd>,
) -> io::Result<UnixStream> {
    if stop_r.is_some_and(fd_ready) {
        return Err(io::Error::new(
            io::ErrorKind::Interrupted,
            "VM exec connect cancelled",
        ));
    }
    if deadline.saturating_duration_since(Instant::now()).is_zero() {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "VM exec connect timed out",
        ));
    }

    let bytes = path.as_os_str().as_bytes();
    let mut addr: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    if bytes.is_empty() || bytes.contains(&0) || bytes.len() >= addr.sun_path.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid socket path {}", path.display()),
        ));
    }
    addr.sun_family = libc::AF_UNIX as libc::sa_family_t;
    for (dst, &src) in addr.sun_path.iter_mut().zip(bytes) {
        *dst = src as libc::c_char;
    }
    let addr_len =
        (std::mem::offset_of!(libc::sockaddr_un, sun_path) + bytes.len() + 1) as libc::socklen_t;
    #[cfg(target_os = "macos")]
    {
        addr.sun_len = addr_len as u8;
    }

    let raw = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
    if raw < 0 {
        return Err(io::Error::last_os_error());
    }
    let fd = unsafe { OwnedFd::from_raw_fd(raw) };
    unsafe {
        if libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) < 0 {
            return Err(io::Error::last_os_error());
        }
        #[cfg(target_os = "macos")]
        {
            let one: libc::c_int = 1;
            let _ = libc::setsockopt(
                fd.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_NOSIGPIPE,
                (&one as *const libc::c_int).cast(),
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
        let fl = libc::fcntl(fd.as_raw_fd(), libc::F_GETFL);
        if fl < 0 || libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, fl | libc::O_NONBLOCK) < 0 {
            return Err(io::Error::last_os_error());
        }
    }

    let rc = unsafe {
        libc::connect(
            fd.as_raw_fd(),
            (&addr as *const libc::sockaddr_un).cast::<libc::sockaddr>(),
            addr_len,
        )
    };
    if rc < 0 {
        let e = io::Error::last_os_error();
        if e.raw_os_error() == Some(libc::EINPROGRESS) || e.kind() == io::ErrorKind::WouldBlock {
            loop {
                if stop_r.is_some_and(fd_ready) {
                    return Err(io::Error::new(
                        io::ErrorKind::Interrupted,
                        "VM exec connect cancelled",
                    ));
                }
                let rem = deadline.saturating_duration_since(Instant::now());
                if rem.is_zero() {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "VM exec connect timed out",
                    ));
                }
                let mut pfds = [
                    libc::pollfd {
                        fd: fd.as_raw_fd(),
                        events: libc::POLLOUT,
                        revents: 0,
                    },
                    libc::pollfd {
                        fd: stop_r.unwrap_or(-1),
                        events: libc::POLLIN,
                        revents: 0,
                    },
                ];
                let nfds = if stop_r.is_some() { 2 } else { 1 };
                let timeout_ms = rem.as_millis().clamp(1, i32::MAX as u128) as i32;
                let prc = unsafe { libc::poll(pfds.as_mut_ptr(), nfds, timeout_ms) };
                if prc < 0 {
                    let pe = io::Error::last_os_error();
                    if pe.kind() == io::ErrorKind::Interrupted {
                        continue;
                    }
                    return Err(pe);
                }
                if prc == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "VM exec connect timed out",
                    ));
                }
                if nfds == 2 && pfds[1].revents != 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::Interrupted,
                        "VM exec connect cancelled",
                    ));
                }
                let mut so_err: libc::c_int = 0;
                let mut err_len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
                if unsafe {
                    libc::getsockopt(
                        fd.as_raw_fd(),
                        libc::SOL_SOCKET,
                        libc::SO_ERROR,
                        (&mut so_err as *mut libc::c_int).cast(),
                        &mut err_len,
                    )
                } < 0
                {
                    return Err(io::Error::last_os_error());
                }
                if so_err != 0 {
                    return Err(io::Error::from_raw_os_error(so_err));
                }
                break;
            }
        } else {
            return Err(e);
        }
    }

    unsafe {
        let fl = libc::fcntl(fd.as_raw_fd(), libc::F_GETFL);
        if fl < 0 || libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, fl & !libc::O_NONBLOCK) < 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(UnixStream::from(fd))
}

enum Probe {
    Ready(UnixStream),
    NoSocket(io::Error),
    ClosedBeforeReady,
    TimedOut(io::Error),
    Failed(anyhow::Error),
}

/// Perform a side-effect-free readiness probe (`Ping` -> `Pong`) within
/// `deadline`, leaving the connection open to submit `Exec` once ready.
fn probe_ready(path: &Path, deadline: Instant) -> Probe {
    let mut conn = match connect_with_deadline(path, deadline) {
        Ok(s) => s,
        Err(e) if e.kind() == io::ErrorKind::TimedOut => return Probe::TimedOut(e),
        Err(e) => return Probe::NoSocket(e),
    };
    let reply = {
        let mut ds = DeadlineStream::new(
            &mut conn,
            deadline,
            "VM exec readiness probe timed out".into(),
        );
        if let Err(e) = vm_exec::write(&mut ds, &VmExecMsg::Ping) {
            if matches!(
                e.kind(),
                io::ErrorKind::BrokenPipe
                    | io::ErrorKind::ConnectionReset
                    | io::ErrorKind::ConnectionAborted
                    | io::ErrorKind::NotConnected
            ) {
                return Probe::ClosedBeforeReady;
            }
            if matches!(
                e.kind(),
                io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
            ) {
                return Probe::TimedOut(e);
            }
            return Probe::Failed(e.into());
        }
        vm_exec::read(&mut ds)
    };
    match reply {
        Ok(Some(VmExecMsg::Pong)) => Probe::Ready(conn),
        Ok(Some(VmExecMsg::Error(e))) => Probe::Failed(anyhow::anyhow!("{e}")),
        Ok(Some(other)) => Probe::Failed(anyhow::anyhow!("unexpected guest response {other:?}")),
        Ok(None) => Probe::ClosedBeforeReady,
        Err(e)
            if matches!(
                e.kind(),
                io::ErrorKind::ConnectionReset
                    | io::ErrorKind::ConnectionAborted
                    | io::ErrorKind::NotConnected
                    | io::ErrorKind::BrokenPipe
                    | io::ErrorKind::UnexpectedEof
            ) =>
        {
            Probe::ClosedBeforeReady
        }
        Err(e)
            if matches!(
                e.kind(),
                io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
            ) =>
        {
            Probe::TimedOut(e)
        }
        Err(e) => Probe::Failed(e.into()),
    }
}

/// Submit `spec` ONCE on an already-ready connection and wait for `Started`
/// within `deadline`. Never retries: once `Exec` may have reached the guest,
/// any disconnect or timeout surfaces an uncertain-outcome error instead of
/// risking replaying a side-effecting root command.
fn submit_exec(
    mut conn: UnixStream,
    spec: &ExecSpec,
    deadline: Instant,
) -> anyhow::Result<(UnixStream, [u8; 16])> {
    let token = {
        let mut ds = DeadlineStream::new(
            &mut conn,
            deadline,
            "VM exec startup handshake timed out".into(),
        );
        if let Err(e) = vm_exec::write(&mut ds, &VmExecMsg::Exec(spec.clone())) {
            if matches!(
                e.kind(),
                io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
            ) {
                bail!("timed out sending the command to the guest; execution outcome is unknown");
            }
            bail!(
                "lost connection to the guest while sending the command ({e}); execution outcome is unknown"
            );
        }
        match vm_exec::read(&mut ds) {
            Ok(Some(VmExecMsg::Started { token })) => token,
            Ok(Some(VmExecMsg::Error(e))) => bail!("{e}"),
            Ok(Some(other)) => bail!("unexpected guest response {other:?}"),
            Ok(None) => bail!(
                "the guest closed the connection after receiving the command before confirming start; execution outcome is unknown"
            ),
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                ) =>
            {
                bail!(
                    "timed out waiting for the guest to confirm command start; execution outcome is unknown"
                );
            }
            Err(e) => bail!(
                "lost connection to the guest after sending the command ({e}); execution outcome is unknown"
            ),
        }
    };
    // Clear any socket timeouts before entering the untimed interactive session
    // (best-effort if the peer closed immediately after sending `Started`).
    let _ = conn.set_read_timeout(None);
    let _ = conn.set_write_timeout(None);
    Ok((conn, token))
}

/// Poll `path` with side-effect-free readiness probes until `deadline`.
/// Consults `poll_status` only for terminal VM failures or an already-running
/// older VM lacking `vm-exec.sock`, never gating on `DockerState`.
fn wait_for_ready_socket(
    path: &Path,
    deadline: Instant,
    mut poll_status: impl FnMut() -> anyhow::Result<Option<Status>>,
) -> anyhow::Result<UnixStream> {
    let mut last_no_socket: Option<io::Error> = None;
    let mut saw_closed = false;
    loop {
        let now = Instant::now();
        if now >= deadline {
            if let Some(e) = last_no_socket {
                return Err(e).with_context(|| {
                    format!(
                        "cannot connect to the VM shell service ({}); if you just upgraded katamaran, run `kat restart`",
                        path.display()
                    )
                });
            }
            if saw_closed {
                bail!(
                    "the guest closed the VM shell connection before becoming ready; check `kat logs --console` or run `kat restart`"
                );
            }
            bail!(
                "timed out waiting for the VM shell service ({}); check `kat logs --console` or run `kat restart`",
                path.display()
            );
        }

        let attempt_deadline = (now + PROBE_ATTEMPT_TIMEOUT).min(deadline);
        match probe_ready(path, attempt_deadline) {
            Probe::Ready(conn) => return Ok(conn),
            Probe::Failed(e) => return Err(e),
            Probe::NoSocket(e) => {
                last_no_socket = Some(e);
                if let Some(s) = poll_status()? {
                    match s.state {
                        VmState::Error | VmState::Stopped => {
                            bail!("the VM did not start: {}", s.error.unwrap_or_default())
                        }
                        VmState::Running => {
                            // Re-probe once in case the VM finished booting
                            // between our probe and the status check.
                            match probe_ready(
                                path,
                                (Instant::now() + PROBE_ATTEMPT_TIMEOUT).min(deadline),
                            ) {
                                Probe::Ready(conn) => return Ok(conn),
                                Probe::Failed(err) => return Err(err),
                                Probe::NoSocket(_) => bail!(
                                    "VM shell service is not available on the running VM ({}); run `kat restart` to upgrade",
                                    path.display()
                                ),
                                Probe::ClosedBeforeReady => saw_closed = true,
                                Probe::TimedOut(_) => {}
                            }
                        }
                        VmState::Starting | VmState::Stopping => {}
                    }
                }
            }
            Probe::ClosedBeforeReady => {
                saw_closed = true;
                last_no_socket = None;
                if let Some(s) = poll_status()?
                    && matches!(s.state, VmState::Error | VmState::Stopped)
                {
                    bail!("the VM did not start: {}", s.error.unwrap_or_default());
                }
            }
            Probe::TimedOut(_) => {
                if let Some(s) = poll_status()?
                    && matches!(s.state, VmState::Error | VmState::Stopped)
                {
                    bail!("the VM did not start: {}", s.error.unwrap_or_default());
                }
            }
        }

        let rem = deadline.saturating_duration_since(Instant::now());
        if rem.is_zero() {
            continue;
        }
        thread::sleep(HANDSHAKE_RETRY.min(rem));
    }
}

/// Connect to the guest's VM exec service (starting the VM if stopped without
/// waiting for Docker readiness), perform a side-effect-free readiness check,
/// then send `spec` once and wait for `Started`.
fn connect_and_start(spec: &ExecSpec) -> anyhow::Result<(UnixStream, [u8; 16], PathBuf)> {
    let home = crate::state_dir()?;
    let path = socket_path(&home);

    let fast_deadline = Instant::now() + HANDSHAKE_TIMEOUT;
    match probe_ready(&path, fast_deadline) {
        Probe::Ready(conn) => {
            let (conn, token) = submit_exec(conn, spec, Instant::now() + HANDSHAKE_TIMEOUT)?;
            return Ok((conn, token, path));
        }
        Probe::Failed(e) => return Err(e),
        Probe::ClosedBeforeReady => {
            // The VM process bound the Unix socket, but kat-init may still be
            // finishing boot before listening on VM_EXEC_PORT.
            let conn = wait_for_ready_socket(&path, fast_deadline, || Ok(None))?;
            let (conn, token) = submit_exec(conn, spec, Instant::now() + HANDSHAKE_TIMEOUT)?;
            return Ok((conn, token, path));
        }
        Probe::TimedOut(e) => {
            return Err(e).context(
                "timed out waiting for the VM shell service; check `kat logs --console` or run `kat restart`",
            );
        }
        Probe::NoSocket(_) => {}
    }

    // Socket is absent: consult the daemon, start the VM if needed, and poll
    // the exec socket directly (never gating on Docker readiness so debugging
    // a failing containerd/dockerd stack works immediately).
    let mut c = crate::cli::ensure_daemon()?;
    let s = c.status()?;
    match s.state {
        VmState::Running => match probe_ready(&path, Instant::now() + HANDSHAKE_TIMEOUT) {
            Probe::Ready(conn) => {
                let (conn, token) = submit_exec(conn, spec, Instant::now() + HANDSHAKE_TIMEOUT)?;
                return Ok((conn, token, path));
            }
            Probe::Failed(e) => return Err(e),
            Probe::NoSocket(_) => bail!(
                "VM shell service is not available on the running VM ({}); run `kat restart` to upgrade",
                path.display()
            ),
            Probe::ClosedBeforeReady | Probe::TimedOut(_) => {}
        },
        VmState::Stopped | VmState::Error => {
            eprintln!("katamaran: starting the VM");
            c.call(&Call::Start)?;
        }
        VmState::Stopping => {
            c.call(&Call::Start)?;
        }
        VmState::Starting => {}
    }

    let conn = wait_for_ready_socket(&path, Instant::now() + VM_START_TIMEOUT, || {
        c.status().map(Some)
    })?;
    let (conn, token) = submit_exec(conn, spec, Instant::now() + HANDSHAKE_TIMEOUT)?;
    Ok((conn, token, path))
}

enum WriterCtrl {
    Resize { rows: u16, cols: u16 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OobSignalResult {
    Acked,
    Cancelled,
    Failed,
}

/// Deliver a non-PTY signal to the guest session on a dedicated out-of-band
/// control connection (`VmExecMsg::SignalSession` -> `VmExecMsg::SignalAck`),
/// completely decoupled from stdio backpressure and strictly bounded by
/// `deadline`.
fn send_signal_oob(
    connect_ctrl: &(impl Fn(Instant, RawFd) -> io::Result<UnixStream> + ?Sized),
    token: [u8; 16],
    signal: i32,
    deadline: Instant,
    stop_r: RawFd,
) -> OobSignalResult {
    if fd_ready(stop_r) {
        return OobSignalResult::Cancelled;
    }
    let conn = match connect_ctrl(deadline, stop_r) {
        Ok(c) => c,
        Err(_) => {
            return if fd_ready(stop_r) {
                OobSignalResult::Cancelled
            } else {
                OobSignalResult::Failed
            };
        }
    };
    if conn.set_nonblocking(true).is_err() {
        return OobSignalResult::Failed;
    }

    let frame = VmExecMsg::SignalSession { token, signal }.encode();
    let mut off = 0usize;
    while off < frame.len() {
        if fd_ready(stop_r) {
            return OobSignalResult::Cancelled;
        }
        let rc = unsafe {
            libc::send(
                conn.as_raw_fd(),
                frame[off..].as_ptr().cast(),
                frame.len() - off,
                libc::MSG_DONTWAIT,
            )
        };
        if rc > 0 {
            off += rc as usize;
            continue;
        }
        if rc == 0 {
            return OobSignalResult::Failed;
        }
        let e = io::Error::last_os_error();
        match e.kind() {
            io::ErrorKind::Interrupted => continue,
            io::ErrorKind::WouldBlock => {
                let rem = deadline.saturating_duration_since(Instant::now());
                if rem.is_zero() {
                    return OobSignalResult::Failed;
                }
                let mut pfds = [
                    libc::pollfd {
                        fd: conn.as_raw_fd(),
                        events: libc::POLLOUT,
                        revents: 0,
                    },
                    libc::pollfd {
                        fd: stop_r,
                        events: libc::POLLIN,
                        revents: 0,
                    },
                ];
                let timeout_ms = rem.as_millis().clamp(1, i32::MAX as u128) as i32;
                let prc = unsafe { libc::poll(pfds.as_mut_ptr(), 2, timeout_ms) };
                if prc < 0 {
                    if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                        continue;
                    }
                    return OobSignalResult::Failed;
                }
                if prc == 0 {
                    return OobSignalResult::Failed;
                }
                if pfds[1].revents != 0 {
                    return OobSignalResult::Cancelled;
                }
                if (pfds[0].revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL)) != 0
                    && (pfds[0].revents & libc::POLLOUT) == 0
                {
                    return OobSignalResult::Failed;
                }
            }
            _ => return OobSignalResult::Failed,
        }
    }

    let mut frames = FrameReader::default();
    loop {
        if fd_ready(stop_r) {
            return OobSignalResult::Cancelled;
        }
        match frames.poll_frame(conn.as_raw_fd()) {
            Ok(PollFrame::Msg(VmExecMsg::SignalAck)) => return OobSignalResult::Acked,
            Ok(PollFrame::Msg(_)) | Ok(PollFrame::Eof) | Err(_) => return OobSignalResult::Failed,
            Ok(PollFrame::WouldBlock) => {}
        }
        let rem = deadline.saturating_duration_since(Instant::now());
        if rem.is_zero() {
            return OobSignalResult::Failed;
        }
        let mut pfds = [
            libc::pollfd {
                fd: conn.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: stop_r,
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        let timeout_ms = rem.as_millis().clamp(1, i32::MAX as u128) as i32;
        let prc = unsafe { libc::poll(pfds.as_mut_ptr(), 2, timeout_ms) };
        if prc < 0 {
            if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return OobSignalResult::Failed;
        }
        if prc == 0 {
            continue;
        }
        if pfds[1].revents != 0 {
            return OobSignalResult::Cancelled;
        }
    }
}

/// Nonblocking single-owner writer for stdin and PTY resize frames.
///
/// Using a single `poll` + nonblocking `send` loop eliminates any shared socket
/// writer mutex and guarantees `Resize` takes priority over further stdin reads.
fn writer_worker(
    sock: UnixStream,
    stdin_fd: RawFd,
    stop_r: OwnedFd,
    ctrl_wake_r: OwnedFd,
    ctrl_rx: mpsc::Receiver<WriterCtrl>,
) {
    let mut out_buf: Vec<u8> = Vec::new();
    let mut out_off: usize = 0;
    let mut pending_resize: Option<(u16, u16)> = None;
    let mut stdin_open = stdin_fd >= 0;
    let mut stdin_buf = vec![0u8; CHUNK];

    loop {
        // 1. Drain any pending high-priority resize events without blocking.
        while let Ok(ev) = ctrl_rx.try_recv() {
            match ev {
                WriterCtrl::Resize { rows, cols } => pending_resize = Some((rows, cols)),
            }
        }

        // 2. If no frame is mid-flight, pick the next frame in priority order.
        if out_off == out_buf.len() {
            out_buf.clear();
            out_off = 0;
            if let Some((rows, cols)) = pending_resize.take() {
                out_buf = VmExecMsg::Resize { rows, cols }.encode();
            }
        }

        // 3. Flush as much of the active frame as possible without blocking.
        if out_off < out_buf.len() {
            let mut flush_done = false;
            loop {
                let rc = unsafe {
                    libc::send(
                        sock.as_raw_fd(),
                        out_buf[out_off..].as_ptr().cast(),
                        out_buf.len() - out_off,
                        libc::MSG_DONTWAIT,
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
                    return;
                } else {
                    let e = io::Error::last_os_error();
                    match e.kind() {
                        io::ErrorKind::Interrupted => continue,
                        io::ErrorKind::WouldBlock => break,
                        _ => return,
                    }
                }
            }
            if flush_done {
                continue;
            }
        }

        // 4. Wait for stop, control wakeup, socket writability (if mid-frame),
        //    or stdin readability (only when idle and no resize is pending).
        let need_write = out_off < out_buf.len();
        let can_read_stdin = stdin_open && !need_write && pending_resize.is_none();

        let mut pfds = [
            libc::pollfd {
                fd: stop_r.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: ctrl_wake_r.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: -1,
                events: 0,
                revents: 0,
            },
            libc::pollfd {
                fd: -1,
                events: 0,
                revents: 0,
            },
        ];
        let mut nfds = 2usize;
        let sock_idx = if need_write {
            let i = nfds;
            pfds[i] = libc::pollfd {
                fd: sock.as_raw_fd(),
                events: libc::POLLOUT,
                revents: 0,
            };
            nfds += 1;
            Some(i)
        } else {
            None
        };
        let stdin_idx = if can_read_stdin {
            let i = nfds;
            pfds[i] = libc::pollfd {
                fd: stdin_fd,
                events: libc::POLLIN,
                revents: 0,
            };
            nfds += 1;
            Some(i)
        } else {
            None
        };

        let prc = unsafe { libc::poll(pfds.as_mut_ptr(), nfds as libc::nfds_t, -1) };
        if prc < 0 {
            if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return;
        }
        if pfds[0].revents != 0 {
            return;
        }
        if pfds[1].revents != 0 {
            drain_wake(&ctrl_wake_r);
            // Loop immediately so high-priority resize frames run before stdin.
            continue;
        }
        if let Some(i) = sock_idx
            && (pfds[i].revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL)) != 0
            && (pfds[i].revents & libc::POLLOUT) == 0
        {
            return;
        }
        if let Some(i) = stdin_idx
            && pfds[i].revents != 0
        {
            let n = loop {
                let rc =
                    unsafe { libc::read(stdin_fd, stdin_buf.as_mut_ptr().cast(), stdin_buf.len()) };
                if rc >= 0 {
                    break Some(rc as usize);
                }
                let e = io::Error::last_os_error();
                match e.kind() {
                    io::ErrorKind::Interrupted => continue,
                    io::ErrorKind::WouldBlock => break None,
                    _ => break Some(0),
                }
            };
            if let Some(n) = n {
                if n == 0 {
                    stdin_open = false;
                    out_buf = VmExecMsg::DataEnd.encode();
                } else {
                    out_buf = VmExecMsg::Data(stdin_buf[..n].to_vec()).encode();
                }
                out_off = 0;
            }
        }
    }
}

enum PollFrame {
    Msg(VmExecMsg),
    WouldBlock,
    Eof,
}

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

enum OutputSink<'a> {
    Fd(RawFd),
    #[cfg(test)]
    Writer(&'a mut dyn Write),
    #[cfg(not(test))]
    _Marker(std::marker::PhantomData<&'a ()>),
}

enum WriteOutcome {
    Written,
    BrokenPipe,
    Cancelled,
}

fn write_cancellable(
    sink: &mut OutputSink<'_>,
    data: &[u8],
    term_wake_r: Option<RawFd>,
) -> io::Result<WriteOutcome> {
    if data.is_empty() {
        return Ok(WriteOutcome::Written);
    }
    match sink {
        #[cfg(test)]
        OutputSink::Writer(w) => match w.write_all(data).and_then(|()| w.flush()) {
            Ok(()) => Ok(WriteOutcome::Written),
            Err(e) if e.kind() == io::ErrorKind::BrokenPipe => Ok(WriteOutcome::BrokenPipe),
            Err(e) => Err(e),
        },
        #[cfg(not(test))]
        OutputSink::_Marker(_) => unreachable!(),
        OutputSink::Fd(fd) => {
            let fd = *fd;
            let mut offset = 0usize;
            while offset < data.len() {
                if term_wake_r.is_some_and(fd_ready) {
                    return Ok(WriteOutcome::Cancelled);
                }
                let rc =
                    unsafe { libc::write(fd, data[offset..].as_ptr().cast(), data.len() - offset) };
                if rc > 0 {
                    offset += rc as usize;
                    continue;
                }
                if rc == 0 {
                    return Ok(WriteOutcome::BrokenPipe);
                }
                let e = io::Error::last_os_error();
                match e.kind() {
                    io::ErrorKind::Interrupted => continue,
                    io::ErrorKind::BrokenPipe => return Ok(WriteOutcome::BrokenPipe),
                    io::ErrorKind::WouldBlock => {
                        let mut pfds = [
                            libc::pollfd {
                                fd,
                                events: libc::POLLOUT,
                                revents: 0,
                            },
                            libc::pollfd {
                                fd: term_wake_r.unwrap_or(-1),
                                events: libc::POLLIN,
                                revents: 0,
                            },
                        ];
                        let nfds = if term_wake_r.is_some() { 2 } else { 1 };
                        let prc = unsafe { libc::poll(pfds.as_mut_ptr(), nfds, -1) };
                        if prc < 0 {
                            let pe = io::Error::last_os_error();
                            if pe.kind() == io::ErrorKind::Interrupted {
                                continue;
                            }
                            return Err(pe);
                        }
                        if nfds == 2 && pfds[1].revents != 0 {
                            return Ok(WriteOutcome::Cancelled);
                        }
                        if (pfds[0].revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL)) != 0
                            && (pfds[0].revents & libc::POLLOUT) == 0
                        {
                            return Ok(WriteOutcome::BrokenPipe);
                        }
                    }
                    _ => return Err(e),
                }
            }
            Ok(WriteOutcome::Written)
        }
    }
}

enum ReadOutcome {
    Msg(VmExecMsg),
    Eof,
    Cancelled,
}

fn read_frame_cancellable(
    conn: &UnixStream,
    frames: &mut FrameReader,
    term_wake_r: Option<RawFd>,
) -> io::Result<ReadOutcome> {
    loop {
        match frames.poll_frame(conn.as_raw_fd())? {
            PollFrame::Msg(m) => return Ok(ReadOutcome::Msg(m)),
            PollFrame::Eof => return Ok(ReadOutcome::Eof),
            PollFrame::WouldBlock => {}
        }
        let mut pfds = [
            libc::pollfd {
                fd: conn.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: term_wake_r.unwrap_or(-1),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        let nfds = if term_wake_r.is_some() { 2 } else { 1 };
        let prc = unsafe { libc::poll(pfds.as_mut_ptr(), nfds, -1) };
        if prc < 0 {
            let e = io::Error::last_os_error();
            if e.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(e);
        }
        if nfds == 2 && pfds[1].revents != 0 {
            return Ok(ReadOutcome::Cancelled);
        }
    }
}

/// Relay stdio for an already-`Started` VM exec session; returns how the
/// session terminated.
fn run_session(
    conn: UnixStream,
    session_token: [u8; 16],
    sock_path: PathBuf,
    initial_tty_size: Option<(u16, u16)>,
) -> anyhow::Result<SessionExit> {
    let mut out = OutputSink::Fd(1);
    let mut err = OutputSink::Fd(2);
    run_session_io(
        conn,
        session_token,
        move |deadline, stop_r| {
            connect_with_deadline_cancellable(&sock_path, deadline, Some(stop_r))
        },
        initial_tty_size,
        0,
        1,
        &mut out,
        &mut err,
        window_size,
        SIGINT_DELIVERY_TIMEOUT,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn run_session_io(
    conn: UnixStream,
    session_token: [u8; 16],
    connect_ctrl: impl Fn(Instant, RawFd) -> io::Result<UnixStream> + Send + 'static,
    initial_tty_size: Option<(u16, u16)>,
    stdin_fd: RawFd,
    stdout_fd: RawFd,
    out: &mut OutputSink<'_>,
    err: &mut OutputSink<'_>,
    get_window_size: impl Fn() -> (u16, u16) + Send + 'static,
    sigint_timeout: Duration,
    on_ready: Option<mpsc::Sender<()>>,
) -> anyhow::Result<SessionExit> {
    let allocate_pty = initial_tty_size.is_some();
    let stdin_is_tty = is_tty(stdin_fd);
    let stdout_is_tty = is_tty(stdout_fd);
    let want_raw = allocate_pty && stdin_is_tty;

    // Install signal wakeups BEFORE entering raw mode or spawning I/O helpers
    // so catchable termination signals always unwind cleanly and restore host
    // termios.
    let sig_setup =
        SignalGuard::install(&[libc::SIGTERM, libc::SIGHUP, libc::SIGINT, libc::SIGQUIT]).ok();

    let raw = if want_raw {
        RawMode::enter_fd(stdin_fd).ok()
    } else {
        None
    };
    let raw_active = raw.is_some();

    // NEVER call `F_SETFL(O_NONBLOCK)` on any inherited stdio descriptor
    // (`stdin`, `stdout`, or `stderr`) because inherited descriptors frequently
    // share an open file description (e.g. an interactive TTY, socketpair, or
    // `2>&1`). Relay blocking stdin and blocking output sinks through private
    // nonblocking pipes backed by `/bin/cat` helpers instead.
    let stdin_helper = StdinHelper::maybe_spawn(stdin_fd)?;
    let worker_stdin_fd = stdin_helper
        .as_ref()
        .map_or(stdin_fd, StdinHelper::as_raw_fd);

    let out_raw_fd = match *out {
        OutputSink::Fd(fd) => Some(fd),
        #[cfg(test)]
        OutputSink::Writer(_) => None,
        #[cfg(not(test))]
        OutputSink::_Marker(_) => None,
    };
    let err_raw_fd = match *err {
        OutputSink::Fd(fd) => Some(fd),
        #[cfg(test)]
        OutputSink::Writer(_) => None,
        #[cfg(not(test))]
        OutputSink::_Marker(_) => None,
    };
    let mut out_helper = match out_raw_fd {
        Some(fd) => OutputHelper::maybe_spawn(fd)?,
        None => None,
    };
    let share_out_helper = out_raw_fd.is_some() && out_raw_fd == err_raw_fd && out_helper.is_some();
    let mut err_helper = if share_out_helper {
        None
    } else {
        match err_raw_fd {
            Some(fd) => OutputHelper::maybe_spawn(fd)?,
            None => None,
        }
    };

    // macOS XNU ignores MSG_DONTWAIT on AF_UNIX SOCK_STREAM sends; put the
    // shared socket open file description in nonblocking mode so writer_worker
    // and FrameReader can never block inside send/recv.
    conn.set_nonblocking(true)?;

    // Independent socket handle used exclusively for lock-free `shutdown(Both)`.
    let shutdown_sock = Arc::new(conn.try_clone()?);
    let writer_sock = conn.try_clone()?;

    let (stop_r, stop_w) = pipe_cloexec(false)?;
    let (ctrl_wake_r, ctrl_wake_w) = pipe_cloexec(true)?;
    let ctrl_wake_w = Arc::new(ctrl_wake_w);
    let (term_wake_r, term_wake_w) = pipe_cloexec(true)?;
    let term_wake_w = Arc::new(term_wake_w);
    let (sigint_wake_r, sigint_wake_w) = pipe_cloexec(true)?;
    let sigint_wake_w = Arc::new(sigint_wake_w);
    let (ctrl_tx, ctrl_rx) = mpsc::channel();
    let (sigint_tx, sigint_rx) = mpsc::channel::<i32>();
    let sigint_in_flight = Arc::new(AtomicBool::new(false));
    let terminated_by = Arc::new(AtomicI32::new(0));

    let mut sig_guard = sig_setup.map(|(mut guard, sig_r)| {
        let shutdown_sock = shutdown_sock.clone();
        let term_wake_w = term_wake_w.clone();
        let sigint_tx = sigint_tx.clone();
        let sigint_wake_w = sigint_wake_w.clone();
        let sigint_in_flight = sigint_in_flight.clone();
        let terminated_by = terminated_by.clone();
        let handle = thread::spawn(move || {
            let mut b = [0u8; 1];
            loop {
                let rc = unsafe { libc::read(sig_r.as_raw_fd(), b.as_mut_ptr().cast(), 1) };
                if rc < 0 {
                    if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                        continue;
                    }
                    return;
                }
                if rc == 0 || b[0] == 0 {
                    return;
                }
                let sig = i32::from(b[0]);
                if raw_active || sig != libc::SIGINT {
                    // Cancel lock-free: wake relay_output (both reads and
                    // blocked output writes) and unblock any socket writes so
                    // RawMode restores before re-raise.
                    let _ =
                        terminated_by.compare_exchange(0, sig, Ordering::AcqRel, Ordering::Acquire);
                    let _ = shutdown_sock.shutdown(Shutdown::Both);
                    notify_wake(&term_wake_w);
                    return;
                }
                // Non-raw SIGINT: a second SIGINT while the previous OOB
                // delivery is still awaiting guest SignalAck cancels locally.
                if sigint_in_flight.swap(true, Ordering::AcqRel) {
                    let _ =
                        terminated_by.compare_exchange(0, sig, Ordering::AcqRel, Ordering::Acquire);
                    let _ = shutdown_sock.shutdown(Shutdown::Both);
                    notify_wake(&term_wake_w);
                    return;
                }
                if sigint_tx.send(sig).is_err() {
                    // Session is already tearing down (e.g. draining output);
                    // interrupt any blocked OutputHelper drain promptly.
                    let _ =
                        terminated_by.compare_exchange(0, sig, Ordering::AcqRel, Ordering::Acquire);
                    notify_wake(&term_wake_w);
                    return;
                }
                notify_wake(&sigint_wake_w);
            }
        });
        guard.attach_dispatcher(handle);
        guard
    });

    let writer_thread = {
        let stop_fd = stop_r.try_clone()?;
        thread::spawn(move || {
            writer_worker(writer_sock, worker_stdin_fd, stop_fd, ctrl_wake_r, ctrl_rx);
        })
    };

    let sigint_thread = if !raw_active {
        let stop_fd = stop_r.try_clone()?;
        let shutdown_sock = shutdown_sock.clone();
        let term_wake_w = term_wake_w.clone();
        let terminated_by = terminated_by.clone();
        let sigint_in_flight = sigint_in_flight.clone();
        Some(thread::spawn(move || {
            loop {
                let mut pfds = [
                    libc::pollfd {
                        fd: stop_fd.as_raw_fd(),
                        events: libc::POLLIN,
                        revents: 0,
                    },
                    libc::pollfd {
                        fd: sigint_wake_r.as_raw_fd(),
                        events: libc::POLLIN,
                        revents: 0,
                    },
                ];
                let prc = unsafe { libc::poll(pfds.as_mut_ptr(), 2, -1) };
                if prc < 0 {
                    if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                        continue;
                    }
                    return;
                }
                if pfds[0].revents != 0 {
                    return;
                }
                if pfds[1].revents != 0 {
                    drain_wake(&sigint_wake_r);
                    while let Ok(sig) = sigint_rx.try_recv() {
                        let deadline = Instant::now() + sigint_timeout;
                        match send_signal_oob(
                            &connect_ctrl,
                            session_token,
                            sig,
                            deadline,
                            stop_fd.as_raw_fd(),
                        ) {
                            OobSignalResult::Acked => {
                                sigint_in_flight.store(false, Ordering::Release);
                            }
                            OobSignalResult::Cancelled => {
                                sigint_in_flight.store(false, Ordering::Release);
                                return;
                            }
                            OobSignalResult::Failed => {
                                let _ = terminated_by.compare_exchange(
                                    0,
                                    sig,
                                    Ordering::AcqRel,
                                    Ordering::Acquire,
                                );
                                let _ = shutdown_sock.shutdown(Shutdown::Both);
                                notify_wake(&term_wake_w);
                                sigint_in_flight.store(false, Ordering::Release);
                                return;
                            }
                        }
                    }
                }
            }
        }))
    } else {
        None
    };

    let resize_thread = if let Some(mut last) = initial_tty_size
        && (stdin_is_tty || stdout_is_tty)
    {
        let stop_fd = stop_r.try_clone()?;
        let ctrl_tx = ctrl_tx.clone();
        let ctrl_wake_w = ctrl_wake_w.clone();
        Some(thread::spawn(move || {
            loop {
                // Compare immediately against the dimensions sent in ExecSpec
                // so a resize during VM startup is delivered right away.
                let now = get_window_size();
                if now != last {
                    last = now;
                    if ctrl_tx
                        .send(WriterCtrl::Resize {
                            rows: now.0,
                            cols: now.1,
                        })
                        .is_err()
                    {
                        return;
                    }
                    notify_wake(&ctrl_wake_w);
                }
                let mut pfd = libc::pollfd {
                    fd: stop_fd.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                };
                let prc = unsafe { libc::poll(&mut pfd, 1, 200) };
                if prc > 0 {
                    return;
                }
            }
        }))
    } else {
        None
    };

    if let Some(tx) = on_ready {
        let _ = tx.send(());
    }

    let mut res = {
        let mut relay_out_fd = out_helper.as_ref().map(|h| OutputSink::Fd(h.as_raw_fd()));
        let mut relay_err_fd = if share_out_helper {
            out_helper.as_ref().map(|h| OutputSink::Fd(h.as_raw_fd()))
        } else {
            err_helper.as_ref().map(|h| OutputSink::Fd(h.as_raw_fd()))
        };
        let relay_out = relay_out_fd.as_mut().unwrap_or(out);
        let relay_err = relay_err_fd.as_mut().unwrap_or(err);
        relay_output_cancellable(conn, relay_out, relay_err, Some(term_wake_r.as_raw_fd()))
    };

    // Shut down the socket lock-free BEFORE joining relay workers so a blocked
    // writer cannot hang cleanup (e.g. after BrokenPipe on stdout) and the
    // guest observes disconnect promptly.
    let _ = shutdown_sock.shutdown(Shutdown::Both);
    // Stop and join relay workers and terminate/reap any blocking-stdin helper
    // so no reader remains on `stdin_fd`.
    drop(stop_w);
    let _ = writer_thread.join();
    if let Some(h) = resize_thread {
        let _ = h.join();
    }
    if let Some(h) = sigint_thread {
        let _ = h.join();
    }
    drop(stdin_helper);

    // On clean exit, drain buffered output through any OutputHelper while
    // remaining interruptible on termination signals; on signal cancellation,
    // broken pipe, or error, abort output helpers immediately.
    let mut drain_broken_pipe = false;
    let should_drain = terminated_by.load(Ordering::Acquire) == 0
        && matches!(res, Ok(code) if code != 128 + libc::SIGPIPE);
    if should_drain {
        if let Some(mut h) = out_helper.take()
            && matches!(h.finish(term_wake_r.as_raw_fd()), HelperFinish::BrokenPipe)
        {
            drain_broken_pipe = true;
        }
        if terminated_by.load(Ordering::Acquire) == 0 {
            if let Some(mut h) = err_helper.take()
                && matches!(h.finish(term_wake_r.as_raw_fd()), HelperFinish::BrokenPipe)
            {
                drain_broken_pipe = true;
            }
        } else if let Some(mut h) = err_helper.take() {
            h.abort();
        }
    } else {
        if let Some(mut h) = out_helper.take() {
            h.abort();
        }
        if let Some(mut h) = err_helper.take() {
            h.abort();
        }
    }

    drop(raw);
    drop(stop_r);
    drop(sig_guard.take());

    let sig = terminated_by.load(Ordering::Acquire);
    if sig != 0 {
        return Ok(SessionExit::Signal(sig));
    }
    if drain_broken_pipe && matches!(res, Ok(0)) {
        res = Ok(128 + libc::SIGPIPE);
    }
    res.map(SessionExit::Code)
}

/// Read output/exit frames from `conn` into `out` and `err`, waking immediately
/// on `term_wake_r` whether blocked in socket reads or output writes.
fn relay_output_cancellable(
    conn: UnixStream,
    out: &mut OutputSink<'_>,
    err: &mut OutputSink<'_>,
    term_wake_r: Option<RawFd>,
) -> anyhow::Result<i32> {
    let mut frames = FrameReader::default();
    loop {
        match read_frame_cancellable(&conn, &mut frames, term_wake_r)? {
            ReadOutcome::Msg(VmExecMsg::Stdout(d)) => {
                match write_cancellable(out, &d, term_wake_r)? {
                    WriteOutcome::Written => {}
                    WriteOutcome::BrokenPipe => return Ok(128 + libc::SIGPIPE),
                    WriteOutcome::Cancelled => return Ok(0),
                }
            }
            ReadOutcome::Msg(VmExecMsg::Stderr(d)) => {
                match write_cancellable(err, &d, term_wake_r)? {
                    WriteOutcome::Written => {}
                    WriteOutcome::BrokenPipe => return Ok(128 + libc::SIGPIPE),
                    WriteOutcome::Cancelled => return Ok(0),
                }
            }
            ReadOutcome::Msg(VmExecMsg::Exit { code }) => return Ok(code),
            ReadOutcome::Msg(VmExecMsg::Error(e)) => bail!("{e}"),
            ReadOutcome::Msg(_) => {}
            ReadOutcome::Cancelled => return Ok(0),
            ReadOutcome::Eof => bail!("the guest closed the connection"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::net::UnixListener;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::*;
    use crate::api::{DaemonInfo, DockerInfo, DockerState, K8sStatus, MountInfo, Versions};
    use crate::config::Config;
    use clap::Parser;

    /// Serializes unit tests that install process-wide signal handlers.
    static SIGNAL_TEST_LOCK: Mutex<()> = Mutex::new(());

    #[derive(Parser)]
    struct Cli {
        #[command(subcommand)]
        cmd: Top,
    }

    #[derive(clap::Subcommand)]
    enum Top {
        #[command(subcommand)]
        Vm(VmCmd),
    }

    fn parse(args: &[&str]) -> VmCmd {
        let Top::Vm(cmd) =
            Cli::try_parse_from(std::iter::once("katamaran").chain(args.iter().copied()))
                .unwrap()
                .cmd;
        cmd
    }

    fn test_dir(tag: &str) -> PathBuf {
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        // Keep paths short so Unix socket paths stay well under macOS SUN_LEN (104).
        let dir = PathBuf::from(format!("/tmp/kve-{tag}-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn dummy_status(state: VmState, docker: DockerState, error: Option<&str>) -> Status {
        Status {
            state,
            since_ms: 0,
            uptime_s: None,
            error: error.map(String::from),
            daemon: DaemonInfo {
                pid: 1,
                home: PathBuf::from("/tmp/kat"),
                started_ms: 0,
                launchd_label: None,
            },
            versions: Versions {
                katamaran: "0.0.0".into(),
                protocol: kat_proto::PROTOCOL_VERSION,
                kernel: None,
            },
            config: Config::default(),
            vm: None,
            usage: None,
            docker: DockerInfo {
                state: docker,
                socket: PathBuf::from("/tmp/docker.sock"),
            },
            k8s: K8sStatus::default(),
            mount: MountInfo {
                enabled: false,
                path: None,
                mounted: false,
                error: None,
            },
            ports: vec![],
            port_errors: vec![],
        }
    }

    fn open_test_pty(rows: u16, cols: u16) -> (OwnedFd, OwnedFd) {
        let (mut master_raw, mut slave_raw) = (0, 0);
        let mut ws = libc::winsize {
            ws_row: rows,
            ws_col: cols,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        assert_eq!(
            unsafe {
                libc::openpty(
                    &mut master_raw,
                    &mut slave_raw,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    &mut ws,
                )
            },
            0
        );
        unsafe {
            (
                OwnedFd::from_raw_fd(master_raw),
                OwnedFd::from_raw_fd(slave_raw),
            )
        }
    }

    fn shrink_socket_pair(a: &UnixStream, b: &UnixStream, bytes: i32) {
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
    fn parses_vm_shell_and_exec() {
        let VmCmd::Shell(s) = parse(&["vm", "shell"]) else {
            panic!()
        };
        assert!(s.workdir.is_none() && s.env.is_empty() && s.cmd.is_empty());

        let VmCmd::Shell(s) = parse(&[
            "vm", "shell", "-w", "/var/log", "-e", "FOO=1", "--", "ls", "-la",
        ]) else {
            panic!()
        };
        assert_eq!(s.workdir.as_deref(), Some("/var/log"));
        assert_eq!(s.env, ["FOO=1"]);
        assert_eq!(s.cmd, ["ls", "-la"]);

        let VmCmd::Exec(e) = parse(&["vm", "exec", "--", "uname", "-a"]) else {
            panic!()
        };
        assert!(!e.tty && e.workdir.is_none() && e.cmd == ["uname", "-a"]);

        let VmCmd::Exec(e) = parse(&["vm", "exec", "ls", "-t", "-la"]) else {
            panic!()
        };
        assert!(!e.tty && e.cmd == ["ls", "-t", "-la"]);

        let VmCmd::Exec(e) = parse(&["vm", "exec", "--tty", "--", "top", "-b"]) else {
            panic!()
        };
        assert!(e.tty && e.cmd == ["top", "-b"]);
        let VmCmd::Exec(e) = parse(&["vm", "exec", "--tty", "top", "-b"]) else {
            panic!()
        };
        assert!(e.tty && e.cmd == ["top", "-b"]);

        let VmCmd::Exec(e) = parse(&[
            "vm", "exec", "-t", "-w", "/etc", "-e", "A=1", "-e", "B=2", "--", "sh", "-c", "id",
        ]) else {
            panic!()
        };
        assert!(e.tty);
        assert_eq!(e.workdir.as_deref(), Some("/etc"));
        assert_eq!(e.env, ["A=1", "B=2"]);
        assert_eq!(e.cmd, ["sh", "-c", "id"]);

        assert!(Cli::try_parse_from(["katamaran", "vm", "exec"]).is_err());
        assert!(Cli::try_parse_from(["katamaran", "vm", "exec", "--"]).is_err());
    }

    #[test]
    fn parses_env_flags_and_omits_values_in_errors() {
        let env = parse_env(
            &[
                "FOO=bar=baz".into(),
                "LANG=C".into(),
                "FROM_HOST".into(),
                "UNSET_HOST".into(),
            ],
            |k| match k {
                "LANG" => Some("en_US.UTF-8".into()),
                "FROM_HOST" => Some("yes".into()),
                _ => None,
            },
        )
        .unwrap();
        assert_eq!(
            env,
            vec![
                ("LANG".into(), "C".into()),
                ("FOO".into(), "bar=baz".into()),
                ("FROM_HOST".into(), "yes".into()),
            ]
        );
        let sentinel = "SUPER_SECRET_SENTINEL_VALUE";
        let err = parse_env(&[format!("={sentinel}")], |_| None).unwrap_err();
        assert!(!err.to_string().contains(sentinel), "{err}");

        let err = parse_env(&[format!("BAD\0KEY={sentinel}")], |_| None).unwrap_err();
        assert!(!err.to_string().contains(sentinel), "{err}");

        let err = parse_env(&[format!("TOKEN={sentinel}\0tail")], |_| None).unwrap_err();
        assert!(err.to_string().contains("TOKEN"), "{err}");
        assert!(!err.to_string().contains(sentinel), "{err}");
        assert!(!err.to_string().contains("tail"), "{err}");

        assert!(parse_env(&["".into()], |_| None).is_err());
    }

    fn no_ctrl(_deadline: Instant, _stop_r: RawFd) -> io::Result<UnixStream> {
        Err(io::Error::new(
            io::ErrorKind::NotConnected,
            "no control socket",
        ))
    }

    #[test]
    fn relays_stdout_stderr_and_exit_code() {
        let (host, mut guest) = UnixStream::pair().unwrap();
        let peer = thread::spawn(move || {
            vm_exec::write(&mut guest, &VmExecMsg::Stdout(b"hello ".to_vec())).unwrap();
            vm_exec::write(&mut guest, &VmExecMsg::Stderr(b"warn\n".to_vec())).unwrap();
            vm_exec::write(&mut guest, &VmExecMsg::Pong).unwrap();
            vm_exec::write(&mut guest, &VmExecMsg::Stdout(b"vm\n".to_vec())).unwrap();
            vm_exec::write(&mut guest, &VmExecMsg::Exit { code: 7 }).unwrap();
        });
        let (mut out_buf, mut err_buf) = (Vec::new(), Vec::new());
        let mut out = OutputSink::Writer(&mut out_buf);
        let mut err = OutputSink::Writer(&mut err_buf);
        let code = relay_output_cancellable(host, &mut out, &mut err, None).unwrap();
        peer.join().unwrap();
        assert_eq!(code, 7);
        assert_eq!(out_buf, b"hello vm\n");
        assert_eq!(err_buf, b"warn\n");
    }

    #[test]
    fn surfaces_guest_errors_and_eof() {
        let (host, mut guest) = UnixStream::pair().unwrap();
        vm_exec::write(&mut guest, &VmExecMsg::Error("boom".into())).unwrap();
        let (mut out_buf, mut err_buf) = (Vec::new(), Vec::new());
        let mut out = OutputSink::Writer(&mut out_buf);
        let mut err = OutputSink::Writer(&mut err_buf);
        let e = relay_output_cancellable(host, &mut out, &mut err, None).unwrap_err();
        assert!(e.to_string().contains("boom"), "{e}");

        let (host, guest) = UnixStream::pair().unwrap();
        drop(guest);
        let e = relay_output_cancellable(host, &mut out, &mut err, None).unwrap_err();
        assert!(e.to_string().contains("closed"), "{e}");
    }

    #[test]
    fn readiness_probe_retries_without_replaying_exec() {
        let dir = test_dir("no-replay");
        let sock = dir.join("vm-exec.sock");
        let listener = UnixListener::bind(&sock).unwrap();
        let exec_count = Arc::new(AtomicUsize::new(0));
        let exec_count_srv = exec_count.clone();

        let server = thread::spawn(move || {
            let (conn, _) = listener.accept().unwrap();
            drop(conn);
            let (mut conn, _) = listener.accept().unwrap();
            assert_eq!(vm_exec::read(&mut conn).unwrap(), Some(VmExecMsg::Ping));
            vm_exec::write(&mut conn, &VmExecMsg::Pong).unwrap();
            assert!(matches!(
                vm_exec::read(&mut conn).unwrap(),
                Some(VmExecMsg::Exec(_))
            ));
            exec_count_srv.fetch_add(1, Ordering::SeqCst);
            drop(conn);
            let (mut conn, _) = listener.accept().unwrap();
            assert_eq!(vm_exec::read(&mut conn).unwrap(), Some(VmExecMsg::Ping));
            vm_exec::write(&mut conn, &VmExecMsg::Pong).unwrap();
            assert!(matches!(
                vm_exec::read(&mut conn).unwrap(),
                Some(VmExecMsg::Exec(_))
            ));
            exec_count_srv.fetch_add(1, Ordering::SeqCst);
            vm_exec::write(&mut conn, &VmExecMsg::Started { token: [7; 16] }).unwrap();
        });

        let spec = ExecSpec {
            argv: vec!["true".into()],
            ..Default::default()
        };

        let ready =
            wait_for_ready_socket(&sock, Instant::now() + Duration::from_secs(2), || Ok(None))
                .unwrap();
        let err = submit_exec(ready, &spec, Instant::now() + Duration::from_secs(2)).unwrap_err();
        assert!(err.to_string().contains("unknown"), "{err:#}");
        assert_eq!(exec_count.load(Ordering::SeqCst), 1);

        let ready =
            wait_for_ready_socket(&sock, Instant::now() + Duration::from_secs(2), || Ok(None))
                .unwrap();
        let (conn, token) =
            submit_exec(ready, &spec, Instant::now() + Duration::from_secs(2)).unwrap();
        assert_eq!(token, [7; 16]);
        assert_eq!(conn.read_timeout().unwrap(), None);
        assert_eq!(conn.write_timeout().unwrap(), None);
        assert_eq!(exec_count.load(Ordering::SeqCst), 2);

        server.join().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn handshake_times_out_against_silent_peer() {
        let dir = test_dir("silent");
        let sock = dir.join("vm-exec.sock");
        let listener = UnixListener::bind(&sock).unwrap();

        let server = thread::spawn(move || {
            let (_c1, _) = listener.accept().unwrap();
            thread::sleep(Duration::from_millis(400));
            let (mut c2, _) = listener.accept().unwrap();
            assert_eq!(vm_exec::read(&mut c2).unwrap(), Some(VmExecMsg::Ping));
            vm_exec::write(&mut c2, &VmExecMsg::Pong).unwrap();
            assert!(matches!(
                vm_exec::read(&mut c2).unwrap(),
                Some(VmExecMsg::Exec(_))
            ));
            thread::sleep(Duration::from_millis(400));
        });

        let t = Instant::now();
        let p = probe_ready(&sock, t + Duration::from_millis(120));
        assert!(matches!(p, Probe::TimedOut(_)));
        assert!(t.elapsed() < Duration::from_secs(1));

        let ready = match probe_ready(&sock, Instant::now() + Duration::from_secs(2)) {
            Probe::Ready(c) => c,
            _ => panic!("expected Ready"),
        };
        let t = Instant::now();
        let err = submit_exec(
            ready,
            &ExecSpec {
                argv: vec!["true".into()],
                ..Default::default()
            },
            t + Duration::from_millis(120),
        )
        .unwrap_err();
        assert!(t.elapsed() < Duration::from_secs(1));
        assert!(err.to_string().contains("unknown"), "{err:#}");

        server.join().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn startup_wait_succeeds_while_docker_stays_starting_and_reports_vm_error() {
        let dir = test_dir("docker-starting");
        let sock = dir.join("vm-exec.sock");
        let sock_bg = sock.clone();

        let server = thread::spawn(move || {
            thread::sleep(Duration::from_millis(120));
            let listener = UnixListener::bind(&sock_bg).unwrap();
            let (mut conn, _) = listener.accept().unwrap();
            assert_eq!(vm_exec::read(&mut conn).unwrap(), Some(VmExecMsg::Ping));
            vm_exec::write(&mut conn, &VmExecMsg::Pong).unwrap();
        });

        let conn = wait_for_ready_socket(&sock, Instant::now() + Duration::from_secs(3), || {
            Ok(Some(dummy_status(
                VmState::Starting,
                DockerState::Starting,
                None,
            )))
        })
        .unwrap();
        drop(conn);
        server.join().unwrap();

        let missing = dir.join("never-bound.sock");
        let err = wait_for_ready_socket(&missing, Instant::now() + Duration::from_secs(5), || {
            Ok(Some(dummy_status(
                VmState::Error,
                DockerState::Starting,
                Some("kernel panic"),
            )))
        })
        .unwrap_err();
        assert!(err.to_string().contains("kernel panic"), "{err:#}");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn host_pty_restores_termios_on_sigterm_under_socket_backpressure() {
        let _lock = SIGNAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let (master, slave) = open_test_pty(24, 80);

        let mut before: libc::termios = unsafe { std::mem::zeroed() };
        assert_eq!(
            unsafe { libc::tcgetattr(slave.as_raw_fd(), &mut before) },
            0
        );
        assert_ne!(before.c_lflag & libc::ICANON, 0);
        assert_ne!(before.c_lflag & libc::ECHO, 0);

        let (host, guest) = UnixStream::pair().unwrap();
        shrink_socket_pair(&host, &guest, 4096);
        let (ready_tx, ready_rx) = mpsc::channel();
        let slave_fd = slave.as_raw_fd();
        let session = thread::spawn(move || {
            let (mut out_buf, mut err_buf) = (Vec::new(), Vec::new());
            let mut out = OutputSink::Writer(&mut out_buf);
            let mut err = OutputSink::Writer(&mut err_buf);
            run_session_io(
                host,
                [0; 16],
                no_ctrl,
                Some((24, 80)),
                slave_fd,
                slave_fd,
                &mut out,
                &mut err,
                || (24, 80),
                SIGINT_DELIVERY_TIMEOUT,
                Some(ready_tx),
            )
        });

        ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let mut during: libc::termios = unsafe { std::mem::zeroed() };
        assert_eq!(
            unsafe { libc::tcgetattr(slave.as_raw_fd(), &mut during) },
            0
        );
        assert_eq!(during.c_lflag & libc::ICANON, 0);

        // Flood master -> slave -> writer_worker while `guest` never reads so
        // the socket send buffer is completely full before SIGTERM arrives.
        // Put `master` in nonblocking mode so the flood thread never blocks on
        // a partial PTY queue after `session` stops reading `slave`.
        let master_fd = master.as_raw_fd();
        unsafe {
            let fl = libc::fcntl(master_fd, libc::F_GETFL);
            assert!(fl >= 0 && libc::fcntl(master_fd, libc::F_SETFL, fl | libc::O_NONBLOCK) == 0);
        }
        let stop_flood = Arc::new(AtomicBool::new(false));
        let stop_flood_bg = stop_flood.clone();
        let flood = thread::spawn(move || {
            let chunk = [b'x'; 2048];
            for _ in 0..64 {
                if stop_flood_bg.load(Ordering::Acquire) {
                    break;
                }
                let mut pfd = libc::pollfd {
                    fd: master_fd,
                    events: libc::POLLOUT,
                    revents: 0,
                };
                if unsafe { libc::poll(&mut pfd, 1, 50) } <= 0 {
                    break;
                }
                let rc = unsafe { libc::write(master_fd, chunk.as_ptr().cast(), chunk.len()) };
                if rc <= 0 {
                    break;
                }
            }
        });
        thread::sleep(Duration::from_millis(80));

        let t = Instant::now();
        unsafe { libc::raise(libc::SIGTERM) };
        let exit = session.join().unwrap().unwrap();
        assert_eq!(exit, SessionExit::Signal(libc::SIGTERM));
        assert!(t.elapsed() < Duration::from_secs(2));

        let mut after: libc::termios = unsafe { std::mem::zeroed() };
        assert_eq!(unsafe { libc::tcgetattr(slave.as_raw_fd(), &mut after) }, 0);
        // macOS sets the transient kernel state bit PENDIN when restoring canonical mode.
        assert_eq!(
            after.c_lflag & !libc::PENDIN,
            before.c_lflag & !libc::PENDIN
        );
        assert_eq!(after.c_iflag, before.c_iflag);
        assert_eq!(after.c_oflag, before.c_oflag);
        assert_eq!(after.c_cflag, before.c_cflag);

        stop_flood.store(true, Ordering::Release);
        drop(guest);
        let _ = flood.join();
        drop(master);
        drop(slave);
    }

    #[test]
    fn host_pty_restores_termios_on_sigterm_with_full_stdout_pipe() {
        let _lock = SIGNAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let (master, slave) = open_test_pty(24, 80);
        let (out_r, out_w) = pipe_cloexec(false).unwrap();
        let (_err_r, err_w) = pipe_cloexec(false).unwrap();

        let mut before: libc::termios = unsafe { std::mem::zeroed() };
        assert_eq!(
            unsafe { libc::tcgetattr(slave.as_raw_fd(), &mut before) },
            0
        );
        let out_orig_flags = unsafe { libc::fcntl(out_w.as_raw_fd(), libc::F_GETFL) };

        let (host, mut guest) = UnixStream::pair().unwrap();
        let (ready_tx, ready_rx) = mpsc::channel();
        let slave_fd = slave.as_raw_fd();
        let out_fd = out_w.as_raw_fd();
        let err_fd = err_w.as_raw_fd();

        let session = thread::spawn(move || {
            let mut out = OutputSink::Fd(out_fd);
            let mut err = OutputSink::Fd(err_fd);
            run_session_io(
                host,
                [0; 16],
                no_ctrl,
                Some((24, 80)),
                slave_fd,
                out_fd,
                &mut out,
                &mut err,
                || (24, 80),
                SIGINT_DELIVERY_TIMEOUT,
                Some(ready_tx),
            )
        });

        ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        // Inherited output fd must stay blocking even during the active session.
        assert_eq!(
            unsafe { libc::fcntl(out_w.as_raw_fd(), libc::F_GETFL) } & libc::O_NONBLOCK,
            0
        );
        // Keep `out_r` open without reading while the guest floods stdout so
        // both `out_w` and OutputHelper's private pipe saturate and
        // `relay_output_cancellable` blocks waiting on the full pipe.
        let guest_flood = thread::spawn(move || {
            let chunk = vec![b'O'; 16 << 10];
            for _ in 0..32 {
                if vm_exec::write(&mut guest, &VmExecMsg::Stdout(chunk.clone())).is_err() {
                    return;
                }
            }
        });
        thread::sleep(Duration::from_millis(120));

        let t = Instant::now();
        unsafe { libc::raise(libc::SIGTERM) };
        let exit = session.join().unwrap().unwrap();
        assert_eq!(exit, SessionExit::Signal(libc::SIGTERM));
        assert!(t.elapsed() < Duration::from_secs(2));

        let mut after: libc::termios = unsafe { std::mem::zeroed() };
        assert_eq!(unsafe { libc::tcgetattr(slave.as_raw_fd(), &mut after) }, 0);
        assert_eq!(
            after.c_lflag & !libc::PENDIN,
            before.c_lflag & !libc::PENDIN
        );
        assert_eq!(after.c_iflag, before.c_iflag);
        assert_eq!(after.c_oflag, before.c_oflag);
        assert_eq!(after.c_cflag, before.c_cflag);
        assert_eq!(
            unsafe { libc::fcntl(out_w.as_raw_fd(), libc::F_GETFL) } & libc::O_NONBLOCK,
            out_orig_flags & libc::O_NONBLOCK
        );

        let _ = guest_flood.join();
        drop(out_r);
        drop(out_w);
        drop(err_w);
        drop(master);
        drop(slave);
    }

    #[test]
    fn shared_stdin_and_output_fd_stays_blocking_during_session() {
        let _lock = SIGNAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let (master, slave) = open_test_pty(24, 80);
        let slave_dup = slave.try_clone().unwrap();
        assert_eq!(
            unsafe { libc::fcntl(slave.as_raw_fd(), libc::F_GETFL) } & libc::O_NONBLOCK,
            0
        );

        let (host, mut guest) = UnixStream::pair().unwrap();
        let (ready_tx, ready_rx) = mpsc::channel();
        let slave_fd = slave.as_raw_fd();
        let session = thread::spawn(move || {
            let mut out = OutputSink::Fd(slave_fd);
            let mut err = OutputSink::Fd(slave_fd);
            run_session_io(
                host,
                [0; 16],
                no_ctrl,
                Some((24, 80)),
                slave_fd,
                slave_fd,
                &mut out,
                &mut err,
                || (24, 80),
                SIGINT_DELIVERY_TIMEOUT,
                Some(ready_tx),
            )
        });

        ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        // When stdin, stdout, and stderr share the SAME open file description
        // (interactive TTY), `O_NONBLOCK` must NEVER be set on it.
        assert_eq!(
            unsafe { libc::fcntl(slave_dup.as_raw_fd(), libc::F_GETFL) } & libc::O_NONBLOCK,
            0
        );

        vm_exec::write(&mut guest, &VmExecMsg::Stdout(b"pty-hello\n".to_vec())).unwrap();
        vm_exec::write(&mut guest, &VmExecMsg::Exit { code: 0 }).unwrap();

        let reader = thread::spawn(move || {
            let mut buf = [0u8; 64];
            let mut got = Vec::new();
            let deadline = Instant::now() + Duration::from_secs(2);
            while !got.windows(9).any(|w| w == b"pty-hello") && Instant::now() < deadline {
                let mut pfd = libc::pollfd {
                    fd: master.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                };
                if unsafe { libc::poll(&mut pfd, 1, 50) } > 0 {
                    let n = unsafe {
                        libc::read(master.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len())
                    };
                    if n > 0 {
                        got.extend_from_slice(&buf[..n as usize]);
                    }
                }
            }
            (got, master)
        });

        assert_eq!(session.join().unwrap().unwrap(), SessionExit::Code(0));
        let (got, master) = reader.join().unwrap();
        assert!(
            got.windows(9).any(|w| w == b"pty-hello"),
            "unexpected pty output: {:?}",
            String::from_utf8_lossy(&got)
        );
        assert_eq!(
            unsafe { libc::fcntl(slave_dup.as_raw_fd(), libc::F_GETFL) } & libc::O_NONBLOCK,
            0
        );
        drop(master);
        drop(slave_dup);
        drop(slave);
    }

    #[test]
    fn non_pty_forwards_sigint_without_closing_session() {
        let _lock = SIGNAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = test_dir("oob-sigint");
        let ctrl_sock = dir.join("ctrl.sock");
        let ctrl_listener = UnixListener::bind(&ctrl_sock).unwrap();
        let token = [0x42; 16];

        let (stdin_r, stdin_w) = pipe_cloexec(false).unwrap();
        let (host, mut guest) = UnixStream::pair().unwrap();
        let (ready_tx, ready_rx) = mpsc::channel();
        let out_buf = Arc::new(Mutex::new(Vec::new()));
        let out_writer = SharedVec(out_buf.clone());
        let stdin_fd = stdin_r.as_raw_fd();
        let ctrl_path = ctrl_sock.clone();

        let session = thread::spawn(move || {
            let mut out_w = out_writer;
            let mut err_buf = Vec::new();
            let mut out = OutputSink::Writer(&mut out_w);
            let mut err = OutputSink::Writer(&mut err_buf);
            run_session_io(
                host,
                token,
                move |dl, stop_r| connect_with_deadline_cancellable(&ctrl_path, dl, Some(stop_r)),
                None,
                stdin_fd,
                -1,
                &mut out,
                &mut err,
                || (24, 80),
                Duration::from_millis(80),
                Some(ready_tx),
            )
        });

        ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        unsafe { libc::raise(libc::SIGINT) };

        let (mut ctrl_conn, _) = ctrl_listener.accept().unwrap();
        assert_eq!(
            vm_exec::read(&mut ctrl_conn).unwrap(),
            Some(VmExecMsg::SignalSession {
                token,
                signal: libc::SIGINT,
            })
        );
        // Acknowledge receipt out-of-band; sleep past sigint_timeout to verify
        // SignalAck disarms the timeout so a trapping guest command finishes.
        vm_exec::write(&mut ctrl_conn, &VmExecMsg::SignalAck).unwrap();
        thread::sleep(Duration::from_millis(160));
        vm_exec::write(&mut guest, &VmExecMsg::Stdout(b"caught-int\n".to_vec())).unwrap();
        vm_exec::write(&mut guest, &VmExecMsg::Exit { code: 77 }).unwrap();

        let exit = session.join().unwrap().unwrap();
        assert_eq!(exit, SessionExit::Code(77));
        assert_eq!(&*out_buf.lock().unwrap(), b"caught-int\n");
        drop(stdin_w);
        drop(stdin_r);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn non_pty_sigint_times_out_when_buffered_locally_without_guest_ack() {
        let _lock = SIGNAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = test_dir("oob-timeout");
        let ctrl_sock = dir.join("ctrl.sock");
        let ctrl_listener = UnixListener::bind(&ctrl_sock).unwrap();
        let ctrl_server = thread::spawn(move || {
            // Accept the OOB control connection so local `send(SignalSession)`
            // succeeds immediately, yet never send `SignalAck`.
            let (_c, _) = ctrl_listener.accept().unwrap();
            thread::sleep(Duration::from_millis(400));
        });

        let (stdin_r, stdin_w) = pipe_cloexec(false).unwrap();
        let (host, guest) = UnixStream::pair().unwrap();

        let (ready_tx, ready_rx) = mpsc::channel();
        let stdin_fd = stdin_r.as_raw_fd();
        let ctrl_path = ctrl_sock.clone();
        let session = thread::spawn(move || {
            let (mut out_buf, mut err_buf) = (Vec::new(), Vec::new());
            let mut out = OutputSink::Writer(&mut out_buf);
            let mut err = OutputSink::Writer(&mut err_buf);
            run_session_io(
                host,
                [0x11; 16],
                move |dl, stop_r| connect_with_deadline_cancellable(&ctrl_path, dl, Some(stop_r)),
                None,
                stdin_fd,
                -1,
                &mut out,
                &mut err,
                || (24, 80),
                Duration::from_millis(120),
                Some(ready_tx),
            )
        });

        ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let t = Instant::now();
        unsafe { libc::raise(libc::SIGINT) };
        let exit = session.join().unwrap().unwrap();
        assert_eq!(exit, SessionExit::Signal(libc::SIGINT));
        assert!(t.elapsed() < Duration::from_secs(2));

        let _ = ctrl_server.join();
        drop(stdin_w);
        drop(stdin_r);
        drop(guest);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn non_pty_sigterm_cancels_and_restores_output_and_stdin_flags() {
        let _lock = SIGNAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let (stdin_r, stdin_w) = pipe_cloexec(false).unwrap();
        let (out_r, out_w) = pipe_cloexec(false).unwrap();
        let (_err_r, err_w) = pipe_cloexec(false).unwrap();

        let in_orig = unsafe { libc::fcntl(stdin_r.as_raw_fd(), libc::F_GETFL) };
        let out_orig = unsafe { libc::fcntl(out_w.as_raw_fd(), libc::F_GETFL) };
        let err_orig = unsafe { libc::fcntl(err_w.as_raw_fd(), libc::F_GETFL) };
        assert_eq!(in_orig & libc::O_NONBLOCK, 0);
        assert_eq!(out_orig & libc::O_NONBLOCK, 0);
        assert_eq!(err_orig & libc::O_NONBLOCK, 0);

        let (host, guest) = UnixStream::pair().unwrap();
        let (ready_tx, ready_rx) = mpsc::channel();
        let stdin_fd = stdin_r.as_raw_fd();
        let out_fd = out_w.as_raw_fd();
        let err_fd = err_w.as_raw_fd();
        let session = thread::spawn(move || {
            let mut out = OutputSink::Fd(out_fd);
            let mut err = OutputSink::Fd(err_fd);
            run_session_io(
                host,
                [0; 16],
                no_ctrl,
                None,
                stdin_fd,
                out_fd,
                &mut out,
                &mut err,
                || (24, 80),
                SIGINT_DELIVERY_TIMEOUT,
                Some(ready_tx),
            )
        });

        ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        // Every inherited stdio descriptor must stay blocking DURING the active
        // session as well as after exit.
        assert_eq!(
            unsafe { libc::fcntl(stdin_r.as_raw_fd(), libc::F_GETFL) } & libc::O_NONBLOCK,
            0
        );
        assert_eq!(
            unsafe { libc::fcntl(out_w.as_raw_fd(), libc::F_GETFL) } & libc::O_NONBLOCK,
            0
        );
        assert_eq!(
            unsafe { libc::fcntl(err_w.as_raw_fd(), libc::F_GETFL) } & libc::O_NONBLOCK,
            0
        );

        let t = Instant::now();
        unsafe { libc::raise(libc::SIGTERM) };
        let exit = session.join().unwrap().unwrap();
        assert_eq!(exit, SessionExit::Signal(libc::SIGTERM));
        assert!(t.elapsed() < Duration::from_secs(2));

        assert_eq!(
            unsafe { libc::fcntl(stdin_r.as_raw_fd(), libc::F_GETFL) } & libc::O_NONBLOCK,
            0
        );
        assert_eq!(
            unsafe { libc::fcntl(out_w.as_raw_fd(), libc::F_GETFL) } & libc::O_NONBLOCK,
            0
        );
        assert_eq!(
            unsafe { libc::fcntl(err_w.as_raw_fd(), libc::F_GETFL) } & libc::O_NONBLOCK,
            0
        );

        drop(stdin_w);
        drop(stdin_r);
        drop(out_r);
        drop(out_w);
        drop(err_w);
        drop(guest);
    }

    #[test]
    fn stdin_worker_exits_promptly_when_shared_stdin_is_drained_and_keeps_stdin_blocking() {
        let _lock = SIGNAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let (stdin_r, stdin_w) = pipe_cloexec(false).unwrap();
        let dup_r = stdin_r.try_clone().unwrap();
        assert_eq!(
            unsafe { libc::fcntl(stdin_r.as_raw_fd(), libc::F_GETFL) } & libc::O_NONBLOCK,
            0
        );

        let (host, mut guest) = UnixStream::pair().unwrap();
        let (ready_tx, ready_rx) = mpsc::channel();
        let stdin_fd = stdin_r.as_raw_fd();
        let session = thread::spawn(move || {
            let (mut out_buf, mut err_buf) = (Vec::new(), Vec::new());
            let mut out = OutputSink::Writer(&mut out_buf);
            let mut err = OutputSink::Writer(&mut err_buf);
            run_session_io(
                host,
                [0; 16],
                no_ctrl,
                None,
                stdin_fd,
                -1,
                &mut out,
                &mut err,
                || (24, 80),
                SIGINT_DELIVERY_TIMEOUT,
                Some(ready_tx),
            )
        });

        ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        // Inherited stdin must STAY blocking during the active session so rival
        // readers sharing the open file description never see spurious EAGAIN.
        assert_eq!(
            unsafe { libc::fcntl(dup_r.as_raw_fd(), libc::F_GETFL) } & libc::O_NONBLOCK,
            0
        );

        // Race a rival blocking reader against StdinHelper, then let the guest
        // exit while stdin_w is still open (so StdinHelper blocks in read(0));
        // teardown must kill/reap the helper promptly without hanging.
        let mut wf = std::fs::File::from(stdin_w);
        wf.write_all(b"x").unwrap();
        let mut scratch = [0u8; 1];
        let mut pfd = libc::pollfd {
            fd: dup_r.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        if unsafe { libc::poll(&mut pfd, 1, 20) } > 0 {
            let _ = unsafe { libc::read(dup_r.as_raw_fd(), scratch.as_mut_ptr().cast(), 1) };
        }

        let t = Instant::now();
        vm_exec::write(&mut guest, &VmExecMsg::Exit { code: 0 }).unwrap();
        let exit = session.join().unwrap().unwrap();
        assert_eq!(exit, SessionExit::Code(0));
        assert!(t.elapsed() < Duration::from_secs(2));
        assert_eq!(
            unsafe { libc::fcntl(stdin_r.as_raw_fd(), libc::F_GETFL) } & libc::O_NONBLOCK,
            0
        );
        drop(wf);
        drop(dup_r);
        drop(stdin_r);
    }

    #[test]
    fn non_pty_oob_sigint_succeeds_when_both_stdin_and_stdout_are_stalled() {
        let _lock = SIGNAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = test_dir("oob-bidir-ok");
        let ctrl_sock = dir.join("ctrl.sock");
        let ctrl_listener = UnixListener::bind(&ctrl_sock).unwrap();
        let token = [0x99; 16];

        let (stdin_r, stdin_w) = pipe_cloexec(false).unwrap();
        let (out_r, out_w) = pipe_cloexec(false).unwrap();
        let (_err_r, err_w) = pipe_cloexec(false).unwrap();

        let (host, mut guest) = UnixStream::pair().unwrap();
        shrink_socket_pair(&host, &guest, 4096);
        let (ready_tx, ready_rx) = mpsc::channel();
        let stdin_fd = stdin_r.as_raw_fd();
        let out_fd = out_w.as_raw_fd();
        let err_fd = err_w.as_raw_fd();
        let ctrl_path = ctrl_sock.clone();
        let sigint_timeout = Duration::from_millis(120);

        let session = thread::spawn(move || {
            let mut out = OutputSink::Fd(out_fd);
            let mut err = OutputSink::Fd(err_fd);
            run_session_io(
                host,
                token,
                move |dl, stop_r| connect_with_deadline_cancellable(&ctrl_path, dl, Some(stop_r)),
                None,
                stdin_fd,
                out_fd,
                &mut out,
                &mut err,
                || (24, 80),
                sigint_timeout,
                Some(ready_tx),
            )
        });

        ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();

        // 1. Saturate host -> guest stdin while guest never reads stdin.
        let feeder = thread::spawn(move || {
            let mut f = std::fs::File::from(stdin_w);
            let chunk = vec![b'I'; 16 << 10];
            for _ in 0..32 {
                if f.write_all(&chunk).is_err() {
                    break;
                }
            }
        });

        // 2. Saturate guest -> host stdout while downstream `out_r` is paused.
        let chunk = vec![b'O'; 16 << 10];
        let expected_chunks = 20usize;
        let expected_len = chunk.len() * expected_chunks;
        let (acked_tx, acked_rx) = mpsc::channel();
        let guest_handle = thread::spawn(move || {
            for _ in 0..expected_chunks {
                vm_exec::write(&mut guest, &VmExecMsg::Stdout(chunk.clone())).unwrap();
            }
            acked_rx.recv_timeout(Duration::from_secs(3)).unwrap();
            vm_exec::write(&mut guest, &VmExecMsg::Exit { code: 77 }).unwrap();
        });

        // 3. OOB control listener accepts SignalSession and sends SignalAck
        //    even though BOTH stdin and stdout on the data socket are stalled.
        let ctrl_server = thread::spawn(move || {
            let (mut c, _) = ctrl_listener.accept().unwrap();
            assert_eq!(
                vm_exec::read(&mut c).unwrap(),
                Some(VmExecMsg::SignalSession {
                    token,
                    signal: libc::SIGINT,
                })
            );
            vm_exec::write(&mut c, &VmExecMsg::SignalAck).unwrap();
            let _ = acked_tx.send(());
        });

        // Allow both pipes/socket directions to saturate, then raise SIGINT.
        thread::sleep(Duration::from_millis(100));
        unsafe { libc::raise(libc::SIGINT) };

        ctrl_server.join().unwrap();
        // Keep stdout paused WELL PAST `sigint_timeout` to confirm the OOB
        // SignalAck genuinely completed without timing out the session.
        thread::sleep(Duration::from_millis(240));

        // Now resume draining stdout; all bytes and guest exit 77 must arrive.
        let reader = thread::spawn(move || {
            let mut f = std::fs::File::from(out_r);
            let mut buf = Vec::new();
            io::Read::read_to_end(&mut f, &mut buf).unwrap();
            buf
        });

        let exit = session.join().unwrap().unwrap();
        assert_eq!(exit, SessionExit::Code(77));
        drop(out_w);
        drop(err_w);
        let received = reader.join().unwrap();
        assert_eq!(received.len(), expected_len);
        guest_handle.join().unwrap();
        drop(stdin_r);
        let _ = feeder.join();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn non_pty_oob_sigint_aborts_when_both_sinks_stalled_and_guest_never_acks() {
        let _lock = SIGNAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = test_dir("oob-bidir-abort");
        let ctrl_sock = dir.join("ctrl.sock");
        let ctrl_listener = UnixListener::bind(&ctrl_sock).unwrap();
        let ctrl_server = thread::spawn(move || {
            // Accept OOB connection so local send succeeds, but stay silent.
            let (_c, _) = ctrl_listener.accept().unwrap();
            thread::sleep(Duration::from_millis(500));
        });

        let (stdin_r, stdin_w) = pipe_cloexec(false).unwrap();
        let (out_r, out_w) = pipe_cloexec(false).unwrap();
        let (_err_r, err_w) = pipe_cloexec(false).unwrap();

        let (host, mut guest) = UnixStream::pair().unwrap();
        shrink_socket_pair(&host, &guest, 4096);
        let (ready_tx, ready_rx) = mpsc::channel();
        let stdin_fd = stdin_r.as_raw_fd();
        let out_fd = out_w.as_raw_fd();
        let err_fd = err_w.as_raw_fd();
        let ctrl_path = ctrl_sock.clone();

        let session = thread::spawn(move || {
            let mut out = OutputSink::Fd(out_fd);
            let mut err = OutputSink::Fd(err_fd);
            run_session_io(
                host,
                [0x55; 16],
                move |dl, stop_r| connect_with_deadline_cancellable(&ctrl_path, dl, Some(stop_r)),
                None,
                stdin_fd,
                out_fd,
                &mut out,
                &mut err,
                || (24, 80),
                Duration::from_millis(120),
                Some(ready_tx),
            )
        });

        ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let feeder = thread::spawn(move || {
            let mut f = std::fs::File::from(stdin_w);
            let chunk = vec![b'I'; 16 << 10];
            for _ in 0..32 {
                if f.write_all(&chunk).is_err() {
                    break;
                }
            }
        });
        let guest_flood = thread::spawn(move || {
            let chunk = vec![b'O'; 16 << 10];
            for _ in 0..32 {
                if vm_exec::write(&mut guest, &VmExecMsg::Stdout(chunk.clone())).is_err() {
                    return;
                }
            }
        });
        thread::sleep(Duration::from_millis(120));

        // Even with BOTH stdin and stdout stalled, a single SIGINT MUST time
        // out cleanly when the guest never acknowledges OOB SignalSession.
        let t = Instant::now();
        unsafe { libc::raise(libc::SIGINT) };
        let exit = session.join().unwrap().unwrap();
        assert_eq!(exit, SessionExit::Signal(libc::SIGINT));
        assert!(t.elapsed() < Duration::from_secs(2));

        let _ = guest_flood.join();
        let _ = ctrl_server.join();
        drop(out_r);
        drop(out_w);
        drop(err_w);
        drop(stdin_r);
        let _ = feeder.join();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn non_pty_second_sigint_cancels_even_when_host_stdout_is_blocked() {
        let _lock = SIGNAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = test_dir("oob-second-sigint");
        let ctrl_sock = dir.join("ctrl.sock");
        let ctrl_listener = UnixListener::bind(&ctrl_sock).unwrap();
        let ctrl_server = thread::spawn(move || {
            let (_c, _) = ctrl_listener.accept().unwrap();
            thread::sleep(Duration::from_millis(500));
        });

        let (stdin_r, stdin_w) = pipe_cloexec(false).unwrap();
        let (out_r, out_w) = pipe_cloexec(false).unwrap();
        let (_err_r, err_w) = pipe_cloexec(false).unwrap();

        let (host, mut guest) = UnixStream::pair().unwrap();
        let (ready_tx, ready_rx) = mpsc::channel();
        let stdin_fd = stdin_r.as_raw_fd();
        let out_fd = out_w.as_raw_fd();
        let err_fd = err_w.as_raw_fd();
        let ctrl_path = ctrl_sock.clone();

        let session = thread::spawn(move || {
            let mut out = OutputSink::Fd(out_fd);
            let mut err = OutputSink::Fd(err_fd);
            run_session_io(
                host,
                [0x33; 16],
                move |dl, stop_r| connect_with_deadline_cancellable(&ctrl_path, dl, Some(stop_r)),
                None,
                stdin_fd,
                out_fd,
                &mut out,
                &mut err,
                || (24, 80),
                Duration::from_secs(5),
                Some(ready_tx),
            )
        });

        ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let guest_flood = thread::spawn(move || {
            let chunk = vec![b'P'; 16 << 10];
            for _ in 0..32 {
                if vm_exec::write(&mut guest, &VmExecMsg::Stdout(chunk.clone())).is_err() {
                    return;
                }
            }
        });
        thread::sleep(Duration::from_millis(120));

        // First SIGINT awaits OOB ack (5s timeout); second SIGINT must cancel
        // immediately without waiting for the timeout.
        unsafe { libc::raise(libc::SIGINT) };
        thread::sleep(Duration::from_millis(60));
        let t = Instant::now();
        unsafe { libc::raise(libc::SIGINT) };
        let exit = session.join().unwrap().unwrap();
        assert_eq!(exit, SessionExit::Signal(libc::SIGINT));
        assert!(t.elapsed() < Duration::from_secs(2));
        assert_eq!(
            unsafe { libc::fcntl(out_w.as_raw_fd(), libc::F_GETFL) } & libc::O_NONBLOCK,
            0
        );

        let _ = guest_flood.join();
        let _ = ctrl_server.join();
        drop(out_r);
        drop(out_w);
        drop(err_w);
        drop(stdin_w);
        drop(stdin_r);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn non_pty_sigint_aborts_cleanly_when_stdin_is_backpressured() {
        let _lock = SIGNAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = test_dir("oob-stdin-bp");
        let ctrl_sock = dir.join("ctrl.sock");
        let ctrl_listener = UnixListener::bind(&ctrl_sock).unwrap();
        let ctrl_server = thread::spawn(move || {
            let (_c, _) = ctrl_listener.accept().unwrap();
            thread::sleep(Duration::from_millis(400));
        });

        let (stdin_r, stdin_w) = pipe_cloexec(false).unwrap();
        let (host, guest) = UnixStream::pair().unwrap();
        shrink_socket_pair(&host, &guest, 4096);

        let (ready_tx, ready_rx) = mpsc::channel();
        let stdin_fd = stdin_r.as_raw_fd();
        let ctrl_path = ctrl_sock.clone();
        let session = thread::spawn(move || {
            let (mut out_buf, mut err_buf) = (Vec::new(), Vec::new());
            let mut out = OutputSink::Writer(&mut out_buf);
            let mut err = OutputSink::Writer(&mut err_buf);
            run_session_io(
                host,
                [0x22; 16],
                move |dl, stop_r| connect_with_deadline_cancellable(&ctrl_path, dl, Some(stop_r)),
                None,
                stdin_fd,
                -1,
                &mut out,
                &mut err,
                || (24, 80),
                Duration::from_millis(120),
                Some(ready_tx),
            )
        });

        ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        // Feed stdin while `guest` never reads so writer_worker blocks on POLLOUT.
        let feeder = thread::spawn(move || {
            let mut f = std::fs::File::from(stdin_w);
            let chunk = vec![b'I'; 16 << 10];
            for _ in 0..32 {
                if f.write_all(&chunk).is_err() {
                    break;
                }
            }
        });
        thread::sleep(Duration::from_millis(80));

        let t = Instant::now();
        unsafe { libc::raise(libc::SIGINT) };
        let exit = session.join().unwrap().unwrap();
        assert_eq!(exit, SessionExit::Signal(libc::SIGINT));
        assert!(t.elapsed() < Duration::from_secs(2));

        let _ = ctrl_server.join();
        drop(stdin_r);
        drop(guest);
        let _ = feeder.join();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn broken_pipe_shuts_down_socket_and_joins_backpressured_stdin_worker() {
        let _lock = SIGNAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let (stdin_r, stdin_w) = pipe_cloexec(false).unwrap();
        let (host, mut guest) = UnixStream::pair().unwrap();
        shrink_socket_pair(&host, &guest, 4096);

        let (ready_tx, ready_rx) = mpsc::channel();
        let stdin_fd = stdin_r.as_raw_fd();
        let session = thread::spawn(move || {
            let mut broken = BrokenPipeWriter;
            let mut err_buf = Vec::new();
            let mut out = OutputSink::Writer(&mut broken);
            let mut err = OutputSink::Writer(&mut err_buf);
            run_session_io(
                host,
                [0; 16],
                no_ctrl,
                None,
                stdin_fd,
                -1,
                &mut out,
                &mut err,
                || (24, 80),
                SIGINT_DELIVERY_TIMEOUT,
                Some(ready_tx),
            )
        });

        ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let feeder = thread::spawn(move || {
            let mut f = std::fs::File::from(stdin_w);
            let chunk = vec![b'B'; 16 << 10];
            for _ in 0..32 {
                if f.write_all(&chunk).is_err() {
                    break;
                }
            }
        });
        thread::sleep(Duration::from_millis(80));

        // Peer remains open and sends Stdout while never draining stdin; the
        // sink returns BrokenPipe, and run_session_io must shut down the socket
        // and join writer_worker promptly instead of hanging.
        let t = Instant::now();
        vm_exec::write(&mut guest, &VmExecMsg::Stdout(b"boom".to_vec())).unwrap();
        let exit = session.join().unwrap().unwrap();
        assert_eq!(exit, SessionExit::Code(128 + libc::SIGPIPE));
        assert!(t.elapsed() < Duration::from_secs(2));

        // Peer must see EOF once buffered frames are drained.
        let mut ds = DeadlineStream::new(
            &mut guest,
            Instant::now() + Duration::from_secs(2),
            "timed out waiting for peer EOF".into(),
        );
        let mut saw_eof = false;
        for _ in 0..64 {
            match vm_exec::read(&mut ds) {
                Ok(None) => {
                    saw_eof = true;
                    break;
                }
                Ok(Some(_)) => continue,
                Err(_) => {
                    saw_eof = true;
                    break;
                }
            }
        }
        drop(ds);
        assert!(saw_eof, "peer never observed socket shutdown");

        drop(stdin_r);
        drop(guest);
        let _ = feeder.join();
    }

    #[test]
    fn resize_watcher_compares_first_check_against_initial_spec_size() {
        let _lock = SIGNAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let (master, slave) = open_test_pty(40, 120);
        let (host, mut guest) = UnixStream::pair().unwrap();
        let slave_fd = slave.as_raw_fd();

        let session = thread::spawn(move || {
            let (mut out_buf, mut err_buf) = (Vec::new(), Vec::new());
            let mut out = OutputSink::Writer(&mut out_buf);
            let mut err = OutputSink::Writer(&mut err_buf);
            run_session_io(
                host,
                [0; 16],
                no_ctrl,
                Some((24, 80)),
                slave_fd,
                slave_fd,
                &mut out,
                &mut err,
                || (40, 120),
                SIGINT_DELIVERY_TIMEOUT,
                None,
            )
        });

        guest
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        assert_eq!(
            vm_exec::read(&mut guest).unwrap(),
            Some(VmExecMsg::Resize {
                rows: 40,
                cols: 120,
            })
        );
        vm_exec::write(&mut guest, &VmExecMsg::Exit { code: 0 }).unwrap();
        assert_eq!(session.join().unwrap().unwrap(), SessionExit::Code(0));
        drop(master);
        drop(slave);
    }

    struct SharedVec(Arc<Mutex<Vec<u8>>>);
    impl Write for SharedVec {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    struct BrokenPipeWriter;
    impl Write for BrokenPipeWriter {
        fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "broken pipe"))
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
}
