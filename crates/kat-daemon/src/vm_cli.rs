//! `kat vm shell` and `kat vm exec`: root shell and command execution inside
//! the Linux VM's own namespaces (not an LXC machine), relayed over
//! `<home>/run/vm-exec.sock` (see [`kat_proto::vm_exec`]).

use std::io::{self, IsTerminal, Write};
use std::net::Shutdown;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex, mpsc};
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
    let conn = connect_and_start(&spec)?;
    match run_session(conn, initial_tty_size)? {
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
    unsafe { libc::isatty(fd) == 1 }
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
        let mut fds = [0; 2];
        if unsafe { libc::pipe(fds.as_mut_ptr()) } < 0 {
            return Err(io::Error::last_os_error());
        }
        let sig_r = unsafe { OwnedFd::from_raw_fd(fds[0]) };
        let sig_w = unsafe { OwnedFd::from_raw_fd(fds[1]) };
        unsafe {
            libc::fcntl(sig_r.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC);
            libc::fcntl(sig_w.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC);
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

/// Connect to a Unix domain stream socket while honoring an absolute `deadline`.
fn connect_with_deadline(path: &Path, deadline: Instant) -> io::Result<UnixStream> {
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
                let rem = deadline.saturating_duration_since(Instant::now());
                if rem.is_zero() {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "VM exec connect timed out",
                    ));
                }
                let mut pfd = libc::pollfd {
                    fd: fd.as_raw_fd(),
                    events: libc::POLLOUT,
                    revents: 0,
                };
                let timeout_ms = rem.as_millis().clamp(1, i32::MAX as u128) as i32;
                let prc = unsafe { libc::poll(&mut pfd, 1, timeout_ms) };
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
    let mut ds = DeadlineStream::new(
        &mut conn,
        deadline,
        "VM exec readiness probe timed out".into(),
    );
    if let Err(e) = vm_exec::write(&mut ds, &VmExecMsg::Ping) {
        if matches!(
            e.kind(),
            io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset
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
    match vm_exec::read(&mut ds) {
        Ok(Some(VmExecMsg::Pong)) => Probe::Ready(conn),
        Ok(Some(VmExecMsg::Error(e))) => Probe::Failed(anyhow::anyhow!("{e}")),
        Ok(Some(other)) => Probe::Failed(anyhow::anyhow!("unexpected guest response {other:?}")),
        Ok(None) => Probe::ClosedBeforeReady,
        Err(e)
            if matches!(
                e.kind(),
                io::ErrorKind::ConnectionReset
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
) -> anyhow::Result<UnixStream> {
    {
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
            Ok(Some(VmExecMsg::Started)) => {}
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
    }
    // Clear any socket timeouts before entering the untimed interactive session
    // (best-effort if the peer closed immediately after sending `Started`).
    let _ = conn.set_read_timeout(None);
    let _ = conn.set_write_timeout(None);
    Ok(conn)
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
fn connect_and_start(spec: &ExecSpec) -> anyhow::Result<UnixStream> {
    let home = crate::state_dir()?;
    let path = socket_path(&home);

    let fast_deadline = Instant::now() + HANDSHAKE_TIMEOUT;
    match probe_ready(&path, fast_deadline) {
        Probe::Ready(conn) => {
            return submit_exec(conn, spec, Instant::now() + HANDSHAKE_TIMEOUT);
        }
        Probe::Failed(e) => return Err(e),
        Probe::ClosedBeforeReady => {
            // The VM process bound the Unix socket, but kat-init may still be
            // finishing boot before listening on VM_EXEC_PORT.
            let conn = wait_for_ready_socket(&path, fast_deadline, || Ok(None))?;
            return submit_exec(conn, spec, Instant::now() + HANDSHAKE_TIMEOUT);
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
                return submit_exec(conn, spec, Instant::now() + HANDSHAKE_TIMEOUT);
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
    submit_exec(conn, spec, Instant::now() + HANDSHAKE_TIMEOUT)
}

/// Relay stdio for an already-`Started` VM exec session; returns how the
/// session terminated.
fn run_session(
    conn: UnixStream,
    initial_tty_size: Option<(u16, u16)>,
) -> anyhow::Result<SessionExit> {
    run_session_io(
        conn,
        initial_tty_size,
        0,
        1,
        io::stdout(),
        io::stderr(),
        window_size,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn run_session_io(
    conn: UnixStream,
    initial_tty_size: Option<(u16, u16)>,
    stdin_fd: RawFd,
    stdout_fd: RawFd,
    mut out: impl Write,
    mut err: impl Write,
    get_window_size: impl Fn() -> (u16, u16) + Send + 'static,
    on_ready: Option<mpsc::Sender<()>>,
) -> anyhow::Result<SessionExit> {
    let allocate_pty = initial_tty_size.is_some();
    let stdin_is_tty = is_tty(stdin_fd);
    let stdout_is_tty = is_tty(stdout_fd);
    let want_raw = allocate_pty && stdin_is_tty;

    // Install signal wakeups BEFORE entering raw mode so catchable termination
    // signals always unwind and restore host termios.
    let signals: &[libc::c_int] = if want_raw {
        &[libc::SIGTERM, libc::SIGHUP, libc::SIGINT, libc::SIGQUIT]
    } else {
        &[libc::SIGINT]
    };
    let sig_setup = SignalGuard::install(signals).ok();

    let raw = if want_raw {
        RawMode::enter_fd(stdin_fd).ok()
    } else {
        None
    };
    let raw_active = raw.is_some();

    let w = Arc::new(Mutex::new(conn.try_clone()?));
    let terminated_by = Arc::new(AtomicI32::new(0));

    let mut sig_guard = sig_setup.map(|(mut guard, sig_r)| {
        let w = w.clone();
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
                if raw_active {
                    terminated_by.store(sig, Ordering::Release);
                    let _ = w.lock().unwrap().shutdown(Shutdown::Both);
                    return;
                }
                // Non-PTY (or non-raw) session: forward SIGINT to the guest
                // process group and keep the relay alive for its exit status.
                if vm_exec::write(&mut *w.lock().unwrap(), &VmExecMsg::Signal { signal: sig })
                    .is_err()
                {
                    return;
                }
            }
        });
        guard.attach_dispatcher(handle);
        guard
    });

    if let Some(tx) = on_ready {
        let _ = tx.send(());
    }

    let mut stop_fds = [0; 2];
    if unsafe { libc::pipe(stop_fds.as_mut_ptr()) } < 0 {
        return Err(io::Error::last_os_error().into());
    }
    let stop_r = unsafe { OwnedFd::from_raw_fd(stop_fds[0]) };
    let stop_w = unsafe { OwnedFd::from_raw_fd(stop_fds[1]) };
    unsafe {
        libc::fcntl(stop_r.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC);
        libc::fcntl(stop_w.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC);
    }

    let stdin_thread = if stdin_fd >= 0 {
        let w = w.clone();
        let stop_fd = stop_r.try_clone()?;
        Some(thread::spawn(move || {
            let mut buf = vec![0u8; CHUNK];
            loop {
                let mut pfds = [
                    libc::pollfd {
                        fd: stdin_fd,
                        events: libc::POLLIN,
                        revents: 0,
                    },
                    libc::pollfd {
                        fd: stop_fd.as_raw_fd(),
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
                if pfds[1].revents != 0 {
                    return;
                }
                if pfds[0].revents == 0 {
                    continue;
                }
                let n = loop {
                    let rc = unsafe { libc::read(stdin_fd, buf.as_mut_ptr().cast(), buf.len()) };
                    if rc >= 0 {
                        break rc as usize;
                    }
                    if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                        continue;
                    }
                    break 0;
                };
                let msg = if n == 0 {
                    VmExecMsg::DataEnd
                } else {
                    VmExecMsg::Data(buf[..n].to_vec())
                };
                if vm_exec::write(&mut *w.lock().unwrap(), &msg).is_err() || n == 0 {
                    return;
                }
            }
        }))
    } else {
        None
    };

    let resize_thread = if let Some(mut last) = initial_tty_size
        && (stdin_is_tty || stdout_is_tty)
    {
        let w = w.clone();
        let stop_fd = stop_r.try_clone()?;
        Some(thread::spawn(move || {
            loop {
                // Compare immediately against the dimensions sent in ExecSpec
                // so a resize during VM startup is delivered right away.
                let now = get_window_size();
                if now != last {
                    last = now;
                    let m = VmExecMsg::Resize {
                        rows: now.0,
                        cols: now.1,
                    };
                    if vm_exec::write(&mut *w.lock().unwrap(), &m).is_err() {
                        return;
                    }
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

    let res = relay_output(conn, &mut out, &mut err);
    drop(stop_w);
    if let Some(h) = stdin_thread {
        let _ = h.join();
    }
    if let Some(h) = resize_thread {
        let _ = h.join();
    }
    drop(stop_r);
    drop(raw);
    drop(sig_guard.take());

    let sig = terminated_by.load(Ordering::Acquire);
    if sig != 0 {
        return Ok(SessionExit::Signal(sig));
    }
    res.map(SessionExit::Code)
}

/// Read output/exit frames from `conn` into `out` and `err`.
fn relay_output(
    mut conn: UnixStream,
    out: &mut impl Write,
    err: &mut impl Write,
) -> anyhow::Result<i32> {
    loop {
        match vm_exec::read(&mut conn)? {
            Some(VmExecMsg::Stdout(d)) => {
                if let Err(e) = out.write_all(&d).and_then(|()| out.flush()) {
                    if e.kind() == io::ErrorKind::BrokenPipe {
                        return Ok(128 + libc::SIGPIPE);
                    }
                    return Err(e.into());
                }
            }
            Some(VmExecMsg::Stderr(d)) => {
                if let Err(e) = err.write_all(&d).and_then(|()| err.flush()) {
                    if e.kind() == io::ErrorKind::BrokenPipe {
                        return Ok(128 + libc::SIGPIPE);
                    }
                    return Err(e.into());
                }
            }
            Some(VmExecMsg::Exit { code }) => return Ok(code),
            Some(VmExecMsg::Error(e)) => bail!("{e}"),
            Some(_) => {}
            None => bail!("the guest closed the connection"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::net::UnixListener;
    use std::sync::atomic::{AtomicUsize, Ordering};

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

        // `kat vm exec -- <cmd>`
        let VmCmd::Exec(e) = parse(&["vm", "exec", "--", "uname", "-a"]) else {
            panic!()
        };
        assert!(!e.tty && e.workdir.is_none() && e.cmd == ["uname", "-a"]);

        // `kat vm exec <cmd>` without `--` keeps command flags intact.
        let VmCmd::Exec(e) = parse(&["vm", "exec", "ls", "-t", "-la"]) else {
            panic!()
        };
        assert!(!e.tty && e.cmd == ["ls", "-t", "-la"]);

        // Long `--tty` flag works both with and without `--`.
        let VmCmd::Exec(e) = parse(&["vm", "exec", "--tty", "--", "top", "-b"]) else {
            panic!()
        };
        assert!(e.tty && e.cmd == ["top", "-b"]);
        let VmCmd::Exec(e) = parse(&["vm", "exec", "--tty", "top", "-b"]) else {
            panic!()
        };
        assert!(e.tty && e.cmd == ["top", "-b"]);

        // Flags before command or `--`.
        let VmCmd::Exec(e) = parse(&[
            "vm", "exec", "-t", "-w", "/etc", "-e", "A=1", "-e", "B=2", "--", "sh", "-c", "id",
        ]) else {
            panic!()
        };
        assert!(e.tty);
        assert_eq!(e.workdir.as_deref(), Some("/etc"));
        assert_eq!(e.env, ["A=1", "B=2"]);
        assert_eq!(e.cmd, ["sh", "-c", "id"]);

        // Missing command fails.
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

    #[test]
    fn relays_stdout_stderr_and_exit_code() {
        let (host, mut guest) = UnixStream::pair().unwrap();
        let peer = thread::spawn(move || {
            vm_exec::write(&mut guest, &VmExecMsg::Stdout(b"hello ".to_vec())).unwrap();
            vm_exec::write(&mut guest, &VmExecMsg::Stderr(b"warn\n".to_vec())).unwrap();
            vm_exec::write(&mut guest, &VmExecMsg::Stdout(b"vm\n".to_vec())).unwrap();
            vm_exec::write(&mut guest, &VmExecMsg::Exit { code: 7 }).unwrap();
        });
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let code = relay_output(host, &mut out, &mut err).unwrap();
        peer.join().unwrap();
        assert_eq!(code, 7);
        assert_eq!(out, b"hello vm\n");
        assert_eq!(err, b"warn\n");
    }

    #[test]
    fn surfaces_guest_errors_and_eof() {
        let (host, mut guest) = UnixStream::pair().unwrap();
        vm_exec::write(&mut guest, &VmExecMsg::Error("boom".into())).unwrap();
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let e = relay_output(host, &mut out, &mut err).unwrap_err();
        assert!(e.to_string().contains("boom"), "{e}");

        let (host, guest) = UnixStream::pair().unwrap();
        drop(guest);
        let e = relay_output(host, &mut out, &mut err).unwrap_err();
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
            // 1. Early close during Ping (e.g. guest still booting): safe to retry.
            let (conn, _) = listener.accept().unwrap();
            drop(conn);
            // 2. Pong succeeds, Exec arrives, then connection drops before Started.
            let (mut conn, _) = listener.accept().unwrap();
            assert_eq!(vm_exec::read(&mut conn).unwrap(), Some(VmExecMsg::Ping));
            vm_exec::write(&mut conn, &VmExecMsg::Pong).unwrap();
            assert!(matches!(
                vm_exec::read(&mut conn).unwrap(),
                Some(VmExecMsg::Exec(_))
            ));
            exec_count_srv.fetch_add(1, Ordering::SeqCst);
            drop(conn);
            // 3. Successful Ping -> Pong -> Exec -> Started clears timeouts.
            let (mut conn, _) = listener.accept().unwrap();
            assert_eq!(vm_exec::read(&mut conn).unwrap(), Some(VmExecMsg::Ping));
            vm_exec::write(&mut conn, &VmExecMsg::Pong).unwrap();
            assert!(matches!(
                vm_exec::read(&mut conn).unwrap(),
                Some(VmExecMsg::Exec(_))
            ));
            exec_count_srv.fetch_add(1, Ordering::SeqCst);
            vm_exec::write(&mut conn, &VmExecMsg::Started).unwrap();
        });

        let spec = ExecSpec {
            argv: vec!["true".into()],
            ..Default::default()
        };

        // Connection 1 closes on Ping; wait_for_ready_socket retries Ping and
        // hands back connection 2 once Pong arrives.
        let ready =
            wait_for_ready_socket(&sock, Instant::now() + Duration::from_secs(2), || Ok(None))
                .unwrap();
        // Connection 2 drops after receiving Exec: MUST return uncertain-outcome
        // error and NEVER replay Exec.
        let err = submit_exec(ready, &spec, Instant::now() + Duration::from_secs(2)).unwrap_err();
        assert!(err.to_string().contains("unknown"), "{err:#}");
        assert_eq!(exec_count.load(Ordering::SeqCst), 1);

        // Connection 3 succeeds and clears socket timeouts before returning.
        let ready =
            wait_for_ready_socket(&sock, Instant::now() + Duration::from_secs(2), || Ok(None))
                .unwrap();
        let conn = submit_exec(ready, &spec, Instant::now() + Duration::from_secs(2)).unwrap();
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
            // 1. Silent during Ping.
            let (_c1, _) = listener.accept().unwrap();
            thread::sleep(Duration::from_millis(400));
            // 2. Answers Pong, then goes silent after Exec.
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

        // Bind and serve Pong after a short delay while Docker remains Starting.
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

        // Terminal VM error is surfaced immediately instead of waiting for timeout.
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
    fn host_pty_restores_termios_on_sigterm() {
        let _lock = SIGNAL_TEST_LOCK.lock().unwrap();
        let (mut master_raw, mut slave_raw) = (0, 0);
        let mut ws = libc::winsize {
            ws_row: 24,
            ws_col: 80,
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
        let master = unsafe { OwnedFd::from_raw_fd(master_raw) };
        let slave = unsafe { OwnedFd::from_raw_fd(slave_raw) };

        let mut before: libc::termios = unsafe { std::mem::zeroed() };
        assert_eq!(
            unsafe { libc::tcgetattr(slave.as_raw_fd(), &mut before) },
            0
        );
        assert_ne!(before.c_lflag & libc::ICANON, 0);
        assert_ne!(before.c_lflag & libc::ECHO, 0);

        let (host, mut guest) = UnixStream::pair().unwrap();
        let (ready_tx, ready_rx) = mpsc::channel();
        let slave_fd = slave.as_raw_fd();
        let session = thread::spawn(move || {
            run_session_io(
                host,
                Some((24, 80)),
                slave_fd,
                slave_fd,
                Vec::new(),
                Vec::new(),
                || (24, 80),
                Some(ready_tx),
            )
        });

        ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        // Verify raw mode is active while the session runs.
        let mut during: libc::termios = unsafe { std::mem::zeroed() };
        assert_eq!(
            unsafe { libc::tcgetattr(slave.as_raw_fd(), &mut during) },
            0
        );
        assert_eq!(during.c_lflag & libc::ICANON, 0);

        // Terminate with SIGTERM: session must unwind, restore termios, and
        // report Signal(SIGTERM).
        unsafe { libc::raise(libc::SIGTERM) };
        let exit = session.join().unwrap().unwrap();
        assert_eq!(exit, SessionExit::Signal(libc::SIGTERM));

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

        // Guest sees EOF because the CLI shut down the socket.
        assert_eq!(vm_exec::read(&mut guest).unwrap(), None);
        drop(master);
        drop(slave);
    }

    #[test]
    fn non_pty_forwards_sigint_without_closing_session() {
        let _lock = SIGNAL_TEST_LOCK.lock().unwrap();
        let mut fds = [0; 2];
        assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
        let stdin_r = unsafe { OwnedFd::from_raw_fd(fds[0]) };
        let stdin_w = unsafe { OwnedFd::from_raw_fd(fds[1]) };

        let (host, mut guest) = UnixStream::pair().unwrap();
        let (ready_tx, ready_rx) = mpsc::channel();
        let out_buf = Arc::new(Mutex::new(Vec::new()));
        let out_writer = SharedVec(out_buf.clone());
        let stdin_fd = stdin_r.as_raw_fd();

        let session = thread::spawn(move || {
            run_session_io(
                host,
                None,
                stdin_fd,
                -1,
                out_writer,
                Vec::new(),
                || (24, 80),
                Some(ready_tx),
            )
        });

        ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        unsafe { libc::raise(libc::SIGINT) };

        // Guest receives VmExecMsg::Signal(SIGINT) and relays output + exit.
        assert_eq!(
            vm_exec::read(&mut guest).unwrap(),
            Some(VmExecMsg::Signal {
                signal: libc::SIGINT
            })
        );
        vm_exec::write(&mut guest, &VmExecMsg::Stdout(b"caught-int\n".to_vec())).unwrap();
        vm_exec::write(&mut guest, &VmExecMsg::Exit { code: 77 }).unwrap();

        let exit = session.join().unwrap().unwrap();
        assert_eq!(exit, SessionExit::Code(77));
        assert_eq!(&*out_buf.lock().unwrap(), b"caught-int\n");
        drop(stdin_w);
        drop(stdin_r);
    }

    #[test]
    fn resize_watcher_compares_first_check_against_initial_spec_size() {
        let _lock = SIGNAL_TEST_LOCK.lock().unwrap();
        let (mut master_raw, mut slave_raw) = (0, 0);
        let mut ws = libc::winsize {
            ws_row: 40,
            ws_col: 120,
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
        let master = unsafe { OwnedFd::from_raw_fd(master_raw) };
        let slave = unsafe { OwnedFd::from_raw_fd(slave_raw) };
        let (host, mut guest) = UnixStream::pair().unwrap();
        let slave_fd = slave.as_raw_fd();

        // ExecSpec sent (24, 80), but by the time the session starts the
        // terminal has already been resized to (40, 120).
        let session = thread::spawn(move || {
            run_session_io(
                host,
                Some((24, 80)),
                slave_fd,
                slave_fd,
                Vec::new(),
                Vec::new(),
                || (40, 120),
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
}
