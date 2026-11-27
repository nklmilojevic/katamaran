//! `kat vm shell` and `kat vm exec`: root shell and command execution inside
//! the Linux VM's own namespaces (not an LXC machine), relayed over
//! `<home>/run/vm-exec.sock` (see [`kat_proto::vm_exec`]).

use std::collections::VecDeque;
use std::ffi::CString;
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
    /// Do not forward stdin: the command reads end-of-file immediately and
    /// none of the caller's input is consumed (like `ssh -n`).
    #[arg(short = 'n', long = "no-stdin")]
    pub no_stdin: bool,
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
    run_spec(args.workdir, &args.env, args.cmd, allocate_pty, false)
}

fn exec_cmd(args: VmExecArgs) -> anyhow::Result<()> {
    run_spec(args.workdir, &args.env, args.cmd, args.tty, args.no_stdin)
}

/// Reopen any closed standard descriptor (`kat vm exec ... >&-`) on
/// `/dev/null`, so it can neither fail local I/O setup nor be silently reused
/// by a later `open`/`pipe` and mistaken for the caller's stdio.
fn ensure_stdio_open() {
    for fd in 0..3 {
        if unsafe { libc::fcntl(fd, libc::F_GETFD) } < 0 {
            let flags = if fd == 0 {
                libc::O_RDONLY
            } else {
                libc::O_WRONLY
            };
            let nfd = unsafe { libc::open(c"/dev/null".as_ptr(), flags) };
            if nfd >= 0 && nfd != fd {
                unsafe {
                    libc::dup2(nfd, fd);
                    libc::close(nfd);
                }
            }
        }
    }
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
    no_stdin: bool,
) -> anyhow::Result<()> {
    ensure_stdio_open();
    let spec = build_spec(workdir, extra_env, argv, allocate_pty)?;
    let initial_tty_size = spec.tty.as_ref().map(|t| (t.rows, t.cols));
    // Every fallible local setup step (helper processes, pipes) happens BEFORE
    // the command is submitted, so a local failure never leaves a root command
    // running in the VM with an unknown outcome.
    let local = LocalIo::prepare(Some(1), Some(2), no_stdin)
        .context("cannot set up local terminal I/O for the VM session")?;
    let (conn, session_token, sock_path) = connect_and_start(&spec)?;
    match run_session(
        conn,
        session_token,
        sock_path,
        initial_tty_size,
        spec.output_window,
        local,
    )? {
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
    // Non-PTY sessions are flow-controlled per stream so a stalled stdout
    // reader can never block stderr (see `kat_proto::vm_exec`).
    let output_window = if tty.is_none() {
        vm_exec::OUTPUT_WINDOW_DEFAULT
    } else {
        0
    };
    let spec = ExecSpec {
        cwd,
        env,
        argv,
        tty,
        output_window,
    };
    // Reject an oversized command line/environment locally, before anything
    // is sent, so the outcome is never ambiguous.
    if vm_exec::write(&mut io::sink(), &VmExecMsg::Exec(spec.clone())).is_err() {
        bail!(
            "the command and environment are too large to send to the VM (limit {} KiB)",
            kat_proto::MAX_FRAME >> 10
        );
    }
    Ok(spec)
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

const RELAY_DEST_STDOUT: u8 = 1;
const RELAY_DEST_STDERR: u8 = 2;

#[derive(Debug)]
enum RelayFinish {
    Drained,
    BrokenPipe,
    Error(io::Error),
    Cancelled,
}

fn dup_ge(fd: RawFd, min_fd: RawFd) -> io::Result<OwnedFd> {
    let raw = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, min_fd) };
    if raw < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(raw) })
}

fn needs_blocking_relay(fd: RawFd) -> bool {
    if fd < 0 {
        return false;
    }
    let fl = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if fl < 0 || (fl & libc::O_NONBLOCK) != 0 {
        return false;
    }
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    !(unsafe { libc::fstat(fd, &mut st) } == 0 && (st.st_mode & libc::S_IFMT) == libc::S_IFREG)
}

/// Entrypoint for the hidden `katamaran vm-io-relay` helper process.
///
/// Reads destination-tagged frames from fd 3 and writes them in strict FIFO
/// order to inherited stdout (fd 4) or stderr (fd 5), reporting any write
/// `errno` on fd 6 before exiting and signalling downstream write progress with
/// a byte on the nonblocking fd 7. Never touches fds 0/1/2 (`/dev/null`).
pub fn run_io_relay_helper() -> ! {
    let code = run_output_relay_fds(3, 4, 5, 6, 7);
    unsafe { libc::_exit(code) }
}

/// Entrypoint for the hidden `katamaran vm-stdin-relay` helper process
/// (fds: 3 = caller's blocking stdin, 4 = private pipe to the session,
/// 5 = death pipe whose write end only the session holds).
pub fn run_stdin_relay_helper() -> ! {
    let code = run_stdin_relay_fds(3, 4, 5);
    unsafe { libc::_exit(code) }
}

/// Copy `in_fd` to `out_fd` without ever touching `in_fd`'s file flags. Reads
/// only when `in_fd` is readable AND the session is still alive: once
/// `death_fd` reports hangup (the session ended or was killed, even by
/// `SIGKILL`), the helper exits without consuming any more of the caller's
/// input.
fn run_stdin_relay_fds(in_fd: RawFd, out_fd: RawFd, death_fd: RawFd) -> i32 {
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);
    }
    let mut buf = vec![0u8; CHUNK];
    loop {
        let mut pfds = [
            libc::pollfd {
                fd: death_fd,
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: in_fd,
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        let rc = unsafe { libc::poll(pfds.as_mut_ptr(), 2, -1) };
        if rc < 0 {
            if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return 1;
        }
        if pfds[0].revents != 0 {
            return 0;
        }
        if pfds[1].revents == 0 {
            continue;
        }
        // Read at most what is available right now, so a concurrent reader of
        // a shared stdin draining it first can leave this helper blocked in
        // `read` (and blind to `death_fd`) for at most one byte's worth of
        // input. Readable-with-0-available (EOF, terminal ^D) reads 1 byte,
        // which returns at once.
        let mut avail: libc::c_int = 0;
        let want = if unsafe { libc::ioctl(in_fd, libc::FIONREAD, &mut avail) } == 0 {
            (avail.max(1) as usize).min(buf.len())
        } else {
            buf.len()
        };
        let n = unsafe { libc::read(in_fd, buf.as_mut_ptr().cast(), want) };
        if n < 0 {
            match io::Error::last_os_error().kind() {
                io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock => continue,
                _ => return 1,
            }
        }
        if n == 0 {
            return 0;
        }
        if relay_write_all(out_fd, &buf[..n as usize], -1).is_err() {
            return 0;
        }
    }
}

fn relay_read_exact(fd: RawFd, buf: &mut [u8]) -> io::Result<bool> {
    let mut off = 0usize;
    while off < buf.len() {
        let rc = unsafe { libc::read(fd, buf[off..].as_mut_ptr().cast(), buf.len() - off) };
        if rc > 0 {
            off += rc as usize;
        } else if rc == 0 {
            return Ok(false);
        } else {
            let e = io::Error::last_os_error();
            if e.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(e);
        }
    }
    Ok(true)
}

/// Best-effort progress notification: one byte on the nonblocking
/// `progress_fd` (dropped when the pipe is already full, which still leaves
/// the reader a pending wakeup).
fn relay_note_progress(progress_fd: RawFd) {
    if progress_fd >= 0 {
        let b = [0u8; 1];
        let _ = unsafe { libc::write(progress_fd, b.as_ptr().cast(), 1) };
    }
}

/// Upper bound per downstream `write` in the relay helper. A blocking pipe
/// write returns only once its whole buffer is accepted, so every
/// `RELAY_WRITE_SLICE` bytes a reader consumes produce a progress note. The
/// split-drain survivor therefore treats a reader as stalled only if it
/// consumes less than one slice (4 KiB) within `SPLIT_DRAIN_SURVIVOR_IDLE`
/// (2 s), while bulk relaying stays at 16 writes per 64 KiB frame.
const RELAY_WRITE_SLICE: usize = 4 << 10;

fn relay_write_all(fd: RawFd, data: &[u8], progress_fd: RawFd) -> Result<(), libc::c_int> {
    let mut off = 0usize;
    while off < data.len() {
        let len = (data.len() - off).min(RELAY_WRITE_SLICE);
        let rc = unsafe { libc::write(fd, data[off..].as_ptr().cast(), len) };
        if rc > 0 {
            off += rc as usize;
            relay_note_progress(progress_fd);
            continue;
        }
        if rc == 0 {
            return Err(libc::EPIPE);
        }
        let e = io::Error::last_os_error();
        match e.kind() {
            io::ErrorKind::Interrupted => continue,
            io::ErrorKind::WouldBlock => {
                // Support callers where one of stdout/stderr was already
                // O_NONBLOCK while the other was blocking.
                let mut pfd = libc::pollfd {
                    fd,
                    events: libc::POLLOUT,
                    revents: 0,
                };
                let prc = unsafe { libc::poll(&mut pfd, 1, -1) };
                if prc < 0 {
                    let pe = io::Error::last_os_error();
                    if pe.kind() == io::ErrorKind::Interrupted {
                        continue;
                    }
                    return Err(pe.raw_os_error().unwrap_or(libc::EIO));
                }
                if (pfd.revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL)) != 0
                    && (pfd.revents & libc::POLLOUT) == 0
                {
                    return Err(libc::EPIPE);
                }
            }
            _ => return Err(e.raw_os_error().unwrap_or(libc::EIO)),
        }
    }
    Ok(())
}

fn report_relay_errno(status_fd: RawFd, errno: libc::c_int) {
    let bytes = errno.to_le_bytes();
    let mut off = 0usize;
    while off < bytes.len() {
        let rc = unsafe { libc::write(status_fd, bytes[off..].as_ptr().cast(), bytes.len() - off) };
        if rc > 0 {
            off += rc as usize;
        } else if rc < 0 && io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
            continue;
        } else {
            break;
        }
    }
}

fn run_output_relay_fds(
    in_fd: RawFd,
    out_fd: RawFd,
    err_fd: RawFd,
    status_fd: RawFd,
    progress_fd: RawFd,
) -> i32 {
    // Ignore SIGPIPE so downstream broken pipes return EPIPE explicitly and
    // can be reported over `status_fd`.
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);
    }
    let mut hdr = [0u8; 5];
    let mut buf = Vec::new();
    loop {
        match relay_read_exact(in_fd, &mut hdr) {
            Ok(true) => {}
            Ok(false) => return 0,
            Err(_) => return 1,
        }
        let target = match hdr[0] {
            RELAY_DEST_STDOUT => out_fd,
            RELAY_DEST_STDERR => err_fd,
            _ => return 1,
        };
        let len = u32::from_le_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
        if len > kat_proto::MAX_FRAME {
            return 1;
        }
        buf.resize(len, 0);
        match relay_read_exact(in_fd, &mut buf) {
            Ok(true) => {}
            Ok(false) => return 0,
            Err(_) => return 1,
        }
        if let Err(errno) = relay_write_all(target, &buf, progress_fd) {
            report_relay_errno(status_fd, errno);
            return 1;
        }
    }
}

struct SpawnActionsGuard(libc::posix_spawn_file_actions_t);
impl Drop for SpawnActionsGuard {
    fn drop(&mut self) {
        unsafe { libc::posix_spawn_file_actions_destroy(&mut self.0) };
    }
}

struct SpawnAttrGuard(libc::posix_spawnattr_t);
impl Drop for SpawnAttrGuard {
    fn drop(&mut self) {
        unsafe { libc::posix_spawnattr_destroy(&mut self.0) };
    }
}

fn init_helper_spawn_attr(reset_sigpipe: bool) -> io::Result<SpawnAttrGuard> {
    let mut attr_raw: libc::posix_spawnattr_t = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::posix_spawnattr_init(&mut attr_raw) };
    if rc != 0 {
        return Err(io::Error::from_raw_os_error(rc));
    }
    let mut attr = SpawnAttrGuard(attr_raw);

    let mut sigmask: libc::sigset_t = unsafe { std::mem::zeroed() };
    unsafe {
        libc::sigemptyset(&mut sigmask);
        for sig in [libc::SIGINT, libc::SIGQUIT, libc::SIGHUP, libc::SIGTERM] {
            libc::sigaddset(&mut sigmask, sig);
        }
    }
    let mut flags: libc::c_short = libc::POSIX_SPAWN_SETSIGMASK as libc::c_short;
    #[cfg(target_os = "macos")]
    {
        flags |= libc::POSIX_SPAWN_CLOEXEC_DEFAULT as libc::c_short;
    }
    if reset_sigpipe {
        let mut sigdef: libc::sigset_t = unsafe { std::mem::zeroed() };
        unsafe {
            libc::sigemptyset(&mut sigdef);
            libc::sigaddset(&mut sigdef, libc::SIGPIPE);
        }
        flags |= libc::POSIX_SPAWN_SETSIGDEF as libc::c_short;
        let rc = unsafe { libc::posix_spawnattr_setsigdefault(&mut attr.0, &sigdef) };
        if rc != 0 {
            return Err(io::Error::from_raw_os_error(rc));
        }
    }
    for rc in [
        unsafe { libc::posix_spawnattr_setsigmask(&mut attr.0, &sigmask) },
        unsafe { libc::posix_spawnattr_setflags(&mut attr.0, flags) },
    ] {
        if rc != 0 {
            return Err(io::Error::from_raw_os_error(rc));
        }
    }
    Ok(attr)
}

struct HelperProc {
    pid: Option<libc::pid_t>,
}

impl HelperProc {
    /// Spawn the single ordered output relay helper with `3 -> cmd_r`,
    /// `4 -> out_fd` (or `/dev/null`), `5 -> err_fd` (or `/dev/null`),
    /// `6 -> status_w`, `7 -> progress_w`.
    fn spawn_output_relay(
        cmd_r: RawFd,
        out_fd: Option<RawFd>,
        err_fd: Option<RawFd>,
        status_w: RawFd,
        progress_w: RawFd,
    ) -> io::Result<Self> {
        Self::spawn_self_helper(
            c"vm-io-relay",
            c"KATAMARAN_TEST_IO_RELAY=output",
            &[
                Some(cmd_r),
                out_fd.filter(|&fd| fd >= 0),
                err_fd.filter(|&fd| fd >= 0),
                Some(status_w),
                Some(progress_w),
            ],
        )
    }

    /// Spawn the stdin relay helper with `3 -> stdin_fd`, `4 -> pipe_w`,
    /// `5 -> death_r` (see [`run_stdin_relay_fds`]).
    fn spawn_stdin_relay(stdin_fd: RawFd, pipe_w: RawFd, death_r: RawFd) -> io::Result<Self> {
        Self::spawn_self_helper(
            c"vm-stdin-relay",
            c"KATAMARAN_TEST_IO_RELAY=stdin",
            &[Some(stdin_fd), Some(pipe_w), Some(death_r)],
        )
    }

    /// Spawn this executable as a hidden helper (`<exe> <mode>`) via
    /// `posix_spawn` with `0/1/2 -> /dev/null`, `fds[i] -> 3 + i` (`None` ->
    /// `/dev/null`), an empty environment, termination signals blocked, and
    /// `POSIX_SPAWN_CLOEXEC_DEFAULT` so nothing else is inherited.
    fn spawn_self_helper(
        mode: &std::ffi::CStr,
        test_env: &std::ffi::CStr,
        fds: &[Option<RawFd>],
    ) -> io::Result<Self> {
        let exe = std::env::current_exe()?;
        let exe_c = CString::new(exe.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL in executable path"))?;

        // Relocate every source descriptor above all target fds so no
        // `dup2` can clobber a source that is still to be mapped.
        let first_free = 3 + fds.len() as RawFd;
        let mut dups: Vec<Option<OwnedFd>> = Vec::with_capacity(fds.len());
        for fd in fds {
            dups.push(match fd {
                Some(fd) => Some(dup_ge(*fd, first_free)?),
                None => None,
            });
        }

        let mut actions_raw: libc::posix_spawn_file_actions_t = unsafe { std::mem::zeroed() };
        let rc = unsafe { libc::posix_spawn_file_actions_init(&mut actions_raw) };
        if rc != 0 {
            return Err(io::Error::from_raw_os_error(rc));
        }
        let mut actions = SpawnActionsGuard(actions_raw);
        let add_devnull = |actions: &mut libc::posix_spawn_file_actions_t,
                           dst: RawFd,
                           flags: libc::c_int|
         -> libc::c_int {
            unsafe {
                libc::posix_spawn_file_actions_addopen(
                    actions,
                    dst,
                    c"/dev/null".as_ptr(),
                    flags,
                    0,
                )
            }
        };
        for (dst, flags) in [
            (0, libc::O_RDONLY),
            (1, libc::O_WRONLY),
            (2, libc::O_WRONLY),
        ] {
            let rc = add_devnull(&mut actions.0, dst, flags);
            if rc != 0 {
                return Err(io::Error::from_raw_os_error(rc));
            }
        }
        for (i, dup) in dups.iter().enumerate() {
            let dst = 3 + i as RawFd;
            let rc = match dup {
                Some(fd) => unsafe {
                    libc::posix_spawn_file_actions_adddup2(&mut actions.0, fd.as_raw_fd(), dst)
                },
                None => add_devnull(&mut actions.0, dst, libc::O_RDWR),
            };
            if rc != 0 {
                return Err(io::Error::from_raw_os_error(rc));
            }
        }

        let attr = init_helper_spawn_attr(false)?;
        let mut pid: libc::pid_t = 0;

        #[cfg(not(test))]
        let rc = {
            let _ = test_env;
            let mut argv = [
                exe_c.as_ptr().cast_mut(),
                mode.as_ptr().cast_mut(),
                std::ptr::null_mut(),
            ];
            let mut envp: [*mut libc::c_char; 1] = [std::ptr::null_mut()];
            unsafe {
                libc::posix_spawn(
                    &mut pid,
                    exe_c.as_ptr(),
                    &actions.0,
                    &attr.0,
                    argv.as_mut_ptr(),
                    envp.as_mut_ptr(),
                )
            }
        };

        #[cfg(test)]
        let rc = {
            let _ = mode;
            let mut argv = [
                exe_c.as_ptr().cast_mut(),
                c"--exact".as_ptr().cast_mut(),
                c"vm_cli::tests::io_relay_helper_entrypoint"
                    .as_ptr()
                    .cast_mut(),
                c"--ignored".as_ptr().cast_mut(),
                c"--nocapture".as_ptr().cast_mut(),
                c"--test-threads=1".as_ptr().cast_mut(),
                std::ptr::null_mut(),
            ];
            let mut envp = [test_env.as_ptr().cast_mut(), std::ptr::null_mut()];
            unsafe {
                libc::posix_spawn(
                    &mut pid,
                    exe_c.as_ptr(),
                    &actions.0,
                    &attr.0,
                    argv.as_mut_ptr(),
                    envp.as_mut_ptr(),
                )
            }
        };

        if rc != 0 {
            return Err(io::Error::from_raw_os_error(rc));
        }
        Ok(Self { pid: Some(pid) })
    }

    fn wait(&mut self) -> Option<libc::c_int> {
        let pid = self.pid.take()?;
        let mut status: libc::c_int = 0;
        loop {
            let rc = unsafe { libc::waitpid(pid, &mut status, 0) };
            if rc > 0 {
                return Some(status);
            }
            if io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
                return None;
            }
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

/// Returns `true` only when `fstat` proves `a` and `b` refer to distinct
/// underlying destinations (different file types, different device numbers for
/// block devices, or distinct nonzero `(st_dev, st_ino)` pairs for pipes and
/// files). Returns `false` conservatively for identical, dup'd, character-
/// device pairs (such as a direct PTY slave paired with `/dev/tty`, which
/// alias the same controlling terminal despite different `st_rdev`), or
/// ambiguous descriptors (such as sockets reporting `st_ino == 0`).
fn provably_distinct_fds(a: RawFd, b: RawFd) -> bool {
    if a < 0 || b < 0 || a == b {
        return false;
    }
    let mut sa: libc::stat = unsafe { std::mem::zeroed() };
    let mut sb: libc::stat = unsafe { std::mem::zeroed() };
    if unsafe { libc::fstat(a, &mut sa) } != 0 || unsafe { libc::fstat(b, &mut sb) } != 0 {
        return false;
    }
    let fa = sa.st_mode & libc::S_IFMT;
    let fb = sb.st_mode & libc::S_IFMT;
    if fa != fb {
        return true;
    }
    if fa == libc::S_IFCHR {
        // Two character devices can alias the same controlling terminal (e.g.
        // direct PTY slave + `/dev/tty`, where XNU `cttywrite` forwards to the
        // controlling terminal despite different `st_rdev`). Keep one ordered
        // relay conservatively.
        return false;
    }
    if fa == libc::S_IFBLK {
        return sa.st_rdev != sb.st_rdev;
    }
    // On macOS, pipes and files carry unique nonzero st_ino; sockets may
    // report st_ino == 0, which we treat conservatively as potentially shared.
    sa.st_ino != 0 && sb.st_ino != 0 && (sa.st_dev != sb.st_dev || sa.st_ino != sb.st_ino)
}

/// Ordered output relay helper for one or both blocking inherited stdio fds.
///
/// Receives destination-tagged frames over a private nonblocking pipe and
/// writes them to inherited stdout/stderr in FIFO order without ever setting
/// `O_NONBLOCK` on any inherited descriptor. Also exposes a pollable `status_r`
/// pipe that wakes the session immediately if a downstream reader closes
/// (`EPIPE`) even when the guest produces no further output.
struct OutputRelay {
    proc: HelperProc,
    cmd_w: Option<OwnedFd>,
    status_r: OwnedFd,
    /// Nonblocking; readable whenever the helper made downstream progress.
    progress_r: OwnedFd,
    handles_out: bool,
    handles_err: bool,
}

impl OutputRelay {
    fn spawn(out_fd: Option<RawFd>, err_fd: Option<RawFd>) -> io::Result<Self> {
        let (cmd_r, cmd_w) = pipe_cloexec(false)?;
        unsafe {
            let wfl = libc::fcntl(cmd_w.as_raw_fd(), libc::F_GETFL);
            if wfl < 0 || libc::fcntl(cmd_w.as_raw_fd(), libc::F_SETFL, wfl | libc::O_NONBLOCK) < 0
            {
                return Err(io::Error::last_os_error());
            }
        }
        let (status_r, status_w) = pipe_cloexec(false)?;
        let (progress_r, progress_w) = pipe_cloexec(true)?;
        let proc = HelperProc::spawn_output_relay(
            cmd_r.as_raw_fd(),
            out_fd,
            err_fd,
            status_w.as_raw_fd(),
            progress_w.as_raw_fd(),
        )?;
        drop(cmd_r);
        drop(status_w);
        drop(progress_w);
        Ok(Self {
            proc,
            cmd_w: Some(cmd_w),
            status_r,
            progress_r,
            handles_out: out_fd.is_some(),
            handles_err: err_fd.is_some(),
        })
    }

    fn maybe_spawn(out_fd: Option<RawFd>, err_fd: Option<RawFd>) -> io::Result<Option<Self>> {
        let need_out = out_fd.is_some_and(needs_blocking_relay);
        let need_err = err_fd.is_some_and(needs_blocking_relay);
        if !need_out && !need_err {
            return Ok(None);
        }
        Self::spawn(out_fd, err_fd).map(Some)
    }

    fn status_fd(&self) -> RawFd {
        self.status_r.as_raw_fd()
    }

    /// Read an optional 4-byte `errno` reported on `status_r`. Returns `None`
    /// on clean EOF (0 bytes).
    fn read_status_errno(&self) -> Option<libc::c_int> {
        let mut buf = [0u8; 4];
        let mut off = 0usize;
        while off < buf.len() {
            let rc = unsafe {
                libc::read(
                    self.status_r.as_raw_fd(),
                    buf[off..].as_mut_ptr().cast(),
                    buf.len() - off,
                )
            };
            if rc > 0 {
                off += rc as usize;
            } else if rc == 0 || io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
                break;
            }
        }
        if off == 4 {
            Some(i32::from_le_bytes(buf))
        } else {
            None
        }
    }

    /// Interpret a `status_r` wakeup during an active session (before `cmd_w`
    /// has been closed) as either `LocalBrokenPipe` or `LocalError`.
    fn classify_active_failure(&self) -> RelayOutcome {
        match self.read_status_errno() {
            None | Some(libc::EPIPE) => RelayOutcome::LocalBrokenPipe,
            Some(errno) => RelayOutcome::LocalError(io::Error::from_raw_os_error(errno)),
        }
    }

    fn collect_finished(&mut self) -> RelayFinish {
        let err = self.read_status_errno();
        let status = self.proc.wait();
        match err {
            Some(libc::EPIPE) => RelayFinish::BrokenPipe,
            Some(errno) => RelayFinish::Error(io::Error::from_raw_os_error(errno)),
            None => {
                if status.is_none_or(|st| libc::WIFEXITED(st) && libc::WEXITSTATUS(st) == 0) {
                    RelayFinish::Drained
                } else {
                    RelayFinish::BrokenPipe
                }
            }
        }
    }

    fn write_slice_cancellable(
        &self,
        data: &[u8],
        term_wake_r: Option<RawFd>,
    ) -> io::Result<WriteOutcome> {
        let fd = self
            .cmd_w
            .as_ref()
            .expect("OutputRelay already closed")
            .as_raw_fd();
        let status_fd = self.status_r.as_raw_fd();
        let mut offset = 0usize;
        while offset < data.len() {
            if term_wake_r.is_some_and(fd_ready) {
                return Ok(WriteOutcome::Cancelled);
            }
            if fd_ready(status_fd) {
                return match self.read_status_errno() {
                    None | Some(libc::EPIPE) => Ok(WriteOutcome::BrokenPipe),
                    Some(errno) => Err(io::Error::from_raw_os_error(errno)),
                };
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
                io::ErrorKind::BrokenPipe => {
                    return match self.read_status_errno() {
                        None | Some(libc::EPIPE) => Ok(WriteOutcome::BrokenPipe),
                        Some(errno) => Err(io::Error::from_raw_os_error(errno)),
                    };
                }
                io::ErrorKind::WouldBlock => {
                    let mut pfds = [
                        libc::pollfd {
                            fd,
                            events: libc::POLLOUT,
                            revents: 0,
                        },
                        libc::pollfd {
                            fd: status_fd,
                            events: libc::POLLIN,
                            revents: 0,
                        },
                        libc::pollfd {
                            fd: term_wake_r.unwrap_or(-1),
                            events: libc::POLLIN,
                            revents: 0,
                        },
                    ];
                    let nfds = if term_wake_r.is_some() { 3 } else { 2 };
                    let prc = unsafe { libc::poll(pfds.as_mut_ptr(), nfds, -1) };
                    if prc < 0 {
                        let pe = io::Error::last_os_error();
                        if pe.kind() == io::ErrorKind::Interrupted {
                            continue;
                        }
                        return Err(pe);
                    }
                    if nfds == 3 && pfds[2].revents != 0 {
                        return Ok(WriteOutcome::Cancelled);
                    }
                    if pfds[1].revents != 0 {
                        return match self.read_status_errno() {
                            None | Some(libc::EPIPE) => Ok(WriteOutcome::BrokenPipe),
                            Some(errno) => Err(io::Error::from_raw_os_error(errno)),
                        };
                    }
                    if (pfds[0].revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL)) != 0
                        && (pfds[0].revents & libc::POLLOUT) == 0
                    {
                        return match self.read_status_errno() {
                            None | Some(libc::EPIPE) => Ok(WriteOutcome::BrokenPipe),
                            Some(errno) => Err(io::Error::from_raw_os_error(errno)),
                        };
                    }
                }
                _ => return Err(e),
            }
        }
        Ok(WriteOutcome::Written)
    }

    fn write_frame_cancellable(
        &self,
        dest: u8,
        data: &[u8],
        term_wake_r: Option<RawFd>,
    ) -> io::Result<WriteOutcome> {
        if data.is_empty() {
            return Ok(WriteOutcome::Written);
        }
        let len_bytes = (data.len() as u32).to_le_bytes();
        let hdr = [dest, len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]];
        match self.write_slice_cancellable(&hdr, term_wake_r)? {
            WriteOutcome::Written => self.write_slice_cancellable(data, term_wake_r),
            other => Ok(other),
        }
    }

    /// Close `cmd_w` and wait for this relay helper to drain all queued frames,
    /// aborting immediately if `term_wake_r` fires.
    fn finish(&mut self, term_wake_r: RawFd) -> RelayFinish {
        self.cmd_w.take();
        loop {
            let mut pfds = [
                libc::pollfd {
                    fd: self.status_r.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                },
                libc::pollfd {
                    fd: term_wake_r,
                    events: libc::POLLIN,
                    revents: 0,
                },
            ];
            let rc = unsafe { libc::poll(pfds.as_mut_ptr(), 2, -1) };
            if rc < 0 {
                let e = io::Error::last_os_error();
                if e.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                self.proc.kill_and_wait();
                return RelayFinish::Error(e);
            }
            if pfds[1].revents != 0 {
                self.proc.kill_and_wait();
                return RelayFinish::Cancelled;
            }
            if pfds[0].revents != 0 {
                return self.collect_finished();
            }
        }
    }

    fn abort(&mut self) {
        self.cmd_w.take();
        self.proc.kill_and_wait();
    }
}

impl Drop for OutputRelay {
    fn drop(&mut self) {
        self.abort();
    }
}

/// Maximum bytes buffered per stream on the host when a split output relay's
/// command pipe is full and the guest is NOT flow-controlled
/// (`ExecSpec::output_window == 0`). Windowed sessions never reach it: the
/// guest sends at most `output_window` unacknowledged bytes per stream, so the
/// host keeps reading the connection (and delivering stderr) unconditionally.
const SPLIT_STREAM_QUEUE_CAP: usize = 2 << 20;
/// After one split channel fails during the post-exit drain, how long the
/// surviving channel may go without observable progress before it is aborted
/// (so an unread peer pipe cannot hang teardown, while a slow but live reader
/// still receives every buffered byte).
#[cfg(not(test))]
const SPLIT_DRAIN_SURVIVOR_IDLE: Duration = Duration::from_secs(2);
/// Shortened so tests can prove that a slow reader outlasting the idle period
/// in total, but progressing within it, still receives every byte.
#[cfg(test)]
const SPLIT_DRAIN_SURVIVOR_IDLE: Duration = Duration::from_millis(400);

#[derive(Default)]
struct PendingBytes {
    /// Encoded chunks with the guest payload bytes (flow-control credit) each
    /// releases once fully written downstream.
    chunks: VecDeque<(Vec<u8>, usize)>,
    head_off: usize,
    bytes: usize,
    /// Credit released by fully written chunks, not yet granted to the guest.
    released: usize,
}

impl PendingBytes {
    fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    fn is_full(&self) -> bool {
        self.bytes >= SPLIT_STREAM_QUEUE_CAP
    }

    fn push(&mut self, data: Vec<u8>, credit: usize) {
        if !data.is_empty() {
            self.bytes += data.len();
            self.chunks.push_back((data, credit));
        }
    }

    fn front(&self) -> Option<&[u8]> {
        self.chunks.front().map(|(v, _)| &v[self.head_off..])
    }

    fn advance(&mut self, n: usize) {
        self.head_off += n;
        self.bytes = self.bytes.saturating_sub(n);
        if let Some((front, credit)) = self.chunks.front()
            && self.head_off >= front.len()
        {
            self.released += *credit;
            self.chunks.pop_front();
            self.head_off = 0;
        }
    }

    fn take_released(&mut self) -> usize {
        std::mem::take(&mut self.released)
    }

    fn clear(&mut self) {
        self.chunks.clear();
        self.head_off = 0;
        self.bytes = 0;
        self.released = 0;
    }
}

enum FlushOutcome {
    Ok,
    BrokenPipe,
    Error(io::Error),
}

fn flush_nonblock_fd(fd: RawFd, pending: &mut PendingBytes) -> FlushOutcome {
    while let Some(slice) = pending.front() {
        let rc = unsafe { libc::write(fd, slice.as_ptr().cast(), slice.len()) };
        if rc > 0 {
            pending.advance(rc as usize);
            continue;
        }
        if rc == 0 {
            return FlushOutcome::BrokenPipe;
        }
        let e = io::Error::last_os_error();
        match e.kind() {
            io::ErrorKind::Interrupted => continue,
            io::ErrorKind::WouldBlock => return FlushOutcome::Ok,
            io::ErrorKind::BrokenPipe => return FlushOutcome::BrokenPipe,
            _ => return FlushOutcome::Error(e),
        }
    }
    FlushOutcome::Ok
}

/// One independent stream (stdout or stderr) inside [`OutputRelays::Split`].
enum SplitChannel {
    Relay {
        relay: OutputRelay,
        dest: u8,
        pending: PendingBytes,
    },
    Direct {
        fd: RawFd,
        pending: PendingBytes,
    },
}

impl SplitChannel {
    fn status_fd(&self) -> Option<RawFd> {
        match self {
            Self::Relay { relay, .. } => Some(relay.status_fd()),
            Self::Direct { .. } => None,
        }
    }

    /// Readable when the relay helper has written downstream since the last
    /// [`drain_wake`] (direct channels report progress via `pending` instead).
    fn progress_fd(&self) -> Option<&OwnedFd> {
        match self {
            Self::Relay { relay, .. } => Some(&relay.progress_r),
            Self::Direct { .. } => None,
        }
    }

    fn write_poll_fd(&self) -> Option<RawFd> {
        match self {
            Self::Relay { relay, pending, .. } if !pending.is_empty() => {
                relay.cmd_w.as_ref().map(AsRawFd::as_raw_fd)
            }
            Self::Direct { fd, pending } if !pending.is_empty() && *fd >= 0 => Some(*fd),
            _ => None,
        }
    }

    fn is_queue_full(&self) -> bool {
        self.pending().is_full()
    }

    fn pending(&self) -> &PendingBytes {
        match self {
            Self::Relay { pending, .. } | Self::Direct { pending, .. } => pending,
        }
    }

    /// Guest payload bytes fully handed downstream since the last call.
    fn take_released(&mut self) -> usize {
        match self {
            Self::Relay { pending, .. } | Self::Direct { pending, .. } => pending.take_released(),
        }
    }

    fn try_flush(&mut self) -> FlushOutcome {
        match self {
            Self::Relay { relay, pending, .. } => {
                if pending.is_empty() {
                    return FlushOutcome::Ok;
                }
                let Some(fd) = relay.cmd_w.as_ref().map(AsRawFd::as_raw_fd) else {
                    return FlushOutcome::BrokenPipe;
                };
                match flush_nonblock_fd(fd, pending) {
                    FlushOutcome::BrokenPipe => match relay.read_status_errno() {
                        None | Some(libc::EPIPE) => FlushOutcome::BrokenPipe,
                        Some(errno) => FlushOutcome::Error(io::Error::from_raw_os_error(errno)),
                    },
                    other => other,
                }
            }
            Self::Direct { fd, pending } => {
                if pending.is_empty() {
                    return FlushOutcome::Ok;
                }
                flush_nonblock_fd(*fd, pending)
            }
        }
    }

    fn enqueue_and_flush(&mut self, data: Vec<u8>) -> FlushOutcome {
        if data.is_empty() {
            return FlushOutcome::Ok;
        }
        let credit = data.len();
        match self {
            Self::Relay { dest, pending, .. } => {
                let mut frame = Vec::with_capacity(5 + data.len());
                frame.push(*dest);
                frame.extend_from_slice(&(data.len() as u32).to_le_bytes());
                frame.extend_from_slice(&data);
                pending.push(frame, credit);
            }
            Self::Direct { pending, .. } => {
                pending.push(data, credit);
            }
        }
        self.try_flush()
    }

    /// Close `cmd_w` once all host-queued bytes have been pushed into the
    /// helper pipe. Returns `true` when this channel has no helper left to
    /// wait on (i.e. an idle `Direct` channel).
    fn close_if_idle(&mut self) -> bool {
        match self {
            Self::Relay { relay, pending, .. } => {
                if pending.is_empty() {
                    relay.cmd_w.take();
                }
                false
            }
            Self::Direct { pending, .. } => pending.is_empty(),
        }
    }

    fn collect_finished(&mut self) -> RelayFinish {
        match self {
            Self::Relay { relay, pending, .. } => {
                let res = relay.collect_finished();
                if matches!(res, RelayFinish::Drained) && !pending.is_empty() {
                    RelayFinish::BrokenPipe
                } else {
                    res
                }
            }
            Self::Direct { .. } => RelayFinish::Drained,
        }
    }

    fn classify_active_failure(&self) -> RelayOutcome {
        match self {
            Self::Relay { relay, .. } => relay.classify_active_failure(),
            Self::Direct { .. } => RelayOutcome::LocalBrokenPipe,
        }
    }

    fn abort(&mut self) {
        match self {
            Self::Relay { relay, pending, .. } => {
                pending.clear();
                relay.abort();
            }
            Self::Direct { pending, .. } => pending.clear(),
        }
    }
}

/// Manages blocking output relay helpers for stdout and stderr:
/// - Uses ONE shared ordered FIFO helper when stdout and stderr share a
///   destination (e.g. `2>&1`, same TTY, `/dev/tty` alias, or ambiguous
///   `fstat` identity), preserving exact guest frame order.
/// - Uses TWO independent nonblocking channels when stdout and stderr are
///   provably distinct (e.g. separate pipes with distinct nonzero `fstat`
///   device/inode pairs, or pipe + TTY) so a full stdout pipe (and even a full
///   stdout relay command pipe) never blocks reading or delivering stderr.
enum OutputRelays {
    None,
    Shared(OutputRelay),
    Split {
        out: SplitChannel,
        err: SplitChannel,
    },
}

impl OutputRelays {
    fn maybe_spawn(out_fd: Option<RawFd>, err_fd: Option<RawFd>) -> io::Result<Self> {
        if let (Some(o), Some(e)) = (out_fd, err_fd)
            && provably_distinct_fds(o, e)
        {
            let out = if needs_blocking_relay(o) {
                SplitChannel::Relay {
                    relay: OutputRelay::spawn(Some(o), None)?,
                    dest: RELAY_DEST_STDOUT,
                    pending: PendingBytes::default(),
                }
            } else {
                SplitChannel::Direct {
                    fd: o,
                    pending: PendingBytes::default(),
                }
            };
            let err = if needs_blocking_relay(e) {
                SplitChannel::Relay {
                    relay: OutputRelay::spawn(None, Some(e))?,
                    dest: RELAY_DEST_STDERR,
                    pending: PendingBytes::default(),
                }
            } else {
                SplitChannel::Direct {
                    fd: e,
                    pending: PendingBytes::default(),
                }
            };
            return Ok(Self::Split { out, err });
        }
        match OutputRelay::maybe_spawn(out_fd, err_fd)? {
            Some(r) => Ok(Self::Shared(r)),
            None => Ok(Self::None),
        }
    }

    fn finish(&mut self, term_wake_r: RawFd) -> RelayFinish {
        match self {
            Self::None => RelayFinish::Drained,
            Self::Shared(r) => r.finish(term_wake_r),
            Self::Split { out, err } => Self::finish_split([out, err], term_wake_r),
        }
    }

    /// Drain both split channels concurrently. A failure on one channel aborts
    /// ONLY that channel and is recorded as the result; the surviving channel
    /// keeps flushing every byte already received from the guest (e.g. stderr
    /// diagnostics after `head` closed stdout) until it drains, fails, or makes
    /// no observable progress for [`SPLIT_DRAIN_SURVIVOR_IDLE`] (an unread peer
    /// pipe), at which point it is aborted too. Progress is either host-queued
    /// bytes leaving `pending` or, once those are all inside a relay helper,
    /// the helper's own downstream writes (its `progress_r` pipe), so a slow
    /// but live reader is never cut off mid-drain.
    fn finish_split(mut chans: [&mut SplitChannel; 2], term_wake_r: RawFd) -> RelayFinish {
        let mut done = [false; 2];
        let mut failure: Option<RelayFinish> = None;
        let mut survivor_deadline: Option<Instant> = None;
        let mut last_pending = [chans[0].pending().bytes, chans[1].pending().bytes];
        // Discard progress notes from the active session; only writes made
        // during this drain count.
        for chan in &chans {
            if let Some(p) = chan.progress_fd() {
                drain_wake(p);
            }
        }

        let fail = |chan: &mut SplitChannel,
                    done: &mut bool,
                    failure: &mut Option<RelayFinish>,
                    deadline: &mut Option<Instant>,
                    f: RelayFinish| {
            chan.abort();
            *done = true;
            if failure.is_none() {
                *failure = Some(f);
            }
            if deadline.is_none() {
                *deadline = Some(Instant::now() + SPLIT_DRAIN_SURVIVOR_IDLE);
            }
        };

        loop {
            // Flush host-queued frames and close `cmd_w` as soon as a channel's
            // host queue empties so both helpers drain concurrently.
            for i in 0..2 {
                if done[i] {
                    continue;
                }
                match chans[i].try_flush() {
                    FlushOutcome::Ok => {
                        if chans[i].close_if_idle() {
                            done[i] = true;
                        }
                    }
                    FlushOutcome::BrokenPipe => fail(
                        chans[i],
                        &mut done[i],
                        &mut failure,
                        &mut survivor_deadline,
                        RelayFinish::BrokenPipe,
                    ),
                    FlushOutcome::Error(e) => fail(
                        chans[i],
                        &mut done[i],
                        &mut failure,
                        &mut survivor_deadline,
                        RelayFinish::Error(e),
                    ),
                }
                // Observable progress by the survivor extends its deadline.
                let now_pending = chans[i].pending().bytes;
                if now_pending < last_pending[i]
                    && let Some(dl) = survivor_deadline.as_mut()
                {
                    *dl = Instant::now() + SPLIT_DRAIN_SURVIVOR_IDLE;
                }
                last_pending[i] = now_pending;
            }
            if done[0] && done[1] {
                return failure.unwrap_or(RelayFinish::Drained);
            }

            let timeout_ms = match survivor_deadline {
                None => -1,
                Some(dl) => {
                    let rem = dl.saturating_duration_since(Instant::now());
                    if rem.is_zero() {
                        for (i, chan) in chans.iter_mut().enumerate() {
                            if !done[i] {
                                chan.abort();
                            }
                        }
                        return failure.unwrap_or(RelayFinish::BrokenPipe);
                    }
                    rem.as_millis().clamp(1, i32::MAX as u128) as i32
                }
            };

            let mut pfds = [libc::pollfd {
                fd: -1,
                events: 0,
                revents: 0,
            }; 7];
            pfds[0] = libc::pollfd {
                fd: term_wake_r,
                events: libc::POLLIN,
                revents: 0,
            };
            // Per channel at base 1 + 3*i: [status_fd POLLIN, write_fd POLLOUT,
            // progress_fd POLLIN].
            for (i, chan) in chans.iter().enumerate() {
                if done[i] {
                    continue;
                }
                let base = 1 + 3 * i;
                if let Some(fd) = chan.status_fd() {
                    pfds[base] = libc::pollfd {
                        fd,
                        events: libc::POLLIN,
                        revents: 0,
                    };
                }
                if let Some(fd) = chan.write_poll_fd() {
                    pfds[base + 1] = libc::pollfd {
                        fd,
                        events: libc::POLLOUT,
                        revents: 0,
                    };
                }
                if let Some(p) = chan.progress_fd() {
                    pfds[base + 2] = libc::pollfd {
                        fd: p.as_raw_fd(),
                        events: libc::POLLIN,
                        revents: 0,
                    };
                }
            }

            let rc =
                unsafe { libc::poll(pfds.as_mut_ptr(), pfds.len() as libc::nfds_t, timeout_ms) };
            if rc < 0 {
                let e = io::Error::last_os_error();
                if e.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                for chan in &mut chans {
                    chan.abort();
                }
                return failure.unwrap_or(RelayFinish::Error(e));
            }
            if pfds[0].revents != 0 {
                for chan in &mut chans {
                    chan.abort();
                }
                return RelayFinish::Cancelled;
            }
            for i in 0..2 {
                if done[i] {
                    continue;
                }
                let base = 1 + 3 * i;
                if pfds[base + 2].revents != 0
                    && let Some(p) = chans[i].progress_fd()
                {
                    drain_wake(p);
                    if let Some(dl) = survivor_deadline.as_mut() {
                        *dl = Instant::now() + SPLIT_DRAIN_SURVIVOR_IDLE;
                    }
                }
                if pfds[base].revents != 0 {
                    match chans[i].collect_finished() {
                        RelayFinish::Drained => done[i] = true,
                        f => fail(
                            chans[i],
                            &mut done[i],
                            &mut failure,
                            &mut survivor_deadline,
                            f,
                        ),
                    }
                    continue;
                }
                let wr = pfds[base + 1].revents;
                if (wr & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL)) != 0
                    && (wr & libc::POLLOUT) == 0
                {
                    let f = match chans[i].try_flush() {
                        FlushOutcome::Ok | FlushOutcome::BrokenPipe => RelayFinish::BrokenPipe,
                        FlushOutcome::Error(e) => RelayFinish::Error(e),
                    };
                    fail(
                        chans[i],
                        &mut done[i],
                        &mut failure,
                        &mut survivor_deadline,
                        f,
                    );
                }
            }
        }
    }

    fn abort(&mut self) {
        match self {
            Self::None => {}
            Self::Shared(r) => r.abort(),
            Self::Split { out, err } => {
                out.abort();
                err.abort();
            }
        }
    }
}

/// Local stdio plumbing for a session, created BEFORE `Exec` is submitted.
///
/// NEVER calls `F_SETFL(O_NONBLOCK)` on any inherited stdio descriptor: blocking
/// stdin is relayed via [`StdinHelper`], blocking stdout/stderr via
/// [`OutputRelays`] (one shared FIFO for merged or ambiguous sinks so `2>&1`
/// preserves frame order; independent relays for provably distinct sinks so a
/// full stdout pipe never blocks stderr).
///
/// The stdin relay is deliberately NOT part of it: it starts only once the
/// session is live and the host terminal is in raw mode (see
/// [`LocalIo::start_stdin`]), so typeahead during VM boot stays in the tty
/// buffer instead of being consumed (and echoed) early.
struct LocalIo {
    output_relays: OutputRelays,
    /// Read end of an already-closed pipe that stands in for stdin (immediate
    /// EOF, so the guest sees `DataEnd`) with `--no-stdin`, or as a fallback.
    eof_stdin: Option<OwnedFd>,
}

impl LocalIo {
    fn prepare(out_fd: Option<RawFd>, err_fd: Option<RawFd>, no_stdin: bool) -> io::Result<Self> {
        Ok(Self {
            output_relays: OutputRelays::maybe_spawn(out_fd, err_fd)?,
            eof_stdin: if no_stdin { Some(eof_pipe()?) } else { None },
        })
    }

    fn forwards_stdin(&self) -> bool {
        self.eof_stdin.is_none()
    }

    /// Start relaying `stdin_fd` (after `Started`, after raw mode). Returns the
    /// helper (if one is needed) and the fd `writer_worker` reads. On failure
    /// the caller ends the session (the guest then hangs up the command): an
    /// interactive session without input could not even be interrupted.
    fn start_stdin(&mut self, stdin_fd: RawFd) -> io::Result<(Option<StdinHelper>, RawFd)> {
        if let Some(r) = &self.eof_stdin {
            return Ok((None, r.as_raw_fd()));
        }
        let helper = StdinHelper::maybe_spawn(stdin_fd)?;
        let fd = helper.as_ref().map_or(stdin_fd, StdinHelper::as_raw_fd);
        Ok((helper, fd))
    }
}

/// Nonblocking read end of a pipe whose write end is already closed.
fn eof_pipe() -> io::Result<OwnedFd> {
    let (r, w) = pipe_cloexec(true)?;
    drop(w);
    Ok(r)
}

/// Relays an inherited blocking `stdin_fd` into a private nonblocking pipe via
/// a dedicated `vm-stdin-relay` helper process so `run_session_io` never modifies
/// `stdin_fd`'s shared open file description (`O_NONBLOCK`), yet teardown can
/// still terminate and reap a blocked reader promptly even if another process
/// drained the last ready byte.
struct StdinHelper {
    _proc: HelperProc,
    read_end: OwnedFd,
    /// Held only by this process; its hangup (on drop or process death) makes
    /// the helper exit without reading more of the caller's input.
    _death_w: OwnedFd,
}

impl StdinHelper {
    fn maybe_spawn(stdin_fd: RawFd) -> io::Result<Option<Self>> {
        if !needs_blocking_relay(stdin_fd) {
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
        let (death_r, death_w) = pipe_cloexec(false)?;
        let proc =
            HelperProc::spawn_stdin_relay(stdin_fd, write_end.as_raw_fd(), death_r.as_raw_fd())?;
        drop(write_end);
        drop(death_r);
        Ok(Some(Self {
            _proc: proc,
            read_end,
            _death_w: death_w,
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
    TimedOut,
    Failed(anyhow::Error),
}

/// Perform a side-effect-free readiness probe (`Ping` -> `Pong`) within
/// `deadline`, leaving the connection open to submit `Exec` once ready.
fn probe_ready(path: &Path, deadline: Instant) -> Probe {
    let mut conn = match connect_with_deadline(path, deadline) {
        Ok(s) => s,
        Err(e) if e.kind() == io::ErrorKind::TimedOut => return Probe::TimedOut,
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
                return Probe::TimedOut;
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
            Probe::TimedOut
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
    mut deadline: Instant,
    mut poll_status: impl FnMut() -> anyhow::Result<Option<Status>>,
) -> anyhow::Result<UnixStream> {
    // Once the daemon reports the VM `Running`, its exec listener only gets
    // the short handshake bound (a guest whose listener is down must fail
    // fast instead of after the whole boot timeout).
    let running_cap = |deadline: &mut Instant, s: &Status| {
        if matches!(s.state, VmState::Running) {
            *deadline = (*deadline).min(Instant::now() + HANDSHAKE_TIMEOUT);
        }
    };
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
                            running_cap(&mut deadline, &s);
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
                                Probe::ClosedBeforeReady => {
                                    saw_closed = true;
                                    last_no_socket = None;
                                }
                                Probe::TimedOut => last_no_socket = None,
                            }
                        }
                        VmState::Starting | VmState::Stopping => {}
                    }
                }
            }
            Probe::ClosedBeforeReady => {
                saw_closed = true;
                last_no_socket = None;
                if let Some(s) = poll_status()? {
                    if matches!(s.state, VmState::Error | VmState::Stopped) {
                        bail!("the VM did not start: {}", s.error.unwrap_or_default());
                    }
                    running_cap(&mut deadline, &s);
                }
            }
            Probe::TimedOut => {
                if let Some(s) = poll_status()? {
                    if matches!(s.state, VmState::Error | VmState::Stopped) {
                        bail!("the VM did not start: {}", s.error.unwrap_or_default());
                    }
                    running_cap(&mut deadline, &s);
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

    match probe_ready(&path, Instant::now() + PROBE_ATTEMPT_TIMEOUT) {
        Probe::Ready(conn) => {
            let (conn, token) = submit_exec(conn, spec, Instant::now() + HANDSHAKE_TIMEOUT)?;
            return Ok((conn, token, path));
        }
        Probe::Failed(e) => return Err(e),
        // Not ready yet (`ClosedBeforeReady` / `TimedOut`: the VM process bound
        // the Unix socket but kat-init may still be booting): like a missing
        // socket, wait under the daemon's VM state — up to `VM_START_TIMEOUT`
        // while it boots, `HANDSHAKE_TIMEOUT` once it reports Running.
        Probe::ClosedBeforeReady | Probe::TimedOut | Probe::NoSocket(_) => {}
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
            // A VM that is already running gets only the short handshake
            // bound: a guest whose exec listener is down must fail fast, not
            // after the full boot timeout.
            Probe::ClosedBeforeReady | Probe::TimedOut => {
                let conn =
                    wait_for_ready_socket(&path, Instant::now() + HANDSHAKE_TIMEOUT, || Ok(None))?;
                let (conn, token) = submit_exec(conn, spec, Instant::now() + HANDSHAKE_TIMEOUT)?;
                return Ok((conn, token, path));
            }
        },
        VmState::Stopped | VmState::Error => {
            eprintln!("katamaran: starting the VM");
            c.call(&Call::Start)?;
        }
        VmState::Stopping => {
            // The daemon rejects Start while stopping: wait for the stop to
            // finish, then start the VM again.
            eprintln!("katamaran: waiting for the VM to stop before starting it");
            let deadline = Instant::now() + VM_START_TIMEOUT;
            loop {
                let s = c.status()?;
                match s.state {
                    VmState::Stopping => {
                        if Instant::now() >= deadline {
                            bail!("timed out waiting for the VM to finish stopping");
                        }
                        thread::sleep(Duration::from_millis(200));
                    }
                    VmState::Stopped | VmState::Error => {
                        eprintln!("katamaran: starting the VM");
                        c.call(&Call::Start)?;
                        break;
                    }
                    VmState::Starting | VmState::Running => break,
                }
            }
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
    Resize {
        rows: u16,
        cols: u16,
    },
    /// Replenish the guest's send window for an output stream.
    OutputWindow {
        stream: u8,
        bytes: u32,
    },
    /// Guest-granted stdin credit (`VmExecMsg::InputWindow`).
    InputWindow(u32),
}

/// Host side of windowed (non-PTY) flow control; see [`kat_proto::vm_exec`].
///
/// Tracks the guest's remaining per-stream output credit, returns credit (via
/// `writer_worker`) once output has been handed to the local sink, and forwards
/// guest stdin credit to `writer_worker`. Because the guest can never have more
/// than `output_window` unacknowledged bytes per stream, the split relay keeps
/// reading the connection while stdout is blocked without unbounded buffering.
struct FlowCtl {
    remaining: [u64; 2],
    ctrl_tx: mpsc::Sender<WriterCtrl>,
    ctrl_wake_w: Arc<OwnedFd>,
}

impl FlowCtl {
    fn new(window: u32, ctrl_tx: mpsc::Sender<WriterCtrl>, ctrl_wake_w: Arc<OwnedFd>) -> Self {
        Self {
            remaining: [u64::from(window); 2],
            ctrl_tx,
            ctrl_wake_w,
        }
    }

    /// Account for an output frame received from the guest.
    fn on_output(&mut self, stream: u8, len: usize) -> anyhow::Result<()> {
        let rem = &mut self.remaining[usize::from(stream)];
        let len = len as u64;
        if len > *rem {
            bail!(
                "the guest exceeded the {} flow-control window",
                if stream == vm_exec::OUTPUT_STREAM_STDOUT {
                    "stdout"
                } else {
                    "stderr"
                }
            );
        }
        *rem -= len;
        Ok(())
    }

    /// Return `bytes` of output credit to the guest.
    fn grant(&mut self, stream: u8, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let bytes = bytes.min(u32::MAX as usize) as u32;
        self.remaining[usize::from(stream)] += u64::from(bytes);
        if self
            .ctrl_tx
            .send(WriterCtrl::OutputWindow { stream, bytes })
            .is_ok()
        {
            notify_wake(&self.ctrl_wake_w);
        }
    }

    fn forward_input_credit(&self, bytes: u32) {
        if bytes > 0 && self.ctrl_tx.send(WriterCtrl::InputWindow(bytes)).is_ok() {
            notify_wake(&self.ctrl_wake_w);
        }
    }
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
/// Pick the next coalesced output-credit frame, alternating between stdout and
/// stderr (`next` is the stream tried first and advances past the stream
/// chosen) so continuously replenished stdout credit can never starve a pending
/// stderr grant.
fn take_output_credit(pending: &mut [u64; 2], next: &mut usize) -> Option<VmExecMsg> {
    let stream = [*next, 1 - *next].into_iter().find(|&i| pending[i] > 0)?;
    let bytes = pending[stream].min(u64::from(u32::MAX));
    pending[stream] -= bytes;
    *next = 1 - stream;
    Some(VmExecMsg::OutputWindow {
        stream: stream as u8,
        bytes: bytes as u32,
    })
}

/// `stdin_window`: `Some(INPUT_WINDOW)` in windowed sessions, where stdin is
/// only read (and sent as `Data`) while guest-granted credit remains, so the
/// guest never has to stop reading the connection; `None` = unmetered (PTY).
fn writer_worker(
    sock: UnixStream,
    stdin_fd: RawFd,
    stop_r: OwnedFd,
    ctrl_wake_r: OwnedFd,
    ctrl_rx: mpsc::Receiver<WriterCtrl>,
    mut stdin_window: Option<u64>,
) {
    let mut out_buf: Vec<u8> = Vec::new();
    let mut out_off: usize = 0;
    let mut pending_resize: Option<(u16, u16)> = None;
    let mut pending_out_credit = [0u64; 2];
    let mut next_credit_stream = 0usize;
    let mut stdin_open = stdin_fd >= 0;
    let mut stdin_buf = vec![0u8; CHUNK];

    loop {
        // 1. Drain any pending high-priority control events without blocking.
        while let Ok(ev) = ctrl_rx.try_recv() {
            match ev {
                WriterCtrl::Resize { rows, cols } => pending_resize = Some((rows, cols)),
                WriterCtrl::OutputWindow { stream, bytes } => {
                    let c = &mut pending_out_credit[usize::from(stream)];
                    *c = c.saturating_add(u64::from(bytes));
                }
                WriterCtrl::InputWindow(bytes) => {
                    if let Some(w) = stdin_window.as_mut() {
                        *w = w.saturating_add(u64::from(bytes));
                    }
                }
            }
        }

        // 2. If no frame is mid-flight, pick the next frame in priority order:
        //    resize, coalesced output credit, then stdin.
        if out_off == out_buf.len() {
            out_buf.clear();
            out_off = 0;
            if let Some((rows, cols)) = pending_resize.take() {
                out_buf = VmExecMsg::Resize { rows, cols }.encode();
            } else if let Some(m) =
                take_output_credit(&mut pending_out_credit, &mut next_credit_stream)
            {
                out_buf = m.encode();
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
        let can_read_stdin = stdin_open
            && !need_write
            && pending_resize.is_none()
            && pending_out_credit == [0, 0]
            && stdin_window != Some(0);

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
            // Loop immediately so resize/credit frames run before stdin.
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
            let max = stdin_window.map_or(stdin_buf.len(), |w| {
                usize::try_from(w)
                    .unwrap_or(usize::MAX)
                    .min(stdin_buf.len())
            });
            let n = loop {
                let rc = unsafe { libc::read(stdin_fd, stdin_buf.as_mut_ptr().cast(), max) };
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
                    if let Some(w) = stdin_window.as_mut() {
                        *w -= n as u64;
                    }
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

impl OutputSink<'_> {
    fn raw_fd(&self) -> Option<RawFd> {
        match *self {
            OutputSink::Fd(fd) => Some(fd),
            #[cfg(test)]
            OutputSink::Writer(_) => None,
            #[cfg(not(test))]
            OutputSink::_Marker(_) => None,
        }
    }
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
    RelayFailed,
}

fn read_frame_cancellable(
    conn: &UnixStream,
    frames: &mut FrameReader,
    term_wake_r: Option<RawFd>,
    relay_status_r: Option<RawFd>,
) -> io::Result<ReadOutcome> {
    loop {
        if term_wake_r.is_some_and(fd_ready) {
            return Ok(ReadOutcome::Cancelled);
        }
        if relay_status_r.is_some_and(fd_ready) {
            return Ok(ReadOutcome::RelayFailed);
        }
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
        let mut nfds = 1usize;
        let term_idx = term_wake_r.map(|fd| {
            let i = nfds;
            pfds[i] = libc::pollfd {
                fd,
                events: libc::POLLIN,
                revents: 0,
            };
            nfds += 1;
            i
        });
        let relay_idx = relay_status_r.map(|fd| {
            let i = nfds;
            pfds[i] = libc::pollfd {
                fd,
                events: libc::POLLIN,
                revents: 0,
            };
            nfds += 1;
            i
        });
        let prc = unsafe { libc::poll(pfds.as_mut_ptr(), nfds as libc::nfds_t, -1) };
        if prc < 0 {
            let e = io::Error::last_os_error();
            if e.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(e);
        }
        if let Some(i) = term_idx
            && pfds[i].revents != 0
        {
            return Ok(ReadOutcome::Cancelled);
        }
        if let Some(i) = relay_idx
            && pfds[i].revents != 0
        {
            return Ok(ReadOutcome::RelayFailed);
        }
    }
}

/// Distinguishes guest-side exits and disconnects (where already-received
/// buffered output must still be drained) from local output failures and
/// signal cancellations.
enum RelayOutcome {
    GuestExit(i32),
    GuestError(anyhow::Error),
    LocalBrokenPipe,
    LocalError(io::Error),
    Cancelled,
}

/// Relay stdio for an already-`Started` VM exec session; returns how the
/// session terminated.
fn run_session(
    conn: UnixStream,
    session_token: [u8; 16],
    sock_path: PathBuf,
    initial_tty_size: Option<(u16, u16)>,
    output_window: u32,
    local: LocalIo,
) -> anyhow::Result<SessionExit> {
    let mut out = OutputSink::Fd(1);
    let mut err = OutputSink::Fd(2);
    run_session_io_windowed(
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
        output_window,
        Some(local),
    )
}

/// [`run_session_io_windowed`] against an unmetered guest (`output_window == 0`).
#[cfg(test)]
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
    run_session_io_windowed(
        conn,
        session_token,
        connect_ctrl,
        initial_tty_size,
        stdin_fd,
        stdout_fd,
        out,
        err,
        get_window_size,
        sigint_timeout,
        on_ready,
        0,
        None,
    )
}

/// `output_window` must equal the `ExecSpec::output_window` the guest accepted
/// (nonzero enables bidirectional flow control for non-PTY sessions). `local`
/// is the I/O prepared before `Exec` was sent (production); `None` prepares it
/// here from `stdin_fd`/`out`/`err` (tests with in-process fake guests).
#[allow(clippy::too_many_arguments)]
fn run_session_io_windowed(
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
    output_window: u32,
    local: Option<LocalIo>,
) -> anyhow::Result<SessionExit> {
    let allocate_pty = initial_tty_size.is_some();
    // The guest ignores `output_window` for PTY sessions; mirror that here.
    let output_window = if allocate_pty { 0 } else { output_window };
    // With `--no-stdin` the terminal is not forwarded, so it must never be put
    // in raw mode (which would also disable local Ctrl-C).
    let forwards_stdin = local.as_ref().is_none_or(LocalIo::forwards_stdin);
    let stdin_is_tty = forwards_stdin && is_tty(stdin_fd);
    let stdout_is_tty = is_tty(stdout_fd);
    let want_raw = allocate_pty && stdin_is_tty;

    // Install signal wakeups BEFORE entering raw mode or starting the stdin
    // relay so catchable termination signals always unwind cleanly and
    // restore host termios.
    let sig_setup =
        SignalGuard::install(&[libc::SIGTERM, libc::SIGHUP, libc::SIGINT, libc::SIGQUIT]).ok();

    let raw = if want_raw {
        RawMode::enter_fd(stdin_fd).ok()
    } else {
        None
    };
    let raw_active = raw.is_some();

    let mut local = match local {
        Some(l) => l,
        None => LocalIo::prepare(out.raw_fd(), err.raw_fd(), false)?,
    };
    let (stdin_helper, worker_stdin_fd) = match local.start_stdin(stdin_fd) {
        Ok(v) => v,
        Err(e) => {
            // Disconnecting makes the guest hang up the command; raw mode and
            // signal handlers are restored as this frame unwinds.
            let _ = conn.shutdown(Shutdown::Both);
            return Err(anyhow::Error::new(e)
                .context("cannot relay stdin to the VM command; the command was hung up"));
        }
    };
    let LocalIo {
        mut output_relays,
        eof_stdin: _eof_stdin,
    } = local;

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
                    let _ =
                        terminated_by.compare_exchange(0, sig, Ordering::AcqRel, Ordering::Acquire);
                    let _ = shutdown_sock.shutdown(Shutdown::Both);
                    notify_wake(&term_wake_w);
                    return;
                }
                if sigint_in_flight.swap(true, Ordering::AcqRel) {
                    let _ =
                        terminated_by.compare_exchange(0, sig, Ordering::AcqRel, Ordering::Acquire);
                    let _ = shutdown_sock.shutdown(Shutdown::Both);
                    notify_wake(&term_wake_w);
                    return;
                }
                if sigint_tx.send(sig).is_err() {
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
            writer_worker(
                writer_sock,
                worker_stdin_fd,
                stop_fd,
                ctrl_wake_r,
                ctrl_rx,
                (output_window > 0).then_some(u64::from(vm_exec::INPUT_WINDOW)),
            );
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

    let mut flow = (output_window > 0)
        .then(|| FlowCtl::new(output_window, ctrl_tx.clone(), ctrl_wake_w.clone()));
    let outcome = relay_output_cancellable(
        conn,
        out,
        err,
        &mut output_relays,
        Some(term_wake_r.as_raw_fd()),
        flow.as_mut(),
    );
    drop(flow);

    // Shut down the socket lock-free BEFORE joining relay workers so a blocked
    // writer cannot hang cleanup (e.g. after BrokenPipe on stdout) and the
    // guest observes disconnect promptly.
    let _ = shutdown_sock.shutdown(Shutdown::Both);
    drop(stop_w);
    let _ = writer_thread.join();
    if let Some(h) = resize_thread {
        let _ = h.join();
    }
    if let Some(h) = sigint_thread {
        let _ = h.join();
    }
    drop(stdin_helper);

    // Drain buffered output on EVERY guest exit (including status 141) AND on
    // guest disconnect/protocol errors (preserving already-received diagnostics)
    // while keeping the drain interruptible on termination signals. Abort
    // immediately only on local sink failure or signal cancellation.
    let mut drain_outcome: Option<RelayFinish> = None;
    // A broken split stream mid-session (e.g. `| head` closed stdout) still
    // delivers what the other stream already received: `finish` aborts only
    // the failed channel and drains the survivor under its idle bound.
    let should_drain = terminated_by.load(Ordering::Acquire) == 0
        && match outcome {
            RelayOutcome::GuestExit(_) | RelayOutcome::GuestError(_) => true,
            RelayOutcome::LocalBrokenPipe => matches!(output_relays, OutputRelays::Split { .. }),
            _ => false,
        };
    if should_drain {
        drain_outcome = Some(output_relays.finish(term_wake_r.as_raw_fd()));
    } else {
        output_relays.abort();
    }

    drop(raw);
    drop(stop_r);
    drop(sig_guard.take());

    let sig = terminated_by.load(Ordering::Acquire);
    if sig != 0 {
        return Ok(SessionExit::Signal(sig));
    }
    match outcome {
        RelayOutcome::GuestExit(code) => match drain_outcome {
            Some(RelayFinish::BrokenPipe) if code == 0 => {
                Ok(SessionExit::Code(128 + libc::SIGPIPE))
            }
            Some(RelayFinish::Error(e)) => Err(e.into()),
            _ => Ok(SessionExit::Code(code)),
        },
        RelayOutcome::GuestError(e) => Err(e),
        RelayOutcome::LocalBrokenPipe => Ok(SessionExit::Code(128 + libc::SIGPIPE)),
        RelayOutcome::LocalError(e) => Err(e.into()),
        RelayOutcome::Cancelled => Ok(SessionExit::Code(0)),
    }
}

/// Return output credit for bytes `ch` has fully handed downstream.
fn grant_released(flow: Option<&mut FlowCtl>, ch: &mut SplitChannel, stream: u8) {
    let released = ch.take_released();
    if let Some(f) = flow {
        f.grant(stream, released);
    }
}

/// Multiplex guest output frames across two independent split channels without
/// letting a full stdout pipe (or full stdout relay command pipe) block
/// reading or delivering stderr frames queued behind it.
///
/// With `flow` (windowed guest), the connection is read unconditionally: each
/// host queue is bounded by the guest's per-stream window, and credit is
/// returned only as bytes leave the queue, so a blocked stdout throttles only
/// the guest's stdout. Without `flow` (unmetered guest), reading pauses once
/// either queue reaches [`SPLIT_STREAM_QUEUE_CAP`] to bound host memory.
fn relay_split_cancellable(
    conn: UnixStream,
    out: &mut SplitChannel,
    err: &mut SplitChannel,
    term_wake_r: Option<RawFd>,
    mut flow: Option<&mut FlowCtl>,
) -> RelayOutcome {
    let mut frames = FrameReader::default();
    let conn_fd = conn.as_raw_fd();
    let windowed = flow.is_some();
    loop {
        if term_wake_r.is_some_and(fd_ready) {
            return RelayOutcome::Cancelled;
        }
        if out.status_fd().is_some_and(fd_ready) {
            return out.classify_active_failure();
        }
        if err.status_fd().is_some_and(fd_ready) {
            return err.classify_active_failure();
        }
        match out.try_flush() {
            FlushOutcome::Ok => {}
            FlushOutcome::BrokenPipe => return RelayOutcome::LocalBrokenPipe,
            FlushOutcome::Error(e) => return RelayOutcome::LocalError(e),
        }
        match err.try_flush() {
            FlushOutcome::Ok => {}
            FlushOutcome::BrokenPipe => return RelayOutcome::LocalBrokenPipe,
            FlushOutcome::Error(e) => return RelayOutcome::LocalError(e),
        }
        grant_released(flow.as_deref_mut(), out, vm_exec::OUTPUT_STREAM_STDOUT);
        grant_released(flow.as_deref_mut(), err, vm_exec::OUTPUT_STREAM_STDERR);

        while windowed || (!out.is_queue_full() && !err.is_queue_full()) {
            if term_wake_r.is_some_and(fd_ready) {
                return RelayOutcome::Cancelled;
            }
            if out.status_fd().is_some_and(fd_ready) {
                return out.classify_active_failure();
            }
            if err.status_fd().is_some_and(fd_ready) {
                return err.classify_active_failure();
            }
            let msg = match frames.poll_frame(conn_fd) {
                Ok(PollFrame::Msg(m)) => m,
                Ok(PollFrame::Eof) => {
                    return RelayOutcome::GuestError(anyhow::anyhow!(
                        "the guest closed the connection"
                    ));
                }
                Ok(PollFrame::WouldBlock) => break,
                Err(e) => return RelayOutcome::GuestError(e.into()),
            };
            let (ch, stream, d) = match msg {
                VmExecMsg::Stdout(d) => (&mut *out, vm_exec::OUTPUT_STREAM_STDOUT, d),
                VmExecMsg::Stderr(d) => (&mut *err, vm_exec::OUTPUT_STREAM_STDERR, d),
                VmExecMsg::Exit { code } => return RelayOutcome::GuestExit(code),
                VmExecMsg::Error(e) => return RelayOutcome::GuestError(anyhow::anyhow!("{e}")),
                VmExecMsg::InputWindow { bytes } => {
                    if let Some(f) = flow.as_deref() {
                        f.forward_input_credit(bytes);
                    }
                    continue;
                }
                _ => continue,
            };
            if let Some(f) = flow.as_deref_mut()
                && let Err(e) = f.on_output(stream, d.len())
            {
                return RelayOutcome::GuestError(e);
            }
            match ch.enqueue_and_flush(d) {
                FlushOutcome::Ok => grant_released(flow.as_deref_mut(), ch, stream),
                FlushOutcome::BrokenPipe => return RelayOutcome::LocalBrokenPipe,
                FlushOutcome::Error(e) => return RelayOutcome::LocalError(e),
            }
        }
        let mut pfds = [libc::pollfd {
            fd: -1,
            events: 0,
            revents: 0,
        }; 6];
        let mut nfds = 0usize;
        let mut push_pfd = |fd: RawFd, events: libc::c_short| -> usize {
            let i = nfds;
            pfds[i] = libc::pollfd {
                fd,
                events,
                revents: 0,
            };
            nfds += 1;
            i
        };

        if windowed || (!out.is_queue_full() && !err.is_queue_full()) {
            push_pfd(conn_fd, libc::POLLIN);
        }
        let term_idx = term_wake_r.map(|fd| push_pfd(fd, libc::POLLIN));
        let out_status_idx = out.status_fd().map(|fd| push_pfd(fd, libc::POLLIN));
        let err_status_idx = err.status_fd().map(|fd| push_pfd(fd, libc::POLLIN));
        let out_write_idx = out.write_poll_fd().map(|fd| push_pfd(fd, libc::POLLOUT));
        let err_write_idx = err.write_poll_fd().map(|fd| push_pfd(fd, libc::POLLOUT));

        let prc = unsafe { libc::poll(pfds.as_mut_ptr(), nfds as libc::nfds_t, -1) };
        if prc < 0 {
            let e = io::Error::last_os_error();
            if e.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return RelayOutcome::GuestError(e.into());
        }
        if let Some(i) = term_idx
            && pfds[i].revents != 0
        {
            return RelayOutcome::Cancelled;
        }
        if let Some(i) = out_status_idx
            && pfds[i].revents != 0
        {
            return out.classify_active_failure();
        }
        if let Some(i) = err_status_idx
            && pfds[i].revents != 0
        {
            return err.classify_active_failure();
        }
        if let Some(i) = out_write_idx
            && (pfds[i].revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL)) != 0
            && (pfds[i].revents & libc::POLLOUT) == 0
        {
            match out.try_flush() {
                FlushOutcome::Ok => return out.classify_active_failure(),
                FlushOutcome::BrokenPipe => return RelayOutcome::LocalBrokenPipe,
                FlushOutcome::Error(e) => return RelayOutcome::LocalError(e),
            }
        }
        if let Some(i) = err_write_idx
            && (pfds[i].revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL)) != 0
            && (pfds[i].revents & libc::POLLOUT) == 0
        {
            match err.try_flush() {
                FlushOutcome::Ok => return err.classify_active_failure(),
                FlushOutcome::BrokenPipe => return RelayOutcome::LocalBrokenPipe,
                FlushOutcome::Error(e) => return RelayOutcome::LocalError(e),
            }
        }
    }
}

/// Read output/exit frames from `conn` into `output_relays` (or direct `out` /
/// `err` sinks), waking immediately on `term_wake_r` or downstream helper
/// failure (`status_fd`) even if the guest sends no further frames.
fn relay_output_cancellable(
    conn: UnixStream,
    out: &mut OutputSink<'_>,
    err: &mut OutputSink<'_>,
    output_relays: &mut OutputRelays,
    term_wake_r: Option<RawFd>,
    mut flow: Option<&mut FlowCtl>,
) -> RelayOutcome {
    if let OutputRelays::Split {
        out: out_ch,
        err: err_ch,
    } = output_relays
    {
        return relay_split_cancellable(conn, out_ch, err_ch, term_wake_r, flow);
    }
    let mut frames = FrameReader::default();
    let shared = match output_relays {
        OutputRelays::Shared(r) => Some(&*r),
        _ => None,
    };
    let relay_status_r = shared.map(OutputRelay::status_fd);
    loop {
        match read_frame_cancellable(&conn, &mut frames, term_wake_r, relay_status_r) {
            Ok(ReadOutcome::Msg(VmExecMsg::Stdout(d))) => {
                if let Some(f) = flow.as_deref_mut()
                    && let Err(e) = f.on_output(vm_exec::OUTPUT_STREAM_STDOUT, d.len())
                {
                    return RelayOutcome::GuestError(e);
                }
                let res = match shared {
                    Some(r) if r.handles_out => {
                        r.write_frame_cancellable(RELAY_DEST_STDOUT, &d, term_wake_r)
                    }
                    _ => write_cancellable(out, &d, term_wake_r),
                };
                match res {
                    Ok(WriteOutcome::Written) => {
                        if let Some(f) = flow.as_deref_mut() {
                            f.grant(vm_exec::OUTPUT_STREAM_STDOUT, d.len());
                        }
                    }
                    Ok(WriteOutcome::BrokenPipe) => return RelayOutcome::LocalBrokenPipe,
                    Ok(WriteOutcome::Cancelled) => return RelayOutcome::Cancelled,
                    Err(e) => return RelayOutcome::LocalError(e),
                }
            }
            Ok(ReadOutcome::Msg(VmExecMsg::Stderr(d))) => {
                if let Some(f) = flow.as_deref_mut()
                    && let Err(e) = f.on_output(vm_exec::OUTPUT_STREAM_STDERR, d.len())
                {
                    return RelayOutcome::GuestError(e);
                }
                let res = match shared {
                    Some(r) if r.handles_err => {
                        r.write_frame_cancellable(RELAY_DEST_STDERR, &d, term_wake_r)
                    }
                    _ => write_cancellable(err, &d, term_wake_r),
                };
                match res {
                    Ok(WriteOutcome::Written) => {
                        if let Some(f) = flow.as_deref_mut() {
                            f.grant(vm_exec::OUTPUT_STREAM_STDERR, d.len());
                        }
                    }
                    Ok(WriteOutcome::BrokenPipe) => return RelayOutcome::LocalBrokenPipe,
                    Ok(WriteOutcome::Cancelled) => return RelayOutcome::Cancelled,
                    Err(e) => return RelayOutcome::LocalError(e),
                }
            }
            Ok(ReadOutcome::Msg(VmExecMsg::Exit { code })) => return RelayOutcome::GuestExit(code),
            Ok(ReadOutcome::Msg(VmExecMsg::InputWindow { bytes })) => {
                if let Some(f) = flow.as_deref() {
                    f.forward_input_credit(bytes);
                }
            }
            Ok(ReadOutcome::Msg(VmExecMsg::Error(e))) => {
                return RelayOutcome::GuestError(anyhow::anyhow!("{e}"));
            }
            Ok(ReadOutcome::Msg(_)) => {}
            Ok(ReadOutcome::Cancelled) => return RelayOutcome::Cancelled,
            Ok(ReadOutcome::RelayFailed) => {
                return shared.map_or(
                    RelayOutcome::LocalBrokenPipe,
                    OutputRelay::classify_active_failure,
                );
            }
            Ok(ReadOutcome::Eof) => {
                return RelayOutcome::GuestError(anyhow::anyhow!(
                    "the guest closed the connection"
                ));
            }
            Err(e) => return RelayOutcome::GuestError(e.into()),
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
        assert!(!e.tty && !e.no_stdin && e.workdir.is_none() && e.cmd == ["uname", "-a"]);
        let VmCmd::Exec(e) = parse(&["vm", "exec", "-n", "uname"]) else {
            panic!()
        };
        assert!(e.no_stdin && e.cmd == ["uname"]);

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
        let outcome = relay_output_cancellable(
            host,
            &mut out,
            &mut err,
            &mut OutputRelays::None,
            None,
            None,
        );
        peer.join().unwrap();
        assert!(matches!(outcome, RelayOutcome::GuestExit(7)));
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
        let RelayOutcome::GuestError(e) = relay_output_cancellable(
            host,
            &mut out,
            &mut err,
            &mut OutputRelays::None,
            None,
            None,
        ) else {
            panic!("expected GuestError");
        };
        assert!(e.to_string().contains("boom"), "{e}");

        let (host, guest) = UnixStream::pair().unwrap();
        drop(guest);
        let RelayOutcome::GuestError(e) = relay_output_cancellable(
            host,
            &mut out,
            &mut err,
            &mut OutputRelays::None,
            None,
            None,
        ) else {
            panic!("expected GuestError");
        };
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
        assert!(matches!(p, Probe::TimedOut));
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
            loop {
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

        // Race a rival blocking reader thread against StdinHelper, then let
        // the guest exit while stdin_w is still open (so StdinHelper blocks in
        // read(0)). Running the rival reader on a background thread avoids any
        // poll-vs-read hang on the main thread if StdinHelper wins the byte;
        // teardown must kill/reap the helper promptly before `wf` is closed.
        let rival = thread::spawn(move || {
            let mut scratch = [0u8; 1];
            loop {
                let rc = unsafe { libc::read(dup_r.as_raw_fd(), scratch.as_mut_ptr().cast(), 1) };
                if rc < 0 && io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                // Blocking fd must never surface EAGAIN to a rival reader.
                assert!(
                    rc >= 0,
                    "rival blocking read failed: {}",
                    io::Error::last_os_error()
                );
                return rc;
            }
        });
        let mut wf = std::fs::File::from(stdin_w);
        wf.write_all(b"x").unwrap();
        thread::sleep(Duration::from_millis(20));

        let t = Instant::now();
        vm_exec::write(&mut guest, &VmExecMsg::Exit { code: 0 }).unwrap();
        let exit = session.join().unwrap().unwrap();
        assert_eq!(exit, SessionExit::Code(0));
        assert!(t.elapsed() < Duration::from_secs(2));
        assert_eq!(
            unsafe { libc::fcntl(stdin_r.as_raw_fd(), libc::F_GETFL) } & libc::O_NONBLOCK,
            0
        );
        // Closing stdin_w delivers EOF if StdinHelper won the single byte.
        drop(wf);
        let _ = rival.join().unwrap();
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
            loop {
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
            loop {
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

    #[test]
    #[ignore = "internal OutputRelay helper entrypoint invoked by unit tests"]
    fn io_relay_helper_entrypoint() {
        let code = match std::env::var("KATAMARAN_TEST_IO_RELAY").as_deref() {
            Ok("output") => run_output_relay_fds(3, 4, 5, 6, 7),
            Ok("stdin") => run_stdin_relay_fds(3, 4, 5),
            _ => return,
        };
        unsafe { libc::_exit(code) };
    }

    #[test]
    fn merged_stdout_and_stderr_preserve_frame_order_across_distinct_fds() {
        let _lock = SIGNAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let (stdin_r, stdin_w) = pipe_cloexec(false).unwrap();
        let (merged_r, out_w) = pipe_cloexec(false).unwrap();
        // Distinct descriptor number pointing to the exact same pipe writer (2>&1).
        let err_w = out_w.try_clone().unwrap();
        assert_ne!(out_w.as_raw_fd(), err_w.as_raw_fd());

        let (host, mut guest) = UnixStream::pair().unwrap();
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
        assert_eq!(
            unsafe { libc::fcntl(out_w.as_raw_fd(), libc::F_GETFL) } & libc::O_NONBLOCK,
            0
        );
        assert_eq!(
            unsafe { libc::fcntl(err_w.as_raw_fd(), libc::F_GETFL) } & libc::O_NONBLOCK,
            0
        );

        let reader = thread::spawn(move || {
            let mut f = std::fs::File::from(merged_r);
            let mut buf = Vec::new();
            io::Read::read_to_end(&mut f, &mut buf).unwrap();
            buf
        });

        let mut expected = Vec::new();
        for i in 0..100u32 {
            let o = format!("O{i:03}\n");
            let e = format!("E{i:03}\n");
            expected.extend_from_slice(o.as_bytes());
            expected.extend_from_slice(e.as_bytes());
            vm_exec::write(&mut guest, &VmExecMsg::Stdout(o.into_bytes())).unwrap();
            vm_exec::write(&mut guest, &VmExecMsg::Stderr(e.into_bytes())).unwrap();
        }
        vm_exec::write(&mut guest, &VmExecMsg::Exit { code: 0 }).unwrap();

        assert_eq!(session.join().unwrap().unwrap(), SessionExit::Code(0));
        drop(out_w);
        drop(err_w);
        let received = reader.join().unwrap();
        assert_eq!(received, expected);
        drop(stdin_w);
        drop(stdin_r);
    }

    #[test]
    fn broken_downstream_pipe_wakes_idle_session_without_further_guest_frames() {
        let _lock = SIGNAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let (stdin_r, stdin_w) = pipe_cloexec(false).unwrap();
        let (out_r, out_w) = pipe_cloexec(false).unwrap();
        let (_err_r, err_w) = pipe_cloexec(false).unwrap();

        let (host, mut guest) = UnixStream::pair().unwrap();
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
        // Close downstream reader, send ONE small stdout frame, and keep the
        // guest socket completely open and idle.
        drop(out_r);
        let t = Instant::now();
        vm_exec::write(&mut guest, &VmExecMsg::Stdout(b"single-chunk\n".to_vec())).unwrap();

        let exit = session.join().unwrap().unwrap();
        assert_eq!(exit, SessionExit::Code(128 + libc::SIGPIPE));
        assert!(t.elapsed() < Duration::from_secs(2));

        // Guest must observe socket shutdown promptly.
        let mut ds = DeadlineStream::new(
            &mut guest,
            Instant::now() + Duration::from_secs(2),
            "guest did not observe disconnect".into(),
        );
        assert!(matches!(vm_exec::read(&mut ds), Ok(None) | Err(_)));
        drop(ds);
        drop(out_w);
        drop(err_w);
        drop(stdin_w);
        drop(stdin_r);
    }

    #[test]
    fn guest_exit_141_drains_buffered_output_to_slow_reader() {
        let _lock = SIGNAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let (stdin_r, stdin_w) = pipe_cloexec(false).unwrap();
        let (out_r, out_w) = pipe_cloexec(false).unwrap();
        let (_err_r, err_w) = pipe_cloexec(false).unwrap();

        let (host, mut guest) = UnixStream::pair().unwrap();
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
        // Send enough output to keep bytes buffered in the output relay while
        // the downstream reader is paused, then exit with guest status 141.
        let payload = vec![b'Z'; 96 << 10];
        let guest_bg = {
            let payload = payload.clone();
            thread::spawn(move || {
                vm_exec::write(&mut guest, &VmExecMsg::Stdout(payload)).unwrap();
                vm_exec::write(&mut guest, &VmExecMsg::Exit { code: 141 }).unwrap();
            })
        };

        thread::sleep(Duration::from_millis(80));
        let reader = thread::spawn(move || {
            let mut f = std::fs::File::from(out_r);
            let mut buf = Vec::new();
            io::Read::read_to_end(&mut f, &mut buf).unwrap();
            buf
        });

        let exit = session.join().unwrap().unwrap();
        assert_eq!(exit, SessionExit::Code(141));
        drop(out_w);
        drop(err_w);
        let received = reader.join().unwrap();
        assert_eq!(received.len(), payload.len());
        assert_eq!(received, payload);
        guest_bg.join().unwrap();
        drop(stdin_w);
        drop(stdin_r);
    }

    #[test]
    fn guest_disconnect_flushes_received_output_before_returning_error() {
        let _lock = SIGNAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let (stdin_r, stdin_w) = pipe_cloexec(false).unwrap();
        let (out_r, out_w) = pipe_cloexec(false).unwrap();
        let (_err_r, err_w) = pipe_cloexec(false).unwrap();

        let (host, mut guest) = UnixStream::pair().unwrap();
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
        let payload = vec![b'D'; 96 << 10];
        let guest_bg = {
            let payload = payload.clone();
            thread::spawn(move || {
                vm_exec::write(&mut guest, &VmExecMsg::Stdout(payload)).unwrap();
                // Disconnect abruptly without sending Exit.
                drop(guest);
            })
        };

        thread::sleep(Duration::from_millis(80));
        let reader = thread::spawn(move || {
            let mut f = std::fs::File::from(out_r);
            let mut buf = Vec::new();
            io::Read::read_to_end(&mut f, &mut buf).unwrap();
            buf
        });

        let err = session.join().unwrap().unwrap_err();
        assert!(err.to_string().contains("closed"), "{err:#}");
        drop(out_w);
        drop(err_w);
        let received = reader.join().unwrap();
        assert_eq!(received.len(), payload.len());
        assert_eq!(received, payload);
        guest_bg.join().unwrap();
        drop(stdin_w);
        drop(stdin_r);
    }

    #[test]
    fn independent_stderr_flows_while_stdout_pipe_is_blocked() {
        let _lock = SIGNAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let (stdin_r, stdin_w) = pipe_cloexec(false).unwrap();
        let (out_r, out_w) = pipe_cloexec(false).unwrap();
        let (err_r, err_w) = pipe_cloexec(false).unwrap();
        let out_dup = out_w.try_clone().unwrap();
        assert!(provably_distinct_fds(out_w.as_raw_fd(), err_w.as_raw_fd()));
        assert!(!provably_distinct_fds(
            out_w.as_raw_fd(),
            out_dup.as_raw_fd()
        ));
        drop(out_dup);
        // Two character devices (e.g. PTY slave vs `/dev/tty`) conservatively
        // share one ordered relay.
        let (_m1, s1) = open_test_pty(24, 80);
        let (_m2, s2) = open_test_pty(24, 80);
        assert!(!provably_distinct_fds(s1.as_raw_fd(), s2.as_raw_fd()));

        let (host, mut guest) = UnixStream::pair().unwrap();
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

        // Overflow BOTH the downstream stdout pipe AND the stdout relay
        // helper's command pipe (512 KiB >> 2 * 64 KiB + helper buffer) while
        // `out_r` stays completely unread, queue another small stdout frame,
        // and then emit a stderr readiness marker. The marker MUST still be
        // read from `conn` and arrive on `err_r` before reading `out_r`.
        let fill_chunk = vec![b'O'; 16 << 10];
        let fill_chunks = 32usize;
        let tail_payload = b"queued-after-fill\n";
        let marker = b"STDERR_READY\n";
        let (unblock_tx, unblock_rx) = mpsc::channel();
        let guest_bg = {
            let fill_chunk = fill_chunk.clone();
            thread::spawn(move || {
                for _ in 0..fill_chunks {
                    vm_exec::write(&mut guest, &VmExecMsg::Stdout(fill_chunk.clone())).unwrap();
                }
                vm_exec::write(&mut guest, &VmExecMsg::Stdout(tail_payload.to_vec())).unwrap();
                vm_exec::write(&mut guest, &VmExecMsg::Stderr(marker.to_vec())).unwrap();
                unblock_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                vm_exec::write(&mut guest, &VmExecMsg::Exit { code: 0 }).unwrap();
            })
        };

        let mut got_err = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut buf = [0u8; 64];
        while got_err.len() < marker.len() {
            let rem = deadline.saturating_duration_since(Instant::now());
            assert!(
                !rem.is_zero(),
                "stderr marker blocked behind full stdout relay queue (got {:?})",
                String::from_utf8_lossy(&got_err)
            );
            let mut pfd = libc::pollfd {
                fd: err_r.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            let rc = unsafe { libc::poll(&mut pfd, 1, rem.as_millis().clamp(1, 2000) as i32) };
            if rc > 0 {
                let n =
                    unsafe { libc::read(err_r.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
                assert!(n > 0);
                got_err.extend_from_slice(&buf[..n as usize]);
            }
        }
        assert_eq!(got_err, marker);

        // Now release guest exit and drain stdout (verifying every host-queued
        // byte flushes cleanly during `OutputRelays::finish`).
        let _ = unblock_tx.send(());
        let out_reader = thread::spawn(move || {
            let mut f = std::fs::File::from(out_r);
            let mut out = Vec::new();
            io::Read::read_to_end(&mut f, &mut out).unwrap();
            out
        });

        assert_eq!(session.join().unwrap().unwrap(), SessionExit::Code(0));
        drop(out_w);
        drop(err_w);
        let received_out = out_reader.join().unwrap();
        assert_eq!(
            received_out.len(),
            fill_chunk.len() * fill_chunks + tail_payload.len()
        );
        assert!(received_out.ends_with(tail_payload));
        guest_bg.join().unwrap();
        drop(err_r);
        drop(stdin_w);
        drop(stdin_r);
    }

    #[test]
    fn split_drain_aborts_blocked_peer_on_epipe_and_preserves_nonzero_exit() {
        let _lock = SIGNAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        for (guest_code, expected) in [(42, 42), (0, 128 + libc::SIGPIPE)] {
            let (stdin_r, stdin_w) = pipe_cloexec(false).unwrap();
            let (out_r, out_w) = pipe_cloexec(false).unwrap();
            let (err_r, err_w) = pipe_cloexec(false).unwrap();

            let (host, mut guest) = UnixStream::pair().unwrap();
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
            // Fill BOTH stdout and stderr beyond pipe+relay capacity while
            // keeping `err_r` open and unread for the entire test. Send Exit,
            // wait for host socket shutdown (proving Exit was received and
            // drain has begun), then close ONLY `out_r`. Split draining must
            // abort ONLY stdout, keep the stderr survivor draining until it
            // makes no progress for `SPLIT_DRAIN_SURVIVOR_IDLE`, then abort
            // the still-blocked stderr helper and return `expected`.
            let guest_bg = thread::spawn(move || {
                let chunk = vec![b'E'; 16 << 10];
                for _ in 0..16 {
                    vm_exec::write(&mut guest, &VmExecMsg::Stdout(chunk.clone())).unwrap();
                    vm_exec::write(&mut guest, &VmExecMsg::Stderr(chunk.clone())).unwrap();
                }
                vm_exec::write(&mut guest, &VmExecMsg::Exit { code: guest_code }).unwrap();
                let mut ds = DeadlineStream::new(
                    &mut guest,
                    Instant::now() + Duration::from_secs(2),
                    "timed out waiting for host shutdown before drain".into(),
                );
                let _ = vm_exec::read(&mut ds);
                drop(ds);
                drop(out_r);
            });

            let t = Instant::now();
            let exit = session.join().unwrap().unwrap();
            assert_eq!(exit, SessionExit::Code(expected));
            assert!(
                t.elapsed() < SPLIT_DRAIN_SURVIVOR_IDLE + Duration::from_secs(2),
                "split drain hung on unread peer pipe after EPIPE"
            );
            guest_bg.join().unwrap();
            drop(err_r);
            drop(out_w);
            drop(err_w);
            drop(stdin_w);
            drop(stdin_r);
        }
    }

    #[test]
    fn nonzero_guest_exit_is_preserved_when_drain_hits_epipe() {
        let _lock = SIGNAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        for (guest_code, expected) in [(42, 42), (0, 128 + libc::SIGPIPE)] {
            let (stdin_r, stdin_w) = pipe_cloexec(false).unwrap();
            let (out_r, out_w) = pipe_cloexec(false).unwrap();
            let (_err_r, err_w) = pipe_cloexec(false).unwrap();

            let (host, mut guest) = UnixStream::pair().unwrap();
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
            // Buffer enough stdout to hold the output relay open during drain,
            // send Exit { code }, then wait for host socket shutdown (which
            // occurs strictly after GuestExit is received and before drain
            // completes) before dropping `out_r` to trigger drain-time EPIPE.
            let guest_bg = thread::spawn(move || {
                vm_exec::write(&mut guest, &VmExecMsg::Stdout(vec![b'E'; 96 << 10])).unwrap();
                vm_exec::write(&mut guest, &VmExecMsg::Exit { code: guest_code }).unwrap();
                let mut ds = DeadlineStream::new(
                    &mut guest,
                    Instant::now() + Duration::from_secs(2),
                    "timed out waiting for host shutdown before drain".into(),
                );
                let _ = vm_exec::read(&mut ds);
                drop(ds);
                drop(out_r);
            });

            let exit = session.join().unwrap().unwrap();
            assert_eq!(exit, SessionExit::Code(expected));
            guest_bg.join().unwrap();
            drop(out_w);
            drop(err_w);
            drop(stdin_w);
            drop(stdin_r);
        }
    }

    #[test]
    fn split_drain_keeps_flushing_stderr_to_slow_reader_after_stdout_epipe() {
        // Host-queued stderr far beyond what the pipe + relay can hold.
        split_drain_after_stdout_epipe(24, 8 << 10, Duration::from_millis(5));
    }

    #[test]
    fn split_drain_tracks_relay_helper_progress_for_slow_stderr_reader() {
        // Everything fits in the relay command pipe + stderr pipe, so the host
        // queue empties at once; the reader then takes several multiples of
        // `SPLIT_DRAIN_SURVIVOR_IDLE` in total while progressing well within
        // it, which only the helper's progress pipe can observe.
        let pause = Duration::from_millis(60);
        let chunk = 16usize << 10;
        let chunks = 6usize;
        let read = 2usize << 10;
        assert!(pause * (chunk * chunks / read) as u32 > SPLIT_DRAIN_SURVIVOR_IDLE * 3);
        split_drain_after_stdout_epipe(chunks, read, pause);
    }

    #[test]
    fn split_drain_survivor_tolerates_reader_consuming_less_than_a_slice_per_read() {
        // Each read takes a quarter of a relay write slice; one slice is
        // consumed well within the idle period, the whole drain is not.
        let pause = Duration::from_millis(60);
        let read = RELAY_WRITE_SLICE / 4;
        assert!(pause * 6 < SPLIT_DRAIN_SURVIVOR_IDLE);
        assert!(pause * ((16 << 10) / read) as u32 > SPLIT_DRAIN_SURVIVOR_IDLE * 2);
        split_drain_after_stdout_epipe(1, read, pause);
    }

    fn split_drain_after_stdout_epipe(chunks: usize, read_size: usize, pause: Duration) {
        let _lock = SIGNAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let (stdin_r, stdin_w) = pipe_cloexec(false).unwrap();
        let (out_r, out_w) = pipe_cloexec(false).unwrap();
        let (err_r, err_w) = pipe_cloexec(false).unwrap();

        let (host, mut guest) = UnixStream::pair().unwrap();
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
        // Buffer far more stderr than the pipe + relay can hold, send Exit,
        // wait until drain has begun, then close stdout (EPIPE during drain)
        // and only THEN start a slow stderr reader: every stderr byte already
        // received from the guest must still be delivered.
        let chunk = 16usize << 10;
        let guest_bg = thread::spawn(move || {
            vm_exec::write(&mut guest, &VmExecMsg::Stdout(vec![b'O'; 96 << 10])).unwrap();
            for i in 0..chunks {
                vm_exec::write(&mut guest, &VmExecMsg::Stderr(vec![b'a' + i as u8; chunk]))
                    .unwrap();
            }
            vm_exec::write(&mut guest, &VmExecMsg::Exit { code: 3 }).unwrap();
            let mut ds = DeadlineStream::new(
                &mut guest,
                Instant::now() + Duration::from_secs(2),
                "timed out waiting for host shutdown before drain".into(),
            );
            let _ = vm_exec::read(&mut ds);
            drop(ds);
            drop(out_r);
            let mut f = std::fs::File::from(err_r);
            let mut got = Vec::new();
            let mut buf = vec![0u8; read_size];
            loop {
                thread::sleep(pause);
                match io::Read::read(&mut f, &mut buf) {
                    Ok(0) => return got,
                    Ok(n) => got.extend_from_slice(&buf[..n]),
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                    Err(e) => panic!("stderr read: {e}"),
                }
            }
        });

        let exit = session.join().unwrap().unwrap();
        // Nonzero guest status wins over the drain-time stdout EPIPE.
        assert_eq!(exit, SessionExit::Code(3));
        drop(out_w);
        drop(err_w);
        let got = guest_bg.join().unwrap();
        let expected: Vec<u8> = (0..chunks)
            .flat_map(|i| std::iter::repeat_n(b'a' + i as u8, chunk))
            .collect();
        assert_eq!(got.len(), expected.len());
        assert!(
            got == expected,
            "stderr bytes lost or reordered after stdout EPIPE"
        );
        drop(stdin_w);
        drop(stdin_r);
    }

    #[test]
    fn windowed_split_reads_stderr_past_host_queue_cap_and_returns_stdout_credit() {
        let _lock = SIGNAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let (stdin_r, stdin_w) = pipe_cloexec(false).unwrap();
        let (out_r, out_w) = pipe_cloexec(false).unwrap();
        let (err_r, err_w) = pipe_cloexec(false).unwrap();
        // A window larger than `SPLIT_STREAM_QUEUE_CAP`: an unmetered host
        // would stop reading the connection at the cap and never see the
        // stderr marker queued behind this much blocked stdout.
        let window = (SPLIT_STREAM_QUEUE_CAP + (1 << 20)) as u32;

        let (host, mut guest) = UnixStream::pair().unwrap();
        let (ready_tx, ready_rx) = mpsc::channel();
        let stdin_fd = stdin_r.as_raw_fd();
        let out_fd = out_w.as_raw_fd();
        let err_fd = err_w.as_raw_fd();
        let session = thread::spawn(move || {
            let mut out = OutputSink::Fd(out_fd);
            let mut err = OutputSink::Fd(err_fd);
            run_session_io_windowed(
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
                window,
                None,
            )
        });
        ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();

        let credits = Arc::new(Mutex::new([0u64; 2]));
        let mut guest_rd = guest.try_clone().unwrap();
        let credit_reader = {
            let credits = credits.clone();
            thread::spawn(move || {
                while let Ok(Some(m)) = vm_exec::read(&mut guest_rd) {
                    if let VmExecMsg::OutputWindow { stream, bytes } = m {
                        credits.lock().unwrap()[usize::from(stream)] += u64::from(bytes);
                    }
                }
            })
        };

        let marker = b"STDERR_READY\n";
        let (exit_tx, exit_rx) = mpsc::channel::<()>();
        let guest_bg = thread::spawn(move || {
            let mut sent = 0usize;
            while sent < window as usize {
                let n = CHUNK.min(window as usize - sent);
                vm_exec::write(&mut guest, &VmExecMsg::Stdout(vec![b'O'; n])).unwrap();
                sent += n;
            }
            vm_exec::write(&mut guest, &VmExecMsg::Stderr(marker.to_vec())).unwrap();
            exit_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            vm_exec::write(&mut guest, &VmExecMsg::Exit { code: 0 }).unwrap();
        });

        // The marker arrives while `out_r` is still completely unread.
        let mut got_err = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut buf = [0u8; 64];
        while got_err.len() < marker.len() {
            let rem = deadline.saturating_duration_since(Instant::now());
            assert!(!rem.is_zero(), "stderr marker blocked behind stdout");
            let mut pfd = libc::pollfd {
                fd: err_r.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            if unsafe { libc::poll(&mut pfd, 1, rem.as_millis().clamp(1, 1000) as i32) } > 0 {
                let n =
                    unsafe { libc::read(err_r.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
                assert!(n > 0);
                got_err.extend_from_slice(&buf[..n as usize]);
            }
        }
        assert_eq!(got_err, marker);
        let early_stdout_credit = credits.lock().unwrap()[0];
        assert!(
            early_stdout_credit < u64::from(window),
            "stdout credit returned for bytes still queued on the host"
        );

        // Draining stdout hands the queued bytes downstream, returning credit.
        let out_reader = thread::spawn(move || {
            let mut f = std::fs::File::from(out_r);
            let mut out = Vec::new();
            io::Read::read_to_end(&mut f, &mut out).unwrap();
            out.len()
        });
        let deadline = Instant::now() + Duration::from_secs(10);
        while credits.lock().unwrap()[0] < u64::from(window) {
            assert!(Instant::now() < deadline, "stdout credit never returned");
            thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(credits.lock().unwrap()[1], marker.len() as u64);
        exit_tx.send(()).unwrap();

        assert_eq!(session.join().unwrap().unwrap(), SessionExit::Code(0));
        drop(out_w);
        drop(err_w);
        assert_eq!(out_reader.join().unwrap(), window as usize);
        guest_bg.join().unwrap();
        credit_reader.join().unwrap();
        drop(stdin_w);
        drop(stdin_r);
    }

    #[test]
    fn flow_ctl_returns_credit_forwards_stdin_credit_and_rejects_window_overrun() {
        let (host, mut guest) = UnixStream::pair().unwrap();
        vm_exec::write(&mut guest, &VmExecMsg::InputWindow { bytes: 77 }).unwrap();
        vm_exec::write(&mut guest, &VmExecMsg::Stdout(vec![b'x'; 1000])).unwrap();
        vm_exec::write(&mut guest, &VmExecMsg::Stderr(vec![b'y'; 1025])).unwrap();
        let (tx, rx) = mpsc::channel();
        let (_wake_r, wake_w) = pipe_cloexec(true).unwrap();
        let mut flow = FlowCtl::new(1024, tx, Arc::new(wake_w));
        let (mut out_buf, mut err_buf) = (Vec::new(), Vec::new());
        let mut out = OutputSink::Writer(&mut out_buf);
        let mut err = OutputSink::Writer(&mut err_buf);
        let RelayOutcome::GuestError(e) = relay_output_cancellable(
            host,
            &mut out,
            &mut err,
            &mut OutputRelays::None,
            None,
            Some(&mut flow),
        ) else {
            panic!("expected GuestError for a stderr window overrun");
        };
        assert!(e.to_string().contains("stderr flow-control window"), "{e}");
        assert_eq!(out_buf.len(), 1000);
        assert!(err_buf.is_empty());
        let ctrl: Vec<_> = rx.try_iter().collect();
        assert!(matches!(ctrl[0], WriterCtrl::InputWindow(77)));
        assert!(matches!(
            ctrl[1],
            WriterCtrl::OutputWindow {
                stream: vm_exec::OUTPUT_STREAM_STDOUT,
                bytes: 1000
            }
        ));
        assert_eq!(ctrl.len(), 2);
    }

    #[test]
    fn output_credit_selection_alternates_so_stderr_is_never_starved() {
        let mut pending = [0u64; 2];
        let mut next = 0usize;
        assert!(take_output_credit(&mut pending, &mut next).is_none());
        let credit = |stream: u8, bytes: u32| VmExecMsg::OutputWindow { stream, bytes };
        // stdout credit is replenished before every selection; stderr credit
        // stays pending and must still get the very next turn.
        pending = [100, 7];
        assert_eq!(
            take_output_credit(&mut pending, &mut next),
            Some(credit(0, 100))
        );
        pending[0] += 50;
        assert_eq!(
            take_output_credit(&mut pending, &mut next),
            Some(credit(1, 7))
        );
        pending[0] += 25;
        assert_eq!(
            take_output_credit(&mut pending, &mut next),
            Some(credit(0, 75))
        );
        pending[1] += 3;
        pending[0] += 10;
        assert_eq!(
            take_output_credit(&mut pending, &mut next),
            Some(credit(1, 3))
        );
        assert_eq!(
            take_output_credit(&mut pending, &mut next),
            Some(credit(0, 10))
        );
        assert!(take_output_credit(&mut pending, &mut next).is_none());
        // Oversized coalesced credit is split into u32-sized grants.
        pending = [u64::from(u32::MAX) + 5, 0];
        assert_eq!(
            take_output_credit(&mut pending, &mut next),
            Some(credit(0, u32::MAX))
        );
        assert_eq!(
            take_output_credit(&mut pending, &mut next),
            Some(credit(0, 5))
        );
    }

    #[test]
    fn writer_sends_stdin_only_within_guest_granted_window() {
        let (host, mut guest) = UnixStream::pair().unwrap();
        host.set_nonblocking(true).unwrap();
        let (stdin_r, stdin_w) = pipe_cloexec(false).unwrap();
        let (stop_r, stop_w) = pipe_cloexec(false).unwrap();
        let (ctrl_wake_r, ctrl_wake_w) = pipe_cloexec(true).unwrap();
        let (ctrl_tx, ctrl_rx) = mpsc::channel();
        let stdin_fd = stdin_r.as_raw_fd();
        let writer = thread::spawn(move || {
            writer_worker(
                host,
                stdin_fd,
                stop_r,
                ctrl_wake_r,
                ctrl_rx,
                Some(u64::from(vm_exec::INPUT_WINDOW)),
            );
        });
        let feeder = thread::spawn(move || {
            let mut f = std::fs::File::from(stdin_w);
            let chunk = vec![b's'; 32 << 10];
            // Far more than the window; blocks once the writer stops reading.
            for _ in 0..32 {
                if io::Write::write_all(&mut f, &chunk).is_err() {
                    return;
                }
            }
        });

        let read_data_until_quiet = |guest: &mut UnixStream| -> usize {
            let mut total = 0usize;
            loop {
                let mut ds = DeadlineStream::new(
                    guest,
                    Instant::now() + Duration::from_millis(300),
                    "quiet".into(),
                );
                match vm_exec::read(&mut ds) {
                    Ok(Some(VmExecMsg::Data(d))) => total += d.len(),
                    Ok(Some(other)) => panic!("unexpected frame {other:?}"),
                    Ok(None) => panic!("writer closed the connection"),
                    Err(e) if e.kind() == io::ErrorKind::TimedOut => return total,
                    Err(e) => panic!("read: {e}"),
                }
            }
        };

        let window = vm_exec::INPUT_WINDOW as usize;
        assert_eq!(read_data_until_quiet(&mut guest), window);
        ctrl_tx.send(WriterCtrl::InputWindow(10_000)).unwrap();
        notify_wake(&ctrl_wake_w);
        assert_eq!(read_data_until_quiet(&mut guest), 10_000);

        // Output credit is sent ahead of further stdin.
        ctrl_tx
            .send(WriterCtrl::OutputWindow {
                stream: vm_exec::OUTPUT_STREAM_STDERR,
                bytes: 5,
            })
            .unwrap();
        notify_wake(&ctrl_wake_w);
        let mut ds = DeadlineStream::new(
            &mut guest,
            Instant::now() + Duration::from_secs(2),
            "no OutputWindow".into(),
        );
        assert_eq!(
            vm_exec::read(&mut ds).unwrap(),
            Some(VmExecMsg::OutputWindow {
                stream: vm_exec::OUTPUT_STREAM_STDERR,
                bytes: 5
            })
        );
        drop(ds);

        drop(stop_w);
        writer.join().unwrap();
        drop(stdin_r);
        drop(guest);
        feeder.join().unwrap();
    }

    #[test]
    fn no_stdin_reads_eof_immediately_and_spec_size_is_checked_locally() {
        let mut local = LocalIo::prepare(None, None, true).unwrap();
        assert!(!local.forwards_stdin());
        let (helper, fd) = local.start_stdin(0).unwrap();
        assert!(helper.is_none());
        let mut b = [0u8; 1];
        let n = unsafe { libc::read(fd, b.as_mut_ptr().cast(), 1) };
        assert_eq!(n, 0, "--no-stdin must present immediate EOF");

        let huge = "x".repeat(kat_proto::MAX_FRAME);
        let err = build_spec(None, &[], vec!["echo".into(), huge], false).unwrap_err();
        assert!(err.to_string().contains("too large"), "{err}");
        assert!(!err.to_string().contains("unknown"), "{err}");
    }

    #[test]
    fn stdin_relay_exits_without_reading_once_session_is_gone() {
        let (in_r, in_w) = pipe_cloexec(false).unwrap();
        let (out_r, out_w) = pipe_cloexec(false).unwrap();
        let (death_r, death_w) = pipe_cloexec(false).unwrap();
        // Input is pending AND the session is gone: nothing may be consumed.
        assert_eq!(
            unsafe { libc::write(in_w.as_raw_fd(), b"typed".as_ptr().cast(), 5) },
            5
        );
        drop(death_w);
        let code = run_stdin_relay_fds(in_r.as_raw_fd(), out_w.as_raw_fd(), death_r.as_raw_fd());
        assert_eq!(code, 0);
        let mut b = [0u8; 8];
        assert_eq!(
            unsafe { libc::read(in_r.as_raw_fd(), b.as_mut_ptr().cast(), 8) },
            5
        );
        assert_eq!(&b[..5], b"typed");
        drop(out_w);
        assert_eq!(
            unsafe { libc::read(out_r.as_raw_fd(), b.as_mut_ptr().cast(), 8) },
            0
        );

        // While the session is alive it relays, and exits on stdin EOF.
        let (in_r, in_w) = pipe_cloexec(false).unwrap();
        let (out_r, out_w) = pipe_cloexec(false).unwrap();
        let (death_r, _death_w) = pipe_cloexec(false).unwrap();
        assert_eq!(
            unsafe { libc::write(in_w.as_raw_fd(), b"hello".as_ptr().cast(), 5) },
            5
        );
        drop(in_w);
        let code = run_stdin_relay_fds(in_r.as_raw_fd(), out_w.as_raw_fd(), death_r.as_raw_fd());
        assert_eq!(code, 0);
        drop(out_w);
        let mut f = std::fs::File::from(out_r);
        let mut got = Vec::new();
        io::Read::read_to_end(&mut f, &mut got).unwrap();
        assert_eq!(got, b"hello");
    }

    #[test]
    fn mid_session_stdout_epipe_still_delivers_received_stderr() {
        let _lock = SIGNAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let (stdin_r, stdin_w) = pipe_cloexec(false).unwrap();
        let (out_r, out_w) = pipe_cloexec(false).unwrap();
        let (err_r, err_w) = pipe_cloexec(false).unwrap();
        drop(out_r); // `| head` already gone: the first stdout write fails.

        let (host, mut guest) = UnixStream::pair().unwrap();
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

        // More stderr than the pipe holds is received BEFORE stdout breaks.
        let chunk = 16usize << 10;
        let chunks = 8usize;
        for i in 0..chunks {
            vm_exec::write(&mut guest, &VmExecMsg::Stderr(vec![b'a' + i as u8; chunk])).unwrap();
        }
        vm_exec::write(&mut guest, &VmExecMsg::Stdout(b"line\n".to_vec())).unwrap();

        let reader = thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            let mut f = std::fs::File::from(err_r);
            let mut got = Vec::new();
            let mut buf = vec![0u8; 4 << 10];
            loop {
                thread::sleep(Duration::from_millis(5));
                match io::Read::read(&mut f, &mut buf) {
                    Ok(0) => return got,
                    Ok(n) => got.extend_from_slice(&buf[..n]),
                    Err(e) => panic!("{e}"),
                }
            }
        });
        let exit = session.join().unwrap().unwrap();
        assert_eq!(exit, SessionExit::Code(128 + libc::SIGPIPE));
        drop(out_w);
        drop(err_w);
        drop(guest);
        let got = reader.join().unwrap();
        assert_eq!(got.len(), chunk * chunks, "stderr lost after stdout EPIPE");
        drop(stdin_w);
        drop(stdin_r);
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
