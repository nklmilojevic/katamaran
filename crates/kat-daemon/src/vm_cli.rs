//! `kat vm shell` and `kat vm exec`: root shell and command execution inside
//! the Linux VM's own namespaces (not an LXC machine), relayed over
//! `<home>/run/vm-exec.sock` (see [`kat_proto::vm_exec`]).

use std::io::{self, IsTerminal, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use kat_proto::vm_exec::{self, CHUNK, ExecSpec, Tty, VmExecMsg};

use crate::api::{Call, DockerState};
use crate::client::Client;
use crate::state::VmState;

pub const SOCKET_NAME: &str = "vm-exec.sock";
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
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

fn run_spec(
    workdir: Option<String>,
    extra_env: &[String],
    argv: Vec<String>,
    allocate_pty: bool,
) -> anyhow::Result<()> {
    let spec = build_spec(workdir, extra_env, argv, allocate_pty)?;
    let conn = connect_and_start(&spec)?;
    let code = run_session(conn, allocate_pty)?;
    std::process::exit(code);
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
            bail!("invalid --env {item:?}: variable name must be non-empty and valid");
        }
        if let Some(val) = value {
            if !vm_exec::value_ok(&val) {
                bail!("invalid --env {item:?}: value cannot contain a NUL byte");
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
struct RawMode(libc::termios);

impl RawMode {
    fn enter() -> io::Result<RawMode> {
        let mut t: libc::termios = unsafe { std::mem::zeroed() };
        if unsafe { libc::tcgetattr(0, &mut t) } < 0 {
            return Err(io::Error::last_os_error());
        }
        let saved = RawMode(t);
        unsafe {
            libc::cfmakeraw(&mut t);
            libc::tcsetattr(0, libc::TCSANOW, &t);
        }
        Ok(saved)
    }
}

impl Drop for RawMode {
    fn drop(&mut self) {
        unsafe { libc::tcsetattr(0, libc::TCSANOW, &self.0) };
    }
}

fn window_size() -> (u16, u16) {
    for fd in [1, 0] {
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

/// Start the daemon and VM if needed and wait until the VM is up (whether
/// Docker succeeded or failed, so debugging a broken Docker stack still works).
fn ensure_vm_running() -> anyhow::Result<()> {
    let mut c = crate::cli::ensure_daemon()?;
    let s = c.status()?;
    if s.state == VmState::Running && s.docker.state != DockerState::Starting {
        return Ok(());
    }
    eprintln!("katamaran: starting the VM");
    c.call(&Call::Start)?;
    let deadline = Instant::now() + Duration::from_secs(180);
    loop {
        let s = c.status()?;
        match (s.state, s.docker.state) {
            (VmState::Running, DockerState::Ready | DockerState::Failed) => return Ok(()),
            (VmState::Error | VmState::Stopped, _) => {
                bail!("the VM did not start: {}", s.error.unwrap_or_default())
            }
            _ => {}
        }
        if Instant::now() > deadline {
            bail!("the VM was not up after 180 s");
        }
        thread::sleep(Duration::from_millis(50));
    }
}

enum Handshake {
    Started(UnixStream),
    NoSocket(io::Error),
    ClosedBeforeStart,
    Failed(anyhow::Error),
}

fn try_handshake(path: &Path, spec: &ExecSpec) -> Handshake {
    let mut conn = match UnixStream::connect(path) {
        Ok(s) => s,
        Err(e) => return Handshake::NoSocket(e),
    };
    if let Err(e) = vm_exec::write(&mut conn, &VmExecMsg::Exec(spec.clone())) {
        if matches!(
            e.kind(),
            io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset
        ) {
            return Handshake::ClosedBeforeStart;
        }
        return Handshake::Failed(e.into());
    }
    match vm_exec::read(&mut conn) {
        Ok(Some(VmExecMsg::Started)) => Handshake::Started(conn),
        Ok(Some(VmExecMsg::Error(e))) => Handshake::Failed(anyhow::anyhow!("{e}")),
        Ok(Some(other)) => {
            Handshake::Failed(anyhow::anyhow!("unexpected guest response {other:?}"))
        }
        Ok(None) => Handshake::ClosedBeforeStart,
        Err(e)
            if matches!(
                e.kind(),
                io::ErrorKind::ConnectionReset
                    | io::ErrorKind::BrokenPipe
                    | io::ErrorKind::UnexpectedEof
            ) =>
        {
            Handshake::ClosedBeforeStart
        }
        Err(e) => Handshake::Failed(e.into()),
    }
}

/// Connect to the guest's VM exec service (starting the VM if it is stopped),
/// send `spec`, and wait for `Started`.
fn connect_and_start(spec: &ExecSpec) -> anyhow::Result<UnixStream> {
    let home = crate::state_dir()?;
    let path = socket_path(&home);

    match try_handshake(&path, spec) {
        Handshake::Started(conn) => return Ok(conn),
        Handshake::Failed(e) => return Err(e),
        Handshake::ClosedBeforeStart => {
            // The VM process bound the Unix socket, but kat-init may still be
            // finishing boot before calling vsock_listen(VM_EXEC_PORT).
            return retry_handshake(&path, spec);
        }
        Handshake::NoSocket(_) => {}
    }

    // If the daemon already has a running VM without vm-exec.sock, the running
    // VM was started by an older katamaran version and needs a restart.
    if let Ok(mut c) = Client::connect(&crate::client::socket_path()?)
        && let Ok(s) = c.status()
        && s.state == VmState::Running
    {
        bail!(
            "VM shell service is not available on the running VM ({}); run `kat restart` to upgrade",
            path.display()
        );
    }

    ensure_vm_running()?;
    retry_handshake(&path, spec)
}

fn retry_handshake(path: &Path, spec: &ExecSpec) -> anyhow::Result<UnixStream> {
    let deadline = Instant::now() + HANDSHAKE_TIMEOUT;
    loop {
        match try_handshake(path, spec) {
            Handshake::Started(conn) => return Ok(conn),
            Handshake::Failed(e) => return Err(e),
            Handshake::NoSocket(e) => {
                if Instant::now() >= deadline {
                    return Err(e).with_context(|| {
                        format!(
                            "cannot connect to the VM shell service ({}); if you just upgraded katamaran, run `kat restart`",
                            path.display()
                        )
                    });
                }
            }
            Handshake::ClosedBeforeStart => {
                if Instant::now() >= deadline {
                    bail!(
                        "the guest closed the VM shell connection before starting; check `kat logs --console` or run `kat restart`"
                    );
                }
            }
        }
        thread::sleep(HANDSHAKE_RETRY);
    }
}

/// Relay stdio for an already-`Started` VM exec session; returns the exit code.
fn run_session(conn: UnixStream, allocate_pty: bool) -> anyhow::Result<i32> {
    let stdin_is_tty = io::stdin().is_terminal();
    let stdout_is_tty = io::stdout().is_terminal();
    let _raw = if allocate_pty && stdin_is_tty {
        RawMode::enter().ok()
    } else {
        None
    };

    let w = Arc::new(Mutex::new(conn.try_clone()?));
    {
        let w = w.clone();
        thread::spawn(move || {
            let mut stdin = io::stdin().lock();
            let mut buf = vec![0u8; CHUNK];
            loop {
                let n = match stdin.read(&mut buf) {
                    Ok(0) | Err(_) => 0,
                    Ok(n) => n,
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
        });
    }
    if allocate_pty && (stdin_is_tty || stdout_is_tty) {
        let w = w.clone();
        thread::spawn(move || {
            let mut last = window_size();
            loop {
                thread::sleep(Duration::from_millis(200));
                let now = window_size();
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
            }
        });
    }

    let mut out = io::stdout();
    let mut err = io::stderr();
    relay_output(conn, &mut out, &mut err)
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
    use super::*;
    use clap::Parser;

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
    fn parses_env_flags_and_overrides() {
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
        assert!(parse_env(&["=bad".into()], |_| None).is_err());
        assert!(parse_env(&["".into()], |_| None).is_err());
        assert!(parse_env(&["A=b\0c".into()], |_| None).is_err());
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
    fn handshake_handles_started_error_and_early_close() {
        let dir = std::env::temp_dir().join(format!("kat-vm-cli-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let sock = dir.join("vm-exec.sock");
        let listener = std::os::unix::net::UnixListener::bind(&sock).unwrap();

        let server = thread::spawn(move || {
            // 1. Early close before Started (e.g. guest still booting).
            let (conn, _) = listener.accept().unwrap();
            drop(conn);
            // 2. Guest error before Started.
            let (mut conn, _) = listener.accept().unwrap();
            assert!(matches!(
                vm_exec::read(&mut conn).unwrap(),
                Some(VmExecMsg::Exec(_))
            ));
            vm_exec::write(&mut conn, &VmExecMsg::Error("no such binary".into())).unwrap();
            // 3. Successful Started handshake.
            let (mut conn, _) = listener.accept().unwrap();
            assert!(matches!(
                vm_exec::read(&mut conn).unwrap(),
                Some(VmExecMsg::Exec(_))
            ));
            vm_exec::write(&mut conn, &VmExecMsg::Started).unwrap();
        });

        let spec = ExecSpec {
            argv: vec!["true".into()],
            ..Default::default()
        };
        assert!(matches!(
            try_handshake(&sock, &spec),
            Handshake::ClosedBeforeStart
        ));
        match try_handshake(&sock, &spec) {
            Handshake::Failed(e) => assert!(e.to_string().contains("no such binary"), "{e}"),
            _ => panic!("expected guest error"),
        }
        assert!(matches!(try_handshake(&sock, &spec), Handshake::Started(_)));

        server.join().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
