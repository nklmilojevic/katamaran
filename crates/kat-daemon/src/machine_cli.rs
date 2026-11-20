//! `katamaran machine …`, `katamaran shell` and `katamaran ssh`.

use std::io::{self, IsTerminal, Read, Write};
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use kat_proto::machine::{self, CHUNK, ExecSpec, MachineMsg, Tty};

use crate::api::{Call, DockerState, MachineCreate};
use crate::machines::Machine;
use crate::state::VmState;

#[derive(clap::Subcommand)]
pub enum MachineCmd {
    /// Create a machine and start it.
    Create {
        name: String,
        /// ubuntu, debian, alpine, fedora, arch, or any distro on images.linuxcontainers.org.
        #[arg(long, short, default_value = "ubuntu")]
        distro: String,
        /// The distro release, such as noble or 3.24 (default: a current one).
        #[arg(long, short = 'r', alias = "release")]
        version: Option<String>,
    },
    /// List machines.
    #[command(alias = "ls")]
    List {
        #[arg(long)]
        json: bool,
    },
    /// Start a machine (default: the default machine).
    Start { name: Option<String> },
    /// Stop a machine; it no longer starts with the VM.
    Stop { name: Option<String> },
    /// Delete a machine and its files.
    #[command(alias = "rm")]
    Delete { name: String },
    /// Show a machine.
    Info {
        name: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Print the default machine, or make NAME the default.
    Default { name: Option<String> },
}

#[derive(clap::Args)]
pub struct ShellArgs {
    /// The machine (default: the default machine).
    pub machine: Option<String>,
    /// Log in as this user (default: your Mac user).
    #[arg(short, long)]
    pub user: Option<String>,
    /// Run this command instead of a login shell.
    #[arg(last = true)]
    pub cmd: Vec<String>,
}

#[derive(clap::Args)]
pub struct SshArgs {
    /// `machine` or `user@machine` (default: the default machine).
    pub dest: Option<String>,
    /// Command to run instead of a shell.
    #[arg(last = true)]
    pub cmd: Vec<String>,
}

/// A daemon with the VM up (started if needed).
fn running_client() -> anyhow::Result<crate::client::Client> {
    let mut c = crate::cli::ensure_daemon()?;
    let s = c.status()?;
    if s.state == VmState::Running && s.docker.state != DockerState::Starting {
        return Ok(c);
    }
    eprintln!("katamaran: starting the VM");
    c.call(&Call::Start)?;
    let deadline = Instant::now() + Duration::from_secs(180);
    loop {
        let s = c.status()?;
        match (s.state, s.docker.state) {
            (VmState::Running, DockerState::Ready | DockerState::Failed) => return Ok(c),
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

fn opt(name: Option<String>) -> String {
    name.unwrap_or_default()
}

pub fn machine(cmd: MachineCmd) -> anyhow::Result<()> {
    let mut c = running_client()?;
    match cmd {
        MachineCmd::Create {
            name,
            distro,
            version,
        } => {
            let t = Instant::now();
            let v = c.call_notified(
                &Call::MachineCreate(MachineCreate {
                    name,
                    distro,
                    release: version,
                }),
                &mut |method, params| {
                    if method == "machine.progress"
                        && let Some(m) = params.get("message").and_then(|m| m.as_str())
                    {
                        eprintln!("katamaran: {m}");
                    }
                },
            )?;
            let m: Machine = serde_json::from_value(v)?;
            println!(
                "{} ({} {}) is {} in {:.1} s; `katamaran shell {}` logs in",
                m.name,
                m.distro,
                m.release,
                m.state,
                t.elapsed().as_secs_f64(),
                m.name
            );
        }
        MachineCmd::List { json } => {
            let list: Vec<Machine> = serde_json::from_value(c.call(&Call::MachineList)?)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&list)?);
            } else {
                print!("{}", table(&list));
            }
        }
        MachineCmd::Start { name } => {
            c.call(&Call::MachineStart { name: opt(name) })?;
        }
        MachineCmd::Stop { name } => {
            c.call(&Call::MachineStop { name: opt(name) })?;
        }
        MachineCmd::Delete { name } => {
            c.call(&Call::MachineDelete { name: name.clone() })?;
            println!("deleted {name}");
        }
        MachineCmd::Info { name, json } => {
            let m: Machine =
                serde_json::from_value(c.call(&Call::MachineInfo { name: opt(name) })?)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&m)?);
            } else {
                print!("{}", describe(&m));
            }
        }
        MachineCmd::Default { name: None } => {
            let m: Machine = serde_json::from_value(c.call(&Call::MachineInfo {
                name: String::new(),
            })?)?;
            println!("{}", m.name);
        }
        MachineCmd::Default { name: Some(n) } => {
            c.call(&Call::MachineDefault { name: n })?;
        }
    }
    Ok(())
}

pub fn table(list: &[Machine]) -> String {
    if list.is_empty() {
        return "no machines; create one with `katamaran machine create <name>`\n".into();
    }
    let mut out = format!(
        "{:<20} {:<10} {:<10} {:<9} {}\n",
        "NAME", "DISTRO", "VERSION", "STATE", "IP"
    );
    for m in list {
        let name = if m.default {
            format!("{} *", m.name)
        } else {
            m.name.clone()
        };
        out.push_str(&format!(
            "{:<20} {:<10} {:<10} {:<9} {}\n",
            name,
            m.distro,
            m.release,
            m.state,
            m.ip.as_deref().unwrap_or("")
        ));
    }
    out
}

pub fn describe(m: &Machine) -> String {
    let mut out = format!("{}{}\n", m.name, if m.default { " (default)" } else { "" });
    let mut line = |k: &str, v: String| out.push_str(&format!("  {k:<9} {v}\n"));
    line(
        "distro",
        format!("{} {} ({})", m.distro, m.release, m.image),
    );
    line("state", m.state.clone());
    if let Some(ip) = &m.ip {
        line("ip", ip.clone());
    }
    line("user", format!("{} (uid {})", m.user, m.uid));
    line("autostart", if m.autostart { "yes" } else { "no" }.into());
    if let Some(p) = &m.path {
        line("files", p.display().to_string());
    }
    out
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
    let mut ws: libc::winsize = unsafe { std::mem::zeroed() };
    if unsafe { libc::ioctl(1, libc::TIOCGWINSZ, &mut ws) } < 0 || ws.ws_row == 0 {
        return (24, 80);
    }
    (ws.ws_row, ws.ws_col)
}

/// The guest's machine service, starting the VM first if needed.
fn machine_conn() -> anyhow::Result<UnixStream> {
    let home = crate::state_dir()?;
    if let Ok(s) = crate::machines::connect(&home) {
        return Ok(s);
    }
    running_client()?;
    crate::machines::connect(&home)
}

pub fn shell(args: ShellArgs) -> anyhow::Result<()> {
    let tty = io::stdin().is_terminal() && io::stdout().is_terminal();
    let (rows, cols) = window_size();
    let spec = ExecSpec {
        machine: opt(args.machine),
        user: opt(args.user),
        cwd: std::env::current_dir()
            .map(|d| d.to_string_lossy().into_owned())
            .unwrap_or_default(),
        env: ["LANG", "LC_ALL", "COLORTERM"]
            .iter()
            .filter_map(|k| Some((k.to_string(), std::env::var(k).ok()?)))
            .collect(),
        argv: args.cmd,
        tty: tty.then(|| Tty {
            term: std::env::var("TERM").unwrap_or_else(|_| "xterm-256color".into()),
            rows,
            cols,
        }),
    };
    let code = exec(machine_conn()?, spec, tty)?;
    std::process::exit(code);
}

/// Run `spec` with this process's stdio; returns the exit code.
fn exec(conn: UnixStream, spec: ExecSpec, tty: bool) -> anyhow::Result<i32> {
    let w = Arc::new(Mutex::new(conn.try_clone()?));
    machine::write(&mut *w.lock().unwrap(), &MachineMsg::Exec(spec))?;
    let _raw = if tty { RawMode::enter().ok() } else { None };
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
                    MachineMsg::DataEnd
                } else {
                    MachineMsg::Data(buf[..n].to_vec())
                };
                if machine::write(&mut *w.lock().unwrap(), &msg).is_err() || n == 0 {
                    return;
                }
            }
        });
    }
    if tty {
        let w = w.clone();
        thread::spawn(move || {
            let mut last = window_size();
            loop {
                thread::sleep(Duration::from_millis(200));
                let now = window_size();
                if now != last {
                    last = now;
                    let m = MachineMsg::Resize {
                        rows: now.0,
                        cols: now.1,
                    };
                    if machine::write(&mut *w.lock().unwrap(), &m).is_err() {
                        return;
                    }
                }
            }
        });
    }
    let mut r = conn;
    let (mut out, mut err) = (io::stdout(), io::stderr());
    loop {
        match machine::read(&mut r)? {
            Some(MachineMsg::Stdout(d)) => {
                out.write_all(&d)?;
                out.flush()?;
            }
            Some(MachineMsg::Stderr(d)) => {
                err.write_all(&d)?;
            }
            Some(MachineMsg::Exit { code }) => return Ok(code),
            Some(MachineMsg::Error(e)) => {
                drop(_raw);
                bail!("{e}")
            }
            Some(_) => {}
            None => bail!("the guest closed the connection"),
        }
    }
}

/// `katamaran ssh [user@]machine [-- cmd]`: the system ssh through the
/// katamaran SSH config.
pub fn ssh(args: SshArgs) -> anyhow::Result<()> {
    let files = crate::ssh::ensure_files()?;
    let dest = match args.dest.as_deref() {
        None | Some("") => files.host.clone(),
        Some(d) => format!("{d}@{}", files.host),
    };
    let err = std::process::Command::new("/usr/bin/ssh")
        .arg("-F")
        .arg(&files.config)
        .arg(dest)
        .args(&args.cmd)
        .exec();
    Err(err).context("run /usr/bin/ssh")
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
        Machine(MachineCmd),
        Shell(ShellArgs),
        Ssh(SshArgs),
    }

    fn parse(args: &[&str]) -> Top {
        Cli::try_parse_from(std::iter::once("katamaran").chain(args.iter().copied()))
            .unwrap()
            .cmd
    }

    #[test]
    fn parses_machine_commands() {
        let Top::Machine(MachineCmd::Create {
            name,
            distro,
            version,
        }) = parse(&[
            "machine",
            "create",
            "dev",
            "--distro",
            "alpine",
            "--version",
            "3.24",
        ])
        else {
            panic!()
        };
        assert_eq!((name.as_str(), distro.as_str()), ("dev", "alpine"));
        assert_eq!(version.as_deref(), Some("3.24"));
        let Top::Machine(MachineCmd::Create {
            distro, version, ..
        }) = parse(&["machine", "create", "u"])
        else {
            panic!()
        };
        assert_eq!((distro.as_str(), version), ("ubuntu", None));
        assert!(matches!(
            parse(&["machine", "ls", "--json"]),
            Top::Machine(MachineCmd::List { json: true })
        ));
        assert!(matches!(
            parse(&["machine", "rm", "dev"]),
            Top::Machine(MachineCmd::Delete { .. })
        ));
        assert!(matches!(
            parse(&["machine", "stop"]),
            Top::Machine(MachineCmd::Stop { name: None })
        ));
        assert!(
            Cli::try_parse_from(["katamaran", "machine", "delete"]).is_err(),
            "delete needs a name"
        );
    }

    #[test]
    fn parses_shell_and_ssh() {
        let Top::Shell(s) = parse(&["shell"]) else {
            panic!()
        };
        assert!(s.machine.is_none() && s.cmd.is_empty() && s.user.is_none());
        let Top::Shell(s) = parse(&["shell", "dev", "-u", "root", "--", "id", "-u"]) else {
            panic!()
        };
        assert_eq!(s.machine.as_deref(), Some("dev"));
        assert_eq!(s.user.as_deref(), Some("root"));
        assert_eq!(s.cmd, ["id", "-u"]);
        let Top::Ssh(s) = parse(&["ssh", "root@dev", "--", "uname"]) else {
            panic!()
        };
        assert_eq!(s.dest.as_deref(), Some("root@dev"));
        assert_eq!(s.cmd, ["uname"]);
    }

    #[test]
    fn prints_tables() {
        let m = Machine {
            name: "dev".into(),
            distro: "alpine".into(),
            release: "3.24".into(),
            image: "20261117_13:01".into(),
            state: "running".into(),
            ip: Some("198.19.249.2".into()),
            user: "me".into(),
            uid: 501,
            default: true,
            autostart: true,
            created_ms: 0,
            path: Some("/Users/me/Katamaran/machines/dev".into()),
        };
        let t = table(std::slice::from_ref(&m));
        assert!(t.lines().nth(1).unwrap().starts_with("dev *"), "{t}");
        assert!(t.contains("198.19.249.2"));
        assert!(table(&[]).contains("no machines"));
        let d = describe(&m);
        assert!(d.starts_with("dev (default)\n"), "{d}");
        assert!(d.contains("me (uid 501)"));
        assert!(d.contains("/Users/me/Katamaran/machines/dev"));
    }
}
