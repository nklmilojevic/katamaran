//! SSH into machines: `ssh <machine>@katamaran`, `ssh <user>@<machine>@katamaran`,
//! or `ssh katamaran` for the default machine.
//!
//! `<home>/ssh/config` names a ProxyCommand, `katamaran ssh-proxy`, which
//! is the SSH server itself: it speaks SSH on its stdin and stdout and maps
//! each session channel to an `Exec` (shell, command or sftp-server) on the
//! guest's machine service, and `direct-tcpip` (`ssh -L`) to a `Connect`.
//! The transport is a process the user runs, so no key is checked; the
//! host key in `<home>/ssh` is pinned by the config's known_hosts file.

use std::collections::HashMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};

use anyhow::{Context, bail};
use kat_proto::machine::{self, ExecSpec, MachineMsg, Tty};
use russh::keys::PrivateKey;
use russh::server::{Auth, ChannelOpenHandle, Msg, Session};
use russh::{Channel, ChannelId, MethodKind, MethodSet, Sig};

pub struct SshFiles {
    pub config: PathBuf,
    /// The Host name in the config: `katamaran`, or `katamaran-<suffix>`.
    pub host: String,
}

/// The ssh directory under the home: host key, known_hosts and the config
/// to Include from `~/.ssh/config`. Created on first use.
pub fn ensure_files() -> anyhow::Result<SshFiles> {
    let home = crate::state_dir()?;
    let host = crate::setup::Identity::resolve(None)?.context();
    let dir = home.join("ssh");
    fs::create_dir_all(&dir)?;
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o700))?;
    let key = dir.join("host_ed25519");
    if !key.exists() {
        let out = Command::new("/usr/bin/ssh-keygen")
            .args([
                "-q",
                "-t",
                "ed25519",
                "-N",
                "",
                "-C",
                "katamaran host key",
                "-f",
            ])
            .arg(&key)
            .output()
            .context("run ssh-keygen")?;
        if !out.status.success() {
            bail!(
                "ssh-keygen: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
    }
    let public = fs::read_to_string(key.with_extension("pub"))
        .with_context(|| format!("read {}.pub", key.display()))?;
    let known_hosts = dir.join("known_hosts");
    fs::write(&known_hosts, known_hosts_line(&host, &public))?;
    let exe = std::env::current_exe()?.canonicalize()?;
    let home_env = std::env::var_os("KATAMARAN_HOME").map(|_| home.as_path());
    let config = dir.join("config");
    fs::write(&config, config_text(&host, &exe, home_env, &known_hosts))?;
    Ok(SshFiles { config, host })
}

pub fn known_hosts_line(host: &str, public: &str) -> String {
    let mut f = public.split_whitespace();
    let (alg, key) = (f.next().unwrap_or(""), f.next().unwrap_or(""));
    format!("{host} {alg} {key}\n")
}

fn sh_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', r"'\''"))
}

pub fn config_text(host: &str, exe: &Path, home: Option<&Path>, known_hosts: &Path) -> String {
    let mut proxy = String::new();
    if let Some(h) = home {
        proxy.push_str(&format!(
            "/usr/bin/env KATAMARAN_HOME={} ",
            sh_quote(&h.to_string_lossy())
        ));
    }
    proxy.push_str(&format!("{} ssh-proxy", sh_quote(&exe.to_string_lossy())));
    format!(
        "# Written by katamaran; Include it from ~/.ssh/config.
# ssh {host}                     the default machine, as your Mac user
# ssh <machine>@{host}           a machine, as your Mac user
# ssh <user>@<machine>@{host}    a machine as <user> (root, ...)
Host {host}
  User default
  ProxyCommand {proxy}
  UserKnownHostsFile \"{kh}\"
  StrictHostKeyChecking yes
  IdentitiesOnly yes
  ServerAliveInterval 0
",
        kh = known_hosts.display()
    )
}

/// `default` (or nothing) → the default machine as the default user,
/// `m` → machine `m`, `u@m` → machine `m` as `u`. Returns (machine, user).
pub fn parse_login(login: &str) -> (String, String) {
    match login.rsplit_once('@') {
        Some((user, m)) => (m.to_string(), user.to_string()),
        None if login == "default" || login.is_empty() => (String::new(), String::new()),
        None => (login.to_string(), String::new()),
    }
}

fn signal_number(s: &Sig) -> Option<i32> {
    Some(match s {
        Sig::ABRT => libc::SIGABRT,
        Sig::ALRM => libc::SIGALRM,
        Sig::FPE => libc::SIGFPE,
        Sig::HUP => libc::SIGHUP,
        Sig::ILL => libc::SIGILL,
        Sig::INT => libc::SIGINT,
        Sig::KILL => libc::SIGKILL,
        Sig::PIPE => libc::SIGPIPE,
        Sig::QUIT => libc::SIGQUIT,
        Sig::SEGV => libc::SIGSEGV,
        Sig::TERM => libc::SIGTERM,
        // Linux numbering for the guest.
        Sig::USR1 => 10,
        Sig::Custom(_) => return None,
    })
}

/// Finds an sftp-server wherever the distro put it.
const SFTP: &str = "for p in /usr/lib/openssh/sftp-server /usr/libexec/openssh/sftp-server \
    /usr/lib/ssh/sftp-server /usr/libexec/sftp-server /usr/lib/sftp-server; do \
    [ -x \"$p\" ] && exec \"$p\"; done; \
    echo 'no sftp-server in this machine; install openssh-sftp-server' >&2; exit 127";

#[derive(Default)]
struct Chan {
    env: Vec<(String, String)>,
    tty: Option<Tty>,
    /// The guest connection once the channel runs something.
    guest: Option<Arc<Mutex<UnixStream>>>,
}

struct Handler {
    home: PathBuf,
    machine: String,
    user: String,
    rt: tokio::runtime::Handle,
    chans: HashMap<ChannelId, Chan>,
}

impl Handler {
    fn send(&mut self, ch: ChannelId, m: &MachineMsg) {
        if let Some(g) = self.chans.get(&ch).and_then(|c| c.guest.clone()) {
            let _ = machine::write(&mut *g.lock().unwrap(), m);
        }
    }

    /// Start `argv` (empty: login shell) for a session channel.
    fn run(&mut self, ch: ChannelId, argv: Vec<String>, session: &mut Session) {
        let c = self.chans.entry(ch).or_default();
        let spec = ExecSpec {
            machine: self.machine.clone(),
            user: self.user.clone(),
            cwd: String::new(),
            env: c.env.clone(),
            argv,
            tty: c.tty.clone(),
        };
        let started = crate::machines::connect(&self.home).and_then(|mut s| {
            machine::write(&mut s, &MachineMsg::Exec(spec))?;
            Ok(s)
        });
        let handle = session.handle();
        match started {
            Ok(s) => {
                let reader = s.try_clone();
                c.guest = Some(Arc::new(Mutex::new(s)));
                let _ = session.channel_success(ch);
                if let Ok(r) = reader {
                    let rt = self.rt.clone();
                    std::thread::spawn(move || pump(rt, handle, ch, r));
                }
            }
            Err(e) => {
                let _ = session.extended_data(ch, 1, format!("katamaran: {e:#}\r\n").into_bytes());
                let _ = session.channel_failure(ch);
                let _ = session.close(ch);
            }
        }
    }
}

/// Guest frames to the SSH channel, until the process exits or the
/// connection ends.
fn pump(rt: tokio::runtime::Handle, h: russh::server::Handle, ch: ChannelId, mut r: UnixStream) {
    loop {
        match machine::read(&mut r) {
            Ok(Some(MachineMsg::Stdout(d) | MachineMsg::Data(d))) => {
                if rt.block_on(h.data(ch, d)).is_err() {
                    return;
                }
            }
            Ok(Some(MachineMsg::Stderr(d))) => {
                let _ = rt.block_on(h.extended_data(ch, 1, d));
            }
            Ok(Some(MachineMsg::Done | MachineMsg::Progress(_))) => {}
            Ok(Some(MachineMsg::Exit { code })) => {
                rt.block_on(async {
                    let _ = h.exit_status_request(ch, code as u32).await;
                    let _ = h.eof(ch).await;
                    let _ = h.close(ch).await;
                });
                return;
            }
            Ok(Some(MachineMsg::Error(e))) => {
                rt.block_on(async {
                    let _ = h
                        .extended_data(ch, 1, format!("katamaran: {e}\r\n").into_bytes())
                        .await;
                    let _ = h.exit_status_request(ch, 255).await;
                    let _ = h.close(ch).await;
                });
                return;
            }
            _ => {
                rt.block_on(async {
                    let _ = h.eof(ch).await;
                    let _ = h.close(ch).await;
                });
                return;
            }
        }
    }
}

impl russh::server::Handler for Handler {
    type Error = russh::Error;

    async fn auth_none(&mut self, user: &str) -> Result<Auth, Self::Error> {
        (self.machine, self.user) = parse_login(user);
        Ok(Auth::Accept)
    }

    async fn channel_open_session(
        &mut self,
        channel: Channel<Msg>,
        reply: ChannelOpenHandle,
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.chans.insert(channel.id(), Chan::default());
        reply.accept().await;
        Ok(())
    }

    async fn channel_open_direct_tcpip(
        &mut self,
        channel: Channel<Msg>,
        host_to_connect: &str,
        port_to_connect: u32,
        _originator_address: &str,
        _originator_port: u32,
        reply: ChannelOpenHandle,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        // Only the machine itself: localhost, or its own name.
        let local = ["localhost", "127.0.0.1", "::1", self.machine.as_str()];
        let Ok(port) = u16::try_from(port_to_connect) else {
            return Ok(());
        };
        if !local.contains(&host_to_connect) {
            return Ok(());
        }
        let conn = crate::machines::connect(&self.home).and_then(|mut s| {
            machine::write(
                &mut s,
                &MachineMsg::Connect {
                    name: self.machine.clone(),
                    port,
                },
            )?;
            match machine::read(&mut s)? {
                Some(MachineMsg::Done) => Ok(s),
                Some(MachineMsg::Error(e)) => bail!("{e}"),
                other => bail!("unexpected answer {other:?}"),
            }
        });
        let Ok(s) = conn else {
            return Ok(());
        };
        let ch = channel.id();
        let reader = s.try_clone()?;
        self.chans.insert(
            ch,
            Chan {
                guest: Some(Arc::new(Mutex::new(s))),
                ..Chan::default()
            },
        );
        reply.accept().await;
        let (rt, h) = (self.rt.clone(), session.handle());
        std::thread::spawn(move || pump(rt, h, ch, reader));
        Ok(())
    }

    async fn pty_request(
        &mut self,
        channel: ChannelId,
        term: &str,
        col_width: u32,
        row_height: u32,
        _pix_width: u32,
        _pix_height: u32,
        _modes: &[(russh::Pty, u32)],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.chans.entry(channel).or_default().tty = Some(Tty {
            term: term.to_string(),
            rows: row_height.min(u16::MAX as u32) as u16,
            cols: col_width.min(u16::MAX as u32) as u16,
        });
        session.channel_success(channel)?;
        Ok(())
    }

    async fn env_request(
        &mut self,
        channel: ChannelId,
        variable_name: &str,
        variable_value: &str,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.chans
            .entry(channel)
            .or_default()
            .env
            .push((variable_name.into(), variable_value.into()));
        session.channel_success(channel)?;
        Ok(())
    }

    async fn shell_request(
        &mut self,
        channel: ChannelId,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.run(channel, Vec::new(), session);
        Ok(())
    }

    async fn exec_request(
        &mut self,
        channel: ChannelId,
        data: &[u8],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        let cmd = String::from_utf8_lossy(data).into_owned();
        self.run(channel, vec!["/bin/sh".into(), "-c".into(), cmd], session);
        Ok(())
    }

    async fn subsystem_request(
        &mut self,
        channel: ChannelId,
        name: &str,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        if name == "sftp" {
            self.run(
                channel,
                vec!["/bin/sh".into(), "-c".into(), SFTP.into()],
                session,
            );
        } else {
            session.channel_failure(channel)?;
        }
        Ok(())
    }

    async fn window_change_request(
        &mut self,
        channel: ChannelId,
        col_width: u32,
        row_height: u32,
        _pix_width: u32,
        _pix_height: u32,
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.send(
            channel,
            &MachineMsg::Resize {
                rows: row_height.min(u16::MAX as u32) as u16,
                cols: col_width.min(u16::MAX as u32) as u16,
            },
        );
        Ok(())
    }

    async fn data(
        &mut self,
        channel: ChannelId,
        data: &[u8],
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.send(channel, &MachineMsg::Data(data.to_vec()));
        Ok(())
    }

    async fn channel_eof(
        &mut self,
        channel: ChannelId,
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.send(channel, &MachineMsg::DataEnd);
        Ok(())
    }

    async fn channel_close(
        &mut self,
        channel: ChannelId,
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        if let Some(c) = self.chans.remove(&channel)
            && let Some(g) = c.guest
        {
            let _ = g.lock().unwrap().shutdown(std::net::Shutdown::Both);
        }
        Ok(())
    }

    async fn signal(
        &mut self,
        channel: ChannelId,
        signal: Sig,
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        if let Some(signal) = signal_number(&signal) {
            self.send(channel, &MachineMsg::Signal { signal });
        }
        Ok(())
    }
}

/// `katamaran ssh-proxy`: serve one SSH connection on stdin/stdout.
pub fn proxy() -> anyhow::Result<()> {
    let home = crate::state_dir()?;
    let key_path = home.join("ssh/host_ed25519");
    let key = PrivateKey::from_openssh(
        fs::read_to_string(&key_path)
            .with_context(|| format!("read {} (run `katamaran setup`)", key_path.display()))?,
    )
    .context("parse the host key")?;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let config = Arc::new(russh::server::Config {
        methods: MethodSet::from(&[MethodKind::None][..]),
        auth_rejection_time: std::time::Duration::ZERO,
        auth_rejection_time_initial: Some(std::time::Duration::ZERO),
        inactivity_timeout: None,
        keys: vec![key],
        nodelay: true,
        ..Default::default()
    });
    let handler = Handler {
        home,
        machine: String::new(),
        user: String::new(),
        rt: rt.handle().clone(),
        chans: HashMap::new(),
    };
    rt.block_on(async move {
        let stream = tokio::io::join(tokio::io::stdin(), tokio::io::stdout());
        let session = russh::server::run_stream(config, stream, handler).await?;
        session.await
    })?;
    Ok(())
}

impl SshFiles {
    /// The line `katamaran setup` asks the user to add to ~/.ssh/config.
    pub fn include_line(&self) -> String {
        format!("Include \"{}\"", self.config.display())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_logins() {
        let p = |s| parse_login(s);
        assert_eq!(p("default"), ("".into(), "".into()));
        assert_eq!(p(""), ("".into(), "".into()));
        assert_eq!(p("dev"), ("dev".into(), "".into()));
        assert_eq!(p("root@dev"), ("dev".into(), "root".into()));
        assert_eq!(p("a@b@dev"), ("dev".into(), "a@b".into()));
    }

    #[test]
    fn writes_ssh_files() {
        let kh = known_hosts_line("katamaran", "ssh-ed25519 AAAAC3Nz katamaran host key\n");
        assert_eq!(kh, "katamaran ssh-ed25519 AAAAC3Nz\n");
        let c = config_text(
            "katamaran-p8",
            Path::new("/Applications/Kat a.app/katamaran"),
            Some(Path::new("/Users/me/.katamaran-p8")),
            Path::new("/Users/me/.katamaran-p8/ssh/known_hosts"),
        );
        assert!(c.contains("\nHost katamaran-p8\n"), "{c}");
        assert!(
            c.contains(
                "  ProxyCommand /usr/bin/env KATAMARAN_HOME='/Users/me/.katamaran-p8' '/Applications/Kat a.app/katamaran' ssh-proxy\n"
            ),
            "{c}"
        );
        assert!(c.contains("UserKnownHostsFile \"/Users/me/.katamaran-p8/ssh/known_hosts\""));
        let c = config_text(
            "katamaran",
            Path::new("/x/katamaran"),
            None,
            Path::new("/k"),
        );
        assert!(
            c.contains("  ProxyCommand '/x/katamaran' ssh-proxy\n"),
            "{c}"
        );
    }

    #[test]
    fn maps_signals() {
        assert_eq!(signal_number(&Sig::TERM), Some(libc::SIGTERM));
        assert_eq!(signal_number(&Sig::USR1), Some(10));
        assert_eq!(signal_number(&Sig::Custom("X".into())), None);
    }
}
