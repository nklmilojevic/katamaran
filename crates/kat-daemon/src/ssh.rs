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
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use anyhow::{Context, bail};
use kat_proto::machine::{ExecSpec, MachineMsg, Tty};
use russh::keys::PrivateKey;
use russh::server::{Auth, ChannelOpenHandle, Msg, Session};
use russh::{Channel, ChannelId, MethodKind, MethodSet, Sig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc;
use tokio::task::AbortHandle;

pub struct SshFiles {
    pub config: PathBuf,
    /// The Host name in the config: `katamaran`, or `katamaran-<suffix>`.
    pub host: String,
}

/// The ssh directory under the home: host key, known_hosts and the config
/// to Include from `~/.ssh/config`. Created on first use.
pub fn ensure_files() -> anyhow::Result<SshFiles> {
    let id = crate::setup::Identity::resolve(None)?;
    ensure_files_for(&id)
}

/// Like [`ensure_files`], using an already-resolved [`crate::setup::Identity`].
pub fn ensure_files_for(id: &crate::setup::Identity) -> anyhow::Result<SshFiles> {
    let home = crate::state_dir()?;
    let host = id.context();
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

async fn connect_guest(home: &Path) -> anyhow::Result<UnixStream> {
    let path = crate::machines::socket_path(home);
    UnixStream::connect(&path).await.with_context(|| {
        format!(
            "the VM is not running ({}); start it with `katamaran start`",
            path.display()
        )
    })
}

async fn write_frame<W: AsyncWriteExt + Unpin>(w: &mut W, msg: &MachineMsg) -> io::Result<()> {
    w.write_all(&msg.encode()).await?;
    w.flush().await
}

async fn read_frame<R: AsyncReadExt + Unpin>(r: &mut R) -> io::Result<Option<MachineMsg>> {
    let mut first = [0u8; 1];
    if r.read(&mut first).await? == 0 {
        return Ok(None);
    }
    let mut len = [0u8; 4];
    len[0] = first[0];
    r.read_exact(&mut len[1..]).await?;
    let len = u32::from_le_bytes(len) as usize;
    if len < 2 {
        return Err(kat_proto::ProtoError::Truncated.into());
    }
    if len > kat_proto::MAX_FRAME {
        return Err(kat_proto::ProtoError::FrameTooLarge(len).into());
    }
    let mut body = vec![0u8; len];
    r.read_exact(&mut body).await?;
    let k = u16::from_le_bytes([body[0], body[1]]);
    Ok(Some(MachineMsg::decode(k, &body[2..])?))
}

/// Validate a `direct-tcpip` (`ssh -L`) target: only localhost or the machine's
/// own non-empty name, and a non-zero TCP port.
fn valid_direct_tcpip_target(host: &str, port: u32, machine: &str) -> Option<u16> {
    let port = u16::try_from(port).ok().filter(|&p| p != 0)?;
    let allowed = matches!(host, "localhost" | "127.0.0.1" | "::1")
        || (!machine.is_empty() && host == machine);
    allowed.then_some(port)
}

/// Bounded per-channel host-to-guest frame queues so stalled guest reads
/// cannot exhaust host memory, while non-blocking dispatch prevents one
/// stalled channel from suspending the SSH handler for other channels.
const GUEST_WRITE_QUEUE: usize = 128;
const GUEST_CTRL_QUEUE: usize = 16;

struct GuestChan {
    data_tx: mpsc::Sender<MachineMsg>,
    ctrl_tx: mpsc::Sender<MachineMsg>,
    reader: AbortHandle,
    writer: AbortHandle,
}

impl GuestChan {
    fn spawn(stream: UnixStream, handle: russh::server::Handle, ch: ChannelId) -> Self {
        let (reader, writer) = stream.into_split();
        let (data_tx, data_rx) = mpsc::channel(GUEST_WRITE_QUEUE);
        let (ctrl_tx, ctrl_rx) = mpsc::channel(GUEST_CTRL_QUEUE);
        let writer_task = tokio::spawn(pump_write(ctrl_rx, data_rx, writer));
        let reader_task = tokio::spawn(pump_read(handle, ch, reader));
        GuestChan {
            data_tx,
            ctrl_tx,
            reader: reader_task.abort_handle(),
            writer: writer_task.abort_handle(),
        }
    }

    /// Try to enqueue a stream frame without indefinitely suspending the SSH
    /// handler. Yields briefly on `Full` so `pump_write` gets a chance to drain
    /// on the single-threaded runtime; returns `false` if the guest is stalled.
    async fn try_enqueue_data(&self, mut msg: MachineMsg) -> bool {
        for attempt in 0..5 {
            match self.data_tx.try_send(msg) {
                Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => return true,
                Err(mpsc::error::TrySendError::Full(next)) => {
                    msg = next;
                    if attempt < 4 {
                        tokio::task::yield_now().await;
                    }
                }
            }
        }
        false
    }

    fn try_enqueue_ctrl(&self, msg: MachineMsg) {
        let _ = self.ctrl_tx.try_send(msg);
    }

    fn close(self) {
        drop(self);
    }
}

impl Drop for GuestChan {
    fn drop(&mut self) {
        self.reader.abort();
        self.writer.abort();
    }
}

#[derive(Default)]
struct Chan {
    env: Vec<(String, String)>,
    tty: Option<Tty>,
    is_session: bool,
    /// The guest connection once the channel runs something.
    guest: Option<GuestChan>,
}

struct Handler {
    home: PathBuf,
    machine: String,
    user: String,
    chans: HashMap<ChannelId, Chan>,
}

impl Handler {
    /// Enqueue stream data (`Data` / `DataEnd`) without blocking other
    /// multiplexed SSH channels. If this channel's bounded queue stays full
    /// because its guest stopped reading, close only this stalled channel.
    async fn send_data(&mut self, ch: ChannelId, m: MachineMsg, session: &mut Session) {
        let ok = {
            let Some(g) = self.chans.get(&ch).and_then(|c| c.guest.as_ref()) else {
                return;
            };
            g.try_enqueue_data(m).await
        };
        if !ok && let Some(c) = self.chans.remove(&ch) {
            if let Some(g) = c.guest {
                g.close();
            }
            if c.is_session {
                let _ = session.extended_data(
                    ch,
                    1,
                    b"katamaran: guest stopped reading; closing channel\r\n".to_vec(),
                );
            }
            let _ = session.close(ch);
        }
    }

    fn send_ctrl(&self, ch: ChannelId, m: MachineMsg) {
        if let Some(g) = self.chans.get(&ch).and_then(|c| c.guest.as_ref()) {
            g.try_enqueue_ctrl(m);
        }
    }

    /// Start `argv` (empty: login shell) for a session channel.
    async fn run(&mut self, ch: ChannelId, argv: Vec<String>, session: &mut Session) {
        if self.chans.get(&ch).is_some_and(|c| c.guest.is_some()) {
            let _ = session.channel_failure(ch);
            return;
        }
        let (env, tty) = {
            let c = self.chans.entry(ch).or_default();
            c.is_session = true;
            (c.env.clone(), c.tty.clone())
        };
        let spec = ExecSpec {
            machine: self.machine.clone(),
            user: self.user.clone(),
            cwd: String::new(),
            env,
            argv,
            tty,
        };
        let started = async {
            let mut s = connect_guest(&self.home).await?;
            write_frame(&mut s, &MachineMsg::Exec(spec)).await?;
            Ok::<_, anyhow::Error>(s)
        }
        .await;
        let handle = session.handle();
        match started {
            Ok(s) => {
                self.chans.entry(ch).or_default().guest = Some(GuestChan::spawn(s, handle, ch));
                let _ = session.channel_success(ch);
            }
            Err(e) => {
                let _ = session.extended_data(ch, 1, format!("katamaran: {e:#}\r\n").into_bytes());
                let _ = session.channel_failure(ch);
                let _ = session.close(ch);
            }
        }
    }
}

/// Write queued host-to-guest frames asynchronously so SSH callbacks never
/// block the runtime or deadlock against guest output. Control frames
/// (`Signal`, `Resize`) take priority over queued stream data.
async fn pump_write(
    mut ctrl_rx: mpsc::Receiver<MachineMsg>,
    mut data_rx: mpsc::Receiver<MachineMsg>,
    mut w: OwnedWriteHalf,
) {
    loop {
        let msg = tokio::select! {
            biased;
            Some(m) = ctrl_rx.recv() => m,
            Some(m) = data_rx.recv() => m,
            else => break,
        };
        if write_frame(&mut w, &msg).await.is_err() {
            break;
        }
    }
    let _ = w.shutdown().await;
}

/// Guest frames to the SSH channel, until the process exits or the
/// connection ends.
async fn pump_read(h: russh::server::Handle, ch: ChannelId, mut r: OwnedReadHalf) {
    loop {
        match read_frame(&mut r).await {
            Ok(Some(MachineMsg::Stdout(d) | MachineMsg::Data(d))) => {
                if h.data(ch, d).await.is_err() {
                    return;
                }
            }
            Ok(Some(MachineMsg::Stderr(d))) => {
                let _ = h.extended_data(ch, 1, d).await;
            }
            Ok(Some(MachineMsg::Done | MachineMsg::Progress(_))) => {}
            Ok(Some(MachineMsg::Exit { code })) => {
                let _ = h.exit_status_request(ch, code as u32).await;
                let _ = h.eof(ch).await;
                let _ = h.close(ch).await;
                return;
            }
            Ok(Some(MachineMsg::Error(e))) => {
                let _ = h
                    .extended_data(ch, 1, format!("katamaran: {e}\r\n").into_bytes())
                    .await;
                let _ = h.exit_status_request(ch, 255).await;
                let _ = h.close(ch).await;
                return;
            }
            _ => {
                let _ = h.eof(ch).await;
                let _ = h.close(ch).await;
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
        let Some(port) = valid_direct_tcpip_target(host_to_connect, port_to_connect, &self.machine)
        else {
            return Ok(());
        };
        let conn = async {
            let mut s = connect_guest(&self.home).await?;
            write_frame(
                &mut s,
                &MachineMsg::Connect {
                    name: self.machine.clone(),
                    port,
                },
            )
            .await?;
            match read_frame(&mut s).await? {
                Some(MachineMsg::Done) => Ok(s),
                Some(MachineMsg::Error(e)) => bail!("{e}"),
                other => bail!("unexpected answer {other:?}"),
            }
        }
        .await;
        let Ok(s) = conn else {
            return Ok(());
        };
        let ch = channel.id();
        reply.accept().await;
        let h = session.handle();
        self.chans.insert(
            ch,
            Chan {
                guest: Some(GuestChan::spawn(s, h, ch)),
                ..Chan::default()
            },
        );
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
        self.run(channel, Vec::new(), session).await;
        Ok(())
    }

    async fn exec_request(
        &mut self,
        channel: ChannelId,
        data: &[u8],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        let cmd = String::from_utf8_lossy(data).into_owned();
        self.run(channel, vec!["/bin/sh".into(), "-c".into(), cmd], session)
            .await;
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
            )
            .await;
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
        self.send_ctrl(
            channel,
            MachineMsg::Resize {
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
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.send_data(channel, MachineMsg::Data(data.to_vec()), session)
            .await;
        Ok(())
    }

    async fn channel_eof(
        &mut self,
        channel: ChannelId,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.send_data(channel, MachineMsg::DataEnd, session).await;
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
            g.close();
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
            self.send_ctrl(channel, MachineMsg::Signal { signal });
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

    #[test]
    fn validates_direct_tcpip_targets() {
        assert_eq!(valid_direct_tcpip_target("localhost", 8080, ""), Some(8080));
        assert_eq!(valid_direct_tcpip_target("127.0.0.1", 80, ""), Some(80));
        assert_eq!(valid_direct_tcpip_target("::1", 443, "dev"), Some(443));
        assert_eq!(valid_direct_tcpip_target("dev", 5432, "dev"), Some(5432));

        // Empty host must never be accepted even when machine name is empty.
        assert_eq!(valid_direct_tcpip_target("", 8080, ""), None);
        assert_eq!(valid_direct_tcpip_target("", 8080, "dev"), None);
        // Port 0 and out-of-range ports must be rejected.
        assert_eq!(valid_direct_tcpip_target("localhost", 0, "dev"), None);
        assert_eq!(valid_direct_tcpip_target("dev", 0, "dev"), None);
        assert_eq!(valid_direct_tcpip_target("localhost", 65536, "dev"), None);
        // Unrelated hosts must be rejected.
        assert_eq!(valid_direct_tcpip_target("example.com", 80, "dev"), None);
        assert_eq!(valid_direct_tcpip_target("other", 80, "dev"), None);
    }

    #[test]
    fn async_frames_round_trip_over_unix_stream() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let (mut a, mut b) = UnixStream::pair().unwrap();
            let sent = MachineMsg::Data(b"hello async".to_vec());
            write_frame(&mut a, &sent).await.unwrap();
            a.shutdown().await.unwrap();
            assert_eq!(read_frame(&mut b).await.unwrap(), Some(sent));
            assert_eq!(read_frame(&mut b).await.unwrap(), None);
        });
    }

    #[test]
    fn stalled_guest_channel_does_not_block_multiplexed_channels_or_close() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            // Channel 1: guest never reads.
            let (host1, mut guest1) = UnixStream::pair().unwrap();
            let (r1, w1) = host1.into_split();
            let (data_tx1, data_rx1) = mpsc::channel(GUEST_WRITE_QUEUE);
            let (ctrl_tx1, ctrl_rx1) = mpsc::channel(GUEST_CTRL_QUEUE);
            let w_task1 = tokio::spawn(pump_write(ctrl_rx1, data_rx1, w1));
            let r_task1 = tokio::spawn(async move {
                let mut r = r1;
                let _ = read_frame(&mut r).await;
            });
            let stalled = GuestChan {
                data_tx: data_tx1,
                ctrl_tx: ctrl_tx1,
                reader: r_task1.abort_handle(),
                writer: w_task1.abort_handle(),
            };

            // Channel 2: healthy multiplexed guest reading concurrently.
            let (host2, mut guest2) = UnixStream::pair().unwrap();
            let (r2, w2) = host2.into_split();
            let (data_tx2, data_rx2) = mpsc::channel(GUEST_WRITE_QUEUE);
            let (ctrl_tx2, ctrl_rx2) = mpsc::channel(GUEST_CTRL_QUEUE);
            let w_task2 = tokio::spawn(pump_write(ctrl_rx2, data_rx2, w2));
            let r_task2 = tokio::spawn(async move {
                let mut r = r2;
                let _ = read_frame(&mut r).await;
            });
            let healthy = GuestChan {
                data_tx: data_tx2,
                ctrl_tx: ctrl_tx2,
                reader: r_task2.abort_handle(),
                writer: w_task2.abort_handle(),
            };

            // Pump frames into channel 1 until its bounded queue overflows.
            // `try_enqueue_data` must return `false` without hanging the runtime.
            let chunk = vec![0x42u8; 32 << 10];
            let mut overflowed = false;
            for _ in 0..(GUEST_WRITE_QUEUE + 256) {
                if !stalled
                    .try_enqueue_data(MachineMsg::Data(chunk.clone()))
                    .await
                {
                    overflowed = true;
                    break;
                }
            }
            assert!(overflowed, "bounded queue must detect stalled guest");

            // Channel 2 must still accept and deliver data immediately while
            // channel 1 is completely stalled.
            let payload = b"healthy multiplexed channel".to_vec();
            assert!(
                healthy
                    .try_enqueue_data(MachineMsg::Data(payload.clone()))
                    .await
            );
            assert_eq!(
                read_frame(&mut guest2).await.unwrap(),
                Some(MachineMsg::Data(payload))
            );

            // Closing the stalled channel aborts both reader & writer so
            // draining `guest1` sees EOF instead of hanging forever.
            stalled.close();
            let _ = w_task1.await;
            let mut buf = vec![0u8; 64 << 10];
            loop {
                let n = guest1.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
            }

            healthy.close();
        });
    }
}
