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
use std::sync::{Arc, Mutex};

use anyhow::{Context, bail};
use kat_proto::machine::{ExecSpec, MachineMsg, Tty};
use russh::keys::PrivateKey;
use russh::server::{Auth, ChannelOpenHandle, Msg, Session};
use russh::{Channel, ChannelId, MethodKind, MethodSet, Sig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::{AbortHandle, JoinHandle};

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
/// cannot exhaust host memory, while bounded backpressure tolerates a
/// temporarily slower reading guest without indefinitely suspending other
/// multiplexed channels.
const GUEST_WRITE_QUEUE: usize = 128;
const GUEST_SIGNAL_QUEUE: usize = 16;
/// Maximum time to wait for queue space when a channel's bounded queue is full
/// before concluding the guest stopped reading and closing only that channel.
const GUEST_WRITE_BACKPRESSURE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
/// Grace period on `channel_close` to flush queued stdin/`DataEnd` to a
/// reading guest before aborting a stalled writer.
const CLOSE_FLUSH_GRACE: std::time::Duration = std::time::Duration::from_millis(250);
/// Maximum time to wait for the guest machine service to accept a new `Exec`
/// or `Connect` handshake.
const GUEST_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

struct AbortOnDrop(AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Coalescing latest-wins slot for terminal resize requests so a storm of
/// `window_change_request` events can never fill or starve `Signal` delivery.
#[derive(Default)]
struct PendingResize {
    latest: Mutex<Option<(u16, u16)>>,
    notify: Notify,
}

impl PendingResize {
    fn set(&self, rows: u16, cols: u16) {
        *self.latest.lock().unwrap_or_else(|e| e.into_inner()) = Some((rows, cols));
        self.notify.notify_one();
    }

    fn take(&self) -> Option<(u16, u16)> {
        self.latest.lock().unwrap_or_else(|e| e.into_inner()).take()
    }

    async fn next(&self) -> (u16, u16) {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(size) = self.take() {
                return size;
            }
            notified.await;
        }
    }
}

struct GuestChan {
    data_tx: Option<mpsc::Sender<MachineMsg>>,
    signal_tx: Option<mpsc::Sender<i32>>,
    resize: Arc<PendingResize>,
    stop_read: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl GuestChan {
    /// Spawn a channel task off the SSH handler loop so connecting and pumping
    /// never stall other multiplexed channels.
    fn spawn_task<F, Fut>(build: F) -> Self
    where
        F: FnOnce(
            mpsc::Receiver<i32>,
            Arc<PendingResize>,
            mpsc::Receiver<MachineMsg>,
            oneshot::Receiver<()>,
        ) -> Fut,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let (data_tx, data_rx) = mpsc::channel(GUEST_WRITE_QUEUE);
        let (signal_tx, signal_rx) = mpsc::channel(GUEST_SIGNAL_QUEUE);
        let resize = Arc::new(PendingResize::default());
        let (stop_read, stop_read_rx) = oneshot::channel();
        let task = tokio::spawn(build(signal_rx, resize.clone(), data_rx, stop_read_rx));
        GuestChan {
            data_tx: Some(data_tx),
            signal_tx: Some(signal_tx),
            resize,
            stop_read: Some(stop_read),
            task: Some(task),
        }
    }

    /// Enqueue a stream frame with bounded backpressure. When the queue is
    /// full because the guest is reading slower than the SSH client, waits up
    /// to `timeout` for capacity instead of disconnecting immediately; returns
    /// `false` only if the guest remains completely blocked for `timeout`.
    async fn enqueue_data(&self, msg: MachineMsg, timeout: std::time::Duration) -> bool {
        let Some(tx) = self.data_tx.as_ref() else {
            return true;
        };
        match tx.try_send(msg) {
            Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => true,
            Err(mpsc::error::TrySendError::Full(msg)) => {
                tokio::time::timeout(timeout, tx.send(msg)).await.is_ok()
            }
        }
    }

    /// Record the latest terminal dimensions, coalescing any unconsumed resize.
    fn set_resize(&self, rows: u16, cols: u16) {
        self.resize.set(rows, cols);
    }

    /// Enqueue a signal on its dedicated high-priority queue (isolated from
    /// coalesced resizes). Returns `false` if the signal queue stays full for
    /// `timeout` so the caller can explicitly close the channel instead of
    /// silently dropping the signal.
    async fn enqueue_signal(&self, signal: i32, timeout: std::time::Duration) -> bool {
        let Some(tx) = self.signal_tx.as_ref() else {
            return true;
        };
        match tx.try_send(signal) {
            Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => true,
            Err(mpsc::error::TrySendError::Full(signal)) => {
                tokio::time::timeout(timeout, tx.send(signal)).await.is_ok()
            }
        }
    }

    /// Close on client `channel_close`: stop reading immediately, drop the
    /// senders so `pump_write` flushes already-queued stdin/`DataEnd` to a
    /// reading guest, and abort the task after `grace` if the guest is stalled.
    fn close_graceful(mut self, grace: std::time::Duration) {
        self.stop_read.take();
        self.data_tx.take();
        self.signal_tx.take();
        if let Some(task) = self.task.take() {
            let abort = task.abort_handle();
            tokio::spawn(async move {
                tokio::select! {
                    _ = task => {}
                    _ = tokio::time::sleep(grace) => {
                        abort.abort();
                    }
                }
            });
        }
    }

    fn abort_now(self) {
        drop(self);
    }
}

impl Drop for GuestChan {
    fn drop(&mut self) {
        self.stop_read.take();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Drive the guest read and write pumps concurrently until the guest closes or
/// the write side finishes draining after `close_graceful`.
async fn run_pumps(
    stream: UnixStream,
    handle: russh::server::Handle,
    ch: ChannelId,
    signal_rx: mpsc::Receiver<i32>,
    resize: Arc<PendingResize>,
    data_rx: mpsc::Receiver<MachineMsg>,
    stop_read: oneshot::Receiver<()>,
) {
    let (reader, writer) = stream.into_split();
    let reader_task = tokio::spawn(async move {
        tokio::select! {
            _ = pump_read(handle, ch, reader) => true,
            _ = stop_read => false,
        }
    });
    let _abort_reader = AbortOnDrop(reader_task.abort_handle());
    tokio::pin!(reader_task);
    let mut write_fut = std::pin::pin!(pump_write(signal_rx, resize, data_rx, writer));
    tokio::select! {
        _ = &mut write_fut => {}
        res = &mut reader_task => {
            if matches!(res, Ok(false)) {
                write_fut.await;
            }
        }
    }
}

/// Connect to the guest's machine service and complete a `Connect` handshake
/// within `timeout`.
async fn connect_direct_tcpip(
    home: &Path,
    machine: &str,
    port: u16,
    timeout: std::time::Duration,
) -> anyhow::Result<UnixStream> {
    tokio::time::timeout(timeout, async {
        let mut s = connect_guest(home).await?;
        write_frame(
            &mut s,
            &MachineMsg::Connect {
                name: machine.to_string(),
                port,
            },
        )
        .await?;
        match read_frame(&mut s).await? {
            Some(MachineMsg::Done) => Ok(s),
            Some(MachineMsg::Error(e)) => bail!("{e}"),
            other => bail!("unexpected answer {other:?}"),
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("guest connect timed out after {}s", timeout.as_secs()))?
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
    fn close_stalled(&mut self, ch: ChannelId, notice: &[u8], session: &mut Session) {
        if let Some(c) = self.chans.remove(&ch) {
            if let Some(g) = c.guest {
                g.abort_now();
            }
            if c.is_session {
                let _ = session.extended_data(ch, 1, notice.to_vec());
            }
            let _ = session.close(ch);
        }
    }

    /// Enqueue stream data (`Data` / `DataEnd`) with bounded backpressure so a
    /// temporarily slower reading guest paces the client while a permanently
    /// stalled guest closes only its own channel.
    async fn send_data(&mut self, ch: ChannelId, m: MachineMsg, session: &mut Session) {
        let ok = {
            let Some(g) = self.chans.get(&ch).and_then(|c| c.guest.as_ref()) else {
                return;
            };
            g.enqueue_data(m, GUEST_WRITE_BACKPRESSURE_TIMEOUT).await
        };
        if !ok {
            self.close_stalled(
                ch,
                b"katamaran: guest stopped reading; closing channel\r\n",
                session,
            );
        }
    }

    fn send_resize(&self, ch: ChannelId, rows: u16, cols: u16) {
        if let Some(g) = self.chans.get(&ch).and_then(|c| c.guest.as_ref()) {
            g.set_resize(rows, cols);
        }
    }

    async fn send_signal(&mut self, ch: ChannelId, signal: i32, session: &mut Session) {
        let ok = {
            let Some(g) = self.chans.get(&ch).and_then(|c| c.guest.as_ref()) else {
                return;
            };
            g.enqueue_signal(signal, GUEST_WRITE_BACKPRESSURE_TIMEOUT)
                .await
        };
        if !ok {
            self.close_stalled(
                ch,
                b"katamaran: guest stopped reading; could not deliver signal, closing channel\r\n",
                session,
            );
        }
    }

    /// Start `argv` (empty: login shell) for a session channel without blocking
    /// the SSH handler loop on guest connection setup.
    fn run(&mut self, ch: ChannelId, argv: Vec<String>, session: &mut Session) {
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
        let home = self.home.clone();
        let handle = session.handle();
        let guest =
            GuestChan::spawn_task(move |signal_rx, resize, data_rx, stop_read| async move {
                let started = tokio::time::timeout(GUEST_CONNECT_TIMEOUT, async {
                    let mut s = connect_guest(&home).await?;
                    write_frame(&mut s, &MachineMsg::Exec(spec)).await?;
                    Ok::<_, anyhow::Error>(s)
                })
                .await
                .map_err(|_| {
                    anyhow::anyhow!(
                        "guest exec timed out after {}s",
                        GUEST_CONNECT_TIMEOUT.as_secs()
                    )
                })
                .and_then(|r| r);
                match started {
                    Ok(s) => {
                        let _ = handle.channel_success(ch).await;
                        run_pumps(s, handle, ch, signal_rx, resize, data_rx, stop_read).await;
                    }
                    Err(e) => {
                        let _ = handle
                            .extended_data(ch, 1, format!("katamaran: {e:#}\r\n").into_bytes())
                            .await;
                        let _ = handle.channel_failure(ch).await;
                        let _ = handle.close(ch).await;
                    }
                }
            });
        self.chans.entry(ch).or_default().guest = Some(guest);
    }
}

/// Write queued host-to-guest frames asynchronously so SSH callbacks never
/// block the runtime or deadlock against guest output. Signals take highest
/// priority, followed by the latest coalesced resize, then queued stream data.
async fn pump_write(
    mut signal_rx: mpsc::Receiver<i32>,
    resize: Arc<PendingResize>,
    mut data_rx: mpsc::Receiver<MachineMsg>,
    mut w: OwnedWriteHalf,
) {
    let mut signals_open = true;
    let mut data_open = true;
    loop {
        let msg = if let Ok(signal) = signal_rx.try_recv() {
            MachineMsg::Signal { signal }
        } else if let Some((rows, cols)) = resize.take() {
            MachineMsg::Resize { rows, cols }
        } else if !signals_open && !data_open {
            break;
        } else {
            tokio::select! {
                biased;
                res = signal_rx.recv(), if signals_open => match res {
                    Some(signal) => MachineMsg::Signal { signal },
                    None => {
                        signals_open = false;
                        continue;
                    }
                },
                (rows, cols) = resize.next() => MachineMsg::Resize { rows, cols },
                res = data_rx.recv(), if data_open => match res {
                    Some(m) => m,
                    None => {
                        data_open = false;
                        continue;
                    }
                },
            }
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
        let ch = channel.id();
        let home = self.home.clone();
        let machine = self.machine.clone();
        let h = session.handle();
        // Run the guest Connect handshake off the SSH handler loop so a stalled
        // guest cannot hold up other multiplexed channels on this connection.
        let guest =
            GuestChan::spawn_task(move |signal_rx, resize, data_rx, stop_read| async move {
                let Ok(s) =
                    connect_direct_tcpip(&home, &machine, port, GUEST_CONNECT_TIMEOUT).await
                else {
                    // Dropping `reply` sends SSH_MSG_CHANNEL_OPEN_FAILURE.
                    drop(reply);
                    return;
                };
                reply.accept().await;
                run_pumps(s, h, ch, signal_rx, resize, data_rx, stop_read).await;
            });
        self.chans.insert(
            ch,
            Chan {
                guest: Some(guest),
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
        self.send_resize(
            channel,
            row_height.min(u16::MAX as u32) as u16,
            col_width.min(u16::MAX as u32) as u16,
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
            g.close_graceful(CLOSE_FLUSH_GRACE);
        }
        Ok(())
    }

    async fn signal(
        &mut self,
        channel: ChannelId,
        signal: Sig,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        if let Some(signal) = signal_number(&signal) {
            self.send_signal(channel, signal, session).await;
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

    fn test_guest_chan(stream: UnixStream) -> GuestChan {
        GuestChan::spawn_task(move |signal_rx, resize, data_rx, stop_read| async move {
            let (mut reader, writer) = stream.into_split();
            let reader_task = tokio::spawn(async move {
                tokio::select! {
                    _ = read_frame(&mut reader) => true,
                    _ = stop_read => false,
                }
            });
            let _abort_reader = AbortOnDrop(reader_task.abort_handle());
            tokio::pin!(reader_task);
            let mut write_fut = std::pin::pin!(pump_write(signal_rx, resize, data_rx, writer));
            tokio::select! {
                _ = &mut write_fut => {}
                res = &mut reader_task => {
                    if matches!(res, Ok(false)) {
                        write_fut.await;
                    }
                }
            }
        })
    }

    #[test]
    fn slow_reading_guest_succeeds_with_bounded_backpressure() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let (host, mut guest) = UnixStream::pair().unwrap();
            let ch = test_guest_chan(host);
            let total = GUEST_WRITE_QUEUE + 48;
            let chunk = vec![0x61u8; 16 << 10];

            // Guest starts reading only after a short delay, then reads slowly
            // so the bounded queue fills before draining.
            let reader = tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
                let mut count = 0usize;
                while let Some(msg) = read_frame(&mut guest).await.unwrap() {
                    match msg {
                        MachineMsg::Data(d) => {
                            assert_eq!(d.len(), 16 << 10);
                            count += 1;
                            if count.is_multiple_of(16) {
                                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                            }
                        }
                        MachineMsg::DataEnd => break,
                        other => panic!("unexpected frame: {other:?}"),
                    }
                }
                count
            });

            for _ in 0..total {
                assert!(
                    ch.enqueue_data(
                        MachineMsg::Data(chunk.clone()),
                        GUEST_WRITE_BACKPRESSURE_TIMEOUT
                    )
                    .await,
                    "slow but reading guest must not be closed as stalled"
                );
            }
            assert!(
                ch.enqueue_data(MachineMsg::DataEnd, GUEST_WRITE_BACKPRESSURE_TIMEOUT)
                    .await
            );
            ch.close_graceful(GUEST_WRITE_BACKPRESSURE_TIMEOUT);

            assert_eq!(reader.await.unwrap(), total);
        });
    }

    #[test]
    fn resizes_coalesce_and_signals_are_never_silently_dropped() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let (host, mut guest) = UnixStream::pair().unwrap();
            let ch = test_guest_chan(host);

            // Fill the socket buffer + data queue so `pump_write` is stalled
            // mid-write on a large frame.
            let chunk = vec![0x33u8; 32 << 10];
            let short = std::time::Duration::from_millis(20);
            for _ in 0..(GUEST_WRITE_QUEUE + 256) {
                if !ch
                    .enqueue_data(MachineMsg::Data(chunk.clone()), short)
                    .await
                {
                    break;
                }
            }

            // A storm of 64 resizes must coalesce into the latest dimensions
            // and never block or fill the dedicated signal queue.
            for i in 1u16..=64 {
                ch.set_resize(24 + i, 80 + i);
            }
            assert!(
                ch.enqueue_signal(libc::SIGINT, short).await,
                "resizes must not fill or block signal delivery"
            );

            // Filling all remaining signal slots then sending one more while
            // still stalled must return `false` instead of silently dropping.
            for _ in 1..GUEST_SIGNAL_QUEUE {
                assert!(ch.enqueue_signal(libc::SIGTERM, short).await);
            }
            assert!(
                !ch.enqueue_signal(libc::SIGHUP, short).await,
                "full signal queue must report failure rather than silently discard"
            );

            // Once the guest resumes reading, drain the in-flight data frames
            // and verify all queued signals arrive before the single coalesced
            // latest resize (88x144).
            let mut got_sigint = false;
            let mut got_resizes = Vec::new();
            let mut sig_count = 0usize;
            ch.close_graceful(std::time::Duration::from_secs(2));
            while let Some(msg) = read_frame(&mut guest).await.unwrap() {
                match msg {
                    MachineMsg::Signal { signal } => {
                        assert!(
                            got_resizes.is_empty(),
                            "signals must be prioritized ahead of coalesced resize"
                        );
                        if signal == libc::SIGINT {
                            got_sigint = true;
                        }
                        sig_count += 1;
                    }
                    MachineMsg::Resize { rows, cols } => {
                        got_resizes.push((rows, cols));
                    }
                    MachineMsg::Data(_) => {}
                    other => panic!("unexpected frame: {other:?}"),
                }
            }
            assert!(got_sigint, "SIGINT must be delivered");
            assert_eq!(sig_count, GUEST_SIGNAL_QUEUE);
            assert_eq!(got_resizes, vec![(24 + 64, 80 + 64)]);
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
            let stalled = test_guest_chan(host1);

            // Channel 2: healthy multiplexed guest reading concurrently.
            let (host2, mut guest2) = UnixStream::pair().unwrap();
            let healthy = test_guest_chan(host2);

            // Pump frames into channel 1 until its bounded queue times out.
            // `enqueue_data` must return `false` once the bounded timeout
            // elapses without hanging the runtime indefinitely.
            let chunk = vec![0x42u8; 32 << 10];
            let short = std::time::Duration::from_millis(20);
            let mut overflowed = false;
            for _ in 0..(GUEST_WRITE_QUEUE + 256) {
                if !stalled
                    .enqueue_data(MachineMsg::Data(chunk.clone()), short)
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
                    .enqueue_data(MachineMsg::Data(payload.clone()), short)
                    .await
            );
            assert_eq!(
                read_frame(&mut guest2).await.unwrap(),
                Some(MachineMsg::Data(payload))
            );

            // Aborting the stalled channel releases both reader & writer so
            // draining `guest1` sees EOF instead of hanging forever.
            stalled.abort_now();
            tokio::task::yield_now().await;
            let mut buf = vec![0u8; 64 << 10];
            loop {
                let n = guest1.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
            }

            healthy.abort_now();
        });
    }

    #[test]
    fn close_graceful_flushes_queued_data_and_eof_and_aborts_stalled() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            // Case 1: client sends Data + DataEnd then immediately closes.
            // Guest must still receive all queued frames and DataEnd before EOF.
            let (host, mut guest) = UnixStream::pair().unwrap();
            let ch = test_guest_chan(host);
            assert!(
                ch.enqueue_data(
                    MachineMsg::Data(b"first".to_vec()),
                    GUEST_WRITE_BACKPRESSURE_TIMEOUT
                )
                .await
            );
            assert!(
                ch.enqueue_data(
                    MachineMsg::Data(b"second".to_vec()),
                    GUEST_WRITE_BACKPRESSURE_TIMEOUT
                )
                .await
            );
            assert!(
                ch.enqueue_data(MachineMsg::DataEnd, GUEST_WRITE_BACKPRESSURE_TIMEOUT)
                    .await
            );
            ch.close_graceful(CLOSE_FLUSH_GRACE);

            assert_eq!(
                read_frame(&mut guest).await.unwrap(),
                Some(MachineMsg::Data(b"first".to_vec()))
            );
            assert_eq!(
                read_frame(&mut guest).await.unwrap(),
                Some(MachineMsg::Data(b"second".to_vec()))
            );
            assert_eq!(
                read_frame(&mut guest).await.unwrap(),
                Some(MachineMsg::DataEnd)
            );
            assert_eq!(read_frame(&mut guest).await.unwrap(), None);

            // Case 2: stalled guest stops reading; close_graceful aborts the
            // writer once the grace period expires so draining sees EOF.
            let (host, mut guest) = UnixStream::pair().unwrap();
            let ch = test_guest_chan(host);
            let chunk = vec![0x55u8; 32 << 10];
            let short = std::time::Duration::from_millis(20);
            for _ in 0..(GUEST_WRITE_QUEUE + 256) {
                if !ch
                    .enqueue_data(MachineMsg::Data(chunk.clone()), short)
                    .await
                {
                    break;
                }
            }
            ch.close_graceful(std::time::Duration::from_millis(30));
            tokio::time::sleep(std::time::Duration::from_millis(60)).await;
            let mut buf = vec![0u8; 64 << 10];
            loop {
                let n = guest.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
            }
        });
    }

    #[test]
    fn connect_direct_tcpip_times_out_when_guest_stalls() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let home = std::env::temp_dir().join(format!("kat-ssh-conn-to-{}", std::process::id()));
        let _ = fs::remove_dir_all(&home);
        fs::create_dir_all(home.join("run")).unwrap();
        let sock = crate::machines::socket_path(&home);

        rt.block_on(async {
            let listener = tokio::net::UnixListener::bind(&sock).unwrap();
            let _server = tokio::spawn(async move {
                // Accept and never answer `Connect`.
                if let Ok((_conn, _)) = listener.accept().await {
                    tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                }
            });

            let t = std::time::Instant::now();
            let err =
                connect_direct_tcpip(&home, "dev", 8080, std::time::Duration::from_millis(80))
                    .await
                    .unwrap_err();
            assert!(
                t.elapsed() < std::time::Duration::from_secs(1),
                "timed out too slowly: {:?}",
                t.elapsed()
            );
            assert!(format!("{err:#}").contains("timed out"), "{err:#}");
        });

        let _ = fs::remove_dir_all(&home);
    }
}
