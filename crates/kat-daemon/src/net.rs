//! Guest networking: gvproxy (containers/gvisor-tap-vsock) as the userspace
//! network stack behind the guest's virtio-net device, and automatic port
//! forwards from the Mac's 127.0.0.1 to listeners the guest reports.
//!
//! gvproxy runs in the VM process when `libgvproxy.dylib` is built (frames
//! then pass by function call, see kat-vmm's inproc.rs), or as its own process
//! behind a datagram socket. Its services API is the same either way.
//!
//! Every gvproxy flag and API path used here was checked against its source at
//! the tag `cargo xtask gvproxy` builds (v0.9.0): cmd/gvproxy/config.go for the
//! flags, pkg/services/forwarder/ports.go for `/forwarder/{expose,unexpose}`,
//! which the `-services` endpoint serves under `/services`.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use kat_proto::PortProto;
use kat_vmm::{InProcess, Network};

/// gvproxy's default subnet, passed explicitly so guest and stack agree.
const SUBNET: &str = "192.168.127.0/24";
const GATEWAY: Ipv4Addr = Ipv4Addr::new(192, 168, 127, 1);
const GUEST: Ipv4Addr = Ipv4Addr::new(192, 168, 127, 2);
/// gvproxy NATs this address to the Mac's 127.0.0.1 (its default NAT table).
const HOST: Ipv4Addr = Ipv4Addr::new(192, 168, 127, 254);
/// The MAC gvproxy's default DHCP lease is bound to.
const GUEST_MAC: [u8; 6] = [0x5a, 0x94, 0xef, 0xe4, 0x0c, 0xee];
/// Largest MTU whose frame fits a macOS unixgram datagram as libkrun sizes
/// it (65562 bytes minus the virtio-net header); gvproxy reads 128 KiB.
pub const DEFAULT_MTU: u16 = 65520;
const SUN_PATH_MAX: usize = 104;

/// A running gvproxy and a thread that reports if it dies, or gvproxy's
/// command line for the VM process to run it in-process.
pub struct Gvproxy {
    child: Option<Arc<Mutex<Child>>>,
    in_process: Option<InProcess>,
    stopping: Arc<AtomicBool>,
    watcher: Option<JoinHandle<()>>,
    pub api_socket: PathBuf,
    vfkit_socket: PathBuf,
    token: PathBuf,
    mtu: u16,
}

impl Gvproxy {
    /// Start gvproxy with its sockets in `run_dir`. Returns at once; the VM
    /// process waits for the vfkit socket, so startup overlaps the VM's.
    pub fn start(bin: &Path, run_dir: &Path, log: &Path, mtu: u16) -> anyhow::Result<Gvproxy> {
        if !bin.is_file() {
            bail!(
                "gvproxy not found at {}; build it with `cargo xtask gvproxy`",
                bin.display()
            );
        }
        // Unix socket paths are limited to 104 bytes on macOS.
        let run_dir =
            &fs::canonicalize(run_dir).with_context(|| format!("resolve {}", run_dir.display()))?;
        let pid_file = run_dir.join("gvproxy.pid");
        kill_stale(&pid_file);
        let vfkit_socket = run_dir.join("gvproxy-net.sock");
        let api_socket = run_dir.join("gvproxy-api.sock");
        for p in [&vfkit_socket, &api_socket] {
            if p.as_os_str().len() >= SUN_PATH_MAX {
                bail!("socket path {} is too long for macOS", p.display());
            }
            let _ = fs::remove_file(p);
        }
        // The forwarder API is also served to the guest on the gateway's port
        // 80. A token only we know keeps containers from opening ports on the
        // Mac; the host-side `-services` socket needs none.
        let token = run_dir.join("gvproxy.token");
        write_token(&token)?;

        let log_file = File::create(log).with_context(|| format!("create {}", log.display()))?;
        let child = Command::new(bin)
            .arg("-listen-vfkit")
            .arg(format!("unixgram://{}", vfkit_socket.display()))
            .args(stack_args(&api_socket, &token, mtu))
            .arg("-pid-file")
            .arg(&pid_file)
            .stdin(Stdio::null())
            .stdout(log_file.try_clone()?)
            .stderr(log_file)
            .spawn()
            .with_context(|| format!("spawn {}", bin.display()))?;

        let child = Arc::new(Mutex::new(child));
        let stopping = Arc::new(AtomicBool::new(false));
        let watcher = {
            let (watch_child, watch_stopping, watch_log) =
                (child.clone(), stopping.clone(), log.to_path_buf());
            match thread::Builder::new()
                .name("gvproxy-watch".into())
                .spawn(move || watch(&watch_child, &watch_stopping, &watch_log))
            {
                Ok(w) => w,
                Err(e) => {
                    let mut c = child.lock().unwrap();
                    let _ = c.kill();
                    let _ = c.wait();
                    return Err(e.into());
                }
            }
        };
        Ok(Gvproxy {
            child: Some(child),
            in_process: None,
            stopping,
            watcher: Some(watcher),
            api_socket,
            vfkit_socket,
            token,
            mtu,
        })
    }

    /// gvproxy for the VM process to load from `library` (built with
    /// `cargo xtask gvproxy`) and run in-process: frames then pass by
    /// function call instead of over a socket. Nothing runs until the VM
    /// starts; the services API appears in `run_dir` as with [`Self::start`].
    pub fn in_process(
        library: &Path,
        run_dir: &Path,
        log: &Path,
        mtu: u16,
    ) -> anyhow::Result<Gvproxy> {
        let run_dir =
            &fs::canonicalize(run_dir).with_context(|| format!("resolve {}", run_dir.display()))?;
        kill_stale(&run_dir.join("gvproxy.pid"));
        let api_socket = run_dir.join("gvproxy-api.sock");
        if api_socket.as_os_str().len() >= SUN_PATH_MAX {
            bail!("socket path {} is too long for macOS", api_socket.display());
        }
        let _ = fs::remove_file(&api_socket);
        let token = run_dir.join("gvproxy.token");
        write_token(&token)?;
        let mut args = stack_args(&api_socket, &token, mtu);
        args.extend(["-log-file".into(), log.display().to_string()]);
        Ok(Gvproxy {
            child: None,
            in_process: Some(InProcess {
                library: library.to_path_buf(),
                args,
            }),
            stopping: Arc::new(AtomicBool::new(false)),
            watcher: None,
            api_socket,
            vfkit_socket: PathBuf::new(),
            token,
            mtu,
        })
    }

    pub fn network(&self) -> Network {
        Network {
            in_process: self.in_process.clone(),
            socket: self.vfkit_socket.clone(),
            mac: GUEST_MAC,
            mtu: self.mtu,
            guest_ip: GUEST,
            prefix_len: 24,
            gateway: GATEWAY,
            host_ip: HOST,
        }
    }

    /// SIGTERM (gvproxy removes its sockets and pid file), then SIGKILL.
    pub fn stop(&mut self) {
        self.stopping.store(true, Ordering::SeqCst);
        if let Some(child) = &self.child {
            let mut child = child.lock().unwrap();
            if let Ok(None) = child.try_wait() {
                unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
                let deadline = Instant::now() + Duration::from_secs(5);
                while matches!(child.try_wait(), Ok(None)) && Instant::now() < deadline {
                    thread::sleep(Duration::from_millis(5));
                }
                let _ = child.kill();
                let _ = child.wait();
            }
        }
        if let Some(w) = self.watcher.take() {
            let _ = w.join();
        }
        if self.in_process.is_some() {
            // It went with the VM process, which could not remove it.
            let _ = fs::remove_file(&self.api_socket);
        }
        let _ = fs::remove_file(&self.token);
    }
}

impl Drop for Gvproxy {
    fn drop(&mut self) {
        self.stop();
    }
}

/// gvproxy's network and services options, shared by both modes.
fn stack_args(api_socket: &Path, token: &Path, mtu: u16) -> Vec<String> {
    [
        "-services".into(),
        format!("unix://{}", api_socket.display()),
        "-ssh-port".into(),
        "-1".into(),
        "-mtu".into(),
        mtu.to_string(),
        "-subnet".into(),
        SUBNET.into(),
        "-gatewayIP".into(),
        GATEWAY.to_string(),
        "-deviceIP".into(),
        GUEST.to_string(),
        "-hostIP".into(),
        HOST.to_string(),
        "-api-token-file".into(),
        token.display().to_string(),
    ]
    .into()
}

/// gvproxy cannot be restarted under a running VM: libkrun's datagram socket
/// stays connected to the old one. So a crash is reported, not retried.
fn watch(child: &Mutex<Child>, stopping: &AtomicBool, log: &Path) {
    loop {
        if stopping.load(Ordering::SeqCst) {
            return;
        }
        let status = child.lock().unwrap().try_wait();
        match status {
            Ok(None) => thread::sleep(Duration::from_millis(100)),
            Ok(Some(status)) => {
                if !stopping.load(Ordering::SeqCst) {
                    eprintln!("katamaran: gvproxy exited ({status}); guest networking is down");
                    crate::vm::print_tail("gvproxy", log);
                }
                return;
            }
            Err(e) => {
                eprintln!("katamaran: cannot wait for gvproxy: {e}");
                return;
            }
        }
    }
}

/// A gvproxy left behind by a daemon that was killed still holds its
/// sockets and forwarded ports. Kill it, but only if the pid is a gvproxy.
fn kill_stale(pid_file: &Path) {
    let Some(pid) = fs::read_to_string(pid_file)
        .ok()
        .and_then(|s| s.trim().parse::<i32>().ok())
    else {
        return;
    };
    if pid > 1 && process_name(pid).is_some_and(|n| n == "gvproxy") {
        eprintln!("katamaran: stopping stale gvproxy (pid {pid})");
        unsafe { libc::kill(pid, libc::SIGKILL) };
        for _ in 0..200 {
            if unsafe { libc::kill(pid, 0) } != 0 {
                break;
            }
            thread::sleep(Duration::from_millis(5));
        }
    }
    let _ = fs::remove_file(pid_file);
}

fn process_name(pid: i32) -> Option<String> {
    let mut buf = vec![0u8; libc::PROC_PIDPATHINFO_MAXSIZE as usize];
    let n = unsafe { libc::proc_pidpath(pid, buf.as_mut_ptr().cast(), buf.len() as u32) };
    if n <= 0 {
        return None;
    }
    buf.truncate(n as usize);
    let path = PathBuf::from(String::from_utf8(buf).ok()?);
    Some(path.file_name()?.to_string_lossy().into_owned())
}

/// 32 random bytes as hex, in a 0600 file (gvproxy refuses other modes).
fn write_token(path: &Path) -> anyhow::Result<()> {
    let mut raw = [0u8; 32];
    File::open("/dev/urandom")?.read_exact(&mut raw)?;
    let hex: String = raw.iter().map(|b| format!("{b:02x}")).collect();
    let _ = fs::remove_file(path);
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .and_then(|mut f| f.write_all(hex.as_bytes()))
        .with_context(|| format!("write {}", path.display()))
}

/// Why a forwarder API call failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ForwardError {
    /// gvproxy could not be reached yet (socket not up / transient I/O).
    Transient(String),
    /// gvproxy answered and refused the port (e.g. already in use).
    Refused(String),
}

impl std::fmt::Display for ForwardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ForwardError::Transient(m) | ForwardError::Refused(m) => f.write_str(m),
        }
    }
}

/// What the reconciler needs from gvproxy; a trait so tests can fake it.
pub trait ForwardApi {
    fn expose(&mut self, proto: PortProto, port: u16) -> Result<(), ForwardError>;
    fn unexpose(&mut self, proto: PortProto, port: u16) -> Result<(), ForwardError>;
    /// Re-verify or recreate an active forward whose previous `unexpose` failed
    /// ambiguously. Returns `Ok(())` once the forward is confirmed live again.
    fn ensure_exposed(&mut self, proto: PortProto, port: u16) -> Result<(), ForwardError> {
        match self.expose(proto, port) {
            Ok(()) => Ok(()),
            Err(ref e) if is_proxy_already_running(e) => Ok(()),
            Err(e) => Err(e),
        }
    }
    /// Whether a failed `expose` may still own upstream state (for example a
    /// gvproxy Unix listener whose rollback failed) that must be cleaned up via
    /// [`Self::unexpose`] when the guest removes the port.
    fn has_pending_cleanup(&self, _proto: PortProto, _port: u16) -> bool {
        false
    }
    /// The Mac addresses `port` is actually listening on (or configured to
    /// listen on when not yet active).
    fn port_addrs(&self, _proto: PortProto, _port: u16) -> Vec<Ipv4Addr> {
        vec![Ipv4Addr::LOCALHOST]
    }
    /// The Mac addresses `port` is actually listening on, for log lines.
    fn port_label(&self, proto: PortProto, port: u16) -> String {
        addrs_label(&self.port_addrs(proto, port))
    }
}

fn is_proxy_already_running(err: &ForwardError) -> bool {
    matches!(
        err,
        ForwardError::Refused(msg) if msg.to_ascii_lowercase().contains("already running")
    )
}

fn is_proxy_not_found(err: &ForwardError) -> bool {
    matches!(
        err,
        ForwardError::Refused(msg) if msg.to_ascii_lowercase().contains("proxy not found")
    )
}

/// gvproxy's forwarder API over its `-services` Unix socket.
pub struct GvproxyApi {
    pub socket: PathBuf,
    pub guest: Ipv4Addr,
}

impl ForwardApi for GvproxyApi {
    fn expose(&mut self, proto: PortProto, port: u16) -> Result<(), ForwardError> {
        let body = format!(
            r#"{{"local":"127.0.0.1:{port}","remote":"{}:{port}","protocol":"{}"}}"#,
            self.guest,
            proto.as_str()
        );
        post(&self.socket, "/services/forwarder/expose", &body)
    }

    fn unexpose(&mut self, proto: PortProto, port: u16) -> Result<(), ForwardError> {
        let body = format!(
            r#"{{"local":"127.0.0.1:{port}","protocol":"{}"}}"#,
            proto.as_str()
        );
        post(&self.socket, "/services/forwarder/unexpose", &body)
    }
}

impl GvproxyApi {
    /// Have gvproxy serve the guest's TCP `port` on the Unix socket `local`.
    /// Callers must handle stale-socket cleanup separately so retries never
    /// unlink a live socket whose first expose response was lost.
    fn expose_unix(&self, local: &Path, port: u16) -> Result<(), ForwardError> {
        if local.as_os_str().len() >= SUN_PATH_MAX {
            return Err(ForwardError::Refused(format!(
                "socket path {} is too long for macOS",
                local.display()
            )));
        }
        let body = serde_json::json!({
            "local": local,
            "remote": format!("tcp://{}:{port}", self.guest),
            "protocol": "unix",
        });
        post(
            &self.socket,
            "/services/forwarder/expose",
            &body.to_string(),
        )
    }

    fn unexpose_unix(&self, local: &Path) -> Result<(), ForwardError> {
        let body = serde_json::json!({ "local": local, "protocol": "unix" });
        post(
            &self.socket,
            "/services/forwarder/unexpose",
            &body.to_string(),
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UpstreamState {
    /// gvproxy is confirmed to be serving this port's Unix socket.
    Exposed,
    /// A previous expose, unexpose, or rollback returned an ambiguous error,
    /// so gvproxy may or may not still hold this pathname in its proxy map.
    Uncertain,
}

/// Published ports when the forward addresses are not just `127.0.0.1`.
/// gvproxy listens on one address per expose, and below port 1024 macOS
/// lets only one wildcard socket own a port, so gvproxy cannot serve
/// `127.0.0.1:443` and `172.16.1.200:443` at once. Instead gvproxy serves
/// the guest port on a Unix socket, and the daemon listens on every forward
/// address and relays to it.
pub struct AddressesApi {
    gvproxy: GvproxyApi,
    sockets: PathBuf,
    listeners: crate::autofwd::AutoForwards,
    upstream: BTreeMap<u16, UpstreamState>,
}

impl AddressesApi {
    /// `sockets` is the (private) directory for gvproxy's per-port sockets.
    pub fn new(gvproxy: GvproxyApi, sockets: &Path, addrs: &[Ipv4Addr]) -> Self {
        AddressesApi {
            gvproxy,
            sockets: sockets.to_path_buf(),
            listeners: crate::autofwd::AutoForwards::with_limit(
                crate::autofwd::Upstream::PerPort(sockets.to_path_buf()),
                addrs,
                crate::autofwd::published_conn_limit(),
            ),
            upstream: BTreeMap::new(),
        }
    }

    fn ensure_upstream_exposed(&mut self, port: u16, sock: &Path) -> Result<(), ForwardError> {
        if self.upstream.get(&port) == Some(&UpstreamState::Exposed) && sock.exists() {
            return Ok(());
        }
        // Only clean up a stale file left by an earlier crashed VM before the
        // first expose attempt for this port in this session; never unlink a
        // pathname whose live gvproxy registration may still exist.
        if !self.upstream.contains_key(&port) {
            let _ = fs::remove_file(sock);
        }
        match self.gvproxy.expose_unix(sock, port) {
            Ok(()) => {
                self.upstream.insert(port, UpstreamState::Exposed);
                Ok(())
            }
            Err(e @ ForwardError::Transient(_)) => {
                self.upstream.insert(port, UpstreamState::Uncertain);
                Err(e)
            }
            Err(ref e) if is_proxy_already_running(e) => {
                if sock.exists() {
                    // The previous expose succeeded in gvproxy before its reply
                    // was lost, and the socket pathname is still intact.
                    self.upstream.insert(port, UpstreamState::Exposed);
                    return Ok(());
                }
                // gvproxy still tracks the proxy but its pathname is missing;
                // explicitly remove the registration before recreating it.
                self.unexpose_upstream(port, sock)?;
                let _ = fs::remove_file(sock);
                match self.gvproxy.expose_unix(sock, port) {
                    Ok(()) => {
                        self.upstream.insert(port, UpstreamState::Exposed);
                        Ok(())
                    }
                    Err(e2 @ ForwardError::Transient(_)) => {
                        self.upstream.insert(port, UpstreamState::Uncertain);
                        Err(e2)
                    }
                    Err(e2) => Err(e2),
                }
            }
            Err(e) => {
                // If an earlier attempt left state uncertain and a dead socket
                // file is blocking bind, confirm gvproxy does not own it before
                // removing the file and retrying once.
                if self.upstream.get(&port) == Some(&UpstreamState::Uncertain) && sock.exists() {
                    match self.gvproxy.unexpose_unix(sock) {
                        Ok(()) => {
                            self.upstream.remove(&port);
                            let _ = fs::remove_file(sock);
                        }
                        Err(ref ue) if is_proxy_not_found(ue) => {
                            self.upstream.remove(&port);
                            let _ = fs::remove_file(sock);
                        }
                        Err(te @ ForwardError::Transient(_)) => return Err(te),
                        Err(_) => return Err(e),
                    }
                    return match self.gvproxy.expose_unix(sock, port) {
                        Ok(()) => {
                            self.upstream.insert(port, UpstreamState::Exposed);
                            Ok(())
                        }
                        Err(e2 @ ForwardError::Transient(_)) => {
                            self.upstream.insert(port, UpstreamState::Uncertain);
                            Err(e2)
                        }
                        Err(e2) => Err(e2),
                    };
                }
                Err(e)
            }
        }
    }

    fn unexpose_upstream(&mut self, port: u16, sock: &Path) -> Result<(), ForwardError> {
        match self.gvproxy.unexpose_unix(sock) {
            Ok(()) => {
                self.upstream.remove(&port);
                Ok(())
            }
            Err(ref e) if is_proxy_not_found(e) => {
                // Confirmed absent in gvproxy (e.g. a previous unexpose closed
                // the socket before its HTTP reply was lost).
                self.upstream.remove(&port);
                Ok(())
            }
            Err(e) => {
                self.upstream.insert(port, UpstreamState::Uncertain);
                Err(e)
            }
        }
    }
}

impl ForwardApi for AddressesApi {
    fn expose(&mut self, proto: PortProto, port: u16) -> Result<(), ForwardError> {
        if proto != PortProto::Tcp {
            return self.gvproxy.expose(proto, port);
        }
        let sock = crate::autofwd::Upstream::socket(&self.sockets, port);
        if self.listeners.contains(port) {
            // If an earlier unexpose was ambiguous, verify or recreate gvproxy's
            // Unix socket before reporting the already-bound listener healthy.
            return match self.ensure_upstream_exposed(port, &sock) {
                Ok(()) => Ok(()),
                Err(e @ ForwardError::Transient(_)) => Err(e),
                Err(e @ ForwardError::Refused(_)) => {
                    self.listeners.remove(port);
                    Err(e)
                }
            };
        }
        // Have gvproxy listen on the per-port Unix socket before opening the
        // Mac's TCP listeners, so the very first accepted connection never
        // races socket creation and transient gvproxy errors do not churn
        // TCP binds.
        self.ensure_upstream_exposed(port, &sock)?;
        match self.listeners.add(port) {
            Ok(_) => Ok(()),
            Err(e) => {
                if self.unexpose_upstream(port, &sock).is_ok() {
                    let _ = fs::remove_file(&sock);
                }
                Err(ForwardError::Refused(e.to_string()))
            }
        }
    }

    fn unexpose(&mut self, proto: PortProto, port: u16) -> Result<(), ForwardError> {
        if proto != PortProto::Tcp {
            return self.gvproxy.unexpose(proto, port);
        }
        let sock = crate::autofwd::Upstream::socket(&self.sockets, port);
        self.unexpose_upstream(port, &sock)?;
        self.listeners.remove(port);
        let _ = fs::remove_file(&sock);
        Ok(())
    }

    fn ensure_exposed(&mut self, proto: PortProto, port: u16) -> Result<(), ForwardError> {
        if proto != PortProto::Tcp {
            return self.gvproxy.ensure_exposed(proto, port);
        }
        self.expose(proto, port)
    }

    fn has_pending_cleanup(&self, proto: PortProto, port: u16) -> bool {
        proto == PortProto::Tcp && self.upstream.contains_key(&port)
    }

    fn port_addrs(&self, proto: PortProto, port: u16) -> Vec<Ipv4Addr> {
        if proto != PortProto::Tcp {
            return vec![Ipv4Addr::LOCALHOST];
        }
        self.listeners
            .bound_addrs(port)
            .map(<[Ipv4Addr]>::to_vec)
            .unwrap_or_else(|| self.listeners.addrs().to_vec())
    }
}

impl Drop for AddressesApi {
    fn drop(&mut self) {
        let mut ports: BTreeSet<u16> = self.listeners.ports().into_iter().collect();
        ports.extend(self.upstream.keys().copied());
        self.listeners.clear();
        for port in ports {
            let _ = fs::remove_file(crate::autofwd::Upstream::socket(&self.sockets, port));
        }
    }
}

fn post(socket: &Path, path: &str, body: &str) -> Result<(), ForwardError> {
    let send = || -> io::Result<String> {
        let mut s = UnixStream::connect(socket)?;
        s.set_read_timeout(Some(Duration::from_secs(5)))?;
        s.write_all(http_post(path, body).as_bytes())?;
        let mut resp = String::new();
        s.read_to_string(&mut resp)?;
        Ok(resp)
    };
    let resp = send().map_err(|e| ForwardError::Transient(format!("gvproxy API: {e}")))?;
    parse_response(&resp)
}

fn http_post(path: &str, body: &str) -> String {
    format!(
        "POST {path} HTTP/1.1\r\nHost: gvproxy\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

/// `Ok` on a 200; `Refused` on a parsed non-200 HTTP response (whose body
/// carries gvproxy's error); `Transient` on a truncated or malformed response.
fn parse_response(resp: &str) -> Result<(), ForwardError> {
    let Some((head, body)) = resp.split_once("\r\n\r\n") else {
        return Err(ForwardError::Transient(format!(
            "gvproxy API: malformed response {resp:?}"
        )));
    };
    let status_line = head.lines().next().unwrap_or("");
    let mut parts = status_line.split_whitespace();
    let version = parts.next().unwrap_or("");
    let status = if version.starts_with("HTTP/") {
        parts.next().and_then(|c| c.parse::<u16>().ok())
    } else {
        None
    };
    match status {
        Some(200) => Ok(()),
        Some(code) => {
            let body = body.trim();
            Err(ForwardError::Refused(if body.is_empty() {
                format!("HTTP {code}")
            } else {
                body.to_string()
            }))
        }
        None => Err(ForwardError::Transient(format!(
            "gvproxy API: malformed response {resp:?}"
        ))),
    }
}

/// The host's view of the forwards: which ports are exposed and which the
/// Mac refused. A refused port is reported once and retried only after the
/// guest stops and starts listening on it again.
pub struct Forwards<A: ForwardApi> {
    api: A,
    active: BTreeSet<(PortProto, u16)>,
    /// Active ports whose last `unexpose` failed ambiguously; a re-add must
    /// verify or recreate the upstream before treating the port as live.
    remove_failed: BTreeSet<(PortProto, u16)>,
    /// Non-active ports (e.g. refused after a failed rollback, or transient
    /// expose) that may still own upstream state requiring `unexpose` on remove.
    cleanup_pending: BTreeSet<(PortProto, u16)>,
    refused: BTreeMap<(PortProto, u16), String>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Event {
    Forwarded(PortProto, u16, Vec<Ipv4Addr>),
    Refused(PortProto, u16, String),
    RefusedCleared(PortProto, u16),
    Transient(PortProto, u16, String),
    Removed(PortProto, u16),
    RemoveFailed(PortProto, u16, String),
}

impl<A: ForwardApi> Forwards<A> {
    pub fn new(api: A) -> Self {
        Forwards {
            api,
            active: BTreeSet::new(),
            remove_failed: BTreeSet::new(),
            cleanup_pending: BTreeSet::new(),
            refused: BTreeMap::new(),
        }
    }

    pub fn add(&mut self, proto: PortProto, port: u16) -> Option<Event> {
        let key = (proto, port);
        if self.remove_failed.contains(&key) {
            return match self.api.ensure_exposed(proto, port) {
                Ok(()) => {
                    self.remove_failed.remove(&key);
                    self.cleanup_pending.remove(&key);
                    None
                }
                Err(ForwardError::Transient(e)) => Some(Event::Transient(proto, port, e)),
                Err(ForwardError::Refused(e)) => {
                    self.active.remove(&key);
                    self.remove_failed.remove(&key);
                    if self.api.has_pending_cleanup(proto, port) {
                        self.cleanup_pending.insert(key);
                    } else {
                        self.cleanup_pending.remove(&key);
                    }
                    self.refused.insert(key, e.clone());
                    Some(Event::Refused(proto, port, e))
                }
            };
        }
        if self.active.contains(&key) || self.refused.contains_key(&key) {
            return None;
        }
        match self.api.expose(proto, port) {
            Ok(()) => {
                self.cleanup_pending.remove(&key);
                self.active.insert(key);
                let addrs = self.api.port_addrs(proto, port);
                Some(Event::Forwarded(proto, port, addrs))
            }
            Err(ForwardError::Refused(e)) => {
                if self.api.has_pending_cleanup(proto, port) {
                    self.cleanup_pending.insert(key);
                } else {
                    self.cleanup_pending.remove(&key);
                }
                self.refused.insert(key, e.clone());
                Some(Event::Refused(proto, port, e))
            }
            Err(ForwardError::Transient(e)) => {
                if self.api.has_pending_cleanup(proto, port) {
                    self.cleanup_pending.insert(key);
                }
                Some(Event::Transient(proto, port, e))
            }
        }
    }

    pub fn remove(&mut self, proto: PortProto, port: u16) -> Option<Event> {
        let key = (proto, port);
        if self.refused.remove(&key).is_some() {
            if !self.cleanup_pending.contains(&key) {
                return Some(Event::RefusedCleared(proto, port));
            }
            return Some(match self.api.unexpose(proto, port) {
                Ok(()) => {
                    self.cleanup_pending.remove(&key);
                    Event::RefusedCleared(proto, port)
                }
                Err(e) => Event::RemoveFailed(proto, port, e.to_string()),
            });
        }
        if self.cleanup_pending.contains(&key) && !self.active.contains(&key) {
            return Some(match self.api.unexpose(proto, port) {
                Ok(()) => {
                    self.cleanup_pending.remove(&key);
                    Event::RefusedCleared(proto, port)
                }
                Err(e) => Event::RemoveFailed(proto, port, e.to_string()),
            });
        }
        if !self.active.contains(&key) {
            return None;
        }
        Some(match self.api.unexpose(proto, port) {
            Ok(()) => {
                self.active.remove(&key);
                self.remove_failed.remove(&key);
                self.cleanup_pending.remove(&key);
                Event::Removed(proto, port)
            }
            Err(e) => {
                self.remove_failed.insert(key);
                Event::RemoveFailed(proto, port, e.to_string())
            }
        })
    }

    fn port_label(&self, proto: PortProto, port: u16) -> String {
        self.api.port_label(proto, port)
    }

    #[cfg(test)]
    fn active(&self) -> impl Iterator<Item = &(PortProto, u16)> {
        self.active.iter()
    }
}

pub enum PortChange {
    Add(PortProto, u16),
    Remove(PortProto, u16),
}

/// Handle to the port-forwarder worker thread. Dropping it signals shutdown,
/// closes the command channel, and waits for any in-flight operation plus RAII
/// listener and socket cleanup before a restart can bind them again.
pub struct Forwarder {
    shutdown: Arc<AtomicBool>,
    tx: Option<Sender<PortChange>>,
    thread: Option<JoinHandle<()>>,
}

impl Forwarder {
    pub fn send(&self, change: PortChange) -> Result<(), mpsc::SendError<PortChange>> {
        match &self.tx {
            Some(tx) => tx.send(change),
            None => Err(mpsc::SendError(change)),
        }
    }
}

impl Drop for Forwarder {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        self.tx.take();
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

const TRANSIENT_RETRY_BASE: Duration = Duration::from_millis(200);
const TRANSIENT_RETRY_MAX: Duration = Duration::from_secs(2);
const MAX_TRANSIENT_RETRIES: u32 = 30;

#[derive(Clone, Copy, PartialEq, Eq)]
enum PendingOp {
    Add,
    Remove,
}

/// Apply guest port changes on a worker thread, so a slow gvproxy call never
/// holds up the control loop. Reports each resulting [`Event`] to `on_event`.
pub fn spawn_forwarder(
    api: impl ForwardApi + Send + 'static,
    on_event: impl FnMut(Event) + Send + 'static,
) -> anyhow::Result<Forwarder> {
    spawn_forwarder_with(api, TRANSIENT_RETRY_BASE, on_event)
}

fn spawn_forwarder_with<A: ForwardApi + Send + 'static>(
    api: A,
    retry_base: Duration,
    mut on_event: impl FnMut(Event) + Send + 'static,
) -> anyhow::Result<Forwarder> {
    let (tx, rx) = mpsc::channel::<PortChange>();
    let shutdown = Arc::new(AtomicBool::new(false));
    let worker_shutdown = shutdown.clone();
    let thread = thread::Builder::new()
        .name("port-forward".into())
        .spawn(move || {
            let mut fwd = Forwards::new(api);
            let mut pending: BTreeMap<(PortProto, u16), (PendingOp, Instant, u32)> =
                BTreeMap::new();
            let retry_max = TRANSIENT_RETRY_MAX.max(retry_base);
            loop {
                if worker_shutdown.load(Ordering::SeqCst) {
                    return;
                }
                let next_due = pending.values().map(|(_, at, _)| *at).min();
                let got = match next_due {
                    Some(at) => rx.recv_timeout(at.saturating_duration_since(Instant::now())),
                    None => rx.recv().map_err(|_| RecvTimeoutError::Disconnected),
                };
                if worker_shutdown.load(Ordering::SeqCst) {
                    return;
                }
                match got {
                    Ok(PortChange::Add(proto, port)) => {
                        let key = (proto, port);
                        pending.remove(&key);
                        match fwd.add(proto, port) {
                            Some(e @ Event::Transient(..)) => {
                                pending
                                    .insert(key, (PendingOp::Add, Instant::now() + retry_base, 1));
                                report(&e, &fwd.port_label(proto, port));
                                on_event(e);
                            }
                            Some(e) => {
                                report(&e, &fwd.port_label(proto, port));
                                on_event(e);
                            }
                            None => {}
                        }
                    }
                    Ok(PortChange::Remove(proto, port)) => {
                        let key = (proto, port);
                        pending.remove(&key);
                        let label = fwd.port_label(proto, port);
                        match fwd.remove(proto, port) {
                            Some(e @ Event::RemoveFailed(..)) => {
                                pending.insert(
                                    key,
                                    (PendingOp::Remove, Instant::now() + retry_base, 1),
                                );
                                report(&e, &label);
                                on_event(e);
                            }
                            Some(e) => {
                                report(&e, &label);
                                on_event(e);
                            }
                            None => {}
                        }
                    }
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => return,
                }

                let now = Instant::now();
                let due: Vec<((PortProto, u16), PendingOp, u32)> = pending
                    .iter()
                    .filter(|(_, (_, at, _))| *at <= now)
                    .map(|(&k, &(op, _, attempts))| (k, op, attempts))
                    .collect();
                for ((proto, port), op, attempts) in due {
                    if worker_shutdown.load(Ordering::SeqCst) {
                        return;
                    }
                    let key = (proto, port);
                    pending.remove(&key);
                    let delay = retry_base
                        .saturating_mul(1u32 << attempts.min(4))
                        .min(retry_max);
                    match op {
                        PendingOp::Add => match fwd.add(proto, port) {
                            Some(e @ Event::Transient(..)) => {
                                if attempts < MAX_TRANSIENT_RETRIES {
                                    pending.insert(
                                        key,
                                        (PendingOp::Add, Instant::now() + delay, attempts + 1),
                                    );
                                } else {
                                    report(&e, &fwd.port_label(proto, port));
                                    on_event(e);
                                }
                            }
                            Some(e) => {
                                report(&e, &fwd.port_label(proto, port));
                                on_event(e);
                            }
                            None => {}
                        },
                        PendingOp::Remove => {
                            let label = fwd.port_label(proto, port);
                            match fwd.remove(proto, port) {
                                Some(e @ Event::RemoveFailed(..)) => {
                                    if attempts < MAX_TRANSIENT_RETRIES {
                                        pending.insert(
                                            key,
                                            (
                                                PendingOp::Remove,
                                                Instant::now() + delay,
                                                attempts + 1,
                                            ),
                                        );
                                    } else {
                                        report(&e, &label);
                                        on_event(e);
                                    }
                                }
                                Some(e) => {
                                    report(&e, &label);
                                    on_event(e);
                                }
                                None => {}
                            }
                        }
                    }
                }
            }
        })?;
    Ok(Forwarder {
        shutdown,
        tx: Some(tx),
        thread: Some(thread),
    })
}

fn report(e: &Event, addrs: &str) {
    match e {
        Event::Forwarded(proto, port, bound) => {
            println!(
                "katamaran: forwarding {}:{port}/{}",
                addrs_label(bound),
                proto.as_str()
            )
        }
        Event::Refused(proto, port, why) => eprintln!(
            "katamaran: cannot forward {addrs}:{port}/{} ({why}); skipped",
            proto.as_str()
        ),
        Event::RefusedCleared(..) => {}
        Event::Transient(proto, port, why) => eprintln!(
            "katamaran: transient error forwarding {addrs}:{port}/{} ({why})",
            proto.as_str()
        ),
        Event::Removed(proto, port) => {
            println!(
                "katamaran: stopped forwarding {addrs}:{port}/{}",
                proto.as_str()
            )
        }
        Event::RemoveFailed(proto, port, why) => eprintln!(
            "katamaran: cannot remove forward {addrs}:{port}/{} ({why})",
            proto.as_str()
        ),
    }
}

/// The Mac addresses forwards listen on, for log lines: `127.0.0.1`, or
/// `{127.0.0.1,172.16.1.200}`.
pub fn addrs_label(addrs: &[Ipv4Addr]) -> String {
    match addrs {
        [one] => one.to_string(),
        many => format!(
            "{{{}}}",
            many.iter()
                .map(Ipv4Addr::to_string)
                .collect::<Vec<_>>()
                .join(",")
        ),
    }
}

/// Whether `addr` is assigned to a loopback interface (`lo0`), so listening
/// on it cannot expose a port to the network.
pub fn is_loopback_interface_addr(addr: Ipv4Addr) -> bool {
    let mut head: *mut libc::ifaddrs = std::ptr::null_mut();
    if unsafe { libc::getifaddrs(&mut head) } != 0 {
        return addr.is_loopback();
    }
    let mut found = false;
    let mut cur = head;
    while !cur.is_null() {
        // SAFETY: `cur` walks the list `getifaddrs` returned, freed below.
        let ifa = unsafe { &*cur };
        cur = ifa.ifa_next;
        if ifa.ifa_addr.is_null()
            || ifa.ifa_flags & libc::IFF_LOOPBACK as libc::c_uint == 0
            || i32::from(unsafe { (*ifa.ifa_addr).sa_family }) != libc::AF_INET
        {
            continue;
        }
        // SAFETY: an AF_INET `ifa_addr` points at a `sockaddr_in`.
        let sin = unsafe { &*ifa.ifa_addr.cast::<libc::sockaddr_in>() };
        if Ipv4Addr::from(u32::from_be(sin.sin_addr.s_addr)) == addr {
            found = true;
            break;
        }
    }
    unsafe { libc::freeifaddrs(head) };
    found
}

/// TCP listeners on `<addr>:<port>` for each Mac address forwards use
/// (`127.0.0.1` by default, see `forward_addresses`).
///
/// On macOS, XNU's default receive model (`net.inet.ip.check_interface`) can
/// accept packets for a non-127/8 `lo0` alias arriving on a physical interface,
/// so specific-address alias sockets are also scoped to `lo0` via `IP_BOUND_IF`
/// before binding and validated per accepted peer in [`Self::accept_on`].
///
/// Below port 1024, XNU rejects unprivileged binds on specific addresses such
/// as `127.0.0.1`, while permitting wildcard (`0.0.0.0`) binds, and only one
/// wildcard socket can own a port. When binding a low port is denied, fall back
/// to a single `0.0.0.0:<port>` socket scoped to `lo0` via `IP_BOUND_IF`, and
/// drop any accepted connection whose local address is not one of the forward
/// addresses or whose remote address is not loopback.
pub struct LoopbackTcpListener {
    inner: Vec<TcpListener>,
    /// The addresses this listener is bound for.
    addrs: Vec<Ipv4Addr>,
    /// Per-socket flag indicating whether accepted connections must pass
    /// [`is_allowed_v4_pair`] (true for the macOS `0.0.0.0` wildcard fallback
    /// and for specific-address alias sockets).
    filter_endpoints: Vec<bool>,
}

/// Result of a single underlying `accept()` call on [`LoopbackTcpListener`].
pub enum AcceptOne {
    /// Accepted a connection whose local and peer addresses passed filtering.
    Accepted(TcpStream, SocketAddr),
    /// Accepted and dropped a connection rejected by the loopback endpoint filter.
    Rejected,
}

impl LoopbackTcpListener {
    pub fn bind(port: u16) -> io::Result<Self> {
        Self::bind_on(&[Ipv4Addr::LOCALHOST], port)
    }

    /// Listen on `port` of each of `addrs`. An address not assigned to a
    /// loopback interface is skipped, so a typo never opens a port to the
    /// network; so is one whose port is taken, as long as one address binds.
    pub fn bind_on(addrs: &[Ipv4Addr], port: u16) -> io::Result<Self> {
        Self::bind_on_inner(addrs, port, false)
    }

    fn bind_on_inner(addrs: &[Ipv4Addr], port: u16, force_lo0_scope: bool) -> io::Result<Self> {
        let mut seen = BTreeSet::new();
        let addrs: Vec<Ipv4Addr> = addrs
            .iter()
            .copied()
            .filter(|a| seen.insert(*a) && is_loopback_interface_addr(*a))
            .collect();
        if addrs.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "no forward address is assigned to lo0",
            ));
        }
        let mut inner = Vec::with_capacity(addrs.len());
        let mut bound = Vec::with_capacity(addrs.len());
        let mut filter_endpoints = Vec::with_capacity(addrs.len());
        let mut failed = Vec::new();
        let mut bind_port = port;
        for &a in &addrs {
            let is_alias = a != Ipv4Addr::LOCALHOST;
            let scope_lo0 = is_alias || force_lo0_scope;
            match bind_specific(a, bind_port, scope_lo0) {
                Ok(l) => {
                    if bind_port == 0 {
                        bind_port = l.local_addr()?.port();
                    }
                    inner.push(l);
                    bound.push(a);
                    filter_endpoints.push(scope_lo0);
                }
                #[cfg(target_os = "macos")]
                Err(e)
                    if port > 0 && port < 1024 && e.kind() == io::ErrorKind::PermissionDenied =>
                {
                    drop(inner);
                    return Ok(Self {
                        inner: vec![bind_darwin_lo0(Ipv4Addr::UNSPECIFIED, port)?],
                        addrs,
                        filter_endpoints: vec![true],
                    });
                }
                Err(e) => failed.push((a, e)),
            }
        }
        if inner.is_empty() {
            return Err(failed.swap_remove(0).1);
        }
        for (a, e) in failed {
            eprintln!("katamaran: cannot listen on {a}:{port} ({e}); skipped");
        }
        Ok(Self {
            inner,
            addrs: bound,
            filter_endpoints,
        })
    }

    #[cfg(test)]
    pub(crate) fn from_listeners_for_test(inner: Vec<TcpListener>) -> Self {
        let addrs = inner
            .iter()
            .filter_map(|l| match l.local_addr().ok()? {
                SocketAddr::V4(v4) => Some(*v4.ip()),
                SocketAddr::V6(_) => None,
            })
            .collect();
        let filter_endpoints = vec![false; inner.len()];
        Self {
            inner,
            addrs,
            filter_endpoints,
        }
    }

    #[cfg(test)]
    pub(crate) fn from_listeners_filtered_for_test(
        inner: Vec<TcpListener>,
        addrs: Vec<Ipv4Addr>,
        filter_endpoints: Vec<bool>,
    ) -> Self {
        assert_eq!(inner.len(), filter_endpoints.len());
        Self {
            inner,
            addrs,
            filter_endpoints,
        }
    }

    pub fn addrs(&self) -> &[Ipv4Addr] {
        &self.addrs
    }

    pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.inner
            .iter()
            .try_for_each(|l| l.set_nonblocking(nonblocking))
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner[0].local_addr()
    }

    /// The listening sockets, to poll; accept with [`Self::accept_one_on`] or
    /// [`Self::accept_on`].
    pub fn raw_fds(&self) -> Vec<RawFd> {
        self.inner.iter().map(AsRawFd::as_raw_fd).collect()
    }

    pub fn accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
        self.accept_on(0)
    }

    /// Perform one underlying `accept()` on the `i`th socket of
    /// [`Self::raw_fds`]. Connections dropped by endpoint filtering return
    /// [`AcceptOne::Rejected`] rather than looping internally, so callers can
    /// bound accept batches and observe shutdown flags even under a flood of
    /// rejected peers.
    pub fn accept_one_on(&self, i: usize) -> io::Result<AcceptOne> {
        let (stream, peer) = self.inner[i].accept()?;
        if !self.filter_endpoints[i] {
            return Ok(AcceptOne::Accepted(stream, peer));
        }
        if let Ok(local) = stream.local_addr()
            && is_allowed_v4_pair(&self.addrs, local, peer)
        {
            return Ok(AcceptOne::Accepted(stream, peer));
        }
        Ok(AcceptOne::Rejected)
    }

    /// Accept on the `i`th socket of [`Self::raw_fds`], skipping rejected peers.
    pub fn accept_on(&self, i: usize) -> io::Result<(TcpStream, SocketAddr)> {
        loop {
            if let AcceptOne::Accepted(stream, peer) = self.accept_one_on(i)? {
                return Ok((stream, peer));
            }
        }
    }
}

fn bind_specific(addr: Ipv4Addr, port: u16, scope_to_lo0: bool) -> io::Result<TcpListener> {
    #[cfg(target_os = "macos")]
    if scope_to_lo0 {
        return bind_darwin_lo0(addr, port);
    }
    let _ = scope_to_lo0;
    TcpListener::bind((addr, port))
}

/// A connection filtered listeners keep: to one of `allowed`, from loopback or
/// from a `lo0` address (a client of an `lo0` alias has that alias or another
/// `lo0` address as its source).
fn is_allowed_v4_pair(allowed: &[Ipv4Addr], local: SocketAddr, peer: SocketAddr) -> bool {
    is_allowed_v4_pair_with(allowed, local, peer, is_loopback_interface_addr)
}

fn is_allowed_v4_pair_with(
    allowed: &[Ipv4Addr],
    local: SocketAddr,
    peer: SocketAddr,
    is_lo_addr: impl FnOnce(Ipv4Addr) -> bool,
) -> bool {
    matches!(
        (local, peer),
        (SocketAddr::V4(l), SocketAddr::V4(p))
            if allowed.contains(l.ip())
                && (p.ip().is_loopback() || allowed.contains(p.ip()) || is_lo_addr(*p.ip()))
    )
}

#[cfg(target_os = "macos")]
const IP_BOUND_IF: libc::c_int = 25;

#[cfg(target_os = "macos")]
fn bind_darwin_lo0(ip: Ipv4Addr, port: u16) -> io::Result<TcpListener> {
    let lo0 = unsafe { libc::if_nametoindex(c"lo0".as_ptr()) };
    if lo0 == 0 {
        return Err(io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            "lo0 interface not found",
        ));
    }

    let raw = unsafe { libc::socket(libc::AF_INET, libc::SOCK_STREAM, 0) };
    if raw < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `raw` is a fresh open socket descriptor owned by this function.
    let fd = unsafe { OwnedFd::from_raw_fd(raw) };

    let flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFD) };
    if flags < 0
        || unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0
    {
        return Err(io::Error::last_os_error());
    }

    let one: libc::c_int = 1;
    if unsafe {
        libc::setsockopt(
            fd.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_REUSEADDR,
            (&one as *const libc::c_int).cast(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    } < 0
    {
        return Err(io::Error::last_os_error());
    }

    let if_idx: libc::c_uint = lo0;
    if unsafe {
        libc::setsockopt(
            fd.as_raw_fd(),
            libc::IPPROTO_IP,
            IP_BOUND_IF,
            (&if_idx as *const libc::c_uint).cast(),
            std::mem::size_of::<libc::c_uint>() as libc::socklen_t,
        )
    } < 0
    {
        return Err(io::Error::last_os_error());
    }

    let addr = libc::sockaddr_in {
        sin_len: std::mem::size_of::<libc::sockaddr_in>() as u8,
        sin_family: libc::AF_INET as libc::sa_family_t,
        sin_port: port.to_be(),
        sin_addr: libc::in_addr {
            s_addr: u32::from(ip).to_be(),
        },
        sin_zero: [0; 8],
    };
    if unsafe {
        libc::bind(
            fd.as_raw_fd(),
            (&addr as *const libc::sockaddr_in).cast(),
            std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
        )
    } < 0
    {
        return Err(io::Error::last_os_error());
    }

    if unsafe { libc::listen(fd.as_raw_fd(), 128) } < 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(TcpListener::from(fd))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct FakeApi {
        busy: BTreeSet<u16>,
        transient: bool,
        calls: Vec<String>,
    }

    impl ForwardApi for &mut FakeApi {
        fn expose(&mut self, proto: PortProto, port: u16) -> Result<(), ForwardError> {
            self.calls.push(format!("expose {port}/{}", proto.as_str()));
            if self.transient {
                return Err(ForwardError::Transient(
                    "gvproxy API: connection refused".into(),
                ));
            }
            if self.busy.contains(&port) {
                return Err(ForwardError::Refused(
                    "listen tcp 127.0.0.1: address already in use".into(),
                ));
            }
            Ok(())
        }

        fn unexpose(&mut self, proto: PortProto, port: u16) -> Result<(), ForwardError> {
            self.calls
                .push(format!("unexpose {port}/{}", proto.as_str()));
            Ok(())
        }
    }

    const TCP: PortProto = PortProto::Tcp;

    #[test]
    fn adds_and_removes_forwards_once() {
        let mut api = FakeApi::default();
        let mut f = Forwards::new(&mut api);
        let lo = vec![Ipv4Addr::LOCALHOST];
        assert_eq!(
            f.add(TCP, 8080),
            Some(Event::Forwarded(TCP, 8080, lo.clone()))
        );
        assert_eq!(f.add(TCP, 8080), None);
        assert_eq!(
            f.add(PortProto::Udp, 8080),
            Some(Event::Forwarded(PortProto::Udp, 8080, lo))
        );
        assert_eq!(f.remove(TCP, 8080), Some(Event::Removed(TCP, 8080)));
        assert_eq!(f.remove(TCP, 8080), None);
        assert_eq!(f.remove(TCP, 9999), None);
        assert_eq!(
            f.active().collect::<Vec<_>>(),
            vec![&(PortProto::Udp, 8080)]
        );
        drop(f);
        assert_eq!(
            api.calls,
            ["expose 8080/tcp", "expose 8080/udp", "unexpose 8080/tcp"]
        );
    }

    #[test]
    fn refused_port_is_reported_once_and_retried_after_relisten() {
        let mut api = FakeApi::default();
        api.busy.insert(5000);
        let mut f = Forwards::new(&mut api);
        assert!(matches!(
            f.add(TCP, 5000),
            Some(Event::Refused(TCP, 5000, _))
        ));
        assert_eq!(f.add(TCP, 5000), None);
        // Never exposed, so removing it clears the refusal without calling unexpose.
        assert_eq!(f.remove(TCP, 5000), Some(Event::RefusedCleared(TCP, 5000)));
        assert_eq!(f.remove(TCP, 5000), None);
        assert!(matches!(f.add(TCP, 5000), Some(Event::Refused(..))));
        drop(f);
        assert_eq!(api.calls, ["expose 5000/tcp", "expose 5000/tcp"]);
    }

    #[test]
    fn transient_gvproxy_error_is_not_permanently_refused() {
        let mut api = FakeApi {
            transient: true,
            ..Default::default()
        };
        let mut f = Forwards::new(&mut api);
        assert!(matches!(
            f.add(TCP, 8080),
            Some(Event::Transient(TCP, 8080, _))
        ));
        f.api.transient = false;
        assert_eq!(
            f.add(TCP, 8080),
            Some(Event::Forwarded(TCP, 8080, vec![Ipv4Addr::LOCALHOST]))
        );
    }

    #[test]
    fn failed_unexpose_keeps_port_active() {
        struct FailUnexpose {
            fail: bool,
        }
        impl ForwardApi for FailUnexpose {
            fn expose(&mut self, _: PortProto, _: u16) -> Result<(), ForwardError> {
                Ok(())
            }
            fn unexpose(&mut self, _: PortProto, _: u16) -> Result<(), ForwardError> {
                if self.fail {
                    Err(ForwardError::Transient("gvproxy API: busy".into()))
                } else {
                    Ok(())
                }
            }
        }
        let mut f = Forwards::new(FailUnexpose { fail: true });
        assert_eq!(
            f.add(TCP, 9000),
            Some(Event::Forwarded(TCP, 9000, vec![Ipv4Addr::LOCALHOST]))
        );
        assert!(matches!(
            f.remove(TCP, 9000),
            Some(Event::RemoveFailed(TCP, 9000, _))
        ));
        assert_eq!(f.active().collect::<Vec<_>>(), vec![&(TCP, 9000)]);
        f.api.fail = false;
        assert_eq!(f.remove(TCP, 9000), Some(Event::Removed(TCP, 9000)));
        assert!(f.active().next().is_none());
    }

    #[test]
    fn forwarder_retries_transient_errors_and_cancels_on_remove() {
        use std::sync::atomic::AtomicUsize;

        struct FlakyApi {
            fail_times: Arc<AtomicUsize>,
            calls: Arc<AtomicUsize>,
        }
        impl ForwardApi for FlakyApi {
            fn expose(&mut self, _: PortProto, _: u16) -> Result<(), ForwardError> {
                let n = self.calls.fetch_add(1, Ordering::SeqCst);
                if n < self.fail_times.load(Ordering::SeqCst) {
                    Err(ForwardError::Transient("socket not ready".into()))
                } else {
                    Ok(())
                }
            }
            fn unexpose(&mut self, _: PortProto, _: u16) -> Result<(), ForwardError> {
                Ok(())
            }
        }

        // Single Add fails transiently twice, then automatically succeeds without another Add.
        let calls = Arc::new(AtomicUsize::new(0));
        let (ev_tx, ev_rx) = mpsc::channel();
        let tx = spawn_forwarder_with(
            FlakyApi {
                fail_times: Arc::new(AtomicUsize::new(2)),
                calls: calls.clone(),
            },
            Duration::from_millis(10),
            move |e| {
                let _ = ev_tx.send(e);
            },
        )
        .unwrap();
        tx.send(PortChange::Add(TCP, 8080)).unwrap();
        assert!(matches!(
            ev_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            Event::Transient(TCP, 8080, _)
        ));
        assert_eq!(
            ev_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            Event::Forwarded(TCP, 8080, vec![Ipv4Addr::LOCALHOST])
        );
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        drop(tx);

        // Remove cancels a pending transient retry so it never forwards later.
        let calls = Arc::new(AtomicUsize::new(0));
        let (ev_tx, ev_rx) = mpsc::channel();
        let tx = spawn_forwarder_with(
            FlakyApi {
                fail_times: Arc::new(AtomicUsize::new(1)),
                calls: calls.clone(),
            },
            Duration::from_millis(50),
            move |e| {
                let _ = ev_tx.send(e);
            },
        )
        .unwrap();
        tx.send(PortChange::Add(TCP, 8081)).unwrap();
        assert!(matches!(
            ev_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            Event::Transient(TCP, 8081, _)
        ));
        tx.send(PortChange::Remove(TCP, 8081)).unwrap();
        assert!(ev_rx.recv_timeout(Duration::from_millis(150)).is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        drop(tx);
    }

    #[test]
    fn forwarder_retries_failed_unexpose_and_cancels_on_re_add() {
        use std::sync::atomic::AtomicUsize;

        struct FlakyUnexposeApi {
            fail_unexpose_times: Arc<AtomicUsize>,
            unexpose_calls: Arc<AtomicUsize>,
        }
        impl ForwardApi for FlakyUnexposeApi {
            fn expose(&mut self, _: PortProto, _: u16) -> Result<(), ForwardError> {
                Ok(())
            }
            fn unexpose(&mut self, _: PortProto, _: u16) -> Result<(), ForwardError> {
                let n = self.unexpose_calls.fetch_add(1, Ordering::SeqCst);
                if n < self.fail_unexpose_times.load(Ordering::SeqCst) {
                    Err(ForwardError::Transient(
                        "gvproxy API: connection reset".into(),
                    ))
                } else {
                    Ok(())
                }
            }
        }

        // Unexpose fails once then automatically retries and succeeds without a second Remove.
        let unexpose_calls = Arc::new(AtomicUsize::new(0));
        let (ev_tx, ev_rx) = mpsc::channel();
        let tx = spawn_forwarder_with(
            FlakyUnexposeApi {
                fail_unexpose_times: Arc::new(AtomicUsize::new(1)),
                unexpose_calls: unexpose_calls.clone(),
            },
            Duration::from_millis(10),
            move |e| {
                let _ = ev_tx.send(e);
            },
        )
        .unwrap();
        tx.send(PortChange::Add(TCP, 9100)).unwrap();
        assert_eq!(
            ev_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            Event::Forwarded(TCP, 9100, vec![Ipv4Addr::LOCALHOST])
        );
        tx.send(PortChange::Remove(TCP, 9100)).unwrap();
        assert!(matches!(
            ev_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            Event::RemoveFailed(TCP, 9100, _)
        ));
        assert_eq!(
            ev_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            Event::Removed(TCP, 9100)
        );
        assert_eq!(unexpose_calls.load(Ordering::SeqCst), 2);
        drop(tx);

        // A new Add cancels a pending unexpose retry while the port is still active.
        let unexpose_calls = Arc::new(AtomicUsize::new(0));
        let (ev_tx, ev_rx) = mpsc::channel();
        let tx = spawn_forwarder_with(
            FlakyUnexposeApi {
                fail_unexpose_times: Arc::new(AtomicUsize::new(1)),
                unexpose_calls: unexpose_calls.clone(),
            },
            Duration::from_millis(50),
            move |e| {
                let _ = ev_tx.send(e);
            },
        )
        .unwrap();
        tx.send(PortChange::Add(TCP, 9101)).unwrap();
        assert_eq!(
            ev_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            Event::Forwarded(TCP, 9101, vec![Ipv4Addr::LOCALHOST])
        );
        tx.send(PortChange::Remove(TCP, 9101)).unwrap();
        assert!(matches!(
            ev_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            Event::RemoveFailed(TCP, 9101, _)
        ));
        tx.send(PortChange::Add(TCP, 9101)).unwrap();
        assert!(ev_rx.recv_timeout(Duration::from_millis(150)).is_err());
        assert_eq!(unexpose_calls.load(Ordering::SeqCst), 1);
        drop(tx);
    }

    #[test]
    fn builds_post_request() {
        let r = http_post("/services/forwarder/expose", "{}");
        assert!(r.starts_with("POST /services/forwarder/expose HTTP/1.1\r\n"));
        assert!(r.contains("\r\nContent-Length: 2\r\n"));
        assert!(r.ends_with("\r\n\r\n{}"));
    }

    #[test]
    fn parses_gvproxy_responses() {
        assert_eq!(
            parse_response("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"),
            Ok(())
        );
        assert_eq!(
            parse_response(
                "HTTP/1.1 500 Internal Server Error\r\nContent-Type: text/plain\r\n\r\nlisten tcp 127.0.0.1:22: bind: permission denied\n"
            ),
            Err(ForwardError::Refused(
                "listen tcp 127.0.0.1:22: bind: permission denied".into()
            ))
        );
        assert_eq!(
            parse_response("HTTP/1.1 404 Not Found\r\n\r\n"),
            Err(ForwardError::Refused("HTTP 404".into()))
        );
        assert!(matches!(
            parse_response(""),
            Err(ForwardError::Transient(_))
        ));
        assert!(matches!(
            parse_response("HTTP/1.1 500"),
            Err(ForwardError::Transient(_))
        ));
        assert!(matches!(
            parse_response("garbage 500\r\n\r\n"),
            Err(ForwardError::Transient(_))
        ));
    }

    #[test]
    fn malformed_gvproxy_response_is_retried_and_succeeds() {
        use std::os::unix::net::UnixListener;

        let dir = std::env::temp_dir().join(format!("kat-gvp-malformed-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let sock = dir.join("gvproxy-api.sock");
        let listener = UnixListener::bind(&sock).unwrap();

        let server = thread::spawn(move || {
            for i in 0..2 {
                let (mut conn, _) = listener.accept().unwrap();
                let mut req = [0u8; 512];
                let _ = conn.read(&mut req);
                let reply = if i == 0 {
                    b"malformed truncated reply".as_slice()
                } else {
                    b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n".as_slice()
                };
                let _ = conn.write_all(reply);
            }
        });

        let (ev_tx, ev_rx) = mpsc::channel();
        let tx = spawn_forwarder_with(
            GvproxyApi {
                socket: sock,
                guest: GUEST,
            },
            Duration::from_millis(10),
            move |e| {
                let _ = ev_tx.send(e);
            },
        )
        .unwrap();
        tx.send(PortChange::Add(TCP, 9200)).unwrap();
        assert!(matches!(
            ev_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            Event::Transient(TCP, 9200, _)
        ));
        assert_eq!(
            ev_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            Event::Forwarded(TCP, 9200, vec![Ipv4Addr::LOCALHOST])
        );
        drop(tx);
        server.join().unwrap();
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn in_process_gvproxy_hands_the_vm_its_command_line() {
        let dir = std::env::temp_dir().join(format!("kat-gvp-inproc-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let dir = fs::canonicalize(&dir).unwrap();
        let library = Path::new("/nonexistent/libgvproxy.dylib");
        let log = dir.join("gvproxy.log");
        let mut gv = Gvproxy::in_process(library, &dir, &log, 1500).unwrap();

        let net = gv.network();
        let p = net.in_process.as_ref().expect("in-process network");
        assert_eq!(p.library, library);
        let args = p.args.join(" ");
        for want in [
            format!(
                "-services unix://{}",
                dir.join("gvproxy-api.sock").display()
            ),
            "-mtu 1500".into(),
            format!("-gatewayIP {GATEWAY} -deviceIP {GUEST} -hostIP {HOST}"),
            format!("-api-token-file {}", dir.join("gvproxy.token").display()),
            format!("-log-file {}", log.display()),
        ] {
            assert!(args.contains(&want), "{want:?} missing from {args}");
        }
        for unwanted in ["-listen-vfkit", "-pid-file"] {
            assert!(!args.contains(unwanted), "{unwanted} in {args}");
        }
        assert!(gv.token.is_file());

        // The VM process served the API socket and could not remove it.
        fs::write(&gv.api_socket, "").unwrap();
        gv.stop();
        assert!(!gv.api_socket.exists() && !gv.token.exists());
        fs::remove_dir_all(&dir).unwrap();
    }

    /// Talks to a real gvproxy if one is built: expose a port, see the Mac
    /// listen on it, unexpose it, see it gone.
    #[test]
    fn gvproxy_forwarder_api_round_trip() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let bin = root.join("target/release/gvproxy");
        if !bin.is_file() {
            eprintln!("skipped: no gvproxy at {}", bin.display());
            return;
        }
        let dir = std::env::temp_dir().join(format!("kat-gvp-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let mut gv = Gvproxy::start(&bin, &dir, &dir.join("gvproxy.log"), 1500).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !gv.network().socket.exists() {
            assert!(Instant::now() < deadline, "gvproxy did not start");
            thread::sleep(Duration::from_millis(5));
        }
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);

        let mut api = GvproxyApi {
            socket: gv.api_socket.clone(),
            guest: GUEST,
        };
        api.expose(TCP, port).unwrap();
        assert!(std::net::TcpListener::bind(("127.0.0.1", port)).is_err());
        let again = api.expose(TCP, port).unwrap_err();
        assert!(again.to_string().contains("already running"), "{again}");
        api.unexpose(TCP, port).unwrap();
        assert!(std::net::TcpListener::bind(("127.0.0.1", port)).is_ok());

        // Privileged port (< 1024): exposes unprivileged on macOS via lo0-scoped wildcard.
        let mut exposed_low = None;
        for candidate in (900u16..=1023).rev() {
            if api.expose(TCP, candidate).is_ok() {
                exposed_low = Some(candidate);
                break;
            }
        }
        let low = exposed_low.expect("gvproxy should expose a free privileged port < 1024");
        assert!(
            LoopbackTcpListener::bind(low).is_err(),
            "duplicate bind on exposed low port {low} should fail"
        );
        api.unexpose(TCP, low).unwrap();
        assert!(LoopbackTcpListener::bind(low).is_ok());

        // Forward addresses beyond 127.0.0.1: gvproxy serves the guest port
        // on a Unix socket and the daemon listens on the Mac's addresses,
        // privileged ports included.
        let gv_dir = gv.api_socket.parent().unwrap().to_path_buf();
        let mut addrs = AddressesApi::new(
            GvproxyApi {
                socket: gv.api_socket.clone(),
                guest: GUEST,
            },
            &gv_dir,
            &[Ipv4Addr::LOCALHOST],
        );
        for p in [port, low] {
            let sock = crate::autofwd::Upstream::socket(&gv_dir, p);
            addrs.expose(TCP, p).unwrap();
            assert!(sock.exists(), "gvproxy did not serve {}", sock.display());
            assert!(
                LoopbackTcpListener::bind(p).is_err(),
                "not listening on {p}"
            );
            addrs.unexpose(TCP, p).unwrap();
            assert!(!sock.exists());
            assert!(
                LoopbackTcpListener::bind(p).is_ok(),
                "still listening on {p}"
            );
        }
        // A port the Mac already uses is refused, and nothing is left behind.
        let busy = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let busy_port = busy.local_addr().unwrap().port();
        assert!(matches!(
            addrs.expose(TCP, busy_port),
            Err(ForwardError::Refused(_))
        ));
        assert!(!crate::autofwd::Upstream::socket(&gv_dir, busy_port).exists());
        assert_eq!(addrs.port_label(TCP, port), "127.0.0.1");

        // Dropping AddressesApi stops active listeners and removes their sockets.
        addrs.expose(TCP, port).unwrap();
        let sock = crate::autofwd::Upstream::socket(&gv_dir, port);
        assert!(sock.exists());
        drop(addrs);
        assert!(!sock.exists());
        assert!(LoopbackTcpListener::bind(port).is_ok());

        gv.stop();
        assert!(!gv.api_socket.exists(), "gvproxy left its socket behind");
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn loopback_tcp_listener_binds_low_port_and_filters_endpoints() {
        let mut bound = None;
        for port in (900u16..=1023).rev() {
            if let Ok(listener) = LoopbackTcpListener::bind(port) {
                bound = Some((port, listener));
                break;
            }
        }
        let (port, listener) = bound.expect("bind a free privileged port < 1024");
        assert_eq!(listener.local_addr().unwrap().port(), port);
        assert!(
            LoopbackTcpListener::bind(port).is_err(),
            "second bind on port {port} should fail"
        );

        let server = thread::spawn(move || {
            let (mut conn, peer) = listener.accept().unwrap();
            assert!(peer.ip().is_loopback());
            let mut buf = [0u8; 4];
            conn.read_exact(&mut buf).unwrap();
            conn.write_all(&buf).unwrap();
        });
        let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();
        client.write_all(b"ping").unwrap();
        let mut reply = [0u8; 4];
        client.read_exact(&mut reply).unwrap();
        assert_eq!(&reply, b"ping");
        server.join().unwrap();

        let lo: SocketAddr = "127.0.0.1:80".parse().unwrap();
        let lo_peer: SocketAddr = "127.0.0.1:50000".parse().unwrap();
        let lan: SocketAddr = "192.168.1.5:80".parse().unwrap();
        let v6: SocketAddr = "[::1]:80".parse().unwrap();
        let only_lo = [Ipv4Addr::LOCALHOST];
        assert!(is_allowed_v4_pair(&only_lo, lo, lo_peer));
        assert!(!is_allowed_v4_pair(&only_lo, lan, lo_peer));
        assert!(!is_allowed_v4_pair(&only_lo, lo, lan));
        assert!(!is_allowed_v4_pair(&only_lo, v6, v6));

        // An lo0 alias: reachable when listed, from itself, loopback, or
        // another lo0 alias, never from a LAN peer.
        let alias: SocketAddr = "172.16.1.200:443".parse().unwrap();
        let alias_peer: SocketAddr = "172.16.1.200:50000".parse().unwrap();
        let other_alias_peer: SocketAddr = "172.16.1.201:50000".parse().unwrap();
        let with_alias = [Ipv4Addr::LOCALHOST, Ipv4Addr::new(172, 16, 1, 200)];
        assert!(!is_allowed_v4_pair(&only_lo, alias, alias_peer));
        assert!(is_allowed_v4_pair(&with_alias, alias, alias_peer));
        assert!(is_allowed_v4_pair(&with_alias, alias, lo_peer));
        assert!(is_allowed_v4_pair(&with_alias, lo, alias_peer));
        assert!(!is_allowed_v4_pair(&with_alias, alias, lan));
        assert!(is_allowed_v4_pair_with(
            &with_alias,
            alias,
            other_alias_peer,
            |ip| ip == Ipv4Addr::new(172, 16, 1, 201),
        ));
        assert!(!is_allowed_v4_pair_with(
            &with_alias,
            alias,
            other_alias_peer,
            |_| false,
        ));
    }

    #[test]
    fn forward_addresses_must_be_on_loopback() {
        assert!(is_loopback_interface_addr(Ipv4Addr::LOCALHOST));
        // TEST-NET-1: assigned to no interface.
        let nowhere = Ipv4Addr::new(192, 0, 2, 1);
        assert!(!is_loopback_interface_addr(nowhere));
        assert_eq!(
            LoopbackTcpListener::bind_on(&[nowhere], 0)
                .err()
                .map(|e| e.kind()),
            Some(io::ErrorKind::AddrNotAvailable)
        );
        // Skipped and deduplicated, while the loopback address still binds.
        let l =
            LoopbackTcpListener::bind_on(&[nowhere, Ipv4Addr::LOCALHOST, Ipv4Addr::LOCALHOST], 0)
                .unwrap();
        assert_eq!(l.raw_fds().len(), 1);
        assert_eq!(l.addrs(), &[Ipv4Addr::LOCALHOST]);
        assert_eq!(l.local_addr().unwrap().ip(), Ipv4Addr::LOCALHOST);
        assert_eq!(addrs_label(&[Ipv4Addr::LOCALHOST]), "127.0.0.1");
        assert_eq!(
            addrs_label(&[Ipv4Addr::LOCALHOST, nowhere]),
            "{127.0.0.1,192.0.2.1}"
        );
    }

    #[test]
    fn dropping_forwarder_cancels_queued_changes_and_retries() {
        struct BlockingApi {
            entered: Sender<u16>,
            release: Arc<Mutex<mpsc::Receiver<()>>>,
            calls: Arc<Mutex<Vec<String>>>,
        }
        impl ForwardApi for BlockingApi {
            fn expose(&mut self, proto: PortProto, port: u16) -> Result<(), ForwardError> {
                self.calls
                    .lock()
                    .unwrap()
                    .push(format!("expose {port}/{}", proto.as_str()));
                if port == 8001 {
                    let _ = self.entered.send(port);
                    let _ = self.release.lock().unwrap().recv();
                }
                Ok(())
            }
            fn unexpose(&mut self, proto: PortProto, port: u16) -> Result<(), ForwardError> {
                self.calls
                    .lock()
                    .unwrap()
                    .push(format!("unexpose {port}/{}", proto.as_str()));
                Ok(())
            }
        }

        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let fwd = spawn_forwarder_with(
            BlockingApi {
                entered: entered_tx,
                release: Arc::new(Mutex::new(release_rx)),
                calls: calls.clone(),
            },
            Duration::from_millis(10),
            |_| {},
        )
        .unwrap();

        fwd.send(PortChange::Add(TCP, 8001)).unwrap();
        assert_eq!(
            entered_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            8001
        );
        // Queue additional operations behind the in-flight call.
        fwd.send(PortChange::Add(TCP, 8002)).unwrap();
        fwd.send(PortChange::Add(TCP, 8003)).unwrap();
        fwd.send(PortChange::Remove(TCP, 8001)).unwrap();

        let shutdown_flag = fwd.shutdown.clone();
        let dropper = thread::spawn(move || drop(fwd));
        let deadline = Instant::now() + Duration::from_secs(2);
        while !shutdown_flag.load(Ordering::SeqCst) {
            assert!(Instant::now() < deadline, "drop did not set shutdown flag");
            thread::sleep(Duration::from_millis(1));
        }
        release_tx.send(()).unwrap();
        dropper.join().unwrap();

        assert_eq!(*calls.lock().unwrap(), vec!["expose 8001/tcp"]);
    }

    struct FakeUnixProxy {
        stop: Arc<AtomicBool>,
        path: PathBuf,
        thread: Option<JoinHandle<()>>,
    }

    impl Drop for FakeUnixProxy {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            let _ = UnixStream::connect(&self.path);
            if let Some(t) = self.thread.take() {
                let _ = t.join();
            }
            let _ = fs::remove_file(&self.path);
        }
    }

    fn accept_unix_until_stop(
        listener: &std::os::unix::net::UnixListener,
        stop: &AtomicBool,
    ) -> Option<UnixStream> {
        loop {
            if stop.load(Ordering::SeqCst) {
                return None;
            }
            let mut pfd = libc::pollfd {
                fd: listener.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            let rc = unsafe { libc::poll(&mut pfd, 1, 20) };
            if rc <= 0 {
                continue;
            }
            match listener.accept() {
                Ok((conn, _)) => {
                    let _ = conn.set_nonblocking(false);
                    return Some(conn);
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                Err(_) => return None,
            }
        }
    }

    /// Minimal fake gvproxy `-services` server implementing `/forwarder/expose`
    /// and `/forwarder/unexpose` for `protocol: "unix"` with echo sockets and
    /// configurable lost-response fault injection.
    struct FakeGvproxy {
        api_sock: PathBuf,
        drop_next_expose_reply: Arc<AtomicBool>,
        drop_next_unexpose_reply: Arc<AtomicBool>,
        fail_next_unexpose_before_remove: Arc<AtomicBool>,
        stop: Arc<AtomicBool>,
        thread: Option<JoinHandle<()>>,
    }

    impl FakeGvproxy {
        fn start(dir: &Path) -> Self {
            use std::os::unix::net::UnixListener;

            let api_sock = dir.join("gvproxy-api.sock");
            let _ = fs::remove_file(&api_sock);
            let listener = UnixListener::bind(&api_sock).unwrap();
            listener.set_nonblocking(true).unwrap();
            let stop = Arc::new(AtomicBool::new(false));
            let drop_next_expose_reply = Arc::new(AtomicBool::new(false));
            let drop_next_unexpose_reply = Arc::new(AtomicBool::new(false));
            let fail_next_unexpose_before_remove = Arc::new(AtomicBool::new(false));

            let (s_stop, s_drop_exp, s_drop_unexp, s_fail_unexp) = (
                stop.clone(),
                drop_next_expose_reply.clone(),
                drop_next_unexpose_reply.clone(),
                fail_next_unexpose_before_remove.clone(),
            );
            let thread = thread::spawn(move || {
                let mut proxies: BTreeMap<PathBuf, FakeUnixProxy> = BTreeMap::new();
                while let Some(mut conn) = accept_unix_until_stop(&listener, &s_stop) {
                    let mut raw = vec![0u8; 4096];
                    let n = conn.read(&mut raw).unwrap_or(0);
                    if n == 0 {
                        continue;
                    }
                    let req = String::from_utf8_lossy(&raw[..n]);
                    let first_line = req.lines().next().unwrap_or("");
                    let path = first_line.split_whitespace().nth(1).unwrap_or("");
                    let body = req.split_once("\r\n\r\n").map_or("", |(_, b)| b);
                    let json: serde_json::Value =
                        serde_json::from_str(body).unwrap_or(serde_json::Value::Null);
                    let local = PathBuf::from(json["local"].as_str().unwrap_or(""));

                    let write_resp = |conn: &mut UnixStream, status: &str, msg: &str| {
                        let r = format!(
                            "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{msg}",
                            msg.len()
                        );
                        let _ = conn.write_all(r.as_bytes());
                    };

                    match path {
                        "/services/forwarder/expose" => {
                            if proxies.contains_key(&local) {
                                write_resp(
                                    &mut conn,
                                    "500 Internal Server Error",
                                    "proxy already running",
                                );
                                continue;
                            }
                            match UnixListener::bind(&local) {
                                Ok(ul) => {
                                    ul.set_nonblocking(true).unwrap();
                                    let p_stop = Arc::new(AtomicBool::new(false));
                                    let p_stop2 = p_stop.clone();
                                    let t = thread::spawn(move || {
                                        while let Some(mut c) =
                                            accept_unix_until_stop(&ul, &p_stop2)
                                        {
                                            let mut data = Vec::new();
                                            let _ = c.read_to_end(&mut data);
                                            let _ = c.write_all(b"gv:");
                                            let _ = c.write_all(&data);
                                            let _ = c.shutdown(std::net::Shutdown::Write);
                                        }
                                    });
                                    proxies.insert(
                                        local.clone(),
                                        FakeUnixProxy {
                                            stop: p_stop,
                                            path: local,
                                            thread: Some(t),
                                        },
                                    );
                                    if s_drop_exp.swap(false, Ordering::SeqCst) {
                                        // Simulate lost HTTP response after socket bound.
                                        continue;
                                    }
                                    write_resp(&mut conn, "200 OK", "");
                                }
                                Err(e) => {
                                    write_resp(
                                        &mut conn,
                                        "500 Internal Server Error",
                                        &format!("listen unix: {e}"),
                                    );
                                }
                            }
                        }
                        "/services/forwarder/unexpose" => {
                            if s_fail_unexp.swap(false, Ordering::SeqCst) {
                                // Drop connection before removing the proxy.
                                continue;
                            }
                            if proxies.remove(&local).is_none() {
                                write_resp(
                                    &mut conn,
                                    "500 Internal Server Error",
                                    "proxy not found",
                                );
                                continue;
                            }
                            if s_drop_unexp.swap(false, Ordering::SeqCst) {
                                // Simulate lost HTTP response after proxy closed.
                                continue;
                            }
                            write_resp(&mut conn, "200 OK", "");
                        }
                        _ => write_resp(&mut conn, "404 Not Found", "not found"),
                    }
                }
                drop(proxies);
            });

            Self {
                api_sock,
                drop_next_expose_reply,
                drop_next_unexpose_reply,
                fail_next_unexpose_before_remove,
                stop,
                thread: Some(thread),
            }
        }
    }

    impl Drop for FakeGvproxy {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            let _ = UnixStream::connect(&self.api_sock);
            if let Some(t) = self.thread.take() {
                let _ = t.join();
            }
            let _ = fs::remove_file(&self.api_sock);
        }
    }

    fn tcp_echo(addr: Ipv4Addr, port: u16, msg: &str) -> String {
        let mut s = TcpStream::connect((addr, port)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        s.write_all(msg.as_bytes()).unwrap();
        s.shutdown(std::net::Shutdown::Write).unwrap();
        let mut out = String::new();
        s.read_to_string(&mut out).unwrap();
        out
    }

    fn free_high_port() -> u16 {
        let probe = TcpListener::bind("127.0.0.1:0").unwrap();
        probe.local_addr().unwrap().port()
    }

    fn free_low_port() -> u16 {
        for candidate in (900u16..=1023).rev() {
            if LoopbackTcpListener::bind(candidate).is_ok() {
                return candidate;
            }
        }
        panic!("could not find a free privileged port in 900..=1023");
    }

    #[test]
    fn addresses_api_reconciles_lost_expose_response_and_failed_rollback() {
        let dir = std::env::temp_dir().join(format!("kat-addr-retry-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let fake = FakeGvproxy::start(&dir);
        let api = AddressesApi::new(
            GvproxyApi {
                socket: fake.api_sock.clone(),
                guest: GUEST,
            },
            &dir,
            &[Ipv4Addr::LOCALHOST],
        );
        // UDP reports 127.0.0.1 even when configured with extra addresses.
        let alias = Ipv4Addr::new(172, 16, 1, 200);
        let udp_check = AddressesApi::new(
            GvproxyApi {
                socket: fake.api_sock.clone(),
                guest: GUEST,
            },
            &dir,
            &[Ipv4Addr::LOCALHOST, alias],
        );
        assert_eq!(
            udp_check.port_addrs(PortProto::Udp, 5353),
            vec![Ipv4Addr::LOCALHOST]
        );
        drop(udp_check);

        let mut fwd = Forwards::new(api);
        let port = free_high_port();
        let sock = crate::autofwd::Upstream::socket(&dir, port);

        // 1. First expose binds the socket in gvproxy, then loses the response.
        fake.drop_next_expose_reply.store(true, Ordering::SeqCst);
        assert!(matches!(
            fwd.add(TCP, port),
            Some(Event::Transient(TCP, p, _)) if p == port
        ));
        assert!(sock.exists(), "live gvproxy socket must exist after bound");

        // Retry must preserve the live socket, reconcile "already running",
        // bind the Mac TCP listener, and relay payloads.
        assert_eq!(
            fwd.add(TCP, port),
            Some(Event::Forwarded(TCP, port, vec![Ipv4Addr::LOCALHOST]))
        );
        assert_eq!(
            tcp_echo(Ipv4Addr::LOCALHOST, port, "retry-ok"),
            "gv:retry-ok"
        );
        assert_eq!(fwd.remove(TCP, port), Some(Event::Removed(TCP, port)));

        // 2. Rollback failure when Mac port is busy and unexpose fails before
        // gvproxy removes the proxy: live socket is preserved and reconciled
        // when the guest removes the refused port and later re-adds it, OR
        // cleaned up promptly via unexpose retries when left removed.
        let busy = TcpListener::bind("127.0.0.1:0").unwrap();
        let busy_port = busy.local_addr().unwrap().port();
        let busy_sock = crate::autofwd::Upstream::socket(&dir, busy_port);
        fake.fail_next_unexpose_before_remove
            .store(true, Ordering::SeqCst);
        assert!(matches!(
            fwd.add(TCP, busy_port),
            Some(Event::Refused(TCP, p, _)) if p == busy_port
        ));
        assert!(
            busy_sock.exists(),
            "failed rollback must not unlink a live gvproxy socket"
        );
        // Removing the refused port without re-adding cleans up the leaked
        // gvproxy Unix socket (even if cleanup itself hits a transient failure
        // and must be retried).
        fake.fail_next_unexpose_before_remove
            .store(true, Ordering::SeqCst);
        assert!(matches!(
            fwd.remove(TCP, busy_port),
            Some(Event::RemoveFailed(TCP, p, _)) if p == busy_port
        ));
        assert!(busy_sock.exists(), "socket still live until retry succeeds");
        assert_eq!(
            fwd.remove(TCP, busy_port),
            Some(Event::RefusedCleared(TCP, busy_port))
        );
        assert!(
            !busy_sock.exists(),
            "removing a refused port after rollback failure must clean up its Unix socket"
        );
        // After freeing the Mac port and re-adding, forwarding works cleanly.
        drop(busy);
        assert_eq!(
            fwd.add(TCP, busy_port),
            Some(Event::Forwarded(TCP, busy_port, vec![Ipv4Addr::LOCALHOST]))
        );
        assert_eq!(
            tcp_echo(Ipv4Addr::LOCALHOST, busy_port, "rollback-ok"),
            "gv:rollback-ok"
        );

        // 3. Verify the worker thread automatically retries upstream cleanup
        // for a refused port left removed without ever re-adding it.
        drop(fwd);
        let busy2 = TcpListener::bind("127.0.0.1:0").unwrap();
        let busy2_port = busy2.local_addr().unwrap().port();
        let busy2_sock = crate::autofwd::Upstream::socket(&dir, busy2_port);
        let (ev_tx, ev_rx) = mpsc::channel();
        let worker = spawn_forwarder_with(
            AddressesApi::new(
                GvproxyApi {
                    socket: fake.api_sock.clone(),
                    guest: GUEST,
                },
                &dir,
                &[Ipv4Addr::LOCALHOST],
            ),
            Duration::from_millis(10),
            move |e| {
                let _ = ev_tx.send(e);
            },
        )
        .unwrap();
        fake.fail_next_unexpose_before_remove
            .store(true, Ordering::SeqCst);
        worker.send(PortChange::Add(TCP, busy2_port)).unwrap();
        assert!(matches!(
            ev_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            Event::Refused(TCP, p, _) if p == busy2_port
        ));
        assert!(busy2_sock.exists());

        fake.fail_next_unexpose_before_remove
            .store(true, Ordering::SeqCst);
        worker.send(PortChange::Remove(TCP, busy2_port)).unwrap();
        assert!(matches!(
            ev_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            Event::RemoveFailed(TCP, p, _) if p == busy2_port
        ));
        assert_eq!(
            ev_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            Event::RefusedCleared(TCP, busy2_port)
        );
        assert!(
            !busy2_sock.exists(),
            "worker retry must remove gvproxy Unix socket left by refused forward"
        );

        drop(busy2);
        drop(worker);
        drop(fake);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn addresses_api_makes_unexpose_idempotent_and_reconciles_readd() {
        let dir = std::env::temp_dir().join(format!("kat-addr-unexp-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let fake = FakeGvproxy::start(&dir);
        let mut fwd = Forwards::new(AddressesApi::new(
            GvproxyApi {
                socket: fake.api_sock.clone(),
                guest: GUEST,
            },
            &dir,
            &[Ipv4Addr::LOCALHOST],
        ));

        // 1. Unexpose closes gvproxy socket, loses response -> retry receives
        // "proxy not found", treats removal as complete, and closes Mac listener.
        let p1 = free_high_port();
        assert_eq!(
            fwd.add(TCP, p1),
            Some(Event::Forwarded(TCP, p1, vec![Ipv4Addr::LOCALHOST]))
        );
        assert_eq!(tcp_echo(Ipv4Addr::LOCALHOST, p1, "before"), "gv:before");
        fake.drop_next_unexpose_reply.store(true, Ordering::SeqCst);
        assert!(matches!(
            fwd.remove(TCP, p1),
            Some(Event::RemoveFailed(TCP, p, _)) if p == p1
        ));
        assert_eq!(fwd.remove(TCP, p1), Some(Event::Removed(TCP, p1)));
        assert!(
            TcpListener::bind(("127.0.0.1", p1)).is_ok(),
            "Mac listener must be closed after idempotent unexpose"
        );

        // 2. Guest immediately re-adds after an ambiguous unexpose whose effect
        // already closed gvproxy's socket: upstream is recreated so the running
        // Mac listener relays again.
        let p2 = free_high_port();
        assert_eq!(
            fwd.add(TCP, p2),
            Some(Event::Forwarded(TCP, p2, vec![Ipv4Addr::LOCALHOST]))
        );
        fake.drop_next_unexpose_reply.store(true, Ordering::SeqCst);
        assert!(matches!(
            fwd.remove(TCP, p2),
            Some(Event::RemoveFailed(TCP, p, _)) if p == p2
        ));
        assert_eq!(fwd.add(TCP, p2), None);
        assert_eq!(tcp_echo(Ipv4Addr::LOCALHOST, p2, "readded"), "gv:readded");

        drop(fwd);
        drop(fake);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn addresses_api_round_trips_regular_and_low_ports_via_fake_gvproxy() {
        let dir = std::env::temp_dir().join(format!("kat-addr-rt-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let fake = FakeGvproxy::start(&dir);
        let mut api = AddressesApi::new(
            GvproxyApi {
                socket: fake.api_sock.clone(),
                guest: GUEST,
            },
            &dir,
            &[Ipv4Addr::LOCALHOST],
        );

        for port in [free_high_port(), free_low_port()] {
            api.expose(TCP, port).unwrap();
            assert_eq!(api.port_addrs(TCP, port), vec![Ipv4Addr::LOCALHOST]);
            assert_eq!(
                tcp_echo(Ipv4Addr::LOCALHOST, port, &format!("p{port}")),
                format!("gv:p{port}")
            );
            api.unexpose(TCP, port).unwrap();
            assert!(LoopbackTcpListener::bind(port).is_ok());
        }

        drop(api);
        drop(fake);
        let _ = fs::remove_dir_all(&dir);
    }

    #[cfg(target_os = "macos")]
    fn bound_if_index(fd: RawFd) -> libc::c_uint {
        let mut idx: libc::c_uint = 0;
        let mut len = std::mem::size_of::<libc::c_uint>() as libc::socklen_t;
        let rc = unsafe {
            libc::getsockopt(
                fd,
                libc::IPPROTO_IP,
                IP_BOUND_IF,
                (&mut idx as *mut libc::c_uint).cast(),
                &mut len,
            )
        };
        assert_eq!(
            rc,
            0,
            "getsockopt(IP_BOUND_IF): {}",
            io::Error::last_os_error()
        );
        idx
    }

    /// Find an active non-loopback IPv4 address on this Mac (e.g. `en0`) to test
    /// non-loopback peer rejection against filtered loopback listeners.
    fn first_non_loopback_v4() -> Option<Ipv4Addr> {
        let mut head: *mut libc::ifaddrs = std::ptr::null_mut();
        if unsafe { libc::getifaddrs(&mut head) } != 0 {
            return None;
        }
        let mut found = None;
        let mut cur = head;
        while !cur.is_null() {
            let ifa = unsafe { &*cur };
            cur = ifa.ifa_next;
            if ifa.ifa_addr.is_null()
                || ifa.ifa_flags & libc::IFF_LOOPBACK as libc::c_uint != 0
                || ifa.ifa_flags & libc::IFF_UP as libc::c_uint == 0
                || i32::from(unsafe { (*ifa.ifa_addr).sa_family }) != libc::AF_INET
            {
                continue;
            }
            let sin = unsafe { &*ifa.ifa_addr.cast::<libc::sockaddr_in>() };
            let ip = Ipv4Addr::from(u32::from_be(sin.sin_addr.s_addr));
            if !ip.is_unspecified() && !ip.is_loopback() && !ip.is_link_local() {
                found = Some(ip);
                break;
            }
        }
        unsafe { libc::freeifaddrs(head) };
        found
    }

    /// Connect from a specific local source IPv4 to `(dst, port)` and confirm
    /// the listener does not echo data (either connect fails or peer is dropped).
    fn assert_non_loopback_source_rejected(src: Ipv4Addr, dst: Ipv4Addr, port: u16) {
        let raw = unsafe { libc::socket(libc::AF_INET, libc::SOCK_STREAM, 0) };
        assert!(raw >= 0, "socket: {}", io::Error::last_os_error());
        let fd = unsafe { OwnedFd::from_raw_fd(raw) };
        let local = libc::sockaddr_in {
            #[cfg(target_os = "macos")]
            sin_len: std::mem::size_of::<libc::sockaddr_in>() as u8,
            sin_family: libc::AF_INET as libc::sa_family_t,
            sin_port: 0,
            sin_addr: libc::in_addr {
                s_addr: u32::from(src).to_be(),
            },
            sin_zero: [0; 8],
        };
        if unsafe {
            libc::bind(
                fd.as_raw_fd(),
                (&local as *const libc::sockaddr_in).cast(),
                std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
            )
        } < 0
        {
            return;
        }
        let remote = libc::sockaddr_in {
            #[cfg(target_os = "macos")]
            sin_len: std::mem::size_of::<libc::sockaddr_in>() as u8,
            sin_family: libc::AF_INET as libc::sa_family_t,
            sin_port: port.to_be(),
            sin_addr: libc::in_addr {
                s_addr: u32::from(dst).to_be(),
            },
            sin_zero: [0; 8],
        };
        if unsafe {
            libc::connect(
                fd.as_raw_fd(),
                (&remote as *const libc::sockaddr_in).cast(),
                std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
            )
        } < 0
        {
            // Blocked at connect time.
            return;
        }
        let mut s = TcpStream::from(fd);
        s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let _ = s.write_all(b"lan-probe");
        let _ = s.shutdown(std::net::Shutdown::Write);
        let mut resp = Vec::new();
        let _ = s.read_to_end(&mut resp);
        assert!(
            resp.is_empty(),
            "non-loopback source {src} unexpectedly received {resp:?} from {dst}:{port}"
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn specific_alias_socket_scopes_to_lo0_and_rejects_non_loopback_peer() {
        let lo0 = unsafe { libc::if_nametoindex(c"lo0".as_ptr()) };
        assert_ne!(lo0, 0);

        // Default localhost path keeps the plain TcpListener without filtering.
        let plain = LoopbackTcpListener::bind_on(&[Ipv4Addr::LOCALHOST], 0).unwrap();
        assert_eq!(plain.filter_endpoints, vec![false]);
        assert_eq!(bound_if_index(plain.raw_fds()[0]), 0);

        // Specific-address alias scope: IP_BOUND_IF == lo0 + local peer filter.
        let scoped = LoopbackTcpListener::bind_on_inner(&[Ipv4Addr::LOCALHOST], 0, true).unwrap();
        assert_eq!(scoped.filter_endpoints, vec![true]);
        assert_eq!(bound_if_index(scoped.raw_fds()[0]), lo0);
        let port = scoped.local_addr().unwrap().port();

        let server = thread::spawn(move || {
            let (mut conn, peer) = scoped.accept().unwrap();
            assert!(
                peer.ip().is_loopback(),
                "unexpected non-loopback peer {peer}"
            );
            let mut buf = [0u8; 5];
            conn.read_exact(&mut buf).unwrap();
            conn.write_all(&buf).unwrap();
        });

        if let Some(non_lo) = first_non_loopback_v4() {
            assert_non_loopback_source_rejected(non_lo, Ipv4Addr::LOCALHOST, port);
        }

        let mut client = TcpStream::connect((Ipv4Addr::LOCALHOST, port)).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        client.write_all(b"lo-ok").unwrap();
        let mut reply = [0u8; 5];
        client.read_exact(&mut reply).unwrap();
        assert_eq!(&reply, b"lo-ok");
        server.join().unwrap();
    }

    /// A real `lo0` alias for integration testing: uses `KATAMARAN_TEST_LO0_ALIAS`,
    /// an already-assigned non-`127.0.0.1` IPv4 on `lo0`, or (when running as root)
    /// provisions and removes a temporary alias via `ifconfig lo0`.
    struct TestLo0Alias {
        addr: Ipv4Addr,
        provisioned: bool,
    }

    impl TestLo0Alias {
        fn acquire() -> Option<Self> {
            if let Ok(val) = std::env::var("KATAMARAN_TEST_LO0_ALIAS") {
                let addr: Ipv4Addr = val
                    .trim()
                    .parse()
                    .expect("KATAMARAN_TEST_LO0_ALIAS must be an IPv4 address");
                assert!(
                    addr != Ipv4Addr::LOCALHOST && is_loopback_interface_addr(addr),
                    "KATAMARAN_TEST_LO0_ALIAS={addr} must be an alias assigned to lo0"
                );
                return Some(Self {
                    addr,
                    provisioned: false,
                });
            }
            // Discover any existing non-127.0.0.1 IPv4 alias on lo0.
            let mut head: *mut libc::ifaddrs = std::ptr::null_mut();
            if unsafe { libc::getifaddrs(&mut head) } == 0 {
                let mut found = None;
                let mut cur = head;
                while !cur.is_null() {
                    let ifa = unsafe { &*cur };
                    cur = ifa.ifa_next;
                    if ifa.ifa_addr.is_null()
                        || ifa.ifa_flags & libc::IFF_LOOPBACK as libc::c_uint == 0
                        || i32::from(unsafe { (*ifa.ifa_addr).sa_family }) != libc::AF_INET
                    {
                        continue;
                    }
                    let sin = unsafe { &*ifa.ifa_addr.cast::<libc::sockaddr_in>() };
                    let ip = Ipv4Addr::from(u32::from_be(sin.sin_addr.s_addr));
                    if ip != Ipv4Addr::LOCALHOST {
                        found = Some(ip);
                        break;
                    }
                }
                unsafe { libc::freeifaddrs(head) };
                if let Some(addr) = found {
                    return Some(Self {
                        addr,
                        provisioned: false,
                    });
                }
            }
            // When running as root, provision a temporary alias and remove it on Drop.
            if unsafe { libc::geteuid() } == 0 {
                let candidate = Ipv4Addr::new(172, 16, 254, 200);
                let ok = Command::new("/sbin/ifconfig")
                    .args(["lo0", "alias", &candidate.to_string()])
                    .status()
                    .is_ok_and(|s| s.success());
                if ok && is_loopback_interface_addr(candidate) {
                    return Some(Self {
                        addr: candidate,
                        provisioned: true,
                    });
                }
            }
            None
        }
    }

    impl Drop for TestLo0Alias {
        fn drop(&mut self) {
            if self.provisioned {
                let _ = Command::new("/sbin/ifconfig")
                    .args(["lo0", "-alias", &self.addr.to_string()])
                    .status();
            }
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn alias_forwarding_round_trip_on_regular_and_low_ports() {
        let Some(alias) = TestLo0Alias::acquire() else {
            eprintln!(
                "skipped: no lo0 alias configured (set KATAMARAN_TEST_LO0_ALIAS or run \
                 `sudo ifconfig lo0 alias 172.16.1.200`)"
            );
            return;
        };
        let lo0 = unsafe { libc::if_nametoindex(c"lo0".as_ptr()) };
        let configured = [Ipv4Addr::LOCALHOST, alias.addr];

        // Verify socket scoping directly on a real specific-address alias bind.
        let direct = LoopbackTcpListener::bind_on(&configured, 0).unwrap();
        assert_eq!(direct.addrs(), &configured);
        assert_eq!(direct.raw_fds().len(), 2);
        assert_eq!(direct.filter_endpoints, vec![false, true]);
        assert_eq!(bound_if_index(direct.raw_fds()[1]), lo0);
        drop(direct);

        // End-to-end AddressesApi payload round trips on both configured
        // addresses for regular (>=1024) and privileged (<1024) ports.
        let dir = std::env::temp_dir().join(format!("kat-alias-rt-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let fake = FakeGvproxy::start(&dir);
        let mut api = AddressesApi::new(
            GvproxyApi {
                socket: fake.api_sock.clone(),
                guest: GUEST,
            },
            &dir,
            &configured,
        );

        let non_lo = first_non_loopback_v4();
        for port in [free_high_port(), free_low_port()] {
            api.expose(TCP, port).unwrap();
            assert_eq!(api.port_addrs(TCP, port), configured.to_vec());
            assert_eq!(
                tcp_echo(Ipv4Addr::LOCALHOST, port, &format!("lo-{port}")),
                format!("gv:lo-{port}")
            );
            assert_eq!(
                tcp_echo(alias.addr, port, &format!("alias-{port}")),
                format!("gv:alias-{port}")
            );
            if let Some(src) = non_lo {
                assert_non_loopback_source_rejected(src, alias.addr, port);
            }
            api.unexpose(TCP, port).unwrap();
        }

        drop(api);
        drop(fake);
        let _ = fs::remove_dir_all(&dir);
    }
}
