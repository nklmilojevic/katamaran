//! Host-side Kubernetes (k3s) manager:
//! - downloads and SHA-256 verifies the `k3s-arm64` binary (the pinned build,
//!   or the `k8s_version` config key) into `<home>/cache/k3s/<version>/k3s-arm64`,
//! - streams requests to the guest's k8s service over `<home>/run/k8s.sock`,
//! - relays `127.0.0.1:<api_port>` to `<home>/run/k8s-api.sock` (`K8S_API_PORT`),
//! - merges/removes the `katamaran` context in `~/.kube/config`,
//! - serves a local UDP DNS responder for `*.k8s.kat.local` -> `127.0.0.1`,
//! - and dials arbitrary Pod/Service IPs inside the guest via `K8sMsg::Connect`.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::net::{Ipv4Addr, Shutdown, SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use anyhow::{Context, bail};
use kat_proto::k8s::{self, CHUNK, K8sInfo, K8sMsg};

const MAX_CONNS: usize = 256;

struct ConnGuard(Arc<AtomicUsize>);

impl ConnGuard {
    fn try_acquire(active: &Arc<AtomicUsize>, limit: usize) -> Option<Self> {
        active
            .try_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < limit).then_some(n + 1)
            })
            .ok()
            .map(|_| ConnGuard(Arc::clone(active)))
    }
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

pub const K3S_VERSION: &str = "v1.37.2+k3s1";
pub const K3S_ARM64_SHA256: &str =
    "a779ccf1e5e2e33a62f5a29c2f1c0bbf99431e100579d4c10f0aa0742a6aa84b";
const K3S_RELEASES: &str = "https://github.com/k3s-io/k3s/releases/download";
/// Release asset listing the SHA-256 of every arm64 artifact.
const K3S_ARM64_SUMS: &str = "sha256sum-arm64.txt";
const K3S_ARM64: &str = "k3s-arm64";

pub const SOCKET_NAME: &str = "k8s.sock";
pub const API_SOCKET_NAME: &str = "k8s-api.sock";
pub const KUBECONFIG_NAME: &str = "kubeconfig.yaml";
pub const PROXY_SOCKET_NAME: &str = "k8s-proxy.sock";
pub const K3S_CONFIG_NAME: &str = "k3s-config.yaml";
pub const MAX_K3S_CONFIG_BYTES: usize = 256 << 10;
pub const DEFAULT_API_PORT: u16 = 6443;

pub const DNS_DOMAIN: &str = "k8s.kat.local";
pub const DEFAULT_DNS_PORT: u16 = 15353;
pub const RESOLVER_PATH: &str = "/etc/resolver/k8s.kat.local";
/// When set to `1` in the daemon's environment, the `*.k8s.kat.local` DNS responder
/// binds an ephemeral loopback port instead of the port named by an installed
/// [`RESOLVER_PATH`]. Integration tests set it so their isolated daemons do not
/// fail on the port held by the developer's real daemon.
pub const ISOLATED_DNS_ENV: &str = "KATAMARAN_ISOLATED_DNS";

pub fn socket_path(home: &Path) -> PathBuf {
    home.join("run").join(SOCKET_NAME)
}

pub fn api_socket_path(home: &Path) -> PathBuf {
    home.join("run").join(API_SOCKET_NAME)
}

/// Plain-HTTP Kubernetes API (the guest's `kubectl proxy`) for the app.
pub fn proxy_socket_path(home: &Path) -> PathBuf {
    home.join("run").join(PROXY_SOCKET_NAME)
}

pub fn kubeconfig_path(home: &Path) -> PathBuf {
    home.join("run").join(KUBECONFIG_NAME)
}

pub fn k3s_config_path(home: &Path) -> PathBuf {
    home.join(K3S_CONFIG_NAME)
}

/// Read `<home>/k3s-config.yaml` if present.
pub fn read_k3s_config(home: &Path) -> anyhow::Result<Option<String>> {
    let path = k3s_config_path(home);
    let f = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(&path)
    {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
    };
    let meta = f
        .metadata()
        .with_context(|| format!("stat {}", path.display()))?;
    if !meta.file_type().is_file() {
        bail!("{} must be a regular file", path.display());
    }
    let mut bytes = Vec::new();
    f.take((MAX_K3S_CONFIG_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .with_context(|| format!("read {}", path.display()))?;
    if bytes.len() > MAX_K3S_CONFIG_BYTES {
        bail!(
            "{} exceeds maximum size of {MAX_K3S_CONFIG_BYTES} bytes",
            path.display()
        );
    }
    let yaml = String::from_utf8(bytes)
        .with_context(|| format!("{} must be valid UTF-8", path.display()))?;
    Ok(Some(yaml))
}

/// Build the guest `K8sMsg::Start` request from `config.toml` and optional `<home>/k3s-config.yaml`.
pub fn build_start_msg(home: &Path) -> anyhow::Result<K8sMsg> {
    let cfg = crate::config::Config::load(home)?;
    let extra_args = cfg.k8s_server_extra_args();
    let config_yaml = read_k3s_config(home)?;
    Ok(K8sMsg::Start {
        extra_args,
        config_yaml,
    })
}

pub fn cache_binary_path(home: &Path, version: &str) -> PathBuf {
    home.join("cache").join("k3s").join(version).join(K3S_ARM64)
}

/// The release asset `name` of k3s `version` (`+` is `%2B` in the tag).
fn release_url(version: &str, name: &str) -> String {
    format!("{K3S_RELEASES}/{}/{name}", version.replace('+', "%2B"))
}

/// The SHA-256 of `k3s-arm64` from a release's `sha256sum-arm64.txt`.
fn parse_arm64_sha256(sums: &str) -> anyhow::Result<String> {
    sums.lines()
        .find_map(|line| {
            let mut it = line.split_whitespace();
            let (hash, name) = (it.next()?, it.next()?);
            (name.trim_start_matches('*') == K3S_ARM64
                && hash.len() == 64
                && hash.bytes().all(|b| b.is_ascii_hexdigit()))
            .then(|| hash.to_ascii_lowercase())
        })
        .with_context(|| format!("{K3S_ARM64_SUMS} has no {K3S_ARM64} checksum"))
}

pub fn context_name() -> anyhow::Result<String> {
    Ok(crate::setup::Identity::resolve(None)?.context())
}

pub fn connect(home: &Path) -> anyhow::Result<UnixStream> {
    let path = socket_path(home);
    UnixStream::connect(&path).with_context(|| {
        format!(
            "the VM is not running ({}); start it with `katamaran start`",
            path.display()
        )
    })
}

pub fn request(
    home: &Path,
    req: &K8sMsg,
    progress: &mut dyn FnMut(&str),
) -> anyhow::Result<K8sMsg> {
    let mut s = connect(home)?;
    request_stream(&mut s, req, progress)
}

pub fn request_stream(
    s: &mut UnixStream,
    req: &K8sMsg,
    progress: &mut dyn FnMut(&str),
) -> anyhow::Result<K8sMsg> {
    k8s::write(&mut *s, req)?;
    read_answer(s, progress)
}

pub fn read_answer(s: &mut UnixStream, progress: &mut dyn FnMut(&str)) -> anyhow::Result<K8sMsg> {
    loop {
        match k8s::read(&mut *s)? {
            Some(K8sMsg::Progress(p)) => progress(&p),
            Some(K8sMsg::Error(e)) => bail!("{e}"),
            Some(m) => return Ok(m),
            None => bail!("the guest closed the Kubernetes connection"),
        }
    }
}

pub fn guest_status(home: &Path) -> anyhow::Result<K8sInfo> {
    let mut noop = |_: &str| {};
    match request(home, &K8sMsg::Status, &mut noop)? {
        K8sMsg::StatusInfo(info) => Ok(info),
        other => bail!("unexpected status response {other:?}"),
    }
}

/// Query the guest Kubernetes service's capability bitmask (see `kat_proto::k8s::CAP_*`).
/// Older guests reject `Capabilities` by closing the connection; when the guest
/// still responds to `Status`, this returns `Ok(0)`.
pub fn guest_capabilities(home: &Path) -> anyhow::Result<u32> {
    let mut s = connect(home)?;
    if let Err(e) = k8s::write(&mut s, &K8sMsg::Capabilities) {
        if guest_status(home).is_ok() {
            return Ok(0);
        }
        return Err(e.into());
    }
    match k8s::read(&mut s) {
        Ok(Some(K8sMsg::CapabilitiesInfo(caps))) => Ok(caps),
        Ok(Some(K8sMsg::Error(e))) => {
            if guest_status(home).is_ok() {
                return Ok(0);
            }
            bail!("{e}");
        }
        Ok(None) => {
            if guest_status(home).is_ok() {
                return Ok(0);
            }
            bail!("the guest closed the Kubernetes connection");
        }
        Ok(Some(other)) => bail!("unexpected capabilities response {other:?}"),
        Err(e) => {
            if matches!(
                e.kind(),
                io::ErrorKind::ConnectionReset
                    | io::ErrorKind::BrokenPipe
                    | io::ErrorKind::UnexpectedEof
            ) && guest_status(home).is_ok()
            {
                return Ok(0);
            }
            Err(e.into())
        }
    }
}

/// Check that the running guest supports extended `Start` payloads before
/// sending non-empty `extra_args` or `config_yaml`. Empty `Start` frames stay
/// compatible with older guests without probing.
pub fn ensure_start_supported(home: &Path, msg: &K8sMsg) -> anyhow::Result<()> {
    if !msg.is_extended_start() {
        return Ok(());
    }
    let caps = guest_capabilities(home)?;
    if caps & k8s::CAP_START_CONFIG == 0 {
        bail!(
            "the running guest initramfs does not support custom k8s_disable, k8s_args, or \
             k3s-config.yaml; update or rebuild the guest initramfs (or clear those settings) \
             and restart the VM"
        );
    }
    Ok(())
}

/// Download `k3s-arm64` into `<home>/cache/k3s/<version>/k3s-arm64` if not
/// already cached and verified, then stream it into the guest if the guest
/// doesn't already have this version installed.
pub fn ensure_installed(home: &Path, progress: &mut dyn FnMut(&str)) -> anyhow::Result<()> {
    let version = crate::config::Config::load(home)?.k3s_version().to_string();
    let info = guest_status(home)?;
    if info.installed && info.version == version {
        return Ok(());
    }
    let cached = ensure_cached_binary(home, &version, progress)?;
    progress(&format!("transferring k3s {version} to VM"));
    let mut s = connect(home)?;
    k8s::write(
        &mut s,
        &K8sMsg::Install {
            version: version.clone(),
        },
    )?;
    let mut f = File::open(&cached).with_context(|| format!("open {}", cached.display()))?;
    let mut buf = vec![0u8; CHUNK];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        if let Err(e) = k8s::write(&mut s, &K8sMsg::Data(buf[..n].to_vec())) {
            let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
            return match read_answer(&mut s, progress) {
                Err(guest) => Err(guest),
                Ok(_) => Err(e.into()),
            };
        }
    }
    if let Err(e) = k8s::write(&mut s, &K8sMsg::DataEnd) {
        let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
        return match read_answer(&mut s, progress) {
            Err(guest) => Err(guest),
            Ok(_) => Err(e.into()),
        };
    }
    match read_answer(&mut s, progress)? {
        K8sMsg::Done => Ok(()),
        other => bail!("unexpected install response {other:?}"),
    }
}

/// The pinned build is checked against [`K3S_ARM64_SHA256`]; any other
/// release against its own `sha256sum-arm64.txt`, whose `k3s-arm64` line is
/// kept next to the cached binary so a cache hit needs no network.
fn ensure_cached_binary(
    home: &Path,
    version: &str,
    progress: &mut dyn FnMut(&str),
) -> anyhow::Result<PathBuf> {
    ensure_cached_binary_with(
        home,
        version,
        progress,
        crate::machines::curl_text,
        crate::machines::curl,
    )
}

fn ensure_cached_binary_with<FText, FBin>(
    home: &Path,
    version: &str,
    progress: &mut dyn FnMut(&str),
    mut fetch_text: FText,
    mut fetch_bin: FBin,
) -> anyhow::Result<PathBuf>
where
    FText: FnMut(&str) -> anyhow::Result<String>,
    FBin: FnMut(&str, &Path) -> anyhow::Result<()>,
{
    let path = cache_binary_path(home, version);
    let parent = path
        .parent()
        .with_context(|| format!("missing parent for {}", path.display()))?;
    let sum_path = parent.join(format!("{K3S_ARM64}.sha256"));
    let pinned = version == K3S_VERSION;
    let known = if pinned {
        Some(K3S_ARM64_SHA256.to_string())
    } else {
        fs::read_to_string(&sum_path)
            .ok()
            .and_then(|s| parse_arm64_sha256(&s).ok())
    };
    if let Some(sum) = &known
        && path.is_file()
        && verify_sha256(&path, sum).is_ok()
    {
        return Ok(path);
    }
    fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    let expected = if pinned {
        K3S_ARM64_SHA256.to_string()
    } else {
        progress(&format!("fetching k3s {version} checksums"));
        let url = release_url(version, K3S_ARM64_SUMS);
        let sums =
            fetch_text(&url).with_context(|| format!("fetch k3s {version} arm64 checksums"))?;
        parse_arm64_sha256(&sums).with_context(|| format!("read {url}"))?
    };
    // The checksum sidecar is best effort; a cached binary that matches the
    // freshly fetched checksum is reused instead of downloading it again.
    if !pinned && path.is_file() && verify_sha256(&path, &expected).is_ok() {
        write_sum_sidecar(parent, &sum_path, &expected);
        return Ok(path);
    }
    let pid = std::process::id();
    let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let tmp = parent.join(format!("{K3S_ARM64}.{pid}.{seq}.part"));
    let _ = fs::remove_file(&tmp);
    progress(&format!("downloading k3s {version} (~74 MB)"));
    if let Err(e) = fetch_bin(&release_url(version, K3S_ARM64), &tmp) {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    progress("verifying k3s checksum");
    if let Err(e) = verify_sha256(&tmp, &expected) {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    if let Err(e) = File::open(&tmp).and_then(|f| f.sync_all()) {
        let _ = fs::remove_file(&tmp);
        return Err(e).with_context(|| format!("sync {}", tmp.display()));
    }
    if let Err(e) = fs::rename(&tmp, &path) {
        let _ = fs::remove_file(&tmp);
        return Err(e).with_context(|| format!("save {}", path.display()));
    }
    if !pinned {
        write_sum_sidecar(parent, &sum_path, &expected);
    }
    Ok(path)
}

/// Best effort: without the sidecar the next start fetches the checksums again
/// (but still reuses a cached binary that matches them).
fn write_sum_sidecar(parent: &Path, sum_path: &Path, expected: &str) {
    let pid = std::process::id();
    let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let sum_tmp = parent.join(format!("{K3S_ARM64}.sha256.{pid}.{seq}.part"));
    let line = format!("{expected}  {K3S_ARM64}\n");
    if fs::write(&sum_tmp, line)
        .and_then(|()| fs::rename(&sum_tmp, sum_path))
        .is_err()
    {
        let _ = fs::remove_file(&sum_tmp);
    }
}

fn verify_sha256(path: &Path, expected: &str) -> anyhow::Result<()> {
    let actual = crate::machines::sha256_file(path)?;
    if actual != expected {
        bail!(
            "checksum mismatch for {}: got {actual}, want {expected}",
            path.display()
        );
    }
    Ok(())
}

/// Relays `127.0.0.1:<port>` on macOS to `<home>/run/k8s-api.sock`, and
/// `<home>/run/k8s-proxy.sock` to the guest's `kubectl proxy`.
pub struct ApiProxy {
    pub port: u16,
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
    rest: Option<RestProxy>,
}

impl ApiProxy {
    pub fn start(home: &Path) -> io::Result<ApiProxy> {
        let rest = RestProxy::start(home)?;
        let api_socket = api_socket_path(home);
        let listener = TcpListener::bind(("127.0.0.1", DEFAULT_API_PORT))
            .or_else(|_| TcpListener::bind(("127.0.0.1", 0)))?;
        let addr = listener.local_addr()?;
        let port = addr.port();
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let active = Arc::new(AtomicUsize::new(0));
        let thread = thread::Builder::new()
            .name(format!("k8s-api-{port}"))
            .spawn(move || {
                for conn in listener.incoming() {
                    if flag.load(Ordering::SeqCst) {
                        return;
                    }
                    let client = match conn {
                        Ok(c) => c,
                        Err(e) => {
                            eprintln!("katamaran: k8s api accept: {e}");
                            thread::sleep(Duration::from_millis(50));
                            continue;
                        }
                    };
                    let Some(guard) = ConnGuard::try_acquire(&active, MAX_CONNS) else {
                        eprintln!("katamaran: k8s api: connection limit ({MAX_CONNS}) reached");
                        continue;
                    };
                    let sock = api_socket.clone();
                    if let Err(e) =
                        thread::Builder::new()
                            .name("k8s-api-conn".into())
                            .spawn(move || {
                                let _guard = guard;
                                let _ = relay_tcp_to_unix(client, &sock);
                            })
                    {
                        eprintln!("katamaran: k8s api spawn worker: {e}");
                    }
                }
            })?;
        Ok(ApiProxy {
            port,
            addr,
            stop,
            thread: Some(thread),
            rest: Some(rest),
        })
    }

    pub fn rest_socket(&self) -> Option<&Path> {
        self.rest.as_ref().map(|r| r.path.as_path())
    }

    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect_timeout(&self.addr, Duration::from_millis(200));
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
        self.rest = None;
    }
}

/// Serves `<home>/run/k8s-proxy.sock`, each connection relayed to the
/// guest's root-only `kubectl proxy` Unix socket via `K8sMsg::Proxy`.
struct RestProxy {
    path: PathBuf,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl RestProxy {
    fn start(home: &Path) -> io::Result<RestProxy> {
        use std::os::unix::fs::PermissionsExt;
        use std::os::unix::net::UnixListener;

        let path = proxy_socket_path(home);
        let _ = fs::remove_file(&path);
        let listener = UnixListener::bind(&path)?;
        if let Err(e) = fs::set_permissions(&path, fs::Permissions::from_mode(0o600)) {
            let _ = fs::remove_file(&path);
            return Err(e);
        }
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let home = home.to_path_buf();
        let active = Arc::new(AtomicUsize::new(0));
        let thread = match thread::Builder::new()
            .name("k8s-rest".into())
            .spawn(move || {
                for conn in listener.incoming() {
                    if flag.load(Ordering::SeqCst) {
                        return;
                    }
                    let client = match conn {
                        Ok(c) => c,
                        Err(e) => {
                            eprintln!("katamaran: k8s rest accept: {e}");
                            thread::sleep(Duration::from_millis(50));
                            continue;
                        }
                    };
                    let Some(guard) = ConnGuard::try_acquire(&active, MAX_CONNS) else {
                        eprintln!("katamaran: k8s rest: connection limit ({MAX_CONNS}) reached");
                        continue;
                    };
                    let home = home.clone();
                    if let Err(e) =
                        thread::Builder::new()
                            .name("k8s-rest-conn".into())
                            .spawn(move || {
                                let _guard = guard;
                                if let Err(e) = relay_k8s_proxy(&home, client) {
                                    eprintln!("katamaran: k8s rest: {e:#}");
                                }
                            })
                    {
                        eprintln!("katamaran: k8s rest spawn worker: {e}");
                    }
                }
            }) {
            Ok(t) => t,
            Err(e) => {
                let _ = fs::remove_file(&path);
                return Err(e);
            }
        };
        Ok(RestProxy {
            path,
            stop,
            thread: Some(thread),
        })
    }
}

impl Drop for RestProxy {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = UnixStream::connect(&self.path);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
        let _ = fs::remove_file(&self.path);
    }
}

/// A byte stream the k8s relays can split into a reader and a writer.
pub trait Duplex: Read + Write + Send + Sized + 'static {
    fn try_clone(&self) -> io::Result<Self>;
    fn shutdown(&self, how: Shutdown) -> io::Result<()>;
}

impl Duplex for TcpStream {
    fn try_clone(&self) -> io::Result<Self> {
        TcpStream::try_clone(self)
    }
    fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        TcpStream::shutdown(self, how)
    }
}

impl Duplex for UnixStream {
    fn try_clone(&self) -> io::Result<Self> {
        UnixStream::try_clone(self)
    }
    fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        UnixStream::shutdown(self, how)
    }
}

impl Drop for ApiProxy {
    fn drop(&mut self) {
        self.stop();
    }
}

fn relay_tcp_to_unix(client: TcpStream, sock: &Path) -> io::Result<()> {
    let _ = client.set_nodelay(true);
    let upstream = UnixStream::connect(sock)?;
    let (mut c_read, mut u_write) = (client.try_clone()?, upstream.try_clone()?);
    let up = thread::Builder::new()
        .name("k8s-api-up".into())
        .spawn(move || {
            let _ = io::copy(&mut c_read, &mut u_write);
            let _ = u_write.shutdown(Shutdown::Write);
        })?;
    let (mut u_read, mut c_write) = (upstream, client);
    let _ = io::copy(&mut u_read, &mut c_write);
    let _ = c_write.shutdown(Shutdown::Write);
    let _ = up.join();
    Ok(())
}

/// Forward a local TCP port on macOS directly to `guest_addr:guest_port`
/// (e.g. a Pod IP `10.42.x.x` or ClusterIP Service `10.43.x.x`) over vsock.
pub fn forward_guest_tcp(
    home: &Path,
    guest_addr: String,
    guest_port: u16,
    local_port: u16,
) -> anyhow::Result<()> {
    let listener = crate::net::LoopbackTcpListener::bind(local_port)
        .with_context(|| format!("bind 127.0.0.1:{local_port}"))?;
    let bound = listener.local_addr()?.port();
    println!("Forwarding 127.0.0.1:{bound} -> {guest_addr}:{guest_port} (Ctrl-C to stop)");
    let active = Arc::new(AtomicUsize::new(0));
    loop {
        let client = match listener.accept() {
            Ok((c, _)) => c,
            Err(e) => {
                eprintln!("katamaran: k8s forward accept: {e}");
                thread::sleep(Duration::from_millis(50));
                continue;
            }
        };
        let Some(guard) = ConnGuard::try_acquire(&active, MAX_CONNS) else {
            eprintln!("katamaran: k8s forward: connection limit ({MAX_CONNS}) reached");
            continue;
        };
        let home = home.to_path_buf();
        let addr = guest_addr.clone();
        if let Err(e) = thread::Builder::new()
            .name("k8s-fwd-conn".into())
            .spawn(move || {
                let _guard = guard;
                let _ = client.set_nodelay(true);
                if let Err(e) = relay_k8s_connect(&home, client, &addr, guest_port) {
                    eprintln!("katamaran: k8s forward {addr}:{guest_port}: {e}");
                }
            })
        {
            eprintln!("katamaran: k8s forward spawn worker: {e}");
        }
    }
}

pub fn relay_k8s_connect<S: Duplex>(
    home: &Path,
    client: S,
    addr: &str,
    port: u16,
) -> anyhow::Result<()> {
    relay_k8s_stream(
        home,
        client,
        &K8sMsg::Connect {
            addr: addr.into(),
            port,
        },
    )
}

pub fn relay_k8s_proxy<S: Duplex>(home: &Path, client: S) -> anyhow::Result<()> {
    relay_k8s_stream(home, client, &K8sMsg::Proxy)
}

fn relay_k8s_stream<S: Duplex>(home: &Path, client: S, req: &K8sMsg) -> anyhow::Result<()> {
    let mut sock = connect(home)?;
    k8s::write(&mut sock, req)?;
    match k8s::read(&mut sock)? {
        Some(K8sMsg::Done) => {}
        Some(K8sMsg::Error(e)) => bail!("{e}"),
        other => bail!("unexpected connect reply {other:?}"),
    }
    let mut sock_write = sock.try_clone()?;
    let mut client_read = client.try_clone()?;
    let up = thread::Builder::new()
        .name("k8s-fwd-up".into())
        .spawn(move || {
            let mut buf = vec![0u8; CHUNK];
            loop {
                match client_read.read(&mut buf) {
                    Ok(0) => {
                        let _ = k8s::write(&mut sock_write, &K8sMsg::DataEnd);
                        return;
                    }
                    Ok(n) => {
                        if k8s::write(&mut sock_write, &K8sMsg::Data(buf[..n].to_vec())).is_err() {
                            return;
                        }
                    }
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                    Err(_) => {
                        let _ = k8s::write(&mut sock_write, &K8sMsg::DataEnd);
                        return;
                    }
                }
            }
        })
        .context("spawn k8s forward reader")?;
    let mut client_write = client;
    let mut clean_eof = false;
    loop {
        match k8s::read(&mut sock) {
            Ok(Some(K8sMsg::Data(d))) => {
                if client_write.write_all(&d).is_err() {
                    break;
                }
            }
            Ok(Some(K8sMsg::DataEnd)) => {
                clean_eof = true;
                break;
            }
            Ok(Some(_)) | Ok(None) | Err(_) => break,
        }
    }
    if clean_eof {
        let _ = client_write.shutdown(Shutdown::Write);
    } else {
        let _ = client_write.shutdown(Shutdown::Both);
    }
    let _ = up.join();
    Ok(())
}

/// Rewrite k3s's default kubeconfig YAML to point at `127.0.0.1:<port>` and use
/// `ctx` (`katamaran` or `katamaran-<suffix>`) as the cluster, user, and context name.
pub fn rewrite_kubeconfig(raw: &str, ctx: &str, api_port: u16) -> String {
    let mut out = String::with_capacity(raw.len() + 64);
    for line in raw.lines() {
        let trimmed = line.trim_start();
        let indent = &line[..line.len() - trimmed.len()];
        if trimmed.starts_with("server:") {
            out.push_str(&format!("{indent}server: https://127.0.0.1:{api_port}\n"));
        } else if trimmed == "name: default" {
            out.push_str(&format!("{indent}name: {ctx}\n"));
        } else if trimmed == "- name: default" {
            out.push_str(&format!("{indent}- name: {ctx}\n"));
        } else if trimmed == "cluster: default" {
            out.push_str(&format!("{indent}cluster: {ctx}\n"));
        } else if trimmed == "user: default" {
            out.push_str(&format!("{indent}user: {ctx}\n"));
        } else if trimmed == "current-context: default" {
            out.push_str(&format!("{indent}current-context: {ctx}\n"));
        } else {
            out.push_str(line);
            out.push('\n');
        }
    }
    out
}

static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

/// Create a private (mode 0600) temp file next to `target`, write `bytes` to
/// it, sync, and atomically rename it over `target`. Credentials are never
/// written to a world-readable file.
fn write_private_atomic(target: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let parent = target
        .parent()
        .with_context(|| format!("missing parent for {}", target.display()))?;
    fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    let name = target
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "kubeconfig".into());
    let pid = std::process::id();
    let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let tmp = parent.join(format!(".{name}.katamaran.{pid}.{seq}.tmp"));
    let _ = fs::remove_file(&tmp);
    let mut f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&tmp)
        .with_context(|| format!("create {}", tmp.display()))?;
    if let Err(e) = f.write_all(bytes).and_then(|()| f.sync_all()) {
        drop(f);
        let _ = fs::remove_file(&tmp);
        return Err(e).with_context(|| format!("write {}", tmp.display()));
    }
    drop(f);
    if let Err(e) = fs::rename(&tmp, target) {
        let _ = fs::remove_file(&tmp);
        return Err(e).with_context(|| format!("rename {} -> {}", tmp.display(), target.display()));
    }
    Ok(())
}

fn lock_kubeconfig_dir(kube_dir: &Path) -> anyhow::Result<File> {
    fs::create_dir_all(kube_dir).with_context(|| format!("create {}", kube_dir.display()))?;
    let lock_path = kube_dir.join(".katamaran-kubeconfig.lock");
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(&lock_path)
        .with_context(|| format!("open {}", lock_path.display()))?;
    loop {
        if unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX) } == 0 {
            return Ok(f);
        }
        let e = io::Error::last_os_error();
        if e.kind() != io::ErrorKind::Interrupted {
            return Err(e).with_context(|| format!("flock {}", lock_path.display()));
        }
    }
}

/// Resolve every symlink in a chain (including dangling chains) so atomic
/// renames update the final target path rather than clobbering dotfiles or
/// Nix symlinks. Detects cycles via a hop limit.
fn resolve_symlink_target(path: &Path) -> anyhow::Result<PathBuf> {
    const MAX_SYMLINK_HOPS: usize = 40;
    let mut cur = path.to_path_buf();
    for _ in 0..MAX_SYMLINK_HOPS {
        match fs::symlink_metadata(&cur) {
            Ok(meta) if meta.file_type().is_symlink() => {
                let link =
                    fs::read_link(&cur).with_context(|| format!("read_link {}", cur.display()))?;
                cur = if link.is_absolute() {
                    link
                } else {
                    cur.parent().unwrap_or_else(|| Path::new(".")).join(link)
                };
            }
            Ok(_) => {
                return Ok(fs::canonicalize(&cur).unwrap_or(cur));
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(cur),
            Err(e) => return Err(e).with_context(|| format!("stat {}", cur.display())),
        }
    }
    bail!(
        "too many levels of symbolic links while resolving {}",
        path.display()
    )
}

/// Save `<home>/run/kubeconfig.yaml` and merge the context into `~/.kube/config`
/// (without clobbering any existing contexts).
pub fn write_and_merge_kubeconfig(
    home: &Path,
    raw: &str,
    api_port: u16,
) -> anyhow::Result<PathBuf> {
    let ctx = context_name()?;
    let user_home = std::env::var_os("HOME").map(PathBuf::from);
    let kubectl = resolve_kubectl_bin();
    write_and_merge_kubeconfig_in(home, user_home.as_deref(), raw, api_port, &ctx, &kubectl)
}

fn write_and_merge_kubeconfig_in(
    home: &Path,
    user_home: Option<&Path>,
    raw: &str,
    api_port: u16,
    ctx: &str,
    kubectl: &Path,
) -> anyhow::Result<PathBuf> {
    let rewritten = rewrite_kubeconfig(raw, ctx, api_port);
    let local_path = kubeconfig_path(home);
    write_private_atomic(&local_path, rewritten.as_bytes())?;

    if let Some(user_home) = user_home {
        let kube_dir = user_home.join(".kube");
        let _lock = lock_kubeconfig_dir(&kube_dir)?;
        let user_cfg = kube_dir.join("config");
        let target_cfg = resolve_symlink_target(&user_cfg)?;
        match fs::read(&target_cfg) {
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                write_private_atomic(&target_cfg, rewritten.as_bytes())
                    .with_context(|| format!("write {}", target_cfg.display()))?;
            }
            Err(e) => {
                return Err(e).with_context(|| format!("read {}", target_cfg.display()));
            }
            Ok(existing) => {
                merge_into_user_kubeconfig(&target_cfg, &existing, &local_path, ctx, kubectl)
                    .with_context(|| {
                        format!("merge context {ctx} into {}", target_cfg.display())
                    })?;
            }
        }
    }
    Ok(local_path)
}

/// Locate `kubectl` even under a minimal LaunchAgent `PATH`.
fn find_kubectl() -> PathBuf {
    let candidates = [
        Some(PathBuf::from("/opt/homebrew/bin/kubectl")),
        Some(PathBuf::from("/usr/local/bin/kubectl")),
        std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".nix-profile/bin/kubectl")),
        Some(PathBuf::from("/run/current-system/sw/bin/kubectl")),
        std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local/bin/kubectl")),
    ];
    for path in candidates.into_iter().flatten() {
        if path.is_file() {
            return path;
        }
    }
    PathBuf::from("kubectl")
}

fn resolve_kubectl_bin() -> PathBuf {
    if let Ok(path) = std::env::var("PATH") {
        for dir in std::env::split_paths(&path) {
            let candidate = dir.join("kubectl");
            if candidate.is_file() {
                return candidate;
            }
        }
    }
    find_kubectl()
}

/// Merge `local_cfg` into `target_cfg` using a private temporary copy so the
/// live `~/.kube/config` is never mutated if `kubectl` is missing or fails.
fn merge_into_user_kubeconfig(
    target_cfg: &Path,
    existing: &[u8],
    local_cfg: &Path,
    ctx: &str,
    kubectl: &Path,
) -> anyhow::Result<()> {
    let parent = target_cfg
        .parent()
        .with_context(|| format!("missing parent for {}", target_cfg.display()))?;
    let pid = std::process::id();
    let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let scratch = parent.join(format!(".config.katamaran.scratch.{pid}.{seq}.tmp"));
    let _ = fs::remove_file(&scratch);
    {
        let mut f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&scratch)
            .with_context(|| format!("create {}", scratch.display()))?;
        f.write_all(existing)?;
    }

    let result = (|| -> anyhow::Result<()> {
        // Strip any stale entry for `ctx` from the scratch copy only.
        for sub in ["delete-context", "delete-cluster", "delete-user"] {
            match Command::new(kubectl)
                .args(["config", sub, ctx])
                .env("KUBECONFIG", &scratch)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
            {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    eprintln!(
                        "katamaran: warning: kubectl not found; wrote {} without merging into {}",
                        local_cfg.display(),
                        target_cfg.display()
                    );
                    return Ok(());
                }
                Err(e) => return Err(e).context("run kubectl"),
            }
        }
        let combined = format!("{}:{}", scratch.display(), local_cfg.display());
        let out = match Command::new(kubectl)
            .args(["config", "view", "--raw"])
            .env("KUBECONFIG", combined)
            .stdin(Stdio::null())
            .output()
        {
            Ok(o) => o,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                eprintln!(
                    "katamaran: warning: kubectl not found; wrote {} without merging into {}",
                    local_cfg.display(),
                    target_cfg.display()
                );
                return Ok(());
            }
            Err(e) => return Err(e).context("run kubectl config view --raw"),
        };
        if !out.status.success() {
            bail!(
                "kubectl config view --raw failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        if out.stdout.is_empty() {
            bail!("kubectl config view --raw produced empty output");
        }
        write_private_atomic(target_cfg, &out.stdout)
    })();

    let _ = fs::remove_file(&scratch);
    result
}

/// Remove `ctx` from `~/.kube/config` and delete `<home>/run/kubeconfig.yaml`.
pub fn remove_kubeconfig(home: &Path, ctx: &str) -> anyhow::Result<()> {
    let user_home = std::env::var_os("HOME").map(PathBuf::from);
    let kubectl = resolve_kubectl_bin();
    remove_kubeconfig_in(home, user_home.as_deref(), ctx, &kubectl)
}

fn remove_kubeconfig_in(
    home: &Path,
    user_home: Option<&Path>,
    ctx: &str,
    kubectl: &Path,
) -> anyhow::Result<()> {
    if let Err(e) = fs::remove_file(kubeconfig_path(home))
        && e.kind() != io::ErrorKind::NotFound
    {
        return Err(e).with_context(|| format!("remove {}", kubeconfig_path(home).display()));
    }
    let Some(user_home) = user_home else {
        return Ok(());
    };
    let kube_dir = user_home.join(".kube");
    if !kube_dir.is_dir() {
        return Ok(());
    }
    let _lock = lock_kubeconfig_dir(&kube_dir)?;
    let user_cfg = kube_dir.join("config");
    let target_cfg = resolve_symlink_target(&user_cfg)?;
    let existing = match fs::read(&target_cfg) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e).with_context(|| format!("read {}", target_cfg.display())),
    };
    let parent = target_cfg
        .parent()
        .with_context(|| format!("missing parent for {}", target_cfg.display()))?;
    let pid = std::process::id();
    let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let scratch = parent.join(format!(".config.katamaran.rm.{pid}.{seq}.tmp"));
    let _ = fs::remove_file(&scratch);
    {
        let mut f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&scratch)
            .with_context(|| format!("create {}", scratch.display()))?;
        f.write_all(&existing)?;
    }

    let result = (|| -> anyhow::Result<()> {
        let cur = match Command::new(kubectl)
            .args(["config", "current-context"])
            .env("KUBECONFIG", &scratch)
            .stdin(Stdio::null())
            .output()
        {
            Ok(o) => Some(o),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                eprintln!(
                    "katamaran: warning: kubectl not found; could not remove context {ctx} from {}",
                    target_cfg.display()
                );
                return Ok(());
            }
            Err(e) => return Err(e).context("run kubectl config current-context"),
        };
        if let Some(o) = cur
            && o.status.success()
            && String::from_utf8_lossy(&o.stdout).trim() == ctx
        {
            let out = Command::new(kubectl)
                .args(["config", "unset", "current-context"])
                .env("KUBECONFIG", &scratch)
                .stdin(Stdio::null())
                .output()
                .context("run kubectl config unset current-context")?;
            if !out.status.success() {
                bail!(
                    "kubectl config unset current-context failed: {}",
                    String::from_utf8_lossy(&out.stderr).trim()
                );
            }
        }
        for key in [
            format!("contexts.{ctx}"),
            format!("clusters.{ctx}"),
            format!("users.{ctx}"),
        ] {
            let out = match Command::new(kubectl)
                .args(["config", "unset", &key])
                .env("KUBECONFIG", &scratch)
                .stdin(Stdio::null())
                .output()
            {
                Ok(o) => o,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    eprintln!(
                        "katamaran: warning: kubectl not found; could not remove context {ctx} from {}",
                        target_cfg.display()
                    );
                    return Ok(());
                }
                Err(e) => return Err(e).with_context(|| format!("run kubectl config unset {key}")),
            };
            if !out.status.success() {
                bail!(
                    "kubectl config unset {key} failed: {}",
                    String::from_utf8_lossy(&out.stderr).trim()
                );
            }
        }
        let updated = fs::read(&scratch).with_context(|| format!("read {}", scratch.display()))?;
        if updated != existing {
            write_private_atomic(&target_cfg, &updated)?;
        }
        Ok(())
    })();

    let _ = fs::remove_file(&scratch);
    result
}

/// Minimal UDP DNS responder for `k8s.kat.local` and `*.k8s.kat.local` -> `127.0.0.1`.
pub struct DnsServer {
    pub port: u16,
    pub isolated: bool,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl DnsServer {
    pub fn start() -> io::Result<DnsServer> {
        let env = std::env::var_os(ISOLATED_DNS_ENV);
        Self::start_with_mode(env.as_deref(), installed_dns_port)
    }

    fn start_with_mode(
        isolated_env: Option<&std::ffi::OsStr>,
        installed: impl FnOnce() -> Option<u16>,
    ) -> io::Result<DnsServer> {
        if isolated_env.is_some_and(|v| v == "1") {
            eprintln!(
                "warning: {ISOLATED_DNS_ENV}=1: serving *.{DNS_DOMAIN} on an ephemeral port; {RESOLVER_PATH} is not used"
            );
            return Self::start_ephemeral();
        }
        Self::start_with_installed_port(installed())
    }

    /// Bind an ephemeral loopback port, ignoring `/etc/resolver/k8s.kat.local`
    /// and `DEFAULT_DNS_PORT`, so a throwaway daemon never contends with the
    /// developer's real daemon for the installed resolver port.
    fn start_ephemeral() -> io::Result<DnsServer> {
        let sock = UdpSocket::bind(("127.0.0.1", 0)).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("bind 127.0.0.1:0 for isolated *.{DNS_DOMAIN} DNS: {e}"),
            )
        })?;
        Self::serve(sock, true)
    }

    fn start_with_installed_port(installed: Option<u16>) -> io::Result<DnsServer> {
        let sock = match installed {
            Some(p) => UdpSocket::bind(("127.0.0.1", p)).map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("bind 127.0.0.1:{p} for *.{DNS_DOMAIN} DNS: {e}"),
                )
            })?,
            None => UdpSocket::bind(("127.0.0.1", DEFAULT_DNS_PORT))
                .or_else(|_| UdpSocket::bind(("127.0.0.1", 0)))
                .map_err(|e| {
                    io::Error::new(
                        e.kind(),
                        format!("bind 127.0.0.1:{DEFAULT_DNS_PORT} for *.{DNS_DOMAIN} DNS: {e}"),
                    )
                })?,
        };
        Self::serve(sock, false)
    }

    fn serve(sock: UdpSocket, isolated: bool) -> io::Result<DnsServer> {
        let port = sock.local_addr()?.port();
        sock.set_read_timeout(Some(Duration::from_millis(250)))?;
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let thread = thread::Builder::new()
            .name(format!("k8s-dns-{port}"))
            .spawn(move || {
                let mut buf = [0u8; 512];
                while !flag.load(Ordering::Relaxed) {
                    match sock.recv_from(&mut buf) {
                        Ok((n, peer)) => {
                            if let Some(resp) = build_dns_response(&buf[..n], Ipv4Addr::LOCALHOST) {
                                let _ = sock.send_to(&resp, peer);
                            }
                        }
                        Err(e)
                            if matches!(
                                e.kind(),
                                io::ErrorKind::WouldBlock
                                    | io::ErrorKind::TimedOut
                                    | io::ErrorKind::Interrupted
                            ) =>
                        {
                            continue;
                        }
                        Err(_) => return,
                    }
                }
            })?;
        Ok(DnsServer {
            port,
            isolated,
            stop,
            thread: Some(thread),
        })
    }

    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

impl Drop for DnsServer {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Parse the configured port from an installed Katamaran resolver file, if present.
pub fn installed_dns_port() -> Option<u16> {
    let text = fs::read_to_string(RESOLVER_PATH).ok()?;
    parse_resolver_port(&text)
}

fn parse_resolver_port(text: &str) -> Option<u16> {
    if !text.lines().any(|l| l.trim() == "nameserver 127.0.0.1") {
        return None;
    }
    text.lines().find_map(|l| {
        l.trim()
            .strip_prefix("port ")
            .and_then(|p| p.trim().parse::<u16>().ok())
            .filter(|p| *p != 0)
    })
}

/// Whether `/etc/resolver/k8s.kat.local` is installed and matches `active_port`
/// (when a DNS listener is running).
pub fn dns_resolver_installed(active_port: Option<u16>) -> bool {
    let Some(installed) = installed_dns_port() else {
        return false;
    };
    match active_port {
        Some(p) => installed == p,
        None => true,
    }
}

pub fn dns_resolver_content(port: u16) -> String {
    format!("# Managed by katamaran for *.{DNS_DOMAIN}\nnameserver 127.0.0.1\nport {port}\n")
}

/// Build a standard DNS response for a single-question UDP query.
/// Resolves `k8s.kat.local` and `*.k8s.kat.local` `A` queries to `ip`,
/// returns `NODATA` for `AAAA` on the same domain, and `NXDOMAIN` otherwise.
pub fn build_dns_response(query: &[u8], ip: Ipv4Addr) -> Option<Vec<u8>> {
    if query.len() < 12 {
        return None;
    }
    // Reject DNS responses (QR != 0) and non-standard opcodes (OPCODE != 0).
    if query[2] & 0xF8 != 0 {
        return None;
    }
    let qdcount = u16::from_be_bytes([query[4], query[5]]);
    if qdcount < 1 {
        return None;
    }
    let (name, qend) = parse_dns_name(query, 12)?;
    if qend + 4 > query.len() {
        return None;
    }
    let qtype = u16::from_be_bytes([query[qend], query[qend + 1]]);
    let qclass = u16::from_be_bytes([query[qend + 2], query[qend + 3]]);
    let lower = name.to_ascii_lowercase();
    let matches_domain = lower == DNS_DOMAIN || lower.ends_with(&format!(".{DNS_DOMAIN}"));

    let mut resp = Vec::with_capacity(qend + 20);
    // Header: copy ID, set QR=1, AA=1, copy RD, set RA=0.
    resp.extend_from_slice(&query[0..2]);
    let rd = query[2] & 0x01;
    resp.push(0x84 | rd); // QR=1, AA=1, RD
    let (rcode, ancount): (u8, u16) = if !matches_domain {
        (3, 0) // NXDOMAIN
    } else if qclass == 1 && qtype == 1 {
        (0, 1) // NOERROR + 1 A record
    } else {
        (0, 0) // NOERROR NODATA (e.g. AAAA)
    };
    resp.push(rcode);
    resp.extend_from_slice(&1u16.to_be_bytes()); // QDCOUNT = 1
    resp.extend_from_slice(&ancount.to_be_bytes()); // ANCOUNT
    resp.extend_from_slice(&0u16.to_be_bytes()); // NSCOUNT
    resp.extend_from_slice(&0u16.to_be_bytes()); // ARCOUNT
    // Echo the question section verbatim.
    resp.extend_from_slice(&query[12..qend + 4]);

    if ancount == 1 {
        // Pointer to name at offset 12 (0xC00C).
        resp.extend_from_slice(&[0xC0, 0x0C]);
        resp.extend_from_slice(&1u16.to_be_bytes()); // TYPE A
        resp.extend_from_slice(&1u16.to_be_bytes()); // CLASS IN
        resp.extend_from_slice(&30u32.to_be_bytes()); // TTL 30s
        resp.extend_from_slice(&4u16.to_be_bytes()); // RDLENGTH 4
        resp.extend_from_slice(&ip.octets());
    }
    Some(resp)
}

fn parse_dns_name(packet: &[u8], mut offset: usize) -> Option<(String, usize)> {
    let mut labels = Vec::new();
    loop {
        let len = *packet.get(offset)? as usize;
        offset += 1;
        if len == 0 {
            break;
        }
        if len & 0xC0 != 0 || offset + len > packet.len() {
            return None;
        }
        let label = std::str::from_utf8(&packet[offset..offset + len]).ok()?;
        labels.push(label);
        offset += len;
    }
    Some((labels.join("."), offset))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn release_urls_escape_the_build_suffix() {
        assert_eq!(
            release_url(K3S_VERSION, K3S_ARM64),
            "https://github.com/k3s-io/k3s/releases/download/v1.37.2%2Bk3s1/k3s-arm64"
        );
        assert_eq!(
            release_url("v1.32.5+k3s1", K3S_ARM64_SUMS),
            "https://github.com/k3s-io/k3s/releases/download/v1.32.5%2Bk3s1/sha256sum-arm64.txt"
        );
    }

    #[test]
    fn finds_the_arm64_binary_checksum() {
        let sums = "\
97c5264b988cac6f26294ded8235e116b52faa0169bdd0c4ea1a5412973d2a8d  k3s-airgap-images-arm64.tar
EB62D8BA346F7C6A0BB5C874EA92335ECAD7CB3FA167508F94888AAEB43E7F22 *k3s-arm64
";
        assert_eq!(
            parse_arm64_sha256(sums).unwrap(),
            "eb62d8ba346f7c6a0bb5c874ea92335ecad7cb3fa167508f94888aaeb43e7f22"
        );
        assert!(parse_arm64_sha256("abc  k3s-arm64\n").is_err());
        assert!(parse_arm64_sha256("").is_err());
        let airgap_only = sums.lines().next().unwrap();
        assert!(parse_arm64_sha256(airgap_only).is_err());
    }

    fn temp_k8s_home(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "kat-k8s-{name}-{}-{}",
            std::process::id(),
            TMP_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn non_pinned_cache_verifies_and_reuses_without_network() {
        let home = temp_k8s_home("cache-reuse");
        let version = "v1.32.5+k3s1";
        let payload = b"fake k3s-arm64 binary payload";
        let probe = home.join("probe.bin");
        fs::write(&probe, payload).unwrap();
        let expected_hash = crate::machines::sha256_file(&probe).unwrap();
        fs::remove_file(&probe).unwrap();

        let mut text_fetches = 0usize;
        let mut bin_fetches = 0usize;
        let mut noop = |_: &str| {};
        let got = ensure_cached_binary_with(
            &home,
            version,
            &mut noop,
            |url| {
                assert_eq!(url, release_url(version, K3S_ARM64_SUMS));
                text_fetches += 1;
                Ok(format!("{expected_hash}  {K3S_ARM64}\n"))
            },
            |url, out| {
                assert_eq!(url, release_url(version, K3S_ARM64));
                bin_fetches += 1;
                fs::write(out, payload)?;
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(got, cache_binary_path(&home, version));
        assert_eq!(text_fetches, 1);
        assert_eq!(bin_fetches, 1);
        assert_eq!(fs::read(&got).unwrap(), payload);

        // Second start with matching cached binary + checksum makes zero network calls.
        let cached = ensure_cached_binary_with(
            &home,
            version,
            &mut noop,
            |url| panic!("unexpected checksum fetch: {url}"),
            |url, _| panic!("unexpected binary fetch: {url}"),
        )
        .unwrap();
        assert_eq!(cached, got);

        // A lost checksum sidecar refetches only the checksums: the cached binary
        // matches them, so it is reused (and the sidecar rewritten) without a download.
        let sidecar = got.with_file_name(format!("{K3S_ARM64}.sha256"));
        fs::remove_file(&sidecar).unwrap();
        let mut text_fetches = 0usize;
        let reused = ensure_cached_binary_with(
            &home,
            version,
            &mut noop,
            |_| {
                text_fetches += 1;
                Ok(format!("{expected_hash}  {K3S_ARM64}\n"))
            },
            |url, _| panic!("unexpected binary fetch: {url}"),
        )
        .unwrap();
        assert_eq!(reused, got);
        assert_eq!(text_fetches, 1);
        assert!(sidecar.is_file());

        let _ = fs::remove_dir_all(&home);
    }

    #[test]
    fn mismatched_download_is_rejected_without_publishing_or_replacing_cache() {
        let home = temp_k8s_home("cache-mismatch");
        let version = "v1.32.5+k3s1";
        let good_payload = b"expected k3s-arm64 payload";
        let bad_payload = b"tampered k3s-arm64 payload";
        let probe = home.join("probe.bin");
        fs::write(&probe, good_payload).unwrap();
        let expected_hash = crate::machines::sha256_file(&probe).unwrap();
        fs::remove_file(&probe).unwrap();

        let mut noop = |_: &str| {};
        let err = ensure_cached_binary_with(
            &home,
            version,
            &mut noop,
            |_| Ok(format!("{expected_hash}  {K3S_ARM64}\n")),
            |_, out| {
                fs::write(out, bad_payload)?;
                Ok(())
            },
        )
        .unwrap_err();
        assert!(format!("{err:#}").contains("checksum mismatch"), "{err:#}");

        let bin_path = cache_binary_path(&home, version);
        let cache_dir = bin_path.parent().unwrap();
        assert!(!bin_path.exists());
        assert!(!cache_dir.join(format!("{K3S_ARM64}.sha256")).exists());
        let leftovers: Vec<_> = fs::read_dir(cache_dir).unwrap().collect();
        assert!(leftovers.is_empty(), "leftover files: {leftovers:?}");

        // Existing corrupted cache entry is not overwritten by another bad download.
        fs::write(&bin_path, b"corrupted-prior-cache").unwrap();
        fs::write(
            cache_dir.join(format!("{K3S_ARM64}.sha256")),
            format!("{expected_hash}  {K3S_ARM64}\n"),
        )
        .unwrap();
        let err = ensure_cached_binary_with(
            &home,
            version,
            &mut noop,
            |_| Ok(format!("{expected_hash}  {K3S_ARM64}\n")),
            |_, out| {
                fs::write(out, bad_payload)?;
                Ok(())
            },
        )
        .unwrap_err();
        assert!(format!("{err:#}").contains("checksum mismatch"), "{err:#}");
        assert_eq!(fs::read(&bin_path).unwrap(), b"corrupted-prior-cache");
        for entry in fs::read_dir(cache_dir).unwrap() {
            let name = entry.unwrap().file_name().to_string_lossy().into_owned();
            assert!(!name.ends_with(".part"), "leftover part file: {name}");
        }

        let _ = fs::remove_dir_all(&home);
    }

    #[test]
    fn checksum_fetch_error_keeps_underlying_cause() {
        let home = temp_k8s_home("checksum-fetch-err");
        let version = "v1.32.5+k3s1";
        let mut noop = |_: &str| {};
        let err = ensure_cached_binary_with(
            &home,
            version,
            &mut noop,
            |_| bail!("curl: (6) Could not resolve host"),
            |_, _| panic!("should not download binary when checksum fetch fails"),
        )
        .unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("fetch k3s v1.32.5+k3s1 arm64 checksums"),
            "{msg}"
        );
        assert!(msg.contains("Could not resolve host"), "{msg}");
        assert!(!msg.contains("not found"), "{msg}");

        let _ = fs::remove_dir_all(&home);
    }

    #[test]
    fn rewrites_kubeconfig_server_and_context() {
        let raw = "\
apiVersion: v1
clusters:
- cluster:
    certificate-authority-data: AAA=
    server: https://127.0.0.1:6443
  name: default
contexts:
- context:
    cluster: default
    user: default
  name: default
current-context: default
kind: Config
users:
- name: default
  user:
    client-certificate-data: BBB=
";
        let got = rewrite_kubeconfig(raw, "katamaran", 16443);
        assert!(got.contains("server: https://127.0.0.1:16443"));
        assert!(got.contains("name: katamaran"));
        assert!(got.contains("cluster: katamaran"));
        assert!(got.contains("user: katamaran"));
        assert!(got.contains("current-context: katamaran"));
        assert!(!got.contains("default"));
    }

    fn make_query(domain: &str, qtype: u16) -> Vec<u8> {
        let mut q = vec![0x12, 0x34, 0x01, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0];
        for label in domain.split('.') {
            q.push(label.len() as u8);
            q.extend_from_slice(label.as_bytes());
        }
        q.push(0);
        q.extend_from_slice(&qtype.to_be_bytes());
        q.extend_from_slice(&1u16.to_be_bytes()); // IN
        q
    }

    #[test]
    fn dns_answers_wildcard_k8s_kat_local() {
        let q = make_query("app.k8s.kat.local", 1);
        let resp = build_dns_response(&q, Ipv4Addr::LOCALHOST).unwrap();
        assert_eq!(&resp[0..2], &[0x12, 0x34]);
        assert_eq!(resp[3] & 0x0F, 0); // NOERROR
        assert_eq!(u16::from_be_bytes([resp[6], resp[7]]), 1); // 1 answer
        assert_eq!(&resp[resp.len() - 4..], &[127, 0, 0, 1]);

        let q_aaaa = make_query("app.k8s.kat.local", 28);
        let resp_aaaa = build_dns_response(&q_aaaa, Ipv4Addr::LOCALHOST).unwrap();
        assert_eq!(resp_aaaa[3] & 0x0F, 0); // NOERROR NODATA
        assert_eq!(u16::from_be_bytes([resp_aaaa[6], resp_aaaa[7]]), 0);

        let q_other = make_query("example.com", 1);
        let resp_other = build_dns_response(&q_other, Ipv4Addr::LOCALHOST).unwrap();
        assert_eq!(resp_other[3] & 0x0F, 3); // NXDOMAIN
    }

    #[test]
    fn dns_rejects_responses_and_non_standard_opcodes() {
        let mut q_resp = make_query("app.k8s.kat.local", 1);
        q_resp[2] |= 0x80; // QR=1 (response)
        assert!(build_dns_response(&q_resp, Ipv4Addr::LOCALHOST).is_none());

        let mut q_iquery = make_query("app.k8s.kat.local", 1);
        q_iquery[2] |= 0x08; // OPCODE=1 (IQUERY)
        assert!(build_dns_response(&q_iquery, Ipv4Addr::LOCALHOST).is_none());

        let mut q_status = make_query("app.k8s.kat.local", 1);
        q_status[2] |= 0x10; // OPCODE=2 (STATUS)
        assert!(build_dns_response(&q_status, Ipv4Addr::LOCALHOST).is_none());
    }

    #[test]
    fn conn_guard_caps_and_releases() {
        let active = Arc::new(AtomicUsize::new(0));
        let g1 = ConnGuard::try_acquire(&active, 2).unwrap();
        let g2 = ConnGuard::try_acquire(&active, 2).unwrap();
        assert!(ConnGuard::try_acquire(&active, 2).is_none());
        assert_eq!(active.load(Ordering::SeqCst), 2);
        drop(g1);
        let g3 = ConnGuard::try_acquire(&active, 2).unwrap();
        assert_eq!(active.load(Ordering::SeqCst), 2);
        drop(g2);
        drop(g3);
        assert_eq!(active.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn dns_server_ephemeral_ignores_installed_port() {
        use std::ffi::OsStr;

        let holder = UdpSocket::bind(("127.0.0.1", 0)).unwrap();
        let held = holder.local_addr().unwrap().port();

        // Exact "1" skips the installed-port lookup and binds an ephemeral port in isolated mode.
        let mut looked_up = false;
        let mut srv = DnsServer::start_with_mode(Some(OsStr::new("1")), || {
            looked_up = true;
            Some(held)
        })
        .unwrap();
        assert!(!looked_up, "isolated mode must skip installed-port lookup");
        assert!(srv.isolated);
        assert_ne!(srv.port, 0);
        assert_ne!(srv.port, held);
        srv.stop();

        // Unset env still consults the installed port and fails when it is held.
        let mut looked_up = false;
        assert!(
            DnsServer::start_with_mode(None, || {
                looked_up = true;
                Some(held)
            })
            .is_err()
        );
        assert!(looked_up);

        // Non-"1" values (e.g. "0") still consult the installed port and fail when it is held.
        let mut looked_up = false;
        assert!(
            DnsServer::start_with_mode(Some(OsStr::new("0")), || {
                looked_up = true;
                Some(held)
            })
            .is_err()
        );
        assert!(looked_up);
    }

    #[test]
    fn dns_server_falls_back_when_default_port_busy() {
        // Hold DEFAULT_DNS_PORT if available, or verify fallback when another server binds first.
        let holder = UdpSocket::bind(("127.0.0.1", DEFAULT_DNS_PORT)).ok();
        let mut srv = DnsServer::start_with_mode(None, || None).unwrap();
        assert!(!srv.isolated);
        assert_ne!(srv.port, 0);
        if holder.is_some() {
            assert_ne!(srv.port, DEFAULT_DNS_PORT);
        }
        srv.stop();
        drop(holder);
    }

    #[test]
    fn rest_proxy_relays_to_guest_kubectl_proxy() {
        use std::os::unix::net::UnixListener;

        let dir = std::env::temp_dir().join(format!(
            "kat-k8s-rest-{}-{}",
            std::process::id(),
            TMP_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(dir.join("run")).unwrap();
        let ulistener = UnixListener::bind(socket_path(&dir)).unwrap();

        // Guest mock: expect a Proxy request, echo one chunk back, end.
        let guest = thread::spawn(move || {
            let (mut conn, _) = ulistener.accept().unwrap();
            let req = k8s::read(&mut conn).unwrap().unwrap();
            assert_eq!(req, K8sMsg::Proxy);
            k8s::write(&mut conn, &K8sMsg::Done).unwrap();
            let Some(K8sMsg::Data(d)) = k8s::read(&mut conn).unwrap() else {
                panic!("expected data");
            };
            k8s::write(&mut conn, &K8sMsg::Data(d)).unwrap();
            k8s::write(&mut conn, &K8sMsg::DataEnd).unwrap();
        });

        let rest = RestProxy::start(&dir).unwrap();
        let mode = fs::metadata(&rest.path).unwrap().permissions();
        assert_eq!(
            std::os::unix::fs::PermissionsExt::mode(&mode) & 0o777,
            0o600
        );
        let mut c = UnixStream::connect(&rest.path).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
        c.write_all(b"GET /api HTTP/1.1\r\n\r\n").unwrap();
        let mut got = Vec::new();
        c.read_to_end(&mut got).unwrap();
        assert_eq!(got, b"GET /api HTTP/1.1\r\n\r\n");
        guest.join().unwrap();

        let path = rest.path.clone();
        drop(rest);
        assert!(!path.exists(), "socket removed on drop");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn relay_k8s_connect_unblocks_on_abrupt_eof() {
        use std::os::unix::net::UnixListener;
        use std::sync::mpsc;

        let dir = std::env::temp_dir().join(format!(
            "kat-k8s-relay-{}-{}",
            std::process::id(),
            TMP_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let run_dir = dir.join("run");
        fs::create_dir_all(&run_dir).unwrap();
        let sock_path = socket_path(&dir);
        let ulistener = UnixListener::bind(&sock_path).unwrap();

        // Guest mock: accept Connect, send Done, then abruptly close without DataEnd.
        let guest = thread::spawn(move || {
            let (mut conn, _) = ulistener.accept().unwrap();
            let req = k8s::read(&mut conn).unwrap().unwrap();
            assert!(matches!(req, K8sMsg::Connect { port: 8080, .. }));
            k8s::write(&mut conn, &K8sMsg::Done).unwrap();
            thread::sleep(Duration::from_millis(30));
            drop(conn);
        });

        // TCP peer that stays open and never writes (simulating an idle client).
        let tlistener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let taddr = tlistener.local_addr().unwrap();
        let _idle_peer = TcpStream::connect(taddr).unwrap();
        let (client, _) = tlistener.accept().unwrap();

        let (done_tx, done_rx) = mpsc::channel();
        let home = dir.clone();
        let relay = thread::spawn(move || {
            let res = relay_k8s_connect(&home, client, "10.42.0.5", 8080);
            let _ = done_tx.send(res);
        });

        let res = done_rx
            .recv_timeout(Duration::from_secs(3))
            .expect("relay_k8s_connect hung instead of unblocking on abrupt EOF");
        assert!(res.is_ok());
        relay.join().unwrap();
        guest.join().unwrap();
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn kubeconfig_preserves_symlink_and_handles_missing_kubectl() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let base = std::env::temp_dir().join(format!(
            "kat-k8s-kc-{}-{}",
            std::process::id(),
            TMP_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let home = base.join("kat");
        let user_home = base.join("user");
        let dotfiles = base.join("dotfiles");
        fs::create_dir_all(&home).unwrap();
        fs::create_dir_all(user_home.join(".kube")).unwrap();
        fs::create_dir_all(&dotfiles).unwrap();

        let real_cfg = dotfiles.join("kubeconfig");
        let link_cfg = user_home.join(".kube/config");
        symlink(&real_cfg, &link_cfg).unwrap();

        let raw = "apiVersion: v1\nclusters:\n- cluster:\n    server: https://127.0.0.1:6443\n  name: default\ncontexts:\n- context:\n    cluster: default\n    user: default\n  name: default\ncurrent-context: default\nkind: Config\nusers:\n- name: default\n  user: {}\n";
        let missing_kubectl = base.join("no-such-kubectl");

        // 1. Dangling symlink target does not exist yet: writes through symlink.
        let local = write_and_merge_kubeconfig_in(
            &home,
            Some(&user_home),
            raw,
            16443,
            "katamaran",
            &missing_kubectl,
        )
        .unwrap();
        assert!(local.is_file());
        assert!(
            fs::symlink_metadata(&link_cfg)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        let written = fs::read_to_string(&real_cfg).unwrap();
        assert!(written.contains("https://127.0.0.1:16443"));
        assert!(written.contains("current-context: katamaran"));

        // 2. Existing config with missing kubectl: warns and succeeds without failing.
        write_and_merge_kubeconfig_in(
            &home,
            Some(&user_home),
            raw,
            26443,
            "katamaran",
            &missing_kubectl,
        )
        .unwrap();
        assert!(
            fs::read_to_string(&local)
                .unwrap()
                .contains("https://127.0.0.1:26443")
        );

        // 3. remove_kubeconfig with mock kubectl verifies current-context and unset keys + symlink preserved.
        let log_file = base.join("kubectl.log");
        let fake_kubectl = base.join("fake-kubectl.sh");
        let script = format!(
            "#!/bin/sh\necho \"$@\" >> '{}'\nif [ \"$1\" = \"config\" ] && [ \"$2\" = \"current-context\" ]; then\n  echo katamaran\n  exit 0\nfi\nif [ \"$1\" = \"config\" ] && [ \"$2\" = \"unset\" ]; then\n  printf 'cleaned\\n' > \"$KUBECONFIG\"\n  exit 0\nfi\nexit 0\n",
            log_file.display()
        );
        fs::write(&fake_kubectl, script).unwrap();
        fs::set_permissions(&fake_kubectl, fs::Permissions::from_mode(0o755)).unwrap();

        remove_kubeconfig_in(&home, Some(&user_home), "katamaran", &fake_kubectl).unwrap();
        assert!(!local.exists());
        assert!(
            fs::symlink_metadata(&link_cfg)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(fs::read_to_string(&real_cfg).unwrap(), "cleaned\n");
        let calls = fs::read_to_string(&log_file).unwrap();
        assert!(calls.contains("config current-context"));
        assert!(calls.contains("config unset current-context"));
        assert!(calls.contains("config unset contexts.katamaran"));
        assert!(calls.contains("config unset clusters.katamaran"));
        assert!(calls.contains("config unset users.katamaran"));

        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn resolve_symlink_target_follows_dangling_chains_and_detects_cycles() {
        use std::os::unix::fs::symlink;

        let base = std::env::temp_dir().join(format!(
            "kat-k8s-symchain-{}-{}",
            std::process::id(),
            TMP_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let home = base.join("kat");
        let user_home = base.join("user");
        let dotfiles = base.join("dotfiles");
        fs::create_dir_all(&home).unwrap();
        fs::create_dir_all(user_home.join(".kube")).unwrap();
        fs::create_dir_all(&dotfiles).unwrap();

        // Chained dangling symlinks: ~/.kube/config -> intermediate -> final_target (absent)
        let final_target = dotfiles.join("final-kubeconfig");
        let intermediate = dotfiles.join("intermediate-link");
        let link_cfg = user_home.join(".kube/config");
        symlink(&final_target, &intermediate).unwrap();
        symlink(&intermediate, &link_cfg).unwrap();

        let resolved = resolve_symlink_target(&link_cfg).unwrap();
        assert_eq!(resolved, final_target);

        let raw = "apiVersion: v1\nclusters:\n- cluster:\n    server: https://127.0.0.1:6443\n  name: default\ncontexts:\n- context:\n    cluster: default\n    user: default\n  name: default\ncurrent-context: default\nkind: Config\nusers:\n- name: default\n  user: {}\n";
        let missing_kubectl = base.join("no-such-kubectl");
        write_and_merge_kubeconfig_in(
            &home,
            Some(&user_home),
            raw,
            16443,
            "katamaran",
            &missing_kubectl,
        )
        .unwrap();
        assert!(
            fs::symlink_metadata(&link_cfg)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert!(
            fs::symlink_metadata(&intermediate)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert!(fs::read_to_string(&final_target).unwrap().contains("16443"));

        // Cycle detection: loop_a -> loop_b -> loop_a
        let loop_a = base.join("loop_a");
        let loop_b = base.join("loop_b");
        symlink(&loop_b, &loop_a).unwrap();
        symlink(&loop_a, &loop_b).unwrap();
        assert!(resolve_symlink_target(&loop_a).is_err());

        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn build_start_msg_includes_config_flags_and_k3s_config_yaml() {
        let home = std::env::temp_dir().join(format!(
            "kat-k8s-start-msg-{}-{}",
            std::process::id(),
            TMP_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&home).unwrap();

        // Empty defaults when neither config.toml nor k3s-config.yaml exist.
        assert_eq!(
            build_start_msg(&home).unwrap(),
            K8sMsg::Start {
                extra_args: vec![],
                config_yaml: None,
            }
        );

        // Combines k8s_disable, k8s_args, and k3s-config.yaml.
        let mut cfg = crate::config::Config::default();
        let host = crate::config::Host {
            cpus: 255,
            memory_mib: 32768,
        };
        cfg.set("k8s_disable", "traefik,servicelb", &host).unwrap();
        cfg.set("k8s_args", "--debug --node-label=env=dev", &host)
            .unwrap();
        cfg.save(&home).unwrap();
        let yaml = "write-kubeconfig-mode: '0600'\n";
        fs::write(k3s_config_path(&home), yaml).unwrap();

        assert_eq!(
            build_start_msg(&home).unwrap(),
            K8sMsg::Start {
                extra_args: vec![
                    "--disable=traefik".into(),
                    "--disable=servicelb".into(),
                    "--debug".into(),
                    "--node-label=env=dev".into(),
                ],
                config_yaml: Some(yaml.into()),
            }
        );

        // Rejects invalid UTF-8 and oversized k3s-config.yaml.
        fs::write(k3s_config_path(&home), [0xff, 0xfe]).unwrap();
        assert!(build_start_msg(&home).is_err());

        fs::write(k3s_config_path(&home), vec![b'a'; MAX_K3S_CONFIG_BYTES + 1]).unwrap();
        assert!(build_start_msg(&home).is_err());

        let _ = fs::remove_dir_all(&home);
    }

    #[test]
    fn read_k3s_config_rejects_fifo_and_dir_and_follows_regular_symlinks() {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;
        use std::os::unix::fs::symlink;

        let home = std::env::temp_dir().join(format!(
            "kat-k8s-config-nonreg-{}-{}",
            std::process::id(),
            TMP_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&home).unwrap();
        let cfg_path = k3s_config_path(&home);

        // Symlink to a regular file is allowed.
        let target = home.join("real-k3s.yaml");
        fs::write(&target, "disable:\n  - traefik\n").unwrap();
        symlink(&target, &cfg_path).unwrap();
        assert_eq!(
            read_k3s_config(&home).unwrap().as_deref(),
            Some("disable:\n  - traefik\n")
        );
        fs::remove_file(&cfg_path).unwrap();

        // Directory is rejected.
        fs::create_dir(&cfg_path).unwrap();
        let err = read_k3s_config(&home).unwrap_err();
        assert!(err.to_string().contains("regular file"), "{err:#}");
        fs::remove_dir(&cfg_path).unwrap();

        // FIFO with no writer is rejected immediately without blocking.
        let c_path = CString::new(cfg_path.as_os_str().as_bytes()).unwrap();
        let rc = unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) };
        assert_eq!(rc, 0, "mkfifo failed: {}", io::Error::last_os_error());
        let err = read_k3s_config(&home).unwrap_err();
        assert!(err.to_string().contains("regular file"), "{err:#}");

        let _ = fs::remove_dir_all(&home);
    }

    #[test]
    fn ensure_start_supported_detects_older_guests_before_extended_start() {
        use std::os::unix::net::UnixListener;

        let home = std::env::temp_dir().join(format!(
            "kat-k8s-caps-{}-{}",
            std::process::id(),
            TMP_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let sock = socket_path(&home);
        fs::create_dir_all(sock.parent().unwrap()).unwrap();

        let empty = K8sMsg::Start {
            extra_args: vec![],
            config_yaml: None,
        };
        let extended = K8sMsg::Start {
            extra_args: vec!["--disable=traefik".into()],
            config_yaml: None,
        };

        // Empty Start does not probe or connect at all.
        ensure_start_supported(&home, &empty).unwrap();

        // 1. Old guest: closes connection on unknown Capabilities, answers Status.
        let listener = UnixListener::bind(&sock).unwrap();
        let old_guest = thread::spawn(move || {
            // First connection: Capabilities -> close immediately (simulating UnknownKind).
            let (mut c1, _) = listener.accept().unwrap();
            assert_eq!(k8s::read(&mut c1).unwrap(), Some(K8sMsg::Capabilities));
            drop(c1);
            // Second connection: Status -> StatusInfo.
            let (mut c2, _) = listener.accept().unwrap();
            assert_eq!(k8s::read(&mut c2).unwrap(), Some(K8sMsg::Status));
            k8s::write(&mut c2, &K8sMsg::StatusInfo(K8sInfo::default())).unwrap();
        });
        let err = ensure_start_supported(&home, &extended).unwrap_err();
        assert!(
            err.to_string().contains("initramfs"),
            "expected initramfs update guidance, got: {err:#}"
        );
        old_guest.join().unwrap();
        let _ = fs::remove_file(&sock);

        // 2. New guest advertising CAP_START_CONFIG succeeds.
        let listener = UnixListener::bind(&sock).unwrap();
        let new_guest = thread::spawn(move || {
            let (mut c, _) = listener.accept().unwrap();
            assert_eq!(k8s::read(&mut c).unwrap(), Some(K8sMsg::Capabilities));
            k8s::write(&mut c, &K8sMsg::CapabilitiesInfo(k8s::CAP_START_CONFIG)).unwrap();
        });
        ensure_start_supported(&home, &extended).unwrap();
        new_guest.join().unwrap();
        let _ = fs::remove_file(&sock);

        // 3. Guest returning CapabilitiesInfo(0) is also rejected clearly.
        let listener = UnixListener::bind(&sock).unwrap();
        let zero_caps_guest = thread::spawn(move || {
            let (mut c, _) = listener.accept().unwrap();
            assert_eq!(k8s::read(&mut c).unwrap(), Some(K8sMsg::Capabilities));
            k8s::write(&mut c, &K8sMsg::CapabilitiesInfo(0)).unwrap();
        });
        let err = ensure_start_supported(&home, &extended).unwrap_err();
        assert!(
            err.to_string().contains("initramfs"),
            "expected initramfs update guidance, got: {err:#}"
        );
        zero_caps_guest.join().unwrap();
        let _ = fs::remove_file(&sock);

        // 4. Unreachable guest reports connection failure, not initramfs mismatch.
        let err = ensure_start_supported(&home, &extended).unwrap_err();
        assert!(
            err.to_string().contains("not running"),
            "expected VM not running error, got: {err:#}"
        );

        let _ = fs::remove_dir_all(&home);
    }
}
