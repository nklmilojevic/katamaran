//! Host-side Kubernetes (k3s) manager:
//! - downloads and SHA-256 verifies the pinned `k3s-arm64` binary into
//!   `<home>/cache/k3s/<version>/k3s-arm64`,
//! - streams requests to the guest's k8s service over `<home>/run/k8s.sock`,
//! - relays `127.0.0.1:<api_port>` to `<home>/run/k8s-api.sock` (`K8S_API_PORT`),
//! - merges/removes the `katamaran` context in `~/.kube/config`,
//! - serves a local UDP DNS responder for `*.k8s.kat.local` -> `127.0.0.1`,
//! - and dials arbitrary Pod/Service IPs inside the guest via `K8sMsg::Connect`.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::net::{Ipv4Addr, Shutdown, SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use anyhow::{Context, bail};
use kat_proto::k8s::{self, CHUNK, K8sInfo, K8sMsg};

pub const K3S_VERSION: &str = "v1.37.2+k3s1";
pub const K3S_ARM64_SHA256: &str =
    "a779ccf1e5e2e33a62f5a29c2f1c0bbf99431e100579d4c10f0aa0742a6aa84b";
pub const K3S_ARM64_URL: &str =
    "https://github.com/k3s-io/k3s/releases/download/v1.37.2%2Bk3s1/k3s-arm64";

pub const SOCKET_NAME: &str = "k8s.sock";
pub const API_SOCKET_NAME: &str = "k8s-api.sock";
pub const KUBECONFIG_NAME: &str = "kubeconfig.yaml";
pub const DEFAULT_API_PORT: u16 = 6443;

pub const DNS_DOMAIN: &str = "k8s.kat.local";
pub const DEFAULT_DNS_PORT: u16 = 15353;
pub const RESOLVER_PATH: &str = "/etc/resolver/k8s.kat.local";

pub fn socket_path(home: &Path) -> PathBuf {
    home.join("run").join(SOCKET_NAME)
}

pub fn api_socket_path(home: &Path) -> PathBuf {
    home.join("run").join(API_SOCKET_NAME)
}

pub fn kubeconfig_path(home: &Path) -> PathBuf {
    home.join("run").join(KUBECONFIG_NAME)
}

pub fn cache_binary_path(home: &Path) -> PathBuf {
    home.join("cache")
        .join("k3s")
        .join(K3S_VERSION)
        .join("k3s-arm64")
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
    k8s::write(&mut s, req)?;
    read_answer(&mut s, progress)
}

fn read_answer(s: &mut UnixStream, progress: &mut dyn FnMut(&str)) -> anyhow::Result<K8sMsg> {
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

/// Download `k3s-arm64` into `<home>/cache/k3s/<version>/k3s-arm64` if not
/// already cached and verified, then stream it into the guest if the guest
/// doesn't already have this version installed.
pub fn ensure_installed(home: &Path, progress: &mut dyn FnMut(&str)) -> anyhow::Result<()> {
    let info = guest_status(home)?;
    if info.installed && info.version == K3S_VERSION {
        return Ok(());
    }
    let cached = ensure_cached_binary(home, progress)?;
    progress(&format!("transferring k3s {K3S_VERSION} to VM"));
    let mut s = connect(home)?;
    k8s::write(
        &mut s,
        &K8sMsg::Install {
            version: K3S_VERSION.into(),
        },
    )?;
    let mut f = File::open(&cached).with_context(|| format!("open {}", cached.display()))?;
    let mut buf = vec![0u8; CHUNK];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        k8s::write(&mut s, &K8sMsg::Data(buf[..n].to_vec()))?;
    }
    k8s::write(&mut s, &K8sMsg::DataEnd)?;
    match read_answer(&mut s, progress)? {
        K8sMsg::Done => Ok(()),
        other => bail!("unexpected install response {other:?}"),
    }
}

fn ensure_cached_binary(home: &Path, progress: &mut dyn FnMut(&str)) -> anyhow::Result<PathBuf> {
    let path = cache_binary_path(home);
    if path.is_file() && verify_sha256(&path, K3S_ARM64_SHA256).is_ok() {
        return Ok(path);
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let tmp = path.with_extension("part");
    let _ = fs::remove_file(&tmp);
    progress(&format!("downloading k3s {K3S_VERSION} (~74 MB)"));
    if let Err(e) = crate::machines::curl(K3S_ARM64_URL, &tmp) {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    progress("verifying k3s checksum");
    if let Err(e) = verify_sha256(&tmp, K3S_ARM64_SHA256) {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    fs::rename(&tmp, &path).with_context(|| format!("save {}", path.display()))?;
    Ok(path)
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

/// Relays `127.0.0.1:<port>` on macOS to `<home>/run/k8s-api.sock`.
pub struct ApiProxy {
    pub port: u16,
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl ApiProxy {
    pub fn start(api_socket: PathBuf) -> io::Result<ApiProxy> {
        let listener = TcpListener::bind(("127.0.0.1", DEFAULT_API_PORT))
            .or_else(|_| TcpListener::bind(("127.0.0.1", 0)))?;
        let addr = listener.local_addr()?;
        let port = addr.port();
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let thread = thread::Builder::new()
            .name(format!("k8s-api-{port}"))
            .spawn(move || {
                for conn in listener.incoming() {
                    if flag.load(Ordering::SeqCst) {
                        return;
                    }
                    let Ok(client) = conn else { continue };
                    let sock = api_socket.clone();
                    let _ = thread::Builder::new()
                        .name("k8s-api-conn".into())
                        .spawn(move || {
                            let _ = relay_tcp_to_unix(client, &sock);
                        });
                }
            })?;
        Ok(ApiProxy {
            port,
            addr,
            stop,
            thread: Some(thread),
        })
    }

    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect_timeout(&self.addr, Duration::from_millis(200));
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
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
    let up = thread::spawn(move || {
        let _ = io::copy(&mut c_read, &mut u_write);
        let _ = u_write.shutdown(Shutdown::Write);
    });
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
    let listener = TcpListener::bind(("127.0.0.1", local_port))
        .with_context(|| format!("bind 127.0.0.1:{local_port}"))?;
    let bound = listener.local_addr()?.port();
    println!("Forwarding 127.0.0.1:{bound} -> {guest_addr}:{guest_port} (Ctrl-C to stop)");
    for conn in listener.incoming() {
        let Ok(client) = conn else { continue };
        let home = home.to_path_buf();
        let addr = guest_addr.clone();
        thread::spawn(move || {
            if let Err(e) = relay_k8s_connect(&home, client, &addr, guest_port) {
                eprintln!("katamaran: k8s forward {addr}:{guest_port}: {e}");
            }
        });
    }
    Ok(())
}

pub fn relay_k8s_connect(
    home: &Path,
    client: TcpStream,
    addr: &str,
    port: u16,
) -> anyhow::Result<()> {
    let _ = client.set_nodelay(true);
    let mut sock = connect(home)?;
    k8s::write(
        &mut sock,
        &K8sMsg::Connect {
            addr: addr.into(),
            port,
        },
    )?;
    match k8s::read(&mut sock)? {
        Some(K8sMsg::Done) => {}
        Some(K8sMsg::Error(e)) => bail!("{e}"),
        other => bail!("unexpected connect reply {other:?}"),
    }
    let mut sock_write = sock.try_clone()?;
    let mut client_read = client.try_clone()?;
    let up = thread::spawn(move || {
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
    });
    let mut client_write = client;
    while let Ok(Some(msg)) = k8s::read(&mut sock) {
        match msg {
            K8sMsg::Data(d) => {
                if client_write.write_all(&d).is_err() {
                    break;
                }
            }
            K8sMsg::DataEnd => {
                let _ = client_write.shutdown(Shutdown::Write);
                break;
            }
            _ => break,
        }
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

/// Save `<home>/run/kubeconfig.yaml` and merge the context into `~/.kube/config`
/// (without clobbering any existing contexts).
pub fn write_and_merge_kubeconfig(
    home: &Path,
    raw: &str,
    api_port: u16,
) -> anyhow::Result<PathBuf> {
    let ctx = context_name()?;
    let rewritten = rewrite_kubeconfig(raw, &ctx, api_port);
    let local_path = kubeconfig_path(home);
    write_private_atomic(&local_path, rewritten.as_bytes())?;

    if let Some(user_home) = std::env::var_os("HOME").map(PathBuf::from) {
        let kube_dir = user_home.join(".kube");
        let user_cfg = kube_dir.join("config");
        if !user_cfg.exists() {
            write_private_atomic(&user_cfg, rewritten.as_bytes())
                .with_context(|| format!("write {}", user_cfg.display()))?;
        } else {
            merge_into_user_kubeconfig(&user_cfg, &local_path, &ctx)
                .with_context(|| format!("merge context {ctx} into {}", user_cfg.display()))?;
        }
    }
    Ok(local_path)
}

/// Merge `local_cfg` into `user_cfg` using a private temporary copy so the
/// live `~/.kube/config` is never mutated if `kubectl` is missing or fails.
fn merge_into_user_kubeconfig(user_cfg: &Path, local_cfg: &Path, ctx: &str) -> anyhow::Result<()> {
    let existing = fs::read(user_cfg).with_context(|| format!("read {}", user_cfg.display()))?;
    let parent = user_cfg
        .parent()
        .with_context(|| format!("missing parent for {}", user_cfg.display()))?;
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
        f.write_all(&existing)?;
    }

    let result = (|| -> anyhow::Result<()> {
        // Strip any stale entry for `ctx` from the scratch copy only.
        for sub in ["delete-context", "delete-cluster", "delete-user"] {
            let _ = Command::new("kubectl")
                .args(["config", sub, ctx])
                .env("KUBECONFIG", &scratch)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .context("run kubectl (is kubectl installed on PATH?)")?;
        }
        let combined = format!("{}:{}", scratch.display(), local_cfg.display());
        let out = Command::new("kubectl")
            .args(["config", "view", "--raw", "--flatten"])
            .env("KUBECONFIG", combined)
            .stdin(Stdio::null())
            .output()
            .context("run kubectl config view --raw --flatten")?;
        if !out.status.success() {
            bail!(
                "kubectl config view --raw --flatten failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        if out.stdout.is_empty() {
            bail!("kubectl config view --raw --flatten produced empty output");
        }
        write_private_atomic(user_cfg, &out.stdout)
    })();

    let _ = fs::remove_file(&scratch);
    result
}

/// Remove `ctx` from `~/.kube/config` and delete `<home>/run/kubeconfig.yaml`.
pub fn remove_kubeconfig(home: &Path, ctx: &str) {
    let _ = fs::remove_file(kubeconfig_path(home));
    let Some(user_home) = std::env::var_os("HOME").map(PathBuf::from) else {
        return;
    };
    let user_cfg = user_home.join(".kube/config");
    if !user_cfg.is_file() {
        return;
    }
    for sub in ["delete-context", "delete-cluster", "delete-user"] {
        let _ = Command::new("kubectl")
            .args(["config", sub, ctx])
            .env("KUBECONFIG", &user_cfg)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

/// Minimal UDP DNS responder for `k8s.kat.local` and `*.k8s.kat.local` -> `127.0.0.1`.
pub struct DnsServer {
    pub port: u16,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl DnsServer {
    pub fn start() -> io::Result<DnsServer> {
        let want_port = installed_dns_port().unwrap_or(DEFAULT_DNS_PORT);
        let sock = UdpSocket::bind(("127.0.0.1", want_port)).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("bind 127.0.0.1:{want_port} for *.{DNS_DOMAIN} DNS: {e}"),
            )
        })?;
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
                            if e.kind() == io::ErrorKind::WouldBlock
                                || e.kind() == io::ErrorKind::TimedOut =>
                        {
                            continue;
                        }
                        Err(_) => return,
                    }
                }
            })?;
        Ok(DnsServer {
            port,
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
}
