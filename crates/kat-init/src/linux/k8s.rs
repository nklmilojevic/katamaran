//! Optional single-node Kubernetes (k3s) inside the guest, served to the
//! host on [`kat_proto::K8S_PORT`] (see [`kat_proto::k8s`]).
//!
//! Nothing runs until Kubernetes is enabled: when disabled, the only footprint
//! is the idle vsock listener. When started, `k3s` shares Katamaran's existing
//! `containerd` (`/run/containerd/containerd.sock`), uses `host-gw` flannel
//! and `nftables` kube-proxy, and mirrors locally built Docker images from the
//! `moby` namespace into `k8s.io`.

use std::collections::{BTreeSet, HashMap};
use std::ffi::CString;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{IpAddr, Shutdown, TcpStream};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt, symlink};
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use kat_proto::k8s::{self, CHUNK, K8sInfo, K8sMsg};
use kat_proto::{K8S_PORT, Msg};

use super::machines::cmd::{pipe, run_status as run_reaped};
use super::service::{
    CONTAINERD_SOCKET, DOCKER_SOCKET, DockerStack, Notifier, Reaper, describe_status,
};
use super::sys;

const PATH: &str = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";
const ROOT: &str = "/var/lib/katamaran/k3s";
const BIN_DIR: &str = "/var/lib/katamaran/k3s/bin";
const BIN: &str = "/var/lib/katamaran/k3s/bin/k3s";
const VERSION_FILE: &str = "/var/lib/katamaran/k3s/bin/version";
const DATA_DIR: &str = "/var/lib/katamaran/k3s/data";
const KUBELET_DIR: &str = "/var/lib/katamaran/k3s/data/agent/kubelet";
const LEGACY_KUBELET_DIR: &str = "/var/lib/kubelet";
const KUBECONFIG: &str = "/var/lib/katamaran/k3s/kubeconfig.yaml";
const LOG_FILE: &str = "/var/log/k3s.log";
const PROXY_DIR: &str = "/run/katamaran/k8s-proxy";
const PROXY_SOCKET: &str = "/run/katamaran/k8s-proxy/proxy.sock";
pub const API_ADDR: &str = "127.0.0.1:6443";

const READY_TIMEOUT: Duration = Duration::from_secs(180);
const STOP_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_K8S_CONNS: usize = 256;

/// Internal k3s/Kubernetes control-plane ports that must not be auto-forwarded
/// to the Mac's `127.0.0.1` by `ports::watch` (`6443` has its own dedicated
/// vsock relay on `K8S_API_PORT`).
const INTERNAL_PORTS: [u16; 6] = [6443, 6444, 10250, 10256, 10257, 10259];

static SERVICE: OnceLock<Arc<K8s>> = OnceLock::new();
static ACTIVE: AtomicBool = AtomicBool::new(false);
static SHUTTING_DOWN: AtomicBool = AtomicBool::new(false);
static FORWARDED: Mutex<BTreeSet<u16>> = Mutex::new(BTreeSet::new());
static IMAGE_SYNC_LOCK: Mutex<()> = Mutex::new(());
static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

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

/// Wrapper around `UnixStream` that checks `SHUTTING_DOWN` on socket read
/// timeouts without losing partially read frame bytes.
struct ShutdownReader<'a>(&'a mut UnixStream);

impl Read for ShutdownReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            if SHUTTING_DOWN.load(Ordering::SeqCst) {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "VM is shutting down",
                ));
            }
            match self.0.read(buf) {
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                    ) =>
                {
                    continue;
                }
                res => return res,
            }
        }
    }
}

pub fn is_internal_port(port: u16) -> bool {
    ACTIVE.load(Ordering::Relaxed) && INTERNAL_PORTS.contains(&port)
}

/// NodePort and LoadBalancer ports exposed by Kubernetes services, reported
/// to the host by `ports::watch` so `127.0.0.1:<port>` reaches the cluster.
pub fn forwarded_ports() -> BTreeSet<u16> {
    if !ACTIVE.load(Ordering::Relaxed) {
        return BTreeSet::new();
    }
    FORWARDED.lock().unwrap().clone()
}

struct Running {
    pid: i32,
    exit_rx: Receiver<()>,
    exited: Arc<Mutex<Option<i32>>>,
    sync_stop: Arc<AtomicBool>,
    proxy_thread: Option<thread::JoinHandle<()>>,
}

struct State {
    generation: u64,
    phase: String,
    api_ready: bool,
    error: String,
    running: Option<Running>,
}

pub struct K8s {
    reaper: Reaper,
    notifier: Notifier,
    docker: DockerStack,
    op_lock: Mutex<()>,
    state: Mutex<State>,
}

/// Whether the host passed `kat.k8s=1` on the kernel command line.
pub fn enabled_in_cmdline(cmdline: &str) -> bool {
    cmdline.split_whitespace().any(|arg| arg == "kat.k8s=1")
}

/// Start listening on [`K8S_PORT`]. Does not start k3s itself.
pub fn start_service(
    reaper: Reaper,
    notifier: Notifier,
    docker: DockerStack,
) -> io::Result<Arc<K8s>> {
    let svc = Arc::new(K8s {
        reaper,
        notifier,
        docker,
        op_lock: Mutex::new(()),
        state: Mutex::new(State {
            generation: 0,
            phase: "stopped".into(),
            api_ready: false,
            error: String::new(),
            running: None,
        }),
    });
    let _ = SERVICE.set(svc.clone());
    let listener = sys::vsock_listen(K8S_PORT)?;
    let s = svc.clone();
    let active = Arc::new(AtomicUsize::new(0));
    thread::Builder::new().name("k8s".into()).spawn(move || {
        loop {
            match sys::accept(&listener) {
                Ok(fd) => {
                    let Some(guard) = ConnGuard::try_acquire(&active, MAX_K8S_CONNS) else {
                        log(&format!(
                            "connection limit ({MAX_K8S_CONNS}) reached on vsock {K8S_PORT}"
                        ));
                        continue;
                    };
                    let s = s.clone();
                    if let Err(e) =
                        thread::Builder::new()
                            .name("k8s-conn".into())
                            .spawn(move || {
                                let _guard = guard;
                                s.serve(UnixStream::from(fd));
                            })
                    {
                        log(&format!("spawn k8s-conn: {e}"));
                    }
                }
                Err(e) => {
                    log(&format!("accept on vsock {K8S_PORT}: {e}"));
                    thread::sleep(Duration::from_millis(50));
                }
            }
        }
    })?;
    Ok(svc)
}

/// Stop k3s cleanly during VM shutdown.
pub fn stop_for_shutdown() {
    SHUTTING_DOWN.store(true, Ordering::SeqCst);
    if let Some(svc) = SERVICE.get() {
        let _ = svc.stop_inner(None);
    }
}

const CRI_PLUGINS: [&str; 3] = [
    "io.containerd.grpc.v1.cri",
    "io.containerd.cri.v1.images",
    "io.containerd.cri.v1.runtime",
];

const CONTAINERD_BASE_CONFIG: &str = "/etc/containerd/config.toml";
const CONTAINERD_CRI_CONFIG: &str = "/run/katamaran/containerd-cri.toml";

fn write_atomic(target: &Path, bytes: &[u8]) -> Result<(), String> {
    let parent = target
        .parent()
        .ok_or_else(|| format!("missing parent for {}", target.display()))?;
    let target_mode = match fs::metadata(target) {
        Ok(meta) => meta.permissions().mode() & 0o7777,
        Err(e) if e.kind() == io::ErrorKind::NotFound => 0o600,
        Err(e) => return Err(format!("stat {}: {e}", target.display())),
    };
    let name = target
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "tmp".into());
    let pid = std::process::id();
    let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let tmp = parent.join(format!(".{name}.katamaran.{pid}.{seq}.tmp"));
    let _ = fs::remove_file(&tmp);
    let write_res = (|| -> io::Result<()> {
        let mut f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        f.set_permissions(fs::Permissions::from_mode(target_mode))?;
        Ok(())
    })();
    if let Err(e) = write_res {
        let _ = fs::remove_file(&tmp);
        return Err(format!("write {}: {e}", tmp.display()));
    }
    if let Err(e) = fs::rename(&tmp, target) {
        let _ = fs::remove_file(&tmp);
        return Err(format!(
            "rename {} -> {}: {e}",
            tmp.display(),
            target.display()
        ));
    }
    Ok(())
}

/// Outcome of preparing an ephemeral CRI-enabled runtime config for containerd.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerdCriConfig {
    /// Path to pass via `containerd --config`.
    pub path: String,
    /// True if the runtime config was created or its contents changed.
    pub updated: bool,
}

/// Prepare an ephemeral CRI-enabled containerd config under `/run/katamaran/`
/// derived from `/etc/containerd/config.toml` without ever modifying the
/// managed base config file.
///
/// The generated file is validated with `containerd --config <tmp> config dump`
/// before it is committed. Returns `Ok(None)` when `/etc/containerd/config.toml`
/// does not exist or does not disable CRI plugins.
pub fn ensure_containerd_cri(reaper: &Reaper) -> Result<Option<ContainerdCriConfig>, String> {
    prepare_containerd_cri_config(
        Path::new(CONTAINERD_BASE_CONFIG),
        Path::new(CONTAINERD_CRI_CONFIG),
        |tmp| validate_containerd_config(reaper, tmp),
    )
}

fn prepare_containerd_cri_config(
    base_path: &Path,
    out_path: &Path,
    validate: impl FnOnce(&Path) -> Result<(), String>,
) -> Result<Option<ContainerdCriConfig>, String> {
    let text = match fs::read_to_string(base_path) {
        Ok(t) => t,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let _ = fs::remove_file(out_path);
            return Ok(None);
        }
        Err(e) => return Err(format!("read {}: {e}", base_path.display())),
    };
    let Some(updated) = strip_cri_from_disabled_plugins(&text) else {
        let _ = fs::remove_file(out_path);
        return Ok(None);
    };

    if matches!(fs::symlink_metadata(out_path), Ok(meta) if meta.file_type().is_file() && (meta.permissions().mode() & 0o777) == 0o600)
        && fs::read_to_string(out_path).ok().as_deref() == Some(updated.as_str())
    {
        return Ok(Some(ContainerdCriConfig {
            path: out_path.to_string_lossy().into_owned(),
            updated: false,
        }));
    }

    let parent = out_path.parent().ok_or_else(|| {
        format!(
            "containerd runtime config {} has no parent directory",
            out_path.display()
        )
    })?;
    fs::create_dir_all(parent).map_err(|e| format!("create {}: {e}", parent.display()))?;

    let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let tmp = parent.join(format!(".containerd-cri.{}.{seq}.tmp", std::process::id()));
    let mut f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&tmp)
        .map_err(|e| format!("create {}: {e}", tmp.display()))?;
    if let Err(e) = f.write_all(updated.as_bytes()).and_then(|()| f.sync_all()) {
        drop(f);
        let _ = fs::remove_file(&tmp);
        return Err(format!("write {}: {e}", tmp.display()));
    }
    drop(f);

    if let Err(e) = validate(&tmp) {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }

    if let Err(e) = fs::rename(&tmp, out_path) {
        let _ = fs::remove_file(&tmp);
        return Err(format!("commit {}: {e}", out_path.display()));
    }
    Ok(Some(ContainerdCriConfig {
        path: out_path.to_string_lossy().into_owned(),
        updated: true,
    }))
}

fn validate_containerd_config(reaper: &Reaper, path: &Path) -> Result<(), String> {
    let mut cmd = Command::new("/usr/bin/containerd");
    cmd.arg("--config").arg(path).args(["config", "dump"]);
    let (code, out) = run_reaped(reaper, &mut cmd, Duration::from_secs(10))
        .map_err(|e| format!("validate containerd config {}: {e}", path.display()))?;
    if code != 0 {
        return Err(format!(
            "containerd rejected generated config {} (exit {code}): {}",
            path.display(),
            out.trim()
        ));
    }
    Ok(())
}

/// Split a TOML line into `(code, comment)` at the first unquoted `#`.
fn strip_toml_comment(line: &str) -> (&str, &str) {
    let bytes = line.as_bytes();
    let mut in_single = false;
    let mut in_double = false;
    let mut escaped = false;
    for (i, &b) in bytes.iter().enumerate() {
        if in_double {
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                in_double = false;
            }
        } else if in_single {
            if b == b'\'' {
                in_single = false;
            }
        } else if b == b'"' {
            in_double = true;
        } else if b == b'\'' {
            in_single = true;
        } else if b == b'#' {
            return (&line[..i], &line[i..]);
        }
    }
    (line, "")
}

/// Split comma-separated TOML array items, ignoring commas inside quoted strings.
fn split_toml_array_items(s: &str) -> Vec<&str> {
    let mut items = Vec::new();
    let bytes = s.as_bytes();
    let mut start = 0;
    let mut in_single = false;
    let mut in_double = false;
    let mut escaped = false;
    for (i, &b) in bytes.iter().enumerate() {
        if in_double {
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                in_double = false;
            }
        } else if in_single {
            if b == b'\'' {
                in_single = false;
            }
        } else if b == b'"' {
            in_double = true;
        } else if b == b'\'' {
            in_single = true;
        } else if b == b',' {
            items.push(&s[start..i]);
            start = i + 1;
        }
    }
    items.push(&s[start..]);
    items
}

/// Remove only CRI plugin IDs from top-level `disabled_plugins = [...]`,
/// preserving all other disabled plugins, nested `[table]` sections, inline
/// comments, and multiline arrays. Returns `None` if no CRI plugin was disabled.
fn strip_cri_from_disabled_plugins(text: &str) -> Option<String> {
    let lines: Vec<&str> = text.lines().collect();
    let mut any_changed = false;
    let mut in_table = false;
    let mut out = String::with_capacity(text.len());
    let mut i = 0;

    while i < lines.len() {
        let line = lines[i];
        let trimmed = line.trim_start();
        if trimmed.starts_with('[') {
            in_table = true;
        }
        if !in_table
            && let Some(rest) = trimmed.strip_prefix("disabled_plugins")
            && let Some(rhs) = rest.trim_start().strip_prefix('=')
        {
            // Strip any trailing `# ...` comment before inspecting `[` / `]`.
            let (code, comment) = strip_toml_comment(rhs);
            let code_trimmed = code.trim();
            if let Some(after_open) = code_trimmed.strip_prefix('[') {
                if let Some(inner) = after_open.strip_suffix(']') {
                    // Single-line array.
                    let mut line_changed = false;
                    let mut kept = Vec::new();
                    for item in split_toml_array_items(inner) {
                        let raw = item.trim();
                        if raw.is_empty() {
                            continue;
                        }
                        let unquoted = raw.trim_matches(|c| c == '"' || c == '\'');
                        if CRI_PLUGINS.contains(&unquoted) {
                            line_changed = true;
                        } else {
                            kept.push(raw);
                        }
                    }
                    if line_changed {
                        any_changed = true;
                        if !kept.is_empty() {
                            let indent = &line[..line.len() - trimmed.len()];
                            let comment_suffix = if comment.is_empty() {
                                String::new()
                            } else {
                                format!(" {comment}")
                            };
                            out.push_str(&format!(
                                "{indent}disabled_plugins = [{}]{comment_suffix}\n",
                                kept.join(", ")
                            ));
                        }
                        i += 1;
                        continue;
                    }
                } else {
                    // Multiline array: `after_open` already has any `# comment`
                    // stripped from the opening line; find the closing `]`.
                    let mut end = None;
                    for (j, l) in lines.iter().enumerate().skip(i + 1) {
                        let (lcode, _) = strip_toml_comment(l);
                        if lcode.trim_end().ends_with(']') {
                            end = Some(j);
                            break;
                        }
                    }
                    if let Some(end_idx) = end {
                        let mut body = String::from(after_open);
                        body.push('\n');
                        for l in &lines[i + 1..end_idx] {
                            let (lcode, _) = strip_toml_comment(l);
                            body.push_str(lcode);
                            body.push('\n');
                        }
                        let (end_code, end_comment) = strip_toml_comment(lines[end_idx]);
                        let end_before_close =
                            end_code.trim_end().strip_suffix(']').unwrap_or(end_code);
                        body.push_str(end_before_close);

                        let mut block_changed = false;
                        let mut kept = Vec::new();
                        for item in split_toml_array_items(&body) {
                            let raw = item.trim();
                            if raw.is_empty() {
                                continue;
                            }
                            let unquoted = raw.trim_matches(|c| c == '"' || c == '\'');
                            if CRI_PLUGINS.contains(&unquoted) {
                                block_changed = true;
                            } else {
                                kept.push(raw.to_string());
                            }
                        }
                        if block_changed {
                            any_changed = true;
                            if !kept.is_empty() {
                                let indent = &line[..line.len() - trimmed.len()];
                                let comment_suffix = if end_comment.is_empty() {
                                    String::new()
                                } else {
                                    format!(" {end_comment}")
                                };
                                out.push_str(&format!(
                                    "{indent}disabled_plugins = [{}]{comment_suffix}\n",
                                    kept.join(", ")
                                ));
                            }
                            i = end_idx + 1;
                            continue;
                        } else {
                            for l in &lines[i..=end_idx] {
                                out.push_str(l);
                                out.push('\n');
                            }
                            i = end_idx + 1;
                            continue;
                        }
                    }
                }
            }
        }
        out.push_str(line);
        out.push('\n');
        i += 1;
    }
    any_changed.then_some(out)
}

impl K8s {
    fn serve(self: &Arc<Self>, mut conn: UnixStream) {
        let req = match k8s::read(&mut conn) {
            Ok(Some(m)) => m,
            Ok(None) => return,
            Err(e) => {
                log(&format!("read request: {e}"));
                return;
            }
        };
        let reply = match req {
            K8sMsg::Status => Ok(K8sMsg::StatusInfo(self.info())),
            K8sMsg::Install { version } => self.install(&mut conn, &version).map(|()| K8sMsg::Done),
            K8sMsg::Start => self.start_inner(&mut conn).map(K8sMsg::StatusInfo),
            K8sMsg::Stop => self.stop_inner(Some(&mut conn)).map(|()| K8sMsg::Done),
            K8sMsg::Reset => self.reset_inner(&mut conn).map(|()| K8sMsg::Done),
            K8sMsg::Kubeconfig => read_kubeconfig().map(K8sMsg::KubeconfigData),
            K8sMsg::Connect { addr, port } => {
                self.connect(conn, &addr, port);
                return;
            }
            K8sMsg::SyncImages => sync_moby_images(&self.reaper).map(|()| K8sMsg::Done),
            K8sMsg::Proxy => {
                relay_unix_proxy(conn, Path::new(PROXY_SOCKET));
                return;
            }
            other => Err(format!("unexpected request {other:?}")),
        };
        let msg = match reply {
            Ok(m) => m,
            Err(e) => K8sMsg::Error(e),
        };
        let _ = k8s::write(&mut conn, &msg);
    }

    fn info(&self) -> K8sInfo {
        let st = self.state.lock().unwrap();
        let installed = Path::new(BIN).is_file();
        let version = fs::read_to_string(VERSION_FILE)
            .unwrap_or_default()
            .trim()
            .to_string();
        K8sInfo {
            state: st.phase.clone(),
            installed,
            version,
            api_ready: st.api_ready,
            error: st.error.clone(),
        }
    }

    fn install(&self, conn: &mut UnixStream, version: &str) -> Result<(), String> {
        if SHUTTING_DOWN.load(Ordering::SeqCst) {
            return Err("aborted: VM is shutting down".into());
        }
        let _guard = self.op_lock.lock().unwrap();
        if SHUTTING_DOWN.load(Ordering::SeqCst) {
            return Err("aborted: VM is shutting down".into());
        }
        if self.state.lock().unwrap().running.is_some() {
            return Err("stop Kubernetes before installing a new k3s binary".into());
        }
        let _ = k8s::write(
            &mut *conn,
            &K8sMsg::Progress(format!("installing k3s {version}")),
        );
        fs::create_dir_all(BIN_DIR).map_err(|e| format!("create {BIN_DIR}: {e}"))?;
        let tmp = format!("{BIN}.tmp");
        let _ = fs::remove_file(&tmp);
        let _ = conn.set_read_timeout(Some(Duration::from_millis(500)));
        let res = (|| -> Result<(), String> {
            let mut f = File::create(&tmp).map_err(|e| format!("create {tmp}: {e}"))?;
            let mut total_bytes = 0u64;
            let mut reader = ShutdownReader(&mut *conn);
            loop {
                match k8s::read(&mut reader).map_err(|e| format!("read binary stream: {e}"))? {
                    Some(K8sMsg::Data(chunk)) => {
                        total_bytes += chunk.len() as u64;
                        f.write_all(&chunk)
                            .map_err(|e| format!("write {tmp}: {e}"))?;
                    }
                    Some(K8sMsg::DataEnd) => break,
                    Some(other) => {
                        return Err(format!("expected Data/DataEnd, got {other:?}"));
                    }
                    None => {
                        return Err("unexpected EOF while receiving k3s binary".into());
                    }
                }
            }
            if total_bytes == 0 {
                return Err("received empty k3s binary".into());
            }
            if SHUTTING_DOWN.load(Ordering::SeqCst) {
                return Err("aborted: VM is shutting down".into());
            }
            f.sync_all().map_err(|e| format!("sync {tmp}: {e}"))?;
            drop(f);
            fs::set_permissions(&tmp, fs::Permissions::from_mode(0o755))
                .map_err(|e| format!("chmod {tmp}: {e}"))?;
            fs::rename(&tmp, BIN).map_err(|e| format!("install {BIN}: {e}"))?;
            write_atomic(Path::new(VERSION_FILE), format!("{version}\n").as_bytes())?;
            Ok(())
        })();
        let _ = conn.set_read_timeout(None);
        if res.is_err() {
            let _ = fs::remove_file(&tmp);
        } else {
            log(&format!("installed k3s {version}"));
        }
        res
    }

    fn start_inner(self: &Arc<Self>, conn: &mut UnixStream) -> Result<K8sInfo, String> {
        if SHUTTING_DOWN.load(Ordering::SeqCst) {
            return Err("aborted: VM is shutting down".into());
        }
        let _guard = self.op_lock.lock().unwrap();
        if SHUTTING_DOWN.load(Ordering::SeqCst) {
            return Err("aborted: VM is shutting down".into());
        }
        {
            let st = self.state.lock().unwrap();
            if st.phase == "running" && st.api_ready {
                drop(st);
                return Ok(self.info());
            }
        }
        if !Path::new(BIN).is_file() {
            return Err("k3s binary is not installed".into());
        }
        let _ = k8s::write(
            &mut *conn,
            &K8sMsg::Progress("preparing k3s environment".into()),
        );
        prepare_dirs_and_sysctls(&self.reaper)?;
        self.activate_containerd_cri(conn)?;
        if SHUTTING_DOWN.load(Ordering::SeqCst) {
            return Err("aborted: VM is shutting down".into());
        }

        let _ = k8s::write(&mut *conn, &K8sMsg::Progress("starting k3s server".into()));
        sys::mkdir("/var/log").map_err(|e| format!("mkdir /var/log: {e}"))?;
        let log_file = File::options()
            .create(true)
            .append(true)
            .open(LOG_FILE)
            .map_err(|e| format!("open {LOG_FILE}: {e}"))?;

        let mut cmd = Command::new(BIN);
        cmd.args([
            "server",
            &format!("--container-runtime-endpoint=unix://{CONTAINERD_SOCKET}"),
            &format!("--data-dir={DATA_DIR}"),
            &format!("--write-kubeconfig={KUBECONFIG}"),
            "--write-kubeconfig-mode=0600",
            "--node-name=katamaran",
            "--flannel-backend=host-gw",
            "--cluster-cidr=10.42.0.0/16",
            "--service-cidr=10.43.0.0/16",
            "--cluster-dns=10.43.0.10",
            "--tls-san=127.0.0.1",
            "--tls-san=localhost",
            "--tls-san=katamaran",
            "--tls-san=k8s.kat.local",
            "--https-listen-port=6443",
            "--kubelet-arg=fail-swap-on=false",
            &format!("--kubelet-arg=root-dir={KUBELET_DIR}"),
            "--kube-proxy-arg=proxy-mode=nftables",
        ])
        .env("PATH", PATH)
        .stdin(Stdio::null())
        .stdout(log_file.try_clone().map_err(|e| e.to_string())?)
        .stderr(log_file);

        enable_bridge_netfilter()?;
        // Mark active before spawning k3s so ports::watch ignores internal
        // control-plane ports (e.g. 6443) from the moment k3s binds them.
        ACTIVE.store(true, Ordering::SeqCst);
        let (pid, status_rx) = match self.reaper.spawn(&mut cmd) {
            Ok(v) => v,
            Err(e) => {
                ACTIVE.store(false, Ordering::SeqCst);
                restore_bridge_netfilter();
                return Err(format!("spawn k3s: {e}"));
            }
        };
        let exited = Arc::new(Mutex::new(None));
        let (exit_tx, exit_rx) = mpsc::channel();
        let sync_stop = Arc::new(AtomicBool::new(false));

        let my_gen = {
            let mut st = self.state.lock().unwrap();
            st.generation += 1;
            st.phase = "starting".into();
            st.api_ready = false;
            st.error.clear();
            st.running = Some(Running {
                pid,
                exit_rx,
                exited: exited.clone(),
                sync_stop: sync_stop.clone(),
                proxy_thread: None,
            });
            st.generation
        };

        // Watch process exit asynchronously so crashes are reported to the host.
        {
            let svc_ref = Arc::clone(self);
            let exited2 = exited.clone();
            let sync_stop2 = sync_stop.clone();
            let status_rx_slot = Arc::new(Mutex::new(Some(status_rx)));
            let status_rx_watch = Arc::clone(&status_rx_slot);
            if let Err(e) = thread::Builder::new()
                .name("k3s-watch".into())
                .spawn(move || {
                    let rx = status_rx_watch.lock().unwrap().take();
                    if let Some(rx) = rx
                        && let Ok(status) = rx.recv()
                    {
                        *exited2.lock().unwrap() = Some(status);
                        sync_stop2.store(true, Ordering::SeqCst);
                        let _ = exit_tx.send(());
                        // Serialize crash cleanup with start_inner/stop_inner so a
                        // replacement cluster cannot launch while this watcher is
                        // tearing down workloads or restoring sysctls. Safe against
                        // deadlock because exited2 and exit_tx were signaled above.
                        let _op = svc_ref.op_lock.lock().unwrap();
                        let (crashed, running) = {
                            let mut st = svc_ref.state.lock().unwrap();
                            // Ignore stale watchers or startup exits already handled
                            // by start_inner/stop_inner.
                            if st.generation != my_gen || st.phase != "running" {
                                return;
                            }
                            let msg = format!("k3s {}", describe_status(status));
                            st.phase = "failed".into();
                            st.api_ready = false;
                            st.error = msg.clone();
                            (msg, st.running.take())
                        };
                        if let Some(mut r) = running
                            && let Some(t) = r.proxy_thread.take()
                        {
                            let _ = t.join();
                        }
                        stop_k8s_workloads(&svc_ref.reaper);
                        ACTIVE.store(false, Ordering::SeqCst);
                        FORWARDED.lock().unwrap().clear();
                        restore_bridge_netfilter();
                        svc_ref.notifier.failed("k8s", crashed);
                    }
                })
            {
                let err = format!("spawn k3s-watch: {e}");
                sync_stop.store(true, Ordering::SeqCst);
                unsafe { libc::kill(pid, libc::SIGTERM) };
                let rx = status_rx_slot.lock().unwrap().take();
                let reaped = match rx {
                    Some(rx) => {
                        if rx.recv_timeout(STOP_TIMEOUT).is_ok() {
                            true
                        } else {
                            log(
                                "k3s did not stop in time after k3s-watch spawn failure, killing it",
                            );
                            unsafe { libc::kill(pid, libc::SIGKILL) };
                            rx.recv_timeout(Duration::from_secs(3)).is_ok()
                        }
                    }
                    None => false,
                };
                if reaped {
                    stop_k8s_workloads(&self.reaper);
                    ACTIVE.store(false, Ordering::SeqCst);
                    FORWARDED.lock().unwrap().clear();
                    restore_bridge_netfilter();
                } else {
                    log(&format!(
                        "k3s pid {pid} did not exit after SIGKILL; skipping workload teardown"
                    ));
                }
                let final_err = if reaped {
                    err
                } else {
                    format!("{err} (k3s pid {pid} did not exit within timeout)")
                };
                let mut st = self.state.lock().unwrap();
                if st.generation == my_gen {
                    st.phase = "failed".into();
                    st.api_ready = false;
                    st.error = final_err.clone();
                    if reaped {
                        st.running = None;
                    }
                }
                return Err(final_err);
            }
        }

        let abort_for_shutdown = || -> Result<K8sInfo, String> {
            let err = "aborted: VM is shutting down".to_string();
            let running = {
                let mut st = self.state.lock().unwrap();
                st.phase = "stopping".into();
                st.api_ready = false;
                st.running.take()
            };
            if let Some(r) = running {
                terminate_running(r);
            }
            stop_k8s_workloads(&self.reaper);
            ACTIVE.store(false, Ordering::SeqCst);
            FORWARDED.lock().unwrap().clear();
            restore_bridge_netfilter();
            let mut st = self.state.lock().unwrap();
            if st.generation == my_gen {
                st.phase = "stopped".into();
                st.api_ready = false;
                st.error.clear();
            }
            Err(err)
        };

        let cleanup_startup_exit = |status: i32| -> Result<K8sInfo, String> {
            let err = format!(
                "k3s exited during startup ({}); see {LOG_FILE}",
                describe_status(status)
            );
            stop_k8s_workloads(&self.reaper);
            let mut st = self.state.lock().unwrap();
            if st.generation == my_gen {
                st.phase = "failed".into();
                st.api_ready = false;
                st.error = err.clone();
                st.running = None;
            }
            ACTIVE.store(false, Ordering::SeqCst);
            FORWARDED.lock().unwrap().clear();
            restore_bridge_netfilter();
            Err(err)
        };

        let _ = k8s::write(
            &mut *conn,
            &K8sMsg::Progress("waiting for Kubernetes API server".into()),
        );
        let deadline = Instant::now() + READY_TIMEOUT;
        loop {
            if SHUTTING_DOWN.load(Ordering::SeqCst) {
                return abort_for_shutdown();
            }
            if let Some(status) = *exited.lock().unwrap() {
                return cleanup_startup_exit(status);
            }
            if api_is_ready(&self.reaper) {
                break;
            }
            if Instant::now() >= deadline {
                let err = format!(
                    "Kubernetes API not ready after {}s; see {LOG_FILE}",
                    READY_TIMEOUT.as_secs()
                );
                // Mark stopping first so the exit watcher does not race us,
                // then reap k3s and clean up workloads before restoring sysctls.
                let running = {
                    let mut st = self.state.lock().unwrap();
                    st.phase = "stopping".into();
                    st.api_ready = false;
                    st.running.take()
                };
                if let Some(r) = running {
                    terminate_running(r);
                }
                stop_k8s_workloads(&self.reaper);
                ACTIVE.store(false, Ordering::SeqCst);
                FORWARDED.lock().unwrap().clear();
                restore_bridge_netfilter();
                let mut st = self.state.lock().unwrap();
                if st.generation == my_gen {
                    st.phase = "failed".into();
                    st.error = err.clone();
                }
                return Err(err);
            }
            thread::sleep(Duration::from_millis(250));
        }

        if SHUTTING_DOWN.load(Ordering::SeqCst) {
            return abort_for_shutdown();
        }
        {
            let mut st = self.state.lock().unwrap();
            if let Some(status) = *exited.lock().unwrap() {
                drop(st);
                return cleanup_startup_exit(status);
            }
            st.phase = "running".into();
            st.api_ready = true;
            st.error.clear();
        }
        self.notifier.send(&Msg::ServiceReady {
            name: "k8s".into(),
            at_ns: sys::boottime_ns(),
        });

        // Start background Docker -> k8s.io image mirror, kubectl proxy, and Service port watcher.
        spawn_image_sync(self.reaper.clone(), sync_stop.clone());
        let proxy_thread = spawn_api_proxy(self.reaper.clone(), sync_stop.clone());
        if let Some(t) = proxy_thread {
            let mut st = self.state.lock().unwrap();
            if st.generation == my_gen
                && let Some(r) = st.running.as_mut()
            {
                r.proxy_thread = Some(t);
            }
        }
        spawn_service_port_watcher(Arc::clone(self), my_gen, sync_stop);
        log("k3s is ready");
        Ok(self.info())
    }

    fn activate_containerd_cri(&self, conn: &mut UnixStream) -> Result<(), String> {
        match ensure_containerd_cri(&self.reaper)? {
            Some(cfg) => {
                let needs_reload = cfg.updated
                    || self.docker.containerd_config().as_deref() != Some(cfg.path.as_str());
                if needs_reload {
                    let _ = k8s::write(
                        &mut *conn,
                        &K8sMsg::Progress("enabling CRI in containerd".into()),
                    );
                    self.docker
                        .reload_containerd_config(Some(cfg.path), Duration::from_secs(30))?;
                } else {
                    wait_containerd()?;
                }
            }
            None => {
                if self.docker.containerd_config().is_some() {
                    self.docker
                        .reload_containerd_config(None, Duration::from_secs(30))?;
                } else {
                    wait_containerd()?;
                }
            }
        }
        Ok(())
    }

    fn stop_inner(&self, conn: Option<&mut UnixStream>) -> Result<(), String> {
        let _guard = self.op_lock.lock().unwrap();
        self.stop_locked(conn)
    }

    /// Caller must hold `self.op_lock`.
    fn stop_locked(&self, conn: Option<&mut UnixStream>) -> Result<(), String> {
        let (running, needs_cleanup) = {
            let mut st = self.state.lock().unwrap();
            let needs_cleanup = st.running.is_some() || st.phase != "stopped";
            if !needs_cleanup {
                st.api_ready = false;
                ACTIVE.store(false, Ordering::SeqCst);
                return Ok(());
            }
            st.phase = "stopping".into();
            st.api_ready = false;
            (st.running.take(), needs_cleanup)
        };
        if let Some(c) = conn {
            let _ = k8s::write(c, &K8sMsg::Progress("stopping k3s".into()));
        }
        if let Some(r) = running {
            terminate_running(r);
        }
        if needs_cleanup {
            stop_k8s_workloads(&self.reaper);
        }
        ACTIVE.store(false, Ordering::SeqCst);
        FORWARDED.lock().unwrap().clear();
        restore_bridge_netfilter();
        let mut st = self.state.lock().unwrap();
        st.phase = "stopped".into();
        st.api_ready = false;
        st.error.clear();
        log("k3s stopped");
        Ok(())
    }

    fn reset_inner(&self, conn: &mut UnixStream) -> Result<(), String> {
        if SHUTTING_DOWN.load(Ordering::SeqCst) {
            return Err("aborted: VM is shutting down".into());
        }
        let _guard = self.op_lock.lock().unwrap();
        if SHUTTING_DOWN.load(Ordering::SeqCst) {
            return Err("aborted: VM is shutting down".into());
        }
        self.stop_locked(Some(&mut *conn))?;
        let _ = k8s::write(conn, &K8sMsg::Progress("removing cluster data".into()));
        unmount_paths_under(&[DATA_DIR, LEGACY_KUBELET_DIR])?;
        if Path::new(DATA_DIR).exists() {
            // Try btrfs subvolume delete first (instant if it is a subvolume),
            // falling back to recursive removal.
            let mut cmd = Command::new("/sbin/btrfs");
            cmd.args(["subvolume", "delete", DATA_DIR]);
            let _ = run_reaped(&self.reaper, &mut cmd, Duration::from_secs(30));
            if Path::new(DATA_DIR).exists() {
                fs::remove_dir_all(DATA_DIR).map_err(|e| format!("remove {DATA_DIR}: {e}"))?;
            }
        }
        if Path::new(LEGACY_KUBELET_DIR).exists() {
            fs::remove_dir_all(LEGACY_KUBELET_DIR)
                .map_err(|e| format!("remove {LEGACY_KUBELET_DIR}: {e}"))?;
        }
        let _ = fs::remove_file(KUBECONFIG);
        log("k3s cluster data reset");
        Ok(())
    }

    fn connect(&self, mut conn: UnixStream, addr: &str, port: u16) {
        let Ok(ip) = addr.parse::<IpAddr>() else {
            let _ = k8s::write(&mut conn, &K8sMsg::Error(format!("invalid IP {addr:?}")));
            return;
        };
        if port == 0 {
            let _ = k8s::write(&mut conn, &K8sMsg::Error("port must be non-zero".into()));
            return;
        }
        let upstream = match TcpStream::connect_timeout(&(ip, port).into(), Duration::from_secs(10))
        {
            Ok(s) => s,
            Err(e) => {
                let _ = k8s::write(
                    &mut conn,
                    &K8sMsg::Error(format!("connect {ip}:{port}: {e}")),
                );
                return;
            }
        };
        let _ = upstream.set_nodelay(true);
        relay_guest_stream(conn, upstream);
    }
}

trait GuestStream: Read + Write + Send + Sized + 'static {
    fn try_clone(&self) -> io::Result<Self>;
    fn shutdown(&self, how: Shutdown) -> io::Result<()>;
}

impl GuestStream for TcpStream {
    fn try_clone(&self) -> io::Result<Self> {
        TcpStream::try_clone(self)
    }
    fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        TcpStream::shutdown(self, how)
    }
}

impl GuestStream for UnixStream {
    fn try_clone(&self) -> io::Result<Self> {
        UnixStream::try_clone(self)
    }
    fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        UnixStream::shutdown(self, how)
    }
}

fn relay_unix_proxy(mut conn: UnixStream, socket_path: &Path) {
    let upstream = match UnixStream::connect(socket_path) {
        Ok(s) => s,
        Err(e) => {
            let _ = k8s::write(
                &mut conn,
                &K8sMsg::Error(format!("connect {}: {e}", socket_path.display())),
            );
            return;
        }
    };
    relay_guest_stream(conn, upstream);
}

fn relay_guest_stream<S: GuestStream>(mut conn: UnixStream, upstream: S) {
    if k8s::write(&mut conn, &K8sMsg::Done).is_err() {
        return;
    }
    let Ok(mut up_read) = upstream.try_clone() else {
        return;
    };
    let Ok(mut conn_write) = conn.try_clone() else {
        return;
    };
    let down = match thread::Builder::new()
        .name("k8s-connect-down".into())
        .spawn(move || {
            let mut buf = vec![0u8; CHUNK];
            loop {
                match up_read.read(&mut buf) {
                    Ok(0) => {
                        let _ = k8s::write(&mut conn_write, &K8sMsg::DataEnd);
                        return;
                    }
                    Ok(n) => {
                        if k8s::write(&mut conn_write, &K8sMsg::Data(buf[..n].to_vec())).is_err() {
                            return;
                        }
                    }
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                    Err(_) => {
                        let _ = k8s::write(&mut conn_write, &K8sMsg::DataEnd);
                        return;
                    }
                }
            }
        }) {
        Ok(h) => h,
        Err(e) => {
            log(&format!("spawn k8s-connect-down: {e}"));
            let _ = upstream.shutdown(Shutdown::Both);
            return;
        }
    };
    let mut up_write = upstream;
    let mut clean_eof = false;
    loop {
        match k8s::read(&mut conn) {
            Ok(Some(K8sMsg::Data(d))) => {
                if up_write.write_all(&d).is_err() {
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
        let _ = up_write.shutdown(Shutdown::Write);
    } else {
        let _ = up_write.shutdown(Shutdown::Both);
    }
    let _ = down.join();
}

fn prepare_dirs_and_sysctls(reaper: &Reaper) -> Result<(), String> {
    fs::create_dir_all(ROOT).map_err(|e| format!("create {ROOT}: {e}"))?;
    if !Path::new(DATA_DIR).exists() {
        // Prefer a btrfs subvolume so reset is instant; fall back to mkdir.
        let mut cmd = Command::new("/sbin/btrfs");
        cmd.args(["subvolume", "create", DATA_DIR]);
        let subvol_ok =
            run_reaped(reaper, &mut cmd, Duration::from_secs(10)).is_ok_and(|(code, _)| code == 0);
        if !subvol_ok {
            fs::create_dir_all(DATA_DIR).map_err(|e| format!("create {DATA_DIR}: {e}"))?;
        }
    }
    let cni_conf_target = format!("{DATA_DIR}/agent/etc/cni/net.d");
    fs::create_dir_all(&cni_conf_target).map_err(|e| format!("create {cni_conf_target}: {e}"))?;
    ensure_symlink("/etc/cni/net.d", &cni_conf_target)?;
    ensure_symlink("/opt/cni/bin", &format!("{DATA_DIR}/data/current/bin"))?;

    Ok(())
}

const BRIDGE_NF_SYSCTLS: [&str; 3] = [
    "/proc/sys/net/ipv4/ip_forward",
    "/proc/sys/net/bridge/bridge-nf-call-iptables",
    "/proc/sys/net/bridge/bridge-nf-call-ip6tables",
];
static SAVED_BRIDGE_NF: Mutex<Option<[Option<String>; 3]>> = Mutex::new(None);

fn enable_bridge_netfilter() -> Result<(), String> {
    let mut saved = SAVED_BRIDGE_NF.lock().unwrap();
    let prev = match *saved {
        Some(ref p) => p.clone(),
        None => {
            let p = [
                fs::read_to_string(BRIDGE_NF_SYSCTLS[0]).ok(),
                fs::read_to_string(BRIDGE_NF_SYSCTLS[1]).ok(),
                fs::read_to_string(BRIDGE_NF_SYSCTLS[2]).ok(),
            ];
            *saved = Some(p.clone());
            p
        }
    };
    for (i, path) in BRIDGE_NF_SYSCTLS.iter().enumerate() {
        if let Err(e) = fs::write(path, "1\n") {
            for j in (0..i).rev() {
                if let Some(ref v) = prev[j]
                    && let Err(re) = fs::write(BRIDGE_NF_SYSCTLS[j], v)
                {
                    log(&format!("rollback {}: {re}", BRIDGE_NF_SYSCTLS[j]));
                }
            }
            *saved = None;
            return Err(format!("enable {path}: {e}"));
        }
    }
    Ok(())
}

fn restore_bridge_netfilter() {
    let Some(prev) = SAVED_BRIDGE_NF.lock().unwrap().take() else {
        return;
    };
    for (path, val) in BRIDGE_NF_SYSCTLS.iter().zip(prev) {
        if let Some(v) = val
            && let Err(e) = fs::write(path, v)
        {
            log(&format!("restore {path}: {e}"));
        }
    }
}

fn terminate_running(mut r: Running) {
    r.sync_stop.store(true, Ordering::SeqCst);
    if r.exited.lock().unwrap().is_none() {
        unsafe { libc::kill(r.pid, libc::SIGTERM) };
        if r.exit_rx.recv_timeout(STOP_TIMEOUT).is_err() {
            log("k3s did not stop in time, killing it");
            unsafe { libc::kill(r.pid, libc::SIGKILL) };
            let _ = r.exit_rx.recv_timeout(Duration::from_secs(3));
        }
    }
    if let Some(t) = r.proxy_thread.take() {
        let _ = t.join();
    }
}

/// Unescape octal escape sequences (`\040`, `\011`, `\012`, `\134`) used in
/// `/proc/self/mountinfo` mount point paths.
fn unescape_mountinfo_path(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' && i + 3 < bytes.len() {
            let d0 = bytes[i + 1];
            let d1 = bytes[i + 2];
            let d2 = bytes[i + 3];
            if (b'0'..=b'3').contains(&d0)
                && (b'0'..=b'7').contains(&d1)
                && (b'0'..=b'7').contains(&d2)
            {
                let val = ((d0 - b'0') << 6) | ((d1 - b'0') << 3) | (d2 - b'0');
                out.push(val);
                i += 4;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn is_under_prefix(path: &str, prefix: &str) -> bool {
    let prefix = prefix.trim_end_matches('/');
    path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with('/'))
}

/// Parse `/proc/self/mountinfo` and return all mount points matching any of
/// `prefixes` (retaining repeated/stacked mounts), sorted deepest nested mount
/// first and topmost stacked mount first.
fn mounts_under_prefixes(mountinfo: &str, prefixes: &[&str]) -> Vec<String> {
    let mut mounts: Vec<(usize, String)> = Vec::new();
    for (idx, line) in mountinfo.lines().enumerate() {
        let Some(raw_mp) = line.split_whitespace().nth(4) else {
            continue;
        };
        let mp = unescape_mountinfo_path(raw_mp);
        if prefixes.iter().any(|p| is_under_prefix(&mp, p)) {
            mounts.push((idx, mp));
        }
    }
    mounts.sort_by(|(ia, a), (ib, b)| b.len().cmp(&a.len()).then_with(|| ib.cmp(ia)));
    mounts.into_iter().map(|(_, mp)| mp).collect()
}

fn umount_detach(target: &str) -> io::Result<()> {
    let c = CString::new(target)?;
    if unsafe { libc::umount2(c.as_ptr(), libc::MNT_DETACH) } < 0 {
        let e = io::Error::last_os_error();
        return Err(io::Error::new(
            e.kind(),
            format!("umount(MNT_DETACH) {target}: {e}"),
        ));
    }
    Ok(())
}

fn unmount_paths_under(prefixes: &[&str]) -> Result<(), String> {
    let text = fs::read_to_string("/proc/self/mountinfo")
        .map_err(|e| format!("read /proc/self/mountinfo: {e}"))?;
    let mut failures = Vec::new();
    for mp in mounts_under_prefixes(&text, prefixes) {
        if let Err(e) = sys::umount(&mp)
            && let Err(de) = umount_detach(&mp)
        {
            let msg = format!("unmount {mp} failed ({e}); detach failed: {de}");
            log(&msg);
            failures.push(msg);
        }
    }
    let after = fs::read_to_string("/proc/self/mountinfo")
        .map_err(|e| format!("re-read /proc/self/mountinfo: {e}"))?;
    let remaining = mounts_under_prefixes(&after, prefixes);
    if !remaining.is_empty() {
        let detail = if failures.is_empty() {
            format!("mounts still active: {}", remaining.join(", "))
        } else {
            format!(
                "mounts still active ({}): {}",
                remaining.join(", "),
                failures.join("; ")
            )
        };
        return Err(detail);
    }
    Ok(())
}

/// Stop and remove Kubernetes tasks/containers in the shared containerd
/// `k8s.io` namespace and tear down CNI interfaces, kube-proxy nftables rules,
/// and kubelet/data-dir mounts without touching Docker's `moby` namespace.
fn stop_k8s_workloads(reaper: &Reaper) {
    let mut ls = Command::new("/usr/bin/ctr");
    ls.args(["-a", CONTAINERD_SOCKET, "-n", "k8s.io", "tasks", "ls", "-q"]);
    if let Ok((0, out, _)) = run_reaped_split(reaper, &mut ls, Duration::from_secs(10)) {
        let ids: Vec<&str> = out
            .lines()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect();
        for id in &ids {
            let mut kill = Command::new("/usr/bin/ctr");
            kill.args([
                "-a",
                CONTAINERD_SOCKET,
                "-n",
                "k8s.io",
                "tasks",
                "kill",
                "-a",
                "-s",
                "SIGTERM",
                id,
            ]);
            let _ = run_reaped(reaper, &mut kill, Duration::from_secs(5));
        }
        if !ids.is_empty() {
            thread::sleep(Duration::from_millis(500));
        }
        for id in &ids {
            let mut kill = Command::new("/usr/bin/ctr");
            kill.args([
                "-a",
                CONTAINERD_SOCKET,
                "-n",
                "k8s.io",
                "tasks",
                "kill",
                "-a",
                "-s",
                "SIGKILL",
                id,
            ]);
            let _ = run_reaped(reaper, &mut kill, Duration::from_secs(5));
            let mut rm = Command::new("/usr/bin/ctr");
            rm.args([
                "-a",
                CONTAINERD_SOCKET,
                "-n",
                "k8s.io",
                "tasks",
                "rm",
                "-f",
                id,
            ]);
            let _ = run_reaped(reaper, &mut rm, Duration::from_secs(5));
        }
    }
    let mut cls = Command::new("/usr/bin/ctr");
    cls.args([
        "-a",
        CONTAINERD_SOCKET,
        "-n",
        "k8s.io",
        "containers",
        "ls",
        "-q",
    ]);
    if let Ok((0, out, _)) = run_reaped_split(reaper, &mut cls, Duration::from_secs(10)) {
        for id in out.lines().map(str::trim).filter(|s| !s.is_empty()) {
            let mut rm = Command::new("/usr/bin/ctr");
            rm.args([
                "-a",
                CONTAINERD_SOCKET,
                "-n",
                "k8s.io",
                "containers",
                "rm",
                id,
            ]);
            let _ = run_reaped(reaper, &mut rm, Duration::from_secs(5));
        }
    }
    for iface in ["cni0", "flannel.1"] {
        let mut del = Command::new("/sbin/ip");
        del.args(["link", "delete", iface]);
        let _ = run_reaped(reaper, &mut del, Duration::from_secs(5));
    }
    let nft_bin = [
        "/usr/sbin/nft",
        "/sbin/nft",
        "/usr/bin/nft",
        "/var/lib/katamaran/k3s/data/current/bin/nft",
    ]
    .into_iter()
    .find(|p| Path::new(p).is_file())
    .unwrap_or("nft");
    for family in ["ip", "ip6"] {
        let mut del = Command::new(nft_bin);
        del.args(["delete", "table", family, "kube-proxy"]);
        let _ = run_reaped(reaper, &mut del, Duration::from_secs(5));
    }
    if let Err(e) = unmount_paths_under(&[DATA_DIR, LEGACY_KUBELET_DIR]) {
        log(&format!("cleanup mounts: {e}"));
    }
}

fn ensure_symlink(link: &str, target: &str) -> Result<(), String> {
    let p = Path::new(link);
    if let Some(parent) = p.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("create {}: {e}", parent.display()))?;
    }
    match fs::symlink_metadata(p) {
        Ok(meta) if meta.file_type().is_symlink() => {
            if fs::read_link(p).ok().as_deref() == Some(Path::new(target)) {
                return Ok(());
            }
            fs::remove_file(p).map_err(|e| format!("replace symlink {link}: {e}"))?;
        }
        Ok(meta) if meta.is_dir() => {
            // Only remove an empty directory; never recursively delete unrelated files.
            fs::remove_dir(p).map_err(|e| {
                format!(
                    "refusing to replace non-empty directory {link} with symlink to {target}: {e}"
                )
            })?;
        }
        Ok(_) => {
            return Err(format!(
                "refusing to replace existing non-symlink file {link} with symlink to {target}"
            ));
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(format!("stat {link}: {e}")),
    }
    symlink(target, p).map_err(|e| format!("symlink {link} -> {target}: {e}"))
}

fn wait_containerd() -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if SHUTTING_DOWN.load(Ordering::SeqCst) {
            return Err("aborted: VM is shutting down".into());
        }
        if UnixStream::connect(CONTAINERD_SOCKET).is_ok() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(50));
    }
    Err(format!("containerd socket {CONTAINERD_SOCKET} not ready"))
}

/// Drain `r` completely to EOF so the child never blocks on a full pipe, while
/// retaining at most `limit` bytes for diagnostics or parsing.
fn drain_capped(mut r: impl Read, limit: usize) -> String {
    let mut captured = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        match r.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => {
                let room = limit.saturating_sub(captured.len());
                if room > 0 {
                    captured.extend_from_slice(&chunk[..n.min(room)]);
                }
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => break,
        }
    }
    String::from_utf8_lossy(&captured).into_owned()
}

/// Run `cmd` under PID 1's reaper capturing `stdout` and `stderr` separately
/// so warnings printed to stderr cannot corrupt stdout parsing.
fn run_reaped_split(
    reaper: &Reaper,
    cmd: &mut Command,
    timeout: Duration,
) -> io::Result<(i32, String, String)> {
    let (out_r, out_w) = pipe()?;
    let (err_r, err_w) = pipe()?;
    cmd.env("PATH", PATH)
        .stdin(Stdio::null())
        .stdout(Stdio::from(out_w))
        .stderr(Stdio::from(err_w));
    let (pid, exited) = reaper.spawn(cmd)?;
    cmd.stdout(Stdio::null()).stderr(Stdio::null());

    let out_t = match thread::Builder::new()
        .name("k8s-cmd-out".into())
        .spawn(move || drain_capped(File::from(out_r), 1 << 20))
    {
        Ok(h) => h,
        Err(e) => {
            unsafe { libc::kill(pid, libc::SIGKILL) };
            let _ = exited.recv_timeout(Duration::from_secs(2));
            return Err(e);
        }
    };
    let err_t = match thread::Builder::new()
        .name("k8s-cmd-err".into())
        .spawn(move || drain_capped(File::from(err_r), 64 << 10))
    {
        Ok(h) => h,
        Err(e) => {
            unsafe { libc::kill(pid, libc::SIGKILL) };
            let _ = exited.recv_timeout(Duration::from_secs(2));
            let _ = out_t.join();
            return Err(e);
        }
    };

    let status = match exited.recv_timeout(timeout) {
        Ok(s) => s,
        Err(RecvTimeoutError::Timeout) => {
            unsafe { libc::kill(pid, libc::SIGKILL) };
            let _ = exited.recv_timeout(Duration::from_secs(2));
            let _ = out_t.join();
            let _ = err_t.join();
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("{:?} timed out", cmd.get_program()),
            ));
        }
        Err(RecvTimeoutError::Disconnected) => -1,
    };
    let stdout = out_t.join().unwrap_or_default();
    let stderr = err_t.join().unwrap_or_default();
    let code = if libc::WIFEXITED(status) {
        libc::WEXITSTATUS(status)
    } else {
        128 + libc::WTERMSIG(status)
    };
    Ok((code, stdout, stderr))
}

fn api_is_ready(reaper: &Reaper) -> bool {
    if !Path::new(KUBECONFIG).is_file() {
        return false;
    }
    if TcpStream::connect_timeout(&API_ADDR.parse().unwrap(), Duration::from_secs(1)).is_err() {
        return false;
    }
    let mut cmd = Command::new(BIN);
    cmd.args([
        "kubectl",
        "--kubeconfig",
        KUBECONFIG,
        "get",
        "serviceaccount",
        "default",
        "-n",
        "default",
    ]);
    matches!(
        run_reaped(reaper, &mut cmd, Duration::from_secs(5)),
        Ok((0, _))
    )
}

fn read_kubeconfig() -> Result<String, String> {
    fs::read_to_string(KUBECONFIG).map_err(|e| format!("read {KUBECONFIG}: {e}"))
}

/// Create `dir` as a root-only (mode 0700) non-symlink directory so that the
/// credentialed `kubectl proxy` Unix socket inside it is never reachable by
/// unprivileged or host-network workloads.
fn prepare_proxy_dir(dir: &Path) -> io::Result<()> {
    if let Some(parent) = dir.parent() {
        fs::create_dir_all(parent)?;
    }
    match fs::symlink_metadata(dir) {
        Ok(meta) if meta.file_type().is_symlink() => {
            return Err(io::Error::other(format!(
                "refusing symlinked proxy directory {}",
                dir.display()
            )));
        }
        Ok(meta) if !meta.file_type().is_dir() => {
            return Err(io::Error::other(format!(
                "proxy directory {} is not a directory",
                dir.display()
            )));
        }
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            fs::create_dir(dir)?;
        }
        Err(e) => return Err(e),
    }
    fs::set_permissions(dir, fs::Permissions::from_mode(0o700))
}

/// Keeps `kubectl proxy` running on a root-only Unix socket (`PROXY_SOCKET`)
/// until `stop`, restarting it if it exits, and removes the socket when `stop`
/// is set. Running on a private Unix socket keeps the credentialed proxy off
/// the guest loopback shared with `hostNetwork: true` pods and containers.
fn spawn_api_proxy(reaper: Reaper, stop: Arc<AtomicBool>) -> Option<thread::JoinHandle<()>> {
    match thread::Builder::new()
        .name("k8s-proxy".into())
        .spawn(move || {
            while !stop.load(Ordering::Acquire) {
                if let Err(e) = prepare_proxy_dir(Path::new(PROXY_DIR)) {
                    log(&format!("prepare {PROXY_DIR}: {e}"));
                    sleep_unless(&stop, Duration::from_secs(2));
                    continue;
                }
                let _ = fs::remove_file(PROXY_SOCKET);
                let mut cmd = Command::new(BIN);
                cmd.args([
                    "kubectl",
                    "--kubeconfig",
                    KUBECONFIG,
                    "proxy",
                    &format!("--unix-socket={PROXY_SOCKET}"),
                ])
                .env("PATH", PATH)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
                // Restrict socket permissions from birth (no group/other bits).
                unsafe {
                    cmd.pre_exec(|| {
                        libc::umask(0o077);
                        Ok(())
                    });
                }
                let (pid, status_rx) = match reaper.spawn(&mut cmd) {
                    Ok(v) => v,
                    Err(e) => {
                        log(&format!("spawn kubectl proxy: {e}"));
                        sleep_unless(&stop, Duration::from_secs(2));
                        continue;
                    }
                };
                loop {
                    if stop.load(Ordering::Acquire) {
                        if status_rx.try_recv().is_err() {
                            unsafe { libc::kill(pid, libc::SIGTERM) };
                            if status_rx.recv_timeout(Duration::from_secs(3)).is_err() {
                                unsafe { libc::kill(pid, libc::SIGKILL) };
                                let _ = status_rx.recv_timeout(Duration::from_secs(1));
                            }
                        }
                        let _ = fs::remove_file(PROXY_SOCKET);
                        return;
                    }
                    match status_rx.recv_timeout(Duration::from_millis(250)) {
                        Ok(status) => {
                            log(&format!("kubectl proxy {}", describe_status(status)));
                            break;
                        }
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                        Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    }
                }
                let _ = fs::remove_file(PROXY_SOCKET);
                sleep_unless(&stop, Duration::from_secs(1));
            }
            let _ = fs::remove_file(PROXY_SOCKET);
        }) {
        Ok(h) => Some(h),
        Err(e) => {
            log(&format!("spawn k8s-proxy thread: {e}"));
            None
        }
    }
}

fn sleep_unless(stop: &AtomicBool, d: Duration) {
    let end = Instant::now() + d;
    while !stop.load(Ordering::Acquire) && Instant::now() < end {
        thread::sleep(Duration::from_millis(100));
    }
}

/// Periodically read NodePort and LoadBalancer ports from Kubernetes services
/// so `ports::watch` forwards them on `127.0.0.1:<port>` on macOS (kube-proxy
/// in nftables mode and ServiceLB use DNAT rules rather than listening sockets).
fn spawn_service_port_watcher(svc: Arc<K8s>, my_gen: u64, stop: Arc<AtomicBool>) {
    let _ = thread::Builder::new()
        .name("k8s-ports".into())
        .spawn(move || {
            let update_if_current = |ports: Option<BTreeSet<u16>>| {
                let st = svc.state.lock().unwrap();
                if ACTIVE.load(Ordering::SeqCst) && st.generation == my_gen {
                    let mut fwd = FORWARDED.lock().unwrap();
                    match ports {
                        Some(p) => *fwd = p,
                        None => fwd.clear(),
                    }
                }
            };
            while !stop.load(Ordering::Relaxed) {
                if let Some(ports) = query_service_ports(&svc.reaper)
                    && !stop.load(Ordering::Relaxed)
                {
                    update_if_current(Some(ports));
                }
                for _ in 0..8 {
                    if stop.load(Ordering::Relaxed) {
                        update_if_current(None);
                        return;
                    }
                    thread::sleep(Duration::from_millis(250));
                }
            }
            update_if_current(None);
        });
}

fn query_service_ports(reaper: &Reaper) -> Option<BTreeSet<u16>> {
    let mut cmd = Command::new(BIN);
    cmd.args([
        "kubectl",
        "--kubeconfig",
        KUBECONFIG,
        "get",
        "svc",
        "-A",
        "-o",
        "jsonpath={range .items[*]}{.spec.type}{\" \"}{range .spec.ports[*]}{.protocol}:{.port}:{.nodePort}{\" \"}{end}{\"\\n\"}{end}",
    ]);
    let (0, stdout, _) = run_reaped_split(reaper, &mut cmd, Duration::from_secs(5)).ok()? else {
        return None;
    };
    Some(parse_service_ports(&stdout))
}

fn parse_service_ports(out: &str) -> BTreeSet<u16> {
    let mut ports = BTreeSet::new();
    for line in out.lines() {
        let mut fields = line.split_whitespace();
        let Some(kind) = fields.next() else {
            continue;
        };
        if kind != "NodePort" && kind != "LoadBalancer" {
            continue;
        }
        for spec in fields {
            let mut parts = spec.split(':');
            let proto = parts.next().unwrap_or("TCP");
            let svc_port = parts
                .next()
                .and_then(|s| s.parse::<u16>().ok())
                .unwrap_or(0);
            let node_port = parts
                .next()
                .and_then(|s| s.parse::<u16>().ok())
                .unwrap_or(0);
            if !proto.is_empty() && !proto.eq_ignore_ascii_case("TCP") {
                continue;
            }
            if node_port != 0 && !INTERNAL_PORTS.contains(&node_port) {
                ports.insert(node_port);
            }
            if kind == "LoadBalancer" && svc_port != 0 && !INTERNAL_PORTS.contains(&svc_port) {
                ports.insert(svc_port);
            }
        }
    }
    ports
}

/// Mirror locally built / pulled Docker images from containerd's `moby`
/// namespace into Kubernetes's `k8s.io` namespace. Both namespaces live in the
/// same containerd content store, so blobs are deduplicated on disk.
fn spawn_image_sync(reaper: Reaper, stop: Arc<AtomicBool>) {
    let _ = thread::Builder::new()
        .name("k8s-img-sync".into())
        .spawn(move || {
            let _ = sync_moby_images(&reaper);
            while !stop.load(Ordering::Relaxed) {
                if let Err(e) = watch_docker_image_events(&reaper, &stop) {
                    if stop.load(Ordering::Relaxed) {
                        return;
                    }
                    log(&format!("image sync watcher: {e}"));
                    for _ in 0..20 {
                        if stop.load(Ordering::Relaxed) {
                            return;
                        }
                        thread::sleep(Duration::from_millis(250));
                    }
                }
            }
        });
}

fn watch_docker_image_events(reaper: &Reaper, stop: &AtomicBool) -> io::Result<()> {
    let mut stream = UnixStream::connect(DOCKER_SOCKET)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    // HTTP/1.0 avoids chunked framing; Docker streams one JSON object per line.
    write!(
        stream,
        "GET /events?filters=%7B%22type%22%3A%5B%22image%22%5D%7D HTTP/1.0\r\nHost: docker\r\n\r\n"
    )?;
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    // Skip HTTP headers.
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => return Ok(()),
            Ok(_) if line == "\r\n" || line == "\n" => break,
            Ok(_) => continue,
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut =>
            {
                if stop.load(Ordering::Relaxed) {
                    return Ok(());
                }
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    loop {
        if stop.load(Ordering::Relaxed) {
            return Ok(());
        }
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => return Ok(()),
            Ok(_) => {
                if is_docker_image_sync_event(&line)
                    && let Err(e) = sync_moby_images(reaper)
                {
                    log(&format!("background image sync: {e}"));
                }
            }
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut =>
            {
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}

fn is_docker_image_sync_event(line: &str) -> bool {
    if line.trim().is_empty() {
        return false;
    }
    let v = match serde_json::from_str::<serde_json::Value>(line) {
        Ok(v) => v,
        Err(e) => {
            log(&format!("malformed Docker event JSON: {e}"));
            return false;
        }
    };
    let action = v
        .get("Action")
        .or_else(|| v.get("status"))
        .and_then(|a| a.as_str())
        .unwrap_or("");
    matches!(action, "tag" | "pull" | "load" | "import" | "commit")
}

fn sync_moby_images(reaper: &Reaper) -> Result<(), String> {
    let _sync_guard = IMAGE_SYNC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let moby = list_ctr_images(reaper, "moby")?;
    let k8s = list_ctr_images(reaper, "k8s.io")?;
    let mut failures = Vec::new();
    for (name, digest) in moby {
        if name.starts_with("sha256:") || name.contains("<none>") {
            continue;
        }
        if k8s.get(&name).is_some_and(|d| d == &digest) {
            continue;
        }
        if let Err(e) = mirror_one_image(reaper, &name) {
            log(&format!("mirror image {name}: {e}"));
            failures.push(format!("{name}: {e}"));
        }
    }
    if !failures.is_empty() {
        return Err(format!("failed to mirror images: {}", failures.join("; ")));
    }
    Ok(())
}

/// Returns `image_ref -> digest` for all images in a containerd namespace.
fn list_ctr_images(reaper: &Reaper, ns: &str) -> Result<HashMap<String, String>, String> {
    let mut cmd = Command::new("/usr/bin/ctr");
    cmd.args(["-a", CONTAINERD_SOCKET, "-n", ns, "images", "ls"]);
    let (code, stdout, stderr) = run_reaped_split(reaper, &mut cmd, Duration::from_secs(15))
        .map_err(|e| format!("ctr images ls ({ns}): {e}"))?;
    if code != 0 {
        let detail = if stderr.trim().is_empty() {
            stdout.trim()
        } else {
            stderr.trim()
        };
        return Err(format!("ctr images ls ({ns}) failed: {detail}"));
    }
    let mut map = HashMap::new();
    for line in stdout.lines().skip(1) {
        let mut fields = line.split_whitespace();
        let Some(name) = fields.next() else {
            continue;
        };
        let _media_type = fields.next();
        let Some(digest) = fields.next() else {
            continue;
        };
        map.insert(name.to_string(), digest.to_string());
    }
    Ok(map)
}

/// Stream an image from `moby` to `k8s.io` via two separate Reaper-managed
/// `ctr` processes connected by an OS pipe, verifying both exit statuses.
fn mirror_one_image(reaper: &Reaper, name: &str) -> Result<(), String> {
    let timeout = Duration::from_secs(120);
    let (pipe_r, pipe_w) = pipe().map_err(|e| format!("pipe: {e}"))?;
    let (exp_err_r, exp_err_w) = pipe().map_err(|e| format!("pipe: {e}"))?;
    let (imp_err_r, imp_err_w) = pipe().map_err(|e| format!("pipe: {e}"))?;

    let mut exp = Command::new("/usr/bin/ctr");
    exp.args([
        "-a",
        CONTAINERD_SOCKET,
        "-n",
        "moby",
        "images",
        "export",
        "-",
        name,
    ])
    .env("PATH", PATH)
    .stdin(Stdio::null())
    .stdout(Stdio::from(pipe_w))
    .stderr(Stdio::from(exp_err_w));
    let (exp_pid, exp_rx) = reaper
        .spawn(&mut exp)
        .map_err(|e| format!("spawn ctr export: {e}"))?;
    exp.stdout(Stdio::null()).stderr(Stdio::null());

    let mut imp = Command::new("/usr/bin/ctr");
    imp.args([
        "-a",
        CONTAINERD_SOCKET,
        "-n",
        "k8s.io",
        "images",
        "import",
        "--digests",
        "-",
    ])
    .env("PATH", PATH)
    .stdin(Stdio::from(pipe_r))
    .stdout(Stdio::null())
    .stderr(Stdio::from(imp_err_w));
    let (imp_pid, imp_rx) = match reaper.spawn(&mut imp) {
        Ok(v) => v,
        Err(e) => {
            unsafe { libc::kill(exp_pid, libc::SIGKILL) };
            let _ = exp_rx.recv_timeout(Duration::from_secs(2));
            return Err(format!("spawn ctr import: {e}"));
        }
    };
    imp.stdin(Stdio::null()).stderr(Stdio::null());

    let kill_both = || {
        unsafe {
            libc::kill(exp_pid, libc::SIGKILL);
            libc::kill(imp_pid, libc::SIGKILL);
        }
        let _ = exp_rx.recv_timeout(Duration::from_secs(2));
        let _ = imp_rx.recv_timeout(Duration::from_secs(2));
    };

    let exp_err_t = match thread::Builder::new()
        .name("k8s-ctr-exp-err".into())
        .spawn(move || {
            drain_capped(File::from(exp_err_r), 64 << 10)
                .trim()
                .to_string()
        }) {
        Ok(h) => h,
        Err(e) => {
            kill_both();
            return Err(format!("spawn ctr export stderr reader: {e}"));
        }
    };
    let imp_err_t = match thread::Builder::new()
        .name("k8s-ctr-imp-err".into())
        .spawn(move || {
            drain_capped(File::from(imp_err_r), 64 << 10)
                .trim()
                .to_string()
        }) {
        Ok(h) => h,
        Err(e) => {
            kill_both();
            let _ = exp_err_t.join();
            return Err(format!("spawn ctr import stderr reader: {e}"));
        }
    };

    let deadline = Instant::now() + timeout;
    let wait_one = |pid: i32, rx: &Receiver<i32>, label: &str| -> Result<i32, String> {
        let left = deadline.saturating_duration_since(Instant::now());
        match rx.recv_timeout(left) {
            Ok(s) => Ok(if libc::WIFEXITED(s) {
                libc::WEXITSTATUS(s)
            } else {
                128 + libc::WTERMSIG(s)
            }),
            Err(_) => {
                unsafe { libc::kill(pid, libc::SIGKILL) };
                let _ = rx.recv_timeout(Duration::from_secs(2));
                Err(format!("{label} timed out"))
            }
        }
    };

    let exp_res = wait_one(exp_pid, &exp_rx, "ctr export");
    let imp_res = wait_one(imp_pid, &imp_rx, "ctr import");
    let exp_err = exp_err_t.join().unwrap_or_default();
    let imp_err = imp_err_t.join().unwrap_or_default();

    let exp_code = exp_res?;
    let imp_code = imp_res?;
    if exp_code != 0 {
        return Err(format!("ctr export exited {exp_code}: {exp_err}"));
    }
    if imp_code != 0 {
        return Err(format!("ctr import exited {imp_code}: {imp_err}"));
    }
    Ok(())
}

fn log(msg: &str) {
    eprintln!("kat-init: k8s: {msg}");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn temp_dir(name: &str) -> PathBuf {
        let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("kat-init-k8s-{name}-{}-{seq}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn strip_cri_preserves_other_disabled_plugins_and_tables() {
        let input = concat!(
            "version = 4\n",
            "disabled_plugins = [\"io.containerd.grpc.v1.cri\", \"io.containerd.snapshotter.v1.zfs\"]\n",
            "[plugins.example]\n",
            "  disabled_plugins = [\"io.containerd.grpc.v1.cri\"]\n",
        );
        let out = strip_cri_from_disabled_plugins(input).unwrap();
        assert_eq!(
            out,
            concat!(
                "version = 4\n",
                "disabled_plugins = [\"io.containerd.snapshotter.v1.zfs\"]\n",
                "[plugins.example]\n",
                "  disabled_plugins = [\"io.containerd.grpc.v1.cri\"]\n",
            )
        );
    }

    #[test]
    fn strip_cri_returns_none_when_no_cri_disabled() {
        let input = "version = 4\ndisabled_plugins = [\"io.containerd.snapshotter.v1.zfs\"]\n";
        assert_eq!(strip_cri_from_disabled_plugins(input), None);
    }

    #[test]
    fn strip_cri_handles_multiple_lines_and_preserves_non_cri() {
        let input = "\
version = 2
disabled_plugins = [\"io.containerd.grpc.v1.cri\", \"other.plugin\"]
[plugins.\"example\"]
  disabled_plugins = [\"keep.me\", \"also.keep\"] # untouched
";
        let got = strip_cri_from_disabled_plugins(input).unwrap();
        assert_eq!(
            got,
            "\
version = 2
disabled_plugins = [\"other.plugin\"]
[plugins.\"example\"]
  disabled_plugins = [\"keep.me\", \"also.keep\"] # untouched
"
        );
        assert!(strip_cri_from_disabled_plugins(&got).is_none());
    }

    #[test]
    fn strip_cri_handles_inline_comments() {
        let input_keep =
            "disabled_plugins = [\"io.containerd.grpc.v1.cri\", \"foo\"] # keep comment\n";
        let got = strip_cri_from_disabled_plugins(input_keep).unwrap();
        assert_eq!(got, "disabled_plugins = [\"foo\"] # keep comment\n");

        let input_all_cri = "disabled_plugins = [\"io.containerd.grpc.v1.cri\"] # disable cri\n";
        let got_all = strip_cri_from_disabled_plugins(input_all_cri).unwrap();
        assert_eq!(got_all, "");
    }

    #[test]
    fn strip_cri_handles_multiline_arrays() {
        let input = "\
version = 2
disabled_plugins = [
  \"io.containerd.grpc.v1.cri\", # cri grpc
  \"io.containerd.cri.v1.images\",
  \"io.containerd.snapshotter.v1.aufs\",
] # end
[other]
disabled_plugins = [
  \"keep.one\",
  \"keep.two\",
]
";
        let got = strip_cri_from_disabled_plugins(input).unwrap();
        assert_eq!(
            got,
            "\
version = 2
disabled_plugins = [\"io.containerd.snapshotter.v1.aufs\"] # end
[other]
disabled_plugins = [
  \"keep.one\",
  \"keep.two\",
]
"
        );
        assert!(strip_cri_from_disabled_plugins(&got).is_none());

        let only_cri_multiline = "\
version = 2
disabled_plugins = [ # opening comment ]
  \"io.containerd.grpc.v1.cri\",
  \"io.containerd.cri.v1.runtime\",
]
";
        assert_eq!(
            strip_cri_from_disabled_plugins(only_cri_multiline).unwrap(),
            "version = 2\n"
        );

        let opening_comment_with_kept = "\
version = 2
disabled_plugins = [ # note
  \"io.containerd.grpc.v1.cri\",
  \"io.containerd.snapshotter.v1.zfs\",
]
";
        assert_eq!(
            strip_cri_from_disabled_plugins(opening_comment_with_kept).unwrap(),
            "version = 2\ndisabled_plugins = [\"io.containerd.snapshotter.v1.zfs\"]\n"
        );
    }

    #[test]
    fn write_atomic_preserves_existing_permissions_and_restricts_new_files() {
        let dir = temp_dir("atomic");
        let existing = dir.join("config.toml");
        fs::write(&existing, "old\n").unwrap();
        fs::set_permissions(&existing, fs::Permissions::from_mode(0o600)).unwrap();
        write_atomic(&existing, b"new\n").unwrap();
        assert_eq!(fs::read_to_string(&existing).unwrap(), "new\n");
        assert_eq!(
            fs::metadata(&existing).unwrap().permissions().mode() & 0o777,
            0o600
        );

        let new_file = dir.join("version");
        write_atomic(&new_file, b"v1.37.2+k3s1\n").unwrap();
        assert_eq!(
            fs::metadata(&new_file).unwrap().permissions().mode() & 0o777,
            0o600
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn prepare_cri_config_never_modifies_base_file_or_symlink() {
        let dir = temp_dir("symlink");
        let real_base = dir.join("managed.toml");
        let base_link = dir.join("config.toml");
        let out_path = dir.join("run/containerd-cri.toml");

        let original = concat!(
            "version = 4\n",
            "disabled_plugins = [\"io.containerd.grpc.v1.cri\", \"io.containerd.cri.v1.images\", \"io.containerd.cri.v1.runtime\"]\n",
            "[grpc]\n",
            "  address = \"/run/containerd/containerd.sock\"\n",
        );
        fs::write(&real_base, original).unwrap();
        fs::set_permissions(&real_base, fs::Permissions::from_mode(0o644)).unwrap();
        symlink(&real_base, &base_link).unwrap();

        let cfg = prepare_containerd_cri_config(&base_link, &out_path, |_| Ok(()))
            .unwrap()
            .unwrap();
        assert!(cfg.updated);
        assert_eq!(cfg.path, out_path.to_string_lossy());

        // Managed config symlink, target, contents, and permissions remain untouched.
        assert!(
            fs::symlink_metadata(&base_link)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(fs::read_link(&base_link).unwrap(), real_base);
        assert_eq!(fs::read_to_string(&real_base).unwrap(), original);
        assert_eq!(
            fs::metadata(&real_base).unwrap().permissions().mode() & 0o777,
            0o644
        );

        // Runtime override is private and stripped of CRI plugins.
        assert_eq!(
            fs::symlink_metadata(&out_path)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        let generated = fs::read_to_string(&out_path).unwrap();
        assert!(!generated.contains("disabled_plugins"));
        assert!(generated.contains("address = \"/run/containerd/containerd.sock\""));

        // Second call with unchanged base is idempotent (updated = false).
        let second = prepare_containerd_cri_config(&base_link, &out_path, |_| {
            panic!("validator should not rerun when generated config is already up to date")
        })
        .unwrap()
        .unwrap();
        assert!(!second.updated);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn prepare_cri_config_rejects_invalid_output_without_committing() {
        let dir = temp_dir("invalid");
        let base = dir.join("config.toml");
        let out_path = dir.join("run/containerd-cri.toml");
        fs::write(
            &base,
            "version = 4\ndisabled_plugins = [\"io.containerd.grpc.v1.cri\"]\n",
        )
        .unwrap();

        let err = prepare_containerd_cri_config(&base, &out_path, |_| {
            Err("invalid containerd config".into())
        })
        .unwrap_err();
        assert!(err.contains("invalid containerd config"));
        assert!(!out_path.exists());

        // No leftover temp files in output directory.
        let leftover: Vec<_> = fs::read_dir(out_path.parent().unwrap()).unwrap().collect();
        assert!(leftover.is_empty());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn prepare_cri_config_removes_stale_override_when_not_needed() {
        let dir = temp_dir("cleanup");
        let base = dir.join("config.toml");
        let out_path = dir.join("run/containerd-cri.toml");
        fs::create_dir_all(out_path.parent().unwrap()).unwrap();
        fs::write(&out_path, "stale\n").unwrap();
        fs::write(&base, "version = 4\n").unwrap();

        let res = prepare_containerd_cri_config(&base, &out_path, |_| Ok(())).unwrap();
        assert_eq!(res, None);
        assert!(!out_path.exists());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn mounts_under_prefixes_sorts_deepest_first_retains_stacked_and_unescapes() {
        let mountinfo = "\
10 1 0:1 / / rw - ext4 /dev/vda rw
20 10 0:2 / /var/lib/katamaran/k3s/data rw - btrfs /dev/vdb rw
21 20 0:3 / /var/lib/katamaran/k3s/data/agent/kubelet/pods/a/volumes/k8s.io~secret/tok rw - tmpfs tmpfs rw
22 20 0:4 / /var/lib/katamaran/k3s/data/agent/kubelet/pods/a rw - tmpfs tmpfs rw
25 22 0:7 / /var/lib/katamaran/k3s/data/agent/kubelet/pods/a rw - overlay overlay rw
23 10 0:5 / /var/lib/kubelet/pods/my\\040pod/vol rw - tmpfs tmpfs rw
24 10 0:6 / /var/lib/kubelet2/unrelated rw - tmpfs tmpfs rw
";
        let got = mounts_under_prefixes(mountinfo, &[DATA_DIR, LEGACY_KUBELET_DIR]);
        assert_eq!(
            got,
            vec![
                "/var/lib/katamaran/k3s/data/agent/kubelet/pods/a/volumes/k8s.io~secret/tok",
                "/var/lib/katamaran/k3s/data/agent/kubelet/pods/a",
                "/var/lib/katamaran/k3s/data/agent/kubelet/pods/a",
                "/var/lib/kubelet/pods/my pod/vol",
                "/var/lib/katamaran/k3s/data",
            ]
        );
    }

    #[test]
    fn drain_capped_reads_past_limit_to_eof() {
        let data = vec![b'x'; 128 << 10];
        let mut cursor = io::Cursor::new(data);
        let out = drain_capped(&mut cursor, 64 << 10);
        assert_eq!(out.len(), 64 << 10);
        assert_eq!(cursor.position(), 128 << 10);
    }

    #[test]
    fn conn_guard_enforces_limit() {
        let active = Arc::new(AtomicUsize::new(0));
        let g1 = ConnGuard::try_acquire(&active, 1).unwrap();
        assert!(ConnGuard::try_acquire(&active, 1).is_none());
        drop(g1);
        assert!(ConnGuard::try_acquire(&active, 1).is_some());
        assert_eq!(active.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn parses_service_ports_correctly() {
        let out = "\
ClusterIP TCP:80:
NodePort TCP:80:30080 UDP:53:30053
LoadBalancer TCP:8080:31080 TCP:6443:31443
";
        let ports = parse_service_ports(out);
        assert_eq!(ports, BTreeSet::from([8080, 30080, 31080, 31443]));
    }

    #[test]
    fn prepare_proxy_dir_sets_private_mode_and_rejects_symlinks() {
        let base = temp_dir("proxy-dir");
        let dir = base.join("k8s-proxy");
        prepare_proxy_dir(&dir).unwrap();
        assert_eq!(
            fs::symlink_metadata(&dir).unwrap().permissions().mode() & 0o777,
            0o700
        );

        // Loosened permissions are tightened back to 0700.
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o755)).unwrap();
        prepare_proxy_dir(&dir).unwrap();
        assert_eq!(
            fs::symlink_metadata(&dir).unwrap().permissions().mode() & 0o777,
            0o700
        );

        // Symlinked proxy directory is rejected.
        let link = base.join("proxy-link");
        symlink(&dir, &link).unwrap();
        assert!(prepare_proxy_dir(&link).is_err());

        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn relay_unix_proxy_forwards_bytes_over_private_socket() {
        use std::os::unix::net::UnixListener;

        let base = temp_dir("proxy-relay");
        let dir = base.join("k8s-proxy");
        prepare_proxy_dir(&dir).unwrap();
        let sock = dir.join("proxy.sock");
        let listener = UnixListener::bind(&sock).unwrap();

        let upstream = thread::spawn(move || {
            let (mut s, _) = listener.accept().unwrap();
            let mut buf = [0u8; 64];
            let n = s.read(&mut buf).unwrap();
            s.write_all(&buf[..n]).unwrap();
        });

        let (mut client, server) = UnixStream::pair().unwrap();
        let sock_for_relay = sock.clone();
        let relay = thread::spawn(move || relay_unix_proxy(server, &sock_for_relay));

        assert_eq!(k8s::read(&mut client).unwrap(), Some(K8sMsg::Done));
        k8s::write(&mut client, &K8sMsg::Data(b"GET /api/v1/pods".to_vec())).unwrap();
        k8s::write(&mut client, &K8sMsg::DataEnd).unwrap();

        let mut received = Vec::new();
        loop {
            match k8s::read(&mut client).unwrap() {
                Some(K8sMsg::Data(d)) => received.extend_from_slice(&d),
                Some(K8sMsg::DataEnd) | None => break,
                other => panic!("unexpected frame {other:?}"),
            }
        }
        assert_eq!(received, b"GET /api/v1/pods");
        relay.join().unwrap();
        upstream.join().unwrap();
        let _ = fs::remove_dir_all(&base);
    }
}
