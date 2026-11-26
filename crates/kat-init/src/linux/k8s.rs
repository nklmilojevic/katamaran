//! Optional single-node Kubernetes (k3s) inside the guest, served to the
//! host on [`kat_proto::K8S_PORT`] (see [`kat_proto::k8s`]).
//!
//! Nothing runs until Kubernetes is enabled: when disabled, the only footprint
//! is the idle vsock listener. When started, `k3s` shares Katamaran's existing
//! `containerd` (`/run/containerd/containerd.sock`), uses `host-gw` flannel
//! and `nftables` kube-proxy, and mirrors locally built Docker images from the
//! `moby` namespace into `k8s.io`.

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::ffi::CString;
use std::fs::{self, DirBuilder, File, OpenOptions};
use std::hash::Hasher;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{IpAddr, Shutdown, TcpStream};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt, symlink};
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use kat_proto::k3s_config::normalize_k3s_config_yaml;
use kat_proto::k8s::{self, CHUNK, K8sInfo, K8sMsg};
use kat_proto::{K8S_PORT, Msg};

use super::machines::cmd::{pipe, run_status as run_reaped};
use super::service::{
    CONTAINERD_SOCKET, DOCKER_SOCKET, DockerStack, Notifier, Reaper, describe_status,
};
use super::sys;
use crate::containerd_grpc;
use crate::oci_rewrite;

const PATH: &str = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";
const ROOT: &str = "/var/lib/katamaran/k3s";
const BIN_DIR: &str = "/var/lib/katamaran/k3s/bin";
const BIN: &str = "/var/lib/katamaran/k3s/bin/k3s";
const VERSION_FILE: &str = "/var/lib/katamaran/k3s/bin/version";
const DATA_DIR: &str = "/var/lib/katamaran/k3s/data";
const DATA_VERSION_FILE: &str = "/var/lib/katamaran/k3s/data/.katamaran-version";
/// Copy of the datastore version kept beside `state.db` in `server/db/`,
/// written before every launch of k3s against an existing datastore. Unlike
/// `.katamaran-version`, it travels with any backup or restore of `server/` or
/// of the `server/db/` directory, so an unmarked datastore that a
/// tracking-enabled release ever opened still records the newest k3s that may
/// have opened it. Excluded from `snapshot_datastore`.
const SERVER_VERSION_STAMP: &str = ".katamaran-k3s-version";
const KUBELET_DIR: &str = "/var/lib/katamaran/k3s/data/agent/kubelet";
const LEGACY_KUBELET_DIR: &str = "/var/lib/kubelet";
const KUBECONFIG: &str = "/var/lib/katamaran/k3s/kubeconfig.yaml";
const KUBECONFIG_MODE: &str = "0600";
/// Katamaran-owned copy of the host's `~/.katamaran/k3s-config.yaml`, selected
/// via `K3S_CONFIG_FILE` only when supplied so any manually managed
/// `/etc/rancher/k3s/config.yaml` inside the guest is left untouched.
const K3S_CONFIG_PATH: &str = "/var/lib/katamaran/k3s/config.yaml";
const LOG_FILE: &str = "/var/log/k3s.log";
const PROXY_DIR: &str = "/run/katamaran/k8s-proxy";
const PROXY_SOCKET: &str = "/run/katamaran/k8s-proxy/proxy.sock";
const API_PORT: &str = "6443";
pub const API_ADDR: &str = "127.0.0.1:6443";

const READY_TIMEOUT: Duration = Duration::from_secs(180);
const STOP_TIMEOUT: Duration = Duration::from_secs(15);
const IMAGE_RM_BATCH_SIZE: usize = 64;
const IMAGE_LS_TIMEOUT: Duration = Duration::from_secs(30);
const IMAGE_LS_STDOUT_LIMIT: usize = 8 << 20;
const IMAGE_RM_BATCH_TIMEOUT: Duration = Duration::from_secs(30);
const IMAGE_RM_SYNC_TIMEOUT: Duration = Duration::from_secs(180);
const PROGRESS_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const IMAGE_GC_LEASE_PREFIX: &str = "katamaran-k8s-reset-gc";
const IMAGE_GC_LEASE_EXPIRES: &str = "5m";
const MAX_K8S_CONNS: usize = 256;

/// Internal k3s/Kubernetes control-plane ports that must not be auto-forwarded
/// to the Mac's `127.0.0.1` by `ports::watch` (`6443` has its own dedicated
/// vsock relay on `K8S_API_PORT`).
const INTERNAL_PORTS: [u16; 6] = [6443, 6444, 10250, 10256, 10257, 10259];

static SERVICE: OnceLock<Arc<K8s>> = OnceLock::new();
static ACTIVE: AtomicBool = AtomicBool::new(false);
static SHUTTING_DOWN: AtomicBool = AtomicBool::new(false);
static FORWARDED: Mutex<BTreeSet<u16>> = Mutex::new(BTreeSet::new());
static IMAGE_SYNC_LOCK: Mutex<SyncLogState> = Mutex::new(SyncLogState::new());
static TMP_SEQ: AtomicU64 = AtomicU64::new(0);
/// A k3s child that survived SIGKILL after its `Running` handle was dropped,
/// tracked by its own exit notification (never by a reusable pid lookup).
/// While it has not exited, nothing may read or modify the datastore.
struct UnreapedK3s {
    pid: i32,
    has_exited: Box<dyn FnMut() -> bool + Send>,
}

static UNREAPED_K3S: Mutex<Option<UnreapedK3s>> = Mutex::new(None);

fn note_unreaped_k3s(pid: i32, has_exited: impl FnMut() -> bool + Send + 'static) {
    *UNREAPED_K3S.lock().unwrap_or_else(|e| e.into_inner()) = Some(UnreapedK3s {
        pid,
        has_exited: Box::new(has_exited),
    });
}

/// Refuse datastore operations while an earlier k3s child has not exited.
fn ensure_no_unreaped_k3s() -> Result<(), String> {
    let mut slot = UNREAPED_K3S.lock().unwrap_or_else(|e| e.into_inner());
    let Some(unreaped) = slot.as_mut() else {
        return Ok(());
    };
    if (unreaped.has_exited)() {
        *slot = None;
        return Ok(());
    }
    Err(format!(
        "an earlier k3s (pid {}) did not exit and may still be using the cluster data; \
         restart the VM (`kat restart`) before starting, installing, marking, or resetting Kubernetes",
        unreaped.pid
    ))
}

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

/// The `k8s_args` and canonical JSON output of `k3s-config.yaml` (or `None`
/// when absent) a k3s instance was started with.
type StartSettings = (Vec<String>, Option<String>);

fn check_running_settings_match(
    running: &StartSettings,
    requested_args: &[String],
    requested_config_json: Option<&str>,
) -> Result<(), String> {
    if running.0 == requested_args && running.1.as_deref() == requested_config_json {
        return Ok(());
    }
    Err(
        "Kubernetes is already running with different k8s_disable, k8s_args or k3s-config.yaml settings; run `kat k8s stop` and start it again to apply them"
            .into(),
    )
}

struct Running {
    pid: i32,
    settings: StartSettings,
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
    write_atomic_inner(target, bytes, None)
}

fn write_atomic_mode(target: &Path, bytes: &[u8], mode: u32) -> Result<(), String> {
    write_atomic_inner(target, bytes, Some(mode))
}

fn write_atomic_inner(target: &Path, bytes: &[u8], forced_mode: Option<u32>) -> Result<(), String> {
    let parent = target
        .parent()
        .ok_or_else(|| format!("missing parent for {}", target.display()))?;
    let target_mode = match forced_mode {
        Some(mode) => mode,
        None => match fs::metadata(target) {
            Ok(meta) => meta.permissions().mode() & 0o7777,
            Err(e) if e.kind() == io::ErrorKind::NotFound => 0o600,
            Err(e) => return Err(format!("stat {}: {e}", target.display())),
        },
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
            .custom_flags(libc::O_NOFOLLOW)
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
    sync_dir(parent)
}

/// fsync `dir` so a rename or link inside it survives a crash. The datastore
/// version files must be durable before k3s is allowed to touch the datastore.
fn sync_dir(dir: &Path) -> Result<(), String> {
    File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(|e| format!("sync {}: {e}", dir.display()))
}

/// Parse `(major, minor)` from a k3s release tag such as `v1.37.2+k3s1`.
fn parse_k3s_minor(version: &str) -> Option<(u32, u32)> {
    // Strict: the guard must never accept a malformed tag such as
    // `v1.37.2+k3sgarbage`; anything that is not a canonical release tag is
    // treated as unreadable and fails closed.
    parse_strict_k3s_tag(version)
}

/// True once k3s has initialized server state under `data_dir`.
/// `prepare_dirs_and_sysctls` only creates `agent/etc/cni/net.d`, never `server/`.
fn datastore_exists(data_dir: &Path) -> Result<bool, String> {
    let server_dir = data_dir.join("server");
    match fs::symlink_metadata(&server_dir) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(format!("stat {}: {e}", server_dir.display())),
    }
}

fn server_version_stamp(data_dir: &Path) -> PathBuf {
    data_dir.join("server/db").join(SERVER_VERSION_STAMP)
}

/// True when the stamp would be written: `server/` and `server/db/` exist.
fn server_db_dir_exists(data_dir: &Path) -> Result<bool, String> {
    for dir in [data_dir.join("server"), data_dir.join("server/db")] {
        match fs::symlink_metadata(&dir) {
            Ok(meta) if meta.file_type().is_dir() => {}
            Ok(_) => {
                return Err(format!(
                    "{} is not a directory; refusing to record its k3s version",
                    dir.display()
                ));
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(format!("stat {}: {e}", dir.display())),
        }
    }
    Ok(true)
}

/// Record `version` in `server/db/SERVER_VERSION_STAMP` when that directory
/// exists (a fresh datastore gets its stamp on its next start).
fn write_server_version_stamp(data_dir: &Path, version: &str) -> Result<(), String> {
    if !server_db_dir_exists(data_dir)? {
        return Ok(());
    }
    let stamp = server_version_stamp(data_dir);
    if matches!(read_marker_file(&stamp)?, MarkerFile::Present(v) if v == version) {
        // Already visible, but an earlier rename may not have been made durable
        // (its directory sync failed), so sync again rather than trusting it.
        return sync_dir(&data_dir.join("server/db"));
    }
    write_atomic(&stamp, format!("{version}\n").as_bytes())
}

fn read_trimmed_file(path: &Path) -> Result<Option<String>, String> {
    match fs::read_to_string(path) {
        Ok(s) => {
            let trimmed = s.trim();
            Ok((!trimmed.is_empty()).then(|| trimmed.to_string()))
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(format!("read {}: {e}", path.display())),
    }
}

/// Oldest supported Kubernetes minor. k3s is launched with
/// `--kube-proxy-arg=proxy-mode=nftables`, and kube-proxy's nftables mode
/// (`NFTablesProxyMode`) is beta and enabled by default only from v1.31, so
/// older releases would fail to start kube-proxy. k3s v1.31.0–v1.31.5 bundle
/// k3s-root v0.14.0 without `nft`; kube-proxy then resolves the rootfs
/// `nftables` package's `/usr/sbin/nft` through `PATH` (see `guest/rootfs/build.sh`
/// and `path_resolves_rootfs_nft_for_oldest_supported_k3s`). `scripts/k3s-nft-check.sh`
/// runs a release in the guest's pinned Alpine image with these flags and checks that
/// kube-proxy builds its nftables tables with the rootfs `nft`.
const MIN_K8S_MINOR: u32 = 31;
const SQLITE3_BIN: &str = "/usr/bin/sqlite3";
/// Scratch root for WAL/journal inspection copies of `state.db`. `/run` is a
/// tmpfs, so a copy (which contains cluster secrets) never reaches the
/// persistent data disk and cannot outlive the VM or survive `kat k8s reset`.
const STATE_DB_SCRATCH_ROOT: &str = "/run/katamaran";
/// Upper bound on the bytes copied into `STATE_DB_SCRATCH_ROOT` (database plus
/// sidecars); larger unmarked datastores fall back to `kat k8s mark-version`.
const STATE_DB_MAX_SCRATCH_BYTES: u64 = 256 << 20;
/// katamaran runs a single node; allow a few stale rows but no more.
const STATE_DB_MAX_NODE_ROWS: usize = 4;
/// A real Node object is a few KiB. The query's hex output for every row must
/// fit the 1 MiB stdout capture of `run_reaped_split`, or the trailing `END`
/// would be cut off and detection would fail silently.
const STATE_DB_MAX_BLOB_BYTES: usize = 96 << 10;
// The query returns up to one extra row to detect overflow.
const _: () =
    assert!((STATE_DB_MAX_NODE_ROWS + 1) * (2 * STATE_DB_MAX_BLOB_BYTES + 32) + 64 < 1 << 20);
/// Every k3s release a pre-tracking katamaran release (v0.5.0 through v0.10.0)
/// could run: those releases always ran their pinned build and never wrote
/// `.katamaran-version`, while every tracking-enabled release writes the
/// marker before launching k3s. An unmarked datastore that katamaran itself
/// created, and whose `server/` carries no `SERVER_VERSION_STAMP` (written before
/// every tracking-era launch), can therefore only have been opened by one of these builds, so a
/// `state.db` node version matching one of them is corroborated by both the
/// datastore and katamaran's release history. Any other detected version is
/// only a lower bound (a newer server may have opened the datastore before its
/// kubelet reported status) and requires an explicit `kat k8s mark-version`.
const PRE_TRACKING_K3S_VERSIONS: &[&str] = &["v1.37.2+k3s1"];

/// Tools and scratch space used to inspect an unmarked `state.db`.
struct StateDbProbe<'a> {
    sqlite3_bin: &'a Path,
    scratch_root: &'a Path,
    /// Bound on the WAL-recovery copy (`STATE_DB_MAX_SCRATCH_BYTES` in production).
    max_copy_bytes: u64,
}

fn state_db_probe() -> StateDbProbe<'static> {
    StateDbProbe {
        sqlite3_bin: Path::new(SQLITE3_BIN),
        scratch_root: Path::new(STATE_DB_SCRATCH_ROOT),
        max_copy_bytes: STATE_DB_MAX_SCRATCH_BYTES,
    }
}

/// Strictly parse a k3s release tag (`v<major>.<minor>.<patch>[-<pre>]+k3s<n>`)
/// with no surrounding whitespace, returning `(major, minor)`.
fn parse_strict_k3s_tag(v: &str) -> Option<(u32, u32)> {
    // Digits only, without leading zeros: release tags never pad numbers.
    let canonical_number = |n: &str| {
        !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()) && (n == "0" || !n.starts_with('0'))
    };
    let rest = v.strip_prefix('v')?;
    let (kube, build) = rest.split_once("+k3s")?;
    if !canonical_number(build) {
        return None;
    }
    let (core, pre) = match kube.split_once('-') {
        Some((c, p)) => (c, Some(p)),
        None => (kube, None),
    };
    if let Some(p) = pre
        && (p.is_empty() || !p.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'.'))
    {
        return None;
    }
    let parts: Vec<u32> = core
        .split('.')
        .map(|n| {
            if !canonical_number(n) {
                return None;
            }
            n.parse().ok()
        })
        .collect::<Option<_>>()?;
    let [major, minor, _patch] = parts[..] else {
        return None;
    };
    Some((major, minor))
}

/// Validate a k3s release tag (`v<major>.<minor>.<patch>[-<pre>]+k3s<n>` with
/// `major == 1` and `minor >= MIN_K8S_MINOR`), matching the host daemon's config check.
fn validate_k3s_release_tag(v: &str) -> Result<(), String> {
    let Some((major, minor)) = parse_strict_k3s_tag(v) else {
        return Err(format!(
            "k3s version {v:?} is not a valid release tag such as v1.37.2+k3s1"
        ));
    };
    if major != 1 || minor < MIN_K8S_MINOR {
        return Err(format!(
            "k3s version must be v1.{MIN_K8S_MINOR} or newer, got {v}"
        ));
    }
    Ok(())
}

/// Atomically create a new mode-0600 file at `target` without ever overwriting
/// an existing file or following a symlink at `target`.
fn write_atomic_new(target: &Path, bytes: &[u8]) -> Result<(), String> {
    let parent = target
        .parent()
        .ok_or_else(|| format!("missing parent for {}", target.display()))?;
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
            .custom_flags(libc::O_NOFOLLOW)
            .open(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        Ok(())
    })();
    if let Err(e) = write_res {
        let _ = fs::remove_file(&tmp);
        return Err(format!("write {}: {e}", tmp.display()));
    }
    let link_res = fs::hard_link(&tmp, target);
    let _ = fs::remove_file(&tmp);
    match link_res {
        Ok(()) => sync_dir(parent).inspect_err(|_| {
            // Do not leave a marker in place that the caller was told failed.
            let _ = fs::remove_file(target);
        }),
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Err(format!(
            "datastore version marker {} already exists; refusing to overwrite an existing marker",
            target.display()
        )),
        Err(e) => Err(format!("create {}: {e}", target.display())),
    }
}

enum MarkerFile {
    Missing,
    Empty,
    NonRegular,
    Present(String),
}

fn read_marker_file(path: &Path) -> Result<MarkerFile, String> {
    let meta = match fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(MarkerFile::Missing),
        Err(e) => return Err(format!("stat {}: {e}", path.display())),
    };
    if !meta.file_type().is_file() {
        return Ok(MarkerFile::NonRegular);
    }
    let mut f = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|e| format!("open {}: {e}", path.display()))?;
    let mut content = String::new();
    f.read_to_string(&mut content)
        .map_err(|e| format!("read {}: {e}", path.display()))?;
    let trimmed = content.trim();
    if trimmed.is_empty() {
        Ok(MarkerFile::Empty)
    } else {
        Ok(MarkerFile::Present(trimmed.to_string()))
    }
}

fn corrupt_datastore_version_error(data_version_file: &Path, reason: &str) -> String {
    format!(
        "datastore version marker {} {reason}; cannot verify minor-version compatibility. \
         While Kubernetes is stopped, either restore a matching datastore backup together \
         with its {} version marker, or back up any state you need and run `kat k8s reset` \
         to delete cluster data and persistent volumes before starting fresh.",
        data_version_file.display(),
        data_version_file.display(),
    )
}

fn missing_datastore_version_error(data_dir: &Path, data_version_file: &Path) -> String {
    format!(
        "existing cluster data under {} has no version marker ({}) and its k3s version \
         could not be verified from server/db/state.db; cannot verify minor-version \
         compatibility. While the VM is running and Kubernetes is stopped, if you have \
         independently verified the k3s release tag that last opened this datastore \
         (for example from a backup or an older katamaran release), run \
         `kat k8s mark-version <version>` to record it (see the Kubernetes guide's \
         unmarked-cluster migration steps). Otherwise, back up any state you need and \
         run `kat k8s reset` to delete cluster data and persistent volumes before \
         starting fresh.",
        data_dir.display(),
        data_version_file.display(),
    )
}

struct ScratchDir(PathBuf);

impl Drop for ScratchDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn encode_sqlite_uri_path(path: &Path) -> String {
    let mut out = String::new();
    for &b in path.as_os_str().as_bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'/' | b'.' | b'-' | b'_' | b'~' | b'+' | b':')
        {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Copy the regular file `src` to a new `dst`, refusing symlinks, non-regular
/// files, and anything that would push `*budget` below zero.
fn copy_regular_file_bounded(src: &Path, dst: &Path, budget: &mut u64) -> io::Result<()> {
    let src_file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(src)?;
    if !src_file.metadata()?.file_type().is_file() {
        return Err(io::Error::other(format!(
            "{} is not a regular file",
            src.display()
        )));
    }
    let mut dst_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(dst)?;
    // Read at most one byte past the budget so growth after the size check is caught.
    let copied = io::copy(&mut src_file.take(budget.saturating_add(1)), &mut dst_file)?;
    if copied > *budget {
        return Err(io::Error::other(format!(
            "{} exceeds the inspection copy limit",
            src.display()
        )));
    }
    *budget -= copied;
    Ok(())
}

fn inspect_regular_companion(path: &Path) -> Result<Option<u64>, String> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_file() => Ok(Some(meta.len())),
        Ok(_) => Err(format!("{} is not a regular file", path.display())),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(format!("stat {}: {e}", path.display())),
    }
}

fn decode_hex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    let hex_val = |b: u8| -> Option<u8> {
        match b {
            b'0'..=b'9' => Some(b - b'0'),
            b'a'..=b'f' => Some(b - b'a' + 10),
            b'A'..=b'F' => Some(b - b'A' + 10),
            _ => None,
        }
    };
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.as_chunks::<2>().0 {
        let hi = hex_val(pair[0])?;
        let lo = hex_val(pair[1])?;
        out.push((hi << 4) | lo);
    }
    Some(out)
}

fn read_proto_varint(buf: &[u8], pos: &mut usize) -> Option<u64> {
    let mut val = 0u64;
    let mut shift = 0u32;
    for _ in 0..10 {
        let b = *buf.get(*pos)?;
        *pos += 1;
        if shift == 63 && b > 1 {
            return None;
        }
        val |= u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Some(val);
        }
        shift += 7;
    }
    None
}

enum ProtoValue<'a> {
    Varint,
    Fixed64,
    Bytes(&'a [u8]),
    Fixed32,
}

fn for_each_proto_field<'a>(
    buf: &'a [u8],
    mut f: impl FnMut(u32, ProtoValue<'a>) -> Option<()>,
) -> Option<()> {
    let mut pos = 0;
    while pos < buf.len() {
        let tag = read_proto_varint(buf, &mut pos)?;
        let field_num = u32::try_from(tag >> 3).ok()?;
        if field_num == 0 {
            return None;
        }
        let wire_type = (tag & 0x7) as u8;
        let val = match wire_type {
            0 => {
                let _ = read_proto_varint(buf, &mut pos)?;
                ProtoValue::Varint
            }
            1 => {
                let end = pos.checked_add(8)?;
                if end > buf.len() {
                    return None;
                }
                pos = end;
                ProtoValue::Fixed64
            }
            2 => {
                let len = usize::try_from(read_proto_varint(buf, &mut pos)?).ok()?;
                let end = pos.checked_add(len)?;
                let slice = buf.get(pos..end)?;
                pos = end;
                ProtoValue::Bytes(slice)
            }
            5 => {
                let end = pos.checked_add(4)?;
                if end > buf.len() {
                    return None;
                }
                pos = end;
                ProtoValue::Fixed32
            }
            _ => return None,
        };
        f(field_num, val)?;
    }
    Some(())
}

/// Extract `status.nodeInfo.kubeletVersion` from a serialized Kubernetes
/// `core/v1.Node` protobuf message (`Node.status = 3`, `NodeStatus.nodeInfo = 7`,
/// `NodeSystemInfo.kubeletVersion = 7`).
fn extract_kubelet_version_from_node_proto(buf: &[u8]) -> Option<String> {
    let mut kubelet_version: Option<String> = None;
    for_each_proto_field(buf, |field, val| {
        if field == 3 {
            let ProtoValue::Bytes(status_buf) = val else {
                return None;
            };
            for_each_proto_field(status_buf, |sfield, sval| {
                if sfield == 7 {
                    let ProtoValue::Bytes(info_buf) = sval else {
                        return None;
                    };
                    for_each_proto_field(info_buf, |ifield, ival| {
                        if ifield == 7 {
                            let ProtoValue::Bytes(ver_bytes) = ival else {
                                return None;
                            };
                            let s = std::str::from_utf8(ver_bytes).ok()?;
                            kubelet_version = Some(s.to_string());
                        }
                        Some(())
                    })?;
                }
                Some(())
            })?;
        }
        Some(())
    })?;
    kubelet_version.filter(|s| !s.is_empty())
}

/// Extract `status.nodeInfo.kubeletVersion` from a Kubernetes `Node` object
/// stored in kine's `value` column. Only the encoding kube-apiserver uses for
/// core types is accepted: the `k8s\0` prefix and an uncompressed
/// `runtime.Unknown` protobuf envelope with `apiVersion: v1`, `kind: Node`, and
/// a protobuf body. Anything else (JSON, compression, other kinds) is rejected.
fn extract_kubelet_version_from_node_blob(blob: &[u8]) -> Option<String> {
    if let Some(body) = blob.strip_prefix(b"k8s\0") {
        let mut api_version = "";
        let mut kind = "";
        let mut raw: Option<&[u8]> = None;
        let mut content_encoding = "";
        let mut content_type = "";
        for_each_proto_field(body, |field, val| {
            match field {
                1 => {
                    let ProtoValue::Bytes(tm_buf) = val else {
                        return None;
                    };
                    for_each_proto_field(tm_buf, |tfield, tval| {
                        match tfield {
                            1 => {
                                let ProtoValue::Bytes(b) = tval else {
                                    return None;
                                };
                                api_version = std::str::from_utf8(b).ok()?;
                            }
                            2 => {
                                let ProtoValue::Bytes(b) = tval else {
                                    return None;
                                };
                                kind = std::str::from_utf8(b).ok()?;
                            }
                            _ => {}
                        }
                        Some(())
                    })?;
                }
                2 => {
                    let ProtoValue::Bytes(b) = val else {
                        return None;
                    };
                    raw = Some(b);
                }
                3 => {
                    let ProtoValue::Bytes(b) = val else {
                        return None;
                    };
                    content_encoding = std::str::from_utf8(b).ok()?;
                }
                4 => {
                    let ProtoValue::Bytes(b) = val else {
                        return None;
                    };
                    content_type = std::str::from_utf8(b).ok()?;
                }
                _ => {}
            }
            Some(())
        })?;
        if !content_encoding.is_empty()
            || api_version != "v1"
            || kind != "Node"
            || !matches!(content_type, "" | "application/vnd.kubernetes.protobuf")
        {
            return None;
        }
        return extract_kubelet_version_from_node_proto(raw.filter(|r| !r.is_empty())?);
    }
    None
}

/// Inspect `data_dir/server/db/state.db` read-only (never modifying any file in
/// `data_dir/server/`) to detect the k3s version recorded in live Kubernetes
/// `Node` objects (`/registry/minions/*`).
///
/// Fails closed (returns `None`) if `sqlite3` is unavailable, any path component
/// is a symlink or non-regular file, `kine` is not a plain table, output is
/// truncated, or live node versions are missing, unparseable, or disagree on
/// `(major, minor)`.
fn detect_datastore_version_from_state_db_with(
    data_dir: &Path,
    probe: &StateDbProbe<'_>,
    mut run_cmd: impl FnMut(&mut Command) -> Result<(i32, String), String>,
) -> Option<String> {
    let sqlite3_bin = probe.sqlite3_bin;
    if !sqlite3_bin.is_file() {
        log(&format!(
            "cannot inspect unmarked state.db: {} is not available",
            sqlite3_bin.display()
        ));
        return None;
    }
    let server_dir = data_dir.join("server");
    if !fs::symlink_metadata(&server_dir).is_ok_and(|m| m.file_type().is_dir()) {
        return None;
    }
    let db_dir = server_dir.join("db");
    if !fs::symlink_metadata(&db_dir).is_ok_and(|m| m.file_type().is_dir()) {
        return None;
    }
    let db_path = db_dir.join("state.db");
    if !fs::symlink_metadata(&db_path).is_ok_and(|m| m.file_type().is_file()) {
        return None;
    }
    let wal_path = db_dir.join("state.db-wal");
    let shm_path = db_dir.join("state.db-shm");
    let journal_path = db_dir.join("state.db-journal");
    let wal_len = match inspect_regular_companion(&wal_path) {
        Ok(v) => v,
        Err(e) => {
            log(&format!("skip state.db detection: {e}"));
            return None;
        }
    };
    if let Err(e) = inspect_regular_companion(&shm_path) {
        log(&format!("skip state.db detection: {e}"));
        return None;
    }
    // kine runs SQLite in WAL mode; a non-empty rollback journal means an
    // interrupted non-WAL write that only a writable open would recover, so fail
    // closed instead of emulating it.
    match inspect_regular_companion(&journal_path) {
        Ok(None | Some(0)) => {}
        Ok(Some(_)) => {
            log("skip state.db detection: state.db-journal is not empty");
            return None;
        }
        Err(e) => {
            log(&format!("skip state.db detection: {e}"));
            return None;
        }
    }

    // A checkpointed database (no WAL content) is read in place with SQLite's
    // `immutable=1`, which opens it read-only, takes no locks, and never creates
    // sidecar files, so any size works. Callers only probe with k3s stopped.
    // WAL content is only visible through a writable open, so that case reads
    // a bounded private copy on the /run tmpfs instead; the copy never reaches
    // the disk and the live files are never opened by sqlite3.
    let mut _scratch = None;
    let sqlite_target = if wal_len.unwrap_or(0) == 0 {
        if !db_path.is_absolute() {
            return None;
        }
        format!("file:{}?immutable=1", encode_sqlite_uri_path(&db_path))
    } else {
        let total = wal_len.unwrap_or(0).saturating_add(
            fs::symlink_metadata(&db_path)
                .map(|m| m.len())
                .unwrap_or(u64::MAX),
        );
        if total > probe.max_copy_bytes {
            log(&format!(
                "skip state.db detection: {total} bytes of database and uncheckpointed WAL \
                 exceed the {}-byte inspection copy limit",
                probe.max_copy_bytes
            ));
            return None;
        }
        if let Err(e) = DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(probe.scratch_root)
        {
            log(&format!(
                "create state.db scratch root {}: {e}",
                probe.scratch_root.display()
            ));
            return None;
        }
        let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
        let scratch_path = probe
            .scratch_root
            .join(format!(".state-db-inspect.{}.{seq}", std::process::id()));
        if let Err(e) = DirBuilder::new().mode(0o700).create(&scratch_path) {
            log(&format!(
                "create state.db scratch dir {}: {e}",
                scratch_path.display()
            ));
            return None;
        }
        let scratch = _scratch.insert(ScratchDir(scratch_path));
        let scratch_db = scratch.0.join("state.db");
        let mut budget = probe.max_copy_bytes;
        if let Err(e) = copy_regular_file_bounded(&db_path, &scratch_db, &mut budget) {
            log(&format!("copy state.db for inspection: {e}"));
            return None;
        }
        if let Err(e) =
            copy_regular_file_bounded(&wal_path, &scratch.0.join("state.db-wal"), &mut budget)
        {
            log(&format!("copy state.db-wal for inspection: {e}"));
            return None;
        }
        format!("file:{}", encode_sqlite_uri_path(&scratch_db))
    };

    let mut query = |sql: &str| -> Option<String> {
        let mut cmd = Command::new(sqlite3_bin);
        cmd.args([
            "-bail",
            "-batch",
            "-noheader",
            "-list",
            "-safe",
            "-cmd",
            ".timeout 5000",
            &sqlite_target,
            sql,
        ]);
        match run_cmd(&mut cmd) {
            Ok((0, out)) => Some(out),
            Ok((code, _)) => {
                log(&format!("sqlite3 inspection exited {code}"));
                None
            }
            Err(e) => {
                log(&format!("sqlite3 inspection failed: {e}"));
                None
            }
        }
    };

    const SCHEMA_SQL: &str = "\
PRAGMA trusted_schema = OFF;
SELECT CASE
    WHEN type = 'table' AND upper(ltrim(coalesce(sql, ''))) NOT LIKE 'CREATE VIRTUAL %'
    THEN 'OK'
    ELSE 'BAD'
END
FROM main.sqlite_master
WHERE name = 'kine';";
    if query(SCHEMA_SQL)?.trim() != "OK" {
        return None;
    }

    let nodes_sql = format!(
        "\
PRAGMA trusted_schema = OFF;
SELECT CASE
    WHEN k.value IS NULL OR length(k.value) = 0 THEN 'EMPTY'
    WHEN length(k.value) > {STATE_DB_MAX_BLOB_BYTES} THEN 'OVERSIZED'
    ELSE length(k.value) || ':' || hex(k.value)
END
FROM main.kine k
INNER JOIN (
    SELECT name, MAX(id) AS max_id
    FROM main.kine
    WHERE name >= '/registry/minions/'
      AND name < '/registry/minions0'
    GROUP BY name
) latest ON k.name = latest.name AND k.id = latest.max_id
WHERE k.deleted = 0
ORDER BY k.id DESC
LIMIT {};
SELECT 'END';",
        STATE_DB_MAX_NODE_ROWS + 1
    );
    let raw_out = query(&nodes_sql)?;
    let lines: Vec<&str> = raw_out
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .collect();
    let (&last, rows) = lines.split_last()?;
    if last != "END" || rows.is_empty() || rows.len() > STATE_DB_MAX_NODE_ROWS {
        return None;
    }

    let mut chosen: Option<(String, (u32, u32))> = None;
    for row in rows {
        let (len_str, hex_str) = row.split_once(':')?;
        let expected_len: usize = len_str.parse().ok()?;
        if expected_len == 0
            || expected_len > STATE_DB_MAX_BLOB_BYTES
            || hex_str.len() != expected_len.checked_mul(2)?
        {
            return None;
        }
        let blob = decode_hex(hex_str)?;
        let version = extract_kubelet_version_from_node_blob(&blob)?;
        let mm = parse_strict_k3s_tag(&version)?;
        match &chosen {
            None => chosen = Some((version, mm)),
            Some((_, prev_mm)) if *prev_mm == mm => {}
            Some(_) => return None,
        }
    }
    chosen.map(|(v, _)| v)
}

/// Record an independently verified k3s release tag in `data_version_file` for
/// an existing unmarked datastore under `data_dir`. Refuses to run when no
/// datastore exists, refuses to overwrite any existing marker, and refuses to
/// mark a lower minor than `server/db/state.db` already records.
fn mark_datastore_version_at_with(
    data_dir: &Path,
    data_version_file: &Path,
    version: &str,
    probe: &StateDbProbe<'_>,
    run_cmd: impl FnMut(&mut Command) -> Result<(i32, String), String>,
) -> Result<(), String> {
    validate_k3s_release_tag(version)?;
    if !datastore_exists(data_dir)? {
        return Err(format!(
            "no existing cluster datastore found under {}; start Kubernetes normally with `kat k8s start`",
            data_dir.display()
        ));
    }
    match fs::symlink_metadata(data_version_file) {
        Ok(meta) => {
            let detail = if !meta.file_type().is_file() {
                " (not a regular file)".to_string()
            } else {
                match read_marker_file(data_version_file) {
                    Ok(MarkerFile::Present(existing)) => format!(" ({existing})"),
                    Ok(MarkerFile::Empty) => " (empty)".to_string(),
                    Ok(MarkerFile::NonRegular) => " (not a regular file)".to_string(),
                    Ok(MarkerFile::Missing) | Err(_) => String::new(),
                }
            };
            return Err(format!(
                "datastore version marker {} already exists{detail}; \
                 refusing to overwrite an existing marker",
                data_version_file.display()
            ));
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(format!("stat {}: {e}", data_version_file.display()));
        }
    }
    // mark-version is only for datastores whose version is genuinely unknown.
    // A stamp (even a corrupt one) or a trusted pre-tracking node record already
    // determines the version, and marking over it could let a skipped minor or
    // a downgrade through.
    let stamp = server_version_stamp(data_dir);
    match read_marker_file(&stamp)? {
        MarkerFile::Missing => {}
        MarkerFile::Present(stamped) if parse_k3s_minor(&stamped).is_some() => {
            return Err(format!(
                "{} already records this datastore's k3s version ({stamped}); \
                 refusing to mark it. Start Kubernetes with a compatible k8s_version instead.",
                stamp.display()
            ));
        }
        MarkerFile::Present(_) | MarkerFile::Empty | MarkerFile::NonRegular => {
            return Err(corrupt_datastore_version_error(
                &stamp,
                "exists but is not a valid version stamp",
            ));
        }
    }
    if let Some(detected) = detect_datastore_version_from_state_db_with(data_dir, probe, run_cmd) {
        if PRE_TRACKING_K3S_VERSIONS.contains(&detected.as_str()) {
            return Err(format!(
                "server/db/state.db shows this datastore was last run by {detected}, which \
                 katamaran already accepts without a marker; refusing to mark it. Start \
                 Kubernetes with a compatible k8s_version instead."
            ));
        }
        if let (Some(supplied_mm), Some(detected_mm)) = (
            parse_strict_k3s_tag(version),
            parse_strict_k3s_tag(&detected),
        ) && supplied_mm < detected_mm
        {
            return Err(format!(
                "refusing to mark datastore as {version}: server/db/state.db already records newer k3s {detected}"
            ));
        }
    }
    write_atomic_new(data_version_file, format!("{version}\n").as_bytes())
}

/// Return the k3s version associated with an existing datastore under `data_dir`.
///
/// Never falls back to `VERSION_FILE` (the installed binary's version). The
/// recorded version is the newer of `data_version_file` and the
/// `server/db/SERVER_VERSION_STAMP` stamp, or the stamp alone when the marker is
/// absent; an empty, non-regular, or invalid record fails closed. The
/// `server/db/state.db` node record is then always applied as a lower bound.
/// With neither record, the node record is accepted only when it is one of
/// `PRE_TRACKING_K3S_VERSIONS`; anything else is rejected with
/// `kat k8s mark-version` guidance.
fn effective_datastore_version_with(
    data_dir: &Path,
    data_version_file: &Path,
    probe: &StateDbProbe<'_>,
    run_cmd: impl FnMut(&mut Command) -> Result<(i32, String), String>,
) -> Result<Option<String>, String> {
    if !datastore_exists(data_dir)? {
        return Ok(None);
    }
    let recorded = match read_marker_file(data_version_file)? {
        MarkerFile::Present(v) => Some(newer_of_marker_and_stamp(data_dir, data_version_file, v)?),
        MarkerFile::Missing => {
            let stamp = server_version_stamp(data_dir);
            match read_marker_file(&stamp)? {
                // Written before every tracking-era launch, so it is at least the
                // newest k3s that a tracking-enabled release ran against `server/`.
                MarkerFile::Present(v) if parse_k3s_minor(&v).is_some() => Some(v),
                MarkerFile::Present(v) => {
                    return Err(corrupt_datastore_version_error(
                        &stamp,
                        &format!("records an invalid k3s version {v:?}"),
                    ));
                }
                MarkerFile::Empty => {
                    return Err(corrupt_datastore_version_error(&stamp, "is empty"));
                }
                MarkerFile::NonRegular => {
                    return Err(corrupt_datastore_version_error(
                        &stamp,
                        "is not a regular file",
                    ));
                }
                // No tracking-enabled release ever launched k3s against this
                // `server/`, so only a pre-tracking pinned build can have opened it.
                MarkerFile::Missing => None,
            }
        }
        MarkerFile::Empty => {
            return Err(corrupt_datastore_version_error(
                data_version_file,
                "is empty",
            ));
        }
        MarkerFile::NonRegular => {
            return Err(corrupt_datastore_version_error(
                data_version_file,
                "is not a regular file",
            ));
        }
    };
    // The node record is always a lower bound: k3s's kubelet reports the server's
    // own release, so whatever it records has opened this datastore. That catches
    // data run by a release that did not update the marker or stamp (for example
    // an older katamaran after a rollback).
    let detected = detect_datastore_version_from_state_db_with(data_dir, probe, run_cmd);
    match (recorded, detected) {
        (Some(recorded), detected) => Ok(Some(newer_minor(recorded, detected))),
        (None, Some(detected)) if PRE_TRACKING_K3S_VERSIONS.contains(&detected.as_str()) => {
            Ok(Some(detected))
        }
        (None, Some(detected)) => Err(format!(
            "{} The node record in server/db/state.db reports k3s {detected}, \
             which no pre-tracking katamaran release ran, so it is only a lower \
             bound (a newer k3s server may have opened this datastore before its \
             kubelet reported status) and cannot be used to verify compatibility.",
            missing_datastore_version_error(data_dir, data_version_file)
        )),
        (None, None) => Err(missing_datastore_version_error(data_dir, data_version_file)),
    }
}

/// `recorded`, unless the state.db node record shows a newer minor.
fn newer_minor(recorded: String, detected: Option<String>) -> String {
    match (detected, parse_k3s_minor(&recorded)) {
        (Some(d), Some(recorded_mm))
            if parse_k3s_minor(&d).is_some_and(|d_mm| d_mm > recorded_mm) =>
        {
            d
        }
        _ => recorded,
    }
}

/// Publish the outer marker and the `server/db/` stamp before spawning k3s. Any
/// failure, including a directory sync failure after a rename already made the
/// new tag visible, runs `restore` (which puts both files back to their prior
/// states) before returning the error, so a failed attempt never leaves a newer
/// recorded version behind for a datastore k3s did not open.
fn publish_version_files(
    data_dir: &Path,
    data_version_file: &Path,
    version: &str,
    write_marker_file: bool,
    write_stamp: bool,
    write_marker: impl FnOnce(&Path, &[u8]) -> Result<(), String>,
    restore: impl FnOnce(),
) -> Result<(), String> {
    let res = (|| {
        if write_marker_file {
            write_marker(data_version_file, format!("{version}\n").as_bytes())?;
        } else if let Some(parent) = data_version_file.parent()
            && matches!(read_marker_file(data_version_file)?, MarkerFile::Present(_))
        {
            // The marker already records this version, possibly from an attempt
            // whose directory sync failed: make it durable before k3s starts.
            sync_dir(parent)?;
        }
        if write_stamp {
            write_server_version_stamp(data_dir, version)?;
        } else if server_db_dir_exists(data_dir)? {
            sync_dir(&data_dir.join("server/db"))?;
        }
        Ok(())
    })();
    if res.is_err() {
        restore();
    }
    res
}

/// Stamp a datastore that k3s created during this start once its `server/db/`
/// exists, clearing `*pending` on success (failures are logged and retried).
fn stamp_new_datastore(data_dir: &Path, version: &str, pending: &mut bool) {
    if !*pending {
        return;
    }
    match server_db_dir_exists(data_dir) {
        Ok(true) => {}
        Ok(false) => return,
        Err(e) => {
            log(&format!("record version stamp for new datastore: {e}"));
            return;
        }
    }
    match write_server_version_stamp(data_dir, version) {
        Ok(()) => *pending = false,
        Err(e) => log(&format!("record version stamp for new datastore: {e}")),
    }
}

/// Katamaran always writes `.katamaran-version` and the `server/db/` stamp
/// together, so they only disagree when one was restored without the other
/// (for example `server/db/` from a newer backup beside a stale outer marker).
/// Use whichever records the newer minor so the downgrade guard still holds.
fn newer_of_marker_and_stamp(
    data_dir: &Path,
    data_version_file: &Path,
    marker: String,
) -> Result<String, String> {
    let Some(marker_mm) = parse_k3s_minor(&marker) else {
        return Err(corrupt_datastore_version_error(
            data_version_file,
            &format!("records an invalid k3s version {marker:?}"),
        ));
    };
    let stamp_path = server_version_stamp(data_dir);
    match read_marker_file(&stamp_path)? {
        MarkerFile::Missing => Ok(marker),
        MarkerFile::Present(stamp) => {
            let Some(stamp_mm) = parse_k3s_minor(&stamp) else {
                return Err(corrupt_datastore_version_error(
                    &stamp_path,
                    &format!("records an invalid k3s version {stamp:?}"),
                ));
            };
            Ok(if marker_mm >= stamp_mm { marker } else { stamp })
        }
        MarkerFile::Empty => Err(corrupt_datastore_version_error(&stamp_path, "is empty")),
        MarkerFile::NonRegular => Err(corrupt_datastore_version_error(
            &stamp_path,
            "is not a regular file",
        )),
    }
}

/// Refuse to install or start a lower Kubernetes minor or skip one or more
/// intermediate minors against an existing datastore without modifying `data_dir`.
fn check_datastore_compatible_with(
    data_dir: &Path,
    data_version_file: &Path,
    target_version: &str,
    probe: &StateDbProbe<'_>,
    run_cmd: impl FnMut(&mut Command) -> Result<(i32, String), String>,
) -> Result<(), String> {
    let Some(existing) =
        effective_datastore_version_with(data_dir, data_version_file, probe, run_cmd)?
    else {
        return Ok(());
    };
    let target_mm = parse_k3s_minor(target_version)
        .ok_or_else(|| format!("invalid k3s version {target_version:?}"))?;
    let existing_mm = parse_k3s_minor(&existing).ok_or_else(|| {
        format!(
            "cannot determine minor version of existing cluster data ({existing:?}); \
             while Kubernetes is stopped, either restore a matching datastore backup \
             together with its {} version marker, or back up any state you need and \
             run `kat k8s reset` before starting {target_version}",
            data_version_file.display()
        )
    })?;
    if target_mm < existing_mm {
        return Err(format!(
            "refusing to use k3s {target_version} with existing {existing} cluster data: \
             Kubernetes does not support minor-version downgrades. To start fresh, back up \
             any state you need, run `kat k8s reset` to delete cluster data and persistent \
             volumes, then run `kat k8s start`. To roll back to a pre-upgrade datastore \
             backup instead, stop Kubernetes and restore both the datastore and its \
             matching {} version marker from that backup before starting {target_version} \
             (see the Kubernetes guide's rollback procedure).",
            data_version_file.display()
        ));
    }
    if target_mm.0 != existing_mm.0 || target_mm.1 > existing_mm.1.saturating_add(1) {
        let next_minor = existing_mm.1.saturating_add(1);
        return Err(format!(
            "refusing to upgrade existing {existing} cluster data directly to k3s \
             {target_version}: Kubernetes does not support skipping minor versions. \
             To preserve cluster state, back up first and upgrade one minor at a time \
             (starting with a v{}.{next_minor}.x+k3s1 release). Or, to start fresh on \
             {target_version}, back up any state you need, run `kat k8s reset` to delete \
             cluster data and persistent volumes, then run `kat k8s start`.",
            existing_mm.0
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DatastoreEntryKind {
    File {
        len: u64,
        hash_sip: u64,
        hash_fnv: u64,
    },
    Dir,
    /// A symlink whose resolved target stays strictly inside `data_dir/server`
    /// (such as k3s's normal `server/agent-token -> token` link), so its target
    /// is fingerprinted by the directory walk itself.
    InternalSymlink(PathBuf),
    Other(u32),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DatastoreEntry {
    rel_path: PathBuf,
    dev: u64,
    ino: u64,
    mtime: (i64, i64),
    ctime: (i64, i64),
    kind: DatastoreEntryKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DatastoreSnapshot {
    Absent,
    Present(Vec<DatastoreEntry>),
}

fn hash_file(path: &Path) -> io::Result<(u64, u64, u64)> {
    let mut f = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    let mut sip = DefaultHasher::new();
    let mut fnv: u64 = 0xcbf29ce484222325;
    let mut total = 0u64;
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        total += n as u64;
        let chunk = &buf[..n];
        sip.write(chunk);
        for &b in chunk {
            fnv ^= u64::from(b);
            fnv = fnv.wrapping_mul(0x100000001b3);
        }
    }
    Ok((total, sip.finish(), fnv))
}

fn collect_datastore_entries(
    path: &Path,
    canonical_root: &Path,
    rel_path: PathBuf,
    out: &mut Vec<DatastoreEntry>,
) -> io::Result<()> {
    let meta = fs::symlink_metadata(path)?;
    let ft = meta.file_type();
    let kind = if ft.is_symlink() {
        let target = fs::read_link(path)?;
        let resolved = fs::canonicalize(path).map_err(|e| {
            io::Error::other(format!(
                "cannot resolve symlinked datastore path {} (-> {}): {e}",
                path.display(),
                target.display()
            ))
        })?;
        if !resolved.starts_with(canonical_root) {
            return Err(io::Error::other(format!(
                "cannot safely fingerprint symlinked datastore path {} resolving outside {}",
                path.display(),
                canonical_root.display()
            )));
        }
        DatastoreEntryKind::InternalSymlink(target)
    } else if ft.is_file() {
        let (len, hash_sip, hash_fnv) = hash_file(path)?;
        DatastoreEntryKind::File {
            len,
            hash_sip,
            hash_fnv,
        }
    } else if ft.is_dir() {
        DatastoreEntryKind::Dir
    } else {
        DatastoreEntryKind::Other(meta.mode())
    };
    out.push(DatastoreEntry {
        rel_path: rel_path.clone(),
        dev: meta.dev(),
        ino: meta.ino(),
        mtime: (meta.mtime(), meta.mtime_nsec()),
        ctime: (meta.ctime(), meta.ctime_nsec()),
        kind,
    });
    if ft.is_dir() {
        let mut children: Vec<_> = fs::read_dir(path)?.collect::<io::Result<Vec<_>>>()?;
        children.sort_by_key(|e| e.file_name());
        for entry in children {
            let child_rel = rel_path.join(entry.file_name());
            collect_datastore_entries(&entry.path(), canonical_root, child_rel, out)?;
        }
    }
    Ok(())
}

/// Fingerprint `data_dir/server` (structure, metadata, and regular-file contents)
/// so a failed startup that exited before touching the datastore can safely roll
/// back `.katamaran-version`, while any modification keeps the updated marker.
///
/// Permits internal symlinks whose resolved target stays inside `data_dir/server`
/// (such as k3s's normal `agent-token -> token` link), but fails closed if
/// `server` itself is a symlink or if any symlink inside it is dangling or
/// resolves outside `data_dir/server`.
fn snapshot_datastore(data_dir: &Path) -> io::Result<DatastoreSnapshot> {
    let server_dir = data_dir.join("server");
    match fs::symlink_metadata(&server_dir) {
        Ok(meta) if meta.file_type().is_symlink() => Err(io::Error::other(format!(
            "cannot safely fingerprint symlinked datastore path {}",
            server_dir.display()
        ))),
        Ok(_) => {
            let canonical_root = fs::canonicalize(&server_dir)?;
            let mut entries = Vec::new();
            collect_datastore_entries(&server_dir, &canonical_root, PathBuf::new(), &mut entries)?;
            // Writing SERVER_VERSION_STAMP is katamaran's own bookkeeping, not a
            // change by k3s: drop the stamp and the times it bumps on `server/db/`.
            let stamp_rel = Path::new("db").join(SERVER_VERSION_STAMP);
            entries.retain(|e| e.rel_path != stamp_rel);
            for e in &mut entries {
                if e.rel_path == Path::new("db") {
                    e.mtime = (0, 0);
                    e.ctime = (0, 0);
                }
            }
            Ok(DatastoreSnapshot::Present(entries))
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(DatastoreSnapshot::Absent),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
fn rollback_data_version_if_untouched(
    data_dir: &Path,
    data_version_file: &Path,
    prev_data_version: Option<&str>,
    before: Option<&DatastoreSnapshot>,
) {
    rollback_versions_if_untouched(data_dir, before, || {
        restore_data_version(data_version_file, prev_data_version);
    });
}

/// Run `restore` (which puts the version marker and stamp back to their prior
/// states) only when `server/` is unchanged since `before` was taken.
fn rollback_versions_if_untouched(
    data_dir: &Path,
    before: Option<&DatastoreSnapshot>,
    restore: impl FnOnce(),
) {
    let Some(before) = before else {
        return;
    };
    match snapshot_datastore(data_dir) {
        Ok(after) if &after == before => restore(),
        Ok(_) => {}
        Err(e) => {
            log(&format!(
                "snapshot {} after startup failure: {e}; keeping updated version marker",
                data_dir.display()
            ));
        }
    }
}

/// Remove a version file that did not exist before and fsync its directory, so
/// a crash cannot bring back a newer recorded version after a rollback.
fn remove_durably(path: &Path) {
    match fs::remove_file(path) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => return,
        Err(e) => {
            log(&format!("remove {}: {e}", path.display()));
            return;
        }
    }
    if let Some(parent) = path.parent()
        && let Err(e) = sync_dir(parent)
    {
        log(&format!("after removing {}: {e}", path.display()));
    }
}

/// Restore the `server/db/` stamp to `prev`, removing it if it did not exist.
fn restore_server_version_stamp(data_dir: &Path, prev: Option<&str>) {
    match prev {
        Some(v) => {
            if let Err(e) = write_server_version_stamp(data_dir, v) {
                log(&format!("restore server version stamp: {e}"));
            }
        }
        None => remove_durably(&server_version_stamp(data_dir)),
    }
}

fn restore_data_version(path: &Path, prev: Option<&str>) {
    match prev {
        Some(v) => {
            if let Err(e) = write_atomic(path, format!("{v}\n").as_bytes()) {
                log(&format!("restore {}: {e}", path.display()));
            }
        }
        None => remove_durably(path),
    }
}

fn commit_installed_binary(
    tmp: &Path,
    bin: &Path,
    version_file: &Path,
    version: &str,
) -> Result<(), String> {
    commit_installed_binary_with(tmp, bin, version_file, version, write_atomic)
}

/// Invalidate any previous version marker before swapping the executable into
/// `bin`, so a failed or interrupted marker write cannot leave the previous
/// version string describing the newly installed binary.
fn commit_installed_binary_with(
    tmp: &Path,
    bin: &Path,
    version_file: &Path,
    version: &str,
    write_version: impl FnOnce(&Path, &[u8]) -> Result<(), String>,
) -> Result<(), String> {
    match fs::remove_file(version_file) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(format!("remove stale {}: {e}", version_file.display()));
        }
    }
    fs::rename(tmp, bin).map_err(|e| format!("install {}: {e}", bin.display()))?;
    write_version(version_file, format!("{version}\n").as_bytes())?;
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
            K8sMsg::Start {
                extra_args,
                config_yaml,
            } => self
                .start_inner(&mut conn, &extra_args, config_yaml.as_deref())
                .map(K8sMsg::StatusInfo),
            K8sMsg::Stop => self.stop_inner(Some(&mut conn)).map(|()| K8sMsg::Done),
            K8sMsg::Reset { images } => self.reset_inner(&mut conn, images).map(|()| K8sMsg::Done),
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
            K8sMsg::MarkDatastoreVersion { version } => {
                self.mark_datastore_version(&version).map(|()| K8sMsg::Done)
            }
            K8sMsg::Capabilities => Ok(K8sMsg::CapabilitiesInfo(k8s::CAP_START_CONFIG)),
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

    fn run_sqlite_cmd(&self, cmd: &mut Command) -> Result<(i32, String), String> {
        let (code, stdout, stderr) = run_reaped_split(&self.reaper, cmd, Duration::from_secs(10))
            .map_err(|e| format!("sqlite3: {e}"))?;
        if code != 0 && !stderr.trim().is_empty() {
            log(&format!("sqlite3 stderr: {}", stderr.trim()));
        }
        Ok((code, stdout))
    }

    fn mark_datastore_version(&self, version: &str) -> Result<(), String> {
        if SHUTTING_DOWN.load(Ordering::SeqCst) {
            return Err("aborted: VM is shutting down".into());
        }
        let _guard = self.op_lock.lock().unwrap();
        if SHUTTING_DOWN.load(Ordering::SeqCst) {
            return Err("aborted: VM is shutting down".into());
        }
        ensure_no_unreaped_k3s()?;
        {
            let st = self.state.lock().unwrap();
            if st.running.is_some()
                || matches!(st.phase.as_str(), "starting" | "running" | "stopping")
            {
                return Err(
                    "stop Kubernetes (`kat k8s stop`) before recording the datastore version"
                        .into(),
                );
            }
        }
        mark_datastore_version_at_with(
            Path::new(DATA_DIR),
            Path::new(DATA_VERSION_FILE),
            version,
            &state_db_probe(),
            |cmd| self.run_sqlite_cmd(cmd),
        )?;
        log(&format!("recorded datastore version {version}"));
        Ok(())
    }

    fn install(&self, conn: &mut UnixStream, version: &str) -> Result<(), String> {
        if SHUTTING_DOWN.load(Ordering::SeqCst) {
            return Err("aborted: VM is shutting down".into());
        }
        let _guard = self.op_lock.lock().unwrap();
        if SHUTTING_DOWN.load(Ordering::SeqCst) {
            return Err("aborted: VM is shutting down".into());
        }
        ensure_no_unreaped_k3s()?;
        validate_k3s_release_tag(version)?;
        if self.state.lock().unwrap().running.is_some() {
            return Err("stop Kubernetes before installing a new k3s binary".into());
        }
        check_datastore_compatible_with(
            Path::new(DATA_DIR),
            Path::new(DATA_VERSION_FILE),
            version,
            &state_db_probe(),
            |cmd| self.run_sqlite_cmd(cmd),
        )?;
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
            commit_installed_binary(
                Path::new(&tmp),
                Path::new(BIN),
                Path::new(VERSION_FILE),
                version,
            )?;
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

    fn start_inner(
        self: &Arc<Self>,
        conn: &mut UnixStream,
        extra_args: &[String],
        config_yaml: Option<&str>,
    ) -> Result<K8sInfo, String> {
        if SHUTTING_DOWN.load(Ordering::SeqCst) {
            return Err("aborted: VM is shutting down".into());
        }
        let _guard = self.op_lock.lock().unwrap();
        if SHUTTING_DOWN.load(Ordering::SeqCst) {
            return Err("aborted: VM is shutting down".into());
        }
        ensure_no_unreaped_k3s()?;
        // Validate extra_args and normalize config_yaml before checking
        // already-running state so invalid inputs always return their
        // validation error and the running-settings comparison uses canonical
        // JSON (comment-only YAML edits match).
        let server_args = build_k3s_server_args(extra_args)?;
        // k3s reads a canonical JSON rewrite of the host YAML, never the raw
        // file; see `kat_proto::k3s_config` for the accepted subset and why.
        let managed_config = match config_yaml {
            Some(yaml) => Some(normalize_k3s_config_yaml(yaml)?),
            None => None,
        };
        {
            let st = self.state.lock().unwrap();
            if st.phase == "running" && st.api_ready {
                // Never report success while silently ignoring new settings.
                let cmp = match st.running.as_ref() {
                    Some(r) => {
                        check_running_settings_match(&r.settings, extra_args, managed_config.as_deref())
                    }
                    None => Err(
                        "Kubernetes is already running with different k8s_disable, k8s_args or k3s-config.yaml settings; run `kat k8s stop` and start it again to apply them"
                            .into(),
                    ),
                };
                drop(st);
                cmp?;
                return Ok(self.info());
            }
        }
        if !Path::new(BIN).is_file() {
            return Err("k3s binary is not installed".into());
        }
        let installed_version = read_trimmed_file(Path::new(VERSION_FILE))?.ok_or_else(|| {
            "k3s installation is incomplete (missing version marker); restart Kubernetes to reinstall"
                .to_string()
        })?;
        // Never copy a malformed tag into the datastore's version files.
        validate_k3s_release_tag(&installed_version).map_err(|e| {
            format!(
                "installed k3s has an invalid version marker ({e}); restart Kubernetes to reinstall"
            )
        })?;
        check_datastore_compatible_with(
            Path::new(DATA_DIR),
            Path::new(DATA_VERSION_FILE),
            &installed_version,
            &state_db_probe(),
            |cmd| self.run_sqlite_cmd(cmd),
        )?;
        if managed_config.is_some() {
            reject_managed_config_dropins(Path::new(K3S_CONFIG_PATH))?;
        }
        let _ = k8s::write(
            &mut *conn,
            &K8sMsg::Progress("preparing k3s environment".into()),
        );
        prepare_dirs_and_sysctls(&self.reaper)?;
        sync_k3s_config(Path::new(K3S_CONFIG_PATH), managed_config.as_deref())?;
        self.activate_containerd_cri(conn)?;
        if SHUTTING_DOWN.load(Ordering::SeqCst) {
            return Err("aborted: VM is shutting down".into());
        }

        if !extra_args.is_empty() || config_yaml.is_some() {
            log(&format!(
                "starting k3s with {} extra arg(s){}",
                extra_args.len(),
                if config_yaml.is_some() {
                    format!(" and {K3S_CONFIG_PATH}")
                } else {
                    String::new()
                }
            ));
        }

        let _ = k8s::write(&mut *conn, &K8sMsg::Progress("starting k3s server".into()));
        sys::mkdir("/var/log").map_err(|e| format!("mkdir /var/log: {e}"))?;
        let log_file = File::options()
            .create(true)
            .append(true)
            .open(LOG_FILE)
            .map_err(|e| format!("open {LOG_FILE}: {e}"))?;

        let mut cmd = Command::new(BIN);
        cmd.args(&server_args)
            .env("PATH", PATH)
            .stdin(Stdio::null())
            .stdout(log_file.try_clone().map_err(|e| e.to_string())?)
            .stderr(log_file);
        configure_k3s_config_env(
            &mut cmd,
            config_yaml.is_some().then_some(Path::new(K3S_CONFIG_PATH)),
        );

        let prev_data_version = match read_marker_file(Path::new(DATA_VERSION_FILE))? {
            MarkerFile::Present(v) => Some(v),
            MarkerFile::Missing | MarkerFile::Empty | MarkerFile::NonRegular => None,
        };
        let wrote_data_version = prev_data_version.as_deref() != Some(installed_version.as_str());
        let prev_stamp = match read_marker_file(&server_version_stamp(Path::new(DATA_DIR)))? {
            MarkerFile::Present(v) => Some(v),
            MarkerFile::Missing | MarkerFile::Empty | MarkerFile::NonRegular => None,
        };
        let server_db_existed = server_db_dir_exists(Path::new(DATA_DIR))?;
        let wrote_stamp =
            server_db_existed && prev_stamp.as_deref() != Some(installed_version.as_str());
        // A fresh datastore has no `server/db/` to stamp before launch; stamp it as
        // soon as k3s creates the directory so even a backup taken after the very
        // first shutdown carries the version.
        let mut fresh_stamp_pending = !server_db_existed;
        let stamp_fresh_datastore = |pending: &mut bool| {
            stamp_new_datastore(Path::new(DATA_DIR), &installed_version, pending);
        };
        // Put each version file back exactly as it was (removing ones that did not exist).
        let restore_versions = || {
            if wrote_data_version {
                restore_data_version(Path::new(DATA_VERSION_FILE), prev_data_version.as_deref());
            }
            if wrote_stamp {
                restore_server_version_stamp(Path::new(DATA_DIR), prev_stamp.as_deref());
            }
        };
        let before_datastore = if wrote_data_version || wrote_stamp {
            match snapshot_datastore(Path::new(DATA_DIR)) {
                Ok(snap) => Some(snap),
                Err(e) => {
                    log(&format!(
                        "snapshot {DATA_DIR} before starting k3s: {e}; \
                         startup failure will keep updated version marker"
                    ));
                    None
                }
            }
        } else {
            None
        };
        publish_version_files(
            Path::new(DATA_DIR),
            Path::new(DATA_VERSION_FILE),
            &installed_version,
            wrote_data_version,
            wrote_stamp,
            write_atomic,
            restore_versions,
        )?;
        if let Err(e) = enable_bridge_netfilter() {
            restore_versions();
            return Err(e);
        }
        // Mark active before spawning k3s so ports::watch ignores internal
        // control-plane ports (e.g. 6443) from the moment k3s binds them.
        ACTIVE.store(true, Ordering::SeqCst);
        let (pid, status_rx) = match self.reaper.spawn(&mut cmd) {
            Ok(v) => v,
            Err(e) => {
                ACTIVE.store(false, Ordering::SeqCst);
                restore_versions();
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
                settings: (extra_args.to_vec(), managed_config),
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
                            let done = rx.recv_timeout(Duration::from_secs(3)).is_ok();
                            if !done {
                                // Keep the reaper's own exit channel for this child.
                                note_unreaped_k3s(pid, move || rx.try_recv().is_ok());
                            }
                            done
                        }
                    }
                    None => {
                        note_unreaped_k3s(pid, || false);
                        false
                    }
                };
                if reaped {
                    stop_k8s_workloads(&self.reaper);
                    {
                        rollback_versions_if_untouched(
                            Path::new(DATA_DIR),
                            before_datastore.as_ref(),
                            restore_versions,
                        );
                    }
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
            let reaped = running.is_none_or(terminate_running);
            stop_k8s_workloads(&self.reaper);
            if reaped {
                rollback_versions_if_untouched(
                    Path::new(DATA_DIR),
                    before_datastore.as_ref(),
                    restore_versions,
                );
            }
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
            rollback_versions_if_untouched(
                Path::new(DATA_DIR),
                before_datastore.as_ref(),
                restore_versions,
            );
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

        // Stop a running-but-unready k3s and fail the start. Marks stopping first so
        // the exit watcher does not race us, then reaps k3s and cleans up workloads
        // before restoring sysctls.
        let fail_running_startup = |err: String| -> Result<K8sInfo, String> {
            let running = {
                let mut st = self.state.lock().unwrap();
                st.phase = "stopping".into();
                st.api_ready = false;
                st.running.take()
            };
            let reaped = running.is_none_or(terminate_running);
            stop_k8s_workloads(&self.reaper);
            if reaped {
                rollback_versions_if_untouched(
                    Path::new(DATA_DIR),
                    before_datastore.as_ref(),
                    restore_versions,
                );
            }
            ACTIVE.store(false, Ordering::SeqCst);
            FORWARDED.lock().unwrap().clear();
            restore_bridge_netfilter();
            let mut st = self.state.lock().unwrap();
            if st.generation == my_gen {
                st.phase = "failed".into();
                st.error = err.clone();
            }
            Err(err)
        };

        let _ = k8s::write(
            &mut *conn,
            &K8sMsg::Progress("waiting for Kubernetes API server".into()),
        );
        let deadline = Instant::now() + READY_TIMEOUT;
        loop {
            stamp_fresh_datastore(&mut fresh_stamp_pending);
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
                return fail_running_startup(format!(
                    "Kubernetes API not ready after {}s; see {LOG_FILE}",
                    READY_TIMEOUT.as_secs()
                ));
            }
            thread::sleep(Duration::from_millis(250));
        }

        if SHUTTING_DOWN.load(Ordering::SeqCst) {
            return abort_for_shutdown();
        }
        // Never report a cluster ready without its datastore version stamp: k3s's
        // SQLite datastore lives in server/db/, so it must exist once the API is up.
        stamp_fresh_datastore(&mut fresh_stamp_pending);
        if fresh_stamp_pending {
            return fail_running_startup(format!(
                "could not record the k3s version in {}; stopped Kubernetes \
                 (see {LOG_FILE} and the kat-init log)",
                server_version_stamp(Path::new(DATA_DIR)).display()
            ));
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
    fn stop_locked(&self, mut conn: Option<&mut UnixStream>) -> Result<(), String> {
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
        if let Some(c) = conn.as_deref_mut() {
            let _ = k8s::write(c, &K8sMsg::Progress("stopping k3s".into()));
        }
        if let Some(r) = running {
            let _ = terminate_running(r);
        }
        if needs_cleanup {
            match conn {
                Some(c) => stop_k8s_workloads_with_progress(&self.reaper, &mut |m| {
                    let _ = k8s::write(&mut *c, &K8sMsg::Progress(m.into()));
                }),
                None => stop_k8s_workloads(&self.reaper),
            }
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

    fn reset_inner(&self, conn: &mut UnixStream, remove_images: bool) -> Result<(), String> {
        if SHUTTING_DOWN.load(Ordering::SeqCst) {
            return Err("aborted: VM is shutting down".into());
        }
        let _guard = self.op_lock.lock().unwrap();
        if SHUTTING_DOWN.load(Ordering::SeqCst) {
            return Err("aborted: VM is shutting down".into());
        }
        ensure_no_unreaped_k3s()?;
        if remove_images {
            k8s::write(&mut *conn, &K8sMsg::ResetAccepted)
                .map_err(|e| format!("send reset ack: {e}"))?;
        }
        self.stop_locked(Some(&mut *conn))?;
        // stop_locked reports success even if k3s survived SIGKILL; never delete
        // the datastore underneath a live k3s.
        ensure_no_unreaped_k3s()?;
        // Remove the managed config copy first: if that fails, nothing has
        // been destroyed yet and the reset error is accurate.
        sync_k3s_config(Path::new(K3S_CONFIG_PATH), None)?;
        let _ = k8s::write(
            &mut *conn,
            &K8sMsg::Progress("removing cluster data".into()),
        );
        unmount_paths_under(&[DATA_DIR, LEGACY_KUBELET_DIR])?;
        if Path::new(DATA_DIR).exists() {
            // Try btrfs subvolume delete first (instant if it is a subvolume),
            // falling back to recursive removal. Send periodic progress during
            // both so large cluster-data directories do not trip idle timeouts.
            let mut cmd = Command::new("/sbin/btrfs");
            cmd.args(["subvolume", "delete", DATA_DIR]);
            let mut beat = || {
                let _ = k8s::write(
                    &mut *conn,
                    &K8sMsg::Progress("removing cluster data".into()),
                );
            };
            let _ = run_reaped_split_checked(
                &self.reaper,
                &mut cmd,
                Duration::from_secs(30),
                64 << 10,
                false,
                Some(&mut beat),
            );
            if Path::new(DATA_DIR).exists() {
                remove_dir_all_with_progress(
                    Path::new(DATA_DIR),
                    PROGRESS_HEARTBEAT_INTERVAL,
                    &mut |m| {
                        let _ = k8s::write(&mut *conn, &K8sMsg::Progress(m.into()));
                    },
                )?;
            }
        }
        if Path::new(LEGACY_KUBELET_DIR).exists() {
            remove_dir_all_with_progress(
                Path::new(LEGACY_KUBELET_DIR),
                PROGRESS_HEARTBEAT_INTERVAL,
                &mut |m| {
                    let _ = k8s::write(&mut *conn, &K8sMsg::Progress(m.into()));
                },
            )?;
        }
        let _ = fs::remove_file(KUBECONFIG);
        if remove_images {
            k8s::write(&mut *conn, &K8sMsg::ResetDataCleared)
                .map_err(|e| format!("send reset data-cleared ack: {e}"))?;
            if SHUTTING_DOWN.load(Ordering::SeqCst) {
                return Err("aborted: VM is shutting down".into());
            }
            let _ = k8s::write(
                &mut *conn,
                &K8sMsg::Progress("removing k8s.io images".into()),
            );
            let mut progress = |msg: &str| {
                let _ = k8s::write(&mut *conn, &K8sMsg::Progress(msg.into()));
            };
            clear_k8s_images(&self.reaper, &mut progress)?;
        }
        log(if remove_images {
            "k3s cluster data and k8s.io images reset"
        } else {
            "k3s cluster data reset"
        });
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

/// k3s also loads every `<config>.d/*.{yaml,yml}` next to the file it is
/// given, and would read those raw. The managed config lives in a
/// katamaran-owned directory where nothing else should create drop-ins, so
/// refuse to start (rather than let k3s read unvalidated YAML) if one exists.
fn reject_managed_config_dropins(config_path: &Path) -> Result<(), String> {
    let dir = PathBuf::from(format!("{}.d", config_path.display()));
    match fs::symlink_metadata(&dir) {
        Ok(_) => Err(format!(
            "{} exists, but katamaran does not support k3s config drop-ins next to its managed config; remove it and put the settings in ~/.katamaran/k3s-config.yaml",
            dir.display()
        )),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(format!("stat {}: {e}", dir.display())),
    }
}

fn configure_k3s_config_env(cmd: &mut Command, managed_config: Option<&Path>) {
    if let Some(path) = managed_config {
        cmd.env("K3S_CONFIG_FILE", path);
    } else {
        cmd.env_remove("K3S_CONFIG_FILE");
    }
}

fn build_k3s_server_args(extra_args: &[String]) -> Result<Vec<String>, String> {
    k8s::validate_k3s_extra_args(extra_args)?;
    let mut args = vec![
        "server".to_string(),
        format!("--container-runtime-endpoint=unix://{CONTAINERD_SOCKET}"),
        format!("--data-dir={DATA_DIR}"),
        format!("--write-kubeconfig={KUBECONFIG}"),
        format!("--write-kubeconfig-mode={KUBECONFIG_MODE}"),
        "--node-name=katamaran".to_string(),
        "--flannel-backend=host-gw".to_string(),
        "--cluster-cidr=10.42.0.0/16".to_string(),
        "--service-cidr=10.43.0.0/16".to_string(),
        "--cluster-dns=10.43.0.10".to_string(),
        "--tls-san=127.0.0.1".to_string(),
        "--tls-san=localhost".to_string(),
        "--tls-san=katamaran".to_string(),
        "--tls-san=k8s.kat.local".to_string(),
        format!("--https-listen-port={API_PORT}"),
        "--kubelet-arg=fail-swap-on=false".to_string(),
        format!("--kubelet-arg=root-dir={KUBELET_DIR}"),
        "--kube-proxy-arg=proxy-mode=nftables".to_string(),
    ];
    args.extend(extra_args.iter().cloned());
    Ok(args)
}

fn sync_k3s_config(path: &Path, config_yaml: Option<&str>) -> Result<(), String> {
    match config_yaml {
        Some(yaml) => {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)
                    .map_err(|e| format!("create {}: {e}", parent.display()))?;
            }
            write_atomic_mode(path, yaml.as_bytes(), 0o600)?;
        }
        None => match fs::remove_file(path) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(format!("remove {}: {e}", path.display())),
        },
    }
    Ok(())
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

fn terminate_running(mut r: Running) -> bool {
    r.sync_stop.store(true, Ordering::SeqCst);
    let reaped = if r.exited.lock().unwrap().is_some() {
        true
    } else {
        unsafe { libc::kill(r.pid, libc::SIGTERM) };
        if r.exit_rx.recv_timeout(STOP_TIMEOUT).is_ok() {
            true
        } else {
            log("k3s did not stop in time, killing it");
            unsafe { libc::kill(r.pid, libc::SIGKILL) };
            r.exit_rx.recv_timeout(Duration::from_secs(3)).is_ok()
        }
    };
    if !reaped {
        // The k3s-watch thread records the reaper's exit status in `exited`.
        let exited = Arc::clone(&r.exited);
        note_unreaped_k3s(r.pid, move || exited.lock().unwrap().is_some());
    }
    if let Some(t) = r.proxy_thread.take() {
        let _ = t.join();
    }
    reaped
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

/// Stop and remove Kubernetes tasks and containers in containerd's `k8s.io`
/// namespace without touching Docker's `moby` namespace.
fn stop_k8s_containers_with_progress(reaper: &Reaper, progress: &mut dyn FnMut(&str)) {
    let mut last_beat = Instant::now();
    let mut maybe_beat = |msg: &str| {
        if last_beat.elapsed() >= PROGRESS_HEARTBEAT_INTERVAL {
            progress(msg);
            last_beat = Instant::now();
        }
    };
    let mut ls = Command::new("/usr/bin/ctr");
    ls.args(["-a", CONTAINERD_SOCKET, "-n", "k8s.io", "tasks", "ls", "-q"]);
    if let Ok((0, out, _)) = run_reaped_split(reaper, &mut ls, Duration::from_secs(10)) {
        let ids: Vec<&str> = out
            .lines()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect();
        for id in &ids {
            maybe_beat("stopping k8s.io tasks");
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
            maybe_beat("removing k8s.io tasks");
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
            maybe_beat("removing k8s.io containers");
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
}

/// Stop and remove Kubernetes tasks/containers in the shared containerd
/// `k8s.io` namespace and tear down CNI interfaces, kube-proxy nftables rules,
/// and kubelet/data-dir mounts without touching Docker's `moby` namespace.
fn stop_k8s_workloads(reaper: &Reaper) {
    stop_k8s_workloads_with_progress(reaper, &mut |_| {});
}

fn stop_k8s_workloads_with_progress(reaper: &Reaper, progress: &mut dyn FnMut(&str)) {
    stop_k8s_containers_with_progress(reaper, progress);
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
    wait_containerd_with_progress(&mut |_| {})
}

fn wait_containerd_with_progress(progress: &mut dyn FnMut(&str)) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last_beat = Instant::now();
    while Instant::now() < deadline {
        if SHUTTING_DOWN.load(Ordering::SeqCst) {
            return Err("aborted: VM is shutting down".into());
        }
        if UnixStream::connect(CONTAINERD_SOCKET).is_ok() {
            return Ok(());
        }
        if last_beat.elapsed() >= PROGRESS_HEARTBEAT_INTERVAL {
            progress("waiting for containerd");
            last_beat = Instant::now();
        }
        thread::sleep(Duration::from_millis(50));
    }
    Err(format!("containerd socket {CONTAINERD_SOCKET} not ready"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CapturedStream {
    text: String,
    truncated: bool,
}

/// Drain `r` completely to EOF so the child never blocks on a full pipe, while
/// retaining at most `limit` bytes and reporting whether any bytes were dropped.
fn drain_capped_checked(mut r: impl Read, limit: usize) -> CapturedStream {
    let mut captured = Vec::new();
    let mut truncated = false;
    let mut chunk = [0u8; 8192];
    loop {
        match r.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => {
                let room = limit.saturating_sub(captured.len());
                if room > 0 {
                    captured.extend_from_slice(&chunk[..n.min(room)]);
                }
                if n > room {
                    truncated = true;
                }
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => {
                truncated = true;
                break;
            }
        }
    }
    CapturedStream {
        text: String::from_utf8_lossy(&captured).into_owned(),
        truncated,
    }
}

/// Drain `r` completely to EOF so the child never blocks on a full pipe, while
/// retaining at most `limit` bytes for diagnostics or parsing.
fn drain_capped(r: impl Read, limit: usize) -> String {
    drain_capped_checked(r, limit).text
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReapedOutput {
    code: i32,
    stdout: String,
    stderr: String,
    stdout_truncated: bool,
}

/// Run `cmd` under PID 1's reaper capturing `stdout` and `stderr` separately
/// so warnings printed to stderr cannot corrupt stdout parsing.
fn run_reaped_split(
    reaper: &Reaper,
    cmd: &mut Command,
    timeout: Duration,
) -> io::Result<(i32, String, String)> {
    let out = run_reaped_split_checked(reaper, cmd, timeout, 1 << 20, false, None)?;
    Ok((out.code, out.stdout, out.stderr))
}

fn remove_dir_all_with_progress(
    path: &Path,
    interval: Duration,
    progress: &mut dyn FnMut(&str),
) -> Result<(), String> {
    run_with_heartbeat(
        interval,
        "removing cluster data",
        progress,
        || fs::remove_dir_all(path).map_err(|e| format!("remove {}: {e}", path.display())),
    )
}

fn run_with_heartbeat<T, F>(
    interval: Duration,
    msg: &str,
    progress: &mut dyn FnMut(&str),
    work: F,
) -> Result<T, String>
where
    T: Send,
    F: FnOnce() -> Result<T, String> + Send,
{
    thread::scope(|s| {
        let (tx, rx) = mpsc::channel();
        let spawned = thread::Builder::new()
            .name("k8s-reset-rm".into())
            .spawn_scoped(s, move || {
                let _ = tx.send(work());
            });
        if spawned.is_err() {
            return Err(format!("spawn worker thread for {msg}"));
        }
        loop {
            match rx.recv_timeout(interval) {
                Ok(res) => return res,
                Err(RecvTimeoutError::Timeout) => progress(msg),
                Err(RecvTimeoutError::Disconnected) => {
                    return Err(format!("worker thread terminated while {msg}"));
                }
            }
        }
    })
}

fn run_reaped_split_checked(
    reaper: &Reaper,
    cmd: &mut Command,
    timeout: Duration,
    stdout_limit: usize,
    abort_on_shutdown: bool,
    mut on_wait: Option<&mut dyn FnMut()>,
) -> io::Result<ReapedOutput> {
    let (out_r, out_w) = pipe()?;
    let (err_r, err_w) = pipe()?;
    cmd.env("PATH", PATH)
        .stdin(Stdio::null())
        .stdout(Stdio::from(out_w))
        .stderr(Stdio::from(err_w));
    let (target, exited) = reaper.spawn_pidfd(cmd)?;
    cmd.stdout(Stdio::null()).stderr(Stdio::null());

    let out_t = match thread::Builder::new()
        .name("k8s-cmd-out".into())
        .spawn(move || drain_capped_checked(File::from(out_r), stdout_limit))
    {
        Ok(h) => h,
        Err(e) => {
            reaper.signal_target(&target, libc::SIGKILL);
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
            reaper.signal_target(&target, libc::SIGKILL);
            let _ = exited.recv_timeout(Duration::from_secs(2));
            let _ = out_t.join();
            return Err(e);
        }
    };

    let deadline = Instant::now() + timeout;
    let mut last_beat = Instant::now();
    let status = loop {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            reaper.signal_target(&target, libc::SIGKILL);
            let _ = exited.recv_timeout(Duration::from_secs(2));
            let _ = out_t.join();
            let _ = err_t.join();
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("{:?} timed out", cmd.get_program()),
            ));
        }
        let step = if abort_on_shutdown {
            left.min(Duration::from_secs(1))
        } else if on_wait.is_some() {
            left.min(PROGRESS_HEARTBEAT_INTERVAL)
        } else {
            left
        };
        match exited.recv_timeout(step) {
            Ok(s) => break s,
            Err(RecvTimeoutError::Timeout) => {
                if abort_on_shutdown && SHUTTING_DOWN.load(Ordering::SeqCst) {
                    reaper.signal_target(&target, libc::SIGKILL);
                    let _ = exited.recv_timeout(Duration::from_secs(2));
                    let _ = out_t.join();
                    let _ = err_t.join();
                    return Err(io::Error::new(
                        io::ErrorKind::Interrupted,
                        "aborted: VM is shutting down",
                    ));
                }
                if let Some(cb) = on_wait.as_deref_mut()
                    && Instant::now() < deadline
                    && last_beat.elapsed() >= PROGRESS_HEARTBEAT_INTERVAL
                {
                    cb();
                    last_beat = Instant::now();
                }
            }
            Err(RecvTimeoutError::Disconnected) => break -1,
        }
    };
    let out = match out_t.join() {
        Ok(o) => o,
        Err(_) => CapturedStream {
            text: String::new(),
            truncated: true,
        },
    };
    let stderr = err_t.join().unwrap_or_default();
    let code = if libc::WIFEXITED(status) {
        libc::WEXITSTATUS(status)
    } else {
        128 + libc::WTERMSIG(status)
    };
    Ok(ReapedOutput {
        code,
        stdout: out.text,
        stderr,
        stdout_truncated: out.truncated,
    })
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
            run_image_sync_loop(&stop, Duration::from_secs(5), || {
                watch_docker_image_events(Path::new(DOCKER_SOCKET), &reaper, &stop)
            });
        });
}

fn run_image_sync_loop(
    stop: &AtomicBool,
    backoff: Duration,
    mut watch_once: impl FnMut() -> io::Result<()>,
) {
    while !stop.load(Ordering::Relaxed) {
        if let Err(e) = watch_once() {
            if stop.load(Ordering::Relaxed) {
                return;
            }
            log(&format!("image sync watcher: {e}"));
        }
        if stop.load(Ordering::Relaxed) {
            return;
        }
        sleep_interruptible(stop, backoff);
    }
}

fn sleep_interruptible(stop: &AtomicBool, total: Duration) {
    let deadline = Instant::now() + total;
    while !stop.load(Ordering::Relaxed) {
        let Some(left) = deadline.checked_duration_since(Instant::now()) else {
            return;
        };
        if left.is_zero() {
            return;
        }
        thread::sleep(left.min(Duration::from_millis(250)));
    }
}

fn watch_docker_image_events(
    socket_path: &Path,
    reaper: &Reaper,
    stop: &AtomicBool,
) -> io::Result<()> {
    watch_docker_image_events_with(socket_path, stop, || {
        if let Err(e) = sync_moby_images_inner(reaper, Some(stop)) {
            log(&format!("background image sync: {e}"));
        }
    })
}

fn watch_docker_image_events_with(
    socket_path: &Path,
    stop: &AtomicBool,
    mut on_sync: impl FnMut(),
) -> io::Result<()> {
    let mut stream = crate::unix_connect::connect_unix_until(
        socket_path,
        Instant::now() + Duration::from_secs(5),
    )?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    // HTTP/1.0 avoids chunked framing; Docker streams one JSON object per line.
    write!(
        stream,
        "GET /events?filters=%7B%22type%22%3A%5B%22image%22%5D%7D HTTP/1.0\r\nHost: docker\r\n\r\n"
    )?;
    stream.flush()?;
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    // Read HTTP response status line and headers before running the initial
    // sync so we never sync against a failed subscription and never miss a
    // Docker change made while the sync runs.
    let mut saw_status = false;
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => return Ok(()),
            Ok(_) if !saw_status => {
                saw_status = true;
                if line.split_whitespace().nth(1) != Some("200") {
                    return Err(io::Error::other(format!(
                        "unexpected Docker /events status: {}",
                        line.trim()
                    )));
                }
            }
            Ok(_) if line == "\r\n" || line == "\n" => break,
            Ok(_) => continue,
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut =>
            {
                if stop.load(Ordering::Acquire) {
                    return Ok(());
                }
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    if stop.load(Ordering::Relaxed) {
        return Ok(());
    }
    // Subscribed: run the initial / post-reconnect sync now. Changes made
    // while this sync runs buffer as events on this stream.
    on_sync();
    loop {
        if stop.load(Ordering::Acquire) {
            return Ok(());
        }
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => return Ok(()),
            Ok(_) => {
                if is_docker_image_sync_event(&line) {
                    on_sync();
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

/// Label recording which `moby` image a re-platformed `k8s.io` copy was made
/// from, since the copy's own digest differs (see [`crate::oci_rewrite`]).
const SOURCE_DIGEST_LABEL: &str = "io.katamaran.k8s.source-digest";

/// The guest's platform architecture, the only one containerd's CRI accepts.
const NATIVE_ARCH: &str = "arm64";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EmulationMode {
    Enabled,
    Disabled,
    Unknown,
}

const MAX_SYNC_RETRIES: usize = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum SyncNoticeKind {
    SkipDigestAmd64,
    KeepMirrorEmulationOff,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SyncNotice {
    kind: SyncNoticeKind,
    name: String,
    digest: String,
}

/// Deduplicates non-failure informational sync logs (`SkipDigestAmd64`,
/// `KeepMirrorEmulationOff`) to once per `(kind, image, digest)` while active.
struct SyncLogState {
    logged: BTreeSet<SyncNotice>,
}

impl SyncLogState {
    const fn new() -> Self {
        Self {
            logged: BTreeSet::new(),
        }
    }

    fn should_log(&mut self, notice: SyncNotice) -> bool {
        self.logged.insert(notice)
    }

    fn retain_active(&mut self, active: &BTreeSet<SyncNotice>) {
        self.logged.retain(|n| active.contains(n));
    }
}

fn sync_moby_images(reaper: &Reaper) -> Result<(), String> {
    sync_moby_images_inner(reaper, None)
}

fn sync_moby_images_inner(reaper: &Reaper, stop: Option<&AtomicBool>) -> Result<(), String> {
    let mut sync_log = IMAGE_SYNC_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    if stop.is_some_and(|s| s.load(Ordering::Acquire)) {
        return Ok(());
    }
    let mut failures: Vec<String> = Vec::new();
    let mut only_refs: Option<HashSet<String>> = None;
    let mut logged_unknown: Vec<String> = Vec::new();
    let mut active_notices: BTreeSet<SyncNotice> = BTreeSet::new();
    let mut completed_full_scan = false;

    for attempt in 0..=MAX_SYNC_RETRIES {
        if stop.is_some_and(|s| s.load(Ordering::Acquire)) {
            return Ok(());
        }
        // Re-read the emulation state on every pass so a retry never acts on
        // a stale reading.
        let emulation = current_emulation_mode(&mut logged_unknown);
        let moby = match list_ctr_images(reaper, "moby") {
            Ok(m) => m,
            Err(e) => {
                failures.push(e);
                break;
            }
        };
        if moby.is_empty() {
            if attempt == 0 {
                completed_full_scan = true;
            }
            break;
        }
        let k8s = match list_ctr_images(reaper, "k8s.io") {
            Ok(k) => k,
            Err(e) => {
                failures.push(e);
                break;
            }
        };
        if attempt == 0 {
            completed_full_scan = true;
        }

        let candidates: Vec<(String, CtrImage)> = match &only_refs {
            Some(filter) => moby
                .iter()
                .filter(|(name, _)| filter.contains(*name))
                .map(|(n, i)| (n.clone(), i.clone()))
                .collect(),
            None => moby.iter().map(|(n, i)| (n.clone(), i.clone())).collect(),
        };

        let mut retry_refs: HashSet<String> = HashSet::new();

        for (name, img) in candidates {
            if stop.is_some_and(|s| s.load(Ordering::Acquire)) {
                return Ok(());
            }
            // `moby-dangling@<digest>` is how Docker's containerd image store
            // keeps untagged images; never mirror or report those.
            if name.starts_with("sha256:")
                || name.starts_with("moby-dangling@")
                || name.contains("<none>")
            {
                continue;
            }
            let k8s_img = k8s.get(&name);
            let source_probe = needs_native_probe(&name, &img, k8s_img, emulation, NATIVE_ARCH)
                .then(|| probe_native_complete(reaper, "moby", &name, &img.digest, NATIVE_ARCH));
            let native = classify_native_availability(
                &name,
                &img,
                k8s_img,
                source_probe.as_ref(),
                emulation,
                NATIVE_ARCH,
            );
            let dest_probe =
                needs_dest_native_probe(&name, &img, k8s_img, native, emulation, NATIVE_ARCH).then(
                    || {
                        let k = k8s_img.expect("destination probe requires k8s_img");
                        probe_native_complete(reaper, "k8s.io", &name, &k.digest, NATIVE_ARCH)
                    },
                );
            let dest_native =
                classify_dest_native_availability(k8s_img, dest_probe.as_ref(), NATIVE_ARCH);
            match plan_sync_action(
                &name,
                &img,
                k8s_img,
                native,
                dest_native,
                emulation,
                NATIVE_ARCH,
            ) {
                SyncAction::Skip => {}
                SyncAction::SkipDigestAmd64 => {
                    let notice = SyncNotice {
                        kind: SyncNoticeKind::SkipDigestAmd64,
                        name: name.clone(),
                        digest: img.digest.clone(),
                    };
                    active_notices.insert(notice.clone());
                    if sync_log.should_log(notice) {
                        log(&format!(
                            "skipping amd64 rewrite for digest-pinned image {name}; tag it locally to run under emulation"
                        ));
                    }
                }
                SyncAction::ReportProbeError => {
                    let detail = source_probe
                        .as_ref()
                        .and_then(|r| r.as_ref().err())
                        .or_else(|| dest_probe.as_ref().and_then(|r| r.as_ref().err()))
                        .map(String::as_str)
                        .unwrap_or("native platform check unavailable");
                    if k8s_img.is_some() {
                        log(&format!(
                            "preserving existing Kubernetes image for {name}: {detail}"
                        ));
                    } else {
                        log(&format!(
                            "skipping {name} due to native platform check error: {detail}"
                        ));
                    }
                    failures.push(format!("{name}: {detail}"));
                }
                SyncAction::Relist => {
                    retry_refs.insert(name);
                }
                SyncAction::ReportEmulationDisabled => {
                    let msg = if k8s_img.is_some() {
                        format!(
                            "{name}: amd64 image not mirrored because amd64 emulation is off; existing Kubernetes copy is kept"
                        )
                    } else {
                        format!("{name}: amd64 image not mirrored because amd64 emulation is off")
                    };
                    log(&msg);
                    failures.push(msg);
                }
                SyncAction::ReportEmulationUnavailable => {
                    let msg = if k8s_img.is_some() {
                        format!(
                            "{name}: amd64 image not mirrored: amd64 emulation is unavailable (see the guest log); existing Kubernetes copy is kept"
                        )
                    } else {
                        format!(
                            "{name}: amd64 image not mirrored: amd64 emulation is unavailable (see the guest log)"
                        )
                    };
                    log(&msg);
                    failures.push(msg);
                }
                SyncAction::ReportStaleMirror => {
                    let msg = format!(
                        "{name}: Docker now points to {} with no linux/{NATIVE_ARCH} or linux/amd64 variant to mirror; Kubernetes keeps the previous build under this tag",
                        img.digest
                    );
                    log(&msg);
                    failures.push(msg);
                }
                SyncAction::ReportDivergedTag => {
                    let msg = format!(
                        "{name}: Kubernetes keeps a different image under this name ({}) than Docker ({}); katamaran never replaces an image it did not publish. Give the Docker image a different local name (`docker tag {name} <new-name> && docker rmi {name}`) and use that name in the pod spec",
                        k8s_img.map(|k| k.digest.as_str()).unwrap_or("-"),
                        img.digest
                    );
                    log(&msg);
                    failures.push(msg);
                }
                SyncAction::ReportUnownedDestination => {
                    let msg = unowned_destination_message(&name);
                    log(&msg);
                    failures.push(msg);
                }
                SyncAction::KeepMirrorEmulationOff => {
                    // Never deleted automatically: a pod may still use it, and
                    // its Docker source may be gone. Leaking it is acceptable.
                    let notice = SyncNotice {
                        kind: SyncNoticeKind::KeepMirrorEmulationOff,
                        name: name.clone(),
                        digest: format!(
                            "{}:{}",
                            img.digest,
                            k8s_img.map(|k| k.digest.as_str()).unwrap_or("")
                        ),
                    };
                    active_notices.insert(notice.clone());
                    if sync_log.should_log(notice) {
                        log(&format!(
                            "keeping amd64 mirror {name} in Kubernetes although amd64 emulation is off"
                        ));
                    }
                }
                SyncAction::MirrorNative { clear_source_label } => {
                    match mirror_native_image(reaper, &name) {
                        Ok(()) => {
                            if clear_source_label
                                && let Err(e) = clear_source_digest_label(reaper, &name)
                            {
                                log(&format!("clear source-digest label on {name}: {e}"));
                                failures.push(format!("{name}: {e}"));
                            }
                        }
                        Err(e) => {
                            log(&format!("mirror image {name}: {e}"));
                            failures.push(format!("{name}: {e}"));
                        }
                    }
                }
                SyncAction::MirrorRewrite => {
                    // Only a Katamaran publication is ever replaced: the tag
                    // must carry the source-digest label AND its manifest the
                    // publication annotation (a stale label on a native import
                    // is not ownership). Anything else is reported, not touched.
                    if let Some(k) = k8s_img {
                        let owned = k.label(SOURCE_DIGEST_LABEL).is_some()
                            && match is_katamaran_published(reaper, &name, &k.digest) {
                                Ok(owned) => owned,
                                Err(e) => {
                                    log(&format!("mirror image {name}: {e}"));
                                    failures.push(format!("{name}: {e}"));
                                    continue;
                                }
                            };
                        if !owned {
                            let msg = unowned_destination_message(&name);
                            log(&msg);
                            failures.push(msg);
                            continue;
                        }
                    }
                    let mut info_client = match containerd_grpc::ContentInfoClient::connect(
                        Path::new(CONTAINERD_SOCKET),
                        Duration::from_secs(30),
                    ) {
                        Ok(c) => c,
                        Err(e) => {
                            log(&format!("mirror image {name}: {e}"));
                            failures.push(format!("{name}: {e}"));
                            continue;
                        }
                    };
                    let prepare_res = {
                        let mut store = MobyContentStore::new(
                            GrpcNamespaceMeta {
                                client: &mut info_client,
                                namespace: "moby",
                            },
                            |digest: &str, max_bytes: usize| {
                                get_ctr_content_blob(reaper, "moby", digest, max_bytes)
                            },
                            |digest: &str| inspect_ctr_config_platform(reaper, "moby", digest),
                        );
                        oci_rewrite::prepare_rewrite_from_store(
                            &img.digest,
                            NATIVE_ARCH,
                            &mut store,
                        )
                    };
                    let plan = match prepare_res {
                        Ok(oci_rewrite::PrepareRewrite::Ready(plan)) => *plan,
                        Ok(oci_rewrite::PrepareRewrite::RootMissing) => {
                            match classify_prepare_root_missing(
                                &name,
                                &img.digest,
                                info_client.image_digest("moby", &name),
                            ) {
                                Ok(()) => {
                                    retry_refs.insert(name);
                                }
                                Err(msg) => {
                                    log(&msg);
                                    failures.push(msg);
                                }
                            }
                            continue;
                        }
                        Ok(oci_rewrite::PrepareRewrite::NotAvailable) => {
                            // No complete amd64 variant to rewrite. With
                            // emulation on, never delete an existing mirror
                            // for this: keep it and report.
                            let msg = format!(
                                "{name}: no complete local linux/amd64 variant to mirror under emulation"
                            );
                            log(&msg);
                            failures.push(msg);
                            continue;
                        }
                        Err(e) => {
                            log(&format!("prepare amd64 rewrite for {name}: {e}"));
                            failures.push(format!("{name}: {e}"));
                            continue;
                        }
                    };
                    let selected_platform = plan.selected_platform.clone();
                    // `publish_rewritten_image` re-verifies the target digest
                    // and labels against this evidence before deleting.
                    let expected_k8s = k8s_img.map(|k| containerd_grpc::ExpectedK8sTarget {
                        digest: k.digest.as_str(),
                        is_cri_managed: k.label("io.cri-containerd.image") == Some("managed"),
                    });
                    match containerd_grpc::publish_rewritten_image(
                        Path::new(CONTAINERD_SOCKET),
                        &name,
                        SOURCE_DIGEST_LABEL,
                        expected_k8s,
                        &plan,
                        Duration::from_secs(120),
                    ) {
                        Ok(_) => log(&format!(
                            "mirrored amd64 image {name} ({selected_platform}) as linux/{NATIVE_ARCH} (runs under emulation)"
                        )),
                        Err(
                            containerd_grpc::PublishError::SourceChanged { .. }
                            | containerd_grpc::PublishError::DestinationChanged { .. },
                        ) => {
                            retry_refs.insert(name);
                        }
                        Err(containerd_grpc::PublishError::ReplacementUncertain(e)) => {
                            // Nothing was released; report it and re-list (when
                            // retries remain) so a missing tag is recreated
                            // from Docker.
                            log(&format!("mirror image {name}: {e}; re-listing"));
                            failures.push(format!("{name}: {e}"));
                            if attempt < MAX_SYNC_RETRIES {
                                retry_refs.insert(name);
                            }
                        }
                        Err(containerd_grpc::PublishError::Failed(e)) => {
                            log(&format!("mirror image {name}: {e}"));
                            failures.push(format!("{name}: {e}"));
                        }
                    }
                }
            }
        }

        if retry_refs.is_empty() {
            break;
        }
        if attempt == MAX_SYNC_RETRIES {
            let mut stuck: Vec<String> = retry_refs.into_iter().collect();
            stuck.sort();
            for name in stuck {
                let msg = format!("{name}: image target changed concurrently during sync");
                log(&msg);
                failures.push(msg);
            }
            break;
        }
        // Brief backoff so a burst of Docker retags can settle before the
        // next pass re-lists.
        thread::sleep(Duration::from_millis(250 * (attempt as u64 + 1)));
        only_refs = Some(retry_refs);
    }

    if completed_full_scan {
        sync_log.retain_active(&active_notices);
    }

    if !failures.is_empty() {
        return Err(format!("failed to mirror images: {}", failures.join("; ")));
    }
    Ok(())
}

fn current_emulation_mode(logged_unknown: &mut Vec<String>) -> EmulationMode {
    match super::amd64::status() {
        super::amd64::EmulationStatus::Enabled => EmulationMode::Enabled,
        super::amd64::EmulationStatus::Disabled => EmulationMode::Disabled,
        super::amd64::EmulationStatus::Unknown(e) => {
            // Not a sync failure by itself: native images still mirror
            // normally, while amd64 tags are held back and reported per image.
            let msg = format!("amd64 emulation state unknown ({e}); not rewriting amd64 images");
            if !logged_unknown.contains(&msg) {
                log(&msg);
                logged_unknown.push(msg);
            }
            EmulationMode::Unknown
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NativeAvailability {
    Present,
    Absent,
    ProbeFailed,
    Stale,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SyncAction {
    Skip,
    SkipDigestAmd64,
    MirrorNative {
        clear_source_label: bool,
    },
    MirrorRewrite,
    /// A Katamaran amd64 mirror whose source would no longer be mirrored
    /// because emulation is off: kept (never deleted automatically) and logged.
    KeepMirrorEmulationOff,
    /// Kubernetes has an unlabelled image under this name whose digest differs
    /// from Docker's, and sync would otherwise leave it silently.
    ReportDivergedTag,
    Relist,
    ReportProbeError,
    /// `k8s.io` already has an unlabelled tag under this name that is neither
    /// a Katamaran mirror nor an exact copy of the `moby` source, and has no
    /// complete local native content. It is never replaced (a Delete→Create
    /// swap cannot be made atomic, and an interrupted swap could lose a tag
    /// Katamaran cannot rebuild); the user is told how to resolve the conflict.
    ReportUnownedDestination,
    /// An amd64 tag that cannot be mirrored because `amd64 = "off"`; any
    /// existing Kubernetes copy is kept.
    ReportEmulationDisabled,
    /// An amd64 tag that would be mirrored under emulation, but the emulation
    /// state is unknown (e.g. the emulator failed to register at boot).
    ReportEmulationUnavailable,
    /// Docker's tag now points to an image Katamaran cannot mirror (no
    /// linux/arm64 or rewritable linux/amd64 variant); the previous build is
    /// kept under the tag.
    ReportStaleMirror,
}

fn unowned_destination_message(name: &str) -> String {
    format!(
        "{name}: not mirroring amd64 image under emulation because Kubernetes already has \
         an image with this name that katamaran cannot safely replace (it was not \
         published by this version of katamaran, or it is a multi-platform image that \
         could still gain local linux/{NATIVE_ARCH} content); katamaran never deletes such \
         an image. Give the Docker image \
         a different local name (`docker tag {name} <new-name> && docker rmi {name}`) and \
         use that name in the pod spec"
    )
}

/// True when a strict native-content probe is needed to tell whether `moby`
/// actually has complete `linux/<native_arch>` content locally (rather than
/// merely advertising a native descriptor in a multi-platform index, or only
/// having 32-bit `linux/arm/*` blobs).
fn needs_native_probe(
    name: &str,
    moby: &CtrImage,
    k8s: Option<&CtrImage>,
    emulation: EmulationMode,
    native_arch: &str,
) -> bool {
    let Some(platforms) = moby.platforms.as_deref() else {
        return false;
    };
    if !oci_rewrite::has_native_platform(platforms, native_arch) {
        return false;
    }
    let k8s_is_rewritten = k8s.is_some_and(|k| k.label(SOURCE_DIGEST_LABEL).is_some());
    if k8s_is_rewritten {
        return true;
    }
    if !oci_rewrite::has_amd64_platform(platforms) {
        return false;
    }
    // When `k8s.io` already holds the exact same unlabelled digest, skip the
    // probe whenever the outcome cannot change the sync action (emulation is
    // not enabled, or the reference is digest-pinned and never rewritten).
    // With emulation enabled on a tagged image, keep probing so an unusable
    // same-digest legacy copy is still reported instead of claimed synced.
    let same_unlabelled =
        k8s.is_some_and(|k| k.label(SOURCE_DIGEST_LABEL).is_none() && k.digest == moby.digest);
    !(same_unlabelled
        && (emulation != EmulationMode::Enabled || oci_rewrite::is_digest_reference(name)))
}

/// True when a strict native-content probe on `k8s.io` is needed before
/// deciding whether to preserve an unlabelled destination tag (an unlabelled
/// multi-platform index can advertise `linux/arm64` in `ctr images ls` even
/// when only its `amd64` blobs are cached locally).
fn needs_dest_native_probe(
    name: &str,
    moby: &CtrImage,
    k8s: Option<&CtrImage>,
    source_native: NativeAvailability,
    emulation: EmulationMode,
    native_arch: &str,
) -> bool {
    if emulation != EmulationMode::Enabled
        || source_native != NativeAvailability::Absent
        || oci_rewrite::is_digest_reference(name)
    {
        return false;
    }
    let Some(moby_platforms) = moby.platforms.as_deref() else {
        return false;
    };
    if !oci_rewrite::has_amd64_platform(moby_platforms) {
        return false;
    }
    let Some(k) = k8s else {
        return false;
    };
    if k.label(SOURCE_DIGEST_LABEL).is_some() {
        return false;
    }
    k.platforms
        .as_deref()
        .is_some_and(|p| oci_rewrite::has_native_platform(p, native_arch))
}

fn classify_native_availability(
    name: &str,
    moby: &CtrImage,
    k8s: Option<&CtrImage>,
    probe: Option<&Result<NativeAvailability, String>>,
    emulation: EmulationMode,
    native_arch: &str,
) -> NativeAvailability {
    let Some(platforms) = moby.platforms.as_deref() else {
        return NativeAvailability::Stale;
    };
    if !oci_rewrite::has_native_platform(platforms, native_arch) {
        return NativeAvailability::Absent;
    }
    if !needs_native_probe(name, moby, k8s, emulation, native_arch) {
        return NativeAvailability::Present;
    }
    match probe {
        Some(Ok(avail)) => *avail,
        Some(Err(_)) | None => NativeAvailability::ProbeFailed,
    }
}

fn classify_dest_native_availability(
    k8s: Option<&CtrImage>,
    probe: Option<&Result<NativeAvailability, String>>,
    native_arch: &str,
) -> NativeAvailability {
    let Some(k) = k8s else {
        return NativeAvailability::Absent;
    };
    if k.label(SOURCE_DIGEST_LABEL).is_some() {
        return NativeAvailability::Absent;
    }
    let Some(platforms) = k.platforms.as_deref() else {
        return NativeAvailability::Stale;
    };
    if !oci_rewrite::has_native_platform(platforms, native_arch) {
        return NativeAvailability::Absent;
    }
    match probe {
        Some(Ok(avail)) => *avail,
        Some(Err(_)) | None => NativeAvailability::ProbeFailed,
    }
}

/// Decide how to synchronize a single `moby` image into `k8s.io`. Never
/// claims an image synced when Kubernetes keeps a different image or an
/// unmirrored amd64 tag under the same name.
fn plan_sync_action(
    name: &str,
    moby: &CtrImage,
    k8s: Option<&CtrImage>,
    native: NativeAvailability,
    dest_native: NativeAvailability,
    emulation: EmulationMode,
    native_arch: &str,
) -> SyncAction {
    let k8s_is_rewritten = k8s.is_some_and(|k| k.label(SOURCE_DIGEST_LABEL).is_some());
    match native {
        NativeAvailability::ProbeFailed => SyncAction::ReportProbeError,
        NativeAvailability::Stale => SyncAction::Relist,
        NativeAvailability::Present => {
            if let Some(k) = k8s
                && k.digest == moby.digest
                && !k8s_is_rewritten
            {
                SyncAction::Skip
            } else {
                SyncAction::MirrorNative {
                    clear_source_label: k8s_is_rewritten,
                }
            }
        }
        NativeAvailability::Absent => {
            let Some(platforms) = moby.platforms.as_deref() else {
                return SyncAction::Relist;
            };
            let has_amd64 = oci_rewrite::has_amd64_platform(platforms);
            let is_digest_ref = oci_rewrite::is_digest_reference(name);
            let resolved = !platforms.trim().is_empty() && platforms.trim() != "-";
            let k8s_matches_source =
                k8s.is_some_and(|k| k.label(SOURCE_DIGEST_LABEL) == Some(moby.digest.as_str()));

            // Digest-pinned references can never be rewritten because rewriting
            // changes the manifest digest; existing mirrors are kept.
            if is_digest_ref {
                if emulation == EmulationMode::Disabled && k8s_is_rewritten {
                    return SyncAction::KeepMirrorEmulationOff;
                }
                if emulation == EmulationMode::Enabled && has_amd64 {
                    return SyncAction::SkipDigestAmd64;
                }
                if k8s_is_rewritten {
                    return SyncAction::Skip;
                }
                if !resolved {
                    return if k8s.is_some_and(|k| k.digest == moby.digest) {
                        SyncAction::Skip
                    } else {
                        SyncAction::MirrorNative {
                            clear_source_label: false,
                        }
                    };
                }
                return SyncAction::Skip;
            }

            if has_amd64 {
                return match emulation {
                    EmulationMode::Enabled => {
                        if let Some(k) = k8s {
                            if k8s_matches_source {
                                match k.platforms.as_deref() {
                                    None => SyncAction::Relist,
                                    Some(p) if oci_rewrite::has_native_platform(p, native_arch) => {
                                        SyncAction::Skip
                                    }
                                    Some(_) => SyncAction::MirrorRewrite,
                                }
                            } else if k8s_is_rewritten {
                                // Stale Katamaran emulation mirror: update via CAS on `k.digest`.
                                SyncAction::MirrorRewrite
                            } else {
                                // Unlabelled `k8s.io` image (a kubelet/`ctr` pull, a
                                // native mirror, or a copy left by an earlier release):
                                // never replaced. Stay quiet only when its local native
                                // arm64 content is verified complete and it matches
                                // Docker's digest; otherwise report the conflict.
                                match dest_native {
                                    NativeAvailability::Present if k.digest == moby.digest => {
                                        SyncAction::Skip
                                    }
                                    NativeAvailability::Present => SyncAction::ReportDivergedTag,
                                    NativeAvailability::Absent => {
                                        SyncAction::ReportUnownedDestination
                                    }
                                    NativeAvailability::Stale => SyncAction::Relist,
                                    NativeAvailability::ProbeFailed => SyncAction::ReportProbeError,
                                }
                            }
                        } else {
                            SyncAction::MirrorRewrite
                        }
                    }
                    EmulationMode::Disabled => {
                        if k8s_matches_source {
                            SyncAction::KeepMirrorEmulationOff
                        } else {
                            SyncAction::ReportEmulationDisabled
                        }
                    }
                    EmulationMode::Unknown => {
                        if k8s_matches_source {
                            SyncAction::Skip
                        } else {
                            SyncAction::ReportEmulationUnavailable
                        }
                    }
                };
            }

            if !resolved {
                return if k8s_is_rewritten {
                    if emulation == EmulationMode::Disabled {
                        SyncAction::KeepMirrorEmulationOff
                    } else {
                        SyncAction::Skip
                    }
                } else if k8s.is_some_and(|k| k.digest == moby.digest) {
                    SyncAction::Skip
                } else {
                    SyncAction::MirrorNative {
                        clear_source_label: false,
                    }
                };
            }

            // Resolved non-native, non-amd64 image (e.g. linux/s390x): keep any
            // existing Kubernetes copy, and report when Docker's tag moved to a
            // different digest that Katamaran cannot mirror.
            if k8s_is_rewritten {
                if !k8s_matches_source {
                    SyncAction::ReportStaleMirror
                } else if emulation == EmulationMode::Disabled {
                    SyncAction::KeepMirrorEmulationOff
                } else {
                    SyncAction::Skip
                }
            } else if k8s.is_some_and(|k| k.digest != moby.digest) {
                SyncAction::ReportStaleMirror
            } else {
                SyncAction::Skip
            }
        }
    }
}

/// Bounded stdout cap for `ctr images ls` and `Images.List`. Exceeding this
/// cap returns an explicit error instead of silently truncating the inventory.
const IMAGE_LIST_LIMIT: usize = 16 << 20;

/// Strictly verify whether `name` in `namespace` (expected to still point to
/// `expected_digest`) has a locally complete `linux/<native_arch>` manifest
/// matching containerd v2.4.2's `images.Manifest` + `platforms.DefaultStrict()`
/// selection (manifest, config, and all layers present locally; never accepting
/// 32-bit `linux/arm/*` or non-baseline variants such as `linux/arm64/v8.2`).
fn probe_native_complete(
    reaper: &Reaper,
    namespace: &str,
    name: &str,
    expected_digest: &str,
    native_arch: &str,
) -> Result<NativeAvailability, String> {
    let mut client = containerd_grpc::ContentInfoClient::connect(
        Path::new(CONTAINERD_SOCKET),
        Duration::from_secs(30),
    )?;
    match client.image_digest(namespace, name)? {
        Some(cur) if cur == expected_digest => {}
        _ => return Ok(NativeAvailability::Stale),
    }
    let check = {
        let mut store = MobyContentStore::new(
            GrpcNamespaceMeta {
                client: &mut client,
                namespace,
            },
            |digest: &str, max_bytes: usize| {
                get_ctr_content_blob(reaper, namespace, digest, max_bytes)
            },
            |digest: &str| inspect_ctr_config_platform(reaper, namespace, digest),
        );
        oci_rewrite::has_complete_native_manifest(
            expected_digest,
            oci_rewrite::NativePlatform::linux(native_arch, None),
            &mut store,
        )?
    };
    classify_native_check_outcome(namespace, expected_digest, check, || {
        client.image_digest(namespace, name)
    })
}

/// Distinguish a genuine concurrent retag (`Stale` -> re-list) from a dangling
/// image record whose root manifest blob is missing from `namespace`'s content
/// store (`Err` -> fail immediately instead of relisting into a misleading
/// "changed concurrently" error).
fn classify_native_check_outcome(
    namespace: &str,
    expected_digest: &str,
    check: oci_rewrite::NativeManifestCheck,
    mut recheck_digest: impl FnMut() -> Result<Option<String>, String>,
) -> Result<NativeAvailability, String> {
    match check {
        oci_rewrite::NativeManifestCheck::Complete => Ok(NativeAvailability::Present),
        oci_rewrite::NativeManifestCheck::Incomplete => Ok(NativeAvailability::Absent),
        oci_rewrite::NativeManifestCheck::RootMissing => match recheck_digest()? {
            Some(cur) if cur == expected_digest => Err(format!(
                "root manifest blob {expected_digest} is missing from {namespace} content store"
            )),
            _ => Ok(NativeAvailability::Stale),
        },
    }
}

fn classify_prepare_root_missing(
    name: &str,
    expected_digest: &str,
    current_digest: Result<Option<String>, String>,
) -> Result<(), String> {
    match current_digest {
        Ok(Some(cur)) if cur == expected_digest => Err(format!(
            "{name}: root manifest blob {expected_digest} is missing from moby content store"
        )),
        Ok(_) => Ok(()),
        Err(e) => Err(format!(
            "{name}: root manifest blob {expected_digest} is missing from moby content store (re-check failed: {e})"
        )),
    }
}

/// Upper bound for a manifest `publish_rewritten_image` can have written: the
/// rewriter's whole metadata budget plus room for the publication annotation.
const PUBLISHED_MANIFEST_MAX_BYTES: usize = oci_rewrite::MAX_TOTAL_METADATA_BYTES + (64 << 10);

/// Whether the `k8s.io` manifest at `digest` was published by Katamaran (it
/// carries the per-publication annotation). The source-digest label alone is
/// not proof (a native import whose label clearing failed still carries it).
/// Content is immutable per digest and every delete is conditional on that
/// digest, so this check cannot race. A manifest larger than any publication
/// could be is not ours.
fn is_katamaran_published(reaper: &Reaper, name: &str, digest: &str) -> Result<bool, String> {
    match get_ctr_content_blob(reaper, "k8s.io", digest, PUBLISHED_MANIFEST_MAX_BYTES) {
        Ok(manifest) => Ok(is_published_mirror_manifest(&manifest)),
        Err(oci_rewrite::BlobReadError::TooLarge(_)) => Ok(false),
        Err(oci_rewrite::BlobReadError::Other(e)) => {
            Err(format!("read k8s.io manifest {digest} of {name}: {e}"))
        }
    }
}

/// True when `manifest` carries [`containerd_grpc::PUBLICATION_ANNOTATION`],
/// which only `publish_rewritten_image` ever writes.
fn is_published_mirror_manifest(manifest: &[u8]) -> bool {
    serde_json::from_slice::<serde_json::Value>(manifest).is_ok_and(|v| {
        v.get("annotations")
            .and_then(|a| a.get(containerd_grpc::PUBLICATION_ANNOTATION))
            .is_some_and(serde_json::Value::is_string)
    })
}

fn clear_source_digest_label(reaper: &Reaper, name: &str) -> Result<(), String> {
    let mut cmd = Command::new("/usr/bin/ctr");
    cmd.args([
        "-a",
        CONTAINERD_SOCKET,
        "-n",
        "k8s.io",
        "images",
        "label",
        name,
        &format!("{SOURCE_DIGEST_LABEL}="),
    ]);
    let (code, stdout, stderr) = run_reaped_split(reaper, &mut cmd, Duration::from_secs(15))
        .map_err(|e| format!("ctr images label {name}: {e}"))?;
    if code == 0 {
        return Ok(());
    }
    let detail = if stderr.trim().is_empty() {
        stdout.trim()
    } else {
        stderr.trim()
    };
    Err(format!("ctr images label {name} exited {code}: {detail}"))
}

trait MobyContentMeta {
    fn blob_size(&mut self, digest: &str) -> Result<Option<u64>, String>;
    fn batch_blob_sizes(&mut self, digests: &[&str]) -> Result<HashMap<String, u64>, String>;
}

struct GrpcNamespaceMeta<'a> {
    client: &'a mut containerd_grpc::ContentInfoClient,
    namespace: &'a str,
}

impl MobyContentMeta for GrpcNamespaceMeta<'_> {
    fn blob_size(&mut self, digest: &str) -> Result<Option<u64>, String> {
        self.client.blob_size(self.namespace, digest)
    }

    fn batch_blob_sizes(&mut self, digests: &[&str]) -> Result<HashMap<String, u64>, String> {
        self.client.batch_blob_sizes(self.namespace, digests)
    }
}

/// Lazy, caching `moby` content-store adapter that resolves authoritative blob
/// sizes via direct digest-keyed `Content.Info` lookups (single and bounded
/// batches; never `Content.List`) so unrelated blobs in `moby` never slow or
/// block a rewrite. Rejects over-budget metadata blobs before running
/// `ctr content get`, keeping the bounded stdout read as a backstop.
struct MobyContentStore<M, F, C> {
    cache: HashMap<String, Option<u64>>,
    meta: M,
    fetch: F,
    inspect_config: C,
}

impl<M, F, C> MobyContentStore<M, F, C>
where
    M: MobyContentMeta,
    F: FnMut(&str, usize) -> Result<Vec<u8>, oci_rewrite::BlobReadError>,
    C: FnMut(&str) -> Result<Option<oci_rewrite::ConfigPlatform>, String>,
{
    fn new(meta: M, fetch: F, inspect_config: C) -> Self {
        Self {
            cache: HashMap::new(),
            meta,
            fetch,
            inspect_config,
        }
    }
}

impl<M, F, C> oci_rewrite::BlobStore for MobyContentStore<M, F, C>
where
    M: MobyContentMeta,
    F: FnMut(&str, usize) -> Result<Vec<u8>, oci_rewrite::BlobReadError>,
    C: FnMut(&str) -> Result<Option<oci_rewrite::ConfigPlatform>, String>,
{
    fn blob_size(&mut self, digest: &str) -> Result<Option<u64>, String> {
        if let Some(&cached) = self.cache.get(digest) {
            return Ok(cached);
        }
        let size = self.meta.blob_size(digest)?;
        self.cache.insert(digest.to_string(), size);
        Ok(size)
    }

    fn batch_blob_sizes(&mut self, digests: &[&str]) -> Result<HashMap<String, u64>, String> {
        let mut out = HashMap::new();
        let mut uncached = Vec::new();
        let mut seen = HashSet::new();
        for &digest in digests {
            if let Some(&cached) = self.cache.get(digest) {
                if let Some(size) = cached {
                    out.insert(digest.to_string(), size);
                }
                continue;
            }
            if seen.insert(digest) {
                uncached.push(digest);
            }
        }
        if !uncached.is_empty() {
            let found = self.meta.batch_blob_sizes(&uncached)?;
            for digest in uncached {
                let size = found.get(digest).copied();
                self.cache.insert(digest.to_string(), size);
                if let Some(sz) = size {
                    out.insert(digest.to_string(), sz);
                }
            }
        }
        Ok(out)
    }

    fn read_json_blob(
        &mut self,
        digest: &str,
        max_bytes: usize,
    ) -> Result<Vec<u8>, oci_rewrite::BlobReadError> {
        let Some(size) = self
            .blob_size(digest)
            .map_err(oci_rewrite::BlobReadError::Other)?
        else {
            return Err(oci_rewrite::BlobReadError::Other(format!(
                "blob {digest} missing from moby content store"
            )));
        };
        if size > max_bytes as u64 {
            return Err(oci_rewrite::BlobReadError::TooLarge(format!(
                "blob {digest} ({size} bytes) exceeds {max_bytes}-byte remaining budget"
            )));
        }
        (self.fetch)(digest, max_bytes)
    }

    fn inspect_config_platform(
        &mut self,
        digest: &str,
    ) -> Result<Option<oci_rewrite::ConfigPlatform>, String> {
        if self.blob_size(digest)?.is_none() {
            return Ok(None);
        }
        (self.inspect_config)(digest)
    }
}

/// Stream-inspect `(os, architecture, variant, os.features)` of `digest` in
/// `ns` directly from `ctr content get`'s stdout pipe using O(1) memory so
/// platformless native configs larger than `MAX_TOTAL_METADATA_BYTES` never
/// block native sync or spike guest PID 1 memory.
fn inspect_ctr_config_platform(
    reaper: &Reaper,
    ns: &str,
    digest: &str,
) -> Result<Option<oci_rewrite::ConfigPlatform>, String> {
    let mut cmd = Command::new("/usr/bin/ctr");
    cmd.args(["-a", CONTAINERD_SOCKET, "-n", ns, "content", "get", digest]);
    let (out_r, out_w) = pipe().map_err(|e| format!("ctr content get {digest} pipe: {e}"))?;
    let (err_r, err_w) = pipe().map_err(|e| format!("ctr content get {digest} pipe: {e}"))?;
    cmd.env("PATH", PATH)
        .stdin(Stdio::null())
        .stdout(Stdio::from(out_w))
        .stderr(Stdio::from(err_w));
    let (pid, exited) = reaper
        .spawn(&mut cmd)
        .map_err(|e| format!("spawn ctr content get {digest}: {e}"))?;
    cmd.stdout(Stdio::null()).stderr(Stdio::null());

    let out_t = match thread::Builder::new()
        .name("k8s-cfg-scan".into())
        .spawn(move || oci_rewrite::parse_config_platform_reader(File::from(out_r)))
    {
        Ok(h) => h,
        Err(e) => {
            unsafe { libc::kill(pid, libc::SIGKILL) };
            let _ = exited.recv_timeout(Duration::from_secs(2));
            return Err(format!("spawn config stream reader: {e}"));
        }
    };
    let err_t = match thread::Builder::new()
        .name("k8s-cfg-err".into())
        .spawn(move || drain_capped(File::from(err_r), 64 << 10))
    {
        Ok(h) => h,
        Err(e) => {
            unsafe { libc::kill(pid, libc::SIGKILL) };
            let _ = exited.recv_timeout(Duration::from_secs(2));
            let _ = out_t.join();
            return Err(format!("spawn config stderr reader: {e}"));
        }
    };

    let status = match exited.recv_timeout(Duration::from_secs(15)) {
        Ok(s) => s,
        Err(RecvTimeoutError::Timeout) => {
            unsafe { libc::kill(pid, libc::SIGKILL) };
            let _ = exited.recv_timeout(Duration::from_secs(2));
            let _ = out_t.join();
            let _ = err_t.join();
            return Err(format!("ctr content get {digest} timed out"));
        }
        Err(RecvTimeoutError::Disconnected) => -1,
    };
    let parsed = out_t
        .join()
        .map_err(|_| "config stream reader panicked".to_string())??;
    let stderr = err_t.join().unwrap_or_default();
    let code = if libc::WIFEXITED(status) {
        libc::WEXITSTATUS(status)
    } else {
        128 + libc::WTERMSIG(status)
    };
    if code != 0 {
        return Err(format!(
            "ctr content get {digest} exited {code}: {}",
            stderr.trim()
        ));
    }
    Ok(parsed)
}

fn get_ctr_content_blob(
    reaper: &Reaper,
    ns: &str,
    digest: &str,
    max_bytes: usize,
) -> Result<Vec<u8>, oci_rewrite::BlobReadError> {
    let mut cmd = Command::new("/usr/bin/ctr");
    cmd.args(["-a", CONTAINERD_SOCKET, "-n", ns, "content", "get", digest]);
    let (code, stdout, stderr) =
        match run_reaped_bytes(reaper, &mut cmd, Duration::from_secs(15), max_bytes) {
            Ok(v) => v,
            Err(RunBytesError::ExceededLimit(msg)) => {
                return Err(oci_rewrite::BlobReadError::TooLarge(format!(
                    "ctr content get {digest}: {msg}"
                )));
            }
            Err(RunBytesError::Other(msg)) => {
                return Err(oci_rewrite::BlobReadError::Other(format!(
                    "ctr content get {digest}: {msg}"
                )));
            }
        };
    if code != 0 {
        return Err(oci_rewrite::BlobReadError::Other(format!(
            "ctr content get {digest} exited {code}: {}",
            stderr.trim()
        )));
    }
    Ok(stdout)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RunBytesError {
    ExceededLimit(String),
    Other(String),
}

impl std::fmt::Display for RunBytesError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ExceededLimit(msg) | Self::Other(msg) => f.write_str(msg),
        }
    }
}

/// Run `cmd` under PID 1's reaper capturing raw `stdout` up to `limit` bytes
/// (returning [`RunBytesError::ExceededLimit`] rather than silently truncating
/// if `stdout` exceeds `limit`) and capped `stderr`.
fn run_reaped_bytes(
    reaper: &Reaper,
    cmd: &mut Command,
    timeout: Duration,
    limit: usize,
) -> Result<(i32, Vec<u8>, String), RunBytesError> {
    let (out_r, out_w) = pipe().map_err(|e| RunBytesError::Other(format!("pipe: {e}")))?;
    let (err_r, err_w) = pipe().map_err(|e| RunBytesError::Other(format!("pipe: {e}")))?;
    cmd.env("PATH", PATH)
        .stdin(Stdio::null())
        .stdout(Stdio::from(out_w))
        .stderr(Stdio::from(err_w));
    let (pid, exited) = reaper
        .spawn(cmd)
        .map_err(|e| RunBytesError::Other(format!("spawn: {e}")))?;
    cmd.stdout(Stdio::null()).stderr(Stdio::null());

    let out_t = match thread::Builder::new()
        .name("k8s-cmd-bytes".into())
        .spawn(move || drain_bytes_checked(File::from(out_r), limit))
    {
        Ok(h) => h,
        Err(e) => {
            unsafe { libc::kill(pid, libc::SIGKILL) };
            let _ = exited.recv_timeout(Duration::from_secs(2));
            return Err(RunBytesError::Other(format!("spawn stdout reader: {e}")));
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
            return Err(RunBytesError::Other(format!("spawn stderr reader: {e}")));
        }
    };

    let status = match exited.recv_timeout(timeout) {
        Ok(s) => s,
        Err(RecvTimeoutError::Timeout) => {
            unsafe { libc::kill(pid, libc::SIGKILL) };
            let _ = exited.recv_timeout(Duration::from_secs(2));
            let _ = out_t.join();
            let _ = err_t.join();
            return Err(RunBytesError::Other(format!(
                "{:?} timed out",
                cmd.get_program()
            )));
        }
        Err(RecvTimeoutError::Disconnected) => -1,
    };
    let stdout = out_t
        .join()
        .map_err(|_| RunBytesError::Other("stdout reader panicked".to_string()))??;
    let stderr = err_t.join().unwrap_or_default();
    let code = if libc::WIFEXITED(status) {
        libc::WEXITSTATUS(status)
    } else {
        128 + libc::WTERMSIG(status)
    };
    Ok((code, stdout, stderr))
}

fn drain_bytes_checked(mut r: impl Read, limit: usize) -> Result<Vec<u8>, RunBytesError> {
    let mut captured = Vec::new();
    let mut overflowed = false;
    let mut chunk = [0u8; 8192];
    loop {
        match r.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => {
                if !overflowed {
                    if captured.len().saturating_add(n) > limit {
                        overflowed = true;
                        captured.clear();
                    } else {
                        captured.extend_from_slice(&chunk[..n]);
                    }
                }
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(RunBytesError::Other(format!("read stdout: {e}"))),
        }
    }
    if overflowed {
        return Err(RunBytesError::ExceededLimit(format!(
            "command stdout exceeded {limit}-byte limit"
        )));
    }
    Ok(captured)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CtrImage {
    digest: String,
    /// Comma-separated `os/arch[/variant]` list from `ctr images ls` when its
    /// digest matches containerd's `Images.List` target; `None` if the image
    /// changed concurrently between the two queries.
    platforms: Option<String>,
    /// Authoritative structured labels from containerd's `Images.List` gRPC API.
    labels: HashMap<String, String>,
}

impl CtrImage {
    fn label(&self, key: &str) -> Option<&str> {
        self.labels.get(key).map(String::as_str)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CtrPlatformRow {
    digest: String,
    platforms: String,
}

/// Remove all images in containerd's `k8s.io` namespace without touching
/// Docker's `moby` namespace. Verifies no `k8s.io` tasks or containers remain
/// to pin snapshots, deletes image refs in batches, and unconditionally waits
/// on a synchronous containerd GC barrier (deleting a short-lived reset-owned
/// lease with `--sync`) so unreferenced `k8s.io` content blobs and snapshots
/// are reclaimed before returning — even on a retry that finds no image refs.
fn clear_k8s_images(reaper: &Reaper, progress: &mut dyn FnMut(&str)) -> Result<(), String> {
    wait_containerd_with_progress(progress)?;
    let _sync_guard = {
        let mut last_beat: Option<Instant> = None;
        loop {
            if SHUTTING_DOWN.load(Ordering::SeqCst) {
                return Err("aborted: VM is shutting down".into());
            }
            match IMAGE_SYNC_LOCK.try_lock() {
                Ok(g) => break g,
                Err(std::sync::TryLockError::Poisoned(e)) => break e.into_inner(),
                Err(std::sync::TryLockError::WouldBlock) => {
                    if last_beat.is_none_or(|t| t.elapsed() >= PROGRESS_HEARTBEAT_INTERVAL) {
                        progress("waiting for background image sync to stop");
                        last_beat = Some(Instant::now());
                    }
                    thread::sleep(Duration::from_millis(50));
                }
            }
        }
    };
    if SHUTTING_DOWN.load(Ordering::SeqCst) {
        return Err("aborted: VM is shutting down".into());
    }
    // Remove any lingering k8s.io tasks/containers (for example from an unclean
    // shutdown when k3s was already in the "stopped" phase) so nothing pins snapshots.
    progress("stopping lingering k8s.io containers");
    stop_k8s_containers_with_progress(reaper, progress);
    let lease_id = next_gc_lease_id();
    clear_k8s_images_with(
        || SHUTTING_DOWN.load(Ordering::SeqCst),
        &lease_id,
        |args, timeout, on_wait| run_ctr_k8s(reaper, args, timeout, on_wait),
        progress,
    )
}

fn next_gc_lease_id() -> String {
    let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
    if let Ok(uuid) = fs::read_to_string("/proc/sys/kernel/random/uuid") {
        let u = uuid.trim();
        if !u.is_empty() {
            return format!("{IMAGE_GC_LEASE_PREFIX}-{u}");
        }
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    format!(
        "{IMAGE_GC_LEASE_PREFIX}-{}-{nanos}-{seq}",
        std::process::id()
    )
}

fn run_ctr_k8s(
    reaper: &Reaper,
    args: &[&str],
    timeout: Duration,
    on_wait: Option<&mut dyn FnMut()>,
) -> Result<ReapedOutput, String> {
    let mut cmd = Command::new("/usr/bin/ctr");
    cmd.args(["-a", CONTAINERD_SOCKET, "-n", "k8s.io"]);
    cmd.args(args);
    run_reaped_split_checked(reaper, &mut cmd, timeout, IMAGE_LS_STDOUT_LIMIT, true, on_wait)
        .map_err(|e| format!("ctr {}: {e}", args.first().copied().unwrap_or_default()))
}

fn ctr_error_detail<'a>(stdout: &'a str, stderr: &'a str) -> &'a str {
    let err = stderr.trim();
    if err.is_empty() { stdout.trim() } else { err }
}

fn parse_ctr_image_refs(stdout: &str) -> Vec<String> {
    stdout
        .lines()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

fn format_remaining_ids(ids: &[String]) -> String {
    const MAX_PREVIEW: usize = 5;
    if ids.len() <= MAX_PREVIEW {
        ids.join(", ")
    } else {
        format!(
            "{}, ... (+{} more)",
            ids[..MAX_PREVIEW].join(", "),
            ids.len() - MAX_PREVIEW
        )
    }
}

fn plan_image_rm_batches(refs: &[String], batch_size: usize) -> Vec<&[String]> {
    let batch_size = batch_size.max(1);
    refs.chunks(batch_size).collect()
}

fn clear_k8s_images_with<S, R, P>(
    shutting_down: S,
    lease_id: &str,
    mut run: R,
    mut progress: P,
) -> Result<(), String>
where
    S: Fn() -> bool,
    R: FnMut(&[&str], Duration, Option<&mut dyn FnMut()>) -> Result<ReapedOutput, String>,
    P: FnMut(&str),
{
    if shutting_down() {
        return Err("aborted: VM is shutting down".into());
    }
    progress("verifying k8s.io tasks and containers are stopped");
    let tasks_out = run(&["tasks", "ls", "-q"], IMAGE_LS_TIMEOUT, None)?;
    if tasks_out.code != 0 {
        return Err(format!(
            "ctr tasks ls (k8s.io) failed: {}",
            ctr_error_detail(&tasks_out.stdout, &tasks_out.stderr)
        ));
    }
    if tasks_out.stdout_truncated {
        return Err(
            "ctr tasks ls (k8s.io) output exceeded capture limit; refusing to purge images".into(),
        );
    }
    let remaining_tasks = parse_ctr_image_refs(&tasks_out.stdout);
    if !remaining_tasks.is_empty() {
        return Err(format!(
            "cannot clear k8s.io images: {} task(s) still remain in k8s.io ({})",
            remaining_tasks.len(),
            format_remaining_ids(&remaining_tasks)
        ));
    }

    if shutting_down() {
        return Err("aborted: VM is shutting down".into());
    }
    let containers_out = run(&["containers", "ls", "-q"], IMAGE_LS_TIMEOUT, None)?;
    if containers_out.code != 0 {
        return Err(format!(
            "ctr containers ls (k8s.io) failed: {}",
            ctr_error_detail(&containers_out.stdout, &containers_out.stderr)
        ));
    }
    if containers_out.stdout_truncated {
        return Err(
            "ctr containers ls (k8s.io) output exceeded capture limit; refusing to purge images"
                .into(),
        );
    }
    let remaining_containers = parse_ctr_image_refs(&containers_out.stdout);
    if !remaining_containers.is_empty() {
        return Err(format!(
            "cannot clear k8s.io images: {} container(s) still remain in k8s.io ({})",
            remaining_containers.len(),
            format_remaining_ids(&remaining_containers)
        ));
    }

    if shutting_down() {
        return Err("aborted: VM is shutting down".into());
    }
    progress("listing k8s.io images");
    let images_out = run(&["images", "ls", "-q"], IMAGE_LS_TIMEOUT, None)?;
    if images_out.code != 0 {
        return Err(format!(
            "ctr images ls (k8s.io) failed: {}",
            ctr_error_detail(&images_out.stdout, &images_out.stderr)
        ));
    }
    if images_out.stdout_truncated {
        return Err(
            "ctr images ls (k8s.io) output exceeded capture limit; refusing to delete from a truncated inventory"
                .into(),
        );
    }
    let refs = parse_ctr_image_refs(&images_out.stdout);
    let batches = plan_image_rm_batches(&refs, IMAGE_RM_BATCH_SIZE);
    let total_batches = batches.len();
    for (idx, batch) in batches.into_iter().enumerate() {
        if shutting_down() {
            return Err("aborted: VM is shutting down".into());
        }
        let msg = format!(
            "removing k8s.io images (batch {}/{})",
            idx + 1,
            total_batches
        );
        progress(&msg);
        let mut args: Vec<&str> = Vec::with_capacity(batch.len() + 3);
        args.extend(["images", "rm", "--"]);
        args.extend(batch.iter().map(String::as_str));
        let mut beat = || progress(&msg);
        let out = run(&args, IMAGE_RM_BATCH_TIMEOUT, Some(&mut beat))?;
        if out.code != 0 {
            return Err(format!(
                "ctr images rm (k8s.io) failed: {}",
                ctr_error_detail(&out.stdout, &out.stderr)
            ));
        }
    }

    // Always run a synchronous GC barrier — even when `refs` is empty (such as
    // a retry after an earlier purge deleted the last ref and timed out waiting
    // for GC) — using a short-lived reset-owned lease so existing leases are
    // never touched.
    if shutting_down() {
        return Err("aborted: VM is shutting down".into());
    }
    let gc_msg = "waiting for containerd garbage collection";
    progress(gc_msg);
    let create_out = run(
        &[
            "leases",
            "create",
            "--id",
            lease_id,
            "--expires",
            IMAGE_GC_LEASE_EXPIRES,
        ],
        IMAGE_LS_TIMEOUT,
        None,
    )?;
    if create_out.code != 0 {
        return Err(format!(
            "ctr leases create (k8s.io) failed: {}",
            ctr_error_detail(&create_out.stdout, &create_out.stderr)
        ));
    }
    if shutting_down() {
        return Err("aborted: VM is shutting down".into());
    }
    let mut gc_beat = || progress(gc_msg);
    let delete_out = run(
        &["leases", "delete", "--sync", "--", lease_id],
        IMAGE_RM_SYNC_TIMEOUT,
        Some(&mut gc_beat),
    )?;
    if delete_out.code != 0 {
        return Err(format!(
            "ctr leases delete --sync (k8s.io) failed: {}",
            ctr_error_detail(&delete_out.stdout, &delete_out.stderr)
        ));
    }
    Ok(())
}

/// Returns `image_ref -> image` for all images in a containerd namespace.
///
/// Image names, target digests, and labels come from containerd's structured
/// `Images.List` gRPC API (never comma-splitting `ctr images ls` text), while
/// platform lists come from `ctr images ls` matched by digest. Both calls fail
/// explicitly if output exceeds [`IMAGE_LIST_LIMIT`] so sync never infers
/// orphaned mirrors from a silently truncated inventory.
fn list_ctr_images(reaper: &Reaper, ns: &str) -> Result<HashMap<String, CtrImage>, String> {
    let grpc_images = containerd_grpc::list_images(
        Path::new(CONTAINERD_SOCKET),
        ns,
        IMAGE_LIST_LIMIT,
        Duration::from_secs(15),
    )?;
    if grpc_images.is_empty() {
        return Ok(HashMap::new());
    }
    let platforms = list_ctr_platforms(reaper, ns)?;
    Ok(merge_image_inventory(grpc_images, &platforms))
}

fn list_ctr_platforms(
    reaper: &Reaper,
    ns: &str,
) -> Result<HashMap<String, CtrPlatformRow>, String> {
    let mut cmd = Command::new("/usr/bin/ctr");
    cmd.args(["-a", CONTAINERD_SOCKET, "-n", ns, "images", "ls"]);
    let (code, stdout_bytes, stderr) =
        run_reaped_bytes(reaper, &mut cmd, Duration::from_secs(15), IMAGE_LIST_LIMIT)
            .map_err(|e| format!("ctr images ls ({ns}): {e}"))?;
    let stdout = String::from_utf8_lossy(&stdout_bytes);
    if code != 0 {
        let detail = if stderr.trim().is_empty() {
            stdout.trim()
        } else {
            stderr.trim()
        };
        return Err(format!("ctr images ls ({ns}) failed: {detail}"));
    }
    Ok(parse_ctr_platforms(&stdout))
}

/// Join authoritative gRPC image records (`name`, `target.digest`, structured
/// `labels`) with digest-matched platform rows from `ctr images ls`.
fn merge_image_inventory(
    grpc_images: Vec<containerd_grpc::ContainerdImage>,
    ctr_platforms: &HashMap<String, CtrPlatformRow>,
) -> HashMap<String, CtrImage> {
    let mut map = HashMap::with_capacity(grpc_images.len());
    for img in grpc_images {
        let platforms = ctr_platforms
            .get(&img.name)
            .filter(|row| row.digest == img.target.digest)
            .map(|row| row.platforms.clone());
        map.insert(
            img.name,
            CtrImage {
                digest: img.target.digest,
                platforms,
                labels: img.labels,
            },
        );
    }
    map
}

/// Read and parse `ctr images ls` platform rows up to `limit` bytes, failing
/// instead of returning a partial inventory when stdout exceeds `limit`.
#[cfg(test)]
fn parse_checked_ctr_platforms(
    r: impl Read,
    limit: usize,
) -> Result<HashMap<String, CtrPlatformRow>, String> {
    let bytes = drain_bytes_checked(r, limit).map_err(|e| e.to_string())?;
    Ok(parse_ctr_platforms(&String::from_utf8_lossy(&bytes)))
}

/// Parse `ctr images ls` output (`REF TYPE DIGEST SIZE PLATFORMS LABELS`),
/// extracting only `(REF -> DIGEST, PLATFORMS)` and ignoring the unescaped
/// `LABELS` column (authoritative labels come from containerd's `Images.List`).
fn parse_ctr_platforms(stdout: &str) -> HashMap<String, CtrPlatformRow> {
    let mut map = HashMap::new();
    for line in stdout.lines().skip(1) {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 3 {
            continue;
        }
        let platforms = fields.get(5).copied().unwrap_or("");
        map.insert(
            fields[0].to_string(),
            CtrPlatformRow {
                digest: fields[2].to_string(),
                platforms: platforms.to_string(),
            },
        );
    }
    map
}

/// Stream a native image from `moby` to `k8s.io` via two Reaper-managed `ctr`
/// processes connected by an OS pipe, verifying both exit statuses.
fn mirror_native_image(reaper: &Reaper, name: &str) -> Result<(), String> {
    let timeout = Duration::from_secs(120);
    let (pipe_r, pipe_w) = pipe().map_err(|e| format!("pipe: {e}"))?;
    let (exp_err_r, exp_err_w) = pipe().map_err(|e| format!("pipe: {e}"))?;
    let (imp_err_r, imp_err_w) = pipe().map_err(|e| format!("pipe: {e}"))?;

    // `--local` avoids containerd's transfer-API progress handler writing
    // status lines onto stdout (the archive stream itself) when exporting to `-`.
    let exp_args = [
        "-a",
        CONTAINERD_SOCKET,
        "-n",
        "moby",
        "images",
        "export",
        "--local",
        "-",
        name,
    ];
    let imp_args = [
        "-a",
        CONTAINERD_SOCKET,
        "-n",
        "k8s.io",
        "images",
        "import",
        "--digests",
        "-",
    ];

    let mut exp = Command::new("/usr/bin/ctr");
    exp.args(exp_args)
        .env("PATH", PATH)
        .stdin(Stdio::null())
        .stdout(Stdio::from(pipe_w))
        .stderr(Stdio::from(exp_err_w));
    let (exp_pid, exp_rx) = reaper
        .spawn(&mut exp)
        .map_err(|e| format!("spawn ctr export: {e}"))?;
    exp.stdout(Stdio::null()).stderr(Stdio::null());

    let mut imp = Command::new("/usr/bin/ctr");
    imp.args(imp_args)
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
    let wait_one =
        |pid: i32, rx: &Receiver<i32>, label: &str, min_grace: Duration| -> Result<i32, String> {
            let left = deadline
                .saturating_duration_since(Instant::now())
                .max(min_grace);
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

    let exp_res = wait_one(exp_pid, &exp_rx, "ctr export", Duration::ZERO);
    let imp_grace = if exp_res.is_err() {
        Duration::from_secs(2)
    } else {
        Duration::ZERO
    };
    let imp_res = wait_one(imp_pid, &imp_rx, "ctr import", imp_grace);
    let exp_err = exp_err_t.join().unwrap_or_default();
    let imp_err = imp_err_t.join().unwrap_or_default();

    combine_mirror_results(exp_res, &exp_err, imp_res, &imp_err)
}

fn format_child_failure(label: &str, res: Result<i32, String>, stderr: &str) -> Option<String> {
    match res {
        Ok(0) => None,
        Ok(code) => {
            if stderr.is_empty() {
                Some(format!("{label} exited {code}"))
            } else {
                Some(format!("{label} exited {code}: {stderr}"))
            }
        }
        Err(timeout) => {
            if stderr.is_empty() {
                Some(timeout)
            } else {
                Some(format!("{timeout}: {stderr}"))
            }
        }
    }
}

fn combine_mirror_results(
    exp_res: Result<i32, String>,
    exp_err: &str,
    imp_res: Result<i32, String>,
    imp_err: &str,
) -> Result<(), String> {
    let mut parts = Vec::new();
    if let Some(exp_msg) = format_child_failure("ctr export", exp_res, exp_err) {
        parts.push(exp_msg);
    }
    if let Some(imp_msg) = format_child_failure("ctr import", imp_res, imp_err) {
        parts.push(imp_msg);
    }
    if !parts.is_empty() {
        return Err(parts.join("; "));
    }
    Ok(())
}

fn log(msg: &str) {
    eprintln!("kat-init: k8s: {msg}");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("kat-init-k8s-{name}-{}-{seq}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn ctr_img(digest: &str, platforms: &str, labels: &[(&str, &str)]) -> CtrImage {
        CtrImage {
            digest: digest.into(),
            platforms: Some(platforms.into()),
            labels: labels
                .iter()
                .map(|&(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    #[test]
    fn merge_image_inventory_uses_structured_labels_and_guards_digest_drift() {
        let out = "\
REF                       TYPE                                                 DIGEST                                                                  SIZE      PLATFORMS                                LABELS
docker.io/library/app:dev application/vnd.oci.image.index.v1+json              sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 12.3 MiB  linux/amd64                              -
docker.io/library/web:1   application/vnd.oci.image.manifest.v1+json           sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 4.0 KiB   linux/amd64,linux/arm64/v8               note=hello,io.katamaran.k8s.source-digest=sha256:forged
";
        let platform_rows = parse_ctr_platforms(out);
        let grpc_images = vec![
            containerd_grpc::ContainerdImage {
                name: "docker.io/library/app:dev".into(),
                labels: HashMap::new(),
                target: containerd_grpc::Descriptor {
                    media_type: "application/vnd.oci.image.index.v1+json".into(),
                    digest: format!("sha256:{}", "a".repeat(64)),
                    size: 128,
                    ..Default::default()
                },
            },
            containerd_grpc::ContainerdImage {
                name: "docker.io/library/web:1".into(),
                labels: HashMap::from([
                    ("io.cri-containerd.image".into(), "managed".into()),
                    (
                        "note".into(),
                        format!("hello,{SOURCE_DIGEST_LABEL}=sha256:forged"),
                    ),
                ]),
                target: containerd_grpc::Descriptor {
                    media_type: "application/vnd.oci.image.manifest.v1+json".into(),
                    digest: format!("sha256:{}", "b".repeat(64)),
                    size: 256,
                    ..Default::default()
                },
            },
            containerd_grpc::ContainerdImage {
                name: "docker.io/library/retagged:1".into(),
                labels: HashMap::new(),
                target: containerd_grpc::Descriptor {
                    media_type: "application/vnd.oci.image.manifest.v1+json".into(),
                    digest: format!("sha256:{}", "c".repeat(64)),
                    size: 256,
                    ..Default::default()
                },
            },
        ];
        let images = merge_image_inventory(grpc_images, &platform_rows);
        let app = &images["docker.io/library/app:dev"];
        assert_eq!(app.digest, format!("sha256:{}", "a".repeat(64)));
        assert_eq!(app.platforms.as_deref(), Some("linux/amd64"));
        assert_eq!(app.label(SOURCE_DIGEST_LABEL), None);

        // Comma-embedded ownership marker in another label's value must NEVER
        // match SOURCE_DIGEST_LABEL or be mistaken for a Katamaran mirror.
        let web = &images["docker.io/library/web:1"];
        assert_eq!(web.platforms.as_deref(), Some("linux/amd64,linux/arm64/v8"));
        assert_eq!(web.label(SOURCE_DIGEST_LABEL), None);
        assert_eq!(web.label("io.cri-containerd.image"), Some("managed"));
        assert_eq!(
            plan_sync_action(
                "docker.io/library/web:1",
                app,
                Some(web),
                NativeAvailability::Absent,
                NativeAvailability::Present,
                EmulationMode::Disabled,
                "arm64"
            ),
            SyncAction::ReportEmulationDisabled
        );
        assert_eq!(
            plan_sync_action(
                "docker.io/library/web:1",
                app,
                Some(web),
                NativeAvailability::Absent,
                NativeAvailability::Present,
                EmulationMode::Enabled,
                "arm64"
            ),
            SyncAction::ReportDivergedTag
        );

        // Missing or mismatched ctr platform row produces `platforms: None` -> Relist.
        let retagged = &images["docker.io/library/retagged:1"];
        assert_eq!(retagged.platforms, None);
        assert_eq!(
            classify_native_availability(
                "docker.io/library/retagged:1",
                retagged,
                None,
                None,
                EmulationMode::Enabled,
                "arm64"
            ),
            NativeAvailability::Stale
        );
    }

    #[test]
    fn rewrites_complete_single_manifest_amd64_image() {
        let ls_out = "\
REF                          TYPE                                            DIGEST                                                                  SIZE     PLATFORMS   LABELS
docker.io/library/app:single application/vnd.docker.distribution.manifest.v2+json sha256:1111111111111111111111111111111111111111111111111111111111111111 4.0 MiB linux/amd64 -
";
        let name = "docker.io/library/app:single";
        let digest = format!("sha256:{}", "1".repeat(64));
        let moby = merge_image_inventory(
            vec![containerd_grpc::ContainerdImage {
                name: name.into(),
                labels: HashMap::new(),
                target: containerd_grpc::Descriptor {
                    media_type: "application/vnd.docker.distribution.manifest.v2+json".into(),
                    digest,
                    size: 128,
                    ..Default::default()
                },
            }],
            &parse_ctr_platforms(ls_out),
        );
        let img = &moby[name];
        let native =
            classify_native_availability(name, img, None, None, EmulationMode::Enabled, "arm64");
        assert_eq!(native, NativeAvailability::Absent);
        assert_eq!(
            plan_sync_action(
                name,
                img,
                None,
                native,
                NativeAvailability::Absent,
                EmulationMode::Enabled,
                "arm64"
            ),
            SyncAction::MirrorRewrite
        );
    }

    #[test]
    fn probe_failure_preserves_ambiguous_mirrors_without_blocking_others() {
        let multi = ctr_img("sha256:multi", "linux/amd64,linux/arm64/v8", &[]);
        let amd64_only = ctr_img("sha256:amd", "linux/amd64", &[]);
        let arm64_only = ctr_img("sha256:arm", "linux/arm64/v8", &[]);
        let k8s_rewritten = ctr_img(
            "sha256:rewritten",
            "linux/arm64",
            &[(SOURCE_DIGEST_LABEL, "sha256:multi")],
        );
        let failed_probe: Result<NativeAvailability, String> =
            Err("containerd Content.Info timed out".into());

        // Ambiguous multi-arch image: probe failure leaves existing mirror untouched and reports error.
        let n_multi = classify_native_availability(
            "app:multi",
            &multi,
            Some(&k8s_rewritten),
            Some(&failed_probe),
            EmulationMode::Enabled,
            "arm64",
        );
        assert_eq!(n_multi, NativeAvailability::ProbeFailed);
        assert_eq!(
            plan_sync_action(
                "app:multi",
                &multi,
                Some(&k8s_rewritten),
                n_multi,
                NativeAvailability::Absent,
                EmulationMode::Enabled,
                "arm64"
            ),
            SyncAction::ReportProbeError
        );

        // Non-ambiguous amd64-only and arm64-only images still proceed without the probe.
        let n_amd = classify_native_availability(
            "app:amd",
            &amd64_only,
            None,
            Some(&failed_probe),
            EmulationMode::Enabled,
            "arm64",
        );
        assert_eq!(
            plan_sync_action(
                "app:amd",
                &amd64_only,
                None,
                n_amd,
                NativeAvailability::Absent,
                EmulationMode::Enabled,
                "arm64"
            ),
            SyncAction::MirrorRewrite
        );
        let n_arm = classify_native_availability(
            "app:arm",
            &arm64_only,
            None,
            Some(&failed_probe),
            EmulationMode::Enabled,
            "arm64",
        );
        assert_eq!(
            plan_sync_action(
                "app:arm",
                &arm64_only,
                None,
                n_arm,
                NativeAvailability::Absent,
                EmulationMode::Enabled,
                "arm64"
            ),
            SyncAction::MirrorNative {
                clear_source_label: false
            }
        );
    }

    #[test]
    fn check_digest_mismatch_requests_relist() {
        let multi = ctr_img("sha256:old", "linux/amd64,linux/arm64/v8", &[]);
        let stale_probe: Result<NativeAvailability, String> = Ok(NativeAvailability::Stale);
        let native = classify_native_availability(
            "app:dev",
            &multi,
            None,
            Some(&stale_probe),
            EmulationMode::Enabled,
            "arm64",
        );
        assert_eq!(native, NativeAvailability::Stale);
        assert_eq!(
            plan_sync_action(
                "app:dev",
                &multi,
                None,
                native,
                NativeAvailability::Absent,
                EmulationMode::Enabled,
                "arm64"
            ),
            SyncAction::Relist
        );
    }

    /// Regression test: an unlabelled `k8s.io` tag is never replaced by an
    /// amd64 rewrite. Its native completeness only decides how it is reported:
    /// incomplete arm64 (conflict reported to the user), verified arm64
    /// (silently preserved), probe error (preserved, error reported), and
    /// stale digest (relist).
    #[test]
    fn unlabelled_destination_index_requires_verified_local_arm64_content() {
        let moby_amd64 = ctr_img("sha256:moby-amd64", "linux/amd64", &[]);
        let k8s_multi_index = ctr_img("sha256:k8s-multi", "linux/amd64,linux/arm64/v8", &[]);

        assert!(needs_dest_native_probe(
            "app:dev",
            &moby_amd64,
            Some(&k8s_multi_index),
            NativeAvailability::Absent,
            EmulationMode::Enabled,
            "arm64"
        ));

        // 1. Only amd64 blobs cached under k8s.io's multi-arch index -> Absent ->
        //    conflict reported, tag never replaced.
        let incomplete_probe: Result<NativeAvailability, String> = Ok(NativeAvailability::Absent);
        let dest_incomplete = classify_dest_native_availability(
            Some(&k8s_multi_index),
            Some(&incomplete_probe),
            "arm64",
        );
        assert_eq!(dest_incomplete, NativeAvailability::Absent);
        assert_eq!(
            plan_sync_action(
                "app:dev",
                &moby_amd64,
                Some(&k8s_multi_index),
                NativeAvailability::Absent,
                dest_incomplete,
                EmulationMode::Enabled,
                "arm64"
            ),
            SyncAction::ReportUnownedDestination
        );

        // 2. Verified complete local arm64 content in k8s.io -> Present: preserved, but reported because it differs from Docker.
        let complete_probe: Result<NativeAvailability, String> = Ok(NativeAvailability::Present);
        let dest_complete = classify_dest_native_availability(
            Some(&k8s_multi_index),
            Some(&complete_probe),
            "arm64",
        );
        assert_eq!(dest_complete, NativeAvailability::Present);
        assert_eq!(
            plan_sync_action(
                "app:dev",
                &moby_amd64,
                Some(&k8s_multi_index),
                NativeAvailability::Absent,
                dest_complete,
                EmulationMode::Enabled,
                "arm64"
            ),
            SyncAction::ReportDivergedTag
        );

        // 3. Probe error on k8s.io -> ProbeFailed -> ReportProbeError (preserve!).
        let err_probe: Result<NativeAvailability, String> = Err("k8s.io probe failed".into());
        let dest_err =
            classify_dest_native_availability(Some(&k8s_multi_index), Some(&err_probe), "arm64");
        assert_eq!(dest_err, NativeAvailability::ProbeFailed);
        assert_eq!(
            plan_sync_action(
                "app:dev",
                &moby_amd64,
                Some(&k8s_multi_index),
                NativeAvailability::Absent,
                dest_err,
                EmulationMode::Enabled,
                "arm64"
            ),
            SyncAction::ReportProbeError
        );

        // 4. Stale / concurrently retagged k8s.io digest -> Stale -> Relist!
        let stale_probe: Result<NativeAvailability, String> = Ok(NativeAvailability::Stale);
        let dest_stale =
            classify_dest_native_availability(Some(&k8s_multi_index), Some(&stale_probe), "arm64");
        assert_eq!(dest_stale, NativeAvailability::Stale);
        assert_eq!(
            plan_sync_action(
                "app:dev",
                &moby_amd64,
                Some(&k8s_multi_index),
                NativeAvailability::Absent,
                dest_stale,
                EmulationMode::Enabled,
                "arm64"
            ),
            SyncAction::Relist
        );
    }

    #[test]
    fn plan_sync_action_handles_emulation_off_unknown_and_digest_refs() {
        let amd64_idx = ctr_img("sha256:src", "linux/amd64", &[]);
        let k8s_rewritten = ctr_img(
            "sha256:new",
            "linux/arm64",
            &[
                ("io.cri-containerd.image", "managed"),
                (SOURCE_DIGEST_LABEL, "sha256:src"),
            ],
        );
        let k8s_stale_rewritten = ctr_img(
            "sha256:old",
            "linux/arm64",
            &[(SOURCE_DIGEST_LABEL, "sha256:older")],
        );
        let k8s_unrelated = ctr_img(
            "sha256:other",
            "linux/arm64",
            &[
                ("io.cri-containerd.image", "managed"),
                ("note", "x,io.katamaran.k8s.source-digest=sha256:src"),
            ],
        );

        // Emulation on: missing / stale -> rewrite; matching rewritten -> skip.
        assert_eq!(
            plan_sync_action(
                "app:dev",
                &amd64_idx,
                None,
                NativeAvailability::Absent,
                NativeAvailability::Absent,
                EmulationMode::Enabled,
                "arm64"
            ),
            SyncAction::MirrorRewrite
        );
        assert_eq!(
            plan_sync_action(
                "app:dev",
                &amd64_idx,
                Some(&k8s_rewritten),
                NativeAvailability::Absent,
                NativeAvailability::Absent,
                EmulationMode::Enabled,
                "arm64"
            ),
            SyncAction::Skip
        );
        assert_eq!(
            plan_sync_action(
                "app:dev",
                &amd64_idx,
                Some(&k8s_stale_rewritten),
                NativeAvailability::Absent,
                NativeAvailability::Absent,
                EmulationMode::Enabled,
                "arm64"
            ),
            SyncAction::MirrorRewrite
        );

        // Emulation on: preserve an existing unlabelled native Kubernetes image
        // when its local arm64 content is verified complete, re-list if drifted,
        // and report (never replace) an unlabelled amd64-only tag in k8s.io.
        assert_eq!(
            plan_sync_action(
                "app:dev",
                &amd64_idx,
                Some(&k8s_unrelated),
                NativeAvailability::Absent,
                NativeAvailability::Present,
                EmulationMode::Enabled,
                "arm64"
            ),
            SyncAction::ReportDivergedTag
        );
        let k8s_unlabelled_drifted = CtrImage {
            digest: "sha256:drifted".into(),
            platforms: None,
            labels: HashMap::new(),
        };
        assert_eq!(
            classify_dest_native_availability(Some(&k8s_unlabelled_drifted), None, "arm64"),
            NativeAvailability::Stale
        );
        assert_eq!(
            plan_sync_action(
                "app:dev",
                &amd64_idx,
                Some(&k8s_unlabelled_drifted),
                NativeAvailability::Absent,
                NativeAvailability::Stale,
                EmulationMode::Enabled,
                "arm64"
            ),
            SyncAction::Relist
        );
        let k8s_unlabelled_amd64 = ctr_img("sha256:legacy-amd64", "linux/amd64", &[]);
        assert_eq!(
            classify_dest_native_availability(Some(&k8s_unlabelled_amd64), None, "arm64"),
            NativeAvailability::Absent
        );
        assert_eq!(
            plan_sync_action(
                "app:dev",
                &amd64_idx,
                Some(&k8s_unlabelled_amd64),
                NativeAvailability::Absent,
                NativeAvailability::Absent,
                EmulationMode::Enabled,
                "arm64"
            ),
            SyncAction::ReportUnownedDestination
        );
        assert!(
            unowned_destination_message("app:dev").contains("docker tag app:dev"),
            "conflict message must tell the user how to resolve it"
        );
        assert!(is_published_mirror_manifest(
            br#"{"annotations":{"io.katamaran.k8s.publication":"lease-1"}}"#
        ));
        assert!(!is_published_mirror_manifest(br#"{"schemaVersion":2}"#));
        assert!(!is_published_mirror_manifest(b"not json"));
        // A legacy unlabelled copy (e.g. an amd64 image mirrored unchanged by
        // 0.10.0, #41) is never adopted or deleted: it is reported with the
        // retag workaround instead.
        let k8s_unchanged_copy = ctr_img(&amd64_idx.digest, "linux/amd64", &[]);
        assert_eq!(
            plan_sync_action(
                "app:dev",
                &amd64_idx,
                Some(&k8s_unchanged_copy),
                NativeAvailability::Absent,
                NativeAvailability::Absent,
                EmulationMode::Enabled,
                "arm64"
            ),
            SyncAction::ReportUnownedDestination
        );
        let k8s_unchanged_multi = ctr_img(&amd64_idx.digest, "linux/amd64,linux/arm64/v8", &[]);
        // When emulation is off or unknown and Kubernetes has a different
        // unlabelled image under the same tag, report the emulation cause
        // directly (not ReportDivergedTag with a retag workaround that still
        // cannot mirror).
        let k8s_native_old = ctr_img("sha256:old-native", "linux/arm64", &[]);
        assert_eq!(
            plan_sync_action(
                "app:dev",
                &amd64_idx,
                Some(&k8s_native_old),
                NativeAvailability::Absent,
                NativeAvailability::Present,
                EmulationMode::Disabled,
                "arm64"
            ),
            SyncAction::ReportEmulationDisabled
        );
        assert_eq!(
            plan_sync_action(
                "app:dev",
                &amd64_idx,
                Some(&k8s_native_old),
                NativeAvailability::Absent,
                NativeAvailability::Present,
                EmulationMode::Unknown,
                "arm64"
            ),
            SyncAction::ReportEmulationUnavailable
        );
        assert_eq!(
            plan_sync_action(
                "app:dev",
                &amd64_idx,
                Some(&k8s_unchanged_multi),
                NativeAvailability::Absent,
                NativeAvailability::Absent,
                EmulationMode::Enabled,
                "arm64"
            ),
            SyncAction::ReportUnownedDestination
        );

        // Emulation disabled for an amd64-only image: keep (never delete)
        // an up-to-date Katamaran mirror, and report unmirrored amd64 tags
        // (whether k8s.io has no copy, a stale mirror, or a different image).
        assert_eq!(
            plan_sync_action(
                "app:dev",
                &amd64_idx,
                Some(&k8s_rewritten),
                NativeAvailability::Absent,
                NativeAvailability::Absent,
                EmulationMode::Disabled,
                "arm64"
            ),
            SyncAction::KeepMirrorEmulationOff
        );
        assert_eq!(
            plan_sync_action(
                "app:dev",
                &amd64_idx,
                Some(&k8s_stale_rewritten),
                NativeAvailability::Absent,
                NativeAvailability::Absent,
                EmulationMode::Disabled,
                "arm64"
            ),
            SyncAction::ReportEmulationDisabled
        );
        assert_eq!(
            plan_sync_action(
                "app:dev",
                &amd64_idx,
                Some(&k8s_unrelated),
                NativeAvailability::Absent,
                NativeAvailability::Present,
                EmulationMode::Disabled,
                "arm64"
            ),
            SyncAction::ReportEmulationDisabled
        );
        assert_eq!(
            plan_sync_action(
                "app:dev",
                &amd64_idx,
                None,
                NativeAvailability::Absent,
                NativeAvailability::Absent,
                EmulationMode::Disabled,
                "arm64"
            ),
            SyncAction::ReportEmulationDisabled
        );
        assert_eq!(
            plan_sync_action(
                "app:dev",
                &amd64_idx,
                Some(&k8s_unchanged_copy),
                NativeAvailability::Absent,
                NativeAvailability::Absent,
                EmulationMode::Disabled,
                "arm64"
            ),
            SyncAction::ReportEmulationDisabled
        );

        // Emulation status Unknown (e.g. transient binfmt_misc read error): NEVER
        // retire existing working Katamaran mirrors and NEVER publish new amd64
        // rewrites (report them instead), while still allowing native arm64
        // images to sync.
        assert_eq!(
            plan_sync_action(
                "app:dev",
                &amd64_idx,
                Some(&k8s_rewritten),
                NativeAvailability::Absent,
                NativeAvailability::Absent,
                EmulationMode::Unknown,
                "arm64"
            ),
            SyncAction::Skip
        );
        assert_eq!(
            plan_sync_action(
                "app:dev",
                &amd64_idx,
                None,
                NativeAvailability::Absent,
                NativeAvailability::Absent,
                EmulationMode::Unknown,
                "arm64"
            ),
            SyncAction::ReportEmulationUnavailable
        );
        assert_eq!(
            plan_sync_action(
                "app:dev",
                &amd64_idx,
                None,
                NativeAvailability::Present,
                NativeAvailability::Absent,
                EmulationMode::Unknown,
                "arm64"
            ),
            SyncAction::MirrorNative {
                clear_source_label: false
            }
        );

        // Native content present over a previously rewritten copy: replace natively
        // and clear the stale source-digest label.
        assert_eq!(
            plan_sync_action(
                "app:dev",
                &amd64_idx,
                Some(&k8s_rewritten),
                NativeAvailability::Present,
                NativeAvailability::Absent,
                EmulationMode::Disabled,
                "arm64"
            ),
            SyncAction::MirrorNative {
                clear_source_label: true
            }
        );

        // Digest-pinned amd64-only reference: never rewrite; keep an existing
        // rewritten copy while emulation is on (retire only when explicitly
        // off); mirror natively when native content is present.
        let digest_ref = "docker.io/library/app@sha256:1111111111111111111111111111111111111111111111111111111111111111";
        assert_eq!(
            plan_sync_action(
                digest_ref,
                &amd64_idx,
                None,
                NativeAvailability::Absent,
                NativeAvailability::Absent,
                EmulationMode::Enabled,
                "arm64"
            ),
            SyncAction::SkipDigestAmd64
        );
        assert_eq!(
            plan_sync_action(
                digest_ref,
                &amd64_idx,
                Some(&k8s_rewritten),
                NativeAvailability::Absent,
                NativeAvailability::Absent,
                EmulationMode::Enabled,
                "arm64"
            ),
            SyncAction::SkipDigestAmd64
        );
        assert_eq!(
            plan_sync_action(
                digest_ref,
                &amd64_idx,
                Some(&k8s_rewritten),
                NativeAvailability::Absent,
                NativeAvailability::Absent,
                EmulationMode::Disabled,
                "arm64"
            ),
            SyncAction::KeepMirrorEmulationOff
        );
        // amd64 = off: a digest ref is not reported as an emulation skip.
        assert_eq!(
            plan_sync_action(
                digest_ref,
                &amd64_idx,
                None,
                NativeAvailability::Absent,
                NativeAvailability::Absent,
                EmulationMode::Disabled,
                "arm64"
            ),
            SyncAction::Skip
        );
        // Emulation on, but the source's platforms are unresolved (`-`) or
        // have no amd64 variant: never delete the existing working mirror.
        // (A resolved non-amd64 source is reported as leaving the previous
        // build in place; an unresolved list just keeps it.)
        for (platforms, want) in [
            ("-", SyncAction::Skip),
            ("linux/s390x", SyncAction::ReportStaleMirror),
        ] {
            let unresolved = ctr_img("sha256:src2", platforms, &[]);
            assert_eq!(
                plan_sync_action(
                    "app:dev",
                    &unresolved,
                    Some(&k8s_rewritten),
                    NativeAvailability::Absent,
                    NativeAvailability::Absent,
                    EmulationMode::Enabled,
                    "arm64"
                ),
                want,
                "platforms {platforms}"
            );
        }
        // Emulation unknown (e.g. registration failed): an amd64 tag with no
        // up-to-date mirror is reported, never silently skipped; an existing
        // mirror is kept.
        assert_eq!(
            plan_sync_action(
                "app:dev",
                &amd64_idx,
                None,
                NativeAvailability::Absent,
                NativeAvailability::Absent,
                EmulationMode::Unknown,
                "arm64"
            ),
            SyncAction::ReportEmulationUnavailable
        );
        assert_eq!(
            plan_sync_action(
                digest_ref,
                &amd64_idx,
                None,
                NativeAvailability::Present,
                NativeAvailability::Absent,
                EmulationMode::Enabled,
                "arm64"
            ),
            SyncAction::MirrorNative {
                clear_source_label: false
            }
        );

        // Mixed 32-bit ARM + amd64 tag with NO arm64 variant: Katamaran's arm64
        // guest kernel disables CONFIG_COMPAT, so sync must rewrite amd64.
        let arm32_and_amd64 = ctr_img("sha256:mixed", "linux/arm/v7,linux/amd64", &[]);
        let native_mixed = classify_native_availability(
            "app:mixed",
            &arm32_and_amd64,
            None,
            None,
            EmulationMode::Enabled,
            "arm64",
        );
        assert_eq!(native_mixed, NativeAvailability::Absent);
        assert_eq!(
            plan_sync_action(
                "app:mixed",
                &arm32_and_amd64,
                None,
                native_mixed,
                NativeAvailability::Absent,
                EmulationMode::Enabled,
                "arm64"
            ),
            SyncAction::MirrorRewrite
        );

        // Non-baseline `linux/arm64/v8.2` + `linux/amd64` on baseline arm64 guest:
        // `ctr images export --local` (`DefaultStrict()`) rejects `v8.2`, so sync
        // must rewrite `linux/amd64`.
        let v82_and_amd64 = ctr_img("sha256:v82", "linux/arm64/v8.2,linux/amd64", &[]);
        let native_v82 = classify_native_availability(
            "app:v82",
            &v82_and_amd64,
            None,
            None,
            EmulationMode::Enabled,
            "arm64",
        );
        assert_eq!(native_v82, NativeAvailability::Absent);
        assert_eq!(
            plan_sync_action(
                "app:v82",
                &v82_and_amd64,
                None,
                native_v82,
                NativeAvailability::Absent,
                EmulationMode::Enabled,
                "arm64"
            ),
            SyncAction::MirrorRewrite
        );

        // Baseline alias `linux/arm64/v8.0` + `linux/amd64`: containerd's
        // `DefaultStrict()` normalizes `v8.0` to baseline `arm64`, so a
        // complete native probe must select `MirrorNative` instead of rewriting.
        let v80_and_amd64 = ctr_img("sha256:v80", "linux/arm64/v8.0,linux/amd64", &[]);
        assert!(needs_native_probe(
            "app:v80",
            &v80_and_amd64,
            None,
            EmulationMode::Enabled,
            "arm64"
        ));
        let ok_present: Result<NativeAvailability, String> = Ok(NativeAvailability::Present);
        let native_v80 = classify_native_availability(
            "app:v80",
            &v80_and_amd64,
            None,
            Some(&ok_present),
            EmulationMode::Enabled,
            "arm64",
        );
        assert_eq!(native_v80, NativeAvailability::Present);
        assert_eq!(
            plan_sync_action(
                "app:v80",
                &v80_and_amd64,
                None,
                native_v80,
                NativeAvailability::Absent,
                EmulationMode::Enabled,
                "arm64"
            ),
            SyncAction::MirrorNative {
                clear_source_label: false
            }
        );

        // Non-arm64/non-amd64 source over a different unlabelled k8s tag
        // reports ReportStaleMirror rather than ReportDivergedTag.
        let s390x = ctr_img("sha256:s390x", "linux/s390x", &[]);
        assert_eq!(
            plan_sync_action(
                "app:dev",
                &s390x,
                Some(&k8s_native_old),
                NativeAvailability::Absent,
                NativeAvailability::Present,
                EmulationMode::Enabled,
                "arm64"
            ),
            SyncAction::ReportStaleMirror
        );
        assert_eq!(
            plan_sync_action(
                "app:dev",
                &s390x,
                None,
                NativeAvailability::Absent,
                NativeAvailability::Absent,
                EmulationMode::Enabled,
                "arm64"
            ),
            SyncAction::Skip
        );
    }

    #[test]
    fn needs_native_probe_skips_same_unlabelled_digest_when_action_cannot_change() {
        let multi = ctr_img("sha256:same", "linux/amd64,linux/arm64/v8", &[]);
        let k8s_same = ctr_img("sha256:same", "linux/amd64,linux/arm64/v8", &[]);
        let k8s_diff = ctr_img("sha256:other", "linux/amd64,linux/arm64/v8", &[]);
        let k8s_rewritten = ctr_img(
            "sha256:rewritten",
            "linux/arm64",
            &[(SOURCE_DIGEST_LABEL, "sha256:same")],
        );
        let digest_ref = "docker.io/library/app@sha256:1111111111111111111111111111111111111111111111111111111111111111";
        let failed_probe: Result<NativeAvailability, String> = Err("transient rpc timeout".into());

        // Emulation Disabled or Unknown with same unlabelled digest: no action
        // can differ, so skip probing and ignore transient probe failures.
        for mode in [EmulationMode::Disabled, EmulationMode::Unknown] {
            assert!(!needs_native_probe(
                "app:dev",
                &multi,
                Some(&k8s_same),
                mode,
                "arm64"
            ));
            let native = classify_native_availability(
                "app:dev",
                &multi,
                Some(&k8s_same),
                Some(&failed_probe),
                mode,
                "arm64",
            );
            assert_eq!(native, NativeAvailability::Present);
            assert_eq!(
                plan_sync_action(
                    "app:dev",
                    &multi,
                    Some(&k8s_same),
                    native,
                    NativeAvailability::Absent,
                    mode,
                    "arm64"
                ),
                SyncAction::Skip
            );
        }

        // Digest-pinned reference with same unlabelled digest: never rewritten,
        // so skip probing even when emulation is enabled.
        assert!(!needs_native_probe(
            digest_ref,
            &multi,
            Some(&k8s_same),
            EmulationMode::Enabled,
            "arm64"
        ));

        // Tagged image with emulation Enabled and same unlabelled digest: MUST
        // still probe so an unusable amd64-only legacy copy is reported.
        assert!(needs_native_probe(
            "app:dev",
            &multi,
            Some(&k8s_same),
            EmulationMode::Enabled,
            "arm64"
        ));

        // Missing, diverged, or Katamaran-rewritten destination: probe still
        // matters even when emulation is off (decides MirrorNative vs report).
        for dst in [None, Some(&k8s_diff), Some(&k8s_rewritten)] {
            assert!(needs_native_probe(
                "app:dev",
                &multi,
                dst,
                EmulationMode::Disabled,
                "arm64"
            ));
        }
    }

    #[test]
    fn root_missing_relists_only_when_tag_changed_and_errors_when_dangling() {
        // Native probe: RootMissing with unchanged digest -> Err (no relist).
        let err = classify_native_check_outcome(
            "moby",
            "sha256:dangling",
            oci_rewrite::NativeManifestCheck::RootMissing,
            || Ok(Some("sha256:dangling".into())),
        )
        .unwrap_err();
        assert!(err.contains("root manifest blob sha256:dangling"), "{err}");
        assert!(err.contains("moby"), "{err}");

        // Native probe: RootMissing after concurrent retag or delete -> Stale (relist).
        assert_eq!(
            classify_native_check_outcome(
                "moby",
                "sha256:old",
                oci_rewrite::NativeManifestCheck::RootMissing,
                || Ok(Some("sha256:new".into())),
            )
            .unwrap(),
            NativeAvailability::Stale
        );
        assert_eq!(
            classify_native_check_outcome(
                "moby",
                "sha256:old",
                oci_rewrite::NativeManifestCheck::RootMissing,
                || Ok(None),
            )
            .unwrap(),
            NativeAvailability::Stale
        );

        // Prepare rewrite: RootMissing with unchanged digest -> Err (no relist).
        let err = classify_prepare_root_missing(
            "app:dev",
            "sha256:dangling",
            Ok(Some("sha256:dangling".into())),
        )
        .unwrap_err();
        assert!(err.contains("root manifest blob sha256:dangling"), "{err}");

        // Prepare rewrite: RootMissing after retag or deletion -> Ok (relist).
        classify_prepare_root_missing("app:dev", "sha256:old", Ok(Some("sha256:new".into())))
            .unwrap();
        classify_prepare_root_missing("app:dev", "sha256:old", Ok(None)).unwrap();

        // Prepare rewrite: re-check RPC failure -> Err.
        let err =
            classify_prepare_root_missing("app:dev", "sha256:old", Err("grpc timeout".into()))
                .unwrap_err();
        assert!(err.contains("grpc timeout"), "{err}");
    }

    #[test]
    fn sync_log_state_deduplicates_per_image_and_digest_and_prunes_removed() {
        let mut state = SyncLogState::new();
        let n1 = SyncNotice {
            kind: SyncNoticeKind::SkipDigestAmd64,
            name: "app@sha256:1111".into(),
            digest: "sha256:1111".into(),
        };
        let n2 = SyncNotice {
            kind: SyncNoticeKind::KeepMirrorEmulationOff,
            name: "app:dev".into(),
            digest: "sha256:src:sha256:k8s".into(),
        };
        let n2_updated = SyncNotice {
            kind: SyncNoticeKind::KeepMirrorEmulationOff,
            name: "app:dev".into(),
            digest: "sha256:src2:sha256:k8s".into(),
        };

        assert!(state.should_log(n1.clone()));
        assert!(!state.should_log(n1.clone()));
        assert!(state.should_log(n2.clone()));
        assert!(!state.should_log(n2.clone()));
        // A different digest logs once again.
        assert!(state.should_log(n2_updated.clone()));
        assert!(!state.should_log(n2_updated.clone()));

        // Retaining only n2_updated prunes n1 and n2 so re-adding n1 logs again.
        let active = BTreeSet::from([n2_updated.clone()]);
        state.retain_active(&active);
        assert!(!state.should_log(n2_updated));
        assert!(state.should_log(n1));
    }

    #[test]
    fn docker_event_watcher_syncs_once_on_subscribe_checks_status_and_backs_off_on_eof() {
        use std::os::unix::net::UnixListener;

        let dir = temp_dir("events-watch");
        let sock = dir.join("docker.sock");
        let listener = UnixListener::bind(&sock).unwrap();

        let server = thread::spawn(move || {
            // Connection 1: 200 OK -> 1 initial sync + 1 tag event -> clean EOF.
            let (mut s1, _) = listener.accept().unwrap();
            let mut req = [0u8; 256];
            let _ = s1.read(&mut req);
            s1.write_all(
                b"HTTP/1.0 200 OK\r\nContent-Type: application/json\r\n\r\n\
                  {\"Action\":\"delete\"}\n\
                  not-json\n\
                  {\"Action\":\"tag\"}\n",
            )
            .unwrap();
            drop(s1);

            // Connection 2: non-200 status -> must error without syncing.
            let (mut s2, _) = listener.accept().unwrap();
            let _ = s2.read(&mut req);
            s2.write_all(b"HTTP/1.0 500 Internal Server Error\r\n\r\nboom\n")
                .unwrap();
            drop(s2);

            // Connection 3: immediate EOF before headers -> Ok without syncing.
            let (mut s3, _) = listener.accept().unwrap();
            let _ = s3.read(&mut req);
            drop(s3);
        });

        let stop = AtomicBool::new(false);
        let mut sync_calls = 0usize;
        watch_docker_image_events_with(&sock, &stop, || sync_calls += 1).unwrap();
        // Exactly 2 syncs: 1 right after subscribing + 1 for the "tag" event.
        assert_eq!(sync_calls, 2);

        let mut err_syncs = 0usize;
        let err = watch_docker_image_events_with(&sock, &stop, || err_syncs += 1).unwrap_err();
        assert!(err.to_string().contains("500"), "{err}");
        assert_eq!(err_syncs, 0);

        let mut eof_syncs = 0usize;
        watch_docker_image_events_with(&sock, &stop, || eof_syncs += 1).unwrap();
        assert_eq!(eof_syncs, 0);

        server.join().unwrap();
        let _ = fs::remove_dir_all(&dir);

        // Clean EOF (`Ok(())`) must wait for the backoff before reconnecting
        // instead of tight-spinning.
        let loop_stop = AtomicBool::new(false);
        let mut attempts = 0usize;
        let started = Instant::now();
        run_image_sync_loop(&loop_stop, Duration::from_millis(60), || {
            attempts += 1;
            if attempts == 2 {
                loop_stop.store(true, Ordering::Relaxed);
            }
            Ok(())
        });
        assert_eq!(attempts, 2);
        assert!(
            started.elapsed() >= Duration::from_millis(50),
            "expected backoff after clean EOF, elapsed {:?}",
            started.elapsed()
        );
    }

    /// Regression test: `MobyContentStore` queries authoritative sizes lazily
    /// (single-digest `Content.Info` + bounded digest-filtered `Content.List`
    /// batches), never enumerates unrelated `moby` blobs, caches lookups across
    /// batch and single checks, handles 511+ absent siblings before the only
    /// complete amd64 manifest, distinguishes `NOT_FOUND` from RPC failures,
    /// and rejects oversized blobs before invoking the payload fetcher.
    #[test]
    fn moby_content_store_uses_lazy_cached_info_and_rejects_oversized_before_fetch() {
        use sha2::{Digest, Sha256};

        struct MockMeta<'a> {
            sizes: &'a HashMap<String, u64>,
            queried: HashMap<String, usize>,
            batch_sizes: Vec<usize>,
            fail_single: bool,
            fail_batch: bool,
        }

        impl MobyContentMeta for MockMeta<'_> {
            fn blob_size(&mut self, digest: &str) -> Result<Option<u64>, String> {
                if self.fail_single {
                    return Err("containerd Content.Info failed (grpc-status 13)".into());
                }
                assert!(
                    !digest.starts_with("sha256:unrelated"),
                    "unrelated moby blob must never be queried: {digest}"
                );
                *self.queried.entry(digest.to_string()).or_default() += 1;
                Ok(self.sizes.get(digest).copied())
            }

            fn batch_blob_sizes(
                &mut self,
                digests: &[&str],
            ) -> Result<HashMap<String, u64>, String> {
                if self.fail_batch {
                    return Err("containerd Content.List failed (grpc-status 13)".into());
                }
                self.batch_sizes.push(digests.len());
                let mut out = HashMap::new();
                for &d in digests {
                    assert!(
                        !d.starts_with("sha256:unrelated"),
                        "unrelated moby blob must never be queried: {d}"
                    );
                    *self.queried.entry(d.to_string()).or_default() += 1;
                    if let Some(&sz) = self.sizes.get(d) {
                        out.insert(d.to_string(), sz);
                    }
                }
                Ok(out)
            }
        }

        let sha = |b: &[u8]| -> String {
            let hex: String = Sha256::digest(b)
                .iter()
                .map(|x| format!("{x:02x}"))
                .collect();
            format!("sha256:{hex}")
        };

        let layer_d = format!("sha256:{}", "2".repeat(64));
        let huge_a = format!("sha256:{}", "a".repeat(64));
        let huge_b = format!("sha256:{}", "b".repeat(64));

        let cfg = serde_json::to_vec(&serde_json::json!({"architecture": "amd64", "os": "linux"}))
            .unwrap();
        let cfg_d = sha(&cfg);
        let manifest = serde_json::to_vec(&serde_json::json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {"mediaType": "application/vnd.oci.image.config.v1+json",
                       "digest": cfg_d, "size": cfg.len()},
            "layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar",
                        "digest": layer_d, "size": 512}],
        }))
        .unwrap();
        let manifest_d = sha(&manifest);

        let mut manifests = vec![
            serde_json::json!({"mediaType": "application/vnd.oci.image.index.v1+json", "digest": huge_a, "size": 10}),
            serde_json::json!({"mediaType": "application/vnd.oci.image.index.v1+json", "digest": huge_b, "size": 10}),
        ];
        // Prepend 511 distinct absent amd64 siblings before the only locally complete manifest.
        for i in 0..511u64 {
            manifests.push(serde_json::json!({
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": format!("sha256:aaaa{i:060x}"),
                "size": 128,
                "platform": {"os": "linux", "architecture": "amd64"},
            }));
        }
        manifests.push(serde_json::json!({
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": manifest_d,
            "size": manifest.len(),
            "platform": {"os": "linux", "architecture": "amd64"},
        }));

        let root = serde_json::to_vec(&serde_json::json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": manifests,
        }))
        .unwrap();
        let root_d = sha(&root);

        let mut sizes = HashMap::from([
            (root_d.clone(), root.len() as u64),
            (
                huge_a.clone(),
                (oci_rewrite::MAX_TOTAL_METADATA_BYTES as u64) + 1,
            ),
            (
                huge_b.clone(),
                (oci_rewrite::MAX_TOTAL_METADATA_BYTES as u64) + 8192,
            ),
            (manifest_d.clone(), manifest.len() as u64),
            (cfg_d.clone(), cfg.len() as u64),
            (layer_d.clone(), 512),
        ]);
        // Simulate thousands of unrelated blobs in `moby`: filtered queries
        // must never touch them.
        for i in 0..5_000u64 {
            sizes.insert(format!("sha256:unrelated{i:055}"), 16 << 10);
        }
        let payloads = HashMap::from([
            (root_d.clone(), root),
            (manifest_d.clone(), manifest),
            (cfg_d, cfg),
        ]);

        let mut fetched = Vec::new();
        let mut store = MobyContentStore::new(
            MockMeta {
                sizes: &sizes,
                queried: HashMap::new(),
                batch_sizes: Vec::new(),
                fail_single: false,
                fail_batch: false,
            },
            |digest: &str, _max: usize| {
                assert_ne!(digest, huge_a, "oversized sibling A must not be fetched");
                assert_ne!(digest, huge_b, "oversized sibling B must not be fetched");
                fetched.push(digest.to_string());
                payloads
                    .get(digest)
                    .cloned()
                    .ok_or_else(|| oci_rewrite::BlobReadError::Other(format!("missing {digest}")))
            },
            |digest: &str| {
                payloads
                    .get(digest)
                    .map(|b| oci_rewrite::parse_config_platform_reader(b.as_slice()))
                    .transpose()
                    .map(Option::flatten)
            },
        );

        let oci_rewrite::PrepareRewrite::Ready(plan) =
            oci_rewrite::prepare_rewrite_from_store(&root_d, "arm64", &mut store).unwrap()
        else {
            panic!(
                "expected Ready plan for complete sibling after 511 absent + oversized siblings"
            );
        };
        assert_eq!(plan.layer_digests, vec![layer_d.clone()]);
        assert!(
            store.meta.queried.values().all(|&n| n == 1),
            "each inspected digest must be queried at most once"
        );
        assert!(
            store
                .meta
                .batch_sizes
                .iter()
                .all(|&n| n > 0 && n <= containerd_grpc::CONTENT_INFO_BATCH_SIZE),
            "batch sizes must stay bounded: {:?}",
            store.meta.batch_sizes
        );
        drop(store);
        assert!(!fetched.contains(&huge_a) && !fetched.contains(&huge_b));

        // Small single-manifest 1-layer case (config + layer = 2 digests) also
        // resolves via direct cached lookups without scanning unrelated blobs.
        let mut small_store = MobyContentStore::new(
            MockMeta {
                sizes: &sizes,
                queried: HashMap::new(),
                batch_sizes: Vec::new(),
                fail_single: false,
                fail_batch: false,
            },
            |digest: &str, _max: usize| {
                payloads
                    .get(digest)
                    .cloned()
                    .ok_or_else(|| oci_rewrite::BlobReadError::Other(format!("missing {digest}")))
            },
            |digest: &str| {
                payloads
                    .get(digest)
                    .map(|b| oci_rewrite::parse_config_platform_reader(b.as_slice()))
                    .transpose()
                    .map(Option::flatten)
            },
        );
        let oci_rewrite::PrepareRewrite::Ready(small_plan) =
            oci_rewrite::prepare_rewrite_from_store(&manifest_d, "arm64", &mut small_store)
                .unwrap()
        else {
            panic!("expected Ready plan for small single-manifest 1-layer image");
        };
        assert_eq!(small_plan.layer_digests, vec![layer_d]);
        assert_eq!(small_store.meta.queried.len(), 3); // manifest, config, layer
        assert!(small_store.meta.queried.values().all(|&n| n == 1));
        assert_eq!(small_store.meta.batch_sizes, vec![2]); // [config, layer]

        // RPC errors on Content.Info must propagate instead of looking like RootMissing.
        let mut err_single_store = MobyContentStore::new(
            MockMeta {
                sizes: &sizes,
                queried: HashMap::new(),
                batch_sizes: Vec::new(),
                fail_single: true,
                fail_batch: false,
            },
            |_digest: &str, _max: usize| unreachable!(),
            |_digest: &str| unreachable!(),
        );
        let err = oci_rewrite::prepare_rewrite_from_store(&root_d, "arm64", &mut err_single_store)
            .unwrap_err();
        assert!(err.contains("grpc-status 13"), "{err}");

        // RPC errors on batched Content.List must also propagate instead of looking like NotAvailable.
        let mut err_batch_store = MobyContentStore::new(
            MockMeta {
                sizes: &sizes,
                queried: HashMap::new(),
                batch_sizes: Vec::new(),
                fail_single: false,
                fail_batch: true,
            },
            |digest: &str, _max: usize| {
                payloads
                    .get(digest)
                    .cloned()
                    .ok_or_else(|| oci_rewrite::BlobReadError::Other(format!("missing {digest}")))
            },
            |_digest: &str| unreachable!(),
        );
        let err = oci_rewrite::prepare_rewrite_from_store(&root_d, "arm64", &mut err_batch_store)
            .unwrap_err();
        assert!(err.contains("grpc-status 13"), "{err}");
    }

    #[test]
    fn combine_mirror_results_preserves_exporter_and_importer_errors_and_timeouts() {
        let err =
            combine_mirror_results(Ok(0), "", Ok(1), "ctr: no space left on device").unwrap_err();
        assert!(
            err.contains("ctr import exited 1: ctr: no space left on device"),
            "{err}"
        );

        let err = combine_mirror_results(
            Err("ctr export timed out".into()),
            "",
            Err("ctr import timed out".into()),
            "stalled",
        )
        .unwrap_err();
        assert!(err.contains("ctr export timed out"), "{err}");
        assert!(err.contains("ctr import timed out: stalled"), "{err}");
    }

    #[test]
    fn drain_bytes_checked_rejects_oversized_stdout() {
        let small = b"sha256:abc\n";
        assert_eq!(drain_bytes_checked(&small[..], 32).unwrap(), small.to_vec());
        let big = [b'a'; 64];
        let err = drain_bytes_checked(&big[..], 32).unwrap_err();
        assert!(
            matches!(err, RunBytesError::ExceededLimit(ref m) if m.contains("exceeded 32-byte limit")),
            "{err:?}"
        );
    }

    #[test]
    fn oversized_moby_listing_errors_instead_of_orphaning_labelled_mirror() {
        // Construct a `moby` listing whose tail contains a native replacement
        // for a labelled `k8s.io` tag past the byte limit: checked parsing must
        // fail explicitly rather than returning a partial map that omits the
        // tail entry and triggers orphan retirement.
        let mut listing = String::from("REF TYPE DIGEST SIZE PLATFORMS LABELS\n");
        for i in 0..20 {
            listing.push_str(&format!(
                "docker.io/library/filler-{i:02}:latest application/vnd.oci.image.manifest.v1+json sha256:{:0>64} 4.0 MiB linux/arm64 -\n",
                i
            ));
        }
        let cutoff = listing.len() - 16;
        listing.push_str(&format!(
            "docker.io/library/app:dev application/vnd.oci.image.manifest.v1+json sha256:{} 4.0 MiB linux/arm64 -\n",
            "f".repeat(64)
        ));

        let err = parse_checked_ctr_platforms(listing.as_bytes(), cutoff).unwrap_err();
        assert!(err.contains("exceeded"), "{err}");

        // Within the limit, the full listing (including the final native entry)
        // parses cleanly so `app:dev` is NOT treated as an orphan.
        let parsed = parse_checked_ctr_platforms(listing.as_bytes(), listing.len() + 64).unwrap();
        assert!(parsed.contains_key("docker.io/library/app:dev"));
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

    #[test]
    fn commit_installed_binary_invalidates_stale_version_on_write_failure() {
        let dir = temp_dir("install-version-marker");
        let bin = dir.join("k3s");
        let tmp = dir.join("k3s.tmp");
        let version_file = dir.join("version");

        fs::write(&bin, b"k3s-v1.37").unwrap();
        fs::write(&version_file, "v1.37.2+k3s1\n").unwrap();
        fs::write(&tmp, b"k3s-v1.32").unwrap();

        let err =
            commit_installed_binary_with(&tmp, &bin, &version_file, "v1.32.5+k3s1", |_, _| {
                Err("simulated write failure".into())
            })
            .unwrap_err();
        assert!(err.contains("simulated write failure"), "{err}");
        assert_eq!(fs::read(&bin).unwrap(), b"k3s-v1.32");
        assert!(
            !version_file.exists(),
            "stale version marker must not survive after swapping binary"
        );

        // Retry with a working marker write succeeds cleanly even without an existing marker.
        fs::write(&tmp, b"k3s-v1.32-retry").unwrap();
        commit_installed_binary(&tmp, &bin, &version_file, "v1.32.5+k3s1").unwrap();
        assert_eq!(fs::read(&bin).unwrap(), b"k3s-v1.32-retry");
        assert_eq!(fs::read_to_string(&version_file).unwrap(), "v1.32.5+k3s1\n");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn datastore_downgrade_is_blocked_without_modifying_datastore() {
        let dir = temp_dir("datastore-downgrade");
        let data_dir = dir.join("data");
        let data_version_file = data_dir.join(".katamaran-version");
        let bin_version_file = dir.join("bin/version");
        fs::create_dir_all(data_dir.join("server/db")).unwrap();
        fs::create_dir_all(bin_version_file.parent().unwrap()).unwrap();
        let db_file = data_dir.join("server/db/state.db");
        fs::write(&db_file, b"v1.37-datastore-bytes").unwrap();

        // Unmarked restored (or legacy) server/ on disk with an older bin/version
        // must be rejected without creating .katamaran-version or modifying data_dir.
        fs::write(&bin_version_file, "v1.32.5+k3s1\n").unwrap();
        let err =
            check_datastore_compatible(&data_dir, &data_version_file, "v1.32.5+k3s1").unwrap_err();
        assert!(err.contains("no version marker"), "{err}");
        assert!(err.contains(".katamaran-version"), "{err}");
        assert!(err.contains("kat k8s mark-version"), "{err}");
        assert!(err.contains("unmarked-cluster migration"), "{err}");
        assert!(err.contains("kat k8s reset"), "{err}");
        assert!(!data_version_file.exists());
        assert_eq!(fs::read(&db_file).unwrap(), b"v1.37-datastore-bytes");

        // Empty or whitespace-only .katamaran-version is reported as corrupt/empty
        // rather than missing, and does NOT recommend `kat k8s mark-version` (which
        // refuses to overwrite any existing marker).
        fs::write(&data_version_file, "   \n").unwrap();
        let err =
            check_datastore_compatible(&data_dir, &data_version_file, "v1.37.2+k3s1").unwrap_err();
        assert!(err.contains("is empty"), "{err}");
        assert!(!err.contains("kat k8s mark-version"), "{err}");
        assert!(err.contains("kat k8s reset"), "{err}");

        // Symlinked (including dangling) .katamaran-version is also rejected as
        // non-regular rather than missing.
        fs::remove_file(&data_version_file).unwrap();
        symlink(dir.join("missing-target"), &data_version_file).unwrap();
        let err =
            check_datastore_compatible(&data_dir, &data_version_file, "v1.37.2+k3s1").unwrap_err();
        assert!(err.contains("not a regular file"), "{err}");
        assert!(!err.contains("kat k8s mark-version"), "{err}");
        fs::remove_file(&data_version_file).unwrap();

        // Marked v1.37 datastore blocks minor downgrades without modifying data_dir.
        write_atomic(&data_version_file, b"v1.37.2+k3s1\n").unwrap();
        let err =
            check_datastore_compatible(&data_dir, &data_version_file, "v1.32.5+k3s1").unwrap_err();
        assert!(err.contains("v1.32.5+k3s1"), "{err}");
        assert!(err.contains("v1.37.2+k3s1"), "{err}");
        assert!(err.contains("kat k8s reset"), "{err}");
        assert!(err.contains(".katamaran-version"), "{err}");
        assert_eq!(fs::read(&db_file).unwrap(), b"v1.37-datastore-bytes");

        // Same minor (patch change) and next-minor upgrades are permitted.
        check_datastore_compatible(&data_dir, &data_version_file, "v1.37.1+k3s1").unwrap();
        check_datastore_compatible(&data_dir, &data_version_file, "v1.38.0+k3s1").unwrap();

        // Multi-minor upgrades (skipping intermediate minors) are blocked without
        // modifying data_dir, including jumping from a custom older minor to a much
        // newer pinned release.
        let err =
            check_datastore_compatible(&data_dir, &data_version_file, "v1.39.0+k3s1").unwrap_err();
        assert!(err.contains("skipping minor versions"), "{err}");
        assert!(err.contains("v1.38.x+k3s1"), "{err}");
        assert!(err.contains("kat k8s reset"), "{err}");
        assert_eq!(fs::read(&db_file).unwrap(), b"v1.37-datastore-bytes");

        // Clearing k8s_version from v1.32 to pinned v1.37 is rejected as a skipped-minor jump.
        write_atomic(&data_version_file, b"v1.32.5+k3s1\n").unwrap();
        let err =
            check_datastore_compatible(&data_dir, &data_version_file, "v1.37.2+k3s1").unwrap_err();
        assert!(err.contains("skipping minor versions"), "{err}");
        assert!(err.contains("v1.33.x+k3s1"), "{err}");
        check_datastore_compatible(&data_dir, &data_version_file, "v1.33.0+k3s1").unwrap();

        // After reset removes data_dir, starting a lower minor is allowed even while
        // bin/version still records a newer installed binary.
        fs::write(&bin_version_file, "v1.37.2+k3s1\n").unwrap();
        fs::remove_dir_all(&data_dir).unwrap();
        check_datastore_compatible(&data_dir, &data_version_file, "v1.32.5+k3s1").unwrap();

        // Empty data_dir without server/ (e.g. failed pre-start) does not block switching,
        // whether or not a stale data marker exists.
        fs::create_dir_all(data_dir.join("agent/etc/cni/net.d")).unwrap();
        check_datastore_compatible(&data_dir, &data_version_file, "v1.32.5+k3s1").unwrap();
        fs::write(&data_version_file, "v1.37.2+k3s1\n").unwrap();
        check_datastore_compatible(&data_dir, &data_version_file, "v1.32.5+k3s1").unwrap();

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn startup_failure_rolls_back_data_version_only_when_datastore_untouched() {
        let dir = temp_dir("datastore-rollback");
        let data_dir = dir.join("data");
        let data_version_file = data_dir.join(".katamaran-version");
        fs::create_dir_all(data_dir.join("server/db")).unwrap();
        let db_file = data_dir.join("server/db/state.db");
        let token_file = data_dir.join("server/token");
        fs::write(&db_file, b"v1.32-state-0001").unwrap();
        fs::write(&token_file, b"cluster-token-v1").unwrap();
        // Normal k3s creates relative internal symlinks inside server/ (e.g. agent-token -> token).
        symlink("token", data_dir.join("server/agent-token")).unwrap();
        symlink("token", data_dir.join("server/node-token")).unwrap();
        fs::write(&data_version_file, "v1.32.5+k3s1\n").unwrap();

        // 1. Process exits during startup without touching server/ (even with k3s's
        // normal internal agent-token/node-token symlinks present): version marker
        // rolls back so the operator can return to v1.32.5+k3s1.
        let before = snapshot_datastore(&data_dir).unwrap();
        write_atomic(&data_version_file, b"v1.33.0+k3s1\n").unwrap();
        rollback_data_version_if_untouched(
            &data_dir,
            &data_version_file,
            Some("v1.32.5+k3s1"),
            Some(&before),
        );
        assert_eq!(
            fs::read_to_string(&data_version_file).unwrap(),
            "v1.32.5+k3s1\n"
        );
        check_datastore_compatible(&data_dir, &data_version_file, "v1.32.5+k3s1").unwrap();

        // Modifying the internal symlink's target file (server/token) IS detected.
        let before = snapshot_datastore(&data_dir).unwrap();
        write_atomic(&data_version_file, b"v1.33.0+k3s1\n").unwrap();
        fs::write(&token_file, b"cluster-token-v2").unwrap();
        rollback_data_version_if_untouched(
            &data_dir,
            &data_version_file,
            Some("v1.32.5+k3s1"),
            Some(&before),
        );
        assert_eq!(
            fs::read_to_string(&data_version_file).unwrap(),
            "v1.33.0+k3s1\n"
        );
        write_atomic(&data_version_file, b"v1.32.5+k3s1\n").unwrap();

        // 2. Process modifies state.db in place (even with identical length) before
        // exiting: version marker stays bumped and blocks returning to v1.32.5+k3s1.
        let before = snapshot_datastore(&data_dir).unwrap();
        write_atomic(&data_version_file, b"v1.33.0+k3s1\n").unwrap();
        fs::write(&db_file, b"v1.33-state-0001").unwrap();
        rollback_data_version_if_untouched(
            &data_dir,
            &data_version_file,
            Some("v1.32.5+k3s1"),
            Some(&before),
        );
        assert_eq!(
            fs::read_to_string(&data_version_file).unwrap(),
            "v1.33.0+k3s1\n"
        );
        assert!(check_datastore_compatible(&data_dir, &data_version_file, "v1.32.5+k3s1").is_err());

        // 3. Process creates a WAL file under server/db/ before exiting: marker stays bumped.
        write_atomic(&data_version_file, b"v1.32.5+k3s1\n").unwrap();
        let before = snapshot_datastore(&data_dir).unwrap();
        write_atomic(&data_version_file, b"v1.33.0+k3s1\n").unwrap();
        fs::write(data_dir.join("server/db/state.db-wal"), b"wal-frame").unwrap();
        rollback_data_version_if_untouched(
            &data_dir,
            &data_version_file,
            Some("v1.32.5+k3s1"),
            Some(&before),
        );
        assert_eq!(
            fs::read_to_string(&data_version_file).unwrap(),
            "v1.33.0+k3s1\n"
        );

        // 4. Missing pre-spawn snapshot fails safe and keeps the updated marker.
        write_atomic(&data_version_file, b"v1.34.0+k3s1\n").unwrap();
        rollback_data_version_if_untouched(
            &data_dir,
            &data_version_file,
            Some("v1.33.0+k3s1"),
            None,
        );
        assert_eq!(
            fs::read_to_string(&data_version_file).unwrap(),
            "v1.34.0+k3s1\n"
        );

        // 5. Symlinked server/db (or server itself, or state.db) fails closed so writes
        // through the symlink cannot bypass the downgrade guard.
        let external_db = dir.join("external-db");
        fs::create_dir_all(&external_db).unwrap();
        let external_state = external_db.join("state.db");
        fs::write(&external_state, b"v1.32-external").unwrap();
        fs::remove_dir_all(data_dir.join("server/db")).unwrap();
        symlink(&external_db, data_dir.join("server/db")).unwrap();
        write_atomic(&data_version_file, b"v1.32.5+k3s1\n").unwrap();
        let symlink_before = snapshot_datastore(&data_dir);
        assert!(
            symlink_before.is_err(),
            "symlinked server/db must fail closed"
        );
        write_atomic(&data_version_file, b"v1.33.0+k3s1\n").unwrap();
        fs::write(&external_state, b"v1.33-external").unwrap();
        rollback_data_version_if_untouched(
            &data_dir,
            &data_version_file,
            Some("v1.32.5+k3s1"),
            symlink_before.ok().as_ref(),
        );
        assert_eq!(
            fs::read_to_string(&data_version_file).unwrap(),
            "v1.33.0+k3s1\n"
        );
        assert!(check_datastore_compatible(&data_dir, &data_version_file, "v1.32.5+k3s1").is_err());

        // Symlinked state.db file inside a real server/db directory also fails closed.
        fs::remove_file(data_dir.join("server/db")).unwrap();
        fs::create_dir_all(data_dir.join("server/db")).unwrap();
        symlink(&external_state, data_dir.join("server/db/state.db")).unwrap();
        assert!(
            snapshot_datastore(&data_dir).is_err(),
            "symlinked state.db must fail closed"
        );

        // Dangling symlink inside server/ also fails closed.
        fs::remove_file(data_dir.join("server/db/state.db")).unwrap();
        symlink(
            dir.join("does-not-exist"),
            data_dir.join("server/db/state.db"),
        )
        .unwrap();
        assert!(
            snapshot_datastore(&data_dir).is_err(),
            "dangling symlink inside server/ must fail closed"
        );

        // Symlinked server/ itself also fails closed.
        let external_server = dir.join("external-server");
        fs::create_dir_all(external_server.join("db")).unwrap();
        fs::write(external_server.join("db/state.db"), b"v1.32-server").unwrap();
        fs::remove_dir_all(data_dir.join("server")).unwrap();
        symlink(&external_server, data_dir.join("server")).unwrap();
        assert!(
            snapshot_datastore(&data_dir).is_err(),
            "symlinked server/ must fail closed"
        );

        // 6. Fresh data_dir without server/: removes marker if server/ stays absent,
        // keeps marker if k3s created server/ before exiting.
        fs::remove_dir_all(&data_dir).unwrap();
        fs::create_dir_all(&data_dir).unwrap();
        let before = snapshot_datastore(&data_dir).unwrap();
        assert_eq!(before, DatastoreSnapshot::Absent);
        write_atomic(&data_version_file, b"v1.37.2+k3s1\n").unwrap();
        rollback_data_version_if_untouched(&data_dir, &data_version_file, None, Some(&before));
        assert!(!data_version_file.exists());

        write_atomic(&data_version_file, b"v1.37.2+k3s1\n").unwrap();
        fs::create_dir_all(data_dir.join("server")).unwrap();
        rollback_data_version_if_untouched(&data_dir, &data_version_file, None, Some(&before));
        assert_eq!(
            fs::read_to_string(&data_version_file).unwrap(),
            "v1.37.2+k3s1\n"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn mark_datastore_version_migrates_pre_tracking_datastore_for_pinned_upgrade() {
        let dir = temp_dir("mark-datastore-version");
        let data_dir = dir.join("data");
        let data_version_file = data_dir.join(".katamaran-version");
        let bin_version_file = dir.join("bin/version");
        fs::create_dir_all(bin_version_file.parent().unwrap()).unwrap();
        fs::write(&bin_version_file, "v1.37.2+k3s1\n").unwrap();

        // 1. Fails when no datastore exists yet.
        let err =
            mark_datastore_version_at(&data_dir, &data_version_file, "v1.37.2+k3s1").unwrap_err();
        assert!(err.contains("no existing cluster datastore"), "{err}");

        // 2. Pre-tracking datastore (server/ exists, no .katamaran-version, same pinned
        // v1.37.2+k3s1 in bin/version) is initially rejected without inference.
        fs::create_dir_all(data_dir.join("server/db")).unwrap();
        fs::write(data_dir.join("server/db/state.db"), b"pre-tracking-v1.37").unwrap();
        assert!(check_datastore_compatible(&data_dir, &data_version_file, "v1.37.2+k3s1").is_err());
        assert!(!data_version_file.exists());

        // 3. Invalid or unsupported release tags are rejected without writing a marker.
        for bad in [
            "",
            "latest",
            "v1.30.9+k3s1",
            "v1.37.2+k3s1\n",
            " v1.37.2+k3s1",
            "v2.0.0+k3s1",
        ] {
            assert!(
                mark_datastore_version_at(&data_dir, &data_version_file, bad).is_err(),
                "{bad:?}"
            );
        }
        assert!(!data_version_file.exists());

        // 4. Explicit migration writes the verified pinned tag with mode 0600 and
        // unblocks starting the same pinned release while still blocking downgrades.
        mark_datastore_version_at(&data_dir, &data_version_file, "v1.37.2+k3s1").unwrap();
        assert_eq!(
            fs::read_to_string(&data_version_file).unwrap(),
            "v1.37.2+k3s1\n"
        );
        assert_eq!(
            fs::symlink_metadata(&data_version_file)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        check_datastore_compatible(&data_dir, &data_version_file, "v1.37.2+k3s1").unwrap();
        assert!(check_datastore_compatible(&data_dir, &data_version_file, "v1.32.5+k3s1").is_err());

        // 5. Refuses to overwrite an existing marker (populated, empty, or symlinked).
        let err =
            mark_datastore_version_at(&data_dir, &data_version_file, "v1.32.5+k3s1").unwrap_err();
        assert!(err.contains("already exists"), "{err}");
        assert_eq!(
            fs::read_to_string(&data_version_file).unwrap(),
            "v1.37.2+k3s1\n"
        );

        fs::write(&data_version_file, "  \n").unwrap();
        let err =
            mark_datastore_version_at(&data_dir, &data_version_file, "v1.37.2+k3s1").unwrap_err();
        assert!(err.contains("already exists (empty)"), "{err}");

        fs::remove_file(&data_version_file).unwrap();
        let target = dir.join("other-file");
        symlink(&target, &data_version_file).unwrap();
        let err =
            mark_datastore_version_at(&data_dir, &data_version_file, "v1.37.2+k3s1").unwrap_err();
        assert!(err.contains("already exists (not a regular file)"), "{err}");
        assert!(!target.exists());

        let _ = fs::remove_dir_all(&dir);
    }

    fn run_test_cmd(cmd: &mut Command) -> Result<(i32, String), String> {
        let out = cmd.output().map_err(|e| e.to_string())?;
        let code = out.status.code().unwrap_or(-1);
        Ok((code, String::from_utf8_lossy(&out.stdout).into_owned()))
    }

    fn test_scratch_root() -> PathBuf {
        std::env::temp_dir().join(format!("kat-init-state-db-scratch-{}", std::process::id()))
    }

    fn with_test_probe<T>(f: impl FnOnce(&StateDbProbe<'_>) -> T) -> T {
        let scratch_root = test_scratch_root();
        f(&StateDbProbe {
            sqlite3_bin: Path::new(SQLITE3_BIN),
            scratch_root: &scratch_root,
            max_copy_bytes: STATE_DB_MAX_SCRATCH_BYTES,
        })
    }

    fn check_datastore_compatible(
        data_dir: &Path,
        data_version_file: &Path,
        target_version: &str,
    ) -> Result<(), String> {
        with_test_probe(|probe| {
            check_datastore_compatible_with(
                data_dir,
                data_version_file,
                target_version,
                probe,
                run_test_cmd,
            )
        })
    }

    fn mark_datastore_version_at(
        data_dir: &Path,
        data_version_file: &Path,
        version: &str,
    ) -> Result<(), String> {
        with_test_probe(|probe| {
            mark_datastore_version_at_with(
                data_dir,
                data_version_file,
                version,
                probe,
                run_test_cmd,
            )
        })
    }

    fn push_proto_varint(out: &mut Vec<u8>, mut v: u64) {
        loop {
            let mut b = (v & 0x7f) as u8;
            v >>= 7;
            if v != 0 {
                b |= 0x80;
            }
            out.push(b);
            if v == 0 {
                break;
            }
        }
    }

    fn push_proto_bytes(out: &mut Vec<u8>, field: u32, bytes: &[u8]) {
        push_proto_varint(out, (u64::from(field) << 3) | 2);
        push_proto_varint(out, bytes.len() as u64);
        out.extend_from_slice(bytes);
    }

    fn encode_k8s_node_proto(kubelet_version: &str) -> Vec<u8> {
        let mut node_info = Vec::new();
        push_proto_bytes(&mut node_info, 1, b"machine-id-123");
        push_proto_bytes(&mut node_info, 4, b"6.18.64");
        push_proto_bytes(&mut node_info, 6, b"containerd://2.0.0");
        push_proto_bytes(&mut node_info, 7, kubelet_version.as_bytes());
        push_proto_bytes(&mut node_info, 9, b"linux");
        push_proto_bytes(&mut node_info, 10, b"arm64");

        let mut node_status = Vec::new();
        push_proto_bytes(&mut node_status, 3, b"Running");
        push_proto_bytes(&mut node_status, 7, &node_info);

        let mut node = Vec::new();
        push_proto_bytes(&mut node, 1, b"\x0a\x09katamaran");
        push_proto_bytes(&mut node, 3, &node_status);

        let mut type_meta = Vec::new();
        push_proto_bytes(&mut type_meta, 1, b"v1");
        push_proto_bytes(&mut type_meta, 2, b"Node");

        let mut unknown = Vec::new();
        push_proto_bytes(&mut unknown, 1, &type_meta);
        push_proto_bytes(&mut unknown, 2, &node);

        let mut out = Vec::with_capacity(4 + unknown.len());
        out.extend_from_slice(b"k8s\0");
        out.extend_from_slice(&unknown);
        out
    }

    fn hex_upper(bytes: &[u8]) -> String {
        let mut s = String::with_capacity(bytes.len() * 2);
        for &b in bytes {
            s.push_str(&format!("{b:02X}"));
        }
        s
    }

    fn sqlite_exec(db: &Path, sql: &str) {
        let out = Command::new(SQLITE3_BIN)
            .args(["-bail", "-batch"])
            .arg(db)
            .arg(sql)
            .output()
            .expect("run sqlite3");
        assert!(
            out.status.success(),
            "sqlite3 failed ({}): {}",
            out.status,
            String::from_utf8_lossy(&out.stderr)
        );
    }

    #[test]
    fn extracts_kubelet_version_only_from_k8s_protobuf_nodes() {
        let proto = encode_k8s_node_proto("v1.37.2+k3s1");
        assert_eq!(
            extract_kubelet_version_from_node_blob(&proto).as_deref(),
            Some("v1.37.2+k3s1")
        );

        // Explicit protobuf contentType inside runtime.Unknown.
        let mut with_ct = proto.clone();
        push_proto_bytes(&mut with_ct, 4, b"application/vnd.kubernetes.protobuf");
        assert_eq!(
            extract_kubelet_version_from_node_blob(&with_ct).as_deref(),
            Some("v1.37.2+k3s1")
        );

        // JSON (plain or inside the envelope) is not an encoding kube-apiserver uses
        // for core types in kine, so it is rejected rather than parsed.
        let json = br#"{"apiVersion":"v1","kind":"Node","status":{"nodeInfo":{"kubeletVersion":"v1.32.5+k3s1"}}}"#;
        assert_eq!(extract_kubelet_version_from_node_blob(json), None);
        let mut unknown_json = b"k8s\0".to_vec();
        let mut type_meta = Vec::new();
        push_proto_bytes(&mut type_meta, 1, b"v1");
        push_proto_bytes(&mut type_meta, 2, b"Node");
        push_proto_bytes(&mut unknown_json, 1, &type_meta);
        push_proto_bytes(&mut unknown_json, 2, json);
        push_proto_bytes(&mut unknown_json, 4, b"application/json");
        assert_eq!(extract_kubelet_version_from_node_blob(&unknown_json), None);

        // Non-empty contentEncoding, wrong kind, truncated protobuf, or missing field fail closed.
        let mut compressed = proto.clone();
        push_proto_bytes(&mut compressed, 3, b"gzip");
        assert_eq!(extract_kubelet_version_from_node_blob(&compressed), None);

        let wrong_kind = encode_k8s_node_proto("v1.37.2+k3s1")
            .windows(4)
            .position(|w| w == b"Node")
            .map(|i| {
                let mut b = encode_k8s_node_proto("v1.37.2+k3s1");
                b[i..i + 4].copy_from_slice(b"Pods");
                b
            })
            .unwrap();
        assert_eq!(extract_kubelet_version_from_node_blob(&wrong_kind), None);

        assert_eq!(
            extract_kubelet_version_from_node_blob(&proto[..proto.len() - 3]),
            None
        );
        assert_eq!(extract_kubelet_version_from_node_blob(b"not-k8s"), None);
    }

    #[test]
    fn unmarked_state_db_enables_seamless_upgrade_and_blocks_downgrade_without_mutation() {
        assert!(
            Path::new(SQLITE3_BIN).is_file(),
            "{SQLITE3_BIN} must be installed in the Linux test environment"
        );

        let dir = temp_dir("state-db-detection");
        let data_dir = dir.join("data");
        let data_version_file = data_dir.join(".katamaran-version");
        let db_dir = data_dir.join("server/db");
        fs::create_dir_all(&db_dir).unwrap();
        let db_path = db_dir.join("state.db");

        let v32 = hex_upper(&encode_k8s_node_proto("v1.32.5+k3s1"));
        let v37 = hex_upper(&encode_k8s_node_proto("v1.37.2+k3s1"));
        let v38 = hex_upper(&encode_k8s_node_proto("v1.38.0+k3s1"));

        // 1. Pre-tracking v1.37.2+k3s1 datastore (no .katamaran-version), plus an older
        // deleted node row that must be ignored because its head revision has deleted=1.
        sqlite_exec(
            &db_path,
            &format!(
                "CREATE TABLE kine (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT,
                    created INTEGER,
                    deleted INTEGER,
                    create_revision INTEGER,
                    prev_revision INTEGER,
                    lease INTEGER,
                    value BLOB,
                    old_value BLOB
                );
                CREATE INDEX kine_name_id_index ON kine (name, id);
                INSERT INTO kine (name, deleted, value) VALUES ('/registry/minions/old-node', 0, X'{v32}');
                INSERT INTO kine (name, deleted, value) VALUES ('/registry/minions/old-node', 1, X'');
                INSERT INTO kine (name, deleted, value) VALUES ('/registry/minions/katamaran', 0, X'{v37}');"
            ),
        );

        let before = snapshot_datastore(&data_dir).unwrap();
        // Seamless start on the unchanged pinned build and 1-minor upgrade both pass.
        check_datastore_compatible(&data_dir, &data_version_file, "v1.37.2+k3s1").unwrap();
        check_datastore_compatible(&data_dir, &data_version_file, "v1.38.0+k3s1").unwrap();
        // Downgrade and skipped-minor jump are both blocked without modifying server/.
        let err =
            check_datastore_compatible(&data_dir, &data_version_file, "v1.32.5+k3s1").unwrap_err();
        assert!(err.contains("v1.32.5+k3s1"), "{err}");
        assert!(err.contains("v1.37.2+k3s1"), "{err}");
        let err =
            check_datastore_compatible(&data_dir, &data_version_file, "v1.39.0+k3s1").unwrap_err();
        assert!(err.contains("skipping minor versions"), "{err}");
        // A trusted pre-tracking node record cannot be re-marked (e.g. as v1.39 to
        // skip a minor).
        let err =
            mark_datastore_version_at(&data_dir, &data_version_file, "v1.39.0+k3s1").unwrap_err();
        assert!(err.contains("already accepts without a marker"), "{err}");
        assert_eq!(snapshot_datastore(&data_dir).unwrap(), before);
        assert!(!data_version_file.exists());

        // 2. A node version that no pre-tracking release ran (v1.38.0+k3s1) is only a
        // lower bound: it authorizes neither the pinned build nor its own release, and
        // blocks `mark-version` with a lower minor.
        sqlite_exec(
            &db_path,
            &format!(
                "INSERT INTO kine (name, deleted, value) VALUES ('/registry/minions/katamaran', 0, X'{v38}');"
            ),
        );
        for target in ["v1.37.2+k3s1", "v1.38.0+k3s1"] {
            let err =
                check_datastore_compatible(&data_dir, &data_version_file, target).unwrap_err();
            assert!(err.contains("reports k3s v1.38.0+k3s1"), "{err}");
            assert!(err.contains("lower bound"), "{err}");
            assert!(err.contains("kat k8s mark-version"), "{err}");
        }
        let err =
            mark_datastore_version_at(&data_dir, &data_version_file, "v1.37.2+k3s1").unwrap_err();
        assert!(
            err.contains("state.db already records newer k3s v1.38.0+k3s1"),
            "{err}"
        );

        // 3. WAL-mode state.db with the pre-tracking v1.37.2+k3s1 node ONLY in an
        // uncheckpointed state.db-wal (main file still says v1.32.5+k3s1): detection reads
        // the WAL from a tmpfs-style scratch copy without modifying server/db/ and cleans up.
        let wal_src = dir.join("wal-src");
        fs::create_dir_all(&wal_src).unwrap();
        let wal_src_db = wal_src.join("state.db");
        sqlite_exec(
            &wal_src_db,
            &format!(
                "PRAGMA journal_mode=WAL;
                CREATE TABLE kine (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, deleted INTEGER, value BLOB);
                CREATE INDEX kine_name_id_index ON kine (name, id);
                INSERT INTO kine (name, deleted, value) VALUES ('/registry/minions/katamaran', 0, X'{v32}');"
            ),
        );
        // Hold an open read transaction while appending v1.37 so it remains uncheckpointed in -wal.
        let mut reader = Command::new(SQLITE3_BIN)
            .arg(&wal_src_db)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        {
            let stdin = reader.stdin.as_mut().unwrap();
            writeln!(stdin, "BEGIN;").unwrap();
            writeln!(stdin, "SELECT count(*) FROM kine;").unwrap();
            stdin.flush().unwrap();
            let mut line = String::new();
            let mut stdout = BufReader::new(reader.stdout.take().unwrap());
            stdout.read_line(&mut line).unwrap();
            assert_eq!(line.trim(), "1");
        }
        sqlite_exec(
            &wal_src_db,
            &format!(
                "PRAGMA wal_autocheckpoint=0;
                INSERT INTO kine (name, deleted, value) VALUES ('/registry/minions/katamaran', 0, X'{v37}');"
            ),
        );

        let wal_root = dir.join("wal-case");
        let wal_data = wal_root.join("data");
        let wal_marker = wal_data.join(".katamaran-version");
        let wal_db_dir = wal_data.join("server/db");
        fs::create_dir_all(&wal_db_dir).unwrap();
        fs::copy(&wal_src_db, wal_db_dir.join("state.db")).unwrap();
        fs::copy(
            wal_src.join("state.db-wal"),
            wal_db_dir.join("state.db-wal"),
        )
        .unwrap();
        drop(reader.stdin.take());
        let _ = reader.wait();
        assert!(
            fs::metadata(wal_db_dir.join("state.db-wal")).unwrap().len() > 0,
            "state.db-wal must be non-empty"
        );

        // Confirm main state.db alone (immutable=1) still has only v1.32, proving v1.37
        // lives strictly in state.db-wal.
        let imm_out = Command::new(SQLITE3_BIN)
            .args([
                "-bail",
                "-batch",
                "-noheader",
                "-list",
                "-safe",
                &format!(
                    "file:{}?immutable=1",
                    encode_sqlite_uri_path(&wal_db_dir.join("state.db"))
                ),
                "SELECT hex(value) FROM kine ORDER BY id DESC LIMIT 1;",
            ])
            .output()
            .unwrap();
        assert_eq!(
            String::from_utf8_lossy(&imm_out.stdout).trim(),
            v32,
            "v1.37 row must reside solely in uncheckpointed state.db-wal"
        );

        let wal_before = snapshot_datastore(&wal_data).unwrap();
        check_datastore_compatible(&wal_data, &wal_marker, "v1.37.2+k3s1").unwrap();
        check_datastore_compatible(&wal_data, &wal_marker, "v1.38.0+k3s1").unwrap();
        assert_eq!(snapshot_datastore(&wal_data).unwrap(), wal_before);
        assert!(!wal_db_dir.join("state.db-shm").exists());
        // No inspection copy is ever placed beside the datastore, and the scratch
        // copy under the scratch root is removed after inspection.
        let leftover_scratch: Vec<_> = fs::read_dir(&wal_root)
            .unwrap()
            .chain(fs::read_dir(test_scratch_root()).unwrap())
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with(".state-db-inspect.")
            })
            .collect();
        assert!(leftover_scratch.is_empty(), "{leftover_scratch:?}");

        // 4. A non-pre-tracking version (v1.36.4+k3s1) is a lower bound only, even for
        // its own release.
        let json_data = dir.join("lower-bound-case/data");
        let json_marker = json_data.join(".katamaran-version");
        fs::create_dir_all(json_data.join("server/db")).unwrap();
        let json_blob = hex_upper(&encode_k8s_node_proto("v1.36.4+k3s1"));
        sqlite_exec(
            &json_data.join("server/db/state.db"),
            &format!(
                "CREATE TABLE kine (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, deleted INTEGER, value BLOB);
                INSERT INTO kine (name, deleted, value) VALUES ('/registry/minions/katamaran', 0, X'{json_blob}');"
            ),
        );
        for target in ["v1.36.4+k3s1", "v1.37.2+k3s1"] {
            let err = check_datastore_compatible(&json_data, &json_marker, target).unwrap_err();
            assert!(err.contains("reports k3s v1.36.4+k3s1"), "{err}");
        }
        let err = mark_datastore_version_at(&json_data, &json_marker, "v1.35.0+k3s1").unwrap_err();
        assert!(err.contains("newer k3s v1.36.4+k3s1"), "{err}");
        assert!(!json_marker.exists());

        // 5. Fail-closed cases (conflicting live node minors, malicious VIEW kine,
        // symlinked state.db, missing sqlite3) fall back to missing_datastore_version_error.
        sqlite_exec(
            &json_data.join("server/db/state.db"),
            &format!(
                "INSERT INTO kine (name, deleted, value) VALUES ('/registry/minions/node2', 0, X'{v37}');"
            ),
        );
        let err = check_datastore_compatible(&json_data, &json_marker, "v1.37.2+k3s1").unwrap_err();
        assert!(err.contains("no version marker"), "{err}");
        assert!(err.contains("kat k8s mark-version"), "{err}");

        let view_data = dir.join("view-case/data");
        let view_marker = view_data.join(".katamaran-version");
        fs::create_dir_all(view_data.join("server/db")).unwrap();
        sqlite_exec(
            &view_data.join("server/db/state.db"),
            &format!(
                "CREATE TABLE backing (id INTEGER PRIMARY KEY, name TEXT, deleted INTEGER, value BLOB);
                INSERT INTO backing VALUES (1, '/registry/minions/katamaran', 0, X'{v37}');
                CREATE VIEW kine AS SELECT * FROM backing;"
            ),
        );
        let err = check_datastore_compatible(&view_data, &view_marker, "v1.37.2+k3s1").unwrap_err();
        assert!(err.contains("no version marker"), "{err}");

        let sym_data = dir.join("sym-case/data");
        let sym_marker = sym_data.join(".katamaran-version");
        fs::create_dir_all(sym_data.join("server/db")).unwrap();
        symlink(&db_path, sym_data.join("server/db/state.db")).unwrap();
        let err = check_datastore_compatible(&sym_data, &sym_marker, "v1.38.0+k3s1").unwrap_err();
        assert!(err.contains("no version marker"), "{err}");

        let scratch_root = test_scratch_root();
        let err = check_datastore_compatible_with(
            &data_dir,
            &data_version_file,
            "v1.38.0+k3s1",
            &StateDbProbe {
                sqlite3_bin: Path::new("/nonexistent/sqlite3"),
                scratch_root: &scratch_root,
                max_copy_bytes: STATE_DB_MAX_SCRATCH_BYTES,
            },
            run_test_cmd,
        )
        .unwrap_err();
        assert!(err.contains("no version marker"), "{err}");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn failed_version_publish_restores_prior_marker_and_stamp() {
        let dir = temp_dir("publish-restore");
        let data_dir = dir.join("data");
        let marker = data_dir.join(".katamaran-version");
        fs::create_dir_all(data_dir.join("server/db")).unwrap();
        write_atomic(&marker, b"v1.37.2+k3s1\n").unwrap();
        write_server_version_stamp(&data_dir, "v1.37.2+k3s1").unwrap();
        let restore = || {
            restore_data_version(&marker, Some("v1.37.2+k3s1"));
            restore_server_version_stamp(&data_dir, Some("v1.37.2+k3s1"));
        };
        // The marker rename succeeds but its directory sync "fails": the new tag was
        // already visible, so it must be rolled back.
        let err = publish_version_files(
            &data_dir,
            &marker,
            "v1.38.0+k3s1",
            true,
            true,
            |p, b| {
                write_atomic(p, b)?;
                Err("sync failed".into())
            },
            restore,
        )
        .unwrap_err();
        assert_eq!(err, "sync failed");
        assert_eq!(fs::read_to_string(&marker).unwrap(), "v1.37.2+k3s1\n");
        assert_eq!(
            fs::read_to_string(server_version_stamp(&data_dir)).unwrap(),
            "v1.37.2+k3s1\n"
        );
        // A stamp failure after the marker was published also restores both.
        fs::remove_dir_all(data_dir.join("server/db")).unwrap();
        fs::write(data_dir.join("server/db"), "not a dir").unwrap();
        assert!(
            publish_version_files(
                &data_dir,
                &marker,
                "v1.38.0+k3s1",
                true,
                true,
                write_atomic,
                || restore_data_version(&marker, Some("v1.37.2+k3s1")),
            )
            .is_err()
        );
        assert_eq!(fs::read_to_string(&marker).unwrap(), "v1.37.2+k3s1\n");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn strict_parsing_rejects_malformed_recorded_versions() {
        for v in [
            "v1.37.2+k3sgarbage",
            "v1.37.2+k3s",
            "v1.037.2+k3s1",
            " v1.37.2+k3s1",
            "1.37.2+k3s1",
        ] {
            assert_eq!(parse_k3s_minor(v), None, "{v}");
        }
        assert_eq!(parse_k3s_minor("v1.37.2+k3s1"), Some((1, 37)));
        let dir = temp_dir("strict-marker");
        let data_dir = dir.join("data");
        let marker = data_dir.join(".katamaran-version");
        fs::create_dir_all(data_dir.join("server/db")).unwrap();
        fs::write(&marker, "v1.37.2+k3sgarbage\n").unwrap();
        let err = check_datastore_compatible(&data_dir, &marker, "v1.37.2+k3s1").unwrap_err();
        assert!(err.contains("invalid k3s version"), "{err}");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn checkpointed_state_db_is_read_in_place_regardless_of_copy_limit() {
        assert!(Path::new(SQLITE3_BIN).is_file());
        let dir = temp_dir("immutable-probe");
        let data_dir = dir.join("data");
        let db_dir = data_dir.join("server/db");
        fs::create_dir_all(&db_dir).unwrap();
        let v37 = hex_upper(&encode_k8s_node_proto("v1.37.2+k3s1"));
        sqlite_exec(
            &db_dir.join("state.db"),
            &format!(
                "CREATE TABLE kine (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, deleted INTEGER, value BLOB);
                INSERT INTO kine (name, deleted, value) VALUES ('/registry/minions/katamaran', 0, X'{v37}');"
            ),
        );
        let scratch_root = test_scratch_root();
        // A 1-byte copy limit would reject any copy; the checkpointed database is read
        // in place, so a datastore larger than the copy limit still migrates.
        let probe = StateDbProbe {
            sqlite3_bin: Path::new(SQLITE3_BIN),
            scratch_root: &scratch_root,
            max_copy_bytes: 1,
        };
        let before = snapshot_datastore(&data_dir).unwrap();
        assert_eq!(
            detect_datastore_version_from_state_db_with(&data_dir, &probe, run_test_cmd).as_deref(),
            Some("v1.37.2+k3s1")
        );
        assert_eq!(snapshot_datastore(&data_dir).unwrap(), before);
        assert!(!db_dir.join("state.db-shm").exists());
        assert!(!db_dir.join("state.db-wal").exists());
        // With uncheckpointed WAL content, the copy limit applies and detection fails closed.
        fs::write(db_dir.join("state.db-wal"), b"x").unwrap();
        assert_eq!(
            detect_datastore_version_from_state_db_with(&data_dir, &probe, run_test_cmd),
            None
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn unreaped_k3s_blocks_datastore_operations_until_it_exits() {
        // Tracked by the child's own exit signal, not a pid lookup that a reused
        // pid could satisfy.
        let (tx, rx) = mpsc::channel::<i32>();
        note_unreaped_k3s(4242, move || rx.try_recv().is_ok());
        let err = ensure_no_unreaped_k3s().unwrap_err();
        assert!(err.contains("pid 4242"), "{err}");
        assert!(ensure_no_unreaped_k3s().is_err());
        tx.send(0).unwrap();
        ensure_no_unreaped_k3s().unwrap();
        assert!(UNREAPED_K3S.lock().unwrap().is_none());
        ensure_no_unreaped_k3s().unwrap();
    }

    #[test]
    fn bounded_copy_refuses_files_over_budget() {
        let dir = temp_dir("bounded-copy");
        let src = dir.join("src");
        fs::write(&src, vec![0u8; 100]).unwrap();
        let mut budget = 99;
        assert!(copy_regular_file_bounded(&src, &dir.join("small"), &mut budget).is_err());
        let mut budget = 150;
        copy_regular_file_bounded(&src, &dir.join("ok"), &mut budget).unwrap();
        assert_eq!(budget, 50);
        assert_eq!(fs::read(dir.join("ok")).unwrap().len(), 100);
        let link = dir.join("link");
        symlink(&src, &link).unwrap();
        assert!(copy_regular_file_bounded(&link, &dir.join("via-link"), &mut 1000).is_err());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn path_resolves_rootfs_nft_for_oldest_supported_k3s() {
        // k3s prepends its own bin dir; when that lacks `nft` (k3s v1.31.0-v1.31.5),
        // kube-proxy's nftables mode must find the rootfs nftables package's binary.
        assert!(PATH.split(':').any(|d| d == "/usr/sbin"), "{PATH}");
        let rootfs = include_str!("../../../../guest/rootfs/build.sh");
        let packages = rootfs
            .split("PACKAGES=\"")
            .nth(1)
            .and_then(|r| r.split('"').next())
            .expect("PACKAGES list in guest/rootfs/build.sh");
        for pkg in ["nftables", "sqlite"] {
            assert!(
                packages.split_whitespace().any(|p| p == pkg),
                "guest rootfs must ship {pkg}: {packages}"
            );
        }
        assert_eq!(validate_k3s_release_tag("v1.31.0+k3s1"), Ok(()));
        assert!(validate_k3s_release_tag("v1.30.9+k3s1").is_err());
        assert!(validate_k3s_release_tag("v1.037.2+k3s1").is_err());
        assert!(validate_k3s_release_tag("v1.37.0+k3s1").is_ok());
    }

    #[test]
    fn server_stamp_outranks_stale_node_record_and_is_ignored_by_snapshots() {
        assert!(Path::new(SQLITE3_BIN).is_file());
        let dir = temp_dir("server-stamp");
        let data_dir = dir.join("data");
        let marker = data_dir.join(".katamaran-version");
        let db_dir = data_dir.join("server/db");
        fs::create_dir_all(&db_dir).unwrap();
        let v37 = hex_upper(&encode_k8s_node_proto("v1.37.2+k3s1"));
        sqlite_exec(
            &db_dir.join("state.db"),
            &format!(
                "CREATE TABLE kine (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, deleted INTEGER, value BLOB);
                INSERT INTO kine (name, deleted, value) VALUES ('/registry/minions/katamaran', 0, X'{v37}');"
            ),
        );

        // Writing the stamp does not count as a datastore change for rollback.
        let before = snapshot_datastore(&data_dir).unwrap();
        write_server_version_stamp(&data_dir, "v1.38.0+k3s1").unwrap();
        assert_eq!(snapshot_datastore(&data_dir).unwrap(), before);

        // A server/ backup without its marker, taken after a v1.38 server opened it but
        // before its kubelet reported status: the node record still says the pre-tracking
        // pin, but the stamp that travels with server/ blocks the older build.
        let err = check_datastore_compatible(&data_dir, &marker, "v1.37.2+k3s1").unwrap_err();
        assert!(err.contains("v1.38.0+k3s1"), "{err}");
        check_datastore_compatible(&data_dir, &marker, "v1.38.0+k3s1").unwrap();
        // mark-version cannot override a stamp in either direction.
        for v in ["v1.37.2+k3s1", "v1.39.0+k3s1"] {
            let err = mark_datastore_version_at(&data_dir, &marker, v).unwrap_err();
            assert!(err.contains("already records"), "{err}");
            assert!(err.contains("v1.38.0+k3s1"), "{err}");
        }
        assert!(!marker.exists());

        // An untouched failed upgrade restores each file to its prior state: with no
        // outer marker and an existing v1.37 stamp, a failed v1.38 start leaves v1.37
        // admissible, and a stamp that did not exist before is removed again.
        restore_server_version_stamp(&data_dir, Some("v1.37.2+k3s1"));
        let before = snapshot_datastore(&data_dir).unwrap();
        write_server_version_stamp(&data_dir, "v1.38.0+k3s1").unwrap();
        rollback_versions_if_untouched(&data_dir, Some(&before), || {
            restore_server_version_stamp(&data_dir, Some("v1.37.2+k3s1"));
        });
        check_datastore_compatible(&data_dir, &marker, "v1.37.2+k3s1").unwrap();
        restore_server_version_stamp(&data_dir, None);
        assert!(!server_version_stamp(&data_dir).exists());
        // Without a stamp, the pre-tracking node record is accepted again.
        check_datastore_compatible(&data_dir, &marker, "v1.37.2+k3s1").unwrap();

        // A touched datastore keeps the updated stamp.
        let before = snapshot_datastore(&data_dir).unwrap();
        write_server_version_stamp(&data_dir, "v1.38.0+k3s1").unwrap();
        fs::write(db_dir.join("state.db-touched"), "x").unwrap();
        rollback_versions_if_untouched(&data_dir, Some(&before), || {
            restore_server_version_stamp(&data_dir, None);
        });
        assert!(check_datastore_compatible(&data_dir, &marker, "v1.37.2+k3s1").is_err());
        fs::remove_file(db_dir.join("state.db-touched")).unwrap();

        // With both files present, the newer minor wins either way round.
        restore_server_version_stamp(&data_dir, Some("v1.38.0+k3s1"));
        fs::write(&marker, "v1.37.2+k3s1\n").unwrap();
        let err = check_datastore_compatible(&data_dir, &marker, "v1.37.2+k3s1").unwrap_err();
        assert!(err.contains("v1.38.0+k3s1"), "{err}");
        check_datastore_compatible(&data_dir, &marker, "v1.38.0+k3s1").unwrap();
        fs::write(&marker, "v1.38.0+k3s1\n").unwrap();
        restore_server_version_stamp(&data_dir, Some("v1.37.2+k3s1"));
        assert!(check_datastore_compatible(&data_dir, &marker, "v1.37.2+k3s1").is_err());
        check_datastore_compatible(&data_dir, &marker, "v1.38.0+k3s1").unwrap();
        fs::remove_file(&marker).unwrap();

        // The node record is a lower bound even with a marker and stamp: data last
        // run by v1.37.2 (e.g. an older katamaran after a rollback) refuses v1.36.
        fs::write(&marker, "v1.36.4+k3s1\n").unwrap();
        restore_server_version_stamp(&data_dir, Some("v1.36.4+k3s1"));
        let err = check_datastore_compatible(&data_dir, &marker, "v1.36.4+k3s1").unwrap_err();
        assert!(err.contains("v1.37.2+k3s1"), "{err}");
        check_datastore_compatible(&data_dir, &marker, "v1.37.2+k3s1").unwrap();
        check_datastore_compatible(&data_dir, &marker, "v1.38.0+k3s1").unwrap();
        fs::remove_file(&marker).unwrap();

        // An invalid stamp fails closed the same way in start and mark-version.
        fs::write(server_version_stamp(&data_dir), "garbage\n").unwrap();
        let err = check_datastore_compatible(&data_dir, &marker, "v1.37.2+k3s1").unwrap_err();
        assert!(err.contains("invalid k3s version"), "{err}");
        let err = mark_datastore_version_at(&data_dir, &marker, "v1.37.2+k3s1").unwrap_err();
        assert!(err.contains("not a valid version stamp"), "{err}");
        assert!(err.contains("kat k8s reset"), "{err}");

        // An invalid marker beside a valid stamp fails closed too.
        restore_server_version_stamp(&data_dir, Some("v1.37.2+k3s1"));
        fs::write(&marker, "v1.39\n").unwrap();
        let err = check_datastore_compatible(&data_dir, &marker, "v1.37.2+k3s1").unwrap_err();
        assert!(err.contains("invalid k3s version \"v1.39\""), "{err}");
        fs::remove_file(&marker).unwrap();

        // An empty stamp fails closed instead of falling back to the node record.
        fs::write(server_version_stamp(&data_dir), "").unwrap();
        let err = check_datastore_compatible(&data_dir, &marker, "v1.37.2+k3s1").unwrap_err();
        assert!(err.contains("is empty"), "{err}");

        // A datastore created during this start is stamped as soon as k3s creates
        // server/db/, so a backup after the first shutdown carries the version even if
        // the node record is stale.
        let new_data = dir.join("new/data");
        let new_marker = new_data.join(".katamaran-version");
        let mut pending = true;
        stamp_new_datastore(&new_data, "v1.38.0+k3s1", &mut pending);
        assert!(pending);
        fs::create_dir_all(new_data.join("server/db")).unwrap();
        fs::copy(db_dir.join("state.db"), new_data.join("server/db/state.db")).unwrap();
        stamp_new_datastore(&new_data, "v1.38.0+k3s1", &mut pending);
        assert!(!pending);
        let err = check_datastore_compatible(&new_data, &new_marker, "v1.37.2+k3s1").unwrap_err();
        assert!(err.contains("v1.38.0+k3s1"), "{err}");

        // Without server/db/, nothing is stamped.
        let fresh = dir.join("fresh");
        write_server_version_stamp(&fresh, "v1.37.2+k3s1").unwrap();
        assert!(!fresh.join("server").exists());
        let _ = fs::remove_dir_all(&dir);
    }

    /// Run against a real k3s data directory (for example one copied from
    /// `rancher/k3s:v1.37.2-k3s1` or a pre-tracking katamaran guest):
    /// `KAT_REAL_K3S_DATA=<data dir> KAT_REAL_K3S_VERSION=v1.37.2+k3s1 cargo test -- --ignored real_k3s`
    /// Verified against a `rancher/k3s:v1.37.2-k3s1` datastore whose node record
    /// lived only in an uncheckpointed `state.db-wal` (`k8s\0` protobuf envelope).
    #[test]
    #[ignore = "needs KAT_REAL_K3S_DATA and KAT_REAL_K3S_VERSION"]
    fn detects_version_from_real_k3s_datastore() {
        let data_dir = PathBuf::from(std::env::var("KAT_REAL_K3S_DATA").unwrap());
        let expected = std::env::var("KAT_REAL_K3S_VERSION").unwrap();
        let before = snapshot_datastore(&data_dir).unwrap();
        let detected = with_test_probe(|probe| {
            detect_datastore_version_from_state_db_with(&data_dir, probe, run_test_cmd)
        });
        assert_eq!(detected.as_deref(), Some(expected.as_str()));
        assert_eq!(snapshot_datastore(&data_dir).unwrap(), before);
    }

    #[test]
    fn build_k3s_server_args_appends_extra_args_and_rejects_invalid() {
        let base = build_k3s_server_args(&[]).unwrap();
        assert_eq!(base[0], "server");
        assert!(base.iter().any(|a| a == "--https-listen-port=6443"));
        assert!(base.iter().any(|a| a == &format!("--data-dir={DATA_DIR}")));

        let extra = vec!["--disable=traefik".to_string(), "--debug".to_string()];
        let with_extra = build_k3s_server_args(&extra).unwrap();
        assert_eq!(with_extra.len(), base.len() + 2);
        assert_eq!(&with_extra[base.len()..], extra.as_slice());

        // Every flag the guest sets itself is rejected in k8s_args (full
        // coverage lives in kat-proto's validate_k3s_extra_args tests).
        for flag in &base[1..] {
            let name = flag.split_once('=').map_or(flag.as_str(), |(n, _)| n);
            if matches!(name, "--kubelet-arg" | "--tls-san" | "--kube-proxy-arg") {
                continue;
            }
            let arg = format!("{name}=x");
            assert!(build_k3s_server_args(&[arg]).is_err(), "{name}");
        }
        assert!(build_k3s_server_args(&[format!("--kubelet-arg=root-dir={KUBELET_DIR}")]).is_err());
        assert!(build_k3s_server_args(&["--token=x".to_string()]).is_err());
    }

    #[test]
    fn sync_k3s_config_enforces_0600_and_preserves_unmanaged_default_config() {
        use std::ffi::OsStr;

        // Managed config lives at a Katamaran-owned path separate from k3s's default.
        assert_ne!(K3S_CONFIG_PATH, "/etc/rancher/k3s/config.yaml");

        let base = temp_dir("k3s-config");
        let managed = base.join("var/lib/katamaran/k3s/config.yaml");
        let default_cfg = base.join("etc/rancher/k3s/config.yaml");
        fs::create_dir_all(default_cfg.parent().unwrap()).unwrap();
        fs::write(&default_cfg, "disable:\n  - servicelb\n").unwrap();

        // Removing when managed file is absent is a no-op and never touches default_cfg.
        sync_k3s_config(&managed, None).unwrap();
        assert!(!managed.exists());
        assert_eq!(
            fs::read_to_string(&default_cfg).unwrap(),
            "disable:\n  - servicelb\n"
        );

        // Replacing an existing 0644 destination forces mode 0600 (unlike write_atomic,
        // which preserves existing permissions for other callers).
        let preserved = base.join("preserved.txt");
        fs::write(&preserved, "v1\n").unwrap();
        fs::set_permissions(&preserved, fs::Permissions::from_mode(0o644)).unwrap();
        write_atomic(&preserved, b"v2\n").unwrap();
        assert_eq!(
            fs::symlink_metadata(&preserved)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o644
        );

        fs::create_dir_all(managed.parent().unwrap()).unwrap();
        fs::write(&managed, "old: true\n").unwrap();
        fs::set_permissions(&managed, fs::Permissions::from_mode(0o644)).unwrap();
        sync_k3s_config(&managed, Some("token: secret\n")).unwrap();
        assert_eq!(fs::read_to_string(&managed).unwrap(), "token: secret\n");
        assert_eq!(
            fs::symlink_metadata(&managed).unwrap().permissions().mode() & 0o777,
            0o600
        );

        // Passing None (absence or reset) removes only the managed copy.
        sync_k3s_config(&managed, None).unwrap();
        assert!(!managed.exists());
        assert_eq!(
            fs::read_to_string(&default_cfg).unwrap(),
            "disable:\n  - servicelb\n"
        );

        // Non-NotFound removal failures (e.g. a directory/mountpoint at the managed path) are reported.
        fs::create_dir(&managed).unwrap();
        assert!(sync_k3s_config(&managed, None).is_err());
        fs::remove_dir(&managed).unwrap();

        // K3S_CONFIG_FILE is set only when host YAML is supplied.
        let mut with_cfg = Command::new(BIN);
        configure_k3s_config_env(&mut with_cfg, Some(&managed));
        assert!(with_cfg.get_envs().any(|(k, v)| {
            k == OsStr::new("K3S_CONFIG_FILE") && v == Some(managed.as_os_str())
        }));

        let mut without_cfg = Command::new(BIN);
        configure_k3s_config_env(&mut without_cfg, None);
        assert!(
            without_cfg
                .get_envs()
                .any(|(k, v)| k == OsStr::new("K3S_CONFIG_FILE") && v.is_none())
        );

        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn running_settings_compare_normalized_json_and_json_round_trips() {
        let yaml_a = "# original comment\ndisable:\n  - traefik\nkubelet-arg: [max-pods=200]\n";
        let yaml_b = "# edited comment\ndisable: [traefik]\nkubelet-arg:\n  - max-pods=200\n";
        let yaml_c = "disable: [servicelb]\n";

        let json_a = normalize_k3s_config_yaml(yaml_a).unwrap();
        let json_b = normalize_k3s_config_yaml(yaml_b).unwrap();
        let json_c = normalize_k3s_config_yaml(yaml_c).unwrap();
        assert_eq!(json_a, json_b);
        assert_ne!(json_a, json_c);

        // Verify pure-std JSON emitter produces valid JSON matching serde_json.
        let parsed: serde_json::Value = serde_json::from_str(&json_a).unwrap();
        assert_eq!(parsed["disable"], serde_json::json!(["traefik"]));
        assert_eq!(parsed["kubelet-arg"], serde_json::json!(["max-pods=200"]));
        let special = normalize_k3s_config_yaml("node-label: ['a\"b\\c=caf\u{e9}']\n").unwrap();
        let parsed_special: serde_json::Value = serde_json::from_str(&special).unwrap();
        assert_eq!(
            parsed_special["node-label"],
            serde_json::json!(["a\"b\\c=caf\u{e9}"])
        );

        let running = (vec!["--debug".to_string()], Some(json_a));
        // Comment/formatting-only YAML edit matches.
        check_running_settings_match(&running, &["--debug".to_string()], Some(&json_b)).unwrap();
        // Changed YAML, removed YAML, or changed extra_args are refused.
        assert!(
            check_running_settings_match(&running, &["--debug".to_string()], Some(&json_c))
                .is_err()
        );
        assert!(check_running_settings_match(&running, &["--debug".to_string()], None).is_err());
        assert!(check_running_settings_match(&running, &[], Some(&json_b)).is_err());
    }

    #[test]
    fn parse_ctr_image_refs_trims_and_skips_blank_lines() {
        let out = "\n  docker.io/library/alpine:3.24  \n\nimport-2026-11-26@sha256:abcd\n  \n";
        assert_eq!(
            parse_ctr_image_refs(out),
            vec![
                "docker.io/library/alpine:3.24".to_string(),
                "import-2026-11-26@sha256:abcd".to_string(),
            ]
        );
        assert!(parse_ctr_image_refs(" \n\t\n").is_empty());
    }

    #[test]
    fn drain_capped_checked_detects_exact_and_overflowing_streams() {
        let exact = drain_capped_checked(io::Cursor::new(b"abcd"), 4);
        assert_eq!(exact.text, "abcd");
        assert!(!exact.truncated);

        let over = drain_capped_checked(io::Cursor::new(b"abcde"), 4);
        assert_eq!(over.text, "abcd");
        assert!(over.truncated);
    }

    #[test]
    fn run_with_heartbeat_emits_periodic_progress_during_slow_work() {
        let mut beats = Vec::new();
        let val = run_with_heartbeat(
            Duration::from_millis(20),
            "removing cluster data",
            &mut |m| beats.push(m.to_string()),
            || {
                thread::sleep(Duration::from_millis(75));
                Ok(42usize)
            },
        )
        .unwrap();
        assert_eq!(val, 42);
        assert!(
            beats.len() >= 2 && beats.iter().all(|m| m == "removing cluster data"),
            "expected periodic heartbeats, got: {beats:?}"
        );
    }

    #[test]
    fn plan_image_rm_batches_chunks_refs() {
        let empty: Vec<String> = Vec::new();
        assert!(plan_image_rm_batches(&empty, 64).is_empty());

        let one = vec!["img:0".to_string()];
        assert_eq!(plan_image_rm_batches(&one, 64), vec![&one[..]]);

        let three: Vec<String> = (0..3).map(|i| format!("img:{i}")).collect();
        assert_eq!(plan_image_rm_batches(&three, 64), vec![&three[..]]);

        let many: Vec<String> = (0..137).map(|i| format!("img:{i}")).collect();
        let plan = plan_image_rm_batches(&many, 64);
        assert_eq!(plan.len(), 3);
        assert_eq!(plan[0].len(), 64);
        assert_eq!(plan[1].len(), 64);
        assert_eq!(plan[2].len(), 9);
    }

    #[test]
    fn clear_k8s_images_with_handles_empty_batches_errors_and_shutdown() {
        let ok_out = |stdout: &str| ReapedOutput {
            code: 0,
            stdout: stdout.into(),
            stderr: String::new(),
            stdout_truncated: false,
        };
        let lease_id = "katamaran-k8s-reset-gc-test";

        // Empty namespace (e.g. retry after an earlier GC timeout): still verifies tasks/containers
        // and runs the synchronous lease GC barrier without deleting any existing leases.
        let mut calls: Vec<Vec<String>> = Vec::new();
        let mut timeouts = Vec::new();
        let mut progress = Vec::new();
        clear_k8s_images_with(
            || false,
            lease_id,
            |args, timeout, on_wait| {
                calls.push(args.iter().map(|s| (*s).to_string()).collect());
                timeouts.push(timeout);
                if let Some(cb) = on_wait {
                    cb();
                }
                Ok(ok_out(""))
            },
            |m| progress.push(m.to_string()),
        )
        .unwrap();
        assert_eq!(
            calls,
            vec![
                vec!["tasks", "ls", "-q"],
                vec!["containers", "ls", "-q"],
                vec!["images", "ls", "-q"],
                vec![
                    "leases",
                    "create",
                    "--id",
                    lease_id,
                    "--expires",
                    IMAGE_GC_LEASE_EXPIRES,
                ],
                vec!["leases", "delete", "--sync", "--", lease_id],
            ]
        );
        assert_eq!(
            timeouts,
            vec![
                IMAGE_LS_TIMEOUT,
                IMAGE_LS_TIMEOUT,
                IMAGE_LS_TIMEOUT,
                IMAGE_LS_TIMEOUT,
                IMAGE_RM_SYNC_TIMEOUT,
            ]
        );
        assert!(
            progress
                .iter()
                .filter(|m| m.as_str() == "waiting for containerd garbage collection")
                .count()
                >= 2,
            "expected GC progress + heartbeat, got: {progress:?}"
        );

        // Multiple images: deletes after `--` in batches, then runs synchronous lease GC barrier.
        calls.clear();
        timeouts.clear();
        clear_k8s_images_with(
            || false,
            lease_id,
            |args, timeout, _| {
                calls.push(args.iter().map(|s| (*s).to_string()).collect());
                timeouts.push(timeout);
                if args == ["images", "ls", "-q"] {
                    Ok(ok_out(
                        "docker.io/library/busybox:latest\n-weird:tag\nrancher/mirrored-pause:3.6\n",
                    ))
                } else {
                    Ok(ok_out(""))
                }
            },
            |_| {},
        )
        .unwrap();
        assert_eq!(
            calls,
            vec![
                vec!["tasks", "ls", "-q"],
                vec!["containers", "ls", "-q"],
                vec!["images", "ls", "-q"],
                vec![
                    "images",
                    "rm",
                    "--",
                    "docker.io/library/busybox:latest",
                    "-weird:tag",
                    "rancher/mirrored-pause:3.6",
                ],
                vec![
                    "leases",
                    "create",
                    "--id",
                    lease_id,
                    "--expires",
                    IMAGE_GC_LEASE_EXPIRES,
                ],
                vec!["leases", "delete", "--sync", "--", lease_id],
            ]
        );
        assert_eq!(
            timeouts,
            vec![
                IMAGE_LS_TIMEOUT,
                IMAGE_LS_TIMEOUT,
                IMAGE_LS_TIMEOUT,
                IMAGE_RM_BATCH_TIMEOUT,
                IMAGE_LS_TIMEOUT,
                IMAGE_RM_SYNC_TIMEOUT,
            ]
        );

        // Non-zero exit from `images ls` surfaces stderr detail and never runs `images rm`.
        let err = clear_k8s_images_with(
            || false,
            lease_id,
            |args, _, _| {
                if args == ["tasks", "ls", "-q"] || args == ["containers", "ls", "-q"] {
                    Ok(ok_out(""))
                } else if args == ["images", "ls", "-q"] {
                    Ok(ReapedOutput {
                        code: 1,
                        stdout: String::new(),
                        stderr: "dial unix failed\n".into(),
                        stdout_truncated: false,
                    })
                } else {
                    panic!("unexpected command after images ls error: {args:?}");
                }
            },
            |_| {},
        )
        .unwrap_err();
        assert!(err.contains("ctr images ls (k8s.io) failed: dial unix failed"));

        // Non-zero exit from the first `images rm` batch stops immediately and never runs
        // subsequent batches or the synchronous GC barrier.
        let many_refs: String = (0..(IMAGE_RM_BATCH_SIZE + 2))
            .map(|i| format!("img:{i}\n"))
            .collect();
        let mut rm_calls = Vec::new();
        let err = clear_k8s_images_with(
            || false,
            lease_id,
            |args, _, _| {
                rm_calls.push(args.iter().map(|s| (*s).to_string()).collect::<Vec<_>>());
                if args == ["tasks", "ls", "-q"] || args == ["containers", "ls", "-q"] {
                    Ok(ok_out(""))
                } else if args == ["images", "ls", "-q"] {
                    Ok(ok_out(&many_refs))
                } else if args.starts_with(&["images", "rm"]) && rm_calls.len() == 4 {
                    Ok(ReapedOutput {
                        code: 1,
                        stdout: String::new(),
                        stderr: "permission denied\n".into(),
                        stdout_truncated: false,
                    })
                } else {
                    panic!("deletion must stop after first failed batch; saw {args:?}");
                }
            },
            |_| {},
        )
        .unwrap_err();
        assert!(err.contains("ctr images rm (k8s.io) failed: permission denied"));
        assert_eq!(rm_calls.len(), 4);

        // Shutdown flag aborts before running `ctr`.
        let err = clear_k8s_images_with(
            || true,
            lease_id,
            |_, _, _| panic!("should not run"),
            |_| {},
        )
        .unwrap_err();
        assert!(err.contains("VM is shutting down"));
    }

    #[test]
    fn clear_k8s_images_with_rejects_lingering_workloads_and_truncated_inventory() {
        let ok_out = |stdout: &str| ReapedOutput {
            code: 0,
            stdout: stdout.into(),
            stderr: String::new(),
            stdout_truncated: false,
        };
        let lease_id = "katamaran-k8s-reset-gc-test";

        // Lingering task stops cleanup before listing or deleting images.
        let err = clear_k8s_images_with(
            || false,
            lease_id,
            |args, _, _| {
                if args == ["tasks", "ls", "-q"] {
                    Ok(ok_out("task-stuck-1\n"))
                } else {
                    panic!("should not continue when tasks remain: {args:?}");
                }
            },
            |_| {},
        )
        .unwrap_err();
        assert!(err.contains("task(s) still remain"), "{err}");
        assert!(err.contains("task-stuck-1"), "{err}");

        // Lingering container stops cleanup before listing or deleting images.
        let err = clear_k8s_images_with(
            || false,
            lease_id,
            |args, _, _| {
                if args == ["tasks", "ls", "-q"] {
                    Ok(ok_out(""))
                } else if args == ["containers", "ls", "-q"] {
                    Ok(ok_out("ctr-stuck-1\nctr-stuck-2\n"))
                } else {
                    panic!("should not continue when containers remain: {args:?}");
                }
            },
            |_| {},
        )
        .unwrap_err();
        assert!(err.contains("2 container(s) still remain"), "{err}");
        assert!(err.contains("ctr-stuck-1"), "{err}");

        // Failed container listing stops cleanup before listing or deleting images.
        let err = clear_k8s_images_with(
            || false,
            lease_id,
            |args, _, _| {
                if args == ["tasks", "ls", "-q"] {
                    Ok(ok_out(""))
                } else if args == ["containers", "ls", "-q"] {
                    Ok(ReapedOutput {
                        code: 1,
                        stdout: String::new(),
                        stderr: "containerd unavailable\n".into(),
                        stdout_truncated: false,
                    })
                } else {
                    panic!("should not continue when containers ls fails: {args:?}");
                }
            },
            |_| {},
        )
        .unwrap_err();
        assert!(
            err.contains("ctr containers ls (k8s.io) failed: containerd unavailable"),
            "{err}"
        );

        // Truncated image inventory fails cleanly and never invokes `images rm` or GC lease.
        let err = clear_k8s_images_with(
            || false,
            lease_id,
            |args, _, _| {
                if args == ["tasks", "ls", "-q"] || args == ["containers", "ls", "-q"] {
                    Ok(ok_out(""))
                } else if args == ["images", "ls", "-q"] {
                    Ok(ReapedOutput {
                        code: 0,
                        stdout: "docker.io/library/alpine:3.24\ndocker.io/library/trunc".into(),
                        stderr: String::new(),
                        stdout_truncated: true,
                    })
                } else {
                    panic!("truncated inventory must never reach deletion: {args:?}");
                }
            },
            |_| {},
        )
        .unwrap_err();
        assert!(err.contains("truncated inventory"), "{err}");

        // Synchronous GC barrier failure surfaces error.
        let err = clear_k8s_images_with(
            || false,
            lease_id,
            |args, _, _| {
                if args == ["leases", "delete", "--sync", "--", lease_id] {
                    Ok(ReapedOutput {
                        code: 1,
                        stdout: String::new(),
                        stderr: "gc timed out\n".into(),
                        stdout_truncated: false,
                    })
                } else {
                    Ok(ok_out(""))
                }
            },
            |_| {},
        )
        .unwrap_err();
        assert!(
            err.contains("ctr leases delete --sync (k8s.io) failed: gc timed out"),
            "{err}"
        );
    }
}
