//! Optional single-node Kubernetes (k3s) inside the guest, served to the
//! host on [`kat_proto::K8S_PORT`] (see [`kat_proto::k8s`]).
//!
//! Nothing runs until Kubernetes is enabled: when disabled, the only footprint
//! is the idle vsock listener. When started, `k3s` shares Katamaran's existing
//! `containerd` (`/run/containerd/containerd.sock`), uses `host-gw` flannel
//! and `nftables` kube-proxy, and mirrors locally built Docker images from the
//! `moby` namespace into `k8s.io`.

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeSet, HashMap};
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

/// Parse `(major, minor)` from a k3s release tag such as `v1.37.2+k3s1`.
fn parse_k3s_minor(version: &str) -> Option<(u32, u32)> {
    let rest = version.trim().strip_prefix('v')?;
    let (kube, build) = rest.split_once("+k3s")?;
    if build.is_empty() {
        return None;
    }
    let core = kube.split_once('-').map_or(kube, |(c, _)| c);
    let mut parts = core.split('.');
    let major: u32 = parts.next()?.parse().ok()?;
    let minor: u32 = parts.next()?.parse().ok()?;
    let _patch: u32 = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((major, minor))
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
        return Ok(());
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
/// and `path_resolves_rootfs_nft_for_oldest_supported_k3s`).
const MIN_K8S_MINOR: u32 = 31;
const SQLITE3_BIN: &str = "/usr/bin/sqlite3";
/// Scratch root for WAL/journal inspection copies of `state.db`. `/run` is a
/// tmpfs, so a copy (which contains cluster secrets) never reaches the
/// persistent data disk and cannot outlive the VM or survive `kat k8s reset`.
const STATE_DB_SCRATCH_ROOT: &str = "/run/katamaran";
/// Upper bound on the bytes copied into `STATE_DB_SCRATCH_ROOT` (database plus
/// sidecars); larger unmarked datastores fall back to `kat k8s mark-version`.
const STATE_DB_MAX_SCRATCH_BYTES: u64 = 256 << 20;
const STATE_DB_MAX_NODE_ROWS: usize = 64;
const STATE_DB_MAX_BLOB_BYTES: usize = 4 << 20;
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
}

fn state_db_probe() -> StateDbProbe<'static> {
    StateDbProbe {
        sqlite3_bin: Path::new(SQLITE3_BIN),
        scratch_root: Path::new(STATE_DB_SCRATCH_ROOT),
    }
}

/// Strictly parse a k3s release tag (`v<major>.<minor>.<patch>[-<pre>]+k3s<n>`)
/// with no surrounding whitespace, returning `(major, minor)`.
fn parse_strict_k3s_tag(v: &str) -> Option<(u32, u32)> {
    let rest = v.strip_prefix('v')?;
    let (kube, build) = rest.split_once("+k3s")?;
    if build.is_empty() || !build.bytes().all(|b| b.is_ascii_digit()) {
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
            if n.is_empty() || !n.bytes().all(|b| b.is_ascii_digit()) {
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
        Ok(()) => Ok(()),
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
            "{} exceeds the {STATE_DB_MAX_SCRATCH_BYTES}-byte inspection limit",
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

fn extract_kubelet_version_from_node_json(buf: &[u8]) -> Option<String> {
    let v: serde_json::Value = serde_json::from_slice(buf).ok()?;
    if let Some(kind) = v.get("kind").and_then(serde_json::Value::as_str)
        && !kind.is_empty()
        && kind != "Node"
    {
        return None;
    }
    if let Some(api_version) = v.get("apiVersion").and_then(serde_json::Value::as_str)
        && !api_version.is_empty()
        && api_version != "v1"
    {
        return None;
    }
    v.get("status")?
        .get("nodeInfo")?
        .get("kubeletVersion")?
        .as_str()
        .filter(|s| !s.is_empty())
        .map(str::to_string)
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
/// stored in kine's `value` column (either `k8s\0` + `runtime.Unknown` protobuf
/// envelope or plain JSON).
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
        if !content_encoding.is_empty() {
            return None;
        }
        if !api_version.is_empty() && api_version != "v1" {
            return None;
        }
        if !kind.is_empty() && kind != "Node" {
            return None;
        }
        let raw = raw.filter(|r| !r.is_empty())?;
        return match content_type {
            "" | "application/vnd.kubernetes.protobuf" => {
                extract_kubelet_version_from_node_proto(raw)
            }
            "application/json" => extract_kubelet_version_from_node_json(raw),
            _ => None,
        };
    }
    let trimmed = blob.trim_ascii_start();
    if trimmed.starts_with(b"{") {
        return extract_kubelet_version_from_node_json(trimmed);
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
    let journal_len = match inspect_regular_companion(&journal_path) {
        Ok(v) => v,
        Err(e) => {
            log(&format!("skip state.db detection: {e}"));
            return None;
        }
    };

    let (_scratch, sqlite_target) = if wal_len.unwrap_or(0) == 0 && journal_len.unwrap_or(0) == 0 {
        if !db_path.is_absolute() {
            return None;
        }
        (
            None,
            format!("file:{}?immutable=1", encode_sqlite_uri_path(&db_path)),
        )
    } else {
        let total = wal_len.unwrap_or(0)
            + journal_len.unwrap_or(0)
            + fs::symlink_metadata(&db_path)
                .map(|m| m.len())
                .unwrap_or(u64::MAX);
        if total > STATE_DB_MAX_SCRATCH_BYTES {
            log(&format!(
                "skip state.db detection: {total} bytes exceeds the \
                 {STATE_DB_MAX_SCRATCH_BYTES}-byte inspection limit"
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
        let guard = ScratchDir(scratch_path);
        let scratch_db = guard.0.join("state.db");
        let mut budget = STATE_DB_MAX_SCRATCH_BYTES;
        if let Err(e) = copy_regular_file_bounded(&db_path, &scratch_db, &mut budget) {
            log(&format!("copy state.db for inspection: {e}"));
            return None;
        }
        if wal_len.is_some()
            && let Err(e) =
                copy_regular_file_bounded(&wal_path, &guard.0.join("state.db-wal"), &mut budget)
        {
            log(&format!("copy state.db-wal for inspection: {e}"));
            return None;
        }
        if journal_len.is_some()
            && let Err(e) = copy_regular_file_bounded(
                &journal_path,
                &guard.0.join("state.db-journal"),
                &mut budget,
            )
        {
            log(&format!("copy state.db-journal for inspection: {e}"));
            return None;
        }
        let uri = format!("file:{}", encode_sqlite_uri_path(&scratch_db));
        (Some(guard), uri)
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
    if let Some(detected) = detect_datastore_version_from_state_db_with(data_dir, probe, run_cmd)
        && let (Some(supplied_mm), Some(detected_mm)) = (
            parse_strict_k3s_tag(version),
            parse_strict_k3s_tag(&detected),
        )
        && supplied_mm < detected_mm
    {
        return Err(format!(
            "refusing to mark datastore as {version}: server/db/state.db already records newer k3s {detected}"
        ));
    }
    if let MarkerFile::Present(stamped) = read_marker_file(&server_version_stamp(data_dir))?
        && let (Some(supplied_mm), Some(stamped_mm)) = (
            parse_strict_k3s_tag(version),
            parse_strict_k3s_tag(&stamped),
        )
        && supplied_mm < stamped_mm
    {
        return Err(format!(
            "refusing to mark datastore as {version}: server/db/{SERVER_VERSION_STAMP} records newer k3s {stamped}"
        ));
    }
    write_atomic_new(data_version_file, format!("{version}\n").as_bytes())
}

/// Return the k3s version associated with an existing datastore under `data_dir`.
///
/// Uses `data_version_file` inside `data_dir` when present, and never falls
/// back to `VERSION_FILE` (the installed binary's version). When
/// `data_version_file` is absent (`NotFound`), inspects `server/db/state.db`
/// first uses `server/SERVER_VERSION_STAMP`, and only when that is absent too
/// inspects `server/db/state.db` read-only for the node's recorded
/// `kubeletVersion`, accepting it only when it is one of `PRE_TRACKING_K3S_VERSIONS`, so clusters created by older
/// katamaran releases upgrade seamlessly. Any other detected version is only a
/// lower bound and is rejected. Only recommends `kat k8s mark-version` when
/// `data_version_file` is absent (`NotFound`), never when an empty or
/// non-regular marker already exists.
fn effective_datastore_version_with(
    data_dir: &Path,
    data_version_file: &Path,
    probe: &StateDbProbe<'_>,
    run_cmd: impl FnMut(&mut Command) -> Result<(i32, String), String>,
) -> Result<Option<String>, String> {
    if !datastore_exists(data_dir)? {
        return Ok(None);
    }
    match read_marker_file(data_version_file)? {
        MarkerFile::Present(v) => newer_of_marker_and_stamp(data_dir, v).map(Some),
        MarkerFile::Missing => {
            let stamp = server_version_stamp(data_dir);
            match read_marker_file(&stamp)? {
                // Written before every tracking-era launch, so it is at least the
                // newest k3s that opened this `server/`.
                MarkerFile::Present(v) => return Ok(Some(v)),
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
                MarkerFile::Missing => {}
            }
            match detect_datastore_version_from_state_db_with(data_dir, probe, run_cmd) {
                Some(detected) if PRE_TRACKING_K3S_VERSIONS.contains(&detected.as_str()) => {
                    Ok(Some(detected))
                }
                Some(detected) => Err(format!(
                    "{} The node record in server/db/state.db reports k3s {detected}, \
                     which no pre-tracking katamaran release ran, so it is only a lower \
                     bound (a newer k3s server may have opened this datastore before its \
                     kubelet reported status) and cannot be used to verify compatibility.",
                    missing_datastore_version_error(data_dir, data_version_file)
                )),
                None => Err(missing_datastore_version_error(data_dir, data_version_file)),
            }
        }
        MarkerFile::Empty => Err(corrupt_datastore_version_error(
            data_version_file,
            "is empty",
        )),
        MarkerFile::NonRegular => Err(corrupt_datastore_version_error(
            data_version_file,
            "is not a regular file",
        )),
    }
}

/// Katamaran always writes `.katamaran-version` and the `server/db/` stamp
/// together, so they only disagree when one was restored without the other
/// (for example `server/db/` from a newer backup beside a stale outer marker).
/// Use whichever records the newer minor so the downgrade guard still holds.
fn newer_of_marker_and_stamp(data_dir: &Path, marker: String) -> Result<String, String> {
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
            match parse_k3s_minor(&marker) {
                Some(marker_mm) if marker_mm >= stamp_mm => Ok(marker),
                _ => Ok(stamp),
            }
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

/// Restore the `server/db/` stamp to `prev`, removing it if it did not exist.
fn restore_server_version_stamp(data_dir: &Path, prev: Option<&str>) {
    match prev {
        Some(v) => {
            if let Err(e) = write_server_version_stamp(data_dir, v) {
                log(&format!("restore server version stamp: {e}"));
            }
        }
        None => {
            let stamp = server_version_stamp(data_dir);
            if let Err(e) = fs::remove_file(&stamp)
                && e.kind() != io::ErrorKind::NotFound
            {
                log(&format!("remove {}: {e}", stamp.display()));
            }
        }
    }
}

fn restore_data_version(path: &Path, prev: Option<&str>) {
    match prev {
        Some(v) => {
            if let Err(e) = write_atomic(path, format!("{v}\n").as_bytes()) {
                log(&format!("restore {}: {e}", path.display()));
            }
        }
        None => {
            if let Err(e) = fs::remove_file(path)
                && e.kind() != io::ErrorKind::NotFound
            {
                log(&format!("remove {}: {e}", path.display()));
            }
        }
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
            K8sMsg::MarkDatastoreVersion { version } => {
                self.mark_datastore_version(&version).map(|()| K8sMsg::Done)
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
        let installed_version = read_trimmed_file(Path::new(VERSION_FILE))?.ok_or_else(|| {
            "k3s installation is incomplete (missing version marker); restart Kubernetes to reinstall"
                .to_string()
        })?;
        check_datastore_compatible_with(
            Path::new(DATA_DIR),
            Path::new(DATA_VERSION_FILE),
            &installed_version,
            &state_db_probe(),
            |cmd| self.run_sqlite_cmd(cmd),
        )?;
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

        let prev_data_version = match read_marker_file(Path::new(DATA_VERSION_FILE))? {
            MarkerFile::Present(v) => Some(v),
            MarkerFile::Missing | MarkerFile::Empty | MarkerFile::NonRegular => None,
        };
        let wrote_data_version = prev_data_version.as_deref() != Some(installed_version.as_str());
        let prev_stamp = match read_marker_file(&server_version_stamp(Path::new(DATA_DIR)))? {
            MarkerFile::Present(v) => Some(v),
            MarkerFile::Missing | MarkerFile::Empty | MarkerFile::NonRegular => None,
        };
        let wrote_stamp = server_db_dir_exists(Path::new(DATA_DIR))?
            && prev_stamp.as_deref() != Some(installed_version.as_str());
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
        if wrote_data_version {
            write_atomic(
                Path::new(DATA_VERSION_FILE),
                format!("{installed_version}\n").as_bytes(),
            )?;
        }
        if wrote_stamp
            && let Err(e) = write_server_version_stamp(Path::new(DATA_DIR), &installed_version)
        {
            restore_versions();
            return Err(e);
        }
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
            let _ = terminate_running(r);
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
    fn extracts_kubelet_version_from_k8s_protobuf_and_json_nodes() {
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

        // Plain JSON Node.
        let json = br#"{"apiVersion":"v1","kind":"Node","status":{"nodeInfo":{"kubeletVersion":"v1.32.5+k3s1"}}}"#;
        assert_eq!(
            extract_kubelet_version_from_node_blob(json).as_deref(),
            Some("v1.32.5+k3s1")
        );

        // JSON wrapped inside k8s\0 runtime.Unknown envelope.
        let mut unknown_json = b"k8s\0".to_vec();
        push_proto_bytes(&mut unknown_json, 2, json);
        push_proto_bytes(&mut unknown_json, 4, b"application/json");
        assert_eq!(
            extract_kubelet_version_from_node_blob(&unknown_json).as_deref(),
            Some("v1.32.5+k3s1")
        );

        // Non-empty contentEncoding, wrong kind, truncated protobuf, or missing field fail closed.
        let mut compressed = proto.clone();
        push_proto_bytes(&mut compressed, 3, b"gzip");
        assert_eq!(extract_kubelet_version_from_node_blob(&compressed), None);

        let wrong_kind =
            br#"{"apiVersion":"v1","kind":"Pod","status":{"nodeInfo":{"kubeletVersion":"v1.37.2+k3s1"}}}"#;
        assert_eq!(extract_kubelet_version_from_node_blob(wrong_kind), None);

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

        // 4. JSON-encoded Node row is also detected accurately; a non-pre-tracking version
        // (v1.36.4+k3s1) is a lower bound only, even for its own release.
        let json_data = dir.join("json-case/data");
        let json_marker = json_data.join(".katamaran-version");
        fs::create_dir_all(json_data.join("server/db")).unwrap();
        let json_blob = hex_upper(
            br#"{"apiVersion":"v1","kind":"Node","status":{"nodeInfo":{"kubeletVersion":"v1.36.4+k3s1"}}}"#,
        );
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
            },
            run_test_cmd,
        )
        .unwrap_err();
        assert!(err.contains("no version marker"), "{err}");

        let _ = fs::remove_dir_all(&dir);
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
        let err = mark_datastore_version_at(&data_dir, &marker, "v1.37.2+k3s1").unwrap_err();
        assert!(err.contains("records newer k3s v1.38.0+k3s1"), "{err}");
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

        // An empty stamp fails closed instead of falling back to the node record.
        fs::write(server_version_stamp(&data_dir), "").unwrap();
        let err = check_datastore_compatible(&data_dir, &marker, "v1.37.2+k3s1").unwrap_err();
        assert!(err.contains("is empty"), "{err}");

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
}
