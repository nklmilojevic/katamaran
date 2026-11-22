//! Optional single-node Kubernetes (k3s) inside the guest, served to the
//! host on [`kat_proto::K8S_PORT`] (see [`kat_proto::k8s`]).
//!
//! Nothing runs until Kubernetes is enabled: when disabled, the only footprint
//! is the idle vsock listener. When started, `k3s` shares Katamaran's existing
//! `containerd` (`/run/containerd/containerd.sock`), uses `host-gw` flannel
//! and `nftables` kube-proxy, and mirrors locally built Docker images from the
//! `moby` namespace into `k8s.io`.

use std::collections::{BTreeSet, HashMap};
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{IpAddr, Shutdown, TcpStream};
use std::os::unix::fs::{PermissionsExt, symlink};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver};
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
const KUBECONFIG: &str = "/var/lib/katamaran/k3s/kubeconfig.yaml";
const LOG_FILE: &str = "/var/log/k3s.log";
pub const API_ADDR: &str = "127.0.0.1:6443";

const READY_TIMEOUT: Duration = Duration::from_secs(180);
const STOP_TIMEOUT: Duration = Duration::from_secs(15);

/// Internal k3s/Kubernetes control-plane ports that must not be auto-forwarded
/// to the Mac's `127.0.0.1` by `ports::watch` (`6443` has its own dedicated
/// vsock relay on `K8S_API_PORT`).
const INTERNAL_PORTS: [u16; 6] = [6443, 6444, 10250, 10256, 10257, 10259];

static SERVICE: OnceLock<Arc<K8s>> = OnceLock::new();
static ACTIVE: AtomicBool = AtomicBool::new(false);
static FORWARDED: Mutex<BTreeSet<u16>> = Mutex::new(BTreeSet::new());

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
    thread::Builder::new().name("k8s".into()).spawn(move || {
        loop {
            match sys::accept(&listener) {
                Ok(fd) => {
                    let s = s.clone();
                    thread::spawn(move || s.serve(UnixStream::from(fd)));
                }
                Err(e) => {
                    log(&format!("accept on vsock {K8S_PORT}: {e}"));
                    return;
                }
            }
        }
    })?;
    Ok(svc)
}

/// Stop k3s cleanly during VM shutdown.
pub fn stop_for_shutdown() {
    if let Some(svc) = SERVICE.get() {
        let _ = svc.stop_inner(None);
    }
}

/// Ensure `/etc/containerd/config.toml` has CRI plugins enabled. Returns
/// `true` if the file was modified (meaning an already-running `containerd`
/// must be restarted to load CRI).
pub fn ensure_containerd_cri() -> bool {
    let path = "/etc/containerd/config.toml";
    let Ok(text) = fs::read_to_string(path) else {
        return false;
    };
    if !text.contains("disabled_plugins") {
        return false;
    }
    let filtered: String = text
        .lines()
        .filter(|l| !l.trim_start().starts_with("disabled_plugins"))
        .map(|l| format!("{l}\n"))
        .collect();
    if let Err(e) = fs::write(path, filtered) {
        log(&format!("update {path}: {e}"));
        return false;
    }
    true
}

impl K8s {
    fn serve(&self, mut conn: UnixStream) {
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
        let _guard = self.op_lock.lock().unwrap();
        let _ = k8s::write(
            &mut *conn,
            &K8sMsg::Progress(format!("installing k3s {version}")),
        );
        fs::create_dir_all(BIN_DIR).map_err(|e| format!("create {BIN_DIR}: {e}"))?;
        let tmp = format!("{BIN}.tmp");
        let mut f = File::create(&tmp).map_err(|e| format!("create {tmp}: {e}"))?;
        loop {
            match k8s::read(&mut *conn).map_err(|e| format!("read binary stream: {e}"))? {
                Some(K8sMsg::Data(chunk)) => {
                    f.write_all(&chunk)
                        .map_err(|e| format!("write {tmp}: {e}"))?;
                }
                Some(K8sMsg::DataEnd) => break,
                Some(other) => {
                    let _ = fs::remove_file(&tmp);
                    return Err(format!("expected Data/DataEnd, got {other:?}"));
                }
                None => {
                    let _ = fs::remove_file(&tmp);
                    return Err("unexpected EOF while receiving k3s binary".into());
                }
            }
        }
        f.sync_all().map_err(|e| format!("sync {tmp}: {e}"))?;
        drop(f);
        fs::set_permissions(&tmp, fs::Permissions::from_mode(0o755))
            .map_err(|e| format!("chmod {tmp}: {e}"))?;
        fs::rename(&tmp, BIN).map_err(|e| format!("install {BIN}: {e}"))?;
        fs::write(VERSION_FILE, format!("{version}\n"))
            .map_err(|e| format!("write {VERSION_FILE}: {e}"))?;
        log(&format!("installed k3s {version}"));
        Ok(())
    }

    fn start_inner(&self, conn: &mut UnixStream) -> Result<K8sInfo, String> {
        let _guard = self.op_lock.lock().unwrap();
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
        if ensure_containerd_cri() {
            let _ = k8s::write(
                &mut *conn,
                &K8sMsg::Progress("enabling CRI in containerd".into()),
            );
            self.docker.restart_containerd();
        }
        wait_containerd()?;

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
            "--kube-proxy-arg=proxy-mode=nftables",
        ])
        .env("PATH", PATH)
        .stdin(Stdio::null())
        .stdout(log_file.try_clone().map_err(|e| e.to_string())?)
        .stderr(log_file);

        let (pid, status_rx) = self
            .reaper
            .spawn(&mut cmd)
            .map_err(|e| format!("spawn k3s: {e}"))?;
        let exited = Arc::new(Mutex::new(None));
        let (exit_tx, exit_rx) = mpsc::channel();
        let sync_stop = Arc::new(AtomicBool::new(false));

        ACTIVE.store(true, Ordering::SeqCst);
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
            });
            st.generation
        };

        // Watch process exit asynchronously so crashes are reported to the host.
        {
            let svc_ref = SERVICE.get().cloned();
            let exited2 = exited.clone();
            let sync_stop2 = sync_stop.clone();
            thread::Builder::new()
                .name("k3s-watch".into())
                .spawn(move || {
                    if let Ok(status) = status_rx.recv() {
                        *exited2.lock().unwrap() = Some(status);
                        sync_stop2.store(true, Ordering::SeqCst);
                        let _ = exit_tx.send(());
                        if let Some(svc) = svc_ref {
                            // Serialize crash cleanup with start_inner/stop_inner so a
                            // replacement cluster cannot launch while this watcher is
                            // tearing down workloads or restoring sysctls. Safe against
                            // deadlock because exited2 and exit_tx were signaled above.
                            let _op = svc.op_lock.lock().unwrap();
                            let crashed = {
                                let mut st = svc.state.lock().unwrap();
                                // Ignore stale watchers or startup exits already handled
                                // by start_inner/stop_inner.
                                if st.generation != my_gen || st.phase != "running" {
                                    return;
                                }
                                let msg = format!("k3s {}", describe_status(status));
                                st.phase = "failed".into();
                                st.api_ready = false;
                                st.error = msg.clone();
                                st.running = None;
                                msg
                            };
                            stop_k8s_workloads(&svc.reaper);
                            ACTIVE.store(false, Ordering::SeqCst);
                            FORWARDED.lock().unwrap().clear();
                            restore_bridge_netfilter();
                            svc.notifier.failed("k8s", crashed);
                        }
                    }
                })
                .ok();
        }

        let _ = k8s::write(
            &mut *conn,
            &K8sMsg::Progress("waiting for Kubernetes API server".into()),
        );
        let deadline = Instant::now() + READY_TIMEOUT;
        loop {
            if let Some(status) = *exited.lock().unwrap() {
                let err = format!(
                    "k3s exited during startup ({}); see {LOG_FILE}",
                    describe_status(status)
                );
                stop_k8s_workloads(&self.reaper);
                let mut st = self.state.lock().unwrap();
                if st.generation == my_gen {
                    st.phase = "failed".into();
                    st.error = err.clone();
                    st.running = None;
                }
                ACTIVE.store(false, Ordering::SeqCst);
                FORWARDED.lock().unwrap().clear();
                restore_bridge_netfilter();
                return Err(err);
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
                    terminate_running(&r);
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

        {
            let mut st = self.state.lock().unwrap();
            st.phase = "running".into();
            st.api_ready = true;
            st.error.clear();
        }
        self.notifier.send(&Msg::ServiceReady {
            name: "k8s".into(),
            at_ns: sys::boottime_ns(),
        });

        // Start background Docker -> k8s.io image mirror and Service port watcher.
        spawn_image_sync(self.reaper.clone(), sync_stop.clone());
        spawn_service_port_watcher(self.reaper.clone(), sync_stop);
        log("k3s is ready");
        Ok(self.info())
    }

    fn stop_inner(&self, conn: Option<&mut UnixStream>) -> Result<(), String> {
        let _guard = self.op_lock.lock().unwrap();
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
            terminate_running(&r);
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
        self.stop_inner(Some(conn))?;
        let _guard = self.op_lock.lock().unwrap();
        let _ = k8s::write(conn, &K8sMsg::Progress("removing cluster data".into()));
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
        if k8s::write(&mut conn, &K8sMsg::Done).is_err() {
            return;
        }
        let Ok(mut up_read) = upstream.try_clone() else {
            return;
        };
        let Ok(mut conn_write) = conn.try_clone() else {
            return;
        };
        let down = thread::spawn(move || {
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
        });
        let mut up_write = upstream;
        while let Ok(Some(msg)) = k8s::read(&mut conn) {
            match msg {
                K8sMsg::Data(d) => {
                    if up_write.write_all(&d).is_err() {
                        break;
                    }
                }
                K8sMsg::DataEnd => {
                    let _ = up_write.shutdown(Shutdown::Write);
                    break;
                }
                _ => break,
            }
        }
        let _ = down.join();
    }
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

    // Enable IP forwarding and bridge netfilter while k3s is active.
    for path in [
        "/proc/sys/net/ipv4/ip_forward",
        "/proc/sys/net/bridge/bridge-nf-call-iptables",
        "/proc/sys/net/bridge/bridge-nf-call-ip6tables",
    ] {
        let _ = fs::write(path, "1\n");
    }
    Ok(())
}

fn restore_bridge_netfilter() {
    for path in [
        "/proc/sys/net/bridge/bridge-nf-call-iptables",
        "/proc/sys/net/bridge/bridge-nf-call-ip6tables",
    ] {
        let _ = fs::write(path, "0\n");
    }
}

fn terminate_running(r: &Running) {
    r.sync_stop.store(true, Ordering::SeqCst);
    if r.exited.lock().unwrap().is_none() {
        unsafe { libc::kill(r.pid, libc::SIGTERM) };
        if r.exit_rx.recv_timeout(STOP_TIMEOUT).is_err() {
            log("k3s did not stop in time, killing it");
            unsafe { libc::kill(r.pid, libc::SIGKILL) };
            let _ = r.exit_rx.recv_timeout(Duration::from_secs(3));
        }
    }
}

/// Stop and remove Kubernetes tasks/containers in the shared containerd
/// `k8s.io` namespace and tear down CNI interfaces without touching Docker's
/// `moby` namespace.
fn stop_k8s_workloads(reaper: &Reaper) {
    let mut ls = Command::new("/usr/bin/ctr");
    ls.args(["-a", CONTAINERD_SOCKET, "-n", "k8s.io", "tasks", "ls", "-q"]);
    if let Ok((0, out)) = run_reaped(reaper, &mut ls, Duration::from_secs(10)) {
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
    if let Ok((0, out)) = run_reaped(reaper, &mut cls, Duration::from_secs(10)) {
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
        if UnixStream::connect(CONTAINERD_SOCKET).is_ok() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(50));
    }
    Err(format!("containerd socket {CONTAINERD_SOCKET} not ready"))
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

/// Periodically read NodePort and LoadBalancer ports from Kubernetes services
/// so `ports::watch` forwards them on `127.0.0.1:<port>` on macOS (kube-proxy
/// in nftables mode and ServiceLB use DNAT rules rather than listening sockets).
fn spawn_service_port_watcher(reaper: Reaper, stop: Arc<AtomicBool>) {
    let _ = thread::Builder::new()
        .name("k8s-ports".into())
        .spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                if let Some(ports) = query_service_ports(&reaper) {
                    *FORWARDED.lock().unwrap() = ports;
                }
                for _ in 0..8 {
                    if stop.load(Ordering::Relaxed) {
                        FORWARDED.lock().unwrap().clear();
                        return;
                    }
                    thread::sleep(Duration::from_millis(250));
                }
            }
            FORWARDED.lock().unwrap().clear();
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
    let (0, out) = run_reaped(reaper, &mut cmd, Duration::from_secs(5)).ok()? else {
        return None;
    };
    Some(parse_service_ports(&out))
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
    let moby = list_ctr_images(reaper, "moby")?;
    let k8s = list_ctr_images(reaper, "k8s.io").unwrap_or_default();
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
    let (code, text) = run_reaped(reaper, &mut cmd, Duration::from_secs(15))
        .map_err(|e| format!("ctr images ls ({ns}): {e}"))?;
    if code != 0 {
        return Err(format!("ctr images ls ({ns}) failed: {}", text.trim()));
    }
    let mut map = HashMap::new();
    for line in text.lines().skip(1) {
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

    let exp_err_t = thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = File::from(exp_err_r).take(64 << 10).read_to_end(&mut buf);
        String::from_utf8_lossy(&buf).trim().to_string()
    });
    let imp_err_t = thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = File::from(imp_err_r).take(64 << 10).read_to_end(&mut buf);
        String::from_utf8_lossy(&buf).trim().to_string()
    });

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
