//! `katamaran daemon`: owns the VM lifecycle in the background and serves
//! the control API (see [`crate::api`]) on `<home>/run/daemon.sock`.
//!
//! One daemon per home: it holds `<home>/run/katamaran.lock` for its whole
//! life, the same lock `katamaran run` takes. The VM runs on a supervisor
//! thread through [`run::run_vm`]; its events update the status that API
//! clients read. SIGTERM (launchd's bootout) and SIGINT stop the VM cleanly
//! before the daemon exits.

use std::collections::{BTreeSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, bail};
use kat_proto::{GuestStats, PROTOCOL_VERSION, PortProto};
use serde_json::Value;

use crate::api::{
    self, Call, ConfigView, DaemonInfo, DockerInfo, DockerState, K8sStatus, LogChunk, LogSource,
    LogsParams, MountInfo, Port, Request, Response, RpcError, Sample, StatsResult, Status,
    Versions, VmInfo,
};
use crate::config::{Config, Host};
use crate::power::{self, PowerEvent};
use crate::run::{self, Control, Event, RunArgs};
use crate::state::{Transition, Trigger, VmState};

/// How often the daemon samples the guest's and the VM process's usage.
pub const STATS_INTERVAL: Duration = Duration::from_secs(2);
/// Five minutes of samples.
const STATS_HISTORY: usize = 150;
const STOP_TIMEOUT: Duration = Duration::from_secs(60);
/// Rotate `daemon.log` at startup beyond this size.
const LOG_ROTATE_BYTES: u64 = 4 << 20;

#[derive(clap::Args)]
pub struct DaemonArgs {
    /// Start the VM as soon as the daemon is up (the LaunchAgent passes this).
    #[arg(long)]
    pub start: bool,
    /// Log to stderr instead of <home>/logs/daemon.log.
    #[arg(long)]
    pub no_log_file: bool,
}

/// Write a timestamped line to the daemon log.
macro_rules! dlog {
    ($($arg:tt)*) => {
        eprintln!("{} {}", $crate::daemon::timestamp(), format_args!($($arg)*))
    };
}

pub fn daemon(args: DaemonArgs) -> anyhow::Result<()> {
    let home = crate::state_dir()?;
    let run_dir = home.join("run");
    let logs = home.join("logs");
    for d in [&run_dir, &logs] {
        fs::create_dir_all(d).with_context(|| format!("create {}", d.display()))?;
    }
    fs::set_permissions(&run_dir, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("chmod 0700 {}", run_dir.display()))?;
    let lock = run::lock(&run_dir.join("katamaran.lock"))?;
    if !args.no_log_file {
        redirect_output(&logs.join("daemon.log"))?;
    }
    // A panic anywhere must not leave a daemon that answers but cannot
    // act; aborting lets launchd's KeepAlive restart it.
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_hook(info);
        std::process::abort();
    }));
    // Before any thread is spawned.
    let signals = run::block_and_watch_signals()?;

    let sock_path = run_dir.join(api::SOCKET_NAME);
    // Holding the lock means any socket left there is stale.
    let _ = fs::remove_file(&sock_path);
    let listener =
        UnixListener::bind(&sock_path).with_context(|| format!("bind {}", sock_path.display()))?;
    fs::set_permissions(&sock_path, fs::Permissions::from_mode(0o600))?;

    let d = Daemon::new(home.clone());
    dlog!(
        "katamaran {} daemon (pid {}) serving {}",
        env!("CARGO_PKG_VERSION"),
        std::process::id(),
        sock_path.display()
    );

    {
        let d = d.clone();
        thread::spawn(move || {
            for sig in signals {
                if let Some(event) = run::simulated_power(sig) {
                    d.power(event);
                    continue;
                }
                dlog!("signal {sig}; stopping");
                d.exit(sock_path.clone());
            }
        });
    }
    {
        let d = d.clone();
        if let Err(e) = power::watch(move |event| d.power(event)) {
            dlog!("no sleep/wake notifications: {e:#}");
        }
    }
    {
        let d = d.clone();
        thread::spawn(move || d.sample_loop());
    }
    if args.start
        && let Err(e) = d.start()
    {
        dlog!("start: {e}");
    }

    for conn in listener.incoming() {
        match conn {
            Ok(conn) => {
                let d = d.clone();
                thread::spawn(move || d.serve(conn));
            }
            Err(e) => dlog!("accept: {e}"),
        }
    }
    drop(lock);
    Ok(())
}

pub struct Daemon {
    home: PathBuf,
    inner: Mutex<Inner>,
    k8s: Mutex<K8sRuntime>,
    /// Serializes Kubernetes start/stop/reset/toggle operations so rapid
    /// preference changes cannot race or overwrite newer user choices.
    k8s_op: Mutex<()>,
    changed: Condvar,
    started: SystemTime,
    launchd_label: Option<String>,
}

struct K8sRuntime {
    state: String,
    installed: bool,
    version: String,
    api_proxy: Option<crate::k8s::ApiProxy>,
    dns: Option<crate::k8s::DnsServer>,
    kubeconfig: Option<PathBuf>,
    error: Option<String>,
    /// Set while `k8s_reset` temporarily boots a stopped VM so Docker
    /// readiness does not auto-start k3s before the reset runs.
    suppress_autostart: bool,
}

impl Default for K8sRuntime {
    fn default() -> Self {
        K8sRuntime {
            state: "stopped".into(),
            installed: false,
            version: String::new(),
            api_proxy: None,
            dns: None,
            kubeconfig: None,
            error: None,
            suppress_autostart: false,
        }
    }
}

struct Inner {
    state: VmState,
    since: SystemTime,
    error: Option<String>,
    control: Option<mpsc::Sender<Control>>,
    run: Option<Run>,
    samples: VecDeque<Sample>,
    /// Counts runs, so a late event from a finished run is ignored.
    generation: u64,
    exiting: bool,
}

/// What the daemon knows about the current VM run.
struct Run {
    applied: Config,
    requested: Instant,
    kernel: Option<String>,
    vm_pid: Option<u32>,
    boot: Option<Duration>,
    booted_at: Option<Instant>,
    docker: DockerState,
    docker_ready: Option<Duration>,
    mount_path: Option<PathBuf>,
    mounted: bool,
    mount_error: Option<String>,
    problem: Option<String>,
    ports: BTreeSet<(PortProto, u16)>,
    prev_guest: Option<GuestStats>,
    prev_host: Option<(Instant, u64)>,
    host_cpu_percent: f64,
    host_footprint_mib: u64,
}

impl Run {
    fn new(applied: Config, mount_path: Option<PathBuf>) -> Run {
        Run {
            applied,
            requested: Instant::now(),
            kernel: None,
            vm_pid: None,
            boot: None,
            booted_at: None,
            docker: DockerState::Starting,
            docker_ready: None,
            mount_path,
            mounted: false,
            mount_error: None,
            problem: None,
            ports: BTreeSet::new(),
            prev_guest: None,
            prev_host: None,
            host_cpu_percent: 0.0,
            host_footprint_mib: 0,
        }
    }
}

impl Daemon {
    pub fn new(home: PathBuf) -> Arc<Daemon> {
        Arc::new(Daemon {
            home,
            inner: Mutex::new(Inner {
                state: VmState::Stopped,
                since: SystemTime::now(),
                error: None,
                control: None,
                run: None,
                samples: VecDeque::new(),
                generation: 0,
                exiting: false,
            }),
            k8s: Mutex::new(K8sRuntime::default()),
            k8s_op: Mutex::new(()),
            changed: Condvar::new(),
            started: SystemTime::now(),
            launchd_label: std::env::var("XPC_SERVICE_NAME")
                .ok()
                .filter(|l| l.starts_with(crate::setup::LABEL_PREFIX)),
        })
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn set_state(&self, g: &mut Inner, to: VmState) {
        if g.state != to {
            dlog!("state {} -> {}", g.state.as_str(), to.as_str());
            g.state = to;
            g.since = SystemTime::now();
            self.changed.notify_all();
        }
    }

    pub fn start(self: &Arc<Self>) -> Result<(), String> {
        let mut g = self.lock();
        if g.exiting {
            return Err("the daemon is shutting down".into());
        }
        match g.state.on(Trigger::Start) {
            Transition::Unchanged => return Ok(()),
            Transition::Invalid(m) => return Err(m),
            Transition::To(_) => {}
        }
        let cfg = Config::load(&self.home).map_err(|e| format!("{e:#}"))?;
        let args = run_args(&cfg).map_err(|e| format!("{e:#}"))?;
        let mount_path = if args.no_mount {
            None
        } else {
            crate::nfs::mount_point(args.mount.as_deref())
        };
        g.generation += 1;
        let generation = g.generation;
        g.error = None;
        g.run = Some(Run::new(cfg.clone(), mount_path));
        g.samples.clear();
        let (tx, rx) = mpsc::channel();
        g.control = Some(tx);
        self.set_state(&mut g, VmState::Starting);
        dlog!(
            "starting the VM ({} CPUs, {} MiB)",
            cfg.cpus,
            cfg.memory_mib
        );
        let d = self.clone();
        thread::Builder::new()
            .name("vm-supervisor".into())
            .spawn(move || {
                let result = run::run_vm(&args, rx, &mut |e| d.on_event(generation, e));
                d.on_exit(generation, result);
            })
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Ask the VM to stop; with `wait`, until it is gone.
    pub fn stop(&self, wait: bool) -> Result<(), String> {
        let mut g = self.lock();
        match g.state.on(Trigger::Stop) {
            Transition::Invalid(m) => return Err(m),
            Transition::Unchanged => {}
            Transition::To(to) => {
                dlog!("stopping the VM");
                if let Some(c) = &g.control {
                    let _ = c.send(Control::Shutdown);
                }
                self.set_state(&mut g, to);
            }
        }
        if wait {
            let deadline = Instant::now() + STOP_TIMEOUT;
            while g.state.is_active() {
                let left = deadline.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    return Err("the VM did not stop in time".into());
                }
                g = self
                    .changed
                    .wait_timeout(g, left)
                    .unwrap_or_else(|e| e.into_inner())
                    .0;
            }
        }
        Ok(())
    }

    pub fn restart(self: &Arc<Self>) -> Result<(), String> {
        self.stop(true)?;
        self.start()
    }

    /// The Mac is going to sleep or woke up: tell the running VM and wait
    /// until it handled it, so a pause happens before the Mac sleeps.
    fn power(&self, event: PowerEvent) {
        let control = self.lock().control.clone();
        dlog!("power: {event:?}");
        if let Some(c) = control {
            run::forward_power(&c, event);
        }
    }

    /// Stop the VM, remove the socket and exit the process.
    fn exit(&self, sock: PathBuf) -> ! {
        self.lock().exiting = true;
        if let Err(e) = self.stop(true) {
            dlog!("stop: {e}");
        }
        let _ = fs::remove_file(&sock);
        dlog!("daemon exiting");
        std::process::exit(0)
    }

    fn on_event(self: &Arc<Self>, generation: u64, e: Event) {
        let mut auto_start_k8s = false;
        let mut k8s_failed: Option<String> = None;
        let mut k8s_reset_runtime = false;
        {
            let mut g = self.lock();
            if g.generation != generation {
                return;
            }
            let Inner { run, samples, .. } = &mut *g;
            let Some(r) = run.as_mut() else { return };
            match e {
                Event::Booted {
                    kernel_release,
                    vm_pid,
                    took,
                } => {
                    r.kernel = Some(kernel_release);
                    r.vm_pid = Some(vm_pid);
                    r.boot = Some(took);
                    r.booted_at = Some(Instant::now());
                }
                Event::ServiceReady(name) => {
                    if name == "docker" {
                        r.docker = DockerState::Ready;
                        let took = r.requested.elapsed();
                        r.docker_ready = Some(took);
                        dlog!("docker ready {:.0} ms after the start request", ms(took));
                        if let Transition::To(to) = g.state.on(Trigger::Ready) {
                            self.set_state(&mut g, to);
                        }
                        if Config::load(&self.home).is_ok_and(|c| c.kubernetes) {
                            auto_start_k8s = true;
                        }
                    }
                }
                Event::ServiceFailed(name, error) => {
                    if name == "k8s" {
                        k8s_failed = Some(error);
                    } else {
                        r.problem = Some(format!("{name}: {error}"));
                    }
                    if name == "docker" {
                        r.docker = DockerState::Failed;
                        // The VM stays up; "running" with Docker failed.
                        if let Transition::To(to) = g.state.on(Trigger::Ready) {
                            self.set_state(&mut g, to);
                        }
                    }
                }
                Event::Mounted(path) => {
                    r.mounted = true;
                    r.mount_path = Some(path);
                }
                Event::MountFailed(error) => {
                    r.problem = Some(format!("files not mounted: {error}"));
                    r.mount_error = Some(error);
                }
                Event::PortAdd(proto, port) => {
                    r.ports.insert((proto, port));
                }
                Event::PortRemove(proto, port) => {
                    r.ports.remove(&(proto, port));
                }
                Event::Restarting { why, delay } => {
                    dlog!("the VM died ({why}); restarting in {:.0} ms", ms(delay));
                    let mut next = Run::new(r.applied.clone(), r.mount_path.clone());
                    next.problem = Some(format!("the VM was restarted: {why}"));
                    *r = next;
                    k8s_reset_runtime = true;
                    if let Transition::To(to) = g.state.on(Trigger::Restarting) {
                        self.set_state(&mut g, to);
                    }
                }
                Event::Stats(st) => {
                    let cpu = match r.prev_guest {
                        Some(p) => cpu_percent(&p, &st),
                        None => 0.0,
                    };
                    r.prev_guest = Some(st);
                    samples.push_back(Sample {
                        t_ms: unix_ms(SystemTime::now()),
                        guest_cpu_percent: cpu,
                        guest_mem_used_mib: st.mem_total_kib.saturating_sub(st.mem_available_kib)
                            >> 10,
                        guest_mem_total_mib: st.mem_total_kib >> 10,
                        host_cpu_percent: r.host_cpu_percent,
                        host_footprint_mib: r.host_footprint_mib,
                    });
                    while samples.len() > STATS_HISTORY {
                        samples.pop_front();
                    }
                }
            }
            self.changed.notify_all();
        }
        if k8s_reset_runtime {
            let mut k = self.k8s.lock().unwrap_or_else(|e| e.into_inner());
            k.api_proxy = None;
            k.dns = None;
            k.state = "stopped".into();
        }
        if let Some(err) = k8s_failed {
            let mut k = self.k8s.lock().unwrap_or_else(|e| e.into_inner());
            k.api_proxy = None;
            k.dns = None;
            k.state = "failed".into();
            k.error = Some(err);
        }
        if auto_start_k8s
            && !self
                .k8s
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .suppress_autostart
        {
            let d = self.clone();
            thread::Builder::new()
                .name("k8s-autostart".into())
                .spawn(move || {
                    if let Err(e) = d.k8s_start_inner(false, true, &mut |m| dlog!("k8s: {m}")) {
                        dlog!("k8s autostart failed: {e}");
                    }
                })
                .ok();
        }
    }

    fn on_exit(&self, generation: u64, result: anyhow::Result<()>) {
        {
            let mut g = self.lock();
            if g.generation != generation {
                return;
            }
            let ok = result.is_ok();
            match &result {
                Ok(()) => dlog!("VM stopped"),
                Err(e) => {
                    dlog!("VM failed: {e:#}");
                    g.error = Some(format!("{e:#}"));
                }
            }
            g.control = None;
            g.run = None;
            if let Transition::To(to) = g.state.on(Trigger::Exited { ok }) {
                self.set_state(&mut g, to);
            }
            self.changed.notify_all();
        }
        let mut k = self.k8s.lock().unwrap_or_else(|e| e.into_inner());
        k.api_proxy = None;
        k.dns = None;
        k.state = "stopped".into();
    }

    /// Every [`STATS_INTERVAL`]: ask the guest for its usage and read the VM
    /// process's usage on the Mac.
    fn sample_loop(&self) {
        loop {
            thread::sleep(STATS_INTERVAL);
            let pid = {
                let g = self.lock();
                if let Some(c) = &g.control {
                    let _ = c.send(Control::Stats);
                }
                g.run.as_ref().and_then(|r| r.vm_pid)
            };
            let Some(pid) = pid else { continue };
            let Some((cpu_ns, footprint)) = proc_usage(pid) else {
                continue;
            };
            let now = Instant::now();
            let mut g = self.lock();
            if let Some(r) = g.run.as_mut().filter(|r| r.vm_pid == Some(pid)) {
                if let Some((t, prev)) = r.prev_host {
                    let wall = now.duration_since(t).as_nanos() as f64;
                    if wall > 0.0 {
                        r.host_cpu_percent = cpu_ns.saturating_sub(prev) as f64 / wall * 100.0;
                    }
                }
                r.prev_host = Some((now, cpu_ns));
                r.host_footprint_mib = footprint >> 20;
            }
        }
    }

    pub fn status(&self) -> Status {
        let g = self.lock();
        let r = g.run.as_ref();
        let saved = Config::load(&self.home).unwrap_or_default();
        let error = g
            .error
            .clone()
            .or_else(|| r.and_then(|r| r.problem.clone()));
        let mount_enabled = r.map_or(saved.mount, |r| r.applied.mount);
        Status {
            state: g.state,
            error,
            since_ms: unix_ms(g.since),
            uptime_s: r
                .and_then(|r| r.booted_at)
                .map(|t| (t.elapsed().as_secs_f64() * 10.0).round() / 10.0),
            daemon: DaemonInfo {
                pid: std::process::id(),
                home: self.home.clone(),
                started_ms: unix_ms(self.started),
                launchd_label: self.launchd_label.clone(),
            },
            versions: Versions {
                katamaran: env!("CARGO_PKG_VERSION").into(),
                protocol: PROTOCOL_VERSION,
                kernel: r.and_then(|r| r.kernel.clone()),
            },
            vm: r.map(|r| VmInfo {
                cpus: r.applied.cpus,
                memory_mib: r.applied.memory_mib,
                pid: r.vm_pid,
                boot_ms: r.boot.map(ms),
                docker_ready_ms: r.docker_ready.map(ms),
            }),
            usage: g.samples.back().copied(),
            docker: DockerInfo {
                state: r.map_or(DockerState::Stopped, |r| r.docker),
                socket: self.home.join("run/docker.sock"),
            },
            k8s: self.k8s_status(saved.kubernetes),
            mount: MountInfo {
                enabled: mount_enabled,
                path: match r {
                    Some(r) => r.mount_path.clone(),
                    None if saved.mount => crate::nfs::mount_point(saved.mount_path.as_deref()),
                    None => None,
                },
                mounted: r.is_some_and(|r| r.mounted),
                error: r.and_then(|r| r.mount_error.clone()),
            },
            ports: r
                .map(|r| {
                    r.ports
                        .iter()
                        .map(|(p, port)| Port {
                            proto: p.as_str().into(),
                            port: *port,
                        })
                        .collect()
                })
                .unwrap_or_default(),
            config: saved,
        }
    }

    fn config_view(&self) -> anyhow::Result<ConfigView> {
        let config = Config::load(&self.home)?;
        let g = self.lock();
        Ok(ConfigView {
            restart_required: g
                .run
                .as_ref()
                .is_some_and(|r| r.applied.needs_restart_for(&config)),
            path: Config::path(&self.home),
            config,
        })
    }

    fn set_config(
        self: &Arc<Self>,
        values: &std::collections::BTreeMap<String, String>,
    ) -> anyhow::Result<ConfigView> {
        let mut cfg = Config::load(&self.home)?;
        let host = Host::current();
        let before = cfg.launch_at_login;
        let before_k8s = cfg.kubernetes;
        for (k, v) in values {
            cfg.set(k, v, &host)?;
        }
        cfg.save(&self.home)?;
        dlog!("config saved: {values:?}");
        if cfg.launch_at_login != before {
            crate::setup::set_run_at_load(cfg.launch_at_login)?;
        }
        if cfg.kubernetes != before_k8s && self.lock().state == VmState::Running {
            let d = self.clone();
            let enable = cfg.kubernetes;
            thread::Builder::new()
                .name("k8s-toggle".into())
                .spawn(move || {
                    if enable {
                        if let Err(e) = d.k8s_start_inner(false, true, &mut |m| dlog!("k8s: {m}")) {
                            dlog!("k8s start failed: {e}");
                        }
                    } else if let Err(e) = d.k8s_stop_inner(false, true) {
                        dlog!("k8s stop failed: {e}");
                    }
                })
                .ok();
        }
        self.config_view()
    }

    fn stats(&self) -> StatsResult {
        StatsResult {
            interval_ms: STATS_INTERVAL.as_millis() as u64,
            samples: self.lock().samples.iter().copied().collect(),
        }
    }

    pub fn log_path(&self, source: LogSource) -> PathBuf {
        match source {
            LogSource::Daemon => self.home.join("logs/daemon.log"),
            LogSource::Console => self.home.join("logs/console.log"),
            LogSource::Vm => self.home.join("run/vm.log"),
            LogSource::Gvproxy => self.home.join("logs/gvproxy.log"),
        }
    }

    /// Serve one client connection until it closes.
    fn serve(self: Arc<Self>, conn: UnixStream) {
        let Ok(mut out) = conn.try_clone() else {
            return;
        };
        let mut reader = BufReader::new(conn);
        let mut line = Vec::new();
        loop {
            line.clear();
            match (&mut reader)
                .take(api::MAX_LINE as u64 + 1)
                .read_until(b'\n', &mut line)
            {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
            if line.len() > api::MAX_LINE {
                let _ = send(
                    &mut out,
                    &Response::err(
                        Value::Null,
                        RpcError::new(api::INVALID_REQUEST, "line too long"),
                    ),
                );
                return;
            }
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            let (id, call) = match serde_json::from_slice::<Request>(&line) {
                Err(e) => (
                    Value::Null,
                    Err(RpcError::new(api::PARSE_ERROR, e.to_string())),
                ),
                Ok(req) => (req.id.clone().unwrap_or(Value::Null), Call::parse(&req)),
            };
            let call = match call {
                Ok(c) => c,
                Err(e) => {
                    if send(&mut out, &Response::err(id, e)).is_err() {
                        return;
                    }
                    continue;
                }
            };
            if let Call::Logs(p) = &call
                && p.follow
            {
                self.follow_logs(id, p, &mut out);
                return;
            }
            if let Call::K8sStart = &call {
                let mut progress = |m: &str| {
                    let _ = send(
                        &mut out.try_clone().unwrap(),
                        &api::Notification {
                            jsonrpc: api::VERSION.into(),
                            method: "k8s.progress".into(),
                            params: serde_json::json!({ "message": m }),
                        },
                    );
                };
                let resp = match self.k8s_start(&mut progress).and_then(|s| {
                    serde_json::to_value(s)
                        .map_err(|e| RpcError::new(api::INTERNAL_ERROR, e.to_string()))
                }) {
                    Ok(v) => Response::ok(id, v),
                    Err(e) => Response::err(id, e),
                };
                if send(&mut out, &resp).is_err() {
                    return;
                }
                continue;
            }
            if let Call::MachineCreate(c) = &call {
                let mut progress = |m: &str| {
                    let _ = send(
                        &mut out.try_clone().unwrap(),
                        &api::Notification {
                            jsonrpc: api::VERSION.into(),
                            method: "machine.progress".into(),
                            params: serde_json::json!({ "message": m }),
                        },
                    );
                };
                let resp = match self.machine_create(c, &mut progress) {
                    Ok(v) => Response::ok(id, v),
                    Err(e) => Response::err(id, e),
                };
                if send(&mut out, &resp).is_err() {
                    return;
                }
                continue;
            }
            let shutdown = call == Call::Shutdown;
            let resp = match self.handle(&call) {
                Ok(v) => Response::ok(id, v),
                Err(e) => Response::err(id, e),
            };
            if send(&mut out, &resp).is_err() {
                return;
            }
            if shutdown {
                let sock = self.home.join("run").join(api::SOCKET_NAME);
                let d = self.clone();
                thread::spawn(move || d.exit(sock));
                return;
            }
        }
    }

    pub fn handle(self: &Arc<Self>, call: &Call) -> Result<Value, RpcError> {
        let failed = |m: String| RpcError::new(api::FAILED, m);
        let value = |v: Result<Value, serde_json::Error>| {
            v.map_err(|e| RpcError::new(api::INTERNAL_ERROR, e.to_string()))
        };
        match call {
            Call::Status => value(serde_json::to_value(self.status())),
            Call::Start => {
                self.start().map_err(failed)?;
                value(serde_json::to_value(self.status()))
            }
            Call::Stop { wait } => {
                self.stop(*wait).map_err(failed)?;
                value(serde_json::to_value(self.status()))
            }
            Call::Restart => {
                self.restart().map_err(failed)?;
                value(serde_json::to_value(self.status()))
            }
            Call::Logs(p) => {
                let chunk =
                    read_log(&self.log_path(p.source), p).map_err(|e| failed(format!("{e:#}")))?;
                value(serde_json::to_value(chunk))
            }
            Call::ConfigGet => {
                let v = self.config_view().map_err(|e| failed(format!("{e:#}")))?;
                value(serde_json::to_value(v))
            }
            Call::ConfigSet(values) => {
                let v = self
                    .set_config(values)
                    .map_err(|e| RpcError::new(api::INVALID_PARAMS, format!("{e:#}")))?;
                value(serde_json::to_value(v))
            }
            Call::Stats => value(serde_json::to_value(self.stats())),
            Call::Version => value(serde_json::to_value(self.status().versions)),
            Call::Shutdown => Ok(Value::Null),
            Call::MachineList => {
                let files = self.files_path();
                let list =
                    crate::machines::list(&self.home).map_err(|e| failed(format!("{e:#}")))?;
                let list: Vec<_> = list
                    .into_iter()
                    .map(|m| crate::machines::Machine::from_info(m, files.as_deref()))
                    .collect();
                value(serde_json::to_value(list))
            }
            Call::MachineInfo { name } => {
                let m = crate::machines::info(&self.home, name)
                    .map_err(|e| failed(format!("{e:#}")))?;
                let files = self.files_path();
                value(serde_json::to_value(crate::machines::Machine::from_info(
                    m,
                    files.as_deref(),
                )))
            }
            Call::MachineCreate(c) => self.machine_create(c, &mut |_| {}),
            Call::MachineStart { name }
            | Call::MachineStop { name }
            | Call::MachineDelete { name }
            | Call::MachineDefault { name } => {
                use kat_proto::machine::MachineMsg;
                let name = name.clone();
                let req = match call {
                    Call::MachineStart { .. } => MachineMsg::Start { name },
                    Call::MachineStop { .. } => MachineMsg::Stop { name },
                    Call::MachineDelete { .. } => MachineMsg::Delete { name },
                    _ => MachineMsg::SetDefault { name },
                };
                crate::machines::simple(&self.home, &req).map_err(|e| failed(format!("{e:#}")))?;
                Ok(Value::Null)
            }
            Call::K8sStart => {
                let st = self.k8s_start(&mut |m| dlog!("k8s: {m}"))?;
                value(serde_json::to_value(st))
            }
            Call::K8sStop => {
                self.k8s_stop(true)?;
                Ok(Value::Null)
            }
            Call::K8sReset => {
                self.k8s_reset()?;
                Ok(Value::Null)
            }
            Call::K8sKubeconfig => {
                let yaml = self.k8s_kubeconfig()?;
                Ok(serde_json::json!({ "kubeconfig": yaml }))
            }
            Call::K8sSyncImages => {
                let mut noop = |_: &str| {};
                crate::k8s::request(&self.home, &kat_proto::k8s::K8sMsg::SyncImages, &mut noop)
                    .map_err(|e| failed(format!("{e:#}")))?;
                Ok(Value::Null)
            }
        }
    }

    fn k8s_status(&self, enabled: bool) -> K8sStatus {
        let k = self.k8s.lock().unwrap_or_else(|e| e.into_inner());
        let dns_port = k.dns.as_ref().map(|d| d.port);
        K8sStatus {
            enabled,
            state: k.state.clone(),
            installed: k.installed,
            version: k.version.clone(),
            api_port: k.api_proxy.as_ref().map(|p| p.port),
            context: crate::k8s::context_name().unwrap_or_default(),
            kubeconfig: k.kubeconfig.clone(),
            dns_domain: crate::k8s::DNS_DOMAIN.into(),
            dns_port,
            dns_installed: crate::k8s::dns_resolver_installed(dns_port),
            error: k.error.clone(),
        }
    }

    fn persist_k8s_enabled(&self, enabled: bool) -> anyhow::Result<()> {
        let mut cfg = Config::load(&self.home)?;
        if cfg.kubernetes != enabled {
            cfg.kubernetes = enabled;
            cfg.save(&self.home)?;
        }
        Ok(())
    }

    fn wait_for_docker_ready(self: &Arc<Self>) -> Result<(), RpcError> {
        let failed = |m: String| RpcError::new(api::FAILED, m);
        {
            let g = self.lock();
            if g.state == VmState::Stopped || g.state == VmState::Error {
                drop(g);
                self.start().map_err(failed)?;
            }
        }
        let deadline = Instant::now() + Duration::from_secs(180);
        let mut g = self.lock();
        loop {
            if g.run
                .as_ref()
                .is_some_and(|r| r.docker == DockerState::Ready)
            {
                return Ok(());
            }
            if g.state == VmState::Error || g.state == VmState::Stopped {
                return Err(failed(g.error.clone().unwrap_or_else(|| {
                    "the VM stopped before Kubernetes could start".into()
                })));
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(failed("timed out waiting for the VM to start".into()));
            }
            g = self
                .changed
                .wait_timeout(g, left)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
    }

    fn k8s_start(self: &Arc<Self>, progress: &mut dyn FnMut(&str)) -> Result<K8sStatus, RpcError> {
        self.k8s_start_inner(true, false, progress)
    }

    fn k8s_start_inner(
        self: &Arc<Self>,
        persist_enabled: bool,
        only_if_config_enabled: bool,
        progress: &mut dyn FnMut(&str),
    ) -> Result<K8sStatus, RpcError> {
        let _op = self.k8s_op.lock().unwrap_or_else(|e| e.into_inner());
        let failed = |m: String| RpcError::new(api::FAILED, m);
        if only_if_config_enabled && !Config::load(&self.home).is_ok_and(|c| c.kubernetes) {
            return Ok(self.k8s_status(false));
        }
        progress("ensuring VM is running");
        self.wait_for_docker_ready()?;
        if only_if_config_enabled && !Config::load(&self.home).is_ok_and(|c| c.kubernetes) {
            return Ok(self.k8s_status(false));
        }

        {
            let mut k = self.k8s.lock().unwrap_or_else(|e| e.into_inner());
            if k.state == "running" && k.api_proxy.is_some() {
                drop(k);
                return Ok(self.k8s_status(true));
            }
            k.state = "starting".into();
            k.error = None;
        }

        let mut guest_started = false;
        let res = (|| -> anyhow::Result<(kat_proto::k8s::K8sInfo, crate::k8s::ApiProxy, crate::k8s::DnsServer, PathBuf)> {
            crate::k8s::ensure_installed(&self.home, progress)?;
            let info = match crate::k8s::request(
                &self.home,
                &kat_proto::k8s::K8sMsg::Start,
                progress,
            )? {
                kat_proto::k8s::K8sMsg::StatusInfo(info) => info,
                other => anyhow::bail!("unexpected k8s start reply {other:?}"),
            };
            guest_started = true;
            let proxy = crate::k8s::ApiProxy::start(crate::k8s::api_socket_path(&self.home))
                .context("start Kubernetes API proxy")?;
            let dns = crate::k8s::DnsServer::start()
                .context("start Kubernetes DNS responder")?;
            let raw_kc = match crate::k8s::request(
                &self.home,
                &kat_proto::k8s::K8sMsg::Kubeconfig,
                progress,
            )? {
                kat_proto::k8s::K8sMsg::KubeconfigData(yaml) => yaml,
                other => anyhow::bail!("unexpected kubeconfig reply {other:?}"),
            };
            progress("updating ~/.kube/config");
            let kc_path = crate::k8s::write_and_merge_kubeconfig(&self.home, &raw_kc, proxy.port)?;
            Ok((info, proxy, dns, kc_path))
        })();

        match res {
            Ok((info, proxy, dns, kc_path)) => {
                if persist_enabled && let Err(e) = self.persist_k8s_enabled(true) {
                    dlog!("k8s config persist failed: {e:#}");
                    let mut noop = |_: &str| {};
                    let _ =
                        crate::k8s::request(&self.home, &kat_proto::k8s::K8sMsg::Stop, &mut noop);
                    let msg = format!("{e:#}");
                    let mut k = self.k8s.lock().unwrap_or_else(|e| e.into_inner());
                    k.state = "failed".into();
                    k.api_proxy = None;
                    k.dns = None;
                    k.error = Some(msg.clone());
                    return Err(failed(msg));
                }
                let mut k = self.k8s.lock().unwrap_or_else(|e| e.into_inner());
                k.state = "running".into();
                k.installed = info.installed;
                k.version = info.version;
                k.api_proxy = Some(proxy);
                k.dns = Some(dns);
                k.kubeconfig = Some(kc_path);
                k.error = None;
                drop(k);
                Ok(self.k8s_status(true))
            }
            Err(e) => {
                if guest_started {
                    let mut noop = |_: &str| {};
                    if let Err(se) =
                        crate::k8s::request(&self.home, &kat_proto::k8s::K8sMsg::Stop, &mut noop)
                    {
                        dlog!("k8s rollback stop failed: {se:#}");
                    }
                }
                let msg = format!("{e:#}");
                let mut k = self.k8s.lock().unwrap_or_else(|e| e.into_inner());
                k.state = "failed".into();
                k.api_proxy = None;
                k.dns = None;
                k.error = Some(msg.clone());
                Err(failed(msg))
            }
        }
    }

    fn k8s_stop(&self, disable_config: bool) -> Result<(), RpcError> {
        self.k8s_stop_inner(disable_config, false)
    }

    fn k8s_stop_inner(
        &self,
        disable_config: bool,
        only_if_config_disabled: bool,
    ) -> Result<(), RpcError> {
        let _op = self.k8s_op.lock().unwrap_or_else(|e| e.into_inner());
        let failed = |m: String| RpcError::new(api::FAILED, m);
        if only_if_config_disabled && Config::load(&self.home).is_ok_and(|c| c.kubernetes) {
            return Ok(());
        }
        let prev_state = {
            let mut k = self.k8s.lock().unwrap_or_else(|e| e.into_inner());
            let prev = k.state.clone();
            k.state = "stopping".into();
            prev
        };
        // Keep API/DNS relays and saved config intact until the guest confirms
        // k3s has actually stopped.
        if self.lock().state.is_active() && crate::k8s::socket_path(&self.home).exists() {
            let mut noop = |_: &str| {};
            if let Err(e) =
                crate::k8s::request(&self.home, &kat_proto::k8s::K8sMsg::Stop, &mut noop)
            {
                let mut k = self.k8s.lock().unwrap_or_else(|e| e.into_inner());
                k.state = prev_state;
                k.error = Some(format!("{e:#}"));
                return Err(failed(format!("{e:#}")));
            }
        }
        if disable_config {
            self.persist_k8s_enabled(false)
                .map_err(|e| failed(format!("{e:#}")))?;
        }
        let ctx = crate::k8s::context_name().map_err(|e| failed(format!("{e:#}")))?;
        crate::k8s::remove_kubeconfig(&self.home, &ctx).map_err(|e| failed(format!("{e:#}")))?;
        let mut k = self.k8s.lock().unwrap_or_else(|e| e.into_inner());
        k.api_proxy = None;
        k.dns = None;
        k.kubeconfig = None;
        k.state = "stopped".into();
        k.error = None;
        Ok(())
    }

    fn k8s_reset(self: &Arc<Self>) -> Result<(), RpcError> {
        let _op = self.k8s_op.lock().unwrap_or_else(|e| e.into_inner());
        let failed = |m: String| RpcError::new(api::FAILED, m);
        let ctx = crate::k8s::context_name().map_err(|e| failed(format!("{e:#}")))?;
        let was_stopped = !self.lock().state.is_active();
        if was_stopped {
            self.k8s
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .suppress_autostart = true;
        }
        let reset_res = (|| -> Result<(), RpcError> {
            if was_stopped {
                self.wait_for_docker_ready()?;
            }
            let mut noop = |_: &str| {};
            crate::k8s::request(&self.home, &kat_proto::k8s::K8sMsg::Reset, &mut noop)
                .map_err(|e| failed(format!("{e:#}")))?;
            Ok(())
        })();
        if was_stopped {
            self.k8s
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .suppress_autostart = false;
            let stop_res = self.stop(true).map_err(failed);
            reset_res?;
            stop_res?;
        } else {
            reset_res?;
        }
        self.persist_k8s_enabled(false)
            .map_err(|e| failed(format!("{e:#}")))?;
        crate::k8s::remove_kubeconfig(&self.home, &ctx).map_err(|e| failed(format!("{e:#}")))?;
        let mut k = self.k8s.lock().unwrap_or_else(|e| e.into_inner());
        k.api_proxy = None;
        k.dns = None;
        k.state = "stopped".into();
        k.kubeconfig = None;
        k.error = None;
        Ok(())
    }

    fn k8s_kubeconfig(&self) -> Result<String, RpcError> {
        let failed = |m: String| RpcError::new(api::FAILED, m);
        let path = crate::k8s::kubeconfig_path(&self.home);
        if let Ok(yaml) = fs::read_to_string(&path) {
            return Ok(yaml);
        }
        let api_port = self
            .k8s
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .api_proxy
            .as_ref()
            .map(|p| p.port)
            .unwrap_or(crate::k8s::DEFAULT_API_PORT);
        let mut noop = |_: &str| {};
        match crate::k8s::request(&self.home, &kat_proto::k8s::K8sMsg::Kubeconfig, &mut noop)
            .map_err(|e| failed(format!("{e:#}")))?
        {
            kat_proto::k8s::K8sMsg::KubeconfigData(raw) => {
                let ctx = crate::k8s::context_name().map_err(|e| failed(format!("{e:#}")))?;
                Ok(crate::k8s::rewrite_kubeconfig(&raw, &ctx, api_port))
            }
            other => Err(failed(format!("unexpected kubeconfig reply {other:?}"))),
        }
    }

    /// Where the guest's files are mounted on the Mac, while they are.
    fn files_path(&self) -> Option<PathBuf> {
        let g = self.lock();
        g.run
            .as_ref()
            .filter(|r| r.mounted)
            .and_then(|r| r.mount_path.clone())
    }

    fn machine_create(
        &self,
        c: &api::MachineCreate,
        progress: &mut dyn FnMut(&str),
    ) -> Result<Value, RpcError> {
        dlog!("machine create {} ({} {:?})", c.name, c.distro, c.release);
        let mut log_progress = |m: &str| {
            dlog!("machine {}: {m}", c.name);
            progress(m);
        };
        let m = crate::machines::create(
            &self.home,
            &c.name,
            &c.distro,
            c.release.as_deref(),
            &mut log_progress,
        )
        .map_err(|e| {
            dlog!("machine create {} failed: {e:#}", c.name);
            RpcError::new(api::FAILED, format!("{e:#}"))
        })?;
        let files = self.files_path();
        serde_json::to_value(crate::machines::Machine::from_info(m, files.as_deref()))
            .map_err(|e| RpcError::new(api::INTERNAL_ERROR, e.to_string()))
    }

    /// Answer with the current tail, then stream `logs.data` notifications
    /// as the file grows, until the client goes away.
    fn follow_logs(&self, id: Value, p: &LogsParams, out: &mut UnixStream) {
        let path = self.log_path(p.source);
        let first = match read_log(&path, p) {
            Ok(c) => c,
            Err(e) => {
                let _ = send(
                    out,
                    &Response::err(id, RpcError::new(api::FAILED, format!("{e:#}"))),
                );
                return;
            }
        };
        let mut offset = first.offset;
        if send(
            out,
            &Response::ok(id, serde_json::to_value(&first).unwrap_or_default()),
        )
        .is_err()
        {
            return;
        }
        loop {
            if client_gone(out, Duration::from_millis(250)) {
                return;
            }
            let next = LogsParams {
                source: p.source,
                lines: None,
                offset: Some(offset),
                follow: false,
            };
            let Ok(chunk) = read_log(&path, &next) else {
                continue;
            };
            let reset = chunk.offset < offset;
            offset = chunk.offset;
            if chunk.data.is_empty() && !reset {
                continue;
            }
            let note = api::Notification {
                jsonrpc: api::VERSION.into(),
                method: "logs.data".into(),
                params: serde_json::to_value(&chunk).unwrap_or_default(),
            };
            if send(out, &note).is_err() {
                return;
            }
        }
    }
}

fn send<T: serde::Serialize>(out: &mut UnixStream, msg: &T) -> io::Result<()> {
    let mut line = serde_json::to_vec(msg)?;
    line.push(b'\n');
    out.write_all(&line)
}

/// Wait up to `timeout` for the client to hang up (or send anything, which
/// a following client must not do).
fn client_gone(s: &UnixStream, timeout: Duration) -> bool {
    let mut pfd = libc::pollfd {
        fd: s.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    let n = unsafe { libc::poll(&mut pfd, 1, timeout.as_millis() as i32) };
    n > 0 && pfd.revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR) != 0
}

/// Most of a log to return at once; older bytes are skipped.
const MAX_CHUNK: u64 = 512 << 10;

/// The tail of a log by lines, or everything after an offset.
pub fn read_log(path: &Path, p: &LogsParams) -> anyhow::Result<LogChunk> {
    let mut f = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Ok(LogChunk {
                data: String::new(),
                offset: 0,
                path: path.to_path_buf(),
            });
        }
        Err(e) => return Err(e).with_context(|| format!("open {}", path.display())),
    };
    let len = f.metadata()?.len();
    let (start, keep_lines) = match p.offset {
        // The file was recreated (the console log is, at every start).
        Some(off) if off > len => (0, None),
        Some(off) => (off.max(len.saturating_sub(MAX_CHUNK)), None),
        None => (len.saturating_sub(MAX_CHUNK), Some(p.lines.unwrap_or(200))),
    };
    f.seek(SeekFrom::Start(start))?;
    let mut buf = Vec::with_capacity((len - start) as usize);
    f.take(len - start).read_to_end(&mut buf)?;
    let end = start + buf.len() as u64;
    let data = match keep_lines {
        Some(n) => tail_lines(&buf, n),
        None => &buf[..],
    };
    Ok(LogChunk {
        data: String::from_utf8_lossy(data).into_owned(),
        offset: end,
        path: path.to_path_buf(),
    })
}

/// The last `n` lines of `buf` (a trailing newline does not count as a line).
fn tail_lines(buf: &[u8], n: usize) -> &[u8] {
    if n == 0 {
        return &[];
    }
    let body = buf.strip_suffix(b"\n").unwrap_or(buf);
    let mut seen = 0;
    for (i, b) in body.iter().enumerate().rev() {
        if *b == b'\n' {
            seen += 1;
            if seen == n {
                return &buf[i + 1..];
            }
        }
    }
    buf
}

/// The VM's settings from the saved config, with the guest images found
/// next to this executable.
fn run_args(cfg: &Config) -> anyhow::Result<RunArgs> {
    let guest = guest_dir()?;
    Ok(RunArgs {
        kernel: guest.join("Image"),
        initramfs: guest.join("initramfs.cpio"),
        rootfs: guest.join("rootfs.erofs"),
        data_template: match guest.join("data.img") {
            p if p.is_file() => p,
            _ => guest.join("data.img.sparse"),
        },
        cpus: cfg.cpus,
        memory_mib: cfg.memory_mib,
        cmdline: if cfg.kubernetes {
            "kat.k8s=1".into()
        } else {
            String::new()
        },
        vm_bin: None,
        no_share: false,
        permissions: "caller".into(),
        mount: cfg.mount_path.clone(),
        no_mount: !cfg.mount,
        no_network: false,
        gvproxy: None,
        mtu: crate::net::DEFAULT_MTU,
        pause_on_sleep: cfg.pause_on_sleep,
        no_auto_forward: !cfg.auto_forward,
        amd64: cfg.amd64,
    })
}

/// `$KATAMARAN_GUEST_DIR`, the app bundle's `Contents/Resources/guest`, or
/// the `guest/out` of the source tree this binary was built in.
pub fn guest_dir() -> anyhow::Result<PathBuf> {
    if let Some(d) = std::env::var_os("KATAMARAN_GUEST_DIR") {
        return Ok(PathBuf::from(d));
    }
    let exe = std::env::current_exe()?.canonicalize()?;
    let dir = exe.parent().context("executable has no directory")?;
    let bundled = dir.join("../Resources/guest");
    if bundled.join("Image").is_file() {
        return Ok(bundled.canonicalize()?);
    }
    for a in dir.ancestors() {
        let out = a.join("guest/out");
        if out.join("Image").is_file() {
            return Ok(out);
        }
    }
    bail!(
        "guest images not found next to {} (set KATAMARAN_GUEST_DIR)",
        exe.display()
    )
}

/// Busy share of the guest's CPUs between two samples, 0–100.
pub fn cpu_percent(prev: &GuestStats, now: &GuestStats) -> f64 {
    let total = now.cpu_total_ticks.saturating_sub(prev.cpu_total_ticks);
    let busy = now.cpu_busy_ticks.saturating_sub(prev.cpu_busy_ticks);
    if total == 0 {
        return 0.0;
    }
    (busy as f64 / total as f64 * 1000.0).round() / 10.0
}

#[repr(C)]
struct MachTimebase {
    numer: u32,
    denom: u32,
}

unsafe extern "C" {
    fn mach_timebase_info(info: *mut MachTimebase) -> libc::c_int;
}

/// CPU time (ns) and physical footprint (bytes) of a process.
pub fn proc_usage(pid: u32) -> Option<(u64, u64)> {
    let mut info: libc::rusage_info_v2 = unsafe { std::mem::zeroed() };
    let rc = unsafe {
        libc::proc_pid_rusage(
            pid as i32,
            libc::RUSAGE_INFO_V2,
            (&mut info as *mut libc::rusage_info_v2).cast(),
        )
    };
    if rc != 0 {
        return None;
    }
    // On Apple silicon these are Mach absolute-time ticks, not ns.
    let mut tb = MachTimebase { numer: 0, denom: 0 };
    unsafe { mach_timebase_info(&mut tb) };
    let ticks = u128::from(info.ri_user_time + info.ri_system_time);
    let ns = ticks * u128::from(tb.numer.max(1)) / u128::from(tb.denom.max(1));
    Some((ns as u64, info.ri_phys_footprint))
}

fn ms(d: Duration) -> f64 {
    (d.as_secs_f64() * 1e4).round() / 10.0
}

pub fn unix_ms(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// Local time as `2026-11-18 17:03:12.345`.
pub fn timestamp() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs() as libc::time_t;
    let mut tm: libc::tm = unsafe { std::mem::zeroed() };
    unsafe { libc::localtime_r(&secs, &mut tm) };
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:03}",
        tm.tm_year + 1900,
        tm.tm_mon + 1,
        tm.tm_mday,
        tm.tm_hour,
        tm.tm_min,
        tm.tm_sec,
        now.subsec_millis()
    )
}

/// Send stdout and stderr to `path`, keeping one rotated copy.
fn redirect_output(path: &Path) -> anyhow::Result<()> {
    if fs::metadata(path).is_ok_and(|m| m.len() > LOG_ROTATE_BYTES) {
        fs::rename(path, path.with_extension("log.1"))?;
    }
    let f = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open {}", path.display()))?;
    for fd in [1, 2] {
        if unsafe { libc::dup2(f.as_raw_fd(), fd) } < 0 {
            bail!("dup2: {}", io::Error::last_os_error());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn temp(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("kat-daemon-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn tails_lines() {
        assert_eq!(tail_lines(b"a\nb\nc\n", 2), b"b\nc\n");
        assert_eq!(tail_lines(b"a\nb\nc", 2), b"b\nc");
        assert_eq!(tail_lines(b"a\nb\n", 5), b"a\nb\n");
        assert_eq!(tail_lines(b"a\nb\n", 0), b"");
        assert_eq!(tail_lines(b"", 3), b"");
    }

    #[test]
    fn reads_logs_by_lines_and_offset() {
        let dir = temp("logs");
        let path = dir.join("x.log");
        let missing = read_log(&path, &LogsParams::default()).unwrap();
        assert_eq!((missing.data.as_str(), missing.offset), ("", 0));

        fs::write(&path, "one\ntwo\nthree\n").unwrap();
        let tail = read_log(
            &path,
            &LogsParams {
                lines: Some(2),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(tail.data, "two\nthree\n");
        assert_eq!(tail.offset, 14);

        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(b"four\n").unwrap();
        let more = read_log(
            &path,
            &LogsParams {
                offset: Some(tail.offset),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(more.data, "four\n");
        assert_eq!(more.offset, 19);

        // Recreated and shorter: start over.
        fs::write(&path, "new\n").unwrap();
        let again = read_log(
            &path,
            &LogsParams {
                offset: Some(more.offset),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!((again.data.as_str(), again.offset), ("new\n", 4));
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn computes_guest_cpu() {
        let a = GuestStats {
            cpu_busy_ticks: 100,
            cpu_total_ticks: 1000,
            ..Default::default()
        };
        let b = GuestStats {
            cpu_busy_ticks: 150,
            cpu_total_ticks: 1200,
            ..Default::default()
        };
        assert_eq!(cpu_percent(&a, &b), 25.0);
        assert_eq!(cpu_percent(&b, &b), 0.0);
        // Counters never go backwards, but a reset must not underflow.
        assert_eq!(cpu_percent(&b, &a), 0.0);
    }

    #[test]
    fn reads_own_usage() {
        let (cpu, footprint) = proc_usage(std::process::id()).unwrap();
        assert!(footprint > 0);
        let spin = Instant::now();
        while spin.elapsed() < Duration::from_millis(30) {}
        let (cpu2, _) = proc_usage(std::process::id()).unwrap();
        // ns, not Mach ticks: 30 ms of spinning is at least ~20 ms.
        assert!(cpu2 - cpu >= 20_000_000, "{} ns", cpu2 - cpu);
        assert!(proc_usage(u32::MAX >> 1).is_none());
    }

    #[test]
    fn stopped_daemon_answers_without_a_vm() {
        let home = temp("api");
        let d = Daemon::new(home.clone());
        let s = d.status();
        assert_eq!(s.state, VmState::Stopped);
        assert_eq!(s.docker.state, DockerState::Stopped);
        assert!(s.vm.is_none());
        // Stop while stopped is a no-op, even when waiting.
        d.handle(&Call::Stop { wait: true }).unwrap();
        let mut values = std::collections::BTreeMap::new();
        values.insert("cpus".to_string(), "1".to_string());
        let v = d.handle(&Call::ConfigSet(values)).unwrap();
        assert_eq!(v["config"]["cpus"], 1);
        assert_eq!(v["restart_required"], false);
        assert_eq!(Config::load(&home).unwrap().cpus, 1);
        let mut bad = std::collections::BTreeMap::new();
        bad.insert("cpus".to_string(), "0".to_string());
        assert_eq!(
            d.handle(&Call::ConfigSet(bad)).unwrap_err().code,
            api::INVALID_PARAMS
        );
        assert_eq!(
            d.handle(&Call::Version).unwrap()["protocol"],
            json!(PROTOCOL_VERSION)
        );
        assert!(
            d.handle(&Call::Stats).unwrap()["samples"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        fs::remove_dir_all(&home).unwrap();
    }
}
