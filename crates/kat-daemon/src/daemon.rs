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
    epoch: u64,
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
            epoch: 0,
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
                        if g.state == VmState::Running
                            && Config::load(&self.home).is_ok_and(|c| c.kubernetes)
                        {
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
            {
                let mut k = self.k8s.lock().unwrap_or_else(|e| e.into_inner());
                k.epoch += 1;
                k.api_proxy = None;
                k.dns = None;
                k.kubeconfig = None;
                k.state = "stopped".into();
            }
            let _ = fs::remove_file(crate::k8s::kubeconfig_path(&self.home));
        }
        if let Some(err) = k8s_failed {
            {
                let mut k = self.k8s.lock().unwrap_or_else(|e| e.into_inner());
                k.epoch += 1;
                k.api_proxy = None;
                k.dns = None;
                k.kubeconfig = None;
                k.state = "failed".into();
                k.error = Some(err);
            }
            let _ = fs::remove_file(crate::k8s::kubeconfig_path(&self.home));
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
                    if let Err(e) =
                        d.k8s_start_inner(false, true, false, &mut |m| dlog!("k8s: {m}"))
                    {
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
        {
            let mut k = self.k8s.lock().unwrap_or_else(|e| e.into_inner());
            k.epoch += 1;
            k.api_proxy = None;
            k.dns = None;
            k.kubeconfig = None;
            k.state = "stopped".into();
        }
        let _ = fs::remove_file(crate::k8s::kubeconfig_path(&self.home));
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
        if cfg.kubernetes != before_k8s {
            if self.lock().state == VmState::Running {
                let d = self.clone();
                let enable = cfg.kubernetes;
                thread::Builder::new()
                    .name("k8s-toggle".into())
                    .spawn(move || {
                        if enable {
                            if let Err(e) =
                                d.k8s_start_inner(false, true, false, &mut |m| dlog!("k8s: {m}"))
                            {
                                dlog!("k8s start failed: {e}");
                            }
                        } else if let Err(e) = d.k8s_stop_inner(false, true) {
                            dlog!("k8s stop failed: {e}");
                        }
                    })
                    .ok();
            } else if !cfg.kubernetes {
                match crate::k8s::context_name() {
                    Ok(ctx) => {
                        if let Err(e) = crate::k8s::remove_kubeconfig(&self.home, &ctx) {
                            dlog!("k8s disable: remove kubeconfig failed: {e:#}");
                        }
                    }
                    Err(e) => {
                        let _ = fs::remove_file(crate::k8s::kubeconfig_path(&self.home));
                        dlog!("k8s disable: context name failed: {e:#}");
                    }
                }
                let mut k = self.k8s.lock().unwrap_or_else(|e| e.into_inner());
                k.kubeconfig = None;
            }
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
                let res = {
                    let mut progress = |m: &str| {
                        let _ = send(
                            &mut out,
                            &api::Notification {
                                jsonrpc: api::VERSION.into(),
                                method: "k8s.progress".into(),
                                params: serde_json::json!({ "message": m }),
                            },
                        );
                    };
                    self.k8s_start(&mut progress).and_then(|s| {
                        serde_json::to_value(s)
                            .map_err(|e| RpcError::new(api::INTERNAL_ERROR, e.to_string()))
                    })
                };
                let resp = match res {
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

    fn wait_for_docker_ready(self: &Arc<Self>, allow_start_vm: bool) -> Result<(), RpcError> {
        let failed = |m: String| RpcError::new(api::FAILED, m);
        {
            let g = self.lock();
            if g.state == VmState::Stopped || g.state == VmState::Error {
                if !allow_start_vm {
                    return Err(failed("the VM is not running".into()));
                }
                drop(g);
                self.start().map_err(failed)?;
            }
        }
        let deadline = Instant::now() + Duration::from_secs(180);
        let mut g = self.lock();
        loop {
            if g.state == VmState::Error
                || g.state == VmState::Stopped
                || g.state == VmState::Stopping
            {
                return Err(failed(g.error.clone().unwrap_or_else(|| {
                    "the VM stopped before Kubernetes could start".into()
                })));
            }
            if let Some(r) = g.run.as_ref() {
                if r.docker == DockerState::Ready {
                    return Ok(());
                }
                if r.docker == DockerState::Failed {
                    return Err(failed(
                        r.problem
                            .clone()
                            .unwrap_or_else(|| "Docker failed to start".into()),
                    ));
                }
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
        self.k8s_start_inner(true, false, true, progress)
    }

    fn k8s_start_inner(
        self: &Arc<Self>,
        persist_enabled: bool,
        only_if_config_enabled: bool,
        allow_start_vm: bool,
        progress: &mut dyn FnMut(&str),
    ) -> Result<K8sStatus, RpcError> {
        let _op = self.k8s_op.lock().unwrap_or_else(|e| e.into_inner());
        let failed = |m: String| RpcError::new(api::FAILED, m);
        if only_if_config_enabled && !Config::load(&self.home).is_ok_and(|c| c.kubernetes) {
            return Ok(self.k8s_status(false));
        }
        progress("ensuring VM is running");
        self.wait_for_docker_ready(allow_start_vm)?;
        if only_if_config_enabled && !Config::load(&self.home).is_ok_and(|c| c.kubernetes) {
            return Ok(self.k8s_status(false));
        }

        let (vm_gen, k8s_epoch, already_running) = {
            let g = self.lock();
            if g.state != VmState::Running {
                return Err(failed("the VM is no longer running".into()));
            }
            let mut k = self.k8s.lock().unwrap_or_else(|e| e.into_inner());
            if k.state == "running" && k.api_proxy.is_some() {
                (g.generation, k.epoch, true)
            } else {
                k.state = "starting".into();
                k.error = None;
                (g.generation, k.epoch, false)
            }
        };
        if already_running {
            if persist_enabled {
                self.persist_k8s_enabled(true)
                    .map_err(|e| failed(format!("{e:#}")))?;
            }
            return Ok(self.k8s_status(true));
        }

        let mut guest_started = false;
        let mut kubeconfig_merged = false;
        let res = (|| -> anyhow::Result<(
            kat_proto::k8s::K8sInfo,
            crate::k8s::ApiProxy,
            crate::k8s::DnsServer,
            PathBuf,
        )> {
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
            kubeconfig_merged = true;
            Ok((info, proxy, dns, kc_path))
        })();

        let is_fresh = |g: &Inner, k: &K8sRuntime| {
            g.state == VmState::Running && g.generation == vm_gen && k.epoch == k8s_epoch
        };

        let cleanup_kubeconfig = |merged: bool| {
            // Caller holds `self.k8s_op`, so no newer start can run concurrently;
            // double-check state != "running" before removing the merged user context.
            let can_remove_user =
                merged && self.k8s.lock().unwrap_or_else(|e| e.into_inner()).state != "running";
            if can_remove_user {
                if let Ok(ctx) = crate::k8s::context_name() {
                    if let Err(re) = crate::k8s::remove_kubeconfig(&self.home, &ctx) {
                        dlog!("k8s rollback remove kubeconfig failed: {re:#}");
                    }
                } else {
                    let _ = fs::remove_file(crate::k8s::kubeconfig_path(&self.home));
                }
            } else {
                let _ = fs::remove_file(crate::k8s::kubeconfig_path(&self.home));
            }
        };

        let e = match res {
            Ok((info, proxy, dns, kc_path)) => {
                let stale = {
                    let g = self.lock();
                    let k = self.k8s.lock().unwrap_or_else(|e| e.into_inner());
                    !is_fresh(&g, &k)
                };
                if stale {
                    drop(proxy);
                    drop(dns);
                    cleanup_kubeconfig(kubeconfig_merged);
                    return Err(failed(
                        "Kubernetes start was superseded by a VM state change".into(),
                    ));
                }
                if persist_enabled && let Err(e) = self.persist_k8s_enabled(true) {
                    drop(proxy);
                    drop(dns);
                    e
                } else {
                    let g = self.lock();
                    let mut k = self.k8s.lock().unwrap_or_else(|e| e.into_inner());
                    if !is_fresh(&g, &k) {
                        drop(k);
                        drop(g);
                        drop(proxy);
                        drop(dns);
                        cleanup_kubeconfig(kubeconfig_merged);
                        return Err(failed(
                            "Kubernetes start was superseded by a VM state change".into(),
                        ));
                    }
                    k.state = "running".into();
                    k.installed = info.installed;
                    k.version = info.version;
                    k.api_proxy = Some(proxy);
                    k.dns = Some(dns);
                    k.kubeconfig = Some(kc_path);
                    k.error = None;
                    drop(k);
                    drop(g);
                    return Ok(self.k8s_status(true));
                }
            }
            Err(e) => e,
        };
        let still_same_session = {
            let g = self.lock();
            let k = self.k8s.lock().unwrap_or_else(|e| e.into_inner());
            is_fresh(&g, &k)
        };
        if guest_started && still_same_session {
            let mut noop = |_: &str| {};
            if let Err(se) =
                crate::k8s::request(&self.home, &kat_proto::k8s::K8sMsg::Stop, &mut noop)
            {
                dlog!("k8s rollback stop failed: {se:#}");
            }
        }
        cleanup_kubeconfig(kubeconfig_merged);
        let msg = format!("{e:#}");
        {
            let g = self.lock();
            let mut k = self.k8s.lock().unwrap_or_else(|e| e.into_inner());
            if is_fresh(&g, &k) {
                k.state = "failed".into();
                k.api_proxy = None;
                k.dns = None;
                k.kubeconfig = None;
                k.error = Some(msg.clone());
            }
        }
        Err(failed(msg))
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
        let vm_state = self.lock().state;
        // Keep API/DNS relays and saved config intact until the guest confirms
        // k3s has actually stopped. Only tolerate k8s.sock connect failures when
        // the VM is still starting or k8s was already stopped; never ignore an
        // error returned by the guest Stop RPC itself.
        if vm_state.is_active() && crate::k8s::socket_path(&self.home).exists() {
            match crate::k8s::connect(&self.home) {
                Ok(mut stream) => {
                    let mut noop = |_: &str| {};
                    if let Err(e) = crate::k8s::request_stream(
                        &mut stream,
                        &kat_proto::k8s::K8sMsg::Stop,
                        &mut noop,
                    ) {
                        let mut k = self.k8s.lock().unwrap_or_else(|e| e.into_inner());
                        k.state = prev_state;
                        k.error = Some(format!("{e:#}"));
                        return Err(failed(format!("{e:#}")));
                    }
                }
                Err(e) if vm_state == VmState::Starting || prev_state == "stopped" => {
                    dlog!(
                        "k8s stop: ignoring k8s.sock connect error while vm={} k8s={prev_state}: {e:#}",
                        vm_state.as_str()
                    );
                }
                Err(e) => {
                    let mut k = self.k8s.lock().unwrap_or_else(|e| e.into_inner());
                    k.state = prev_state;
                    k.error = Some(format!("{e:#}"));
                    return Err(failed(format!("{e:#}")));
                }
            }
        }
        // Always tear down host relays and state once the guest stopped k3s,
        // even if persisting config or removing ~/.kube/config later fails.
        {
            let mut k = self.k8s.lock().unwrap_or_else(|e| e.into_inner());
            k.epoch += 1;
            k.api_proxy = None;
            k.dns = None;
            k.kubeconfig = None;
            k.state = "stopped".into();
            k.error = None;
        }
        let _ = fs::remove_file(crate::k8s::kubeconfig_path(&self.home));
        let mut first_err: Option<anyhow::Error> = None;
        if disable_config && let Err(e) = self.persist_k8s_enabled(false) {
            first_err = Some(e);
        }
        match crate::k8s::context_name() {
            Ok(ctx) => {
                if let Err(e) = crate::k8s::remove_kubeconfig(&self.home, &ctx) {
                    dlog!("k8s stop: remove kubeconfig failed: {e:#}");
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
            }
            Err(e) => {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }
        if let Some(e) = first_err {
            return Err(failed(format!("{e:#}")));
        }
        Ok(())
    }

    fn k8s_reset(self: &Arc<Self>) -> Result<(), RpcError> {
        let _op = self.k8s_op.lock().unwrap_or_else(|e| e.into_inner());
        let failed = |m: String| RpcError::new(api::FAILED, m);
        let ctx = crate::k8s::context_name().map_err(|e| failed(format!("{e:#}")))?;
        let initial_state = self.lock().state;
        let was_stopped = !initial_state.is_active();
        let needs_wait_docker = was_stopped || initial_state == VmState::Starting;
        if needs_wait_docker {
            self.k8s
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .suppress_autostart = true;
        }
        let mut guest_reset_attempted = false;
        let reset_res = (|| -> Result<(), RpcError> {
            if needs_wait_docker {
                self.wait_for_docker_ready(was_stopped)?;
            }
            let mut stream =
                crate::k8s::connect(&self.home).map_err(|e| failed(format!("{e:#}")))?;
            kat_proto::k8s::write(&mut stream, &kat_proto::k8s::K8sMsg::Reset)
                .map_err(|e| failed(format!("{e:#}")))?;
            guest_reset_attempted = true;
            let mut noop = |_: &str| {};
            crate::k8s::read_answer(&mut stream, &mut noop)
                .map_err(|e| failed(format!("{e:#}")))?;
            Ok(())
        })();

        let stop_res = if was_stopped {
            let res = self.stop(true).map_err(failed);
            self.k8s
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .suppress_autostart = false;
            res
        } else {
            if needs_wait_docker {
                self.k8s
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .suppress_autostart = false;
            }
            Ok(())
        };

        if guest_reset_attempted {
            {
                let mut k = self.k8s.lock().unwrap_or_else(|e| e.into_inner());
                k.epoch += 1;
                k.api_proxy = None;
                k.dns = None;
                k.kubeconfig = None;
                k.state = "stopped".into();
                k.error = reset_res.as_ref().err().map(|e| e.message.clone());
            }
            if let Err(e) = crate::k8s::remove_kubeconfig(&self.home, &ctx) {
                dlog!("k8s reset: remove kubeconfig failed: {e:#}");
            }
        }

        reset_res?;
        stop_res?;

        self.persist_k8s_enabled(false)
            .map_err(|e| failed(format!("{e:#}")))?;
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

    #[test]
    fn wait_for_docker_ready_respects_allow_start_vm_and_fails_on_docker_error() {
        let home = temp("k8s-wait");
        let d = Daemon::new(home.clone());

        // Stopped VM with allow_start_vm = false fails immediately without starting VM.
        let err = d.wait_for_docker_ready(false).unwrap_err();
        assert!(err.message.contains("not running"), "got: {}", err.message);
        assert_eq!(d.status().state, VmState::Stopped);

        // Running VM with DockerState::Failed fails immediately instead of hanging 180s.
        {
            let mut g = d.lock();
            g.state = VmState::Running;
            let mut run = Run::new(Config::default(), None);
            run.docker = DockerState::Failed;
            run.problem = Some("dockerd crashed".into());
            g.run = Some(run);
        }
        let err = d.wait_for_docker_ready(false).unwrap_err();
        assert!(
            err.message.contains("dockerd crashed"),
            "got: {}",
            err.message
        );

        // Stopping VM also fails immediately even if docker was Ready.
        {
            let mut g = d.lock();
            g.state = VmState::Stopping;
            if let Some(r) = g.run.as_mut() {
                r.docker = DockerState::Ready;
            }
        }
        let err = d.wait_for_docker_ready(false).unwrap_err();
        assert!(err.message.contains("stopped"), "got: {}", err.message);

        fs::remove_dir_all(&home).unwrap();
    }

    #[test]
    fn k8s_lifecycle_events_bump_epoch_and_clear_kubeconfig() {
        let home = temp("k8s-events");
        let d = Daemon::new(home.clone());
        let kc = crate::k8s::kubeconfig_path(&home);
        fs::create_dir_all(kc.parent().unwrap()).unwrap();
        fs::write(&kc, "dummy").unwrap();

        {
            let mut g = d.lock();
            g.generation = 1;
            g.state = VmState::Running;
            g.run = Some(Run::new(Config::default(), None));
            let mut k = d.k8s.lock().unwrap();
            k.state = "running".into();
            k.kubeconfig = Some(kc.clone());
        }

        // Event::Restarting bumps epoch, clears kubeconfig, and deletes run/kubeconfig.yaml.
        d.on_event(
            1,
            Event::Restarting {
                why: "crash".into(),
                delay: Duration::from_secs(1),
            },
        );
        {
            let k = d.k8s.lock().unwrap();
            assert_eq!(k.epoch, 1);
            assert_eq!(k.state, "stopped");
            assert!(k.kubeconfig.is_none());
        }
        assert!(!kc.exists());

        // Event::ServiceReady("docker") while Stopping must NOT trigger autostart.
        let cfg = Config {
            kubernetes: true,
            ..Config::default()
        };
        cfg.save(&home).unwrap();
        {
            let mut g = d.lock();
            g.state = VmState::Stopping;
            g.run = Some(Run::new(cfg, None));
        }
        d.on_event(1, Event::ServiceReady("docker".into()));
        assert_eq!(d.lock().state, VmState::Stopping);
        assert_eq!(d.k8s.lock().unwrap().state, "stopped");

        // on_exit bumps epoch and removes run/kubeconfig.yaml.
        fs::write(&kc, "dummy2").unwrap();
        d.k8s.lock().unwrap().kubeconfig = Some(kc.clone());
        d.on_exit(1, Ok(()));
        {
            let k = d.k8s.lock().unwrap();
            assert_eq!(k.epoch, 2);
            assert_eq!(k.state, "stopped");
            assert!(k.kubeconfig.is_none());
        }
        assert!(!kc.exists());

        fs::remove_dir_all(&home).unwrap();
    }

    #[test]
    fn k8s_stop_tolerates_missing_guest_when_starting_or_stopped() {
        let home = temp("k8s-stop");
        let d = Daemon::new(home.clone());
        // Create a dummy (unconnectable) k8s.sock path to simulate Starting VM before guest listens.
        let sock = crate::k8s::socket_path(&home);
        fs::create_dir_all(sock.parent().unwrap()).unwrap();
        fs::write(&sock, b"").unwrap();
        {
            let mut g = d.lock();
            g.state = VmState::Starting;
        }
        // Should tolerate connect error while Starting and reset k8s state cleanly.
        d.k8s_stop_inner(true, false).unwrap();
        assert_eq!(d.k8s.lock().unwrap().state, "stopped");
        assert!(d.k8s.lock().unwrap().epoch >= 1);

        // Must NOT ignore an explicit guest Stop RPC error even when prev_state == "stopped".
        let _ = fs::remove_file(&sock);
        let listener = UnixListener::bind(&sock).unwrap();
        let guest = thread::spawn(move || {
            let (mut conn, _) = listener.accept().unwrap();
            let req = kat_proto::k8s::read(&mut conn).unwrap().unwrap();
            assert_eq!(req, kat_proto::k8s::K8sMsg::Stop);
            kat_proto::k8s::write(
                &mut conn,
                &kat_proto::k8s::K8sMsg::Error("still busy".into()),
            )
            .unwrap();
        });
        {
            let mut g = d.lock();
            g.state = VmState::Running;
        }
        let err = d.k8s_stop_inner(true, false).unwrap_err();
        assert!(err.message.contains("still busy"), "got: {}", err.message);
        guest.join().unwrap();

        fs::remove_dir_all(&home).unwrap();
    }

    #[test]
    fn k8s_reset_only_tears_down_host_state_after_reset_request_is_sent() {
        let home = temp("k8s-reset");
        let d = Daemon::new(home.clone());
        let kc = crate::k8s::kubeconfig_path(&home);
        fs::create_dir_all(kc.parent().unwrap()).unwrap();
        fs::write(&kc, "dummy-kc").unwrap();

        // 1. Connection failure before Reset is sent must NOT tear down running host state.
        {
            let mut g = d.lock();
            g.state = VmState::Running;
            let mut k = d.k8s.lock().unwrap();
            k.state = "running".into();
            k.kubeconfig = Some(kc.clone());
        }
        let err = d.k8s_reset().unwrap_err();
        assert!(
            err.message.contains("not running"),
            "unexpected err: {}",
            err.message
        );
        {
            let k = d.k8s.lock().unwrap();
            assert_eq!(k.state, "running");
            assert_eq!(k.kubeconfig.as_deref(), Some(kc.as_path()));
            assert_eq!(k.epoch, 0);
        }
        assert!(kc.exists());

        // 2. Once guest receives Reset and fails mid-cleanup, host state and kubeconfig ARE torn down.
        let sock = crate::k8s::socket_path(&home);
        let listener = UnixListener::bind(&sock).unwrap();
        let guest = thread::spawn(move || {
            let (mut conn, _) = listener.accept().unwrap();
            let req = kat_proto::k8s::read(&mut conn).unwrap().unwrap();
            assert_eq!(req, kat_proto::k8s::K8sMsg::Reset);
            kat_proto::k8s::write(
                &mut conn,
                &kat_proto::k8s::K8sMsg::Error("remove data failed".into()),
            )
            .unwrap();
        });
        let err = d.k8s_reset().unwrap_err();
        assert!(
            err.message.contains("remove data failed"),
            "unexpected err: {}",
            err.message
        );
        {
            let k = d.k8s.lock().unwrap();
            assert_eq!(k.state, "stopped");
            assert!(k.kubeconfig.is_none());
            assert_eq!(k.epoch, 1);
            assert!(
                k.error
                    .as_deref()
                    .is_some_and(|m| m.contains("remove data failed"))
            );
        }
        assert!(!kc.exists());
        guest.join().unwrap();

        fs::remove_dir_all(&home).unwrap();
    }
}
