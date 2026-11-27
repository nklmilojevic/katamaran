//! `katamaran daemon`: owns the VM lifecycle in the background and serves
//! the control API (see [`crate::api`]) on `<home>/run/daemon.sock`.
//!
//! One daemon per home: it holds `<home>/run/katamaran.lock` for its whole
//! life, the same lock `katamaran run` takes. The VM runs on a supervisor
//! thread through [`run::run_vm`]; its events update the status that API
//! clients read. SIGTERM (launchd's bootout) and SIGINT stop the VM cleanly
//! before the daemon exits.

use std::collections::{BTreeMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, bail};
use kat_proto::{GuestStats, PROTOCOL_VERSION, PortProto};
use serde_json::Value;

use crate::api::{
    self, Call, ConfigView, DaemonInfo, DockerInfo, DockerState, K8sStatus, LogChunk, LogSource,
    LogsParams, MountInfo, Port, PortError, Request, Response, RpcError, Sample, StatsResult,
    Status, Versions, VmInfo,
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
const PROGRESS_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const PROGRESS_WRITE_TIMEOUT: Duration = Duration::from_secs(2);
const KUBECONFIG_CLEANUP_TIMEOUT: Duration = Duration::from_secs(30);
/// Rotate `daemon.log` at startup beyond this size.
const LOG_ROTATE_BYTES: u64 = 4 << 20;
/// Maximum concurrent client connections served by the daemon socket.
const MAX_CLIENTS: usize = 128;
const ACCEPT_ERR_BACKOFF: Duration = Duration::from_millis(50);

struct ClientGuard(Arc<AtomicUsize>);

impl ClientGuard {
    fn try_acquire(counter: &Arc<AtomicUsize>, max: usize) -> Option<Self> {
        let mut cur = counter.load(Ordering::Acquire);
        loop {
            if cur >= max {
                return None;
            }
            match counter.compare_exchange_weak(cur, cur + 1, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return Some(ClientGuard(counter.clone())),
                Err(next) => cur = next,
            }
        }
    }
}

impl Drop for ClientGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Release);
    }
}

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
    // Lift launchd's 256-fd soft limit before opening sockets or spawning
    // gvproxy / katamaran-vm so forwarding gets its full default budget.
    crate::autofwd::raise_nofile_limit();

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

    let active_clients = Arc::new(AtomicUsize::new(0));
    for conn in listener.incoming() {
        match conn {
            Ok(conn) => {
                let Some(guard) = ClientGuard::try_acquire(&active_clients, MAX_CLIENTS) else {
                    dlog!("rejecting client: connection limit ({MAX_CLIENTS}) reached");
                    continue;
                };
                let d = d.clone();
                if let Err(e) =
                    thread::Builder::new()
                        .name("daemon-client".into())
                        .spawn(move || {
                            let _guard = guard;
                            d.serve(conn);
                        })
                {
                    dlog!("spawn client thread: {e}");
                }
            }
            Err(e) => {
                dlog!("accept: {e}");
                thread::sleep(ACCEPT_ERR_BACKOFF);
            }
        }
    }
    drop(lock);
    Ok(())
}

pub struct Daemon {
    home: PathBuf,
    inner: Mutex<Inner>,
    /// Serializes config read-modify-write updates in [`Self::set_config`].
    config_lock: Mutex<()>,
    k8s: Mutex<K8sRuntime>,
    /// Serializes Kubernetes start/stop/reset/toggle operations so rapid
    /// preference changes cannot race or overwrite newer user choices.
    k8s_op: Mutex<()>,
    changed: Condvar,
    started: SystemTime,
    launchd_label: Option<String>,
    /// Shared process-wide relay descriptor budget preserved across VM restarts.
    relay_budget: crate::autofwd::RelayBudget,
    #[cfg(test)]
    test_user_home: Option<PathBuf>,
    #[cfg(test)]
    test_kube_context: Option<String>,
    #[cfg(test)]
    test_heartbeat_interval: Option<Duration>,
    #[cfg(test)]
    test_progress_write_timeout: Option<Duration>,
    #[cfg(test)]
    test_kubeconfig_timeout: Option<Duration>,
    #[cfg(test)]
    test_kubectl_bin: Option<PathBuf>,
}

struct K8sRuntime {
    epoch: u64,
    state: String,
    installed: bool,
    version: String,
    api_proxy: Option<crate::k8s::ApiProxy>,
    dns: Option<crate::k8s::DnsServer>,
    kubeconfig: Option<PathBuf>,
    started_settings: Option<crate::k8s::K8sStartSettings>,
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
            started_settings: None,
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
    /// Active forwards keyed by `(proto, port, origin)` so removing one origin
    /// never erases a live forward from the other origin during a handoff.
    ports: BTreeMap<(PortProto, u16, run::PortOrigin), Vec<std::net::Ipv4Addr>>,
    port_errors: BTreeMap<(PortProto, u16, run::PortOrigin), String>,
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
            ports: BTreeMap::new(),
            port_errors: BTreeMap::new(),
            prev_guest: None,
            prev_host: None,
            host_cpu_percent: 0.0,
            host_footprint_mib: 0,
        }
    }

    /// Merge published and automatic forwards into one status entry per
    /// `(proto, port)`, preferring a live published forward when both exist.
    fn merged_ports(&self) -> BTreeMap<(PortProto, u16), Vec<std::net::Ipv4Addr>> {
        let mut merged = BTreeMap::new();
        // `PortOrigin::Published < PortOrigin::Automatic`, so `Published` is
        // visited first and wins when both origins are momentarily active.
        for (&(proto, port, _origin), addrs) in &self.ports {
            merged.entry((proto, port)).or_insert_with(|| addrs.clone());
        }
        merged
    }

    /// Merge port errors per `(proto, port)`, suppressing errors for ports
    /// that currently have a live forward from either origin.
    fn merged_port_errors(
        &self,
        live: &BTreeMap<(PortProto, u16), Vec<std::net::Ipv4Addr>>,
    ) -> BTreeMap<(PortProto, u16), String> {
        let mut merged = BTreeMap::new();
        for (&(proto, port, _origin), error) in &self.port_errors {
            if live.contains_key(&(proto, port)) {
                continue;
            }
            merged.entry((proto, port)).or_insert_with(|| error.clone());
        }
        merged
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
            config_lock: Mutex::new(()),
            k8s: Mutex::new(K8sRuntime::default()),
            k8s_op: Mutex::new(()),
            changed: Condvar::new(),
            started: SystemTime::now(),
            launchd_label: std::env::var("XPC_SERVICE_NAME")
                .ok()
                .filter(|l| l.starts_with(crate::setup::LABEL_PREFIX)),
            relay_budget: crate::autofwd::RelayBudget::from_process_limits(),
            #[cfg(test)]
            test_user_home: None,
            #[cfg(test)]
            test_kube_context: None,
            #[cfg(test)]
            test_heartbeat_interval: None,
            #[cfg(test)]
            test_progress_write_timeout: None,
            #[cfg(test)]
            test_kubeconfig_timeout: None,
            #[cfg(test)]
            test_kubectl_bin: None,
        })
    }

    #[cfg(test)]
    fn new_for_test(home: PathBuf, user_home: PathBuf, ctx: impl Into<String>) -> Arc<Daemon> {
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
            config_lock: Mutex::new(()),
            k8s: Mutex::new(K8sRuntime::default()),
            k8s_op: Mutex::new(()),
            changed: Condvar::new(),
            started: SystemTime::now(),
            launchd_label: None,
            test_user_home: Some(user_home),
            test_kube_context: Some(ctx.into()),
            test_heartbeat_interval: None,
            test_progress_write_timeout: None,
            test_kubeconfig_timeout: None,
            test_kubectl_bin: None,
        })
    }

    fn heartbeat_interval(&self) -> Duration {
        #[cfg(test)]
        if let Some(d) = self.test_heartbeat_interval {
            return d;
        }
        PROGRESS_HEARTBEAT_INTERVAL
    }

    fn progress_write_timeout(&self) -> Duration {
        #[cfg(test)]
        if let Some(d) = self.test_progress_write_timeout {
            return d;
        }
        PROGRESS_WRITE_TIMEOUT
    }

    fn kubeconfig_cleanup_timeout(&self) -> Duration {
        #[cfg(test)]
        if let Some(d) = self.test_kubeconfig_timeout {
            return d;
        }
        KUBECONFIG_CLEANUP_TIMEOUT
    }

    fn kube_context_name(&self) -> anyhow::Result<String> {
        #[cfg(test)]
        if let Some(ctx) = &self.test_kube_context {
            return Ok(ctx.clone());
        }
        crate::k8s::context_name()
    }

    fn remove_kubeconfig(&self, ctx: &str) -> anyhow::Result<()> {
        #[cfg(test)]
        if let Some(user_home) = &self.test_user_home {
            return crate::k8s::remove_kubeconfig_for_user_home(
                &self.home,
                Some(user_home.as_path()),
                ctx,
            );
        }
        crate::k8s::remove_kubeconfig(&self.home, ctx)
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
        let prev_state = g.state;
        let prev_error = g.error.clone();
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
        if let Err(e) = thread::Builder::new()
            .name("vm-supervisor".into())
            .spawn(move || {
                let result = run::run_vm(&args, d.relay_budget.clone(), rx, &mut |e| {
                    d.on_event(generation, e)
                });
                d.on_exit(generation, result);
            })
        {
            g.control = None;
            g.run = None;
            g.error = prev_error;
            self.set_state(&mut g, prev_state);
            return Err(e.to_string());
        }
        Ok(())
    }

    /// Ask the VM to stop; with `wait`, until it is gone.
    pub fn stop(&self, wait: bool) -> Result<(), String> {
        self.stop_with_progress(wait, &mut |_| {})
    }

    fn stop_with_progress(&self, wait: bool, progress: &mut dyn FnMut(&str)) -> Result<(), String> {
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
            let mut last_beat = Instant::now();
            while g.state.is_active() {
                let left = deadline.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    return Err("the VM did not stop in time".into());
                }
                let step = left.min(PROGRESS_HEARTBEAT_INTERVAL);
                g = self
                    .changed
                    .wait_timeout(g, step)
                    .unwrap_or_else(|e| e.into_inner())
                    .0;
                if g.state.is_active()
                    && Instant::now() < deadline
                    && last_beat.elapsed() >= PROGRESS_HEARTBEAT_INTERVAL
                {
                    drop(g);
                    progress("stopping temporary VM");
                    last_beat = Instant::now();
                    g = self.lock();
                }
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
                Event::PortAdd(origin, proto, port, addrs) => {
                    r.port_errors.remove(&(proto, port, origin));
                    r.ports.insert((proto, port, origin), addrs);
                }
                Event::PortRemove(origin, proto, port) => {
                    r.ports.remove(&(proto, port, origin));
                    r.port_errors.remove(&(proto, port, origin));
                }
                Event::PortFailed(origin, proto, port, error) => {
                    r.ports.remove(&(proto, port, origin));
                    r.port_errors.insert((proto, port, origin), error);
                }
                Event::PortErrorClear(origin, proto, port) => {
                    r.port_errors.remove(&(proto, port, origin));
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
                k.started_settings = None;
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
                k.started_settings = None;
                k.state = "failed".into();
                k.error = Some(err);
            }
            let _ = fs::remove_file(crate::k8s::kubeconfig_path(&self.home));
        }
        let allow_autostart = {
            let k = self.k8s.lock().unwrap_or_else(|e| e.into_inner());
            !k.suppress_autostart && k.state != "failed"
        };
        if auto_start_k8s && allow_autostart {
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
            k.started_settings = None;
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
                    r.merged_ports()
                        .into_iter()
                        .map(|((p, port), addresses)| Port {
                            proto: p.as_str().into(),
                            port,
                            addresses,
                        })
                        .collect()
                })
                .unwrap_or_default(),
            port_errors: r
                .map(|r| {
                    let live = r.merged_ports();
                    r.merged_port_errors(&live)
                        .into_iter()
                        .map(|((p, port), error)| PortError {
                            proto: p.as_str().into(),
                            port,
                            error,
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
            k8s_toggled: false,
            path: Config::path(&self.home),
            config,
        })
    }

    fn set_config(
        self: &Arc<Self>,
        values: &std::collections::BTreeMap<String, String>,
    ) -> anyhow::Result<ConfigView> {
        self.set_config_with(values, |home| match self.kube_context_name() {
            Ok(ctx) => {
                if let Err(e) = self.remove_kubeconfig(&ctx) {
                    dlog!("k8s disable: remove kubeconfig failed: {e:#}");
                }
            }
            Err(e) => {
                let _ = fs::remove_file(crate::k8s::kubeconfig_path(home));
                dlog!("k8s disable: context name failed: {e:#}");
            }
        })
    }

    fn set_config_with(
        self: &Arc<Self>,
        values: &std::collections::BTreeMap<String, String>,
        cleanup_disabled_kubeconfig: impl FnOnce(&Path),
    ) -> anyhow::Result<ConfigView> {
        let (cfg_kubernetes, before_k8s) = {
            let _cfg_guard = self.config_lock.lock().unwrap_or_else(|e| e.into_inner());
            let mut cfg = Config::load(&self.home)?;
            let host = Host::current();
            let before = cfg.launch_at_login;
            let before_k8s = cfg.kubernetes;
            for (k, v) in values {
                cfg.set(k, v, &host)?;
            }
            cfg.save(&self.home)?;
            let keys: Vec<&str> = values.keys().map(String::as_str).collect();
            dlog!("config saved: {keys:?}");
            if cfg.launch_at_login != before {
                crate::setup::set_run_at_load(cfg.launch_at_login, self.launchd_label.as_deref())?;
            }
            (cfg.kubernetes, before_k8s)
        };
        let k8s_toggled = cfg_kubernetes != before_k8s;
        if k8s_toggled {
            if self.lock().state == VmState::Running {
                let d = self.clone();
                let enable = cfg_kubernetes;
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
            } else if !cfg_kubernetes {
                cleanup_disabled_kubeconfig(&self.home);
                let mut k = self.k8s.lock().unwrap_or_else(|e| e.into_inner());
                k.kubeconfig = None;
            }
        }
        let mut view = self.config_view()?;
        view.k8s_toggled = k8s_toggled;
        Ok(view)
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
            if let Call::K8sReset {
                images,
                progress: true,
            } = &call
            {
                if self.serve_k8s_reset(id, *images, &mut out).is_err() {
                    return;
                }
                continue;
            }
            if let Call::MachineCreate(c) = &call {
                let mut progress = |m: &str| {
                    let _ = send(
                        &mut out,
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

    fn serve_k8s_reset(
        self: &Arc<Self>,
        id: Value,
        remove_images: bool,
        out: &mut UnixStream,
    ) -> Result<(), ()> {
        let prev_write_timeout = out.write_timeout().ok().flatten();
        let write_timeout = self.progress_write_timeout();
        let mut write_failed = false;
        let res = {
            let mut progress = |m: &str| {
                if write_failed {
                    return;
                }
                let note = api::Notification {
                    jsonrpc: api::VERSION.into(),
                    method: "k8s.progress".into(),
                    params: serde_json::json!({ "message": m }),
                };
                if send_bounded(out, &note, write_timeout).is_err() {
                    write_failed = true;
                }
            };
            self.k8s_reset(remove_images, &mut progress)
        };
        if write_failed {
            // A progress write failed or timed out (possibly leaving a partial
            // JSON line on the stream). Do not write the terminal response and
            // close the connection now that k8s_reset has finished.
            return Err(());
        }
        let resp = match res {
            Ok(()) => Response::ok(id, Value::Null),
            Err(e) => Response::err(id, e),
        };
        if send_bounded(out, &resp, write_timeout).is_err() {
            return Err(());
        }
        if out.set_write_timeout(prev_write_timeout).is_err() {
            return Err(());
        }
        Ok(())
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
            Call::K8sReset { images, .. } => {
                self.k8s_reset(*images, &mut |_| {})?;
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
            Call::K8sMarkVersion { version } => {
                self.k8s_mark_version(version)?;
                Ok(Value::Null)
            }
        }
    }

    fn k8s_status(&self, enabled: bool) -> K8sStatus {
        let k = self.k8s.lock().unwrap_or_else(|e| e.into_inner());
        let dns_port = k.dns.as_ref().map(|d| d.port);
        let dns_isolated = k.dns.as_ref().is_some_and(|d| d.isolated);
        K8sStatus {
            enabled,
            state: k.state.clone(),
            installed: k.installed,
            version: k.version.clone(),
            api_port: k.api_proxy.as_ref().map(|p| p.port),
            proxy_socket: k
                .api_proxy
                .as_ref()
                .filter(|_| k.state == "running")
                .and_then(|p| p.rest_socket())
                .map(Path::to_path_buf),
            context: self.kube_context_name().unwrap_or_default(),
            kubeconfig: k.kubeconfig.clone(),
            dns_domain: crate::k8s::DNS_DOMAIN.into(),
            dns_port,
            dns_installed: !dns_isolated && crate::k8s::dns_resolver_installed(dns_port),
            dns_isolated,
            error: k.error.clone(),
        }
    }

    fn persist_k8s_enabled(&self, enabled: bool) -> anyhow::Result<()> {
        let _cfg_guard = self.config_lock.lock().unwrap_or_else(|e| e.into_inner());
        let mut cfg = Config::load(&self.home)?;
        if cfg.kubernetes != enabled {
            cfg.kubernetes = enabled;
            cfg.save(&self.home)?;
        }
        Ok(())
    }

    fn wait_for_docker_ready(self: &Arc<Self>, allow_start_vm: bool) -> Result<(), RpcError> {
        self.wait_for_docker_ready_with_progress(allow_start_vm, &mut |_| {})
    }

    fn wait_for_docker_ready_with_progress(
        self: &Arc<Self>,
        allow_start_vm: bool,
        progress: &mut dyn FnMut(&str),
    ) -> Result<(), RpcError> {
        let failed = |m: String| RpcError::new(api::FAILED, m);
        {
            let g = self.lock();
            if g.state == VmState::Stopped || g.state == VmState::Error {
                if !allow_start_vm {
                    return Err(failed("the VM is not running".into()));
                }
                drop(g);
                progress("starting VM");
                self.start().map_err(failed)?;
            }
        }
        let deadline = Instant::now() + Duration::from_secs(180);
        let mut last_beat = Instant::now();
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
            let step = left.min(PROGRESS_HEARTBEAT_INTERVAL);
            g = self
                .changed
                .wait_timeout(g, step)
                .unwrap_or_else(|e| e.into_inner())
                .0;
            if Instant::now() < deadline && last_beat.elapsed() >= PROGRESS_HEARTBEAT_INTERVAL {
                drop(g);
                progress("waiting for VM and Docker");
                last_beat = Instant::now();
                g = self.lock();
            }
        }
    }

    fn lock_k8s_op_with_progress(
        &self,
        progress: &mut dyn FnMut(&str),
    ) -> Result<MutexGuard<'_, ()>, RpcError> {
        const K8S_OP_WAIT_TIMEOUT: Duration = Duration::from_secs(300);
        self.lock_k8s_op_with_timeout(K8S_OP_WAIT_TIMEOUT, self.heartbeat_interval(), progress)
    }

    fn lock_k8s_op_with_timeout(
        &self,
        timeout: Duration,
        heartbeat: Duration,
        progress: &mut dyn FnMut(&str),
    ) -> Result<MutexGuard<'_, ()>, RpcError> {
        let deadline = Instant::now() + timeout;
        let mut last_beat: Option<Instant> = None;
        loop {
            match self.k8s_op.try_lock() {
                Ok(g) => return Ok(g),
                Err(std::sync::TryLockError::Poisoned(e)) => return Ok(e.into_inner()),
                Err(std::sync::TryLockError::WouldBlock) => {
                    let left = deadline.saturating_duration_since(Instant::now());
                    if left.is_zero() {
                        return Err(RpcError::new(
                            api::FAILED,
                            "timed out waiting for active Kubernetes operation",
                        ));
                    }
                    if last_beat.is_none_or(|t| t.elapsed() >= heartbeat) {
                        progress("waiting for active Kubernetes operation");
                        last_beat = Some(Instant::now());
                    }
                    thread::sleep(left.min(heartbeat).min(Duration::from_millis(100)));
                }
            }
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

        let (vm_gen, k8s_epoch, already_running_settings) = {
            let g = self.lock();
            if g.state != VmState::Running {
                return Err(failed("the VM is no longer running".into()));
            }
            let mut k = self.k8s.lock().unwrap_or_else(|e| e.into_inner());
            if k.state == "running" && k.api_proxy.is_some() {
                (g.generation, k.epoch, Some(k.started_settings.clone()))
            } else {
                k.state = "starting".into();
                k.error = None;
                (g.generation, k.epoch, None)
            }
        };
        if let Some(started) = already_running_settings {
            // Never report success while silently ignoring changed or invalid settings.
            let prepared =
                crate::k8s::prepare_start(&self.home).map_err(|e| failed(format!("{e:#}")))?;
            if started.as_ref() != Some(&prepared.settings) {
                return Err(failed(
                    "Kubernetes is already running with different k8s_version, k8s_disable, k8s_args or k3s-config.yaml settings; run `kat k8s stop` and start it again to apply them"
                        .into(),
                ));
            }
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
            crate::k8s::K8sStartSettings,
        )> {
            // Validate config.toml and k3s-config.yaml and check guest support
            // before downloading and installing k3s (the VM is already up here).
            let prepared = crate::k8s::prepare_start(&self.home)?;
            crate::k8s::ensure_start_supported(&self.home, &prepared.msg)?;
            crate::k8s::ensure_installed(&self.home, &prepared.settings.version, progress)?;
            let info = match crate::k8s::request(&self.home, &prepared.msg, progress)? {
                kat_proto::k8s::K8sMsg::StatusInfo(info) => info,
                other => anyhow::bail!("unexpected k8s start reply {other:?}"),
            };
            guest_started = true;
            let proxy = crate::k8s::ApiProxy::start(&self.home)
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
            Ok((info, proxy, dns, kc_path, prepared.settings))
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
                if let Ok(ctx) = self.kube_context_name() {
                    if let Err(re) = self.remove_kubeconfig(&ctx) {
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
            Ok((info, proxy, dns, kc_path, started_settings)) => {
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
                    k.started_settings = Some(started_settings);
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
                k.started_settings = None;
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
            k.started_settings = None;
            k.state = "stopped".into();
            k.error = None;
        }
        let _ = fs::remove_file(crate::k8s::kubeconfig_path(&self.home));
        let mut first_err: Option<anyhow::Error> = None;
        if disable_config && let Err(e) = self.persist_k8s_enabled(false) {
            first_err = Some(e);
        }
        match self.kube_context_name() {
            Ok(ctx) => {
                if let Err(e) = self.remove_kubeconfig(&ctx) {
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
            let msg = format!("{e:#}");
            self.k8s.lock().unwrap_or_else(|e| e.into_inner()).error = Some(msg.clone());
            return Err(failed(msg));
        }
        Ok(())
    }

    fn k8s_reset(
        self: &Arc<Self>,
        remove_images: bool,
        progress: &mut dyn FnMut(&str),
    ) -> Result<(), RpcError> {
        let _op = self.lock_k8s_op_with_progress(progress)?;
        let failed = |m: String| RpcError::new(api::FAILED, m);
        let ctx = self
            .kube_context_name()
            .map_err(|e| failed(format!("{e:#}")))?;
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
        let mut guest_data_deleted = false;
        let mut guest_reply_terminal = false;
        let mut guest_replied = false;
        let mut persist_res: Option<anyhow::Result<()>> = None;
        let reset_res = (|| -> Result<(), RpcError> {
            if needs_wait_docker {
                self.wait_for_docker_ready_with_progress(was_stopped, progress)?;
            }
            let mut stream =
                crate::k8s::connect(&self.home).map_err(|e| failed(format!("{e:#}")))?;
            kat_proto::k8s::write(
                &mut stream,
                &kat_proto::k8s::K8sMsg::Reset {
                    images: remove_images,
                },
            )
            .map_err(|e| failed(format!("{e:#}")))?;
            if !remove_images {
                // Legacy handling for plain resets: older guests do not emit
                // ResetAccepted before mutating cluster state.
                guest_reset_attempted = true;
            }
            let mut last_logged = String::new();
            let mut on_progress = |m: &str| {
                guest_replied = true;
                if last_logged != m {
                    dlog!("k8s reset: {m}");
                    last_logged.clear();
                    last_logged.push_str(m);
                }
                progress(m);
            };
            // Persist `kubernetes = false` immediately when the guest confirms
            // active cluster data was detached so even a daemon crash during
            // deferred reclamation or image purge cannot leave auto-start on.
            let mut on_data_deleted = || {
                if persist_res.is_none() {
                    persist_res = Some(self.persist_k8s_enabled(false));
                }
            };
            crate::k8s::read_reset_answer_terminal(
                &mut stream,
                remove_images,
                &mut guest_reset_attempted,
                &mut guest_data_deleted,
                &mut guest_reply_terminal,
                &mut on_data_deleted,
                &mut on_progress,
            )
            .map_err(|e| failed(format!("{e:#}")))?;
            Ok(())
        })();

        let stop_res = if was_stopped {
            progress("stopping temporary VM");
            let res = self.stop_with_progress(true, progress).map_err(failed);
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

        // A started reset whose reply ended without `Done` / `Error` (the guest
        // connection failed) may already have deleted cluster data without the
        // host seeing the checkpoint; disable auto-start conservatively so the
        // next boot never silently starts a fresh, empty cluster. A guest that
        // never replied at all (for example one that dropped the connection at
        // its connection limit without reading the request) did not start the
        // reset, so auto-start is left alone. Compatible guests only count as
        // started after RESET_STARTED_PROGRESS / ResetAccepted; older guests
        // count once they sent any progress.
        let outcome_unknown = reset_res.is_err()
            && guest_replied
            && guest_reset_attempted
            && !guest_data_deleted
            && !guest_reply_terminal;
        let persist_res = match persist_res {
            Some(Ok(())) => Ok(()),
            _ if guest_data_deleted || outcome_unknown => self.persist_k8s_enabled(false),
            _ => Ok(()),
        };

        // A plain reset counts as attempted only once the guest answered with
        // any frame (progress or a final Done/Error): a guest that dropped the
        // request unread (e.g. at its connection limit) sends nothing, so host
        // kubeconfig/proxy/DNS state is left untouched.
        if !remove_images && !guest_replied && !guest_reply_terminal && !guest_data_deleted {
            guest_reset_attempted = false;
        }
        let mut kubeconfig_err: Option<anyhow::Error> = None;
        if guest_reset_attempted {
            {
                let mut k = self.k8s.lock().unwrap_or_else(|e| e.into_inner());
                k.epoch += 1;
                k.api_proxy = None;
                k.dns = None;
                k.kubeconfig = None;
                k.started_settings = None;
                k.state = "stopped".into();
                k.error = None;
            }
            let _ = fs::remove_file(crate::k8s::kubeconfig_path(&self.home));
            let rm_res = self.remove_kubeconfig_with_progress(&ctx, progress);
            if let Err(e) = rm_res {
                dlog!("k8s reset: remove kubeconfig failed: {e:#}");
                kubeconfig_err = Some(e);
            }
        }

        if let Err(reset_err) = reset_res {
            if guest_data_deleted {
                let mut msg = if remove_images {
                    format!(
                        "Kubernetes cluster data was deleted, but finishing image reset failed: {}",
                        reset_err.message
                    )
                } else {
                    format!(
                        "Kubernetes cluster data was deleted, but finishing cluster reset failed: {}",
                        reset_err.message
                    )
                };
                if let Err(e) = persist_res {
                    msg.push_str(&format!(
                        " (also failed to disable Kubernetes auto-start: {e:#})"
                    ));
                }
                if let Err(e) = stop_res {
                    msg.push_str(&format!(
                        " (also failed to stop temporary VM: {})",
                        e.message
                    ));
                }
                if let Some(e) = kubeconfig_err {
                    msg.push_str(&format!(" (also failed to clean up ~/.kube/config: {e:#})"));
                }
                self.k8s.lock().unwrap_or_else(|e| e.into_inner()).error = Some(msg.clone());
                return Err(failed(msg));
            }
            if guest_reset_attempted {
                let mut msg = reset_err.message;
                if outcome_unknown {
                    match &persist_res {
                        Ok(()) => msg.push_str(
                            " (the guest may have deleted Kubernetes cluster data; Kubernetes auto-start was disabled, run `kat k8s start` to start it again)",
                        ),
                        Err(e) => msg.push_str(&format!(
                            " (the guest may have deleted Kubernetes cluster data; also failed to disable Kubernetes auto-start: {e:#})"
                        )),
                    }
                }
                if let Some(ke) = kubeconfig_err {
                    msg.push_str(&format!(
                        " (also failed to clean up ~/.kube/config: {ke:#})"
                    ));
                }
                self.k8s.lock().unwrap_or_else(|e| e.into_inner()).error = Some(msg.clone());
                return Err(failed(msg));
            }
            return Err(reset_err);
        }

        if let Err(e) = persist_res {
            let mut msg = format!("{e:#}");
            if let Err(se) = stop_res {
                msg.push_str(&format!(
                    " (also failed to stop temporary VM: {})",
                    se.message
                ));
            }
            if let Some(ke) = kubeconfig_err {
                msg.push_str(&format!(
                    " (also failed to clean up ~/.kube/config: {ke:#})"
                ));
            }
            self.k8s.lock().unwrap_or_else(|e| e.into_inner()).error = Some(msg.clone());
            return Err(failed(msg));
        }
        if let Err(se) = stop_res {
            let mut msg = se.message;
            if let Some(ke) = kubeconfig_err {
                msg.push_str(&format!(
                    " (also failed to clean up ~/.kube/config: {ke:#})"
                ));
            }
            self.k8s.lock().unwrap_or_else(|e| e.into_inner()).error = Some(msg.clone());
            return Err(failed(msg));
        }
        if let Some(ke) = kubeconfig_err {
            let msg = if remove_images {
                format!(
                    "Kubernetes cluster data and cached k8s.io images were reset, but cleaning up ~/.kube/config failed: {ke:#}"
                )
            } else {
                format!(
                    "Kubernetes cluster data was reset, but cleaning up ~/.kube/config failed: {ke:#}"
                )
            };
            self.k8s.lock().unwrap_or_else(|e| e.into_inner()).error = Some(msg.clone());
            return Err(failed(msg));
        }
        Ok(())
    }

    fn k8s_mark_version(&self, version: &str) -> Result<(), RpcError> {
        crate::config::validate_k8s_version(version)
            .map_err(|e| RpcError::new(api::INVALID_PARAMS, format!("{e:#}")))?;
        let _op = self.k8s_op.lock().unwrap_or_else(|e| e.into_inner());
        let failed = |m: String| RpcError::new(api::FAILED, m);
        match self.lock().state {
            VmState::Running => {}
            VmState::Starting => {
                return Err(failed(
                    "the VM is still starting; wait for it to finish starting with Kubernetes \
                     stopped, then retry `kat k8s mark-version`"
                        .into(),
                ));
            }
            VmState::Stopping => {
                return Err(failed(
                    "the VM is stopping; disable Kubernetes auto-start \
                     (`kat config set kubernetes=off`) if enabled, start the VM (`kat start`), \
                     then retry `kat k8s mark-version`"
                        .into(),
                ));
            }
            state @ (VmState::Stopped | VmState::Error) => {
                return Err(failed(format!(
                    "the VM is {}; `kat k8s mark-version` requires a running VM with Kubernetes \
                     stopped; disable Kubernetes auto-start (`kat config set kubernetes=off`) if \
                     enabled, start the VM (`kat start`), then retry",
                    state.as_str()
                )));
            }
        }
        {
            let k = self.k8s.lock().unwrap_or_else(|e| e.into_inner());
            if matches!(k.state.as_str(), "starting" | "running" | "stopping") {
                return Err(failed(
                    "stop Kubernetes (`kat k8s stop`) before recording the datastore version"
                        .into(),
                ));
            }
        }
        let mut noop = |_: &str| {};
        match crate::k8s::request(
            &self.home,
            &kat_proto::k8s::K8sMsg::MarkDatastoreVersion {
                version: version.to_string(),
            },
            &mut noop,
        )
        .map_err(|e| failed(format!("{e:#}")))?
        {
            kat_proto::k8s::K8sMsg::Done => {}
            other => return Err(failed(format!("unexpected mark-version reply {other:?}"))),
        }
        {
            // Clear only the failure this command resolves (an unmarked datastore);
            // any other startup failure stays visible until the next start.
            let mut k = self.k8s.lock().unwrap_or_else(|e| e.into_inner());
            if k.state == "failed"
                && k.error
                    .as_deref()
                    .is_some_and(|e| e.contains("has no version marker"))
            {
                k.state = "stopped".into();
                k.error = None;
            }
        }
        Ok(())
    }

    fn remove_kubeconfig_with_progress(
        &self,
        ctx: &str,
        progress: &mut dyn FnMut(&str),
    ) -> anyhow::Result<()> {
        progress("cleaning up kubeconfig");
        let timeout = self.kubeconfig_cleanup_timeout();
        let heartbeat = self.heartbeat_interval();
        #[cfg(test)]
        let kubectl_override = self.test_kubectl_bin.as_deref();
        #[cfg(not(test))]
        let kubectl_override: Option<&Path> = None;
        #[cfg(test)]
        if let Some(user_home) = &self.test_user_home {
            return crate::k8s::remove_kubeconfig_bounded(
                &self.home,
                Some(user_home.as_path()),
                ctx,
                kubectl_override,
                timeout,
                heartbeat,
                progress,
            );
        }
        let user_home = std::env::var_os("HOME").map(PathBuf::from);
        crate::k8s::remove_kubeconfig_bounded(
            &self.home,
            user_home.as_deref(),
            ctx,
            kubectl_override,
            timeout,
            heartbeat,
            progress,
        )
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
                let ctx = self
                    .kube_context_name()
                    .map_err(|e| failed(format!("{e:#}")))?;
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

fn send_bounded<T: serde::Serialize>(
    out: &mut UnixStream,
    msg: &T,
    timeout: Duration,
) -> io::Result<()> {
    let mut line = serde_json::to_vec(msg)?;
    line.push(b'\n');
    let deadline = Instant::now() + timeout;
    let mut written = 0;
    while written < line.len() {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "client write timed out",
            ));
        }
        out.set_write_timeout(Some(left))?;
        match out.write(&line[written..]) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "client closed connection during write",
                ));
            }
            Ok(n) => written += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(())
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
        Some(off) if off > len => (len.saturating_sub(MAX_CHUNK), None),
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
        cmdline: guest_cmdline(cfg),
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
        forward_addresses: cfg.forward_addresses.clone(),
        amd64: cfg.amd64,
    })
}

/// Kernel command-line options that carry `cfg` to the guest's init.
fn guest_cmdline(cfg: &Config) -> String {
    let mut c = format!("kat.nofile={}", cfg.nofile);
    if cfg.kubernetes {
        c.push_str(" kat.k8s=1");
    }
    c
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
        use std::os::unix::ffi::OsStringExt;
        let template = std::env::temp_dir().join(format!("kat-{name}-XXXXXX"));
        let mut bytes = template.into_os_string().into_vec();
        bytes.push(0);
        let ptr = unsafe { libc::mkdtemp(bytes.as_mut_ptr().cast()) };
        assert!(
            !ptr.is_null(),
            "mkdtemp failed: {}",
            io::Error::last_os_error()
        );
        bytes.pop();
        PathBuf::from(std::ffi::OsString::from_vec(bytes))
    }

    #[test]
    fn guest_cmdline_carries_nofile_and_kubernetes() {
        let mut cfg = Config::default();
        assert_eq!(guest_cmdline(&cfg), "kat.nofile=1048576");
        cfg.nofile = 65536;
        cfg.kubernetes = true;
        assert_eq!(guest_cmdline(&cfg), "kat.nofile=65536 kat.k8s=1");
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

        // Recreated file larger than MAX_CHUNK with offset > len is capped to MAX_CHUNK.
        let big_len = MAX_CHUNK as usize + 1024;
        fs::write(&path, vec![b'a'; big_len]).unwrap();
        let capped = read_log(
            &path,
            &LogsParams {
                offset: Some(big_len as u64 + 100),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(capped.data.len(), MAX_CHUNK as usize);
        assert_eq!(capped.offset, big_len as u64);
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
        let root = temp("k8s-stop");
        let home = root.join("h");
        let user_home = root.join("u");
        fs::create_dir_all(&home).unwrap();
        fs::create_dir_all(&user_home).unwrap();
        let d = Daemon::new_for_test(home.clone(), user_home, "katamaran-test-stop");
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

        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn k8s_reset_only_tears_down_host_state_after_reset_request_is_sent() {
        let root = temp("k8s-rst");
        let home = root.join("h");
        let user_home = root.join("u");
        fs::create_dir_all(&home).unwrap();
        fs::create_dir_all(&user_home).unwrap();
        let d = Daemon::new_for_test(home.clone(), user_home, "katamaran-test-reset");
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
        let err = d.k8s_reset(false, &mut |_| {}).unwrap_err();
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
            assert_eq!(req, kat_proto::k8s::K8sMsg::Reset { images: false });
            kat_proto::k8s::write(
                &mut conn,
                &kat_proto::k8s::K8sMsg::Error("remove data failed".into()),
            )
            .unwrap();
        });
        let err = d.k8s_reset(false, &mut |_| {}).unwrap_err();
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

        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn k8s_mark_version_validates_state_and_forwards_tag() {
        let home = temp("k8s-mark-version");
        let d = Daemon::new(home.clone());
        let sock = crate::k8s::socket_path(&home);
        fs::create_dir_all(sock.parent().unwrap()).unwrap();

        // 1. Invalid version tag is rejected with INVALID_PARAMS before touching the guest.
        let err = d.k8s_mark_version("latest").unwrap_err();
        assert_eq!(err.code, api::INVALID_PARAMS);
        let err = d.k8s_mark_version("v1.30.9+k3s1").unwrap_err();
        assert_eq!(err.code, api::INVALID_PARAMS);

        // 2. Non-running VM states are rejected without booting or toggling auto-start suppression.
        for (state, expected_hint) in [
            (VmState::Stopped, "kat config set kubernetes=off"),
            (VmState::Error, "kat start"),
            (VmState::Starting, "still starting"),
            (VmState::Stopping, "stopping"),
        ] {
            d.lock().state = state;
            let err = d.k8s_mark_version("v1.37.2+k3s1").unwrap_err();
            assert_eq!(err.code, api::FAILED, "state={state:?}");
            assert!(
                err.message.contains(expected_hint),
                "state={state:?}: {}",
                err.message
            );
            let g = d.lock();
            assert_eq!(g.state, state, "mark-version must not mutate VM state");
            assert!(g.run.is_none(), "mark-version must not boot the VM");
            assert!(!d.k8s.lock().unwrap().suppress_autostart);
        }

        // 3. Running Kubernetes cluster is rejected before contacting the guest.
        {
            let mut g = d.lock();
            g.state = VmState::Running;
            let mut k = d.k8s.lock().unwrap();
            k.state = "running".into();
        }
        let err = d.k8s_mark_version("v1.37.2+k3s1").unwrap_err();
        assert_eq!(err.code, api::FAILED);
        assert!(err.message.contains("kat k8s stop"), "{}", err.message);

        // 4. Stopped/failed cluster on a running VM forwards MarkDatastoreVersion and clears stale error.
        {
            let mut k = d.k8s.lock().unwrap();
            k.state = "failed".into();
            k.error = Some("existing cluster data has no version marker".into());
        }
        let listener = UnixListener::bind(&sock).unwrap();
        let guest = thread::spawn(move || {
            let (mut conn, _) = listener.accept().unwrap();
            let req = kat_proto::k8s::read(&mut conn).unwrap().unwrap();
            assert_eq!(
                req,
                kat_proto::k8s::K8sMsg::MarkDatastoreVersion {
                    version: "v1.37.2+k3s1".into(),
                }
            );
            kat_proto::k8s::write(&mut conn, &kat_proto::k8s::K8sMsg::Done).unwrap();
        });
        d.k8s_mark_version("v1.37.2+k3s1").unwrap();
        guest.join().unwrap();
        assert_eq!(d.lock().state, VmState::Running);
        {
            let k = d.k8s.lock().unwrap();
            assert_eq!(k.state, "stopped");
            assert!(k.error.is_none());
        }

        fs::remove_dir_all(&home).unwrap();
    }

    #[test]
    fn k8s_reset_forwards_images_flag_to_guest() {
        let root = temp("k8s-img");
        let home = root.join("h");
        let user_home = root.join("u");
        fs::create_dir_all(&home).unwrap();
        fs::create_dir_all(user_home.join(".kube")).unwrap();
        let user_kc = user_home.join(".kube/config");
        fs::write(&user_kc, "apiVersion: v1\nkind: Config\n").unwrap();
        let d = Daemon::new_for_test(home.clone(), user_home, "katamaran-test-reset-images");
        let sock = crate::k8s::socket_path(&home);
        fs::create_dir_all(sock.parent().unwrap()).unwrap();
        let listener = UnixListener::bind(&sock).unwrap();
        {
            let mut g = d.lock();
            g.state = VmState::Running;
        }
        let guest = thread::spawn(move || {
            let (mut conn, _) = listener.accept().unwrap();
            let req = kat_proto::k8s::read(&mut conn).unwrap().unwrap();
            assert_eq!(req, kat_proto::k8s::K8sMsg::Reset { images: true });
            kat_proto::k8s::write(&mut conn, &kat_proto::k8s::K8sMsg::ResetAccepted).unwrap();
            kat_proto::k8s::write(&mut conn, &kat_proto::k8s::K8sMsg::ResetDataCleared).unwrap();
            kat_proto::k8s::write(
                &mut conn,
                &kat_proto::k8s::K8sMsg::Progress("removing k8s.io images".into()),
            )
            .unwrap();
            kat_proto::k8s::write(&mut conn, &kat_proto::k8s::K8sMsg::Done).unwrap();
        });
        let mut seen_progress = Vec::new();
        d.k8s_reset(true, &mut |m| seen_progress.push(m.to_string()))
            .unwrap();
        guest.join().unwrap();
        assert_eq!(d.k8s.lock().unwrap().state, "stopped");
        assert!(
            seen_progress.iter().any(|m| m == "removing k8s.io images"),
            "missing guest progress: {seen_progress:?}"
        );

        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn k8s_reset_images_preserves_host_state_when_older_guest_closes_without_ack() {
        let root = temp("k8s-old");
        let home = root.join("h");
        let user_home = root.join("u");
        fs::create_dir_all(&home).unwrap();
        fs::create_dir_all(&user_home).unwrap();
        let cfg = Config {
            kubernetes: true,
            ..Config::default()
        };
        cfg.save(&home).unwrap();

        let d = Daemon::new_for_test(home.clone(), user_home, "katamaran-test-old-guest");
        let kc = crate::k8s::kubeconfig_path(&home);
        fs::create_dir_all(kc.parent().unwrap()).unwrap();
        fs::write(&kc, "live-kubeconfig").unwrap();
        let sock = crate::k8s::socket_path(&home);
        let listener = UnixListener::bind(&sock).unwrap();

        {
            let mut g = d.lock();
            g.state = VmState::Running;
            let mut k = d.k8s.lock().unwrap();
            k.state = "running".into();
            k.kubeconfig = Some(kc.clone());
        }

        // Simulate an older guest that rejects the trailing flag byte and closes
        // immediately without replying.
        let guest = thread::spawn(move || {
            let (mut conn, _) = listener.accept().unwrap();
            let req = kat_proto::k8s::read(&mut conn).unwrap().unwrap();
            assert_eq!(req, kat_proto::k8s::K8sMsg::Reset { images: true });
            drop(conn);
        });

        let err = d.k8s_reset(true, &mut |_| {}).unwrap_err();
        guest.join().unwrap();
        assert!(
            err.message.contains("before accepting the image reset"),
            "unexpected err: {}",
            err.message
        );
        {
            let k = d.k8s.lock().unwrap();
            assert_eq!(k.state, "running");
            assert_eq!(k.kubeconfig.as_deref(), Some(kc.as_path()));
            assert_eq!(k.epoch, 0);
            assert!(k.error.is_none());
        }
        assert!(kc.exists(), "host kubeconfig must stay intact");
        assert!(
            Config::load(&home).unwrap().kubernetes,
            "kubernetes config must stay enabled"
        );

        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn k8s_reset_disables_autostart_when_started_reset_outcome_is_unknown() {
        let root = temp("k8s-unknown");
        let home = root.join("h");
        let user_home = root.join("u");
        fs::create_dir_all(&home).unwrap();
        fs::create_dir_all(&user_home).unwrap();
        let d = Daemon::new_for_test(home.clone(), user_home, "katamaran-test-unknown");
        let kc = crate::k8s::kubeconfig_path(&home);
        fs::create_dir_all(kc.parent().unwrap()).unwrap();
        let sock = crate::k8s::socket_path(&home);
        let listener = UnixListener::bind(&sock).unwrap();

        // (guest frames, expect auto-start disabled)
        let cases: [(Vec<kat_proto::k8s::K8sMsg>, bool); 4] = [
            // Started plain reset, then the connection drops: outcome unknown.
            (
                vec![
                    kat_proto::k8s::K8sMsg::Progress(kat_proto::k8s::RESET_QUEUED_PROGRESS.into()),
                    kat_proto::k8s::K8sMsg::Progress(kat_proto::k8s::RESET_STARTED_PROGRESS.into()),
                    kat_proto::k8s::K8sMsg::Progress("removing cluster data".into()),
                ],
                true,
            ),
            // The guest drops the connection without reading or replying (for
            // example at its connection limit): nothing started.
            (vec![], false),
            // Queued only, then the connection drops: nothing started.
            (
                vec![kat_proto::k8s::K8sMsg::Progress(
                    kat_proto::k8s::RESET_QUEUED_PROGRESS.into(),
                )],
                false,
            ),
            // Started, then a clean guest error: data known to be intact.
            (
                vec![
                    kat_proto::k8s::K8sMsg::Progress(kat_proto::k8s::RESET_QUEUED_PROGRESS.into()),
                    kat_proto::k8s::K8sMsg::Progress(kat_proto::k8s::RESET_STARTED_PROGRESS.into()),
                    kat_proto::k8s::K8sMsg::Error("unmount busy".into()),
                ],
                false,
            ),
        ];
        for (frames, expect_disabled) in cases {
            let no_reply = frames.is_empty();
            Config {
                kubernetes: true,
                ..Config::default()
            }
            .save(&home)
            .unwrap();
            {
                let mut g = d.lock();
                g.state = VmState::Running;
                d.k8s.lock().unwrap().state = "running".into();
            }
            let l = listener.try_clone().unwrap();
            let guest = thread::spawn(move || {
                let (mut conn, _) = l.accept().unwrap();
                let req = kat_proto::k8s::read(&mut conn).unwrap().unwrap();
                assert_eq!(req, kat_proto::k8s::K8sMsg::Reset { images: false });
                for f in &frames {
                    kat_proto::k8s::write(&mut conn, f).unwrap();
                }
            });
            let err = d.k8s_reset(false, &mut |_| {}).unwrap_err();
            guest.join().unwrap();
            assert_eq!(
                !Config::load(&home).unwrap().kubernetes,
                expect_disabled,
                "{}",
                err.message
            );
            assert_eq!(
                err.message
                    .contains("may have deleted Kubernetes cluster data"),
                expect_disabled,
                "{}",
                err.message
            );
            if no_reply {
                assert_eq!(
                    d.k8s.lock().unwrap().state,
                    "running",
                    "host state must be kept when the guest never answered"
                );
            }
        }
    }

    #[test]
    fn k8s_reset_images_handles_post_ack_and_partial_image_cleanup_failures() {
        let root = temp("k8s-part");
        let home = root.join("h");
        let user_home = root.join("u");
        fs::create_dir_all(&home).unwrap();
        fs::create_dir_all(&user_home).unwrap();
        let d = Daemon::new_for_test(home.clone(), user_home, "katamaran-test-partial");
        let kc = crate::k8s::kubeconfig_path(&home);
        fs::create_dir_all(kc.parent().unwrap()).unwrap();
        let sock = crate::k8s::socket_path(&home);
        let listener = UnixListener::bind(&sock).unwrap();

        // 1. Failure after ResetAccepted but before ResetDataCleared: host state is torn down,
        //    but kubernetes auto-start is NOT disabled because data deletion wasn't confirmed.
        Config {
            kubernetes: true,
            ..Config::default()
        }
        .save(&home)
        .unwrap();
        fs::write(&kc, "kc-1").unwrap();
        {
            let mut g = d.lock();
            g.state = VmState::Running;
            let mut k = d.k8s.lock().unwrap();
            k.state = "running".into();
            k.kubeconfig = Some(kc.clone());
        }
        let listener_clone = listener.try_clone().unwrap();
        let guest1 = thread::spawn(move || {
            let (mut conn, _) = listener_clone.accept().unwrap();
            let req = kat_proto::k8s::read(&mut conn).unwrap().unwrap();
            assert_eq!(req, kat_proto::k8s::K8sMsg::Reset { images: true });
            kat_proto::k8s::write(&mut conn, &kat_proto::k8s::K8sMsg::ResetAccepted).unwrap();
            kat_proto::k8s::write(
                &mut conn,
                &kat_proto::k8s::K8sMsg::Error("unmount busy".into()),
            )
            .unwrap();
        });
        let err = d.k8s_reset(true, &mut |_| {}).unwrap_err();
        guest1.join().unwrap();
        assert!(err.message.contains("unmount busy"), "got: {}", err.message);
        assert_eq!(d.k8s.lock().unwrap().state, "stopped");
        assert!(!kc.exists());
        assert!(
            Config::load(&home).unwrap().kubernetes,
            "data deletion was not confirmed, so kubernetes config should remain enabled"
        );

        // 2. Partial success: ResetAccepted + ResetDataCleared, then image purge fails.
        //    Host state is torn down, kubernetes=false IS persisted, and the error reports both.
        fs::write(&kc, "kc-2").unwrap();
        {
            let mut k = d.k8s.lock().unwrap();
            k.state = "running".into();
            k.kubeconfig = Some(kc.clone());
        }
        let guest2 = thread::spawn(move || {
            let (mut conn, _) = listener.accept().unwrap();
            let req = kat_proto::k8s::read(&mut conn).unwrap().unwrap();
            assert_eq!(req, kat_proto::k8s::K8sMsg::Reset { images: true });
            kat_proto::k8s::write(&mut conn, &kat_proto::k8s::K8sMsg::ResetAccepted).unwrap();
            kat_proto::k8s::write(&mut conn, &kat_proto::k8s::K8sMsg::ResetDataCleared).unwrap();
            kat_proto::k8s::write(
                &mut conn,
                &kat_proto::k8s::K8sMsg::Error("ctr images rm (k8s.io) failed: busy".into()),
            )
            .unwrap();
        });
        let err = d.k8s_reset(true, &mut |_| {}).unwrap_err();
        guest2.join().unwrap();
        assert!(
            err.message.contains("cluster data was deleted"),
            "expected partial-outcome message, got: {}",
            err.message
        );
        assert!(
            err.message.contains("ctr images rm (k8s.io) failed: busy"),
            "expected image error detail, got: {}",
            err.message
        );
        assert_eq!(d.k8s.lock().unwrap().state, "stopped");
        assert!(!kc.exists());
        assert!(
            !Config::load(&home).unwrap().kubernetes,
            "kubernetes auto-start must be disabled after confirmed cluster data deletion"
        );

        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn serve_streams_progress_for_k8s_reset_images() {
        let root = temp("k8s-srv");
        let home = root.join("h");
        let user_home = root.join("u");
        fs::create_dir_all(&home).unwrap();
        fs::create_dir_all(&user_home).unwrap();
        let d = Daemon::new_for_test(home.clone(), user_home, "katamaran-test-serve");
        let sock = crate::k8s::socket_path(&home);
        fs::create_dir_all(sock.parent().unwrap()).unwrap();
        let guest_listener = UnixListener::bind(&sock).unwrap();
        {
            let mut g = d.lock();
            g.state = VmState::Running;
        }
        let guest = thread::spawn(move || {
            let (mut conn, _) = guest_listener.accept().unwrap();
            let req = kat_proto::k8s::read(&mut conn).unwrap().unwrap();
            assert_eq!(req, kat_proto::k8s::K8sMsg::Reset { images: true });
            kat_proto::k8s::write(&mut conn, &kat_proto::k8s::K8sMsg::ResetAccepted).unwrap();
            kat_proto::k8s::write(
                &mut conn,
                &kat_proto::k8s::K8sMsg::Progress("removing cluster data".into()),
            )
            .unwrap();
            kat_proto::k8s::write(&mut conn, &kat_proto::k8s::K8sMsg::ResetDataCleared).unwrap();
            kat_proto::k8s::write(
                &mut conn,
                &kat_proto::k8s::K8sMsg::Progress(
                    "waiting for containerd garbage collection".into(),
                ),
            )
            .unwrap();
            kat_proto::k8s::write(&mut conn, &kat_proto::k8s::K8sMsg::Done).unwrap();
        });

        let (client_sock, server_sock) = UnixStream::pair().unwrap();
        let daemon_srv = d.clone();
        let srv = thread::spawn(move || daemon_srv.serve(server_sock));

        let mut writer = client_sock.try_clone().unwrap();
        let mut line = serde_json::to_vec(&Request::new(
            1,
            &Call::K8sReset {
                images: true,
                progress: true,
            },
        ))
        .unwrap();
        line.push(b'\n');
        writer.write_all(&line).unwrap();
        writer.shutdown(std::net::Shutdown::Write).unwrap();
        drop(writer);

        let mut reader = BufReader::new(client_sock);
        let mut notifications = Vec::new();
        let mut final_resp = None;
        let mut raw = String::new();
        while reader.read_line(&mut raw).unwrap() > 0 {
            let v: Value = serde_json::from_str(&raw).unwrap();
            if v.get("id").is_none() {
                assert_eq!(v["method"], "k8s.progress");
                notifications.push(v["params"]["message"].as_str().unwrap().to_string());
            } else {
                final_resp = Some(v);
            }
            raw.clear();
        }
        srv.join().unwrap();
        guest.join().unwrap();

        assert!(
            notifications
                .iter()
                .any(|m| m == "waiting for containerd garbage collection"),
            "got notifications: {notifications:?}"
        );
        let resp = final_resp.expect("expected terminal response");
        assert_eq!(resp["id"], 1);
        assert!(resp.get("error").is_none());

        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn serve_streams_progress_for_plain_k8s_reset_only_when_requested() {
        let root = temp("k8s-plain-p");
        let home = root.join("h");
        let user_home = root.join("u");
        fs::create_dir_all(&home).unwrap();
        fs::create_dir_all(&user_home).unwrap();

        let mut d = Daemon::new_for_test(home.clone(), user_home, "katamaran-test-plain-p");
        Arc::get_mut(&mut d).unwrap().test_heartbeat_interval = Some(Duration::from_millis(30));
        let sock = crate::k8s::socket_path(&home);
        fs::create_dir_all(sock.parent().unwrap()).unwrap();
        let guest_listener = UnixListener::bind(&sock).unwrap();
        {
            let mut g = d.lock();
            g.state = VmState::Running;
            d.k8s.lock().unwrap().state = "running".into();
        }
        let guest = thread::spawn(move || {
            for _ in 0..2 {
                let (mut conn, _) = guest_listener.accept().unwrap();
                let req = kat_proto::k8s::read(&mut conn).unwrap().unwrap();
                assert_eq!(req, kat_proto::k8s::K8sMsg::Reset { images: false });
                kat_proto::k8s::write(
                    &mut conn,
                    &kat_proto::k8s::K8sMsg::Progress("removing cluster data".into()),
                )
                .unwrap();
                kat_proto::k8s::write(
                    &mut conn,
                    &kat_proto::k8s::K8sMsg::reset_data_cleared(false),
                )
                .unwrap();
                kat_proto::k8s::write(&mut conn, &kat_proto::k8s::K8sMsg::Done).unwrap();
            }
        });

        // 1. Plain reset with `progress: true` streams lock-wait and guest progress.
        let held_op = d.k8s_op.lock().unwrap();
        let (client_sock, server_sock) = UnixStream::pair().unwrap();
        let daemon_srv = d.clone();
        let srv = thread::spawn(move || daemon_srv.serve(server_sock));

        let mut writer = client_sock.try_clone().unwrap();
        let mut line = serde_json::to_vec(&Request::new(
            1,
            &Call::K8sReset {
                images: false,
                progress: true,
            },
        ))
        .unwrap();
        line.push(b'\n');
        writer.write_all(&line).unwrap();
        writer.shutdown(std::net::Shutdown::Write).unwrap();
        drop(writer);

        let mut reader = BufReader::new(client_sock);
        let mut notifications = Vec::new();
        let mut final_resp = None;
        let mut raw = String::new();
        let mut held_op = Some(held_op);
        while reader.read_line(&mut raw).unwrap() > 0 {
            let v: Value = serde_json::from_str(&raw).unwrap();
            if v.get("id").is_none() {
                assert_eq!(v["method"], "k8s.progress");
                let msg = v["params"]["message"].as_str().unwrap().to_string();
                if msg == "waiting for active Kubernetes operation" {
                    held_op.take();
                }
                notifications.push(msg);
            } else {
                final_resp = Some(v);
            }
            raw.clear();
        }
        drop(held_op);
        srv.join().unwrap();

        assert!(
            notifications
                .iter()
                .any(|m| m == "waiting for active Kubernetes operation"),
            "expected op-lock heartbeat, got: {notifications:?}"
        );
        assert!(
            notifications.iter().any(|m| m == "removing cluster data"),
            "expected guest progress, got: {notifications:?}"
        );
        let resp = final_resp.expect("expected terminal response");
        assert_eq!(resp["id"], 1);
        assert!(resp.get("error").is_none());

        // 2. Legacy plain reset without `progress` emits zero notifications.
        let (client_sock, server_sock) = UnixStream::pair().unwrap();
        let daemon_srv = d.clone();
        let srv = thread::spawn(move || daemon_srv.serve(server_sock));

        let mut writer = client_sock.try_clone().unwrap();
        let mut line = serde_json::to_vec(&Request::new(
            2,
            &Call::K8sReset {
                images: false,
                progress: false,
            },
        ))
        .unwrap();
        line.push(b'\n');
        writer.write_all(&line).unwrap();
        writer.shutdown(std::net::Shutdown::Write).unwrap();
        drop(writer);

        let mut reader = BufReader::new(client_sock);
        let mut legacy_notes = Vec::new();
        let mut legacy_resp = None;
        raw.clear();
        while reader.read_line(&mut raw).unwrap() > 0 {
            let v: Value = serde_json::from_str(&raw).unwrap();
            if v.get("id").is_none() {
                legacy_notes.push(v);
            } else {
                legacy_resp = Some(v);
            }
            raw.clear();
        }
        srv.join().unwrap();
        guest.join().unwrap();

        assert!(
            legacy_notes.is_empty(),
            "legacy k8s.reset must not emit unsolicited notifications: {legacy_notes:?}"
        );
        let resp = legacy_resp.expect("expected legacy terminal response");
        assert_eq!(resp["id"], 2);
        assert!(resp.get("error").is_none());

        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn serve_k8s_reset_images_completes_cleanup_when_client_stops_reading() {
        let root = temp("k8s-stall");
        let home = root.join("h");
        let user_home = root.join("u");
        fs::create_dir_all(&home).unwrap();
        fs::create_dir_all(&user_home).unwrap();
        Config {
            kubernetes: true,
            ..Config::default()
        }
        .save(&home)
        .unwrap();

        let mut d = Daemon::new_for_test(home.clone(), user_home, "katamaran-test-stall");
        Arc::get_mut(&mut d).unwrap().test_progress_write_timeout =
            Some(Duration::from_millis(100));
        let kc = crate::k8s::kubeconfig_path(&home);
        fs::create_dir_all(kc.parent().unwrap()).unwrap();
        fs::write(&kc, "kc-stall").unwrap();
        let sock = crate::k8s::socket_path(&home);
        let guest_listener = UnixListener::bind(&sock).unwrap();
        {
            let mut g = d.lock();
            g.state = VmState::Running;
            let mut k = d.k8s.lock().unwrap();
            k.state = "running".into();
            k.kubeconfig = Some(kc.clone());
        }

        // Guest sends enough progress frames to overflow the Unix socket buffer
        // while the client keeps the socket open without reading.
        let guest = thread::spawn(move || {
            let (mut conn, _) = guest_listener.accept().unwrap();
            let req = kat_proto::k8s::read(&mut conn).unwrap().unwrap();
            assert_eq!(req, kat_proto::k8s::K8sMsg::Reset { images: true });
            kat_proto::k8s::write(&mut conn, &kat_proto::k8s::K8sMsg::ResetAccepted).unwrap();
            let big = "p".repeat(4096);
            for _ in 0..256 {
                kat_proto::k8s::write(&mut conn, &kat_proto::k8s::K8sMsg::Progress(big.clone()))
                    .unwrap();
            }
            kat_proto::k8s::write(&mut conn, &kat_proto::k8s::K8sMsg::ResetDataCleared).unwrap();
            kat_proto::k8s::write(&mut conn, &kat_proto::k8s::K8sMsg::Done).unwrap();
        });

        let (mut client_sock, server_sock) = UnixStream::pair().unwrap();
        let daemon_srv = d.clone();
        let srv = thread::spawn(move || daemon_srv.serve(server_sock));

        let mut line = serde_json::to_vec(&Request::new(
            1,
            &Call::K8sReset {
                images: true,
                progress: true,
            },
        ))
        .unwrap();
        line.push(b'\n');
        client_sock.write_all(&line).unwrap();
        // Keep `client_sock` open without reading until the server finishes.

        let started = Instant::now();
        srv.join().unwrap();
        guest.join().unwrap();
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "serve stalled on unread client socket: {:?}",
            started.elapsed()
        );
        drop(client_sock);

        assert_eq!(d.k8s.lock().unwrap().state, "stopped");
        assert!(!kc.exists(), "host kubeconfig file must be cleaned up");
        assert!(
            !Config::load(&home).unwrap().kubernetes,
            "kubernetes=false must be persisted even when client stopped reading"
        );
        assert!(
            d.k8s_op.try_lock().is_ok(),
            "k8s_op lock must be released after reset"
        );

        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn serve_k8s_reset_images_heartbeats_and_persists_while_kubeconfig_lock_held() {
        let root = temp("k8s-flock");
        let home = root.join("h");
        let user_home = root.join("u");
        let kube_dir = user_home.join(".kube");
        fs::create_dir_all(&home).unwrap();
        fs::create_dir_all(&kube_dir).unwrap();
        fs::write(kube_dir.join("config"), "apiVersion: v1\nkind: Config\n").unwrap();
        Config {
            kubernetes: true,
            ..Config::default()
        }
        .save(&home)
        .unwrap();

        // Hold the shared kubeconfig lock before starting the reset.
        let lock_path = kube_dir.join(".katamaran-kubeconfig.lock");
        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .unwrap();
        assert_eq!(
            unsafe { libc::flock(lock_file.as_raw_fd(), libc::LOCK_EX) },
            0
        );
        let mut held_lock = Some(lock_file);

        let mut d = Daemon::new_for_test(home.clone(), user_home, "katamaran-test-flock");
        Arc::get_mut(&mut d).unwrap().test_heartbeat_interval = Some(Duration::from_millis(50));
        let sock = crate::k8s::socket_path(&home);
        fs::create_dir_all(sock.parent().unwrap()).unwrap();
        let guest_listener = UnixListener::bind(&sock).unwrap();
        {
            let mut g = d.lock();
            g.state = VmState::Running;
            d.k8s.lock().unwrap().state = "running".into();
        }
        let guest = thread::spawn(move || {
            let (mut conn, _) = guest_listener.accept().unwrap();
            let req = kat_proto::k8s::read(&mut conn).unwrap().unwrap();
            assert_eq!(req, kat_proto::k8s::K8sMsg::Reset { images: true });
            kat_proto::k8s::write(&mut conn, &kat_proto::k8s::K8sMsg::ResetAccepted).unwrap();
            kat_proto::k8s::write(&mut conn, &kat_proto::k8s::K8sMsg::ResetDataCleared).unwrap();
            kat_proto::k8s::write(&mut conn, &kat_proto::k8s::K8sMsg::Done).unwrap();
        });

        let (client_sock, server_sock) = UnixStream::pair().unwrap();
        let daemon_srv = d.clone();
        let srv = thread::spawn(move || daemon_srv.serve(server_sock));

        let mut writer = client_sock.try_clone().unwrap();
        let mut line = serde_json::to_vec(&Request::new(
            1,
            &Call::K8sReset {
                images: true,
                progress: true,
            },
        ))
        .unwrap();
        line.push(b'\n');
        writer.write_all(&line).unwrap();

        let mut reader = BufReader::new(client_sock);
        let mut lock_heartbeats = 0usize;
        let mut raw = String::new();
        let reset_resp = loop {
            raw.clear();
            assert!(reader.read_line(&mut raw).unwrap() > 0);
            let v: Value = serde_json::from_str(&raw).unwrap();
            if v.get("id").is_none() {
                assert_eq!(v["method"], "k8s.progress");
                if v["params"]["message"] == "waiting for kubeconfig lock" {
                    lock_heartbeats += 1;
                    if lock_heartbeats == 2 {
                        // Confirmed cluster deletion must ALREADY be persisted
                        // before waiting on the shared kubeconfig lock.
                        assert!(
                            !Config::load(&home).unwrap().kubernetes,
                            "kubernetes=false must be persisted before kubeconfig cleanup"
                        );
                        held_lock.take();
                    }
                }
            } else {
                break v;
            }
        };
        assert_eq!(reset_resp["id"], 1);
        assert!(reset_resp.get("error").is_none());
        assert!(
            lock_heartbeats >= 2,
            "expected periodic kubeconfig lock heartbeats, got {lock_heartbeats}"
        );

        // Verify the healthy connection can still be reused after restoring write timeout.
        let mut status_req = serde_json::to_vec(&Request::new(2, &Call::Status)).unwrap();
        status_req.push(b'\n');
        writer.write_all(&status_req).unwrap();
        writer.shutdown(std::net::Shutdown::Write).unwrap();
        drop(writer);
        raw.clear();
        assert!(reader.read_line(&mut raw).unwrap() > 0);
        let status_resp: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(status_resp["id"], 2);
        assert!(status_resp.get("error").is_none());

        srv.join().unwrap();
        guest.join().unwrap();
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn k8s_reset_images_bounds_stuck_kubeconfig_lock_and_kubectl_with_partial_outcome() {
        use std::os::unix::fs::PermissionsExt;

        // 1. Stuck shared kubeconfig lock: emits heartbeats within the finite deadline,
        //    then returns a partial outcome stating the guest reset succeeded while
        //    ~/.kube/config cleanup timed out.
        let root1 = temp("k8s-kcto1");
        let home1 = root1.join("h");
        let user1 = root1.join("u");
        let kube1 = user1.join(".kube");
        fs::create_dir_all(&home1).unwrap();
        fs::create_dir_all(&kube1).unwrap();
        fs::write(kube1.join("config"), "apiVersion: v1\nkind: Config\n").unwrap();
        Config {
            kubernetes: true,
            ..Config::default()
        }
        .save(&home1)
        .unwrap();

        let lock1 = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(kube1.join(".katamaran-kubeconfig.lock"))
            .unwrap();
        assert_eq!(unsafe { libc::flock(lock1.as_raw_fd(), libc::LOCK_EX) }, 0);

        let mut d1 = Daemon::new_for_test(home1.clone(), user1, "katamaran-test-kcto1");
        {
            let dm = Arc::get_mut(&mut d1).unwrap();
            dm.test_heartbeat_interval = Some(Duration::from_millis(40));
            dm.test_kubeconfig_timeout = Some(Duration::from_millis(180));
        }
        let sock1 = crate::k8s::socket_path(&home1);
        fs::create_dir_all(sock1.parent().unwrap()).unwrap();
        let listener1 = UnixListener::bind(&sock1).unwrap();
        {
            let mut g = d1.lock();
            g.state = VmState::Running;
            d1.k8s.lock().unwrap().state = "running".into();
        }
        let guest1 = thread::spawn(move || {
            let (mut conn, _) = listener1.accept().unwrap();
            let req = kat_proto::k8s::read(&mut conn).unwrap().unwrap();
            assert_eq!(req, kat_proto::k8s::K8sMsg::Reset { images: true });
            kat_proto::k8s::write(&mut conn, &kat_proto::k8s::K8sMsg::ResetAccepted).unwrap();
            kat_proto::k8s::write(&mut conn, &kat_proto::k8s::K8sMsg::ResetDataCleared).unwrap();
            kat_proto::k8s::write(&mut conn, &kat_proto::k8s::K8sMsg::Done).unwrap();
        });

        let mut beats = Vec::new();
        let started = Instant::now();
        let err = d1
            .k8s_reset(true, &mut |m| beats.push(m.to_string()))
            .unwrap_err();
        guest1.join().unwrap();
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "bounded cleanup exceeded deadline: {:?}",
            started.elapsed()
        );
        assert!(
            beats
                .iter()
                .filter(|m| m.as_str() == "waiting for kubeconfig lock")
                .count()
                >= 1,
            "expected lock heartbeat before timeout, got: {beats:?}"
        );
        assert!(
            err.message
                .contains("were reset, but cleaning up ~/.kube/config failed"),
            "expected partial outcome error, got: {}",
            err.message
        );
        assert!(err.message.contains("timed out"), "got: {}", err.message);
        assert!(!Config::load(&home1).unwrap().kubernetes);
        drop(lock1);
        fs::remove_dir_all(&root1).unwrap();

        // 2. Stuck kubectl subprocess: killed cleanly at the finite deadline and reports
        //    partial outcome without leaving a blocked worker thread.
        let root2 = temp("k8s-kcto2");
        let home2 = root2.join("h");
        let user2 = root2.join("u");
        let kube2 = user2.join(".kube");
        fs::create_dir_all(&home2).unwrap();
        fs::create_dir_all(&kube2).unwrap();
        fs::write(kube2.join("config"), "apiVersion: v1\nkind: Config\n").unwrap();
        let fake_kubectl = root2.join("kubectl");
        fs::write(&fake_kubectl, "#!/bin/sh\nsleep 30\n").unwrap();
        fs::set_permissions(&fake_kubectl, fs::Permissions::from_mode(0o755)).unwrap();

        let mut d2 = Daemon::new_for_test(home2.clone(), user2, "katamaran-test-kcto2");
        {
            let dm = Arc::get_mut(&mut d2).unwrap();
            dm.test_heartbeat_interval = Some(Duration::from_millis(40));
            dm.test_kubeconfig_timeout = Some(Duration::from_millis(200));
            dm.test_kubectl_bin = Some(fake_kubectl);
        }
        let sock2 = crate::k8s::socket_path(&home2);
        fs::create_dir_all(sock2.parent().unwrap()).unwrap();
        let listener2 = UnixListener::bind(&sock2).unwrap();
        {
            let mut g = d2.lock();
            g.state = VmState::Running;
            d2.k8s.lock().unwrap().state = "running".into();
        }
        let guest2 = thread::spawn(move || {
            let (mut conn, _) = listener2.accept().unwrap();
            let req = kat_proto::k8s::read(&mut conn).unwrap().unwrap();
            assert_eq!(req, kat_proto::k8s::K8sMsg::Reset { images: true });
            kat_proto::k8s::write(&mut conn, &kat_proto::k8s::K8sMsg::ResetAccepted).unwrap();
            kat_proto::k8s::write(&mut conn, &kat_proto::k8s::K8sMsg::ResetDataCleared).unwrap();
            kat_proto::k8s::write(&mut conn, &kat_proto::k8s::K8sMsg::Done).unwrap();
        });

        let mut beats2 = Vec::new();
        let started2 = Instant::now();
        let err2 = d2
            .k8s_reset(true, &mut |m| beats2.push(m.to_string()))
            .unwrap_err();
        guest2.join().unwrap();
        assert!(
            started2.elapsed() < Duration::from_secs(3),
            "hung kubectl was not killed at deadline: {:?}",
            started2.elapsed()
        );
        assert!(
            beats2
                .iter()
                .filter(|m| m.as_str() == "cleaning up kubeconfig")
                .count()
                >= 2,
            "expected heartbeats during slow kubectl, got: {beats2:?}"
        );
        assert!(
            err2.message
                .contains("were reset, but cleaning up ~/.kube/config failed"),
            "got: {}",
            err2.message
        );
        assert!(err2.message.contains("timed out"), "got: {}", err2.message);
        // Verify both the shared kubeconfig lock and the daemon's k8s_op lock were released.
        let lock2 = OpenOptions::new()
            .read(true)
            .write(true)
            .open(kube2.join(".katamaran-kubeconfig.lock"))
            .unwrap();
        let rc = unsafe { libc::flock(lock2.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        assert_eq!(
            rc,
            0,
            "kubeconfig lock must be released after kubectl timeout: {}",
            std::io::Error::last_os_error()
        );
        drop(lock2);
        assert!(
            d2.k8s_op.try_lock().is_ok(),
            "k8s_op lock must be released after kubectl timeout"
        );
        fs::remove_dir_all(&root2).unwrap();

        // 3. Plain reset with stuck shared kubeconfig lock: also reports partial outcome
        //    (without claiming cached images were cleared) and persists kubernetes = false.
        let root3 = temp("k8s-kcto3");
        let home3 = root3.join("h");
        let user3 = root3.join("u");
        let kube3 = user3.join(".kube");
        fs::create_dir_all(&home3).unwrap();
        fs::create_dir_all(&kube3).unwrap();
        fs::write(kube3.join("config"), "apiVersion: v1\nkind: Config\n").unwrap();
        Config {
            kubernetes: true,
            ..Config::default()
        }
        .save(&home3)
        .unwrap();

        let lock3 = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(kube3.join(".katamaran-kubeconfig.lock"))
            .unwrap();
        assert_eq!(unsafe { libc::flock(lock3.as_raw_fd(), libc::LOCK_EX) }, 0);

        let mut d3 = Daemon::new_for_test(home3.clone(), user3, "katamaran-test-kcto3");
        {
            let dm = Arc::get_mut(&mut d3).unwrap();
            dm.test_heartbeat_interval = Some(Duration::from_millis(40));
            dm.test_kubeconfig_timeout = Some(Duration::from_millis(180));
        }
        let sock3 = crate::k8s::socket_path(&home3);
        fs::create_dir_all(sock3.parent().unwrap()).unwrap();
        let listener3 = UnixListener::bind(&sock3).unwrap();
        {
            let mut g = d3.lock();
            g.state = VmState::Running;
            d3.k8s.lock().unwrap().state = "running".into();
        }
        let guest3 = thread::spawn(move || {
            let (mut conn, _) = listener3.accept().unwrap();
            let req = kat_proto::k8s::read(&mut conn).unwrap().unwrap();
            assert_eq!(req, kat_proto::k8s::K8sMsg::Reset { images: false });
            kat_proto::k8s::write(
                &mut conn,
                &kat_proto::k8s::K8sMsg::reset_data_cleared(false),
            )
            .unwrap();
            kat_proto::k8s::write(&mut conn, &kat_proto::k8s::K8sMsg::Done).unwrap();
        });

        let mut beats3 = Vec::new();
        let err3 = d3
            .k8s_reset(false, &mut |m| beats3.push(m.to_string()))
            .unwrap_err();
        guest3.join().unwrap();
        assert!(
            err3.message.contains(
                "Kubernetes cluster data was reset, but cleaning up ~/.kube/config failed"
            ),
            "expected plain-reset partial outcome, got: {}",
            err3.message
        );
        assert!(
            !err3.message.contains("cached k8s.io images"),
            "plain reset must not claim cached images were cleared: {}",
            err3.message
        );
        assert!(err3.message.contains("timed out"), "got: {}", err3.message);
        assert!(!Config::load(&home3).unwrap().kubernetes);
        drop(lock3);
        fs::remove_dir_all(&root3).unwrap();
    }

    #[test]
    fn lock_k8s_op_with_timeout_emits_heartbeats_and_times_out() {
        let root = temp("k8s-opto");
        let home = root.join("h");
        let user = root.join("u");
        fs::create_dir_all(&home).unwrap();
        let d = Daemon::new_for_test(home, user, "katamaran-test-opto");
        let _held = d.k8s_op.lock().unwrap();
        let mut beats = Vec::new();
        let started = Instant::now();
        let err = d
            .lock_k8s_op_with_timeout(
                Duration::from_millis(140),
                Duration::from_millis(35),
                &mut |m| beats.push(m.to_string()),
            )
            .unwrap_err();
        assert!(
            err.message
                .contains("timed out waiting for active Kubernetes operation"),
            "got: {}",
            err.message
        );
        assert!(beats.len() >= 2, "expected heartbeats, got: {beats:?}");
        assert!(started.elapsed() < Duration::from_secs(2));
        drop(_held);
        let _ok = d
            .lock_k8s_op_with_timeout(
                Duration::from_millis(100),
                Duration::from_millis(20),
                &mut |_| {},
            )
            .unwrap();
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn k8s_plain_reset_disables_autostart_when_deferred_reclamation_fails_after_detach() {
        let root = temp("k8s-det");
        let home = root.join("h");
        let user_home = root.join("u");
        fs::create_dir_all(&home).unwrap();
        Config {
            kubernetes: true,
            ..Config::default()
        }
        .save(&home)
        .unwrap();
        let d = Daemon::new_for_test(home.clone(), user_home, "katamaran-test-det");
        let sock = crate::k8s::socket_path(&home);
        fs::create_dir_all(sock.parent().unwrap()).unwrap();
        let listener = UnixListener::bind(&sock).unwrap();
        {
            let mut g = d.lock();
            g.state = VmState::Running;
            d.k8s.lock().unwrap().state = "running".into();
        }
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let home_check = home.clone();
        let guest = thread::spawn(move || {
            // 1. Reclamation error after plain-reset Progress data-cleared marker.
            let (mut conn, _) = listener.accept().unwrap();
            let req = kat_proto::k8s::read(&mut conn).unwrap().unwrap();
            assert_eq!(req, kat_proto::k8s::K8sMsg::Reset { images: false });
            kat_proto::k8s::write(
                &mut conn,
                &kat_proto::k8s::K8sMsg::reset_data_cleared(false),
            )
            .unwrap();
            // Wait until the daemon has reacted to the checkpoint before sending the
            // follow-up error — verifying `kubernetes = false` is persisted IMMEDIATELY
            // while the reset is still in flight.
            release_rx.recv().unwrap();
            assert!(
                !Config::load(&home_check).unwrap().kubernetes,
                "kubernetes=false must be persisted immediately upon data-cleared marker, before reset finishes"
            );
            kat_proto::k8s::write(
                &mut conn,
                &kat_proto::k8s::K8sMsg::Error("remove .data.removing-1: timed out".into()),
            )
            .unwrap();

            // 2. Lost connection after plain-reset Progress data-cleared marker.
            let (mut conn, _) = listener.accept().unwrap();
            let req = kat_proto::k8s::read(&mut conn).unwrap().unwrap();
            assert_eq!(req, kat_proto::k8s::K8sMsg::Reset { images: false });
            kat_proto::k8s::write(
                &mut conn,
                &kat_proto::k8s::K8sMsg::reset_data_cleared(false),
            )
            .unwrap();
            drop(conn);
        });
        let err = d
            .k8s_reset(false, &mut |m| {
                if m == kat_proto::k8s::RESET_DATA_CLEARED_PROGRESS {
                    let _ = release_tx.send(());
                }
            })
            .unwrap_err();
        assert!(
            err.message.contains("cluster data was deleted")
                && err.message.contains("finishing cluster reset failed")
                && err.message.contains("remove .data.removing-1: timed out"),
            "got: {}",
            err.message
        );
        assert!(
            !Config::load(&home).unwrap().kubernetes,
            "kubernetes auto-start must be disabled after detached datastore failure"
        );

        // Re-enable kubernetes and verify a lost guest reply after immediate
        // ResetDataCleared still disables auto-start.
        Config {
            kubernetes: true,
            ..Config::default()
        }
        .save(&home)
        .unwrap();
        let err2 = d.k8s_reset(false, &mut |_| {}).unwrap_err();
        guest.join().unwrap();
        assert!(
            err2.message.contains("cluster data was deleted")
                && err2.message.contains("closed the Kubernetes connection"),
            "got: {}",
            err2.message
        );
        assert!(
            !Config::load(&home).unwrap().kubernetes,
            "kubernetes auto-start must be disabled if guest disconnects after ResetDataCleared"
        );
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn k8s_reset_retries_persisting_autostart_disable_after_transient_checkpoint_write_failure() {
        use std::os::unix::fs::PermissionsExt;

        struct RestoreMode(PathBuf);
        impl Drop for RestoreMode {
            fn drop(&mut self) {
                let _ = fs::set_permissions(&self.0, fs::Permissions::from_mode(0o755));
            }
        }

        let root = temp("k8s-retry-p");
        let home = root.join("h");
        let user_home = root.join("u");
        let run_dir = home.join("run");
        fs::create_dir_all(&run_dir).unwrap();
        Config {
            kubernetes: true,
            ..Config::default()
        }
        .save(&home)
        .unwrap();

        let d = Daemon::new_for_test(home.clone(), user_home, "katamaran-test-retry-p");
        let sock = crate::k8s::socket_path(&home);
        let listener = UnixListener::bind(&sock).unwrap();
        {
            let mut g = d.lock();
            g.state = VmState::Running;
            d.k8s.lock().unwrap().state = "running".into();
        }

        // Make `home` read-only so the immediate `config.toml.tmp.*` write at the
        // ResetDataCleared checkpoint fails transiently, then restore writability
        // before the guest sends `Done`.
        let _restore = RestoreMode(home.clone());
        fs::set_permissions(&home, fs::Permissions::from_mode(0o555)).unwrap();

        let (checkpoint_tx, checkpoint_rx) = std::sync::mpsc::channel::<()>();
        let home_guest = home.clone();
        let guest = thread::spawn(move || {
            let (mut conn, _) = listener.accept().unwrap();
            let req = kat_proto::k8s::read(&mut conn).unwrap().unwrap();
            assert_eq!(req, kat_proto::k8s::K8sMsg::Reset { images: true });
            kat_proto::k8s::write(&mut conn, &kat_proto::k8s::K8sMsg::ResetAccepted).unwrap();
            kat_proto::k8s::write(&mut conn, &kat_proto::k8s::K8sMsg::ResetDataCleared).unwrap();
            checkpoint_rx.recv().unwrap();
            // Confirm the checkpoint save failed while `home` was read-only.
            assert!(
                Config::load(&home_guest).unwrap().kubernetes,
                "checkpoint save should have failed while home was read-only"
            );
            // Restore writability before sending Done so the completion retry succeeds.
            fs::set_permissions(&home_guest, fs::Permissions::from_mode(0o755)).unwrap();
            kat_proto::k8s::write(&mut conn, &kat_proto::k8s::K8sMsg::Done).unwrap();
        });

        d.k8s_reset(true, &mut |m| {
            if m == kat_proto::k8s::RESET_DATA_CLEARED_PROGRESS {
                let _ = checkpoint_tx.send(());
            }
        })
        .unwrap();
        guest.join().unwrap();

        assert!(
            !Config::load(&home).unwrap().kubernetes,
            "completion retry must persist kubernetes=false once home is writable again"
        );
        drop(_restore);
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn k8s_plain_reset_preserves_host_state_and_kubeconfig_when_guest_queue_times_out() {
        let root = temp("k8s-qto");
        let home = root.join("h");
        let user_home = root.join("u");
        let kube_dir = user_home.join(".kube");
        fs::create_dir_all(&home).unwrap();
        fs::create_dir_all(&kube_dir).unwrap();
        let user_cfg = kube_dir.join("config");
        fs::write(
            &user_cfg,
            "apiVersion: v1\ncurrent-context: katamaran-qto\n",
        )
        .unwrap();
        Config {
            kubernetes: true,
            ..Config::default()
        }
        .save(&home)
        .unwrap();

        let d = Daemon::new_for_test(home.clone(), user_home, "katamaran-qto");
        let kc = crate::k8s::kubeconfig_path(&home);
        fs::create_dir_all(kc.parent().unwrap()).unwrap();
        fs::write(&kc, "standalone-kc").unwrap();
        let sock = crate::k8s::socket_path(&home);
        let listener = UnixListener::bind(&sock).unwrap();
        {
            let mut g = d.lock();
            g.state = VmState::Running;
            let mut k = d.k8s.lock().unwrap();
            k.state = "running".into();
            k.kubeconfig = Some(kc.clone());
        }
        let guest = thread::spawn(move || {
            let (mut conn, _) = listener.accept().unwrap();
            let req = kat_proto::k8s::read(&mut conn).unwrap().unwrap();
            assert_eq!(req, kat_proto::k8s::K8sMsg::Reset { images: false });
            kat_proto::k8s::write(
                &mut conn,
                &kat_proto::k8s::K8sMsg::Progress(
                    "waiting for active guest Kubernetes operation".into(),
                ),
            )
            .unwrap();
            kat_proto::k8s::write(
                &mut conn,
                &kat_proto::k8s::K8sMsg::Error(format!(
                    "{}timed out waiting for active guest Kubernetes operation",
                    kat_proto::k8s::RESET_NOT_STARTED_PREFIX
                )),
            )
            .unwrap();
        });

        let mut progress = Vec::new();
        let err = d
            .k8s_reset(false, &mut |m| progress.push(m.to_string()))
            .unwrap_err();
        guest.join().unwrap();

        assert!(
            err.message
                .contains("timed out waiting for active guest Kubernetes operation"),
            "got: {}",
            err.message
        );
        assert!(
            progress
                .iter()
                .any(|m| m == "waiting for active guest Kubernetes operation"),
            "expected queued progress heartbeat, got: {progress:?}"
        );
        assert_eq!(
            d.k8s.lock().unwrap().state,
            "running",
            "runtime state must stay running when guest reset never started"
        );
        assert_eq!(
            d.k8s.lock().unwrap().kubeconfig.as_ref(),
            Some(&kc),
            "kubeconfig runtime handle must remain intact"
        );
        assert!(kc.exists(), "standalone kubeconfig file must remain intact");
        assert!(
            fs::read_to_string(&user_cfg)
                .unwrap()
                .contains("katamaran-qto"),
            "merged ~/.kube/config context must remain intact"
        );
        assert!(
            Config::load(&home).unwrap().kubernetes,
            "kubernetes auto-start must remain enabled"
        );
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn tracks_and_clears_port_forward_errors() {
        let home = temp("port-errors");
        let d = Daemon::new(home.clone());
        {
            let mut g = d.lock();
            g.generation = 1;
            g.state = VmState::Running;
            g.run = Some(Run::new(Config::default(), None));
        }
        d.on_event(
            1,
            Event::PortFailed(
                run::PortOrigin::Published,
                PortProto::Tcp,
                80,
                "address already in use".into(),
            ),
        );
        let st = d.status();
        assert!(st.ports.is_empty());
        assert_eq!(
            st.port_errors,
            vec![PortError {
                proto: "tcp".into(),
                port: 80,
                error: "address already in use".into(),
            }]
        );

        // Successful bind replaces the error and records bound addresses.
        let alias = std::net::Ipv4Addr::new(172, 16, 1, 200);
        d.on_event(
            1,
            Event::PortAdd(run::PortOrigin::Published, PortProto::Tcp, 80, vec![alias]),
        );
        let st = d.status();
        assert_eq!(
            st.ports,
            vec![Port {
                proto: "tcp".into(),
                port: 80,
                addresses: vec![alias],
            }]
        );
        assert!(st.port_errors.is_empty());

        // Another failure is cleared on PortErrorClear.
        d.on_event(
            1,
            Event::PortFailed(
                run::PortOrigin::Published,
                PortProto::Tcp,
                443,
                "address already in use".into(),
            ),
        );
        assert_eq!(d.status().port_errors.len(), 1);
        d.on_event(
            1,
            Event::PortErrorClear(run::PortOrigin::Published, PortProto::Tcp, 443),
        );
        assert!(d.status().port_errors.is_empty());

        fs::remove_dir_all(&home).unwrap();
    }

    #[test]
    fn set_config_reports_k8s_toggled_only_when_enable_state_changes() {
        let home = temp("k8s-toggled");
        let d = Daemon::new(home.clone());
        let kc = crate::k8s::kubeconfig_path(&home);
        fs::create_dir_all(kc.parent().unwrap()).unwrap();
        fs::write(&kc, "fixture-kubeconfig\n").unwrap();
        d.k8s.lock().unwrap().kubeconfig = Some(kc.clone());

        // Enabling kubernetes transitions false -> true without invoking disable cleanup.
        let mut enable = std::collections::BTreeMap::new();
        enable.insert("kubernetes".into(), "on".into());
        let mut cleaned = false;
        let v = d.set_config_with(&enable, |_| cleaned = true).unwrap();
        assert!(v.k8s_toggled);
        assert!(v.config.kubernetes);
        assert!(!cleaned);

        // Passing kubernetes=on again with new k8s_disable does NOT toggle kubernetes.
        let mut unchanged = std::collections::BTreeMap::new();
        unchanged.insert("kubernetes".into(), "on".into());
        unchanged.insert("k8s_disable".into(), "traefik".into());
        let v = d.set_config_with(&unchanged, |_| cleaned = true).unwrap();
        assert!(!v.k8s_toggled);
        assert_eq!(v.config.k8s_disable, vec!["traefik"]);
        assert!(!cleaned);

        // Disabling kubernetes transitions true -> false and runs only the injected fixture cleanup.
        let mut disable = std::collections::BTreeMap::new();
        disable.insert("kubernetes".into(), "off".into());
        let v = d
            .set_config_with(&disable, |h| {
                cleaned = true;
                let _ = fs::remove_file(crate::k8s::kubeconfig_path(h));
            })
            .unwrap();
        assert!(v.k8s_toggled);
        assert!(!v.config.kubernetes);
        assert!(cleaned);
        assert!(!kc.exists());
        assert!(d.k8s.lock().unwrap().kubeconfig.is_none());

        // ConfigGet never reports k8s_toggled and omits it from JSON.
        let get = d.handle(&Call::ConfigGet).unwrap();
        assert!(get.get("k8s_toggled").is_none());

        fs::remove_dir_all(&home).unwrap();
    }

    #[test]
    fn k8s_start_refuses_silent_noop_when_settings_changed_while_running() {
        let home = temp("k8s-run");
        fs::create_dir_all(home.join("run")).unwrap();
        let d = Daemon::new(home.clone());

        let host = Host {
            cpus: 255,
            memory_mib: 32768,
        };
        let mut cfg = Config::default();
        cfg.set("k8s_disable", "traefik", &host).unwrap();
        cfg.set("k8s_args", "--debug", &host).unwrap();
        cfg.save(&home).unwrap();
        let yaml_path = crate::k8s::k3s_config_path(&home);
        fs::write(&yaml_path, "# initial\ndisable:\n  - metrics-server\n").unwrap();

        let initial = crate::k8s::prepare_start(&home).unwrap();
        let proxy = crate::k8s::ApiProxy::start(&home).unwrap();
        {
            let mut g = d.lock();
            g.generation = 1;
            g.state = VmState::Running;
            let mut run = Run::new(cfg.clone(), None);
            run.docker = DockerState::Ready;
            g.run = Some(run);
            let mut k = d.k8s.lock().unwrap();
            k.state = "running".into();
            k.api_proxy = Some(proxy);
            k.started_settings = Some(initial.settings);
        }

        let mut noop = |_: &str| {};
        // 1. Identical settings succeed as a no-op.
        let st = d.k8s_start_inner(false, false, false, &mut noop).unwrap();
        assert_eq!(st.state, "running");

        // 2. Comment/formatting-only YAML edit still matches normalized JSON.
        fs::write(&yaml_path, "# edited comment\ndisable: [metrics-server]\n").unwrap();
        let st = d.k8s_start_inner(false, false, false, &mut noop).unwrap();
        assert_eq!(st.state, "running");

        // 3. Changing k8s_disable is refused instead of silently returning Ok,
        //    and leaves the running cluster intact.
        let mut changed_disable = cfg.clone();
        changed_disable
            .set("k8s_disable", "traefik,servicelb", &host)
            .unwrap();
        changed_disable.save(&home).unwrap();
        let err = d
            .k8s_start_inner(false, false, false, &mut noop)
            .unwrap_err();
        assert!(err.message.contains("kat k8s stop"), "got: {}", err.message);
        assert_eq!(d.k8s.lock().unwrap().state, "running");
        cfg.save(&home).unwrap();

        // 4. Changing k8s_args is refused.
        let mut changed_args = cfg.clone();
        changed_args
            .set("k8s_args", "--debug --kubelet-arg=max-pods=200", &host)
            .unwrap();
        changed_args.save(&home).unwrap();
        let err = d
            .k8s_start_inner(false, false, false, &mut noop)
            .unwrap_err();
        assert!(err.message.contains("kat k8s stop"), "got: {}", err.message);
        cfg.save(&home).unwrap();

        // 5. Changing k8s_version is refused.
        let mut changed_ver = cfg.clone();
        changed_ver
            .set("k8s_version", "v1.32.5+k3s1", &host)
            .unwrap();
        changed_ver.save(&home).unwrap();
        let err = d
            .k8s_start_inner(false, false, false, &mut noop)
            .unwrap_err();
        assert!(err.message.contains("kat k8s stop"), "got: {}", err.message);
        cfg.save(&home).unwrap();

        // 6. Changing k3s-config.yaml content is refused.
        fs::write(&yaml_path, "disable: [servicelb]\n").unwrap();
        let err = d
            .k8s_start_inner(false, false, false, &mut noop)
            .unwrap_err();
        assert!(err.message.contains("kat k8s stop"), "got: {}", err.message);

        // 7. Removing k3s-config.yaml is refused.
        fs::remove_file(&yaml_path).unwrap();
        let err = d
            .k8s_start_inner(false, false, false, &mut noop)
            .unwrap_err();
        assert!(err.message.contains("kat k8s stop"), "got: {}", err.message);

        // 8. Invalid k3s-config.yaml returns the validation error and keeps state running.
        fs::write(&yaml_path, "bind-address: 0.0.0.0\n").unwrap();
        let err = d
            .k8s_start_inner(false, false, false, &mut noop)
            .unwrap_err();
        assert!(err.message.contains("bind-address"), "got: {}", err.message);
        assert_eq!(d.k8s.lock().unwrap().state, "running");

        // Clean up ApiProxy before removing temp dir.
        d.k8s.lock().unwrap().api_proxy = None;
        fs::remove_dir_all(&home).unwrap();
    }

    #[test]
    fn published_removal_preserves_live_automatic_forward_on_same_port() {
        let home = temp("port-handoff");
        let d = Daemon::new(home.clone());
        {
            let mut g = d.lock();
            g.generation = 1;
            g.state = VmState::Running;
            g.run = Some(Run::new(Config::default(), None));
        }

        let alias = std::net::Ipv4Addr::new(172, 16, 1, 200);
        // 1. Published forward is active on port 8080.
        d.on_event(
            1,
            Event::PortAdd(
                run::PortOrigin::Published,
                PortProto::Tcp,
                8080,
                vec![std::net::Ipv4Addr::LOCALHOST, alias],
            ),
        );
        // 2. Automatic forward for the same port arrives while the listener
        // pool is full (queued with a capacity error; published stays live).
        d.on_event(
            1,
            Event::PortFailed(
                run::PortOrigin::Automatic,
                PortProto::Tcp,
                8080,
                "forward descriptor limit reached".into(),
            ),
        );
        let st = d.status();
        assert_eq!(
            st.ports,
            vec![Port {
                proto: "tcp".into(),
                port: 8080,
                addresses: vec![std::net::Ipv4Addr::LOCALHOST, alias],
            }]
        );
        assert!(
            st.port_errors.is_empty(),
            "live published port must suppress transient automatic error"
        );

        // 3. Published worker releases the listener, firing the capacity wake
        // BEFORE its Removed event reaches the session: automatic binds first.
        d.on_event(
            1,
            Event::PortAdd(
                run::PortOrigin::Automatic,
                PortProto::Tcp,
                8080,
                vec![alias],
            ),
        );
        // While both are momentarily recorded, published takes precedence.
        assert_eq!(
            d.status().ports,
            vec![Port {
                proto: "tcp".into(),
                port: 8080,
                addresses: vec![std::net::Ipv4Addr::LOCALHOST, alias],
            }]
        );

        // 4. Published Removed arrives afterwards: must clear ONLY Published,
        // keeping the live Automatic alias visible in status.
        d.on_event(
            1,
            Event::PortRemove(run::PortOrigin::Published, PortProto::Tcp, 8080),
        );
        let st = d.status();
        assert_eq!(
            st.ports,
            vec![Port {
                proto: "tcp".into(),
                port: 8080,
                addresses: vec![alias],
            }]
        );
        assert!(st.port_errors.is_empty());

        // 5. Removing Automatic clears the status entry cleanly.
        d.on_event(
            1,
            Event::PortRemove(run::PortOrigin::Automatic, PortProto::Tcp, 8080),
        );
        assert!(d.status().ports.is_empty());

        fs::remove_dir_all(&home).unwrap();
    }
}
