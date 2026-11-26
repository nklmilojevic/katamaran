//! `katamaran run`: boot the full system VM in the foreground with the system
//! image, the persistent data disk and the Docker socket, until Ctrl-C.

use std::fs::{self, File, OpenOptions};
use std::os::fd::AsRawFd;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use kat_proto::{
    AUTO_FORWARD_PORT, DOCKER_PORT, GuestStats, K8S_API_PORT, K8S_PORT, MACHINE_PORT, Msg,
    NFS_PORT, PortProto, VM_EXEC_PORT, read_msg,
};
use kat_vmm::{Disk, HostListener, Permissions, Share, VmConfig};

use crate::lifecycle::{self, ClockSync, Restarts, Watchdog};
use crate::net::{self, Gvproxy, GvproxyApi, PortChange};
use crate::nfs;
use crate::power::{self, PowerEvent};
use crate::vm::{self, ms};

#[derive(clap::Args)]
pub struct RunArgs {
    #[arg(long, default_value = "guest/out/Image")]
    pub kernel: PathBuf,
    #[arg(long, default_value = "guest/out/initramfs.cpio")]
    pub initramfs: PathBuf,
    /// Read-only system image (EROFS).
    #[arg(long, default_value = "guest/out/rootfs.erofs")]
    pub rootfs: PathBuf,
    /// Data disk template, cloned to ~/.katamaran/data/data.img on first run.
    #[arg(long, default_value = "guest/out/data.img")]
    pub data_template: PathBuf,
    /// Data disk size ceiling in GiB (grows data.img before boot, never shrinks).
    /// When omitted, keeps the template or existing image size unchanged.
    #[arg(
        long,
        value_parser = clap::value_parser!(u32)
            .range(i64::from(crate::config::MIN_DISK_GIB)..=i64::from(crate::config::MAX_DISK_GIB))
    )]
    pub disk_gib: Option<u32>,
    #[arg(long, default_value_t = 4)]
    pub cpus: u8,
    /// Guest memory ceiling. Memory the guest frees goes back to macOS, so the
    /// default is generous: half of the Mac's memory, at most 16 GiB.
    #[arg(long, default_value_t = crate::config::default_memory_mib(crate::config::Host::current()))]
    pub memory_mib: u32,
    #[arg(long, default_value = "")]
    pub cmdline: String,
    #[arg(long)]
    pub vm_bin: Option<PathBuf>,
    /// Do not share /Users into the guest.
    #[arg(long)]
    pub no_share: bool,
    /// Permission mode for /Users: caller (files owned by the caller), caller-uncached, fixed (root) or xattr.
    #[arg(long, default_value = "caller")]
    pub permissions: String,
    /// Where the guest's files appear on the Mac (default: ~/Katamaran, or
    /// $KATAMARAN_MOUNT; nothing when KATAMARAN_HOME is set).
    #[arg(long)]
    pub mount: Option<PathBuf>,
    /// Do not mount the guest's files on the Mac.
    #[arg(long)]
    pub no_mount: bool,
    /// Boot without a network device (no internet, no port forwarding).
    #[arg(long)]
    pub no_network: bool,
    /// Path to a gvproxy binary to run as its own process. By default gvproxy
    /// runs in the VM process from libgvproxy.dylib next to this executable,
    /// or, without it, from the gvproxy binary next to it.
    #[arg(long)]
    pub gvproxy: Option<PathBuf>,
    /// MTU of the guest's eth0 and gvproxy's stack.
    #[arg(long, default_value_t = net::DEFAULT_MTU)]
    pub mtu: u16,
    /// Freeze the VM's vCPUs while the Mac sleeps.
    #[arg(long)]
    pub pause_on_sleep: bool,
    /// Do not forward unpublished container ports to the Mac's forward addresses.
    #[arg(long)]
    pub no_auto_forward: bool,
    /// A Mac address forwarded ports listen on: 127.0.0.1, or an alias added
    /// to lo0 (`sudo ifconfig lo0 alias 172.16.1.200`). Repeat for several.
    #[arg(long = "forward-address", default_value = "127.0.0.1")]
    pub forward_addresses: Vec<std::net::Ipv4Addr>,
    /// How x86-64 (amd64) programs run: Rosetta, qemu-user, or not at all;
    /// auto uses Rosetta when it is installed.
    #[arg(long, value_enum, default_value_t)]
    pub amd64: crate::amd64::Emulation,
}

/// Which forwarding path owns a port status event, so concurrent handoffs
/// between published and automatic forwards on the same `(proto, port)` never
/// erase each other's live status entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum PortOrigin {
    Published,
    Automatic,
}

/// What [`run_vm`] reports while the VM runs, for the daemon's status.
#[derive(Debug)]
pub enum Event {
    /// The host-side `data.img` has been cloned (if needed) and grown (if
    /// requested); `bytes` is the final apparent size attached to the VM.
    DiskPrepared {
        bytes: u64,
    },
    Booted {
        kernel_release: String,
        vm_pid: u32,
        took: Duration,
    },
    ServiceReady(String),
    ServiceFailed(String, String),
    Mounted(PathBuf),
    MountFailed(String),
    PortAdd(PortOrigin, PortProto, u16, Vec<std::net::Ipv4Addr>),
    PortRemove(PortOrigin, PortProto, u16),
    PortFailed(PortOrigin, PortProto, u16, String),
    PortErrorClear(PortOrigin, PortProto, u16),
    Stats(GuestStats),
    /// The VM died and starts again after `delay`; [`Event::Booted`] and
    /// the rest follow as for the first boot.
    Restarting {
        why: String,
        delay: Duration,
    },
}

/// Requests to a running [`run_vm`].
#[derive(Debug)]
pub enum Control {
    Shutdown,
    /// Ask the guest for an [`Event::Stats`].
    Stats,
    /// The Mac is going to sleep or woke up. The sender, if any, is told
    /// once the VM is ready for it (paused before sleep, when configured).
    Power(PowerEvent, Option<Sender<()>>),
}

enum Input {
    /// A message from the guest of VM session `.0`, or `None` when its
    /// control channel closed.
    Guest(u64, Option<Msg>),
    Control(Control),
    /// Result of applying a port forward change for VM session `.0`.
    Forward(u64, net::Event),
    /// Result of mounting the guest's NFS share for VM session `.0`.
    Mounted(u64, PathBuf, Result<Duration, String>),
    /// Shared listener descriptors were freed for VM session `.0`; retry any
    /// capacity-blocked automatic forwards.
    ListenerCapacityAvailable(u64),
}

/// Coordinates guest `AutoForwardAdd` / `AutoForwardRemove` events with
/// [`crate::autofwd::AutoForwards`].
///
/// Level-triggered: every automatic port the guest wants but that is not
/// bound stays in `unbound`, and every signal that something on the Mac was
/// released (listener capacity, a published port removed or its refusal
/// cleared, a published remove attempted) retries all of them. Whatever
/// blocked a bind (descriptor budget, a published listener still closing,
/// another process), the forward binds as soon as a later release lets it,
/// without another `AutoForwardAdd` from the guest. A failure is reported once
/// per distinct error.
///
/// Published-to-automatic handoff: the guest sends `PortRemove` before
/// `AutoForwardAdd` for the same port, but the published worker may not have
/// closed its listener yet. While the latest published remove dispatched for
/// the port has not been attempted, no bind is tried at all, so the automatic
/// forward cannot bind only the addresses the published listener left free.
struct AutoForwardTracker {
    forwards: crate::autofwd::AutoForwards,
    /// Wanted automatic ports that are not bound, in arrival order.
    unbound: Vec<Unbound>,
    /// For each port, the sequence number of the latest published remove
    /// dispatched to the worker whose `RemoveDone` has not arrived. An
    /// earlier remove's completion may arrive after the port was re-published
    /// and removed again, so only the latest one counts.
    published_removing: std::collections::BTreeMap<(PortProto, u16), u64>,
    /// Last sequence number handed out by [`Self::next_remove_seq`].
    remove_seq: u64,
}

/// Initial delay before retrying an unbound forward that failed to bind on the
/// Mac (for example because an unrelated Mac process held the port).
const UNBOUND_RETRY_INITIAL: Duration = Duration::from_millis(250);
/// Maximum delay between retries for a still-wanted unbound forward.
const UNBOUND_RETRY_MAX: Duration = Duration::from_secs(2);
/// Maximum number of due automatic forwards retried in a single session-loop
/// turn before yielding back to dispatch queued inputs.
const UNBOUND_RETRY_BATCH: usize = 8;

/// Outcome of polling the session input channel alongside automatic-forward
/// retry deadlines.
#[derive(Debug, PartialEq, Eq)]
enum SessionPoll<T> {
    /// A queued or freshly arrived input ready to dispatch.
    Input(T),
    /// A scheduled retry deadline elapsed (either on idle timeout or yielded
    /// between queued inputs so traffic cannot starve due retries).
    RetryDue,
    /// All input senders dropped.
    Disconnected,
}

/// Interleaves session input reception with due automatic-forward retries so
/// neither a busy input queue nor a backlog of due retries can starve the
/// other.
struct SessionPoller {
    /// Alternates between receiving one queued input and running one bounded
    /// retry batch whenever both are ready. Starts `true` so a queued guest
    /// removal, published-handoff update, or shutdown is handled before an
    /// overdue retry.
    prefer_input: bool,
}

impl SessionPoller {
    fn new() -> Self {
        Self { prefer_input: true }
    }

    fn next<T>(
        &mut self,
        rx: &mpsc::Receiver<T>,
        next_retry_at: Option<Instant>,
        now: Instant,
    ) -> SessionPoll<T> {
        let Some(at) = next_retry_at else {
            self.prefer_input = true;
            return rx
                .recv()
                .map(SessionPoll::Input)
                .unwrap_or(SessionPoll::Disconnected);
        };
        if at <= now {
            if self.prefer_input {
                match rx.try_recv() {
                    Ok(msg) => {
                        self.prefer_input = false;
                        return SessionPoll::Input(msg);
                    }
                    Err(mpsc::TryRecvError::Empty) => return SessionPoll::RetryDue,
                    Err(mpsc::TryRecvError::Disconnected) => return SessionPoll::Disconnected,
                }
            }
            self.prefer_input = true;
            return SessionPoll::RetryDue;
        }
        self.prefer_input = true;
        match rx.recv_timeout(at.saturating_duration_since(now)) {
            Ok(msg) => SessionPoll::Input(msg),
            Err(mpsc::RecvTimeoutError::Timeout) => SessionPoll::RetryDue,
            Err(mpsc::RecvTimeoutError::Disconnected) => SessionPoll::Disconnected,
        }
    }
}

struct Unbound {
    key: (PortProto, u16),
    /// Errors already reported for this port, so repeated or alternating
    /// failures across retries are logged and sent to status once each.
    reported: std::collections::BTreeSet<String>,
    /// Whether the wait for a published remove was already logged.
    wait_logged: bool,
    /// When to next retry binding this port on a timer (`Some` after an I/O
    /// bind failure such as an external listener holding the port; `None`
    /// while waiting for internal capacity or a published remove).
    retry_at: Option<Instant>,
    /// Backoff for the next timer retry.
    retry_delay: Duration,
}

impl AutoForwardTracker {
    fn new(forwards: crate::autofwd::AutoForwards) -> Self {
        Self {
            forwards,
            unbound: Vec::new(),
            published_removing: std::collections::BTreeMap::new(),
            remove_seq: 0,
        }
    }

    /// The guest wants `port` forwarded.
    fn add(&mut self, proto: PortProto, port: u16, on_event: &mut dyn FnMut(Event)) {
        self.add_at(proto, port, Instant::now(), on_event);
    }

    fn add_at(
        &mut self,
        proto: PortProto,
        port: u16,
        now: Instant,
        on_event: &mut dyn FnMut(Event),
    ) {
        let key = (proto, port);
        if self.forwards.contains(port) {
            return;
        }
        if !self.unbound.iter().any(|u| u.key == key) {
            self.unbound.push(Unbound {
                key,
                reported: std::collections::BTreeSet::new(),
                wait_logged: false,
                retry_at: None,
                retry_delay: UNBOUND_RETRY_INITIAL,
            });
        }
        self.try_bind(key, now, on_event);
    }

    /// Try to bind one wanted, unbound port.
    fn try_bind(&mut self, key: (PortProto, u16), now: Instant, on_event: &mut dyn FnMut(Event)) {
        let (proto, port) = key;
        let Some(idx) = self.unbound.iter().position(|u| u.key == key) else {
            return;
        };
        if self.published_removing.contains_key(&key) {
            self.unbound[idx].retry_at = None;
            if !std::mem::replace(&mut self.unbound[idx].wait_logged, true) {
                eprintln!(
                    "katamaran: auto-forward {}:{port}/{} waits for the published forward to release the port",
                    self.forwards.label(),
                    proto.as_str()
                );
            }
            return;
        }
        let err = match self.forwards.add(port) {
            Ok(added) => {
                self.unbound.remove(idx);
                if added {
                    let addrs = self
                        .forwards
                        .bound_addrs(port)
                        .map(<[std::net::Ipv4Addr]>::to_vec)
                        .unwrap_or_else(|| vec![std::net::Ipv4Addr::LOCALHOST]);
                    println!(
                        "katamaran: auto-forwarding {}:{port}/{}",
                        net::addrs_label(&addrs),
                        proto.as_str()
                    );
                    on_event(Event::PortAdd(PortOrigin::Automatic, proto, port, addrs));
                }
                return;
            }
            Err(crate::autofwd::AddError::CapacityExhausted(reason)) => {
                self.unbound[idx].retry_at = None;
                self.unbound[idx].retry_delay = UNBOUND_RETRY_INITIAL;
                reason
            }
            Err(crate::autofwd::AddError::Io(e)) => {
                let delay = self.unbound[idx].retry_delay;
                self.unbound[idx].retry_at = Some(now + delay);
                self.unbound[idx].retry_delay = delay.saturating_mul(2).min(UNBOUND_RETRY_MAX);
                e.to_string()
            }
        };
        if !self.unbound[idx].reported.contains(&err) {
            eprintln!(
                "katamaran: cannot auto-forward {}:{port}/{} yet ({err}); will retry",
                self.forwards.label(),
                proto.as_str()
            );
            on_event(Event::PortFailed(
                PortOrigin::Automatic,
                proto,
                port,
                err.clone(),
            ));
            self.unbound[idx].reported.insert(err);
        }
    }

    /// Earliest scheduled timer retry across wanted, unbound ports.
    fn next_retry_at(&self) -> Option<Instant> {
        self.unbound.iter().filter_map(|u| u.retry_at).min()
    }

    /// Retry up to [`UNBOUND_RETRY_BATCH`] wanted, unbound ports whose timer
    /// deadline has elapsed so a large backlog of retries cannot starve input
    /// dispatch.
    fn retry_due(&mut self, now: Instant, on_event: &mut dyn FnMut(Event)) {
        let keys: Vec<_> = self
            .unbound
            .iter()
            .filter(|u| u.retry_at.is_some_and(|at| at <= now))
            .take(UNBOUND_RETRY_BATCH)
            .map(|u| u.key)
            .collect();
        for key in keys {
            self.try_bind(key, now, on_event);
        }
    }

    /// Something on the Mac was released: retry every wanted, unbound port.
    fn retry_unbound(&mut self, on_event: &mut dyn FnMut(Event)) {
        let now = Instant::now();
        for u in &mut self.unbound {
            u.retry_delay = UNBOUND_RETRY_INITIAL;
        }
        let keys: Vec<_> = self.unbound.iter().map(|u| u.key).collect();
        for key in keys {
            self.try_bind(key, now, on_event);
        }
    }

    /// The guest no longer wants `port` forwarded.
    fn remove(&mut self, proto: PortProto, port: u16, on_event: &mut dyn FnMut(Event)) {
        let key = (proto, port);
        self.unbound.retain(|u| u.key != key);
        let label = self.forwards.bound_label(port);
        if self.forwards.remove(port) {
            println!(
                "katamaran: stopped auto-forwarding {label}:{port}/{}",
                proto.as_str()
            );
            on_event(Event::PortRemove(PortOrigin::Automatic, proto, port));
            self.retry_unbound(on_event);
        } else {
            on_event(Event::PortErrorClear(PortOrigin::Automatic, proto, port));
        }
    }

    /// The sequence number for the next published remove.
    fn next_remove_seq(&mut self) -> u64 {
        self.remove_seq += 1;
        self.remove_seq
    }

    /// Published remove `seq` for `port` was dispatched to the worker.
    fn published_removing(&mut self, proto: PortProto, port: u16, seq: u64) {
        let key = (proto, port);
        self.published_removing.insert(key, seq);
        if let Some(u) = self.unbound.iter_mut().find(|u| u.key == key) {
            u.retry_at = None;
        }
    }

    /// The worker attempted published remove `seq` for `port`. If it is the
    /// latest one dispatched, binds for the port may proceed; completions of
    /// superseded removes are ignored.
    fn published_remove_done(
        &mut self,
        proto: PortProto,
        port: u16,
        seq: u64,
        on_event: &mut dyn FnMut(Event),
    ) {
        let key = (proto, port);
        if self.published_removing.get(&key) != Some(&seq) {
            return;
        }
        self.published_removing.remove(&key);
        self.retry_unbound(on_event);
    }
}

/// How a VM session ended.
enum End {
    Stopped,
    Crashed(String),
}

/// How long a sleep notification waits for the VM before letting the Mac
/// sleep anyway.
const POWER_ACK_TIMEOUT: Duration = Duration::from_secs(2);

/// Hand a sleep or wake to `control` and wait until the VM handled it.
pub fn forward_power(control: &Sender<Control>, event: PowerEvent) {
    let (ack, done) = mpsc::channel();
    if control.send(Control::Power(event, Some(ack))).is_ok() {
        let _ = done.recv_timeout(POWER_ACK_TIMEOUT);
    }
}

/// The power event a signal simulates: SIGUSR1 sleep, SIGUSR2 wake.
pub fn simulated_power(sig: i32) -> Option<PowerEvent> {
    match sig {
        libc::SIGUSR1 => Some(PowerEvent::WillSleep),
        libc::SIGUSR2 => Some(PowerEvent::Woke),
        _ => None,
    }
}

/// `katamaran run`: the VM in the foreground until SIGINT or SIGTERM.
pub fn run(args: RunArgs) -> anyhow::Result<()> {
    crate::config::validate_forward_addresses(&args.forward_addresses)?;
    let run_dir = crate::state_dir()?.join("run");
    fs::create_dir_all(&run_dir).with_context(|| format!("create {}", run_dir.display()))?;
    let _lock = lock(&run_dir.join("katamaran.lock"))?;
    // Before any thread is spawned (gvproxy's watcher is one).
    let signals = block_and_watch_signals()?;
    crate::autofwd::raise_nofile_limit();
    let (tx, rx) = mpsc::channel();
    {
        let tx = tx.clone();
        thread::spawn(move || {
            for sig in signals {
                match simulated_power(sig) {
                    Some(event) => forward_power(&tx, event),
                    None => {
                        let _ = tx.send(Control::Shutdown);
                    }
                }
            }
        });
    }
    if let Err(e) = power::watch(move |event| forward_power(&tx, event)) {
        eprintln!("katamaran: no sleep/wake notifications: {e:#}");
    }
    let relay_budget = crate::autofwd::RelayBudget::from_process_limits();
    run_vm(&args, relay_budget, rx, &mut |_| {})
}

/// Boot the VM and serve it until [`Control::Shutdown`]. A VM that dies is
/// cleaned up after and started again with backoff; `Err` means the first
/// boot failed or the VM kept dying. The caller holds the state directory's
/// lock and has blocked SIGINT and SIGTERM.
pub fn run_vm(
    args: &RunArgs,
    relay_budget: crate::autofwd::RelayBudget,
    control: Receiver<Control>,
    on_event: &mut dyn FnMut(Event),
) -> anyhow::Result<()> {
    crate::config::validate_forward_addresses(&args.forward_addresses)?;
    let (tx, rx) = mpsc::channel::<Input>();
    {
        let tx = tx.clone();
        thread::spawn(move || {
            while let Ok(c) = control.recv() {
                if tx.send(Input::Control(c)).is_err() {
                    return;
                }
            }
        });
    }
    let mut restarts = Restarts::default();
    let mut id = 0;
    loop {
        id += 1;
        let started = Instant::now();
        let why = match session(args, &relay_budget, &tx, &rx, id, on_event) {
            Ok(End::Stopped) => return Ok(()),
            Ok(End::Crashed(why)) => why,
            // A VM that never came up is a setup problem, not a crash.
            Err(e) if id == 1 => return Err(e),
            Err(e) => format!("{e:#}"),
        };
        let Some(delay) = restarts.next(started.elapsed()) else {
            bail!("the VM keeps failing ({why}); giving up");
        };
        eprintln!("katamaran: {why}; restarting the VM in {}", ms(delay));
        on_event(Event::Restarting { why, delay });
        if wait_for_stop(&rx, delay) {
            return Ok(());
        }
    }
}

/// Wait `delay` for a stop request, answering power events (there is no VM
/// to pause). Returns true if a stop came.
fn wait_for_stop(rx: &Receiver<Input>, delay: Duration) -> bool {
    let deadline = Instant::now() + delay;
    loop {
        match rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
            Ok(Input::Control(Control::Shutdown)) => return true,
            Ok(Input::Control(Control::Power(_, Some(ack)))) => {
                let _ = ack.send(());
            }
            Ok(_) => {}
            Err(_) => return false,
        }
    }
}

/// Boot one VM and serve it until it is stopped or dies. Everything the
/// session started (gvproxy, mounts, forwards, watchers) is gone when this
/// returns, and the VM process has exited.
fn session(
    args: &RunArgs,
    relay_budget: &crate::autofwd::RelayBudget,
    tx: &Sender<Input>,
    rx: &Receiver<Input>,
    id: u64,
    on_event: &mut dyn FnMut(Event),
) -> anyhow::Result<End> {
    let state = crate::state_dir()?;
    let run_dir = state.join("run");
    let logs = state.join("logs");
    let data_dir = state.join("data");
    for d in [&run_dir, &logs, &data_dir] {
        fs::create_dir_all(d).with_context(|| format!("create {}", d.display()))?;
    }
    // Restrict the run directory to the owning user before libkrun binds
    // host-side vsock Unix sockets (docker.sock, machine.sock, k8s.sock, k8s-api.sock, vm-exec.sock).
    fs::set_permissions(&run_dir, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("chmod 0700 {}", run_dir.display()))?;

    let data_img = data_dir.join("data.img");
    let disk_bytes = prepare_data_disk(&data_img, &args.data_template, args.disk_gib)?;
    on_event(Event::DiskPrepared { bytes: disk_bytes });
    let docker_sock = run_dir.join("docker.sock");
    // Unpublished container ports, relayed over vsock (see autofwd.rs).
    let auto_forward = !args.no_network && !args.no_auto_forward;
    let autofwd_sock = run_dir.join("autofwd.sock");
    // The guest's files on the Mac: (mount point, NFS port file).
    let nfs_port = nfs::port_file(&run_dir)
        .map_err(|e| eprintln!("katamaran: guest files will not be served: {e:#}"))
        .ok();
    let files = match (&nfs_port, args.no_mount) {
        (Some(port), false) => nfs::mount_point(args.mount.as_deref()).map(|d| (d, port.clone())),
        _ => None,
    };
    if let Some((dir, port_file)) = &files
        && nfs::recover(dir, port_file)?
    {
        println!("katamaran: removed a stale mount at {}", dir.display());
    }
    if let Some(port) = &nfs_port {
        let _ = fs::remove_file(port);
    }
    let console_log = logs.join("console.log");
    File::create(&console_log)?;

    let gvproxy = if args.no_network {
        None
    } else {
        // gvproxy in the VM process when built as a library, unless a
        // gvproxy binary was asked for.
        let exe = std::env::current_exe()?.canonicalize()?;
        let library = exe.with_file_name("libgvproxy.dylib");
        let log = logs.join("gvproxy.log");
        Some(match &args.gvproxy {
            None if library.is_file() => Gvproxy::in_process(&library, &run_dir, &log, args.mtu)?,
            Some(bin) => Gvproxy::start(bin, &run_dir, &log, args.mtu)?,
            None => {
                let bin = exe.with_file_name("gvproxy");
                Gvproxy::start(&bin, &run_dir, &log, args.mtu)?
            }
        })
    };

    let cfg = VmConfig {
        kernel: vm::absolute(&args.kernel)?,
        initramfs: vm::absolute(&args.initramfs)?,
        cmdline_extra: if auto_forward {
            args.cmdline.clone()
        } else {
            format!("{} kat.autoforward=0", args.cmdline)
                .trim()
                .to_string()
        },
        cpus: args.cpus,
        memory_mib: args.memory_mib,
        console_log: console_log.clone(),
        control_socket: run_dir.join("control.sock"),
        disks: vec![
            Disk {
                id: "system".into(),
                path: vm::absolute(&args.rootfs)?,
                read_only: true,
            },
            Disk {
                id: "data".into(),
                path: data_img,
                read_only: false,
            },
        ],
        host_listeners: std::iter::once(HostListener {
            port: DOCKER_PORT,
            path: docker_sock.clone(),
            kernel_only: false,
            loopback_tcp: false,
        })
        .chain(std::iter::once(HostListener {
            port: MACHINE_PORT,
            path: crate::machines::socket_path(&state),
            kernel_only: false,
            loopback_tcp: false,
        }))
        .chain(std::iter::once(HostListener {
            port: K8S_PORT,
            path: crate::k8s::socket_path(&state),
            kernel_only: false,
            loopback_tcp: false,
        }))
        .chain(std::iter::once(HostListener {
            port: K8S_API_PORT,
            path: crate::k8s::api_socket_path(&state),
            kernel_only: false,
            loopback_tcp: false,
        }))
        .chain(std::iter::once(HostListener {
            port: VM_EXEC_PORT,
            path: crate::vm_cli::socket_path(&state),
            kernel_only: false,
            loopback_tcp: false,
        }))
        .chain(nfs_port.iter().map(|path| HostListener {
            port: NFS_PORT,
            path: path.clone(),
            kernel_only: true,
            loopback_tcp: true,
        }))
        .chain(auto_forward.then(|| HostListener {
            port: AUTO_FORWARD_PORT,
            path: autofwd_sock.clone(),
            kernel_only: false,
            loopback_tcp: false,
        }))
        .collect(),
        // The Mac's /Users appears at the same path in the guest, so bind
        // mounts of host paths need no translation.
        shares: if args.no_share {
            vec![]
        } else {
            vec![Share {
                tag: "users".into(),
                host_path: "/Users".into(),
                guest_path: "/Users".into(),
                permissions: match args.permissions.as_str() {
                    "caller" => Permissions::Caller,
                    "caller-uncached" => Permissions::CallerUncached,
                    "fixed" => Permissions::Fixed { uid: 0, gid: 0 },
                    "xattr" => Permissions::Xattr,
                    other => bail!(
                        "unknown --permissions {other}; use caller, caller-uncached, fixed or xattr"
                    ),
                },
            }]
        },
        network: gvproxy.as_ref().map(Gvproxy::network),
        amd64: args.amd64.resolve_installed()?,
    };

    let vm_bin = vm::vm_bin(args.vm_bin.as_deref())?;
    let mut running = match vm::start(&cfg, &vm_bin, Duration::from_secs(60)) {
        Ok(r) => r,
        Err(e) => {
            vm::print_tail("guest console", &console_log);
            vm::print_tail("katamaran-vm", &run_dir.join("vm.log"));
            return Err(e);
        }
    };
    println!(
        "katamaran: guest {} up in {} ({} MiB, {} CPUs)",
        running.hello.kernel_release,
        ms(running.t_hello),
        running.hello.mem_total_kib / 1024,
        args.cpus
    );
    let vm_pid = running.child.id();
    on_event(Event::Booted {
        kernel_release: running.hello.kernel_release.clone(),
        vm_pid,
        took: running.t_hello,
    });
    let pid_file = run_dir.join("vm.pid");
    fs::write(&pid_file, format!("{vm_pid}\n"))?;

    for a in &args.forward_addresses {
        if !net::is_loopback_interface_addr(*a) {
            eprintln!(
                "katamaran: forward address {a} is not assigned to lo0 \
                 (sudo ifconfig lo0 alias {a}); ports will not listen on it"
            );
        }
    }
    let uses_published_relays =
        gvproxy.is_some() && args.forward_addresses != [std::net::Ipv4Addr::LOCALHOST];
    let forwarder = match &gvproxy {
        Some(g) => {
            let fwd_tx = tx.clone();
            let on_event = move |e| {
                let _ = fwd_tx.send(Input::Forward(id, e));
            };
            let api = GvproxyApi {
                socket: g.api_socket.clone(),
                guest: g.network().guest_ip,
            };
            Some(if !uses_published_relays {
                net::spawn_forwarder(api, on_event)?
            } else {
                // gvproxy's per-port sockets go next to its API socket, in
                // the private run directory.
                let sockets = g.api_socket.parent().unwrap_or(&run_dir).to_path_buf();
                net::spawn_forwarder(
                    net::AddressesApi::with_budget(
                        api,
                        &sockets,
                        &args.forward_addresses,
                        relay_budget.clone(),
                    ),
                    on_event,
                )?
            })
        }
        None => None,
    };

    let control = lifecycle::Control::new(&running.conn)?;
    // File changes on the Mac, for the paths guest processes watch.
    let shares = cfg
        .shares
        .iter()
        .map(|s| (s.guest_path.clone(), s.host_path.clone()))
        .collect();
    let watcher = crate::fswatch::Watcher::start(shares, control.clone());

    let clock = ClockSync::start(control.clone())?;
    let watchdog = Watchdog::start(control.clone())?;

    // Messages from the guest arrive on a reader thread, so the main thread
    // can also react to control requests.
    {
        let mut reader = running.conn.try_clone()?;
        let tx = tx.clone();
        thread::spawn(move || {
            loop {
                match read_msg(&mut reader) {
                    Ok(Some(m)) => {
                        if tx.send(Input::Guest(id, Some(m))).is_err() {
                            return;
                        }
                    }
                    _ => {
                        let _ = tx.send(Input::Guest(id, None));
                        return;
                    }
                }
            }
        });
    }

    let auto_limit =
        crate::autofwd::auto_conn_limit_for_budget(relay_budget, uses_published_relays);
    let auto_listener_fds =
        crate::autofwd::auto_listener_fd_limit_for_budget(relay_budget, uses_published_relays);
    let mut auto_forwards =
        AutoForwardTracker::new(crate::autofwd::AutoForwards::with_limits_and_budget(
            crate::autofwd::Upstream::AutoForward(autofwd_sock.clone()),
            &args.forward_addresses,
            auto_limit,
            auto_listener_fds,
            relay_budget.clone(),
        ));
    let _auto_capacity_sub = {
        let cap_tx = tx.clone();
        relay_budget.subscribe_listener_release(move || {
            let _ = cap_tx.send(Input::ListenerCapacityAvailable(id));
        })
    };
    let mut mounted = false;
    let mut paused = false;
    let mount_cancel = Arc::new(AtomicBool::new(false));
    let mut mount_task: Option<thread::JoinHandle<()>> = None;
    let mut poller = SessionPoller::new();
    let end = loop {
        let input = match poller.next(rx, auto_forwards.next_retry_at(), Instant::now()) {
            SessionPoll::Input(msg) => Ok(msg),
            SessionPoll::RetryDue => {
                auto_forwards.retry_due(Instant::now(), on_event);
                continue;
            }
            SessionPoll::Disconnected => Err(()),
        };
        match input {
            // From an earlier session's reader or worker.
            Ok(Input::Guest(from, _))
            | Ok(Input::Forward(from, _))
            | Ok(Input::Mounted(from, _, _))
            | Ok(Input::ListenerCapacityAvailable(from))
                if from != id => {}
            Ok(Input::Guest(_, Some(Msg::ServiceReady { name, .. }))) => {
                println!(
                    "katamaran: {name} ready after {}",
                    ms(running.t_spawn.elapsed())
                );
                // dockerd restarted by the guest is ready again, but the
                // mount is still there.
                let docker = name == "docker" && !mounted;
                on_event(Event::ServiceReady(name));
                if docker {
                    mounted = true;
                    println!("  export DOCKER_HOST=unix://{}", docker_sock.display());
                    if mount_task.is_none()
                        && let Some((dir, port_file)) = files.clone()
                    {
                        let tx = tx.clone();
                        let cancel = mount_cancel.clone();
                        match thread::Builder::new().name("nfs-mount-task".into()).spawn(
                            move || {
                                let res = nfs::mount_cancellable(&dir, &port_file, &cancel)
                                    .map_err(|e| format!("{e:#}"));
                                let _ = tx.send(Input::Mounted(id, dir, res));
                            },
                        ) {
                            Ok(h) => mount_task = Some(h),
                            Err(e) => {
                                eprintln!("katamaran: files not mounted: {e}");
                                on_event(Event::MountFailed(e.to_string()));
                            }
                        }
                    }
                    // Online discard loses what it had queued when the VM
                    // died; a trim gives that space back.
                    if id > 1 {
                        let _ = control.send(&Msg::Trim);
                    }
                }
            }
            Ok(Input::Mounted(_, dir, res)) => {
                if let Some(h) = mount_task.take() {
                    let _ = h.join();
                }
                match res {
                    Ok(t) => {
                        println!("katamaran: files mounted at {} in {}", dir.display(), ms(t));
                        on_event(Event::Mounted(dir));
                    }
                    Err(e) => {
                        eprintln!("katamaran: files not mounted: {e}");
                        on_event(Event::MountFailed(e));
                    }
                }
            }
            Ok(Input::Guest(_, Some(Msg::ServiceFailed { name, error }))) => {
                eprintln!("katamaran: {name} failed: {error}");
                on_event(Event::ServiceFailed(name, error));
            }
            Ok(Input::Guest(_, Some(Msg::PortAdd { proto, port }))) => {
                if let Some(f) = &forwarder {
                    let _ = f.send(PortChange::Add(proto, port));
                }
            }
            Ok(Input::Guest(_, Some(Msg::PortRemove { proto, port }))) => {
                if let Some(f) = &forwarder {
                    let seq = auto_forwards.next_remove_seq();
                    if f.send(PortChange::Remove(proto, port, seq)).is_ok() {
                        auto_forwards.published_removing(proto, port, seq);
                    }
                }
            }
            Ok(Input::Forward(_, net::Event::Forwarded(proto, port, addrs))) => {
                on_event(Event::PortAdd(PortOrigin::Published, proto, port, addrs));
            }
            Ok(Input::Forward(_, net::Event::Removed(proto, port))) => {
                on_event(Event::PortRemove(PortOrigin::Published, proto, port));
                auto_forwards.retry_unbound(on_event);
            }
            Ok(Input::Forward(_, net::Event::Refused(proto, port, why)))
            | Ok(Input::Forward(_, net::Event::CapacityWait(proto, port, why))) => {
                on_event(Event::PortFailed(PortOrigin::Published, proto, port, why));
            }
            Ok(Input::Forward(_, net::Event::RefusedCleared(proto, port))) => {
                on_event(Event::PortErrorClear(PortOrigin::Published, proto, port));
                auto_forwards.retry_unbound(on_event);
            }
            Ok(Input::Forward(_, net::Event::RemoveDone(proto, port, seq))) => {
                auto_forwards.published_remove_done(proto, port, seq, on_event);
            }
            Ok(Input::Forward(_, net::Event::RemoveFailed(proto, port, why))) => {
                // The guest no longer publishes the port: show the failure
                // instead of a live forward while the worker keeps retrying.
                on_event(Event::PortFailed(
                    PortOrigin::Published,
                    proto,
                    port,
                    format!("could not stop forwarding: {why}"),
                ));
            }
            Ok(Input::Forward(_, net::Event::Transient(proto, port, why))) => {
                // Sent on the first failed attempt and when retries give up;
                // a later `Forwarded` replaces it.
                on_event(Event::PortFailed(
                    PortOrigin::Published,
                    proto,
                    port,
                    format!("gvproxy did not answer: {why}"),
                ));
            }
            Ok(Input::ListenerCapacityAvailable(_)) => {
                auto_forwards.retry_unbound(on_event);
            }
            Ok(Input::Guest(_, Some(Msg::AutoForwardAdd { proto, port }))) => {
                auto_forwards.add(proto, port, on_event);
            }
            Ok(Input::Guest(_, Some(Msg::AutoForwardRemove { proto, port }))) => {
                auto_forwards.remove(proto, port, on_event);
            }
            Ok(Input::Guest(
                _,
                Some(m @ (Msg::FsWatch { .. } | Msg::FsUnwatch { .. } | Msg::FsWatchReset)),
            )) => watcher.handle(m),
            Ok(Input::Guest(_, Some(Msg::Stats(st)))) => on_event(Event::Stats(st)),
            Ok(Input::Guest(_, Some(Msg::Pong { .. }))) => watchdog.pong(),
            Ok(Input::Guest(_, Some(Msg::TrimDone { bytes, error }))) => {
                if error.is_empty() {
                    println!("katamaran: trimmed {} MiB of the data disk", bytes >> 20);
                } else {
                    eprintln!("katamaran: trim failed: {error}");
                }
            }
            Ok(Input::Control(Control::Stats)) => {
                if let Err(e) = control.send(&Msg::StatsRequest) {
                    eprintln!("katamaran: stats request: {e}");
                }
            }
            Ok(Input::Control(Control::Power(event, ack))) => {
                let sleeping = event == PowerEvent::WillSleep;
                if sleeping {
                    println!("katamaran: the Mac is going to sleep");
                } else {
                    println!("katamaran: the Mac woke up");
                }
                // Before the ack: the Mac sleeps once it has it.
                if args.pause_on_sleep {
                    match lifecycle::set_vm_paused(vm_pid as i32, sleeping) {
                        Ok(_) => paused = sleeping,
                        Err(e) => eprintln!("katamaran: pause={sleeping}: {e}"),
                    }
                }
                watchdog.set_paused(sleeping);
                if !sleeping {
                    clock.resync("wake");
                }
                if let Some(ack) = ack {
                    let _ = ack.send(());
                }
            }
            Ok(Input::Control(Control::Shutdown)) | Err(_) => {
                watcher.stop();
                watchdog.stop();
                println!("katamaran: shutting down");
                if paused && let Err(e) = lifecycle::set_vm_paused(vm_pid as i32, false) {
                    eprintln!("katamaran: resume before shutdown: {e}");
                }
                mount_cancel.store(true, Ordering::Release);
                if let Some(h) = mount_task.take() {
                    let _ = h.join();
                }
                unmount(files.as_ref(), false);
                // A stop must never turn into a restart, so failures here
                // are reported, not returned.
                let t = Instant::now();
                let _ = control.send(&Msg::Shutdown);
                match vm::wait_with_timeout(&mut running.child, lifecycle::SHUTDOWN_TIMEOUT) {
                    Ok(status) => {
                        println!("katamaran: VM stopped in {} ({status})", ms(t.elapsed()))
                    }
                    Err(e) => eprintln!("katamaran: {e:#}; killed it"),
                }
                break End::Stopped;
            }
            Ok(Input::Guest(_, Some(other))) => {
                eprintln!("katamaran: unexpected message {other:?}")
            }
            Ok(Input::Guest(_, None)) => {
                watcher.stop();
                watchdog.stop();
                mount_cancel.store(true, Ordering::Release);
                if let Some(h) = mount_task.take() {
                    let _ = h.join();
                }
                unmount(files.as_ref(), true);
                // The disk must be closed before a new VM opens it.
                let status = vm::wait_with_timeout(&mut running.child, Duration::from_secs(10))
                    .map_or_else(|e| format!("{e:#}"), |s| s.to_string());
                vm::print_tail("guest console", &console_log);
                break End::Crashed(format!("the VM exited ({status})"));
            }
        }
    };
    if !matches!(running.child.try_wait(), Ok(Some(_))) {
        let _ = running.child.kill();
        let _ = running.child.wait();
    }
    if let Some((dir, port_file)) = files.as_ref() {
        match nfs::finish_shutdown_cleanup(dir, port_file, Duration::from_secs(2)) {
            Ok(true) => println!("katamaran: unmounted late NFS mount {}", dir.display()),
            Ok(false) => {}
            Err(e) => eprintln!("katamaran: {e:#}"),
        }
    }
    let _ = fs::remove_file(&pid_file);
    Ok(end)
}

/// Unmount the guest's files before the guest goes away, so nothing on the
/// Mac is left waiting on a dead server.
fn unmount(files: Option<&(PathBuf, PathBuf)>, force: bool) {
    let Some((dir, port_file)) = files else {
        return;
    };
    let res = if force {
        nfs::unmount_force(dir, port_file)
    } else {
        nfs::unmount(dir, port_file)
    };
    match res {
        Ok(true) => println!("katamaran: unmounted {}", dir.display()),
        Ok(false) => {}
        Err(e) => eprintln!("katamaran: unmount {}: {e:#}", dir.display()),
    }
}

/// Clone the data-disk template. On APFS `fs::copy` uses `fclonefileat`, so
/// the copy is instant and stays sparse.
fn clone_template(template: &Path, dest: &Path) -> anyhow::Result<()> {
    if !template.is_file() {
        bail!(
            "data disk template not found at {}; build it with guest/disk/build.sh",
            template.display()
        );
    }
    if template
        .extension()
        .is_some_and(|e| e == crate::sparse::EXTENSION)
    {
        crate::sparse::unpack(template, dest)?;
        println!("katamaran: created data disk {}", dest.display());
        return Ok(());
    }
    let tmp = dest.with_extension("img.tmp");
    let res = fs::copy(template, &tmp)
        .with_context(|| format!("clone {} to {}", template.display(), tmp.display()))
        .and_then(|_| fs::rename(&tmp, dest).map_err(anyhow::Error::from));
    if let Err(e) = res {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    println!("katamaran: created data disk {}", dest.display());
    Ok(())
}

/// Ensure `data_img` exists (cloning `data_template` if missing) and grow it
/// to `target_gib` GiB when requested. Validates `target_gib` before cloning
/// the template so an invalid size never leaves a stray `data.img`. Returns
/// the final apparent size of `data_img` in bytes.
pub(crate) fn prepare_data_disk(
    data_img: &Path,
    data_template: &Path,
    target_gib: Option<u32>,
) -> anyhow::Result<u64> {
    if let Some(gib) = target_gib
        && !(crate::config::MIN_DISK_GIB..=crate::config::MAX_DISK_GIB).contains(&gib)
    {
        bail!(
            "disk_gib must be between {} and {}",
            crate::config::MIN_DISK_GIB,
            crate::config::MAX_DISK_GIB
        );
    }
    if !data_img.exists() {
        clone_template(&vm::absolute(data_template)?, data_img)?;
    }
    match target_gib {
        Some(gib) => grow_data_disk(data_img, gib),
        None => Ok(fs::metadata(data_img)
            .with_context(|| format!("stat {}", data_img.display()))?
            .len()),
    }
}

/// Extend `data.img` to `target_gib` GiB (sparse on APFS). Never shrinks an
/// existing image. Returns the final apparent size in bytes.
fn grow_data_disk(path: &Path, target_gib: u32) -> anyhow::Result<u64> {
    if !(crate::config::MIN_DISK_GIB..=crate::config::MAX_DISK_GIB).contains(&target_gib) {
        bail!(
            "disk_gib must be between {} and {}",
            crate::config::MIN_DISK_GIB,
            crate::config::MAX_DISK_GIB
        );
    }
    let target_bytes = u64::from(target_gib) << 30;
    let current_bytes = fs::metadata(path)
        .with_context(|| format!("stat {}", path.display()))?
        .len();
    if target_bytes == current_bytes {
        return Ok(current_bytes);
    }
    if target_bytes < current_bytes {
        eprintln!(
            "katamaran: data disk {} is already {} GiB; keeping current size (shrinking is not supported)",
            path.display(),
            current_bytes.div_ceil(1 << 30)
        );
        return Ok(current_bytes);
    }
    let f = OpenOptions::new()
        .write(true)
        .open(path)
        .with_context(|| format!("open {}", path.display()))?;
    f.set_len(target_bytes)
        .with_context(|| format!("grow {} to {target_gib} GiB", path.display()))?;
    println!(
        "katamaran: grew data disk {} ({} GiB -> {target_gib} GiB)",
        path.display(),
        current_bytes >> 30
    );
    Ok(target_bytes)
}

/// Only one `katamaran run` may own the state directory at a time.
pub fn lock(path: &Path) -> anyhow::Result<File> {
    let f = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)?;
    if unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        bail!("another katamaran is already running ({})", path.display());
    }
    Ok(f)
}

/// Block SIGINT, SIGTERM, SIGUSR1 and SIGUSR2 (simulated sleep and wake,
/// see [`simulated_power`]) in every thread and deliver them on a channel
/// instead. Must run before any other thread is spawned.
pub fn block_and_watch_signals() -> anyhow::Result<mpsc::Receiver<i32>> {
    let mut set: libc::sigset_t = unsafe { std::mem::zeroed() };
    unsafe {
        // A shell starts background jobs with SIGINT ignored, and an ignored
        // signal is discarded instead of being delivered to sigwait.
        libc::signal(libc::SIGINT, libc::SIG_DFL);
        libc::signal(libc::SIGTERM, libc::SIG_DFL);
        libc::sigemptyset(&mut set);
        libc::sigaddset(&mut set, libc::SIGINT);
        libc::sigaddset(&mut set, libc::SIGTERM);
        libc::sigaddset(&mut set, libc::SIGUSR1);
        libc::sigaddset(&mut set, libc::SIGUSR2);
        if libc::pthread_sigmask(libc::SIG_BLOCK, &set, std::ptr::null_mut()) != 0 {
            bail!("pthread_sigmask failed");
        }
    }
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        loop {
            let mut sig = 0;
            if unsafe { libc::sigwait(&set, &mut sig) } == 0 && tx.send(sig).is_err() {
                return;
            }
        }
    });
    Ok(rx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use std::io::{Read, Write};
    use std::os::unix::fs::MetadataExt;

    #[derive(Parser)]
    struct Wrapper {
        #[command(flatten)]
        args: RunArgs,
    }

    #[test]
    fn foreground_run_validates_forward_addresses_before_setup() {
        for bad in [
            &[
                "prog",
                "--forward-address",
                "127.0.0.1",
                "--forward-address",
                "127.0.0.1",
            ][..],
            &["prog", "--forward-address", "0.0.0.0"][..],
            &["prog", "--forward-address", "224.0.0.1"][..],
            &["prog", "--forward-address", "255.255.255.255"][..],
        ] {
            let parsed = Wrapper::try_parse_from(bad).unwrap();
            let err = run(parsed.args).unwrap_err();
            assert!(
                err.to_string().contains("forward_addresses"),
                "expected forward_addresses validation error for {bad:?}, got {err:#}"
            );
        }
    }

    #[test]
    fn auto_forward_waits_for_published_release_during_handoff() {
        use std::net::{Ipv4Addr, TcpListener};

        // Ports from a per-process range outside the ephemeral one, so a
        // parallel test cannot take a port this test drops and rebinds.
        let mut next = 41_000 + ((std::process::id() % 1000) as u16) * 16;
        let mut bind_fresh = || loop {
            let port = next;
            next += 1;
            if let Ok(l) = TcpListener::bind(("127.0.0.1", port)) {
                return (l, port);
            }
        };

        let budget = crate::autofwd::RelayBudget::with_limits(64, 16);
        let mut tracker =
            AutoForwardTracker::new(crate::autofwd::AutoForwards::with_limits_and_budget(
                crate::autofwd::Upstream::AutoForward(PathBuf::from("/nonexistent")),
                &[Ipv4Addr::LOCALHOST],
                crate::autofwd::ConnLimit::Shared(4),
                None,
                budget,
            ));
        let mut events = Vec::new();
        let bound_auto = |events: &[Event], port: u16| {
            matches!(
                events,
                [Event::PortAdd(PortOrigin::Automatic, PortProto::Tcp, p, a)]
                    if *p == port && a == &[Ipv4Addr::LOCALHOST]
            )
        };
        let failed_auto = |events: &[Event], port: u16| {
            matches!(
                events,
                [Event::PortFailed(PortOrigin::Automatic, PortProto::Tcp, p, _)] if *p == port
            )
        };
        let tcp = PortProto::Tcp;
        let unbound =
            |t: &AutoForwardTracker| -> Vec<u16> { t.unbound.iter().map(|u| u.key.1).collect() };

        // The worker has bound the published port, but its events are queued
        // behind the guest's PortRemove and AutoForwardAdd: the automatic add
        // waits without attempting a bind, then binds once the remove has
        // been attempted, without a second AutoForwardAdd.
        let (published, port) = bind_fresh();
        let seq = tracker.next_remove_seq();
        tracker.published_removing(tcp, port, seq);
        tracker.add(tcp, port, &mut |e| events.push(e));
        assert!(events.is_empty(), "handoff conflict reported: {events:?}");
        assert_eq!(unbound(&tracker), vec![port]);
        drop(published);
        tracker.published_remove_done(tcp, port, seq, &mut |e| events.push(e));
        assert!(bound_auto(&events, port), "{events:?}");
        assert!(tracker.unbound.is_empty());
        events.clear();
        tracker.remove(tcp, port, &mut |e| events.push(e));
        events.clear();

        // Remove, re-publish, remove again: the first remove's completion
        // arrives while the second published listener still owns the port.
        // Only the latest remove's completion lets the bind proceed.
        let (first, port) = bind_fresh();
        let seq1 = tracker.next_remove_seq();
        tracker.published_removing(tcp, port, seq1);
        let seq2 = tracker.next_remove_seq();
        tracker.published_removing(tcp, port, seq2);
        tracker.add(tcp, port, &mut |e| events.push(e));
        drop(first);
        let second = TcpListener::bind(("127.0.0.1", port)).unwrap();
        tracker.published_remove_done(tcp, port, seq1, &mut |e| events.push(e));
        assert!(events.is_empty(), "stale completion acted on: {events:?}");
        assert!(!tracker.forwards.contains(port));
        drop(second);
        tracker.published_remove_done(tcp, port, seq2, &mut |e| events.push(e));
        assert!(bound_auto(&events, port), "{events:?}");
        events.clear();
        tracker.remove(tcp, port, &mut |e| events.push(e));
        events.clear();

        // While a remove is in flight no bind is attempted even if the port
        // looks free, so a published listener still holding some addresses
        // cannot leave a partial automatic bind behind.
        let (free, port) = bind_fresh();
        drop(free);
        let seq = tracker.next_remove_seq();
        tracker.published_removing(tcp, port, seq);
        tracker.add(tcp, port, &mut |e| events.push(e));
        assert!(events.is_empty());
        assert!(!tracker.forwards.contains(port));
        tracker.published_remove_done(tcp, port, seq, &mut |e| events.push(e));
        assert!(bound_auto(&events, port), "{events:?}");
        events.clear();
        tracker.remove(tcp, port, &mut |e| events.push(e));
        events.clear();

        // If the guest drops the automatic port first, a later release does
        // not resurrect it.
        let (published, port) = bind_fresh();
        let seq = tracker.next_remove_seq();
        tracker.published_removing(tcp, port, seq);
        tracker.add(tcp, port, &mut |e| events.push(e));
        tracker.remove(tcp, port, &mut |e| events.push(e));
        assert!(tracker.unbound.is_empty());
        events.clear();
        drop(published);
        tracker.published_remove_done(tcp, port, seq, &mut |e| events.push(e));
        tracker.retry_unbound(&mut |e| events.push(e));
        assert!(events.is_empty());
        assert!(!tracker.forwards.contains(port));

        // The first remove attempt failed, so the published listener is still
        // bound. Whether the automatic add arrives before or after that
        // attempt, its bind fails and is reported once (not again on
        // unrelated releases), then succeeds when a background retry reports
        // the port removed.
        for add_after_done in [false, true] {
            let (stuck, port) = bind_fresh();
            let seq = tracker.next_remove_seq();
            tracker.published_removing(tcp, port, seq);
            if add_after_done {
                tracker.published_remove_done(tcp, port, seq, &mut |e| events.push(e));
                tracker.add(tcp, port, &mut |e| events.push(e));
            } else {
                tracker.add(tcp, port, &mut |e| events.push(e));
                tracker.published_remove_done(tcp, port, seq, &mut |e| events.push(e));
            }
            assert!(failed_auto(&events, port), "{events:?}");
            events.clear();
            tracker.retry_unbound(&mut |e| events.push(e));
            assert!(events.is_empty(), "same failure reported again: {events:?}");
            drop(stuck);
            tracker.retry_unbound(&mut |e| events.push(e));
            assert!(bound_auto(&events, port), "{events:?}");
            events.clear();
            tracker.remove(tcp, port, &mut |e| events.push(e));
            events.clear();
        }
    }

    #[test]
    fn auto_forward_tracker_resumes_capacity_blocked_port_and_cancels_on_remove() {
        use std::io::{Read, Write};
        use std::net::{Ipv4Addr, Shutdown, TcpListener, TcpStream};
        use std::os::unix::net::UnixListener;

        let dir = std::env::temp_dir().join(format!("kat-run-autofwd-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        // Tiny guest echo server for Upstream::AutoForward.
        let guest_sock = dir.join("autofwd.sock");
        let guest_listener = UnixListener::bind(&guest_sock).unwrap();
        let guest_stop = Arc::new(AtomicBool::new(false));
        let g_stop = guest_stop.clone();
        let g_path = guest_sock.clone();
        let guest_thread = thread::spawn(move || {
            while let Ok((mut s, _)) = guest_listener.accept() {
                if g_stop.load(Ordering::SeqCst) {
                    return;
                }
                let mut hdr = [0u8; 2];
                if s.read_exact(&mut hdr).is_err() {
                    continue;
                }
                let p = u16::from_le_bytes(hdr);
                let _ = s.write_all(format!("{p}:").as_bytes());
                let mut data = Vec::new();
                let _ = s.read_to_end(&mut data);
                let _ = s.write_all(&data);
                let _ = s.shutdown(Shutdown::Write);
            }
        });

        // Pick distinct high ports without opening throwaway listening sockets
        // (which a parallel test subprocess could briefly inherit during
        // macOS's non-atomic `socket()` + `FIOCLEX` window).
        let mut next_port = 38_000 + ((std::process::id() % 1000) as u16) * 16;
        let mut free_port = || {
            loop {
                let candidate = next_port;
                next_port += 1;
                if TcpStream::connect_timeout(
                    &std::net::SocketAddr::from((Ipv4Addr::LOCALHOST, candidate)),
                    Duration::from_millis(20),
                )
                .is_err()
                {
                    return candidate;
                }
            }
        };

        // Shared budget of 6 listener fds (2 single-address ports total).
        let budget = crate::autofwd::RelayBudget::with_limits(6, 4);
        let mut tracker =
            AutoForwardTracker::new(crate::autofwd::AutoForwards::with_limits_and_budget(
                crate::autofwd::Upstream::AutoForward(guest_sock),
                &[Ipv4Addr::LOCALHOST],
                crate::autofwd::ConnLimit::Shared(4),
                None,
                budget.clone(),
            ));
        let mut events = Vec::new();

        let p1 = free_port();
        let p2 = free_port();
        let p3 = free_port();
        let p4 = free_port();

        // A host conflict (port already in use) is reported and the port stays
        // wanted, to be retried on later releases.
        // From the test's own range, not the ephemeral one, so a parallel
        // test cannot take it once `busy` is dropped below.
        let busy_port = free_port();
        let busy = TcpListener::bind(("127.0.0.1", busy_port)).unwrap();
        tracker.add(PortProto::Tcp, busy_port, &mut |e| events.push(e));
        assert!(matches!(
            events.pop(),
            Some(Event::PortFailed(PortOrigin::Automatic, PortProto::Tcp, p, _)) if p == busy_port
        ));
        assert!(
            tracker
                .unbound
                .iter()
                .any(|u| u.key == (PortProto::Tcp, busy_port))
        );

        // Fill both available listener slots.
        tracker.add(PortProto::Tcp, p1, &mut |e| events.push(e));
        assert!(matches!(
            events.pop(),
            Some(Event::PortAdd(PortOrigin::Automatic, PortProto::Tcp, p, ref a)) if p == p1 && a == &[Ipv4Addr::LOCALHOST]
        ));
        tracker.add(PortProto::Tcp, p2, &mut |e| events.push(e));
        assert!(matches!(
            events.pop(),
            Some(Event::PortAdd(PortOrigin::Automatic, PortProto::Tcp, p, ref a)) if p == p2 && a == &[Ipv4Addr::LOCALHOST]
        ));
        assert_eq!(budget.listener_fds(), 6);

        // Next two ports are capacity-blocked and retained as pending.
        tracker.add(PortProto::Tcp, p3, &mut |e| events.push(e));
        assert!(matches!(
            events.pop(),
            Some(Event::PortFailed(PortOrigin::Automatic, PortProto::Tcp, p, _)) if p == p3
        ));
        tracker.add(PortProto::Tcp, p4, &mut |e| events.push(e));
        assert!(matches!(
            events.pop(),
            Some(Event::PortFailed(PortOrigin::Automatic, PortProto::Tcp, p, _)) if p == p4
        ));

        // Guest cancels p4 before any slot frees up.
        tracker.remove(PortProto::Tcp, p4, &mut |e| events.push(e));
        assert!(matches!(
            events.pop(),
            Some(Event::PortErrorClear(PortOrigin::Automatic, PortProto::Tcp, p)) if p == p4
        ));

        // Stopping p1 frees 3 descriptors and automatically resumes p3 without
        // a second AutoForwardAdd from the guest.
        tracker.remove(PortProto::Tcp, p1, &mut |e| events.push(e));
        assert_eq!(events.len(), 2);
        assert!(matches!(
            events[0],
            Event::PortRemove(PortOrigin::Automatic, PortProto::Tcp, p) if p == p1
        ));
        assert!(matches!(
            events[1],
            Event::PortAdd(PortOrigin::Automatic, PortProto::Tcp, p, ref a) if p == p3 && a == &[Ipv4Addr::LOCALHOST]
        ));
        events.clear();
        assert!(!tracker.forwards.contains(p4));
        assert!(!tracker.forwards.contains(busy_port));

        let mut s = TcpStream::connect(("127.0.0.1", p3)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        s.write_all(b"ok").unwrap();
        s.shutdown(Shutdown::Write).unwrap();
        let mut resp = String::new();
        s.read_to_string(&mut resp).unwrap();
        assert_eq!(resp, format!("{p3}:ok"));

        // Once the other process frees the conflicting port, the next release
        // binds it without another AutoForwardAdd.
        drop(busy);
        tracker.remove(PortProto::Tcp, p3, &mut |e| events.push(e));
        assert!(matches!(
            events.as_slice(),
            [
                Event::PortRemove(PortOrigin::Automatic, PortProto::Tcp, a),
                Event::PortAdd(PortOrigin::Automatic, PortProto::Tcp, b, _),
            ] if *a == p3 && *b == busy_port
        ));
        events.clear();

        // Cross-forwarder release: a published forwarder releasing a slot fires
        // the subscription callback so `retry_unbound` resumes an auto port.
        let (wake_tx, wake_rx) = mpsc::channel();
        let _sub = budget.subscribe_listener_release(move || {
            let _ = wake_tx.send(());
        });
        tracker.remove(PortProto::Tcp, p2, &mut |e| events.push(e));
        events.clear();
        while wake_rx.try_recv().is_ok() {}

        let mut published = crate::autofwd::AutoForwards::with_limits_and_budget(
            crate::autofwd::Upstream::PerPort(dir.clone()),
            &[Ipv4Addr::LOCALHOST],
            crate::autofwd::ConnLimit::Shared(2),
            None,
            budget.clone(),
        );
        let pub_port = free_port();
        assert!(published.add(pub_port).unwrap());
        assert_eq!(budget.listener_fds(), 6);

        let p5 = free_port();
        tracker.add(PortProto::Tcp, p5, &mut |e| events.push(e));
        assert!(matches!(
            events.pop(),
            Some(Event::PortFailed(PortOrigin::Automatic, PortProto::Tcp, p, _)) if p == p5
        ));
        assert!(published.remove(pub_port));
        wake_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        tracker.retry_unbound(&mut |e| events.push(e));
        assert!(matches!(
            events.pop(),
            Some(Event::PortAdd(PortOrigin::Automatic, PortProto::Tcp, p, ref a)) if p == p5 && a == &[Ipv4Addr::LOCALHOST]
        ));

        drop(published);
        drop(tracker);
        assert_eq!(budget.listener_fds(), 0);
        guest_stop.store(true, Ordering::SeqCst);
        let _ = std::os::unix::net::UnixStream::connect(&g_path);
        let _ = guest_thread.join();
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn auto_forward_tracker_retries_after_external_listener_exits() {
        use std::net::{Ipv4Addr, TcpListener, TcpStream};

        let dir = std::env::temp_dir().join(format!("kat-run-ext-retry-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let mut next_port = 22_000 + ((std::process::id() % 1000) as u16) * 8;
        let mut free_port = || loop {
            let candidate = next_port;
            next_port += 1;
            if TcpStream::connect_timeout(
                &std::net::SocketAddr::from((Ipv4Addr::LOCALHOST, candidate)),
                Duration::from_millis(20),
            )
            .is_err()
            {
                return candidate;
            }
        };

        // Budget of 3 listener fds (1 single-address port total) so we can verify
        // capacity-blocked ports do not schedule a polling timer.
        let budget = crate::autofwd::RelayBudget::with_limits(3, 4);
        let mut tracker =
            AutoForwardTracker::new(crate::autofwd::AutoForwards::with_limits_and_budget(
                crate::autofwd::Upstream::AutoForward(dir.join("autofwd.sock")),
                &[Ipv4Addr::LOCALHOST],
                crate::autofwd::ConnLimit::Shared(4),
                None,
                budget,
            ));
        let mut events = Vec::new();
        let base = Instant::now();

        let busy_port = free_port();
        let busy = TcpListener::bind((Ipv4Addr::LOCALHOST, busy_port)).unwrap();
        tracker.add_at(PortProto::Tcp, busy_port, base, &mut |e| events.push(e));
        assert!(matches!(
            events.pop(),
            Some(Event::PortFailed(PortOrigin::Automatic, PortProto::Tcp, p, _)) if p == busy_port
        ));
        // Initial retry scheduled after UNBOUND_RETRY_INITIAL (250ms).
        let first_retry = tracker.next_retry_at().expect("timer retry scheduled");
        assert_eq!(first_retry, base + UNBOUND_RETRY_INITIAL);

        // Retrying while still held backs off (500ms) without repeating PortFailed.
        tracker.retry_due(first_retry, &mut |e| events.push(e));
        assert!(events.is_empty(), "duplicate error must not re-emit");
        let second_retry = tracker.next_retry_at().expect("second retry scheduled");
        assert_eq!(second_retry, first_retry + UNBOUND_RETRY_INITIAL * 2);

        // External listener exits with no Katamaran release event: timer retry binds it.
        // Loop briefly in case a parallel test subprocess inherited `busy`'s fd
        // during macOS's non-atomic `socket()` + `FIOCLEX` window.
        drop(busy);
        let mut retry_now = second_retry;
        let bound_deadline = Instant::now() + Duration::from_secs(3);
        loop {
            tracker.retry_due(retry_now, &mut |e| events.push(e));
            if !events.is_empty() || Instant::now() >= bound_deadline {
                break;
            }
            retry_now = tracker.next_retry_at().unwrap_or(retry_now);
            thread::sleep(Duration::from_millis(20));
        }
        assert!(matches!(
            events.pop(),
            Some(Event::PortAdd(PortOrigin::Automatic, PortProto::Tcp, p, ref a))
                if p == busy_port && a == &[Ipv4Addr::LOCALHOST]
        ));
        assert_eq!(tracker.next_retry_at(), None);

        // Capacity-blocked port does NOT schedule a polling timer (it wakes via budget release).
        let cap_port = free_port();
        tracker.add_at(PortProto::Tcp, cap_port, retry_now, &mut |e| events.push(e));
        assert!(matches!(
            events.pop(),
            Some(Event::PortFailed(PortOrigin::Automatic, PortProto::Tcp, p, _)) if p == cap_port
        ));
        assert_eq!(tracker.next_retry_at(), None);

        drop(tracker);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn session_poller_runs_due_retries_with_queued_inputs() {
        use std::net::{Ipv4Addr, TcpListener, TcpStream};

        let dir =
            std::env::temp_dir().join(format!("kat-run-poll-interleave-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let mut next_port = 26_000 + ((std::process::id() % 1000) as u16) * 16;
        let mut free_port = || loop {
            let candidate = next_port;
            next_port += 1;
            if TcpStream::connect_timeout(
                &std::net::SocketAddr::from((Ipv4Addr::LOCALHOST, candidate)),
                Duration::from_millis(20),
            )
            .is_err()
            {
                return candidate;
            }
        };

        let budget = crate::autofwd::RelayBudget::with_limits(64, 16);
        let mut tracker =
            AutoForwardTracker::new(crate::autofwd::AutoForwards::with_limits_and_budget(
                crate::autofwd::Upstream::AutoForward(dir.join("autofwd.sock")),
                &[Ipv4Addr::LOCALHOST],
                crate::autofwd::ConnLimit::Shared(4),
                None,
                budget,
            ));
        let mut events = Vec::new();
        let base = Instant::now();

        let busy_port = free_port();
        let busy = TcpListener::bind((Ipv4Addr::LOCALHOST, busy_port)).unwrap();
        tracker.add_at(PortProto::Tcp, busy_port, base, &mut |e| events.push(e));
        assert!(matches!(
            events.pop(),
            Some(Event::PortFailed(PortOrigin::Automatic, PortProto::Tcp, p, _)) if p == busy_port
        ));
        let first_retry = tracker.next_retry_at().expect("timer retry scheduled");

        // Release the external listener while unrelated messages are already
        // queued in the session channel.
        drop(busy);
        let (tx, rx) = mpsc::channel::<usize>();
        for i in 0..8 {
            tx.send(i).unwrap();
        }
        let mut poller = SessionPoller::new();

        // First poll dispatches one queued message (preserving queued removal,
        // handoff, or shutdown handling), and the very next poll yields
        // RetryDue without waiting for the remaining 7 queued messages to drain.
        assert_eq!(
            poller.next(&rx, tracker.next_retry_at(), first_retry),
            SessionPoll::Input(0)
        );
        assert_eq!(
            poller.next(&rx, tracker.next_retry_at(), first_retry),
            SessionPoll::RetryDue
        );

        // Retry until bound (tolerating macOS's brief non-atomic FIOCLEX fd
        // inheritance window by parallel test subprocesses) while keeping
        // unrelated messages queued in `rx`.
        let mut retry_now = first_retry;
        let mut dispatched_before_bind = vec![0usize];
        let mut next_msg_id = 8usize;
        let bound_deadline = Instant::now() + Duration::from_secs(3);
        loop {
            tracker.retry_due(retry_now, &mut |e| events.push(e));
            if !events.is_empty() || Instant::now() >= bound_deadline {
                break;
            }
            // Keep the channel nonempty across any transient retry backoff.
            tx.send(next_msg_id).unwrap();
            next_msg_id += 1;
            retry_now = tracker.next_retry_at().unwrap_or(retry_now);
            thread::sleep(Duration::from_millis(20));
            assert_eq!(
                poller.next(&rx, tracker.next_retry_at(), retry_now),
                SessionPoll::Input(dispatched_before_bind.len())
            );
            dispatched_before_bind.push(dispatched_before_bind.len());
            assert_eq!(
                poller.next(&rx, tracker.next_retry_at(), retry_now),
                SessionPoll::RetryDue
            );
        }

        assert!(matches!(
            events.pop(),
            Some(Event::PortAdd(PortOrigin::Automatic, PortProto::Tcp, p, ref a))
                if p == busy_port && a == &[Ipv4Addr::LOCALHOST]
        ));
        assert_eq!(tracker.next_retry_at(), None);
        // Unrelated messages are still queued and were not required to drain
        // before the timer retry bound the port.
        let remaining: Vec<_> = rx.try_iter().collect();
        assert!(
            remaining.len() >= 7,
            "expected queued inputs to still remain after retry bound port, got {remaining:?}"
        );

        // A queued guest removal ahead of an overdue retry is processed first
        // so the removed port is not spuriously bound.
        let removed_port = free_port();
        let hold = TcpListener::bind((Ipv4Addr::LOCALHOST, removed_port)).unwrap();
        tracker.add_at(PortProto::Tcp, removed_port, retry_now, &mut |e| {
            events.push(e)
        });
        events.clear();
        let removed_retry = tracker.next_retry_at().expect("retry scheduled");
        drop(hold);
        let (rtx, rrx) = mpsc::channel::<&str>();
        rtx.send("remove").unwrap();
        rtx.send("unrelated").unwrap();
        let mut guard_poller = SessionPoller::new();
        assert_eq!(
            guard_poller.next(&rrx, tracker.next_retry_at(), removed_retry),
            SessionPoll::Input("remove")
        );
        tracker.remove(PortProto::Tcp, removed_port, &mut |e| events.push(e));
        assert_eq!(tracker.next_retry_at(), None);
        assert_eq!(
            guard_poller.next(&rrx, tracker.next_retry_at(), removed_retry),
            SessionPoll::Input("unrelated")
        );
        assert!(
            matches!(
                events.as_slice(),
                [Event::PortErrorClear(PortOrigin::Automatic, PortProto::Tcp, p)]
                    if *p == removed_port
            ),
            "removed port must clear its error and not bind after queued removal: {events:?}"
        );

        // When idle, SessionPoller still waits via recv_timeout and wakes on
        // the scheduled deadline.
        let (_itx, irx) = mpsc::channel::<usize>();
        let mut idle_poller = SessionPoller::new();
        let now = Instant::now();
        assert_eq!(
            idle_poller.next(&irx, Some(now + Duration::from_millis(10)), now),
            SessionPoll::RetryDue
        );

        // When more than UNBOUND_RETRY_BATCH ports are due simultaneously,
        // retry_due bounds work per turn so queued inputs interleave between
        // retry batches.
        let mut held = Vec::new();
        let batch_base = Instant::now();
        for _ in 0..(UNBOUND_RETRY_BATCH + 2) {
            let p = free_port();
            held.push(TcpListener::bind((Ipv4Addr::LOCALHOST, p)).unwrap());
            tracker.add_at(PortProto::Tcp, p, batch_base, &mut |e| events.push(e));
        }
        let batch_due = tracker.next_retry_at().expect("batch retry scheduled");
        let (btx, brx) = mpsc::channel::<&str>();
        for m in ["m0", "m1", "m2"] {
            btx.send(m).unwrap();
        }
        let mut batch_poller = SessionPoller::new();
        assert_eq!(
            batch_poller.next(&brx, tracker.next_retry_at(), batch_due),
            SessionPoll::Input("m0")
        );
        assert_eq!(
            batch_poller.next(&brx, tracker.next_retry_at(), batch_due),
            SessionPoll::RetryDue
        );
        tracker.retry_due(batch_due, &mut |e| events.push(e));
        // Exactly UNBOUND_RETRY_BATCH moved forward; 2 remain due at batch_due,
        // and the next poll interleaves queued input before the second batch.
        assert_eq!(tracker.next_retry_at(), Some(batch_due));
        assert_eq!(
            batch_poller.next(&brx, tracker.next_retry_at(), batch_due),
            SessionPoll::Input("m1")
        );
        assert_eq!(
            batch_poller.next(&brx, tracker.next_retry_at(), batch_due),
            SessionPoll::RetryDue
        );
        tracker.retry_due(batch_due, &mut |e| events.push(e));
        assert!(
            tracker.next_retry_at().is_some_and(|at| at > batch_due),
            "second batch should advance all remaining due retries"
        );
        assert_eq!(
            batch_poller.next(&brx, tracker.next_retry_at(), batch_due),
            SessionPoll::Input("m2")
        );

        drop(held);
        drop(tracker);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn grow_data_disk_extends_sparsely_and_never_shrinks() {
        let dir = std::env::temp_dir().join(format!("kat-grow-disk-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let img = dir.join("data.img");

        let mut f = File::create(&img).unwrap();
        f.write_all(b"btrfs-header").unwrap();
        f.set_len(64u64 << 30).unwrap();
        drop(f);

        // Same size is a no-op.
        assert_eq!(grow_data_disk(&img, 64).unwrap(), 64u64 << 30);
        assert_eq!(fs::metadata(&img).unwrap().len(), 64u64 << 30);

        // Growing extends apparent size sparsely and preserves existing bytes.
        assert_eq!(grow_data_disk(&img, 200).unwrap(), 200u64 << 30);
        let meta = fs::metadata(&img).unwrap();
        assert_eq!(meta.len(), 200u64 << 30);
        assert!(
            meta.blocks() * 512 < (64 << 20),
            "grown image should remain sparse: {} blocks",
            meta.blocks()
        );
        let mut head = [0u8; 12];
        File::open(&img).unwrap().read_exact(&mut head).unwrap();
        assert_eq!(&head, b"btrfs-header");

        // Smaller target never truncates.
        assert_eq!(grow_data_disk(&img, 128).unwrap(), 200u64 << 30);
        assert_eq!(fs::metadata(&img).unwrap().len(), 200u64 << 30);

        // Invalid sizes fail.
        assert!(grow_data_disk(&img, 32).is_err());
        assert!(grow_data_disk(&img, crate::config::MAX_DISK_GIB + 1).is_err());

        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn run_args_validate_disk_gib_range_at_parse_time() {
        use clap::Parser;

        #[derive(clap::Parser)]
        struct Wrapper {
            #[command(flatten)]
            run: RunArgs,
        }

        // Omitting --disk-gib leaves it unset so smaller custom templates are not grown.
        let default_args = Wrapper::try_parse_from(["run"]).unwrap().run;
        assert_eq!(default_args.disk_gib, None);

        let min_args = Wrapper::try_parse_from(["run", "--disk-gib", "64"])
            .unwrap()
            .run;
        assert_eq!(min_args.disk_gib, Some(64));

        let big_args = Wrapper::try_parse_from(["run", "--disk-gib", "500"])
            .unwrap()
            .run;
        assert_eq!(big_args.disk_gib, Some(500));

        let max_str = crate::config::MAX_DISK_GIB.to_string();
        let max_args = Wrapper::try_parse_from(["run", "--disk-gib", &max_str])
            .unwrap()
            .run;
        assert_eq!(max_args.disk_gib, Some(crate::config::MAX_DISK_GIB));

        // Out-of-range sizes fail during argument parsing, before session can clone a template.
        assert!(Wrapper::try_parse_from(["run", "--disk-gib", "0"]).is_err());
        assert!(Wrapper::try_parse_from(["run", "--disk-gib", "32"]).is_err());
        let over_str = (crate::config::MAX_DISK_GIB + 1).to_string();
        assert!(Wrapper::try_parse_from(["run", "--disk-gib", &over_str]).is_err());
    }

    #[test]
    fn prepare_data_disk_rejects_bad_size_before_cloning_and_keeps_small_template_when_unset() {
        let dir = std::env::temp_dir().join(format!("kat-prep-disk-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let template = dir.join("small-template.img");
        let small_bytes = 8u64 << 30;
        let mut f = File::create(&template).unwrap();
        f.write_all(b"small-btrfs").unwrap();
        f.set_len(small_bytes).unwrap();
        drop(f);

        let dest = dir.join("data.img");

        // Invalid target fails BEFORE cloning the template.
        assert!(prepare_data_disk(&dest, &template, Some(32)).is_err());
        assert!(
            !dest.exists(),
            "failed range check must not create data.img"
        );

        // Omitting target keeps an intentionally smaller custom template at its exact size.
        let prepared = prepare_data_disk(&dest, &template, None).unwrap();
        assert_eq!(prepared, small_bytes);
        assert_eq!(fs::metadata(&dest).unwrap().len(), small_bytes);

        // Explicit growth extends the existing image and returns the new size.
        let grown = prepare_data_disk(&dest, &template, Some(128)).unwrap();
        assert_eq!(grown, 128u64 << 30);
        assert_eq!(fs::metadata(&dest).unwrap().len(), 128u64 << 30);

        fs::remove_dir_all(&dir).unwrap();
    }
}
