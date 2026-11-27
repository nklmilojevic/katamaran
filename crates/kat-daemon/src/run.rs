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
    NFS_PORT, PortProto, read_msg,
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
    /// Do not forward unpublished container ports to the Mac's 127.0.0.1.
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
/// [`crate::autofwd::AutoForwards`], retaining ports blocked by the listener
/// descriptor budget and retrying them as soon as capacity frees up.
///
/// It also covers the published-to-automatic handoff: the guest sends
/// `PortRemove` before `AutoForwardAdd` for the same port, but the published
/// worker may not have closed its listener yet (or even reported binding it).
/// While the published side holds the port or a dispatched remove has not
/// completed, an automatic add is deferred without attempting a bind (so it
/// can neither fail for good nor bind only some addresses), then retried when
/// the worker confirms the port is released.
struct AutoForwardTracker {
    forwards: crate::autofwd::AutoForwards,
    /// FIFO queue of `(proto, port)` waiting for listener descriptors.
    pending_capacity: Vec<(PortProto, u16)>,
    /// Ports waiting for the published worker to release the same port.
    pending_release: Vec<(PortProto, u16)>,
    /// Ports the published worker reported `Forwarded` and has not yet
    /// reported `Removed`.
    published_held: std::collections::BTreeSet<(PortProto, u16)>,
    /// For each port, the sequence number of the latest published remove
    /// dispatched to the worker whose `RemoveDone` has not arrived. Only that
    /// remove's completion releases the port: an earlier remove's completion
    /// may arrive after the port was re-published and removed again.
    published_removing: std::collections::BTreeMap<(PortProto, u16), u64>,
    /// Last sequence number handed out by [`Self::next_remove_seq`].
    remove_seq: u64,
}

/// Why [`AutoForwardTracker::attempt`] is binding a port.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Attempt {
    /// A guest `AutoForwardAdd`.
    Guest,
    /// Retrying after listener capacity was released.
    Capacity,
    /// Retrying after the published worker released the port.
    Release,
}

impl AutoForwardTracker {
    fn new(forwards: crate::autofwd::AutoForwards) -> Self {
        Self {
            forwards,
            pending_capacity: Vec::new(),
            pending_release: Vec::new(),
            published_held: std::collections::BTreeSet::new(),
            published_removing: std::collections::BTreeMap::new(),
            remove_seq: 0,
        }
    }

    fn add(&mut self, proto: PortProto, port: u16, on_event: &mut dyn FnMut(Event)) {
        self.attempt(proto, port, Attempt::Guest, on_event);
    }

    fn attempt(
        &mut self,
        proto: PortProto,
        port: u16,
        why: Attempt,
        on_event: &mut dyn FnMut(Event),
    ) {
        let key = (proto, port);
        if !self.forwards.contains(port)
            && (self.published_held.contains(&key) || self.published_removing.contains_key(&key))
        {
            self.pending_capacity.retain(|&k| k != key);
            if !self.pending_release.contains(&key) {
                self.pending_release.push(key);
                eprintln!(
                    "katamaran: auto-forward {}:{port}/{} waits for the published forward to release the port",
                    self.forwards.label(),
                    proto.as_str()
                );
            }
            return;
        }
        match self.forwards.add(port) {
            Ok(added) => {
                self.pending_capacity.retain(|&k| k != key);
                self.pending_release.retain(|&k| k != key);
                if !added {
                    return;
                }
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
            Err(crate::autofwd::AddError::CapacityExhausted(reason)) => {
                self.pending_release.retain(|&k| k != key);
                if !self.pending_capacity.contains(&key) {
                    self.pending_capacity.push(key);
                    if why != Attempt::Capacity {
                        eprintln!(
                            "katamaran: deferring auto-forward {}:{port}/{} ({reason}); will retry when a listener closes",
                            self.forwards.label(),
                            proto.as_str()
                        );
                        on_event(Event::PortFailed(
                            PortOrigin::Automatic,
                            proto,
                            port,
                            reason,
                        ));
                    }
                }
            }
            Err(crate::autofwd::AddError::Io(e)) => {
                self.pending_capacity.retain(|&k| k != key);
                self.pending_release.retain(|&k| k != key);
                eprintln!(
                    "katamaran: cannot auto-forward {}:{port}/{} ({e}); skipped",
                    self.forwards.label(),
                    proto.as_str()
                );
                on_event(Event::PortFailed(
                    PortOrigin::Automatic,
                    proto,
                    port,
                    e.to_string(),
                ));
            }
        }
    }

    fn remove(&mut self, proto: PortProto, port: u16, on_event: &mut dyn FnMut(Event)) {
        let key = (proto, port);
        self.pending_capacity.retain(|&k| k != key);
        self.pending_release.retain(|&k| k != key);
        let label = self.forwards.bound_label(port);
        if self.forwards.remove(port) {
            println!(
                "katamaran: stopped auto-forwarding {label}:{port}/{}",
                proto.as_str()
            );
            on_event(Event::PortRemove(PortOrigin::Automatic, proto, port));
            self.retry_pending(on_event);
        } else {
            on_event(Event::PortErrorClear(PortOrigin::Automatic, proto, port));
        }
    }

    fn retry_pending(&mut self, on_event: &mut dyn FnMut(Event)) {
        for (proto, port) in std::mem::take(&mut self.pending_capacity) {
            self.attempt(proto, port, Attempt::Capacity, on_event);
        }
    }

    /// The published worker now listens on `port`.
    fn published_forwarded(&mut self, proto: PortProto, port: u16) {
        self.published_held.insert((proto, port));
    }

    /// The sequence number for the next published remove.
    fn next_remove_seq(&mut self) -> u64 {
        self.remove_seq += 1;
        self.remove_seq
    }

    /// Published remove `seq` for `port` was dispatched to the worker.
    fn published_removing(&mut self, proto: PortProto, port: u16, seq: u64) {
        self.published_removing.insert((proto, port), seq);
    }

    /// The published worker closed its listener on `port`.
    fn published_closed(&mut self, proto: PortProto, port: u16) {
        self.published_held.remove(&(proto, port));
    }

    /// The worker finished published remove `seq` for `port`. If it is the
    /// latest one dispatched, the published side is done with the port, so
    /// retry an automatic forward that was waiting for it. Completions of
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
        self.published_held.remove(&key);
        if self.pending_release.contains(&key) {
            self.pending_release.retain(|&k| k != key);
            self.attempt(proto, port, Attempt::Release, on_event);
        }
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
    // host-side vsock Unix sockets (docker.sock, machine.sock, k8s.sock, k8s-api.sock).
    fs::set_permissions(&run_dir, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("chmod 0700 {}", run_dir.display()))?;

    let data_img = data_dir.join("data.img");
    if !data_img.exists() {
        clone_template(&vm::absolute(&args.data_template)?, &data_img)?;
    }
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
    let end = loop {
        match rx.recv() {
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
                auto_forwards.published_forwarded(proto, port);
                on_event(Event::PortAdd(PortOrigin::Published, proto, port, addrs));
            }
            Ok(Input::Forward(_, net::Event::Removed(proto, port))) => {
                on_event(Event::PortRemove(PortOrigin::Published, proto, port));
                auto_forwards.published_closed(proto, port);
            }
            Ok(Input::Forward(_, net::Event::Refused(proto, port, why)))
            | Ok(Input::Forward(_, net::Event::CapacityWait(proto, port, why))) => {
                on_event(Event::PortFailed(PortOrigin::Published, proto, port, why));
            }
            Ok(Input::Forward(_, net::Event::RefusedCleared(proto, port))) => {
                on_event(Event::PortErrorClear(PortOrigin::Published, proto, port));
            }
            Ok(Input::Forward(_, net::Event::RemoveDone(proto, port, seq))) => {
                auto_forwards.published_remove_done(proto, port, seq, on_event);
            }
            Ok(Input::Forward(_, net::Event::Transient(..) | net::Event::RemoveFailed(..))) => {}
            Ok(Input::ListenerCapacityAvailable(_)) => {
                auto_forwards.retry_pending(on_event);
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
        let tcp = PortProto::Tcp;

        // The worker has bound the published port but its `Forwarded` is
        // queued behind the guest's PortRemove and AutoForwardAdd.
        let published = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = published.local_addr().unwrap().port();
        let seq = tracker.next_remove_seq();
        tracker.published_removing(tcp, port, seq);
        tracker.add(tcp, port, &mut |e| events.push(e));
        assert!(events.is_empty(), "handoff conflict reported: {events:?}");
        assert_eq!(tracker.pending_release, vec![(tcp, port)]);
        tracker.published_forwarded(tcp, port);
        assert!(!tracker.forwards.contains(port));
        // The worker closes the port and finishes the remove: the automatic
        // forward binds without a second AutoForwardAdd.
        drop(published);
        tracker.published_closed(tcp, port);
        assert!(events.is_empty());
        tracker.published_remove_done(tcp, port, seq, &mut |e| events.push(e));
        assert!(bound_auto(&events, port), "{events:?}");
        assert!(tracker.pending_release.is_empty());
        events.clear();
        tracker.remove(tcp, port, &mut |e| events.push(e));
        events.clear();

        // Remove, re-publish, remove again: the first remove's completion
        // arrives while the second published listener still owns the port.
        // Only the latest remove's completion releases the automatic forward.
        let first = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = first.local_addr().unwrap().port();
        tracker.published_forwarded(tcp, port);
        let seq1 = tracker.next_remove_seq();
        tracker.published_removing(tcp, port, seq1);
        let seq2 = tracker.next_remove_seq();
        tracker.published_removing(tcp, port, seq2);
        tracker.add(tcp, port, &mut |e| events.push(e));
        drop(first);
        tracker.published_closed(tcp, port);
        let second = TcpListener::bind(("127.0.0.1", port)).unwrap();
        tracker.published_remove_done(tcp, port, seq1, &mut |e| events.push(e));
        assert!(events.is_empty(), "stale completion acted on: {events:?}");
        assert_eq!(tracker.pending_release, vec![(tcp, port)]);
        assert!(!tracker.forwards.contains(port));
        tracker.published_forwarded(tcp, port);
        drop(second);
        tracker.published_closed(tcp, port);
        tracker.published_remove_done(tcp, port, seq2, &mut |e| events.push(e));
        assert!(bound_auto(&events, port), "{events:?}");
        events.clear();
        tracker.remove(tcp, port, &mut |e| events.push(e));
        events.clear();

        // While a remove is in flight no bind is attempted even if the port
        // looks free, so a published listener still holding some addresses
        // cannot leave a partial automatic bind behind. A remove that found
        // no listener completes the handoff too.
        let free = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = free.local_addr().unwrap().port();
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

        // If the guest drops the automatic port before the release, the
        // release does not resurrect it.
        let published = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = published.local_addr().unwrap().port();
        tracker.published_forwarded(tcp, port);
        let seq = tracker.next_remove_seq();
        tracker.published_removing(tcp, port, seq);
        tracker.add(tcp, port, &mut |e| events.push(e));
        tracker.remove(tcp, port, &mut |e| events.push(e));
        assert!(tracker.pending_release.is_empty());
        events.clear();
        drop(published);
        tracker.published_closed(tcp, port);
        tracker.published_remove_done(tcp, port, seq, &mut |e| events.push(e));
        assert!(events.is_empty());
        assert!(!tracker.forwards.contains(port));

        // Without a published forward on the port, a bind conflict is still a
        // permanent failure.
        let busy = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = busy.local_addr().unwrap().port();
        tracker.add(tcp, port, &mut |e| events.push(e));
        assert!(matches!(
            events.as_slice(),
            [Event::PortFailed(PortOrigin::Automatic, PortProto::Tcp, p, _)] if *p == port
        ));
        assert!(tracker.pending_release.is_empty());
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

        // Ordinary host conflict (port already in use) stays on the permanent
        // failure path even with capacity available.
        let busy = TcpListener::bind("127.0.0.1:0").unwrap();
        let busy_port = busy.local_addr().unwrap().port();
        tracker.add(PortProto::Tcp, busy_port, &mut |e| events.push(e));
        assert!(matches!(
            events.pop(),
            Some(Event::PortFailed(PortOrigin::Automatic, PortProto::Tcp, p, _)) if p == busy_port
        ));
        assert!(tracker.pending_capacity.is_empty());

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

        // Drop busy socket to verify ordinary bind failures are NOT retried
        // when capacity frees up.
        drop(busy);

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

        // Cross-forwarder release: a published forwarder releasing a slot fires
        // the subscription callback so `retry_pending` resumes an auto port.
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
        tracker.retry_pending(&mut |e| events.push(e));
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
}
