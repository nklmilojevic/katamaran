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
    /// How x86-64 (amd64) programs run: Rosetta, qemu-user, or not at all;
    /// auto uses Rosetta when it is installed.
    #[arg(long, value_enum, default_value_t)]
    pub amd64: crate::amd64::Emulation,
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
    PortAdd(PortProto, u16),
    PortRemove(PortProto, u16),
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
    let run_dir = crate::state_dir()?.join("run");
    fs::create_dir_all(&run_dir).with_context(|| format!("create {}", run_dir.display()))?;
    let _lock = lock(&run_dir.join("katamaran.lock"))?;
    // Before any thread is spawned (gvproxy's watcher is one).
    let signals = block_and_watch_signals()?;
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
    run_vm(&args, rx, &mut |_| {})
}

/// Boot the VM and serve it until [`Control::Shutdown`]. A VM that dies is
/// cleaned up after and started again with backoff; `Err` means the first
/// boot failed or the VM kept dying. The caller holds the state directory's
/// lock and has blocked SIGINT and SIGTERM.
pub fn run_vm(
    args: &RunArgs,
    control: Receiver<Control>,
    on_event: &mut dyn FnMut(Event),
) -> anyhow::Result<()> {
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
        let why = match session(args, &tx, &rx, id, on_event) {
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

    let forwarder = match &gvproxy {
        Some(g) => {
            let fwd_tx = tx.clone();
            Some(net::spawn_forwarder(
                GvproxyApi {
                    socket: g.api_socket.clone(),
                    guest: g.network().guest_ip,
                },
                move |e| {
                    let _ = fwd_tx.send(Input::Forward(id, e));
                },
            )?)
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

    let mut auto_forwards = crate::autofwd::AutoForwards::new(&autofwd_sock);
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
                    if let Some((dir, port_file)) = files.clone() {
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
                    let _ = f.send(PortChange::Remove(proto, port));
                }
            }
            Ok(Input::Forward(_, net::Event::Forwarded(proto, port))) => {
                on_event(Event::PortAdd(proto, port));
            }
            Ok(Input::Forward(
                _,
                net::Event::Removed(proto, port) | net::Event::RemoveFailed(proto, port, _),
            )) => {
                on_event(Event::PortRemove(proto, port));
            }
            Ok(Input::Forward(_, net::Event::Refused(..) | net::Event::Transient(..))) => {}
            Ok(Input::Guest(_, Some(Msg::AutoForwardAdd { proto, port }))) => {
                match auto_forwards.add(port) {
                    Ok(true) => {
                        println!(
                            "katamaran: auto-forwarding 127.0.0.1:{port}/{}",
                            proto.as_str()
                        );
                        on_event(Event::PortAdd(proto, port));
                    }
                    Ok(false) => {}
                    Err(e) => eprintln!(
                        "katamaran: cannot auto-forward 127.0.0.1:{port}/{} ({e}); skipped",
                        proto.as_str()
                    ),
                }
            }
            Ok(Input::Guest(_, Some(Msg::AutoForwardRemove { proto, port }))) => {
                if auto_forwards.remove(port) {
                    println!(
                        "katamaran: stopped auto-forwarding 127.0.0.1:{port}/{}",
                        proto.as_str()
                    );
                    on_event(Event::PortRemove(proto, port));
                }
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
