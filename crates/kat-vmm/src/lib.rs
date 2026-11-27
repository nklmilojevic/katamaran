//! The VM process. libkrun runs its event loop on the calling thread and
//! `_exit`s the process when the guest powers off, so every VM gets its own
//! `katamaran-vm` process, started and supervised by the daemon.

use std::net::Ipv4Addr;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[cfg(target_os = "macos")]
pub mod fscache;
#[cfg(target_os = "macos")]
pub mod fsevents;
#[cfg(target_os = "macos")]
mod inproc;
#[cfg(target_os = "macos")]
pub mod vmctl;

/// Everything `katamaran-vm` needs to boot a guest. The daemon writes this as
/// JSON and passes the path as the only argument.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VmConfig {
    pub kernel: PathBuf,
    pub initramfs: PathBuf,
    /// Extra kernel command line, appended to [`BASE_CMDLINE`].
    #[serde(default)]
    pub cmdline_extra: String,
    pub cpus: u8,
    pub memory_mib: u32,
    /// The guest console (hvc0) is written here.
    pub console_log: PathBuf,
    /// Unix socket the daemon listens on. A guest connection to vsock port
    /// [`kat_proto::CONTROL_PORT`] on the host is proxied to it.
    pub control_socket: PathBuf,
    /// virtio-blk disks, in order: the first is `/dev/vda`.
    #[serde(default)]
    pub disks: Vec<Disk>,
    /// Unix sockets libkrun listens on for the host. A connection to `path` is
    /// proxied to the guest's vsock `port`, where the agent must be listening.
    #[serde(default)]
    pub host_listeners: Vec<HostListener>,
    /// Host directories shared into the guest over virtio-fs.
    #[serde(default)]
    pub shares: Vec<Share>,
    /// virtio-net device backed by a userspace network stack.
    #[serde(default)]
    pub network: Option<Network>,
    /// How the guest runs x86-64 programs.
    #[serde(default)]
    pub amd64: Amd64,
}

/// The x86-64 emulator the guest registers with binfmt_misc, passed as
/// `kat.amd64=`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case", tag = "kind")]
pub enum Amd64 {
    #[default]
    Off,
    /// qemu-user from the system image.
    Qemu,
    /// Rosetta for Linux from `dir` (the RosettaLinux directory), shared
    /// read-only under the tag `rosetta` with its ioctls answered.
    Rosetta { dir: PathBuf },
}

impl Amd64 {
    fn cmdline_arg(&self) -> Option<&'static str> {
        match self {
            Amd64::Off => None,
            Amd64::Qemu => Some("kat.amd64=qemu"),
            Amd64::Rosetta { .. } => Some("kat.amd64=rosetta"),
        }
    }
}

/// A virtio-net device whose frames go to gvproxy over its vfkit-protocol
/// unixgram socket, or by function call to gvproxy loaded into the VM
/// process. The guest configures itself statically from the `kat.net=`
/// kernel argument, which must match what gvproxy serves.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Network {
    /// Run gvproxy in the VM process instead of using `socket`.
    #[serde(default)]
    pub in_process: Option<InProcess>,
    /// gvproxy's `-listen-vfkit` socket. The VM waits for it to appear.
    pub socket: PathBuf,
    pub mac: [u8; 6],
    pub mtu: u16,
    pub guest_ip: Ipv4Addr,
    pub prefix_len: u8,
    /// Default route and DNS server (gvproxy answers DNS on the gateway).
    pub gateway: Ipv4Addr,
    /// Address that gvproxy maps to the Mac's 127.0.0.1.
    pub host_ip: Ipv4Addr,
}

/// gvproxy built as a library (`libgvproxy.dylib`), started in the VM
/// process with `args`, its command line without a VM socket.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InProcess {
    pub library: PathBuf,
    pub args: Vec<String>,
}

impl Network {
    /// `kat.net=<guest>/<prefix>,<gateway>,<host>,<mtu>`
    pub fn cmdline_arg(&self) -> String {
        format!(
            "kat.net={}/{},{},{},{}",
            self.guest_ip, self.prefix_len, self.gateway, self.host_ip, self.mtu
        )
    }
}

/// A host directory exported over virtio-fs under `tag`. The guest mounts it
/// at `guest_path`, passed on the kernel command line as
/// `kat.share=tag:path[:mount-options]`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Share {
    pub tag: String,
    pub host_path: PathBuf,
    pub guest_path: String,
    #[serde(default)]
    pub permissions: Permissions,
}

/// How Linux ownership and modes map onto host files.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case", tag = "kind")]
pub enum Permissions {
    /// Ownership and mode stored in an xattr per file (libkrun `LinuxComplete`).
    /// Costs a `getxattr` on every lookup.
    Xattr,
    /// Files appear owned by the caller, chown is ignored, modes go to the host
    /// file, so a container user can write the Mac files it mounts whatever
    /// its uid. The host reports a fixed owner so attributes stay cacheable
    /// (libkrun `LinuxSimplifiedFixedOwner`), and the
    /// guest kernel's `owner_caller` mount option shows the caller as owner.
    #[default]
    Caller,
    /// Like `Caller`, but the host substitutes the caller per request (libkrun
    /// `LinuxSimplified`), which forces attribute caching off.
    CallerUncached,
    /// Files appear owned by a fixed uid/gid; attributes stay cacheable
    /// (libkrun `LinuxSimplifiedFixedOwner`).
    Fixed { uid: u32, gid: u32 },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Disk {
    pub id: String,
    pub path: PathBuf,
    #[serde(default)]
    pub read_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HostListener {
    pub port: u32,
    pub path: PathBuf,
    /// Accept only connections the macOS kernel makes itself (peer pid 0),
    /// such as its NFS client's; any user process is refused.
    #[serde(default)]
    pub kernel_only: bool,
    /// Listen on a free TCP port of 127.0.0.1 instead, and write its number
    /// to `path`. Only with `kernel_only`.
    #[serde(default)]
    pub loopback_tcp: bool,
}

/// Request queues per virtio-fs share: one per vCPU, so file requests from
/// different vCPUs do not queue behind each other, up to libkrun's limit.
pub fn fs_request_queues(cpus: u8) -> u32 {
    u32::from(cpus).clamp(1, 16)
}

/// Boot flags borrowed from kiri's measurements: expedited RCU removes a 20 ms
/// stall at the cgroup2 mount, and the virtio devices address all of memory
/// so no swiotlb bounce buffer is needed.
pub const BASE_CMDLINE: &str =
    "console=hvc0 rdinit=/init panic=-1 rcupdate.rcu_expedited=1 swiotlb=noforce quiet loglevel=4";

impl Permissions {
    /// Extra virtiofs mount options the guest needs for this mode.
    pub fn guest_mount_options(self) -> Option<&'static str> {
        match self {
            Permissions::Caller => Some("owner_caller"),
            _ => None,
        }
    }
}

impl VmConfig {
    pub fn cmdline(&self) -> String {
        let mut c = BASE_CMDLINE.to_string();
        for s in &self.shares {
            c.push_str(&format!(" kat.share={}:{}", s.tag, s.guest_path));
            if let Some(opts) = s.permissions.guest_mount_options() {
                c.push(':');
                c.push_str(opts);
            }
        }
        if let Some(n) = &self.network {
            c.push(' ');
            c.push_str(&n.cmdline_arg());
        }
        if let Some(a) = self.amd64.cmdline_arg() {
            c.push(' ');
            c.push_str(a);
        }
        if !self.cmdline_extra.trim().is_empty() {
            c.push(' ');
            c.push_str(self.cmdline_extra.trim());
        }
        c
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.cpus >= 1, "cpus must be at least 1");
        anyhow::ensure!(self.memory_mib >= 256, "memory_mib must be at least 256");
        for (what, p) in [("kernel", &self.kernel), ("initramfs", &self.initramfs)] {
            anyhow::ensure!(p.is_file(), "{what} not found at {}", p.display());
        }
        for d in &self.disks {
            anyhow::ensure!(
                d.path.is_file(),
                "disk {} not found at {}",
                d.id,
                d.path.display()
            );
        }
        for s in &self.shares {
            anyhow::ensure!(
                s.host_path.is_dir(),
                "share {} not found at {}",
                s.tag,
                s.host_path.display()
            );
            let valid_tag = !s.tag.is_empty() && !s.tag.contains([':', ' ']);
            let valid_path = s.guest_path.starts_with('/') && !s.guest_path.contains([':', ' ']);
            anyhow::ensure!(
                valid_tag && valid_path,
                "share {} has an invalid tag or guest path",
                s.tag
            );
        }
        if let Amd64::Rosetta { dir } = &self.amd64 {
            anyhow::ensure!(
                dir.join("rosetta").is_file(),
                "Rosetta not found in {}",
                dir.display()
            );
        }
        if let Some(n) = &self.network {
            anyhow::ensure!(
                (1..=30).contains(&n.prefix_len) && (68..=65520).contains(&n.mtu),
                "network prefix length or MTU out of range"
            );
        }
        for l in &self.host_listeners {
            anyhow::ensure!(l.port != 0, "vsock host listener port must be non-zero");
            anyhow::ensure!(
                l.path.is_absolute(),
                "vsock host listener path must be absolute: {}",
                l.path.display()
            );
            if l.port == kat_proto::VM_EXEC_PORT {
                anyhow::ensure!(
                    !l.loopback_tcp && !l.kernel_only,
                    "VM_EXEC_PORT must use a host-listening Unix socket"
                );
            }
        }
        let mut ports: Vec<u32> = self.host_listeners.iter().map(|l| l.port).collect();
        ports.push(kat_proto::CONTROL_PORT);
        ports.push(kat_proto::FSCACHE_PORT);
        ports.sort_unstable();
        anyhow::ensure!(
            ports.windows(2).all(|w| w[0] != w[1]),
            "vsock ports must be unique"
        );
        Ok(())
    }
}

#[cfg(target_os = "macos")]
pub mod run {
    use std::fs::OpenOptions;
    use std::os::fd::AsRawFd;
    use std::path::Path;
    use std::time::{Duration, Instant};

    use anyhow::{Context, anyhow};
    use krun::{
        BalloonDevice, BlockDevice, CachePolicy, ConsoleDevice, DiskFormat, FsDevice, KernelFormat,
        MmioDeviceManager, NetDevice, NetFlags, Payload, PermissionSemantics, RngDevice, TsiFlags,
        VmmBuilder, VsockDevice,
    };

    use super::{Amd64, Permissions, VmConfig};

    /// Guest CID. The host is always CID 2.
    const GUEST_CID: u64 = 3;
    /// gvproxy sends frames up to its MTU (64 KiB) and never segments.
    /// Offering the guest-receive offloads makes Linux post 64 KiB receive
    /// buffers ("big packets") instead of 1.5 KiB ones, and lets gvproxy
    /// in-process leave TCP and UDP checksums to the guest (NEEDS_CSUM).
    /// For transmit only checksum offload (CSUM) is offered: the guest sends
    /// whole frames (the MTU makes TSO pointless) with scatter-gather and
    /// without computing checksums, which gVisor does not verify (its link
    /// declares receive checksum offload) since nothing on the way can
    /// corrupt a frame.
    const NET_FEATURES: u32 = (1 << VIRTIO_NET_F_CSUM)
        | (1 << VIRTIO_NET_F_GUEST_CSUM)
        | (1 << VIRTIO_NET_F_GUEST_TSO4)
        | (1 << VIRTIO_NET_F_GUEST_TSO6);
    const VIRTIO_NET_F_CSUM: u32 = 0;
    const VIRTIO_NET_F_GUEST_CSUM: u32 = 1;
    const VIRTIO_NET_F_GUEST_TSO4: u32 = 7;
    const VIRTIO_NET_F_GUEST_TSO6: u32 = 8;

    fn set_interactive_qos() {
        unsafe extern "C" {
            fn pthread_set_qos_class_self_np(qos: u32, relpri: i32) -> i32;
        }
        const QOS_CLASS_USER_INTERACTIVE: u32 = 0x21;
        unsafe { pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0) };
    }

    /// Build the VM and run it on this thread. Returns only on a setup error;
    /// once the guest runs, libkrun exits the process when it powers off.
    pub fn run(cfg: &VmConfig) -> anyhow::Result<()> {
        cfg.validate()?;
        let err = |e: krun::VmmError| anyhow!("{e}");
        crate::vmctl::block_signals().context("block control signals")?;
        // Keep vCPU threads, in-process gvproxy and device workers on P-cores
        // with low wakeup latency when the host has background load.
        set_interactive_qos();

        let console_out = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&cfg.console_log)
            .with_context(|| format!("open console log {}", cfg.console_log.display()))?;
        // Port 0 with an empty name and terminal properties is the guest's hvc0.
        // libkrun duplicates the descriptor, so ours can close.
        let output = krun::port_io::output_to_raw_fd_dup(console_out.as_raw_fd())
            .map_err(|e| anyhow!("console fd: {e}"))?;
        let mut console = ConsoleDevice::builder();
        console.add_console_port("", output);
        let console = console.build().map_err(err)?;

        // `TsiFlags::empty()` disables all TSI hijacking so guest processes
        // cannot proxy into host Unix/TCP sockets. Every `HostListener` below
        // (including `VM_EXEC_PORT`, which grants VM root) is registered with
        // `listen = true`: libkrun's vsock muxer (`muxer.rs:process_op_request`
        // and `send_stream_pkt`) rejects guest-initiated `VSOCK_OP_REQUEST` /
        // `is_host_listener_id` packets with `MuxerRx::Reset`, and `muxer_rxq.rs`
        // stamps `src_cid = VSOCK_HOST_CID` only on connections accepted from
        // the host-side `UnixAcceptorProxy` socket inside the mode-0700 run dir.
        let mut vsock = VsockDevice::new(GUEST_CID, TsiFlags::empty()).map_err(err)?;
        let control = cfg
            .control_socket
            .to_str()
            .context("control socket path is not utf-8")?;
        vsock.add_unix_port(kat_proto::CONTROL_PORT, control, false);
        // Host changes to the shares, dropped from the guest's caches.
        let fscache = match crate::fscache::Timeouts::from_env() {
            Some(t) if !cfg.shares.is_empty() => {
                let (path, listener) = crate::fscache::listen(&cfg.control_socket)?;
                let path = path.to_str().context("fscache socket path is not utf-8")?;
                vsock.add_unix_port(kat_proto::FSCACHE_PORT, path, false);
                Some((t, listener, Vec::new()))
            }
            _ => None,
        };
        for l in &cfg.host_listeners {
            // libkrun refuses to bind over a stale socket from a previous run.
            let _ = std::fs::remove_file(&l.path);
            let path = l.path.to_str().context("listener path is not utf-8")?;
            if l.kernel_only && l.loopback_tcp {
                vsock.add_tcp_port_kernel_only(l.port, path);
            } else if l.kernel_only {
                vsock.add_unix_port_kernel_only(l.port, path);
            } else {
                vsock.add_unix_port(l.port, path, true);
            }
        }

        let mut devices = MmioDeviceManager::new();
        devices.add(console);
        devices.add(vsock);
        devices.add(RngDevice::new().map_err(err)?);
        // Free page reporting gives memory the guest frees back to macOS.
        if let Some(mode) = crate::vmctl::balloon_mode() {
            let mut balloon = BalloonDevice::new().map_err(err)?;
            balloon.set_free_page_reporting_mode(mode);
            devices.add(balloon);
        }
        for d in &cfg.disks {
            let path = d.path.to_str().context("disk path is not utf-8")?;
            let mut blk = BlockDevice::new(&d.id, path, DiskFormat::Raw).map_err(err)?;
            blk.set_read_only(d.read_only);
            devices.add(blk);
        }
        let mut fscache = fscache;
        for s in &cfg.shares {
            let path = s.host_path.to_str().context("share path is not utf-8")?;
            let mut fs = FsDevice::new(&s.tag, path).map_err(err)?;
            match s.permissions {
                Permissions::Xattr => {
                    fs.set_permission_semantics(PermissionSemantics::LinuxComplete)
                }
                Permissions::Caller => fs
                    .set_permission_semantics(PermissionSemantics::LinuxSimplifiedFixedOwner)
                    .and_then(|()| fs.set_fixed_owner(0, 0))
                    .and_then(|()| fs.set_caller_owns(true)),
                Permissions::CallerUncached => {
                    fs.set_permission_semantics(PermissionSemantics::LinuxSimplified)
                }
                Permissions::Fixed { uid, gid } => fs
                    .set_permission_semantics(PermissionSemantics::LinuxSimplifiedFixedOwner)
                    .and_then(|()| fs.set_fixed_owner(uid, gid)),
            }
            .map_err(err)?;
            fs.set_request_queues(crate::fs_request_queues(cfg.cpus))
                .map_err(err)?;
            if let Some((t, _, caches)) = &mut fscache {
                fs.set_cache_timeouts(t.entry, t.attr, t.negative)
                    .map_err(err)?;
                caches.push(crate::fscache::Share {
                    root: s.host_path.clone(),
                    cache: fs.guest_cache().context("share without a file system")?,
                });
            }
            devices.add(fs);
        }
        if let Amd64::Rosetta { dir } = &cfg.amd64 {
            let path = dir.to_str().context("Rosetta path is not utf-8")?;
            let mut fs = FsDevice::new_read_only("rosetta", path).map_err(err)?;
            // Nothing changes the runtime while the VM runs, and every x86
            // exec opens it (/proc/self/exe): keep its pages and entries.
            let day = Duration::from_secs(86400);
            fs.set_permission_semantics(PermissionSemantics::LinuxSimplifiedFixedOwner)
                .and_then(|()| fs.set_fixed_owner(0, 0))
                .and_then(|()| fs.set_cache_policy(CachePolicy::Always))
                .and_then(|()| fs.set_cache_timeouts(day, day, day))
                .and_then(|()| fs.set_rosetta())
                .map_err(err)?;
            devices.add(fs);
        }
        let mut gvproxy_starting = None;
        if let Some(n) = &cfg.network {
            let net = if let Some(p) = &n.in_process {
                let (stack, starting) = crate::inproc::start(&p.library, &p.args)?;
                gvproxy_starting = Some(starting);
                NetDevice::new_in_process("net0", stack, &n.mac, NET_FEATURES).map_err(err)?
            } else {
                let path = n
                    .socket
                    .to_str()
                    .context("network socket path is not utf-8")?;
                NetDevice::new_unixgram_path("net0", path, &n.mac, NET_FEATURES, NetFlags::VFKIT)
                    .map_err(err)?
            };
            devices.add(net);
        }

        let kernel = cfg.kernel.to_str().context("kernel path is not utf-8")?;
        let initramfs = cfg
            .initramfs
            .to_str()
            .context("initramfs path is not utf-8")?;
        let payload =
            Payload::load_external(kernel, KernelFormat::Raw, Some(initramfs), &cfg.cmdline())
                .map_err(err)?;

        if let Some((_, listener, shares)) = fscache {
            crate::fscache::start(shares, listener).context("watch the shares")?;
        }

        let vmm = VmmBuilder::new()
            .vcpus(cfg.cpus)
            .map_err(err)?
            .ram_mib(cfg.memory_mib)
            .map_err(err)?
            .payload(payload)
            .devices(devices)
            .build()
            .map_err(err)?;
        crate::vmctl::serve_signals(vmm.handle().map_err(err)?).context("serve control signals")?;
        // libkrun connects to the network socket when the guest driver
        // activates the device, so gvproxy only has to be up by then; the
        // daemon starts both processes at once and this wait overlaps them.
        if let Some(n) = cfg.network.as_ref().filter(|n| n.in_process.is_none()) {
            wait_for_socket(&n.socket, Duration::from_secs(10))?;
        }
        if let Some(starting) = gvproxy_starting {
            starting.wait()?;
        }
        vmm.run();
        Err(anyhow!("libkrun event loop returned"))
    }

    fn wait_for_socket(path: &Path, timeout: Duration) -> anyhow::Result<()> {
        let deadline = Instant::now() + timeout;
        while !path.exists() {
            anyhow::ensure!(
                Instant::now() < deadline,
                "network socket {} did not appear within {}s",
                path.display(),
                timeout.as_secs()
            );
            std::thread::sleep(Duration::from_micros(500));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_fs_request_queue_per_vcpu_up_to_sixteen() {
        assert_eq!(fs_request_queues(0), 1);
        assert_eq!(fs_request_queues(1), 1);
        assert_eq!(fs_request_queues(14), 14);
        assert_eq!(fs_request_queues(64), 16);
    }

    fn cfg() -> VmConfig {
        VmConfig {
            kernel: "/k".into(),
            initramfs: "/i".into(),
            cmdline_extra: String::new(),
            cpus: 2,
            memory_mib: 2048,
            console_log: "/c".into(),
            control_socket: "/s".into(),
            disks: vec![],
            host_listeners: vec![],
            shares: vec![],
            network: None,
            amd64: Amd64::Off,
        }
    }

    #[test]
    fn cmdline_names_the_amd64_emulator() {
        let mut c = cfg();
        c.amd64 = Amd64::Qemu;
        assert_eq!(c.cmdline(), format!("{BASE_CMDLINE} kat.amd64=qemu"));
        c.amd64 = Amd64::Rosetta {
            dir: "/Library/Apple/usr/libexec/oah/RosettaLinux".into(),
        };
        assert_eq!(c.cmdline(), format!("{BASE_CMDLINE} kat.amd64=rosetta"));
    }

    #[test]
    fn amd64_defaults_to_off_in_older_configs() {
        let mut v = serde_json::to_value(cfg()).unwrap();
        v.as_object_mut().unwrap().remove("amd64");
        let c: VmConfig = serde_json::from_value(v).unwrap();
        assert_eq!(c.amd64, Amd64::Off);
    }

    #[test]
    fn cmdline_appends_extra() {
        let mut c = cfg();
        assert_eq!(c.cmdline(), BASE_CMDLINE);
        c.cmdline_extra = " loglevel=7 ".into();
        assert_eq!(c.cmdline(), format!("{BASE_CMDLINE} loglevel=7"));
    }

    #[test]
    fn cmdline_lists_shares() {
        let mut c = cfg();
        c.shares.push(Share {
            tag: "users".into(),
            host_path: "/Users".into(),
            guest_path: "/Users".into(),
            permissions: Permissions::default(),
        });
        assert_eq!(
            c.cmdline(),
            format!("{BASE_CMDLINE} kat.share=users:/Users:owner_caller")
        );
        c.shares[0].permissions = Permissions::Xattr;
        assert_eq!(
            c.cmdline(),
            format!("{BASE_CMDLINE} kat.share=users:/Users")
        );
    }

    fn network() -> Network {
        Network {
            in_process: None,
            socket: "/n".into(),
            mac: [0x5a, 0x94, 0xef, 0xe4, 0x0c, 0xee],
            mtu: 1500,
            guest_ip: Ipv4Addr::new(192, 168, 127, 2),
            prefix_len: 24,
            gateway: Ipv4Addr::new(192, 168, 127, 1),
            host_ip: Ipv4Addr::new(192, 168, 127, 254),
        }
    }

    #[test]
    fn cmdline_carries_network() {
        let mut c = cfg();
        c.network = Some(network());
        assert_eq!(
            c.cmdline(),
            format!("{BASE_CMDLINE} kat.net=192.168.127.2/24,192.168.127.1,192.168.127.254,1500")
        );
    }

    #[test]
    fn config_round_trips_as_json() {
        let mut c = cfg();
        c.network = Some(network());
        let s = serde_json::to_string(&c).unwrap();
        assert_eq!(serde_json::from_str::<VmConfig>(&s).unwrap(), c);
    }

    #[test]
    fn validate_rejects_duplicate_vsock_ports() {
        let dir = std::env::temp_dir().join(format!("kat-vmm-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("k"), b"").unwrap();
        std::fs::write(dir.join("i"), b"").unwrap();
        let mut c = cfg();
        c.kernel = dir.join("k");
        c.initramfs = dir.join("i");
        c.validate().unwrap();
        c.host_listeners.push(HostListener {
            port: kat_proto::CONTROL_PORT,
            path: "/x".into(),
            kernel_only: false,
            loopback_tcp: false,
        });
        assert!(c.validate().unwrap_err().to_string().contains("unique"));
        c.host_listeners[0].port = kat_proto::FSCACHE_PORT;
        assert!(c.validate().unwrap_err().to_string().contains("unique"));
        c.host_listeners.clear();
        c.shares.push(Share {
            tag: "s".into(),
            host_path: dir.clone(),
            guest_path: "/mnt/a:b".into(),
            permissions: Permissions::default(),
        });
        assert!(c.validate().unwrap_err().to_string().contains("invalid"));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn validate_rejects_missing_files_and_bad_sizes() {
        let mut c = cfg();
        assert!(c.validate().is_err());
        c.cpus = 0;
        assert!(c.validate().unwrap_err().to_string().contains("cpus"));
    }

    #[test]
    fn validate_enforces_vm_exec_host_listener_invariants() {
        let dir = std::env::temp_dir().join(format!("kat-vmm-exec-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("k"), b"").unwrap();
        std::fs::write(dir.join("i"), b"").unwrap();
        let mut c = cfg();
        c.kernel = dir.join("k");
        c.initramfs = dir.join("i");

        c.host_listeners.push(HostListener {
            port: kat_proto::VM_EXEC_PORT,
            path: dir.join("vm-exec.sock"),
            kernel_only: false,
            loopback_tcp: false,
        });
        c.validate().unwrap();

        // VM_EXEC_PORT must never be exposed via loopback TCP or kernel-only mode.
        c.host_listeners[0].loopback_tcp = true;
        assert!(c.validate().is_err());
        c.host_listeners[0].loopback_tcp = false;
        c.host_listeners[0].kernel_only = true;
        assert!(c.validate().is_err());
        c.host_listeners[0].kernel_only = false;

        // Relative paths and port 0 are rejected for all host listeners.
        c.host_listeners[0].path = PathBuf::from("vm-exec.sock");
        assert!(c.validate().is_err());
        c.host_listeners[0].path = dir.join("vm-exec.sock");
        c.host_listeners[0].port = 0;
        assert!(c.validate().is_err());

        std::fs::remove_dir_all(&dir).unwrap();
    }
}
