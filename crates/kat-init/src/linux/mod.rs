mod amd64;
mod autofwd;
mod clock;
mod fscache;
mod fsnotify;
mod k8s;
mod machines;
mod memory;
mod net;
pub mod nfs;
mod ports;
mod relay;
mod service;
mod shutdown;
mod stats;
mod sys;
mod trim;

use std::fs::File;
use std::io;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use kat_proto::{CONTROL_PORT, DOCKER_PORT, Hello, K8S_API_PORT, Msg, PROTOCOL_VERSION, read_msg};

use service::{DockerStack, Notifier, Reaper};

const HOST_CID: u32 = 2;
const DIAL_ATTEMPTS: u32 = 2000;

/// The system image (read-only EROFS) and the data disk (btrfs).
const SYSTEM_DISK: &str = "/dev/vda";
const DATA_DISK: &str = "/dev/vdb";
const NEW_ROOT: &str = "/mnt/root";
/// Data-disk subvolumes and where they are mounted in the system image.
const DATA_MOUNTS: [(&str, &str); 3] = [
    ("katamaran", "/var/lib/katamaran"),
    ("docker", "/var/lib/docker"),
    ("containerd", "/var/lib/containerd"),
];

pub fn run() -> ! {
    let init_start_ns = sys::boottime_ns();
    let (system, setup_error) = match setup() {
        Ok(system) => (system, None),
        Err(e) => {
            log(&format!("setup failed: {e}"));
            (false, Some(e.to_string()))
        }
    };
    match serve(init_start_ns, system, setup_error) {
        Ok(()) => log("powering off"),
        Err(e) => log(&format!("control channel failed: {e}; powering off")),
    }
    sys::power_off()
}

/// Mount the basics. With a system disk attached, also mount the system
/// image and data disk and switch into the system image. Returns whether the
/// system image is running.
fn setup() -> io::Result<bool> {
    let nosuid_nodev_noexec = libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC;
    sys::mount("proc", "/proc", "proc", nosuid_nodev_noexec, None)?;
    sys::mount("sysfs", "/sys", "sysfs", nosuid_nodev_noexec, None)?;
    sys::mount(
        "devtmpfs",
        "/dev",
        "devtmpfs",
        libc::MS_NOSUID,
        Some("mode=0755"),
    )?;

    let system = sys::exists(SYSTEM_DISK);
    if system {
        mount_system()?;
    }

    sys::mount(
        "devpts",
        "/dev/pts",
        "devpts",
        libc::MS_NOSUID | libc::MS_NOEXEC,
        Some("ptmxmode=0666,mode=0620,gid=5"),
    )?;
    sys::mount(
        "tmpfs",
        "/dev/shm",
        "tmpfs",
        libc::MS_NOSUID | libc::MS_NODEV,
        Some("mode=1777"),
    )?;
    sys::mount(
        "tmpfs",
        "/run",
        "tmpfs",
        libc::MS_NOSUID | libc::MS_NODEV,
        Some("mode=0755"),
    )?;
    sys::mount(
        "tmpfs",
        "/tmp",
        "tmpfs",
        libc::MS_NOSUID | libc::MS_NODEV,
        Some("mode=1777"),
    )?;
    sys::mount(
        "cgroup2",
        "/sys/fs/cgroup",
        "cgroup2",
        nosuid_nodev_noexec,
        Some("nsdelegate,memory_recursiveprot"),
    )?;
    sys::sethostname("katamaran")?;
    if system {
        std::fs::write("/etc/hostname", "katamaran\n")?;
        sys::link_up("lo")?;
    }
    let cmdline = std::fs::read_to_string("/proc/cmdline")?;
    mount_shares(&cmdline)?;
    if let Err(e) = amd64::setup(&cmdline) {
        log(&format!("amd64 programs will not run: {e}"));
    }
    Ok(system)
}

/// Mount the virtio-fs shares the host lists as `kat.share=tag:path[:options]`.
fn mount_shares(cmdline: &str) -> io::Result<()> {
    for (tag, path, opts) in parse_shares(cmdline) {
        sys::mount(tag, path, "virtiofs", libc::MS_NODEV, opts)?;
    }
    Ok(())
}

fn parse_shares(cmdline: &str) -> Vec<(&str, &str, Option<&str>)> {
    cmdline
        .split_whitespace()
        .filter_map(|arg| arg.strip_prefix("kat.share="))
        .filter_map(|v| {
            let mut parts = v.splitn(3, ':');
            let (tag, path) = (parts.next()?, parts.next()?);
            Some((tag, path, parts.next().filter(|o| !o.is_empty())))
        })
        .filter(|(tag, path, _)| !tag.is_empty() && path.starts_with('/'))
        .collect()
}

/// System image as an overlay (read-only EROFS lower, tmpfs upper), data disk
/// subvolumes inside it, then switch root.
fn mount_system() -> io::Result<()> {
    let ro = libc::MS_RDONLY | libc::MS_NODEV;
    sys::mount(SYSTEM_DISK, "/mnt/system", "erofs", ro, None)?;
    sys::mount(
        "tmpfs",
        "/mnt/overlay",
        "tmpfs",
        libc::MS_NODEV,
        Some("mode=0755"),
    )?;
    sys::mkdir("/mnt/overlay/upper")?;
    sys::mkdir("/mnt/overlay/work")?;
    sys::mount(
        "overlay",
        NEW_ROOT,
        "overlay",
        0,
        Some("lowerdir=/mnt/system,upperdir=/mnt/overlay/upper,workdir=/mnt/overlay/work"),
    )?;

    if sys::exists(DATA_DISK) {
        for (subvol, target) in DATA_MOUNTS {
            let opts = format!("subvol={subvol},discard=async");
            sys::mount(
                DATA_DISK,
                &format!("{NEW_ROOT}{target}"),
                "btrfs",
                libc::MS_NOATIME,
                Some(&opts),
            )?;
        }
    } else {
        log("no data disk; Docker data lives in memory");
    }

    for m in ["/dev", "/proc", "/sys"] {
        sys::move_mount(m, &format!("{NEW_ROOT}{m}"))?;
    }
    sys::switch_root(NEW_ROOT)
}

fn serve(init_start_ns: u64, system: bool, setup_error: Option<String>) -> io::Result<()> {
    let conn = dial_host(CONTROL_PORT)?;
    let notifier = Notifier(Arc::new(Mutex::new(conn.try_clone()?)));
    notifier.send(&Msg::Hello(Hello {
        protocol: PROTOCOL_VERSION,
        init_start_ns,
        ready_ns: sys::boottime_ns(),
        mem_total_kib: mem_total_kib().unwrap_or(0),
        kernel_release: sys::kernel_release(),
    }));
    if let Some(error) = setup_error {
        notifier.send(&Msg::ServiceFailed {
            name: "system".into(),
            error,
        });
    }

    start_network(&notifier);
    memory::tune();
    memory::start();

    let docker = if system {
        let reaper = Reaper::start();
        let cmdline = std::fs::read_to_string("/proc/cmdline").unwrap_or_default();
        if k8s::enabled_in_cmdline(&cmdline)
            && let Err(e) = k8s::ensure_containerd_cri()
        {
            notifier.failed("k8s", e);
        }
        relay::serve_unix_on_vsock(DOCKER_PORT, service::DOCKER_SOCKET)?;
        relay::serve_tcp_on_vsock(K8S_API_PORT, k8s::API_ADDR)?;
        let nfs = nfs::Nfsd::start(reaper.clone(), notifier.clone())?;
        let dns = net::parse(&cmdline).map(|n| n.gateway);
        if let Err(e) = machines::start(reaper.clone(), dns) {
            notifier.failed("machines", e.to_string());
        }
        trim::start_periodic();
        let stack = DockerStack::start(reaper.clone(), notifier.clone());
        if let Err(e) = k8s::start_service(reaper, notifier.clone(), stack.clone()) {
            notifier.failed("k8s", e.to_string());
        }
        Some((stack, nfs))
    } else {
        None
    };

    let shares = std::fs::read_to_string("/proc/cmdline").unwrap_or_default();
    let mounts = fsnotify::share_mounts(parse_shares(&shares).into_iter().map(|(_, p, _)| p));
    let fsnotify = fsnotify::FsNotify::start(mounts, notifier.clone())
        .inspect_err(|e| log(&format!("no host file-change events: {e}")))
        .ok()
        .flatten();
    if let Some(f) = &fsnotify {
        let paths = parse_shares(&shares)
            .into_iter()
            .map(|(_, p, _)| p.to_owned());
        fscache::start(f.device(), paths.collect());
    }

    let result = control_loop(conn, &notifier, fsnotify.as_ref());
    shutdown::run(docker.as_ref().map(|(d, nfs)| (d, nfs)), system);
    result
}

/// eth0 takes a few ioctls, so it is configured inline: dockerd must find
/// resolv.conf in place when it starts.
fn start_network(notifier: &Notifier) {
    let cmdline = std::fs::read_to_string("/proc/cmdline").unwrap_or_default();
    match net::setup(&cmdline) {
        Ok(Some(_)) => {
            let auto = autofwd::enabled(&cmdline)
                .then(autofwd::AutoForward::start)
                .and_then(|r| {
                    r.inspect_err(|e| log(&format!("no auto-forward: {e}")))
                        .ok()
                });
            ports::watch(notifier.clone(), auto)
        }
        Ok(None) => {}
        Err(e) => {
            log(&format!("network setup failed: {e}"));
            notifier.send(&Msg::ServiceFailed {
                name: "network".into(),
                error: e.to_string(),
            });
        }
    }
}

fn control_loop(
    mut conn: File,
    notifier: &Notifier,
    fsnotify: Option<&fsnotify::FsNotify>,
) -> io::Result<()> {
    while let Some(msg) = read_msg(&mut conn)? {
        match msg {
            Msg::SetTime { unix_ns } => {
                if let Err(e) = clock::sync(unix_ns) {
                    log(&format!("clock not set: {e}"));
                }
            }
            Msg::Trim => {
                let notifier = notifier.clone();
                thread::spawn(move || trim::trim_and_report(&notifier));
            }
            Msg::Ping { seq } => notifier.send(&Msg::Pong { seq }),
            Msg::StatsRequest => notifier.send(&Msg::Stats(stats::read())),
            Msg::FsEvents(events) => {
                if let Some(f) = fsnotify {
                    f.apply(&events);
                }
            }
            Msg::Shutdown => {
                log("shutdown requested");
                return Ok(());
            }
            other => log(&format!("ignoring unexpected message {other:?}")),
        }
    }
    log("host closed the control channel");
    Ok(())
}

/// Connect to the host over vsock. The host registers its listener before
/// starting the VM, so this normally succeeds on the first attempt; the retry
/// covers the vsock device finishing its own setup.
fn dial_host(port: u32) -> io::Result<File> {
    let mut last = io::Error::other("no attempt made");
    for _ in 0..DIAL_ATTEMPTS {
        match sys::vsock_connect(HOST_CID, port) {
            Ok(fd) => return Ok(File::from(fd)),
            Err(e) => last = e,
        }
        thread::sleep(Duration::from_millis(1));
    }
    Err(last)
}

fn mem_total_kib() -> Option<u64> {
    parse_mem_total(&std::fs::read_to_string("/proc/meminfo").ok()?)
}

fn parse_mem_total(meminfo: &str) -> Option<u64> {
    meminfo
        .lines()
        .find_map(|l| l.strip_prefix("MemTotal:"))
        .and_then(|v| v.trim().trim_end_matches("kB").trim().parse().ok())
}

fn log(msg: &str) {
    eprintln!("kat-init: {msg}");
}

#[cfg(test)]
mod tests {
    use super::{parse_mem_total, parse_shares};

    #[test]
    fn parses_shares_from_cmdline() {
        let c = "console=hvc0 kat.share=users:/Users:owner_caller quiet kat.share=bad kat.share=x:rel kat.share=tmp:/tmp/x\n";
        assert_eq!(
            parse_shares(c),
            vec![
                ("users", "/Users", Some("owner_caller")),
                ("tmp", "/tmp/x", None)
            ]
        );
    }

    #[test]
    fn parses_mem_total() {
        let s = "MemTotal:        4028396 kB\nMemFree:  100 kB\n";
        assert_eq!(parse_mem_total(s), Some(4028396));
        assert_eq!(parse_mem_total("MemFree: 1 kB\n"), None);
    }
}
