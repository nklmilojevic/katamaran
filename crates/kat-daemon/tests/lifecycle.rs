//! VM lifecycle: memory and disk space going back to macOS, services and
//! the VM restarting after a crash, and the guest clock and devices across a
//! (simulated) sleep. Boots the system VM; ignored by default, `cargo xtask
//! test-vm` builds everything and runs it.
//!
//! The test image comes from the host's Docker (`docker save`;
//! KAT_TEST_IMAGE_CONTEXT names the context, default the current one),
//! read-only.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::MetadataExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const IMAGE: &str = "alpine:3.24.4";
const MIB: u64 = 1 << 20;
const GIB: u64 = 1 << 30;

fn root() -> PathBuf {
    // Canonical, so socket paths stay under SUN_LEN.
    std::fs::canonicalize(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")).unwrap()
}

struct Vm {
    child: Child,
    lines: mpsc::Receiver<String>,
    output: Vec<String>,
    home: PathBuf,
}

impl Vm {
    fn start(extra: &[&str]) -> Vm {
        let root = root();
        let home = root.join(format!("target/test-life-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&home);
        std::fs::create_dir_all(&home).unwrap();
        let mut child = Command::new(root.join("target/release/katamaran"))
            .current_dir(&root)
            .env("KATAMARAN_HOME", &home)
            .args(["run", "--cpus", "2"])
            .args(if extra.contains(&"--memory-mib") {
                &[][..]
            } else {
                &["--memory-mib", "2048"][..]
            })
            .args(extra)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn katamaran run");
        let (tx, lines) = mpsc::channel();
        for stream in [
            Box::new(child.stdout.take().unwrap()) as Box<dyn Read + Send>,
            Box::new(child.stderr.take().unwrap()),
        ] {
            let tx = tx.clone();
            thread::spawn(move || {
                for line in BufReader::new(stream).lines().map_while(Result::ok) {
                    let _ = tx.send(line);
                }
            });
        }
        let mut vm = Vm {
            child,
            lines,
            output: Vec::new(),
            home,
        };
        vm.wait_for("docker ready", Duration::from_secs(120));
        vm.load_image();
        vm
    }

    /// Wait for a line containing `needle`; panic on timeout with all output.
    /// Returns when it arrived.
    fn wait_for(&mut self, needle: &str, timeout: Duration) -> Instant {
        let deadline = Instant::now() + timeout;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            match self.lines.recv_timeout(left) {
                Ok(line) => {
                    eprintln!("| {line}");
                    let hit = line.contains(needle);
                    self.output.push(line);
                    if hit {
                        return Instant::now();
                    }
                }
                Err(_) => panic!(
                    "no `{needle}` within {timeout:?}; output:\n{}",
                    self.output.join("\n")
                ),
            }
        }
    }

    fn docker_host(&self) -> String {
        format!("unix://{}", self.home.join("run/docker.sock").display())
    }

    fn try_docker(&self, args: &[&str]) -> Output {
        Command::new("docker")
            .env("DOCKER_HOST", self.docker_host())
            .args(args)
            .output()
            .expect("run docker")
    }

    fn docker(&self, args: &[&str]) -> String {
        let out = self.try_docker(args);
        assert!(
            out.status.success(),
            "docker {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).into_owned()
    }

    fn load_image(&self) {
        let mut save = image_docker()
            .args(["save", IMAGE])
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let load = Command::new("docker")
            .env("DOCKER_HOST", self.docker_host())
            .args(["load", "-q"])
            .stdin(save.stdout.take().unwrap())
            .output()
            .unwrap();
        assert!(
            save.wait().unwrap().success() && load.status.success(),
            "load {IMAGE}"
        );
    }

    fn vm_pid(&self) -> i32 {
        std::fs::read_to_string(self.home.join("run/vm.pid"))
            .unwrap()
            .trim()
            .parse()
            .unwrap()
    }

    fn signal(&self, sig: i32) {
        assert_eq!(unsafe { libc::kill(self.child.id() as i32, sig) }, 0);
    }

    fn data_img_bytes(&self) -> u64 {
        std::fs::metadata(self.home.join("data/data.img"))
            .unwrap()
            .blocks()
            * 512
    }

    fn docker_ping(&self, timeout: Duration) -> bool {
        let Ok(mut s) = UnixStream::connect(self.home.join("run/docker.sock")) else {
            return false;
        };
        s.set_read_timeout(Some(timeout)).unwrap();
        if s.write_all(b"GET /_ping HTTP/1.0\r\nHost: docker\r\n\r\n")
            .is_err()
        {
            return false;
        }
        let mut resp = String::new();
        let _ = s.read_to_string(&mut resp);
        resp.starts_with("HTTP/1.0 200")
    }

    fn guest_time(&self) -> u64 {
        self.docker(&["run", "--rm", IMAGE, "date", "+%s"])
            .trim()
            .parse()
            .unwrap()
    }

    fn stop(mut self) {
        self.signal(libc::SIGINT);
        self.wait_for("VM stopped", Duration::from_secs(60));
        let status = self.child.wait().unwrap();
        assert!(status.success(), "katamaran run exited with {status}");
    }
}

/// A failing assertion must not leave a VM running.
impl Drop for Vm {
    fn drop(&mut self) {
        if let Ok(None) = self.child.try_wait() {
            unsafe { libc::kill(self.child.id() as i32, libc::SIGINT) };
            for _ in 0..600 {
                if let Ok(Some(_)) = self.child.try_wait() {
                    break;
                }
                thread::sleep(Duration::from_millis(100));
            }
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        // A VM process left over after the daemon was killed.
        if let Some(pid) = std::fs::read_to_string(self.home.join("run/vm.pid"))
            .ok()
            .and_then(|s| s.trim().parse::<i32>().ok())
        {
            unsafe { libc::kill(pid, libc::SIGKILL) };
        }
        let _ = Command::new("/sbin/umount")
            .arg("-f")
            .arg(self.home.join("mnt"))
            .output();
        let _ = std::fs::remove_dir_all(&self.home);
    }
}

/// `docker` on the engine that supplies the test images: the context named by
/// KAT_TEST_IMAGE_CONTEXT, or else the current Docker context.
fn image_docker() -> Command {
    let mut docker = Command::new("docker");
    if let Ok(ctx) = std::env::var("KAT_TEST_IMAGE_CONTEXT") {
        docker.args(["--context", &ctx]);
    }
    docker
}

/// phys_footprint of `pid` (what Activity Monitor shows as Memory).
fn footprint(pid: i32) -> u64 {
    let mut info: libc::rusage_info_v4 = unsafe { std::mem::zeroed() };
    let rc = unsafe {
        libc::proc_pid_rusage(
            pid,
            libc::RUSAGE_INFO_V4,
            (&mut info as *mut libc::rusage_info_v4).cast(),
        )
    };
    assert_eq!(rc, 0, "proc_pid_rusage({pid})");
    info.ri_phys_footprint
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn within(timeout: Duration, what: &str, mut check: impl FnMut() -> bool) -> Duration {
    let t = Instant::now();
    while !check() {
        assert!(t.elapsed() < timeout, "{what} not within {timeout:?}");
        thread::sleep(Duration::from_millis(250));
    }
    t.elapsed()
}

/// An HTTP responder on the Mac's 127.0.0.1, for containers to reach as
/// host.docker.internal.
fn mac_listener() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    thread::spawn(move || {
        for mut s in l.incoming().map_while(Result::ok) {
            let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
            let mut buf = [0u8; 1024];
            let _ = s.read(&mut buf);
            let _ = s.write_all(b"HTTP/1.0 200 OK\r\nContent-Length: 7\r\n\r\nfrommac");
        }
    });
    port
}

/// The guest reaches the Mac through gvproxy, and the Mac reaches a
/// published container port through gvproxy's forwarder.
fn check_network(vm: &Vm, mac_port: u16, name: &str) {
    let url = format!("http://host.docker.internal:{mac_port}/");
    let got = vm.docker(&["run", "--rm", IMAGE, "wget", "-qO-", &url]);
    assert!(got.starts_with("frommac"), "from the Mac: {got:?}");

    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let publish = format!("{port}:80");
    let server = r#"while true; do printf 'HTTP/1.0 200 OK\r\nContent-Length: 8\r\n\r\ninguest\n' | nc -l -p 80; done"#;
    vm.docker(&[
        "run", "-d", "--rm", "--name", name, "-p", &publish, IMAGE, "sh", "-c", server,
    ]);
    within(Duration::from_secs(15), "the published port", || {
        let Ok(mut s) = TcpStream::connect(("127.0.0.1", port)) else {
            return false;
        };
        let _ = s.set_read_timeout(Some(Duration::from_secs(1)));
        let _ = s.write_all(b"GET / HTTP/1.0\r\n\r\n");
        let mut resp = String::new();
        let _ = s.read_to_string(&mut resp);
        resp.contains("inguest")
    });
    vm.docker(&["rm", "-f", name]);
}

#[test]
#[ignore = "boots the system VM; run with `cargo xtask test-vm`"]
fn memory_goes_back_to_macos_after_a_4gib_container() {
    let vm = Vm::start(&["--memory-mib", "8192"]);
    let pid = vm.vm_pid();
    // Let boot-time free memory be reported.
    thread::sleep(Duration::from_secs(20));
    let idle = footprint(pid);

    let peak = Arc::new(AtomicU64::new(idle));
    let running = Arc::new(AtomicBool::new(true));
    let sampler = {
        let (peak, running) = (peak.clone(), running.clone());
        thread::spawn(move || {
            while running.load(Ordering::SeqCst) {
                peak.fetch_max(footprint(pid), Ordering::SeqCst);
                thread::sleep(Duration::from_millis(200));
            }
        })
    };
    let out = vm.docker(&[
        "run",
        "--rm",
        "--tmpfs",
        "/m:size=5g",
        IMAGE,
        "sh",
        "-c",
        "dd if=/dev/urandom of=/m/x bs=1M count=4096 2>&1 | tail -1",
    ]);
    assert!(out.contains("4294967296 bytes"), "dd: {out}");
    running.store(false, Ordering::SeqCst);
    sampler.join().unwrap();
    let peak = peak.load(Ordering::SeqCst);
    assert!(peak >= idle + 3 * GIB, "idle {idle}, peak {peak}");

    let took = within(Duration::from_secs(60), "footprint back near idle", || {
        footprint(pid) <= idle + 512 * MIB
    });
    eprintln!(
        "footprint: idle {} MiB, peak {} MiB, {} MiB after {took:?}",
        idle / MIB,
        peak / MIB,
        footprint(pid) / MIB
    );
    vm.stop();
}

#[test]
#[ignore = "boots the system VM; run with `cargo xtask test-vm`"]
fn idle_page_cache_is_trimmed_and_given_back() {
    let vm = Vm::start(&["--memory-mib", "8192"]);
    let pid = vm.vm_pid();
    thread::sleep(Duration::from_secs(20));
    let idle = footprint(pid);
    // A file that stays: its pages stay cached until the guest trims them.
    vm.docker(&["run", "-d", "--name", "cache", IMAGE, "sleep", "600"]);
    vm.docker(&[
        "exec",
        "cache",
        "sh",
        "-c",
        "dd if=/dev/urandom of=/keep bs=1M count=3072 2>/dev/null && sync",
    ]);
    let full = footprint(pid);
    assert!(full >= idle + 2 * GIB, "idle {idle}, with the file {full}");
    // 8 GiB of guest memory keeps a 512 MiB cache. macOS charges the VM
    // about 1.6x for guest pages that went through the disk, so allow 1 GiB.
    let took = within(Duration::from_secs(60), "cache trimmed", || {
        footprint(pid) <= idle + GIB
    });
    assert!(
        full >= footprint(pid) + 3 * GIB,
        "only {} MiB released",
        (full - footprint(pid)) / MIB
    );
    eprintln!(
        "footprint: idle {} MiB, {} MiB with a cached 3 GiB file, {} MiB {took:?} later",
        idle / MIB,
        full / MIB,
        footprint(pid) / MIB
    );
    vm.docker(&["rm", "-f", "cache"]);
    vm.stop();
}

#[test]
#[ignore = "boots the system VM; run with `cargo xtask test-vm`"]
fn zram_swap_absorbs_memory_spike_and_reclaims() {
    let vm = Vm::start(&["--memory-mib", "2048"]);
    let pid = vm.vm_pid();
    let swaps = vm.docker(&["run", "--rm", IMAGE, "cat", "/proc/swaps"]);
    assert!(swaps.contains("/dev/zram0"), "expected zram0 swap: {swaps}");
    let meminfo = vm.docker(&["run", "--rm", IMAGE, "cat", "/proc/meminfo"]);
    let swap_total_kib: u64 = meminfo
        .lines()
        .find_map(|l| l.strip_prefix("SwapTotal:"))
        .and_then(|v| v.trim().trim_end_matches("kB").trim().parse().ok())
        .unwrap_or(0);
    assert!(
        swap_total_kib >= 1500 * 1024,
        "SwapTotal too small ({swap_total_kib} KiB): {meminfo}"
    );

    thread::sleep(Duration::from_secs(15));
    let idle = footprint(pid);

    // Allocate 2.5 GiB of non-zero compressible data inside a 2 GiB VM:
    // without zram swap this hits global OOM immediately; with LZ4 zram it
    // completes cleanly and discards the swapped pages back to free RAM on exit.
    let out = vm.docker(&[
        "run",
        "--rm",
        "--tmpfs",
        "/m:size=3g",
        IMAGE,
        "sh",
        "-c",
        "yes 'katamaran-zram-compressible-payload-0123456789abcdef' | head -c 2684354560 > /m/x && stat -c %s /m/x",
    ]);
    assert!(
        out.trim().ends_with("2684354560"),
        "write failed or OOMed: {out}"
    );

    let took = within(Duration::from_secs(60), "footprint back near idle", || {
        footprint(pid) <= idle + 512 * MIB
    });
    eprintln!(
        "zram spike: swap {} MiB, idle {} MiB, {} MiB after {took:?}",
        swap_total_kib / 1024,
        idle / MIB,
        footprint(pid) / MIB
    );
    vm.stop();
}

#[test]
#[ignore = "boots the system VM; run with `cargo xtask test-vm`"]
fn disk_space_goes_back_to_apfs_after_deleting_5gib() {
    let vm = Vm::start(&[]);
    let before = vm.data_img_bytes();
    vm.docker(&["run", "-d", "--name", "filler", IMAGE, "sleep", "600"]);
    vm.docker(&[
        "exec",
        "filler",
        "sh",
        "-c",
        "dd if=/dev/urandom of=/big bs=1M count=5120 2>/dev/null && sync",
    ]);
    let full = vm.data_img_bytes();
    assert!(
        full >= before + 5 * GIB - 256 * MIB,
        "before {before}, full {full}"
    );

    vm.docker(&["exec", "filler", "sh", "-c", "rm /big && sync"]);
    let took = within(Duration::from_secs(120), "data.img to shrink", || {
        vm.data_img_bytes() <= before + 512 * MIB
    });
    eprintln!(
        "data.img: {} MiB, {} MiB with 5 GiB, {} MiB {took:?} after the delete",
        before / MIB,
        full / MIB,
        vm.data_img_bytes() / MIB
    );
    vm.docker(&["rm", "-f", "filler"]);
    vm.stop();
}

#[test]
#[ignore = "boots the system VM; run with `cargo xtask test-vm`"]
fn dockerd_and_containerd_are_restarted_when_they_die() {
    let mut vm = Vm::start(&[]);
    vm.docker(&[
        "run",
        "-d",
        "--rm",
        "--privileged",
        "--pid=host",
        IMAGE,
        "sh",
        "-c",
        "sleep 1; kill -9 $(pidof dockerd)",
    ]);
    let killed = vm.wait_for(
        "dockerd failed: dockerd killed by signal 9; restarting",
        Duration::from_secs(30),
    );
    let ready = vm.wait_for("docker ready", Duration::from_secs(60));
    eprintln!("dockerd back {:?} after it was killed", ready - killed);
    vm.docker(&["run", "--rm", IMAGE, "true"]);

    vm.docker(&[
        "run",
        "-d",
        "--rm",
        "--privileged",
        "--pid=host",
        IMAGE,
        "sh",
        "-c",
        "sleep 1; kill -9 $(pidof containerd)",
    ]);
    let killed = vm.wait_for(
        "containerd failed: containerd killed by signal 9; restarting",
        Duration::from_secs(30),
    );
    let took = within(
        Duration::from_secs(60),
        "docker run after containerd",
        || {
            vm.try_docker(&["run", "--rm", IMAGE, "true"])
                .status
                .success()
        },
    );
    eprintln!(
        "docker run works {:?} after containerd was killed",
        killed.elapsed().max(took)
    );
    vm.stop();
}

#[test]
#[ignore = "boots the system VM; run with `cargo xtask test-vm`"]
fn a_killed_vm_process_is_restarted() {
    let mut vm = Vm::start(&[]);
    let mac_port = mac_listener();
    let old = vm.vm_pid();
    vm.docker(&["volume", "create", "kept"]);
    vm.docker(&[
        "run",
        "--rm",
        "-v",
        "kept:/v",
        IMAGE,
        "sh",
        "-c",
        "echo before > /v/f && sync",
    ]);

    let t = Instant::now();
    assert_eq!(unsafe { libc::kill(old, libc::SIGKILL) }, 0);
    vm.wait_for("restarting the VM", Duration::from_secs(30));
    let up = vm.wait_for("docker ready", Duration::from_secs(120));
    eprintln!("Docker back {:?} after SIGKILL of the VM process", up - t);
    assert_ne!(vm.vm_pid(), old);
    // A trim recovers what online discard had queued when the VM died.
    vm.wait_for("trimmed", Duration::from_secs(60));

    // The data disk survived, and networking came back with a new gvproxy.
    let f = vm.docker(&["run", "--rm", "-v", "kept:/v", IMAGE, "cat", "/v/f"]);
    assert_eq!(f.trim(), "before");
    vm.docker(&["image", "inspect", IMAGE]);
    check_network(&vm, mac_port, "web-after-restart");
    vm.stop();
}

#[test]
#[ignore = "boots the system VM; run with `cargo xtask test-vm`"]
fn guest_clock_is_resynced_on_wake() {
    let mut vm = Vm::start(&[]);
    vm.docker(&[
        "run",
        "--rm",
        "--privileged",
        IMAGE,
        "date",
        "-s",
        "2001-01-01 00:00:00",
    ]);
    let wrong = vm.guest_time();
    assert!(wrong < 1_000_000_000, "guest clock not set back: {wrong}");

    vm.signal(libc::SIGUSR2);
    vm.wait_for("guest clock synced (wake)", Duration::from_secs(10));
    let skew = vm.guest_time().abs_diff(unix_now());
    assert!(skew <= 2, "guest clock {skew} s off after the wake");
    vm.stop();
}

#[test]
#[ignore = "boots the system VM; run with `cargo xtask test-vm`"]
fn paused_sleep_keeps_network_files_and_clock() {
    let root = root();
    let mount = root.join(format!("target/test-life-{}/mnt", std::process::id()));
    let mount_arg = mount.to_str().unwrap().to_string();
    let mut vm = Vm::start(&["--pause-on-sleep", "--mount", &mount_arg]);
    let mac_port = mac_listener();
    vm.docker(&["volume", "create", "shared"]);

    vm.signal(libc::SIGUSR1);
    vm.wait_for("going to sleep", Duration::from_secs(10));
    within(Duration::from_secs(5), "the VM to pause", || {
        std::fs::read_to_string(vm.home.join("run/vm.log"))
            .is_ok_and(|l| l.contains("pause requested"))
    });
    // Paused vCPUs serve nothing.
    assert!(
        !vm.docker_ping(Duration::from_secs(2)),
        "docker answered while paused"
    );
    thread::sleep(Duration::from_secs(5));

    let t = Instant::now();
    vm.signal(libc::SIGUSR2);
    vm.wait_for("woke up", Duration::from_secs(10));
    within(Duration::from_secs(10), "docker after resume", || {
        vm.docker_ping(Duration::from_secs(1))
    });
    eprintln!("Docker answers {:?} after the wake", t.elapsed());
    // The guest's clock stood still while paused; the wake resynced it.
    vm.wait_for("guest clock synced (wake)", Duration::from_secs(10));
    let skew = vm.guest_time().abs_diff(unix_now());
    assert!(skew <= 2, "guest clock {skew} s off after the wake");

    check_network(&vm, mac_port, "web-after-wake");
    vm.docker(&[
        "run",
        "--rm",
        "-v",
        "shared:/v",
        IMAGE,
        "sh",
        "-c",
        "echo hello > /v/a.txt",
    ]);
    let file = mount.join("docker/volumes/shared/a.txt");
    within(
        Duration::from_secs(10),
        "the volume file on the Mac",
        || std::fs::read_to_string(&file).is_ok_and(|s| s == "hello\n"),
    );
    vm.stop();
    assert!(!Path::new(&file).exists(), "mount left behind");
}

/// `katamaran <args>` against `home`.
fn cli(home: &Path, args: &[&str]) -> String {
    let out = Command::new(root().join("target/release/katamaran"))
        .args(args)
        .env("KATAMARAN_HOME", home)
        .stdin(Stdio::null())
        .output()
        .expect("run katamaran");
    assert!(
        out.status.success(),
        "katamaran {args:?}: {}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// A `katamaran daemon` this test spawned: SIGTERM (which stops the VM),
/// then SIGKILL.
struct Daemon {
    child: Child,
    home: PathBuf,
}

impl Daemon {
    fn status(&self) -> serde_json::Value {
        serde_json::from_str(&cli(&self.home, &["status", "--json"])).unwrap()
    }

    fn log(&self) -> String {
        std::fs::read_to_string(self.home.join("logs/daemon.log")).unwrap_or_default()
    }

    fn docker(&self, args: &[&str]) -> String {
        let out = Command::new("docker")
            .env(
                "DOCKER_HOST",
                format!("unix://{}", self.home.join("run/docker.sock").display()),
            )
            .args(args)
            .output()
            .expect("run docker");
        assert!(
            out.status.success(),
            "docker {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).into_owned()
    }

    /// Stop the daemon child and wait for its control socket to go away,
    /// keeping `self.home` intact for offline CLI checks.
    fn shutdown(&mut self) {
        if let Ok(None) = self.child.try_wait() {
            unsafe { libc::kill(self.child.id() as i32, libc::SIGTERM) };
            within(Duration::from_secs(60), "daemon process to exit", || {
                matches!(self.child.try_wait(), Ok(Some(_)))
            });
        }
        within(Duration::from_secs(10), "daemon socket to go away", || {
            UnixStream::connect(self.home.join("run/daemon.sock")).is_err()
        });
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        if let Ok(None) = self.child.try_wait() {
            unsafe { libc::kill(self.child.id() as i32, libc::SIGTERM) };
            for _ in 0..600 {
                if let Ok(Some(_)) = self.child.try_wait() {
                    break;
                }
                thread::sleep(Duration::from_millis(100));
            }
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        if let Some(pid) = std::fs::read_to_string(self.home.join("run/vm.pid"))
            .ok()
            .and_then(|s| s.trim().parse::<i32>().ok())
        {
            unsafe { libc::kill(pid, libc::SIGKILL) };
        }
        let _ = std::fs::remove_dir_all(&self.home);
    }
}

#[test]
#[ignore = "boots the system VM; run with `cargo xtask test-vm`"]
fn the_daemon_restarts_its_vm_and_resyncs_the_clock_on_wake() {
    let home = root().join(format!("target/tlh-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&home);
    std::fs::create_dir_all(&home).unwrap();
    cli(
        &home,
        &["config", "set", "cpus=2", "memory=2G", "mount=off"],
    );
    let child = Command::new(root().join("target/release/katamaran"))
        .arg("daemon")
        .env("KATAMARAN_HOME", &home)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn katamaran daemon");
    let d = Daemon { child, home };
    within(Duration::from_secs(10), "the daemon socket", || {
        UnixStream::connect(d.home.join("run/daemon.sock")).is_ok()
    });
    cli(&d.home, &["start"]);
    let mut save = image_docker()
        .args(["save", IMAGE])
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let load = Command::new("docker")
        .env(
            "DOCKER_HOST",
            format!("unix://{}", d.home.join("run/docker.sock").display()),
        )
        .args(["load", "-q"])
        .stdin(save.stdout.take().unwrap())
        .output()
        .unwrap();
    assert!(save.wait().unwrap().success() && load.status.success());

    // A simulated wake resyncs a guest clock that is far off.
    d.docker(&[
        "run",
        "--rm",
        "--privileged",
        IMAGE,
        "date",
        "-s",
        "2001-01-01 00:00:00",
    ]);
    assert_eq!(unsafe { libc::kill(d.child.id() as i32, libc::SIGUSR2) }, 0);
    within(
        Duration::from_secs(15),
        "the guest clock after the wake",
        || {
            let t: u64 = d
                .docker(&["run", "--rm", IMAGE, "date", "+%s"])
                .trim()
                .parse()
                .unwrap();
            t.abs_diff(unix_now()) <= 2
        },
    );
    assert!(d.log().contains("power: Woke"), "daemon log:\n{}", d.log());

    // A VM process that dies is restarted, and the status follows it.
    let old = d.status()["vm"]["pid"].as_i64().unwrap() as i32;
    let t = Instant::now();
    assert_eq!(unsafe { libc::kill(old, libc::SIGKILL) }, 0);
    within(Duration::from_secs(120), "the restarted VM running", || {
        let s = d.status();
        s["state"] == "running"
            && s["docker"]["state"] == "ready"
            && s["vm"]["pid"].as_i64().is_some_and(|p| p as i32 != old)
    });
    eprintln!(
        "daemon: Docker back {:?} after SIGKILL of the VM",
        t.elapsed()
    );
    let s = d.status();
    assert!(
        s["error"].as_str().is_some_and(|e| e.contains("restarted")),
        "status: {s}"
    );
    d.docker(&["run", "--rm", IMAGE, "true"]);
    cli(&d.home, &["stop"]);
    assert_eq!(d.status()["state"], "stopped");
}

#[test]
#[ignore = "boots the system VM; run with `cargo xtask test-vm`"]
fn data_disk_grows_and_resizes_btrfs_across_restart() {
    let home = root().join(format!("target/tld-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&home);
    std::fs::create_dir_all(&home).unwrap();
    cli(
        &home,
        &["config", "set", "cpus=2", "memory=2G", "mount=off"],
    );
    let child = Command::new(root().join("target/release/katamaran"))
        .arg("daemon")
        .env("KATAMARAN_HOME", &home)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn katamaran daemon");
    let mut d = Daemon { child, home };
    within(Duration::from_secs(10), "the daemon socket", || {
        UnixStream::connect(d.home.join("run/daemon.sock")).is_ok()
    });
    cli(&d.home, &["start"]);

    let saved = image_docker().args(["save", IMAGE]).output().unwrap();
    if saved.status.success() {
        let mut load = Command::new("docker")
            .env(
                "DOCKER_HOST",
                format!("unix://{}", d.home.join("run/docker.sock").display()),
            )
            .args(["load", "-q"])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        load.stdin.take().unwrap().write_all(&saved.stdout).unwrap();
        let out = load.wait_with_output().unwrap();
        assert!(
            out.status.success(),
            "docker load: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    } else {
        d.docker(&["pull", "-q", IMAGE]);
    }

    let fs_kib = || -> u64 {
        let out = d.docker(&[
            "run",
            "--rm",
            IMAGE,
            "sh",
            "-c",
            "df -Pk / | awk 'NR==2 { print $2 }'",
        ]);
        out.trim()
            .parse()
            .unwrap_or_else(|e| panic!("parse df output {out:?}: {e}"))
    };

    let img = d.home.join("data/data.img");
    assert_eq!(std::fs::metadata(&img).unwrap().len(), 64 * GIB);
    let before_kib = fs_kib();
    assert!(
        (60 << 20..=66 << 20).contains(&before_kib),
        "expected ~64 GiB filesystem before resize, got {before_kib} KiB"
    );

    // Write persistent data that must survive the resize.
    d.docker(&["volume", "create", "persist"]);
    d.docker(&[
        "run",
        "--rm",
        "-v",
        "persist:/v",
        IMAGE,
        "sh",
        "-c",
        "echo keep-me > /v/sentinel && sync",
    ]);

    // An already-padded data.img with no configured disk_gib target must NOT
    // have its Btrfs filesystem expanded on boot.
    cli(&d.home, &["stop"]);
    std::fs::OpenOptions::new()
        .write(true)
        .open(&img)
        .unwrap()
        .set_len(100 * GIB)
        .unwrap();
    cli(&d.home, &["start"]);
    assert_eq!(std::fs::metadata(&img).unwrap().len(), 100 * GIB);
    assert_eq!(d.status()["vm"]["disk_gib"], 100);
    let padded_console = std::fs::read_to_string(d.home.join("logs/console.log"))
        .expect("read guest console after booting padded image without disk target");
    assert!(
        !padded_console.contains("resized data disk filesystem"),
        "boot without disk_gib should leave padded image untouched:\n{padded_console}"
    );
    let padded_kib = fs_kib();
    assert!(
        (60 << 20..=66 << 20).contains(&padded_kib),
        "expected ~64 GiB filesystem to remain untouched on padded image, got {padded_kib} KiB"
    );

    let set_out = cli(&d.home, &["config", "set", "disk=500G"]);
    assert!(set_out.contains("restart"), "{set_out}");
    let pending = cli(&d.home, &["status"]);
    assert!(
        pending.contains("pending") && pending.contains("500 GiB disk"),
        "{pending}"
    );

    cli(&d.home, &["restart"]);

    let meta = std::fs::metadata(&img).unwrap();
    assert_eq!(meta.len(), 500 * GIB);
    assert!(
        meta.blocks() * 512 < 512 * MIB,
        "grown data.img should remain sparse: {} MiB allocated",
        meta.blocks() * 512 / MIB
    );
    assert_eq!(d.status()["vm"]["disk_gib"], 500);

    let console = std::fs::read_to_string(d.home.join("logs/console.log"))
        .expect("read guest console after resize");
    assert!(
        console.contains("resized data disk filesystem (64 GiB -> 500 GiB)"),
        "console log:\n{console}"
    );

    let after_kib = fs_kib();
    assert!(
        after_kib >= 490 << 20,
        "expected ~500 GiB filesystem after resize, got {after_kib} KiB"
    );
    let sentinel = d.docker(&[
        "run",
        "--rm",
        "-v",
        "persist:/v",
        IMAGE,
        "cat",
        "/v/sentinel",
    ]);
    assert_eq!(sentinel.trim(), "keep-me");

    // Shrinking below the current 500 GiB data.img is refused.
    let shrink = Command::new(root().join("target/release/katamaran"))
        .args(["config", "set", "disk=200G"])
        .env("KATAMARAN_HOME", &d.home)
        .stdin(Stdio::null())
        .output()
        .expect("run katamaran");
    assert!(!shrink.status.success());
    assert!(
        String::from_utf8_lossy(&shrink.stderr).contains("cannot shrink"),
        "{shrink:?}"
    );

    // A subsequent restart does not re-run the resize.
    cli(&d.home, &["restart"]);
    let console2 = std::fs::read_to_string(d.home.join("logs/console.log"))
        .expect("read guest console after the second restart");
    assert!(
        !console2.contains("resized data disk filesystem"),
        "second boot should skip resize:\n{console2}"
    );
    assert!(fs_kib() >= 490 << 20);

    cli(&d.home, &["stop"]);
    assert_eq!(d.status()["state"], "stopped");

    // Shut down the daemon without deleting home to exercise offline CLI
    // shrink rejection against the 500 GiB data.img.
    d.shutdown();
    let offline_shrink = Command::new(root().join("target/release/katamaran"))
        .args(["config", "set", "disk=200G"])
        .env("KATAMARAN_HOME", &d.home)
        .stdin(Stdio::null())
        .output()
        .expect("run katamaran offline");
    assert!(!offline_shrink.status.success());
    assert!(
        String::from_utf8_lossy(&offline_shrink.stderr).contains("cannot shrink"),
        "{offline_shrink:?}"
    );
    assert_eq!(cli(&d.home, &["config", "get", "disk"]).trim(), "500");
}
