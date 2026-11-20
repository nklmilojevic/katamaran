//! The guest's files on the Mac: boots the system VM with its NFS export
//! mounted, and checks files move both ways between a Docker volume and the
//! mount, that only the kernel reaches the socket, and that no mount is left
//! behind on shutdown or after a crash. Ignored by default; `cargo xtask
//! test-vm` builds everything and runs it.
//!
//! The VM has no network yet, so the alpine image comes from the host's
//! Docker (`docker save`; KAT_TEST_IMAGE_CONTEXT names the context, default the
//! current one), read-only.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

const IMAGE: &str = "alpine:3.24.4";

/// `docker` on the engine that supplies the test images: the context named by
/// KAT_TEST_IMAGE_CONTEXT, or else the current Docker context.
fn image_docker() -> Command {
    let mut docker = Command::new("docker");
    if let Ok(ctx) = std::env::var("KAT_TEST_IMAGE_CONTEXT") {
        docker.args(["--context", &ctx]);
    }
    docker
}

/// Resolved, so state paths stay short enough for Unix sockets.
fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

/// `$KATAMARAN_TEST_MOUNT`, or `~/Katamaran-nfs-dev`; never `~/Katamaran`.
fn mount_point() -> PathBuf {
    std::env::var_os("KATAMARAN_TEST_MOUNT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(std::env::var("HOME").unwrap()).join("Katamaran-nfs-dev"))
}

/// The `mount` line for `dir`, if anything is mounted there.
fn mount_line(dir: &Path) -> Option<String> {
    let out = Command::new("/sbin/mount").output().unwrap();
    let on = format!(" on {} (", dir.display());
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .find(|l| l.contains(&on))
        .map(str::to_string)
}

struct Vm {
    child: Child,
    lines: mpsc::Receiver<String>,
    output: Vec<String>,
    home: PathBuf,
    mount: PathBuf,
    /// Keep the state directory (for a restart in the same test).
    keep_home: bool,
}

impl Vm {
    fn start(home: &Path) -> Vm {
        let root = root();
        std::fs::create_dir_all(home).unwrap();
        let mount = mount_point();
        let mut child = Command::new(root.join("target/release/katamaran"))
            .current_dir(&root)
            .env("KATAMARAN_HOME", home)
            .args(["run", "--cpus", "2", "--memory-mib", "2048", "--mount"])
            .arg(&mount)
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
        Vm {
            child,
            lines,
            output: Vec::new(),
            home: home.to_path_buf(),
            mount,
            keep_home: false,
        }
    }

    fn wait_for(&mut self, needle: &str, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            match self.lines.recv_timeout(left) {
                Ok(line) => {
                    let hit = line.contains(needle);
                    self.output.push(line);
                    if hit {
                        return;
                    }
                }
                Err(_) => panic!(
                    "no `{needle}` within {timeout:?}; output:\n{}",
                    self.output.join("\n")
                ),
            }
        }
    }

    fn saw(&self, needle: &str) -> bool {
        self.output.iter().any(|l| l.contains(needle))
    }

    fn nfs_port(&self) -> u16 {
        let f = self.home.join("run/nfs/nfs.port");
        std::fs::read_to_string(&f)
            .unwrap_or_else(|e| panic!("{}: {e}", f.display()))
            .trim()
            .parse()
            .unwrap()
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

    /// Copy the test image from the host's Docker into the guest.
    fn load_image(&self) {
        let mut save = image_docker()
            .args(["save", IMAGE])
            .stdout(Stdio::piped())
            .spawn()
            .expect("docker save");
        let load = Command::new("docker")
            .env(
                "DOCKER_HOST",
                format!("unix://{}", self.home.join("run/docker.sock").display()),
            )
            .arg("load")
            .stdin(save.stdout.take().unwrap())
            .output()
            .expect("docker load");
        assert!(
            save.wait().unwrap().success(),
            "the host's Docker has no {IMAGE}"
        );
        assert!(
            load.status.success(),
            "docker load: {}",
            String::from_utf8_lossy(&load.stderr)
        );
    }

    /// SIGINT and wait for a clean exit.
    fn stop(mut self) -> Vec<String> {
        unsafe { libc::kill(self.child.id() as i32, libc::SIGINT) };
        self.wait_for("VM stopped", Duration::from_secs(60));
        let status = self.child.wait().unwrap();
        assert!(status.success(), "katamaran run exited with {status}");
        std::mem::take(&mut self.output)
    }

    /// SIGKILL the daemon and its VM process, as a crash would.
    fn crash(mut self) {
        let pid = self.child.id();
        let out = Command::new("/usr/bin/pgrep")
            .arg("-P")
            .arg(pid.to_string())
            .output()
            .unwrap();
        for vm in String::from_utf8_lossy(&out.stdout).split_whitespace() {
            unsafe { libc::kill(vm.parse().unwrap(), libc::SIGKILL) };
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.keep_home = true;
    }
}

/// A failing assertion must not leave a VM running or a mount behind.
impl Drop for Vm {
    fn drop(&mut self) {
        if let Ok(None) = self.child.try_wait() {
            unsafe { libc::kill(self.child.id() as i32, libc::SIGINT) };
            for _ in 0..300 {
                if let Ok(Some(_)) = self.child.try_wait() {
                    break;
                }
                thread::sleep(Duration::from_millis(100));
            }
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        if self.keep_home {
            return;
        }
        if mount_line(&self.mount).is_some_and(|l| l.starts_with("Katamaran:/ ")) {
            let _ = Command::new("/sbin/umount")
                .arg("-f")
                .arg(&self.mount)
                .status();
        }
        let _ = std::fs::remove_dir_all(&self.home);
    }
}

/// Poll `f` until it holds; return how long that took.
fn within(limit: Duration, what: &str, f: impl Fn() -> bool) -> Duration {
    let t = Instant::now();
    while !f() {
        assert!(t.elapsed() < limit, "{what}: not within {limit:?}");
        thread::sleep(Duration::from_millis(10));
    }
    t.elapsed()
}

/// The root holds empty, read-only marker files that tell Spotlight not to
/// index the mount, and Spotlight takes the hint.
fn spotlight_stays_out(mount: &Path) {
    const MARKERS: [&str; 3] = [
        ".metadata-never-index",
        ".metadata_never_index",
        ".metadata_never_index_unless_rootfs",
    ];
    let out = Command::new("/bin/ls")
        .arg("-a")
        .arg(mount)
        .output()
        .unwrap();
    assert!(out.status.success());
    let mut names: Vec<String> = String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(str::to_string)
        .collect();
    names.sort();
    let mut want = vec![".", "..", "docker", "machines"];
    want.extend(MARKERS);
    want.sort();
    assert_eq!(names, want);
    for m in MARKERS {
        let p = mount.join(m);
        let meta = std::fs::metadata(&p).unwrap();
        assert!(meta.is_file(), "{m}");
        assert_eq!(meta.len(), 0, "{m}");
        assert_eq!(std::fs::read(&p).unwrap(), b"", "{m}");
        assert!(std::fs::write(&p, b"x").is_err(), "{m} is writable");
        assert!(std::fs::remove_file(&p).is_err(), "{m} was removed");
        assert!(p.is_file(), "{m}");
    }
    let t = Instant::now();
    loop {
        let out = Command::new("/usr/bin/mdutil")
            .arg("-s")
            .arg(mount)
            .output()
            .unwrap();
        let text = String::from_utf8_lossy(&out.stdout) + String::from_utf8_lossy(&out.stderr);
        let text = text.trim();
        eprintln!("mdutil -s after {:?}: {text}", t.elapsed());
        if text.contains("Indexing and searching disabled.") {
            break;
        }
        assert!(
            t.elapsed() < Duration::from_secs(20),
            "Spotlight still indexes the mount: {text}"
        );
        thread::sleep(Duration::from_millis(500));
    }
}

fn home(tag: &str) -> PathBuf {
    root().join(format!("target/test-home-{}-{tag}", std::process::id()))
}

#[test]
#[ignore = "boots the system VM and mounts NFS; run with `cargo xtask test-vm`"]
fn guest_files_appear_on_the_mac_and_changes_go_both_ways() {
    let mut vm = Vm::start(&home("nfs"));
    vm.wait_for("files mounted at", Duration::from_secs(120));
    let line = mount_line(&vm.mount).expect("mounted");
    assert!(line.contains("(nfs"), "{line}");
    assert!(line.starts_with("Katamaran:/ "), "{line}");
    assert!(line.contains("noatime"), "{line}");
    spotlight_stays_out(&vm.mount);
    let vols = vm.mount.join("docker/volumes");
    vm.load_image();

    // A volume filled by a container appears on the Mac within a second.
    vm.docker(&["volume", "create", "shared"]);
    vm.docker(&[
        "run",
        "--rm",
        "-v",
        "shared:/d",
        IMAGE,
        "sh",
        "-c",
        "echo hello > /d/a.txt && mkdir /d/sub && echo deep > /d/sub/b.txt",
    ]);
    let a = vols.join("shared/a.txt");
    let took = within(Duration::from_secs(1), "a.txt on the Mac", || {
        std::fs::read_to_string(&a).is_ok_and(|s| s == "hello\n")
    });
    eprintln!("volume file visible on the Mac {took:?} after the container exited");
    assert_eq!(
        std::fs::read_to_string(vols.join("shared/sub/b.txt")).unwrap(),
        "deep\n"
    );

    // Written on the Mac, read in a container.
    std::fs::write(vols.join("shared/mac.txt"), "from the mac\n").unwrap();
    std::fs::create_dir(vols.join("shared/macdir")).unwrap();
    let out = vm.docker(&["run", "--rm", "-v", "shared:/d", IMAGE, "cat", "/d/mac.txt"]);
    assert_eq!(out, "from the mac\n");

    // Deleted in a container, gone on the Mac.
    vm.docker(&[
        "run",
        "--rm",
        "-v",
        "shared:/d",
        IMAGE,
        "rm",
        "-r",
        "/d/a.txt",
        "/d/sub",
    ]);
    within(Duration::from_secs(2), "a.txt gone on the Mac", || {
        !a.exists()
    });
    assert!(!vols.join("shared/sub").exists());

    // Deleted on the Mac, gone in the container.
    std::fs::remove_file(vols.join("shared/mac.txt")).unwrap();
    std::fs::remove_dir(vols.join("shared/macdir")).unwrap();
    let out = vm.docker(&["run", "--rm", "-v", "shared:/d", IMAGE, "ls", "-A", "/d"]);
    assert_eq!(out, "");

    // A running container's root filesystem.
    vm.docker(&["run", "-d", "--name", "web", IMAGE, "sleep", "1000"]);
    let release = vm.mount.join("docker/containers/web/etc/alpine-release");
    within(
        Duration::from_secs(2),
        "container rootfs on the Mac",
        || release.is_file(),
    );
    std::fs::write(vm.mount.join("docker/containers/web/tmp/x"), "x").unwrap();
    assert_eq!(vm.docker(&["exec", "web", "cat", "/tmp/x"]), "x");
    assert!(vm.mount.join("machines").is_dir());

    // Only the kernel's NFS client gets through: this process is refused.
    let mut s = TcpStream::connect(("127.0.0.1", vm.nfs_port())).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let null_call = [
        0x80, 0, 0, 40, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 1, 0x86, 0xa3, 0, 0, 0, 3, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ];
    let _ = s.write_all(&null_call);
    let mut buf = [0u8; 64];
    assert_eq!(
        s.read(&mut buf).unwrap_or(0),
        0,
        "a user process got an NFS reply"
    );

    let mount = vm.mount.clone();
    let output = vm.stop();
    assert!(
        output.iter().any(|l| l.contains("unmounted")),
        "{}",
        output.join("\n")
    );
    assert_eq!(mount_line(&mount), None, "a mount was left behind");
}

#[test]
#[ignore = "boots the system VM and mounts NFS; run with `cargo xtask test-vm`"]
fn a_crash_leaves_a_mount_the_next_start_removes() {
    let home = home("crash");
    let mut vm = Vm::start(&home);
    vm.wait_for("files mounted at", Duration::from_secs(120));
    let mount = vm.mount.clone();
    vm.crash();
    assert!(
        mount_line(&mount).is_some(),
        "the crashed daemon's mount is still there"
    );

    // The dead mount fails calls instead of hanging them.
    let t = Instant::now();
    let _ = std::fs::read_dir(mount.join("docker/containers"));
    assert!(
        t.elapsed() < Duration::from_secs(15),
        "a call hung for {:?}",
        t.elapsed()
    );

    let mut vm = Vm::start(&home);
    vm.wait_for("files mounted at", Duration::from_secs(120));
    assert!(vm.saw("removed a stale mount"), "{}", vm.output.join("\n"));
    assert!(mount.join("docker/volumes").is_dir());
    vm.stop();
    assert_eq!(mount_line(&mount), None, "a mount was left behind");
}

/// `shasum -a 256` of what `fill` writes to its stdin.
fn sha256_of(fill: impl FnOnce(&mut dyn Write)) -> String {
    let mut sum = Command::new("/usr/bin/shasum")
        .args(["-a", "256"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn shasum");
    fill(sum.stdin.as_mut().unwrap());
    drop(sum.stdin.take());
    let out = sum.wait_with_output().unwrap();
    assert!(out.status.success());
    String::from_utf8_lossy(&out.stdout)
        .split_whitespace()
        .next()
        .unwrap()
        .to_string()
}

/// Deterministic, incompressible test data: xorshift64* in 1 MiB chunks.
fn chunks(n: usize) -> impl Iterator<Item = Vec<u8>> {
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    (0..n).map(move |_| {
        let mut c = Vec::with_capacity(1 << 20);
        for _ in 0..(1 << 17) {
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            c.extend_from_slice(&x.wrapping_mul(0x2545_f491_4f6c_dd1d).to_le_bytes());
        }
        c
    })
}

#[test]
#[ignore = "boots the system VM and mounts NFS; run with `cargo xtask test-vm`"]
fn a_gigabyte_round_trips_intact() {
    const MIB: usize = 1024;
    let mut vm = Vm::start(&home("gib"));
    vm.wait_for("files mounted at", Duration::from_secs(120));
    vm.load_image();
    vm.docker(&["volume", "create", "big"]);
    let vol = vm.mount.join("docker/volumes/big");
    within(Duration::from_secs(5), "the volume on the Mac", || {
        vol.is_dir()
    });
    let in_guest = |vm: &Vm, file: &str| -> String {
        let out = vm.docker(&["run", "--rm", "-v", "big:/d", IMAGE, "sha256sum", file]);
        out.split_whitespace().next().unwrap().to_string()
    };

    // Mac to guest: written through the mount, checked in a container.
    let up = vol.join("up.bin");
    let t = Instant::now();
    let mut file = std::fs::File::create(&up).unwrap();
    let sent = sha256_of(|sum| {
        for c in chunks(MIB) {
            file.write_all(&c).unwrap();
            sum.write_all(&c).unwrap();
        }
    });
    file.sync_all().unwrap();
    drop(file);
    eprintln!("1 GiB written to the guest in {:?}", t.elapsed());
    assert_eq!(std::fs::metadata(&up).unwrap().len(), (MIB << 20) as u64);
    assert_eq!(
        in_guest(&vm, "/d/up.bin"),
        sent,
        "the guest got other bytes"
    );

    // Guest to Mac: written in a container, read through the mount.
    let made = vm.docker(&[
        "run",
        "--rm",
        "-v",
        "big:/d",
        IMAGE,
        "sh",
        "-c",
        "head -c 1073741824 /dev/urandom > /d/down.bin && sha256sum /d/down.bin",
    ]);
    let made = made.split_whitespace().next().unwrap().to_string();
    let down = vol.join("down.bin");
    within(Duration::from_secs(5), "down.bin at full size", || {
        std::fs::metadata(&down).is_ok_and(|m| m.len() == (MIB << 20) as u64)
    });
    let t = Instant::now();
    let mut file = std::fs::File::open(&down).unwrap();
    let got = sha256_of(|sum| {
        std::io::copy(&mut file, sum).unwrap();
    });
    eprintln!("1 GiB read from the guest in {:?}", t.elapsed());
    assert_eq!(got, made, "the Mac read other bytes");

    let mount = vm.mount.clone();
    vm.stop();
    assert_eq!(mount_line(&mount), None, "a mount was left behind");
}
