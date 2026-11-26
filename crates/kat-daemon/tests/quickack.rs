//! Guest TCP quick ACK end to end on real Kubernetes (k3s) pods, local Docker
//! bridges (`docker0`), and the `--cmdline kat.quickack=0` opt-out. Needs
//! internet access (to pull k3s and alpine) and the `kubectl` and `docker`
//! CLIs on `PATH`. Ignored by default; `cargo xtask test-vm` builds everything
//! and runs it.

use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

const BASE_IMAGE: &str = "alpine:3.24.4";
const TEST_IMAGE: &str = "kat-quickack-test:1";
const QUICKACK_PARAM: &str = "/sys/module/kat/parameters/quickack";

/// Tiny Python HTTP/1.1 keep-alive server that explicitly leaves Nagle enabled
/// (`TCP_NODELAY = 0`) and sends each response in two separate `send()` calls
/// (headers first, then a 10 KiB body — below the 65468-byte guest MSS).
/// Without quick ACK, the client's delayed-ACK timer stalls every keep-alive
/// request for ~40–50 ms.
const SPLIT_WRITE_SERVER_PY: &str = r#"
import socket, threading

BODY = b"x" * 10240
HDR = (
    b"HTTP/1.1 200 OK\r\n"
    b"Content-Length: " + str(len(BODY)).encode() + b"\r\n"
    b"Connection: keep-alive\r\n"
    b"\r\n"
)

def handle(conn):
    try:
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 0)
        buf = b""
        while True:
            while b"\r\n\r\n" not in buf:
                chunk = conn.recv(4096)
                if not chunk:
                    return
                buf += chunk
            _, buf = buf.split(b"\r\n\r\n", 1)
            conn.sendall(HDR)
            conn.sendall(BODY)
    finally:
        conn.close()

srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("0.0.0.0", 8080))
srv.listen(32)
print("READY", flush=True)
while True:
    c, _ = srv.accept()
    threading.Thread(target=handle, args=(c,), daemon=True).start()
"#;

/// Keep-alive client that issues `count` consecutive requests on a single TCP
/// connection to `host:8080`, verifies each 10 KiB body, and prints
/// `<p50_ms> <total_ms>` to stdout.
const KEEP_ALIVE_CLIENT_PY: &str = r#"
import socket, sys, time

host = sys.argv[1]
count = int(sys.argv[2])
req = b"GET / HTTP/1.1\r\nHost: test\r\nConnection: keep-alive\r\n\r\n"

def recv_exact(sock, n, buf):
    while len(buf) < n:
        chunk = sock.recv(65536)
        if not chunk:
            raise RuntimeError("unexpected EOF")
        buf += chunk
    return buf[:n], buf[n:]

sock = socket.create_connection((host, 8080), timeout=10)
sock.settimeout(10)
buf = b""
times_ms = []
t_all = time.monotonic()
for _ in range(count):
    t0 = time.monotonic()
    sock.sendall(req)
    while b"\r\n\r\n" not in buf:
        chunk = sock.recv(65536)
        if not chunk:
            raise RuntimeError("unexpected EOF in headers")
        buf += chunk
    hdr, buf = buf.split(b"\r\n\r\n", 1)
    assert b"200 OK" in hdr, hdr
    body, buf = recv_exact(sock, 10240, buf)
    assert len(body) == 10240
    times_ms.append((time.monotonic() - t0) * 1000.0)
total_ms = (time.monotonic() - t_all) * 1000.0
sock.close()
times_ms.sort()
p50 = times_ms[len(times_ms) // 2]
print(f"{p50:.3f} {total_ms:.3f}")
"#;

fn root() -> PathBuf {
    // Canonical, so Unix socket paths stay under macOS's 104-byte limit.
    std::fs::canonicalize(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")).unwrap()
}

fn katamaran() -> PathBuf {
    root().join("target/release/katamaran")
}

fn cli(home: &Path, user_home: &Path, args: &[&str]) -> Output {
    Command::new(katamaran())
        .args(args)
        .env("KATAMARAN_HOME", home)
        .env("HOME", user_home)
        .stdin(Stdio::null())
        .output()
        .expect("run katamaran")
}

fn ok(out: &Output, what: &str) -> String {
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(
        out.status.success(),
        "{what} failed ({}):\nstdout:\n{stdout}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    stdout
}

fn wait_until(what: &str, timeout: Duration, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(200));
    }
}

struct Daemon {
    child: Child,
    home: PathBuf,
}

impl Daemon {
    fn spawn(home: &Path, user_home: &Path) -> Daemon {
        std::fs::create_dir_all(home).unwrap();
        std::fs::create_dir_all(user_home).unwrap();
        let child = Command::new(katamaran())
            .arg("daemon")
            .env("KATAMARAN_HOME", home)
            .env("HOME", user_home)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn katamaran daemon");
        let d = Daemon {
            child,
            home: home.to_path_buf(),
        };
        let sock = home.join("run/daemon.sock");
        wait_until("the daemon socket", Duration::from_secs(10), || {
            UnixStream::connect(&sock).is_ok()
        });
        d
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
        let _ = std::fs::remove_dir_all(&self.home);
    }
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct ContainerGuard {
    docker_host: String,
    id: String,
}

impl Drop for ContainerGuard {
    fn drop(&mut self) {
        let _ = docker(&self.docker_host, &["rm", "-f", &self.id]);
    }
}

struct QuickackRestoreGuard {
    docker_host: String,
    restored: bool,
}

impl QuickackRestoreGuard {
    fn restore(mut self) {
        ok(
            &docker(
                &self.docker_host,
                &[
                    "run",
                    "--rm",
                    "--privileged",
                    TEST_IMAGE,
                    "sh",
                    "-c",
                    &format!("echo 1 > {QUICKACK_PARAM}"),
                ],
            ),
            "re-enable kat.quickack",
        );
        self.restored = true;
    }
}

impl Drop for QuickackRestoreGuard {
    fn drop(&mut self) {
        if !self.restored {
            let _ = docker(
                &self.docker_host,
                &[
                    "run",
                    "--rm",
                    "--privileged",
                    TEST_IMAGE,
                    "sh",
                    "-c",
                    &format!("echo 1 > {QUICKACK_PARAM}"),
                ],
            );
        }
    }
}

fn kubectl(kubeconfig: &Path, args: &[&str]) -> Output {
    Command::new("kubectl")
        .arg("--kubeconfig")
        .arg(kubeconfig)
        .args(args)
        .stdin(Stdio::null())
        .output()
        .expect("run kubectl")
}

fn docker(docker_host: &str, args: &[&str]) -> Output {
    Command::new("docker")
        .env("DOCKER_HOST", docker_host)
        .env_remove("DOCKER_CONTEXT")
        .args(args)
        .stdin(Stdio::null())
        .output()
        .expect("run docker")
}

fn build_test_image(docker_host: &str) {
    let mut build = Command::new("docker")
        .env("DOCKER_HOST", docker_host)
        .env_remove("DOCKER_CONTEXT")
        .args(["build", "-q", "-t", TEST_IMAGE, "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn docker build");
    write!(
        build.stdin.take().unwrap(),
        "FROM {BASE_IMAGE}\nRUN apk add --no-cache python3 iproute2\n"
    )
    .unwrap();
    let out = build.wait_with_output().expect("wait docker build");
    ok(&out, "build test image");
}

fn parse_latencies(out: &str, what: &str) -> (f64, f64) {
    let line = out
        .lines()
        .rev()
        .find(|l| !l.trim().is_empty())
        .unwrap_or_else(|| panic!("{what}: empty output"));
    let mut parts = line.split_whitespace();
    let p50: f64 = parts
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("{what}: bad p50 in {line:?}"));
    let total: f64 = parts
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("{what}: bad total in {line:?}"));
    (p50, total)
}

/// Reuse a locally cached k3s binary from the user's real `~/.katamaran/cache/k3s`
/// if present so repeated test runs avoid re-downloading ~60 MiB.
fn seed_k3s_cache(home: &Path) {
    let Some(real_home) = std::env::var_os("HOME") else {
        return;
    };
    let src_root = PathBuf::from(real_home).join(".katamaran/cache/k3s");
    let Ok(entries) = std::fs::read_dir(&src_root) else {
        return;
    };
    let dst_root = home.join("cache/k3s");
    for entry in entries.flatten() {
        let src_bin = entry.path().join("k3s-arm64");
        if src_bin.is_file() {
            let dst_dir = dst_root.join(entry.file_name());
            if std::fs::create_dir_all(&dst_dir).is_ok() {
                let _ = std::fs::copy(&src_bin, dst_dir.join("k3s-arm64"));
            }
        }
    }
}

#[test]
#[ignore = "boots the system VM with k3s; run with `cargo xtask test-vm`"]
fn pod_and_bridge_split_write_keepalive_has_no_delack_stall() {
    let home = root().join(format!("target/tqa-{}", std::process::id()));
    let user_home = home.join("user-home");
    let _ = std::fs::remove_dir_all(&home);
    std::fs::create_dir_all(&user_home).unwrap();
    seed_k3s_cache(&home);

    ok(
        &cli(
            &home,
            &user_home,
            &["config", "set", "cpus=2", "memory=4G", "mount=off"],
        ),
        "config set",
    );

    let _daemon = Daemon::spawn(&home, &user_home);
    ok(&cli(&home, &user_home, &["start"]), "kat start");
    ok(&cli(&home, &user_home, &["k8s", "start"]), "kat k8s start");

    let kubeconfig = home.join("run/kubeconfig.yaml");
    assert!(
        kubeconfig.is_file(),
        "missing kubeconfig at {}",
        kubeconfig.display()
    );
    let docker_host = format!("unix://{}", home.join("run/docker.sock").display());

    // Build a single alpine image containing python3 + iproute2 and sync it
    // into k3s so pods do not depend on in-cluster DNS or repeated apk fetches.
    build_test_image(&docker_host);
    ok(
        &cli(&home, &user_home, &["k8s", "sync-images"]),
        "kat k8s sync-images",
    );

    // Verify the guest kernel exposes kat.quickack enabled by default.
    let param_val = ok(
        &docker(
            &docker_host,
            &["run", "--rm", TEST_IMAGE, "cat", QUICKACK_PARAM],
        ),
        "read kat.quickack kernel parameter",
    );
    assert_eq!(
        param_val.trim(),
        "Y",
        "expected {QUICKACK_PARAM} to default to Y"
    );

    // Launch two ordinary non-host-network pods.
    for name in ["qa-peer", "qa-probe"] {
        ok(
            &kubectl(
                &kubeconfig,
                &[
                    "run",
                    name,
                    "--restart=Never",
                    &format!("--image={TEST_IMAGE}"),
                    "--image-pull-policy=Never",
                    "--",
                    "sleep",
                    "300",
                ],
            ),
            &format!("create pod {name}"),
        );
    }
    for name in ["qa-peer", "qa-probe"] {
        ok(
            &kubectl(
                &kubeconfig,
                &[
                    "wait",
                    "--for=condition=Ready",
                    "--timeout=120s",
                    &format!("pod/{name}"),
                ],
            ),
            &format!("wait for pod {name}"),
        );
    }

    let peer_ip = ok(
        &kubectl(
            &kubeconfig,
            &["get", "pod", "qa-peer", "-o", "jsonpath={.status.podIP}"],
        ),
        "get qa-peer IP",
    );
    let peer_ip = peer_ip.trim().to_string();
    assert!(!peer_ip.is_empty(), "qa-peer has empty podIP");

    // Start the split-write keep-alive server in qa-peer.
    let _peer_srv = ChildGuard(
        Command::new("kubectl")
            .arg("--kubeconfig")
            .arg(&kubeconfig)
            .args([
                "exec",
                "qa-peer",
                "--",
                "python3",
                "-c",
                SPLIT_WRITE_SERVER_PY,
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start split-write server in qa-peer"),
    );

    wait_until("qa-peer server ready", Duration::from_secs(15), || {
        kubectl(
            &kubeconfig,
            &[
                "exec",
                "qa-probe",
                "--",
                "python3",
                "-c",
                KEEP_ALIVE_CLIENT_PY,
                &peer_ip,
                "1",
            ],
        )
        .status
        .success()
    });

    // 1) Pod-to-pod split-write keep-alive (25 requests on one connection).
    //    With quickack active, each request completes in < 1 ms; without it,
    //    delayed ACK stalls each request for ~40–50 ms (~1000–1250 ms total).
    let pod_out = ok(
        &kubectl(
            &kubeconfig,
            &[
                "exec",
                "qa-probe",
                "--",
                "python3",
                "-c",
                KEEP_ALIVE_CLIENT_PY,
                &peer_ip,
                "25",
            ],
        ),
        "pod-to-pod split-write keep-alive",
    );
    let (pod_p50, pod_total) = parse_latencies(&pod_out, "pod-to-pod keep-alive");
    eprintln!("pod-to-pod keep-alive (quickack=Y): p50={pod_p50:.2}ms total={pod_total:.2}ms");
    assert!(
        pod_p50 < 15.0 && pod_total < 400.0,
        "pod-to-pod keep-alive stalled (p50={pod_p50:.2}ms total={pod_total:.2}ms)"
    );

    // 2) Verify toggling kat.quickack=0 reproduces the 40–50 ms stall on the
    //    same fixture (confirming the test is non-vacuous) and restoring 1
    //    eliminates it again.
    {
        let restore = QuickackRestoreGuard {
            docker_host: docker_host.clone(),
            restored: false,
        };
        ok(
            &docker(
                &docker_host,
                &[
                    "run",
                    "--rm",
                    "--privileged",
                    TEST_IMAGE,
                    "sh",
                    "-c",
                    &format!("echo 0 > {QUICKACK_PARAM}"),
                ],
            ),
            "disable kat.quickack",
        );
        let off_out = ok(
            &kubectl(
                &kubeconfig,
                &[
                    "exec",
                    "qa-probe",
                    "--",
                    "python3",
                    "-c",
                    KEEP_ALIVE_CLIENT_PY,
                    &peer_ip,
                    "8",
                ],
            ),
            "pod-to-pod keep-alive with quickack=0",
        );
        restore.restore();
        let (off_p50, off_total) = parse_latencies(&off_out, "quickack=0 keep-alive");
        eprintln!("pod-to-pod keep-alive (quickack=N): p50={off_p50:.2}ms total={off_total:.2}ms");
        assert!(
            off_p50 >= 25.0,
            "expected delayed-ACK stall with quickack=0 (got p50={off_p50:.2}ms total={off_total:.2}ms)"
        );
    }

    // 3) Root namespace (`--network host`) to bridged Docker container over
    //    `docker0`: verifies root bridge dsts get quickack without mutating
    //    live routing tables or touching the `eth0` uplink.
    let srv_id = ok(
        &docker(
            &docker_host,
            &[
                "run",
                "-d",
                "--rm",
                TEST_IMAGE,
                "python3",
                "-c",
                SPLIT_WRITE_SERVER_PY,
            ],
        ),
        "start bridged docker server",
    )
    .trim()
    .to_string();
    let _srv_guard = ContainerGuard {
        docker_host: docker_host.clone(),
        id: srv_id.clone(),
    };

    let srv_ip = ok(
        &docker(
            &docker_host,
            &[
                "inspect",
                "-f",
                "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                &srv_id,
            ],
        ),
        "inspect bridged docker server IP",
    )
    .trim()
    .to_string();
    assert!(!srv_ip.is_empty(), "empty bridged container IP");

    wait_until(
        "bridged docker server ready",
        Duration::from_secs(15),
        || {
            docker(
                &docker_host,
                &[
                    "run",
                    "--rm",
                    "--network",
                    "host",
                    TEST_IMAGE,
                    "python3",
                    "-c",
                    KEEP_ALIVE_CLIENT_PY,
                    &srv_ip,
                    "1",
                ],
            )
            .status
            .success()
        },
    );

    let bridge_out = ok(
        &docker(
            &docker_host,
            &[
                "run",
                "--rm",
                "--network",
                "host",
                TEST_IMAGE,
                "python3",
                "-c",
                KEEP_ALIVE_CLIENT_PY,
                &srv_ip,
                "25",
            ],
        ),
        "root-to-bridge split-write keep-alive",
    );
    let (br_p50, br_total) = parse_latencies(&bridge_out, "root-to-bridge keep-alive");
    eprintln!("root-to-bridge keep-alive (quickack=Y): p50={br_p50:.2}ms total={br_total:.2}ms");
    assert!(
        br_p50 < 15.0 && br_total < 400.0,
        "root-to-bridge keep-alive stalled (p50={br_p50:.2}ms total={br_total:.2}ms)"
    );

    // 4) Confirm the guest root routing table is completely unmutated (no
    //    `quickack 1` rewritten onto `eth0` or bridge routes).
    let root_routes = ok(
        &docker(
            &docker_host,
            &[
                "run",
                "--rm",
                "--network",
                "host",
                TEST_IMAGE,
                "sh",
                "-c",
                "ip -4 route show && ip -6 route show",
            ],
        ),
        "inspect root namespace routes",
    );
    assert!(
        root_routes.lines().any(|l| l.contains("dev eth0")),
        "expected eth0 route in root table:\n{root_routes}"
    );
    for line in root_routes.lines() {
        assert!(
            !line.contains("quickack 1"),
            "routing table must not be rewritten by a userspace watcher: {line}\n{root_routes}"
        );
    }

    ok(&cli(&home, &user_home, &["stop"]), "kat stop");
}

#[test]
#[ignore = "boots a foreground VM with --cmdline kat.quickack=0; run with `cargo xtask test-vm`"]
fn cmdline_opt_out_disables_quickack() {
    let root = root();
    let home = root.join(format!("target/tqa-off-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&home);
    std::fs::create_dir_all(&home).unwrap();

    struct Cleanup(Child, PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            if let Ok(None) = self.0.try_wait() {
                unsafe { libc::kill(self.0.id() as i32, libc::SIGTERM) };
                for _ in 0..200 {
                    if let Ok(Some(_)) = self.0.try_wait() {
                        break;
                    }
                    thread::sleep(Duration::from_millis(50));
                }
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
            let _ = std::fs::remove_dir_all(&self.1);
        }
    }

    let mut child = Command::new(katamaran())
        .current_dir(&root)
        .env("KATAMARAN_HOME", &home)
        .args([
            "run",
            "--no-mount",
            "--no-share",
            "--cpus",
            "2",
            "--memory-mib",
            "1024",
            "--cmdline",
            "kat.quickack=0",
        ])
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
    let _vm = Cleanup(child, home.clone());

    let deadline = Instant::now() + Duration::from_secs(60);
    let mut output = Vec::new();
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        match lines.recv_timeout(left) {
            Ok(line) => {
                let ready = line.contains("docker ready");
                output.push(line);
                if ready {
                    break;
                }
            }
            Err(_) => panic!(
                "timed out waiting for foreground VM docker ready:\n{}",
                output.join("\n")
            ),
        }
    }

    let docker_host = format!("unix://{}", home.join("run/docker.sock").display());
    let val = ok(
        &docker(
            &docker_host,
            &["run", "--rm", BASE_IMAGE, "cat", QUICKACK_PARAM],
        ),
        "read kat.quickack under --cmdline kat.quickack=0",
    );
    assert_eq!(
        val.trim(),
        "N",
        "expected {QUICKACK_PARAM} to be N with --cmdline kat.quickack=0"
    );
}
