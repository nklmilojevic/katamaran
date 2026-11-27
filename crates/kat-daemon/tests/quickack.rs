//! Guest TCP quick ACK end to end on real Kubernetes (k3s) pods, local Docker
//! bridges (`docker0`), and the `--cmdline kat.quickack=0` opt-out. Needs
//! internet access (to pull k3s and alpine) and the `kubectl` and `docker`
//! CLIs on `PATH`. Ignored by default; `cargo xtask test-vm` builds everything
//! and runs it.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
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

/// Keep-alive client that opens ONE persistent TCP connection to `host:8080`,
/// reads burst counts from stdin (one integer per line), runs that many
/// consecutive split-write requests on the SAME established socket, and prints
/// `<p50_ms> <total_ms>` flushed after each burst. Passing an optional count
/// on argv runs a single burst and exits.
const KEEP_ALIVE_CLIENT_PY: &str = r#"
import socket, sys, time

host = sys.argv[1]
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

def run_burst(count):
    global buf
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
    times_ms.sort()
    p50 = times_ms[len(times_ms) // 2]
    print(f"{p50:.3f} {total_ms:.3f}", flush=True)

if len(sys.argv) > 2:
    run_burst(int(sys.argv[2]))
else:
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        run_burst(int(line))
sock.close()
"#;

/// Probes whether Linux's delayed-ACK timer fires (`TcpExt: DelayedACKs` in
/// `/proc/net/netstat`) on a keep-alive TCP connection to `host:port`.
/// Warms up with 25 quick exchanges (draining initial quickacks and entering
/// pingpong mode), then issues 6 exchanges pausing 100 ms after reading each
/// response so the 40–50 ms delayed-ACK timer fires unless quick ACK is active.
/// Prints `<delta>` (change in `DelayedACKs`) to stdout.
const DELACK_PROBE_PY: &str = r#"
import socket, sys, time

host = sys.argv[1]
port = int(sys.argv[2])
req = b"GET / HTTP/1.1\r\nHost: test\r\nConnection: keep-alive\r\n\r\n"

def delayed_acks():
    with open("/proc/net/netstat") as f:
        lines = f.read().splitlines()
    for i in range(0, len(lines) - 1, 2):
        keys = lines[i].split()
        vals = lines[i + 1].split()
        if keys and keys[0] == "TcpExt:" and "DelayedACKs" in keys:
            return int(vals[keys.index("DelayedACKs")])
    raise RuntimeError("DelayedACKs not found in /proc/net/netstat")

def recv_resp(sock, buf):
    while b"\r\n\r\n" not in buf:
        chunk = sock.recv(65536)
        if not chunk:
            raise RuntimeError("unexpected EOF in headers")
        buf += chunk
    hdr, buf = buf.split(b"\r\n\r\n", 1)
    cl = 0
    for line in hdr.split(b"\r\n")[1:]:
        if line.lower().startswith(b"content-length:"):
            cl = int(line.split(b":", 1)[1].strip())
    while len(buf) < cl:
        chunk = sock.recv(65536)
        if not chunk:
            raise RuntimeError("unexpected EOF in body")
        buf += chunk
    return buf[cl:]

sock = socket.create_connection((host, port), timeout=10)
sock.settimeout(10)
buf = b""
for _ in range(25):
    sock.sendall(req)
    buf = recv_resp(sock, buf)

before = delayed_acks()
for _ in range(6):
    # Re-enter pingpong mode after the previous delayed-ACK timer expiry
    # exited it (TCP_PINGPONG_THRESH = 3), then pause past the 40-50 ms timer.
    for _ in range(4):
        sock.sendall(req)
        buf = recv_resp(sock, buf)
    time.sleep(0.10)
after = delayed_acks()
sock.close()
print(after - before)
"#;

fn root() -> PathBuf {
    // Canonical, so Unix socket paths stay under macOS's 104-byte limit.
    std::fs::canonicalize(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")).unwrap()
}

fn katamaran() -> PathBuf {
    root().join("target/release/katamaran")
}

fn run_child_with_timeout(mut child: Child, timeout: Duration) -> Result<Output, String> {
    let stdout_rx = child.stdout.take().map(|mut pipe| {
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let mut buf = Vec::new();
            let _ = pipe.read_to_end(&mut buf);
            let _ = tx.send(buf);
        });
        rx
    });
    let stderr_rx = child.stderr.take().map(|mut pipe| {
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let mut buf = Vec::new();
            let _ = pipe.read_to_end(&mut buf);
            let _ = tx.send(buf);
        });
        rx
    });

    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let stdout = stdout_rx
                    .and_then(|rx| rx.recv_timeout(Duration::from_secs(5)).ok())
                    .unwrap_or_default();
                let stderr = stderr_rx
                    .and_then(|rx| rx.recv_timeout(Duration::from_secs(5)).ok())
                    .unwrap_or_default();
                return Ok(Output {
                    status,
                    stdout,
                    stderr,
                });
            }
            Ok(None) => {
                let left = deadline.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    let _ = child.kill();
                    let _ = child.wait();
                    let stdout = stdout_rx
                        .and_then(|rx| rx.recv_timeout(Duration::from_secs(1)).ok())
                        .unwrap_or_default();
                    let stderr = stderr_rx
                        .and_then(|rx| rx.recv_timeout(Duration::from_secs(1)).ok())
                        .unwrap_or_default();
                    return Err(format!(
                        "timed out after {:.1}s\nstdout:\n{}\nstderr:\n{}",
                        timeout.as_secs_f64(),
                        String::from_utf8_lossy(&stdout),
                        String::from_utf8_lossy(&stderr),
                    ));
                }
                thread::sleep(left.min(Duration::from_millis(25)));
            }
            Err(e) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!("wait failed: {e}"));
            }
        }
    }
}

fn run_output_timeout(cmd: &mut Command, timeout: Duration) -> Result<Output, String> {
    let child = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawn failed: {e}"))?;
    run_child_with_timeout(child, timeout)
}

fn cli(home: &Path, user_home: &Path, args: &[&str]) -> Output {
    let mut cmd = Command::new(katamaran());
    cmd.args(args)
        .env("KATAMARAN_HOME", home)
        .env("HOME", user_home);
    run_output_timeout(&mut cmd, Duration::from_secs(240))
        .unwrap_or_else(|e| panic!("run katamaran {args:?} failed: {e}"))
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

fn ok_result(res: Result<Output, String>, what: &str) -> String {
    let out = res.unwrap_or_else(|e| panic!("{what} failed: {e}"));
    ok(&out, what)
}

fn wait_until(what: &str, timeout: Duration, mut f: impl FnMut(Duration) -> bool) {
    let deadline = Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(!remaining.is_zero(), "timed out waiting for {what}");
        if f(remaining) {
            return;
        }
        let left = deadline.saturating_duration_since(Instant::now());
        assert!(!left.is_zero(), "timed out waiting for {what}");
        thread::sleep(left.min(Duration::from_millis(200)));
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
        wait_until("the daemon socket", Duration::from_secs(10), |_| {
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
        let _ = docker_timeout(
            &self.docker_host,
            &["rm", "-f", &self.id],
            Duration::from_secs(15),
        );
    }
}

struct QuickackRestoreGuard {
    docker_host: String,
    restored: bool,
}

impl QuickackRestoreGuard {
    fn restore(mut self) {
        ok_result(
            docker_timeout(
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
                Duration::from_secs(30),
            ),
            "re-enable kat.quickack",
        );
        self.restored = true;
    }
}

impl Drop for QuickackRestoreGuard {
    fn drop(&mut self) {
        if !self.restored {
            let _ = docker_timeout(
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
                Duration::from_secs(15),
            );
        }
    }
}

fn kubectl_timeout(kubeconfig: &Path, args: &[&str], timeout: Duration) -> Result<Output, String> {
    let mut cmd = Command::new("kubectl");
    cmd.arg("--kubeconfig").arg(kubeconfig).args(args);
    run_output_timeout(&mut cmd, timeout)
}

fn kubectl(kubeconfig: &Path, args: &[&str]) -> Output {
    kubectl_timeout(kubeconfig, args, Duration::from_secs(60))
        .unwrap_or_else(|e| panic!("run kubectl {args:?} failed: {e}"))
}

fn docker_timeout(docker_host: &str, args: &[&str], timeout: Duration) -> Result<Output, String> {
    let mut cmd = Command::new("docker");
    cmd.env("DOCKER_HOST", docker_host)
        .env_remove("DOCKER_CONTEXT")
        .args(args);
    run_output_timeout(&mut cmd, timeout)
}

fn docker(docker_host: &str, args: &[&str]) -> Output {
    docker_timeout(docker_host, args, Duration::from_secs(60))
        .unwrap_or_else(|e| panic!("run docker {args:?} failed: {e}"))
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
    ok_result(
        run_child_with_timeout(build, Duration::from_secs(180)),
        "build test image",
    );
}

struct HostHttpServer {
    port: u16,
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl HostHttpServer {
    /// Minimal keep-alive HTTP/1.1 server on the Mac host (`127.0.0.1`),
    /// reached by guest containers through `host.docker.internal` over `eth0`.
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind host HTTP server");
        listener
            .set_nonblocking(true)
            .expect("set nonblocking on host listener");
        let port = listener.local_addr().unwrap().port();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_bg = stop.clone();
        let handle = thread::spawn(move || {
            let resp =
                b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: keep-alive\r\n\r\npong";
            while !stop_bg.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let _ = stream.set_nonblocking(false);
                        let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
                        let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
                        let _ = stream.set_nodelay(true);
                        thread::spawn(move || {
                            let mut buf = Vec::new();
                            let mut tmp = [0u8; 1024];
                            loop {
                                while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                                    match stream.read(&mut tmp) {
                                        Ok(0) | Err(_) => return,
                                        Ok(n) => buf.extend_from_slice(&tmp[..n]),
                                    }
                                }
                                let pos =
                                    buf.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
                                buf.drain(..pos);
                                if stream.write_all(resp).is_err() {
                                    return;
                                }
                            }
                        });
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(20));
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            port,
            stop,
            handle: Some(handle),
        }
    }
}

impl Drop for HostHttpServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
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
        ok_result(
            kubectl_timeout(
                &kubeconfig,
                &[
                    "wait",
                    "--for=condition=Ready",
                    "--timeout=120s",
                    &format!("pod/{name}"),
                ],
                Duration::from_secs(150),
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

    wait_until("qa-peer server ready", Duration::from_secs(15), |left| {
        kubectl_timeout(
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
            left.min(Duration::from_secs(5)),
        )
        .is_ok_and(|o| o.status.success())
    });

    // 1) Open ONE persistent keep-alive TCP connection from qa-probe to qa-peer
    //    and exercise three bursts on that exact established socket:
    //    - quickack=Y (default): fast (< 1 ms/req, no delayed-ACK stall)
    //    - toggle kat.quickack=0 at runtime: ALREADY-ESTABLISHED socket
    //      immediately hits the ~40–50 ms delayed-ACK stall
    //    - toggle kat.quickack=1 at runtime: SAME established socket
    //      immediately returns to < 1 ms/req
    let mut client = ChildGuard(
        Command::new("kubectl")
            .arg("--kubeconfig")
            .arg(&kubeconfig)
            .args([
                "exec",
                "-i",
                "qa-probe",
                "--",
                "python3",
                "-c",
                KEEP_ALIVE_CLIENT_PY,
                &peer_ip,
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("start persistent keep-alive client in qa-probe"),
    );
    let mut client_in = client.0.stdin.take().unwrap();
    let client_stdout = client.0.stdout.take().unwrap();
    let (line_tx, line_rx) = mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(client_stdout).lines() {
            match line {
                Ok(l) => {
                    if line_tx.send(l).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });
    let mut burst = |count: usize, what: &str| -> (f64, f64) {
        writeln!(client_in, "{count}").expect("write burst count");
        client_in.flush().expect("flush burst count");
        let line = line_rx
            .recv_timeout(Duration::from_secs(30))
            .unwrap_or_else(|e| {
                panic!("{what}: timed out or closed waiting for burst result: {e}")
            });
        parse_latencies(&line, what)
    };

    let (pod_p50, pod_total) = burst(25, "pod-to-pod keep-alive (quickack=Y)");
    eprintln!("pod-to-pod keep-alive (quickack=Y): p50={pod_p50:.2}ms total={pod_total:.2}ms");
    assert!(
        pod_p50 < 15.0 && pod_total < 400.0,
        "pod-to-pod keep-alive stalled (p50={pod_p50:.2}ms total={pod_total:.2}ms)"
    );

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
        // Same already-established socket immediately observes quickack=0.
        let (off_p50, off_total) = burst(8, "established pod keep-alive (quickack=N)");
        eprintln!(
            "pod-to-pod established keep-alive (quickack=N): p50={off_p50:.2}ms total={off_total:.2}ms"
        );
        assert!(
            off_p50 >= 25.0,
            "expected delayed-ACK stall on established socket with quickack=0 (got p50={off_p50:.2}ms total={off_total:.2}ms)"
        );

        restore.restore();
        // Same already-established socket immediately observes quickack=1 again.
        let (on_p50, on_total) = burst(15, "established pod keep-alive restored (quickack=Y)");
        eprintln!(
            "pod-to-pod established keep-alive restored (quickack=Y): p50={on_p50:.2}ms total={on_total:.2}ms"
        );
        assert!(
            on_p50 < 15.0 && on_total < 300.0,
            "established socket stayed stalled after restoring quickack=1 (p50={on_p50:.2}ms total={on_total:.2}ms)"
        );
    }
    drop(client_in);
    drop(client);

    // 2) Kubernetes ClusterIP Service path (qa-probe -> qa-svc ClusterIP ->
    //    qa-peer via kube-proxy DNAT): routed via pod's default gateway, stays
    //    in-VM (never crosses virtio-net), so quick ACK still applies.
    ok(
        &kubectl(
            &kubeconfig,
            &[
                "expose",
                "pod",
                "qa-peer",
                "--name=qa-svc",
                "--port=8080",
                "--target-port=8080",
            ],
        ),
        "expose qa-peer as ClusterIP service",
    );
    let svc_ip = ok(
        &kubectl(
            &kubeconfig,
            &["get", "svc", "qa-svc", "-o", "jsonpath={.spec.clusterIP}"],
        ),
        "get qa-svc ClusterIP",
    )
    .trim()
    .to_string();
    assert!(!svc_ip.is_empty(), "qa-svc has empty clusterIP");
    wait_until("qa-svc endpoints ready", Duration::from_secs(30), |left| {
        kubectl_timeout(
            &kubeconfig,
            &[
                "exec",
                "qa-probe",
                "--",
                "python3",
                "-c",
                KEEP_ALIVE_CLIENT_PY,
                &svc_ip,
                "1",
            ],
            left.min(Duration::from_secs(5)),
        )
        .is_ok_and(|o| o.status.success())
    });
    let svc_out = ok(
        &kubectl(
            &kubeconfig,
            &[
                "exec",
                "qa-probe",
                "--",
                "python3",
                "-c",
                KEEP_ALIVE_CLIENT_PY,
                &svc_ip,
                "15",
            ],
        ),
        "pod-to-ClusterIP split-write keep-alive",
    );
    let (svc_p50, svc_total) = parse_latencies(&svc_out, "pod-to-ClusterIP keep-alive");
    eprintln!(
        "pod-to-ClusterIP keep-alive (quickack=Y): p50={svc_p50:.2}ms total={svc_total:.2}ms"
    );
    assert!(
        svc_p50 < 15.0 && svc_total < 300.0,
        "pod-to-ClusterIP keep-alive stalled (p50={svc_p50:.2}ms total={svc_total:.2}ms)"
    );

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
        |left| {
            docker_timeout(
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
                left.min(Duration::from_secs(10)),
            )
            .is_ok_and(|o| o.status.success())
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

    // 4) Container-to-uplink (`eth0` / virtio-net -> host.docker.internal)
    //    vs container-to-container (`docker0`, in-VM): verifies packets
    //    arriving over virtio-net (`eth0`) do NOT force quick ACKs (delayed
    //    ACKs fire normally), while in-VM container traffic sends immediate
    //    quick ACKs (zero delayed ACKs).
    let host_srv = HostHttpServer::start();
    let uplink_delacks: u64 = ok(
        &docker(
            &docker_host,
            &[
                "run",
                "--rm",
                "--add-host=host.docker.internal:host-gateway",
                TEST_IMAGE,
                "python3",
                "-c",
                DELACK_PROBE_PY,
                "host.docker.internal",
                &host_srv.port.to_string(),
            ],
        ),
        "container-to-uplink DelayedACKs probe",
    )
    .trim()
    .parse()
    .expect("parse uplink DelayedACKs delta");
    let local_delacks: u64 = ok(
        &docker(
            &docker_host,
            &[
                "run",
                "--rm",
                TEST_IMAGE,
                "python3",
                "-c",
                DELACK_PROBE_PY,
                &srv_ip,
                "8080",
            ],
        ),
        "container-to-container DelayedACKs probe",
    )
    .trim()
    .parse()
    .expect("parse local DelayedACKs delta");
    eprintln!(
        "DelayedACKs delta (6 paused responses): uplink={uplink_delacks}, local={local_delacks}"
    );
    assert!(
        uplink_delacks >= 4,
        "expected normal delayed ACKs on container->eth0 uplink connection (delta={uplink_delacks})"
    );
    assert_eq!(
        local_delacks, 0,
        "expected immediate quick ACKs (0 delayed ACKs) on in-VM container connection"
    );

    // 5) Confirm the guest root routing table is completely unmutated (no
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
    let val = ok_result(
        docker_timeout(
            &docker_host,
            &["run", "--rm", BASE_IMAGE, "cat", QUICKACK_PARAM],
            Duration::from_secs(120),
        ),
        "read kat.quickack under --cmdline kat.quickack=0",
    );
    assert_eq!(
        val.trim(),
        "N",
        "expected {QUICKACK_PARAM} to be N with --cmdline kat.quickack=0"
    );
}
