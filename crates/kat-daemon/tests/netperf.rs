//! Network data path under load: a 10 GiB download from the Mac whose
//! SHA-256 must match while other downloads saturate the link, an upload
//! checked the same way while other uploads run, a download through a
//! published port checked the same way, and a burst of short connections
//! that must all succeed. Prints throughput; asserts correctness. Needs the internet (to
//! build a small alpine image with curl and coreutils, whose sha256sum is
//! eight times busybox's) and the `docker` CLI. Ignored by default; `cargo xtask
//! test-vm` builds everything and runs it. KAT_NETPERF_GIB overrides the
//! size of the checked download.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

const BASE: &str = "alpine:3.24.4";
const IMAGE: &str = "kat-netperf-test";
const GIB: u64 = 1 << 30;

/// Resolved, so the Unix sockets under the test home fit macOS's 104 bytes.
fn root() -> PathBuf {
    std::fs::canonicalize(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../.."))
        .expect("resolve the workspace root")
}

struct Vm {
    child: Child,
    lines: mpsc::Receiver<String>,
    output: Vec<String>,
    home: PathBuf,
}

impl Vm {
    fn start() -> Vm {
        let root = root();
        let home = root.join(format!("target/test-home-netperf-{}", std::process::id()));
        std::fs::create_dir_all(&home).unwrap();
        let mut child = Command::new(root.join("target/release/katamaran"))
            .current_dir(&root)
            .env("KATAMARAN_HOME", &home)
            .args(["run", "--cpus", "4", "--memory-mib", "4096"])
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
            home,
        }
    }

    /// Wait for a line containing `needle`; panic on timeout with all output.
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

    fn docker_cmd(&self, args: &[&str]) -> Command {
        let mut cmd = Command::new("docker");
        cmd.env(
            "DOCKER_HOST",
            format!("unix://{}", self.home.join("run/docker.sock").display()),
        )
        .env_remove("DOCKER_CONTEXT")
        .args(args);
        cmd
    }

    fn docker(&self, args: &[&str]) -> Output {
        let out = self.docker_cmd(args).output().expect("run the docker CLI");
        assert!(
            out.status.success(),
            "docker {args:?} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        out
    }

    /// `sh -c script` in a throwaway container; its stdout, trimmed.
    fn sh(&self, script: &str) -> String {
        let out = self.docker(&["run", "--rm", IMAGE, "sh", "-c", script]);
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    fn stop(mut self) {
        unsafe { libc::kill(self.child.id() as i32, libc::SIGINT) };
        self.wait_for("VM stopped", Duration::from_secs(30));
        let status = self.child.wait().unwrap();
        assert!(status.success(), "katamaran run exited with {status}");
    }
}

/// A failing assertion must not leave a VM running.
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
        let _ = std::fs::remove_dir_all(&self.home);
    }
}

/// SHA-256 from CommonCrypto (libSystem), so the test needs no crate.
struct Sha256([u32; 32]);

unsafe extern "C" {
    fn CC_SHA256_Init(c: *mut [u32; 32]) -> i32;
    fn CC_SHA256_Update(c: *mut [u32; 32], data: *const u8, len: u32) -> i32;
    fn CC_SHA256_Final(md: *mut u8, c: *mut [u32; 32]) -> i32;
}

impl Sha256 {
    fn new() -> Self {
        let mut s = Sha256([0; 32]);
        unsafe { CC_SHA256_Init(&mut s.0) };
        s
    }

    fn update(&mut self, data: &[u8]) {
        for chunk in data.chunks(u32::MAX as usize) {
            unsafe { CC_SHA256_Update(&mut self.0, chunk.as_ptr(), chunk.len() as u32) };
        }
    }

    fn hex(mut self) -> String {
        let mut md = [0u8; 32];
        unsafe { CC_SHA256_Final(md.as_mut_ptr(), &mut self.0) };
        md.iter().map(|b| format!("{b:02x}")).collect()
    }
}

/// A deterministic pseudo-random byte stream (xorshift64*), fast enough
/// not to limit the transfer.
struct Stream(u64);

impl Stream {
    fn fill(&mut self, buf: &mut [u8]) {
        for w in buf.as_chunks_mut::<8>().0 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            w.copy_from_slice(&self.0.wrapping_mul(0x2545_f491_4f6c_dd1d).to_le_bytes());
        }
    }
}

/// The Mac side, on 127.0.0.1 (containers reach it as host.docker.internal):
///   GET /stream/<bytes>/<seed>  the stream; its hash is reported on `hashes`
///   GET /zeros/<bytes>          zeros, as fast as possible
///   POST /sink                  replies with the body's SHA-256
///   GET /small                  "ok"
fn serve(hashes: mpsc::Sender<String>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    thread::spawn(move || {
        for conn in listener.incoming().map_while(Result::ok) {
            let hashes = hashes.clone();
            thread::spawn(move || {
                let _ = handle(conn, &hashes);
            });
        }
    });
    port
}

fn handle(conn: TcpStream, hashes: &mpsc::Sender<String>) -> std::io::Result<()> {
    let mut reader = BufReader::new(conn.try_clone()?);
    let mut request = String::new();
    reader.read_line(&mut request)?;
    let mut content_length = 0u64;
    loop {
        let mut line = String::new();
        reader.read_line(&mut line)?;
        if line.trim().is_empty() {
            break;
        }
        if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
            content_length = v.trim().parse().unwrap_or(0);
        }
    }
    let path = request.split_whitespace().nth(1).unwrap_or("");
    let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    let mut out = conn;
    let mut buf = vec![0u8; 1 << 20];
    match parts.as_slice() {
        ["stream", n, seed] => {
            let n: u64 = n.parse().unwrap();
            let mut stream = Stream(seed.parse::<u64>().unwrap() | 1);
            let mut sha = Sha256::new();
            write!(out, "HTTP/1.0 200 OK\r\nContent-Length: {n}\r\n\r\n")?;
            let mut left = n;
            while left > 0 {
                let k = left.min(buf.len() as u64) as usize;
                stream.fill(&mut buf[..k]);
                sha.update(&buf[..k]);
                out.write_all(&buf[..k])?;
                left -= k as u64;
            }
            let _ = hashes.send(sha.hex());
        }
        ["zeros", n] => {
            let n: u64 = n.parse().unwrap();
            write!(out, "HTTP/1.0 200 OK\r\nContent-Length: {n}\r\n\r\n")?;
            let mut left = n;
            while left > 0 {
                let k = left.min(buf.len() as u64) as usize;
                out.write_all(&buf[..k])?;
                left -= k as u64;
            }
        }
        ["sink"] => {
            let mut sha = Sha256::new();
            let mut left = content_length;
            while left > 0 {
                let want = left.min(buf.len() as u64) as usize;
                let k = reader.read(&mut buf[..want])?;
                if k == 0 {
                    break;
                }
                sha.update(&buf[..k]);
                left -= k as u64;
            }
            let body = format!("{} {}\n", sha.hex(), content_length - left);
            write!(
                out,
                "HTTP/1.0 200 OK\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            )?;
        }
        _ => out.write_all(b"HTTP/1.0 200 OK\r\nContent-Length: 2\r\n\r\nok")?,
    }
    out.flush()
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// The trimmed stdout of `docker args` once it succeeds, within a minute.
fn wait_for_output(vm: &Vm, args: &[&str]) -> String {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let out = vm.docker_cmd(args).output().expect("run the docker CLI");
        let text = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if out.status.success() && !text.is_empty() {
            return text;
        }
        assert!(Instant::now() < deadline, "docker {args:?} never succeeded");
        thread::sleep(Duration::from_millis(200));
    }
}

/// Read everything 127.0.0.1:port sends; its SHA-256, size and how long it
/// took. Retries while the forward or the server is not up yet, which shows
/// as a connection closed without data.
fn download_published(port: u16) -> (String, u64, Duration) {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut buf = vec![0u8; 1 << 20];
    loop {
        if let Ok(mut conn) = TcpStream::connect(("127.0.0.1", port)) {
            let t = Instant::now();
            let mut sha = Sha256::new();
            let mut n = 0u64;
            while let Ok(k) = conn.read(&mut buf) {
                if k == 0 {
                    break;
                }
                sha.update(&buf[..k]);
                n += k as u64;
            }
            if n > 0 {
                return (sha.hex(), n, t.elapsed());
            }
        }
        assert!(
            Instant::now() < deadline,
            "nothing served on 127.0.0.1:{port}"
        );
        thread::sleep(Duration::from_millis(200));
    }
}

fn gib_per_s(bytes: u64, d: Duration) -> f64 {
    bytes as f64 / GIB as f64 / d.as_secs_f64()
}

#[test]
#[ignore = "boots the system VM and uses the internet; run with `cargo xtask test-vm`"]
fn data_path_stays_correct_under_load() {
    let checked_gib: u64 = std::env::var("KAT_NETPERF_GIB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);
    let mut vm = Vm::start();
    vm.wait_for("docker ready", Duration::from_secs(120));
    let mut build = vm
        .docker_cmd(&["build", "-q", "-t", IMAGE, "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    write!(
        build.stdin.take().unwrap(),
        "FROM {BASE}\nRUN apk add --no-cache curl coreutils socat\n"
    )
    .unwrap();
    assert!(build.wait().unwrap().success(), "docker build failed");

    let (hashes, sent_hashes) = mpsc::channel();
    let port = serve(hashes);
    let url = |path: &str| format!("http://host.docker.internal:{port}{path}");

    // Throughput alone, for the log.
    let t = Instant::now();
    let n = vm.sh(&format!(
        "curl -sS -o /dev/null -w '%{{size_download}}' {}",
        url(&format!("/zeros/{}", 4 * GIB))
    ));
    assert_eq!(n, (4 * GIB).to_string());
    eprintln!("download: {:.2} GiB/s", gib_per_s(4 * GIB, t.elapsed()));

    // The checked download, with two full-speed downloads alongside.
    let load: Vec<Child> = (0..2)
        .map(|_| {
            vm.docker_cmd(&[
                "run",
                "--rm",
                IMAGE,
                "sh",
                "-c",
                &format!(
                    "curl -sS -o /dev/null -w '%{{size_download}}' {}",
                    url(&format!("/zeros/{}", 4 * GIB))
                ),
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap()
        })
        .collect();
    let bytes = checked_gib * GIB;
    let t = Instant::now();
    let got = vm.sh(&format!(
        "curl -sS {} | sha256sum",
        url(&format!("/stream/{bytes}/7"))
    ));
    let elapsed = t.elapsed();
    let want = sent_hashes
        .recv_timeout(Duration::from_secs(10))
        .expect("the server hashed what it sent");
    assert_eq!(got.split_whitespace().next(), Some(want.as_str()));
    eprintln!(
        "{checked_gib} GiB checked download beside two others: {:.2} GiB/s",
        gib_per_s(bytes, elapsed)
    );
    for child in load {
        let out = child.wait_with_output().unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(
            String::from_utf8_lossy(&out.stdout).trim(),
            (4 * GIB).to_string()
        );
    }

    // Upload: the container hashes a random file and posts it while two
    // other uploads run alongside. busybox wget's --post-file stops at the
    // first NUL byte, so requests are written by hand and sent with nc.
    let load = 4 * GIB;
    let script = format!(
        "head -c {GIB} /dev/urandom > /tmp/f && sha256sum /tmp/f | cut -d' ' -f1 && \
         up() {{ {{ printf 'POST /sink HTTP/1.0\\r\\nContent-Length: %s\\r\\n\\r\\n' $1; cat; }} \
         | nc host.docker.internal {port} | tail -n 1; }} && \
         for i in 1 2; do head -c {load} /dev/zero | up {load} > /tmp/load$i & done; \
         up {GIB} < /tmp/f; wait; cat /tmp/load1 /tmp/load2"
    );
    let t = Instant::now();
    let out = vm.sh(&script);
    eprintln!(
        "1 GiB upload beside two others (with file creation): {:?}",
        t.elapsed()
    );
    let lines: Vec<&str> = out.lines().collect();
    let [local, remote, load1, load2] = lines.as_slice() else {
        panic!("upload: {out}");
    };
    assert_eq!(*remote, format!("{local} {GIB}"), "upload: {out}");
    for l in [load1, load2] {
        assert!(l.ends_with(&format!(" {load}")), "load upload: {out}");
    }

    // Published port: the Mac downloads a random file a container serves
    // through -p, and its hash must match the container's.
    let pub_port = free_port();
    let container = "kat-netperf-pub";
    vm.docker(&[
        "run",
        "-d",
        "--rm",
        "--name",
        container,
        "-p",
        &format!("{pub_port}:8080"),
        IMAGE,
        "sh",
        "-c",
        &format!(
            "head -c {GIB} /dev/urandom > /tmp/f && sha256sum /tmp/f | cut -d' ' -f1 > /tmp/sum \
             && exec socat -u FILE:/tmp/f TCP-LISTEN:8080,reuseaddr"
        ),
    ]);
    let want = wait_for_output(&vm, &["exec", container, "cat", "/tmp/sum"]);
    let (got, n, elapsed) = download_published(pub_port);
    // Usually gone already: socat exits after one connection (--rm).
    let _ = vm.docker_cmd(&["rm", "-f", container]).output();
    assert_eq!((got.as_str(), n), (want.as_str(), GIB), "published port");
    eprintln!(
        "1 GiB through a published port: {:.2} GiB/s",
        gib_per_s(GIB, elapsed)
    );

    // Short connections, each a new flow through the stack's forwarder,
    // from successive containers: each starts with the same source port
    // sequence, so 4-tuples repeat while the stack still holds the earlier
    // ones in TIME_WAIT.
    for round in 0..4 {
        let failures = vm.sh(&format!(
            "f=0; for i in $(seq 500); do curl -sf -m 5 -o /dev/null {} || f=$((f+1)); done; echo $f",
            url("/small")
        ));
        assert_eq!(
            failures, "0",
            "short connections failed in container {round}"
        );
    }

    // The same source port again right away: the Mac side closed first, so
    // the stack holds the 4-tuple in TIME_WAIT when the next SYN arrives.
    let reuse = vm.sh(&format!(
        "for i in 1 2 3; do curl -sf -m 5 --local-port 40000 -o /dev/null {} || echo fail; done; echo done",
        url("/small")
    ));
    assert_eq!(reuse, "done", "a reused source port could not connect");

    vm.stop();
}
