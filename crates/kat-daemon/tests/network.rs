//! Guest networking end to end: a real `docker pull`, outbound HTTP from a
//! container, automatic port forwarding to the Mac's 127.0.0.1 and
//! host.docker.internal. Needs internet access and the `docker` CLI. Ignored
//! by default; `cargo xtask test-vm` builds everything and runs it.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

const IMAGE: &str = "alpine:3.24.4";

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
        let home = root.join(format!("target/test-home-net-{}", std::process::id()));
        std::fs::create_dir_all(&home).unwrap();
        let mut child = Command::new(root.join("target/release/katamaran"))
            .current_dir(&root)
            .env("KATAMARAN_HOME", &home)
            .args(["run", "--cpus", "2", "--memory-mib", "2048"])
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

    fn docker(&self, args: &[&str]) -> Output {
        let out = Command::new("docker")
            .env(
                "DOCKER_HOST",
                format!("unix://{}", self.home.join("run/docker.sock").display()),
            )
            .env_remove("DOCKER_CONTEXT")
            .args(args)
            .output()
            .expect("run the docker CLI");
        assert!(
            out.status.success(),
            "docker {args:?} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        out
    }

    fn stop(mut self) {
        unsafe { libc::kill(self.child.id() as i32, libc::SIGINT) };
        self.wait_for("VM stopped", Duration::from_secs(30));
        let status = self.child.wait().unwrap();
        assert!(status.success(), "katamaran run exited with {status}");
        assert!(
            !self.home.join("run/gvproxy-api.sock").exists(),
            "gvproxy was not stopped"
        );
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

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn free_low_port() -> Option<u16> {
    std::iter::once(80u16)
        .chain((900u16..=1023).rev())
        .find(|&p| TcpListener::bind(("0.0.0.0", p)).is_ok())
}

fn http_get(port: u16) -> Option<String> {
    let mut s = TcpStream::connect(("127.0.0.1", port)).ok()?;
    s.set_read_timeout(Some(Duration::from_millis(500))).ok()?;
    s.write_all(b"GET / HTTP/1.0\r\nHost: test\r\n\r\n").ok()?;
    let mut resp = String::new();
    s.read_to_string(&mut resp).ok()?;
    Some(resp)
}

#[test]
#[ignore = "boots the system VM and uses the internet; run with `cargo xtask test-vm`"]
fn containers_reach_the_internet_and_the_mac_and_ports_are_forwarded() {
    let mut vm = Vm::start();
    vm.wait_for("docker ready", Duration::from_secs(120));

    // A fresh data disk, so this is a real pull over the network.
    let t = Instant::now();
    vm.docker(&["pull", "-q", IMAGE]);
    eprintln!("pulled {IMAGE} in {:?}", t.elapsed());

    let page = vm.docker(&["run", "--rm", IMAGE, "wget", "-qO-", "http://example.com"]);
    let page = String::from_utf8_lossy(&page.stdout);
    assert!(page.contains("Example Domain"), "example.com: {page}");

    // DNS goes through the Mac's resolver: answers carry real TTLs, a name
    // that does not exist is NXDOMAIN, and AAAA for an IPv4-only name is
    // NODATA (NOERROR), without which musl's getaddrinfo fails outright.
    let script = r#"apk add -q bind-tools >/dev/null 2>&1 || exit 3
status() { dig +tries=1 +time=5 +noall +comments "$@" | sed -n 's/.*status: \([A-Z]*\).*/\1/p'; }
echo "aaaa=$(status github.com AAAA)"
echo "nx=$(status nothing-$$.invalid A)"
echo "ttl=$(dig +tries=1 +time=5 +noall +answer example.com A | awk 'NR == 1 { print $2 }')"
getent hosts github.com >/dev/null && echo getaddrinfo=ok"#;
    let dns = vm.docker(&["run", "--rm", IMAGE, "sh", "-c", script]);
    let dns = String::from_utf8_lossy(&dns.stdout);
    let field = |key: &str| {
        dns.lines()
            .find_map(|l| l.strip_prefix(&format!("{key}=")))
            .unwrap_or_default()
    };
    assert_eq!(field("aaaa"), "NOERROR", "AAAA github.com: {dns}");
    assert_eq!(field("nx"), "NXDOMAIN", "{dns}");
    assert!(
        field("ttl").parse::<u32>().is_ok_and(|t| t > 0),
        "example.com's TTL: {dns}"
    );
    assert_eq!(field("getaddrinfo"), "ok", "{dns}");

    // host.docker.internal reaches a listener on the Mac's 127.0.0.1.
    let mac = TcpListener::bind("127.0.0.1:0").unwrap();
    let mac_port = mac.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut c, _) = mac.accept().unwrap();
        c.write_all(b"hello-from-the-mac\n").unwrap();
    });
    let got = vm.docker(&[
        "run",
        "--rm",
        IMAGE,
        "nc",
        "-w",
        "5",
        "host.docker.internal",
        &mac_port.to_string(),
    ]);
    assert_eq!(String::from_utf8_lossy(&got.stdout), "hello-from-the-mac\n");
    server.join().unwrap();

    // `-p port:80` is forwarded from the Mac's 127.0.0.1 without any flag.
    let port = free_port();
    let server = r#"while true; do printf 'HTTP/1.0 200 OK\r\nContent-Length: 15\r\n\r\nhello-from-k8n\n' | nc -l -p 80; done"#;
    let cid = vm.docker(&[
        "run",
        "-d",
        "-p",
        &format!("{port}:80"),
        IMAGE,
        "sh",
        "-c",
        server,
    ]);
    let cid = String::from_utf8_lossy(&cid.stdout).trim().to_string();
    let started = Instant::now();
    let latency = loop {
        if http_get(port).is_some_and(|r| r.contains("hello-from-k8n")) {
            break started.elapsed();
        }
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "127.0.0.1:{port} not reachable within 2 s"
        );
        thread::sleep(Duration::from_millis(10));
    };
    eprintln!("port {port} reachable {latency:?} after the container started");

    // Removing the container removes the forward and frees the Mac's port.
    vm.docker(&["rm", "-f", &cid]);
    vm.wait_for(
        &format!("stopped forwarding 127.0.0.1:{port}/tcp"),
        Duration::from_secs(5),
    );
    assert!(TcpListener::bind(("127.0.0.1", port)).is_ok());

    // Privileged port (< 1024, e.g. `-p 80:80`) is forwarded without root.
    if let Some(low) = free_low_port() {
        let cid = vm.docker(&[
            "run",
            "-d",
            "-p",
            &format!("{low}:80"),
            IMAGE,
            "sh",
            "-c",
            server,
        ]);
        let cid = String::from_utf8_lossy(&cid.stdout).trim().to_string();
        let started = Instant::now();
        loop {
            if http_get(low).is_some_and(|r| r.contains("hello-from-k8n")) {
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(3),
                "127.0.0.1:{low} not reachable within 3 s"
            );
            thread::sleep(Duration::from_millis(10));
        }
        eprintln!(
            "privileged port {low} reachable {:?} after the container started",
            started.elapsed()
        );
        vm.docker(&["rm", "-f", &cid]);
        vm.wait_for(
            &format!("stopped forwarding 127.0.0.1:{low}/tcp"),
            Duration::from_secs(5),
        );
    }

    vm.stop();
}
