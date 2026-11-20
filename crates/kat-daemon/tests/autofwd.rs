//! Auto-forwarding of unpublished container ports: a server in a container
//! started without `-p` is reachable on the Mac's 127.0.0.1 at the same port,
//! stops being forwarded when the container goes, keeps its first owner when
//! two containers listen on one port, and loopback-only listeners and ports
//! the Mac already uses are left alone. Needs the internet (to pull alpine)
//! and the `docker` CLI. Ignored by default; `cargo xtask test-vm` runs it.

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
        let home = root.join(format!("target/test-home-autofwd-{}", std::process::id()));
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

fn http_get(port: u16) -> Option<String> {
    let mut s = TcpStream::connect(("127.0.0.1", port)).ok()?;
    s.set_read_timeout(Some(Duration::from_millis(500))).ok()?;
    s.write_all(b"GET / HTTP/1.0\r\nHost: test\r\n\r\n").ok()?;
    let mut resp = String::new();
    s.read_to_string(&mut resp).ok()?;
    Some(resp)
}

/// A container answering every connection on `port` with `reply`.
fn serve(vm: &Vm, name: &str, bind: &str, port: u16, reply: &str) {
    let server = format!(
        "while true; do printf 'HTTP/1.0 200 OK\\r\\n\\r\\n{reply}\\n' | nc -l -s {bind} -p {port}; done"
    );
    vm.docker(&["run", "-d", "--name", name, IMAGE, "sh", "-c", &server]);
}

fn wait_reply(port: u16, want: &str, within: Duration) -> Duration {
    let started = Instant::now();
    loop {
        if http_get(port).is_some_and(|r| r.contains(want)) {
            return started.elapsed();
        }
        assert!(
            started.elapsed() < within,
            "127.0.0.1:{port} did not answer {want:?} within {within:?}"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
#[ignore = "boots the system VM and uses the internet; run with `cargo xtask test-vm`"]
fn unpublished_container_ports_are_forwarded() {
    let mut vm = Vm::start();
    vm.wait_for("docker ready", Duration::from_secs(120));
    vm.docker(&["pull", "-q", IMAGE]);

    // Unpublished, on every address: forwarded at the same port.
    let port = free_port();
    serve(&vm, "kat-af-a", "0.0.0.0", port, "from-a");
    let latency = wait_reply(port, "from-a", Duration::from_secs(3));
    eprintln!("unpublished port {port} reachable {latency:?} after docker run returned");
    vm.wait_for(
        &format!("auto-forwarding 127.0.0.1:{port}/tcp"),
        Duration::from_secs(5),
    );

    // A second container on the same port: the first keeps it, then the
    // second takes over without the Mac's port ever going away.
    serve(&vm, "kat-af-b", "0.0.0.0", port, "from-b");
    thread::sleep(Duration::from_secs(1));
    assert!(http_get(port).is_some_and(|r| r.contains("from-a")));
    vm.docker(&["rm", "-f", "kat-af-a"]);
    wait_reply(port, "from-b", Duration::from_secs(3));

    // Loopback-only inside the container stays private.
    let private = free_port();
    serve(&vm, "kat-af-lo", "127.0.0.1", private, "private");
    thread::sleep(Duration::from_secs(2));
    assert!(TcpStream::connect(("127.0.0.1", private)).is_err());

    // A port the Mac already uses is skipped and the Mac's listener keeps it.
    let mac = TcpListener::bind("127.0.0.1:0").unwrap();
    let busy = mac.local_addr().unwrap().port();
    serve(&vm, "kat-af-busy", "0.0.0.0", busy, "busy");
    vm.wait_for(
        &format!("cannot auto-forward 127.0.0.1:{busy}/tcp"),
        Duration::from_secs(5),
    );
    drop(mac);

    // `-p other:inner`: the published port works, the inner port is not
    // taken on the Mac.
    let (outer, inner) = (free_port(), free_port());
    let server = format!(
        "while true; do printf 'HTTP/1.0 200 OK\\r\\n\\r\\npublished\\n' | nc -l -p {inner}; done"
    );
    vm.docker(&[
        "run",
        "-d",
        "--name",
        "kat-af-pub",
        "-p",
        &format!("{outer}:{inner}"),
        IMAGE,
        "sh",
        "-c",
        &server,
    ]);
    wait_reply(outer, "published", Duration::from_secs(3));
    thread::sleep(Duration::from_secs(1));
    assert!(TcpStream::connect(("127.0.0.1", inner)).is_err());

    // Removing the owner removes the forward and frees the Mac's port.
    vm.docker(&[
        "rm",
        "-f",
        "kat-af-b",
        "kat-af-lo",
        "kat-af-busy",
        "kat-af-pub",
    ]);
    vm.wait_for(
        &format!("stopped auto-forwarding 127.0.0.1:{port}/tcp"),
        Duration::from_secs(5),
    );
    assert!(TcpListener::bind(("127.0.0.1", port)).is_ok());

    vm.stop();
}
