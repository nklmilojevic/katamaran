//! Runs the full system VM (system image, data disk, containerd, dockerd)
//! and talks to the Docker API over the forwarded socket. Ignored by
//! default; `cargo xtask test-vm` builds everything and runs it.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

fn root() -> PathBuf {
    // Canonical, so the control socket path stays under SUN_LEN.
    std::fs::canonicalize(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")).unwrap()
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
        let home = root.join(format!("target/test-home-{}", std::process::id()));
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
        let deadline = std::time::Instant::now() + timeout;
        loop {
            let left = deadline.saturating_duration_since(std::time::Instant::now());
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

    fn docker_socket(&self) -> PathBuf {
        self.home.join("run/docker.sock")
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

/// Send a raw HTTP request, optionally half-closing before reading.
fn http(socket: &Path, request: &str, half_close: bool) -> String {
    let mut s = UnixStream::connect(socket).expect("connect docker socket");
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    s.write_all(request.as_bytes()).unwrap();
    if half_close {
        s.shutdown(Shutdown::Write).unwrap();
    }
    let mut resp = String::new();
    let _ = s.read_to_string(&mut resp);
    resp
}

#[test]
#[ignore = "boots the system VM; run with `cargo xtask test-vm`"]
fn docker_api_is_served_and_survives_half_close() {
    let mut vm = Vm::start();
    vm.wait_for("docker ready", Duration::from_secs(120));
    let sock = vm.docker_socket();

    let ping = http(
        &sock,
        "GET /_ping HTTP/1.1\r\nHost: docker\r\nConnection: close\r\n\r\n",
        false,
    );
    assert!(ping.starts_with("HTTP/1.1 200"), "ping: {ping}");

    let info = http(
        &sock,
        "GET /info HTTP/1.1\r\nHost: docker\r\nConnection: close\r\n\r\n",
        false,
    );
    assert!(info.contains("\"Driver\":\"overlayfs\""), "info: {info}");
    assert!(info.contains("\"CgroupVersion\":\"2\""), "info: {info}");

    // The docker CLI half-closes its side of attach and exec streams. The
    // response must still arrive (libkrun used to turn this into a reset).
    let half = http(
        &sock,
        "GET /_ping HTTP/1.1\r\nHost: docker\r\nConnection: close\r\n\r\n",
        true,
    );
    assert!(
        half.starts_with("HTTP/1.1 200"),
        "half-closed ping: {half:?}"
    );

    vm.stop();
}
