//! Child process supervision for PID 1: reaping every child (orphans
//! included), and running containerd + dockerd with readiness and exit
//! reporting to the host.

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read, Write};
use std::os::unix::net::UnixStream;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use kat_proto::Msg;
use kat_proto::backoff::Backoff;

use super::sys;

const PATH: &str = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";
pub const CONTAINERD_SOCKET: &str = "/run/containerd/containerd.sock";
pub const DOCKER_SOCKET: &str = "/run/docker.sock";
const READY_TIMEOUT: Duration = Duration::from_secs(120);

/// Reaps all children. PID 1 inherits every orphan, so something must wait
/// for them; processes started through [`Reaper::spawn`] also get their exit
/// status delivered on a channel.
#[derive(Clone)]
pub struct Reaper {
    watched: Arc<Mutex<HashMap<i32, Sender<i32>>>>,
}

impl Reaper {
    pub fn start() -> Reaper {
        let r = Reaper {
            watched: Arc::new(Mutex::new(HashMap::new())),
        };
        let watched = r.watched.clone();
        thread::Builder::new()
            .name("reaper".into())
            .spawn(move || reap_loop(&watched))
            .expect("spawn reaper thread");
        r
    }

    /// Spawn `cmd` and return its pid plus a channel that yields its wait
    /// status. The lock is held across spawn and registration so an
    /// immediate exit cannot be reaped before it is watched.
    pub fn spawn(&self, cmd: &mut Command) -> io::Result<(i32, Receiver<i32>)> {
        let mut watched = self.watched.lock().unwrap();
        let child = cmd.spawn()?;
        let pid = child.id() as i32;
        let (tx, rx) = mpsc::channel();
        watched.insert(pid, tx);
        Ok((pid, rx))
    }
}

fn reap_loop(watched: &Mutex<HashMap<i32, Sender<i32>>>) {
    loop {
        let mut status = 0;
        let pid = unsafe { libc::waitpid(-1, &mut status, 0) };
        if pid < 0 {
            // ECHILD: nothing to wait for yet.
            thread::sleep(Duration::from_millis(50));
            continue;
        }
        if let Some(tx) = watched.lock().unwrap().remove(&pid) {
            let _ = tx.send(status);
        }
    }
}

pub fn describe_status(status: i32) -> String {
    if libc::WIFEXITED(status) {
        format!("exited with status {}", libc::WEXITSTATUS(status))
    } else if libc::WIFSIGNALED(status) {
        format!("killed by signal {}", libc::WTERMSIG(status))
    } else {
        format!("wait status {status}")
    }
}

/// Sends control messages to the host from any thread.
#[derive(Clone)]
pub struct Notifier(pub Arc<Mutex<File>>);

impl Notifier {
    pub fn send(&self, msg: &Msg) {
        let mut conn = self.0.lock().unwrap();
        if let Err(e) = kat_proto::write_msg(&mut *conn, msg) {
            eprintln!("kat-init: failed to notify host: {e}");
        }
    }

    pub fn failed(&self, name: &str, error: String) {
        self.send(&Msg::ServiceFailed {
            name: name.into(),
            error,
        });
    }
}

/// The two services of the Docker stack, in start order.
const CONTAINERD: usize = 0;
const DOCKERD: usize = 1;
const NAMES: [&str; 2] = ["containerd", "dockerd"];
/// Stop timeouts, after which the service is killed. dockerd's covers
/// containers the shutdown sequence did not stop already.
const STOP_TIMEOUT: [Duration; 2] = [Duration::from_secs(5), Duration::from_secs(10)];
/// Give up after this many restarts without a stable run.
const MAX_RESTARTS: u32 = 10;

fn backoff() -> Backoff {
    Backoff::new(
        Duration::from_millis(500),
        Duration::from_secs(30),
        Duration::from_secs(60),
    )
}

/// A running service process.
struct Service {
    name: &'static str,
    pid: i32,
    started: Instant,
    exited: Arc<Mutex<Option<i32>>>,
    exit_rx: Receiver<()>,
}

impl Service {
    /// Spawn `program`; its exit is sent on `exits` as `(index, wait status)`.
    fn spawn(
        reaper: &Reaper,
        exits: &Sender<(usize, i32)>,
        index: usize,
        program: &str,
        args: &[&str],
    ) -> io::Result<Service> {
        let name = NAMES[index];
        sys::mkdir("/var/log")?;
        // Appended, so a restarted service keeps the log of its crash.
        let log = File::options()
            .create(true)
            .append(true)
            .open(format!("/var/log/{name}.log"))?;
        let mut cmd = Command::new(program);
        cmd.args(args)
            .env("PATH", PATH)
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log);
        let (pid, status_rx) = reaper.spawn(&mut cmd)?;
        let exited = Arc::new(Mutex::new(None));
        let (exit_tx, exit_rx) = mpsc::channel();
        let (exited2, exits) = (exited.clone(), exits.clone());
        thread::spawn(move || {
            if let Ok(status) = status_rx.recv() {
                *exited2.lock().unwrap() = Some(status);
                let _ = exit_tx.send(());
                let _ = exits.send((index, status));
            }
        });
        Ok(Service {
            name,
            pid,
            started: Instant::now(),
            exited,
            exit_rx,
        })
    }

    fn has_exited(&self) -> bool {
        self.exited.lock().unwrap().is_some()
    }

    /// SIGTERM, wait up to `timeout`, then SIGKILL.
    fn stop(&self, timeout: Duration) {
        if self.has_exited() {
            return;
        }
        let t = Instant::now();
        unsafe { libc::kill(self.pid, libc::SIGTERM) };
        if self.exit_rx.recv_timeout(timeout).is_err() {
            eprintln!(
                "kat-init: {} did not stop in {}s, killing it",
                self.name,
                timeout.as_secs()
            );
            unsafe { libc::kill(self.pid, libc::SIGKILL) };
            let _ = self.exit_rx.recv_timeout(Duration::from_secs(2));
        }
        eprintln!(
            "kat-init: shutdown: {} stopped in {} ms",
            self.name,
            t.elapsed().as_millis()
        );
    }
}

/// Poll `check` until it succeeds, the process exits or the timeout passes.
fn wait_ready(
    name: &str,
    exited: &Mutex<Option<i32>>,
    check: impl Fn() -> bool,
) -> Result<(), String> {
    let deadline = Instant::now() + READY_TIMEOUT;
    loop {
        if exited.lock().unwrap().is_some() {
            return Err(format!("{name} exited during startup"));
        }
        if check() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{name} not ready after {}s, see /var/log/{name}.log",
                READY_TIMEOUT.as_secs(),
            ));
        }
        thread::sleep(Duration::from_millis(2));
    }
}

#[derive(Default)]
struct Inner {
    services: Mutex<[Option<Service>; 2]>,
    stopping: AtomicBool,
    reloading_containerd: AtomicBool,
}

/// containerd and dockerd, started in order on a background thread and
/// restarted with backoff when either exits. Each exit is reported to the
/// host as `ServiceFailed`, and dockerd's return to service as
/// `ServiceReady { name: "docker" }`.
#[derive(Clone, Default)]
pub struct DockerStack {
    inner: Arc<Inner>,
}

impl DockerStack {
    pub fn start(reaper: Reaper, notifier: Notifier) -> DockerStack {
        let stack = DockerStack::default();
        let inner = stack.inner.clone();
        thread::Builder::new()
            .name("docker-supervisor".into())
            .spawn(move || supervise(&reaper, &notifier, &inner))
            .expect("spawn docker supervisor");
        stack
    }

    /// Restart containerd cleanly (without reporting a failure or backoff)
    /// so config changes such as enabling CRI take effect on demand.
    pub fn restart_containerd(&self) {
        self.inner
            .reloading_containerd
            .store(true, Ordering::SeqCst);
        let services = self.inner.services.lock().unwrap();
        if let Some(s) = &services[CONTAINERD] {
            s.stop(STOP_TIMEOUT[CONTAINERD]);
        }
    }

    /// Stop in reverse start order: dockerd first, then containerd. No
    /// restart happens after this.
    pub fn stop(&self) {
        self.inner.stopping.store(true, Ordering::SeqCst);
        let services = self.inner.services.lock().unwrap();
        for i in [DOCKERD, CONTAINERD] {
            if let Some(s) = &services[i] {
                s.stop(STOP_TIMEOUT[i]);
            }
        }
    }
}

fn supervise(reaper: &Reaper, notifier: &Notifier, inner: &Inner) {
    let (exits, exited) = mpsc::channel();
    let mut backoff = [backoff(), backoff()];
    let mut to_start = vec![CONTAINERD, DOCKERD];
    loop {
        for i in std::mem::take(&mut to_start) {
            match start_one(reaper, &exits, inner, i) {
                Ok(()) if i == DOCKERD => notifier.send(&Msg::ServiceReady {
                    name: "docker".into(),
                    at_ns: sys::boottime_ns(),
                }),
                Ok(()) => {}
                Err(None) => return,
                // The process is gone or killed; its exit restarts it below.
                Err(Some(e)) => notifier.failed(NAMES[i], e),
            }
        }
        let Ok((i, status)) = exited.recv() else {
            return;
        };
        if inner.stopping.load(Ordering::SeqCst) {
            return;
        }
        if i == CONTAINERD && inner.reloading_containerd.swap(false, Ordering::SeqCst) {
            to_start.push(CONTAINERD);
            continue;
        }
        let ran_for = inner.services.lock().unwrap()[i]
            .as_ref()
            .map_or(Duration::ZERO, |s| s.started.elapsed());
        let delay = backoff[i].next(ran_for);
        let name = NAMES[i];
        if backoff[i].failures() > MAX_RESTARTS {
            notifier.failed(
                name,
                format!(
                    "{name} {}; gave up after {MAX_RESTARTS} restarts",
                    describe_status(status)
                ),
            );
            continue;
        }
        notifier.failed(
            name,
            format!(
                "{name} {}; restarting in {} ms",
                describe_status(status),
                delay.as_millis()
            ),
        );
        if !sleep_unless_stopping(delay, &inner.stopping) {
            return;
        }
        to_start.push(i);
    }
}

/// Start service `i` and wait until it serves. `Err(None)` means the stack
/// is stopping; `Err(Some)` describes a failed start, after which the
/// process has exited or is killed.
fn start_one(
    reaper: &Reaper,
    exits: &Sender<(usize, i32)>,
    inner: &Inner,
    i: usize,
) -> Result<(), Option<String>> {
    let dockerd_args = super::net::dockerd_args();
    let (program, args): (&str, Vec<&str>) = match i {
        CONTAINERD => ("/usr/bin/containerd", vec![]),
        _ => (
            "/usr/bin/dockerd",
            dockerd_args.iter().map(String::as_str).collect(),
        ),
    };
    let mut services = inner.services.lock().unwrap();
    // Checked under the lock, so stop() never misses a service.
    if inner.stopping.load(Ordering::SeqCst) {
        return Err(None);
    }
    let service = Service::spawn(reaper, exits, i, program, &args)
        .map_err(|e| Some(format!("start {}: {e}", NAMES[i])))?;
    let (pid, exited) = (service.pid, service.exited.clone());
    services[i] = Some(service);
    drop(services);

    let ready = match i {
        CONTAINERD => wait_ready(NAMES[i], &exited, || {
            UnixStream::connect(CONTAINERD_SOCKET).is_ok()
        }),
        _ => wait_ready(NAMES[i], &exited, docker_ping),
    };
    ready.map_err(|e| {
        if exited.lock().unwrap().is_none() {
            unsafe { libc::kill(pid, libc::SIGKILL) };
        }
        Some(e)
    })
}

/// Sleep for `d`, waking early if `stopping` is set. Returns false if it was.
fn sleep_unless_stopping(d: Duration, stopping: &AtomicBool) -> bool {
    let deadline = Instant::now() + d;
    while Instant::now() < deadline {
        if stopping.load(Ordering::SeqCst) {
            return false;
        }
        thread::sleep(Duration::from_millis(20).min(deadline - Instant::now()));
    }
    !stopping.load(Ordering::SeqCst)
}

/// `GET /_ping` over the Docker socket.
fn docker_ping() -> bool {
    let Ok(mut s) = UnixStream::connect(DOCKER_SOCKET) else {
        return false;
    };
    let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
    if s.write_all(b"GET /_ping HTTP/1.1\r\nHost: docker\r\nConnection: close\r\n\r\n")
        .is_err()
    {
        return false;
    }
    let mut buf = [0u8; 64];
    match s.read(&mut buf) {
        Ok(n) => is_http_ok(&buf[..n]),
        Err(_) => false,
    }
}

fn is_http_ok(resp: &[u8]) -> bool {
    resp.starts_with(b"HTTP/1.1 200") || resp.starts_with(b"HTTP/1.0 200")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognises_http_ok() {
        assert!(is_http_ok(b"HTTP/1.1 200 OK\r\n"));
        assert!(!is_http_ok(b"HTTP/1.1 500 Internal"));
        assert!(!is_http_ok(b""));
    }

    #[test]
    fn describes_wait_status() {
        assert_eq!(describe_status(1 << 8), "exited with status 1");
        assert_eq!(describe_status(9), "killed by signal 9");
    }
}
