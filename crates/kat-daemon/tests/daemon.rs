//! The background daemon driven through the CLI, and the LaunchAgent that
//! `katamaran setup` installs. Ignored by default; `cargo xtask test-vm`
//! builds everything and runs them. Every test uses its own KATAMARAN_HOME
//! (and, for setup, its own HOME and a suffixed label), and a Drop guard
//! stops whatever it started even when an assertion fails.

use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

fn root() -> PathBuf {
    // Canonical, so socket paths stay under SUN_LEN.
    std::fs::canonicalize(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")).unwrap()
}

fn katamaran() -> PathBuf {
    root().join("target/release/katamaran")
}

/// Run the CLI against `home` (and optionally a fake user HOME).
fn cli(home: &Path, user_home: Option<&Path>, args: &[&str]) -> Output {
    let mut cmd = Command::new(katamaran());
    cmd.args(args)
        .env("KATAMARAN_HOME", home)
        .stdin(Stdio::null());
    if let Some(h) = user_home {
        cmd.env("HOME", h);
    }
    cmd.output().expect("run katamaran")
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

fn status_json(home: &Path) -> serde_json::Value {
    let out = cli(home, None, &["status", "--json"]);
    serde_json::from_str(&ok(&out, "status --json")).unwrap()
}

fn wait_until(what: &str, timeout: Duration, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(100));
    }
}

/// A daemon this test spawned: SIGTERM (which stops the VM), then SIGKILL.
struct Daemon {
    child: Child,
    home: PathBuf,
}

impl Daemon {
    fn spawn(home: &Path) -> Daemon {
        std::fs::create_dir_all(home).unwrap();
        let child = Command::new(katamaran())
            .arg("daemon")
            .env("KATAMARAN_HOME", home)
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

fn docker_ping(socket: &Path) -> String {
    let mut s = UnixStream::connect(socket).expect("connect docker socket");
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    s.write_all(b"GET /_ping HTTP/1.1\r\nHost: docker\r\nConnection: close\r\n\r\n")
        .unwrap();
    let mut resp = String::new();
    let _ = s.read_to_string(&mut resp);
    resp
}

#[test]
#[ignore = "boots the system VM; run with `cargo xtask test-vm`"]
fn daemon_runs_the_vm_for_the_cli() {
    let home = root().join(format!("target/tdh-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&home);
    std::fs::create_dir_all(&home).unwrap();
    // Small VM; saved before the daemon starts, applied at start.
    ok(
        &cli(
            &home,
            None,
            &["config", "set", "cpus=2", "memory=2G", "mount=off"],
        ),
        "config set",
    );
    let mut daemon = Daemon::spawn(&home);
    assert_eq!(status_json(&home)["state"], "stopped");

    let t = Instant::now();
    let start = ok(&cli(&home, None, &["start"]), "start");
    let took = t.elapsed();
    assert!(start.contains("Docker ready in"), "start: {start}");
    eprintln!("start: {} (wall {took:?})", start.trim());

    let s = status_json(&home);
    assert_eq!(s["state"], "running", "{s}");
    assert_eq!(s["docker"]["state"], "ready", "{s}");
    assert_eq!(s["vm"]["cpus"], 2, "{s}");
    assert_eq!(s["vm"]["memory_mib"], 2048, "{s}");
    assert_eq!(s["mount"]["enabled"], false, "{s}");
    assert_eq!(s["daemon"]["pid"], daemon.child.id(), "{s}");
    let sock = PathBuf::from(s["docker"]["socket"].as_str().unwrap());
    assert!(docker_ping(&sock).starts_with("HTTP/1.1 200"));

    let human = ok(&cli(&home, None, &["status"]), "status");
    assert!(human.starts_with("katamaran: running"), "{human}");
    assert!(human.contains("2 CPUs · 2048 MiB"), "{human}");

    // Guest usage arrives within a couple of sample intervals.
    wait_until("a usage sample", Duration::from_secs(10), || {
        status_json(&home)["usage"]["guest_mem_total_mib"]
            .as_u64()
            .is_some_and(|m| m > 1000)
    });

    let logs = ok(&cli(&home, None, &["logs", "-n", "100"]), "logs");
    assert!(logs.contains("docker ready"), "logs: {logs}");
    assert!(logs.contains("state starting -> running"), "logs: {logs}");

    // Follow: a line written after the follower attached reaches it.
    let mut follower = Command::new(katamaran())
        .args(["logs", "-f", "-n", "1"])
        .env("KATAMARAN_HOME", &home)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let stdout = follower.stdout.take().unwrap();
    thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            let _ = tx.send(line);
        }
    });
    thread::sleep(Duration::from_millis(500));
    ok(
        &cli(&home, None, &["config", "set", "cpus", "1"]),
        "config set",
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut seen = Vec::new();
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        match rx.recv_timeout(left) {
            Ok(l) if l.contains("config saved") => break,
            Ok(l) => seen.push(l),
            Err(_) => panic!("follower never saw the config change; got {seen:?}"),
        }
    }
    let _ = follower.kill();
    let _ = follower.wait();
    let pending = ok(&cli(&home, None, &["status"]), "status");
    assert!(pending.contains("pending"), "{pending}");
    ok(
        &cli(&home, None, &["config", "set", "cpus", "2"]),
        "config set",
    );

    // One daemon per home: a second daemon and `run` both refuse.
    let second = cli(&home, None, &["daemon", "--no-log-file"]);
    assert!(!second.status.success());
    assert!(
        String::from_utf8_lossy(&second.stderr).contains("already running"),
        "{second:?}"
    );
    let run = cli(&home, None, &["run"]);
    assert!(!run.status.success());
    assert!(String::from_utf8_lossy(&run.stderr).contains("already running"));
    // A start while running is a no-op.
    let again = ok(&cli(&home, None, &["start"]), "start again");
    assert!(again.contains("already running"), "{again}");

    let stop = ok(&cli(&home, None, &["stop"]), "stop");
    assert!(stop.contains("stopped in"), "stop: {stop}");
    eprintln!("stop: {}", stop.trim());
    let s = status_json(&home);
    assert_eq!(s["state"], "stopped", "{s}");
    assert_eq!(s["docker"]["state"], "stopped", "{s}");
    assert!(s.get("vm").is_none(), "{s}");
    // The daemon outlives the VM.
    assert!(matches!(daemon.child.try_wait(), Ok(None)));

    // Restart from stopped boots again; SIGTERM stops the VM and the daemon.
    ok(&cli(&home, None, &["restart"]), "restart");
    assert_eq!(status_json(&home)["state"], "running");
    unsafe { libc::kill(daemon.child.id() as i32, libc::SIGTERM) };
    let deadline = Instant::now() + Duration::from_secs(60);
    let status = loop {
        if let Some(s) = daemon.child.try_wait().unwrap() {
            break s;
        }
        assert!(Instant::now() < deadline, "daemon did not exit on SIGTERM");
        thread::sleep(Duration::from_millis(100));
    };
    assert!(status.success(), "daemon exited with {status}");
    assert!(!home.join("run/daemon.sock").exists());
    let log = std::fs::read_to_string(home.join("logs/daemon.log")).unwrap();
    assert!(log.contains("VM stopped"), "{log}");
}

/// Undo `katamaran setup` whatever happens.
struct Installed {
    home: PathBuf,
    user_home: PathBuf,
    label: String,
}

impl Drop for Installed {
    fn drop(&mut self) {
        let target = format!("gui/{}/{}", unsafe { libc::getuid() }, self.label);
        let _ = Command::new("launchctl")
            .args(["bootout", &target])
            .stderr(Stdio::null())
            .status();
        let _ = std::fs::remove_dir_all(&self.user_home);
        let _ = std::fs::remove_dir_all(&self.home);
    }
}

fn loaded(label: &str) -> bool {
    Command::new("launchctl")
        .args([
            "print",
            &format!("gui/{}/{label}", unsafe { libc::getuid() }),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap()
        .success()
}

#[test]
#[ignore = "installs a suffixed LaunchAgent and boots the VM; run with `cargo xtask test-vm`"]
fn setup_installs_a_launch_agent_and_uninstall_removes_it() {
    let pid = std::process::id();
    let home = root().join(format!("target/tsh-{pid}"));
    let user_home = root().join(format!("target/tsu-{pid}"));
    let _ = std::fs::remove_dir_all(&home);
    let _ = std::fs::remove_dir_all(&user_home);
    std::fs::create_dir_all(user_home.join(".local/bin")).unwrap();
    let suffix = format!("tsh-{pid}");
    let label = format!("dev.katamaran.daemon.{suffix}");
    let guard = Installed {
        home: home.clone(),
        user_home: user_home.clone(),
        label: label.clone(),
    };
    ok(
        &cli(
            &home,
            Some(&user_home),
            &["config", "set", "cpus=2", "memory=2G", "mount=off"],
        ),
        "config set",
    );

    let out = ok(&cli(&home, Some(&user_home), &["setup"]), "setup");
    eprintln!("{out}");
    let plist = user_home.join(format!("Library/LaunchAgents/{label}.plist"));
    assert!(plist.is_file(), "{out}");
    assert!(loaded(&label));
    let link = user_home.join(format!(".local/bin/kat-{suffix}"));
    assert_eq!(std::fs::read_link(&link).unwrap(), katamaran());
    let context = format!("katamaran-{suffix}");
    let docker = |args: &[&str]| {
        Command::new("docker")
            .args(args)
            .env("HOME", &user_home)
            .env_remove("DOCKER_HOST")
            .env_remove("DOCKER_CONTEXT")
            .output()
    };
    if let Ok(inspect) = docker(&["context", "inspect", &context]) {
        assert!(inspect.status.success(), "{out}");
        let text = String::from_utf8_lossy(&inspect.stdout);
        assert!(text.contains(&home.join("run/docker.sock").display().to_string()));
        // Not made the default without --default.
        let current = docker(&["context", "show"]).unwrap();
        assert_ne!(String::from_utf8_lossy(&current.stdout).trim(), context);
    }

    // launchd runs the daemon (RunAtLoad with --start); the CLI waits for Docker.
    let start = ok(&cli(&home, Some(&user_home), &["start"]), "start");
    assert!(
        start.contains("Docker ready") || start.contains("already running"),
        "{start}"
    );
    let s = status_json(&home);
    assert_eq!(s["daemon"]["launchd_label"], label.as_str(), "{s}");
    let first_pid = s["daemon"]["pid"].as_u64().unwrap();

    // KeepAlive: a crashed daemon comes back (launchd throttles respawns to 10 s).
    unsafe { libc::kill(first_pid as i32, libc::SIGABRT) };
    wait_until(
        "launchd to restart the daemon",
        Duration::from_secs(40),
        || {
            let out = cli(&home, None, &["status", "--json"]);
            serde_json::from_slice::<serde_json::Value>(&out.stdout)
                .ok()
                .and_then(|v| v["daemon"]["pid"].as_u64())
                .is_some_and(|p| p != first_pid)
        },
    );
    let restarted = ok(
        &cli(&home, Some(&user_home), &["start"]),
        "start after crash",
    );
    assert!(restarted.contains("Docker ready") || restarted.contains("already running"));

    let out = ok(&cli(&home, Some(&user_home), &["uninstall"]), "uninstall");
    eprintln!("{out}");
    assert!(!plist.exists());
    assert!(!loaded(&label));
    assert!(std::fs::symlink_metadata(&link).is_err());
    assert!(UnixStream::connect(home.join("run/daemon.sock")).is_err());
    if let Ok(inspect) = docker(&["context", "inspect", &context]) {
        assert!(!inspect.status.success());
    }
    drop(guard);
}
