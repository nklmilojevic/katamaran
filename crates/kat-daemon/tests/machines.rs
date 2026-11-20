//! Linux machines through the CLI and the daemon: create an Alpine machine,
//! shell into it as the Mac user (same uid, passwordless sudo), see /Users
//! and the machine's files under the Mac mount, stop, autostart across a VM
//! restart, delete. Prints the create time and the machine's idle memory.
//! Ignored by default; `cargo xtask test-vm` builds everything and runs it.
//! Needs the internet (images.linuxcontainers.org).

use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

fn root() -> PathBuf {
    // Canonical, so socket paths stay under SUN_LEN.
    std::fs::canonicalize(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")).unwrap()
}

fn cli_in(home: &Path, dir: &Path, args: &[&str]) -> Output {
    Command::new(root().join("target/release/katamaran"))
        .args(args)
        .current_dir(dir)
        .env("KATAMARAN_HOME", home)
        .stdin(Stdio::null())
        .output()
        .expect("run katamaran")
}

fn cli(home: &Path, args: &[&str]) -> Output {
    cli_in(home, &root(), args)
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

fn sh(home: &Path, machine: &str, script: &str) -> String {
    let out = cli(home, &["shell", machine, "--", "sh", "-c", script]);
    ok(&out, script).trim().to_string()
}

fn machine_json(home: &Path, name: &str) -> serde_json::Value {
    serde_json::from_str(&ok(
        &cli(home, &["machine", "info", name, "--json"]),
        "machine info",
    ))
    .unwrap()
}

fn wait_until(what: &str, timeout: Duration, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(200));
    }
}

/// The daemon this test spawned: SIGTERM stops the VM, then the daemon.
struct Daemon {
    child: Child,
    home: PathBuf,
}

impl Daemon {
    fn spawn(home: &Path) -> Daemon {
        let child = Command::new(root().join("target/release/katamaran"))
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
        wait_until("the daemon socket", Duration::from_secs(10), || {
            UnixStream::connect(home.join("run/daemon.sock")).is_ok()
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

fn mac_user() -> String {
    let out = Command::new("id").arg("-un").output().unwrap();
    String::from_utf8_lossy(&out.stdout).trim().to_lowercase()
}

#[test]
#[ignore = "boots the system VM and downloads an image; run with `cargo xtask test-vm`"]
fn machine_lifecycle_user_and_files() {
    let home = root().join(format!("target/tmh-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&home);
    std::fs::create_dir_all(&home).unwrap();
    let files = home.join("files");
    ok(
        &cli(
            &home,
            &[
                "config",
                "set",
                "cpus=2",
                "memory=2G",
                &format!("mount_path={}", files.display()),
            ],
        ),
        "config set",
    );
    let _daemon = Daemon::spawn(&home);
    ok(&cli(&home, &["start"]), "start");

    // Create: download (or cache), unpack, user setup, boot, sudo install.
    let t = Instant::now();
    ok(
        &cli(&home, &["machine", "create", "t1", "--distro", "alpine"]),
        "machine create",
    );
    println!("create: {:.1} s", t.elapsed().as_secs_f64());
    let m = machine_json(&home, "t1");
    assert_eq!(m["state"], "running", "{m}");
    assert_eq!(m["distro"], "alpine", "{m}");
    assert_eq!(m["default"], true, "{m}");
    assert!(
        m["ip"].as_str().unwrap_or("").starts_with("198.19.249."),
        "{m}"
    );
    let list = ok(&cli(&home, &["machine", "list"]), "machine list");
    assert!(list.contains("t1 *"), "{list}");

    // The Mac user, same uid, own group, home, passwordless sudo.
    let uid = unsafe { libc::getuid() };
    let user = mac_user();
    assert_eq!(sh(&home, "t1", "id -u"), uid.to_string());
    assert_eq!(sh(&home, "t1", "id -un"), user);
    assert_eq!(sh(&home, "t1", "echo $HOME"), format!("/home/{user}"));
    assert_eq!(sh(&home, "t1", "sudo -n id -u"), "0");
    assert_eq!(
        ok(
            &cli(&home, &["shell", "-u", "root", "t1", "--", "id", "-u"]),
            "root"
        ),
        "0\n"
    );
    // The default machine when none is named, and the exit code comes back.
    assert_eq!(sh(&home, "", "hostname"), "t1");
    let out = cli(&home, &["shell", "--", "sh", "-c", "exit 7"]);
    assert_eq!(out.status.code(), Some(7));

    // /Users at the same path, owned by the user; the shell starts in the
    // Mac's working directory.
    let probe = home.join("probe.txt");
    std::fs::write(&probe, "from the mac\n").unwrap();
    assert_eq!(
        sh(&home, "t1", &format!("cat '{}'", probe.display())),
        "from the mac"
    );
    assert_eq!(
        sh(&home, "t1", &format!("stat -c %u '{}'", probe.display())),
        uid.to_string()
    );
    let pwd = cli_in(&home, &home, &["shell", "t1", "--", "pwd"]);
    assert_eq!(ok(&pwd, "pwd").trim(), home.to_str().unwrap());
    sh(
        &home,
        "t1",
        &format!("echo back > '{}/from-linux'", home.display()),
    );
    assert_eq!(
        std::fs::read_to_string(home.join("from-linux")).unwrap(),
        "back\n"
    );

    // The machine's files on the Mac, both ways.
    let mfiles = files.join("machines/t1");
    wait_until("the files mount", Duration::from_secs(30), || {
        mfiles.join("etc/alpine-release").is_file()
    });
    sh(&home, "t1", "echo hi > ~/note");
    let note = mfiles.join(format!("home/{user}/note"));
    wait_until("the note on the Mac", Duration::from_secs(10), || {
        std::fs::read_to_string(&note).is_ok_and(|s| s == "hi\n")
    });

    // Internet through the VM.
    sh(
        &home,
        "t1",
        "wget -q -O /dev/null https://images.linuxcontainers.org/meta/1.0/index-system",
    );

    // Idle overhead: the machine's own cgroup.
    let mem = sh(&home, "t1", "cat /sys/fs/cgroup/memory.current");
    println!(
        "idle machine memory: {:.1} MiB",
        mem.parse::<f64>().unwrap() / (1 << 20) as f64
    );

    // Stop, start, then a VM restart brings a running machine back.
    ok(&cli(&home, &["machine", "stop", "t1"]), "machine stop");
    assert_eq!(machine_json(&home, "t1")["state"], "stopped");
    ok(&cli(&home, &["machine", "start", "t1"]), "machine start");
    assert_eq!(machine_json(&home, "t1")["state"], "running");
    ok(&cli(&home, &["restart"]), "restart");
    wait_until(
        "autostart after a VM restart",
        Duration::from_secs(60),
        || machine_json(&home, "t1")["state"] == "running",
    );
    assert_eq!(sh(&home, "t1", "cat ~/note"), "hi");

    // A stopped machine stays stopped across a restart.
    ok(&cli(&home, &["machine", "stop", "t1"]), "machine stop");
    ok(&cli(&home, &["restart"]), "restart");
    thread::sleep(Duration::from_secs(3));
    assert_eq!(machine_json(&home, "t1")["state"], "stopped");

    ok(&cli(&home, &["machine", "delete", "t1"]), "machine delete");
    let list = ok(&cli(&home, &["machine", "list"]), "machine list");
    assert!(list.contains("no machines"), "{list}");
    assert!(!mfiles.exists());
    let gone = cli(&home, &["shell", "t1", "--", "true"]);
    assert!(!gone.status.success());
}

#[test]
#[ignore = "boots the system VM and downloads an image; run with `cargo xtask test-vm`"]
fn machine_over_ssh() {
    let home = root().join(format!("target/tms-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&home);
    std::fs::create_dir_all(&home).unwrap();
    ok(
        &cli(
            &home,
            &["config", "set", "cpus=2", "memory=2G", "mount=off"],
        ),
        "config set",
    );
    let _daemon = Daemon::spawn(&home);
    ok(&cli(&home, &["start"]), "start");
    ok(
        &cli(&home, &["machine", "create", "s1", "--distro", "alpine"]),
        "machine create",
    );
    // `katamaran ssh` runs the system ssh with the katamaran config, whose
    // ProxyCommand is the SSH server.
    assert_eq!(
        ok(&cli(&home, &["ssh", "s1", "--", "id", "-un"]), "ssh").trim(),
        mac_user()
    );
    assert_eq!(
        ok(
            &cli(&home, &["ssh", "root@s1", "--", "id", "-u"]),
            "ssh root"
        )
        .trim(),
        "0"
    );
    let out = cli(&home, &["ssh", "s1", "--", "exit 3"]);
    assert_eq!(out.status.code(), Some(3));
}
