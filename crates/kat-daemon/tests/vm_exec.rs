//! Root shell and command execution inside the Linux VM itself (`kat vm shell`
//! and `kat vm exec`). Ignored by default; `cargo xtask test-vm` builds
//! everything and runs it.

use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

fn root() -> PathBuf {
    // Canonical, so socket paths stay under SUN_LEN.
    std::fs::canonicalize(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")).unwrap()
}

fn katamaran() -> PathBuf {
    root().join("target/release/katamaran")
}

fn cli(home: &Path, args: &[&str]) -> Output {
    Command::new(katamaran())
        .args(args)
        .env("KATAMARAN_HOME", home)
        .stdin(Stdio::null())
        .output()
        .expect("run katamaran")
}

fn cli_with_stdin(home: &Path, args: &[&str], input: &[u8]) -> Output {
    let mut child = Command::new(katamaran())
        .args(args)
        .env("KATAMARAN_HOME", home)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn katamaran");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input)
        .expect("write stdin");
    child.wait_with_output().expect("wait for katamaran")
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
        thread::sleep(Duration::from_millis(100));
    }
}

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

#[test]
#[ignore = "boots the system VM; run with `cargo xtask test-vm`"]
fn vm_shell_and_exec_run_as_root_in_vm_namespaces() {
    let home = root().join(format!("target/tve-{}", std::process::id()));
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

    // Running `kat vm exec` while the VM is stopped auto-starts it cleanly
    // without polluting stdout.
    let uid = ok(&cli(&home, &["vm", "exec", "--", "id", "-u"]), "vm exec id");
    assert_eq!(uid.trim(), "0");

    // Runs in the VM's own namespaces (not an LXC machine): hostname is
    // katamaran, PID 1 is kat-init, containerd socket and sysctls are visible.
    assert_eq!(
        ok(&cli(&home, &["vm", "exec", "--", "hostname"]), "hostname").trim(),
        "katamaran"
    );
    assert_eq!(
        ok(
            &cli(&home, &["vm", "exec", "--", "cat", "/proc/1/comm"]),
            "pid 1 comm"
        )
        .trim(),
        "init"
    );
    ok(
        &cli(
            &home,
            &[
                "vm",
                "exec",
                "--",
                "test",
                "-S",
                "/run/containerd/containerd.sock",
            ],
        ),
        "containerd socket visible",
    );
    assert_eq!(
        ok(
            &cli(
                &home,
                &["vm", "exec", "--", "sysctl", "-n", "kernel.ostype"]
            ),
            "sysctl"
        )
        .trim(),
        "Linux"
    );
    let limits = ok(
        &cli(
            &home,
            &[
                "vm",
                "exec",
                "--",
                "grep",
                "Max open files",
                "/proc/1/limits",
            ],
        ),
        "rlimits",
    );
    assert!(limits.contains("Max open files"), "{limits}");

    // Stdout and stderr stay separate without a PTY, and exit codes propagate.
    let split = cli(
        &home,
        &[
            "vm",
            "exec",
            "--",
            "sh",
            "-c",
            "echo to-out; echo to-err >&2; exit 42",
        ],
    );
    assert_eq!(split.status.code(), Some(42));
    assert_eq!(String::from_utf8_lossy(&split.stdout), "to-out\n");
    assert_eq!(String::from_utf8_lossy(&split.stderr), "to-err\n");

    // Stdin piping works cleanly.
    let echoed = cli_with_stdin(&home, &["vm", "exec", "--", "cat"], b"hello from mac\n");
    assert_eq!(ok(&echoed, "stdin cat"), "hello from mac\n");

    // Workdir and env flags work; invalid workdir returns an error.
    assert_eq!(
        ok(
            &cli(&home, &["vm", "exec", "-w", "/var/log", "--", "pwd"]),
            "workdir"
        )
        .trim(),
        "/var/log"
    );
    let bad_dir = cli(&home, &["vm", "exec", "-w", "/no-such-dir", "--", "pwd"]);
    assert!(!bad_dir.status.success());
    assert!(
        String::from_utf8_lossy(&bad_dir.stderr).contains("workdir"),
        "{bad_dir:?}"
    );
    assert_eq!(
        ok(
            &cli(
                &home,
                &[
                    "vm",
                    "exec",
                    "-e",
                    "KAT_TEST=ok=1",
                    "--",
                    "sh",
                    "-c",
                    "printf %s \"$KAT_TEST\""
                ]
            ),
            "env"
        ),
        "ok=1"
    );

    // `-t` / `--tty` allocates a real PTY inside the guest.
    let pty = ok(
        &cli(
            &home,
            &[
                "vm",
                "exec",
                "-t",
                "--",
                "sh",
                "-c",
                "test -t 0 && test -t 1 && echo is-tty",
            ],
        ),
        "pty exec",
    );
    assert!(pty.contains("is-tty"), "{pty:?}");

    // `kat vm shell` with piped stdin runs `/bin/sh` as root in `/root`.
    let piped_shell = cli_with_stdin(&home, &["vm", "shell"], b"id -u\npwd\n");
    assert_eq!(ok(&piped_shell, "vm shell piped"), "0\n/root\n");

    // `kat vm shell -- <cmd>` also works.
    assert_eq!(
        ok(
            &cli(&home, &["vm", "shell", "--", "uname", "-s"]),
            "vm shell cmd"
        )
        .trim(),
        "Linux"
    );

    // Missing executable surfaces a clear error.
    let missing = cli(&home, &["vm", "exec", "--", "/no/such/binary"]);
    assert!(!missing.status.success());
    assert!(
        String::from_utf8_lossy(&missing.stderr).contains("/no/such/binary"),
        "{missing:?}"
    );
}
