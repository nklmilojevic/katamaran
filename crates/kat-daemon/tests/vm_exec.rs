//! Root shell and command execution inside the Linux VM itself (`kat vm shell`
//! and `kat vm exec`). Ignored by default; `cargo xtask test-vm` builds
//! everything and runs it.

use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

const BOOT_CMD_TIMEOUT: Duration = Duration::from_secs(180);
const CMD_TIMEOUT: Duration = Duration::from_secs(30);

fn root() -> PathBuf {
    // Canonical, so socket paths stay under SUN_LEN.
    std::fs::canonicalize(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")).unwrap()
}

fn katamaran() -> PathBuf {
    root().join("target/release/katamaran")
}

/// Ensures a spawned child is killed and reaped even if a test assertion panics.
struct ChildGuard(Option<Child>);

impl ChildGuard {
    fn new(child: Child) -> Self {
        Self(Some(child))
    }

    fn as_mut(&mut self) -> &mut Child {
        self.0.as_mut().expect("child already reaped")
    }

    fn try_wait(&mut self) -> io::Result<Option<std::process::ExitStatus>> {
        let Some(child) = self.0.as_mut() else {
            return Ok(None);
        };
        let status = child.try_wait()?;
        if status.is_some() {
            // Already reaped by `try_wait`.
            self.0 = None;
        }
        Ok(status)
    }

    fn kill_and_wait(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        self.kill_and_wait();
    }
}

/// Run `katamaran <args>` with a hard wall-clock `timeout`, concurrently
/// draining stdout and stderr (and feeding optional stdin on a worker thread).
/// Kills and reaps the child on timeout so [`Daemon::drop`] always runs.
fn run_cli_bounded(home: &Path, args: &[&str], input: Option<&[u8]>, timeout: Duration) -> Output {
    let mut cmd = Command::new(katamaran());
    cmd.args(args)
        .env("KATAMARAN_HOME", home)
        .stdin(if input.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = ChildGuard::new(cmd.spawn().expect("spawn katamaran"));

    let mut out_pipe = child.as_mut().stdout.take().unwrap();
    let mut err_pipe = child.as_mut().stderr.take().unwrap();
    let out_handle = thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = out_pipe.read_to_end(&mut buf);
        buf
    });
    let err_handle = thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = err_pipe.read_to_end(&mut buf);
        buf
    });

    let stdin_handle = input.map(|data| {
        let mut stdin = child.as_mut().stdin.take().unwrap();
        let owned = data.to_vec();
        thread::spawn(move || {
            let _ = stdin.write_all(&owned);
            drop(stdin);
        })
    });

    let deadline = Instant::now() + timeout;
    let status = loop {
        if let Some(status) = child.try_wait().expect("try_wait katamaran") {
            break status;
        }
        if Instant::now() >= deadline {
            child.kill_and_wait();
            let stdout = out_handle.join().unwrap_or_default();
            let stderr = err_handle.join().unwrap_or_default();
            if let Some(h) = stdin_handle {
                let _ = h.join();
            }
            panic!(
                "katamaran {args:?} timed out after {}s\nstdout:\n{}\nstderr:\n{}",
                timeout.as_secs(),
                String::from_utf8_lossy(&stdout),
                String::from_utf8_lossy(&stderr),
            );
        }
        thread::sleep(Duration::from_millis(25));
    };

    if let Some(h) = stdin_handle {
        let _ = h.join();
    }
    let stdout = out_handle.join().unwrap_or_default();
    let stderr = err_handle.join().unwrap_or_default();
    Output {
        status,
        stdout,
        stderr,
    }
}

fn cli(home: &Path, args: &[&str]) -> Output {
    run_cli_bounded(home, args, None, CMD_TIMEOUT)
}

fn cli_with_stdin(home: &Path, args: &[&str], input: &[u8]) -> Output {
    run_cli_bounded(home, args, Some(input), CMD_TIMEOUT)
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

/// Read from `fd` into `acc` until `acc` contains `needle` or `deadline` passes.
fn read_fd_until(fd: i32, acc: &mut String, needle: &str, deadline: Instant) {
    let mut buf = [0u8; 4096];
    while !acc.contains(needle) {
        let rem = deadline.saturating_duration_since(Instant::now());
        assert!(
            !rem.is_zero(),
            "timed out waiting for {needle:?}; output so far:\n{acc}"
        );
        let mut pfd = libc::pollfd {
            fd,
            events: libc::POLLIN,
            revents: 0,
        };
        let timeout_ms = rem.as_millis().clamp(1, 200) as i32;
        let rc = unsafe { libc::poll(&mut pfd, 1, timeout_ms) };
        if rc < 0 {
            if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                continue;
            }
            panic!("poll failed: {}", io::Error::last_os_error());
        }
        if rc == 0 {
            continue;
        }
        if (pfd.revents & libc::POLLIN) != 0 {
            let n = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), buf.len()) };
            if n > 0 {
                acc.push_str(&String::from_utf8_lossy(&buf[..n as usize]));
                continue;
            }
            if n == 0 {
                break;
            }
            let e = io::Error::last_os_error();
            if e.kind() != io::ErrorKind::Interrupted && e.kind() != io::ErrorKind::WouldBlock {
                break;
            }
        } else if (pfd.revents & (libc::POLLHUP | libc::POLLERR)) != 0 {
            break;
        }
    }
    assert!(
        acc.contains(needle),
        "stream ended before {needle:?}; output:\n{acc}"
    );
}

fn write_all_fd(fd: i32, mut data: &[u8]) {
    while !data.is_empty() {
        let n = unsafe { libc::write(fd, data.as_ptr().cast(), data.len()) };
        if n > 0 {
            data = &data[n as usize..];
        } else {
            let e = io::Error::last_os_error();
            assert_eq!(e.kind(), io::ErrorKind::Interrupted, "write fd failed: {e}");
        }
    }
}

/// Exercise `kat vm shell` attached to a real host PTY: verify automatic PTY
/// allocation, raw mode, live window resizing, clean exit, and host termios
/// restoration.
fn verify_host_pty_shell_and_resize(home: &Path) {
    let (mut master_raw, mut slave_raw) = (0, 0);
    let mut initial_ws = libc::winsize {
        ws_row: 24,
        ws_col: 80,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master_raw,
                &mut slave_raw,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut initial_ws,
            )
        },
        0,
        "openpty failed: {}",
        io::Error::last_os_error()
    );
    let master = unsafe { OwnedFd::from_raw_fd(master_raw) };
    let slave = unsafe { OwnedFd::from_raw_fd(slave_raw) };
    unsafe {
        libc::fcntl(master.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC);
        libc::fcntl(slave.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC);
    }

    let mut before: libc::termios = unsafe { std::mem::zeroed() };
    assert_eq!(
        unsafe { libc::tcgetattr(slave.as_raw_fd(), &mut before) },
        0
    );
    assert_ne!(before.c_lflag & libc::ICANON, 0);
    assert_ne!(before.c_lflag & libc::ECHO, 0);

    let mut cmd = Command::new(katamaran());
    cmd.args(["vm", "shell"])
        .env("KATAMARAN_HOME", home)
        .env("TERM", "xterm-256color")
        .stdin(Stdio::from(slave.try_clone().unwrap()))
        .stdout(Stdio::from(slave.try_clone().unwrap()))
        .stderr(Stdio::from(slave.try_clone().unwrap()));
    unsafe {
        cmd.pre_exec(|| {
            if libc::setsid() < 0 || libc::ioctl(0, libc::TIOCSCTTY.into(), 0) < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = ChildGuard::new(cmd.spawn().expect("spawn kat vm shell on host pty"));

    let deadline = Instant::now() + CMD_TIMEOUT;
    let mut output = String::new();

    // 1. Confirm auto-PTY allocation inside the guest. Split the marker across
    // printf arguments so terminal echo of the command line cannot satisfy the
    // check before the guest actually runs `test -t 0 && test -t 1`.
    write_all_fd(
        master.as_raw_fd(),
        b"test -t 0 && test -t 1 && printf '%s%s\\n' HOST_PTY _OK\n",
    );
    read_fd_until(master.as_raw_fd(), &mut output, "HOST_PTY_OK", deadline);

    // 2. Confirm host terminal entered raw mode while interactive shell runs.
    let mut during: libc::termios = unsafe { std::mem::zeroed() };
    assert_eq!(
        unsafe { libc::tcgetattr(slave.as_raw_fd(), &mut during) },
        0
    );
    assert_eq!(
        during.c_lflag & libc::ICANON,
        0,
        "expected raw mode on host PTY"
    );

    // 3. Resize the host PTY and confirm the guest observes the new dimensions.
    let resized_ws = libc::winsize {
        ws_row: 42,
        ws_col: 132,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    assert_eq!(
        unsafe { libc::ioctl(master.as_raw_fd(), libc::TIOCSWINSZ, &resized_ws) },
        0
    );
    // Allow the 200ms resize watcher to deliver Resize, then poll `stty size`.
    let resize_deadline = (Instant::now() + Duration::from_secs(5)).min(deadline);
    loop {
        thread::sleep(Duration::from_millis(300));
        write_all_fd(master.as_raw_fd(), b"stty size\n");
        let step_deadline = (Instant::now() + Duration::from_secs(2)).min(resize_deadline);
        let mut buf = [0u8; 4096];
        while !output.contains("42 132") && Instant::now() < step_deadline {
            let mut pfd = libc::pollfd {
                fd: master.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            let rc = unsafe { libc::poll(&mut pfd, 1, 100) };
            if rc > 0 && (pfd.revents & libc::POLLIN) != 0 {
                let n =
                    unsafe { libc::read(master.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
                if n > 0 {
                    output.push_str(&String::from_utf8_lossy(&buf[..n as usize]));
                }
            }
        }
        if output.contains("42 132") {
            break;
        }
        assert!(
            Instant::now() < resize_deadline,
            "guest never observed resized PTY 42x132; output:\n{output}"
        );
    }

    // 4. Exit cleanly and confirm host termios was restored.
    write_all_fd(master.as_raw_fd(), b"exit 0\n");
    let status = loop {
        if let Some(s) = child.try_wait().unwrap() {
            break s;
        }
        assert!(
            Instant::now() < deadline,
            "kat vm shell did not exit cleanly; output:\n{output}"
        );
        thread::sleep(Duration::from_millis(25));
    };
    assert!(status.success(), "kat vm shell exited with {status}");

    let mut after: libc::termios = unsafe { std::mem::zeroed() };
    assert_eq!(unsafe { libc::tcgetattr(slave.as_raw_fd(), &mut after) }, 0);
    // macOS sets the transient kernel state bit PENDIN when restoring canonical mode.
    assert_eq!(
        after.c_lflag & !libc::PENDIN,
        before.c_lflag & !libc::PENDIN,
        "host c_lflag not restored"
    );
    assert_eq!(after.c_iflag, before.c_iflag, "host c_iflag not restored");
    assert_eq!(after.c_oflag, before.c_oflag, "host c_oflag not restored");
    assert_eq!(after.c_cflag, before.c_cflag, "host c_cflag not restored");
}

/// Exercise non-PTY `SIGINT` forwarding: the guest traps `INT` and `HUP`
/// differently and must receive `SIGINT`, not a disconnect-induced `SIGHUP`.
fn verify_non_pty_sigint_forwarding(home: &Path) {
    let mut cmd = Command::new(katamaran());
    cmd.args([
        "vm",
        "exec",
        "--",
        "sh",
        "-c",
        r#"trap 'echo CAUGHT_INT; exit 77' INT; trap 'echo CAUGHT_HUP; exit 88' HUP; echo READY; while true; do sleep 1; done"#,
    ])
    .env("KATAMARAN_HOME", home)
    .stdin(Stdio::null())
    .stdout(Stdio::piped())
    .stderr(Stdio::piped());
    let mut child = ChildGuard::new(cmd.spawn().expect("spawn kat vm exec for sigint"));

    let mut stdout_pipe = child.as_mut().stdout.take().unwrap();
    let mut stderr_pipe = child.as_mut().stderr.take().unwrap();
    let (ready_tx, ready_rx) = mpsc::channel();
    let out_handle = thread::spawn(move || {
        let mut all = Vec::new();
        let mut buf = [0u8; 1024];
        let mut signaled_ready = false;
        loop {
            match stdout_pipe.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    all.extend_from_slice(&buf[..n]);
                    if !signaled_ready && String::from_utf8_lossy(&all).contains("READY") {
                        signaled_ready = true;
                        let _ = ready_tx.send(());
                    }
                }
            }
        }
        all
    });
    let err_handle = thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = stderr_pipe.read_to_end(&mut buf);
        buf
    });

    ready_rx
        .recv_timeout(CMD_TIMEOUT)
        .expect("timed out waiting for guest trap setup");
    unsafe { libc::kill(child.as_mut().id() as i32, libc::SIGINT) };

    let deadline = Instant::now() + CMD_TIMEOUT;
    let status = loop {
        if let Some(s) = child.try_wait().unwrap() {
            break s;
        }
        assert!(
            Instant::now() < deadline,
            "kat vm exec did not exit after SIGINT"
        );
        thread::sleep(Duration::from_millis(25));
    };
    let stdout = String::from_utf8_lossy(&out_handle.join().unwrap_or_default()).into_owned();
    let stderr = String::from_utf8_lossy(&err_handle.join().unwrap_or_default()).into_owned();
    assert_eq!(
        status.code(),
        Some(77),
        "expected INT trap exit 77, got {status}\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(stdout.contains("CAUGHT_INT"), "stdout:\n{stdout}");
    assert!(!stdout.contains("CAUGHT_HUP"), "stdout:\n{stdout}");
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
    let uid = ok(
        &run_cli_bounded(
            &home,
            &["vm", "exec", "--", "id", "-u"],
            None,
            BOOT_CMD_TIMEOUT,
        ),
        "vm exec id",
    );
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
    // Containerd socket may take a moment after vm-exec is already serving.
    wait_until(
        "containerd socket inside VM",
        Duration::from_secs(30),
        || {
            cli(
                &home,
                &[
                    "vm",
                    "exec",
                    "--",
                    "test",
                    "-S",
                    "/run/containerd/containerd.sock",
                ],
            )
            .status
            .success()
        },
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

    // Canonical PTY EOF works after piped input without a trailing newline.
    let pty_partial = ok(
        &cli_with_stdin(&home, &["vm", "exec", "-t", "--", "cat"], b"partial-no-nl"),
        "pty partial line eof",
    );
    assert!(pty_partial.contains("partial-no-nl"), "{pty_partial:?}");

    // Host terminal auto-allocation, resize propagation, and termios restoration.
    verify_host_pty_shell_and_resize(&home);

    // Non-PTY SIGINT forwards INT rather than triggering a disconnect HUP.
    verify_non_pty_sigint_forwarding(&home);

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

/// Spawn the real `katamaran vm exec` binary against a mock `vm-exec.sock`
/// inheriting shared blocking output and input pipes, terminate it with
/// `SIGTERM`, and verify the parent's duplicate pipe descriptors still have
/// `O_NONBLOCK` clear afterward.
#[test]
fn non_pty_sigterm_restores_shared_pipe_flags_in_subprocess() {
    use kat_proto::vm_exec::{self, VmExecMsg};
    use std::os::unix::net::UnixListener;
    use std::os::unix::process::ExitStatusExt;

    let dir = PathBuf::from(format!("/tmp/kve-sigterm-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let run_dir = dir.join("run");
    std::fs::create_dir_all(&run_dir).unwrap();
    let sock = run_dir.join("vm-exec.sock");
    let listener = UnixListener::bind(&sock).unwrap();

    let (stop_srv_tx, stop_srv_rx) = mpsc::channel::<()>();
    let server = thread::spawn(move || {
        let (mut conn, _) = listener.accept().unwrap();
        assert_eq!(vm_exec::read(&mut conn).unwrap(), Some(VmExecMsg::Ping));
        vm_exec::write(&mut conn, &VmExecMsg::Pong).unwrap();
        assert!(matches!(
            vm_exec::read(&mut conn).unwrap(),
            Some(VmExecMsg::Exec(_))
        ));
        vm_exec::write(&mut conn, &VmExecMsg::Started).unwrap();
        vm_exec::write(&mut conn, &VmExecMsg::Stdout(b"READY\n".to_vec())).unwrap();
        let _ = stop_srv_rx.recv_timeout(Duration::from_secs(5));
    });

    let mk_pipe = || -> (OwnedFd, OwnedFd) {
        let mut fds = [0; 2];
        assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
        unsafe {
            let r = OwnedFd::from_raw_fd(fds[0]);
            let w = OwnedFd::from_raw_fd(fds[1]);
            libc::fcntl(r.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC);
            libc::fcntl(w.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC);
            (r, w)
        }
    };

    let (in_r, in_w) = mk_pipe();
    let (out_r, out_w) = mk_pipe();
    let (err_r, err_w) = mk_pipe();
    let in_r_child = in_r.try_clone().unwrap();
    let out_w_child = out_w.try_clone().unwrap();
    let err_w_child = err_w.try_clone().unwrap();

    assert_eq!(
        unsafe { libc::fcntl(out_w.as_raw_fd(), libc::F_GETFL) } & libc::O_NONBLOCK,
        0
    );
    assert_eq!(
        unsafe { libc::fcntl(in_r.as_raw_fd(), libc::F_GETFL) } & libc::O_NONBLOCK,
        0
    );

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_katamaran"));
    cmd.args(["vm", "exec", "--", "sleep", "60"])
        .env("KATAMARAN_HOME", &dir)
        .stdin(Stdio::from(in_r_child))
        .stdout(Stdio::from(out_w_child))
        .stderr(Stdio::from(err_w_child));
    let mut child = ChildGuard::new(cmd.spawn().expect("spawn katamaran vm exec"));

    // Wait for `READY\n` so we know run_session_io has entered its relay loop
    // and enabled O_NONBLOCK on the inherited output descriptors.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut out = String::new();
    read_fd_until(out_r.as_raw_fd(), &mut out, "READY", deadline);

    // Inherited stdin MUST remain blocking while the CLI is actively relaying
    // so other readers sharing the pipe open file description never see EAGAIN.
    assert_eq!(
        unsafe { libc::fcntl(in_r.as_raw_fd(), libc::F_GETFL) } & libc::O_NONBLOCK,
        0,
        "shared stdin pipe was switched to O_NONBLOCK during active session"
    );

    let pid = child.as_mut().id() as libc::pid_t;
    assert_eq!(unsafe { libc::kill(pid, libc::SIGTERM) }, 0);

    let status = loop {
        if let Some(s) = child.try_wait().unwrap() {
            break s;
        }
        assert!(
            Instant::now() < deadline,
            "katamaran vm exec did not exit promptly after SIGTERM"
        );
        thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(
        status.signal(),
        Some(libc::SIGTERM),
        "expected SIGTERM termination, got {status}"
    );
    assert_eq!(
        unsafe { libc::fcntl(out_w.as_raw_fd(), libc::F_GETFL) } & libc::O_NONBLOCK,
        0,
        "shared stdout pipe was left nonblocking after SIGTERM"
    );
    assert_eq!(
        unsafe { libc::fcntl(err_w.as_raw_fd(), libc::F_GETFL) } & libc::O_NONBLOCK,
        0,
        "shared stderr pipe was left nonblocking after SIGTERM"
    );
    assert_eq!(
        unsafe { libc::fcntl(in_r.as_raw_fd(), libc::F_GETFL) } & libc::O_NONBLOCK,
        0,
        "shared stdin pipe was left nonblocking after SIGTERM"
    );

    let _ = stop_srv_tx.send(());
    server.join().unwrap();
    drop(in_w);
    drop(in_r);
    drop(out_r);
    drop(out_w);
    drop(err_r);
    drop(err_w);
    let _ = std::fs::remove_dir_all(&dir);
}
