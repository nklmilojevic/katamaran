//! VM root shell and exec service, served to the host on
//! [`kat_proto::VM_EXEC_PORT`] (see [`kat_proto::vm_exec`]).
//!
//! Spawns root shells and arbitrary commands directly in PID 1's own
//! namespaces so developers can inspect containerd, dockerd, k3s, sysctls,
//! rlimits, and guest logs without needing Kubernetes or an LXC machine.
//! Idle overhead is a single vsock listener thread.

use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::net::Shutdown;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use kat_proto::VM_EXEC_PORT;
use kat_proto::vm_exec::{self, CHUNK, ExecSpec, Tty, VmExecMsg};

use super::machines::cmd;
use super::service::{ChildTarget, Reaper};
use super::sys;

const DEFAULT_SHELL: &str = "/bin/sh";
const DEFAULT_HOME: &str = "/root";
const VM_PATH: &str =
    "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/var/lib/katamaran/k3s/bin";
const DEFAULT_PS1: &str = r"\h:\w\$ ";
const MAX_CONNS: usize = 64;
const ACCEPT_ERR_BACKOFF: Duration = Duration::from_millis(50);
const MAX_ERROR_BYTES: usize = 64 << 10;

struct ConnGuard(Arc<AtomicUsize>);

impl ConnGuard {
    fn try_acquire(active: &Arc<AtomicUsize>, limit: usize) -> Option<Self> {
        let mut cur = active.load(Ordering::Acquire);
        loop {
            if cur >= limit {
                return None;
            }
            match active.compare_exchange_weak(cur, cur + 1, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return Some(ConnGuard(Arc::clone(active))),
                Err(next) => cur = next,
            }
        }
    }
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Release);
    }
}

struct ExecWriter {
    stream: UnixStream,
    closed: bool,
}

type Writer = Arc<Mutex<ExecWriter>>;

fn send(w: &Writer, m: &VmExecMsg) -> io::Result<()> {
    let mut guard = w.lock().unwrap();
    if guard.closed {
        return Err(io::Error::new(io::ErrorKind::BrokenPipe, "exec closed"));
    }
    vm_exec::write(&mut guard.stream, m)
}

fn send_final(w: &Writer, m: &VmExecMsg) -> io::Result<()> {
    let mut guard = w.lock().unwrap();
    if guard.closed {
        return Ok(());
    }
    guard.closed = true;
    vm_exec::write(&mut guard.stream, m)
}

enum ChildHandle {
    /// PTY session leader spawned with `setsid` and `pidfd`.
    Pty(ChildTarget),
    /// Non-PTY process group leader spawned with `setpgid(0, 0)`.
    Pipe(i32),
}

impl ChildHandle {
    fn signal(&self, reaper: &Reaper, sig: i32) {
        match self {
            ChildHandle::Pty(t) => reaper.signal_target(t, sig),
            ChildHandle::Pipe(pid) => reaper.signal_pgrp(*pid, sig),
        }
    }
}

fn signal_handle(reaper: &Reaper, handle: &Mutex<Option<ChildHandle>>, sig: i32) {
    let guard = handle.lock().unwrap();
    if let Some(h) = guard.as_ref() {
        h.signal(reaper, sig);
    }
}

fn valid_signal(sig: i32) -> bool {
    (1..64).contains(&sig)
}

fn truncate_error(s: &str) -> String {
    if s.len() <= MAX_ERROR_BYTES {
        return s.to_string();
    }
    let suffix = "...";
    let mut end = MAX_ERROR_BYTES.saturating_sub(suffix.len());
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    let mut out = String::with_capacity(end + suffix.len());
    out.push_str(&s[..end]);
    out.push_str(suffix);
    out
}

fn winsize(rows: u16, cols: u16) -> libc::winsize {
    libc::winsize {
        ws_row: rows,
        ws_col: cols,
        ws_xpixel: 0,
        ws_ypixel: 0,
    }
}

fn openpty(t: &Tty) -> io::Result<(OwnedFd, OwnedFd)> {
    let (mut master, mut slave) = (0, 0);
    let ws = winsize(t.rows.max(1), t.cols.max(1));
    let rc = unsafe {
        libc::openpty(
            &mut master,
            &mut slave,
            std::ptr::null_mut(),
            std::ptr::null(),
            &ws,
        )
    };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    unsafe {
        libc::fcntl(master, libc::F_SETFD, libc::FD_CLOEXEC);
        libc::fcntl(slave, libc::F_SETFD, libc::FD_CLOEXEC);
        Ok((OwnedFd::from_raw_fd(master), OwnedFd::from_raw_fd(slave)))
    }
}

fn pump(mut from: File, w: Writer, kind: fn(Vec<u8>) -> VmExecMsg) {
    let mut buf = vec![0u8; CHUNK];
    loop {
        match from.read(&mut buf) {
            Ok(0) | Err(_) => return,
            Ok(n) => {
                if send(&w, &kind(buf[..n].to_vec())).is_err() {
                    return;
                }
            }
        }
    }
}

fn log(msg: &str) {
    eprintln!("kat-init: vm-exec: {msg}");
}

/// Start listening on [`VM_EXEC_PORT`] for host VM shell/exec sessions.
pub fn start(reaper: Reaper) -> io::Result<()> {
    let listener = sys::vsock_listen(VM_EXEC_PORT)?;
    let active = Arc::new(AtomicUsize::new(0));
    thread::Builder::new()
        .name("vm-exec".into())
        .spawn(move || {
            loop {
                match sys::accept(&listener) {
                    Ok(fd) => {
                        let Some(guard) = ConnGuard::try_acquire(&active, MAX_CONNS) else {
                            log(&format!(
                                "connection limit ({MAX_CONNS}) reached on vsock {VM_EXEC_PORT}"
                            ));
                            continue;
                        };
                        let reaper = reaper.clone();
                        if let Err(e) =
                            thread::Builder::new()
                                .name("vm-exec-conn".into())
                                .spawn(move || {
                                    let _guard = guard;
                                    serve(&reaper, UnixStream::from(fd));
                                })
                        {
                            log(&format!("spawn vm-exec-conn: {e}"));
                        }
                    }
                    Err(e) => {
                        log(&format!("accept on vsock {VM_EXEC_PORT}: {e}"));
                        thread::sleep(ACCEPT_ERR_BACKOFF);
                    }
                }
            }
        })?;
    Ok(())
}

fn serve(reaper: &Reaper, mut conn: UnixStream) {
    let req = match vm_exec::read(&mut conn) {
        Ok(Some(m)) => m,
        Ok(None) => return,
        Err(e) => {
            let _ = vm_exec::write(&mut conn, &VmExecMsg::Error(truncate_error(&e.to_string())));
            return;
        }
    };
    let spec = match req {
        VmExecMsg::Exec(s) => s,
        other => {
            let _ = vm_exec::write(
                &mut conn,
                &VmExecMsg::Error(truncate_error(&format!("unexpected request {other:?}"))),
            );
            return;
        }
    };
    if let Err(e) = exec(reaper, conn.try_clone(), &spec) {
        let _ = vm_exec::write(&mut conn, &VmExecMsg::Error(truncate_error(&e)));
    }
}

fn resolve_workdir(cwd: &str) -> Result<PathBuf, String> {
    if !vm_exec::value_ok(cwd) {
        return Err("workdir contains NUL byte".into());
    }
    if cwd.is_empty() {
        return Ok(if Path::new(DEFAULT_HOME).is_dir() {
            PathBuf::from(DEFAULT_HOME)
        } else {
            PathBuf::from("/")
        });
    }
    match fs::metadata(cwd) {
        Ok(m) if m.is_dir() => Ok(PathBuf::from(cwd)),
        Ok(_) => Err(format!("workdir {cwd:?}: not a directory")),
        Err(e) => Err(format!("workdir {cwd:?}: {e}")),
    }
}

fn build_command(spec: &ExecSpec) -> Result<(Command, String), String> {
    let cwd = resolve_workdir(&spec.cwd)?;
    for (k, v) in &spec.env {
        if !vm_exec::env_name_ok(k) {
            return Err(format!("invalid environment variable name {k:?}"));
        }
        if !vm_exec::value_ok(v) {
            return Err(format!("invalid environment value for {k:?}"));
        }
    }
    if let Some(t) = &spec.tty
        && !vm_exec::value_ok(&t.term)
    {
        return Err("TERM contains NUL byte".into());
    }

    let (program, args, is_shell): (&str, &[String], bool) = if spec.argv.is_empty() {
        (DEFAULT_SHELL, &[], true)
    } else {
        if spec.argv[0].is_empty() {
            return Err("command cannot be empty".into());
        }
        for a in &spec.argv {
            if !vm_exec::value_ok(a) {
                return Err("command argument contains NUL byte".into());
            }
        }
        (&spec.argv[0], &spec.argv[1..], false)
    };

    let mut c = Command::new(program);
    c.args(args)
        .current_dir(&cwd)
        .env_clear()
        .env("PATH", VM_PATH)
        .env("HOME", DEFAULT_HOME)
        .env("USER", "root")
        .env("LOGNAME", "root")
        .env("SHELL", DEFAULT_SHELL)
        .env("CHARSET", "UTF-8");
    if is_shell {
        c.env("PS1", DEFAULT_PS1);
    }
    if let Some(term) = spec
        .tty
        .as_ref()
        .map(|t| t.term.as_str())
        .filter(|t| !t.is_empty())
    {
        c.env("TERM", term);
    }
    for (k, v) in &spec.env {
        c.env(k, v);
    }
    Ok((c, program.to_string()))
}

fn exit_code(status: i32) -> i32 {
    if libc::WIFEXITED(status) {
        libc::WEXITSTATUS(status)
    } else if libc::WIFSIGNALED(status) {
        128 + libc::WTERMSIG(status)
    } else {
        1
    }
}

/// Spawn and relay `spec`. Returns `Err` only before `Started` has been sent
/// (so the caller can reply with a single `Error` frame); once `Started` has
/// been sent, all terminal frames (`Exit` or `Error`) are sent through `w`.
fn exec(reaper: &Reaper, conn: io::Result<UnixStream>, spec: &ExecSpec) -> Result<(), String> {
    let conn = conn.map_err(|e| format!("clone stream: {e}"))?;
    let (mut c, program) = build_command(spec)?;

    let w: Writer = Arc::new(Mutex::new(ExecWriter {
        stream: conn.try_clone().map_err(|e| format!("clone stream: {e}"))?,
        closed: false,
    }));
    let mut reader = conn;

    let (handle, exited, stdin, master, out_fds) = match &spec.tty {
        Some(t) => {
            let (master, slave) = openpty(t).map_err(|e| format!("openpty: {e}"))?;
            c.stdin(Stdio::from(
                slave.try_clone().map_err(|e| format!("pty stdin: {e}"))?,
            ))
            .stdout(Stdio::from(
                slave.try_clone().map_err(|e| format!("pty stdout: {e}"))?,
            ))
            .stderr(Stdio::from(slave));
            // SAFETY: setsid and ioctl(TIOCSCTTY) are async-signal-safe.
            unsafe {
                c.pre_exec(|| {
                    if libc::setsid() < 0 || libc::ioctl(0, libc::TIOCSCTTY, 0) < 0 {
                        return Err(io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
            let (target, exited) = reaper
                .spawn_pidfd(&mut c)
                .map_err(|e| format!("exec {program:?}: {e}"))?;
            drop(c);
            let out = File::from(
                master
                    .try_clone()
                    .map_err(|e| format!("pty master dup: {e}"))?,
            );
            let stdin = Some(File::from(
                master
                    .try_clone()
                    .map_err(|e| format!("pty master dup: {e}"))?,
            ));
            (
                ChildHandle::Pty(target),
                exited,
                stdin,
                Some(master),
                vec![(out, VmExecMsg::Stdout as fn(Vec<u8>) -> VmExecMsg)],
            )
        }
        None => {
            let (in_r, in_w) = cmd::pipe().map_err(|e| format!("pipe: {e}"))?;
            let (out_r, out_w) = cmd::pipe().map_err(|e| format!("pipe: {e}"))?;
            let (err_r, err_w) = cmd::pipe().map_err(|e| format!("pipe: {e}"))?;
            c.stdin(Stdio::from(in_r))
                .stdout(Stdio::from(out_w))
                .stderr(Stdio::from(err_w));
            // Put non-PTY commands in their own process group so host
            // disconnect or signals reach descendant processes too.
            // SAFETY: setpgid is async-signal-safe.
            unsafe {
                c.pre_exec(|| {
                    if libc::setpgid(0, 0) < 0 {
                        return Err(io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
            let (pid, exited) = reaper
                .spawn(&mut c)
                .map_err(|e| format!("exec {program:?}: {e}"))?;
            drop(c);
            (
                ChildHandle::Pipe(pid),
                exited,
                Some(File::from(in_w)),
                None,
                vec![
                    (
                        File::from(out_r),
                        VmExecMsg::Stdout as fn(Vec<u8>) -> VmExecMsg,
                    ),
                    (File::from(err_r), VmExecMsg::Stderr),
                ],
            )
        }
    };

    let handle = Arc::new(Mutex::new(Some(handle)));

    if send(&w, &VmExecMsg::Started).is_err() {
        signal_handle(reaper, &handle, libc::SIGKILL);
        *handle.lock().unwrap() = None;
        let _ = exited.recv_timeout(Duration::from_secs(2));
        return Ok(());
    }

    let tty = spec.tty.is_some();
    let handle_in = handle.clone();
    let reaper_in = reaper.clone();
    let stdin_thread = thread::Builder::new()
        .name("vm-exec-in".into())
        .spawn(move || {
            let mut stdin = stdin;
            let master = master;
            while let Ok(Some(m)) = vm_exec::read(&mut reader) {
                match m {
                    VmExecMsg::Data(d) => {
                        if let Some(s) = stdin.as_mut()
                            && s.write_all(&d).is_err()
                        {
                            stdin = None;
                        }
                    }
                    // A pty has no EOF of its own; the line discipline's ^D is
                    // the closest equivalent.
                    VmExecMsg::DataEnd if tty => {
                        if let Some(s) = stdin.as_mut() {
                            let _ = s.write_all(&[4]);
                        }
                    }
                    VmExecMsg::DataEnd => stdin = None,
                    VmExecMsg::Resize { rows, cols } => {
                        if let Some(m) = &master {
                            let ws = winsize(rows.max(1), cols.max(1));
                            unsafe { libc::ioctl(m.as_raw_fd(), libc::TIOCSWINSZ, &ws) };
                        }
                    }
                    VmExecMsg::Signal { signal } if valid_signal(signal) => {
                        signal_handle(&reaper_in, &handle_in, signal);
                    }
                    _ => {}
                }
            }
            drop(stdin);
            drop(master);
            // The host went away: hang up the session or process group unless
            // the child has already been reaped.
            signal_handle(&reaper_in, &handle_in, libc::SIGHUP);
        });
    if let Err(e) = stdin_thread {
        signal_handle(reaper, &handle, libc::SIGKILL);
        *handle.lock().unwrap() = None;
        let _ = send_final(
            &w,
            &VmExecMsg::Error(truncate_error(&format!("spawn stdin thread: {e}"))),
        );
        let _ = exited.recv_timeout(Duration::from_secs(2));
        return Ok(());
    }

    let mut outputs = Vec::with_capacity(out_fds.len());
    for (out, kind) in out_fds {
        let w2 = w.clone();
        match thread::Builder::new()
            .name("vm-exec-out".into())
            .spawn(move || pump(out, w2, kind))
        {
            Ok(h) => outputs.push(h),
            Err(e) => {
                signal_handle(reaper, &handle, libc::SIGKILL);
                *handle.lock().unwrap() = None;
                let _ = send_final(
                    &w,
                    &VmExecMsg::Error(truncate_error(&format!("spawn output thread: {e}"))),
                );
                let _ = w.lock().unwrap().stream.shutdown(Shutdown::Read);
                let _ = exited.recv_timeout(Duration::from_secs(2));
                return Ok(());
            }
        }
    }

    let status = exited.recv().unwrap_or(-1);
    // Clear the handle before waking the stdin reader so it never signals
    // after the reaper has reaped the process.
    *handle.lock().unwrap() = None;
    let _ = w.lock().unwrap().stream.shutdown(Shutdown::Read);

    // Wait for output pumps to flush buffered output without hanging forever
    // if a detached background process kept stdout/stderr or the pty open.
    for o in outputs {
        for _ in 0..200 {
            if o.is_finished() {
                let _ = o.join();
                break;
            }
            thread::sleep(Duration::from_millis(5));
        }
    }
    let _ = send_final(
        &w,
        &VmExecMsg::Exit {
            code: exit_code(status),
        },
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_default_shell_and_exec_commands() {
        let (cmd, prog) = build_command(&ExecSpec {
            cwd: "/".into(),
            env: vec![("FOO".into(), "bar".into())],
            argv: vec![],
            tty: Some(Tty {
                term: "xterm-256color".into(),
                rows: 24,
                cols: 80,
            }),
        })
        .unwrap();
        assert_eq!(prog, DEFAULT_SHELL);
        assert_eq!(cmd.get_program(), DEFAULT_SHELL);
        assert_eq!(cmd.get_args().count(), 0);
        let envs: Vec<(String, String)> = cmd
            .get_envs()
            .filter_map(|(k, v)| {
                Some((
                    k.to_string_lossy().into_owned(),
                    v?.to_string_lossy().into_owned(),
                ))
            })
            .collect();
        assert!(
            envs.iter()
                .any(|(k, v)| k == "PATH" && v.contains("k3s/bin"))
        );
        assert!(envs.iter().any(|(k, v)| k == "PS1" && v == DEFAULT_PS1));
        assert!(
            envs.iter()
                .any(|(k, v)| k == "TERM" && v == "xterm-256color")
        );
        assert!(envs.iter().any(|(k, v)| k == "FOO" && v == "bar"));

        let (cmd, prog) = build_command(&ExecSpec {
            cwd: "/".into(),
            env: vec![],
            argv: vec!["sysctl".into(), "-n".into(), "kernel.ostype".into()],
            tty: None,
        })
        .unwrap();
        assert_eq!(prog, "sysctl");
        let args: Vec<_> = cmd
            .get_args()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert_eq!(args, vec!["-n", "kernel.ostype"]);
        assert!(!cmd.get_envs().any(|(k, _)| k == "PS1"));
    }

    #[test]
    fn rejects_invalid_spec_inputs() {
        assert!(
            build_command(&ExecSpec {
                cwd: "/no/such/dir/katamaran-test".into(),
                ..Default::default()
            })
            .is_err()
        );
        assert!(
            build_command(&ExecSpec {
                cwd: "/\0bad".into(),
                ..Default::default()
            })
            .is_err()
        );
        assert!(
            build_command(&ExecSpec {
                cwd: "/".into(),
                env: vec![("BAD=NAME".into(), "1".into())],
                ..Default::default()
            })
            .is_err()
        );
        assert!(
            build_command(&ExecSpec {
                cwd: "/".into(),
                env: vec![("OK".into(), "a\0b".into())],
                ..Default::default()
            })
            .is_err()
        );
        assert!(
            build_command(&ExecSpec {
                cwd: "/".into(),
                argv: vec!["".into()],
                ..Default::default()
            })
            .is_err()
        );
        assert!(
            build_command(&ExecSpec {
                cwd: "/".into(),
                argv: vec!["echo".into(), "a\0b".into()],
                ..Default::default()
            })
            .is_err()
        );
        assert!(
            build_command(&ExecSpec {
                cwd: "/".into(),
                tty: Some(Tty {
                    term: "xterm\0".into(),
                    rows: 24,
                    cols: 80,
                }),
                ..Default::default()
            })
            .is_err()
        );
    }

    #[test]
    fn validates_signals_and_exit_codes() {
        assert!(valid_signal(libc::SIGINT));
        assert!(valid_signal(libc::SIGTERM));
        assert!(valid_signal(libc::SIGHUP));
        assert!(!valid_signal(0));
        assert!(!valid_signal(-1));
        assert!(!valid_signal(64));
        assert_eq!(exit_code(7 << 8), 7);
        assert_eq!(exit_code(libc::SIGKILL), 128 + libc::SIGKILL);
    }

    #[test]
    fn truncates_errors_on_utf8_boundaries() {
        let short = "boom";
        assert_eq!(truncate_error(short), "boom");
        let long = "é".repeat(MAX_ERROR_BYTES);
        let t = truncate_error(&long);
        assert!(t.len() <= MAX_ERROR_BYTES);
        assert!(t.ends_with("..."));
    }
}
