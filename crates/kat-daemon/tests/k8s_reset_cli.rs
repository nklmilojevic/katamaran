//! Subprocess tests for `kat k8s reset` stderr backpressure and error exit handling.

use std::ffi::OsString;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::ffi::OsStringExt;
use std::os::unix::io::{FromRawFd, RawFd};
use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

struct TempDirGuard(PathBuf);

impl TempDirGuard {
    fn create(prefix: &str) -> Self {
        let template = std::env::temp_dir().join(format!("{prefix}-XXXXXX"));
        let mut bytes = template.into_os_string().into_vec();
        bytes.push(0);
        let ptr = unsafe { libc::mkdtemp(bytes.as_mut_ptr().cast()) };
        assert!(
            !ptr.is_null(),
            "mkdtemp failed: {}",
            std::io::Error::last_os_error()
        );
        bytes.pop();
        Self(PathBuf::from(OsString::from_vec(bytes)))
    }
}

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn make_full_pipe() -> (File, File) {
    let mut fds: [RawFd; 2] = [-1, -1];
    assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
    let read_end = unsafe { File::from_raw_fd(fds[0]) };
    let write_end = unsafe { File::from_raw_fd(fds[1]) };

    let flags = unsafe { libc::fcntl(fds[1], libc::F_GETFL) };
    assert!(flags >= 0);
    assert_eq!(
        unsafe { libc::fcntl(fds[1], libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let chunk = [b'x'; 4096];
    loop {
        let n = unsafe { libc::write(fds[1], chunk.as_ptr().cast(), chunk.len()) };
        if n < 0 {
            let e = std::io::Error::last_os_error();
            assert_eq!(e.kind(), std::io::ErrorKind::WouldBlock, "{e}");
            break;
        }
    }
    // Restore blocking mode on the write end so the child sees a normal blocking pipe
    // whose kernel buffer is already full and unread.
    assert_eq!(unsafe { libc::fcntl(fds[1], libc::F_SETFL, flags) }, 0);
    (read_end, write_end)
}

#[test]
fn k8s_reset_exits_nonzero_on_rpc_error_even_when_stderr_pipe_is_full_and_unread() {
    let home = TempDirGuard::create("kat-rst-err");
    let run_dir = home.0.join("run");
    fs::create_dir_all(&run_dir).unwrap();
    let sock = run_dir.join("daemon.sock");
    let listener = UnixListener::bind(&sock).unwrap();

    let srv = thread::spawn(move || {
        for _ in 0..2 {
            let (mut conn, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(conn.try_clone().unwrap());
            let mut line = String::new();
            assert!(reader.read_line(&mut line).unwrap() > 0);
            let req: serde_json::Value = serde_json::from_str(&line).unwrap();
            assert_eq!(req["method"], "k8s.reset");
            let id = req["id"].clone();

            for i in 0..40 {
                let note = serde_json::json!({
                    "jsonrpc": "2.0",
                    "method": "k8s.progress",
                    "params": { "message": format!("progress step {i}") },
                });
                let mut out = serde_json::to_vec(&note).unwrap();
                out.push(b'\n');
                conn.write_all(&out).unwrap();
            }
            let resp = serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": { "code": 1, "message": "simulated reset failure" },
            });
            let mut out = serde_json::to_vec(&resp).unwrap();
            out.push(b'\n');
            conn.write_all(&out).unwrap();
        }
    });

    // 1. Full, unread stderr pipe: CLI must still exit nonzero promptly without draining it.
    let (_unread_pipe_read, full_pipe_write) = make_full_pipe();
    let mut child = Command::new(env!("CARGO_BIN_EXE_katamaran"))
        .args(["k8s", "reset"])
        .env("KATAMARAN_HOME", &home.0)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::from(full_pipe_write))
        .spawn()
        .unwrap();

    let started = Instant::now();
    let status = loop {
        if let Some(st) = child.try_wait().unwrap() {
            break st;
        }
        if started.elapsed() >= Duration::from_secs(5) {
            let _ = child.kill();
            let _ = child.wait();
            panic!("katamaran k8s reset hung on full unread stderr pipe");
        }
        thread::sleep(Duration::from_millis(20));
    };
    assert!(
        !status.success(),
        "expected nonzero exit status on RPC error, got {status}"
    );
    drop(_unread_pipe_read);

    // 2. Writable stderr pipe: CLI still exits nonzero and preserves progress + error diagnostic.
    let mut child2 = Command::new(env!("CARGO_BIN_EXE_katamaran"))
        .args(["k8s", "reset"])
        .env("KATAMARAN_HOME", &home.0)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stderr_buf = String::new();
    child2
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut stderr_buf)
        .unwrap();
    let status2 = child2.wait().unwrap();
    srv.join().unwrap();

    assert!(!status2.success(), "expected nonzero exit, got {status2}");
    assert!(
        stderr_buf.contains("katamaran: progress step 0"),
        "missing progress in stderr: {stderr_buf}"
    );
    assert!(
        stderr_buf.contains("simulated reset failure"),
        "missing terminal error diagnostic in stderr: {stderr_buf}"
    );

    // 3. Daemon connection failure with a full, unread stderr pipe also exits
    //    nonzero promptly without hanging on stderr.
    let missing_home = TempDirGuard::create("kat-rst-no");
    let (_unread_pipe_read3, full_pipe_write3) = make_full_pipe();
    let mut child3 = Command::new(env!("CARGO_BIN_EXE_katamaran"))
        .args(["k8s", "reset"])
        .env("KATAMARAN_HOME", &missing_home.0)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::from(full_pipe_write3))
        .spawn()
        .unwrap();
    let started3 = Instant::now();
    let status3 = loop {
        if let Some(st) = child3.try_wait().unwrap() {
            break st;
        }
        if started3.elapsed() >= Duration::from_secs(5) {
            let _ = child3.kill();
            let _ = child3.wait();
            panic!("katamaran k8s reset hung on connect failure with full unread stderr pipe");
        }
        thread::sleep(Duration::from_millis(20));
    };
    assert!(
        !status3.success(),
        "expected nonzero exit status on daemon connect error, got {status3}"
    );
    drop(_unread_pipe_read3);
}
