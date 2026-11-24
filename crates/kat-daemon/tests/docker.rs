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

/// Build a valid POSIX ustar header for a single regular file `name` of `size` bytes.
fn tar_header(name: &str, size: u64) -> [u8; 512] {
    let mut hdr = [0u8; 512];
    hdr[..name.len()].copy_from_slice(name.as_bytes());
    hdr[100..108].copy_from_slice(b"0000644\0");
    hdr[108..116].copy_from_slice(b"0000000\0");
    hdr[116..124].copy_from_slice(b"0000000\0");
    let size_oct = format!("{size:011o}\0");
    hdr[124..136].copy_from_slice(size_oct.as_bytes());
    hdr[136..148].copy_from_slice(b"00000000000\0");
    hdr[148..156].fill(b' ');
    hdr[156] = b'0';
    hdr[257..263].copy_from_slice(b"ustar\0");
    hdr[263..265].copy_from_slice(b"00");
    let sum: u32 = hdr.iter().map(|&b| u32::from(b)).sum();
    let cksum = format!("{sum:06o}\0 ");
    hdr[148..156].copy_from_slice(cksum.as_bytes());
    hdr
}

/// Read an HTTP response header up to `\r\n\r\n`, returning `(header, leftover_body_bytes)`.
fn read_http_header(stream: &mut UnixStream) -> (String, Vec<u8>) {
    let mut raw = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        let n = stream.read(&mut buf).expect("read HTTP response header");
        assert!(
            n > 0,
            "EOF before end of HTTP header: {:?}",
            String::from_utf8_lossy(&raw)
        );
        raw.extend_from_slice(&buf[..n]);
        if let Some(pos) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
            let hdr = String::from_utf8_lossy(&raw[..pos]).into_owned();
            let rest = raw[pos + 4..].to_vec();
            return (hdr, rest);
        }
    }
}

/// Split an HTTP response into `(header, decoded_body)`, decoding `chunked`
/// transfer encoding when present.
fn decode_http_response(resp: &str) -> (&str, String) {
    let (hdr, raw_body) = resp
        .split_once("\r\n\r\n")
        .unwrap_or_else(|| panic!("malformed HTTP response: {resp:?}"));
    let is_chunked = hdr.lines().any(|l| {
        let lower = l.to_ascii_lowercase();
        lower.starts_with("transfer-encoding:") && lower.contains("chunked")
    });
    if !is_chunked {
        return (hdr, raw_body.to_string());
    }
    let mut out = String::new();
    let mut rest = raw_body;
    loop {
        let (size_line, after_size) = rest
            .split_once("\r\n")
            .unwrap_or_else(|| panic!("truncated chunk size in {resp:?}"));
        let size_hex = size_line.split(';').next().unwrap().trim();
        let size = usize::from_str_radix(size_hex, 16)
            .unwrap_or_else(|e| panic!("bad chunk size {size_hex:?}: {e}"));
        if size == 0 {
            break;
        }
        assert!(
            after_size.len() >= size + 2,
            "truncated chunk payload of {size} bytes"
        );
        out.push_str(&after_size[..size]);
        rest = &after_size[size + 2..];
    }
    (hdr, out)
}

/// Verify a Docker `POST /images/create` response succeeded at both the HTTP
/// and streamed JSON layers, returning the created image ID (`sha256:...`).
fn assert_import_succeeded(label: &str, resp: &str) -> String {
    let (hdr, body) = decode_http_response(resp);
    assert!(
        hdr.starts_with("HTTP/1.1 200"),
        "{label} HTTP status: {resp}"
    );
    let mut image_id = None;
    for item in serde_json::Deserializer::from_str(&body).into_iter::<serde_json::Value>() {
        let obj = item.unwrap_or_else(|e| panic!("{label}: invalid JSON record ({e}): {body}"));
        assert!(
            obj.get("error").is_none() && obj.get("errorDetail").is_none(),
            "{label}: Docker reported stream error: {obj}"
        );
        if let Some(status) = obj.get("status").and_then(|s| s.as_str())
            && status.starts_with("sha256:")
        {
            image_id = Some(status.to_string());
        }
    }
    image_id.unwrap_or_else(|| panic!("{label}: no imported image ID in stream: {body}"))
}

/// Regression test for the virtio-vsock blocking-send deadlock:
/// 1. A stalled host reader on a >12 MiB guest->host Docker stream must not
///    freeze the VMM event loop or starve other Docker / control-channel I/O.
/// 2. Piping a >16 MiB guest->host Docker stream (`images/get`) directly into a
///    concurrent host->guest Docker stream (`images/create?fromSrc=-`) over
///    `docker.sock` must complete without deadlocking on vsock credit updates.
#[test]
#[ignore = "boots the system VM; run with `cargo xtask test-vm`"]
fn concurrent_bidirectional_docker_streams_do_not_deadlock() {
    let mut vm = Vm::start();
    vm.wait_for("docker ready", Duration::from_secs(120));
    let sock = vm.docker_socket();

    // Import an incompressible 16 MiB image so both host socket buffers (4 MiB)
    // and the vsock per-connection credit window (8 MiB) overflow.
    const BLOB_BYTES: usize = 16 * 1024 * 1024;
    let tar_len = 512 + BLOB_BYTES + 1024;
    {
        let mut s = UnixStream::connect(&sock).expect("connect docker socket for import");
        s.set_read_timeout(Some(Duration::from_secs(60))).unwrap();
        s.set_write_timeout(Some(Duration::from_secs(60))).unwrap();
        write!(
            s,
            "POST /images/create?fromSrc=-&repo=kattest&tag=blob HTTP/1.1\r\n\
             Host: docker\r\n\
             Content-Type: application/x-tar\r\n\
             Content-Length: {tar_len}\r\n\
             Connection: close\r\n\r\n"
        )
        .unwrap();
        s.write_all(&tar_header("blob.bin", BLOB_BYTES as u64))
            .unwrap();
        let mut state: u64 = 0x9e37_79b9_7f4a_7c15;
        let mut chunk = vec![0u8; 64 * 1024];
        let mut left = BLOB_BYTES;
        while left > 0 {
            let n = left.min(chunk.len());
            for word in chunk[..n].as_chunks_mut::<8>().0 {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                *word = state.to_le_bytes();
            }
            s.write_all(&chunk[..n]).unwrap();
            left -= n;
        }
        s.write_all(&[0u8; 1024]).unwrap();
        let mut resp = String::new();
        s.read_to_string(&mut resp).unwrap();
        assert_import_succeeded("seed import", &resp);
    }

    // Connection A: stream the 16 MiB image guest -> host (HTTP/1.0 avoids chunking).
    let mut export = UnixStream::connect(&sock).expect("connect docker socket for export");
    export
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    export
        .write_all(b"GET /images/kattest:blob/get HTTP/1.0\r\nHost: docker\r\n\r\n")
        .unwrap();
    let (export_hdr, export_rest) = read_http_header(&mut export);
    assert!(
        export_hdr.starts_with("HTTP/1.0 200"),
        "export header: {export_hdr}"
    );

    // Pause reading Connection A so the guest fills its 4 MiB host socket buffer
    // and overflows into the non-blocking vsock TX queue. A concurrent /_ping on
    // a separate socket must still succeed promptly instead of hanging.
    thread::sleep(Duration::from_millis(500));
    let ping_while_stalled = http(
        &sock,
        "GET /_ping HTTP/1.1\r\nHost: docker\r\nConnection: close\r\n\r\n",
        false,
    );
    assert!(
        ping_while_stalled.starts_with("HTTP/1.1 200"),
        "ping stalled while export reader was paused: {ping_while_stalled:?}"
    );

    // Connection B: pipe Connection A directly into a concurrent host -> guest
    // import over docker.sock, mirroring `docker export | docker run -i`.
    let mut import = UnixStream::connect(&sock).expect("connect docker socket for pipe import");
    import
        .set_read_timeout(Some(Duration::from_secs(60)))
        .unwrap();
    import
        .set_write_timeout(Some(Duration::from_secs(60)))
        .unwrap();
    import
        .write_all(
            b"POST /images/create?fromSrc=-&repo=kattest&tag=copy HTTP/1.1\r\n\
              Host: docker\r\n\
              Content-Type: application/x-tar\r\n\
              Transfer-Encoding: chunked\r\n\
              Connection: close\r\n\r\n",
        )
        .unwrap();

    let mut piped_bytes: usize = 0;
    let write_chunk = |dst: &mut UnixStream, data: &[u8]| {
        if data.is_empty() {
            return;
        }
        write!(dst, "{:x}\r\n", data.len()).unwrap();
        dst.write_all(data).unwrap();
        dst.write_all(b"\r\n").unwrap();
    };
    write_chunk(&mut import, &export_rest);
    piped_bytes += export_rest.len();

    let mut buf = vec![0u8; 32 * 1024];
    loop {
        let n = export.read(&mut buf).expect("read export stream");
        if n == 0 {
            break;
        }
        write_chunk(&mut import, &buf[..n]);
        piped_bytes += n;
    }
    import.write_all(b"0\r\n\r\n").unwrap();
    assert!(
        piped_bytes >= BLOB_BYTES,
        "expected >= {BLOB_BYTES} piped bytes, got {piped_bytes}"
    );

    let mut import_resp = String::new();
    import.read_to_string(&mut import_resp).unwrap();
    let copied_id = assert_import_succeeded("piped import", &import_resp);

    let inspect_resp = http(
        &sock,
        "GET /images/kattest:copy/json HTTP/1.1\r\nHost: docker\r\nConnection: close\r\n\r\n",
        false,
    );
    let (inspect_hdr, inspect_body) = decode_http_response(&inspect_resp);
    assert!(
        inspect_hdr.starts_with("HTTP/1.1 200"),
        "inspect kattest:copy failed: {inspect_resp}"
    );
    let inspected: serde_json::Value =
        serde_json::from_str(&inspect_body).expect("parse image inspect JSON");
    assert_eq!(
        inspected.get("Id").and_then(|v| v.as_str()),
        Some(copied_id.as_str()),
        "inspected image ID mismatch: {inspected}"
    );
    let inspected_size = inspected.get("Size").and_then(|v| v.as_u64()).unwrap_or(0);
    assert!(
        inspected_size >= BLOB_BYTES as u64,
        "expected kattest:copy size >= {BLOB_BYTES}, got {inspected_size}"
    );

    let ping_after = http(
        &sock,
        "GET /_ping HTTP/1.1\r\nHost: docker\r\nConnection: close\r\n\r\n",
        false,
    );
    assert!(
        ping_after.starts_with("HTTP/1.1 200"),
        "ping after bidirectional pipe: {ping_after:?}"
    );

    vm.stop();
}
