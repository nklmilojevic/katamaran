//! Starting a `katamaran-vm` process and completing the guest handshake.

use std::fs;
use std::io::{self, BufRead};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, bail};
use kat_proto::{Hello, Msg, PROTOCOL_VERSION, read_msg, write_msg};
use kat_vmm::VmConfig;

pub struct RunningVm {
    pub child: Child,
    pub conn: UnixStream,
    pub hello: Hello,
    pub t_spawn: Instant,
    /// Spawn until the guest's `Hello`.
    pub t_hello: Duration,
}

/// Write `cfg` next to its control socket, listen, spawn the VM process and
/// wait for the guest's `Hello`. On error the VM process is killed.
pub fn start(cfg: &VmConfig, vm_bin: &Path, timeout: Duration) -> anyhow::Result<RunningVm> {
    cfg.validate()?;
    let dir = cfg
        .control_socket
        .parent()
        .context("control socket has no parent directory")?;
    let cfg_path = dir.join("vm.json");
    fs::write(&cfg_path, serde_json::to_vec_pretty(cfg)?)?;

    // Listen before the VM starts, so the guest's first dial succeeds and
    // neither side has to poll.
    let _ = fs::remove_file(&cfg.control_socket);
    let listener = UnixListener::bind(&cfg.control_socket)
        .with_context(|| format!("bind {}", cfg.control_socket.display()))?;
    listener.set_nonblocking(true)?;

    let vm_log = fs::File::create(dir.join("vm.log"))?;
    let t_spawn = Instant::now();
    let mut child = Command::new(vm_bin)
        .arg(&cfg_path)
        .stdin(Stdio::null())
        .stdout(vm_log.try_clone()?)
        .stderr(vm_log)
        .spawn()
        .with_context(|| format!("spawn {}", vm_bin.display()))?;

    match handshake(&listener, &mut child, timeout) {
        Ok((conn, hello)) => Ok(RunningVm {
            child,
            conn,
            hello,
            t_spawn,
            t_hello: t_spawn.elapsed(),
        }),
        Err(e) => {
            child.kill().ok();
            child.wait().ok();
            Err(e)
        }
    }
}

/// Enforces a single wall-clock `deadline` across every `read`/`write` syscall
/// during the guest handshake, so a trickling peer cannot extend the timeout.
struct DeadlineStream<'a> {
    conn: &'a mut UnixStream,
    deadline: Instant,
    timeout: Duration,
}

impl DeadlineStream<'_> {
    fn remaining(&self) -> io::Result<Duration> {
        let left = self.deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "guest handshake timed out after {}s",
                    self.timeout.as_secs()
                ),
            ))
        } else {
            Ok(left)
        }
    }
}

impl io::Read for DeadlineStream<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.conn.set_read_timeout(Some(self.remaining()?))?;
        self.conn.read(buf)
    }
}

impl io::Write for DeadlineStream<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.conn.set_write_timeout(Some(self.remaining()?))?;
        self.conn.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.remaining()?;
        self.conn.flush()
    }
}

fn handshake(
    listener: &UnixListener,
    child: &mut Child,
    timeout: Duration,
) -> anyhow::Result<(UnixStream, Hello)> {
    let deadline = Instant::now() + timeout;
    let mut conn = accept_guest(listener, child, deadline, timeout)?;
    let hello = handshake_io(&mut conn, deadline, timeout)?;
    Ok((conn, hello))
}

fn handshake_io(
    conn: &mut UnixStream,
    deadline: Instant,
    timeout: Duration,
) -> anyhow::Result<Hello> {
    conn.set_nonblocking(false)?;
    let mut ds = DeadlineStream {
        conn,
        deadline,
        timeout,
    };
    let hello = match read_msg(&mut ds)? {
        Some(Msg::Hello(h)) => h,
        Some(other) => bail!("expected Hello, got {other:?}"),
        None => bail!("guest closed the control channel before Hello"),
    };
    if hello.protocol != PROTOCOL_VERSION {
        bail!(
            "guest speaks protocol {}, host speaks {PROTOCOL_VERSION}",
            hello.protocol
        );
    }
    write_msg(
        &mut ds,
        &Msg::SetTime {
            unix_ns: unix_now_ns(),
        },
    )?;
    conn.set_read_timeout(None)?;
    conn.set_write_timeout(None)?;
    Ok(hello)
}

impl RunningVm {
    /// Ask the guest to power off and wait for the VM process to exit.
    pub fn shutdown(&mut self, timeout: Duration) -> anyhow::Result<(ExitStatus, Duration)> {
        let t = Instant::now();
        write_msg(&mut self.conn, &Msg::Shutdown)?;
        let status = wait_with_timeout(&mut self.child, timeout)?;
        Ok((status, t.elapsed()))
    }
}

impl Drop for RunningVm {
    fn drop(&mut self) {
        if !matches!(self.child.try_wait(), Ok(Some(_))) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn accept_guest(
    listener: &UnixListener,
    child: &mut Child,
    deadline: Instant,
    timeout: Duration,
) -> anyhow::Result<UnixStream> {
    loop {
        match listener.accept() {
            Ok((conn, _)) => return Ok(conn),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
            Err(e) => return Err(e).context("accept guest connection"),
        }
        if let Some(status) = child.try_wait()? {
            bail!("katamaran-vm exited before the guest connected ({status})");
        }
        if Instant::now() >= deadline {
            bail!("guest did not connect within {}s", timeout.as_secs());
        }
        thread::sleep(Duration::from_micros(200));
    }
}

pub fn wait_with_timeout(child: &mut Child, timeout: Duration) -> anyhow::Result<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            child.kill().ok();
            child.wait().ok();
            bail!("VM did not power off within {}s", timeout.as_secs());
        }
        thread::sleep(Duration::from_micros(200));
    }
}

pub fn vm_bin(explicit: Option<&Path>) -> anyhow::Result<PathBuf> {
    match explicit {
        Some(p) => Ok(p.to_path_buf()),
        None => Ok(std::env::current_exe()?
            .canonicalize()?
            .with_file_name("katamaran-vm")),
    }
}

pub fn unix_now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64)
}

pub fn absolute(p: &Path) -> anyhow::Result<PathBuf> {
    std::path::absolute(p).with_context(|| format!("resolve {}", p.display()))
}

pub fn ms(d: Duration) -> String {
    format!("{:.1} ms", d.as_secs_f64() * 1e3)
}

pub fn print_tail(label: &str, path: &Path) {
    let Ok(f) = fs::File::open(path) else { return };
    let lines: Vec<String> = io::BufReader::new(f)
        .lines()
        .map_while(Result::ok)
        .collect();
    if lines.is_empty() {
        return;
    }
    eprintln!("--- last lines of {label} ({}) ---", path.display());
    for l in &lines[lines.len().saturating_sub(40)..] {
        eprintln!("{l}");
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    #[test]
    fn handshake_enforces_total_deadline_against_trickling_peer() {
        let (mut host, mut guest) = UnixStream::pair().unwrap();
        let trickle = thread::spawn(move || {
            // Advertise a 100-byte frame, then trickle 1 byte every 40 ms so a
            // per-read socket timeout would keep resetting indefinitely.
            let header = 100u32.to_le_bytes();
            for &b in &header {
                if guest.write_all(&[b]).is_err() {
                    return;
                }
                thread::sleep(Duration::from_millis(40));
            }
            for _ in 0..100 {
                if guest.write_all(&[0u8]).is_err() {
                    return;
                }
                thread::sleep(Duration::from_millis(40));
            }
        });

        let timeout = Duration::from_millis(150);
        let t = Instant::now();
        let err = handshake_io(&mut host, t + timeout, timeout).unwrap_err();
        let elapsed = t.elapsed();
        drop(host);
        let _ = trickle.join();

        assert!(
            elapsed < Duration::from_secs(1),
            "trickling peer extended handshake to {elapsed:?}: {err:#}"
        );
    }

    #[test]
    fn handshake_clears_socket_timeouts_on_success() {
        let (mut host, mut guest) = UnixStream::pair().unwrap();
        let peer = thread::spawn(move || {
            write_msg(
                &mut guest,
                &Msg::Hello(Hello {
                    protocol: PROTOCOL_VERSION,
                    kernel_release: "6.18".into(),
                    mem_total_kib: 1024,
                    init_start_ns: 1,
                    ready_ns: 2,
                }),
            )
            .unwrap();
            assert!(matches!(
                read_msg(&mut guest).unwrap(),
                Some(Msg::SetTime { .. })
            ));
        });

        let timeout = Duration::from_secs(5);
        let hello = handshake_io(&mut host, Instant::now() + timeout, timeout).unwrap();
        assert_eq!(hello.kernel_release, "6.18");
        assert_eq!(host.read_timeout().unwrap(), None);
        assert_eq!(host.write_timeout().unwrap(), None);
        peer.join().unwrap();
    }
}
