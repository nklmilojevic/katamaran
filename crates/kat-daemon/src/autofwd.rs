//! The Mac side of auto-forwarded container ports: a listener on
//! 127.0.0.1:<port> whose connections go over vsock (a Unix socket libkrun
//! connects to the guest's `AUTO_FORWARD_PORT`), each prefixed with the port
//! so the guest agent knows which container to dial.
//!
//! This path uses no IP routes at all, so a VPN that takes over the Mac's
//! routes cannot break it.

use std::collections::BTreeMap;
use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const MAX_CONNS: usize = 256;
const COPY_BUF_SIZE: usize = 64 << 10;
const ACCEPT_ERR_BACKOFF: Duration = Duration::from_millis(50);

struct Forward {
    stop: Arc<AtomicBool>,
    wake_w: OwnedFd,
    thread: JoinHandle<()>,
}

/// RAII guard for an active relayed connection slot.
struct ConnGuard(Arc<AtomicUsize>);

impl ConnGuard {
    fn try_acquire(counter: &Arc<AtomicUsize>, max: usize) -> Option<Self> {
        let mut cur = counter.load(Ordering::Acquire);
        loop {
            if cur >= max {
                return None;
            }
            match counter.compare_exchange_weak(cur, cur + 1, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return Some(ConnGuard(counter.clone())),
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

/// Create a non-blocking, `O_CLOEXEC` wakeup pipe.
fn wakeup_pipe() -> io::Result<(OwnedFd, OwnedFd)> {
    let mut fds = [-1; 2];
    if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `pipe` succeeded and returned two valid open file descriptors.
    let r = unsafe { OwnedFd::from_raw_fd(fds[0]) };
    let w = unsafe { OwnedFd::from_raw_fd(fds[1]) };
    for fd in [r.as_raw_fd(), w.as_raw_fd()] {
        let fd_flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        if fd_flags < 0
            || unsafe { libc::fcntl(fd, libc::F_SETFD, fd_flags | libc::FD_CLOEXEC) } < 0
        {
            return Err(io::Error::last_os_error());
        }
        let status_flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        if status_flags < 0
            || unsafe { libc::fcntl(fd, libc::F_SETFL, status_flags | libc::O_NONBLOCK) } < 0
        {
            return Err(io::Error::last_os_error());
        }
    }
    Ok((r, w))
}

/// The auto-forwards in place.
pub struct AutoForwards {
    guest: PathBuf,
    active: BTreeMap<u16, Forward>,
    active_conns: Arc<AtomicUsize>,
    max_conns: usize,
}

impl AutoForwards {
    /// `guest` is the Unix socket libkrun relays to the guest's
    /// `AUTO_FORWARD_PORT`.
    pub fn new(guest: &Path) -> AutoForwards {
        Self::with_max_conns(guest, MAX_CONNS)
    }

    fn with_max_conns(guest: &Path, max_conns: usize) -> AutoForwards {
        AutoForwards {
            guest: guest.to_path_buf(),
            active: BTreeMap::new(),
            active_conns: Arc::new(AtomicUsize::new(0)),
            max_conns,
        }
    }

    /// Listen on 127.0.0.1:`port`. Fails if the Mac already uses it; the
    /// guest reports the port again only after it stops and starts again.
    pub fn add(&mut self, port: u16) -> io::Result<bool> {
        if self.active.contains_key(&port) {
            return Ok(false);
        }
        let listener = TcpListener::bind(("127.0.0.1", port))?;
        listener.set_nonblocking(true)?;
        let (wake_r, wake_w) = wakeup_pipe()?;
        let stop = Arc::new(AtomicBool::new(false));
        let (guest, flag, conns, max_conns) = (
            self.guest.clone(),
            stop.clone(),
            self.active_conns.clone(),
            self.max_conns,
        );
        let thread = thread::Builder::new()
            .name(format!("autofwd-{port}"))
            .spawn(move || accept_loop(listener, wake_r, &guest, port, &flag, &conns, max_conns))?;
        self.active.insert(
            port,
            Forward {
                stop,
                wake_w,
                thread,
            },
        );
        Ok(true)
    }

    /// Stop listening on `port`. Open connections run to their end.
    pub fn remove(&mut self, port: u16) -> bool {
        let Some(f) = self.active.remove(&port) else {
            return false;
        };
        f.stop.store(true, Ordering::SeqCst);
        // Wake the poll loop via the non-blocking pipe; dropping `wake_w`
        // also signals POLLHUP even if the pipe buffer were full.
        let b = [1u8; 1];
        unsafe { libc::write(f.wake_w.as_raw_fd(), b.as_ptr().cast(), 1) };
        drop(f.wake_w);
        let _ = f.thread.join();
        true
    }

    pub fn clear(&mut self) {
        let ports: Vec<u16> = self.active.keys().copied().collect();
        for p in ports {
            self.remove(p);
        }
    }
}

impl Drop for AutoForwards {
    fn drop(&mut self) {
        self.clear();
    }
}

fn accept_loop(
    listener: TcpListener,
    wake_r: OwnedFd,
    guest: &Path,
    port: u16,
    stop: &AtomicBool,
    active_conns: &Arc<AtomicUsize>,
    max_conns: usize,
) {
    let mut pfds = [
        libc::pollfd {
            fd: listener.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        },
        libc::pollfd {
            fd: wake_r.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        },
    ];
    loop {
        if stop.load(Ordering::SeqCst) {
            return;
        }
        pfds[0].revents = 0;
        pfds[1].revents = 0;
        let rc = unsafe { libc::poll(pfds.as_mut_ptr(), pfds.len() as libc::nfds_t, -1) };
        if rc < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            thread::sleep(ACCEPT_ERR_BACKOFF);
            continue;
        }
        if stop.load(Ordering::SeqCst) || pfds[1].revents != 0 {
            return;
        }
        if pfds[0].revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0 {
            return;
        }
        if pfds[0].revents & libc::POLLIN != 0 {
            loop {
                if stop.load(Ordering::SeqCst) {
                    return;
                }
                match listener.accept() {
                    Ok((conn, _)) => {
                        if conn.set_nonblocking(false).is_err() {
                            continue;
                        }
                        let Some(guard) = ConnGuard::try_acquire(active_conns, max_conns) else {
                            eprintln!(
                                "katamaran: auto-forward 127.0.0.1:{port}: connection limit ({max_conns}) reached"
                            );
                            continue;
                        };
                        let guest = guest.to_path_buf();
                        if let Err(e) = thread::Builder::new()
                            .name(format!("autofwd-{port}-conn"))
                            .spawn(move || {
                                let _guard = guard;
                                if let Err(e) = relay(conn, &guest, port) {
                                    eprintln!("katamaran: auto-forward 127.0.0.1:{port}: {e}");
                                }
                            })
                        {
                            eprintln!("katamaran: auto-forward 127.0.0.1:{port}: spawn: {e}");
                        }
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                    Err(e) => {
                        eprintln!("katamaran: auto-forward 127.0.0.1:{port}: accept: {e}");
                        thread::sleep(ACCEPT_ERR_BACKOFF);
                        break;
                    }
                }
            }
        }
    }
}

fn relay(client: TcpStream, guest: &Path, port: u16) -> io::Result<()> {
    let _ = client.set_nodelay(true);
    let mut upstream = UnixStream::connect(guest)?;
    upstream.write_all(&port.to_le_bytes())?;
    let (mut c_read, mut u_write) = (client.try_clone()?, upstream.try_clone()?);
    let up = thread::Builder::new()
        .name(format!("autofwd-{port}-up"))
        .spawn(move || {
            let _ = copy(&mut c_read, &mut u_write);
            let _ = u_write.shutdown(Shutdown::Write);
        })?;
    let mut client = client;
    let _ = copy(&mut upstream, &mut client);
    let _ = client.shutdown(Shutdown::Write);
    let _ = up.join();
    Ok(())
}

/// `io::copy` with a 64 KiB buffer: avoids the syscall overhead of 8 KiB
/// without allocating megabytes per idle connection.
fn copy(r: &mut impl Read, w: &mut impl Write) -> io::Result<u64> {
    let mut buf = vec![0u8; COPY_BUF_SIZE];
    let mut total = 0;
    loop {
        let n = match r.read(&mut buf) {
            Ok(0) => return Ok(total),
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        w.write_all(&buf[..n])?;
        total += n as u64;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::os::unix::net::UnixListener;
    use std::time::Duration;

    /// A fake guest: reads the port header, answers "<port>:" and echoes.
    fn fake_guest(dir: &Path) -> PathBuf {
        let sock = dir.join("autofwd.sock");
        let listener = UnixListener::bind(&sock).unwrap();
        thread::spawn(move || {
            for conn in listener.incoming() {
                let mut conn = conn.unwrap();
                thread::spawn(move || {
                    let mut port = [0u8; 2];
                    conn.read_exact(&mut port).unwrap();
                    write!(conn, "{}:", u16::from_le_bytes(port)).unwrap();
                    let mut rest = conn.try_clone().unwrap();
                    copy(&mut rest, &mut conn).unwrap();
                    conn.shutdown(Shutdown::Write).unwrap();
                });
            }
        });
        sock
    }

    /// Bind a fresh port outside macOS's ephemeral range (49152..=65535)
    /// directly via `fwd.add`, avoiding a throwaway probe listener whose fd
    /// could briefly be inherited by a parallel test subprocess on macOS.
    fn add_free_port(fwd: &mut AutoForwards) -> u16 {
        use std::sync::atomic::AtomicU16;
        static NEXT: AtomicU16 = AtomicU16::new(0);
        let base = (std::process::id() % 8_000) as u16;
        for _ in 0..256 {
            let off = NEXT.fetch_add(1, Ordering::Relaxed);
            let port = 36_000u16 + (base.wrapping_add(off) % 10_000);
            if matches!(fwd.add(port), Ok(true)) {
                return port;
            }
        }
        panic!("could not find a free test port");
    }

    fn round_trip(port: u16, msg: &str) -> String {
        let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        // Written while the echo is read, or both directions' buffers fill.
        let mut w = s.try_clone().unwrap();
        let msg = msg.to_string();
        let writer = thread::spawn(move || {
            w.write_all(msg.as_bytes()).unwrap();
            w.shutdown(Shutdown::Write).unwrap();
        });
        let mut out = String::new();
        s.read_to_string(&mut out).unwrap();
        writer.join().unwrap();
        out
    }

    #[test]
    fn forwards_connections_with_their_port_until_removed() {
        let dir = std::env::temp_dir().join(format!("kat-autofwd-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut fwd = AutoForwards::new(&fake_guest(&dir));
        let a = add_free_port(&mut fwd);
        assert!(!fwd.add(a).unwrap(), "added twice");
        let b = add_free_port(&mut fwd);
        assert_eq!(round_trip(a, "hello"), format!("{a}:hello"));
        // Large enough to need many reads in both directions.
        let big = "x".repeat(4 << 20);
        assert_eq!(round_trip(b, &big), format!("{b}:{big}"));

        // A relayed connection outlives the forward's removal.
        let mut open = TcpStream::connect(("127.0.0.1", a)).unwrap();
        open.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        open.write_all(b"x").unwrap();
        let mut head = vec![0u8; format!("{a}:x").len()];
        open.read_exact(&mut head).unwrap();
        assert_eq!(head, format!("{a}:x").as_bytes());
        assert!(fwd.remove(a));
        assert!(!fwd.remove(a));
        assert!(
            TcpStream::connect(("127.0.0.1", a)).is_err(),
            "still listening"
        );
        open.write_all(b"y").unwrap();
        let mut tail = [0u8; 1];
        open.read_exact(&mut tail).unwrap();
        assert_eq!(&tail, b"y");

        // The Mac's port is busy: refused, nothing kept.
        let busy = TcpListener::bind("127.0.0.1:0").unwrap();
        let busy_port = busy.local_addr().unwrap().port();
        assert!(fwd.add(busy_port).is_err());
        assert!(!fwd.remove(busy_port));

        fwd.clear();
        assert!(TcpStream::connect(("127.0.0.1", b)).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn remove_shuts_down_promptly_and_enforces_concurrency_cap() {
        let dir = std::env::temp_dir().join(format!("kat-autofwd-cap-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut fwd = AutoForwards::with_max_conns(&fake_guest(&dir), 2);
        let port = add_free_port(&mut fwd);

        // Hold two open relayed connections (fills the cap of 2).
        let mut c1 = TcpStream::connect(("127.0.0.1", port)).unwrap();
        c1.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        c1.write_all(b"a").unwrap();
        let mut buf = vec![0u8; format!("{port}:a").len()];
        c1.read_exact(&mut buf).unwrap();

        let mut c2 = TcpStream::connect(("127.0.0.1", port)).unwrap();
        c2.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        c2.write_all(b"b").unwrap();
        c2.read_exact(&mut buf).unwrap();

        // A third concurrent connection is rejected immediately (EOF / reset),
        // never silently queued until the read timeout expires.
        let mut c3 = TcpStream::connect(("127.0.0.1", port)).unwrap();
        c3.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let mut one = [0u8; 1];
        match c3.read(&mut one) {
            Ok(0) => {}
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::ConnectionReset
                        | io::ErrorKind::ConnectionAborted
                        | io::ErrorKind::BrokenPipe
                        | io::ErrorKind::UnexpectedEof
                ) => {}
            other => panic!("expected cap rejection (EOF or reset), got {other:?}"),
        }
        drop(c3);

        // Removing the forward returns promptly via the wakeup pipe even while
        // existing connections remain open.
        let t = std::time::Instant::now();
        assert!(fwd.remove(port));
        assert!(
            t.elapsed() < Duration::from_secs(1),
            "remove took {:?}",
            t.elapsed()
        );
        // The listening socket is closed once remove() returns. Poll briefly on
        // AddrInUse in case a parallel test subprocess briefly inherited the fd
        // during macOS's non-atomic socket()+FIOCLEX window.
        let bind_deadline = std::time::Instant::now() + Duration::from_millis(500);
        loop {
            match TcpListener::bind(("127.0.0.1", port)) {
                Ok(rebound) => {
                    drop(rebound);
                    break;
                }
                Err(e)
                    if e.kind() == io::ErrorKind::AddrInUse
                        && std::time::Instant::now() < bind_deadline =>
                {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(e) => panic!("still listening on {port}: {e:?}"),
            }
        }

        // Releasing slots restores capacity when re-added.
        drop(c1);
        drop(c2);
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while fwd.active_conns.load(Ordering::Acquire) != 0 && std::time::Instant::now() < deadline
        {
            thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(fwd.active_conns.load(Ordering::Acquire), 0);
        assert!(fwd.add(port).unwrap());
        assert_eq!(round_trip(port, "ok"), format!("{port}:ok"));
        fwd.clear();
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
