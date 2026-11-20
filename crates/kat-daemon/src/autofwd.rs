//! The Mac side of auto-forwarded container ports: a listener on
//! 127.0.0.1:<port> whose connections go over vsock (a Unix socket libkrun
//! connects to the guest's `AUTO_FORWARD_PORT`), each prefixed with the port
//! so the guest agent knows which container to dial.
//!
//! This path uses no IP routes at all, so a VPN that takes over the Mac's
//! routes cannot break it.

use std::collections::BTreeMap;
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};

struct Forward {
    stop: Arc<AtomicBool>,
    addr: SocketAddr,
    thread: JoinHandle<()>,
}

/// The auto-forwards in place.
pub struct AutoForwards {
    guest: PathBuf,
    active: BTreeMap<u16, Forward>,
}

impl AutoForwards {
    /// `guest` is the Unix socket libkrun relays to the guest's
    /// `AUTO_FORWARD_PORT`.
    pub fn new(guest: &Path) -> AutoForwards {
        AutoForwards {
            guest: guest.to_path_buf(),
            active: BTreeMap::new(),
        }
    }

    /// Listen on 127.0.0.1:`port`. Fails if the Mac already uses it; the
    /// guest reports the port again only after it stops and starts again.
    pub fn add(&mut self, port: u16) -> io::Result<bool> {
        if self.active.contains_key(&port) {
            return Ok(false);
        }
        let listener = TcpListener::bind(("127.0.0.1", port))?;
        let addr = listener.local_addr()?;
        let stop = Arc::new(AtomicBool::new(false));
        let (guest, flag) = (self.guest.clone(), stop.clone());
        let thread = thread::Builder::new()
            .name(format!("autofwd-{port}"))
            .spawn(move || accept_loop(listener, &guest, port, &flag))?;
        self.active.insert(port, Forward { stop, addr, thread });
        Ok(true)
    }

    /// Stop listening on `port`. Open connections run to their end.
    pub fn remove(&mut self, port: u16) -> bool {
        let Some(f) = self.active.remove(&port) else {
            return false;
        };
        f.stop.store(true, Ordering::SeqCst);
        // Wake the blocking accept; it sees the flag and closes the listener.
        let _ = TcpStream::connect(f.addr);
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

fn accept_loop(listener: TcpListener, guest: &Path, port: u16, stop: &AtomicBool) {
    for conn in listener.incoming() {
        if stop.load(Ordering::SeqCst) {
            return;
        }
        let Ok(conn) = conn else { continue };
        let guest = guest.to_path_buf();
        let _ = thread::Builder::new()
            .name(format!("autofwd-{port}-conn"))
            .spawn(move || {
                if let Err(e) = relay(conn, &guest, port) {
                    eprintln!("katamaran: auto-forward 127.0.0.1:{port}: {e}");
                }
            });
    }
}

fn relay(client: TcpStream, guest: &Path, port: u16) -> io::Result<()> {
    let _ = client.set_nodelay(true);
    let mut upstream = UnixStream::connect(guest)?;
    upstream.write_all(&port.to_le_bytes())?;
    let (mut c_read, mut u_write) = (client.try_clone()?, upstream.try_clone()?);
    let up = thread::spawn(move || {
        let _ = copy(&mut c_read, &mut u_write);
        let _ = u_write.shutdown(Shutdown::Write);
    });
    let mut client = client;
    let _ = copy(&mut upstream, &mut client);
    let _ = client.shutdown(Shutdown::Write);
    let _ = up.join();
    Ok(())
}

/// `io::copy` with a large buffer: its 8 KiB one costs a syscall pair per
/// 8 KiB, which caps a bulk transfer well below what the link carries.
fn copy(r: &mut impl Read, w: &mut impl Write) -> io::Result<u64> {
    let mut buf = vec![0u8; 1 << 20];
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

    fn free_port() -> u16 {
        TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
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
        let (a, b) = (free_port(), free_port());
        assert!(fwd.add(a).unwrap());
        assert!(!fwd.add(a).unwrap(), "added twice");
        assert!(fwd.add(b).unwrap());
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
}
