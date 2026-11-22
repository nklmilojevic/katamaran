//! Serves a guest Unix socket on a vsock port. libkrun listens on a Unix
//! socket on the Mac and connects each client to this port.

use std::io;
use std::net::{Shutdown, TcpStream};
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use super::sys;

const MAX_TCP_RELAY_CONNS: usize = 256;

struct ConnGuard(Arc<AtomicUsize>);

impl ConnGuard {
    fn try_acquire(active: &Arc<AtomicUsize>, limit: usize) -> Option<Self> {
        active
            .try_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < limit).then_some(n + 1)
            })
            .ok()
            .map(|_| ConnGuard(Arc::clone(active)))
    }
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

pub fn serve_unix_on_vsock(port: u32, target: &'static str) -> io::Result<()> {
    let listener = sys::vsock_listen(port)?;
    thread::Builder::new()
        .name(format!("relay-{port}"))
        .spawn(move || {
            loop {
                match sys::accept(&listener) {
                    Ok(conn) => {
                        thread::spawn(move || {
                            if let Err(e) = relay(conn, target) {
                                eprintln!("kat-init: relay {port} -> {target}: {e}");
                            }
                        });
                    }
                    Err(e) => {
                        eprintln!("kat-init: accept on vsock {port}: {e}");
                        return;
                    }
                }
            }
        })?;
    Ok(())
}

fn relay(conn: OwnedFd, target: &str) -> io::Result<()> {
    // A vsock fd supports read, write and shutdown like any stream socket.
    let client = UnixStream::from(conn);
    let upstream = UnixStream::connect(target)?;
    let (mut c_read, mut u_write) = (client.try_clone()?, upstream.try_clone()?);
    let up = thread::spawn(move || {
        let _ = io::copy(&mut c_read, &mut u_write);
        let _ = u_write.shutdown(Shutdown::Write);
    });
    let (mut u_read, mut c_write) = (upstream, client);
    let _ = io::copy(&mut u_read, &mut c_write);
    let _ = c_write.shutdown(Shutdown::Write);
    let _ = up.join();
    Ok(())
}

/// Serves a guest TCP endpoint (e.g. `127.0.0.1:6443`) on a vsock port.
pub fn serve_tcp_on_vsock(port: u32, target: &'static str) -> io::Result<()> {
    let listener = sys::vsock_listen(port)?;
    let active = Arc::new(AtomicUsize::new(0));
    thread::Builder::new()
        .name(format!("relay-tcp-{port}"))
        .spawn(move || {
            loop {
                match sys::accept(&listener) {
                    Ok(conn) => {
                        let Some(guard) = ConnGuard::try_acquire(&active, MAX_TCP_RELAY_CONNS)
                        else {
                            eprintln!(
                                "kat-init: relay-tcp {port}: connection limit ({MAX_TCP_RELAY_CONNS}) reached"
                            );
                            continue;
                        };
                        if let Err(e) = thread::Builder::new()
                            .name(format!("relay-tcp-conn-{port}"))
                            .spawn(move || {
                                let _guard = guard;
                                let _ = relay_tcp(conn, target);
                            })
                        {
                            eprintln!("kat-init: spawn relay-tcp {port} worker: {e}");
                        }
                    }
                    Err(e) => {
                        eprintln!("kat-init: accept on vsock {port}: {e}");
                        thread::sleep(Duration::from_millis(50));
                    }
                }
            }
        })?;
    Ok(())
}

fn relay_tcp(conn: OwnedFd, target: &str) -> io::Result<()> {
    let client = UnixStream::from(conn);
    let upstream = TcpStream::connect(target)?;
    let _ = upstream.set_nodelay(true);
    let (mut c_read, mut u_write) = (client.try_clone()?, upstream.try_clone()?);
    let up = thread::Builder::new()
        .name("relay-tcp-up".into())
        .spawn(move || {
            let _ = io::copy(&mut c_read, &mut u_write);
            let _ = u_write.shutdown(Shutdown::Write);
        })?;
    let (mut u_read, mut c_write) = (upstream, client);
    let _ = io::copy(&mut u_read, &mut c_write);
    let _ = c_write.shutdown(Shutdown::Write);
    let _ = up.join();
    Ok(())
}
