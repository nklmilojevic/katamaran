//! Serves a guest Unix socket on a vsock port. libkrun listens on a Unix
//! socket on the Mac and connects each client to this port.

use std::io;
use std::net::Shutdown;
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream;
use std::thread;

use super::sys;

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
