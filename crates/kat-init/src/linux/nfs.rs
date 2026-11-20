//! The guest's files on the Mac: an NFSv3 server (`kat-nfs`) on vsock
//! [`NFS_PORT`], which libkrun relays to a Unix socket only the Mac's kernel
//! NFS client may connect to.
//!
//! The server runs as a child process (`/init nfsd`, the same binary), so a
//! bug in it can never take down PID 1 and with it the VM. PID 1 owns the
//! listening socket and hands it to each child as fd 3: connections queue
//! while a crashed server restarts, and the Mac's client reconnects to the
//! new one.

use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::thread;
use std::time::Duration;

use kat_proto::{Msg, NFS_PORT};

use super::service::{Notifier, Reaper, describe_status};
use super::sys;

/// The listening socket's descriptor in the server process.
const LISTEN_FD: i32 = 3;
const LOG: &str = "/var/log/nfsd.log";

pub struct Nfsd {
    stopping: Arc<AtomicBool>,
    pid: Arc<AtomicI32>,
}

impl Nfsd {
    /// Listen on vsock and keep a server process running until [`Nfsd::stop`].
    pub fn start(reaper: Reaper, notifier: Notifier) -> io::Result<Nfsd> {
        let listener = sys::vsock_listen(NFS_PORT)?;
        let nfsd = Nfsd {
            stopping: Arc::new(AtomicBool::new(false)),
            pid: Arc::new(AtomicI32::new(0)),
        };
        let (stopping, pid) = (nfsd.stopping.clone(), nfsd.pid.clone());
        thread::Builder::new()
            .name("nfsd-supervisor".into())
            .spawn(move || supervise(&listener, &reaper, &notifier, &stopping, &pid))?;
        Ok(nfsd)
    }

    pub fn stop(&self) {
        self.stopping.store(true, Ordering::SeqCst);
        let pid = self.pid.load(Ordering::SeqCst);
        if pid > 0 {
            unsafe { libc::kill(pid, libc::SIGTERM) };
        }
    }
}

fn supervise(
    listener: &OwnedFd,
    reaper: &Reaper,
    notifier: &Notifier,
    stopping: &AtomicBool,
    pid: &AtomicI32,
) {
    let mut announced = false;
    while !stopping.load(Ordering::SeqCst) {
        let (child, exited) = match spawn(listener, reaper) {
            Ok(c) => c,
            Err(e) => {
                notifier.send(&Msg::ServiceFailed {
                    name: "nfs".into(),
                    error: format!("start nfsd: {e}"),
                });
                return;
            }
        };
        pid.store(child, Ordering::SeqCst);
        if !announced {
            // The socket listens already; the Mac may connect right away.
            notifier.send(&Msg::ServiceReady {
                name: "nfs".into(),
                at_ns: sys::boottime_ns(),
            });
            announced = true;
        }
        let status = exited.recv().unwrap_or(0);
        pid.store(0, Ordering::SeqCst);
        if stopping.load(Ordering::SeqCst) {
            return;
        }
        notifier.send(&Msg::ServiceFailed {
            name: "nfs".into(),
            error: format!("nfsd {}, restarting", describe_status(status)),
        });
        thread::sleep(Duration::from_secs(1));
    }
}

fn spawn(listener: &OwnedFd, reaper: &Reaper) -> io::Result<(i32, std::sync::mpsc::Receiver<i32>)> {
    sys::mkdir("/var/log")?;
    let log = File::options().create(true).append(true).open(LOG)?;
    let fd = listener.as_raw_fd();
    let mut cmd = Command::new("/proc/self/exe");
    cmd.arg("nfsd")
        .stdin(Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log);
    // SAFETY: dup2 is async-signal-safe; the new descriptor has no CLOEXEC.
    unsafe {
        cmd.pre_exec(move || {
            if libc::dup2(fd, LISTEN_FD) < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    reaper.spawn(&mut cmd)
}

/// `/init nfsd`: serve NFS on the inherited listener until killed.
pub fn main() -> ! {
    // Modes from the Mac are applied as given.
    unsafe { libc::umask(0) };
    let listener = unsafe { OwnedFd::from_raw_fd(LISTEN_FD) };
    let server = Arc::new(kat_nfs::Server::new(kat_nfs::Layout::docker()));
    kat_nfs::stats::report_on_sigusr1();
    eprintln!("nfsd: serving on vsock port {NFS_PORT}");
    loop {
        match sys::accept(&listener) {
            Ok(conn) => {
                if let Err(e) = widen_window(&conn) {
                    eprintln!("nfsd: vsock buffer size: {e}");
                }
                let server = server.clone();
                let _ = thread::Builder::new()
                    .name("nfs-conn".into())
                    .spawn(move || {
                        if let Err(e) = kat_nfs::serve(server, UnixStream::from(conn)) {
                            eprintln!("nfsd: connection: {e}");
                        }
                    });
            }
            Err(e) => {
                eprintln!("nfsd: accept: {e}");
                std::process::exit(1);
            }
        }
    }
}

/// The connection's receive buffer, which is also the window the host may
/// send into before it waits for a credit update. The default 256 KiB holds a
/// quarter of a 1 MiB WRITE, and makes the host send a credit update for
/// every 128 KiB of READ data it relays.
const VSOCK_BUFFER: u64 = 4 << 20;

fn widen_window(conn: &OwnedFd) -> io::Result<()> {
    const SO_VM_SOCKETS_BUFFER_SIZE: libc::c_int = 0;
    const SO_VM_SOCKETS_BUFFER_MAX_SIZE: libc::c_int = 2;
    for opt in [SO_VM_SOCKETS_BUFFER_MAX_SIZE, SO_VM_SOCKETS_BUFFER_SIZE] {
        // SAFETY: setsockopt with a u64 value, as AF_VSOCK expects.
        let ret = unsafe {
            libc::setsockopt(
                conn.as_raw_fd(),
                libc::AF_VSOCK,
                opt,
                &VSOCK_BUFFER as *const u64 as *const libc::c_void,
                std::mem::size_of::<u64>() as libc::socklen_t,
            )
        };
        if ret != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}
