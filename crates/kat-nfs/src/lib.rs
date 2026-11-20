//! katamaran's NFSv3 server: the guest's Docker volumes and container
//! filesystems, exported read-write to the Mac's kernel NFS client.
//!
//! It runs inside the guest and answers on a vsock port; libkrun relays a
//! Unix socket on the Mac to it, and the Mac mounts that socket with
//! `mount_nfs -o proto=ticotsord`. MOUNT and NFS share the one connection,
//! so no portmapper is involved.

pub mod docker;
pub mod fsops;
pub mod layout;
pub mod nodes;
pub mod rpc;
pub mod server;
pub mod stats;
pub mod xdr;

use std::io;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

pub use layout::Layout;
pub use server::Server;

/// Requests worked on at once per connection. The Mac's client keeps up to
/// `readahead` READs in flight on one connection.
pub const WORKERS: usize = 8;

/// Serve one client connection until it closes. Requests are read in order
/// and answered by a small pool, so slow requests do not hold up others;
/// replies go out whole, in whatever order they finish. A quick request
/// that arrives alone, with nothing in flight and nothing more read, is
/// answered by the reader itself: the Mac's client walks small files one
/// call at a time, and handing each call to a worker costs a thread
/// wake-up as long as answering it. Requests that come in a stream (READs
/// with read-ahead) find others in flight or buffered and go to the pool.
///
/// READ replies and WRITE requests are up to 1 MiB, and a fresh buffer that
/// size costs a mapping, page faults and zeroing every time: each thread
/// keeps its reply buffer, and request buffers go back to the reader.
pub fn serve(server: Arc<Server>, stream: UnixStream) -> io::Result<()> {
    let (tx, rx) = mpsc::sync_channel::<(Vec<u8>, Instant)>(WORKERS * 2);
    let rx = Arc::new(Mutex::new(rx));
    let (spare_tx, spare_rx) = mpsc::sync_channel::<Vec<u8>>(WORKERS);
    let conn = Arc::new(Conn {
        server,
        writer: Mutex::new(stream.try_clone()?),
        inflight: AtomicU64::new(0),
        spare: spare_tx,
    });
    let mut workers = Vec::with_capacity(WORKERS);
    for i in 0..WORKERS {
        let (conn, rx) = (conn.clone(), rx.clone());
        workers.push(
            thread::Builder::new()
                .name(format!("nfs-{i}"))
                .spawn(move || {
                    let mut reply_buf = Vec::new();
                    loop {
                        let (record, arrived) = match rx.lock().unwrap().recv() {
                            Ok(r) => r,
                            Err(_) => return,
                        };
                        if conn.answer(record, arrived, &mut reply_buf).is_err() {
                            return;
                        }
                    }
                })?,
        );
    }
    let mut reader = io::BufReader::with_capacity(256 << 10, stream);
    let mut reply_buf = Vec::new();
    let result = loop {
        let mut record = spare_rx.try_recv().unwrap_or_default();
        match rpc::read_record(&mut reader, &mut record) {
            Ok(Some(())) => {
                let ahead = conn.inflight.fetch_add(1, Ordering::Relaxed);
                stats::arrived(ahead);
                let arrived = Instant::now();
                if ahead == 0 && reader.buffer().is_empty() && quick(&record) {
                    if let Err(e) = conn.answer(record, arrived, &mut reply_buf) {
                        break Err(e);
                    }
                } else if tx.send((record, arrived)).is_err() {
                    break Ok(());
                }
            }
            Ok(None) => break Ok(()),
            Err(e) => break Err(e),
        }
    };
    drop(tx);
    for w in workers {
        let _ = w.join();
    }
    result
}

/// What the threads serving one connection share.
struct Conn {
    server: Arc<Server>,
    writer: Mutex<UnixStream>,
    /// Requests read and not yet answered.
    inflight: AtomicU64,
    /// Request buffers on their way back to the reader.
    spare: mpsc::SyncSender<Vec<u8>>,
}

impl Conn {
    /// Answer one request, building the reply in `reply_buf`.
    fn answer(&self, record: Vec<u8>, arrived: Instant, reply_buf: &mut Vec<u8>) -> io::Result<()> {
        let started = Instant::now();
        let reply = self.server.handle_in(&record, std::mem::take(reply_buf));
        let handled = Instant::now();
        let len = reply.as_ref().map_or(0, Vec::len);
        let mut locked = handled;
        if let Some(mut reply) = reply {
            let mut w = self.writer.lock().unwrap();
            locked = Instant::now();
            rpc::write_record(&mut *w, &mut reply)?;
            *reply_buf = reply;
        }
        self.inflight.fetch_sub(1, Ordering::Relaxed);
        let times = stats::Times {
            arrived,
            started,
            handled,
            locked,
            sent: Instant::now(),
        };
        stats::answered(stats::slot(&record), &times, len);
        let _ = self.spare.try_send(record);
        Ok(())
    }
}

/// A request the reader may answer itself: lookups, attributes, access
/// checks and READs, which come from the guest's caches. Calls that change
/// or list directories, and WRITEs, which wait for the disk, go to the pool.
fn quick(record: &[u8]) -> bool {
    match rpc::parse_call(record) {
        Ok(call) if call.prog == server::PROG_NFS && call.vers == 3 => {
            // NULL, GETATTR, LOOKUP, ACCESS, READLINK, READ, FSSTAT, FSINFO,
            // PATHCONF.
            matches!(call.proc_, 0 | 1 | 3 | 4 | 5 | 6 | 18 | 19 | 20)
        }
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nfs_call(proc_: u32, args: &[u8]) -> Vec<u8> {
        let cred = rpc::Cred { uid: 1, gid: 1 };
        rpc::call(1, server::PROG_NFS, 3, proc_, cred, args)[4..].to_vec()
    }

    #[test]
    fn quick_requests() {
        assert!(quick(&nfs_call(1, &[])), "GETATTR");
        assert!(quick(&nfs_call(6, &[])), "READ");
        assert!(!quick(&nfs_call(7, &[])), "WRITE");
        assert!(!quick(&nfs_call(17, &[])), "READDIRPLUS");
        assert!(quick(&[0, 0]), "garbage, answered at once");
    }
}
