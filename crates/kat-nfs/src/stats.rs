//! Where the server's time goes, per procedure: how long a request waits
//! for a worker, how long it takes to answer (and how much of that is the
//! file read or write), and how long sending the reply takes (and how much
//! of that is the socket write rather than waiting for the writer). Also
//! how many requests a connection has in flight when a new one arrives,
//! which shows how far the client pipelines.
//!
//! Counting costs a few clock reads per request. `SIGUSR1` prints the
//! counts since the last report to stderr (`/var/log/nfsd.log`) and starts
//! over.

use std::fmt::Write as _;
use std::io;
use std::sync::LazyLock;
use std::sync::atomic::{
    AtomicI32, AtomicU64,
    Ordering::{Acquire, Relaxed, Release},
};
use std::thread;
use std::time::{Duration, Instant};

const NAMES: [&str; 23] = [
    "NULL",
    "GETATTR",
    "SETATTR",
    "LOOKUP",
    "ACCESS",
    "READLINK",
    "READ",
    "WRITE",
    "CREATE",
    "MKDIR",
    "SYMLINK",
    "MKNOD",
    "REMOVE",
    "RMDIR",
    "RENAME",
    "LINK",
    "READDIR",
    "READDIRPLUS",
    "FSSTAT",
    "FSINFO",
    "PATHCONF",
    "COMMIT",
    "other",
];
const OTHER: usize = NAMES.len() - 1;
pub const READ: usize = 6;
pub const WRITE: usize = 7;

#[derive(Default)]
struct Proc {
    calls: AtomicU64,
    queue_ns: AtomicU64,
    handle_ns: AtomicU64,
    io_ns: AtomicU64,
    send_ns: AtomicU64,
    write_ns: AtomicU64,
    reply_bytes: AtomicU64,
}

#[derive(Default)]
struct Stats {
    procs: [Proc; NAMES.len()],
    /// Sum over arrivals of the requests already in flight, and the most.
    inflight_sum: AtomicU64,
    inflight_max: AtomicU64,
}

static STATS: LazyLock<Stats> = LazyLock::new(Stats::default);
/// Write end of the pipe that wakes the report thread; -1 until it runs.
static WAKE: AtomicI32 = AtomicI32::new(-1);

/// The procedure slot of an NFS call record (xid, type, rpcvers, prog,
/// vers, proc); MOUNT and anything unparseable count as "other".
pub fn slot(record: &[u8]) -> usize {
    let word = |i: usize| {
        record
            .get(i..i + 4)
            .map(|b| u32::from_be_bytes(b.try_into().unwrap()))
    };
    match (word(12), word(20)) {
        (Some(crate::server::PROG_NFS), Some(p)) if (p as usize) < OTHER => p as usize,
        _ => OTHER,
    }
}

fn ns(d: Duration) -> u64 {
    d.as_nanos().min(u64::MAX as u128) as u64
}

/// A request arrived with `inflight` others not yet answered.
pub fn arrived(inflight: u64) {
    STATS.inflight_sum.fetch_add(inflight, Relaxed);
    STATS.inflight_max.fetch_max(inflight, Relaxed);
}

/// When a request reached each step: read off the socket, picked up by a
/// worker, answered, holding the connection's writer, reply sent.
pub struct Times {
    pub arrived: Instant,
    pub started: Instant,
    pub handled: Instant,
    pub locked: Instant,
    pub sent: Instant,
}

/// One request answered with a reply of `reply_bytes`.
pub fn answered(slot: usize, t: &Times, reply_bytes: usize) {
    let p = &STATS.procs[slot.min(OTHER)];
    p.calls.fetch_add(1, Relaxed);
    p.queue_ns.fetch_add(ns(t.started - t.arrived), Relaxed);
    p.handle_ns.fetch_add(ns(t.handled - t.started), Relaxed);
    p.send_ns.fetch_add(ns(t.sent - t.handled), Relaxed);
    p.write_ns.fetch_add(ns(t.sent - t.locked), Relaxed);
    p.reply_bytes.fetch_add(reply_bytes as u64, Relaxed);
}

/// File data read or written for a request of `slot`, since `since`.
pub fn io(slot: usize, since: Instant) {
    STATS.procs[slot.min(OTHER)]
        .io_ns
        .fetch_add(ns(since.elapsed()), Relaxed);
}

/// The counts since the last report, as a table; and start over.
pub fn report() -> String {
    let mut out = String::new();
    let mut total = 0;
    let _ = writeln!(
        out,
        "{:<12} {:>8} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
        "proc", "calls", "queue_us", "handle_us", "io_us", "send_us", "write_us", "MiB"
    );
    for (name, p) in NAMES.iter().zip(&STATS.procs) {
        let calls = p.calls.swap(0, Relaxed);
        let (q, h, i, s, w, b) = (
            p.queue_ns.swap(0, Relaxed),
            p.handle_ns.swap(0, Relaxed),
            p.io_ns.swap(0, Relaxed),
            p.send_ns.swap(0, Relaxed),
            p.write_ns.swap(0, Relaxed),
            p.reply_bytes.swap(0, Relaxed),
        );
        if calls == 0 {
            continue;
        }
        total += calls;
        let avg = |v: u64| v as f64 / calls as f64 / 1000.0;
        let _ = writeln!(
            out,
            "{name:<12} {calls:>8} {:>9.1} {:>9.1} {:>9.1} {:>9.1} {:>9.1} {:>9.1}",
            avg(q),
            avg(h),
            avg(i),
            avg(s),
            avg(w),
            b as f64 / 1048576.0
        );
    }
    let sum = STATS.inflight_sum.swap(0, Relaxed);
    let max = STATS.inflight_max.swap(0, Relaxed);
    let _ = writeln!(
        out,
        "in flight at arrival: mean {:.2}, max {max} (averages per call)",
        if total > 0 {
            sum as f64 / total as f64
        } else {
            0.0
        }
    );
    out
}

#[cfg(target_os = "linux")]
unsafe fn errno() -> *mut libc::c_int {
    unsafe { libc::__errno_location() }
}

#[cfg(not(target_os = "linux"))]
unsafe fn errno() -> *mut libc::c_int {
    unsafe { libc::__error() }
}

extern "C" fn on_usr1(_: libc::c_int) {
    // SAFETY: save and restore thread-local errno across async-signal-safe write().
    let saved = unsafe { *errno() };
    let fd = WAKE.load(Acquire);
    if fd >= 0 {
        // SAFETY: write() is async-signal-safe and the byte is on the stack.
        unsafe { libc::write(fd, [1u8].as_ptr().cast(), 1) };
    }
    unsafe { *errno() = saved };
}

/// Print [`report`] to stderr whenever the process gets `SIGUSR1`. The
/// thread sleeps on a pipe the handler writes to, so an idle server does
/// not wake up for it.
pub fn report_on_sigusr1() {
    let mut fds = [0; 2];
    // SAFETY: pipe() fills the two descriptors.
    if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
        return;
    }
    let [read_fd, write_fd] = fds;
    for fd in fds {
        // SAFETY: the descriptor was just created.
        unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) };
    }
    // SAFETY: the descriptor was just created; a full pipe only drops wakeups.
    unsafe { libc::fcntl(write_fd, libc::F_SETFL, libc::O_NONBLOCK) };
    WAKE.store(write_fd, Release);
    // SAFETY: the handler only writes to the pipe.
    unsafe { libc::signal(libc::SIGUSR1, on_usr1 as *const () as libc::sighandler_t) };
    let _ = thread::Builder::new()
        .name("nfs-stats".into())
        .spawn(move || {
            let mut buf = [0u8; 64];
            loop {
                // SAFETY: reads into a local buffer of that length.
                let n = unsafe { libc::read(read_fd, buf.as_mut_ptr().cast(), buf.len()) };
                if n > 0 {
                    eprint!("nfsd: stats\n{}", report());
                } else if n == 0 || io::Error::last_os_error().kind() != io::ErrorKind::Interrupted
                {
                    return;
                }
            }
        });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slots_and_report() {
        let cred = crate::rpc::Cred { uid: 1, gid: 1 };
        let read = crate::rpc::call(1, crate::server::PROG_NFS, 3, 6, cred, &[]);
        assert_eq!(slot(&read[4..]), READ);
        let mount = crate::rpc::call(1, crate::server::PROG_MOUNT, 3, 1, cred, &[]);
        assert_eq!(slot(&mount[4..]), OTHER);
        assert_eq!(slot(&[0; 8]), OTHER);

        let t = Instant::now();
        arrived(3);
        let us = |n| t + Duration::from_micros(n);
        let times = Times {
            arrived: t,
            started: t,
            handled: us(10),
            locked: us(12),
            sent: us(15),
        };
        answered(READ, &times, 1 << 20);
        let r = report();
        assert!(r.contains("READ"), "{r}");
        assert!(r.contains("max 3"), "{r}");
        assert!(!report().contains("READ "));
    }
}
