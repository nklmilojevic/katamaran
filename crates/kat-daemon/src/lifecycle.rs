//! Keeping a running VM healthy: the guest clock after sleep, a watchdog
//! for a hung guest, pausing across sleep, and restarting a VM process that
//! died.

use std::io;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use kat_proto::Msg;
use kat_proto::backoff::Backoff;

use crate::vm::unix_now_ns;

/// How long the host waits for the guest's ordered shutdown (about 30 s in
/// the worst case, see kat-init `shutdown.rs`) before killing the VM.
pub const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(45);

/// The host side of the control channel, shared by the threads that write
/// to it. Each message is written whole under the lock.
#[derive(Clone)]
pub struct Control(Arc<Mutex<UnixStream>>);

impl Control {
    pub fn new(conn: &UnixStream) -> io::Result<Control> {
        Ok(Control(Arc::new(Mutex::new(conn.try_clone()?))))
    }

    pub fn send(&self, msg: &Msg) -> io::Result<()> {
        kat_proto::write_msg(&mut *self.0.lock().unwrap(), msg)
    }
}

// ---------------------------------------------------------------------------
// Clock

/// How often the drift detector samples.
const CLOCK_CHECK: Duration = Duration::from_secs(5);
/// Wall time running ahead of the guest's clock by more than this means
/// the Mac slept (or its clock was set).
const CLOCK_DRIFT: Duration = Duration::from_secs(1);
/// Resync at least this often even without drift.
const CLOCK_PERIOD: Duration = Duration::from_secs(10 * 60);

/// Detects that the guest's clock fell behind. The guest counts time with
/// the same counter as the host's `CLOCK_UPTIME_RAW`, which stops while the
/// Mac sleeps; wall time does not. A gap between the two means the guest
/// needs `SetTime` (kiri's wall-vs-monotonic check).
#[derive(Debug)]
pub struct DriftDetector {
    last: Option<(u64, u64)>,
    threshold_ns: u64,
}

impl DriftDetector {
    pub fn new(threshold: Duration) -> DriftDetector {
        DriftDetector {
            last: None,
            threshold_ns: threshold.as_nanos() as u64,
        }
    }

    /// Feed a sample of wall and uptime clocks; returns the drift since the
    /// previous sample if it exceeds the threshold.
    pub fn check(&mut self, wall_ns: u64, uptime_ns: u64) -> Option<i64> {
        let prev = self.last.replace((wall_ns, uptime_ns));
        let (w0, u0) = prev?;
        let drift = (wall_ns as i128 - w0 as i128) - (uptime_ns as i128 - u0 as i128);
        let drift = drift.clamp(i64::MIN as i128, i64::MAX as i128) as i64;
        (drift.unsigned_abs() > self.threshold_ns).then_some(drift)
    }
}

fn uptime_ns() -> u64 {
    let mut ts: libc::timespec = unsafe { std::mem::zeroed() };
    unsafe { libc::clock_gettime(libc::CLOCK_UPTIME_RAW, &mut ts) };
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

/// Keeps the guest's wall clock right: on demand ([`ClockSync::resync`],
/// for example on wake), when drift is detected, and periodically.
pub struct ClockSync {
    nudge: Sender<&'static str>,
}

impl ClockSync {
    pub fn start(control: Control) -> io::Result<ClockSync> {
        let (nudge, rx) = mpsc::channel::<&'static str>();
        thread::Builder::new().name("clock".into()).spawn(move || {
            let mut detector = DriftDetector::new(CLOCK_DRIFT);
            detector.check(unix_now_ns(), uptime_ns());
            let mut synced = Instant::now();
            loop {
                let why = match rx.recv_timeout(CLOCK_CHECK) {
                    Ok(why) => Some(why.to_string()),
                    Err(RecvTimeoutError::Disconnected) => return,
                    Err(RecvTimeoutError::Timeout) => {
                        match detector.check(unix_now_ns(), uptime_ns()) {
                            Some(d) => Some(format!("drift of {:.1} s", d as f64 / 1e9)),
                            None if synced.elapsed() >= CLOCK_PERIOD => None,
                            None => continue,
                        }
                    }
                };
                if control
                    .send(&Msg::SetTime {
                        unix_ns: unix_now_ns(),
                    })
                    .is_err()
                {
                    return;
                }
                detector.check(unix_now_ns(), uptime_ns());
                synced = Instant::now();
                if let Some(why) = why {
                    println!("katamaran: guest clock synced ({why})");
                }
            }
        })?;
        Ok(ClockSync { nudge })
    }

    pub fn resync(&self, why: &'static str) {
        let _ = self.nudge.send(why);
    }
}

// ---------------------------------------------------------------------------
// Watchdog

const PING_EVERY: Duration = Duration::from_secs(5);
const HANG_AFTER: Duration = Duration::from_secs(20);

/// Tracks answers to pings. A guest that has not answered for `hang_after`
/// is reported once, and its recovery once.
#[derive(Debug)]
pub struct HangDetector {
    hang_after: Duration,
    last_answer: Instant,
    hung: bool,
}

impl HangDetector {
    pub fn new(hang_after: Duration, now: Instant) -> HangDetector {
        HangDetector {
            hang_after,
            last_answer: now,
            hung: false,
        }
    }

    /// `Some(silence)` the first time the guest has been silent too long.
    pub fn check(&mut self, now: Instant) -> Option<Duration> {
        let silent = now.saturating_duration_since(self.last_answer);
        if !self.hung && silent >= self.hang_after {
            self.hung = true;
            return Some(silent);
        }
        None
    }

    /// The guest answered. `Some(silence)` if it had been reported hung.
    pub fn answered(&mut self, now: Instant) -> Option<Duration> {
        let silent = now.saturating_duration_since(self.last_answer);
        self.last_answer = now;
        std::mem::replace(&mut self.hung, false).then_some(silent)
    }

    /// Forget silence that was expected (the VM was paused).
    pub fn reset(&mut self, now: Instant) {
        self.last_answer = now;
        self.hung = false;
    }
}

/// Pings the guest and logs when it stops answering.
pub struct Watchdog {
    detector: Arc<Mutex<HangDetector>>,
    paused: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
}

impl Watchdog {
    pub fn start(control: Control) -> io::Result<Watchdog> {
        let w = Watchdog {
            detector: Arc::new(Mutex::new(HangDetector::new(HANG_AFTER, Instant::now()))),
            paused: Arc::new(AtomicBool::new(false)),
            stop: Arc::new(AtomicBool::new(false)),
        };
        let (detector, paused, stop) = (w.detector.clone(), w.paused.clone(), w.stop.clone());
        thread::Builder::new()
            .name("watchdog".into())
            .spawn(move || {
                let mut seq = 0;
                while !stop.load(Ordering::SeqCst) {
                    thread::sleep(PING_EVERY);
                    if stop.load(Ordering::SeqCst) {
                        return;
                    }
                    if paused.load(Ordering::SeqCst) {
                        detector.lock().unwrap().reset(Instant::now());
                        continue;
                    }
                    seq += 1;
                    if control.send(&Msg::Ping { seq }).is_err() {
                        return;
                    }
                    if let Some(silent) = detector.lock().unwrap().check(Instant::now()) {
                        eprintln!("katamaran: guest not responding for {} s", silent.as_secs());
                    }
                }
            })?;
        Ok(w)
    }

    pub fn pong(&self) {
        if let Some(silent) = self.detector.lock().unwrap().answered(Instant::now()) {
            println!(
                "katamaran: guest responding again after {} s",
                silent.as_secs()
            );
        }
    }

    pub fn set_paused(&self, paused: bool) {
        self.paused.store(paused, Ordering::SeqCst);
        self.detector.lock().unwrap().reset(Instant::now());
    }

    pub fn stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        self.stop();
    }
}

// ---------------------------------------------------------------------------
// Pause and restart

/// Freeze or thaw the VM's vCPUs (handled in `kat_vmm::vmctl`). Returns
/// how long the signal took to deliver.
pub fn set_vm_paused(pid: i32, paused: bool) -> io::Result<Duration> {
    let t = Instant::now();
    let sig = if paused {
        kat_vmm::vmctl::PAUSE_SIGNAL
    } else {
        kat_vmm::vmctl::RESUME_SIGNAL
    };
    if pid <= 0 || unsafe { libc::kill(pid, sig) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(t.elapsed())
}

/// When to restart a VM process that died: 1 s doubling to 1 min, starting
/// over after 5 min of uptime, and giving up after 8 quick deaths in a row.
pub struct Restarts {
    backoff: Backoff,
    max: u32,
}

impl Default for Restarts {
    fn default() -> Restarts {
        Restarts {
            backoff: Backoff::new(
                Duration::from_secs(1),
                Duration::from_secs(60),
                Duration::from_secs(5 * 60),
            ),
            max: 8,
        }
    }
}

impl Restarts {
    /// The delay before the next start, or `None` to give up.
    pub fn next(&mut self, ran_for: Duration) -> Option<Duration> {
        let delay = self.backoff.next(ran_for);
        (self.backoff.failures() <= self.max).then_some(delay)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: u64 = 1_000_000_000;

    #[test]
    fn drift_needs_two_samples_and_a_gap() {
        let mut d = DriftDetector::new(Duration::from_secs(1));
        assert_eq!(d.check(1000 * S, 50 * S), None);
        // Both advanced 5 s: no drift.
        assert_eq!(d.check(1005 * S, 55 * S), None);
        // Wall advanced an hour while uptime advanced 5 s: the Mac slept.
        assert_eq!(d.check(4610 * S, 60 * S), Some(3600 * S as i64));
        // Wall stepped back (clock set): also a resync.
        assert_eq!(d.check(4600 * S, 65 * S), Some(-15 * S as i64));
        // Small slew is ignored.
        assert_eq!(d.check(4605 * S + S / 2, 70 * S), None);
    }

    #[test]
    fn hang_is_reported_once_and_recovery_once() {
        let t0 = Instant::now();
        let mut h = HangDetector::new(Duration::from_secs(20), t0);
        assert_eq!(h.check(t0 + Duration::from_secs(19)), None);
        assert_eq!(
            h.check(t0 + Duration::from_secs(20)),
            Some(Duration::from_secs(20))
        );
        assert_eq!(h.check(t0 + Duration::from_secs(25)), None);
        assert_eq!(
            h.answered(t0 + Duration::from_secs(30)),
            Some(Duration::from_secs(30))
        );
        assert_eq!(h.answered(t0 + Duration::from_secs(31)), None);
        assert_eq!(h.check(t0 + Duration::from_secs(50)), None);
    }

    #[test]
    fn pause_resets_the_hang_clock() {
        let t0 = Instant::now();
        let mut h = HangDetector::new(Duration::from_secs(20), t0);
        h.reset(t0 + Duration::from_secs(100));
        assert_eq!(h.check(t0 + Duration::from_secs(110)), None);
    }

    #[test]
    fn restarts_back_off_and_give_up() {
        let mut r = Restarts::default();
        let quick = Duration::from_secs(2);
        let delays: Vec<_> = (0..8).map(|_| r.next(quick).unwrap().as_secs()).collect();
        assert_eq!(delays, [1, 2, 4, 8, 16, 32, 60, 60]);
        assert_eq!(r.next(quick), None);
        // A long run starts over.
        assert_eq!(
            r.next(Duration::from_secs(600)),
            Some(Duration::from_secs(1))
        );
    }

    #[test]
    fn control_writes_whole_frames() {
        let (a, mut b) = UnixStream::pair().unwrap();
        let c = Control::new(&a).unwrap();
        // More than the socket buffer holds, so the reader runs alongside.
        let reader = thread::spawn(move || {
            let mut n = 0;
            while let Some(m) = kat_proto::read_msg(&mut b).unwrap() {
                assert!(matches!(m, Msg::Ping { .. }));
                n += 1;
            }
            n
        });
        let threads: Vec<_> = (0..8)
            .map(|i| {
                let c = c.clone();
                thread::spawn(move || {
                    for seq in 0..100 {
                        c.send(&Msg::Ping {
                            seq: i * 1000 + seq,
                        })
                        .unwrap();
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        drop((a, c));
        assert_eq!(reader.join().unwrap(), 800);
    }
}
