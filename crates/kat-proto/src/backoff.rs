//! Restart delays for supervised processes, shared by the host (the VM
//! process) and the guest (containerd, dockerd).

use std::time::Duration;

/// Exponential backoff that resets once a run has lasted `stable`.
#[derive(Debug, Clone)]
pub struct Backoff {
    initial: Duration,
    max: Duration,
    stable: Duration,
    last: Option<Duration>,
    failures: u32,
}

impl Backoff {
    pub fn new(initial: Duration, max: Duration, stable: Duration) -> Backoff {
        Backoff {
            initial,
            max,
            stable,
            last: None,
            failures: 0,
        }
    }

    /// The delay before the next start, given how long the last run lasted.
    /// A run of at least `stable` counts as healthy and starts over at
    /// `initial`; shorter runs double the delay up to `max`.
    pub fn next(&mut self, ran_for: Duration) -> Duration {
        if ran_for >= self.stable {
            self.last = None;
            self.failures = 0;
        }
        let delay = match self.last {
            None => self.initial,
            Some(d) => d.saturating_mul(2).min(self.max),
        };
        self.last = Some(delay);
        self.failures += 1;
        delay
    }

    /// Restarts since the last stable run.
    pub fn failures(&self) -> u32 {
        self.failures
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: Duration = Duration::from_millis(1);

    #[test]
    fn doubles_up_to_max() {
        let mut b = Backoff::new(100 * MS, 1000 * MS, 60_000 * MS);
        let delays: Vec<_> = (0..6).map(|_| b.next(10 * MS)).collect();
        assert_eq!(
            delays,
            [100, 200, 400, 800, 1000, 1000].map(|n| n * MS).to_vec()
        );
        assert_eq!(b.failures(), 6);
    }

    #[test]
    fn stable_run_resets() {
        let mut b = Backoff::new(100 * MS, 1000 * MS, 5000 * MS);
        b.next(Duration::ZERO);
        b.next(Duration::ZERO);
        assert_eq!(b.next(Duration::ZERO), 400 * MS);
        assert_eq!(b.next(5000 * MS), 100 * MS);
        assert_eq!(b.failures(), 1);
        assert_eq!(b.next(4999 * MS), 200 * MS);
    }

    #[test]
    fn huge_delays_saturate() {
        let big = Duration::from_secs(u64::MAX / 2 + 1);
        let mut b = Backoff::new(big, Duration::MAX, Duration::MAX);
        b.next(Duration::ZERO);
        assert_eq!(b.next(Duration::ZERO), Duration::MAX);
        assert_eq!(b.next(Duration::ZERO), Duration::MAX);
    }
}
