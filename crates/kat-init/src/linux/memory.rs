//! Give memory the guest does not need back to the Mac.
//!
//! The host attaches a virtio balloon with free page reporting: the kernel
//! reports free blocks of 16 KiB and up (`page_reporting_order` 2, the Mac's
//! page size) a couple of seconds after they appear. libkrun replaces whole
//! 64 KiB blocks with fresh memory and gives smaller ones back with
//! `MADV_FREE` while they are unmapped from the guest; both release them on
//! macOS. Reporting only 2 MiB
//! blocks left every free page next to a used one on the Mac: after a
//! `docker load`, over a gigabyte of free guest memory in smaller blocks,
//! which compaction did not merge. Two things keep free memory from being
//! reported:
//!
//! - **Page cache.** Files the guest read or wrote stay cached until memory
//!   runs short, which inside a VM that the Mac sized generously is never.
//!   Once the cache has stopped growing for a while, it is trimmed to a
//!   budget with the root cgroup's `memory.reclaim`, which takes the coldest
//!   pages first (MGLRU). The data disk's blocks stay in the Mac's own file
//!   cache, so a page the guest drops is cheap to read again.
//! - **Fragmentation.** Free 4 KiB pages that are not part of a free 16 KiB
//!   block are never reported. When much free memory sits in those, the
//!   kernel is asked to compact. Proactive compaction stays off: it moves
//!   pages into memory that was already given back, which the Mac then has
//!   to supply again, and wakes kcompactd twice a second.
//!
//! The controller samples once a second and does nothing while the guest is
//! under memory pressure (PSI), when the kernel is reclaiming by itself.

use std::fs;
use std::thread;
use std::time::{Duration, Instant};

const MIB: u64 = 1 << 20;
const TICK: Duration = Duration::from_secs(2);
/// 64 KiB on 4 KiB pages: matches libkrun's `RECLAIM_BLOCK_SIZE` so every
/// reported block is replaced outright and smaller fragmented blocks trigger
/// compaction instead of lingering in macOS's physical footprint.
const REPORTING_ORDER: usize = 4;
const PAGE: u64 = 4096;

/// Kernel settings for prompt reporting, applied once at boot.
pub fn tune() {
    let settings = [
        (
            "/sys/module/page_reporting/parameters/page_reporting_order",
            REPORTING_ORDER.to_string(),
        ),
        ("/proc/sys/vm/compaction_proactiveness", "0".into()),
    ];
    for (path, value) in settings {
        if let Err(e) = fs::write(path, &value) {
            super::log(&format!("memory: {path} = {value}: {e}"));
        }
    }
}

/// Start the controller thread.
pub fn start() {
    let r = thread::Builder::new()
        .name("memory".into())
        .spawn(|| run(Policy::for_total(read_sample().map_or(0, |s| s.total))));
    if let Err(e) = r {
        super::log(&format!("memory controller not started: {e}"));
    }
}

fn run(policy: Policy) -> ! {
    let mut state = State::default();
    loop {
        thread::sleep(TICK);
        let Some(sample) = read_sample() else {
            continue;
        };
        for action in policy.decide(&sample, &mut state, Instant::now()) {
            let t = Instant::now();
            let r = match action {
                Action::Reclaim(bytes) => {
                    fs::write("/sys/fs/cgroup/memory.reclaim", bytes.to_string())
                }
                Action::Compact => fs::write("/proc/sys/vm/compact_memory", "1"),
            };
            // EAGAIN means the kernel reclaimed less than asked; the rest is
            // in use, which the next sample shows.
            match r {
                Err(e) if e.raw_os_error() != Some(libc::EAGAIN) => {
                    super::log(&format!("memory: {action:?}: {e}"))
                }
                _ if t.elapsed() > Duration::from_millis(200) => super::log(&format!(
                    "memory: {action:?} took {} ms",
                    t.elapsed().as_millis()
                )),
                _ => {}
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Write this many bytes to the root cgroup's `memory.reclaim`.
    Reclaim(u64),
    /// Compact all zones.
    Compact,
}

/// What the controller looks at, in bytes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Sample {
    pub total: u64,
    pub free: u64,
    /// File-backed pages on the LRU lists: `Active(file) + Inactive(file)`.
    pub file: u64,
    /// Free memory in blocks too small to be reported.
    pub fragmented: u64,
    /// PSI `some avg10` for memory, in hundredths of a percent.
    pub pressure: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Policy {
    /// File cache kept when idle.
    pub cache_budget: u64,
    /// The cache counts as idle once it has not grown by more than `growth`
    /// for `settle`.
    pub settle: Duration,
    pub growth: u64,
    /// At most this much is reclaimed per step, so a running workload that
    /// starts reading again loses little.
    pub step: u64,
    pub reclaim_every: Duration,
    /// Compact when this much free memory is fragmented.
    pub fragmented: u64,
    pub compact_every: Duration,
    /// Do nothing at or above this PSI `some avg10` (hundredths of a percent).
    pub max_pressure: u32,
}

impl Policy {
    /// A cache of 1/128 of memory, between 96 and 128 MiB (enough to keep
    /// dockerd and containerd mapped binaries hot without holding cold disk
    /// cache that macOS already caches for the data disk).
    pub fn for_total(total: u64) -> Policy {
        Policy {
            cache_budget: (total / 128).clamp(96 * MIB, 128 * MIB),
            settle: Duration::from_secs(10),
            growth: 32 * MIB,
            step: 1024 * MIB,
            reclaim_every: Duration::from_secs(2),
            fragmented: 64 * MIB,
            compact_every: Duration::from_secs(30),
            max_pressure: 100,
        }
    }

    pub fn decide(&self, s: &Sample, st: &mut State, now: Instant) -> Vec<Action> {
        // Track when the cache last grew noticeably.
        match st.file_mark {
            Some((mark, _)) if s.file <= mark + self.growth => {}
            _ => st.file_mark = Some((s.file, now)),
        }
        // Lower the mark as the cache shrinks, so growth is measured from
        // the lowest point.
        if let Some((mark, since)) = st.file_mark
            && s.file < mark
        {
            st.file_mark = Some((s.file, since));
        }
        if s.pressure >= self.max_pressure {
            return Vec::new();
        }
        let mut actions = Vec::new();
        let quiet = st
            .file_mark
            .is_some_and(|(_, since)| now.duration_since(since) >= self.settle);
        let due =
            |last: Option<Instant>, every| last.is_none_or(|t| now.duration_since(t) >= every);
        if quiet && s.file > self.cache_budget && due(st.last_reclaim, self.reclaim_every) {
            actions.push(Action::Reclaim((s.file - self.cache_budget).min(self.step)));
            st.last_reclaim = Some(now);
        }
        // Lower the baseline as fragmentation or free memory drops, so a
        // high-water spike cannot permanently suppress later compactions.
        if let Some((prev_frag, prev_free)) = st.last_compact_stats {
            st.last_compact_stats = Some((prev_frag.min(s.fragmented), prev_free.min(s.free)));
        }
        // Avoid compacting in a loop when unmovable slabs pin scattered pages:
        // once compacted, wait until fragmented or free memory grew again.
        let compact_worthwhile = match st.last_compact_stats {
            None => true,
            Some((prev_frag, prev_free)) => {
                s.fragmented >= prev_frag + 32 * MIB || s.free >= prev_free + 64 * MIB
            }
        };
        if s.fragmented >= self.fragmented
            && compact_worthwhile
            && due(st.last_compact, self.compact_every)
        {
            actions.push(Action::Compact);
            st.last_compact = Some(now);
            st.last_compact_stats = Some((s.fragmented, s.free));
        }
        actions
    }
}

#[derive(Debug, Default)]
pub struct State {
    /// The file cache size when it last grew, and when.
    file_mark: Option<(u64, Instant)>,
    last_reclaim: Option<Instant>,
    last_compact: Option<Instant>,
    /// `(fragmented, free)` at the last compaction, to avoid idle re-compaction.
    last_compact_stats: Option<(u64, u64)>,
}

fn read_sample() -> Option<Sample> {
    let meminfo = fs::read_to_string("/proc/meminfo").ok()?;
    let buddy = fs::read_to_string("/proc/buddyinfo").unwrap_or_default();
    let psi = fs::read_to_string("/proc/pressure/memory").unwrap_or_default();
    Some(parse_sample(&meminfo, &buddy, &psi))
}

pub fn parse_sample(meminfo: &str, buddyinfo: &str, psi: &str) -> Sample {
    let kib = |key: &str| -> u64 {
        meminfo
            .lines()
            .find_map(|l| l.strip_prefix(key)?.strip_prefix(':'))
            .and_then(|v| v.trim().trim_end_matches("kB").trim().parse::<u64>().ok())
            .unwrap_or(0)
            * 1024
    };
    let free = kib("MemFree");
    Sample {
        total: kib("MemTotal"),
        free,
        file: kib("Active(file)") + kib("Inactive(file)") + kib("SReclaimable"),
        fragmented: free.saturating_sub(reportable(buddyinfo)),
        pressure: parse_psi_some_avg10(psi),
    }
}

/// Free bytes in blocks of at least the reporting order, from
/// `/proc/buddyinfo` (one line per zone, free block counts by order).
fn reportable(buddyinfo: &str) -> u64 {
    buddyinfo
        .lines()
        .filter_map(|l| l.split_once("zone"))
        .flat_map(|(_, rest)| {
            rest.split_whitespace()
                .skip(1)
                .enumerate()
                .filter(|(order, _)| *order >= REPORTING_ORDER)
                .filter_map(|(order, n)| Some(n.parse::<u64>().ok()? * (PAGE << order)))
        })
        .sum()
}

/// `some avg10=1.23 ...` → 123.
fn parse_psi_some_avg10(psi: &str) -> u32 {
    psi.lines()
        .find_map(|l| l.strip_prefix("some "))
        .and_then(|l| l.split_whitespace().find_map(|f| f.strip_prefix("avg10=")))
        .and_then(|v| v.parse::<f64>().ok())
        .map_or(0, |v| (v * 100.0).round() as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1 << 30;

    fn policy() -> Policy {
        Policy::for_total(4 * GIB)
    }

    fn sample(file: u64) -> Sample {
        Sample {
            total: 4 * GIB,
            free: GIB,
            file,
            fragmented: 0,
            pressure: 0,
        }
    }

    #[test]
    fn budget_scales_with_memory() {
        assert_eq!(Policy::for_total(2 * GIB).cache_budget, 96 * MIB);
        assert_eq!(Policy::for_total(14 * GIB).cache_budget, 112 * MIB);
        assert_eq!(Policy::for_total(64 * GIB).cache_budget, 128 * MIB);
    }

    #[test]
    fn reclaims_idle_cache_down_to_budget_in_steps() {
        let p = policy();
        let mut st = State::default();
        let t0 = Instant::now();
        let s = sample(3 * GIB);
        assert!(p.decide(&s, &mut st, t0).is_empty(), "not settled yet");
        assert!(
            p.decide(&s, &mut st, t0 + Duration::from_secs(9))
                .is_empty()
        );
        assert_eq!(
            p.decide(&s, &mut st, t0 + Duration::from_secs(10)),
            vec![Action::Reclaim(GIB)],
            "a step at most"
        );
        assert!(
            p.decide(&s, &mut st, t0 + Duration::from_secs(11))
                .is_empty(),
            "rate limited"
        );
        let s = sample(p.cache_budget + 100 * MIB);
        assert_eq!(
            p.decide(&s, &mut st, t0 + Duration::from_secs(12)),
            vec![Action::Reclaim(100 * MIB)],
            "shrinking keeps the cache settled"
        );
        let s = sample(p.cache_budget);
        assert!(
            p.decide(&s, &mut st, t0 + Duration::from_secs(20))
                .is_empty()
        );
    }

    #[test]
    fn growing_cache_is_left_alone() {
        let p = policy();
        let mut st = State::default();
        let t0 = Instant::now();
        for i in 0..30u64 {
            let s = sample(GIB + i * 64 * MIB);
            assert!(
                p.decide(&s, &mut st, t0 + Duration::from_secs(i))
                    .is_empty()
            );
        }
        // Small growth does not reset the clock.
        let s = sample(GIB + 29 * 64 * MIB + 16 * MIB);
        assert!(
            p.decide(&s, &mut st, t0 + Duration::from_secs(35))
                .is_empty()
        );
        assert!(
            !p.decide(&s, &mut st, t0 + Duration::from_secs(39))
                .is_empty()
        );
    }

    #[test]
    fn pressure_blocks_everything() {
        let p = policy();
        let mut st = State::default();
        let t0 = Instant::now();
        let mut s = sample(3 * GIB);
        s.fragmented = GIB;
        s.pressure = 250;
        p.decide(&s, &mut st, t0);
        assert!(
            p.decide(&s, &mut st, t0 + Duration::from_secs(60))
                .is_empty()
        );
        s.pressure = 0;
        assert_eq!(
            p.decide(&s, &mut st, t0 + Duration::from_secs(61)),
            vec![Action::Reclaim(GIB), Action::Compact]
        );
    }

    #[test]
    fn compacts_fragmented_memory_rate_limited() {
        let p = policy();
        let mut st = State::default();
        let t0 = Instant::now();
        let mut s = sample(0);
        s.fragmented = 32 * MIB;
        assert!(p.decide(&s, &mut st, t0).is_empty());
        s.fragmented = 200 * MIB;
        assert_eq!(p.decide(&s, &mut st, t0), vec![Action::Compact]);
        assert!(
            p.decide(&s, &mut st, t0 + Duration::from_secs(29))
                .is_empty()
        );
        // Unchanged fragmentation does not compact again in an idle loop.
        assert!(
            p.decide(&s, &mut st, t0 + Duration::from_secs(30))
                .is_empty()
        );
        // More fragmented memory after the cooldown triggers compaction again.
        s.fragmented = 240 * MIB;
        assert_eq!(
            p.decide(&s, &mut st, t0 + Duration::from_secs(30)),
            vec![Action::Compact]
        );
    }

    #[test]
    fn compaction_recovers_after_high_water_spike() {
        let p = policy();
        let mut st = State::default();
        let t0 = Instant::now();
        let mut s = sample(0);
        // High-water spike compacts at 500 MiB fragmented, 2 GiB free.
        s.fragmented = 500 * MIB;
        s.free = 2 * GIB;
        assert_eq!(p.decide(&s, &mut st, t0), vec![Action::Compact]);
        // Fragmentation and free memory drop after compaction / workload activity.
        s.fragmented = 20 * MIB;
        s.free = 512 * MIB;
        assert!(
            p.decide(&s, &mut st, t0 + Duration::from_secs(10))
                .is_empty()
        );
        // Later fragmentation grows to 100 MiB (well below the old 500 MiB spike,
        // but above the 64 MiB threshold and +32 MiB above the lowered 20 MiB baseline).
        s.fragmented = 100 * MIB;
        assert_eq!(
            p.decide(&s, &mut st, t0 + Duration::from_secs(30)),
            vec![Action::Compact]
        );
    }

    #[test]
    fn parses_proc_files() {
        let meminfo = "MemTotal:        4102520 kB\nMemFree:         3276380 kB\n\
            Cached:           682924 kB\nActive(file):     600000 kB\nInactive(file):    82924 kB\n\
            SReclaimable:      10000 kB\n";
        let buddy = "Node 0, zone      DMA      3      1      1      2      1      2      4      3      1      1    414 \n\
            Node 0, zone   Normal    296    159    151    119     84     66     23     14      7     20    364 \n";
        let psi = "some avg10=1.25 avg60=0.00 avg300=0.00 total=10\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n";
        let s = parse_sample(meminfo, buddy, psi);
        assert_eq!(s.total, 4102520 * 1024);
        assert_eq!(s.file, (682924 + 10000) * 1024);
        assert_eq!(s.pressure, 125);
        // Orders 4 and up (64 KiB and up), per zone.
        let kib = |counts: [u64; 7]| -> u64 {
            counts
                .iter()
                .enumerate()
                .map(|(i, n)| n * (64 << i))
                .sum::<u64>()
                * 1024
        };
        let reportable = kib([1, 2, 4, 3, 1, 1, 414]) + kib([84, 66, 23, 14, 7, 20, 364]);
        assert_eq!(s.fragmented, 3276380 * 1024 - reportable);
    }
}
