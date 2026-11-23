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

use std::ffi::CString;
use std::fs;
use std::io::{self, Write};
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

const MIB: u64 = 1 << 20;
const TICK: Duration = Duration::from_secs(2);
/// 64 KiB on 4 KiB pages: matches libkrun's `RECLAIM_BLOCK_SIZE` so every
/// reported block is replaced outright and smaller fragmented blocks trigger
/// compaction instead of lingering in macOS's physical footprint.
const REPORTING_ORDER: usize = 4;
const PAGE: u64 = 4096;

const ZRAM_DEV: &str = "/dev/zram0";
const ZRAM_SYSFS: &str = "/sys/block/zram0";
/// Minimum zram disk size (16 MiB) so tiny test configs never create a tiny
/// swap area.
const ZRAM_MIN_BYTES: u64 = 16 * MIB;
const SWAP_FLAG_PREFER: libc::c_int = 0x8000;
const SWAP_FLAG_PRIO_MASK: libc::c_int = 0x7fff;
const SWAP_FLAG_DISCARD: libc::c_int = 0x10000;
const SWAP_FLAG_DISCARD_PAGES: libc::c_int = 0x40000;
const ZRAM_SWAP_PRIO: libc::c_int = 100;

/// Kernel settings for prompt reporting and compressed zram swap, applied once
/// at boot.
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
    match setup_zram_swap() {
        Ok(Some(bytes)) => {
            // zram is compressed RAM with no seek cost: disable multi-page
            // swap readahead and treat cold anonymous pages equally with file
            // cache under pressure so workloads do not thrash executable/metadata
            // pages down to zero before swapping idle anonymous memory.
            for (path, value) in [
                ("/proc/sys/vm/page-cluster", "0"),
                ("/proc/sys/vm/swappiness", "100"),
            ] {
                if let Err(e) = fs::write(path, value) {
                    super::log(&format!("memory: {path} = {value}: {e}"));
                }
            }
            super::log(&format!(
                "memory: enabled {} MiB lz4 zram swap on {ZRAM_DEV}",
                bytes / MIB
            ));
        }
        Ok(None) => {}
        Err(e) => super::log(&format!("memory: zram swap not enabled: {e}")),
    }
}

/// Target zram virtual swap size: 100% of guest RAM (matching OrbStack),
/// aligned down to 4 KiB pages and clamped to the Linux v1 swap header limit.
pub fn zram_disksize(total_bytes: u64) -> Option<u64> {
    let max_bytes = u64::from(u32::MAX) * PAGE;
    let aligned = (total_bytes.min(max_bytes) / PAGE) * PAGE;
    (aligned >= ZRAM_MIN_BYTES).then_some(aligned)
}

/// Build a Linux v1 swap header page (`union swap_header` in `include/linux/swap.h`)
/// for a swap area of `size_bytes` bytes (4 KiB pages).
pub fn swap_header(size_bytes: u64) -> Option<[u8; PAGE as usize]> {
    let pages = size_bytes / PAGE;
    if pages < 10 || pages > u64::from(u32::MAX) {
        return None;
    }
    let mut page = [0u8; PAGE as usize];
    // `version = 1` at offset 1024, `last_page = pages - 1` at offset 1028,
    // `nr_badpages = 0` at offset 1032, `SWAPSPACE2` magic at PAGE_SIZE - 10.
    page[1024..1028].copy_from_slice(&1u32.to_ne_bytes());
    page[1028..1032].copy_from_slice(&((pages - 1) as u32).to_ne_bytes());
    page[PAGE as usize - 10..].copy_from_slice(b"SWAPSPACE2");
    Some(page)
}

/// Configure `/dev/zram0` as an LZ4 compressed swap device sized to guest RAM.
/// Returns `Ok(None)` when the kernel has no zram device or it is already sized.
fn setup_zram_swap() -> io::Result<Option<u64>> {
    let disksize_path = format!("{ZRAM_SYSFS}/disksize");
    if !Path::new(&disksize_path).exists() || !Path::new(ZRAM_DEV).exists() {
        return Ok(None);
    }
    if fs::read_to_string(&disksize_path)
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .is_some_and(|n| n > 0)
    {
        return Ok(None);
    }
    let total = read_sample()
        .map(|s| s.total)
        .ok_or_else(|| io::Error::other("could not read MemTotal"))?;
    let Some(size) = zram_disksize(total) else {
        return Ok(None);
    };
    let comp_path = format!("{ZRAM_SYSFS}/comp_algorithm");
    if let Err(e) = fs::write(&comp_path, "lz4") {
        super::log(&format!("memory: {comp_path} = lz4: {e}"));
    }
    fs::write(&disksize_path, size.to_string())?;
    let header = swap_header(size).ok_or_else(|| io::Error::other("invalid zram swap size"))?;
    {
        let mut dev = fs::OpenOptions::new().write(true).open(ZRAM_DEV)?;
        dev.write_all(&header)?;
        dev.flush()?;
    }
    let dev_c = CString::new(ZRAM_DEV)?;
    let flags = SWAP_FLAG_PREFER
        | (ZRAM_SWAP_PRIO & SWAP_FLAG_PRIO_MASK)
        | SWAP_FLAG_DISCARD
        | SWAP_FLAG_DISCARD_PAGES;
    if unsafe { libc::swapon(dev_c.as_ptr(), flags) } < 0 {
        let e = io::Error::last_os_error();
        let _ = fs::write(format!("{ZRAM_SYSFS}/reset"), "1");
        return Err(e);
    }
    Ok(Some(size))
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
                    // Trim only idle file/slab cache (`swappiness=0`) so proactive
                    // cache trimming never pushes active container anonymous pages
                    // into zram; natural memory pressure still swaps via vm.swappiness.
                    fs::write(
                        "/sys/fs/cgroup/memory.reclaim",
                        format!("{bytes} swappiness=0"),
                    )
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

    #[test]
    fn zram_size_and_swap_header_layout() {
        assert_eq!(zram_disksize(8 * MIB), None);
        assert_eq!(zram_disksize(2 * GIB + 1234), Some(2 * GIB));
        assert_eq!(zram_disksize(16 * GIB), Some(16 * GIB));

        assert!(swap_header(0).is_none());
        assert!(swap_header(8 * PAGE).is_none());
        let hdr = swap_header(2 * GIB).expect("valid swap header");
        assert_eq!(
            u32::from_ne_bytes(hdr[1024..1028].try_into().unwrap()),
            1,
            "swap version"
        );
        assert_eq!(
            u32::from_ne_bytes(hdr[1028..1032].try_into().unwrap()),
            ((2 * GIB / PAGE) - 1) as u32,
            "last_page"
        );
        assert_eq!(
            u32::from_ne_bytes(hdr[1032..1036].try_into().unwrap()),
            0,
            "nr_badpages"
        );
        assert_eq!(&hdr[PAGE as usize - 10..], b"SWAPSPACE2");
    }
}
