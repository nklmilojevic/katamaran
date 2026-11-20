//! Resource usage for the host's status view: `/proc/meminfo` and the
//! aggregate `cpu` line of `/proc/stat`.

use kat_proto::GuestStats;

pub fn read() -> GuestStats {
    let mut st = GuestStats::default();
    if let Ok(m) = std::fs::read_to_string("/proc/meminfo") {
        st.mem_total_kib = meminfo_kib(&m, "MemTotal:").unwrap_or(0);
        st.mem_available_kib = meminfo_kib(&m, "MemAvailable:").unwrap_or(0);
    }
    if let Ok(s) = std::fs::read_to_string("/proc/stat") {
        (st.cpu_busy_ticks, st.cpu_total_ticks, st.cpus) = cpu_ticks(&s);
    }
    st
}

fn meminfo_kib(meminfo: &str, key: &str) -> Option<u64> {
    meminfo
        .lines()
        .find_map(|l| l.strip_prefix(key))
        .and_then(|v| v.trim().trim_end_matches("kB").trim().parse().ok())
}

/// `(busy, total, cpus)`; busy is everything but idle (4th) and iowait (5th).
fn cpu_ticks(stat: &str) -> (u64, u64, u32) {
    let mut busy = 0;
    let mut total = 0;
    let mut cpus = 0;
    for line in stat.lines() {
        let mut fields = line.split_ascii_whitespace();
        match fields.next() {
            Some("cpu") => {
                for (i, v) in fields.filter_map(|f| f.parse::<u64>().ok()).enumerate() {
                    // guest and guest_nice are already counted in user and nice.
                    if i >= 8 {
                        break;
                    }
                    total += v;
                    if i != 3 && i != 4 {
                        busy += v;
                    }
                }
            }
            Some(c) if c.starts_with("cpu") => cpus += 1,
            _ => {}
        }
    }
    (busy, total, cpus)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_meminfo() {
        let m = "MemTotal:        4028396 kB\nMemFree:  100 kB\nMemAvailable:    3000000 kB\n";
        assert_eq!(meminfo_kib(m, "MemTotal:"), Some(4028396));
        assert_eq!(meminfo_kib(m, "MemAvailable:"), Some(3000000));
        assert_eq!(meminfo_kib(m, "Cached:"), None);
    }

    #[test]
    fn parses_cpu_ticks() {
        let s = "cpu  10 2 30 400 5 6 7 8 9 10\ncpu0 5 1 15 200 2 3 3 4 0 0\ncpu1 5 1 15 200 3 3 4 4 0 0\nintr 1 2\n";
        // user nice system irq softirq steal = 10+2+30+6+7+8
        assert_eq!(cpu_ticks(s), (63, 468, 2));
    }
}
