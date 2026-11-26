//! The open-files limit (RLIMIT_NOFILE) PID 1 sets for itself, so every
//! service and container inherits it (applied in linux/mod.rs).

/// What Docker Desktop, OrbStack and Colima give containerd
/// (`LimitNOFILE=1048576`), and the kernel's default `fs.nr_open`.
pub const DEFAULT: u64 = 1 << 20;
/// Below this, dockerd and containerd themselves run short.
pub const MIN: u64 = 1024;

/// The limit the host asked for with `kat.nofile=<n>`, or [`DEFAULT`].
/// Out-of-range or malformed values fall back to the nearest valid one.
pub fn from_cmdline(cmdline: &str) -> u64 {
    cmdline
        .split_whitespace()
        .filter_map(|a| a.strip_prefix("kat.nofile="))
        .next_back()
        .and_then(|v| v.parse::<u64>().ok())
        .map_or(DEFAULT, |n| n.max(MIN))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_docker_desktop_limit() {
        assert_eq!(from_cmdline(""), DEFAULT);
        assert_eq!(from_cmdline("console=hvc0 kat.k8s=1"), DEFAULT);
        assert_eq!(from_cmdline("kat.nofile=lots"), DEFAULT);
    }

    #[test]
    fn reads_the_host_value() {
        assert_eq!(from_cmdline("kat.k8s=1 kat.nofile=65536"), 65536);
        assert_eq!(from_cmdline("kat.nofile=1 kat.nofile=262144"), 262144);
        assert_eq!(from_cmdline("kat.nofile=10"), MIN);
    }
}
