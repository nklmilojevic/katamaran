//! `<home>/config.toml`: the VM's resources and options. The daemon reads it
//! at every VM start, so a change applies on the next start.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::net::Ipv4Addr;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, bail};
use serde::{Deserialize, Serialize};

use crate::amd64::Emulation;

pub const FILE_NAME: &str = "config.toml";
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub cpus: u8,
    /// A ceiling, not a reservation: memory the guest frees goes back to
    /// macOS, so the default is generous (see [`default_memory_mib`]).
    pub memory_mib: u32,
    /// Show the guest's files on the Mac over NFS.
    pub mount: bool,
    /// Where the guest's files appear; empty means `~/Katamaran` (nothing
    /// when `KATAMARAN_HOME` is set, so development copies stay apart).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mount_path: Option<PathBuf>,
    /// Start the daemon, and with it the VM, when the user logs in.
    pub launch_at_login: bool,
    /// Freeze the VM's vCPUs while the Mac sleeps, so it does no work in the
    /// Mac's dark wakes.
    pub pause_on_sleep: bool,
    /// Forward the Mac's 127.0.0.1:<port> to container ports that were not
    /// published with `-p`.
    pub auto_forward: bool,
    /// The Mac addresses forwarded ports listen on: `127.0.0.1`, and any
    /// alias added to `lo0` (e.g. `sudo ifconfig lo0 alias 172.16.1.200`).
    #[serde(skip_serializing_if = "is_default_forward_addresses")]
    pub forward_addresses: Vec<Ipv4Addr>,
    /// How x86-64 (amd64) programs run in the guest.
    pub amd64: Emulation,
    /// Run an optional single-node Kubernetes (k3s) cluster inside the VM.
    pub kubernetes: bool,
    /// The open-files limit (RLIMIT_NOFILE, soft and hard) the guest sets at
    /// boot; containerd, dockerd and workloads inherit it unless they set
    /// their own. Not written when default, so older versions, which deny
    /// unknown keys, still read a saved config.
    #[serde(skip_serializing_if = "is_default_nofile")]
    pub nofile: u32,
    /// The k3s release to run, e.g. `v1.32.5+k3s1`; unset means the build
    /// this katamaran release pins ([`crate::k8s::K3S_VERSION`]).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub k8s_version: Option<String>,
    /// Bundled k3s components to disable (for example `["traefik"]`), passed as `--disable=<name>`.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub k8s_disable: Vec<String>,
    /// Extra arguments appended to `k3s server`.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub k8s_args: Vec<String>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            // Idle vCPUs cost nothing, so the VM gets every core by default.
            cpus: Host::current().cpus.min(u32::from(u8::MAX)) as u8,
            memory_mib: default_memory_mib(Host::current()),
            mount: true,
            mount_path: None,
            launch_at_login: true,
            pause_on_sleep: false,
            auto_forward: true,
            forward_addresses: default_forward_addresses(),
            amd64: Emulation::default(),
            kubernetes: false,
            nofile: DEFAULT_NOFILE,
            k8s_version: None,
            k8s_disable: Vec::new(),
            k8s_args: Vec::new(),
        }
    }
}

/// The keys `config get` and `config set` accept.
pub const KEYS: [&str; 14] = [
    "cpus",
    "memory_mib",
    "mount",
    "mount_path",
    "launch_at_login",
    "pause_on_sleep",
    "auto_forward",
    "forward_addresses",
    "amd64",
    "kubernetes",
    "nofile",
    "k8s_version",
    "k8s_disable",
    "k8s_args",
];

/// What Docker Desktop, OrbStack and Colima give containers.
pub const DEFAULT_NOFILE: u32 = 1 << 20;
/// Below this, dockerd and containerd themselves run short.
pub const MIN_NOFILE: u32 = 1024;
/// Within the kernel's ceiling for `fs.nr_open`.
pub const MAX_NOFILE: u32 = 1 << 30;

fn is_default_nofile(n: &u32) -> bool {
    *n == DEFAULT_NOFILE
}

/// Upper bound on the total size of extra k3s server arguments so they stay well
/// within the host-guest protocol frame limit.
pub const MAX_K8S_ARGS_BYTES: usize = 64 << 10;

pub fn default_forward_addresses() -> Vec<Ipv4Addr> {
    vec![Ipv4Addr::LOCALHOST]
}

fn is_default_forward_addresses(v: &[Ipv4Addr]) -> bool {
    v == [Ipv4Addr::LOCALHOST]
}

/// Accepts `127.0.0.1,172.16.1.200` (commas or spaces); empty is the default.
fn parse_addresses(v: &str) -> anyhow::Result<Vec<Ipv4Addr>> {
    let addrs = v
        .split(|c: char| c == ',' || c.is_whitespace())
        .filter(|s| !s.is_empty())
        .map(|s| {
            s.parse::<Ipv4Addr>()
                .with_context(|| format!("forward_addresses: {s:?} is not an IPv4 address"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(if addrs.is_empty() {
        default_forward_addresses()
    } else {
        addrs
    })
}

/// The smallest guest that still runs dockerd comfortably.
pub const MIN_MEMORY_MIB: u32 = 1024;

/// Half of the Mac's memory, between 2 and 16 GiB, and within what
/// [`Config::validate`] accepts.
pub const fn default_memory_mib(host: Host) -> u32 {
    let half = host.memory_mib / 2;
    let v = if half < 2048 {
        2048
    } else if half > 16384 {
        16384
    } else {
        half
    };
    let max = max_memory_mib(host);
    if v > max { max } else { v }
}

/// What the Mac keeps for itself: 2 GiB.
const fn max_memory_mib(host: Host) -> u32 {
    let max = host.memory_mib.saturating_sub(2048);
    if max < MIN_MEMORY_MIB {
        MIN_MEMORY_MIB
    } else {
        max
    }
}

impl Config {
    pub fn path(home: &Path) -> PathBuf {
        home.join(FILE_NAME)
    }

    /// The saved config, or the defaults when there is none yet.
    pub fn load(home: &Path) -> anyhow::Result<Config> {
        let path = Config::path(home);
        match fs::read_to_string(&path) {
            Ok(text) => Config::parse(&text).with_context(|| format!("read {}", path.display())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Config::default()),
            Err(e) => Err(e).with_context(|| format!("read {}", path.display())),
        }
    }

    pub fn parse(text: &str) -> anyhow::Result<Config> {
        let cfg: Config = toml::from_str(text)?;
        cfg.validate(&Host::current())?;
        Ok(cfg)
    }

    /// Write atomically, so a crash never leaves half a file.
    pub fn save(&self, home: &Path) -> anyhow::Result<()> {
        fs::create_dir_all(home)?;
        let path = Config::path(home);
        let n = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmp = home.join(format!("{FILE_NAME}.tmp.{}.{n}", std::process::id()));
        // A crash mid-save can leave this name behind for a later process
        // with the same PID; `create_new` below would then always fail.
        let _ = fs::remove_file(&tmp);
        let text = format!(
            "# katamaran settings; applied at the next VM start.\n{}",
            toml::to_string(self)?
        );
        let mut f = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&tmp)
            .with_context(|| format!("write {}", tmp.display()))?;
        let write_res = f
            .write_all(text.as_bytes())
            .with_context(|| format!("write {}", tmp.display()));
        drop(f);
        let res = write_res.and_then(|()| {
            fs::rename(&tmp, &path).with_context(|| format!("write {}", path.display()))
        });
        if let Err(e) = res {
            let _ = fs::remove_file(&tmp);
            return Err(e);
        }
        Ok(())
    }

    pub fn get(&self, key: &str) -> anyhow::Result<String> {
        Ok(match key {
            "cpus" => self.cpus.to_string(),
            "memory_mib" => self.memory_mib.to_string(),
            "mount" => self.mount.to_string(),
            "mount_path" => self
                .mount_path
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_default(),
            "launch_at_login" => self.launch_at_login.to_string(),
            "pause_on_sleep" => self.pause_on_sleep.to_string(),
            "auto_forward" => self.auto_forward.to_string(),
            "forward_addresses" => self
                .forward_addresses
                .iter()
                .map(Ipv4Addr::to_string)
                .collect::<Vec<_>>()
                .join(","),
            "amd64" => self.amd64.to_string(),
            "kubernetes" => self.kubernetes.to_string(),
            "nofile" => self.nofile.to_string(),
            "k8s_version" => self.k8s_version.clone().unwrap_or_default(),
            "k8s_disable" => self.k8s_disable.join(","),
            "k8s_args" => format_k8s_args(&self.k8s_args),
            other => bail!("unknown key {other}; use one of {}", KEYS.join(", ")),
        })
    }

    /// Set one key from its text form and check the result against `host`.
    pub fn set(&mut self, key: &str, value: &str, host: &Host) -> anyhow::Result<()> {
        let mut next = self.clone();
        let value = value.trim();
        match key {
            "cpus" => next.cpus = value.parse().with_context(|| format!("cpus: {value:?}"))?,
            "memory_mib" | "memory" => next.memory_mib = parse_mib(value)?,
            "mount" => next.mount = parse_bool(value)?,
            "mount_path" => {
                next.mount_path = if value.is_empty() {
                    None
                } else {
                    Some(PathBuf::from(value))
                }
            }
            "launch_at_login" => next.launch_at_login = parse_bool(value)?,
            "pause_on_sleep" => next.pause_on_sleep = parse_bool(value)?,
            "auto_forward" => next.auto_forward = parse_bool(value)?,
            "forward_addresses" => next.forward_addresses = parse_addresses(value)?,
            "amd64" => next.amd64 = value.parse()?,
            "kubernetes" => next.kubernetes = parse_bool(value)?,
            "nofile" => {
                next.nofile = value
                    .parse()
                    .with_context(|| format!("nofile: {value:?}"))?
            }
            "k8s_version" => {
                next.k8s_version = match value {
                    "" | "default" => None,
                    // Only a bare version number gets the `v`; anything else is
                    // validated as typed, so errors quote the user's input.
                    v if v.starts_with(|c: char| c.is_ascii_digit()) => Some(format!("v{v}")),
                    v => Some(v.to_string()),
                }
            }
            "k8s_disable" => next.k8s_disable = parse_k8s_disable(value)?,
            "k8s_args" => next.k8s_args = parse_k8s_args(value)?,
            other => bail!("unknown key {other}; use one of {}", KEYS.join(", ")),
        }
        next.validate(host)?;
        next.validate_k8s_key(key)?;
        *self = next;
        Ok(())
    }

    /// Extra `k3s server` flags derived from `k8s_disable` and `k8s_args`.
    pub fn k8s_server_extra_args(&self) -> Vec<String> {
        let mut out = Vec::with_capacity(self.k8s_disable.len() + self.k8s_args.len());
        for component in &self.k8s_disable {
            out.push(format!("--disable={component}"));
        }
        out.extend(self.k8s_args.iter().cloned());
        out
    }

    /// Whether a VM started with `self` has to restart to use `saved`.
    /// `launch_at_login` applies to launchd directly, and Kubernetes settings
    /// (`kubernetes`, `k8s_version`, `k8s_disable`, `k8s_args`) apply when k3s starts.
    pub fn needs_restart_for(&self, saved: &Config) -> bool {
        let applied = Config {
            launch_at_login: saved.launch_at_login,
            kubernetes: saved.kubernetes,
            k8s_version: saved.k8s_version.clone(),
            k8s_disable: saved.k8s_disable.clone(),
            k8s_args: saved.k8s_args.clone(),
            ..self.clone()
        };
        applied != *saved
    }

    pub fn validate(&self, host: &Host) -> anyhow::Result<()> {
        if self.cpus == 0 || u32::from(self.cpus) > host.cpus {
            bail!("cpus must be between 1 and {} (this Mac's CPUs)", host.cpus);
        }
        let max = max_memory_mib(*host);
        if self.memory_mib < MIN_MEMORY_MIB || self.memory_mib > max {
            bail!("memory_mib must be between {MIN_MEMORY_MIB} and {max}");
        }
        if let Some(p) = &self.mount_path
            && !p.is_absolute()
        {
            bail!("mount_path must be absolute");
        }
        if !(MIN_NOFILE..=MAX_NOFILE).contains(&self.nofile) {
            bail!("nofile must be between {MIN_NOFILE} and {MAX_NOFILE}");
        }
        if let Some(v) = &self.k8s_version {
            validate_k8s_version(v)?;
        }
        // `k8s_disable` / `k8s_args` are deliberately not checked here: this
        // runs on every load, and a bad Kubernetes setting in a hand-edited
        // config.toml must fail only the Kubernetes start, not the VM.
        Ok(())
    }

    /// Full check of `k8s_disable` and `k8s_args`, the same one the guest
    /// applies before starting k3s. Run at every Kubernetes start; `set`
    /// checks only the key being set (see [`Config::validate_k8s_key`]).
    pub fn validate_k8s_settings(&self) -> anyhow::Result<()> {
        self.validate_k8s_key("k8s_disable")?;
        self.validate_k8s_key("k8s_args")
    }

    /// Check one of `k8s_disable` / `k8s_args`, plus the combined size limit.
    /// Checking only the key being set lets `kat config set` repair a
    /// hand-edited config.toml in which both keys are invalid.
    fn validate_k8s_key(&self, key: &str) -> anyhow::Result<()> {
        match key {
            "k8s_disable" => {
                for component in &self.k8s_disable {
                    validate_k8s_disable_item(component)?;
                }
            }
            "k8s_args" => kat_proto::k8s::validate_k3s_extra_args(&self.k8s_args)
                .map_err(anyhow::Error::msg)?,
            _ => return Ok(()),
        }
        let total_k8s_bytes = self
            .k8s_disable
            .iter()
            .map(|c| "--disable=".len() + c.len())
            .chain(self.k8s_args.iter().map(String::len))
            .fold(0usize, usize::saturating_add);
        if total_k8s_bytes > MAX_K8S_ARGS_BYTES {
            bail!("k8s_disable and k8s_args exceed maximum size of {MAX_K8S_ARGS_BYTES} bytes");
        }
        if self.forward_addresses.is_empty() {
            bail!("forward_addresses must list at least one address");
        }
        for (i, a) in self.forward_addresses.iter().enumerate() {
            if a.is_unspecified() || a.is_multicast() || a.is_broadcast() {
                bail!("forward_addresses: {a} is not a host address");
            }
            if self.forward_addresses[..i].contains(a) {
                bail!("forward_addresses: {a} is listed twice");
            }
        }
        Ok(())
    }

    /// The k3s release to run: `k8s_version`, or the pinned build.
    pub fn k3s_version(&self) -> &str {
        self.k8s_version
            .as_deref()
            .unwrap_or(crate::k8s::K3S_VERSION)
    }
}

/// The oldest Kubernetes minor the guest's k3s flags work with: kube-proxy's
/// `NFTablesProxyMode` feature gate is enabled by default from 1.31, and the
/// guest rootfs ships `nft` (Alpine's `nftables` package) so releases whose
/// bundled `k3s-root` lacks `nft` (such as v1.31.0–v1.31.5) still work.
const MIN_K8S_MINOR: u32 = 31;

/// A k3s release tag, `v<major>.<minor>.<patch>[-<pre>]+k3s<n>`, no older
/// than v1.[`MIN_K8S_MINOR`].
pub(crate) fn validate_k8s_version(v: &str) -> anyhow::Result<()> {
    let bad = || {
        anyhow::anyhow!(
            "k8s_version {v:?} is not a k3s release tag such as {}{}",
            crate::k8s::K3S_VERSION,
            if v.starts_with('v') {
                ""
            } else {
                " (config.toml needs the leading `v`)"
            }
        )
    };
    // Digits only, without leading zeros: release tags never pad numbers.
    let canonical_number = |n: &str| {
        !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()) && (n == "0" || !n.starts_with('0'))
    };
    let rest = v.strip_prefix('v').ok_or_else(bad)?;
    let (kube, build) = rest.split_once("+k3s").ok_or_else(bad)?;
    if !canonical_number(build) {
        return Err(bad());
    }
    let (core, pre) = match kube.split_once('-') {
        Some((c, p)) => (c, Some(p)),
        None => (kube, None),
    };
    if let Some(p) = pre
        && (p.is_empty() || !p.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'.'))
    {
        return Err(bad());
    }
    let parts: Vec<u32> = core
        .split('.')
        .map(|n| {
            if !canonical_number(n) {
                return Err(bad());
            }
            n.parse().map_err(|_| bad())
        })
        .collect::<anyhow::Result<_>>()?;
    let [major, minor, _patch] = parts[..] else {
        return Err(bad());
    };
    if major != 1 || minor < MIN_K8S_MINOR {
        bail!("k8s_version must be v1.{MIN_K8S_MINOR} or newer, got {v}");
    }
    Ok(())
}

fn validate_k8s_disable_item(item: &str) -> anyhow::Result<()> {
    if item.is_empty() {
        bail!("k8s_disable must not contain empty component names");
    }
    if item.starts_with('-') {
        bail!(
            "k8s_disable entry {item:?} must be a component name (for example \"traefik\"), not a flag"
        );
    }
    if !item
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        bail!(
            "k8s_disable entry {item:?} contains invalid characters; expected letters, digits, '-' or '_'"
        );
    }
    Ok(())
}

/// Parse a comma-separated list of k3s component names (empty clears the list).
fn parse_k8s_disable(v: &str) -> anyhow::Result<Vec<String>> {
    let v = v.trim();
    if v.is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for raw in v.split(',') {
        let item = raw.trim();
        validate_k8s_disable_item(item)?;
        if !out.iter().any(|existing| existing == item) {
            out.push(item.to_string());
        }
    }
    Ok(out)
}

/// Split shell-style whitespace-separated arguments (supporting single quotes,
/// double quotes, and backslash escapes). An empty string clears the list.
fn parse_k8s_args(v: &str) -> anyhow::Result<Vec<String>> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_token = false;
    let mut chars = v.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            c if c.is_whitespace() => {
                if in_token {
                    out.push(std::mem::take(&mut cur));
                    in_token = false;
                }
            }
            '\'' => {
                in_token = true;
                let mut closed = false;
                for sc in chars.by_ref() {
                    if sc == '\'' {
                        closed = true;
                        break;
                    }
                    cur.push(sc);
                }
                if !closed {
                    bail!("k8s_args: unterminated single quote");
                }
            }
            '"' => {
                in_token = true;
                let mut closed = false;
                while let Some(dc) = chars.next() {
                    if dc == '"' {
                        closed = true;
                        break;
                    }
                    if dc == '\\' {
                        match chars.peek().copied() {
                            Some(esc @ ('\\' | '"')) => {
                                chars.next();
                                cur.push(esc);
                            }
                            Some(_) => cur.push('\\'),
                            None => bail!("k8s_args: unterminated double quote"),
                        }
                    } else {
                        cur.push(dc);
                    }
                }
                if !closed {
                    bail!("k8s_args: unterminated double quote");
                }
            }
            '\\' => {
                in_token = true;
                let Some(esc) = chars.next() else {
                    bail!("k8s_args: trailing backslash");
                };
                cur.push(esc);
            }
            other => {
                in_token = true;
                cur.push(other);
            }
        }
    }
    if in_token {
        out.push(cur);
    }
    Ok(out)
}

fn format_k8s_args(args: &[String]) -> String {
    args.iter()
        .map(|arg| {
            if !arg.is_empty()
                && !arg
                    .chars()
                    .any(|c| c.is_whitespace() || matches!(c, '\'' | '"' | '\\'))
            {
                arg.clone()
            } else {
                format!("'{}'", arg.replace('\'', "'\\''"))
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Accepts `4096`, `4096M`, `4096MiB`, `4G` and `4GiB`.
fn parse_mib(v: &str) -> anyhow::Result<u32> {
    let lower = v.to_ascii_lowercase();
    let (num, mult) = if let Some(n) = lower
        .strip_suffix("gib")
        .or_else(|| lower.strip_suffix("gb"))
        .or_else(|| lower.strip_suffix('g'))
    {
        (n, 1024)
    } else if let Some(n) = lower
        .strip_suffix("mib")
        .or_else(|| lower.strip_suffix("mb"))
        .or_else(|| lower.strip_suffix('m'))
    {
        (n, 1)
    } else {
        (lower.as_str(), 1)
    };
    let n: u32 = num
        .trim()
        .parse()
        .with_context(|| format!("memory: {v:?}"))?;
    n.checked_mul(mult)
        .with_context(|| format!("memory: {v:?} is too large"))
}

fn parse_bool(v: &str) -> anyhow::Result<bool> {
    match v.to_ascii_lowercase().as_str() {
        "true" | "on" | "yes" | "1" => Ok(true),
        "false" | "off" | "no" | "0" => Ok(false),
        _ => bail!("expected true or false, got {v:?}"),
    }
}

/// What the Mac can give the VM.
#[derive(Debug, Clone, Copy)]
pub struct Host {
    pub cpus: u32,
    pub memory_mib: u32,
}

impl Host {
    pub fn current() -> Host {
        Host {
            cpus: sysctl_u64(c"hw.ncpu").map_or(1, |n| n as u32),
            memory_mib: sysctl_u64(c"hw.memsize").map_or(8192, |b| (b >> 20) as u32),
        }
    }
}

fn sysctl_u64(name: &std::ffi::CStr) -> Option<u64> {
    let mut buf = [0u8; 8];
    let mut len = buf.len();
    let rc = unsafe {
        libc::sysctlbyname(
            name.as_ptr(),
            buf.as_mut_ptr().cast(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    match (rc, len) {
        (0, 4) => Some(u32::from_ne_bytes(buf[..4].try_into().unwrap()).into()),
        (0, 8) => Some(u64::from_ne_bytes(buf)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // More cores than any test Mac, so the default (this Mac's count) is valid.
    const HOST: Host = Host {
        cpus: 255,
        memory_mib: 32768,
    };

    fn temp_home(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("kat-config-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn missing_file_gives_defaults() {
        let home = temp_home("missing");
        assert_eq!(Config::load(&home).unwrap(), Config::default());
    }

    #[test]
    fn save_and_load_round_trip() {
        let home = temp_home("roundtrip");
        let mut cfg = Config::default();
        cfg.set("cpus", "2", &HOST).unwrap();
        cfg.set("memory", "2G", &HOST).unwrap();
        cfg.set("mount", "off", &HOST).unwrap();
        cfg.set("mount_path", "/tmp/kat-files", &HOST).unwrap();
        cfg.save(&home).unwrap();
        let text = fs::read_to_string(Config::path(&home)).unwrap();
        assert!(text.contains("memory_mib = 2048"), "{text}");
        let back = Config::load(&home).unwrap();
        assert_eq!(back, cfg);
        assert_eq!(back.get("mount_path").unwrap(), "/tmp/kat-files");
        fs::remove_dir_all(&home).unwrap();
    }

    #[test]
    fn partial_file_fills_in_defaults() {
        let cfg = Config::parse("cpus = 2\n").unwrap();
        assert_eq!(cfg.cpus, 2);
        assert_eq!(cfg.memory_mib, Config::default().memory_mib);
        assert!(cfg.mount);
    }

    #[test]
    fn rejects_unknown_keys_and_bad_values() {
        assert!(Config::parse("cpu = 2\n").is_err());
        assert!(Config::parse("cpus = 0\n").is_err());
        let mut cfg = Config::default();
        assert!(cfg.set("cpus", "256", &HOST).is_err());
        assert!(cfg.set("cpus", "many", &HOST).is_err());
        assert!(cfg.set("memory_mib", "512", &HOST).is_err());
        assert!(cfg.set("memory_mib", "40G", &HOST).is_err());
        assert!(cfg.set("mount", "maybe", &HOST).is_err());
        assert!(cfg.set("mount_path", "relative", &HOST).is_err());
        assert!(cfg.set("disk", "1", &HOST).is_err());
        // A rejected value leaves the config as it was.
        assert_eq!(cfg, Config::default());
    }

    #[test]
    fn only_vm_settings_need_a_restart() {
        let applied = Config::default();
        let mut saved = applied.clone();
        saved.launch_at_login = !applied.launch_at_login;
        assert!(!applied.needs_restart_for(&saved));
        saved.cpus = 2;
        assert!(applied.needs_restart_for(&saved));
        let mut saved = applied.clone();
        saved.mount = !applied.mount;
        assert!(applied.needs_restart_for(&saved));
    }

    #[test]
    fn parses_memory_units() {
        assert_eq!(parse_mib("4096").unwrap(), 4096);
        assert_eq!(parse_mib("2048M").unwrap(), 2048);
        assert_eq!(parse_mib("8GiB").unwrap(), 8192);
        assert_eq!(parse_mib("3g").unwrap(), 3072);
        assert!(parse_mib("lots").is_err());
    }

    #[test]
    fn default_memory_is_half_of_the_mac_up_to_16_gib() {
        let host = |gib: u32| Host {
            cpus: 255,
            memory_mib: gib * 1024,
        };
        assert_eq!(default_memory_mib(host(4)), 2048);
        assert_eq!(default_memory_mib(host(8)), 4096);
        assert_eq!(default_memory_mib(host(18)), 9216);
        assert_eq!(default_memory_mib(host(36)), 16384);
        assert_eq!(default_memory_mib(host(128)), 16384);
        // Never more than validate() accepts.
        assert_eq!(default_memory_mib(host(3)), 1024);
        for gib in [3, 4, 8, 16, 24, 36, 64, 128] {
            let cfg = Config {
                memory_mib: default_memory_mib(host(gib)),
                ..Config::default()
            };
            cfg.validate(&host(gib)).unwrap();
        }
    }

    #[test]
    fn pause_on_sleep_is_a_vm_setting() {
        let mut cfg = Config::default();
        assert!(!cfg.pause_on_sleep);
        cfg.set("pause_on_sleep", "on", &HOST).unwrap();
        assert_eq!(cfg.get("pause_on_sleep").unwrap(), "true");
        assert!(Config::default().needs_restart_for(&cfg));
    }

    #[test]
    fn amd64_is_a_vm_setting() {
        let mut cfg = Config::default();
        assert_eq!(cfg.get("amd64").unwrap(), "auto");
        cfg.set("amd64", "qemu", &HOST).unwrap();
        assert_eq!(cfg.amd64, Emulation::Qemu);
        assert!(Config::default().needs_restart_for(&cfg));
        assert!(cfg.set("amd64", "x86", &HOST).is_err());
        let text = toml::to_string(&cfg).unwrap();
        assert!(text.contains("amd64 = \"qemu\""), "{text}");
        assert_eq!(Config::parse(&text).unwrap(), cfg);
    }

    #[test]
    fn auto_forward_is_on_by_default_and_a_vm_setting() {
        let mut cfg = Config::parse("").unwrap();
        assert!(cfg.auto_forward);
        let started = cfg.clone();
        cfg.set("auto_forward", "off", &HOST).unwrap();
        assert_eq!(cfg.get("auto_forward").unwrap(), "false");
        assert!(started.needs_restart_for(&cfg));
        assert!(!Config::parse("auto_forward = false").unwrap().auto_forward);
    }

    #[test]
    fn nofile_defaults_high_and_is_a_vm_setting() {
        let mut cfg = Config::parse("").unwrap();
        assert_eq!(cfg.nofile, 1_048_576);
        let started = cfg.clone();
        cfg.set("nofile", "65536", &HOST).unwrap();
        assert_eq!(cfg.get("nofile").unwrap(), "65536");
        assert!(started.needs_restart_for(&cfg));
        assert!(cfg.set("nofile", "512", &HOST).is_err());
        assert!(cfg.set("nofile", "lots", &HOST).is_err());
        assert!(Config::parse("nofile = 100").is_err());
        assert_eq!(Config::parse("nofile = 262144").unwrap().nofile, 262_144);
    }

    #[test]
    fn default_nofile_is_not_saved() {
        let text = toml::to_string(&Config::default()).unwrap();
        assert!(!text.contains("nofile"), "{text}");
        let mut cfg = Config::default();
        cfg.set("nofile", "65536", &HOST).unwrap();
        let text = toml::to_string(&cfg).unwrap();
        assert!(text.contains("nofile = 65536"), "{text}");
        assert_eq!(Config::parse(&text).unwrap(), cfg);
    }

    #[test]
    fn k8s_version_defaults_to_the_pinned_build() {
        let mut cfg = Config::parse("").unwrap();
        assert_eq!(cfg.k8s_version, None);
        assert_eq!(cfg.k3s_version(), crate::k8s::K3S_VERSION);
        assert_eq!(cfg.get("k8s_version").unwrap(), "");
        assert!(!toml::to_string(&cfg).unwrap().contains("k8s_version"));

        let started = cfg.clone();
        cfg.set("k8s_version", "v1.32.5+k3s1", &HOST).unwrap();
        assert_eq!(cfg.k3s_version(), "v1.32.5+k3s1");
        // Read at each Kubernetes start, not at VM start.
        assert!(!started.needs_restart_for(&cfg));
        let text = toml::to_string(&cfg).unwrap();
        assert!(text.contains("k8s_version = \"v1.32.5+k3s1\""), "{text}");
        assert_eq!(Config::parse(&text).unwrap(), cfg);

        cfg.set("k8s_version", "1.33.1+k3s2", &HOST).unwrap();
        assert_eq!(cfg.get("k8s_version").unwrap(), "v1.33.1+k3s2");
        cfg.set("k8s_version", "v1.34.0-rc1+k3s1", &HOST).unwrap();
        cfg.set("k8s_version", "", &HOST).unwrap();
        assert_eq!(cfg.k8s_version, None);
        cfg.set("k8s_version", "v1.32.5+k3s1", &HOST).unwrap();
        cfg.set("k8s_version", "default", &HOST).unwrap();
        assert_eq!(cfg.k8s_version, None);
    }

    #[test]
    fn rejects_bad_k8s_versions() {
        let mut cfg = Config::default();
        for v in [
            "latest",
            "v1.32",
            "v1.32.5",
            "v1.32.5+k3s",
            "v1.32.5+k3sx",
            "v1.32.x+k3s1",
            "v1.32.5-+k3s1",
            "v1.32.5/../+k3s1",
            "v2.0.0+k3s1",
            "v1.30.9+k3s1",
            "v1.037.2+k3s1",
            "v1.37.02+k3s1",
            "v1.37.2+k3s01",
        ] {
            assert!(cfg.set("k8s_version", v, &HOST).is_err(), "{v}");
        }
        assert_eq!(cfg, Config::default());
        assert!(Config::parse("k8s_version = \"v1.28.0+k3s1\"\n").is_err());
        let err = Config::parse("k8s_version = \"1.32.5+k3s1\"\n").unwrap_err();
        assert!(
            format!("{err:#}").contains("needs the leading `v`"),
            "{err:#}"
        );
        assert!(cfg.set("k8s_version", "v1.37.0+k3s1", &HOST).is_ok());
        let err = cfg.set("k8s_version", "V1.32.5+k3s1", &HOST).unwrap_err();
        assert!(format!("{err:#}").contains("\"V1.32.5+k3s1\""), "{err:#}");
    }

    #[test]
    fn forward_addresses_default_to_localhost_and_are_a_vm_setting() {
        let mut cfg = Config::parse("").unwrap();
        assert_eq!(cfg.forward_addresses, [Ipv4Addr::LOCALHOST]);
        assert_eq!(cfg.get("forward_addresses").unwrap(), "127.0.0.1");
        // The default stays out of the file, so older versions still read it.
        assert!(!toml::to_string(&cfg).unwrap().contains("forward_addresses"));
        let started = cfg.clone();
        cfg.set("forward_addresses", "127.0.0.1, 172.16.1.200", &HOST)
            .unwrap();
        assert_eq!(
            cfg.forward_addresses,
            [Ipv4Addr::LOCALHOST, Ipv4Addr::new(172, 16, 1, 200)]
        );
        assert_eq!(
            cfg.get("forward_addresses").unwrap(),
            "127.0.0.1,172.16.1.200"
        );
        assert!(started.needs_restart_for(&cfg));
        let text = toml::to_string(&cfg).unwrap();
        assert!(
            text.contains(r#"forward_addresses = ["127.0.0.1", "172.16.1.200"]"#),
            "{text}"
        );
        assert_eq!(Config::parse(&text).unwrap(), cfg);
        cfg.set("forward_addresses", "", &HOST).unwrap();
        assert_eq!(cfg.forward_addresses, [Ipv4Addr::LOCALHOST]);
    }

    #[test]
    fn rejects_bad_forward_addresses() {
        let mut cfg = Config::default();
        for bad in [
            "localhost",
            "0.0.0.0",
            "224.0.0.1",
            "255.255.255.255",
            "::1",
        ] {
            assert!(cfg.set("forward_addresses", bad, &HOST).is_err(), "{bad}");
        }
        assert!(
            cfg.set("forward_addresses", "127.0.0.1,127.0.0.1", &HOST)
                .is_err()
        );
        assert!(Config::parse("forward_addresses = []").is_err());
        assert_eq!(cfg, Config::default());
    }

    #[test]
    fn host_reports_this_mac() {
        let h = Host::current();
        assert!(h.cpus >= 1);
        assert!(h.memory_mib >= 1024);
    }

    #[test]
    fn k8s_disable_and_args_round_trip_and_do_not_require_vm_restart() {
        let mut cfg = Config::default();
        assert!(cfg.k8s_disable.is_empty());
        assert!(cfg.k8s_args.is_empty());
        assert_eq!(cfg.get("k8s_disable").unwrap(), "");
        assert_eq!(cfg.get("k8s_args").unwrap(), "");

        // Empty lists are omitted from serialized config.toml.
        let default_toml = toml::to_string(&cfg).unwrap();
        assert!(!default_toml.contains("k8s_disable"), "{default_toml}");
        assert!(!default_toml.contains("k8s_args"), "{default_toml}");

        cfg.set("k8s_disable", "traefik, servicelb, traefik", &HOST)
            .unwrap();
        assert_eq!(cfg.k8s_disable, vec!["traefik", "servicelb"]);
        assert_eq!(cfg.get("k8s_disable").unwrap(), "traefik,servicelb");

        cfg.set(
            "k8s_args",
            "--kube-apiserver-arg=feature-gates=Foo=true --node-label='env=my dev' --node-taint=\"a'b\"",
            &HOST,
        )
        .unwrap();
        assert_eq!(
            cfg.k8s_args,
            vec![
                "--kube-apiserver-arg=feature-gates=Foo=true",
                "--node-label=env=my dev",
                "--node-taint=a'b",
            ]
        );
        assert_eq!(
            cfg.k8s_server_extra_args(),
            vec![
                "--disable=traefik",
                "--disable=servicelb",
                "--kube-apiserver-arg=feature-gates=Foo=true",
                "--node-label=env=my dev",
                "--node-taint=a'b",
            ]
        );

        // Display format round-trips through set().
        let formatted = cfg.get("k8s_args").unwrap();
        let mut reparsed = Config::default();
        reparsed.set("k8s_args", &formatted, &HOST).unwrap();
        assert_eq!(reparsed.k8s_args, cfg.k8s_args);

        // TOML round-trips cleanly.
        let toml_text = toml::to_string(&cfg).unwrap();
        assert!(toml_text.contains("k8s_disable = ["), "{toml_text}");
        assert!(toml_text.contains("k8s_args = ["), "{toml_text}");
        assert_eq!(Config::parse(&toml_text).unwrap(), cfg);

        // Changing k8s_disable or k8s_args does not require restarting the VM.
        assert!(!Config::default().needs_restart_for(&cfg));

        // Setting empty strings clears both lists.
        cfg.set("k8s_disable", "", &HOST).unwrap();
        cfg.set("k8s_args", "", &HOST).unwrap();
        assert!(cfg.k8s_disable.is_empty());
        assert!(cfg.k8s_args.is_empty());
    }

    #[test]
    fn k8s_disable_and_args_reject_invalid_values() {
        let mut cfg = Config::default();
        assert!(cfg.set("k8s_disable", "--traefik", &HOST).is_err());
        assert!(cfg.set("k8s_disable", "traefik,,servicelb", &HOST).is_err());
        assert!(cfg.set("k8s_disable", "traefik servicelb", &HOST).is_err());
        assert!(cfg.set("k8s_args", "--foo='unterminated", &HOST).is_err());
        assert!(cfg.set("k8s_args", "--foo=\"unterminated", &HOST).is_err());
        assert!(cfg.set("k8s_args", "--foo\\", &HOST).is_err());
        assert!(cfg.set("k8s_args", "''", &HOST).is_err());
        // Hand-edited values load (the VM must still start) but fail the
        // Kubernetes-start check.
        for text in ["k8s_args = [\"\"]\n", "k8s_disable = [\"--traefik\"]\n"] {
            let loaded = Config::parse(text).unwrap();
            assert!(loaded.validate_k8s_settings().is_err(), "{text}");
        }
        assert_eq!(cfg, Config::default());
    }

    #[test]
    fn save_ignores_stale_temp_file_from_a_crashed_save() {
        let home = std::env::temp_dir().join(format!("kat-cfg-stale-{}", std::process::id()));
        let _ = fs::remove_dir_all(&home);
        fs::create_dir_all(&home).unwrap();
        // Every name this process can pick for its next few saves.
        let n = TMP_COUNTER.load(Ordering::Relaxed);
        for i in n..n + 4 {
            fs::write(
                home.join(format!("{FILE_NAME}.tmp.{}.{i}", std::process::id())),
                "stale",
            )
            .unwrap();
        }
        Config::default().save(&home).unwrap();
        assert!(Config::path(&home).is_file());
        fs::remove_dir_all(&home).unwrap();
    }

    #[test]
    fn reserved_k8s_args_are_rejected_by_set_but_do_not_break_loading() {
        let host = HOST;
        let mut cfg = Config::default();
        for bad in [
            "--token=x",
            "--data-dir=/tmp/k3s",
            "--kubelet-arg=root_dir=/x",
        ] {
            let err = cfg.set("k8s_args", bad, &host).unwrap_err();
            assert!(err.to_string().starts_with("k8s_args"), "{bad}: {err:#}");
            assert!(cfg.k8s_args.is_empty(), "{bad} must not be saved");
        }
        cfg.set("k8s_args", "--disable=traefik", &host).unwrap();

        // A hand-edited config.toml with a reserved flag still loads (so the
        // VM and Docker start) and can be repaired with `config set`.
        let mut loaded =
            Config::parse("k8s_args = [\"--token=x\", \"\"]\nk8s_disable = [\"traefik \"]\n")
                .unwrap();
        assert!(loaded.validate_k8s_settings().is_err());
        // Each bad key can be repaired on its own, in either order.
        loaded.set("cpus", "2", &host).unwrap();
        loaded.set("k8s_args", "", &host).unwrap();
        assert!(loaded.validate_k8s_settings().is_err()); // k8s_disable still bad
        loaded.set("k8s_disable", "traefik", &host).unwrap();
        loaded.validate_k8s_settings().unwrap();
    }

    #[test]
    fn save_replaces_world_readable_config_with_mode_0600() {
        use std::os::unix::fs::PermissionsExt;

        let home = temp_home("mode-0600");
        fs::create_dir_all(&home).unwrap();
        let path = Config::path(&home);
        fs::write(&path, "cpus = 2\n").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o644
        );

        let mut cfg = Config::default();
        cfg.set("k8s_args", "--debug", &HOST).unwrap();
        cfg.save(&home).unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );

        fs::remove_dir_all(&home).unwrap();
    }
}
