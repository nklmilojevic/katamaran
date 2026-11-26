//! The user-facing commands that talk to the daemon.

use std::io::Write;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, bail};

use crate::api::{self, Call, ConfigView, DockerState, LogSource, LogsParams, Status};
use crate::client::{self, Client};
use crate::config;
use crate::state::VmState;

#[derive(clap::Args)]
pub struct StartArgs {
    /// Seconds to wait for Docker.
    #[arg(long, default_value_t = 180)]
    pub timeout: u64,
}

#[derive(clap::Args)]
pub struct StatusArgs {
    /// Print the full status as JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(clap::Args)]
pub struct LogsArgs {
    /// Keep printing new lines as they arrive.
    #[arg(short, long)]
    pub follow: bool,
    /// Lines to show from the end.
    #[arg(short = 'n', long, default_value_t = 100)]
    pub lines: usize,
    /// Which log: daemon, console (the guest), vm or gvproxy.
    #[arg(long, value_enum, default_value = "daemon")]
    pub source: Source,
    /// Shorthand for --source console.
    #[arg(long, conflicts_with = "source")]
    pub console: bool,
}

#[derive(Clone, Copy, clap::ValueEnum)]
pub enum Source {
    Daemon,
    Console,
    Vm,
    Gvproxy,
}

#[derive(clap::Subcommand)]
pub enum ConfigCmd {
    /// Print one setting, or all of them.
    Get { key: Option<String> },
    /// Change settings: `set cpus 2`, `set cpus=2 memory=8G disk=200G mount=off`, or `set k8s_args="--debug"` (use `--` for bare hyphen-leading values: `set -- k8s_args --debug`).
    Set {
        #[arg(required = true, num_args = 1..)]
        pairs: Vec<String>,
    },
}

/// Make sure a daemon answers, starting one if needed.
/// If an idle daemon is running a different version, recycle it so the current
/// binary takes over cleanly.
pub fn ensure_daemon() -> anyhow::Result<Client> {
    ensure_daemon_inner(false)
}

fn ensure_daemon_inner(mut recycled_stale: bool) -> anyhow::Result<Client> {
    let sock = client::socket_path()?;
    if let Ok(mut c) = Client::connect(&sock) {
        if let Ok(status) = c.status()
            && !status.state.is_active()
            && status.versions.katamaran != env!("CARGO_PKG_VERSION")
        {
            let pid = Some(status.daemon.pid);
            let _ = c.call(&Call::Shutdown);
            crate::setup::wait_gone(&sock, pid, Duration::from_secs(70))?;
            recycled_stale = true;
        } else {
            return Ok(c);
        }
    }
    let started_by_launchd = if recycled_stale {
        crate::setup::restart_launchd_for_current_exe()?
    } else {
        crate::setup::kickstart_if_installed()?
    };
    if !started_by_launchd {
        spawn_daemon()?;
    }
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Ok(mut c) = Client::connect(&sock) {
            if recycled_stale {
                let status = c.status()?;
                verify_restarted_version(&status.versions.katamaran, env!("CARGO_PKG_VERSION"))?;
            }
            return Ok(c);
        }
        if Instant::now() > deadline {
            let log = crate::state_dir()?.join("logs/daemon.log");
            bail!("the daemon did not come up; see {}", log.display());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn verify_restarted_version(actual: &str, expected: &str) -> anyhow::Result<()> {
    if actual != expected {
        bail!(
            "daemon restarted as version {actual}, expected {expected}; run `kat setup` to update the LaunchAgent"
        );
    }
    Ok(())
}

/// Run `katamaran daemon` detached from this terminal.
fn spawn_daemon() -> anyhow::Result<()> {
    let exe = std::env::current_exe()?.canonicalize()?;
    Command::new(exe)
        .arg("daemon")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .process_group(0)
        .spawn()
        .context("spawn the daemon")?;
    Ok(())
}

pub fn start(args: StartArgs) -> anyhow::Result<()> {
    start_with(args, false)
}

fn start_with(args: StartArgs, recycled_stale: bool) -> anyhow::Result<()> {
    let t = Instant::now();
    let mut c = ensure_daemon_inner(recycled_stale)?;
    let status: Status = serde_json::from_value(c.call(&Call::Start)?)?;
    if status.docker.state == DockerState::Ready {
        let _ = crate::setup::ensure_default_docker_context();
        println!("katamaran is already running");
        return Ok(());
    }
    let deadline = t + Duration::from_secs(args.timeout);
    loop {
        let s = c.status()?;
        match (s.state, s.docker.state) {
            (_, DockerState::Ready) => {
                let _ = crate::setup::ensure_default_docker_context();
                println!(
                    "katamaran: Docker ready in {:.2} s ({})",
                    t.elapsed().as_secs_f64(),
                    s.docker.socket.display()
                );
                if let Some(err) = &s.error {
                    eprintln!("katamaran: warning: {err}");
                }
                return Ok(());
            }
            (_, DockerState::Failed) => {
                bail!("Docker failed to start: {}", s.error.unwrap_or_default())
            }
            (VmState::Error, _) => bail!("the VM failed: {}", s.error.unwrap_or_default()),
            (VmState::Stopped, _) => bail!("the VM stopped while starting"),
            _ => {}
        }
        if Instant::now() > deadline {
            bail!("Docker was not ready after {}s", args.timeout);
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

pub fn stop() -> anyhow::Result<()> {
    let t = Instant::now();
    let Ok(mut c) = Client::connect(&client::socket_path()?) else {
        println!("katamaran is not running");
        return Ok(());
    };
    c.call(&Call::Stop { wait: true })?;
    println!("katamaran: stopped in {:.2} s", t.elapsed().as_secs_f64());
    Ok(())
}

pub fn restart(args: StartArgs) -> anyhow::Result<()> {
    let sock = client::socket_path()?;
    let mut recycled_stale = false;
    if let Ok(mut c) = Client::connect(&sock) {
        let s = c.status()?;
        if s.versions.katamaran != env!("CARGO_PKG_VERSION") {
            let t = Instant::now();
            println!(
                "katamaran: restarting daemon ({} -> {})",
                s.versions.katamaran,
                env!("CARGO_PKG_VERSION")
            );
            c.call(&Call::Shutdown)?;
            crate::setup::wait_gone(&sock, Some(s.daemon.pid), Duration::from_secs(70))?;
            println!("katamaran: stopped in {:.2} s", t.elapsed().as_secs_f64());
            recycled_stale = true;
        } else if s.state.is_active() {
            let t = Instant::now();
            c.call(&Call::Stop { wait: true })?;
            println!("katamaran: stopped in {:.2} s", t.elapsed().as_secs_f64());
        }
    }
    start_with(args, recycled_stale)
}

pub fn status(args: StatusArgs) -> anyhow::Result<()> {
    let status = match Client::connect(&client::socket_path()?) {
        Ok(mut c) => c.status()?,
        Err(_) => {
            if args.json {
                println!("{{\"state\":\"stopped\",\"daemon\":null}}");
            } else {
                println!("katamaran: not running (no daemon)");
            }
            return Ok(());
        }
    };
    if args.json {
        println!("{}", serde_json::to_string_pretty(&status)?);
    } else {
        print!("{}", summary(&status));
    }
    Ok(())
}

/// The compact human view of a status.
pub fn summary(s: &Status) -> String {
    summary_for(s, env!("CARGO_PKG_VERSION"))
}

fn summary_for(s: &Status, cli_version: &str) -> String {
    let mut out = String::new();
    let mut line = |k: &str, v: String| out.push_str(&format!("  {k:<10} {v}\n"));
    let head = match (s.state, s.uptime_s) {
        (VmState::Running, Some(up)) => format!("running, up {}", duration(up)),
        (state, _) => state.as_str().to_string(),
    };
    let mut text = format!("katamaran: {head}\n");
    if let Some(e) = &s.error {
        line("error", e.clone());
    }
    let docker = match (
        s.docker.state,
        s.vm.as_ref().and_then(|v| v.docker_ready_ms),
    ) {
        (DockerState::Ready, Some(ms)) => format!("ready in {:.2} s", ms / 1000.0),
        (d, _) => format!("{d:?}").to_lowercase(),
    };
    line(
        "docker",
        format!("{docker} · unix://{}", s.docker.socket.display()),
    );
    if s.k8s.enabled || s.k8s.state != "stopped" {
        let mut k = s.k8s.state.clone();
        if let Some(p) = s.k8s.api_port {
            k.push_str(&format!(
                " · https://127.0.0.1:{p} (context {})",
                s.k8s.context
            ));
        }
        line("k8s", k);
    }
    if let Some(vm) = &s.vm {
        let mut v = format!(
            "{} CPUs · {} MiB · {} disk{}",
            vm.cpus,
            vm.memory_mib,
            format_disk_bytes(vm.attached_disk_bytes()),
            if vm.disk_resize_failed {
                " (filesystem resize failed)"
            } else {
                ""
            }
        );
        if let Some(u) = &s.usage {
            v.push_str(&format!(
                " · guest {:.0}% CPU, {}/{} MiB",
                u.guest_cpu_percent, u.guest_mem_used_mib, u.guest_mem_total_mib
            ));
        }
        line("vm", v);
        if let Some(u) = &s.usage {
            line(
                "host",
                format!(
                    "katamaran-vm {:.0}% CPU, {} MiB footprint",
                    u.host_cpu_percent, u.host_footprint_mib
                ),
            );
        }
    } else {
        line(
            "resources",
            format!(
                "{} CPUs · {} MiB · {} GiB disk",
                s.config.cpus,
                s.config.memory_mib,
                s.saved_disk_gib()
            ),
        );
    }
    let files = match (&s.mount.path, s.mount.mounted, &s.mount.error) {
        _ if !s.mount.enabled => "off".to_string(),
        (Some(p), true, _) => format!("{} (mounted)", p.display()),
        (_, false, Some(e)) => format!("not mounted: {e}"),
        (Some(p), false, None) => format!("{} (not mounted)", p.display()),
        (None, _, _) => "no mount point (KATAMARAN_HOME is set; set mount_path)".into(),
    };
    line("files", files);
    if !s.ports.is_empty() {
        let ports: Vec<String> = s
            .ports
            .iter()
            .map(|p| {
                if p.addresses.is_empty() || p.addresses == [std::net::Ipv4Addr::LOCALHOST] {
                    format!("{}/{}", p.port, p.proto)
                } else {
                    format!(
                        "{}:{}/{}",
                        crate::net::addrs_label(&p.addresses),
                        p.port,
                        p.proto
                    )
                }
            })
            .collect();
        line("ports", ports.join(", "));
    }
    if !s.port_errors.is_empty() {
        let failed: Vec<String> = s
            .port_errors
            .iter()
            .map(|p| format!("{}/{} ({})", p.port, p.proto, p.error))
            .collect();
        line("port err", failed.join("; "));
    }
    let mut versions = format!(
        "katamaran {} · protocol {}",
        s.versions.katamaran, s.versions.protocol
    );
    if let Some(k) = &s.versions.kernel {
        versions.push_str(&format!(" · kernel {k}"));
    }
    line("versions", versions);
    if s.versions.katamaran != cli_version {
        line(
            "upgrade",
            format!(
                "daemon is on {}, CLI is {cli_version}; run `kat restart` to upgrade",
                s.versions.katamaran
            ),
        );
    }
    if let Some(vm) = &s.vm {
        let attached_bytes = vm.attached_disk_bytes();
        let explicit_disk = s.explicit_disk_target_gib();
        let disk_pending = vm.disk_resize_failed
            || explicit_disk.is_some_and(|target| (u64::from(target) << 30) > attached_bytes);
        if vm.cpus != s.config.cpus || vm.memory_mib != s.config.memory_mib || disk_pending {
            let next_disk_bytes = explicit_disk.map_or(attached_bytes, |target| {
                (u64::from(target) << 30).max(attached_bytes)
            });
            line(
                "pending",
                format!(
                    "{} CPUs · {} MiB · {} disk after a restart",
                    s.config.cpus,
                    s.config.memory_mib,
                    format_disk_bytes(next_disk_bytes)
                ),
            );
        }
    }
    text.push_str(&out);
    text
}

fn format_disk_bytes(bytes: u64) -> String {
    const GIB: u64 = 1 << 30;
    if bytes.is_multiple_of(GIB) {
        format!("{} GiB", bytes / GIB)
    } else {
        let tenths = (u128::from(bytes) * 10 / u128::from(GIB)) as u64;
        format!("{}.{} GiB", tenths / 10, tenths % 10)
    }
}

fn duration(secs: f64) -> String {
    let s = secs as u64;
    match s {
        0..60 => format!("{s}s"),
        60..3600 => format!("{}m{:02}s", s / 60, s % 60),
        _ => format!("{}h{:02}m", s / 3600, (s % 3600) / 60),
    }
}

pub fn logs(args: LogsArgs) -> anyhow::Result<()> {
    let source = if args.console {
        LogSource::Console
    } else {
        match args.source {
            Source::Daemon => LogSource::Daemon,
            Source::Console => LogSource::Console,
            Source::Vm => LogSource::Vm,
            Source::Gvproxy => LogSource::Gvproxy,
        }
    };
    let params = LogsParams {
        source,
        lines: Some(args.lines),
        offset: None,
        follow: false,
    };
    let mut c = Client::connect_default()?;
    let mut stdout = std::io::stdout().lock();
    if args.follow {
        c.follow(params, |chunk| {
            stdout.write_all(chunk.data.as_bytes()).is_ok() && stdout.flush().is_ok()
        })
    } else {
        let chunk: api::LogChunk = serde_json::from_value(c.call(&Call::Logs(params))?)?;
        stdout.write_all(chunk.data.as_bytes())?;
        Ok(())
    }
}

const OFFLINE_DISK_LOCK_WAIT: Duration = Duration::from_secs(2);

enum ConfigTarget {
    Daemon(Client),
    Offline {
        _config_lock: std::fs::File,
        _disk_lock: Option<std::fs::File>,
    },
}

/// Either connect to the running daemon or acquire the short-lived per-home
/// config lock (and, for disk edits, the short-lived disk preparation lock).
///
/// Unlike `katamaran.lock` (held for a foreground VM's entire lifetime),
/// `config.lock` and `disk.lock` are only held during config saves and
/// host-side `data.img` preparation, so settings remain editable while an
/// already-started foreground VM runs.
fn connect_or_lock_offline(
    home: &Path,
    sock: &Path,
    edits_disk: bool,
    wait: Duration,
) -> anyhow::Result<ConfigTarget> {
    if let Ok(c) = Client::connect(sock) {
        return Ok(ConfigTarget::Daemon(c));
    }
    let deadline = Instant::now() + wait;
    loop {
        let config_lock = config::lock_config(home)?;
        if let Ok(c) = Client::connect(sock) {
            return Ok(ConfigTarget::Daemon(c));
        }
        if !edits_disk {
            return Ok(ConfigTarget::Offline {
                _config_lock: config_lock,
                _disk_lock: None,
            });
        }
        if let Some(disk_lock) = config::try_lock_disk(home)? {
            return Ok(ConfigTarget::Offline {
                _config_lock: config_lock,
                _disk_lock: Some(disk_lock),
            });
        }
        // Release config.lock before waiting so unrelated config saves and
        // daemon RPCs stay unblocked while disk preparation finishes.
        drop(config_lock);
        if Instant::now() >= deadline {
            bail!(
                "cannot change disk_gib while a VM is preparing its data disk; retry in a moment"
            );
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

/// Apply `values` offline while holding `<home>/run/config.lock` (plus
/// `<home>/run/disk.lock` when editing disk), or via daemon RPC if a daemon
/// is listening.
fn apply_config_set_at(
    home: &Path,
    sock: &Path,
    values: std::collections::BTreeMap<String, String>,
    k8s_opts_set: bool,
    wait: Duration,
) -> anyhow::Result<(bool, bool, Option<bool>)> {
    let edits_disk = values.contains_key("disk") || values.contains_key("disk_gib");
    match connect_or_lock_offline(home, sock, edits_disk, wait)? {
        ConfigTarget::Daemon(mut c) => {
            let v: ConfigView = serde_json::from_value(c.call(&Call::ConfigSet(values))?)?;
            let k8s_state = if k8s_opts_set && !v.k8s_toggled {
                c.status().ok().map(|s| s.k8s.state)
            } else {
                None
            };
            let k8s_active = should_show_k8s_restart_hint(k8s_opts_set, &v, k8s_state.as_deref());
            Ok((v.restart_required, k8s_active, None))
        }
        ConfigTarget::Offline {
            _config_lock,
            _disk_lock,
        } => {
            let mut cfg = config::Config::load(home)?;
            let host = config::Host::current();
            cfg.apply_values(&values, &host, home)?;
            cfg.save(home)?;
            let launch_at_login = values
                .contains_key("launch_at_login")
                .then_some(cfg.launch_at_login);
            Ok((false, false, launch_at_login))
        }
    }
}

pub fn config(cmd: ConfigCmd) -> anyhow::Result<()> {
    // Settings work without a daemon too; the file is the source of truth.
    let home = crate::state_dir()?;
    let sock = client::socket_path()?;
    match cmd {
        ConfigCmd::Get { key } => {
            let cfg = match Client::connect(&sock).ok() {
                Some(mut c) => serde_json::from_value::<ConfigView>(c.call(&Call::ConfigGet)?)?
                    .resolved_config(),
                None => config::Config::load(&home)?,
            };
            match key {
                Some(k) => println!("{}", cfg.get(&k)?),
                None => {
                    for k in config::KEYS {
                        println!("{k} = {}", cfg.get(k)?);
                    }
                }
            }
        }
        ConfigCmd::Set { pairs } => {
            let values = parse_pairs(&pairs)?;
            // Settings that apply at the next Kubernetes start.
            let k8s_opts_set = ["k8s_disable", "k8s_args", "k8s_version"]
                .iter()
                .any(|k| values.contains_key(*k));
            let (restart, k8s_restart, launch_at_login) = apply_config_set_at(
                &home,
                &sock,
                values,
                k8s_opts_set,
                OFFLINE_DISK_LOCK_WAIT,
            )?;
            if let Some(enabled) = launch_at_login {
                crate::setup::set_run_at_load(enabled, None)?;
            }
            println!("saved");
            if restart {
                println!("`katamaran restart` applies it to the running VM");
            }
            if k8s_restart {
                println!(
                    "`kat k8s stop && kat k8s start` applies Kubernetes changes to the running cluster"
                );
            }
        }
    }
    Ok(())
}

fn should_show_k8s_restart_hint(
    k8s_opts_set: bool,
    view: &ConfigView,
    k8s_state: Option<&str>,
) -> bool {
    // A start in progress may already have read the saved settings, so a
    // starting cluster gets the hint too (at worst one unneeded restart).
    k8s_opts_set && !view.k8s_toggled && matches!(k8s_state, Some("running" | "starting"))
}

/// `["cpus", "2"]` or `["cpus=2", "memory=8G"]`.
pub fn parse_pairs(args: &[String]) -> anyhow::Result<std::collections::BTreeMap<String, String>> {
    let mut out = std::collections::BTreeMap::new();
    if args.len() == 2 && !args[0].contains('=') {
        out.insert(args[0].clone(), args[1].clone());
        return Ok(out);
    }
    for a in args {
        let (k, v) = a
            .split_once('=')
            .with_context(|| format!("expected key=value, got {a:?}"))?;
        out.insert(k.trim().to_string(), v.trim().to_string());
    }
    Ok(out)
}

pub fn version() -> anyhow::Result<()> {
    println!(
        "katamaran {} (protocol {})",
        env!("CARGO_PKG_VERSION"),
        kat_proto::PROTOCOL_VERSION
    );
    if let Ok(mut c) = Client::connect(&client::socket_path()?)
        && let Ok(v) = c.call(&Call::Version)
        && let Ok(v) = serde_json::from_value::<api::Versions>(v)
    {
        print!("daemon    {} (protocol {})", v.katamaran, v.protocol);
        match v.kernel {
            Some(k) => println!(", guest kernel {k}"),
            None => println!(),
        }
        if v.katamaran != env!("CARGO_PKG_VERSION") {
            println!("note      daemon version differs; run `kat restart` to upgrade");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::*;
    use crate::config::WireConfig;

    #[test]
    fn parses_config_pairs() {
        let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        let p = parse_pairs(&s(&["cpus", "2"])).unwrap();
        assert_eq!(p["cpus"], "2");
        let p = parse_pairs(&s(&["cpus=2", "memory = 8G"])).unwrap();
        assert_eq!(p["memory"], "8G");
        assert!(parse_pairs(&s(&["cpus"])).is_err());
        assert!(parse_pairs(&s(&["cpus", "2", "3"])).is_err());
    }

    #[test]
    fn summarises_a_running_vm() {
        let s = Status {
            state: VmState::Running,
            error: None,
            since_ms: 0,
            uptime_s: Some(75.0),
            daemon: DaemonInfo {
                pid: 1,
                home: "/h".into(),
                started_ms: 0,
                launchd_label: None,
            },
            versions: Versions {
                katamaran: "0.1.0".into(),
                protocol: 3,
                kernel: Some("6.18".into()),
            },
            config: WireConfig {
                cpus: 2,
                ..WireConfig::default()
            },
            disk_gib: Some(64),
            disk_target_gib: None,
            vm: Some(VmInfo {
                cpus: 4,
                memory_mib: 4096,
                disk_gib: 64,
                disk_bytes: None,
                requested_disk_gib: Some(64),
                disk_resize_failed: false,
                pid: Some(2),
                boot_ms: Some(100.0),
                docker_ready_ms: Some(1500.0),
            }),
            usage: Some(Sample {
                guest_cpu_percent: 3.0,
                guest_mem_used_mib: 500,
                guest_mem_total_mib: 3900,
                ..Default::default()
            }),
            docker: DockerInfo {
                state: DockerState::Ready,
                socket: "/h/run/docker.sock".into(),
            },
            k8s: K8sStatus::default(),
            mount: MountInfo {
                enabled: true,
                path: Some("/Users/me/Katamaran".into()),
                mounted: true,
                error: None,
            },
            ports: vec![Port {
                proto: "tcp".into(),
                port: 8080,
                addresses: vec![std::net::Ipv4Addr::LOCALHOST],
            }],
            port_errors: vec![],
        };
        let text = summary(&s);
        assert!(text.starts_with("katamaran: running, up 1m15s\n"), "{text}");
        assert!(text.contains("ready in 1.50 s"), "{text}");
        assert!(text.contains("guest 3% CPU, 500/3900 MiB"), "{text}");
        assert!(text.contains("/Users/me/Katamaran (mounted)"), "{text}");
        assert!(text.contains("8080/tcp"), "{text}");
        assert!(text.contains("4 CPUs · 4096 MiB · 64 GiB disk"), "{text}");
        assert!(!text.contains("port err"), "{text}");
        assert!(text.contains("pending"), "{text}");

        // Pending also triggers when only disk_gib changed.
        let mut disk_pending = s.clone();
        disk_pending.config.cpus = 4;
        disk_pending.config.memory_mib = 4096;
        disk_pending.disk_gib = Some(500);
        disk_pending.disk_target_gib = Some(500);
        let disk_text = summary(&disk_pending);
        assert!(
            disk_text.contains("4 CPUs · 4096 MiB · 500 GiB disk after a restart"),
            "{disk_text}"
        );

        // Retaining a larger foreground-grown image reports 500 GiB attached
        // without creating perpetual pending work when config stays at 64 GiB.
        let mut retained = s.clone();
        retained.config.cpus = 4;
        retained.config.memory_mib = 4096;
        retained.disk_gib = Some(64);
        retained.disk_target_gib = None;
        retained.vm.as_mut().unwrap().disk_gib = 500;
        retained.vm.as_mut().unwrap().requested_disk_gib = Some(64);
        let retained_text = summary(&retained);
        assert!(
            retained_text.contains("4 CPUs · 4096 MiB · 500 GiB disk"),
            "{retained_text}"
        );
        assert!(
            !retained_text.contains("pending"),
            "retained larger disk must not show pending: {retained_text}"
        );

        // If another setting changes while retaining a larger disk, pending
        // reports the effective 500 GiB disk size rather than claiming a shrink.
        let mut retained_cpu = retained.clone();
        retained_cpu.config.cpus = 2;
        let retained_cpu_text = summary(&retained_cpu);
        assert!(
            retained_cpu_text.contains("2 CPUs · 4096 MiB · 500 GiB disk after a restart"),
            "{retained_cpu_text}"
        );

        // If another setting changes while retaining a smaller custom image
        // under default disk_gib, pending keeps the smaller 8 GiB size.
        let mut retained_small = s.clone();
        retained_small.config.cpus = 2;
        retained_small.config.memory_mib = 4096;
        retained_small.disk_gib = Some(64);
        retained_small.disk_target_gib = None;
        retained_small.vm.as_mut().unwrap().disk_gib = 8;
        retained_small.vm.as_mut().unwrap().requested_disk_gib = Some(64);
        let retained_small_text = summary(&retained_small);
        assert!(
            retained_small_text.contains("2 CPUs · 4096 MiB · 8 GiB disk after a restart"),
            "{retained_small_text}"
        );

        // Explicitly configuring disk=64G against an attached 8 GiB image
        // shows pending growth to 64 GiB after a restart.
        let mut explicit_small = s.clone();
        explicit_small.config.cpus = 4;
        explicit_small.config.memory_mib = 4096;
        explicit_small.disk_gib = Some(64);
        explicit_small.disk_target_gib = Some(64);
        explicit_small.vm.as_mut().unwrap().disk_gib = 8;
        explicit_small.vm.as_mut().unwrap().requested_disk_gib = Some(64);
        let explicit_small_text = summary(&explicit_small);
        assert!(
            explicit_small_text.contains("4 CPUs · 4096 MiB · 64 GiB disk after a restart"),
            "{explicit_small_text}"
        );

        // A fractional 63.5 GiB image raised explicitly to 64 GiB still shows
        // pending growth to 64 GiB (not masked by GiB ceil rounding).
        let mut fractional = s.clone();
        fractional.config.cpus = 4;
        fractional.config.memory_mib = 4096;
        fractional.disk_gib = Some(64);
        fractional.disk_target_gib = Some(64);
        fractional.vm.as_mut().unwrap().disk_gib = 64;
        fractional.vm.as_mut().unwrap().disk_bytes = Some((63u64 << 30) + (512u64 << 20));
        let fractional_text = summary(&fractional);
        assert!(
            fractional_text.contains("4 CPUs · 4096 MiB · 63.5 GiB disk"),
            "{fractional_text}"
        );
        assert!(
            fractional_text.contains("4 CPUs · 4096 MiB · 64 GiB disk after a restart"),
            "{fractional_text}"
        );
        assert_eq!(format_disk_bytes((64u64 << 30) - 4096), "63.9 GiB");

        // A guest resize failure keeps a restart pending even when the host
        // image already matches the saved target.
        let mut resize_failed = s.clone();
        resize_failed.config.cpus = 4;
        resize_failed.config.memory_mib = 4096;
        resize_failed.disk_gib = Some(500);
        resize_failed.disk_target_gib = Some(500);
        resize_failed.vm.as_mut().unwrap().disk_gib = 500;
        resize_failed.vm.as_mut().unwrap().disk_bytes = Some(500u64 << 30);
        resize_failed.vm.as_mut().unwrap().disk_resize_failed = true;
        let resize_failed_text = summary(&resize_failed);
        assert!(
            resize_failed_text
                .contains("4 CPUs · 4096 MiB · 500 GiB disk (filesystem resize failed)"),
            "{resize_failed_text}"
        );
        assert!(
            resize_failed_text.contains("4 CPUs · 4096 MiB · 500 GiB disk after a restart"),
            "{resize_failed_text}"
        );

        assert!(
            !text.contains("k8s"),
            "default disabled k8s should be omitted: {text}"
        );

        let mut with_port_err = s.clone();
        with_port_err.ports.push(Port {
            proto: "tcp".into(),
            port: 8443,
            addresses: vec![std::net::Ipv4Addr::new(172, 16, 1, 200)],
        });
        with_port_err.port_errors.push(PortError {
            proto: "tcp".into(),
            port: 80,
            error: "address already in use".into(),
        });
        let err_text = summary(&with_port_err);
        assert!(
            err_text.contains("8080/tcp, 172.16.1.200:8443/tcp"),
            "{err_text}"
        );
        assert!(
            err_text.contains("port err   80/tcp (address already in use)"),
            "{err_text}"
        );

        let same = summary_for(&s, "0.1.0");
        assert!(
            !same.contains("upgrade"),
            "matching daemon version should not warn: {same}"
        );
        let stale = summary_for(&s, "0.6.0");
        assert!(
            stale.contains("daemon is on 0.1.0, CLI is 0.6.0; run `kat restart` to upgrade"),
            "mismatched daemon version should warn: {stale}"
        );
        assert!(verify_restarted_version("0.6.0", "0.6.0").is_ok());
        let err = verify_restarted_version("0.1.0", "0.6.0").unwrap_err();
        assert!(
            err.to_string().contains("kat setup"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn config_set_cli_accepts_hyphenated_k8s_args_and_preserves_help() {
        use clap::Parser;

        #[derive(clap::Parser)]
        struct Wrapper {
            #[command(subcommand)]
            cmd: ConfigCmd,
        }

        // Explicit `--` allows bare hyphen-leading values.
        let w = Wrapper::try_parse_from(["config", "set", "--", "k8s_args", "--disable=traefik"])
            .unwrap();
        let ConfigCmd::Set { pairs } = w.cmd else {
            panic!("expected ConfigCmd::Set");
        };
        let map = parse_pairs(&pairs).unwrap();
        assert_eq!(
            map.get("k8s_args").map(String::as_str),
            Some("--disable=traefik")
        );

        // Inline `k8s_args=...` works directly without `--`.
        let w2 = Wrapper::try_parse_from([
            "config",
            "set",
            "k8s_args=--disable=traefik --debug",
            "k8s_disable=traefik",
        ])
        .unwrap();
        let ConfigCmd::Set { pairs } = w2.cmd else {
            panic!("expected ConfigCmd::Set");
        };
        let map2 = parse_pairs(&pairs).unwrap();
        assert_eq!(
            map2.get("k8s_args").map(String::as_str),
            Some("--disable=traefik --debug")
        );
        assert_eq!(map2.get("k8s_disable").map(String::as_str), Some("traefik"));

        // `--help` after a setting still triggers Clap's help output.
        let err = Wrapper::try_parse_from(["config", "set", "cpus=2", "--help"])
            .err()
            .expect("expected DisplayHelp");
        assert_eq!(err.kind(), clap::error::ErrorKind::DisplayHelp);
    }

    #[test]
    fn k8s_restart_hint_respects_actual_kubernetes_transition() {
        let mut view = ConfigView {
            config: Config {
                kubernetes: true,
                ..Config::default()
            },
            path: "/tmp/config.toml".into(),
            restart_required: false,
            k8s_toggled: false,
        };

        // Running cluster receiving unchanged kubernetes=on + new k8s_disable shows the hint.
        assert!(should_show_k8s_restart_hint(true, &view, Some("running")));
        assert!(should_show_k8s_restart_hint(true, &view, Some("starting")));
        assert!(!should_show_k8s_restart_hint(true, &view, Some("stopped")));
        assert!(!should_show_k8s_restart_hint(false, &view, Some("running")));

        // When kubernetes actually toggled (daemon starts or stops k3s), suppress the hint.
        view.k8s_toggled = true;
        assert!(!should_show_k8s_restart_hint(true, &view, Some("running")));
    }

    #[test]
    fn offline_config_set_serializes_with_disk_prep_and_allows_edits_during_foreground_vm() {
        use std::os::unix::net::UnixListener;

        let home = std::env::temp_dir().join(format!("kat-cli-lock-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&home);
        let run_dir = home.join("run");
        let data_dir = home.join("data");
        std::fs::create_dir_all(&run_dir).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();
        let sock = run_dir.join(api::SOCKET_NAME);
        let vm_lock_path = run_dir.join("katamaran.lock");

        // Pre-existing 64 GiB image with saved target = 500 GiB.
        std::fs::File::create(data_dir.join("data.img"))
            .unwrap()
            .set_len(64u64 << 30)
            .unwrap();
        let cfg = config::Config {
            disk_gib: Some(500),
            ..config::Config::default()
        };
        cfg.save(&home).unwrap();

        // Hold katamaran.lock for the entire test to simulate a foreground
        // `katamaran run` owning the home (no daemon.sock).
        let _vm_lock = crate::run::lock(&vm_lock_path).expect("acquire katamaran.lock");

        // 1. While startup disk preparation holds disk.lock (before extending
        //    data.img), an offline disk shrink edit MUST fail.
        let disk_prep_lock = config::lock_disk(&home).expect("acquire disk.lock");
        let mut shrink = std::collections::BTreeMap::new();
        shrink.insert("disk".into(), "200G".into());
        let err = apply_config_set_at(
            &home,
            &sock,
            shrink.clone(),
            false,
            Duration::from_millis(80),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("preparing its data disk"),
            "unexpected err: {err:#}"
        );
        assert_eq!(config::Config::load(&home).unwrap().disk_gib, Some(500));

        // 2. Unrelated offline config edits STILL succeed even while both
        //    katamaran.lock and disk.lock are held.
        let mut cpu_edit = std::collections::BTreeMap::new();
        cpu_edit.insert("cpus".into(), "1".into());
        apply_config_set_at(&home, &sock, cpu_edit, false, Duration::from_millis(80)).unwrap();
        assert_eq!(config::Config::load(&home).unwrap().cpus, 1);

        // 3. If a daemon binds daemon.sock while disk.lock is held,
        //    connect_or_lock_offline switches over to Daemon(client).
        let sock_for_thread = sock.clone();
        let binder = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(40));
            let listener = UnixListener::bind(&sock_for_thread).unwrap();
            let (_conn, _) = listener.accept().unwrap();
        });
        let target = connect_or_lock_offline(&home, &sock, true, Duration::from_secs(2)).unwrap();
        assert!(matches!(target, ConfigTarget::Daemon(_)));
        drop(target);
        binder.join().unwrap();
        let _ = std::fs::remove_file(&sock);

        // 4. Once disk preparation finishes (extending data.img to 500 GiB and
        //    releasing disk.lock) while the foreground VM STILL holds
        //    katamaran.lock: unrelated edits continue working, shrink is
        //    rejected against the 500 GiB image, and growing succeeds.
        std::fs::OpenOptions::new()
            .write(true)
            .open(data_dir.join("data.img"))
            .unwrap()
            .set_len(500u64 << 30)
            .unwrap();
        drop(disk_prep_lock);

        let mut mount_edit = std::collections::BTreeMap::new();
        mount_edit.insert("mount".into(), "off".into());
        apply_config_set_at(&home, &sock, mount_edit, false, Duration::from_millis(80)).unwrap();
        assert!(!config::Config::load(&home).unwrap().mount);

        let err = apply_config_set_at(&home, &sock, shrink, false, Duration::from_millis(80))
            .unwrap_err();
        assert!(
            err.to_string().contains("cannot shrink"),
            "unexpected err: {err:#}"
        );
        assert_eq!(config::Config::load(&home).unwrap().disk_gib, Some(500));

        let mut grow = std::collections::BTreeMap::new();
        grow.insert("disk".into(), "600G".into());
        apply_config_set_at(&home, &sock, grow, false, Duration::from_millis(80)).unwrap();
        assert_eq!(config::Config::load(&home).unwrap().disk_gib, Some(600));

        std::fs::remove_dir_all(&home).unwrap();
    }
}
