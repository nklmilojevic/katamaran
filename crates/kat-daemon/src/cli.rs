//! The user-facing commands that talk to the daemon.

use std::io::Write;
use std::os::unix::process::CommandExt;
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
    /// Change settings: `set cpus 2`, or `set cpus=2 memory=8G mount=off`.
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
        let mut v = format!("{} CPUs · {} MiB", vm.cpus, vm.memory_mib);
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
            format!("{} CPUs · {} MiB", s.config.cpus, s.config.memory_mib),
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
            .map(|p| format!("{}/{}", p.port, p.proto))
            .collect();
        line("ports", ports.join(", "));
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
    if let Some(vm) = &s.vm
        && (vm.cpus != s.config.cpus || vm.memory_mib != s.config.memory_mib)
    {
        line(
            "pending",
            format!(
                "{} CPUs · {} MiB after a restart",
                s.config.cpus, s.config.memory_mib
            ),
        );
    }
    text.push_str(&out);
    text
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

pub fn config(cmd: ConfigCmd) -> anyhow::Result<()> {
    // Settings work without a daemon too; the file is the source of truth.
    let daemon = Client::connect(&client::socket_path()?).ok();
    match cmd {
        ConfigCmd::Get { key } => {
            let cfg = match daemon {
                Some(mut c) => {
                    serde_json::from_value::<ConfigView>(c.call(&Call::ConfigGet)?)?.config
                }
                None => config::Config::load(&crate::state_dir()?)?,
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
            let restart = match daemon {
                Some(mut c) => {
                    let v: ConfigView = serde_json::from_value(c.call(&Call::ConfigSet(values))?)?;
                    v.restart_required
                }
                None => {
                    let home = crate::state_dir()?;
                    let mut cfg = config::Config::load(&home)?;
                    let host = config::Host::current();
                    for (k, v) in &values {
                        cfg.set(k, v, &host)?;
                    }
                    cfg.save(&home)?;
                    if values.contains_key("launch_at_login") {
                        crate::setup::set_run_at_load(cfg.launch_at_login, None)?;
                    }
                    false
                }
            };
            println!("saved");
            if restart {
                println!("`katamaran restart` applies it to the running VM");
            }
        }
    }
    Ok(())
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
    use crate::config::Config;

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
            config: Config {
                cpus: 2,
                ..Config::default()
            },
            vm: Some(VmInfo {
                cpus: 4,
                memory_mib: 4096,
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
            }],
        };
        let text = summary(&s);
        assert!(text.starts_with("katamaran: running, up 1m15s\n"), "{text}");
        assert!(text.contains("ready in 1.50 s"), "{text}");
        assert!(text.contains("guest 3% CPU, 500/3900 MiB"), "{text}");
        assert!(text.contains("/Users/me/Katamaran (mounted)"), "{text}");
        assert!(text.contains("8080/tcp"), "{text}");
        assert!(text.contains("pending"), "{text}");
        assert!(
            !text.contains("k8s"),
            "default disabled k8s should be omitted: {text}"
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
}
