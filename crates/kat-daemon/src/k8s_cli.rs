//! `kat k8s`: manage the optional single-node Kubernetes (k3s) cluster.

use std::process::Command;
use std::time::Instant;

use anyhow::{Context, bail};
use serde_json::Value;

use crate::api::{Call, K8sStatus};
use crate::cli;
use crate::client::Client;
use crate::k8s;

#[derive(clap::Subcommand)]
pub enum K8sCmd {
    /// Show Kubernetes status, API endpoint, kubeconfig context and DNS state.
    Status {
        /// Print the status as JSON.
        #[arg(long)]
        json: bool,
    },
    /// Enable and start Kubernetes (downloads k3s on first use).
    Start,
    /// Stop Kubernetes and disable auto-start (zero overhead when off).
    Stop,
    /// Stop Kubernetes and wipe all cluster state and persistent volumes.
    Reset,
    /// Print the kubeconfig YAML (or its file path with --path).
    Kubeconfig {
        /// Print the path to `<home>/run/kubeconfig.yaml` instead of its contents.
        #[arg(long)]
        path: bool,
    },
    /// Mirror locally built Docker images into Kubernetes immediately.
    SyncImages,
    /// Manage macOS DNS resolution for `*.k8s.kat.local`.
    #[command(subcommand)]
    Dns(DnsCmd),
    /// Forward a local port directly to a Pod IP or Service ClusterIP in the guest.
    Forward {
        /// Guest target `<ip>:<port>` (for example `10.43.0.1:443` or `10.42.0.5:8080`).
        target: String,
        /// Local port on 127.0.0.1 (defaults to the target port).
        #[arg(long)]
        local: Option<u16>,
    },
}

#[derive(clap::Subcommand)]
pub enum DnsCmd {
    /// Show whether `*.k8s.kat.local` DNS resolution is installed on macOS.
    Status,
    /// Install `/etc/resolver/k8s.kat.local` (uses sudo once).
    Install,
    /// Remove `/etc/resolver/k8s.kat.local` (uses sudo once).
    Uninstall,
}

pub fn run(cmd: K8sCmd) -> anyhow::Result<()> {
    match cmd {
        K8sCmd::Status { json } => status(json),
        K8sCmd::Start => start(),
        K8sCmd::Stop => stop(),
        K8sCmd::Reset => reset(),
        K8sCmd::Kubeconfig { path } => kubeconfig(path),
        K8sCmd::SyncImages => sync_images(),
        K8sCmd::Dns(d) => dns(d),
        K8sCmd::Forward { target, local } => forward(&target, local),
    }
}

fn status(json: bool) -> anyhow::Result<()> {
    let mut c = Client::connect_default()?;
    let st = c.status()?;
    if json {
        println!("{}", serde_json::to_string_pretty(&st.k8s)?);
        return Ok(());
    }
    print!("{}", format_status(&st.k8s));
    Ok(())
}

pub fn format_status(k: &K8sStatus) -> String {
    let mut out = String::new();
    let mut line = |label: &str, val: String| out.push_str(&format!("  {label:<12} {val}\n"));
    let state = if !k.enabled && k.state == "stopped" {
        "disabled".into()
    } else {
        k.state.clone()
    };
    line("state", state);
    if !k.version.is_empty() {
        line("version", k.version.clone());
    }
    if let Some(port) = k.api_port {
        line("api", format!("https://127.0.0.1:{port}"));
    }
    if !k.context.is_empty() {
        line("context", k.context.clone());
    }
    if let Some(path) = &k.kubeconfig {
        line("kubeconfig", path.display().to_string());
    }
    let dns_state = if k.dns_installed {
        match k.dns_port {
            Some(p) => format!("*.{} (127.0.0.1:{p}, active)", k.dns_domain),
            None => format!("*.{} (installed, waiting for k8s)", k.dns_domain),
        }
    } else {
        format!(
            "*.{} (not installed; run `kat k8s dns install`)",
            k.dns_domain
        )
    };
    line("dns", dns_state);
    if let Some(err) = &k.error {
        line("error", err.clone());
    }
    out
}

fn start() -> anyhow::Result<()> {
    let t = Instant::now();
    let mut c = cli::ensure_daemon()?;
    let mut on_note = |method: &str, params: &Value| {
        if method == "k8s.progress"
            && let Some(msg) = params.get("message").and_then(Value::as_str)
        {
            eprintln!("katamaran: {msg}");
        }
    };
    let val = c.call_notified(&Call::K8sStart, &mut on_note)?;
    let st: K8sStatus = serde_json::from_value(val)?;
    println!(
        "katamaran: Kubernetes ready in {:.2} s (context: {})",
        t.elapsed().as_secs_f64(),
        st.context
    );
    Ok(())
}

fn stop() -> anyhow::Result<()> {
    let mut c = Client::connect_default()?;
    c.call(&Call::K8sStop)?;
    println!("katamaran: Kubernetes stopped");
    Ok(())
}

fn reset() -> anyhow::Result<()> {
    let mut c = Client::connect_default()?;
    c.call(&Call::K8sReset)?;
    println!("katamaran: Kubernetes cluster reset");
    Ok(())
}

fn kubeconfig(path_only: bool) -> anyhow::Result<()> {
    if path_only {
        let p = k8s::kubeconfig_path(&crate::state_dir()?);
        println!("{}", p.display());
        return Ok(());
    }
    let mut c = Client::connect_default()?;
    let val = c.call(&Call::K8sKubeconfig)?;
    let yaml = val
        .get("kubeconfig")
        .and_then(Value::as_str)
        .context("missing kubeconfig in response")?;
    print!("{yaml}");
    Ok(())
}

fn sync_images() -> anyhow::Result<()> {
    let mut c = Client::connect_default()?;
    c.call(&Call::K8sSyncImages)?;
    println!("katamaran: Docker images synced to Kubernetes");
    Ok(())
}

fn dns(cmd: DnsCmd) -> anyhow::Result<()> {
    match cmd {
        DnsCmd::Status => {
            let active_port = Client::connect_default()
                .ok()
                .and_then(|mut c| c.status().ok())
                .and_then(|s| s.k8s.dns_port);
            if k8s::dns_resolver_installed(active_port) {
                println!(
                    "*.{} resolver installed at {}",
                    k8s::DNS_DOMAIN,
                    k8s::RESOLVER_PATH
                );
            } else {
                println!(
                    "*.{} resolver not installed ({}); run `kat k8s dns install`",
                    k8s::DNS_DOMAIN,
                    k8s::RESOLVER_PATH
                );
            }
            Ok(())
        }
        DnsCmd::Install => {
            let mut c = Client::connect_default().context(
                "start Kubernetes first (`kat k8s start`) before installing DNS resolution",
            )?;
            let st = c.status()?;
            let Some(port) = st.k8s.dns_port else {
                bail!("Kubernetes DNS responder is not running; run `kat k8s start` first");
            };
            let content = k8s::dns_resolver_content(port);
            let script = format!(
                "mkdir -p /etc/resolver && printf '%s' '{}' > {}",
                content.replace('\'', "'\\''"),
                k8s::RESOLVER_PATH
            );
            let status = Command::new("sudo")
                .args(["sh", "-c", &script])
                .status()
                .context("run sudo to install /etc/resolver/k8s.kat.local")?;
            if !status.success() {
                bail!("failed to install {}", k8s::RESOLVER_PATH);
            }
            println!(
                "Installed {} (resolving *.{} via 127.0.0.1:{port})",
                k8s::RESOLVER_PATH,
                k8s::DNS_DOMAIN
            );
            Ok(())
        }
        DnsCmd::Uninstall => {
            let status = Command::new("sudo")
                .args(["rm", "-f", k8s::RESOLVER_PATH])
                .status()
                .context("run sudo to remove /etc/resolver/k8s.kat.local")?;
            if !status.success() {
                bail!("failed to remove {}", k8s::RESOLVER_PATH);
            }
            println!("Removed {}", k8s::RESOLVER_PATH);
            Ok(())
        }
    }
}

fn parse_forward_target(target: &str) -> anyhow::Result<(String, u16)> {
    let sock_addr: std::net::SocketAddr = target
        .parse()
        .with_context(|| format!("expected <ip>:<port> or [<ipv6>]:<port>, got {target:?}"))?;
    if sock_addr.port() == 0 {
        bail!("target port must be non-zero");
    }
    Ok((sock_addr.ip().to_string(), sock_addr.port()))
}

fn forward(target: &str, local: Option<u16>) -> anyhow::Result<()> {
    let (addr, port) = parse_forward_target(target)?;
    let local_port = local.unwrap_or(port);
    let home = crate::state_dir()?;
    k8s::forward_guest_tcp(&home, addr, port, local_port)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_forward_targets_ipv4_and_ipv6() {
        assert_eq!(
            parse_forward_target("10.43.0.1:443").unwrap(),
            ("10.43.0.1".to_string(), 443)
        );
        assert_eq!(
            parse_forward_target("[fd00::1]:8080").unwrap(),
            ("fd00::1".to_string(), 8080)
        );
        assert_eq!(
            parse_forward_target("[::1]:6443").unwrap(),
            ("::1".to_string(), 6443)
        );
        assert!(parse_forward_target("10.43.0.1:0").is_err());
        assert!(parse_forward_target("[::1]:0").is_err());
        assert!(parse_forward_target("not-an-ip:80").is_err());
        assert!(parse_forward_target("10.43.0.1").is_err());
        assert!(parse_forward_target("::1:80").is_err());
    }
}
