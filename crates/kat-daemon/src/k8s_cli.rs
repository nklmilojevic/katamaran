//! `kat k8s`: manage the optional single-node Kubernetes (k3s) cluster.

use std::process::Command;
use std::time::{Duration, Instant};

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
    ///
    /// Cached images in containerd's `k8s.io` namespace are kept by default so
    /// they remain available for reuse (subject to each workload's
    /// `imagePullPolicy`). Pass `--images` to also clear all images in `k8s.io`
    /// (Docker's `moby` namespace is untouched).
    Reset {
        /// Also remove all images in containerd's `k8s.io` namespace.
        #[arg(long)]
        images: bool,
    },
    /// Record the verified k3s version tag of an existing unmarked cluster datastore.
    MarkVersion {
        /// Independently verified k3s release tag (for example `v1.37.2+k3s1`).
        version: String,
    },
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
        K8sCmd::Reset { images } => reset(images),
        K8sCmd::MarkVersion { version } => mark_version(&version),
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
    let dns_state = if k.dns_isolated {
        match k.dns_port {
            Some(p) => format!(
                "*.{} (127.0.0.1:{p}, isolated; {} not used)",
                k.dns_domain,
                k8s::RESOLVER_PATH
            ),
            None => format!(
                "*.{} (isolated; {} not used)",
                k.dns_domain,
                k8s::RESOLVER_PATH
            ),
        }
    } else if k.dns_installed {
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

/// Asynchronous bounded progress writer so a closed (`EPIPE`) or undrained
/// stderr pipe can never panic or block the CLI while reading daemon RPC
/// notifications and the terminal reset result.
struct ProgressPrinter {
    tx: Option<std::sync::mpsc::SyncSender<String>>,
    done: std::sync::mpsc::Receiver<()>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl ProgressPrinter {
    fn spawn<W: std::io::Write + Send + 'static>(mut writer: W) -> Self {
        let (tx, rx) = std::sync::mpsc::sync_channel::<String>(32);
        let (done_tx, done_rx) = std::sync::mpsc::sync_channel::<()>(1);
        let worker = std::thread::Builder::new()
            .name("k8s-reset-progress".into())
            .spawn(move || {
                while let Ok(line) = rx.recv() {
                    if writer.write_all(line.as_bytes()).is_err() {
                        break;
                    }
                }
                let _ = done_tx.try_send(());
            })
            .ok();
        Self {
            tx: worker.as_ref().map(|_| tx),
            done: done_rx,
            worker,
        }
    }

    fn send(&mut self, msg: &str) {
        if let Some(tx) = &self.tx
            && matches!(
                tx.try_send(format!("katamaran: {msg}\n")),
                Err(std::sync::mpsc::TrySendError::Disconnected(_))
            )
        {
            self.tx = None;
        }
    }

    fn finish(mut self, flush_timeout: Duration) {
        self.tx = None;
        if self.done.recv_timeout(flush_timeout).is_ok()
            && let Some(worker) = self.worker.take()
        {
            let _ = worker.join();
        }
    }
}

fn reset(images: bool) -> anyhow::Result<()> {
    let mut c = Client::connect_default()?;
    let mut printer = ProgressPrinter::spawn(std::io::stderr());
    let res = reset_with_client(&mut c, images, &mut |msg| printer.send(msg));
    printer.finish(Duration::from_millis(250));
    res?;
    if images {
        let _ = std::io::Write::write_all(
            &mut std::io::stdout(),
            b"katamaran: Kubernetes cluster reset (k8s.io images cleared)\n",
        );
    } else {
        let _ = std::io::Write::write_all(
            &mut std::io::stdout(),
            b"katamaran: Kubernetes cluster reset\n",
        );
    }
    Ok(())
}

fn mark_version(version: &str) -> anyhow::Result<()> {
    // Accept the same input as `kat config set k8s_version=...` (leading `v` optional).
    let version = &if version.starts_with(|c: char| c.is_ascii_digit()) {
        format!("v{version}")
    } else {
        version.to_string()
    };
    crate::config::validate_k8s_version(version)?;
    let mut c = Client::connect_default().context(
        "`kat k8s mark-version` requires a running VM with Kubernetes stopped; \
         disable Kubernetes auto-start (`kat config set kubernetes=off`) if enabled, \
         start the VM (`kat start`), then retry",
    )?;
    c.call(&Call::K8sMarkVersion {
        version: version.to_string(),
    })?;
    println!("katamaran: recorded cluster datastore version {version}");
    Ok(())
}

fn reset_with_client(
    client: &mut Client,
    images: bool,
    on_progress: &mut dyn FnMut(&str),
) -> anyhow::Result<()> {
    let mut last = String::new();
    let mut on_note = |method: &str, params: &Value| {
        if method == "k8s.progress"
            && let Some(msg) = params.get("message").and_then(Value::as_str)
            && last != msg
        {
            last.clear();
            last.push_str(msg);
            on_progress(msg);
        }
    };
    let res = client.call_notified(
        &Call::K8sReset {
            images,
            progress: true,
        },
        &mut on_note,
    );
    match res {
        Ok(_) => Ok(()),
        Err(e)
            if images
                && e.downcast_ref::<crate::api::RpcError>()
                    .is_some_and(|r| r.code == crate::api::METHOD_NOT_FOUND) =>
        {
            bail!(
                "the running daemon does not support `kat k8s reset --images`; restart the daemon to upgrade"
            )
        }
        Err(e) => Err(e),
    }
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

fn dns_status_line(
    k8s_status: Option<&K8sStatus>,
    resolver_installed: impl FnOnce(Option<u16>) -> bool,
) -> String {
    if let Some(st) = k8s_status
        && st.dns_isolated
    {
        return match st.dns_port {
            Some(port) => format!(
                "*.{} DNS responder is running in isolated mode on 127.0.0.1:{port} ({}=1); {} is not used",
                k8s::DNS_DOMAIN,
                k8s::ISOLATED_DNS_ENV,
                k8s::RESOLVER_PATH
            ),
            None => format!(
                "*.{} DNS responder is running in isolated mode ({}=1); {} is not used",
                k8s::DNS_DOMAIN,
                k8s::ISOLATED_DNS_ENV,
                k8s::RESOLVER_PATH
            ),
        };
    }
    let active_port = k8s_status.and_then(|s| s.dns_port);
    if resolver_installed(active_port) {
        format!(
            "*.{} resolver installed at {}",
            k8s::DNS_DOMAIN,
            k8s::RESOLVER_PATH
        )
    } else {
        format!(
            "*.{} resolver not installed ({}); run `kat k8s dns install`",
            k8s::DNS_DOMAIN,
            k8s::RESOLVER_PATH
        )
    }
}

fn dns_install_port(k8s_status: &K8sStatus) -> anyhow::Result<u16> {
    if k8s_status.dns_isolated {
        bail!(
            "refusing to install {} for an isolated DNS responder ({}=1); \
             run against a non-isolated daemon instead",
            k8s::RESOLVER_PATH,
            k8s::ISOLATED_DNS_ENV
        );
    }
    let Some(port) = k8s_status.dns_port else {
        bail!("Kubernetes DNS responder is not running; run `kat k8s start` first");
    };
    Ok(port)
}

fn dns(cmd: DnsCmd) -> anyhow::Result<()> {
    match cmd {
        DnsCmd::Status => {
            let k8s_status = Client::connect_default()
                .ok()
                .and_then(|mut c| c.status().ok())
                .map(|s| s.k8s);
            println!(
                "{}",
                dns_status_line(k8s_status.as_ref(), k8s::dns_resolver_installed)
            );
            Ok(())
        }
        DnsCmd::Install => {
            let mut c = Client::connect_default().context(
                "start Kubernetes first (`kat k8s start`) before installing DNS resolution",
            )?;
            let st = c.status()?;
            let port = dns_install_port(&st.k8s)?;
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

    #[test]
    fn dns_install_rejects_isolated_responder_before_sudo() {
        let isolated = K8sStatus {
            dns_domain: k8s::DNS_DOMAIN.into(),
            dns_port: Some(54321),
            dns_isolated: true,
            ..K8sStatus::default()
        };
        let err = dns_install_port(&isolated).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("isolated"), "{msg}");
        assert!(msg.contains(k8s::RESOLVER_PATH), "{msg}");

        let normal = K8sStatus {
            dns_domain: k8s::DNS_DOMAIN.into(),
            dns_port: Some(15353),
            dns_isolated: false,
            ..K8sStatus::default()
        };
        assert_eq!(dns_install_port(&normal).unwrap(), 15353);

        let stopped = K8sStatus::default();
        assert!(dns_install_port(&stopped).is_err());
    }

    #[test]
    fn dns_status_reports_isolation_without_recommending_install() {
        let isolated = K8sStatus {
            dns_domain: k8s::DNS_DOMAIN.into(),
            dns_port: Some(54321),
            dns_isolated: true,
            ..K8sStatus::default()
        };
        let line = dns_status_line(Some(&isolated), |_| {
            panic!("isolated DNS status should not check host resolver installation")
        });
        assert!(line.contains("isolated"), "{line}");
        assert!(line.contains("54321"), "{line}");
        assert!(!line.contains("kat k8s dns install"), "{line}");

        let formatted = format_status(&isolated);
        assert!(formatted.contains("isolated"), "{formatted}");
        assert!(!formatted.contains("kat k8s dns install"), "{formatted}");

        let normal = K8sStatus {
            dns_domain: k8s::DNS_DOMAIN.into(),
            dns_port: Some(15353),
            dns_isolated: false,
            ..K8sStatus::default()
        };
        let not_installed = dns_status_line(Some(&normal), |p| {
            assert_eq!(p, Some(15353));
            false
        });
        assert!(
            not_installed.contains("kat k8s dns install"),
            "{not_installed}"
        );

        let installed = dns_status_line(Some(&normal), |p| {
            assert_eq!(p, Some(15353));
            true
        });
        assert!(installed.contains("resolver installed"), "{installed}");
        assert!(!installed.contains("kat k8s dns install"), "{installed}");
    }

    #[test]
    fn parses_reset_images_flag() {
        use clap::Parser;

        #[derive(Parser)]
        struct Wrapper {
            #[command(subcommand)]
            cmd: K8sCmd,
        }

        let default = Wrapper::try_parse_from(["k8s", "reset"]).unwrap();
        assert!(matches!(default.cmd, K8sCmd::Reset { images: false }));

        let with_images = Wrapper::try_parse_from(["k8s", "reset", "--images"]).unwrap();
        assert!(matches!(with_images.cmd, K8sCmd::Reset { images: true }));
    }

    struct TempDirGuard(std::path::PathBuf);

    impl TempDirGuard {
        fn create(prefix: &str) -> Self {
            use std::os::unix::ffi::OsStringExt;
            let template = std::env::temp_dir().join(format!("{prefix}-XXXXXX"));
            let mut bytes = template.into_os_string().into_vec();
            bytes.push(0);
            let ptr = unsafe { libc::mkdtemp(bytes.as_mut_ptr().cast()) };
            assert!(
                !ptr.is_null(),
                "mkdtemp failed: {}",
                std::io::Error::last_os_error()
            );
            bytes.pop();
            Self(std::path::PathBuf::from(std::ffi::OsString::from_vec(
                bytes,
            )))
        }
    }

    impl Drop for TempDirGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn reset_images_fails_on_pre_feature_daemon_without_falling_back() {
        use std::io::{BufRead, BufReader, Write};
        use std::os::unix::net::UnixListener;
        use std::sync::{Arc, Mutex};
        use std::thread;

        let dir = TempDirGuard::create("kat-cli-rc");
        let sock = dir.0.join("daemon.sock");
        let listener = UnixListener::bind(&sock).unwrap();
        let seen = Arc::new(Mutex::new(Vec::<String>::new()));
        let seen_srv = seen.clone();

        let srv = thread::spawn(move || {
            // Simulate a pre-feature daemon that only implements `k8s.reset`.
            let (mut conn, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(conn.try_clone().unwrap());
            let mut line = String::new();
            while reader.read_line(&mut line).unwrap() > 0 {
                let req: crate::api::Request = serde_json::from_str(&line).unwrap();
                seen_srv.lock().unwrap().push(req.method.clone());
                let id = req.id.unwrap_or(Value::Null);
                let resp = if req.method == "k8s.reset" {
                    crate::api::Response::ok(id, Value::Null)
                } else {
                    crate::api::Response::err(
                        id,
                        crate::api::RpcError::new(
                            crate::api::METHOD_NOT_FOUND,
                            format!("unknown method {}", req.method),
                        ),
                    )
                };
                let mut out = serde_json::to_vec(&resp).unwrap();
                out.push(b'\n');
                conn.write_all(&out).unwrap();
                line.clear();
            }
        });

        let mut client = Client::connect(&sock).unwrap();
        let err = reset_with_client(&mut client, true, &mut |_| {}).unwrap_err();
        assert!(
            err.to_string()
                .contains("does not support `kat k8s reset --images`"),
            "unexpected error: {err:#}"
        );
        drop(client);
        srv.join().unwrap();

        assert_eq!(*seen.lock().unwrap(), vec!["k8s.reset_images".to_string()]);
    }

    #[test]
    fn reset_images_consumes_and_deduplicates_progress_notifications() {
        use std::io::{BufRead, BufReader, Write};
        use std::os::unix::net::UnixListener;
        use std::thread;

        let dir = TempDirGuard::create("kat-cli-rp");
        let sock = dir.0.join("daemon.sock");
        let listener = UnixListener::bind(&sock).unwrap();

        let srv = thread::spawn(move || {
            let (mut conn, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(conn.try_clone().unwrap());
            let mut line = String::new();
            assert!(reader.read_line(&mut line).unwrap() > 0);
            let req: crate::api::Request = serde_json::from_str(&line).unwrap();
            assert_eq!(req.method, "k8s.reset_images");
            assert!(req.params.is_null());
            let id = req.id.unwrap();
            for msg in [
                "removing cluster data",
                "waiting for containerd garbage collection",
                "waiting for containerd garbage collection",
                "stopping temporary VM",
            ] {
                let note = crate::api::Notification {
                    jsonrpc: crate::api::VERSION.into(),
                    method: "k8s.progress".into(),
                    params: serde_json::json!({ "message": msg }),
                };
                let mut out = serde_json::to_vec(&note).unwrap();
                out.push(b'\n');
                conn.write_all(&out).unwrap();
            }
            let resp = crate::api::Response::ok(id, Value::Null);
            let mut out = serde_json::to_vec(&resp).unwrap();
            out.push(b'\n');
            conn.write_all(&out).unwrap();
        });

        let mut client = Client::connect(&sock).unwrap();
        let mut progress = Vec::new();
        reset_with_client(&mut client, true, &mut |m| progress.push(m.to_string())).unwrap();
        srv.join().unwrap();

        assert_eq!(
            progress,
            vec![
                "removing cluster data",
                "waiting for containerd garbage collection",
                "stopping temporary VM",
            ]
        );
    }

    #[test]
    fn plain_reset_requests_progress_and_works_with_legacy_and_notified_daemons() {
        use std::io::{BufRead, BufReader, Write};
        use std::os::unix::net::UnixListener;
        use std::thread;

        let dir = TempDirGuard::create("kat-cli-pl");
        let sock = dir.0.join("daemon.sock");
        let listener = UnixListener::bind(&sock).unwrap();

        let srv = thread::spawn(move || {
            // 1. Notified daemon streams progress for plain `k8s.reset`.
            let (mut conn, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(conn.try_clone().unwrap());
            let mut line = String::new();
            assert!(reader.read_line(&mut line).unwrap() > 0);
            let req: crate::api::Request = serde_json::from_str(&line).unwrap();
            assert_eq!(req.method, "k8s.reset");
            assert_eq!(req.params, serde_json::json!({ "progress": true }));
            let id = req.id.unwrap();
            for msg in [
                "waiting for active Kubernetes operation",
                "waiting for active Kubernetes operation",
                "removing cluster data",
            ] {
                let note = crate::api::Notification {
                    jsonrpc: crate::api::VERSION.into(),
                    method: "k8s.progress".into(),
                    params: serde_json::json!({ "message": msg }),
                };
                let mut out = serde_json::to_vec(&note).unwrap();
                out.push(b'\n');
                conn.write_all(&out).unwrap();
            }
            let resp = crate::api::Response::ok(id, Value::Null);
            let mut out = serde_json::to_vec(&resp).unwrap();
            out.push(b'\n');
            conn.write_all(&out).unwrap();

            // 2. Pre-feature daemon ignores `params` and returns only the response.
            let (mut conn, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(conn.try_clone().unwrap());
            line.clear();
            assert!(reader.read_line(&mut line).unwrap() > 0);
            let req: crate::api::Request = serde_json::from_str(&line).unwrap();
            assert_eq!(req.method, "k8s.reset");
            let resp = crate::api::Response::ok(req.id.unwrap(), Value::Null);
            let mut out = serde_json::to_vec(&resp).unwrap();
            out.push(b'\n');
            conn.write_all(&out).unwrap();
        });

        let mut client = Client::connect(&sock).unwrap();
        let mut progress = Vec::new();
        reset_with_client(&mut client, false, &mut |m| progress.push(m.to_string())).unwrap();
        assert_eq!(
            progress,
            vec![
                "waiting for active Kubernetes operation",
                "removing cluster data",
            ]
        );

        let mut legacy_client = Client::connect(&sock).unwrap();
        let mut legacy_progress = Vec::new();
        reset_with_client(&mut legacy_client, false, &mut |m| {
            legacy_progress.push(m.to_string())
        })
        .unwrap();
        assert!(legacy_progress.is_empty());
        srv.join().unwrap();
    }

    #[test]
    fn reset_keeps_reading_result_when_progress_writer_fails_or_blocks() {
        use std::io::{self, BufRead, BufReader, Write};
        use std::os::unix::net::UnixListener;
        use std::sync::{Arc, Mutex, mpsc};
        use std::thread;

        struct BrokenPipeWriter(Arc<Mutex<usize>>);
        impl Write for BrokenPipeWriter {
            fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
                *self.0.lock().unwrap() += 1;
                Err(io::Error::new(io::ErrorKind::BrokenPipe, "broken pipe"))
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        struct BlockingWriter(mpsc::Receiver<()>);
        impl Write for BlockingWriter {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                let _ = self.0.recv();
                Ok(buf.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let dir = TempDirGuard::create("kat-cli-ep");
        let sock = dir.0.join("daemon.sock");
        let listener = UnixListener::bind(&sock).unwrap();
        let srv = thread::spawn(move || {
            // 1. BrokenPipe progress writer still reads notifications through Done.
            let (mut conn, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(conn.try_clone().unwrap());
            let mut line = String::new();
            assert!(reader.read_line(&mut line).unwrap() > 0);
            let req: crate::api::Request = serde_json::from_str(&line).unwrap();
            let id = req.id.unwrap();
            for msg in ["stopping k3s", "removing cluster data"] {
                let note = crate::api::Notification {
                    jsonrpc: crate::api::VERSION.into(),
                    method: "k8s.progress".into(),
                    params: serde_json::json!({ "message": msg }),
                };
                let mut out = serde_json::to_vec(&note).unwrap();
                out.push(b'\n');
                conn.write_all(&out).unwrap();
            }
            let resp = crate::api::Response::ok(id, Value::Null);
            let mut out = serde_json::to_vec(&resp).unwrap();
            out.push(b'\n');
            conn.write_all(&out).unwrap();

            // 2. Undrained/blocking stderr writer (50 distinct progress messages > channel
            //    capacity 32) never stalls reading the terminal reset result.
            let (mut conn, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(conn.try_clone().unwrap());
            line.clear();
            assert!(reader.read_line(&mut line).unwrap() > 0);
            let req: crate::api::Request = serde_json::from_str(&line).unwrap();
            let id = req.id.unwrap();
            for i in 0..50 {
                let note = crate::api::Notification {
                    jsonrpc: crate::api::VERSION.into(),
                    method: "k8s.progress".into(),
                    params: serde_json::json!({ "message": format!("step {i}") }),
                };
                let mut out = serde_json::to_vec(&note).unwrap();
                out.push(b'\n');
                conn.write_all(&out).unwrap();
            }
            let resp = crate::api::Response::ok(id, Value::Null);
            let mut out = serde_json::to_vec(&resp).unwrap();
            out.push(b'\n');
            conn.write_all(&out).unwrap();
        });

        let attempts = Arc::new(Mutex::new(0usize));
        let mut client1 = Client::connect(&sock).unwrap();
        let mut printer1 = ProgressPrinter::spawn(BrokenPipeWriter(attempts.clone()));
        reset_with_client(&mut client1, false, &mut |msg| printer1.send(msg)).unwrap();
        printer1.finish(Duration::from_millis(250));
        assert!(*attempts.lock().unwrap() >= 1);

        let (unblock_tx, unblock_rx) = mpsc::channel::<()>();
        let mut client2 = Client::connect(&sock).unwrap();
        let mut printer2 = ProgressPrinter::spawn(BlockingWriter(unblock_rx));
        let started = Instant::now();
        reset_with_client(&mut client2, false, &mut |msg| printer2.send(msg)).unwrap();
        printer2.finish(Duration::from_millis(50));
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "undrained progress writer blocked reset completion: {:?}",
            started.elapsed()
        );
        drop(unblock_tx);
        srv.join().unwrap();
    }
}
