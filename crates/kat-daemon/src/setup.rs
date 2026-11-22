//! `katamaran setup` and `katamaran uninstall`: the LaunchAgent that runs
//! the daemon, the Docker context and the `kat` symlink.
//!
//! A home other than `~/.katamaran` (`KATAMARAN_HOME`, used by development
//! copies and tests) always gets a suffix on everything it installs: the
//! label `dev.katamaran.daemon.<suffix>`, the context `katamaran-<suffix>`
//! and the link `kat-<suffix>`, so it never touches the real ones.
//!
//! launchctl is used in its modern forms only (see `man launchctl`):
//! `bootstrap gui/<uid> <plist>`, `bootout gui/<uid>/<label>`,
//! `kickstart gui/<uid>/<label>` and `print gui/<uid>/<label>` (exit status
//! only; its output is not an API).

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, bail};

use crate::client::Client;
use crate::config::Config;

pub const LABEL_PREFIX: &str = "dev.katamaran.daemon";

#[derive(clap::Args)]
pub struct SetupArgs {
    /// Also make the Docker context the default one (`docker context use`).
    #[arg(long)]
    pub default: bool,
    /// Suffix for the LaunchAgent label, Docker context and `kat` link.
    /// Defaults to $KATAMARAN_LABEL_SUFFIX, or one derived from
    /// $KATAMARAN_HOME when that is set.
    #[arg(long)]
    pub suffix: Option<String>,
}

#[derive(clap::Args)]
pub struct UninstallArgs {
    #[arg(long)]
    pub suffix: Option<String>,
}

/// The names one installation uses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    pub suffix: Option<String>,
}

impl Identity {
    pub fn resolve(explicit: Option<&str>) -> anyhow::Result<Identity> {
        let home = std::env::var_os("KATAMARAN_HOME").map(PathBuf::from);
        let default_home = std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".katamaran"));
        let env = std::env::var("KATAMARAN_LABEL_SUFFIX").ok();
        let suffix = match (explicit, env.as_deref().filter(|s| !s.is_empty())) {
            (Some(s), _) | (None, Some(s)) => Some(s.to_string()),
            (None, None) => match home {
                Some(h) if Some(&h) != default_home.as_ref() => Some(derive_suffix(&h)),
                _ => None,
            },
        };
        if let Some(s) = &suffix
            && (s.is_empty()
                || !s
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'))
        {
            bail!("suffix {s:?} may only contain letters, digits, - and _");
        }
        Ok(Identity { suffix })
    }

    pub fn label(&self) -> String {
        match &self.suffix {
            Some(s) => format!("{LABEL_PREFIX}.{s}"),
            None => LABEL_PREFIX.into(),
        }
    }

    pub fn context(&self) -> String {
        match &self.suffix {
            Some(s) => format!("katamaran-{s}"),
            None => "katamaran".into(),
        }
    }

    pub fn link_name(&self) -> String {
        match &self.suffix {
            Some(s) => format!("kat-{s}"),
            None => "kat".into(),
        }
    }
}

/// `~/.katamaran-app` → `app`; `/tmp/test-home-12` → `test-home-12`.
pub fn derive_suffix(home: &Path) -> String {
    let base = home
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let base = base.trim_start_matches('.');
    let base = base.strip_prefix("katamaran").unwrap_or(base);
    let s: String = base
        .trim_start_matches(['-', '_', '.'])
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    if s.is_empty() { "dev".into() } else { s }
}

fn user_home() -> anyhow::Result<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .context("HOME is not set")
}

pub fn plist_path(label: &str) -> anyhow::Result<PathBuf> {
    Ok(user_home()?
        .join("Library/LaunchAgents")
        .join(format!("{label}.plist")))
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// The LaunchAgent: `katamaran daemon --start`, restarted after a crash
/// (not after a clean exit, so `uninstall` and `daemon.shutdown` stick),
/// with time to stop the VM on SIGTERM.
pub fn plist(
    label: &str,
    exe: &Path,
    home: Option<&Path>,
    logs: &Path,
    run_at_load: bool,
) -> String {
    let esc = |p: &Path| xml_escape(&p.to_string_lossy());
    let env = match home {
        Some(h) => format!(
            "\t<key>EnvironmentVariables</key>\n\t<dict>\n\t\t<key>KATAMARAN_HOME</key>\n\t\t<string>{}</string>\n\t</dict>\n",
            esc(h)
        ),
        None => String::new(),
    };
    let log = esc(&logs.join("launchd.log"));
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>Label</key>
	<string>{label}</string>
	<key>ProgramArguments</key>
	<array>
		<string>{exe}</string>
		<string>daemon</string>
		<string>--start</string>
	</array>
{env}	<key>RunAtLoad</key>
	<{run_at_load}/>
	<key>KeepAlive</key>
	<dict>
		<key>Crashed</key>
		<true/>
	</dict>
	<key>ProcessType</key>
	<string>Interactive</string>
	<key>ExitTimeOut</key>
	<integer>60</integer>
	<key>StandardOutPath</key>
	<string>{log}</string>
	<key>StandardErrorPath</key>
	<string>{log}</string>
</dict>
</plist>
"#,
        label = xml_escape(label),
        exe = esc(exe),
    )
}

/// Point an installed LaunchAgent's RunAtLoad at `value`. launchd reads it
/// at the next login, which is exactly when it matters.
pub fn set_run_at_load(value: bool) -> anyhow::Result<()> {
    let id = Identity::resolve(None)?;
    let path = plist_path(&id.label())?;
    let Ok(text) = fs::read_to_string(&path) else {
        return Ok(());
    };
    let (from, to) = if value {
        (
            "<key>RunAtLoad</key>\n\t<false/>",
            "<key>RunAtLoad</key>\n\t<true/>",
        )
    } else {
        (
            "<key>RunAtLoad</key>\n\t<true/>",
            "<key>RunAtLoad</key>\n\t<false/>",
        )
    };
    if text.contains(from) {
        fs::write(&path, text.replace(from, to))
            .with_context(|| format!("write {}", path.display()))?;
    }
    Ok(())
}

fn uid() -> u32 {
    unsafe { libc::getuid() }
}

pub fn service_target(label: &str) -> String {
    format!("gui/{}/{label}", uid())
}

/// Whether launchd has the service loaded (`launchctl print` succeeds).
pub fn is_loaded(label: &str) -> bool {
    Command::new("launchctl")
        .args(["print", &service_target(label)])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

fn launchctl(args: &[&str]) -> anyhow::Result<()> {
    let out = Command::new("launchctl")
        .args(args)
        .output()
        .context("run launchctl")?;
    if !out.status.success() {
        bail!(
            "launchctl {} failed ({}): {}",
            args.join(" "),
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(())
}

/// Start the installed LaunchAgent now, if there is one. Returns whether it was.
pub fn kickstart_if_installed() -> anyhow::Result<bool> {
    let label = Identity::resolve(None)?.label();
    if !is_loaded(&label) {
        return Ok(false);
    }
    launchctl(&["kickstart", &service_target(&label)])?;
    Ok(true)
}

/// Refuse to install the real label from a development build, unless asked
/// to: a build under `target/` is a development copy.
fn guard_unsuffixed(id: &Identity, exe: &Path) -> anyhow::Result<()> {
    if id.suffix.is_some() {
        return Ok(());
    }
    let dev_build = exe.components().any(|c| c.as_os_str() == "target");
    if dev_build && std::env::var_os("KATAMARAN_ALLOW_DEV_INSTALL").is_none() {
        bail!(
            "{} is a development build; refusing to install the {LABEL_PREFIX} LaunchAgent. \
             Use --suffix NAME (or set KATAMARAN_HOME), or set KATAMARAN_ALLOW_DEV_INSTALL=1",
            exe.display()
        );
    }
    Ok(())
}

pub fn setup(args: SetupArgs) -> anyhow::Result<()> {
    let id = Identity::resolve(args.suffix.as_deref())?;
    let exe = std::env::current_exe()?.canonicalize()?;
    guard_unsuffixed(&id, &exe)?;
    let home = crate::state_dir()?;
    let logs = home.join("logs");
    fs::create_dir_all(&logs)?;
    let label = id.label();
    let cfg = Config::load(&home)?;

    // A daemon that is not launchd's must make way, or the agent's daemon
    // would find the home locked.
    let sock = crate::client::socket_path()?;
    if let Ok(mut c) = Client::connect(&sock)
        && let Ok(status) = c.status()
        && status.daemon.launchd_label.as_deref() != Some(label.as_str())
    {
        println!(
            "Handing the running daemon (pid {}) over to launchd",
            status.daemon.pid
        );
        let was_up = status.state.is_active();
        c.call(&crate::api::Call::Shutdown)?;
        wait_gone(&sock, Duration::from_secs(70))?;
        if was_up && !cfg.launch_at_login {
            println!("The VM was stopped; `katamaran start` brings it back.");
        }
    }

    let plist_file = plist_path(&label)?;
    fs::create_dir_all(plist_file.parent().unwrap())?;
    let home_env = std::env::var_os("KATAMARAN_HOME").map(|_| home.as_path());
    fs::write(
        &plist_file,
        plist(&label, &exe, home_env, &logs, cfg.launch_at_login),
    )
    .with_context(|| format!("write {}", plist_file.display()))?;
    if is_loaded(&label) {
        launchctl(&["bootout", &service_target(&label)])?;
    }
    launchctl(&[
        "bootstrap",
        &format!("gui/{}", uid()),
        &plist_file.to_string_lossy(),
    ])?;
    println!("LaunchAgent  {label} ({})", plist_file.display());

    match docker_context(&id.context(), &home.join("run/docker.sock"), args.default) {
        Ok(msg) => println!("Docker       {msg}"),
        Err(e) => println!("Docker       context not created: {e:#}"),
    }
    match link(&id.link_name(), &exe) {
        Ok(msg) => println!("CLI          {msg}"),
        Err(e) => println!("CLI          {e:#}"),
    }
    match crate::ssh::ensure_files() {
        Ok(f) => {
            println!(
                "SSH          `ssh <machine>@{}` after adding this line at the top of ~/.ssh/config:",
                f.host
            );
            println!("               {}", f.include_line());
        }
        Err(e) => println!("SSH          not set up: {e:#}"),
    }
    Ok(())
}

pub fn uninstall(args: UninstallArgs) -> anyhow::Result<()> {
    let id = Identity::resolve(args.suffix.as_deref())?;
    let label = id.label();
    let plist_file = plist_path(&label)?;
    let sock = crate::client::socket_path()?;
    if is_loaded(&label) {
        // launchd sends SIGTERM; the daemon stops the VM before it exits.
        launchctl(&["bootout", &service_target(&label)])?;
        wait_gone(&sock, Duration::from_secs(70))?;
        println!("Stopped and unloaded {label}");
    }
    // A daemon started by the CLI rather than launchd.
    if let Ok(mut c) = Client::connect(&sock) {
        c.call(&crate::api::Call::Shutdown)?;
        wait_gone(&sock, Duration::from_secs(70))?;
        println!("Stopped the daemon");
    }
    if plist_file.exists() {
        fs::remove_file(&plist_file)?;
        println!("Removed {}", plist_file.display());
    }
    match remove_docker_context(&id.context()) {
        Ok(true) => println!("Removed the Docker context {}", id.context()),
        Ok(false) => {}
        Err(e) => println!("Docker context {} not removed: {e:#}", id.context()),
    }
    if let Ok(home) = crate::state_dir() {
        crate::k8s::remove_kubeconfig(&home, &id.context());
    }
    if let Some(p) = unlink(&id.link_name())? {
        println!("Removed {}", p.display());
    }
    println!(
        "Your data stays in {}; delete that folder to remove it too.",
        crate::state_dir()?.display()
    );
    Ok(())
}

/// Wait until nothing answers on the daemon socket.
fn wait_gone(sock: &Path, timeout: Duration) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    while Client::connect(sock).is_ok() {
        if Instant::now() > deadline {
            bail!("the daemon did not exit within {}s", timeout.as_secs());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Ok(())
}

fn find_docker() -> PathBuf {
    let candidates = [
        Some(PathBuf::from("/opt/homebrew/bin/docker")),
        Some(PathBuf::from("/usr/local/bin/docker")),
        std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".nix-profile/bin/docker")),
        Some(PathBuf::from("/run/current-system/sw/bin/docker")),
        std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local/bin/docker")),
        Some(PathBuf::from(
            "/Applications/Docker.app/Contents/Resources/bin/docker",
        )),
    ];
    for path in candidates.into_iter().flatten() {
        if path.is_file() {
            return path;
        }
    }
    PathBuf::from("docker")
}

fn docker(args: &[&str]) -> anyhow::Result<std::process::Output> {
    match Command::new("docker")
        .args(args)
        .stdin(Stdio::null())
        .output()
    {
        Ok(out) => Ok(out),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Command::new(find_docker())
            .args(args)
            .stdin(Stdio::null())
            .output()
            .context("the docker CLI is not on PATH"),
        Err(e) => Err(e).context("the docker CLI is not on PATH"),
    }
}

/// Ensure the `katamaran` Docker context exists and is set as the default.
/// No-op for suffixed/isolated homes (`KATAMARAN_HOME` / `KATAMARAN_LABEL_SUFFIX`)
/// so development copies and tests never hijack the user's Docker context.
pub fn ensure_default_docker_context() -> anyhow::Result<()> {
    let id = Identity::resolve(None)?;
    if id.suffix.is_some() {
        return Ok(());
    }
    let home = crate::state_dir()?;
    docker_context(&id.context(), &home.join("run/docker.sock"), true)?;
    Ok(())
}

fn docker_ok(args: &[&str]) -> anyhow::Result<String> {
    let out = docker(args)?;
    if !out.status.success() {
        bail!(
            "docker {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn docker_context(name: &str, sock: &Path, make_default: bool) -> anyhow::Result<String> {
    let endpoint = format!("host=unix://{}", sock.display());
    let exists = docker(&["context", "inspect", name])?.status.success();
    let verb = if exists { "update" } else { "create" };
    docker_ok(&[
        "context",
        verb,
        name,
        "--description",
        "katamaran",
        "--docker",
        &endpoint,
    ])?;
    if make_default {
        docker_ok(&["context", "use", name])?;
        Ok(format!("context {name} (now the default)"))
    } else {
        Ok(format!(
            "context {name}; `docker context use {name}` makes it the default, or run setup with --default"
        ))
    }
}

fn remove_docker_context(name: &str) -> anyhow::Result<bool> {
    if !docker(&["context", "inspect", name])?.status.success() {
        return Ok(false);
    }
    if docker_ok(&["context", "show"])? == name {
        docker_ok(&["context", "use", "default"])?;
        println!("The default Docker context is `default` again");
    }
    docker_ok(&["context", "rm", name])?;
    Ok(true)
}

fn bin_dir() -> anyhow::Result<PathBuf> {
    Ok(user_home()?.join(".local/bin"))
}

/// Put `name` → `exe` in ~/.local/bin, replacing only an earlier link to a
/// katamaran binary.
fn link(name: &str, exe: &Path) -> anyhow::Result<String> {
    let dir = bin_dir()?;
    if !dir.is_dir() {
        return Ok(format!(
            "{} does not exist; to use `{name}`, run: ln -s {} /usr/local/bin/{name}",
            dir.display(),
            exe.display()
        ));
    }
    let path = dir.join(name);
    match fs::read_link(&path) {
        Ok(target) if is_katamaran(&target) => fs::remove_file(&path)?,
        Ok(target) => bail!(
            "{} already points to {}; leaving it alone",
            path.display(),
            target.display()
        ),
        Err(_) if path.exists() => bail!("{} exists; leaving it alone", path.display()),
        Err(_) => {}
    }
    std::os::unix::fs::symlink(exe, &path).with_context(|| format!("link {}", path.display()))?;
    Ok(format!("{} -> {}", path.display(), exe.display()))
}

fn unlink(name: &str) -> anyhow::Result<Option<PathBuf>> {
    let path = bin_dir()?.join(name);
    match fs::read_link(&path) {
        Ok(target) if is_katamaran(&target) => {
            fs::remove_file(&path)?;
            Ok(Some(path))
        }
        _ => Ok(None),
    }
}

fn is_katamaran(target: &Path) -> bool {
    target.file_name().is_some_and(|n| n == "katamaran")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_suffixes() {
        assert_eq!(derive_suffix(Path::new("/Users/me/.katamaran-app")), "app");
        assert_eq!(
            derive_suffix(Path::new("/x/target/test-home-42")),
            "test-home-42"
        );
        assert_eq!(derive_suffix(Path::new("/x/.katamaran")), "dev");
        assert_eq!(derive_suffix(Path::new("/x/My Home")), "my-home");
    }

    #[test]
    fn names_follow_the_suffix() {
        let real = Identity { suffix: None };
        assert_eq!(real.label(), "dev.katamaran.daemon");
        assert_eq!(real.context(), "katamaran");
        assert_eq!(real.link_name(), "kat");
        let dev = Identity {
            suffix: Some("app".into()),
        };
        assert_eq!(dev.label(), "dev.katamaran.daemon.app");
        assert_eq!(dev.context(), "katamaran-app");
        assert_eq!(dev.link_name(), "kat-app");
    }

    #[test]
    fn refuses_unsuffixed_dev_installs() {
        let real = Identity { suffix: None };
        let dev_exe = Path::new("/Users/me/src/katamaran/target/release/katamaran");
        if std::env::var_os("KATAMARAN_ALLOW_DEV_INSTALL").is_none() {
            assert!(guard_unsuffixed(&real, dev_exe).is_err());
        }
        assert!(
            guard_unsuffixed(
                &real,
                Path::new("/Applications/Katamaran.app/Contents/MacOS/katamaran")
            )
            .is_ok()
        );
        let dev = Identity {
            suffix: Some("x".into()),
        };
        assert!(guard_unsuffixed(&dev, dev_exe).is_ok());
    }

    #[test]
    fn generates_a_valid_plist() {
        let p = plist(
            "dev.katamaran.daemon.t",
            Path::new("/opt/k&k/katamaran"),
            Some(Path::new("/tmp/home")),
            Path::new("/tmp/home/logs"),
            true,
        );
        assert!(p.contains("<string>dev.katamaran.daemon.t</string>"));
        assert!(p.contains("<string>/opt/k&amp;k/katamaran</string>"));
        assert!(p.contains("<key>KATAMARAN_HOME</key>\n\t\t<string>/tmp/home</string>"));
        assert!(p.contains("<key>RunAtLoad</key>\n\t<true/>"));
        assert!(p.contains("<key>Crashed</key>\n\t\t<true/>"));
        assert!(p.contains("<string>/tmp/home/logs/launchd.log</string>"));
        // plutil is the reference parser for launchd plists.
        let dir = std::env::temp_dir().join(format!("kat-plist-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let f = dir.join("t.plist");
        fs::write(&f, &p).unwrap();
        let out = Command::new("plutil")
            .arg("-lint")
            .arg(&f)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stdout)
        );
        let json = Command::new("plutil")
            .args(["-convert", "json", "-o", "-"])
            .arg(&f)
            .output()
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&json.stdout).unwrap();
        assert_eq!(v["ProgramArguments"][1], "daemon");
        assert_eq!(v["KeepAlive"]["Crashed"], true);
        assert_eq!(v["ExitTimeOut"], 60);
        fs::remove_dir_all(&dir).unwrap();

        let no_home = plist("l", Path::new("/k"), None, Path::new("/l"), false);
        assert!(!no_home.contains("EnvironmentVariables"));
        assert!(no_home.contains("<key>RunAtLoad</key>\n\t<false/>"));
    }
}
