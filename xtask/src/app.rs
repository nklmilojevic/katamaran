//! `cargo xtask app`: build Katamaran.app (the AppKit menu-bar app plus the
//! CLI/daemon, the VM process, gvproxy and the guest images), ad-hoc signed.
//! `cargo xtask app-smoke`: launch the built app headless against a daemon
//! in a throwaway home and check that it connects.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, bail};

pub const BUNDLE: &str = "target/Katamaran.app";
/// The app's deployment target, as in app/Package.swift.
const MIN_MACOS: &str = "15.0";
const BUNDLE_ID: &str = "dev.katamaran.app";
const APP_EXE: &str = "KatamaranApp";

/// The terminal engine the app links (`app/Package.swift` looks for it here).
pub fn build_term(root: &Path) -> anyhow::Result<()> {
    crate::run(
        Command::new(crate::cargo())
            .args([
                "build",
                "--release",
                "--package",
                "kat-term",
                "--target",
                "aarch64-apple-darwin",
            ])
            .current_dir(root),
    )
}

pub fn build(root: &Path) -> anyhow::Result<PathBuf> {
    build_at(root, &root.join(BUNDLE))
}

/// Build the bundle at `bundle` (`cargo xtask dmg` uses target/dist, so a
/// running target/Katamaran.app is left alone).
pub fn build_at(root: &Path, bundle: &Path) -> anyhow::Result<PathBuf> {
    let package = root.join("app");
    build_term(root)?;
    crate::run(
        Command::new("swift")
            .args(["build", "-c", "release", "--package-path"])
            .arg(&package),
    )?;
    let bin_dir = String::from_utf8(
        Command::new("swift")
            .args([
                "build",
                "-c",
                "release",
                "--show-bin-path",
                "--package-path",
            ])
            .arg(&package)
            .output()?
            .stdout,
    )?;
    let app_bin = PathBuf::from(bin_dir.trim()).join(APP_EXE);

    let bundle = bundle.to_path_buf();
    if bundle.exists() {
        fs::remove_dir_all(&bundle)?;
    }
    let macos = bundle.join("Contents/MacOS");
    let frameworks = bundle.join("Contents/Frameworks");
    let guest = bundle.join("Contents/Resources/guest");
    fs::create_dir_all(&macos)?;
    fs::create_dir_all(&frameworks)?;
    fs::create_dir_all(&guest)?;
    let release = root.join("target/release");
    copy(&app_bin, &macos.join(APP_EXE))?;
    stamp_sdk(&macos.join(APP_EXE))?;
    for b in ["katamaran", "katamaran-vm", "gvproxy", "libgvproxy.dylib"] {
        copy(&release.join(b), &macos.join(b))?;
    }
    let sparkle_src = find_sparkle_framework(&package)?;
    let sparkle_dst = frameworks.join("Sparkle.framework");
    crate::run(Command::new("ditto").arg(&sparkle_src).arg(&sparkle_dst))?;
    let iconset = root.join("target/icon/Katamaran.iconset");
    if iconset.exists() {
        fs::remove_dir_all(&iconset)?;
    }
    crate::run(
        Command::new("swift")
            .arg(package.join("Icon/make-icon.swift"))
            .arg(&iconset),
    )?;
    crate::run(
        Command::new("iconutil")
            .args(["-c", "icns", "-o"])
            .arg(bundle.join("Contents/Resources/Katamaran.icns"))
            .arg(&iconset),
    )?;
    let out = root.join("guest/out");
    for f in ["Image", "initramfs.cpio", "rootfs.erofs"] {
        copy(&out.join(f), &guest.join(f))?;
    }
    // codesign hashes every byte of a bundled file, holes too: 64 GiB of
    // zeros for the data-disk template. Ship its few MiB of data instead.
    crate::run(
        Command::new(release.join("katamaran"))
            .arg("pack-image")
            .arg(out.join("data.img"))
            .arg(guest.join("data.img.sparse")),
    )?;
    let ver = version(root)?;
    let build_number = non_empty_env("KATAMARAN_BUILD_NUMBER").unwrap_or_else(|| ver.clone());
    let sparkle = match (
        non_empty_env("KATAMARAN_FEED_URL"),
        non_empty_env("KATAMARAN_SPARKLE_PUBLIC_KEY"),
    ) {
        (Some(feed), Some(key)) => Some((feed, key)),
        _ => None,
    };
    fs::write(
        bundle.join("Contents/Info.plist"),
        info_plist_full(
            &ver,
            &build_number,
            sparkle.as_ref().map(|(f, k)| (f.as_str(), k.as_str())),
        ),
    )?;

    // Inside out: Sparkle framework components first (preserving Downloader.xpc's
    // own entitlements), then nested binaries, katamaran-vm with its entitlement,
    // then the main executable and bundle (without --deep).
    sign_sparkle(&sparkle_dst)?;
    for b in ["katamaran", "gvproxy", "libgvproxy.dylib", APP_EXE] {
        sign(&macos.join(b), None, false)?;
    }
    sign(
        &macos.join("katamaran-vm"),
        Some(&root.join("crates/kat-vmm/katamaran-vm.entitlements")),
        false,
    )?;
    sign(&bundle, None, false)?;
    crate::run(
        Command::new("codesign")
            .args(["--verify", "--deep", "--strict"])
            .arg(&bundle),
    )?;
    println!("built {}", bundle.display());
    Ok(bundle)
}

/// SwiftPM links with an explicit `-target macos15.0` and no SDKROOT, so
/// clang records the deployment target as the SDK version too. AppKit then
/// treats the app as built against macOS 15 and draws the old design (a
/// legacy vibrant sidebar with a black divider). Record the real SDK.
fn stamp_sdk(bin: &Path) -> anyhow::Result<()> {
    let sdk = Command::new("xcrun")
        .args(["--sdk", "macosx", "--show-sdk-version"])
        .output()?;
    anyhow::ensure!(sdk.status.success(), "xcrun --show-sdk-version failed");
    let sdk = String::from_utf8(sdk.stdout)?.trim().to_string();
    crate::run(
        Command::new("xcrun")
            .args([
                "vtool",
                "-set-build-version",
                "macos",
                MIN_MACOS,
                &sdk,
                "-replace",
                "-output",
            ])
            .arg(bin)
            .arg(bin),
    )
}

fn copy(from: &Path, to: &Path) -> anyhow::Result<()> {
    fs::copy(from, to).with_context(|| format!("copy {} to {}", from.display(), to.display()))?;
    Ok(())
}

fn non_empty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn find_sparkle_framework(package: &Path) -> anyhow::Result<PathBuf> {
    let out = Command::new("find")
        .arg(package.join(".build/artifacts"))
        .args([
            "-path",
            "*Sparkle.xcframework/macos-*/Sparkle.framework",
            "-maxdepth",
            "6",
            "-type",
            "d",
        ])
        .output()
        .context("locate Sparkle.framework under app/.build/artifacts")?;
    let found = String::from_utf8(out.stdout)?
        .lines()
        .next()
        .map(PathBuf::from)
        .filter(|p| p.is_dir())
        .context("Sparkle.framework not found under app/.build/artifacts")?;
    Ok(found)
}

/// Signing identity from `KATAMARAN_SIGN_IDENTITY` when set and non-empty
/// (and not `-`); otherwise local ad-hoc signing.
fn sign_identity() -> Option<String> {
    non_empty_env("KATAMARAN_SIGN_IDENTITY").filter(|s| s != "-")
}

fn sign(
    path: &Path,
    entitlements: Option<&Path>,
    preserve_entitlements: bool,
) -> anyhow::Result<()> {
    let mut cmd = Command::new("codesign");
    match sign_identity() {
        Some(identity) => {
            cmd.args(["--force", "--options", "runtime", "--timestamp", "--sign"])
                .arg(&identity);
        }
        None => {
            cmd.args(["--force", "--sign", "-", "--timestamp=none"]);
        }
    }
    if preserve_entitlements {
        cmd.arg("--preserve-metadata=entitlements");
    }
    if let Some(e) = entitlements {
        cmd.arg("--entitlements").arg(e);
    }
    crate::run(cmd.arg(path))
}

fn sign_sparkle(framework: &Path) -> anyhow::Result<()> {
    let ver = framework.join("Versions/B");
    sign(&ver.join("XPCServices/Installer.xpc"), None, false)?;
    sign(&ver.join("XPCServices/Downloader.xpc"), None, true)?;
    sign(&ver.join("Autoupdate"), None, false)?;
    sign(&ver.join("Updater.app"), None, false)?;
    sign(framework, None, false)
}

fn sign_dmg(path: &Path) -> anyhow::Result<()> {
    let Some(identity) = sign_identity() else {
        return Ok(());
    };
    crate::run(
        Command::new("codesign")
            .args(["--force", "--timestamp", "--sign"])
            .arg(&identity)
            .arg(path),
    )?;
    crate::run(
        Command::new("codesign")
            .args(["--verify", "--strict"])
            .arg(path),
    )
}

fn version(root: &Path) -> anyhow::Result<String> {
    let toml = fs::read_to_string(root.join("Cargo.toml"))?;
    toml.lines()
        .find_map(|l| l.strip_prefix("version = \""))
        .and_then(|v| v.strip_suffix('"'))
        .map(str::to_string)
        .context("no workspace version in Cargo.toml")
}

#[cfg(test)]
pub fn info_plist(version: &str) -> String {
    info_plist_full(version, version, None)
}

pub fn info_plist_full(
    short_version: &str,
    build_version: &str,
    sparkle: Option<(&str, &str)>,
) -> String {
    let sparkle_entries = match sparkle {
        Some((feed_url, public_key)) => format!(
            "\t<key>SUFeedURL</key>\n\t<string>{feed_url}</string>\n\t<key>SUPublicEDKey</key>\n\t<string>{public_key}</string>\n"
        ),
        None => String::new(),
    };
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>CFBundleIdentifier</key>
	<string>{BUNDLE_ID}</string>
	<key>CFBundleName</key>
	<string>Katamaran</string>
	<key>CFBundleDisplayName</key>
	<string>Katamaran</string>
	<key>CFBundleIconFile</key>
	<string>Katamaran</string>
	<key>CFBundleExecutable</key>
	<string>{APP_EXE}</string>
	<key>CFBundlePackageType</key>
	<string>APPL</string>
	<key>NSHumanReadableCopyright</key>
	<string>Copyright © 2026 Nikola Milojević. AGPL-3.0-or-later.</string>
	<key>CFBundleShortVersionString</key>
	<string>{short_version}</string>
	<key>CFBundleVersion</key>
	<string>{build_version}</string>
	<key>LSMinimumSystemVersion</key>
	<string>{MIN_MACOS}</string>
	<key>LSUIElement</key>
	<true/>
	<key>NSHighResolutionCapable</key>
	<true/>
{sparkle_entries}</dict>
</plist>
"#
    )
}

/// Kills the test daemon even when the check fails.
struct DaemonGuard {
    child: std::process::Child,
    home: PathBuf,
}

impl Drop for DaemonGuard {
    fn drop(&mut self) {
        unsafe { libc_kill(self.child.id() as i32, 15) };
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if let Ok(Some(_)) = self.child.try_wait() {
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = fs::remove_dir_all(&self.home);
    }
}

unsafe extern "C" {
    #[link_name = "kill"]
    fn libc_kill(pid: i32, sig: i32) -> i32;
}

/// Start a daemon (no VM) in a throwaway home, run the app with
/// `--smoke-test` against it, and require it to report the connection.
pub fn smoke(root: &Path) -> anyhow::Result<()> {
    let bundle = root.join(BUNDLE);
    let macos = bundle.join("Contents/MacOS");
    if !macos.join(APP_EXE).is_file() {
        bail!("{} is missing; run `cargo xtask app`", bundle.display());
    }
    let home = root.join(format!("target/app-smoke-{}", std::process::id()));
    fs::create_dir_all(&home)?;
    let guard = DaemonGuard {
        child: Command::new(macos.join("katamaran"))
            .args(["daemon", "--no-log-file"])
            .env("KATAMARAN_HOME", &home)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
        home: home.clone(),
    };
    let sock = home.join("run/daemon.sock");
    let deadline = Instant::now() + Duration::from_secs(10);
    while !sock.exists() {
        if Instant::now() > deadline {
            bail!("the test daemon did not create {}", sock.display());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let t = Instant::now();
    let out = Command::new(macos.join(APP_EXE))
        .arg("--smoke-test")
        .env("KATAMARAN_HOME", &home)
        .output()?;
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    drop(guard);
    let want = "katamaran-app: connected to daemon";
    if !out.status.success() || !stdout.contains(want) {
        bail!(
            "app smoke test failed ({}):\nstdout:\n{stdout}\nstderr:\n{stderr}",
            out.status
        );
    }
    println!(
        "app smoke test passed in {:.1} s: {}",
        t.elapsed().as_secs_f64(),
        stdout
            .lines()
            .find(|l| l.contains(want))
            .unwrap_or_default()
    );
    Ok(())
}

/// `cargo xtask dmg`: target/dist/Katamaran-<version>.dmg with the app and an
/// Applications link, built from a fresh bundle in target/dist.
pub fn dmg(root: &Path) -> anyhow::Result<PathBuf> {
    let dist = root.join("target/dist");
    let stage = dist.join("stage");
    if stage.exists() {
        fs::remove_dir_all(&stage)?;
    }
    fs::create_dir_all(&stage)?;
    let bundle = build_at(root, &stage.join("Katamaran.app"))?;
    std::os::unix::fs::symlink("/Applications", stage.join("Applications"))?;
    let out = dist.join(format!("Katamaran-{}.dmg", version(root)?));
    if out.exists() {
        fs::remove_file(&out)?;
    }
    crate::run(
        Command::new("hdiutil")
            .args([
                "create",
                "-volname",
                "Katamaran",
                "-fs",
                "APFS",
                "-format",
                "ULMO",
                "-srcfolder",
            ])
            .arg(&stage)
            .arg(&out),
    )?;
    sign_dmg(&out)?;
    crate::run(Command::new("hdiutil").args(["verify"]).arg(&out))?;
    println!("built {} from {}", out.display(), bundle.display());
    Ok(out)
}

#[cfg(test)]
mod tests {
    #[test]
    fn info_plist_is_valid() {
        let dir = std::env::temp_dir().join(format!("xtask-plist-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let f = dir.join("Info.plist");
        std::fs::write(&f, super::info_plist("1.2.3")).unwrap();
        let out = std::process::Command::new("plutil")
            .arg("-lint")
            .arg(&f)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stdout)
        );
        let text = std::fs::read_to_string(&f).unwrap();
        assert!(text.contains("<key>LSUIElement</key>\n\t<true/>"));
        assert!(text.contains("<string>1.2.3</string>"));
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
