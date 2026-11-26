mod app;
mod cpio;
mod gvproxy;

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, bail};
use clap::{Parser, Subcommand};

const GUEST_TARGET: &str = "aarch64-unknown-linux-musl";

#[derive(Parser)]
#[command(name = "xtask", about = "katamaran build tasks")]
struct Cli {
    #[command(subcommand)]
    task: Task,
}

#[derive(Subcommand)]
enum Task {
    /// Build kat-init for the guest and pack guest/out/initramfs.cpio.
    Initramfs,
    /// Build the guest kernel (guest/kernel/build.sh) into guest/out/Image.
    Kernel,
    /// Build the system image (guest/rootfs/build.sh) into guest/out/rootfs.erofs.
    Rootfs,
    /// Build the data-disk template (guest/disk/build.sh) into guest/out/data.img.
    Disk,
    /// Build gvproxy (the userspace network stack) from its pinned release into target/release.
    Gvproxy,
    /// Build the host binaries and sign katamaran-vm with the hypervisor entitlement.
    Host,
    /// Initramfs + host binaries.
    Build,
    /// rustfmt our crates only; `cargo fmt --all` would also reformat vendor/libkrun.
    Fmt,
    /// Build, then run the tests that boot real VMs.
    TestVm,
    /// Build, then run the VM with Docker in the foreground until Ctrl-C.
    Run {
        /// Arguments passed through to `katamaran run`.
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Build target/Katamaran.app: the AppKit menu-bar app with every binary and the guest images, ad-hoc signed.
    App,
    /// Launch the built app headless against a test daemon and check it connects.
    AppSmoke,
    /// Build target/dist/Katamaran-<version>.dmg (ad-hoc signed app + Applications link).
    Dmg,
    /// Build, then boot a throwaway VM and print boot timings.
    Boot {
        /// Arguments passed through to `katamaran debug-boot`.
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
}

fn main() -> anyhow::Result<()> {
    let root = workspace_root();
    std::env::set_current_dir(&root)?;
    match Cli::parse().task {
        Task::Initramfs => initramfs(&root),
        Task::Kernel => run(Command::new("bash").arg("guest/kernel/build.sh")),
        Task::Rootfs => run(Command::new("bash").arg("guest/rootfs/build.sh")),
        Task::Disk => run(Command::new("bash").arg("guest/disk/build.sh")),
        Task::Run { args } => {
            initramfs(&root)?;
            host(&root)?;
            require_guest_images(&root)?;
            run(Command::new(root.join("target/release/katamaran"))
                .arg("run")
                .args(args))
        }
        Task::Gvproxy => gvproxy::build(&root),
        Task::App => {
            initramfs(&root)?;
            host(&root)?;
            require_guest_images(&root)?;
            app::build(&root).map(drop)
        }
        Task::AppSmoke => app::smoke(&root),
        Task::Dmg => {
            initramfs(&root)?;
            host(&root)?;
            require_guest_images(&root)?;
            app::dmg(&root).map(drop)
        }
        Task::Host => host(&root),
        Task::Fmt => run(Command::new(cargo()).args([
            "fmt",
            "-p",
            "kat-proto",
            "-p",
            "kat-vmm",
            "-p",
            "kat-init",
            "-p",
            "kat-daemon",
            "-p",
            "kat-nfs",
            "-p",
            "kat-term",
            "-p",
            "xtask",
        ])),
        Task::Build => {
            initramfs(&root)?;
            host(&root)
        }
        Task::TestVm => {
            require_tool("kubectl", "running `cargo xtask test-vm`")?;
            require_tool("docker", "running `cargo xtask test-vm`")?;
            // Fail before building anything if guest/out/Image predates the current kernel
            // patches (e.g. the quickack suite needs kat.quickack from patch 0012).
            require_current_kernel(&root)?;
            initramfs(&root)?;
            host(&root)?;
            require_guest_images(&root)?;
            run(Command::new(cargo())
                .env("KATAMARAN_GUEST_DIR", root.join("guest/out"))
                .args([
                    "test",
                    "--release",
                    "--package",
                    "kat-daemon",
                    "--test",
                    "vm_boot",
                    "--test",
                    "docker",
                    "--test",
                    "network",
                    "--test",
                    "autofwd",
                    "--test",
                    "quickack",
                    "--test",
                    "nfs",
                    "--test",
                    "fsnotify",
                    "--test",
                    "fscore",
                    "--test",
                    "fscache",
                    "--test",
                    "netperf",
                    "--test",
                    "daemon",
                    "--test",
                    "lifecycle",
                    "--test",
                    "machines",
                    "--test",
                    "vm_exec",
                    "--test",
                    "amd64",
                    "--test",
                    "k8s_config",
                    "--",
                    "--ignored",
                    "--test-threads",
                    "1",
                ]))
        }
        Task::Boot { args } => {
            initramfs(&root)?;
            host(&root)?;
            require_kernel(&root)?;
            run(Command::new(root.join("target/release/katamaran"))
                .arg("debug-boot")
                .args(args))
        }
    }
}

fn initramfs(root: &Path) -> anyhow::Result<()> {
    run(Command::new(cargo()).args([
        "build",
        "--package",
        "kat-init",
        "--profile",
        "init",
        "--target",
        GUEST_TARGET,
    ]))?;
    let init = root.join("target").join(GUEST_TARGET).join("init/kat-init");
    let init_bytes = std::fs::read(&init).with_context(|| format!("read {}", init.display()))?;

    let mut archive = cpio::Writer::new();
    for dir in ["dev", "proc", "sys", "run", "tmp", "etc", "mnt"] {
        archive.dir(dir, 0o755);
    }
    archive.char_device("dev/console", 0o600, 5, 1);
    archive.file("init", 0o755, &init_bytes);
    let out = root.join("guest/out/initramfs.cpio");
    std::fs::create_dir_all(out.parent().unwrap())?;
    std::fs::write(&out, archive.finish())?;
    println!(
        "wrote {} ({} KiB, kat-init {} KiB)",
        out.display(),
        std::fs::metadata(&out)?.len() / 1024,
        init_bytes.len() / 1024
    );
    Ok(())
}

fn require_kernel(root: &Path) -> anyhow::Result<()> {
    if !root.join("guest/out/Image").is_file() {
        bail!("guest/out/Image is missing; run `cargo xtask kernel` first");
    }
    Ok(())
}

/// Fails unless `guest/out/Image.stamp` (written by `guest/kernel/build.sh` next to the Image)
/// matches both the current kernel inputs (`input=<sha256>`) and the installed `guest/out/Image`
/// bytes (`image=<sha256>`).
fn require_current_kernel(root: &Path) -> anyhow::Result<()> {
    require_kernel(root)?;
    let out = Command::new("bash")
        .arg(root.join("guest/kernel/build.sh"))
        .arg("stamp")
        .output()
        .context("run guest/kernel/build.sh stamp")?;
    if !out.status.success() {
        bail!(
            "guest/kernel/build.sh stamp failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    let want_input = String::from_utf8_lossy(&out.stdout);
    verify_kernel_artifacts(
        &root.join("guest/out/Image"),
        &root.join("guest/out/Image.stamp"),
        &want_input,
    )
}

fn verify_kernel_artifacts(
    image_path: &Path,
    stamp_path: &Path,
    want_input: &str,
) -> anyhow::Result<()> {
    let image_sha = sha256_file(image_path)?;
    let have = std::fs::read_to_string(stamp_path).ok();
    check_kernel_stamp(have.as_deref(), want_input, &image_sha)
}

fn is_sha256_hex(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

fn check_kernel_stamp(
    have: Option<&str>,
    want_input: &str,
    actual_image_sha: &str,
) -> anyhow::Result<()> {
    let want_input = want_input.trim();
    if !is_sha256_hex(want_input) {
        bail!("guest/kernel/build.sh stamp printed an invalid digest: {want_input:?}");
    }
    let actual_image_sha = actual_image_sha.trim();
    if !is_sha256_hex(actual_image_sha) {
        bail!("computed invalid SHA-256 digest for guest/out/Image: {actual_image_sha:?}");
    }
    let Some(have) = have else {
        bail!(
            "guest/out/Image has no build stamp (built before guest/out/Image.stamp existed, or by hand); run `cargo xtask kernel` first"
        );
    };
    let mut recorded_input: Option<&str> = None;
    let mut recorded_image: Option<&str> = None;
    for raw in have.lines() {
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        let Some((key, val)) = line.split_once('=') else {
            bail!(
                "guest/out/Image.stamp uses an older or invalid format; run `cargo xtask kernel` first"
            );
        };
        let val = val.trim();
        if !is_sha256_hex(val) {
            bail!(
                "guest/out/Image.stamp has an invalid {key} digest; run `cargo xtask kernel` first"
            );
        }
        match key {
            "input" if recorded_input.is_none() => recorded_input = Some(val),
            "image" if recorded_image.is_none() => recorded_image = Some(val),
            _ => bail!(
                "guest/out/Image.stamp uses an older or invalid format; run `cargo xtask kernel` first"
            ),
        }
    }
    let (Some(recorded_input), Some(recorded_image)) = (recorded_input, recorded_image) else {
        bail!(
            "guest/out/Image.stamp uses an older or incomplete format (missing input or image digest); run `cargo xtask kernel` first"
        );
    };
    if recorded_input != want_input {
        bail!(
            "guest/out/Image was built from older kernel patches or config; run `cargo xtask kernel` first"
        );
    }
    if recorded_image != actual_image_sha {
        bail!(
            "guest/out/Image does not match guest/out/Image.stamp (Image was replaced without rebuilding); run `cargo xtask kernel` first"
        );
    }
    Ok(())
}

/// FIPS 180-4 SHA-256 of a file, returned as 64 lowercase hex characters.
fn sha256_file(path: &Path) -> anyhow::Result<String> {
    use std::io::Read;

    let mut file = std::fs::File::open(path)
        .with_context(|| format!("open {} for hashing", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = file
            .read(&mut buf)
            .with_context(|| format!("read {} for hashing", path.display()))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finish_hex())
}

struct Sha256 {
    state: [u32; 8],
    buf: [u8; 64],
    buf_len: usize,
    total_len: u64,
}

impl Sha256 {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];

    fn new() -> Self {
        Self {
            state: [
                0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
                0x5be0cd19,
            ],
            buf: [0; 64],
            buf_len: 0,
            total_len: 0,
        }
    }

    fn compress(&mut self, block: &[u8; 64]) {
        let mut w = [0u32; 64];
        for (dst, chunk) in w.iter_mut().zip(block.as_chunks::<4>().0) {
            *dst = u32::from_be_bytes(*chunk);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = self.state;
        for (&k, &wi) in Self::K.iter().zip(&w) {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let t1 = h
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(k)
                .wrapping_add(wi);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(maj);
            h = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }
        for (s, v) in self.state.iter_mut().zip([a, b, c, d, e, f, g, h]) {
            *s = s.wrapping_add(v);
        }
    }

    fn update(&mut self, mut data: &[u8]) {
        self.total_len = self.total_len.wrapping_add(data.len() as u64);
        if self.buf_len > 0 {
            let take = data.len().min(64 - self.buf_len);
            self.buf[self.buf_len..self.buf_len + take].copy_from_slice(&data[..take]);
            self.buf_len += take;
            data = &data[take..];
            if self.buf_len == 64 {
                let block = self.buf;
                self.compress(&block);
                self.buf_len = 0;
            }
        }
        let (blocks, rem) = data.as_chunks::<64>();
        for block in blocks {
            self.compress(block);
        }
        if !rem.is_empty() {
            self.buf[..rem.len()].copy_from_slice(rem);
            self.buf_len = rem.len();
        }
    }

    fn finish_hex(mut self) -> String {
        let bit_len = self.total_len.wrapping_mul(8);
        self.buf[self.buf_len] = 0x80;
        self.buf_len += 1;
        if self.buf_len > 56 {
            self.buf[self.buf_len..64].fill(0);
            let block = self.buf;
            self.compress(&block);
            self.buf_len = 0;
        }
        self.buf[self.buf_len..56].fill(0);
        self.buf[56..64].copy_from_slice(&bit_len.to_be_bytes());
        let block = self.buf;
        self.compress(&block);

        let mut out = String::with_capacity(64);
        for word in self.state {
            use std::fmt::Write;
            let _ = write!(&mut out, "{word:08x}");
        }
        out
    }
}

fn require_guest_images(root: &Path) -> anyhow::Result<()> {
    require_kernel(root)?;
    for (file, task) in [("rootfs.erofs", "rootfs"), ("data.img", "disk")] {
        if !root.join("guest/out").join(file).is_file() {
            bail!("guest/out/{file} is missing; run `cargo xtask {task}` first");
        }
    }
    Ok(())
}

fn require_tool(bin: &str, why: &str) -> anyhow::Result<()> {
    let path = std::env::var_os("PATH").unwrap_or_default();
    require_tool_in(bin, why, &path)
}

fn require_tool_in(bin: &str, why: &str, path: &std::ffi::OsStr) -> anyhow::Result<()> {
    let found = std::env::split_paths(path).any(|dir| is_executable_file(&dir.join(bin)));
    if !found {
        bail!("no {bin} executable by the current user on PATH; install {bin} before {why}");
    }
    Ok(())
}

/// A regular file the current effective user may execute (`faccessat(X_OK, AT_EACCESS)`),
/// not merely one with some execute bit set.
fn is_executable_file(path: &Path) -> bool {
    use std::os::unix::ffi::OsStrExt;

    if !std::fs::metadata(path).is_ok_and(|m| m.is_file()) {
        return false;
    }
    let Ok(c_path) = std::ffi::CString::new(path.as_os_str().as_bytes()) else {
        return false;
    };
    // SAFETY: c_path is a valid NUL-terminated string that outlives the call.
    unsafe {
        libc::faccessat(
            libc::AT_FDCWD,
            c_path.as_ptr(),
            libc::X_OK,
            libc::AT_EACCESS,
        ) == 0
    }
}

fn host(root: &Path) -> anyhow::Result<()> {
    gvproxy::build(root)?;
    run(Command::new(cargo()).args([
        "build",
        "--release",
        "--package",
        "kat-vmm",
        "--package",
        "kat-daemon",
    ]))?;
    let vm = root.join("target/release/katamaran-vm");
    run(Command::new("codesign")
        .args(["--force", "--sign", "-", "--entitlements"])
        .arg(root.join("crates/kat-vmm/katamaran-vm.entitlements"))
        .arg(&vm))
}

fn run(cmd: &mut Command) -> anyhow::Result<()> {
    let status = cmd.status().with_context(|| format!("run {cmd:?}"))?;
    if !status.success() {
        bail!("{cmd:?} failed with {status}");
    }
    Ok(())
}

fn cargo() -> String {
    std::env::var("CARGO").unwrap_or_else(|_| "cargo".into())
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::{
        Sha256, check_kernel_stamp, require_tool_in, sha256_file, verify_kernel_artifacts,
    };
    use std::ffi::OsStr;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn require_tool_reports_missing_binary() {
        let err = require_tool_in(
            "katamaran-nonexistent-tool-xyz",
            "running `cargo xtask test-vm`",
            OsStr::new("/usr/bin:/bin"),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("katamaran-nonexistent-tool-xyz"), "{msg}");
        assert!(msg.contains("PATH"), "{msg}");
    }

    #[test]
    fn require_tool_finds_existing_executable() {
        require_tool_in("sh", "testing", OsStr::new("/bin:/usr/bin")).unwrap();
    }

    #[test]
    fn require_tool_rejects_file_the_user_cannot_execute() {
        let dir = std::env::temp_dir().join(format!("xtask-tool-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let tool = dir.join("kat-fake-tool");
        std::fs::write(&tool, "#!/bin/sh\n").unwrap();
        std::fs::set_permissions(&tool, std::fs::Permissions::from_mode(0o644)).unwrap();
        let denied = require_tool_in("kat-fake-tool", "testing", dir.as_os_str());
        std::fs::set_permissions(&tool, std::fs::Permissions::from_mode(0o755)).unwrap();
        let allowed = require_tool_in("kat-fake-tool", "testing", dir.as_os_str());
        let _ = std::fs::remove_dir_all(&dir);
        // Root on Linux (CAP_DAC_OVERRIDE) can bypass non-executable modes
        // depending on kernel/filesystem checks, so only assert denial as non-root.
        if unsafe { libc::geteuid() } != 0 {
            assert!(denied.is_err());
        }
        allowed.unwrap();
    }

    #[test]
    fn sha256_matches_nist_vectors() {
        let mut h = Sha256::new();
        assert_eq!(
            h.finish_hex(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        h = Sha256::new();
        h.update(b"abc");
        assert_eq!(
            h.finish_hex(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn kernel_stamp_must_match_input_and_image() {
        let input = "a".repeat(64);
        let other_input = "b".repeat(64);
        let image = "c".repeat(64);
        let other_image = "d".repeat(64);
        let valid = format!("input={input}\nimage={image}\n");

        check_kernel_stamp(Some(&valid), &format!("{input}\n"), &image).unwrap();

        // Legacy single-line stamp format is rejected.
        let legacy = check_kernel_stamp(Some(&format!("{input}\n")), &input, &image).unwrap_err();
        assert!(
            legacy.to_string().contains("cargo xtask kernel"),
            "{legacy}"
        );

        // Mismatched input digest is rejected.
        let stale_input = check_kernel_stamp(Some(&valid), &other_input, &image).unwrap_err();
        assert!(
            stale_input.to_string().contains("cargo xtask kernel"),
            "{stale_input}"
        );

        // Mismatched image digest is rejected.
        let stale_image = check_kernel_stamp(Some(&valid), &input, &other_image).unwrap_err();
        assert!(
            stale_image.to_string().contains("cargo xtask kernel"),
            "{stale_image}"
        );

        // Missing stamp is rejected.
        let missing = check_kernel_stamp(None, &input, &image).unwrap_err();
        assert!(
            missing.to_string().contains("cargo xtask kernel"),
            "{missing}"
        );

        // Invalid expected stamp is rejected.
        assert!(check_kernel_stamp(Some(&valid), "", &image).is_err());
    }

    #[test]
    fn kernel_stamp_rejects_replaced_image_with_unchanged_stamp() {
        let dir = std::env::temp_dir().join(format!("xtask-kstamp-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let image = dir.join("Image");
        let stamp = dir.join("Image.stamp");
        let input = "0123456789abcdef".repeat(4);

        std::fs::write(&image, b"original kernel image bytes").unwrap();
        let orig_sha = sha256_file(&image).unwrap();
        std::fs::write(&stamp, format!("input={input}\nimage={orig_sha}\n")).unwrap();
        verify_kernel_artifacts(&image, &stamp, &input).unwrap();

        // Replace Image without updating Image.stamp.
        std::fs::write(&image, b"replaced release kernel bytes").unwrap();
        let err = verify_kernel_artifacts(&image, &stamp, &input).unwrap_err();
        let _ = std::fs::remove_dir_all(&dir);
        assert!(
            err.to_string().contains("cargo xtask kernel"),
            "unexpected error: {err}"
        );
    }
}
