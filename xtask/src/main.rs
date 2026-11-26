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
            initramfs(&root)?;
            host(&root)?;
            require_guest_images(&root)?;
            run(Command::new(cargo()).args([
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
                "amd64",
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

fn require_guest_images(root: &Path) -> anyhow::Result<()> {
    require_kernel(root)?;
    for (file, task) in [("rootfs.erofs", "rootfs"), ("data.img", "disk")] {
        if !root.join("guest/out").join(file).is_file() {
            bail!("guest/out/{file} is missing; run `cargo xtask {task}` first");
        }
    }
    Ok(())
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
