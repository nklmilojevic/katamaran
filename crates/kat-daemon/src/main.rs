mod amd64;
mod api;
mod autofwd;
mod boot;
mod cli;
mod client;
mod config;
mod daemon;
mod fsevents;
mod fswatch;
mod k8s;
mod k8s_cli;
mod lifecycle;
mod machine_cli;
mod machines;
mod net;
mod nfs;
mod power;
mod run;
mod setup;
mod skill;
mod sparse;
mod ssh;
mod state;
mod vm;
mod vm_cli;

use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "katamaran",
    about = "Docker in a lightweight Linux VM",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start the VM (and the daemon if needed); wait until Docker is ready.
    Start(cli::StartArgs),
    /// Stop the VM; the daemon keeps running.
    Stop,
    /// Stop, then start the VM, applying changed settings.
    Restart(cli::StartArgs),
    /// Show the VM's state, resources, Docker, files and ports.
    Status(cli::StatusArgs),
    /// Print the daemon's or the guest's log.
    Logs(cli::LogsArgs),
    /// Read or change settings (applied at the next start).
    #[command(subcommand)]
    Config(cli::ConfigCmd),
    /// Install the LaunchAgent, the Docker context and the `kat` link.
    Setup(setup::SetupArgs),
    /// Manage the coding-agent skill (`show`, `setup`, `status`, `remove`).
    #[command(subcommand)]
    Skill(skill::SkillCmd),
    /// Ensure the `katamaran` Docker context exists and is the default.
    #[command(hide = true)]
    UseContext,
    /// Remove what `setup` installed (your data stays).
    Uninstall(setup::UninstallArgs),
    /// Create, list, start, stop and delete Linux machines.
    #[command(subcommand)]
    Machine(machine_cli::MachineCmd),
    /// Manage the optional single-node Kubernetes (k3s) cluster.
    #[command(subcommand, visible_alias = "kubernetes")]
    K8s(k8s_cli::K8sCmd),
    /// Open a root shell or run commands inside the Linux VM itself.
    #[command(subcommand)]
    Vm(vm_cli::VmCmd),
    /// Internal ordered blocking-output relay helper for `kat vm shell`/`exec`.
    #[command(hide = true)]
    VmIoRelay,
    /// Open a shell (or run a command) in a Linux machine.
    Shell(machine_cli::ShellArgs),
    /// SSH into a Linux machine through the katamaran SSH config.
    Ssh(machine_cli::SshArgs),
    /// The SSH server for `ssh <machine>@katamaran` (the ProxyCommand).
    #[command(hide = true)]
    SshProxy,
    /// Print the CLI's and the daemon's versions.
    Version,
    /// Run the background daemon that owns the VM (launchd runs this).
    Daemon(daemon::DaemonArgs),
    /// Run the VM in the foreground with Docker, until Ctrl-C.
    Run(run::RunArgs),
    /// Pack a sparse disk image for the app bundle.
    #[command(hide = true)]
    PackImage(sparse::PackArgs),
    /// Boot a throwaway VM, report per-stage boot timings and shut it down.
    DebugBoot(boot::BootArgs),
}

fn main() -> anyhow::Result<()> {
    // Fast-path the internal ordered I/O relay helper before clap parsing so
    // it adds minimal startup overhead and never touches stdout/stderr (0/1/2).
    if std::env::args_os().nth(1).as_deref() == Some(std::ffi::OsStr::new("vm-io-relay")) {
        vm_cli::run_io_relay_helper();
    }
    match Cli::parse().command {
        Command::Start(args) => cli::start(args),
        Command::Stop => cli::stop(),
        Command::Restart(args) => cli::restart(args),
        Command::Status(args) => cli::status(args),
        Command::Logs(args) => cli::logs(args),
        Command::Config(cmd) => cli::config(cmd),
        Command::Setup(args) => setup::setup(args),
        Command::Skill(cmd) => skill::run(cmd),
        Command::UseContext => setup::ensure_default_docker_context(),
        Command::Uninstall(args) => setup::uninstall(args),
        Command::Machine(cmd) => machine_cli::machine(cmd),
        Command::K8s(cmd) => k8s_cli::run(cmd),
        Command::Vm(cmd) => vm_cli::run(cmd),
        Command::VmIoRelay => vm_cli::run_io_relay_helper(),
        Command::Shell(args) => machine_cli::shell(args),
        Command::Ssh(args) => machine_cli::ssh(args),
        Command::SshProxy => ssh::proxy(),
        Command::Version => cli::version(),
        Command::Daemon(args) => daemon::daemon(args),
        Command::Run(args) => run::run(args),
        Command::PackImage(args) => sparse::pack_cmd(args),
        Command::DebugBoot(args) => boot::debug_boot(args),
    }
}

/// The root of all host-side state: `$KATAMARAN_HOME`, or `~/.katamaran`.
pub fn state_dir() -> anyhow::Result<PathBuf> {
    if let Some(dir) = std::env::var_os("KATAMARAN_HOME") {
        return Ok(PathBuf::from(dir));
    }
    let home = std::env::var_os("HOME").ok_or_else(|| anyhow::anyhow!("HOME is not set"))?;
    Ok(PathBuf::from(home).join(".katamaran"))
}
