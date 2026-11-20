use anyhow::Context;
use kat_vmm::VmConfig;

fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn"))
        .format_timestamp_micros()
        .init();
    let path = std::env::args_os()
        .nth(1)
        .context("usage: katamaran-vm <config.json>")?;
    let cfg: VmConfig = serde_json::from_slice(
        &std::fs::read(&path).with_context(|| format!("read {}", path.to_string_lossy()))?,
    )
    .context("parse VM config")?;
    boot(&cfg)
}

#[cfg(target_os = "macos")]
fn boot(cfg: &VmConfig) -> anyhow::Result<()> {
    kat_vmm::run::run(cfg)
}

#[cfg(not(target_os = "macos"))]
fn boot(_cfg: &VmConfig) -> anyhow::Result<()> {
    anyhow::bail!("katamaran-vm runs only on macOS")
}
