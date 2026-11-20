use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use kat_proto::{Hello, Msg, read_msg, write_msg};
use kat_vmm::VmConfig;

use crate::vm::{self, ms};

#[derive(clap::Args)]
pub struct BootArgs {
    /// Guest kernel (raw arm64 Image).
    #[arg(long, default_value = "guest/out/Image")]
    kernel: PathBuf,
    /// Guest initramfs (uncompressed newc cpio).
    #[arg(long, default_value = "guest/out/initramfs.cpio")]
    initramfs: PathBuf,
    #[arg(long, default_value_t = 2)]
    cpus: u8,
    #[arg(long, default_value_t = 2048)]
    memory_mib: u32,
    /// Extra kernel command line.
    #[arg(long, default_value = "")]
    cmdline: String,
    /// Path to the katamaran-vm binary. Defaults to the one next to this executable.
    #[arg(long)]
    vm_bin: Option<PathBuf>,
    /// Number of control-channel round trips to time.
    #[arg(long, default_value_t = 1000)]
    pings: u32,
    /// Seconds to wait for the guest to say hello.
    #[arg(long, default_value_t = 30)]
    timeout: u64,
}

pub fn debug_boot(args: BootArgs) -> anyhow::Result<()> {
    let run_dir = crate::state_dir()?
        .join("run")
        .join(format!("debug-boot-{}", std::process::id()));
    fs::create_dir_all(&run_dir).with_context(|| format!("create {}", run_dir.display()))?;
    let result = boot_once(&args, &run_dir);
    if result.is_err() {
        vm::print_tail("guest console", &run_dir.join("console.log"));
        vm::print_tail("katamaran-vm", &run_dir.join("vm.log"));
        eprintln!("state kept in {}", run_dir.display());
    } else {
        fs::remove_dir_all(&run_dir).ok();
    }
    result
}

fn boot_once(args: &BootArgs, run_dir: &Path) -> anyhow::Result<()> {
    let cfg = VmConfig {
        kernel: vm::absolute(&args.kernel)?,
        initramfs: vm::absolute(&args.initramfs)?,
        cmdline_extra: args.cmdline.clone(),
        cpus: args.cpus,
        memory_mib: args.memory_mib,
        console_log: run_dir.join("console.log"),
        control_socket: run_dir.join("control.sock"),
        disks: vec![],
        host_listeners: vec![],
        shares: vec![],
        network: None,
        amd64: kat_vmm::Amd64::Off,
    };
    let vm_bin = vm::vm_bin(args.vm_bin.as_deref())?;
    let mut running = vm::start(&cfg, &vm_bin, Duration::from_secs(args.timeout))?;

    let mut rtts = Vec::with_capacity(args.pings as usize);
    for seq in 0..u64::from(args.pings) {
        let t = Instant::now();
        write_msg(&mut running.conn, &Msg::Ping { seq })?;
        match read_msg(&mut running.conn)? {
            Some(Msg::Pong { seq: s }) if s == seq => rtts.push(t.elapsed()),
            other => {
                running.child.kill().ok();
                bail!("expected Pong {seq}, got {other:?}");
            }
        }
    }

    let (status, shutdown) = running.shutdown(Duration::from_secs(10))?;
    report(&running.hello, running.t_hello, &mut rtts, shutdown);
    println!("  vm exit status          {status}");
    Ok(())
}

fn report(hello: &Hello, t_hello: Duration, rtts: &mut [Duration], shutdown: Duration) {
    rtts.sort();
    let pct = |p: f64| -> Duration {
        if rtts.is_empty() {
            return Duration::ZERO;
        }
        rtts[((rtts.len() - 1) as f64 * p).round() as usize]
    };
    println!("katamaran debug-boot");
    println!("  kernel                  {}", hello.kernel_release);
    println!(
        "  guest memory            {} MiB",
        hello.mem_total_kib / 1024
    );
    println!("  spawn -> agent ready    {}", ms(t_hello));
    println!(
        "  kernel -> PID 1         {}",
        ms(Duration::from_nanos(hello.init_start_ns))
    );
    println!(
        "  PID 1 -> ready          {}",
        ms(Duration::from_nanos(
            hello.ready_ns.saturating_sub(hello.init_start_ns)
        ))
    );
    println!(
        "  control rtt p50 / p99   {} / {}",
        us(pct(0.50)),
        us(pct(0.99))
    );
    println!("  shutdown                {}", ms(shutdown));
}

fn us(d: Duration) -> String {
    format!("{:.0} µs", d.as_secs_f64() * 1e6)
}
