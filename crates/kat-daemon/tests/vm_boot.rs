//! Boots a real VM. Needs Hypervisor.framework, a signed `katamaran-vm` and
//! the guest artefacts, so it is ignored by default; `cargo xtask test-vm`
//! builds everything and runs it.

use std::path::PathBuf;
use std::process::Command;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

#[test]
#[ignore = "boots a VM; run with `cargo xtask test-vm`"]
fn boots_says_hello_and_powers_off() {
    let root = root();
    let out = Command::new(root.join("target/release/katamaran"))
        .current_dir(&root)
        .args(["debug-boot", "--pings", "100", "--timeout", "20"])
        .output()
        .expect("run katamaran debug-boot");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "debug-boot failed\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );

    let field = |name: &str| -> f64 {
        let line = stdout
            .lines()
            .find(|l| l.trim_start().starts_with(name))
            .unwrap_or_else(|| panic!("no `{name}` in output:\n{stdout}"));
        line.split_whitespace()
            .rev()
            .nth(1)
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| panic!("cannot parse `{line}`"))
    };
    assert!(
        stdout.contains("vm exit status          exit status: 0"),
        "{stdout}"
    );
    let ready_ms = field("spawn -> agent ready");
    assert!(ready_ms < 1000.0, "boot took {ready_ms} ms");
    assert!(field("guest memory") > 1500.0, "{stdout}");
}
