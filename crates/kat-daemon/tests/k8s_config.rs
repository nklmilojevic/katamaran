//! Real-VM end-to-end verification for Kubernetes `k8s_disable`, `k8s_args`,
//! and normalized `~/.katamaran/k3s-config.yaml` (`K3S_CONFIG_FILE`).
//!
//! Ignored by default; run with `cargo xtask test-vm` (or `cargo xtask build &&
//! cargo test --release -p kat-daemon --test k8s_config -- --ignored`).

use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

fn root() -> PathBuf {
    std::fs::canonicalize(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")).unwrap()
}

struct K8sDaemon {
    child: Child,
    home: PathBuf,
}

impl K8sDaemon {
    fn spawn(home: &Path, user_home: &Path) -> Self {
        std::fs::create_dir_all(home).unwrap();
        std::fs::create_dir_all(user_home).unwrap();
        let child = Command::new(root().join("target/release/katamaran"))
            .arg("daemon")
            .env("KATAMARAN_HOME", home)
            .env("HOME", user_home)
            .env("KATAMARAN_ISOLATED_DNS", "1")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn katamaran daemon");
        let sock = home.join("run/daemon.sock");
        let deadline = Instant::now() + Duration::from_secs(10);
        while UnixStream::connect(&sock).is_err() {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for daemon socket {}",
                sock.display()
            );
            thread::sleep(Duration::from_millis(100));
        }
        Self {
            child,
            home: home.to_path_buf(),
        }
    }
}

impl Drop for K8sDaemon {
    fn drop(&mut self) {
        if let Ok(None) = self.child.try_wait() {
            unsafe { libc::kill(self.child.id() as i32, libc::SIGTERM) };
            for _ in 0..600 {
                if let Ok(Some(_)) = self.child.try_wait() {
                    break;
                }
                thread::sleep(Duration::from_millis(100));
            }
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        let _ = std::fs::remove_dir_all(&self.home);
    }
}

fn run_ok(cmd: &mut Command, what: &str) -> String {
    let out: Output = cmd.output().unwrap_or_else(|e| panic!("spawn {what}: {e}"));
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(
        out.status.success(),
        "{what} failed ({}):\nstdout:\n{stdout}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    stdout
}

fn seed_k3s_cache(home: &Path) {
    let Some(real_home) = std::env::var_os("HOME") else {
        return;
    };
    let src_root = PathBuf::from(real_home).join(".katamaran/cache/k3s");
    let Ok(entries) = std::fs::read_dir(&src_root) else {
        return;
    };
    let dst_root = home.join("cache/k3s");
    for entry in entries.flatten() {
        let Ok(files) = std::fs::read_dir(entry.path()) else {
            continue;
        };
        for file in files.flatten() {
            let name = file.file_name();
            if name.to_str().is_some_and(|s| s.starts_with("k3s-")) && file.path().is_file() {
                let dst_dir = dst_root.join(entry.file_name());
                if std::fs::create_dir_all(&dst_dir).is_ok() {
                    let _ = std::fs::copy(file.path(), dst_dir.join(&name));
                }
            }
        }
    }
}

/// Boots a real VM and starts real `k3s server` with `k8s_disable`, `k8s_args`,
/// and a `k3s-config.yaml` exercising block/flow sequences, `+` append keys,
/// comments, and quoted scalars. Confirms that k3s parses Katamaran's
/// canonical JSON `K3S_CONFIG_FILE`, merges both CLI and YAML settings, treats
/// comment-only YAML edits as a clean no-op, and refuses silent no-op starts
/// when effective settings change while running.
#[test]
#[ignore = "boots the system VM with k3s; run with `cargo xtask test-vm`"]
fn k3s_applies_extra_args_and_normalized_config_yaml() {
    let kat = root().join("target/release/katamaran");
    let home = root().join(format!("target/test-home-k8s-cfg-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&home);
    let user_home = home.join("fake-home");
    std::fs::create_dir_all(&user_home).unwrap();
    seed_k3s_cache(&home);

    let kat_cmd = |args: &[&str]| {
        let mut c = Command::new(&kat);
        c.args(args)
            .env("KATAMARAN_HOME", &home)
            .env("HOME", &user_home)
            .stdin(Stdio::null());
        c
    };

    run_ok(
        &mut kat_cmd(&[
            "config",
            "set",
            "cpus=2",
            "memory=2G",
            "mount=off",
            "k8s_disable=traefik",
            "k8s_args=--debug --node-label=katamaran.dev/cli=yes",
        ]),
        "config set",
    );

    let yaml_path = home.join("k3s-config.yaml");
    std::fs::write(
        &yaml_path,
        "# Live k3s compatibility check\n\
         node-label:\n\
         \x20 - katamaran.dev/yaml-a=one\n\
         node-label+: [\"katamaran.dev/yaml-b=two\"]\n\
         disable:\n\
         \x20 - \"servicelb\"\n",
    )
    .unwrap();

    let _daemon = K8sDaemon::spawn(&home, &user_home);
    run_ok(&mut kat_cmd(&["start"]), "kat start");
    run_ok(&mut kat_cmd(&["k8s", "start"]), "kat k8s start");

    let kubeconfig = run_ok(
        &mut kat_cmd(&["k8s", "kubeconfig", "--path"]),
        "kat k8s kubeconfig --path",
    )
    .trim()
    .to_string();
    let kubectl_cmd = |args: &[&str]| {
        let mut c = Command::new("kubectl");
        c.args(["--kubeconfig", &kubeconfig])
            .args(args)
            .stdin(Stdio::null());
        c
    };

    // Real k3s must have parsed both CLI `k8s_args` and the canonical JSON
    // `K3S_CONFIG_FILE` (including `+` append merge and flow/block lists).
    let labels = run_ok(
        &mut kubectl_cmd(&["get", "nodes", "-o", "jsonpath={.items[0].metadata.labels}"]),
        "kubectl get node labels",
    );
    for expected in [
        "\"katamaran.dev/cli\":\"yes\"",
        "\"katamaran.dev/yaml-a\":\"one\"",
        "\"katamaran.dev/yaml-b\":\"two\"",
    ] {
        assert!(
            labels.contains(expected),
            "expected node label {expected} in {labels}"
        );
    }

    // Both CLI `k8s_disable=traefik` and YAML `disable: ["servicelb"]` took effect.
    let deployments = run_ok(
        &mut kubectl_cmd(&["get", "deployments", "-n", "kube-system", "-o", "name"]),
        "kubectl get kube-system deployments",
    );
    assert!(
        !deployments.contains("traefik"),
        "traefik should be disabled; deployments:\n{deployments}"
    );
    let ds = run_ok(
        &mut kubectl_cmd(&["get", "daemonsets", "-n", "kube-system", "-o", "name"]),
        "kubectl get kube-system daemonsets",
    );
    assert!(
        !ds.contains("svclb"),
        "servicelb should be disabled; daemonsets:\n{ds}"
    );

    // Comment/formatting-only YAML edit keeps `kat k8s start` a clean no-op.
    std::fs::write(
        &yaml_path,
        "# Updated comment only\n\
         node-label: [katamaran.dev/yaml-a=one, katamaran.dev/yaml-b=two]\n\
         disable: [servicelb]\n",
    )
    .unwrap();
    run_ok(
        &mut kat_cmd(&["k8s", "start"]),
        "comment-only YAML edit no-op start",
    );

    // Effective YAML edit while running must be refused until `kat k8s stop`.
    std::fs::write(
        &yaml_path,
        "node-label: [katamaran.dev/yaml-a=changed]\n\
         disable: [servicelb]\n",
    )
    .unwrap();
    let changed = kat_cmd(&["k8s", "start"]).output().unwrap();
    assert!(
        !changed.status.success(),
        "expected k8s start to reject effective YAML change while running"
    );
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&changed.stdout),
        String::from_utf8_lossy(&changed.stderr)
    );
    assert!(
        combined.contains("kat k8s stop"),
        "expected restart hint in error, got:\n{combined}"
    );

    run_ok(&mut kat_cmd(&["k8s", "stop"]), "kat k8s stop");
    run_ok(&mut kat_cmd(&["stop"]), "kat stop");
}
