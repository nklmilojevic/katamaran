//! Guest `quickack` route tuning end to end on real Kubernetes (k3s) pods and
//! root container bridges. Needs internet access (to pull k3s and alpine) and
//! the `kubectl` and `docker` CLIs. Ignored by default; `cargo xtask test-vm`
//! builds everything and runs it.

use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const IMAGE: &str = "alpine:3.24.4";

fn root() -> PathBuf {
    // Canonical, so Unix socket paths stay under macOS's 104-byte limit.
    std::fs::canonicalize(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")).unwrap()
}

fn katamaran() -> PathBuf {
    root().join("target/release/katamaran")
}

fn cli(home: &Path, user_home: &Path, args: &[&str]) -> Output {
    Command::new(katamaran())
        .args(args)
        .env("KATAMARAN_HOME", home)
        .env("HOME", user_home)
        .stdin(Stdio::null())
        .output()
        .expect("run katamaran")
}

fn ok(out: &Output, what: &str) -> String {
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(
        out.status.success(),
        "{what} failed ({}):\nstdout:\n{stdout}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    stdout
}

fn wait_until(what: &str, timeout: Duration, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(200));
    }
}

struct Daemon {
    child: Child,
    home: PathBuf,
}

impl Daemon {
    fn spawn(home: &Path, user_home: &Path) -> Daemon {
        std::fs::create_dir_all(home).unwrap();
        std::fs::create_dir_all(user_home).unwrap();
        let child = Command::new(katamaran())
            .arg("daemon")
            .env("KATAMARAN_HOME", home)
            .env("HOME", user_home)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn katamaran daemon");
        let d = Daemon {
            child,
            home: home.to_path_buf(),
        };
        let sock = home.join("run/daemon.sock");
        wait_until("the daemon socket", Duration::from_secs(10), || {
            UnixStream::connect(&sock).is_ok()
        });
        d
    }
}

impl Drop for Daemon {
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

fn kubectl(kubeconfig: &Path, args: &[&str]) -> Output {
    Command::new("kubectl")
        .arg("--kubeconfig")
        .arg(kubeconfig)
        .args(args)
        .stdin(Stdio::null())
        .output()
        .expect("run kubectl")
}

/// Reuse a locally cached k3s binary from the user's real `~/.katamaran/cache/k3s`
/// if present so repeated test runs avoid re-downloading ~60 MiB.
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
        let src_bin = entry.path().join("k3s-arm64");
        if src_bin.is_file() {
            let dst_dir = dst_root.join(entry.file_name());
            if std::fs::create_dir_all(&dst_dir).is_ok() {
                let _ = std::fs::copy(&src_bin, dst_dir.join("k3s-arm64"));
            }
        }
    }
}

#[test]
#[ignore = "boots the system VM with k3s; run with `cargo xtask test-vm`"]
fn pod_routes_and_local_bridges_get_quickack() {
    let home = root().join(format!("target/tqa-{}", std::process::id()));
    let user_home = home.join("user-home");
    let _ = std::fs::remove_dir_all(&home);
    std::fs::create_dir_all(&user_home).unwrap();
    seed_k3s_cache(&home);

    ok(
        &cli(
            &home,
            &user_home,
            &["config", "set", "cpus=2", "memory=4G", "mount=off"],
        ),
        "config set",
    );

    let _daemon = Daemon::spawn(&home, &user_home);
    ok(&cli(&home, &user_home, &["start"]), "kat start");
    ok(&cli(&home, &user_home, &["k8s", "start"]), "kat k8s start");

    let kubeconfig = home.join("run/kubeconfig.yaml");
    assert!(
        kubeconfig.is_file(),
        "missing kubeconfig at {}",
        kubeconfig.display()
    );

    // Launch two ordinary non-host-network pods and inspect the route inside
    // qa-probe toward qa-peer's Pod IP.
    for name in ["qa-peer", "qa-probe"] {
        ok(
            &kubectl(
                &kubeconfig,
                &[
                    "run",
                    name,
                    "--restart=Never",
                    &format!("--image={IMAGE}"),
                    "--",
                    "sleep",
                    "300",
                ],
            ),
            &format!("create pod {name}"),
        );
    }
    for name in ["qa-peer", "qa-probe"] {
        ok(
            &kubectl(
                &kubeconfig,
                &[
                    "wait",
                    "--for=condition=Ready",
                    "--timeout=120s",
                    &format!("pod/{name}"),
                ],
            ),
            &format!("wait for pod {name}"),
        );
    }

    let peer_ip = ok(
        &kubectl(
            &kubeconfig,
            &["get", "pod", "qa-peer", "-o", "jsonpath={.status.podIP}"],
        ),
        "get qa-peer IP",
    );
    let peer_ip = peer_ip.trim().to_string();
    assert!(!peer_ip.is_empty(), "qa-peer has empty podIP");

    // BusyBox `ip route` hides route metrics; install iproute2 inside qa-probe.
    ok(
        &kubectl(
            &kubeconfig,
            &["exec", "qa-probe", "--", "apk", "add", "-q", "iproute2"],
        ),
        "install iproute2 in qa-probe",
    );

    let mut matched_routes = String::new();
    let mut all_routes = String::new();
    wait_until(
        "quickack 1 on pod route to peer",
        Duration::from_secs(30),
        || {
            let out = kubectl(
                &kubeconfig,
                &[
                    "exec", "qa-probe", "--", "ip", "-4", "route", "show", "match", &peer_ip,
                ],
            );
            if !out.status.success() {
                return false;
            }
            matched_routes = String::from_utf8_lossy(&out.stdout).into_owned();
            if let Ok(full) = Command::new("kubectl")
                .arg("--kubeconfig")
                .arg(&kubeconfig)
                .args(["exec", "qa-probe", "--", "ip", "-4", "route", "show"])
                .output()
            {
                all_routes = String::from_utf8_lossy(&full.stdout).into_owned();
            }
            !matched_routes.trim().is_empty()
                && matched_routes
                    .lines()
                    .filter(|l| !l.trim().is_empty())
                    .all(|l| l.contains("quickack 1"))
        },
    );
    eprintln!("pod route matching {peer_ip}:\n{matched_routes}all pod routes:\n{all_routes}");

    // Verify in the guest root namespace that local container bridges (cni0,
    // docker0) carry `quickack 1`, while `eth0` routes remain untouched.
    let docker_host = format!("unix://{}", home.join("run/docker.sock").display());
    let mut root_routes = String::new();
    wait_until(
        "quickack 1 on root cni0/docker0 routes",
        Duration::from_secs(15),
        || {
            let out = Command::new("docker")
                .env("DOCKER_HOST", &docker_host)
                .env_remove("DOCKER_CONTEXT")
                .args([
                    "run",
                    "--rm",
                    "--network",
                    "host",
                    IMAGE,
                    "sh",
                    "-c",
                    "apk add -q iproute2 >/dev/null && ip -4 route show",
                ])
                .output()
                .expect("run docker");
            if !out.status.success() {
                return false;
            }
            root_routes = String::from_utf8_lossy(&out.stdout).into_owned();
            let has_cni = root_routes
                .lines()
                .any(|l| l.contains("dev cni0") && l.contains("quickack 1"));
            let has_docker = root_routes
                .lines()
                .any(|l| l.contains("dev docker0") && l.contains("quickack 1"));
            has_cni && has_docker
        },
    );
    for line in root_routes.lines().filter(|l| l.contains("dev eth0")) {
        assert!(
            !line.contains("quickack 1"),
            "root eth0 route must not get quickack: {line}\nfull table:\n{root_routes}"
        );
    }
    eprintln!("root routes:\n{root_routes}");

    ok(&cli(&home, &user_home, &["stop"]), "kat stop");
}
