//! amd64 images: `docker run --platform linux/amd64` under each x86-64
//! emulator (`run --amd64 rosetta|qemu`). Under Rosetta, x86 code must also
//! keep x86's store order (TSO), which the guest kernel switches on per task.
//! Ignored by default; `cargo xtask test-vm` builds everything and runs it.

use std::io::{BufRead, BufReader, Read};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

const ROSETTA: &str = "/Library/Apple/usr/libexec/oah/RosettaLinux/rosetta";

/// Message passing: the writer stores data then flag, the reader loads flag
/// then data. Under x86's TSO the reader never sees a newer flag than data.
const LITMUS: &str = r#"
#include <pthread.h>
#include <stdio.h>
#define N 5000000L
static volatile long data, flag;
static void *writer(void *a) {
    for (long i = 1; i <= N; i++) {
        data = i;
        __asm__ volatile("" ::: "memory");
        flag = i;
    }
    return a;
}
int main(void) {
    pthread_t t;
    long bad = 0;
    pthread_create(&t, 0, writer, 0);
    for (;;) {
        long f = flag;
        __asm__ volatile("" ::: "memory");
        long d = data;
        if (d < f) bad++;
        if (f == N) break;
    }
    pthread_join(t, 0);
    printf("reorderings=%ld\n", bad);
    return 0;
}
"#;

fn root() -> PathBuf {
    std::fs::canonicalize(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")).unwrap()
}

struct Vm {
    child: Child,
    lines: mpsc::Receiver<String>,
    output: Vec<String>,
    home: PathBuf,
}

impl Vm {
    fn start(amd64: &str) -> Vm {
        let root = root();
        let home = root.join(format!(
            "target/test-home-amd64-{amd64}-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&home).unwrap();
        let mut child = Command::new(root.join("target/release/katamaran"))
            .current_dir(&root)
            .env("KATAMARAN_HOME", &home)
            .args(["run", "--cpus", "2", "--memory-mib", "2048"])
            .args(["--amd64", amd64])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn katamaran run");
        let (tx, lines) = mpsc::channel();
        for stream in [
            Box::new(child.stdout.take().unwrap()) as Box<dyn Read + Send>,
            Box::new(child.stderr.take().unwrap()),
        ] {
            let tx = tx.clone();
            thread::spawn(move || {
                for line in BufReader::new(stream).lines().map_while(Result::ok) {
                    let _ = tx.send(line);
                }
            });
        }
        Vm {
            child,
            lines,
            output: Vec::new(),
            home,
        }
    }

    fn wait_for(&mut self, needle: &str, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            match self.lines.recv_timeout(left) {
                Ok(line) => {
                    let hit = line.contains(needle);
                    self.output.push(line);
                    if hit {
                        return;
                    }
                }
                Err(_) => panic!(
                    "no `{needle}` within {timeout:?}; output:\n{}",
                    self.output.join("\n")
                ),
            }
        }
    }

    /// `docker run --rm --platform linux/amd64 <args>`; panics on failure.
    fn run_amd64(&self, args: &[&str]) -> String {
        let out = Command::new("docker")
            .env(
                "DOCKER_HOST",
                format!("unix://{}", self.home.join("run/docker.sock").display()),
            )
            .env_remove("DOCKER_CONTEXT")
            .args(["run", "--rm", "--platform", "linux/amd64"])
            .args(args)
            .output()
            .expect("run docker");
        let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
        assert!(
            out.status.success(),
            "docker run {args:?}: {}\n{stdout}{}",
            out.status,
            String::from_utf8_lossy(&out.stderr)
        );
        stdout
    }

    fn stop(mut self) {
        unsafe { libc::kill(self.child.id() as i32, libc::SIGINT) };
        self.wait_for("VM stopped", Duration::from_secs(30));
        let status = self.child.wait().unwrap();
        assert!(status.success(), "katamaran run exited with {status}");
    }
}

/// A failing assertion must not leave a VM running.
impl Drop for Vm {
    fn drop(&mut self) {
        if let Ok(None) = self.child.try_wait() {
            unsafe { libc::kill(self.child.id() as i32, libc::SIGINT) };
            for _ in 0..300 {
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

/// Boot with `amd64`, check that x86-64 programs run, and that a threaded
/// x86 program sees no store reordering.
fn amd64_images_run(amd64: &str) {
    let mut vm = Vm::start(amd64);
    vm.wait_for("docker ready", Duration::from_secs(120));

    assert_eq!(
        vm.run_amd64(&["alpine:3.24.4", "uname", "-m"]).trim(),
        "x86_64"
    );

    // A real workload: install a compiler, build and run a threaded program.
    // The work directory is under /Users, which the guest shares.
    let work = vm.home.join("work");
    std::fs::create_dir_all(&work).unwrap();
    std::fs::write(work.join("mp.c"), LITMUS).unwrap();
    let mount = format!("{}:/w", work.display());
    let started = Instant::now();
    let out = vm.run_amd64(&[
        "-v",
        &mount,
        "-w",
        "/w",
        "alpine:3.24.4",
        "sh",
        "-c",
        "apk add -q gcc musl-dev >/dev/null && gcc -O2 -pthread -o mp mp.c && ./mp",
    ]);
    eprintln!("{amd64}: apk + gcc + litmus took {:?}", started.elapsed());
    assert_eq!(
        out.trim(),
        "reorderings=0",
        "{amd64}: x86 stores were reordered"
    );
    assert!(Path::new(&work.join("mp")).is_file());
    vm.stop();
}

#[test]
#[ignore = "boots the system VM; run with `cargo xtask test-vm`"]
fn amd64_images_run_under_rosetta() {
    if !Path::new(ROSETTA).is_file() {
        eprintln!("skipped: Rosetta is not installed ({ROSETTA})");
        return;
    }
    amd64_images_run("rosetta");
}

#[test]
#[ignore = "boots the system VM; run with `cargo xtask test-vm`"]
fn amd64_images_run_under_qemu() {
    amd64_images_run("qemu");
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

/// Regression test for single-manifest `linux/amd64` Docker-to-CRI mirroring:
/// `ctr images check` reports `complete` for single-manifest amd64 images even
/// on arm64, so `kat k8s sync-images` must still rewrite the config platform.
/// Running the pod with `imagePullPolicy=Never` guarantees kubelet resolves the
/// synced tag locally through CRI (failing with `ErrImageNeverPull` if sync
/// took the unchanged native path) and executes the x86-64 binary under emulation.
#[test]
#[ignore = "boots the system VM with k3s; run with `cargo xtask test-vm`"]
fn k8s_runs_synced_single_manifest_amd64_image() {
    let kat = root().join("target/release/katamaran");
    let home = root().join(format!("target/test-home-k8s-amd64-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&home);
    let user_home = home.join("fake-home");
    std::fs::create_dir_all(&user_home).unwrap();

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
            "amd64=auto",
        ]),
        "config set",
    );
    let _daemon = K8sDaemon::spawn(&home, &user_home);
    run_ok(&mut kat_cmd(&["start"]), "kat start");
    run_ok(&mut kat_cmd(&["k8s", "start"]), "kat k8s start");

    let docker_host = format!("unix://{}", home.join("run/docker.sock").display());
    let docker_cmd = |args: &[&str]| {
        let mut c = Command::new("docker");
        c.args(args)
            .env("DOCKER_HOST", &docker_host)
            .env_remove("DOCKER_CONTEXT")
            .stdin(Stdio::null());
        c
    };

    // Build and `docker load` a local single-manifest linux/amd64 image (not a
    // multi-arch OCI index, and not present in any registry).
    run_ok(
        &mut docker_cmd(&["pull", "--platform", "linux/amd64", "alpine:3.24.4"]),
        "docker pull amd64 alpine",
    );
    let cid = run_ok(
        &mut docker_cmd(&[
            "create",
            "--platform",
            "linux/amd64",
            "alpine:3.24.4",
            "/bin/sh",
        ]),
        "docker create amd64",
    )
    .trim()
    .to_string();
    let tag = "kat-single-amd64-test:dev";
    run_ok(
        &mut docker_cmd(&["commit", &cid, tag]),
        "docker commit single-manifest amd64",
    );
    run_ok(&mut docker_cmd(&["rm", &cid]), "docker rm");
    let tar_path = home.join("single-amd64.tar");
    let tar_str = tar_path.to_str().unwrap();
    run_ok(
        &mut docker_cmd(&["save", "-o", tar_str, tag]),
        "docker save single-manifest amd64",
    );
    run_ok(&mut docker_cmd(&["rmi", tag]), "docker rmi");
    run_ok(
        &mut docker_cmd(&["load", "-i", tar_str]),
        "docker load single-manifest amd64",
    );
    let arch = run_ok(
        &mut docker_cmd(&["image", "inspect", "--format", "{{.Architecture}}", tag]),
        "docker image inspect architecture",
    );
    assert_eq!(arch.trim(), "amd64");

    // Sync into k8s.io explicitly.
    run_ok(&mut kat_cmd(&["k8s", "sync-images"]), "kat k8s sync-images");

    // Resolve the tag through CRI with imagePullPolicy=Never and assert x86_64.
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
    run_ok(
        &mut kubectl_cmd(&[
            "run",
            "amd64-single-check",
            &format!("--image={tag}"),
            "--image-pull-policy=Never",
            "--restart=Never",
            "--",
            "uname",
            "-m",
        ]),
        "kubectl run amd64-single-check",
    );

    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let phase = run_ok(
            &mut kubectl_cmd(&[
                "get",
                "pod",
                "amd64-single-check",
                "-o",
                "jsonpath={.status.phase}",
            ]),
            "kubectl get pod phase",
        );
        match phase.trim() {
            "Succeeded" => break,
            "Failed" => {
                let desc = run_ok(
                    &mut kubectl_cmd(&["describe", "pod", "amd64-single-check"]),
                    "kubectl describe pod",
                );
                panic!("amd64 pod failed:\n{desc}");
            }
            _ => {
                let waiting = run_ok(
                    &mut kubectl_cmd(&[
                        "get",
                        "pod",
                        "amd64-single-check",
                        "-o",
                        "jsonpath={.status.containerStatuses[0].state.waiting.reason}",
                    ]),
                    "kubectl get waiting reason",
                );
                assert!(
                    waiting.trim() != "ErrImageNeverPull" && waiting.trim() != "ImagePullBackOff",
                    "CRI rejected local single-manifest amd64 image ({waiting}); sync likely took the unchanged native path"
                );
                assert!(
                    Instant::now() < deadline,
                    "timed out waiting for amd64-single-check pod (phase={phase}, waiting={waiting})"
                );
                thread::sleep(Duration::from_millis(500));
            }
        }
    }

    let logs = run_ok(
        &mut kubectl_cmd(&["logs", "amd64-single-check"]),
        "kubectl logs amd64-single-check",
    );
    assert_eq!(
        logs.trim(),
        "x86_64",
        "expected pod to execute x86_64 binary under emulation"
    );

    run_ok(&mut kat_cmd(&["k8s", "stop"]), "kat k8s stop");
    run_ok(&mut kat_cmd(&["stop"]), "kat stop");
}
