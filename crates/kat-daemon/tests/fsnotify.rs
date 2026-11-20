//! File changes made on the Mac reach inotify watchers in a container, and
//! the guest's cached attributes for a changed file are dropped. Boots the
//! system VM; ignored by default, `cargo xtask test-vm` builds everything and
//! runs it. The test image (Alpine with inotify-tools) is built once on
//! the host's Docker (KAT_TEST_IMAGE_CONTEXT, default the current context) and
//! loaded into the VM.

use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const IMAGE: &str = "katamaran-fsnotify-test:1";
const BASE: &str =
    "alpine:3.24.4@sha256:f59abe656b692d901e184be3445e084786602108599d2a51d229c0f5a48c71bf";
/// Each event must reach the container within this.
const DEADLINE: Duration = Duration::from_millis(500);

fn root() -> PathBuf {
    std::fs::canonicalize(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../.."))
        .expect("workspace root")
}

struct Vm {
    child: Child,
    lines: mpsc::Receiver<String>,
    output: Vec<String>,
    home: PathBuf,
}

impl Vm {
    fn start() -> Vm {
        let root = root();
        let home = root.join(format!("target/test-home-fsn-{}", std::process::id()));
        std::fs::create_dir_all(&home).unwrap();
        let mut child = Command::new(root.join("target/release/katamaran"))
            .current_dir(&root)
            .env("KATAMARAN_HOME", &home)
            .env("KATAMARAN_FSWATCH_DEBUG", "1")
            .args(["run", "--cpus", "2", "--memory-mib", "2048"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn katamaran run");
        let lines = lines_of(&mut child, true);
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

    fn docker(&self) -> Command {
        let mut c = Command::new("docker");
        c.arg("-H").arg(format!(
            "unix://{}",
            self.home.join("run/docker.sock").display()
        ));
        c
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

/// Lines of the child's stdout and stderr, as they come; `echo` also
/// prints the watcher's trace, shown when the test fails.
fn lines_of(child: &mut Child, echo: bool) -> mpsc::Receiver<String> {
    let (tx, rx) = mpsc::channel();
    for stream in [
        Box::new(child.stdout.take().unwrap()) as Box<dyn Read + Send>,
        Box::new(child.stderr.take().unwrap()),
    ] {
        let tx = tx.clone();
        thread::spawn(move || {
            for line in BufReader::new(stream).lines().map_while(Result::ok) {
                if echo && line.starts_with("fswatch ") {
                    eprintln!("{line}");
                }
                let _ = tx.send(line);
            }
        });
    }
    rx
}

fn run(cmd: &mut Command) -> String {
    let out = cmd.output().unwrap_or_else(|e| panic!("{cmd:?}: {e}"));
    assert!(
        out.status.success(),
        "{cmd:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// `docker` on the engine that supplies the test images: the context named by
/// KAT_TEST_IMAGE_CONTEXT, or else the current Docker context.
fn image_docker() -> Command {
    let mut docker = Command::new("docker");
    if let Ok(ctx) = std::env::var("KAT_TEST_IMAGE_CONTEXT") {
        docker.args(["--context", &ctx]);
    }
    docker
}

/// The saved test image, built on the host's Docker the first time.
fn image_tar() -> PathBuf {
    let tar = root().join("target/fsnotify-test-image.tar");
    if !tar.is_file() {
        let mut build = image_docker();
        build
            .args(["build", "-q", "--platform", "linux/arm64"])
            .args(["-t", IMAGE, "-"])
            .stdin(Stdio::piped())
            .stdout(Stdio::null());
        let mut child = build.spawn().expect("docker build");
        std::io::Write::write_all(
            child.stdin.as_mut().unwrap(),
            format!("FROM {BASE}\nRUN apk add --no-cache inotify-tools\n").as_bytes(),
        )
        .unwrap();
        drop(child.stdin.take());
        assert!(child.wait().unwrap().success(), "docker build failed");
        run(image_docker().args(["save", "-o"]).arg(&tar).arg(IMAGE));
    }
    tar
}

/// `inotifywait -m` in a container, its event lines timestamped on arrival.
struct Watch {
    child: Child,
    lines: mpsc::Receiver<String>,
    seen: Vec<String>,
}

impl Watch {
    fn start(vm: &Vm, dir: &Path) -> Watch {
        let mut child = vm
            .docker()
            // A terminal, so inotifywait writes each line as it happens.
            .args(["run", "--rm", "-t", "--name", "fsnotify-watch"])
            .arg("-v")
            .arg(format!("{0}:{0}", dir.display()))
            .args([IMAGE, "inotifywait", "-m", "--format", "%e %f"])
            .arg(dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("docker run inotifywait");
        let lines = lines_of(&mut child, false);
        let mut w = Watch {
            child,
            lines,
            seen: Vec::new(),
        };
        w.expect("Watches established.", Duration::from_secs(30));
        w
    }

    /// Wait for a line equal to `want`; returns how long it took.
    fn expect(&mut self, want: &str, timeout: Duration) -> Duration {
        let t = Instant::now();
        loop {
            let left = timeout.saturating_sub(t.elapsed());
            match self.lines.recv_timeout(left) {
                Ok(line) => {
                    let line = line.trim_end_matches('\r').to_owned();
                    let hit = line == want;
                    self.seen.push(line);
                    if hit {
                        return t.elapsed();
                    }
                }
                Err(_) => panic!(
                    "no `{want}` within {timeout:?}; inotifywait printed:\n{}",
                    self.seen.join("\n")
                ),
            }
        }
    }
}

impl Drop for Watch {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
#[ignore = "boots the system VM; run with `cargo xtask test-vm`"]
fn mac_changes_reach_inotify_and_drop_cached_attributes() {
    let tar = image_tar();
    let dir = root().join(format!("target/fsnotify-test-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let dir = std::fs::canonicalize(&dir).unwrap();
    std::fs::write(dir.join("sized"), "abc").unwrap();

    let mut vm = Vm::start();
    vm.wait_for("docker ready", Duration::from_secs(120));
    run(vm.docker().arg("load").arg("-q").arg("-i").arg(&tar));

    // A second container caches the attributes of `sized` before the watch.
    let stat = |vm: &Vm| -> String {
        run(vm
            .docker()
            .args(["exec", "fsnotify-stat", "stat", "-c", "%s"])
            .arg(dir.join("sized")))
        .trim()
        .to_owned()
    };
    run(vm
        .docker()
        .args(["run", "-d", "--rm", "--name", "fsnotify-stat", "-v"])
        .arg(format!("{0}:{0}", dir.display()))
        .args([IMAGE, "sleep", "300"]));
    assert_eq!(stat(&vm), "3");

    let mut w = Watch::start(&vm, &dir);
    let mut took = Vec::new();
    let mut step = |w: &mut Watch, what: &str, change: &dyn Fn(), want: &[&str]| {
        let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        eprintln!("test {}.{:03}: {what}", t.as_secs(), t.subsec_millis());
        change();
        for line in want {
            let t = w.expect(line, Duration::from_secs(5));
            assert!(t <= DEADLINE, "{what}: `{line}` took {t:?}");
            took.push((what.to_owned(), t));
        }
    };
    step(
        &mut w,
        "create",
        &|| std::fs::write(dir.join("a"), "").unwrap(),
        &["CREATE a"],
    );
    step(
        &mut w,
        "modify",
        &|| std::fs::write(dir.join("a"), "data").unwrap(),
        &["MODIFY a", "CLOSE_WRITE,CLOSE a"],
    );
    step(
        &mut w,
        "chmod",
        &|| {
            std::fs::set_permissions(
                dir.join("a"),
                std::os::unix::fs::PermissionsExt::from_mode(0o600),
            )
            .unwrap()
        },
        &["ATTRIB a"],
    );
    step(
        &mut w,
        "rename",
        &|| std::fs::rename(dir.join("a"), dir.join("b")).unwrap(),
        &["MOVED_FROM a", "MOVED_TO b"],
    );
    // A rename right after another change: FSEvents' event for that change
    // can name the path once it is already renamed.
    for i in 0..30 {
        let (from, to) = if i % 2 == 0 { ("b", "a") } else { ("a", "b") };
        step(
            &mut w,
            "chmod",
            &|| {
                std::fs::set_permissions(
                    dir.join(from),
                    std::os::unix::fs::PermissionsExt::from_mode(0o600 | ((i + 1) % 2) << 5),
                )
                .unwrap()
            },
            &[&format!("ATTRIB {from}")],
        );
        step(
            &mut w,
            "rename",
            &|| std::fs::rename(dir.join(from), dir.join(to)).unwrap(),
            &[&format!("MOVED_FROM {from}"), &format!("MOVED_TO {to}")],
        );
    }
    step(
        &mut w,
        "delete",
        &|| std::fs::remove_file(dir.join("b")).unwrap(),
        &["DELETE b"],
    );
    step(
        &mut w,
        "mkdir",
        &|| std::fs::create_dir(dir.join("d")).unwrap(),
        &["CREATE,ISDIR d"],
    );
    step(
        &mut w,
        "rmdir",
        &|| std::fs::remove_dir(dir.join("d")).unwrap(),
        &["DELETE,ISDIR d"],
    );

    // The guest cached `sized` as 3 bytes, with a 5 s attribute timeout. Once
    // the change is reported its new size shows at once.
    step(
        &mut w,
        "grow",
        &|| std::fs::write(dir.join("sized"), "abcdefgh").unwrap(),
        &["MODIFY sized"],
    );
    assert_eq!(stat(&vm), "8", "cached size not dropped");

    for (what, t) in &took {
        println!("{what}: {t:?}");
    }
    drop(w);
    let _ = vm.docker().args(["rm", "-f", "fsnotify-stat"]).output();
    vm.stop();
    let _ = std::fs::remove_dir_all(&dir);
}
