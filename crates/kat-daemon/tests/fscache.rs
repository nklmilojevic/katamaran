//! The guest caches the /Users share for long, and every change made on the
//! Mac still reaches containers at once, with no inotify watch anywhere:
//! the VM process drops exactly what the guest caches. Boots the system VM;
//! ignored by default, `cargo xtask test-vm` builds everything and runs it.
//! The test image (Alpine with git) is built once on the
//! host's Docker (KAT_TEST_IMAGE_CONTEXT, default the current context) and
//! loaded into the VM.

use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

const IMAGE: &str = "katamaran-fscache-test:1";
const BASE: &str =
    "alpine:3.24.4@sha256:f59abe656b692d901e184be3445e084786602108599d2a51d229c0f5a48c71bf";
/// A change to a file or a listing must show in the container within this.
const DEADLINE: Duration = Duration::from_millis(100);
/// `git checkout` on the Mac to a clean `git status` in the container.
const GIT_DEADLINE: Duration = Duration::from_secs(1);
/// A wait that missed its deadline still ends here, with the elapsed time.
const GIVE_UP: Duration = Duration::from_secs(15);

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
    fn start(tag: &str) -> Vm {
        let root = root();
        let home = root.join(format!("target/test-home-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&home).unwrap();
        let mut child = Command::new(root.join("target/release/katamaran"))
            .current_dir(&root)
            .env("KATAMARAN_HOME", &home)
            .env("KATAMARAN_FSCACHE_DEBUG", "1")
            .args(["run", "--cpus", "2", "--memory-mib", "2048"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn katamaran run");
        let lines = lines_of(&mut child);
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

    /// Boot, wait for Docker and load the test image.
    fn ready(tag: &str) -> Vm {
        let tar = image_tar();
        let mut vm = Vm::start(tag);
        vm.wait_for("docker ready", Duration::from_secs(120));
        run(vm.docker().arg("load").arg("-q").arg("-i").arg(&tar));
        vm
    }

    /// A long-running container with `dir` bind-mounted at the same path.
    fn container(&self, name: &str, dir: &Path, extra: &[&str]) -> Container<'_> {
        run(self
            .docker()
            .args(["run", "-d", "--rm", "--name", name, "-v"])
            .arg(format!("{0}:{0}", dir.display()))
            .args(extra)
            .args([IMAGE, "sleep", "600"]));
        Container {
            vm: self,
            name: name.to_owned(),
        }
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

fn lines_of(child: &mut Child) -> mpsc::Receiver<String> {
    let (tx, rx) = mpsc::channel();
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
    rx
}

fn run(cmd: &mut Command) -> String {
    let out = cmd.output().unwrap_or_else(|e| panic!("{cmd:?}: {e}"));
    assert!(
        out.status.success(),
        "{cmd:?}: {}{}",
        String::from_utf8_lossy(&out.stdout),
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
    let tar = root().join("target/fscache-test-image.tar");
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
            format!("FROM {BASE}\nRUN apk add --no-cache git\n").as_bytes(),
        )
        .unwrap();
        drop(child.stdin.take());
        assert!(child.wait().unwrap().success(), "docker build failed");
        run(image_docker().args(["save", "-o"]).arg(&tar).arg(IMAGE));
    }
    tar
}

struct Container<'a> {
    vm: &'a Vm,
    name: String,
}

impl Container<'_> {
    fn sh(&self, script: &str) -> String {
        run(self
            .vm
            .docker()
            .args(["exec", &self.name, "sh", "-c", script]))
    }

    /// Run `cond` (a shell condition) in the container until it holds after
    /// `change` is made on the Mac; returns how long that took. `cond` is
    /// evaluated, and so cached by the guest, before the change, and must not
    /// hold yet. `pause` is a shell command run between tries.
    fn until(&self, cond: &str, pause: &str, change: impl FnOnce()) -> Duration {
        // Errors while the change lands (a file replaced under an open) are
        // not the answer: only "ready" and "done" are read.
        let script = format!(
            "exec 2>/dev/null; if {cond}; then echo early; exit 0; fi; echo ready; \
             until {cond}; do {pause}; done; echo done"
        );
        let mut child = self
            .vm
            .docker()
            .args(["exec", &self.name, "sh", "-c", &script])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("docker exec");
        let lines = lines_of(&mut child);
        let next = |timeout: Duration| lines.recv_timeout(timeout).ok();
        let first = next(Duration::from_secs(30));
        assert_eq!(
            first.as_deref(),
            Some("ready"),
            "{cond}: holds before the change"
        );
        let t = Instant::now();
        change();
        let got = next(GIVE_UP);
        let took = t.elapsed();
        let _ = child.kill();
        let _ = child.wait();
        assert_eq!(
            got.as_deref(),
            Some("done"),
            "{cond}: never held after the change"
        );
        took
    }
}

impl Drop for Container<'_> {
    fn drop(&mut self) {
        let _ = self.vm.docker().args(["rm", "-f", &self.name]).output();
    }
}

struct TestDir(PathBuf);

impl TestDir {
    fn new(tag: &str) -> TestDir {
        let dir = root().join(format!("target/fscache-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        TestDir(std::fs::canonicalize(&dir).unwrap())
    }

    fn p(&self, rel: &str) -> PathBuf {
        self.0.join(rel)
    }

    fn s(&self, rel: &str) -> String {
        self.p(rel).display().to_string()
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn check(took: &mut Vec<(String, Duration)>, what: &str, t: Duration, deadline: Duration) {
    println!("{what}: {t:?}");
    took.push((what.to_owned(), t));
    assert!(t <= deadline, "{what}: took {t:?}, more than {deadline:?}");
}

#[test]
#[ignore = "boots the system VM; run with `cargo xtask test-vm`"]
fn mac_changes_reach_containers_without_watches() {
    let d = TestDir::new("files");
    std::fs::write(d.p("f"), "old").unwrap();
    std::fs::write(d.p("gone"), "x").unwrap();
    std::fs::write(d.p("moved"), "x").unwrap();
    std::fs::write(d.p("target"), "before save").unwrap();
    std::fs::write(d.p("README"), "case").unwrap();
    std::fs::create_dir(d.p("sub")).unwrap();
    std::fs::write(d.p("sub/host-file"), "").unwrap();

    let vm = Vm::ready("fscache");
    // A volume mounted over a share directory, as `-v vol:/app/node_modules`
    // does: host changes next to it and on it must not detach it.
    run(vm.docker().args(["volume", "create", "fscache-vol"]));
    let c = vm.container(
        "fscache-files",
        &d.0,
        &["-v", &format!("fscache-vol:{}", d.s("sub"))],
    );
    c.sh(&format!("touch {}", d.s("sub/in-volume")));
    let mut took = Vec::new();

    // Longer: a stale cached size would cut it short.
    let t = c.until(
        &format!("[ \"$(cat {})\" = 'new and longer' ]", d.s("f")),
        ":",
        || std::fs::write(d.p("f"), "new and longer").unwrap(),
    );
    check(&mut took, "modify", t, DEADLINE);

    let t = c.until(&format!("ls {} | grep -qx created", d.s("")), ":", || {
        std::fs::write(d.p("created"), "").unwrap()
    });
    check(&mut took, "create (ls)", t, DEADLINE);

    let t = c.until(
        &format!(
            "! ls {} | grep -qx gone && [ ! -e {} ]",
            d.s(""),
            d.s("gone")
        ),
        ":",
        || std::fs::remove_file(d.p("gone")).unwrap(),
    );
    check(&mut took, "delete (ls)", t, DEADLINE);

    // The listing, and the entry of the old name (cached by the listings).
    let t = c.until(
        &format!(
            "ls {} | grep -qx renamed && [ ! -e {} ]",
            d.s(""),
            d.s("moved")
        ),
        ":",
        || std::fs::rename(d.p("moved"), d.p("renamed")).unwrap(),
    );
    check(&mut took, "rename (ls, old name)", t, DEADLINE);

    // A name the guest looked up and found missing (cached as missing).
    let t = c.until(&format!("[ -e {} ]", d.s("later")), ":", || {
        std::fs::write(d.p("later"), "").unwrap()
    });
    check(&mut took, "create (cached as missing)", t, DEADLINE);

    // An editor's atomic save: a temporary file renamed over the target.
    let t = c.until(
        &format!("[ \"$(cat {})\" = 'after save' ]", d.s("target")),
        ":",
        || {
            std::fs::write(d.p(".target.swp"), "after save").unwrap();
            std::fs::rename(d.p(".target.swp"), d.p("target")).unwrap();
        },
    );
    check(&mut took, "atomic save", t, DEADLINE);

    // Looked up under another case (APFS is case-insensitive), removed on the Mac.
    let t = c.until(&format!("[ ! -e {} ]", d.s("readme")), ":", || {
        std::fs::remove_file(d.p("README")).unwrap()
    });
    check(&mut took, "delete (other case)", t, DEADLINE);

    // Changes to the directory a volume covers, and next to it.
    let t = c.until(&format!("ls {} | grep -qx sub2", d.s("")), ":", || {
        std::fs::set_permissions(
            d.p("sub"),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        std::fs::write(d.p("sub/host-file"), "changed").unwrap();
        std::fs::create_dir(d.p("sub2")).unwrap();
    });
    check(&mut took, "mkdir next to a volume", t, DEADLINE);
    thread::sleep(Duration::from_millis(200));
    assert_eq!(
        c.sh(&format!("ls {}", d.s("sub"))).trim(),
        "in-volume",
        "the volume over a share directory was detached"
    );

    drop(c);
    let _ = vm.docker().args(["volume", "rm", "fscache-vol"]).output();
    vm.stop();
}

fn git(dir: &Path, args: &[&str]) -> String {
    run(Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["-c", "user.name=t", "-c", "user.email=t@example.com"])
        .args(["-c", "core.fsmonitor=false"])
        .args(args))
}

#[test]
#[ignore = "boots the system VM; run with `cargo xtask test-vm`"]
fn git_checkout_on_the_mac_is_clean_in_a_container() {
    let d = TestDir::new("git");
    let repo = d.p("repo");
    std::fs::create_dir(&repo).unwrap();
    git(&repo, &["init", "-q", "-b", "one"]);
    for i in 0..300 {
        let dir = repo.join(format!("dir{}", i % 10));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(format!("file{i}")), format!("one {i}\n")).unwrap();
    }
    std::fs::write(repo.join("branch"), "one\n").unwrap();
    git(&repo, &["add", "-A"]);
    git(&repo, &["commit", "-q", "-m", "one"]);
    git(&repo, &["checkout", "-q", "-b", "two"]);
    for i in (0..300).step_by(3) {
        let f = repo.join(format!("dir{}/file{i}", i % 10));
        std::fs::write(&f, format!("two {i} and longer\n")).unwrap();
    }
    for i in (1..300).step_by(7) {
        std::fs::remove_file(repo.join(format!("dir{}/file{i}", i % 10))).unwrap();
    }
    std::fs::create_dir(repo.join("newdir")).unwrap();
    std::fs::write(repo.join("newdir/added"), "two\n").unwrap();
    std::fs::write(repo.join("branch"), "two\n").unwrap();
    git(&repo, &["add", "-A"]);
    git(&repo, &["commit", "-q", "-m", "two"]);
    let two = git(&repo, &["rev-parse", "HEAD"]).trim().to_owned();
    git(&repo, &["checkout", "-q", "one"]);

    let vm = Vm::ready("fscache-git");
    let c = vm.container("fscache-git", &d.0, &["-w", &repo.display().to_string()]);
    // The container's git status caches the tree and rewrites the index
    // with the guest's view of it; then it is clean.
    c.sh("git config --global --add safe.directory '*' && git status --porcelain");
    assert_eq!(c.sh("git status --porcelain"), "");

    let mut took = Vec::new();
    let t = c.until(
        &format!(
            "[ \"$(git rev-parse HEAD)\" = {two} ] && [ \"$(cat branch)\" = two ] \
             && [ -z \"$(git status --porcelain)\" ]"
        ),
        "sleep 0.02",
        || {
            git(&repo, &["checkout", "-q", "two"]);
        },
    );
    check(&mut took, "git checkout", t, GIT_DEADLINE);
    assert_eq!(c.sh("cat dir3/file3"), "two 3 and longer\n");
    assert!(c.sh("ls dir1").lines().all(|f| f != "file1"));

    drop(c);
    vm.stop();
}
