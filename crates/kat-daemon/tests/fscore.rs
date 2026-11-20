//! virtio-fs behaviour that keeps the synchronous (vCPU) path safe: a
//! blocking host open must not stall a vCPU, fsync works, and a whole-file
//! copy becomes an APFS clone. Boots the system VM with xattr ownership (so a
//! host FIFO can be shown to the guest as a regular file, whose open then
//! blocks on the host) and runs containers from an image loaded from the
//! host's Docker. Ignored by default; `cargo xtask test-vm` runs it.
//!
//! KAT_TEST_IMAGE (default debian:trixie, needs GNU coreutils) is loaded from
//! the Docker context KAT_TEST_IMAGE_CONTEXT (default the current context).
//! KAT_TEST_VM_BIN runs another `katamaran-vm`.

use std::ffi::CString;
use std::io::{BufRead, BufReader, Read};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

fn root() -> PathBuf {
    // Canonical, so the control socket path stays under SUN_LEN.
    std::fs::canonicalize(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")).unwrap()
}

struct Vm {
    child: Child,
    lines: mpsc::Receiver<String>,
    output: Vec<String>,
    home: PathBuf,
}

impl Vm {
    fn start(extra: &[&str]) -> Vm {
        let root = root();
        let home = root.join(format!("target/tfs-{}", std::process::id()));
        std::fs::create_dir_all(&home).unwrap();
        let mut child = Command::new(root.join("target/release/katamaran"))
            .current_dir(&root)
            .env("KATAMARAN_HOME", &home)
            .env("KATAMARAN_FS_STATS", "1")
            .args(["run", "--cpus", "2", "--memory-mib", "2048"])
            .args(extra)
            .args(
                std::env::var("KAT_TEST_VM_BIN")
                    .map(|b| vec!["--vm-bin".to_string(), b])
                    .unwrap_or_default(),
            )
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

    fn docker(&self) -> Command {
        let mut c = Command::new("docker");
        c.env(
            "DOCKER_HOST",
            format!("unix://{}", self.home.join("run/docker.sock").display()),
        );
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
        // A wedged guest can outlive a killed daemon.
        let _ = Command::new("pkill")
            .args(["-9", "-f"])
            .arg(self.home.join("run/vm.json"))
            .status();
        let _ = std::fs::remove_dir_all(&self.home);
    }
}

fn ok(out: Output, what: &str) -> String {
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(
        out.status.success(),
        "{what} failed ({})\nstdout:\n{stdout}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    stdout
}

/// Run `cmd`, killing it and failing the test after `limit`.
fn output_within(mut cmd: Command, limit: Duration, what: &str) -> Output {
    let mut child = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| panic!("{what}: {e}"));
    let started = Instant::now();
    while child.try_wait().unwrap().is_none() {
        if started.elapsed() > limit {
            let _ = child.kill();
            let _ = child.wait();
            panic!("{what}: no result within {limit:?}");
        }
        thread::sleep(Duration::from_millis(50));
    }
    child.wait_with_output().unwrap()
}

fn ok_within(cmd: Command, limit: Duration, what: &str) -> String {
    ok(output_within(cmd, limit, what), what)
}

fn image() -> String {
    std::env::var("KAT_TEST_IMAGE").unwrap_or_else(|_| "debian:trixie".into())
}

fn load_image(vm: &Vm) {
    let mut save = Command::new("docker");
    if let Ok(ctx) = std::env::var("KAT_TEST_IMAGE_CONTEXT") {
        save.args(["--context", &ctx]);
    }
    let mut save = save
        .args(["save", &image()])
        .stdout(Stdio::piped())
        .spawn()
        .expect("docker save");
    let load = vm
        .docker()
        .arg("load")
        .stdin(save.stdout.take().unwrap())
        .output()
        .expect("docker load");
    assert!(save.wait().unwrap().success(), "docker save {}", image());
    ok(load, "docker load");
}

/// `docker run --rm` with the test directory mounted at the same path.
fn run(vm: &Vm, dir: &Path, args: &[&str], script: &str) -> Command {
    let mut c = vm.docker();
    let mount = format!("{0}:{0}", dir.display());
    c.args(["run", "--rm", "-v", &mount, "-w"])
        .arg(dir)
        .args(args)
        .arg(image())
        .args(["sh", "-c", script]);
    c
}

/// libkrun's xattr ownership: what the guest sees as owner and mode.
fn override_stat(path: &Path, value: &str) {
    let p = CString::new(path.as_os_str().as_bytes()).unwrap();
    let k = c"user.containers.override_stat";
    let r = unsafe {
        libc::setxattr(
            p.as_ptr(),
            k.as_ptr(),
            value.as_ptr().cast(),
            value.len(),
            0,
            libc::XATTR_NOFOLLOW,
        )
    };
    assert_eq!(r, 0, "setxattr: {}", std::io::Error::last_os_error());
}

/// APFS clone id: equal for files that share their data blocks.
fn clone_id(path: &Path) -> u64 {
    #[repr(C)]
    struct Out {
        len: u32,
        id: u64,
    }
    let mut al: libc::attrlist = unsafe { std::mem::zeroed() };
    al.bitmapcount = libc::ATTR_BIT_MAP_COUNT;
    al.forkattr = 0x0000_0100; // ATTR_CMNEXT_CLONEID
    let mut out = [0u8; 16];
    let p = CString::new(path.as_os_str().as_bytes()).unwrap();
    let r = unsafe {
        libc::getattrlist(
            p.as_ptr(),
            (&mut al as *mut libc::attrlist).cast(),
            out.as_mut_ptr().cast(),
            std::mem::size_of::<Out>(),
            0x20, // FSOPT_ATTR_CMN_EXTENDED
        )
    };
    assert_eq!(r, 0, "getattrlist: {}", std::io::Error::last_os_error());
    u64::from_ne_bytes(out[4..12].try_into().unwrap())
}

#[test]
#[ignore = "boots the system VM; run with `cargo xtask test-vm`"]
fn blocking_open_fsync_and_clone() {
    let mut vm = Vm::start(&["--permissions", "xattr"]);
    vm.wait_for("docker ready", Duration::from_secs(120));
    load_image(&vm);

    let dir = vm.home.join("share");
    std::fs::create_dir_all(dir.join("files")).unwrap();
    for i in 0..400 {
        std::fs::write(dir.join(format!("files/f{i}")), i.to_string()).unwrap();
    }

    // A FIFO that the guest sees as a regular file: its open blocks on the
    // host until a writer appears, like a dataless iCloud file or a stalled
    // network volume.
    let fifo = dir.join("stall");
    let c = CString::new(fifo.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(c.as_ptr(), 0o644) }, 0);
    override_stat(&fifo, "0:0:0100644");

    // Both containers run on vCPU 0: if the blocking open held that vCPU,
    // the second container could not run at all.
    let mut blocker = run(
        &vm,
        &dir,
        &["--cpuset-cpus", "0", "--name", "stall"],
        "cat stall >/dev/null; echo done",
    )
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()
    .expect("start blocker");
    // Wait until the guest is inside the open.
    let started = Instant::now();
    loop {
        let mut ps = vm.docker();
        ps.args(["ps", "-q", "-f", "name=stall"]);
        let ps = ok_within(ps, Duration::from_secs(10), "docker ps");
        if !ps.trim().is_empty() {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "blocker never started"
        );
        thread::sleep(Duration::from_millis(200));
    }
    thread::sleep(Duration::from_secs(1));

    let t = Instant::now();
    let out = ok_within(
        run(
            &vm,
            &dir,
            &["--cpuset-cpus", "0"],
            "s=$(date +%s%N); stat files/* >/dev/null; cat files/f7; \
             echo new >created; e=$(date +%s%N); echo; echo stat_ms=$(( (e-s)/1000000 ))",
        ),
        Duration::from_secs(30),
        "stat loop while an open blocks",
    );
    let wall = t.elapsed();
    let stat_ms: u64 = out
        .lines()
        .find_map(|l| l.strip_prefix("stat_ms="))
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| panic!("no stat_ms in {out:?}"));
    eprintln!(
        "400 stats + open + create while an open blocks: {stat_ms} ms in the guest, {wall:?} with docker run"
    );
    assert!(out.starts_with('7'), "{out}");
    assert!(stat_ms < 3000, "stats stalled: {stat_ms} ms");
    assert!(wall < Duration::from_secs(20), "{wall:?}");
    assert_eq!(std::fs::read(dir.join("created")).unwrap(), b"new\n");

    // Release the open: the blocked reader finishes once a writer appears.
    // The open succeeds only while a host reader (the guest's open) waits.
    let started = Instant::now();
    let w = loop {
        let fd = unsafe { libc::open(c.as_ptr(), libc::O_WRONLY | libc::O_NONBLOCK) };
        if fd >= 0 {
            break fd;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the guest's open never reached the host"
        );
        thread::sleep(Duration::from_millis(50));
    };
    unsafe { libc::close(w) };
    let started = Instant::now();
    while blocker.try_wait().unwrap().is_none() {
        if started.elapsed() > Duration::from_secs(30) {
            let _ = blocker.kill();
            panic!("blocked open never completed");
        }
        thread::sleep(Duration::from_millis(100));
    }

    // fsync and fdatasync through FUSE (F_BARRIERFSYNC on the host).
    ok_within(
        run(
            &vm,
            &dir,
            &[],
            "dd if=/dev/zero of=synced bs=1M count=8 conv=fsync status=none && sync synced && sync -d synced",
        ),
        Duration::from_secs(30),
        "fsync",
    );
    assert_eq!(
        std::fs::metadata(dir.join("synced")).unwrap().len(),
        8 << 20
    );

    // cp --reflink=auto of 100 MB: copy_file_range becomes an APFS clone.
    let src = dir.join("big.src");
    let mut data = vec![0u8; 100 << 20];
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    for chunk in data.chunks_mut(8) {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        chunk.copy_from_slice(&x.to_ne_bytes()[..chunk.len()]);
    }
    std::fs::write(&src, &data).unwrap();
    let out = ok_within(
        run(
            &vm,
            &dir,
            &[],
            "s=$(date +%s%N); cp --reflink=auto big.src big.dst; e=$(date +%s%N); \
             cmp big.src big.dst && echo cp_ms=$(( (e-s)/1000000 ))",
        ),
        Duration::from_secs(60),
        "cp --reflink=auto",
    );
    let cp_ms: u64 = out
        .lines()
        .find_map(|l| l.strip_prefix("cp_ms="))
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| panic!("no cp_ms in {out:?}"));
    eprintln!("cp --reflink=auto of 100 MB: {cp_ms} ms");
    assert!(cp_ms < 500, "copy took {cp_ms} ms");
    assert!(
        std::fs::read(dir.join("big.dst")).unwrap() == data,
        "copied data differs"
    );
    assert_eq!(
        clone_id(&src),
        clone_id(&dir.join("big.dst")),
        "not an APFS clone"
    );

    vm.stop();
}

/// Sum of a FUSE opcode's counts in the VM's `fs ops` stats lines.
fn fs_op_count(vm: &Vm, opcode: u32) -> u64 {
    let log = std::fs::read_to_string(vm.home.join("run/vm.log")).unwrap_or_default();
    let key = format!("{opcode}:");
    log.lines()
        .filter_map(|l| l.split_once("fs ops (opcode:count): ").map(|(_, ops)| ops))
        .flat_map(|ops| ops.split(", "))
        .filter_map(|op| {
            op.strip_prefix(&key)?
                .split(' ')
                .next()?
                .parse::<u64>()
                .ok()
        })
        .sum()
}

/// Wait out the fs stats reporter (every 2 s) so the counts are complete.
fn settled_fs_op_count(vm: &Vm, opcode: u32) -> u64 {
    thread::sleep(Duration::from_secs(5));
    fs_op_count(vm, opcode)
}

#[test]
#[ignore = "boots the system VM; run with `cargo xtask test-vm`"]
fn caller_share_access_atime_queues_and_idle_polling() {
    const ACCESS: u32 = 34;
    const GETATTR: u32 = 3;
    let mut vm = Vm::start(&[]);
    vm.wait_for("docker ready", Duration::from_secs(120));
    load_image(&vm);

    let dir = vm.home.join("share");
    std::fs::create_dir_all(dir.join("php")).unwrap();
    for i in 0..500 {
        std::fs::write(
            dir.join(format!("php/f{i}.php")),
            format!("<?php return {i};"),
        )
        .unwrap();
    }
    use std::os::unix::fs::PermissionsExt;
    for (name, mode) in [
        ("ro", 0o444),
        ("rw", 0o644),
        ("exe", 0o755),
        ("none", 0o000),
    ] {
        std::fs::write(dir.join(name), "x").unwrap();
        std::fs::set_permissions(dir.join(name), std::fs::Permissions::from_mode(mode)).unwrap();
    }

    // access(2) answers as the host would for a caller that owns every
    // file, for root and for another user, without asking the host.
    let script = "for u in 0 24128; do \
         setpriv --reuid=$u --regid=$u --clear-groups sh -c '\
           r=\"\"; for f in ro rw exe none missing; do for m in r w x e; do \
             if test -$m $f; then r=\"$r $f:$m\"; fi; done; done; echo \"$(id -u):$r\"'; \
       done";
    let out = ok_within(
        run(&vm, &dir, &[], script),
        Duration::from_secs(60),
        "access",
    );
    assert!(
        out.contains(
            "0: ro:r ro:w ro:e rw:r rw:w rw:e exe:r exe:w exe:x exe:e none:r none:w none:e"
        ),
        "{out}"
    );
    assert!(
        out.contains("24128: ro:r ro:e rw:r rw:w rw:e exe:r exe:w exe:x exe:e none:e"),
        "{out}"
    );

    // PHP's include pattern: file_exists() (access), open and read, stat.
    let includes = |rounds: u32| {
        let script = format!(
            "perl -MPOSIX -e 'for $r (1..{rounds}) {{ for $i (0..499) {{ $f = \"php/f$i.php\"; \
               POSIX::access($f, 0) or die \"access $f\"; open my $h, \"<\", $f or die; \
               sysread $h, $b, 100; close $h; my @s = stat $f; }} }} print \"ok\\n\"'"
        );
        let out = ok_within(
            run(&vm, &dir, &["-u", "24128"], &script),
            Duration::from_secs(60),
            "include loop",
        );
        assert_eq!(out.trim(), "ok");
    };
    // The first round looks the files up, which the host just created.
    // The first rounds look the files up and take the host's invalidations
    // for their creation (a refetch of each file's attributes, once).
    includes(1);
    thread::sleep(Duration::from_secs(5));
    includes(1);
    let access_before = settled_fs_op_count(&vm, ACCESS);
    let getattr_before = fs_op_count(&vm, GETATTR);
    includes(4);
    let access = settled_fs_op_count(&vm, ACCESS) - access_before;
    let getattr = fs_op_count(&vm, GETATTR) - getattr_before;
    eprintln!("2000 access + read + stat: {access} FUSE_ACCESS, {getattr} FUSE_GETATTR");
    assert!(access < 50, "{access} FUSE_ACCESS for 2000 access(2) calls");
    assert!(
        getattr < 200,
        "{getattr} FUSE_GETATTR for 2000 stats after reads"
    );

    // One request queue per vCPU (and the high-priority queue).
    let out = ok_within(
        run(
            &vm,
            &dir,
            &[],
            "ls /sys/fs/virtiofs/*/mqs | grep -c '^[0-9]'; cat /sys/module/kernel/parameters/idle_poll_ns",
        ),
        Duration::from_secs(30),
        "queues",
    );
    assert_eq!(
        out.split_whitespace().collect::<Vec<_>>(),
        ["3", "200000"],
        "{out}"
    );

    // A ping-pong between two vCPUs: an idle vCPU that polls is woken
    // without an IPI, so far fewer IPIs than wakeups.
    let script = "ipis() { awk '/^ *IPI/ { for (i = 2; i <= 3; i++) s += $i } END { print s }' /proc/interrupts; }; \
        cd /tmp && mkfifo a b || exit 1; \
        taskset -c 1 perl -e 'open R, \"<\", \"a\"; open W, \">\", \"b\"; while (sysread R, $x, 1) { syswrite W, $x }' & \
        before=$(ipis); s=$(date +%s%N); \
        taskset -c 0 perl -e 'open W, \">\", \"a\"; open R, \"<\", \"b\"; for (1..20000) { syswrite W, \"x\"; sysread R, $x, 1 }'; \
        e=$(date +%s%N); wait; echo ipis=$(( $(ipis) - before )) rtt_ns=$(( (e - s) / 20000 ))";
    let out = ok_within(
        run(&vm, &dir, &[], script),
        Duration::from_secs(60),
        "ping-pong",
    );
    let field = |k: &str| -> u64 {
        out.split_whitespace()
            .find_map(|w| w.strip_prefix(k))
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| panic!("no {k} in {out:?}"))
    };
    let (ipis, rtt_ns) = (field("ipis="), field("rtt_ns="));
    eprintln!("20000 cross-vCPU round trips: {ipis} IPIs, {rtt_ns} ns each");
    assert!(ipis < 10_000, "{ipis} IPIs for 40000 wakeups");

    vm.stop();
}
