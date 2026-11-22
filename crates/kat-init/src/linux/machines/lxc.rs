//! LXC: each machine is a privileged system container with its own init,
//! network namespace (a veth on the machine bridge) and cgroup, and the
//! Mac's /Users bind-mounted at the same path.

use std::fs;
use std::io;
use std::net::Ipv4Addr;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use super::super::service::Reaper;
use super::{bridge, cmd, provision, store};

const START_TIMEOUT: Duration = Duration::from_secs(30);
/// What a clean halt gets before lxc-stop kills the machine.
pub const STOP_SECS: u64 = 8;

/// The signal that makes the machine's init power off cleanly.
pub fn halt_signal(rootfs: &Path) -> &'static str {
    let init_path = provision::resolve_in_rootfs(rootfs, "sbin")
        .map(|sbin| sbin.join("init"))
        .unwrap_or_else(|_| rootfs.join("sbin/init"));
    let init = fs::read_link(init_path)
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_default();
    if init.contains("systemd") {
        "SIGRTMIN+3"
    } else if init.contains("busybox") {
        // busybox init: SIGUSR2 is poweroff (SIGUSR1 halt, SIGTERM reboot).
        "SIGUSR2"
    } else {
        "SIGPWR"
    }
}

pub fn config(name: &str, ip: Ipv4Addr, mtu: u32, halt: &str) -> String {
    let rootfs = store::rootfs(name);
    format!(
        "# Written by katamaran.
lxc.include = /usr/share/lxc/config/common.conf
lxc.arch = linux64
lxc.uts.name = {name}
lxc.rootfs.path = dir:{rootfs}
lxc.signal.halt = {halt}
lxc.mount.auto =
lxc.mount.auto = proc:mixed sys:mixed cgroup:rw:force
lxc.mount.entry = /Users Users none rbind,create=dir,optional 0 0
lxc.cgroup2.devices.allow = c 10:200 rwm
lxc.net.0.type = veth
lxc.net.0.link = {bridge}
lxc.net.0.flags = up
lxc.net.0.name = eth0
lxc.net.0.hwaddr = {mac}
lxc.net.0.mtu = {mtu}
lxc.log.file = {log}
lxc.log.level = WARN
",
        rootfs = rootfs.display(),
        bridge = bridge::BRIDGE,
        mac = store::mac(ip),
        log = store::dir(name).join("lxc.log").display(),
    )
}

/// `/etc/hostname` and `/etc/hosts`, the two files the image's LXC
/// templates fill in.
pub fn write_identity(rootfs: &Path, name: &str) -> io::Result<()> {
    let hostname = provision::resolve_in_rootfs(rootfs, "etc/hostname")?;
    let hosts = provision::resolve_in_rootfs(rootfs, "etc/hosts")?;
    fs::write(hostname, format!("{name}\n"))?;
    fs::write(
        hosts,
        format!(
            "127.0.0.1\tlocalhost\n127.0.1.1\t{name}\n::1\tlocalhost ip6-localhost ip6-loopback\n\
             ff02::1\tip6-allnodes\nff02::2\tip6-allrouters\n"
        ),
    )
}

fn lxc(tool: &str, name: &str) -> Command {
    let mut c = Command::new(tool);
    c.args(["-n", name, "-P", store::DIR]);
    c
}

pub fn start(reaper: &Reaper, name: &str) -> io::Result<()> {
    cmd::run(reaper, lxc("lxc-start", name).arg("-d"), START_TIMEOUT)
        .map_err(|e| io::Error::other(format!("{e} (see {})", log_hint(name))))?;
    cmd::run(
        reaper,
        lxc("lxc-wait", name).args(["-s", "RUNNING", "-t", "10"]),
        START_TIMEOUT,
    )?;
    Ok(())
}

/// Halt cleanly, kill after [`STOP_SECS`]; `kill` skips the halt.
pub fn stop(reaper: &Reaper, name: &str, kill: bool) -> io::Result<()> {
    let mut c = lxc("lxc-stop", name);
    if kill {
        c.arg("-k");
    } else {
        c.args(["-t", &STOP_SECS.to_string()]);
    }
    match cmd::run_status(reaper, &mut c, Duration::from_secs(STOP_SECS + 10))? {
        // 2: the container was not running.
        (0 | 2, _) => Ok(()),
        (_, out) => Err(io::Error::other(format!("lxc-stop: {}", out.trim()))),
    }
}

/// (state, init pid) as lxc-info reports them; `STOPPED` and no pid when
/// lxc knows nothing about it.
pub fn info(reaper: &Reaper, name: &str) -> (String, Option<i32>) {
    let out = cmd::run_status(
        reaper,
        lxc("lxc-info", name).args(["-s", "-p", "-H"]),
        Duration::from_secs(5),
    );
    match out {
        Ok((0, out)) => parse_info(&out),
        _ => ("STOPPED".into(), None),
    }
}

/// `lxc-info -s -p -H` prints `State: RUNNING` and `PID: 123` (older
/// releases print the bare values).
pub fn parse_info(out: &str) -> (String, Option<i32>) {
    let (mut state, mut pid) = (None, None);
    for (i, line) in out
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .enumerate()
    {
        let (key, value) = match line.split_once(':') {
            Some((k, v)) => (k.trim().to_ascii_lowercase(), v.trim()),
            None => ((if i == 0 { "state" } else { "pid" }).to_string(), line),
        };
        match key.as_str() {
            "state" => state = Some(value.to_ascii_uppercase()),
            "pid" => pid = value.parse().ok().filter(|p: &i32| *p > 0),
            _ => {}
        }
    }
    (state.unwrap_or_else(|| "STOPPED".into()), pid)
}

fn log_hint(name: &str) -> String {
    let log = store::dir(name).join("lxc.log");
    let tail = fs::read_to_string(&log).unwrap_or_default();
    let last: Vec<&str> = tail.lines().rev().take(3).collect();
    if last.is_empty() {
        log.display().to_string()
    } else {
        last.into_iter().rev().collect::<Vec<_>>().join(" | ")
    }
}

/// The lxc-attach command line that runs `script` as `user` through a
/// login `su`, with `env` exported first.
pub fn attach_args(name: &str, user: &str, term: Option<&str>, script: &str) -> Vec<String> {
    let mut a: Vec<String> = ["lxc-attach", "-n", name, "-P", store::DIR, "--clear-env"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    a.extend(["-v".into(), format!("PATH={}", cmd::PATH)]);
    if let Some(t) = term {
        a.extend(["-v".into(), format!("TERM={t}")]);
    }
    a.extend(
        ["--", "su", "-l", "-c", script, user]
            .iter()
            .map(|s| s.to_string()),
    );
    a
}

/// Quote for a POSIX shell.
pub fn quote(s: &str) -> String {
    if !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_./=:@%+,".contains(&b))
    {
        return s.to_string();
    }
    format!("'{}'", s.replace('\'', r"'\''"))
}

fn is_valid_env_name(k: &str) -> bool {
    let mut bytes = k.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == b'_')
        && bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

/// The script `su -c` runs: export the environment, change to `cwd` if it
/// exists (else stay in the home `su -l` chose), then run `argv`, or the
/// login shell.
pub fn script(cwd: &str, env: &[(String, String)], argv: &[String]) -> String {
    let mut s = String::new();
    for (k, v) in env {
        if is_valid_env_name(k) {
            s.push_str(&format!("export {k}={}; ", quote(v)));
        }
    }
    if !cwd.is_empty() {
        s.push_str(&format!("cd -- {} 2>/dev/null; ", quote(cwd)));
    }
    if argv.is_empty() {
        s.push_str("exec \"$SHELL\" -l");
    } else {
        let cmd: Vec<String> = argv.iter().map(|a| quote(a)).collect();
        s.push_str(&format!("exec -- {}", cmd.join(" ")));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_a_config() {
        let c = config("dev", Ipv4Addr::new(198, 19, 249, 3), 65520, "SIGRTMIN+3");
        assert!(c.contains("lxc.uts.name = dev\n"));
        assert!(c.contains("lxc.rootfs.path = dir:/var/lib/katamaran/machines/dev/rootfs\n"));
        assert!(c.contains("lxc.net.0.hwaddr = 02:6b:61:13:f9:03\n"));
        assert!(c.contains("lxc.signal.halt = SIGRTMIN+3\n"));
        assert!(c.contains("lxc.mount.entry = /Users Users none rbind,create=dir,optional 0 0"));
    }

    #[test]
    fn parses_lxc_info() {
        assert_eq!(
            parse_info("RUNNING\n1234\n"),
            ("RUNNING".into(), Some(1234))
        );
        assert_eq!(
            parse_info("State:          RUNNING\nPID:            811\n"),
            ("RUNNING".into(), Some(811))
        );
        assert_eq!(parse_info("STOPPED\n"), ("STOPPED".into(), None));
        assert_eq!(parse_info(""), ("STOPPED".into(), None));
    }

    #[test]
    fn builds_scripts() {
        assert_eq!(quote("abc"), "abc");
        assert_eq!(quote("a b"), "'a b'");
        assert_eq!(quote("it's"), r"'it'\''s'");
        assert_eq!(quote(""), "''");
        let s = script(
            "/Users/me/my app",
            &[
                ("LANG".into(), "C.UTF-8".into()),
                ("_OK_1".into(), "y".into()),
                ("1BAD".into(), "z".into()),
                ("BAD;".into(), "x".into()),
            ],
            &["echo".into(), "$HOME".into()],
        );
        assert_eq!(
            s,
            "export LANG=C.UTF-8; export _OK_1=y; cd -- '/Users/me/my app' 2>/dev/null; exec -- echo '$HOME'"
        );
        assert_eq!(
            script("-dir", &[], &["-cmd".into()]),
            "cd -- -dir 2>/dev/null; exec -- -cmd"
        );
        assert_eq!(script("", &[], &[]), "exec \"$SHELL\" -l");
    }
}
