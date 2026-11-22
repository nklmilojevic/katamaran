//! The order the guest stops in when the host asks it to power off, each
//! step bounded so the whole sequence fits the host's wait:
//!
//! 1. stop running containers through the Docker API (`t` seconds each, in
//!    parallel);
//! 2. stop the NFS server;
//! 3. stop dockerd, then containerd (SIGTERM, then SIGKILL on timeout);
//! 4. SIGTERM every remaining process, then SIGKILL;
//! 5. sync and unmount the data disk, so btrfs is clean;
//! 6. power off (the caller).

use std::fs;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::thread;
use std::time::{Duration, Instant};

use super::nfs::Nfsd;
use super::service::{DOCKER_SOCKET, DockerStack};
use super::{DATA_MOUNTS, sys};

/// Seconds Docker gives each container between SIGTERM and SIGKILL.
const CONTAINER_STOP_SECS: u64 = 8;
const CONTAINERS_TIMEOUT: Duration = Duration::from_secs(CONTAINER_STOP_SECS + 2);
/// With dockerd's 10 s and containerd's 5 s, the worst case of the whole
/// sequence is about 30 s; the host waits longer than that.
const PROCESSES_TIMEOUT: Duration = Duration::from_secs(2);

pub fn run(docker: Option<(&DockerStack, &Nfsd)>, system: bool) {
    let t = Instant::now();
    // Machines halt in parallel with the containers; each has
    // `lxc::STOP_SECS` before it is killed.
    let k8s_stop = match thread::Builder::new()
        .name("shutdown-k8s".into())
        .spawn(|| {
            let t = Instant::now();
            super::k8s::stop_for_shutdown();
            t.elapsed()
        }) {
        Ok(h) => Some(h),
        Err(e) => {
            super::log(&format!(
                "shutdown: spawn k8s stop thread failed ({e}); stopping inline"
            ));
            let t = Instant::now();
            super::k8s::stop_for_shutdown();
            super::log(&format!(
                "shutdown: k8s stopped in {} ms",
                t.elapsed().as_millis()
            ));
            None
        }
    };
    let machines = thread::spawn(|| {
        let t = Instant::now();
        let n = super::machines::stop_all();
        (n, t.elapsed())
    });
    if let Some((_, nfs)) = docker {
        step("containers", || {
            let n = stop_containers(CONTAINERS_TIMEOUT);
            format!("{n} stopped")
        });
        nfs.stop();
    }
    if let Some(h) = k8s_stop
        && let Ok(took) = h.join()
    {
        super::log(&format!("shutdown: k8s stopped in {} ms", took.as_millis()));
    }
    if let Ok((n, took)) = machines.join() {
        super::log(&format!(
            "shutdown: machines: {n} stopped in {} ms",
            took.as_millis()
        ));
    }
    if let Some((stack, _)) = docker {
        stack.stop();
    }
    if system {
        step("processes", || {
            let left = stop_processes(PROCESSES_TIMEOUT);
            format!("{left} killed")
        });
        step("data disk", || {
            unsafe { libc::sync() };
            unmount_data()
        });
    }
    super::log(&format!("shutdown: done in {} ms", t.elapsed().as_millis()));
}

fn step(name: &str, f: impl FnOnce() -> String) {
    let t = Instant::now();
    let what = f();
    super::log(&format!(
        "shutdown: {name}: {what} in {} ms",
        t.elapsed().as_millis()
    ));
}

/// Stop every running container in parallel. Returns how many were asked.
fn stop_containers(timeout: Duration) -> usize {
    let Some(list) = docker_request("GET /containers/json") else {
        return 0;
    };
    let ids = container_ids(&list);
    let deadline = Instant::now() + timeout;
    let workers: Vec<_> = ids
        .iter()
        .map(|id| {
            let req = format!("POST /containers/{id}/stop?t={CONTAINER_STOP_SECS}");
            thread::spawn(move || docker_request(&req))
        })
        .collect();
    for w in workers {
        while !w.is_finished() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        if w.is_finished() {
            let _ = w.join();
        }
    }
    ids.len()
}

/// A request without a body over the Docker socket. HTTP/1.0 keeps the
/// response unchunked, so the body can be scanned as is.
fn docker_request(request_line: &str) -> Option<String> {
    let mut s = UnixStream::connect(DOCKER_SOCKET).ok()?;
    s.set_read_timeout(Some(CONTAINERS_TIMEOUT)).ok()?;
    write!(s, "{request_line} HTTP/1.0\r\nHost: docker\r\n\r\n").ok()?;
    let mut resp = String::new();
    s.read_to_string(&mut resp).ok()?;
    Some(resp)
}

/// The `"Id"` of each container in a `/containers/json` response.
fn container_ids(resp: &str) -> Vec<String> {
    resp.split("\"Id\":\"")
        .skip(1)
        .filter_map(|rest| rest.split('"').next())
        .filter(|id| !id.is_empty() && id.bytes().all(|b| b.is_ascii_hexdigit()))
        .map(str::to_string)
        .collect()
}

/// SIGTERM everything but PID 1, wait for user processes to go, then
/// SIGKILL what is left. Returns how many had to be killed.
fn stop_processes(timeout: Duration) -> usize {
    unsafe { libc::kill(-1, libc::SIGTERM) };
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if user_processes() == 0 {
            return 0;
        }
        thread::sleep(Duration::from_millis(20));
    }
    let left = user_processes();
    unsafe { libc::kill(-1, libc::SIGKILL) };
    let deadline = Instant::now() + Duration::from_secs(1);
    while user_processes() > 0 && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    left
}

/// Live user-space processes other than PID 1. Kernel threads and zombies
/// have an empty command line.
fn user_processes() -> usize {
    let Ok(dir) = fs::read_dir("/proc") else {
        return 0;
    };
    dir.filter_map(Result::ok)
        .filter_map(|e| e.file_name().to_str()?.parse::<u32>().ok())
        .filter(|&pid| pid > 1)
        .filter(|pid| fs::read(format!("/proc/{pid}/cmdline")).is_ok_and(|c| !c.is_empty()))
        .count()
}

/// Unmount the data disk's subvolumes. If one stays busy, remount the file
/// system read-only, which still writes everything out.
fn unmount_data() -> String {
    let mut busy = Vec::new();
    for (_, target) in DATA_MOUNTS.iter().rev() {
        if let Err(e) = sys::umount(target) {
            super::log(&format!("{e}"));
            busy.push(*target);
        }
    }
    if let Some(target) = busy.first() {
        let ro = libc::MS_REMOUNT | libc::MS_RDONLY;
        if let Err(e) = sys::mount("none", target, "btrfs", ro, None) {
            return format!("{} busy, read-only remount failed: {e}", busy.join(", "));
        }
        return format!("{} busy, remounted read-only", busy.join(", "));
    }
    "unmounted".into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_container_ids() {
        let resp = "HTTP/1.0 200 OK\r\nContent-Type: application/json\r\n\r\n\
            [{\"Id\":\"8dfafdbc3a40\",\"Names\":[\"/a\"]},{\"Id\":\"9cd87474be90\",\"Image\":\"x\"}]";
        assert_eq!(container_ids(resp), vec!["8dfafdbc3a40", "9cd87474be90"]);
        assert!(container_ids("HTTP/1.0 200 OK\r\n\r\n[]").is_empty());
        assert!(container_ids("[{\"Id\":\"../etc\"}]").is_empty());
    }
}
