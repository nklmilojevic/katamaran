//! The order the guest stops in when the host asks it to power off, each
//! step bounded so the whole sequence fits the host's wait:
//!
//! 1. stop running containers through the Docker API (`t` seconds each, in
//!    parallel);
//! 2. stop the NFS server;
//! 3. stop dockerd, then containerd (SIGTERM, then SIGKILL on timeout);
//! 4. SIGTERM every remaining process, then SIGKILL;
//! 5. detach leftover Kubernetes mounts, then sync and unmount the data
//!    disk, so btrfs is clean;
//! 6. power off (the caller).

use std::fs;
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::path::Path;
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
    // The k8s stop runs on its own thread and is waited for at most
    // `SHUTDOWN_K8S_BUDGET`, so nothing in the guest Kubernetes service (a
    // reset still holding its lock, a slow k3s exit, serial `ctr` cleanup)
    // can delay the Docker stop, process sweep, or data-disk sync.
    let k8s_budget = Instant::now() + super::k8s::SHUTDOWN_K8S_BUDGET;
    let (k8s_tx, k8s_rx) = std::sync::mpsc::channel();
    let k8s_stop = thread::Builder::new()
        .name("shutdown-k8s".into())
        .spawn(move || {
            let t = Instant::now();
            super::k8s::stop_for_shutdown();
            let _ = k8s_tx.send(t.elapsed());
        });
    let machines = thread::Builder::new()
        .name("shutdown-machines".into())
        .spawn(|| {
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
    match k8s_stop {
        Ok(_detached) => {
            match k8s_rx.recv_timeout(k8s_budget.saturating_duration_since(Instant::now())) {
                Ok(took) => {
                    super::log(&format!("shutdown: k8s stopped in {} ms", took.as_millis()));
                }
                Err(_) => {
                    super::k8s::abandon_shutdown_stop();
                    super::log(&format!(
                        "shutdown: k8s stop still running after {} s; cutting off Kubernetes cleanup and continuing",
                        super::k8s::SHUTDOWN_K8S_BUDGET.as_secs()
                    ));
                }
            }
        }
        Err(e) => {
            // Without a thread the stop could not be bounded: leave k3s to the
            // process sweep rather than risk delaying the data-disk sync.
            super::log(&format!(
                "shutdown: spawn k8s stop thread: {e}; leaving k3s to the process sweep"
            ));
            super::k8s::abandon_shutdown_stop();
        }
    }
    match machines {
        Ok(h) => {
            if let Ok((n, took)) = h.join() {
                super::log(&format!(
                    "shutdown: machines: {n} stopped in {} ms",
                    took.as_millis()
                ));
            }
        }
        Err(e) => {
            super::log(&format!("shutdown: spawn machines stop thread: {e}"));
            let t = Instant::now();
            let n = super::machines::stop_all();
            super::log(&format!(
                "shutdown: machines: {n} stopped in {} ms",
                t.elapsed().as_millis()
            ));
        }
    }
    if let Some((stack, _)) = docker {
        stack.stop();
    }
    if system {
        step("processes", || {
            let left = stop_processes(PROCESSES_TIMEOUT);
            format!("{left} killed")
        });
        step("k8s mounts", super::k8s::detach_mounts_for_shutdown);
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
    let deadline = Instant::now() + timeout;
    let Some(list) = docker_request_until("GET /containers/json", deadline) else {
        return 0;
    };
    let ids = container_ids(&list);
    let mut workers = Vec::with_capacity(ids.len());
    let mut fallback_streams = Vec::new();
    for id in &ids {
        let req = format!("POST /containers/{id}/stop?t={CONTAINER_STOP_SECS}");
        match thread::Builder::new()
            .name("shutdown-container".into())
            .spawn(move || docker_request_until(&req, deadline))
        {
            Ok(w) => workers.push(w),
            Err(e) => {
                super::log(&format!("shutdown: spawn container stop thread: {e}"));
                if let Some(stream) = send_docker_request(
                    &format!("POST /containers/{id}/stop?t={CONTAINER_STOP_SECS}"),
                    deadline,
                ) {
                    fallback_streams.push(stream);
                }
            }
        }
    }
    if !fallback_streams.is_empty() {
        drain_streams_until(fallback_streams, deadline);
    }
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

/// Connect to a Unix domain stream socket without blocking past `deadline`.
pub(super) fn connect_unix_until(path: &str, deadline: Instant) -> Option<UnixStream> {
    crate::unix_connect::connect_unix_until(Path::new(path), deadline).ok()
}

/// Send an HTTP/1.0 request without a body over the Docker socket, bounded by
/// `deadline`, and return the open socket so the caller can read or poll its
/// response without exceeding the shared time budget.
fn send_docker_request(request_line: &str, deadline: Instant) -> Option<UnixStream> {
    let mut s = connect_unix_until(DOCKER_SOCKET, deadline)?;
    let rem = deadline.checked_duration_since(Instant::now())?;
    if rem.is_zero() {
        return None;
    }
    s.set_write_timeout(Some(rem)).ok()?;
    write!(s, "{request_line} HTTP/1.0\r\nHost: docker\r\n\r\n").ok()?;
    Some(s)
}

/// A request without a body over the Docker socket, strictly bounded by
/// `deadline` across both write and read I/O. HTTP/1.0 keeps the response
/// unchunked, so the body can be scanned as is.
fn docker_request_until(request_line: &str, deadline: Instant) -> Option<String> {
    let mut s = send_docker_request(request_line, deadline)?;
    let mut resp = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        let rem = deadline.checked_duration_since(Instant::now())?;
        if rem.is_zero() {
            break;
        }
        s.set_read_timeout(Some(rem)).ok()?;
        match s.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => resp.extend_from_slice(&buf[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => return None,
        }
    }
    String::from_utf8(resp).ok()
}

/// Poll and drain fallback stop sockets concurrently until all finish or the
/// shared `deadline` expires.
fn drain_streams_until(mut streams: Vec<UnixStream>, deadline: Instant) {
    let mut buf = [0u8; 4096];
    while !streams.is_empty() {
        let Some(rem) = deadline.checked_duration_since(Instant::now()) else {
            break;
        };
        if rem.is_zero() {
            break;
        }
        let mut pfds: Vec<libc::pollfd> = streams
            .iter()
            .map(|s| libc::pollfd {
                fd: s.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            })
            .collect();
        let timeout_ms = rem.as_millis().min(i32::MAX as u128) as i32;
        let rc = unsafe { libc::poll(pfds.as_mut_ptr(), pfds.len() as libc::nfds_t, timeout_ms) };
        if rc < 0 {
            if std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            break;
        }
        if rc == 0 {
            break;
        }
        let mut i = 0;
        while i < streams.len() {
            if pfds[i].revents != 0 {
                let Some(rem) = deadline.checked_duration_since(Instant::now()) else {
                    return;
                };
                if rem.is_zero() {
                    return;
                }
                let _ = streams[i].set_read_timeout(Some(rem));
                match streams[i].read(&mut buf) {
                    Ok(n) if n > 0 => i += 1,
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => i += 1,
                    _ => {
                        streams.swap_remove(i);
                        pfds.swap_remove(i);
                    }
                }
            } else {
                i += 1;
            }
        }
    }
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

    #[test]
    fn drain_streams_respects_shared_deadline() {
        let (a1, _a2) = UnixStream::pair().unwrap();
        let (b1, _b2) = UnixStream::pair().unwrap();
        let t = Instant::now();
        drain_streams_until(vec![a1, b1], t + Duration::from_millis(60));
        assert!(t.elapsed() < Duration::from_millis(500));
    }

    #[test]
    fn connect_unix_until_connects_and_honors_expired_deadline() {
        let path = std::env::temp_dir().join(format!("kat-shutdown-{}.sock", std::process::id()));
        let _ = fs::remove_file(&path);
        let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
        let path_str = path.to_str().unwrap();
        assert!(connect_unix_until(path_str, Instant::now()).is_none());
        let stream = connect_unix_until(path_str, Instant::now() + Duration::from_secs(1));
        assert!(stream.is_some());
        drop(stream);
        drop(listener);
        let _ = fs::remove_file(&path);
    }
}
