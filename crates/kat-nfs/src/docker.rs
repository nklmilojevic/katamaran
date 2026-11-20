//! Running containers from the guest's Docker API, and their root
//! filesystems as `/proc/<pid>/root` of each container's init process: the
//! merged rootfs plus the container's own mounts, whatever the storage
//! driver.

use std::io::{self, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::layout::Containers;

/// How long a listing is reused: Finder and `ls` ask several times per
/// directory view.
const CACHE: Duration = Duration::from_millis(250);

type Cached = Option<(Instant, Vec<(String, PathBuf)>)>;

pub struct DockerContainers {
    pub socket: PathBuf,
    /// containerd's runc v2 task directory: `<id>/init.pid` per container.
    pub tasks: PathBuf,
    cache: Mutex<Cached>,
}

impl Default for DockerContainers {
    fn default() -> Self {
        DockerContainers {
            socket: "/run/docker.sock".into(),
            tasks: "/run/containerd/io.containerd.runtime.v2.task/moby".into(),
            cache: Mutex::new(None),
        }
    }
}

impl Containers for DockerContainers {
    fn list(&self) -> Vec<(String, PathBuf)> {
        let mut cache = self.cache.lock().unwrap();
        if let Some((at, list)) = &*cache
            && at.elapsed() < CACHE
        {
            return list.clone();
        }
        let list = self.fetch().unwrap_or_default();
        *cache = Some((Instant::now(), list.clone()));
        list
    }
}

impl DockerContainers {
    fn fetch(&self) -> io::Result<Vec<(String, PathBuf)>> {
        let body = http_get(&self.socket, "/containers/json")?;
        Ok(parse_containers(&body)
            .into_iter()
            .filter_map(|(id, name)| {
                let pid = std::fs::read_to_string(self.tasks.join(&id).join("init.pid")).ok()?;
                let pid: u32 = pid.trim().parse().ok()?;
                Some((name, PathBuf::from(format!("/proc/{pid}/root"))))
            })
            .collect())
    }
}

/// (id, name) of each container in a `/containers/json` answer.
pub fn parse_containers(body: &[u8]) -> Vec<(String, String)> {
    let Ok(serde_json::Value::Array(list)) = serde_json::from_slice(body) else {
        return Vec::new();
    };
    list.iter()
        .filter_map(|c| {
            let id = c.get("Id")?.as_str()?;
            let name = c.get("Names")?.as_array()?.first()?.as_str()?;
            let name = name.strip_prefix('/').unwrap_or(name);
            let id_ok = !id.is_empty() && id.bytes().all(|b| b.is_ascii_hexdigit());
            id_ok.then(|| (id.to_string(), name.to_string()))
        })
        .collect()
}

/// A minimal HTTP/1.0 GET over a Unix socket (no keep-alive, no chunking).
pub fn http_get(socket: &std::path::Path, path: &str) -> io::Result<Vec<u8>> {
    let mut s = UnixStream::connect(socket)?;
    s.set_read_timeout(Some(Duration::from_secs(5)))?;
    s.write_all(format!("GET {path} HTTP/1.0\r\nHost: docker\r\n\r\n").as_bytes())?;
    let mut resp = Vec::new();
    s.take(16 << 20).read_to_end(&mut resp)?;
    split_response(&resp)
}

fn split_response(resp: &[u8]) -> io::Result<Vec<u8>> {
    let end = resp
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| io::Error::other("no HTTP header end"))?;
    let status = &resp[..end];
    if !(status.starts_with(b"HTTP/1.0 200") || status.starts_with(b"HTTP/1.1 200")) {
        let line = status.split(|&b| b == b'\r').next().unwrap_or_default();
        return Err(io::Error::other(String::from_utf8_lossy(line).into_owned()));
    }
    Ok(resp[end + 4..].to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_container_list() {
        let body = br#"[{"Id":"abc123","Names":["/web"],"State":"running"},
            {"Id":"def","Names":["/db","/web/db"]},{"Id":"../x","Names":["/evil"]},{"Names":["/noid"]}]"#;
        assert_eq!(
            parse_containers(body),
            vec![
                ("abc123".to_string(), "web".to_string()),
                ("def".to_string(), "db".to_string())
            ]
        );
        assert!(parse_containers(b"{}").is_empty());
        assert!(parse_containers(b"not json").is_empty());
    }

    #[test]
    fn splits_http_responses() {
        assert_eq!(
            split_response(b"HTTP/1.0 200 OK\r\nA: b\r\n\r\n[1]").unwrap(),
            b"[1]"
        );
        let e = split_response(b"HTTP/1.0 500 Internal\r\n\r\nx").unwrap_err();
        assert!(e.to_string().contains("500"));
        assert!(split_response(b"garbage").is_err());
    }

    #[test]
    fn lists_containers_from_a_fake_daemon() {
        let d = std::env::temp_dir().join(format!("kat-nfs-docker-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(d.join("tasks/abc")).unwrap();
        std::fs::write(d.join("tasks/abc/init.pid"), "4242\n").unwrap();
        let sock = d.join("d.sock");
        let listener = std::os::unix::net::UnixListener::bind(&sock).unwrap();
        let t = std::thread::spawn(move || {
            let (mut c, _) = listener.accept().unwrap();
            let mut req = [0u8; 256];
            let n = c.read(&mut req).unwrap();
            assert!(req[..n].starts_with(b"GET /containers/json HTTP/1.0"));
            c.write_all(b"HTTP/1.0 200 OK\r\n\r\n[{\"Id\":\"abc\",\"Names\":[\"/web\"]},{\"Id\":\"fed\",\"Names\":[\"/gone\"]}]")
                .unwrap();
        });
        let dc = DockerContainers {
            socket: sock,
            tasks: d.join("tasks"),
            cache: Mutex::new(None),
        };
        let want = vec![("web".to_string(), PathBuf::from("/proc/4242/root"))];
        assert_eq!(dc.list(), want);
        t.join().unwrap();
        // Cached: the fake daemon is gone, the answer is the same.
        assert_eq!(dc.list(), want);
        std::fs::remove_dir_all(&d).unwrap();
    }
}
