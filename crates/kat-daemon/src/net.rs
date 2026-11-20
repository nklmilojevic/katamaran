//! Guest networking: gvproxy (containers/gvisor-tap-vsock) as the userspace
//! network stack behind the guest's virtio-net device, and automatic port
//! forwards from the Mac's 127.0.0.1 to listeners the guest reports.
//!
//! gvproxy runs in the VM process when `libgvproxy.dylib` is built (frames
//! then pass by function call, see kat-vmm's inproc.rs), or as its own process
//! behind a datagram socket. Its services API is the same either way.
//!
//! Every gvproxy flag and API path used here was checked against its source at
//! the tag `cargo xtask gvproxy` builds (v0.9.0): cmd/gvproxy/config.go for the
//! flags, pkg/services/forwarder/ports.go for `/forwarder/{expose,unexpose}`,
//! which the `-services` endpoint serves under `/services`.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::net::Ipv4Addr;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use kat_proto::PortProto;
use kat_vmm::{InProcess, Network};

/// gvproxy's default subnet, passed explicitly so guest and stack agree.
const SUBNET: &str = "192.168.127.0/24";
const GATEWAY: Ipv4Addr = Ipv4Addr::new(192, 168, 127, 1);
const GUEST: Ipv4Addr = Ipv4Addr::new(192, 168, 127, 2);
/// gvproxy NATs this address to the Mac's 127.0.0.1 (its default NAT table).
const HOST: Ipv4Addr = Ipv4Addr::new(192, 168, 127, 254);
/// The MAC gvproxy's default DHCP lease is bound to.
const GUEST_MAC: [u8; 6] = [0x5a, 0x94, 0xef, 0xe4, 0x0c, 0xee];
/// Largest MTU whose frame fits a macOS unixgram datagram as libkrun sizes
/// it (65562 bytes minus the virtio-net header); gvproxy reads 128 KiB.
pub const DEFAULT_MTU: u16 = 65520;
const SUN_PATH_MAX: usize = 104;

/// A running gvproxy and a thread that reports if it dies, or gvproxy's
/// command line for the VM process to run it in-process.
pub struct Gvproxy {
    child: Option<Arc<Mutex<Child>>>,
    in_process: Option<InProcess>,
    stopping: Arc<AtomicBool>,
    watcher: Option<JoinHandle<()>>,
    pub api_socket: PathBuf,
    vfkit_socket: PathBuf,
    token: PathBuf,
    mtu: u16,
}

impl Gvproxy {
    /// Start gvproxy with its sockets in `run_dir`. Returns at once; the VM
    /// process waits for the vfkit socket, so startup overlaps the VM's.
    pub fn start(bin: &Path, run_dir: &Path, log: &Path, mtu: u16) -> anyhow::Result<Gvproxy> {
        if !bin.is_file() {
            bail!(
                "gvproxy not found at {}; build it with `cargo xtask gvproxy`",
                bin.display()
            );
        }
        // Unix socket paths are limited to 104 bytes on macOS.
        let run_dir =
            &fs::canonicalize(run_dir).with_context(|| format!("resolve {}", run_dir.display()))?;
        let pid_file = run_dir.join("gvproxy.pid");
        kill_stale(&pid_file);
        let vfkit_socket = run_dir.join("gvproxy-net.sock");
        let api_socket = run_dir.join("gvproxy-api.sock");
        for p in [&vfkit_socket, &api_socket] {
            if p.as_os_str().len() >= SUN_PATH_MAX {
                bail!("socket path {} is too long for macOS", p.display());
            }
            let _ = fs::remove_file(p);
        }
        // The forwarder API is also served to the guest on the gateway's port
        // 80. A token only we know keeps containers from opening ports on the
        // Mac; the host-side `-services` socket needs none.
        let token = run_dir.join("gvproxy.token");
        write_token(&token)?;

        let log_file = File::create(log).with_context(|| format!("create {}", log.display()))?;
        let child = Command::new(bin)
            .arg("-listen-vfkit")
            .arg(format!("unixgram://{}", vfkit_socket.display()))
            .args(stack_args(&api_socket, &token, mtu))
            .arg("-pid-file")
            .arg(&pid_file)
            .stdin(Stdio::null())
            .stdout(log_file.try_clone()?)
            .stderr(log_file)
            .spawn()
            .with_context(|| format!("spawn {}", bin.display()))?;

        let child = Arc::new(Mutex::new(child));
        let stopping = Arc::new(AtomicBool::new(false));
        let watcher = {
            let (child, stopping, log) = (child.clone(), stopping.clone(), log.to_path_buf());
            thread::Builder::new()
                .name("gvproxy-watch".into())
                .spawn(move || watch(&child, &stopping, &log))?
        };
        Ok(Gvproxy {
            child: Some(child),
            in_process: None,
            stopping,
            watcher: Some(watcher),
            api_socket,
            vfkit_socket,
            token,
            mtu,
        })
    }

    /// gvproxy for the VM process to load from `library` (built with
    /// `cargo xtask gvproxy`) and run in-process: frames then pass by
    /// function call instead of over a socket. Nothing runs until the VM
    /// starts; the services API appears in `run_dir` as with [`Self::start`].
    pub fn in_process(
        library: &Path,
        run_dir: &Path,
        log: &Path,
        mtu: u16,
    ) -> anyhow::Result<Gvproxy> {
        let run_dir =
            &fs::canonicalize(run_dir).with_context(|| format!("resolve {}", run_dir.display()))?;
        kill_stale(&run_dir.join("gvproxy.pid"));
        let api_socket = run_dir.join("gvproxy-api.sock");
        if api_socket.as_os_str().len() >= SUN_PATH_MAX {
            bail!("socket path {} is too long for macOS", api_socket.display());
        }
        let _ = fs::remove_file(&api_socket);
        let token = run_dir.join("gvproxy.token");
        write_token(&token)?;
        let mut args = stack_args(&api_socket, &token, mtu);
        args.extend(["-log-file".into(), log.display().to_string()]);
        Ok(Gvproxy {
            child: None,
            in_process: Some(InProcess {
                library: library.to_path_buf(),
                args,
            }),
            stopping: Arc::new(AtomicBool::new(false)),
            watcher: None,
            api_socket,
            vfkit_socket: PathBuf::new(),
            token,
            mtu,
        })
    }

    pub fn network(&self) -> Network {
        Network {
            in_process: self.in_process.clone(),
            socket: self.vfkit_socket.clone(),
            mac: GUEST_MAC,
            mtu: self.mtu,
            guest_ip: GUEST,
            prefix_len: 24,
            gateway: GATEWAY,
            host_ip: HOST,
        }
    }

    /// SIGTERM (gvproxy removes its sockets and pid file), then SIGKILL.
    pub fn stop(&mut self) {
        self.stopping.store(true, Ordering::SeqCst);
        if let Some(child) = &self.child {
            let mut child = child.lock().unwrap();
            if let Ok(None) = child.try_wait() {
                unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
                let deadline = Instant::now() + Duration::from_secs(5);
                while matches!(child.try_wait(), Ok(None)) && Instant::now() < deadline {
                    thread::sleep(Duration::from_millis(5));
                }
                let _ = child.kill();
                let _ = child.wait();
            }
        }
        if let Some(w) = self.watcher.take() {
            let _ = w.join();
        }
        if self.in_process.is_some() {
            // It went with the VM process, which could not remove it.
            let _ = fs::remove_file(&self.api_socket);
        }
        let _ = fs::remove_file(&self.token);
    }
}

impl Drop for Gvproxy {
    fn drop(&mut self) {
        self.stop();
    }
}

/// gvproxy's network and services options, shared by both modes.
fn stack_args(api_socket: &Path, token: &Path, mtu: u16) -> Vec<String> {
    [
        "-services".into(),
        format!("unix://{}", api_socket.display()),
        "-ssh-port".into(),
        "-1".into(),
        "-mtu".into(),
        mtu.to_string(),
        "-subnet".into(),
        SUBNET.into(),
        "-gatewayIP".into(),
        GATEWAY.to_string(),
        "-deviceIP".into(),
        GUEST.to_string(),
        "-hostIP".into(),
        HOST.to_string(),
        "-api-token-file".into(),
        token.display().to_string(),
    ]
    .into()
}

/// gvproxy cannot be restarted under a running VM: libkrun's datagram socket
/// stays connected to the old one. So a crash is reported, not retried.
fn watch(child: &Mutex<Child>, stopping: &AtomicBool, log: &Path) {
    loop {
        if stopping.load(Ordering::SeqCst) {
            return;
        }
        let status = child.lock().unwrap().try_wait();
        match status {
            Ok(None) => thread::sleep(Duration::from_millis(100)),
            Ok(Some(status)) => {
                if !stopping.load(Ordering::SeqCst) {
                    eprintln!("katamaran: gvproxy exited ({status}); guest networking is down");
                    crate::vm::print_tail("gvproxy", log);
                }
                return;
            }
            Err(e) => {
                eprintln!("katamaran: cannot wait for gvproxy: {e}");
                return;
            }
        }
    }
}

/// A gvproxy left behind by a daemon that was killed still holds its
/// sockets and forwarded ports. Kill it, but only if the pid is a gvproxy.
fn kill_stale(pid_file: &Path) {
    let Some(pid) = fs::read_to_string(pid_file)
        .ok()
        .and_then(|s| s.trim().parse::<i32>().ok())
    else {
        return;
    };
    if pid > 1 && process_name(pid).is_some_and(|n| n == "gvproxy") {
        eprintln!("katamaran: stopping stale gvproxy (pid {pid})");
        unsafe { libc::kill(pid, libc::SIGKILL) };
        for _ in 0..200 {
            if unsafe { libc::kill(pid, 0) } != 0 {
                break;
            }
            thread::sleep(Duration::from_millis(5));
        }
    }
    let _ = fs::remove_file(pid_file);
}

fn process_name(pid: i32) -> Option<String> {
    let mut buf = vec![0u8; libc::PROC_PIDPATHINFO_MAXSIZE as usize];
    let n = unsafe { libc::proc_pidpath(pid, buf.as_mut_ptr().cast(), buf.len() as u32) };
    if n <= 0 {
        return None;
    }
    buf.truncate(n as usize);
    let path = PathBuf::from(String::from_utf8(buf).ok()?);
    Some(path.file_name()?.to_string_lossy().into_owned())
}

/// 32 random bytes as hex, in a 0600 file (gvproxy refuses other modes).
fn write_token(path: &Path) -> anyhow::Result<()> {
    let mut raw = [0u8; 32];
    File::open("/dev/urandom")?.read_exact(&mut raw)?;
    let hex: String = raw.iter().map(|b| format!("{b:02x}")).collect();
    let _ = fs::remove_file(path);
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .and_then(|mut f| f.write_all(hex.as_bytes()))
        .with_context(|| format!("write {}", path.display()))
}

/// What the reconciler needs from gvproxy; a trait so tests can fake it.
pub trait ForwardApi {
    fn expose(&mut self, proto: PortProto, port: u16) -> Result<(), String>;
    fn unexpose(&mut self, proto: PortProto, port: u16) -> Result<(), String>;
}

/// gvproxy's forwarder API over its `-services` Unix socket.
pub struct GvproxyApi {
    pub socket: PathBuf,
    pub guest: Ipv4Addr,
}

impl ForwardApi for GvproxyApi {
    fn expose(&mut self, proto: PortProto, port: u16) -> Result<(), String> {
        let body = format!(
            r#"{{"local":"127.0.0.1:{port}","remote":"{}:{port}","protocol":"{}"}}"#,
            self.guest,
            proto.as_str()
        );
        post(&self.socket, "/services/forwarder/expose", &body)
    }

    fn unexpose(&mut self, proto: PortProto, port: u16) -> Result<(), String> {
        let body = format!(
            r#"{{"local":"127.0.0.1:{port}","protocol":"{}"}}"#,
            proto.as_str()
        );
        post(&self.socket, "/services/forwarder/unexpose", &body)
    }
}

fn post(socket: &Path, path: &str, body: &str) -> Result<(), String> {
    let send = || -> io::Result<String> {
        let mut s = UnixStream::connect(socket)?;
        s.set_read_timeout(Some(Duration::from_secs(5)))?;
        s.write_all(http_post(path, body).as_bytes())?;
        let mut resp = String::new();
        s.read_to_string(&mut resp)?;
        Ok(resp)
    };
    let resp = send().map_err(|e| format!("gvproxy API: {e}"))?;
    parse_response(&resp)
}

fn http_post(path: &str, body: &str) -> String {
    format!(
        "POST {path} HTTP/1.1\r\nHost: gvproxy\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

/// `Ok` on a 200; otherwise the body, which gvproxy fills with the error.
fn parse_response(resp: &str) -> Result<(), String> {
    let status = resp
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse::<u16>().ok());
    match status {
        Some(200) => Ok(()),
        Some(code) => {
            let body = resp.split_once("\r\n\r\n").map_or("", |(_, b)| b).trim();
            Err(if body.is_empty() {
                format!("HTTP {code}")
            } else {
                body.to_string()
            })
        }
        None => Err(format!("malformed response {resp:?}")),
    }
}

/// The host's view of the forwards: which ports are exposed and which the
/// Mac refused. A refused port is reported once and retried only after the
/// guest stops and starts listening on it again.
pub struct Forwards<A: ForwardApi> {
    api: A,
    active: BTreeSet<(PortProto, u16)>,
    refused: BTreeMap<(PortProto, u16), String>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Event {
    Forwarded(PortProto, u16),
    Refused(PortProto, u16, String),
    Removed(PortProto, u16),
    RemoveFailed(PortProto, u16, String),
}

impl<A: ForwardApi> Forwards<A> {
    pub fn new(api: A) -> Self {
        Forwards {
            api,
            active: BTreeSet::new(),
            refused: BTreeMap::new(),
        }
    }

    pub fn add(&mut self, proto: PortProto, port: u16) -> Option<Event> {
        let key = (proto, port);
        if self.active.contains(&key) || self.refused.contains_key(&key) {
            return None;
        }
        match self.api.expose(proto, port) {
            Ok(()) => {
                self.active.insert(key);
                Some(Event::Forwarded(proto, port))
            }
            Err(e) => {
                self.refused.insert(key, e.clone());
                Some(Event::Refused(proto, port, e))
            }
        }
    }

    pub fn remove(&mut self, proto: PortProto, port: u16) -> Option<Event> {
        let key = (proto, port);
        if self.refused.remove(&key).is_some() {
            return None;
        }
        if !self.active.remove(&key) {
            return None;
        }
        Some(match self.api.unexpose(proto, port) {
            Ok(()) => Event::Removed(proto, port),
            Err(e) => Event::RemoveFailed(proto, port, e),
        })
    }

    #[cfg(test)]
    fn active(&self) -> impl Iterator<Item = &(PortProto, u16)> {
        self.active.iter()
    }
}

pub enum PortChange {
    Add(PortProto, u16),
    Remove(PortProto, u16),
}

/// Apply guest port changes on a worker thread, so a slow gvproxy call never
/// holds up the control loop.
pub fn spawn_forwarder(api: GvproxyApi) -> anyhow::Result<Sender<PortChange>> {
    let (tx, rx) = mpsc::channel::<PortChange>();
    thread::Builder::new()
        .name("port-forward".into())
        .spawn(move || {
            let mut fwd = Forwards::new(api);
            for change in rx {
                let event = match change {
                    PortChange::Add(proto, port) => fwd.add(proto, port),
                    PortChange::Remove(proto, port) => fwd.remove(proto, port),
                };
                if let Some(e) = event {
                    report(&e);
                }
            }
        })?;
    Ok(tx)
}

fn report(e: &Event) {
    match e {
        Event::Forwarded(proto, port) => {
            println!("katamaran: forwarding 127.0.0.1:{port}/{}", proto.as_str())
        }
        Event::Refused(proto, port, why) => eprintln!(
            "katamaran: cannot forward 127.0.0.1:{port}/{} ({why}); skipped",
            proto.as_str()
        ),
        Event::Removed(proto, port) => {
            println!(
                "katamaran: stopped forwarding 127.0.0.1:{port}/{}",
                proto.as_str()
            )
        }
        Event::RemoveFailed(proto, port, why) => eprintln!(
            "katamaran: cannot remove forward 127.0.0.1:{port}/{} ({why})",
            proto.as_str()
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct FakeApi {
        busy: BTreeSet<u16>,
        calls: Vec<String>,
    }

    impl ForwardApi for &mut FakeApi {
        fn expose(&mut self, proto: PortProto, port: u16) -> Result<(), String> {
            self.calls.push(format!("expose {port}/{}", proto.as_str()));
            if self.busy.contains(&port) {
                return Err("listen tcp 127.0.0.1: address already in use".into());
            }
            Ok(())
        }

        fn unexpose(&mut self, proto: PortProto, port: u16) -> Result<(), String> {
            self.calls
                .push(format!("unexpose {port}/{}", proto.as_str()));
            Ok(())
        }
    }

    const TCP: PortProto = PortProto::Tcp;

    #[test]
    fn adds_and_removes_forwards_once() {
        let mut api = FakeApi::default();
        let mut f = Forwards::new(&mut api);
        assert_eq!(f.add(TCP, 8080), Some(Event::Forwarded(TCP, 8080)));
        assert_eq!(f.add(TCP, 8080), None);
        assert_eq!(
            f.add(PortProto::Udp, 8080),
            Some(Event::Forwarded(PortProto::Udp, 8080))
        );
        assert_eq!(f.remove(TCP, 8080), Some(Event::Removed(TCP, 8080)));
        assert_eq!(f.remove(TCP, 8080), None);
        assert_eq!(f.remove(TCP, 9999), None);
        assert_eq!(
            f.active().collect::<Vec<_>>(),
            vec![&(PortProto::Udp, 8080)]
        );
        drop(f);
        assert_eq!(
            api.calls,
            ["expose 8080/tcp", "expose 8080/udp", "unexpose 8080/tcp"]
        );
    }

    #[test]
    fn refused_port_is_reported_once_and_retried_after_relisten() {
        let mut api = FakeApi::default();
        api.busy.insert(5000);
        let mut f = Forwards::new(&mut api);
        assert!(matches!(
            f.add(TCP, 5000),
            Some(Event::Refused(TCP, 5000, _))
        ));
        assert_eq!(f.add(TCP, 5000), None);
        // Never exposed, so removing it must not call unexpose.
        assert_eq!(f.remove(TCP, 5000), None);
        assert!(matches!(f.add(TCP, 5000), Some(Event::Refused(..))));
        drop(f);
        assert_eq!(api.calls, ["expose 5000/tcp", "expose 5000/tcp"]);
    }

    #[test]
    fn builds_post_request() {
        let r = http_post("/services/forwarder/expose", "{}");
        assert!(r.starts_with("POST /services/forwarder/expose HTTP/1.1\r\n"));
        assert!(r.contains("\r\nContent-Length: 2\r\n"));
        assert!(r.ends_with("\r\n\r\n{}"));
    }

    #[test]
    fn parses_gvproxy_responses() {
        assert_eq!(
            parse_response("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"),
            Ok(())
        );
        assert_eq!(
            parse_response(
                "HTTP/1.1 500 Internal Server Error\r\nContent-Type: text/plain\r\n\r\nlisten tcp 127.0.0.1:22: bind: permission denied\n"
            ),
            Err("listen tcp 127.0.0.1:22: bind: permission denied".into())
        );
        assert_eq!(
            parse_response("HTTP/1.1 404 Not Found\r\n\r\n"),
            Err("HTTP 404".into())
        );
        assert!(parse_response("").is_err());
    }

    #[test]
    fn in_process_gvproxy_hands_the_vm_its_command_line() {
        let dir = std::env::temp_dir().join(format!("kat-gvp-inproc-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let dir = fs::canonicalize(&dir).unwrap();
        let library = Path::new("/nonexistent/libgvproxy.dylib");
        let log = dir.join("gvproxy.log");
        let mut gv = Gvproxy::in_process(library, &dir, &log, 1500).unwrap();

        let net = gv.network();
        let p = net.in_process.as_ref().expect("in-process network");
        assert_eq!(p.library, library);
        let args = p.args.join(" ");
        for want in [
            format!(
                "-services unix://{}",
                dir.join("gvproxy-api.sock").display()
            ),
            "-mtu 1500".into(),
            format!("-gatewayIP {GATEWAY} -deviceIP {GUEST} -hostIP {HOST}"),
            format!("-api-token-file {}", dir.join("gvproxy.token").display()),
            format!("-log-file {}", log.display()),
        ] {
            assert!(args.contains(&want), "{want:?} missing from {args}");
        }
        for unwanted in ["-listen-vfkit", "-pid-file"] {
            assert!(!args.contains(unwanted), "{unwanted} in {args}");
        }
        assert!(gv.token.is_file());

        // The VM process served the API socket and could not remove it.
        fs::write(&gv.api_socket, "").unwrap();
        gv.stop();
        assert!(!gv.api_socket.exists() && !gv.token.exists());
        fs::remove_dir_all(&dir).unwrap();
    }

    /// Talks to a real gvproxy if one is built: expose a port, see the Mac
    /// listen on it, unexpose it, see it gone.
    #[test]
    fn gvproxy_forwarder_api_round_trip() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let bin = root.join("target/release/gvproxy");
        if !bin.is_file() {
            eprintln!("skipped: no gvproxy at {}", bin.display());
            return;
        }
        let dir = std::env::temp_dir().join(format!("kat-gvp-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let mut gv = Gvproxy::start(&bin, &dir, &dir.join("gvproxy.log"), 1500).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !gv.network().socket.exists() {
            assert!(Instant::now() < deadline, "gvproxy did not start");
            thread::sleep(Duration::from_millis(5));
        }
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);

        let mut api = GvproxyApi {
            socket: gv.api_socket.clone(),
            guest: GUEST,
        };
        api.expose(TCP, port).unwrap();
        assert!(std::net::TcpListener::bind(("127.0.0.1", port)).is_err());
        let again = api.expose(TCP, port).unwrap_err();
        assert!(again.contains("already running"), "{again}");
        api.unexpose(TCP, port).unwrap();
        assert!(std::net::TcpListener::bind(("127.0.0.1", port)).is_ok());
        gv.stop();
        assert!(!gv.api_socket.exists(), "gvproxy left its socket behind");
        fs::remove_dir_all(&dir).unwrap();
    }
}
