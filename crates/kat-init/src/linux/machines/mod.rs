//! Linux machines: full distributions (systemd, OpenRC, …) in LXC system
//! containers, each on its own btrfs subvolume of the data disk, served to
//! the host on [`kat_proto::MACHINE_PORT`] (see [`kat_proto::machine`]).
//!
//! Nothing here runs until a machine is used: the vsock listener is the only
//! cost of a VM without machines. Machines that were running when the VM
//! went down start again once Docker is up, so they never delay it.

mod bridge;
mod cmd;
mod exec;
mod lxc;
mod provision;
mod store;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, File};
use std::io::{self, Write};
use std::net::Ipv4Addr;
use std::os::unix::net::UnixStream;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use kat_proto::MACHINE_PORT;
use kat_proto::machine::{self, CreateSpec, MachineInfo, MachineMsg};

use super::service::{Reaper, describe_status};
use super::sys;

const PORT_INTERVAL: Duration = Duration::from_millis(500);
const FIRST_BOOT_TIMEOUT: Duration = Duration::from_secs(300);

pub struct Machines {
    reaper: Reaper,
    dns: Option<Ipv4Addr>,
    bridge: Mutex<Option<bridge::Bridge>>,
    /// One lifecycle operation per machine at a time.
    locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    /// Creates hold this while they pick an address and a name.
    alloc: Mutex<()>,
    /// Running machines: name → (init pid, address).
    running: Mutex<BTreeMap<String, (i32, Ipv4Addr)>>,
}

/// Ports the machines listen on that the host forwards (see
/// [`forwarded_ports`]).
static FORWARDED: Mutex<BTreeSet<u16>> = Mutex::new(BTreeSet::new());
static SERVICE: OnceLock<Arc<Machines>> = OnceLock::new();

/// Machine ports currently DNATed from eth0; the port watcher reports them
/// to the host along with the root namespace's.
pub fn forwarded_ports() -> BTreeSet<u16> {
    FORWARDED.lock().unwrap().clone()
}

/// Serve the machine port and start the machines marked for autostart.
pub fn start(reaper: Reaper, dns: Option<Ipv4Addr>) -> io::Result<Arc<Machines>> {
    fs::create_dir_all(store::DIR)?;
    let m = Arc::new(Machines {
        reaper,
        dns,
        bridge: Mutex::new(None),
        locks: Mutex::new(HashMap::new()),
        alloc: Mutex::new(()),
        running: Mutex::new(BTreeMap::new()),
    });
    let _ = SERVICE.set(m.clone());
    let listener = sys::vsock_listen(MACHINE_PORT)?;
    {
        let m = m.clone();
        thread::Builder::new()
            .name("machines".into())
            .spawn(move || {
                loop {
                    match sys::accept(&listener) {
                        Ok(fd) => {
                            let m = m.clone();
                            thread::spawn(move || m.serve(UnixStream::from(fd)));
                        }
                        Err(e) => {
                            log(&format!("accept on vsock {MACHINE_PORT}: {e}"));
                            return;
                        }
                    }
                }
            })?;
    }
    {
        let m = m.clone();
        thread::spawn(move || m.autostart());
    }
    {
        let m = m.clone();
        thread::spawn(move || m.watch_ports());
    }
    Ok(m)
}

/// Stop every running machine in parallel (VM shutdown). Their autostart
/// marks stay, so they come back with the VM.
pub fn stop_all() -> usize {
    let Some(m) = SERVICE.get() else { return 0 };
    let names: Vec<String> = m.running.lock().unwrap().keys().cloned().collect();
    let workers: Vec<_> = names
        .iter()
        .map(|n| {
            let (m, n) = (m.clone(), n.clone());
            thread::spawn(move || {
                if let Err(e) = lxc::stop(&m.reaper, &n, false) {
                    log(&format!("stop {n}: {e}"));
                }
            })
        })
        .collect();
    for w in workers {
        let _ = w.join();
    }
    names.len()
}

fn log(msg: &str) {
    eprintln!("kat-init: machines: {msg}");
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

impl Machines {
    fn lock_for(&self, name: &str) -> Arc<Mutex<()>> {
        self.locks
            .lock()
            .unwrap()
            .entry(name.to_string())
            .or_default()
            .clone()
    }

    /// An empty name is the default machine.
    fn resolve(&self, name: &str) -> Result<String, String> {
        if name.is_empty() {
            return store::default_machine().ok_or_else(|| {
                "no default machine; create one with `katamaran machine create <name>`".into()
            });
        }
        if !machine::name_ok(name) || !store::exists(name) {
            return Err(format!("no machine named {name:?}"));
        }
        Ok(name.to_string())
    }

    fn serve(self: Arc<Self>, mut conn: UnixStream) {
        let req = match machine::read(&mut conn) {
            Ok(Some(m)) => m,
            Ok(None) => return,
            Err(e) => {
                let _ = machine::write(&mut conn, &MachineMsg::Error(e.to_string()));
                return;
            }
        };
        let mut progress = conn.try_clone().ok();
        let mut say = |s: &str| {
            if let Some(c) = progress.as_mut() {
                let _ = machine::write(c, &MachineMsg::Progress(s.into()));
            }
        };
        let result: Result<Option<MachineMsg>, String> = match req {
            MachineMsg::List => Ok(Some(MachineMsg::Machines(
                store::names().iter().map(|n| self.info(n)).collect(),
            ))),
            MachineMsg::Info { name } => self
                .resolve(&name)
                .map(|n| Some(MachineMsg::Machines(vec![self.info(&n)]))),
            MachineMsg::Create(spec) => {
                let r = self.create(&spec, &mut conn, &mut say);
                r.map(|()| Some(MachineMsg::Done))
            }
            MachineMsg::Start { name } => self
                .resolve(&name)
                .and_then(|n| self.start_machine(&n, true))
                .map(|()| Some(MachineMsg::Done)),
            MachineMsg::Stop { name } => self
                .resolve(&name)
                .and_then(|n| self.stop_machine(&n))
                .map(|()| Some(MachineMsg::Done)),
            MachineMsg::Delete { name } => self
                .resolve(&name)
                .and_then(|n| self.delete(&n))
                .map(|()| Some(MachineMsg::Done)),
            MachineMsg::SetDefault { name } => self
                .resolve(&name)
                .and_then(|n| store::set_default(Some(&n)).map_err(|e| e.to_string()))
                .map(|()| Some(MachineMsg::Done)),
            MachineMsg::Exec(mut spec) => match self.prepare(&spec.machine) {
                Err(e) => Err(e),
                Ok(name) => {
                    let meta = store::load(&name).unwrap_or_default();
                    let user = match spec.user.as_str() {
                        "" => meta.user.clone(),
                        u => u.to_string(),
                    };
                    if !machine::user_ok(&user) {
                        Err(format!("invalid user {user:?}"))
                    } else {
                        spec.machine = name;
                        match exec::exec(&self.reaper, conn.try_clone().unwrap(), &spec, &user) {
                            Ok(()) => Ok(None),
                            Err(e) => Err(format!("exec: {e}")),
                        }
                    }
                }
            },
            MachineMsg::Connect { name, port } => match self.prepare(&name) {
                Err(e) => Err(e),
                Ok(name) => {
                    let target = self.running.lock().unwrap().get(&name).copied();
                    match target {
                        Some((pid, ip)) => {
                            let _ = exec::connect(conn.try_clone().unwrap(), pid, ip, port);
                            Ok(None)
                        }
                        None => Err(format!("{name} has no address")),
                    }
                }
            },
            other => Err(format!("unexpected request {other:?}")),
        };
        let reply = match result {
            Ok(Some(m)) => m,
            Ok(None) => return,
            Err(e) => MachineMsg::Error(e),
        };
        let _ = machine::write(&mut conn, &reply);
    }

    /// Resolve a machine for exec/connect and start it if it is stopped.
    fn prepare(&self, name: &str) -> Result<String, String> {
        let name = self.resolve(name)?;
        if !self.is_running(&name) {
            self.start_machine(&name, true)?;
        }
        Ok(name)
    }

    fn is_running(&self, name: &str) -> bool {
        let pid = self.running.lock().unwrap().get(name).map(|r| r.0);
        match pid {
            Some(pid) if unsafe { libc::kill(pid, 0) } == 0 => true,
            Some(_) => {
                self.running.lock().unwrap().remove(name);
                false
            }
            None => false,
        }
    }

    fn info(&self, name: &str) -> MachineInfo {
        let meta = store::load(name).unwrap_or_default();
        let (state, _) = lxc::info(&self.reaper, name);
        let running = state == "RUNNING";
        if !running {
            self.running.lock().unwrap().remove(name);
        }
        MachineInfo {
            name: name.into(),
            distro: meta.distro,
            release: meta.release,
            image: meta.image,
            state: state.to_lowercase(),
            ip: match (running, meta.ip) {
                (true, Some(ip)) => ip.to_string(),
                _ => String::new(),
            },
            user: meta.user,
            uid: meta.uid,
            default: store::default_machine().as_deref() == Some(name),
            autostart: meta.autostart,
            created_unix: meta.created_unix,
        }
    }

    /// The bridge and dnsmasq, set up on first use, with the DHCP table
    /// refreshed.
    fn network(&self) -> io::Result<u32> {
        let mut b = self.bridge.lock().unwrap();
        let hosts: Vec<(String, Ipv4Addr)> = store::names()
            .into_iter()
            .filter_map(|n| Some((n.clone(), store::load(&n).ok()?.ip?)))
            .collect();
        if b.is_none() {
            *b = Some(bridge::Bridge::up(&self.reaper, self.dns, &hosts)?);
        } else {
            b.as_ref().unwrap().set_hosts(&hosts)?;
        }
        Ok(b.as_ref().unwrap().mtu)
    }

    fn start_machine(&self, name: &str, mark: bool) -> Result<(), String> {
        let lock = self.lock_for(name);
        let _g = lock.lock().unwrap();
        let mut meta = store::load(name).map_err(|e| format!("{name}: {e}"))?;
        let ip = meta.ip.ok_or(format!("{name} has no address"))?;
        let t = Instant::now();
        if !self.is_running(name) {
            let mtu = self
                .network()
                .map_err(|e| format!("machine network: {e}"))?;
            let rootfs = store::rootfs(name);
            let config = lxc::config(name, ip, mtu, lxc::halt_signal(&rootfs));
            fs::write(store::dir(name).join("config"), config).map_err(|e| e.to_string())?;
            lxc::start(&self.reaper, name).map_err(|e| format!("start {name}: {e}"))?;
            let (_, pid) = lxc::info(&self.reaper, name);
            let pid = pid.ok_or(format!("{name} did not stay up"))?;
            self.running.lock().unwrap().insert(name.into(), (pid, ip));
            log(&format!("{name} started in {} ms", t.elapsed().as_millis()));
        }
        if mark && !meta.autostart {
            meta.autostart = true;
            store::save(name, &meta).map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    fn stop_machine(&self, name: &str) -> Result<(), String> {
        let lock = self.lock_for(name);
        let _g = lock.lock().unwrap();
        let t = Instant::now();
        lxc::stop(&self.reaper, name, false).map_err(|e| format!("stop {name}: {e}"))?;
        self.running.lock().unwrap().remove(name);
        log(&format!("{name} stopped in {} ms", t.elapsed().as_millis()));
        let mut meta = store::load(name).map_err(|e| e.to_string())?;
        if meta.autostart {
            meta.autostart = false;
            store::save(name, &meta).map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    fn delete(&self, name: &str) -> Result<(), String> {
        let lock = self.lock_for(name);
        let _g = lock.lock().unwrap();
        lxc::stop(&self.reaper, name, true).map_err(|e| format!("stop {name}: {e}"))?;
        self.running.lock().unwrap().remove(name);
        remove(&self.reaper, name).map_err(|e| format!("delete {name}: {e}"))?;
        if store::default_machine().is_none() {
            let next = store::names().into_iter().next();
            let _ = store::set_default(next.as_deref());
        }
        Ok(())
    }

    /// Unpack the image the host streams, set the machine up, boot it and
    /// run its first-boot setup.
    fn create(
        &self,
        spec: &CreateSpec,
        conn: &mut UnixStream,
        say: &mut dyn FnMut(&str),
    ) -> Result<(), String> {
        let name = spec.name.as_str();
        if !machine::name_ok(name) {
            return Err(format!(
                "invalid machine name {name:?}: use lowercase letters, digits and dashes"
            ));
        }
        if !machine::user_ok(&spec.user) || spec.uid == 0 {
            return Err(format!("invalid user {:?} (uid {})", spec.user, spec.uid));
        }
        let t = Instant::now();
        let lock = self.lock_for(name);
        let _g = lock.lock().unwrap();
        let ip = {
            let _a = self.alloc.lock().unwrap();
            if store::dir(name).exists() {
                return Err(format!("a machine named {name} already exists"));
            }
            let used: Vec<Ipv4Addr> = store::names()
                .iter()
                .filter_map(|n| store::load(n).ok()?.ip)
                .collect();
            let ip = store::free_ip(&used).ok_or("no free machine address")?;
            fs::create_dir_all(store::dir(name)).map_err(|e| e.to_string())?;
            ip
        };
        let result = self.populate(spec, ip, conn, say);
        if let Err(e) = result {
            let _ = remove(&self.reaper, name);
            return Err(e);
        }
        log(&format!(
            "{name} unpacked and set up in {} ms",
            t.elapsed().as_millis()
        ));
        if store::default_machine().is_none() {
            let _ = store::set_default(Some(name));
        }
        drop(_g);
        say("starting");
        self.start_machine(name, true)?;
        say(&format!("setting up {}", spec.user));
        let (code, out) = cmd::run_status(
            &self.reaper,
            Command::new("lxc-attach").args([
                "-n",
                name,
                "-P",
                store::DIR,
                "--clear-env",
                "--",
                "/bin/sh",
                "-c",
                provision::FIRST_BOOT,
            ]),
            FIRST_BOOT_TIMEOUT,
        )
        .map_err(|e| format!("first boot setup: {e}"))?;
        if code != 0 {
            say(&format!(
                "warning: sudo could not be installed ({}): {}",
                code,
                out.trim()
            ));
        }
        log(&format!("{name} created in {} ms", t.elapsed().as_millis()));
        Ok(())
    }

    fn populate(
        &self,
        spec: &CreateSpec,
        ip: Ipv4Addr,
        conn: &mut UnixStream,
        say: &mut dyn FnMut(&str),
    ) -> Result<(), String> {
        let name = spec.name.as_str();
        let rootfs = store::rootfs(name);
        cmd::run(
            &self.reaper,
            Command::new("btrfs")
                .args(["subvolume", "create"])
                .arg(&rootfs),
            Duration::from_secs(10),
        )
        .map_err(|e| e.to_string())?;
        say("unpacking");
        unpack(&self.reaper, &rootfs, conn)?;
        lxc::write_identity(&rootfs, name).map_err(|e| format!("hostname: {e}"))?;
        provision::add_user(&rootfs, &spec.user, spec.uid)
            .map_err(|e| format!("user {}: {e}", spec.user))?;
        store::save(
            name,
            &store::Meta {
                distro: spec.distro.clone(),
                release: spec.release.clone(),
                image: spec.image.clone(),
                user: spec.user.clone(),
                uid: spec.uid,
                ip: Some(ip),
                created_unix: now_unix(),
                autostart: false,
            },
        )
        .map_err(|e| e.to_string())
    }

    /// Start the machines that were running when the VM last stopped, once
    /// Docker is up (or after a while without it).
    fn autostart(&self) {
        let names: Vec<String> = store::names()
            .into_iter()
            .filter(|n| store::load(n).is_ok_and(|m| m.autostart))
            .collect();
        if names.is_empty() {
            return;
        }
        let deadline = Instant::now() + Duration::from_secs(30);
        while !docker_answers() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(50));
        }
        thread::scope(|s| {
            for n in &names {
                s.spawn(move || {
                    if let Err(e) = self.start_machine(n, false) {
                        log(&format!("autostart: {e}"));
                    }
                });
            }
        });
    }

    /// Forward the TCP ports machines listen on (on every address) from
    /// eth0, so the Mac reaches them on localhost. The root namespace wins a
    /// port both use; between machines, the first by name.
    fn watch_ports(&self) {
        let mut active: BTreeMap<u16, Ipv4Addr> = BTreeMap::new();
        loop {
            let running: Vec<(String, (i32, Ipv4Addr))> = self
                .running
                .lock()
                .unwrap()
                .iter()
                .map(|(n, r)| (n.clone(), *r))
                .collect();
            if running.is_empty() && active.is_empty() {
                thread::sleep(Duration::from_secs(5));
                continue;
            }
            thread::sleep(PORT_INTERVAL);
            let root = super::ports::listening();
            let mut want: BTreeMap<u16, Ipv4Addr> = BTreeMap::new();
            for (_, (pid, ip)) in &running {
                for t in ["tcp", "tcp6"] {
                    let Ok(table) = fs::read_to_string(format!("/proc/{pid}/net/{t}")) else {
                        continue;
                    };
                    for port in super::ports::wildcard_listeners(&table) {
                        if !root.contains(&port) {
                            want.entry(port).or_insert(*ip);
                        }
                    }
                }
            }
            let stale: Vec<(u16, Ipv4Addr)> = active
                .iter()
                .filter(|(p, ip)| want.get(p) != Some(ip))
                .map(|(p, ip)| (*p, *ip))
                .collect();
            for (port, ip) in stale {
                if let Err(e) = bridge::dnat(&self.reaper, port, ip, false) {
                    log(&format!("unforward {port}: {e}"));
                }
                active.remove(&port);
            }
            for (port, ip) in &want {
                if active.contains_key(port) {
                    continue;
                }
                match bridge::dnat(&self.reaper, *port, *ip, true) {
                    Ok(()) => {
                        active.insert(*port, *ip);
                    }
                    Err(e) => log(&format!("forward {port} to {ip}: {e}")),
                }
            }
            *FORWARDED.lock().unwrap() = active.keys().copied().collect();
        }
    }
}

/// Whether dockerd answers `/_ping` (the socket exists well before that).
fn docker_answers() -> bool {
    let Ok(mut s) = UnixStream::connect(super::service::DOCKER_SOCKET) else {
        return false;
    };
    let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
    let mut resp = [0u8; 12];
    s.write_all(b"GET /_ping HTTP/1.0\r\nHost: docker\r\n\r\n")
        .is_ok()
        && io::Read::read_exact(&mut s, &mut resp).is_ok()
        && resp.ends_with(b" 200")
}

/// Pipe the `Data` frames on `conn` into `tar -xJ` in `rootfs`.
fn unpack(reaper: &Reaper, rootfs: &std::path::Path, conn: &mut UnixStream) -> Result<(), String> {
    let (in_r, in_w) = cmd::pipe().map_err(|e| e.to_string())?;
    let (out_r, out_w) = cmd::pipe().map_err(|e| e.to_string())?;
    let mut c = Command::new("tar");
    c.args([
        "-xJ",
        "--numeric-owner",
        "--xattrs",
        "--xattrs-include=*",
        "-f",
        "-",
        "-C",
    ])
    .arg(rootfs)
    .env("PATH", cmd::PATH)
    .stdin(Stdio::from(in_r))
    .stdout(Stdio::from(out_w.try_clone().map_err(|e| e.to_string())?))
    .stderr(Stdio::from(out_w));
    let (_pid, exited) = reaper.spawn(&mut c).map_err(|e| format!("tar: {e}"))?;
    drop(c);
    let errors = thread::spawn(move || {
        let mut s = String::new();
        let _ = io::Read::read_to_string(&mut File::from(out_r), &mut s);
        s
    });
    let mut tar_in = Some(File::from(in_w));
    let mut bytes = 0u64;
    let stream = loop {
        match machine::read(conn) {
            Ok(Some(MachineMsg::Data(d))) => {
                bytes += d.len() as u64;
                if let Some(t) = tar_in.as_mut()
                    && t.write_all(&d).is_err()
                {
                    tar_in = None;
                }
            }
            Ok(Some(MachineMsg::DataEnd)) => break Ok(()),
            Ok(Some(other)) => break Err(format!("unexpected {other:?} in the image stream")),
            Ok(None) => break Err("the host hung up while sending the image".to_string()),
            Err(e) => break Err(format!("image stream: {e}")),
        }
    };
    drop(tar_in);
    let status = exited.recv().unwrap_or(-1);
    let out = errors.join().unwrap_or_default();
    stream?;
    if status != 0 {
        return Err(format!(
            "unpacking the image ({bytes} bytes): tar {}: {}",
            describe_status(status),
            out.trim()
        ));
    }
    Ok(())
}

/// Delete a machine's subvolume (and any nested ones) and its directory.
fn remove(reaper: &Reaper, name: &str) -> io::Result<()> {
    let rootfs = store::rootfs(name);
    if rootfs.exists() {
        let t = Duration::from_secs(120);
        let recursive = cmd::run_status(
            reaper,
            Command::new("btrfs")
                .args(["subvolume", "delete", "-R"])
                .arg(&rootfs),
            t,
        )?;
        if recursive.0 != 0 {
            cmd::run(
                reaper,
                Command::new("btrfs")
                    .args(["subvolume", "delete"])
                    .arg(&rootfs),
                t,
            )?;
        }
    }
    match fs::remove_dir_all(store::dir(name)) {
        Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}
