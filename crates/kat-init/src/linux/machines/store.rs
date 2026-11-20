//! What the guest keeps about each machine, next to its LXC config:
//!
//! ```text
//! /var/lib/katamaran/machines/            lxc.lxcpath, on the data disk
//!   .default                              name of the default machine
//!   <name>/config                         LXC config (written by lxc::config)
//!   <name>/katamaran.meta                 key=value, see [`Meta`]
//!   <name>/rootfs/                        btrfs subvolume, the machine's /
//! ```

use std::fs;
use std::io;
use std::net::Ipv4Addr;
use std::path::PathBuf;

pub const DIR: &str = "/var/lib/katamaran/machines";
const META: &str = "katamaran.meta";
const DEFAULT: &str = ".default";

/// The machine bridge's network: the bridge is .1, machines get .2–.254.
pub const SUBNET: [u8; 3] = [198, 19, 249];
pub const PREFIX: u8 = 24;

pub fn gateway() -> Ipv4Addr {
    Ipv4Addr::new(SUBNET[0], SUBNET[1], SUBNET[2], 1)
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Meta {
    pub distro: String,
    pub release: String,
    pub image: String,
    pub user: String,
    pub uid: u32,
    pub ip: Option<Ipv4Addr>,
    pub created_unix: u64,
    /// Started with the VM: set by `start`, cleared by a requested `stop`.
    pub autostart: bool,
}

impl Meta {
    pub fn encode(&self) -> String {
        let mut s = String::new();
        let mut kv = |k: &str, v: &str| s.push_str(&format!("{k}={v}\n"));
        kv("distro", &self.distro);
        kv("release", &self.release);
        kv("image", &self.image);
        kv("user", &self.user);
        kv("uid", &self.uid.to_string());
        kv("ip", &self.ip.map(|i| i.to_string()).unwrap_or_default());
        kv("created", &self.created_unix.to_string());
        kv("autostart", if self.autostart { "1" } else { "0" });
        s
    }

    pub fn decode(text: &str) -> Meta {
        let mut m = Meta::default();
        for (k, v) in text.lines().filter_map(|l| l.split_once('=')) {
            match k {
                "distro" => m.distro = v.into(),
                "release" => m.release = v.into(),
                "image" => m.image = v.into(),
                "user" => m.user = v.into(),
                "uid" => m.uid = v.parse().unwrap_or(0),
                "ip" => m.ip = v.parse().ok(),
                "created" => m.created_unix = v.parse().unwrap_or(0),
                "autostart" => m.autostart = v == "1",
                _ => {}
            }
        }
        m
    }
}

pub fn dir(name: &str) -> PathBuf {
    PathBuf::from(DIR).join(name)
}

pub fn rootfs(name: &str) -> PathBuf {
    dir(name).join("rootfs")
}

pub fn load(name: &str) -> io::Result<Meta> {
    fs::read_to_string(dir(name).join(META)).map(|t| Meta::decode(&t))
}

pub fn save(name: &str, meta: &Meta) -> io::Result<()> {
    let path = dir(name).join(META);
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, meta.encode())?;
    fs::rename(tmp, path)
}

pub fn exists(name: &str) -> bool {
    dir(name).join(META).is_file()
}

/// Every machine, sorted by name.
pub fn names() -> Vec<String> {
    let mut out: Vec<String> = fs::read_dir(DIR)
        .into_iter()
        .flatten()
        .filter_map(|e| e.ok()?.file_name().into_string().ok())
        .filter(|n| kat_proto::machine::name_ok(n) && exists(n))
        .collect();
    out.sort();
    out
}

pub fn default_machine() -> Option<String> {
    let name = fs::read_to_string(PathBuf::from(DIR).join(DEFAULT)).ok()?;
    let name = name.trim();
    exists(name).then(|| name.to_string())
}

pub fn set_default(name: Option<&str>) -> io::Result<()> {
    let path = PathBuf::from(DIR).join(DEFAULT);
    match name {
        Some(n) => fs::write(path, format!("{n}\n")),
        None => match fs::remove_file(path) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        },
    }
}

/// The lowest free machine address.
pub fn free_ip(used: &[Ipv4Addr]) -> Option<Ipv4Addr> {
    (2..=254u8)
        .map(|h| Ipv4Addr::new(SUBNET[0], SUBNET[1], SUBNET[2], h))
        .find(|ip| !used.contains(ip))
}

/// A locally administered MAC derived from the address, so a machine keeps
/// its DHCP lease across restarts.
pub fn mac(ip: Ipv4Addr) -> String {
    let o = ip.octets();
    format!("02:6b:61:{:02x}:{:02x}:{:02x}", o[1], o[2], o[3])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_round_trips() {
        let m = Meta {
            distro: "alpine".into(),
            release: "3.24".into(),
            image: "20261117_13:01".into(),
            user: "me".into(),
            uid: 501,
            ip: Some(Ipv4Addr::new(198, 19, 249, 2)),
            created_unix: 5,
            autostart: true,
        };
        assert_eq!(Meta::decode(&m.encode()), m);
        assert_eq!(Meta::decode("junk\nuid=x\n"), Meta::default());
    }

    #[test]
    fn allocates_addresses() {
        let a = |h| Ipv4Addr::new(198, 19, 249, h);
        assert_eq!(free_ip(&[]), Some(a(2)));
        assert_eq!(free_ip(&[a(2), a(4)]), Some(a(3)));
        let all: Vec<_> = (2..=254).map(a).collect();
        assert_eq!(free_ip(&all), None);
        assert_eq!(mac(a(7)), "02:6b:61:13:f9:07");
    }
}
