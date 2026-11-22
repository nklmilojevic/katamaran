//! Linux machines, host side: images from images.linuxcontainers.org
//! (downloaded on the Mac with the system curl, checked against the image's
//! SHA256SUMS and cached under `<home>/cache/images`), and requests to the
//! guest's machine service over `<home>/run/machine.sock` (see
//! [`kat_proto::machine`]).

use std::fs::{self, File};
use std::io::Read;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

use anyhow::{Context, bail};
use kat_proto::machine::{self, CHUNK, CreateSpec, MachineInfo, MachineMsg};
use serde::{Deserialize, Serialize};

pub const SOCKET_NAME: &str = "machine.sock";
const IMAGES: &str = "https://images.linuxcontainers.org";
const ARCH: &str = "arm64";
const VARIANT: &str = "default";

/// A machine as the API reports it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Machine {
    pub name: String,
    pub distro: String,
    pub release: String,
    /// The image build it was created from.
    pub image: String,
    /// `running`, `stopped`, `starting` or `stopping`.
    pub state: String,
    /// Address on the machine network, while running.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ip: Option<String>,
    pub user: String,
    pub uid: u32,
    pub default: bool,
    pub autostart: bool,
    pub created_ms: u64,
    /// The machine's files on the Mac.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<PathBuf>,
}

impl Machine {
    pub fn from_info(m: MachineInfo, files: Option<&Path>) -> Machine {
        Machine {
            path: files.map(|f| f.join("machines").join(&m.name)),
            name: m.name,
            distro: m.distro,
            release: m.release,
            image: m.image,
            state: m.state,
            ip: Some(m.ip).filter(|i| !i.is_empty()),
            user: m.user,
            uid: m.uid,
            default: m.default,
            autostart: m.autostart,
            created_ms: m.created_unix * 1000,
        }
    }
}

pub fn socket_path(home: &Path) -> PathBuf {
    home.join("run").join(SOCKET_NAME)
}

/// A connection to the guest's machine service.
pub fn connect(home: &Path) -> anyhow::Result<UnixStream> {
    let path = socket_path(home);
    UnixStream::connect(&path).with_context(|| {
        format!(
            "the VM is not running ({}); start it with `katamaran start`",
            path.display()
        )
    })
}

/// Send `req` and wait for its answer, passing `Progress` lines on.
pub fn request(
    home: &Path,
    req: &MachineMsg,
    progress: &mut dyn FnMut(&str),
) -> anyhow::Result<MachineMsg> {
    let mut s = connect(home)?;
    machine::write(&mut s, req)?;
    answer(&mut s, progress)
}

fn answer(s: &mut UnixStream, progress: &mut dyn FnMut(&str)) -> anyhow::Result<MachineMsg> {
    loop {
        match machine::read(s)? {
            Some(MachineMsg::Progress(p)) => progress(&p),
            Some(MachineMsg::Error(e)) => bail!("{e}"),
            Some(m) => return Ok(m),
            None => bail!("the guest closed the machine connection"),
        }
    }
}

pub fn list(home: &Path) -> anyhow::Result<Vec<MachineInfo>> {
    match request(home, &MachineMsg::List, &mut |_| {})? {
        MachineMsg::Machines(l) => Ok(l),
        other => bail!("unexpected answer {other:?}"),
    }
}

pub fn info(home: &Path, name: &str) -> anyhow::Result<MachineInfo> {
    match request(home, &MachineMsg::Info { name: name.into() }, &mut |_| {})? {
        MachineMsg::Machines(mut l) if l.len() == 1 => Ok(l.remove(0)),
        other => bail!("unexpected answer {other:?}"),
    }
}

/// Start, stop, delete or make default.
pub fn simple(home: &Path, req: &MachineMsg) -> anyhow::Result<()> {
    match request(home, req, &mut |_| {})? {
        MachineMsg::Done => Ok(()),
        other => bail!("unexpected answer {other:?}"),
    }
}

/// The Mac user machines are created for: login name and uid.
pub fn mac_user() -> anyhow::Result<(String, u32)> {
    let uid = unsafe { libc::getuid() };
    let pw = unsafe { libc::getpwuid(uid) };
    let name = if pw.is_null() {
        std::env::var("USER").context("cannot tell the user name")?
    } else {
        unsafe { std::ffi::CStr::from_ptr((*pw).pw_name) }
            .to_string_lossy()
            .into_owned()
    };
    let name = name.to_lowercase();
    if !machine::user_ok(&name) {
        bail!("the user name {name:?} cannot be used in Linux");
    }
    Ok((name, uid))
}

/// Create a machine: resolve and fetch the image, stream it to the guest,
/// and wait while the guest sets the machine up and boots it.
pub fn create(
    home: &Path,
    name: &str,
    distro: &str,
    release: Option<&str>,
    progress: &mut dyn FnMut(&str),
) -> anyhow::Result<MachineInfo> {
    if !machine::name_ok(name) {
        bail!("invalid machine name {name:?}: use lowercase letters, digits and dashes");
    }
    let distro = canonical_distro(distro);
    let t = Instant::now();
    progress(&format!("finding the {distro} image"));
    let image = resolve(distro, release)?;
    let file = fetch(&home.join("cache/images"), &image, progress)?;
    let (user, uid) = mac_user()?;
    let mut s = connect(home)?;
    machine::write(
        &mut s,
        &MachineMsg::Create(CreateSpec {
            name: name.into(),
            distro: image.distro.clone(),
            release: image.release.clone(),
            image: image.serial.clone(),
            user,
            uid,
        }),
    )?;
    progress(&format!(
        "creating {name} from {} {} ({})",
        image.distro, image.release, image.serial
    ));
    let mut f = File::open(&file)?;
    let mut buf = vec![0u8; CHUNK];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        if let Err(e) = machine::write(&mut s, &MachineMsg::Data(buf[..n].to_vec())) {
            // The guest gave up early; its error says why.
            return match answer(&mut s, progress) {
                Err(guest) => Err(guest),
                Ok(_) => Err(e.into()),
            };
        }
    }
    machine::write(&mut s, &MachineMsg::DataEnd)?;
    match answer(&mut s, progress)? {
        MachineMsg::Done => {}
        other => bail!("unexpected answer {other:?}"),
    }
    progress(&format!(
        "created {name} in {:.1} s",
        t.elapsed().as_secs_f64()
    ));
    info(home, name)
}

/// `arch` is `archlinux` on the image server.
pub fn canonical_distro(d: &str) -> &str {
    match d {
        "arch" => "archlinux",
        other => other,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Image {
    pub distro: String,
    pub release: String,
    pub serial: String,
    /// Directory of the build on the image server, `/images/…/`.
    pub path: String,
}

/// Releases picked when none is asked for. Others get the newest numbered
/// release in the index (alpine, fedora), or the last one listed.
const DEFAULT_RELEASES: [(&str, &str); 3] = [
    ("ubuntu", "noble"),
    ("debian", "trixie"),
    ("archlinux", "current"),
];

/// Pick an image from the server's `index-system`
/// (`distro;release;arch;variant;serial;path` per line).
pub fn pick(index: &str, distro: &str, release: Option<&str>) -> Result<Image, String> {
    let all: Vec<Image> = index
        .lines()
        .filter_map(|l| {
            let f: Vec<&str> = l.trim().split(';').collect();
            (f.len() == 6 && f[0] == distro && f[2] == ARCH && f[3] == VARIANT).then(|| Image {
                distro: f[0].into(),
                release: f[1].into(),
                serial: f[4].into(),
                path: f[5].into(),
            })
        })
        .collect();
    if all.is_empty() {
        let mut distros: Vec<&str> = index
            .lines()
            .filter_map(|l| l.split(';').next())
            .filter(|d| !d.is_empty())
            .collect();
        distros.sort();
        distros.dedup();
        return Err(format!(
            "no {ARCH} image for {distro:?}; available: {}",
            distros.join(", ")
        ));
    }
    let release = match release {
        Some(r) => r.to_string(),
        None => DEFAULT_RELEASES
            .iter()
            .find(|(d, r)| *d == distro && all.iter().any(|i| i.release == *r))
            .map(|(_, r)| r.to_string())
            .or_else(|| newest_numbered(&all))
            .unwrap_or_else(|| all.last().unwrap().release.clone()),
    };
    all.iter()
        .filter(|i| i.release == release)
        .max_by(|a, b| a.serial.cmp(&b.serial))
        .cloned()
        .ok_or_else(|| {
            let rs: Vec<&str> = all.iter().map(|i| i.release.as_str()).collect();
            format!(
                "no {distro} release {release:?}; available: {}",
                rs.join(", ")
            )
        })
}

fn newest_numbered(all: &[Image]) -> Option<String> {
    let key = |r: &str| -> Option<Vec<u32>> { r.split('.').map(|p| p.parse().ok()).collect() };
    all.iter()
        .filter_map(|i| Some((key(&i.release)?, i.release.clone())))
        .max()
        .map(|(_, r)| r)
}

/// The sha256 of `name` in a SHA256SUMS file.
pub fn sha256_of(sums: &str, name: &str) -> Option<String> {
    sums.lines().find_map(|l| {
        let (hash, file) = l.split_once(char::is_whitespace)?;
        (file.trim().trim_start_matches('*') == name && hash.len() == 64).then(|| hash.into())
    })
}

/// The cache file name of an image (no `:` in file names).
pub fn cache_name(i: &Image) -> String {
    format!("{}-{}-{}.tar.xz", i.distro, i.release, i.serial).replace([':', '/'], "-")
}

pub(crate) fn curl(url: &str, out: &Path) -> anyhow::Result<()> {
    let status = Command::new("/usr/bin/curl")
        .args(["-fsSL", "--retry", "3", "--connect-timeout", "15", "-o"])
        .arg(out)
        .arg(url)
        .status()
        .context("run curl")?;
    if !status.success() {
        bail!("download of {url} failed ({status})");
    }
    Ok(())
}

fn curl_text(url: &str) -> anyhow::Result<String> {
    let out = Command::new("/usr/bin/curl")
        .args(["-fsSL", "--retry", "3", "--connect-timeout", "15", url])
        .output()
        .context("run curl")?;
    if !out.status.success() {
        bail!(
            "download of {url} failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

pub(crate) fn sha256_file(path: &Path) -> anyhow::Result<String> {
    let out = Command::new("/usr/bin/shasum")
        .args(["-a", "256"])
        .arg(path)
        .output()
        .context("run shasum")?;
    String::from_utf8_lossy(&out.stdout)
        .split_whitespace()
        .next()
        .map(str::to_string)
        .context("shasum printed nothing")
}

pub fn resolve(distro: &str, release: Option<&str>) -> anyhow::Result<Image> {
    let index = curl_text(&format!("{IMAGES}/meta/1.0/index-system"))?;
    pick(&index, distro, release).map_err(anyhow::Error::msg)
}

/// The image's root filesystem tarball, from the cache or the server.
/// Older builds of the same release are dropped from the cache.
pub fn fetch(
    cache: &Path,
    image: &Image,
    progress: &mut dyn FnMut(&str),
) -> anyhow::Result<PathBuf> {
    fs::create_dir_all(cache)?;
    let file = cache.join(cache_name(image));
    if file.is_file() {
        progress("using the cached image");
        return Ok(file);
    }
    let base = format!("{IMAGES}{}", image.path.replace(':', "%3A"));
    let sums = curl_text(&format!("{base}SHA256SUMS"))?;
    let want = sha256_of(&sums, "rootfs.tar.xz").context("rootfs.tar.xz is not in SHA256SUMS")?;
    progress(&format!("downloading {} {}", image.distro, image.release));
    let t = Instant::now();
    let tmp = file.with_extension("part");
    curl(&format!("{base}rootfs.tar.xz"), &tmp)?;
    let got = sha256_file(&tmp)?;
    if got != want {
        let _ = fs::remove_file(&tmp);
        bail!("rootfs.tar.xz checksum mismatch: {got}, expected {want}");
    }
    let size = fs::metadata(&tmp)?.len();
    progress(&format!(
        "downloaded {:.1} MB in {:.1} s",
        size as f64 / 1e6,
        t.elapsed().as_secs_f64()
    ));
    fs::rename(&tmp, &file)?;
    let prefix = format!("{}-{}-", image.distro, image.release).replace([':', '/'], "-");
    for e in fs::read_dir(cache)?.flatten() {
        let n = e.file_name().to_string_lossy().into_owned();
        if n.starts_with(&prefix) && n.ends_with(".tar.xz") && e.path() != file {
            let _ = fs::remove_file(e.path());
        }
    }
    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::*;

    const INDEX: &str = "alpine;3.23;arm64;default;20261117_13:02;/images/alpine/3.23/arm64/default/20261117_13:02/
alpine;3.24;arm64;default;20261117_13:01;/images/alpine/3.24/arm64/default/20261117_13:01/
alpine;3.24;amd64;default;20261118_13:01;/images/alpine/3.24/amd64/default/20261118_13:01/
alpine;3.24;arm64;cloud;20261118_13:01;/images/alpine/3.24/arm64/cloud/20261118_13:01/
alpine;edge;arm64;default;20261117_13:00;/images/alpine/edge/arm64/default/20261117_13:00/
ubuntu;jammy;arm64;default;20261118_07:42;/images/ubuntu/jammy/arm64/default/20261118_07:42/
ubuntu;noble;arm64;default;20261118_07:42;/images/ubuntu/noble/arm64/default/20261118_07:42/
fedora;43;arm64;default;20261117_20:33;/images/fedora/43/arm64/default/20261117_20:33/
fedora;44;arm64;default;20261117_21:56;/images/fedora/44/arm64/default/20261117_21:56/
archlinux;current;arm64;default;20261118_04:18;/images/archlinux/current/arm64/default/20261118_04:18/
";

    #[test]
    fn picks_images() {
        let a = pick(INDEX, "alpine", None).unwrap();
        assert_eq!(
            (a.release.as_str(), a.serial.as_str()),
            ("3.24", "20261117_13:01")
        );
        assert_eq!(a.path, "/images/alpine/3.24/arm64/default/20261117_13:01/");
        assert_eq!(pick(INDEX, "alpine", Some("edge")).unwrap().release, "edge");
        assert_eq!(pick(INDEX, "ubuntu", None).unwrap().release, "noble");
        assert_eq!(
            pick(INDEX, "ubuntu", Some("jammy")).unwrap().release,
            "jammy"
        );
        assert_eq!(pick(INDEX, "fedora", None).unwrap().release, "44");
        assert_eq!(
            pick(INDEX, canonical_distro("arch"), None).unwrap().release,
            "current"
        );
        let e = pick(INDEX, "gentoo", None).unwrap_err();
        assert!(e.contains("alpine, archlinux, fedora, ubuntu"), "{e}");
        let e = pick(INDEX, "ubuntu", Some("focal")).unwrap_err();
        assert!(e.contains("jammy, noble"), "{e}");
    }

    #[test]
    fn reads_checksums_and_names_cache_files() {
        let sums = "afae4127  x\nafae412719df420f68f327b01b2656536ce9cd889dfe1bf09864ff6b729fbb0e  rootfs.tar.xz\n";
        assert_eq!(
            sha256_of(sums, "rootfs.tar.xz").unwrap(),
            "afae412719df420f68f327b01b2656536ce9cd889dfe1bf09864ff6b729fbb0e"
        );
        assert_eq!(sha256_of(sums, "meta.tar.xz"), None);
        let i = pick(INDEX, "alpine", None).unwrap();
        assert_eq!(cache_name(&i), "alpine-3.24-20261117_13-01.tar.xz");
    }

    #[test]
    fn converts_guest_info() {
        let m = Machine::from_info(
            MachineInfo {
                name: "dev".into(),
                state: "running".into(),
                ip: "198.19.249.2".into(),
                created_unix: 2,
                ..Default::default()
            },
            Some(Path::new("/Users/me/Katamaran")),
        );
        assert_eq!(m.ip.as_deref(), Some("198.19.249.2"));
        assert_eq!(m.created_ms, 2000);
        assert_eq!(
            m.path.unwrap(),
            Path::new("/Users/me/Katamaran/machines/dev")
        );
        let v = serde_json::to_value(Machine::from_info(MachineInfo::default(), None)).unwrap();
        assert!(v.get("ip").is_none() && v.get("path").is_none());
    }
}
