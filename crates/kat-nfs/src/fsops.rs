//! File operations relative to directory descriptors. Below an anchor every
//! path is walked one name at a time with `O_NOFOLLOW`, so a symlink planted
//! inside a container or volume can never lead the server (root in the
//! guest) outside of it. Builds on macOS too, so the server's tests run on
//! the host. Field widths differ between the two, hence casts that are
//! no-ops on one of them.
#![allow(clippy::unnecessary_cast)]

use std::ffi::{CString, OsStr};
use std::io;
use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

/// Directories walked through only: no read access needed, no atime.
#[cfg(target_os = "linux")]
const WALK: libc::c_int = libc::O_PATH | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC;
#[cfg(not(target_os = "linux"))]
const WALK: libc::c_int = libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Stat {
    pub dev: u64,
    pub ino: u64,
    pub mode: u32,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub blocks: u64,
    pub rdev: u64,
    pub atime: (i64, u32),
    pub mtime: (i64, u32),
    pub ctime: (i64, u32),
}

impl Stat {
    pub fn ident(&self) -> (u64, u64) {
        (self.dev, self.ino)
    }

    pub fn file_type(&self) -> u32 {
        self.mode & libc::S_IFMT as u32
    }

    pub fn is_dir(&self) -> bool {
        self.file_type() == libc::S_IFDIR as u32
    }

    pub fn is_reg(&self) -> bool {
        self.file_type() == libc::S_IFREG as u32
    }

    pub fn is_symlink(&self) -> bool {
        self.file_type() == libc::S_IFLNK as u32
    }

    /// Device numbers as (major, minor).
    pub fn rdev_parts(&self) -> (u32, u32) {
        let d = self.rdev;
        if cfg!(target_os = "linux") {
            (
                (((d >> 8) & 0xfff) | ((d >> 32) & !0xfff)) as u32,
                ((d & 0xff) | ((d >> 12) & !0xff)) as u32,
            )
        } else {
            (((d >> 24) & 0xff) as u32, (d & 0xff_ffff) as u32)
        }
    }

    fn from_raw(st: &libc::stat) -> Stat {
        Stat {
            dev: st.st_dev as u64,
            ino: st.st_ino as u64,
            mode: st.st_mode as u32,
            nlink: st.st_nlink as u32,
            uid: st.st_uid,
            gid: st.st_gid,
            size: st.st_size as u64,
            blocks: st.st_blocks as u64,
            rdev: st.st_rdev as u64,
            atime: (st.st_atime as i64, st.st_atime_nsec as u32),
            mtime: (st.st_mtime as i64, st.st_mtime_nsec as u32),
            ctime: (st.st_ctime as i64, st.st_ctime_nsec as u32),
        }
    }
}

fn check(rc: libc::c_int) -> io::Result<libc::c_int> {
    if rc < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(rc)
    }
}

pub fn cname(name: &[u8]) -> io::Result<CString> {
    CString::new(name).map_err(|_| io::Error::from_raw_os_error(libc::EINVAL))
}

fn cpath(p: &Path) -> io::Result<CString> {
    cname(p.as_os_str().as_bytes())
}

/// Open an anchor's directory. Unlike everything below it, the path may
/// contain symlinks: `/proc/<pid>/root` is one.
pub fn open_anchor(base: &Path) -> io::Result<OwnedFd> {
    let p = cpath(base)?;
    let flags = WALK & !libc::O_NOFOLLOW;
    let fd = check(unsafe { libc::open(p.as_ptr(), flags) })?;
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Step into a subdirectory without following a symlink.
pub fn walk(dir: &OwnedFd, name: &[u8]) -> io::Result<OwnedFd> {
    openat(dir, name, WALK, 0)
}

pub fn openat(dir: &OwnedFd, name: &[u8], flags: libc::c_int, mode: u32) -> io::Result<OwnedFd> {
    let n = cname(name)?;
    let flags = flags | libc::O_NOFOLLOW | libc::O_CLOEXEC;
    let fd =
        check(unsafe { libc::openat(dir.as_raw_fd(), n.as_ptr(), flags, mode as libc::c_uint) })?;
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Reopen a directory we hold (possibly an `O_PATH` one) with `flags`.
pub fn reopen_dir(dir: &OwnedFd, flags: libc::c_int) -> io::Result<OwnedFd> {
    openat(dir, b".", flags | libc::O_DIRECTORY, 0)
}

pub fn fstat(fd: &OwnedFd) -> io::Result<Stat> {
    let mut st = MaybeUninit::<libc::stat>::uninit();
    check(unsafe { libc::fstat(fd.as_raw_fd(), st.as_mut_ptr()) })?;
    let st = unsafe { st.assume_init() };
    Ok(Stat::from_raw(&st))
}

pub fn statat(dir: &OwnedFd, name: &[u8]) -> io::Result<Stat> {
    let n = cname(name)?;
    let mut st = MaybeUninit::<libc::stat>::uninit();
    check(unsafe {
        libc::fstatat(
            dir.as_raw_fd(),
            n.as_ptr(),
            st.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    })?;
    let st = unsafe { st.assume_init() };
    Ok(Stat::from_raw(&st))
}

pub fn stat_path(p: &Path) -> io::Result<Stat> {
    let p = cpath(p)?;
    let mut st = MaybeUninit::<libc::stat>::uninit();
    check(unsafe { libc::stat(p.as_ptr(), st.as_mut_ptr()) })?;
    let st = unsafe { st.assume_init() };
    Ok(Stat::from_raw(&st))
}

pub fn mkdirat(dir: &OwnedFd, name: &[u8], mode: u32) -> io::Result<()> {
    let n = cname(name)?;
    check(unsafe { libc::mkdirat(dir.as_raw_fd(), n.as_ptr(), mode as libc::mode_t) })?;
    Ok(())
}

pub fn symlinkat(target: &[u8], dir: &OwnedFd, name: &[u8]) -> io::Result<()> {
    let (t, n) = (cname(target)?, cname(name)?);
    check(unsafe { libc::symlinkat(t.as_ptr(), dir.as_raw_fd(), n.as_ptr()) })?;
    Ok(())
}

pub fn unlinkat(dir: &OwnedFd, name: &[u8], is_dir: bool) -> io::Result<()> {
    let n = cname(name)?;
    let flags = if is_dir { libc::AT_REMOVEDIR } else { 0 };
    check(unsafe { libc::unlinkat(dir.as_raw_fd(), n.as_ptr(), flags) })?;
    Ok(())
}

pub fn renameat(from: &OwnedFd, fname: &[u8], to: &OwnedFd, tname: &[u8]) -> io::Result<()> {
    let (f, t) = (cname(fname)?, cname(tname)?);
    check(unsafe { libc::renameat(from.as_raw_fd(), f.as_ptr(), to.as_raw_fd(), t.as_ptr()) })?;
    Ok(())
}

pub fn linkat(from: &OwnedFd, fname: &[u8], to: &OwnedFd, tname: &[u8]) -> io::Result<()> {
    let (f, t) = (cname(fname)?, cname(tname)?);
    check(unsafe { libc::linkat(from.as_raw_fd(), f.as_ptr(), to.as_raw_fd(), t.as_ptr(), 0) })?;
    Ok(())
}

pub fn readlinkat(dir: &OwnedFd, name: &[u8]) -> io::Result<Vec<u8>> {
    let n = cname(name)?;
    let mut buf = vec![0u8; libc::PATH_MAX as usize];
    let len = unsafe {
        libc::readlinkat(
            dir.as_raw_fd(),
            n.as_ptr(),
            buf.as_mut_ptr().cast(),
            buf.len(),
        )
    };
    if len < 0 {
        return Err(io::Error::last_os_error());
    }
    buf.truncate(len as usize);
    Ok(buf)
}

/// chown without following a symlink. `None` leaves an id unchanged.
pub fn chownat(dir: &OwnedFd, name: &[u8], uid: Option<u32>, gid: Option<u32>) -> io::Result<()> {
    let n = cname(name)?;
    check(unsafe {
        libc::fchownat(
            dir.as_raw_fd(),
            n.as_ptr(),
            uid.unwrap_or(u32::MAX),
            gid.unwrap_or(u32::MAX),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    })?;
    Ok(())
}

pub fn fchown(fd: &OwnedFd, uid: Option<u32>, gid: Option<u32>) -> io::Result<()> {
    check(unsafe {
        libc::fchown(
            fd.as_raw_fd(),
            uid.unwrap_or(u32::MAX),
            gid.unwrap_or(u32::MAX),
        )
    })?;
    Ok(())
}

pub fn fchmod(fd: &OwnedFd, mode: u32) -> io::Result<()> {
    check(unsafe { libc::fchmod(fd.as_raw_fd(), mode as libc::mode_t) })?;
    Ok(())
}

/// chmod of a non-symlink by name (symlinks have no mode of their own).
pub fn chmodat(dir: &OwnedFd, name: &[u8], mode: u32) -> io::Result<()> {
    let n = cname(name)?;
    check(unsafe { libc::fchmodat(dir.as_raw_fd(), n.as_ptr(), mode as libc::mode_t, 0) })?;
    Ok(())
}

/// A time to set: unchanged, now, or a given (seconds, nanoseconds).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetTime {
    Keep,
    Now,
    At(i64, u32),
}

fn timespec(t: SetTime) -> libc::timespec {
    let mut ts: libc::timespec = unsafe { std::mem::zeroed() };
    match t {
        SetTime::Keep => ts.tv_nsec = libc::UTIME_OMIT,
        SetTime::Now => ts.tv_nsec = libc::UTIME_NOW,
        SetTime::At(s, ns) => {
            ts.tv_sec = s as _;
            ts.tv_nsec = ns as _;
        }
    }
    ts
}

pub fn utimensat(dir: &OwnedFd, name: &[u8], atime: SetTime, mtime: SetTime) -> io::Result<()> {
    let n = cname(name)?;
    let ts = [timespec(atime), timespec(mtime)];
    check(unsafe {
        libc::utimensat(
            dir.as_raw_fd(),
            n.as_ptr(),
            ts.as_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    })?;
    Ok(())
}

pub fn futimens(fd: &OwnedFd, atime: SetTime, mtime: SetTime) -> io::Result<()> {
    let ts = [timespec(atime), timespec(mtime)];
    check(unsafe { libc::futimens(fd.as_raw_fd(), ts.as_ptr()) })?;
    Ok(())
}

pub fn ftruncate(fd: &OwnedFd, size: u64) -> io::Result<()> {
    check(unsafe { libc::ftruncate(fd.as_raw_fd(), size as libc::off_t) })?;
    Ok(())
}

/// Append up to `count` bytes read at `off` to `buf`, without zeroing the
/// space first; short only at end of file. On error `buf` is unchanged.
pub fn pread_append(fd: &OwnedFd, buf: &mut Vec<u8>, count: usize, off: u64) -> io::Result<usize> {
    buf.reserve(count);
    let base = buf.len();
    let mut done = 0;
    while done < count {
        // SAFETY: the kernel writes at most `count - done` bytes into the
        // reserved, unused tail of `buf`; only what it wrote becomes part of
        // `buf` below.
        let n = unsafe {
            libc::pread(
                fd.as_raw_fd(),
                buf.as_mut_ptr().add(base + done).cast(),
                count - done,
                (off + done as u64) as libc::off_t,
            )
        };
        if n < 0 {
            let e = io::Error::last_os_error();
            if e.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(e);
        }
        if n == 0 {
            break;
        }
        done += n as usize;
    }
    // SAFETY: `done` bytes past `base` were written by pread.
    unsafe { buf.set_len(base + done) };
    Ok(done)
}

pub fn pwrite_all(fd: &OwnedFd, data: &[u8], off: u64) -> io::Result<()> {
    let mut done = 0;
    while done < data.len() {
        let n = unsafe {
            libc::pwrite(
                fd.as_raw_fd(),
                data[done..].as_ptr().cast(),
                data.len() - done,
                (off + done as u64) as libc::off_t,
            )
        };
        if n < 0 {
            let e = io::Error::last_os_error();
            if e.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(e);
        }
        done += n as usize;
    }
    Ok(())
}

/// The names in a directory, without `.` and `..`, in on-disk order.
pub fn list(dir: &OwnedFd) -> io::Result<Vec<Vec<u8>>> {
    let fd = reopen_dir(dir, libc::O_RDONLY)?;
    let raw: RawFd = std::os::fd::IntoRawFd::into_raw_fd(fd);
    let d = unsafe { libc::fdopendir(raw) };
    if d.is_null() {
        let e = io::Error::last_os_error();
        unsafe { libc::close(raw) };
        return Err(e);
    }
    let mut names = Vec::new();
    loop {
        // readdir signals errors only through errno.
        unsafe { *errno() = 0 };
        let ent = unsafe { libc::readdir(d) };
        if ent.is_null() {
            let err = unsafe { *errno() };
            unsafe { libc::closedir(d) };
            if err != 0 {
                return Err(io::Error::from_raw_os_error(err));
            }
            return Ok(names);
        }
        let name = unsafe { std::ffi::CStr::from_ptr((*ent).d_name.as_ptr()) }.to_bytes();
        if name != b"." && name != b".." {
            names.push(name.to_vec());
        }
    }
}

#[cfg(target_os = "linux")]
unsafe fn errno() -> *mut libc::c_int {
    unsafe { libc::__errno_location() }
}

#[cfg(not(target_os = "linux"))]
unsafe fn errno() -> *mut libc::c_int {
    unsafe { libc::__error() }
}

/// Filesystem totals: (bytes total, free, available, files total, free).
pub fn fsstat(fd: &OwnedFd) -> io::Result<(u64, u64, u64, u64, u64)> {
    let mut s = MaybeUninit::<libc::statvfs>::uninit();
    check(unsafe { libc::fstatvfs(fd.as_raw_fd(), s.as_mut_ptr()) })?;
    let s = unsafe { s.assume_init() };
    let f = s.f_frsize as u64;
    Ok((
        s.f_blocks as u64 * f,
        s.f_bfree as u64 * f,
        s.f_bavail as u64 * f,
        s.f_files as u64,
        s.f_ffree as u64,
    ))
}

/// Whether a directory belongs to a kernel pseudo filesystem (proc, sysfs,
/// cgroup and friends). Their files lie about sizes and some reads block,
/// so the server lists them as empty.
#[cfg(target_os = "linux")]
pub fn is_pseudo_fs(fd: &OwnedFd) -> bool {
    const PSEUDO: [i64; 9] = [
        0x9fa0,      // proc
        0x6265_6572, // sysfs
        0x6367_7270, // cgroup2
        0x0027_e0eb, // cgroup
        0x1cd1,      // devpts
        0x6462_6720, // debugfs
        0x7472_6163, // tracefs
        0x7363_6673, // securityfs
        0xcafe_4a11, // bpf
    ];
    let mut s = MaybeUninit::<libc::statfs>::uninit();
    if unsafe { libc::fstatfs(fd.as_raw_fd(), s.as_mut_ptr()) } < 0 {
        return false;
    }
    let t = unsafe { s.assume_init() }.f_type as i64;
    PSEUDO.contains(&t)
}

#[cfg(not(target_os = "linux"))]
pub fn is_pseudo_fs(_fd: &OwnedFd) -> bool {
    false
}

pub fn name_ok(name: &[u8]) -> bool {
    !name.is_empty() && name.len() <= 255 && !name.contains(&b'/') && !name.contains(&0)
}

pub fn os(name: &[u8]) -> &OsStr {
    OsStr::from_bytes(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("kat-nfs-fsops-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn walking_refuses_symlinks() {
        let d = tmp("walk");
        std::fs::create_dir(d.join("real")).unwrap();
        std::os::unix::fs::symlink("/", d.join("link")).unwrap();
        let root = open_anchor(&d).unwrap();
        assert!(walk(&root, b"real").is_ok());
        assert!(walk(&root, b"link").is_err());
        assert!(openat(&root, b"link", libc::O_RDONLY, 0).is_err());
        assert!(statat(&root, b"link").unwrap().is_symlink());
        std::fs::remove_dir_all(&d).unwrap();
    }

    #[test]
    fn lists_reads_writes_and_renames() {
        let d = tmp("ops");
        let root = open_anchor(&d).unwrap();
        mkdirat(&root, b"sub", 0o755).unwrap();
        let f = openat(&root, b"f", libc::O_CREAT | libc::O_RDWR, 0o644).unwrap();
        pwrite_all(&f, b"hello world", 0).unwrap();
        let mut v = b"head".to_vec();
        assert_eq!(pread_append(&f, &mut v, 32, 6).unwrap(), 5);
        assert_eq!(v, b"headworld");
        let mut names = list(&root).unwrap();
        names.sort();
        assert_eq!(names, vec![b"f".to_vec(), b"sub".to_vec()]);
        let sub = walk(&root, b"sub").unwrap();
        renameat(&root, b"f", &sub, b"g").unwrap();
        assert_eq!(list(&sub).unwrap(), vec![b"g".to_vec()]);
        symlinkat(b"../x", &sub, b"l").unwrap();
        assert_eq!(readlinkat(&sub, b"l").unwrap(), b"../x");
        utimensat(&sub, b"g", SetTime::At(1000, 5), SetTime::At(2000, 7)).unwrap();
        let st = statat(&sub, b"g").unwrap();
        assert_eq!((st.atime, st.mtime), ((1000, 5), (2000, 7)));
        assert!(unlinkat(&root, b"sub", true).is_err());
        unlinkat(&sub, b"g", false).unwrap();
        unlinkat(&sub, b"l", false).unwrap();
        unlinkat(&root, b"sub", true).unwrap();
        assert!(list(&root).unwrap().is_empty());
        assert!(fsstat(&root).unwrap().0 > 0);
        std::fs::remove_dir_all(&d).unwrap();
    }

    #[test]
    fn validates_names() {
        assert!(name_ok(b"a.txt"));
        assert!(!name_ok(b""));
        assert!(!name_ok(b"a/b"));
        assert!(!name_ok(b"a\0b"));
        assert!(!name_ok(&[b'x'; 256]));
    }
}
