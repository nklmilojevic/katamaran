//! File data operations with macOS-specific implementations: barrier fsync
//! and copy_file_range backed by APFS clones.

use std::ffi::{CStr, CString};
use std::fs::File;
use std::io;
use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

const CLONE_NOOWNERCOPY: u32 = 0x0002;
/// One COPY_FILE_RANGE request copies at most this much; the guest loops.
const MAX_COPY: u64 = 128 << 20;
const COPY_CHUNK: usize = 1 << 20;

/// FSYNC: `F_BARRIERFSYNC` orders the writes on the device without the full
/// cache flush of `F_FULLFSYNC`, which is far slower on Apple SSDs. Falls back
/// to `fsync` where the volume does not support barriers. With `no_sync` the
/// call is a no-op (the share's data is disposable).
pub fn fsync(fd: RawFd, no_sync: bool) -> io::Result<()> {
    if no_sync {
        return Ok(());
    }
    if unsafe { libc::fcntl(fd, libc::F_BARRIERFSYNC) } == 0 {
        return Ok(());
    }
    if unsafe { libc::fsync(fd) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn fstat(fd: RawFd) -> io::Result<libc::stat> {
    let mut st = MaybeUninit::<libc::stat>::zeroed();
    if unsafe { libc::fstat(fd, st.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { st.assume_init() })
}

fn fd_path(fd: RawFd) -> io::Result<CString> {
    let mut buf = vec![0u8; libc::PATH_MAX as usize];
    if unsafe { libc::fcntl(fd, libc::F_GETPATH, buf.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let len = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    buf.truncate(len);
    CString::new(buf).map_err(|_| io::Error::from_raw_os_error(libc::EINVAL))
}

fn xattr_names(fd: RawFd) -> io::Result<Vec<CString>> {
    let opts = 0;
    let size = unsafe { libc::flistxattr(fd, std::ptr::null_mut(), 0, opts) };
    if size < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut buf = vec![0u8; size as usize];
    let size = unsafe { libc::flistxattr(fd, buf.as_mut_ptr().cast(), buf.len(), opts) };
    if size < 0 {
        return Err(io::Error::last_os_error());
    }
    buf.truncate(size as usize);
    Ok(buf
        .split(|&b| b == 0)
        .filter(|n| !n.is_empty())
        .map(|n| CString::new(n).unwrap())
        .collect())
}

/// Give `to` exactly the extended attributes of `from`.
fn replace_xattrs(from: RawFd, to: RawFd) -> io::Result<()> {
    for name in xattr_names(to)? {
        unsafe { libc::fremovexattr(to, name.as_ptr(), 0) };
    }
    for name in xattr_names(from)? {
        let n = unsafe { libc::fgetxattr(from, name.as_ptr(), std::ptr::null_mut(), 0, 0, 0) };
        if n < 0 {
            continue;
        }
        let mut val = vec![0u8; n as usize];
        let n = unsafe {
            libc::fgetxattr(
                from,
                name.as_ptr(),
                val.as_mut_ptr().cast(),
                val.len(),
                0,
                0,
            )
        };
        if n < 0 {
            continue;
        }
        let res =
            unsafe { libc::fsetxattr(to, name.as_ptr(), val.as_ptr().cast(), n as usize, 0, 0) };
        // macOS manages some com.apple.* attributes itself (provenance).
        if res != 0 && !name.to_bytes().starts_with(b"com.apple.") {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

/// Replace the empty file open as `dst` with an APFS clone of `src`, keeping
/// the path, mode and extended attributes of `dst`. The new file has a
/// different inode, so the caller must point its node and handles at the
/// returned file (opened with `dst`'s access mode).
///
/// Returns `None` when a clone does not apply (not a whole-file copy into an
/// empty file on the same volume); the caller then copies the bytes.
pub fn clone_whole_file(
    src: RawFd,
    off_in: u64,
    dst: RawFd,
    off_out: u64,
    len: u64,
) -> io::Result<Option<(File, u64)>> {
    let (s, d) = (fstat(src)?, fstat(dst)?);
    let regular = |st: &libc::stat| st.st_mode & libc::S_IFMT == libc::S_IFREG;
    if off_in != 0
        || off_out != 0
        || !regular(&s)
        || !regular(&d)
        || s.st_size <= 0
        || (len as i64) < s.st_size
        || d.st_size != 0
        || d.st_nlink != 1
        || s.st_dev != d.st_dev
    {
        debug!("clone: not a whole-file copy into an empty file");
        return Ok(None);
    }

    let dst_path = fd_path(dst)?;
    let path = Path::new(std::ffi::OsStr::from_bytes(dst_path.as_bytes()));
    let (Some(dir), Some(name)) = (path.parent(), path.file_name()) else {
        return Ok(None);
    };
    static N: AtomicU64 = AtomicU64::new(0);
    let mut tmp_name = b".kat-clone-".to_vec();
    tmp_name.extend_from_slice(name.as_bytes());
    tmp_name.extend_from_slice(
        format!(
            ".{}.{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        )
        .as_bytes(),
    );
    let tmp_path = CString::new(
        dir.join(std::ffi::OsStr::from_bytes(&tmp_name))
            .as_os_str()
            .as_bytes(),
    )
    .map_err(|_| io::Error::from_raw_os_error(libc::EINVAL))?;

    if unsafe { libc::fclonefileat(src, libc::AT_FDCWD, tmp_path.as_ptr(), CLONE_NOOWNERCOPY) } != 0
    {
        let e = io::Error::last_os_error();
        debug!("fclonefileat failed, copying instead: {e}");
        return Ok(None);
    }
    let unlink_tmp = || unsafe { libc::unlink(tmp_path.as_ptr()) };

    let accmode = unsafe { libc::fcntl(dst, libc::F_GETFL) } & (libc::O_ACCMODE | libc::O_APPEND);
    let fd = unsafe {
        libc::open(
            tmp_path.as_ptr(),
            accmode | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if fd < 0 {
        let e = io::Error::last_os_error();
        unlink_tmp();
        return Err(e);
    }
    let file = unsafe { File::from_raw_fd(fd) };
    let finish = || -> io::Result<()> {
        replace_xattrs(dst, file.as_raw_fd())?;
        if unsafe { libc::fchmod(file.as_raw_fd(), d.st_mode & 0o7777) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // A copy is a new write: give it a fresh mtime like a byte copy would.
        if unsafe { libc::futimens(file.as_raw_fd(), std::ptr::null()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // Only replace the path if it still names the file the guest created.
        let cur = lstat(&dst_path)?;
        if cur.st_ino != d.st_ino || cur.st_dev != d.st_dev {
            return Err(io::Error::from_raw_os_error(libc::ESTALE));
        }
        if unsafe { libc::rename(tmp_path.as_ptr(), dst_path.as_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    };
    if let Err(e) = finish() {
        unlink_tmp();
        debug!("clone of {:?} abandoned, copying instead: {e}", dst_path);
        return Ok(None);
    }
    Ok(Some((file, s.st_size as u64)))
}

fn lstat(path: &CStr) -> io::Result<libc::stat> {
    let mut st = MaybeUninit::<libc::stat>::zeroed();
    if unsafe { libc::lstat(path.as_ptr(), st.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { st.assume_init() })
}

/// Copy up to `len` bytes from `src` at `off_in` to `dst` at `off_out` with
/// positioned reads and writes, stopping at the end of `src`.
pub fn copy_bytes(src: RawFd, off_in: u64, dst: RawFd, off_out: u64, len: u64) -> io::Result<u64> {
    let len = len.min(MAX_COPY);
    let mut buf = vec![0u8; COPY_CHUNK.min(len as usize).max(1)];
    let mut done = 0u64;
    while done < len {
        let want = ((len - done) as usize).min(buf.len());
        let n = unsafe {
            libc::pread(
                src,
                buf.as_mut_ptr().cast(),
                want,
                (off_in + done) as libc::off_t,
            )
        };
        if n < 0 {
            let e = io::Error::last_os_error();
            if e.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return if done > 0 { Ok(done) } else { Err(e) };
        }
        if n == 0 {
            break;
        }
        let mut written = 0usize;
        while written < n as usize {
            let w = unsafe {
                libc::pwrite(
                    dst,
                    buf[written..].as_ptr().cast(),
                    n as usize - written,
                    (off_out + done + written as u64) as libc::off_t,
                )
            };
            if w < 0 {
                let e = io::Error::last_os_error();
                if e.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return if done + written as u64 > 0 {
                    Ok(done + written as u64)
                } else {
                    Err(e)
                };
            }
            written += w as usize;
        }
        done += n as u64;
    }
    Ok(done)
}

/// How a COPY_FILE_RANGE was carried out.
pub enum Copied {
    /// `dst` was replaced by a clone of the whole of `src`, now open as this file.
    Cloned(File, u64),
    Bytes(u64),
}

pub fn copy_file_range(
    src: RawFd,
    off_in: u64,
    dst: RawFd,
    off_out: u64,
    len: u64,
) -> io::Result<Copied> {
    if let Some((file, n)) = clone_whole_file(src, off_in, dst, off_out, len)? {
        return Ok(Copied::Cloned(file, n));
    }
    copy_bytes(src, off_in, dst, off_out, len).map(Copied::Bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::virtio::fs::macos::test_util::TempDir;
    use std::fs::OpenOptions;
    use std::io::{Read, Write};
    use std::os::unix::fs::MetadataExt;

    fn data(n: usize) -> Vec<u8> {
        (0..n).map(|i| (i * 7 + i / 4096) as u8).collect()
    }

    fn clone_id(path: &Path) -> u64 {
        // getattrlist(ATTR_CMNEXT_CLONEID)
        #[repr(C)]
        struct AttrList {
            bitmapcount: u16,
            reserved: u16,
            commonattr: u32,
            volattr: u32,
            dirattr: u32,
            fileattr: u32,
            forkattr: u32,
        }
        #[repr(C, packed)]
        struct Out {
            len: u32,
            id: u64,
        }
        const ATTR_BIT_MAP_COUNT: u16 = 5;
        const ATTR_CMNEXT_CLONEID: u32 = 0x0000_0100;
        const FSOPT_ATTR_CMN_EXTENDED: u32 = 0x0000_0020;
        let al = AttrList {
            bitmapcount: ATTR_BIT_MAP_COUNT,
            reserved: 0,
            commonattr: 0,
            volattr: 0,
            dirattr: 0,
            fileattr: 0,
            forkattr: ATTR_CMNEXT_CLONEID,
        };
        let mut out = Out { len: 0, id: 0 };
        let c = CString::new(path.as_os_str().as_bytes()).unwrap();
        let r = unsafe {
            libc::getattrlist(
                c.as_ptr(),
                (&al as *const AttrList).cast_mut().cast(),
                (&mut out as *mut Out).cast(),
                std::mem::size_of::<Out>(),
                FSOPT_ATTR_CMN_EXTENDED,
            )
        };
        assert_eq!(r, 0, "{}", io::Error::last_os_error());
        out.id
    }

    #[test]
    fn whole_file_copy_into_new_file_is_a_clone() {
        let dir = TempDir::new();
        let (sp, dp) = (dir.0.join("src"), dir.0.join("dst"));
        let content = data(3 << 20);
        std::fs::write(&sp, &content).unwrap();
        let src = File::open(&sp).unwrap();
        let dst = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&dp)
            .unwrap();
        std::fs::set_permissions(&dp, std::os::unix::fs::PermissionsExt::from_mode(0o640)).unwrap();
        let k = CString::new("user.containers.override_stat").unwrap();
        let v = b"0:0:0100644";
        assert_eq!(
            unsafe {
                libc::fsetxattr(
                    dst.as_raw_fd(),
                    k.as_ptr(),
                    v.as_ptr().cast(),
                    v.len(),
                    0,
                    0,
                )
            },
            0
        );
        let old_ino = dst.metadata().unwrap().ino();

        let res = copy_file_range(src.as_raw_fd(), 0, dst.as_raw_fd(), 0, u64::MAX >> 1).unwrap();
        let Copied::Cloned(mut new, n) = res else {
            panic!("expected a clone")
        };
        assert_eq!(n, content.len() as u64);
        assert_ne!(new.metadata().unwrap().ino(), old_ino);
        assert_eq!(
            std::fs::metadata(&dp).unwrap().ino(),
            new.metadata().unwrap().ino()
        );
        assert_eq!(std::fs::read(&dp).unwrap(), content);
        assert_eq!(clone_id(&sp), clone_id(&dp), "data blocks are shared");
        assert_eq!(std::fs::metadata(&dp).unwrap().mode() & 0o7777, 0o640);
        let mut buf = [0u8; 32];
        let n = unsafe {
            libc::getxattr(
                CString::new(dp.as_os_str().as_bytes()).unwrap().as_ptr(),
                k.as_ptr(),
                buf.as_mut_ptr().cast(),
                buf.len(),
                0,
                0,
            )
        };
        assert_eq!(&buf[..n as usize], v);
        // The returned file is writable like the handle it replaces.
        new.write_all(b"x").unwrap();
        // No temporary files are left behind.
        let names: Vec<_> = std::fs::read_dir(&dir.0)
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names.len(), 2, "{names:?}");
        // The source is unchanged.
        let mut s = Vec::new();
        File::open(&sp).unwrap().read_to_end(&mut s).unwrap();
        assert_eq!(s, content);
    }

    #[test]
    fn partial_and_offset_copies_fall_back_to_bytes() {
        let dir = TempDir::new();
        let (sp, dp) = (dir.0.join("src"), dir.0.join("dst"));
        let content = data(200_000);
        std::fs::write(&sp, &content).unwrap();
        let src = File::open(&sp).unwrap();

        // Into a non-empty file at an offset.
        std::fs::write(&dp, b"0123456789").unwrap();
        let dst = OpenOptions::new().write(true).open(&dp).unwrap();
        let ino = dst.metadata().unwrap().ino();
        let Copied::Bytes(n) =
            copy_file_range(src.as_raw_fd(), 100, dst.as_raw_fd(), 5, 1000).unwrap()
        else {
            panic!("expected a byte copy")
        };
        assert_eq!(n, 1000);
        let got = std::fs::read(&dp).unwrap();
        assert_eq!(&got[..5], b"01234");
        assert_eq!(&got[5..], &content[100..1100]);
        assert_eq!(std::fs::metadata(&dp).unwrap().ino(), ino);

        // A partial range of the source into an empty file.
        let ep = dir.0.join("empty");
        let e = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&ep)
            .unwrap();
        let Copied::Bytes(n) = copy_file_range(src.as_raw_fd(), 0, e.as_raw_fd(), 0, 4096).unwrap()
        else {
            panic!("expected a byte copy")
        };
        assert_eq!(n, 4096);
        assert_eq!(std::fs::read(&ep).unwrap(), &content[..4096]);

        // Reading past the end copies what is there.
        let Copied::Bytes(n) =
            copy_file_range(src.as_raw_fd(), 199_990, e.as_raw_fd(), 4096, 100).unwrap()
        else {
            panic!("expected a byte copy")
        };
        assert_eq!(n, 10);
    }

    #[test]
    fn barrier_fsync_and_no_sync() {
        let dir = TempDir::new();
        let mut f = File::create(dir.0.join("f")).unwrap();
        f.write_all(b"data").unwrap();
        fsync(f.as_raw_fd(), false).unwrap();
        fsync(f.as_raw_fd(), true).unwrap();
        assert!(fsync(-1, false).is_err());
        assert!(fsync(-1, true).is_ok());
    }
}
