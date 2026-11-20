// Directory listing with getattrlistbulk(2): one call returns the names and attributes of a batch
// of entries, so READDIRPLUS does not need an lstat per entry.

use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::io;
use std::mem::{self, MaybeUninit};
use std::os::unix::io::RawFd;

use libc::{
    ATTR_BIT_MAP_COUNT, ATTR_CMN_ACCESSMASK, ATTR_CMN_ACCTIME, ATTR_CMN_CHGTIME, ATTR_CMN_DEVID,
    ATTR_CMN_FILEID, ATTR_CMN_FLAGS, ATTR_CMN_GRPID, ATTR_CMN_MODTIME, ATTR_CMN_NAME,
    ATTR_CMN_OBJTYPE, ATTR_CMN_OWNERID, ATTR_CMN_RETURNED_ATTRS, ATTR_CMNEXT_EXT_FLAGS,
    ATTR_DIR_ALLOCSIZE, ATTR_DIR_DATALENGTH, ATTR_DIR_ENTRYCOUNT, ATTR_DIR_IOBLOCKSIZE,
    ATTR_DIR_MOUNTSTATUS, ATTR_FILE_ALLOCSIZE, ATTR_FILE_DATALENGTH, ATTR_FILE_DEVTYPE,
    ATTR_FILE_IOBLOCKSIZE, ATTR_FILE_LINKCOUNT, DIR_MNTSTATUS_MNTPOINT, FSOPT_ATTR_CMN_EXTENDED,
    attrgroup_t, attrlist,
};

use super::super::super::linux_errno::linux_error;
use super::super::bindings;

// Not exported by the libc crate.
const ATTR_CMN_ERROR: attrgroup_t = 0x2000_0000;
const EF_NO_XATTRS: u64 = 0x2;
const SF_FIRMLINK: u32 = 0x0080_0000;

const VREG: u32 = 1;
const VDIR: u32 = 2;
const VBLK: u32 = 3;
const VCHR: u32 = 4;
const VLNK: u32 = 5;
const VSOCK: u32 = 6;
const VFIFO: u32 = 7;

const BUF_SIZE: usize = 65536;

thread_local! {
    // Reused across directory reads on the same thread; u64 keeps 8-byte alignment.
    static BULK_BUF: RefCell<Vec<u64>> =
        RefCell::new(vec![0u64; BUF_SIZE / mem::size_of::<u64>()]);
}

const BASIC_COMMON: attrgroup_t = ATTR_CMN_NAME | ATTR_CMN_OBJTYPE | ATTR_CMN_FILEID;

// Each group is packed in ascending bit order; the sizes follow getattrlist(2).
const COMMON: &[(attrgroup_t, usize)] = &[
    (ATTR_CMN_NAME, 8),
    (ATTR_CMN_DEVID, 4),
    (ATTR_CMN_OBJTYPE, 4),
    (ATTR_CMN_MODTIME, 16),
    (ATTR_CMN_CHGTIME, 16),
    (ATTR_CMN_ACCTIME, 16),
    (ATTR_CMN_OWNERID, 4),
    (ATTR_CMN_GRPID, 4),
    (ATTR_CMN_ACCESSMASK, 4),
    (ATTR_CMN_FLAGS, 4),
    (ATTR_CMN_FILEID, 8),
];
const DIR: &[(attrgroup_t, usize)] = &[
    (ATTR_DIR_ENTRYCOUNT, 4),
    (ATTR_DIR_MOUNTSTATUS, 4),
    (ATTR_DIR_ALLOCSIZE, 8),
    (ATTR_DIR_IOBLOCKSIZE, 4),
    (ATTR_DIR_DATALENGTH, 8),
];
const FILE: &[(attrgroup_t, usize)] = &[
    (ATTR_FILE_LINKCOUNT, 4),
    (ATTR_FILE_ALLOCSIZE, 8),
    (ATTR_FILE_IOBLOCKSIZE, 4),
    (ATTR_FILE_DEVTYPE, 4),
    (ATTR_FILE_DATALENGTH, 8),
];
const EXT: &[(attrgroup_t, usize)] = &[(ATTR_CMNEXT_EXT_FLAGS, 8)];

const STAT_COMMON: attrgroup_t = ATTR_CMN_DEVID
    | ATTR_CMN_OBJTYPE
    | ATTR_CMN_MODTIME
    | ATTR_CMN_CHGTIME
    | ATTR_CMN_ACCTIME
    | ATTR_CMN_OWNERID
    | ATTR_CMN_GRPID
    | ATTR_CMN_ACCESSMASK
    | ATTR_CMN_FLAGS
    | ATTR_CMN_FILEID;
const STAT_DIR: attrgroup_t = ATTR_DIR_ENTRYCOUNT
    | ATTR_DIR_MOUNTSTATUS
    | ATTR_DIR_ALLOCSIZE
    | ATTR_DIR_IOBLOCKSIZE
    | ATTR_DIR_DATALENGTH;
const STAT_FILE: attrgroup_t =
    ATTR_FILE_LINKCOUNT | ATTR_FILE_ALLOCSIZE | ATTR_FILE_IOBLOCKSIZE | ATTR_FILE_DATALENGTH;

fn mask(fields: &[(attrgroup_t, usize)]) -> attrgroup_t {
    fields.iter().fold(0, |m, (bit, _)| m | bit)
}

#[cfg(test)]
pub(super) struct BulkEntry {
    pub name: Box<[u8]>,
    pub ino: u64,
    pub type_: u8,
    /// The host's `lstat` of the entry, or `None` when it has to be looked up by path instead:
    /// mountpoints and firmlinks (whose listed attributes are those of the covered directory),
    /// entries that failed, and devices of unknown type.
    pub attr: Option<bindings::stat64>,
    /// The file has no extended attributes, so it has no ownership override either.
    pub no_xattrs: bool,
}

/// `getattrlistbulk` fills `st_nlink` of a directory from its entry count, which matches `lstat`
/// only on APFS. Other file systems are listed with readdir.
pub(super) fn supported(fd: RawFd) -> bool {
    let mut sfs = MaybeUninit::<libc::statfs>::zeroed();
    if unsafe { libc::fstatfs(fd, sfs.as_mut_ptr()) } != 0 {
        return false;
    }
    let sfs = unsafe { sfs.assume_init() };
    let name = unsafe { CStr::from_ptr(sfs.f_fstypename.as_ptr()) };
    name.to_bytes() == b"apfs"
}

/// Streams every entry of the directory open at `fd` (from its current offset) into `on_entry`.
/// When `need_attr` is false (plain `READDIR`), only requests name, object type and file id so
/// APFS does not have to fetch each child inode. `xattr_flags` (only with `need_attr`) also asks
/// which entries have no extended attributes.
pub(super) fn read_dir_with(
    fd: RawFd,
    need_attr: bool,
    xattr_flags: bool,
    mut on_entry: impl FnMut(&[u8], u64, u8, Option<bindings::stat64>, bool),
) -> io::Result<()> {
    let mut attrs = if need_attr {
        attrlist {
            bitmapcount: ATTR_BIT_MAP_COUNT,
            reserved: 0,
            commonattr: ATTR_CMN_RETURNED_ATTRS | ATTR_CMN_ERROR | mask(COMMON),
            volattr: 0,
            dirattr: mask(DIR),
            fileattr: mask(FILE),
            forkattr: if xattr_flags { mask(EXT) } else { 0 },
        }
    } else {
        attrlist {
            bitmapcount: ATTR_BIT_MAP_COUNT,
            reserved: 0,
            commonattr: ATTR_CMN_RETURNED_ATTRS | ATTR_CMN_ERROR | BASIC_COMMON,
            volattr: 0,
            dirattr: 0,
            fileattr: 0,
            forkattr: 0,
        }
    };
    let options = if need_attr && xattr_flags {
        FSOPT_ATTR_CMN_EXTENDED
    } else {
        0
    };

    BULK_BUF.with(|cell| {
        let mut buf = cell.borrow_mut();
        loop {
            let count = unsafe {
                libc::getattrlistbulk(
                    fd,
                    (&mut attrs as *mut attrlist).cast(),
                    buf.as_mut_ptr().cast(),
                    BUF_SIZE,
                    u64::from(options),
                )
            };
            if count < 0 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(linux_error(err));
            }
            if count == 0 {
                return Ok(());
            }

            let bytes: &[u8] =
                unsafe { std::slice::from_raw_parts(buf.as_ptr().cast(), BUF_SIZE) };
            let mut offset = 0;
            for _ in 0..count {
                let len = read_u32(bytes, offset).ok_or_else(eio)? as usize;
                if len == 0 {
                    return Err(eio());
                }
                let record = bytes.get(offset..offset + len).ok_or_else(eio)?;
                if need_attr {
                    if let Some((name, ino, type_, attr, no_xattrs)) = parse_full(fd, record) {
                        on_entry(name, ino, type_, attr, no_xattrs);
                    }
                } else if let Some((name, ino, type_)) = parse_basic(fd, record) {
                    on_entry(name, ino, type_, None, false);
                }
                offset += len;
            }
        }
    })
}

#[cfg(test)]
pub(super) fn read_dir(
    fd: RawFd,
    xattr_flags: bool,
    entries: &mut Vec<BulkEntry>,
) -> io::Result<()> {
    read_dir_with(fd, true, xattr_flags, |name, ino, type_, attr, no_xattrs| {
        entries.push(BulkEntry {
            name: name.into(),
            ino,
            type_,
            attr,
            no_xattrs,
        });
    })
}

fn eio() -> io::Error {
    linux_error(io::Error::from_raw_os_error(libc::EIO))
}

fn read_u32(buf: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_ne_bytes(buf.get(at..at + 4)?.try_into().ok()?))
}

fn read_u64(buf: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_ne_bytes(buf.get(at..at + 8)?.try_into().ok()?))
}

/// The attribute values of one record, by bit, in the order of the group tables.
struct Group {
    offsets: [Option<usize>; 16],
}

impl Group {
    fn walk(fields: &[(attrgroup_t, usize)], returned: attrgroup_t, pos: &mut usize) -> Self {
        let mut offsets = [None; 16];
        for (i, (bit, size)) in fields.iter().enumerate() {
            if returned & bit != 0 {
                offsets[i] = Some(*pos);
                *pos += size;
            }
        }
        Group { offsets }
    }

    fn at(&self, fields: &[(attrgroup_t, usize)], bit: attrgroup_t) -> Option<usize> {
        let i = fields.iter().position(|(b, _)| *b == bit)?;
        self.offsets[i]
    }
}

fn objtype_to_dt(objtype: Option<u32>) -> u8 {
    match objtype {
        Some(VREG) => libc::DT_REG,
        Some(VDIR) => libc::DT_DIR,
        Some(VBLK) => libc::DT_BLK,
        Some(VCHR) => libc::DT_CHR,
        Some(VLNK) => libc::DT_LNK,
        Some(VSOCK) => libc::DT_SOCK,
        Some(VFIFO) => libc::DT_FIFO,
        _ => libc::DT_UNKNOWN,
    }
}

fn parse_basic(dirfd: RawFd, record: &[u8]) -> Option<(&[u8], u64, u8)> {
    let commonattr = read_u32(record, 4)?;
    let mut pos = 4 + mem::size_of::<libc::attribute_set_t>();
    if commonattr & ATTR_CMN_ERROR != 0 {
        pos += 4;
    }
    if commonattr & ATTR_CMN_NAME == 0 {
        return None;
    }
    let name_at = pos;
    let name_offset = read_u32(record, name_at)? as i32;
    let name_len = read_u32(record, name_at + 4)? as usize;
    pos += 8;
    let name_start = name_at.checked_add_signed(name_offset as isize)?;
    let name = record.get(name_start..name_start + name_len)?;
    let name = name.split(|c| *c == 0).next()?;
    if name.is_empty() || name == b"." || name == b".." {
        return None;
    }
    let objtype = if commonattr & ATTR_CMN_OBJTYPE != 0 {
        let v = read_u32(record, pos)?;
        pos += 4;
        Some(v)
    } else {
        None
    };
    let ino = if commonattr & ATTR_CMN_FILEID != 0 {
        read_u64(record, pos)?
    } else {
        fstatat_ino(dirfd, name).unwrap_or(0)
    };
    Some((name, ino, objtype_to_dt(objtype)))
}

fn parse_full(dirfd: RawFd, record: &[u8]) -> Option<(&[u8], u64, u8, Option<bindings::stat64>, bool)> {
    let returned = libc::attribute_set_t {
        commonattr: read_u32(record, 4)?,
        volattr: read_u32(record, 8)?,
        dirattr: read_u32(record, 12)?,
        fileattr: read_u32(record, 16)?,
        forkattr: read_u32(record, 20)?,
    };
    let mut pos = 4 + mem::size_of::<libc::attribute_set_t>();
    let mut error = 0;
    if returned.commonattr & ATTR_CMN_ERROR != 0 {
        error = read_u32(record, pos)?;
        pos += 4;
    }

    let common = Group::walk(COMMON, returned.commonattr, &mut pos);
    let dir = Group::walk(DIR, returned.dirattr, &mut pos);
    let file = Group::walk(FILE, returned.fileattr, &mut pos);
    let ext = Group::walk(EXT, returned.forkattr, &mut pos);

    let c32 = |bit| read_u32(record, common.at(COMMON, bit)?);
    let c64 = |bit| read_u64(record, common.at(COMMON, bit)?);
    let time = |bit| {
        let at = common.at(COMMON, bit)?;
        Some((
            read_u64(record, at)? as i64,
            read_u64(record, at + 8)? as i64,
        ))
    };

    let name_at = common.at(COMMON, ATTR_CMN_NAME)?;
    let name_offset = read_u32(record, name_at)? as i32;
    let name_len = read_u32(record, name_at + 4)? as usize;
    let name_start = name_at.checked_add_signed(name_offset as isize)?;
    let name = record.get(name_start..name_start + name_len)?;
    let name = name.split(|c| *c == 0).next()?;
    if name.is_empty() || name == b"." || name == b".." {
        return None;
    }

    let objtype = c32(ATTR_CMN_OBJTYPE);
    let type_ = objtype_to_dt(objtype);
    let ino = match c64(ATTR_CMN_FILEID) {
        Some(ino) => ino,
        None => fstatat_ino(dirfd, name).unwrap_or(0),
    };
    let no_xattrs = ext
        .at(EXT, ATTR_CMNEXT_EXT_FLAGS)
        .and_then(|at| read_u64(record, at))
        .is_some_and(|flags| flags & EF_NO_XATTRS != 0);

    let attr = (|| {
        if error != 0 || returned.commonattr & STAT_COMMON != STAT_COMMON {
            return None;
        }
        let flags = c32(ATTR_CMN_FLAGS)?;
        if flags & SF_FIRMLINK != 0 {
            return None;
        }

        let mut st: bindings::stat64 = unsafe { mem::zeroed() };
        st.st_dev = c32(ATTR_CMN_DEVID)? as i32;
        st.st_ino = ino;
        st.st_uid = c32(ATTR_CMN_OWNERID)?;
        st.st_gid = c32(ATTR_CMN_GRPID)?;
        st.st_flags = flags;
        (st.st_mtime, st.st_mtime_nsec) = time(ATTR_CMN_MODTIME)?;
        (st.st_ctime, st.st_ctime_nsec) = time(ATTR_CMN_CHGTIME)?;
        (st.st_atime, st.st_atime_nsec) = time(ATTR_CMN_ACCTIME)?;
        let perm = c32(ATTR_CMN_ACCESSMASK)? as u16 & 0o7777;

        let alloc = if objtype == Some(VDIR) {
            if returned.dirattr & STAT_DIR != STAT_DIR {
                return None;
            }
            let d32 = |bit| read_u32(record, dir.at(DIR, bit)?);
            let d64 = |bit| read_u64(record, dir.at(DIR, bit)?);
            if d32(ATTR_DIR_MOUNTSTATUS)? & DIR_MNTSTATUS_MNTPOINT != 0 {
                return None;
            }
            st.st_mode = libc::S_IFDIR | perm;
            // APFS counts a directory's links as its entries plus "." and "..".
            st.st_nlink = (u64::from(d32(ATTR_DIR_ENTRYCOUNT)?) + 2).min(u16::MAX.into()) as u16;
            st.st_blksize = d32(ATTR_DIR_IOBLOCKSIZE)? as i32;
            st.st_size = d64(ATTR_DIR_DATALENGTH)? as i64;
            d64(ATTR_DIR_ALLOCSIZE)?
        } else {
            if returned.fileattr & STAT_FILE != STAT_FILE {
                return None;
            }
            let f32 = |bit| read_u32(record, file.at(FILE, bit)?);
            let f64 = |bit| read_u64(record, file.at(FILE, bit)?);
            let fmt = match objtype? {
                VREG => libc::S_IFREG,
                VLNK => libc::S_IFLNK,
                VSOCK => libc::S_IFSOCK,
                VFIFO => libc::S_IFIFO,
                VBLK => libc::S_IFBLK,
                VCHR => libc::S_IFCHR,
                _ => return None,
            };
            if fmt == libc::S_IFBLK || fmt == libc::S_IFCHR {
                st.st_rdev = f32(ATTR_FILE_DEVTYPE)? as i32;
            }
            st.st_mode = fmt | perm;
            st.st_nlink = f32(ATTR_FILE_LINKCOUNT)?.min(u16::MAX.into()) as u16;
            st.st_blksize = f32(ATTR_FILE_IOBLOCKSIZE)? as i32;
            st.st_size = f64(ATTR_FILE_DATALENGTH)? as i64;
            f64(ATTR_FILE_ALLOCSIZE)?
        };
        st.st_blocks = alloc.div_ceil(512) as i64;
        Some(st)
    })();

    Some((name, ino, type_, attr, no_xattrs))
}

#[cfg(test)]
fn parse(dirfd: RawFd, record: &[u8]) -> Option<BulkEntry> {
    parse_full(dirfd, record).map(|(name, ino, type_, attr, no_xattrs)| BulkEntry {
        name: name.into(),
        ino,
        type_,
        attr,
        no_xattrs,
    })
}

fn fstatat_ino(dirfd: RawFd, name: &[u8]) -> Option<u64> {
    let name = CString::new(name).ok()?;
    let mut st = MaybeUninit::<libc::stat>::zeroed();
    let res = unsafe {
        libc::fstatat(
            dirfd,
            name.as_ptr(),
            st.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    (res == 0).then(|| unsafe { st.assume_init() }.st_ino)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, File};
    use std::os::unix::io::AsRawFd;
    use std::path::Path;

    #[test]
    fn a_failed_entry_keeps_its_name_without_attributes() {
        let returned = ATTR_CMN_RETURNED_ATTRS | ATTR_CMN_ERROR | ATTR_CMN_NAME;
        let mut record = Vec::new();
        record.extend(40u32.to_ne_bytes());
        for group in [returned, 0, 0, 0, 0] {
            record.extend(group.to_ne_bytes());
        }
        record.extend((libc::ENOENT as u32).to_ne_bytes());
        // The name reference sits at offset 28 and points at offset 36.
        record.extend(8i32.to_ne_bytes());
        record.extend(5u32.to_ne_bytes());
        record.extend(b"gone\0\0\0\0");

        let dir = File::open(std::env::temp_dir()).unwrap();
        let entry = parse(dir.as_raw_fd(), &record).unwrap();
        assert_eq!(&*entry.name, b"gone");
        assert_eq!(entry.type_, libc::DT_UNKNOWN);
        assert_eq!(entry.ino, 0);
        assert!(entry.attr.is_none());
        assert!(!entry.no_xattrs);
    }

    fn compare_tree(dir: &Path, checked: &mut usize, fallbacks: &mut usize) {
        let fd = File::open(dir).unwrap();
        let mut entries = Vec::new();
        read_dir(fd.as_raw_fd(), true, &mut entries).unwrap();
        for e in entries {
            let path = dir.join(std::ffi::OsStr::from_bytes(&e.name));
            let host = fs::symlink_metadata(&path).unwrap();
            let Some(st) = e.attr else {
                *fallbacks += 1;
                continue;
            };
            let got = (
                st.st_dev as u64,
                st.st_ino,
                u32::from(st.st_mode),
                u64::from(st.st_nlink),
                st.st_uid,
                st.st_gid,
                st.st_size as u64,
                st.st_blocks as u64,
                st.st_blksize as u64,
                (st.st_mtime, st.st_mtime_nsec),
                (st.st_ctime, st.st_ctime_nsec),
            );
            let want = (
                host.dev(),
                host.ino(),
                host.mode(),
                host.nlink(),
                host.uid(),
                host.gid(),
                host.size(),
                host.blocks(),
                host.blksize(),
                (host.mtime(), host.mtime_nsec()),
                (host.ctime(), host.ctime_nsec()),
            );
            assert_eq!(got, want, "{}", path.display());
            *checked += 1;
            if host.is_dir() {
                compare_tree(&path, checked, fallbacks);
            }
        }
    }

    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::MetadataExt;

    /// KAT_BULK_TREE=<dir> cargo test -- --ignored compares every entry under <dir> with lstat.
    #[test]
    #[ignore]
    fn bulk_attributes_match_lstat_over_a_tree() {
        let root = std::env::var("KAT_BULK_TREE").expect("KAT_BULK_TREE");
        let (mut checked, mut fallbacks) = (0, 0);
        compare_tree(Path::new(&root), &mut checked, &mut fallbacks);
        eprintln!("{root}: {checked} entries match lstat, {fallbacks} fall back to lookup");
    }
}
