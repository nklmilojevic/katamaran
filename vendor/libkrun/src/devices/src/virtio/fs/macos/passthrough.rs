// Copyright 2019 The Chromium OS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

use std::collections::HashMap;
use std::env;
use std::ffi::{CStr, CString};
use std::fs::File;
use std::io;
use std::mem::MaybeUninit;
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};
use std::os::unix::net::UnixListener;
use std::path::Path;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::Duration;

use crossbeam_channel::{Sender, unbounded};
use nix::errno::Errno;
use utils::worker_message::WorkerMessage;

use crate::virtio::fs::filesystem::SecContext;

use super::super::super::linux_errno::{LINUX_ERANGE, linux_error};
use super::super::bindings;
use super::super::filesystem::{
    Context, DirEntry, Entry, ExportTable, Extensions, FileSystem, FsOptions, GetxattrReply,
    ListxattrReply, OpenOptions, SetattrValid, ZeroCopyReader, ZeroCopyWriter,
};
use super::super::fuse;
use super::super::inode_alloc::InodeAllocator;
use super::super::multikey::MultikeyBTreeMap;
pub use super::super::{CachePolicy, PermissionSemantics};
use super::attrlist;
use super::guest_cache::InodeTable;
pub use super::guest_cache::{CachedNode, GuestCache, GuestCacheView};
use super::handles::HandleTable;
use super::pins::{Pin, PinPool, PinSlot};
use super::{fileops, syncopen};

const XATTR_KEY: &[u8] = b"user.containers.override_stat\0";
const SECURITY_CAPABILITY: &[u8] = b"security.capability\0";

const MACOS_XATTR_PREFIX: &[u8] = b"com.apple.";

const UID_MAX: u32 = u32::MAX - 1;

type Inode = u64;
type Handle = u64;

#[derive(Clone, Copy, PartialOrd, Ord, PartialEq, Eq)]
pub(super) struct InodeAltKey {
    pub(super) ino: u64,
    pub(super) dev: i32,
}

pub(super) struct InodeData {
    pub(super) inode: Inode,
    ino: u64,
    dev: i32,
    refcount: AtomicU64,
    unlinked_fd: AtomicI64,
    // For regular files: shared read-only host file across open handles.
    // For directories: weak handle to an active opendir fd, borrowed for *at() calls.
    read_only_file: Mutex<Weak<RwLock<File>>>,
    pub(super) kind: u16,
    pin: Option<Arc<PinSlot>>,
    // The parent and name this inode was last found by. Opens try it before the /.vol path and
    // check the identity of what they get, since the name may have moved to another file since.
    pub(super) anchor: Mutex<Option<(Inode, CString)>>,
    // Lock-free copy of `anchor`'s parent inode (0 when unset), used to skip keep_if_open.
    anchor_parent: AtomicU64,
    // Set when the non-directory inode was observed with st_nlink > 1 or hardlinked via FUSE.
    hardlinked: AtomicBool,
    open_handles: AtomicU32,
    // One more than the host mtime (ns) at the last open (or dir stamp for directories),
    // zero before the first: an open of an unchanged file/directory keeps the guest's cache.
    open_mtime: AtomicU64,
}

impl InodeData {
    fn new(inode: Inode, st: &bindings::stat64, refcount: u64) -> Self {
        let kind = st.st_mode & libc::S_IFMT;
        InodeData {
            inode,
            ino: st.st_ino,
            dev: st.st_dev,
            refcount: AtomicU64::new(refcount),
            unlinked_fd: AtomicI64::new(-1),
            read_only_file: Mutex::new(Weak::new()),
            kind,
            pin: (kind == libc::S_IFDIR).then(Arc::default),
            anchor: Mutex::new(None),
            anchor_parent: AtomicU64::new(0),
            hardlinked: AtomicBool::new(kind != libc::S_IFDIR && st.st_nlink > 1),
            open_handles: AtomicU32::new(0),
            open_mtime: AtomicU64::new(0),
        }
    }

    fn set_anchor(&self, parent: Inode, name: &CStr) -> bool {
        let mut anchor = self.anchor.lock().unwrap();
        if anchor
            .as_ref()
            .is_none_or(|(p, n)| *p != parent || n.as_c_str() != name)
        {
            *anchor = Some((parent, name.to_owned()));
            self.anchor_parent.store(parent, Ordering::Release);
            return true;
        }
        false
    }

    fn active_dir_file(&self) -> Option<Arc<RwLock<File>>> {
        if self.kind != libc::S_IFDIR || self.unlinked_fd.load(Ordering::Acquire) >= 0 {
            return None;
        }
        self.read_only_file.lock().unwrap().upgrade()
    }
}

/// Tracks open handles by directory vs non-directory so `unlink`, `rmdir`, and `rename` can
/// skip `keep_if_open` (which costs an extra `fstatat` syscall) when no open handle can refer
/// to the target.
struct OpenTracker {
    non_dirs: AtomicUsize,
    dirs: AtomicUsize,
    open_non_dir_inodes: RwLock<Vec<Arc<InodeData>>>,
    open_dir_inodes: RwLock<Vec<Arc<InodeData>>>,
}

impl OpenTracker {
    fn new() -> Arc<Self> {
        Arc::new(OpenTracker {
            non_dirs: AtomicUsize::new(0),
            dirs: AtomicUsize::new(0),
            open_non_dir_inodes: RwLock::new(Vec::new()),
            open_dir_inodes: RwLock::new(Vec::new()),
        })
    }

    fn register(&self, node: &Arc<InodeData>) {
        let is_dir = node.kind == libc::S_IFDIR;
        let (counter, list) = if is_dir {
            (&self.dirs, &self.open_dir_inodes)
        } else {
            (&self.non_dirs, &self.open_non_dir_inodes)
        };
        if node.open_handles.fetch_add(1, Ordering::AcqRel) == 0 {
            let mut guard = list.write().unwrap();
            if node.open_handles.load(Ordering::Acquire) > 0
                && !guard.iter().any(|n| Arc::ptr_eq(n, node))
            {
                guard.push(node.clone());
            }
        }
        counter.fetch_add(1, Ordering::Release);
    }

    fn unregister(&self, node: &Arc<InodeData>) {
        let is_dir = node.kind == libc::S_IFDIR;
        let (counter, list) = if is_dir {
            (&self.dirs, &self.open_dir_inodes)
        } else {
            (&self.non_dirs, &self.open_non_dir_inodes)
        };
        counter.fetch_sub(1, Ordering::AcqRel);
        if node.open_handles.fetch_sub(1, Ordering::AcqRel) == 1 {
            let mut guard = list.write().unwrap();
            if node.open_handles.load(Ordering::Acquire) == 0
                && let Some(pos) = guard.iter().position(|n| Arc::ptr_eq(n, node))
            {
                guard.swap_remove(pos);
            }
        }
    }

    /// True if any currently-open handle might refer to a child of `parent` (directory if
    /// `is_dir`, non-directory otherwise). False guarantees `keep_if_open` would return `None`.
    fn may_have_open_target(&self, parent: Inode, is_dir: bool) -> bool {
        if is_dir {
            if self.dirs.load(Ordering::Acquire) == 0 {
                return false;
            }
            let guard = self.open_dir_inodes.read().unwrap();
            guard.iter().any(|n| {
                let p = n.anchor_parent.load(Ordering::Acquire);
                p == 0 || p == parent
            })
        } else {
            if self.non_dirs.load(Ordering::Acquire) == 0 {
                return false;
            }
            let guard = self.open_non_dir_inodes.read().unwrap();
            guard.iter().any(|n| {
                if n.hardlinked.load(Ordering::Acquire) {
                    return true;
                }
                let p = n.anchor_parent.load(Ordering::Acquire);
                p == 0 || p == parent
            })
        }
    }
}

/// Calls `f` with a NUL-terminated `"/.vol/{dev}/{ino}"` C string on the stack.
fn with_vol_path<R>(dev: i32, ino: u64, f: impl FnOnce(&CStr) -> R) -> R {
    use std::io::Write;
    let mut buf = [0u8; 64];
    let mut cur = io::Cursor::new(&mut buf[..]);
    let _ = write!(cur, "/.vol/{dev}/{ino}\0");
    let len = cur.position() as usize;
    let cstr = CStr::from_bytes_with_nul(&buf[..len]).unwrap();
    f(cstr)
}

/// Calls `f` with a NUL-terminated `"/.vol/{dev}/{ino}/{name}"` C string, using a stack buffer
/// for names that fit.
fn with_vol_child_path<R>(
    dev: i32,
    ino: u64,
    name: &CStr,
    f: impl FnOnce(&CStr) -> R,
) -> io::Result<R> {
    use std::io::Write;
    let name_bytes = name.to_bytes_with_nul();
    let mut buf = [0u8; 512];
    let mut cur = io::Cursor::new(&mut buf[..]);
    let _ = write!(cur, "/.vol/{dev}/{ino}/");
    let prefix_len = cur.position() as usize;
    if prefix_len + name_bytes.len() <= buf.len() {
        buf[prefix_len..prefix_len + name_bytes.len()].copy_from_slice(name_bytes);
        let cstr = CStr::from_bytes_with_nul(&buf[..prefix_len + name_bytes.len()])
            .map_err(|_| einval())?;
        Ok(f(cstr))
    } else {
        let mut heap = Vec::with_capacity(prefix_len + name_bytes.len());
        heap.extend_from_slice(&buf[..prefix_len]);
        heap.extend_from_slice(name_bytes);
        let cstr = CStr::from_bytes_with_nul(&heap).map_err(|_| einval())?;
        Ok(f(cstr))
    }
}

/// Calls `f` with a NUL-terminated `CStr` copy of `name` on the stack for typical filenames.
fn with_c_name<R>(name: &[u8], f: impl FnOnce(&CStr) -> io::Result<R>) -> io::Result<R> {
    if name.contains(&0) {
        return Err(einval());
    }
    let mut buf = [0u8; 260];
    if name.len() < buf.len() {
        buf[..name.len()].copy_from_slice(name);
        buf[name.len()] = 0;
        let cstr = CStr::from_bytes_with_nul(&buf[..name.len() + 1]).map_err(|_| einval())?;
        f(cstr)
    } else {
        let cstr = CString::new(name).map_err(|_| einval())?;
        f(&cstr)
    }
}

enum InodeHandle {
    Fd(RawFd),
    Path(CString),
}

struct CachedDirEntry {
    ino: bindings::ino64_t,
    name_start: u32,
    name_len: u32,
    type_: u8,
    // Host attributes from getattrlistbulk, before the ownership policy applies.
    attr: Option<bindings::stat64>,
    no_xattrs: bool,
}

struct DirStream {
    entries: Vec<CachedDirEntry>,
    names: Vec<u8>,
    ready: bool,
    rewound: bool,
}

impl DirStream {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            names: Vec::new(),
            ready: false,
            rewound: true,
        }
    }

    fn push_entry(
        &mut self,
        name: &[u8],
        ino: bindings::ino64_t,
        type_: u8,
        attr: Option<bindings::stat64>,
        no_xattrs: bool,
    ) {
        let name_start = self.names.len() as u32;
        self.names.extend_from_slice(name);
        self.entries.push(CachedDirEntry {
            ino,
            name_start,
            name_len: name.len() as u32,
            type_,
            attr,
            no_xattrs,
        });
    }

    fn get_entry<'a>(&'a self, offset: u64) -> Option<(DirEntry<'a>, &'a CachedDirEntry)> {
        self.entries.get(offset as usize).map(|e| {
            let start = e.name_start as usize;
            let end = start + e.name_len as usize;
            let entry = DirEntry {
                ino: e.ino,
                // offset points to the next entry, not the current one
                offset: offset + 1,
                type_: u32::from(e.type_),
                name: &self.names[start..end],
            };
            (entry, e)
        })
    }

    // Reads the whole directory from its start. getattrlistbulk cannot seek, so the stream keeps
    // every entry and serves offsets as indexes into them.
    fn fill(
        &mut self,
        fd: RawFd,
        use_bulk: bool,
        need_attr: bool,
        xattr_flags: bool,
    ) -> io::Result<()> {
        if !self.rewound {
            unsafe { libc::lseek(fd, 0, libc::SEEK_SET) };
        }
        self.rewound = false;
        if !use_bulk {
            return self.fill_from_fd(fd);
        }
        attrlist::read_dir_with(fd, need_attr, xattr_flags, |name, ino, type_, attr, no_xattrs| {
            self.push_entry(name, ino, type_, attr, no_xattrs);
        })
    }

    fn fill_from_fd(&mut self, fd: RawFd) -> io::Result<()> {
        // fdopendir() takes ownership of the fd, so we need to obtain a new one
        // to be donated.
        let newfd = unsafe { libc::dup(fd) };
        if newfd < 0 {
            return Err(linux_error(io::Error::last_os_error()));
        }
        let dir = unsafe { libc::fdopendir(newfd) };
        if dir.is_null() {
            let err = io::Error::last_os_error();
            let _ = unsafe { libc::close(newfd) };
            return Err(linux_error(err));
        }

        loop {
            // To detect if error happened in readdir we should clear errno
            // before the call and then verify it after
            Errno::clear();
            let dentry = unsafe { libc::readdir(dir) };
            if dentry.is_null() {
                let errno = Errno::last_raw();
                if errno != 0 {
                    let err = io::Error::from_raw_os_error(errno);
                    let _ = unsafe { libc::closedir(dir) };
                    // Error happened in readdir, but we keep the entries we
                    // already read to handle the partial read.
                    return Err(linux_error(err));
                }
                break;
            }
            // SAFETY: dentry is not null.
            let name = unsafe {
                let name_len = usize::from((*dentry).d_namlen);
                let name_ptr = (*dentry).d_name.as_ptr().cast();
                let name = std::slice::from_raw_parts(name_ptr, name_len);

                if name == b"." || name == b".." {
                    continue;
                }
                name
            };

            // SAFETY: dentry is not null.
            let ino = unsafe { (*dentry).d_ino };
            // SAFETY: dentry is not null. The entry types use the same
            // exact constants (`libc::DT_*`) on macOS, Linux, and FUSE.
            let type_ = unsafe { (*dentry).d_type };

            self.push_entry(name, ino, type_, None, false);
        }

        unsafe { libc::closedir(dir) };
        Ok(())
    }
}

struct HandleData {
    inode: Inode,
    node: Arc<InodeData>,
    tracker: Arc<OpenTracker>,
    file: Arc<RwLock<File>>,
    dirstream: Mutex<DirStream>,
}

impl HandleData {
    fn new(node: Arc<InodeData>, tracker: Arc<OpenTracker>, file: Arc<RwLock<File>>) -> Self {
        tracker.register(&node);
        HandleData {
            inode: node.inode,
            node,
            tracker,
            file,
            dirstream: Mutex::new(DirStream::new()),
        }
    }
}

impl Drop for HandleData {
    fn drop(&mut self) {
        self.tracker.unregister(&self.node);
    }
}

fn ebadf() -> io::Error {
    linux_error(io::Error::from_raw_os_error(libc::EBADF))
}

fn einval() -> io::Error {
    linux_error(io::Error::from_raw_os_error(libc::EINVAL))
}

fn item_to_value(item: &[u8], radix: u32) -> Option<u32> {
    match std::str::from_utf8(item) {
        Ok(val) => match u32::from_str_radix(val, radix) {
            Ok(i) => Some(i),
            Err(e) => {
                debug!("invalid value: {radix} err={e}");
                None
            }
        },
        Err(_) => None,
    }
}

fn get_xattr_common(buf: &[u8]) -> io::Result<(Option<u32>, Option<u32>, Option<u32>)> {
    let mut items = buf.split(|c| *c == b':');

    let uid = match items.next() {
        Some(item) => item_to_value(item, 10),
        None => None,
    };
    let gid = match items.next() {
        Some(item) => item_to_value(item, 10),
        None => None,
    };
    let mode = match items.next() {
        Some(item) => item_to_value(item, 8),
        None => None,
    };

    Ok((uid, gid, mode))
}

fn get_xattr_fstat(
    fd: RawFd,
    st: bindings::stat64,
) -> io::Result<(Option<u32>, Option<u32>, Option<u32>)> {
    let mut buf: Vec<u8> = vec![0; 32];
    let options = if (st.st_mode & libc::S_IFMT) == libc::S_IFLNK {
        libc::XATTR_NOFOLLOW
    } else {
        0
    };
    let res = unsafe {
        libc::fgetxattr(
            fd,
            XATTR_KEY.as_ptr() as *const i8,
            buf.as_mut_ptr() as *mut libc::c_void,
            buf.len(),
            0,
            options,
        )
    };
    if res < 0 {
        debug!("fget_xattr error: {res}");
        return Ok((None, None, None));
    }

    buf.resize(res as usize, 0);

    get_xattr_common(&buf)
}

fn get_xattr_lstat(
    path: &CString,
    st: bindings::stat64,
) -> io::Result<(Option<u32>, Option<u32>, Option<u32>)> {
    let mut buf: Vec<u8> = vec![0; 32];
    let options = if (st.st_mode & libc::S_IFMT) == libc::S_IFLNK {
        libc::XATTR_NOFOLLOW
    } else {
        0
    };
    let res = unsafe {
        libc::getxattr(
            path.as_ptr(),
            XATTR_KEY.as_ptr() as *const i8,
            buf.as_mut_ptr() as *mut libc::c_void,
            buf.len(),
            0,
            options,
        )
    };
    if res < 0 {
        debug!("fget_xattr error: {res}");
        return Ok((None, None, None));
    }

    buf.resize(res as usize, 0);

    get_xattr_common(&buf)
}

fn is_valid_owner(owner: Option<(u32, u32)>) -> bool {
    if let Some(owner) = owner
        && owner.0 < UID_MAX
        && owner.1 < UID_MAX
    {
        return true;
    }

    false
}

// We won't need this once expressions like "if let ... &&" are allowed.
#[allow(clippy::unnecessary_unwrap)]
fn set_xattr_stat(
    ctx: &Context,
    file: &InodeHandle,
    st: Option<bindings::stat64>,
    owner: Option<(u32, u32)>,
    mode: Option<u32>,
) -> io::Result<()> {
    let st = st.unwrap_or(istat(ctx, Ownership::Xattr, file, true)?);
    let options = if (st.st_mode & libc::S_IFMT) == libc::S_IFLNK {
        libc::XATTR_NOFOLLOW
    } else {
        0
    };

    let buf = if is_valid_owner(owner) && mode.is_some() {
        let owner = owner.unwrap();
        let mode = mode.unwrap();
        format!("{}:{}:0{:o}", owner.0, owner.1, mode)
    } else {
        let (orig_uid, orig_gid, orig_mode) = match file {
            InodeHandle::Fd(fd) => get_xattr_fstat(*fd, st)?,
            InodeHandle::Path(c_path) => get_xattr_lstat(c_path, st)?,
        };

        let (uid, gid) = match owner {
            Some(o) => {
                let uid = if o.0 < UID_MAX { Some(o.0) } else { orig_uid };
                let gid = if o.1 < UID_MAX { Some(o.1) } else { orig_gid };
                (uid, gid)
            }
            None => (orig_uid, orig_gid),
        };

        let mut buf = String::new();
        if let Some(uid) = uid {
            buf.push_str(&format!("{uid}"));
        } else {
            buf.push('x');
        }
        if let Some(gid) = gid {
            buf.push_str(&format!(":{gid}:"));
        } else {
            buf.push_str(":x:");
        }
        if let Some(mode) = mode {
            buf.push_str(&format!("0{:o}", mode));
        } else if let Some(orig_mode) = orig_mode {
            buf.push_str(&format!("0{:o}", orig_mode));
        } else {
            buf.push('x');
        }
        buf
    };

    let res = match file {
        InodeHandle::Path(path) => unsafe {
            libc::setxattr(
                path.as_ptr(),
                XATTR_KEY.as_ptr() as *const i8,
                buf.as_ptr() as *mut libc::c_void,
                buf.len() as libc::size_t,
                0,
                options,
            )
        },
        InodeHandle::Fd(fd) => unsafe {
            libc::fsetxattr(
                *fd,
                XATTR_KEY.as_ptr() as *const i8,
                buf.as_ptr() as *mut libc::c_void,
                buf.len() as libc::size_t,
                0,
                options,
            )
        },
    };

    if res < 0 {
        Err(linux_error(io::Error::last_os_error()))
    } else {
        Ok(())
    }
}

fn set_host_stat(
    file: &InodeHandle,
    _owner: Option<(u32, u32)>,
    mode: Option<u32>,
) -> io::Result<()> {
    // We're only using set_host_stat for LinuxSimplified semantics, and in this
    // mode we ignore the host's owner bits, so don't attempt to write them here.

    if let Some(mode) = mode {
        let res = match file {
            InodeHandle::Path(path) => unsafe { libc::chmod(path.as_ptr(), mode as u16) },
            InodeHandle::Fd(fd) => unsafe { libc::fchmod(*fd, mode as u16) },
        };

        if res < 0 {
            return Err(linux_error(io::Error::last_os_error()));
        }
    }

    Ok(())
}

fn set_stat(
    ctx: &Context,
    semantics: PermissionSemantics,
    file: &InodeHandle,
    st: Option<bindings::stat64>,
    owner: Option<(u32, u32)>,
    mode: Option<u32>,
) -> io::Result<()> {
    match semantics {
        PermissionSemantics::LinuxComplete => set_xattr_stat(ctx, file, st, owner, mode),
        PermissionSemantics::LinuxSimplified | PermissionSemantics::LinuxSimplifiedFixedOwner => {
            set_host_stat(file, owner, mode)
        }
    }
}

/// Where the ownership that the guest sees comes from.
#[derive(Debug, Clone, Copy)]
enum Ownership {
    Xattr,
    Requester,
    Fixed(u32, u32),
}

fn stat_xattr_common(
    st: &mut bindings::stat64,
    uid: Option<u32>,
    gid: Option<u32>,
    mode: Option<u32>,
) -> io::Result<bindings::stat64> {
    if let Some(uid) = uid {
        st.st_uid = uid;
    }
    if let Some(gid) = gid {
        st.st_gid = gid;
    }
    if let Some(mode) = mode {
        if mode as u16 & libc::S_IFMT == 0 {
            st.st_mode = (st.st_mode & libc::S_IFMT) | mode as u16;
        } else {
            st.st_mode = mode as u16;
        }
    }

    Ok(*st)
}

fn fstat(
    ctx: &Context,
    ownership: Ownership,
    fd: RawFd,
    host: bool,
) -> io::Result<bindings::stat64> {
    let mut st = MaybeUninit::<bindings::stat64>::zeroed();

    // Safe because the kernel will only write data in `st` and we check the return
    // value.
    let res = unsafe { libc::fstat(fd, st.as_mut_ptr()) };
    if res >= 0 {
        // Safe because the kernel guarantees that the struct is now fully initialized.
        let mut st = unsafe { st.assume_init() };
        if !host {
            match ownership {
                Ownership::Xattr => {
                    let (uid, gid, mode) = get_xattr_fstat(fd, st)?;
                    stat_xattr_common(&mut st, uid, gid, mode)
                }
                Ownership::Requester => {
                    st.st_uid = ctx.uid;
                    st.st_gid = ctx.gid;
                    Ok(st)
                }
                Ownership::Fixed(uid, gid) => {
                    st.st_uid = uid;
                    st.st_gid = gid;
                    Ok(st)
                }
            }
        } else {
            Ok(st)
        }
    } else {
        Err(linux_error(io::Error::last_os_error()))
    }
}

fn lstat(
    ctx: &Context,
    ownership: Ownership,
    c_path: &CString,
    host: bool,
) -> io::Result<bindings::stat64> {
    let mut st = MaybeUninit::<bindings::stat64>::zeroed();

    // Safe because the kernel will only write data in `st` and we check the return
    // value.
    let res = unsafe { libc::lstat(c_path.as_ptr(), st.as_mut_ptr()) };
    if res >= 0 {
        // Safe because the kernel guarantees that the struct is now fully initialized.
        let mut st = unsafe { st.assume_init() };
        if !host {
            match ownership {
                Ownership::Xattr => {
                    let (uid, gid, mode) = get_xattr_lstat(c_path, st)?;
                    stat_xattr_common(&mut st, uid, gid, mode)
                }
                Ownership::Requester => {
                    st.st_uid = ctx.uid;
                    st.st_gid = ctx.gid;
                    Ok(st)
                }
                Ownership::Fixed(uid, gid) => {
                    st.st_uid = uid;
                    st.st_gid = gid;
                    Ok(st)
                }
            }
        } else {
            Ok(st)
        }
    } else {
        Err(linux_error(io::Error::last_os_error()))
    }
}

fn set_owner(ctx: &Context, ownership: Ownership, st: &mut bindings::stat64) {
    match ownership {
        Ownership::Xattr => {}
        Ownership::Requester => {
            st.st_uid = ctx.uid;
            st.st_gid = ctx.gid;
        }
        Ownership::Fixed(uid, gid) => {
            st.st_uid = uid;
            st.st_gid = gid;
        }
    }
}

/// A directory descriptor to resolve names in: the directory's pin, or one opened for the call.
enum DirFd {
    Pinned(Arc<Pin>),
    Opened(File),
}

impl DirFd {
    fn fd(&self) -> RawFd {
        match self {
            DirFd::Pinned(pin) => pin.fd(),
            DirFd::Opened(file) => file.as_raw_fd(),
        }
    }
}

fn is_fd_exhaustion(err: &io::Error) -> bool {
    matches!(err.raw_os_error(), Some(libc::EMFILE | libc::ENFILE))
}

fn istat(
    ctx: &Context,
    ownership: Ownership,
    ihandle: &InodeHandle,
    host: bool,
) -> io::Result<bindings::stat64> {
    match ihandle {
        InodeHandle::Fd(fd) => fstat(ctx, ownership, *fd, host),
        InodeHandle::Path(c_path) => lstat(ctx, ownership, c_path, host),
    }
}

/// Options that configure the behavior of the file system.
#[derive(Debug, Clone)]
pub struct Config {
    /// How long the FUSE client should consider directory entries to be valid. If the contents of a
    /// directory can only be modified by the FUSE client (i.e., the file system has exclusive
    /// access), then this should be a large value.
    ///
    /// The default value for this option is 5 seconds.
    pub entry_timeout: Duration,

    /// How long the FUSE client should consider file and directory attributes to be valid. If the
    /// attributes of a file or directory can only be modified by the FUSE client (i.e., the file
    /// system has exclusive access), then this should be set to a large value.
    ///
    /// The default value for this option is 5 seconds.
    pub attr_timeout: Duration,

    /// How long the FUSE client may cache a failed lookup. Zero (the default) caches none.
    pub negative_timeout: Duration,

    /// The caching policy the file system should use. See the documentation of `CachePolicy` for
    /// more details.
    pub cache_policy: CachePolicy,

    /// Whether the file system should enabled writeback caching. This can improve performance as it
    /// allows the FUSE client to cache and coalesce multiple writes before sending them to the file
    /// system. However, enabling this option can increase the risk of data corruption if the file
    /// contents can change without the knowledge of the FUSE client (i.e., the server does **NOT**
    /// have exclusive access). Additionally, the file system should have read access to all files
    /// in the directory it is serving as the FUSE client may send read requests even for files
    /// opened with `O_WRONLY`.
    ///
    /// Therefore callers should only enable this option when they can guarantee that: 1) the file
    /// system has exclusive access to the directory and 2) the file system has read permissions for
    /// all files in that directory.
    ///
    /// The default value for this option is `false`.
    pub writeback: bool,

    /// The path of the root directory.
    ///
    /// The default is `/`.
    pub root_dir: String,

    /// Whether the file system should support Extended Attributes (xattr). Enabling this feature may
    /// have a significant impact on performance, especially on write parallelism. This is the result
    /// of FUSE attempting to remove the special file privileges after each write request.
    ///
    /// The default value for this options is `false`.
    pub xattr: bool,

    /// Optional file descriptor for /proc/self/fd. Callers can obtain a file descriptor and pass it
    /// here, so there's no need to open it in PassthroughFs::new(). This is specially useful for
    /// sandboxing.
    ///
    /// The default is `None`.
    pub proc_sfd_rawfd: Option<RawFd>,

    /// ID of this filesystem to uniquely identify exports. Not supported for macos.
    pub export_fsid: u64,

    /// Table of exported FDs to share with other subsystems. Not supported for macos.
    pub export_table: Option<ExportTable>,

    /// The permission semantics to be emulated. See the documentation for `PermissionSemantics` for
    /// more details.
    pub semantics: PermissionSemantics,

    /// The owner that `PermissionSemantics::LinuxSimplifiedFixedOwner` reports for every file.
    pub fixed_owner: Option<(u32, u32)>,

    /// The guest shows every file as owned by the caller (its `owner_caller` mount option) and
    /// sends permission checks here, so `access(2)` checks the owner bits for every caller.
    pub caller_owns: bool,

    /// Make FSYNC and FSYNCDIR no-ops (for disposable data). Off by default.
    pub no_sync_io: bool,

    /// Shows a host-side watcher which inodes the guest holds, so it can invalidate exactly the
    /// changed ones (and the guest can cache entries and attributes for long).
    pub guest_cache: GuestCache,
}

impl Config {
    fn ownership(&self) -> Ownership {
        match (self.semantics, self.fixed_owner) {
            (PermissionSemantics::LinuxComplete, _) => Ownership::Xattr,
            (PermissionSemantics::LinuxSimplifiedFixedOwner, Some((uid, gid))) => {
                Ownership::Fixed(uid, gid)
            }
            _ => Ownership::Requester,
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            entry_timeout: Duration::from_secs(5),
            attr_timeout: Duration::from_secs(5),
            negative_timeout: Duration::ZERO,
            cache_policy: Default::default(),
            writeback: false,
            root_dir: String::from("/"),
            xattr: true,
            proc_sfd_rawfd: None,
            export_fsid: 0,
            export_table: None,
            semantics: PermissionSemantics::LinuxComplete,
            fixed_owner: None,
            caller_owns: false,
            no_sync_io: false,
            guest_cache: GuestCache::default(),
        }
    }
}

/// A file system that simply "passes through" all requests it receives to the underlying file
/// system. To keep the implementation simple it servers the contents of its root directory. Users
/// that wish to serve only a specific directory should set up the environment so that that
/// directory ends up as the root of the file system process. One way to accomplish this is via a
/// combination of mount namespaces and the pivot_root system call.
pub struct PassthroughFs {
    inodes: Arc<InodeTable>,
    inode_alloc: Arc<InodeAllocator>,

    handles: HandleTable<HandleData>,

    map_windows: Mutex<HashMap<u64, u64>>,

    // Whether writeback caching is enabled for this directory. This will only be true when
    // `cfg.writeback` is true and `init` was called with `FsOptions::WRITEBACK_CACHE`.
    writeback: AtomicBool,
    announce_submounts: AtomicBool,
    // KATAMARAN_FS_BULK=0 lists directories with readdir and one lstat per entry instead.
    bulk_readdir: bool,
    apfs_dev: AtomicI32,
    dev_apfs: RwLock<HashMap<i32, bool>>,
    open_tracker: Arc<OpenTracker>,
    pins: Arc<PinPool>,
    cfg: Config,
}

impl PassthroughFs {
    pub fn new(cfg: Config, inode_alloc: Arc<InodeAllocator>) -> io::Result<PassthroughFs> {
        let root = CString::new(cfg.root_dir.as_str()).expect("CString::new failed");

        // Safe because this doesn't modify any memory and we check the return value.
        let fd = unsafe {
            libc::openat(
                libc::AT_FDCWD,
                root.as_ptr(),
                libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err(linux_error(io::Error::last_os_error()));
        }

        unsafe { libc::close(fd) };

        let inodes = Arc::new(RwLock::new(MultikeyBTreeMap::new()));
        cfg.guest_cache.attach(&inodes);
        Ok(PassthroughFs {
            inodes,
            inode_alloc,

            handles: HandleTable::new(),

            map_windows: Mutex::new(HashMap::new()),

            writeback: AtomicBool::new(false),
            announce_submounts: AtomicBool::new(false),
            bulk_readdir: env::var("KATAMARAN_FS_BULK").as_deref() != Ok("0"),
            apfs_dev: AtomicI32::new(0),
            dev_apfs: RwLock::new(HashMap::new()),
            open_tracker: OpenTracker::new(),
            pins: PinPool::new(),
            cfg,
        })
    }

    fn is_apfs(&self, dev: i32, fd: RawFd) -> bool {
        if dev != 0 && self.apfs_dev.load(Ordering::Relaxed) == dev {
            return true;
        }
        if let Some(&supported) = self.dev_apfs.read().unwrap().get(&dev) {
            return supported;
        }
        let supported = attrlist::supported(fd);
        if supported && dev != 0 {
            let _ = self
                .apfs_dev
                .compare_exchange(0, dev, Ordering::Relaxed, Ordering::Relaxed);
        }
        self.dev_apfs.write().unwrap().insert(dev, supported);
        supported
    }

    fn node(&self, inode: Inode) -> io::Result<Arc<InodeData>> {
        self.inodes
            .read()
            .unwrap()
            .get(&inode)
            .cloned()
            .ok_or_else(ebadf)
    }

    /// The pin of a directory that names are being resolved in, once it is used often enough.
    fn parent_pin(&self, data: &InodeData) -> Option<Arc<Pin>> {
        if data.kind != libc::S_IFDIR || data.unlinked_fd.load(Ordering::Acquire) >= 0 {
            return None;
        }
        let slot = data.pin.as_ref()?;
        self.pins
            .parent_pin(slot, data.inode, data.dev, data.ino)
    }

    fn dir_fd(&self, data: &InodeData) -> io::Result<DirFd> {
        if let Some(pin) = self.parent_pin(data) {
            return Ok(DirFd::Pinned(pin));
        }
        let fd = match self.inode_to_handle(data.inode, true)? {
            InodeHandle::Path(c_path) => unsafe {
                libc::open(c_path.as_ptr(), libc::O_NOFOLLOW | libc::O_CLOEXEC)
            },
            InodeHandle::Fd(fd) => unsafe { libc::dup(fd) },
        };
        if fd < 0 {
            return Err(linux_error(io::Error::last_os_error()));
        }
        Ok(DirFd::Opened(unsafe { File::from_raw_fd(fd) }))
    }

    fn stat_child(
        &self,
        ctx: &Context,
        parent_data: &InodeData,
        name: &CStr,
    ) -> io::Result<bindings::stat64> {
        let ownership = self.cfg.ownership();
        if !matches!(ownership, Ownership::Xattr)
            && let Some(pin) = self.parent_pin(parent_data)
        {
            let mut st = MaybeUninit::<bindings::stat64>::zeroed();
            let res = unsafe {
                libc::fstatat(
                    pin.fd(),
                    name.as_ptr(),
                    st.as_mut_ptr(),
                    libc::AT_SYMLINK_NOFOLLOW,
                )
            };
            if res < 0 {
                return Err(linux_error(io::Error::last_os_error()));
            }
            let mut st = unsafe { st.assume_init() };
            set_owner(ctx, ownership, &mut st);
            return Ok(st);
        }
        let c_path = self.name_to_path(parent_data.inode, name)?;
        lstat(ctx, ownership, &c_path, false)
    }

    /// Opens the inode relative to a pinned directory instead of by its /.vol path: a directory
    /// through its own pin, a regular file through its anchor. `None` when that is not possible
    /// or the anchor now names another file.
    /// A bounded open through the parent's pin (a directory's own), when there is one: what
    /// `syncopen::open` does with the `/.vol` path, a microsecond and a half faster. None when
    /// there is no pin or the name no longer leads to this node; the caller then opens the path.
    fn open_pinned_inline(&self, data: &InodeData, flags: i32) -> Option<RawFd> {
        if flags & (libc::O_TRUNC | libc::O_CREAT) != 0 {
            return None;
        }
        let (fd, st) = match data.kind {
            libc::S_IFDIR => {
                if let Some(pin) = data.pin.as_ref().and_then(|s| self.pins.current(s)) {
                    unsafe { syncopen::open_inline_at(pin.fd(), c".".as_ptr(), flags) }?
                } else {
                    let (parent, name) = data.anchor.lock().unwrap().clone()?;
                    let parent_data = self.inodes.read().unwrap().get(&parent).cloned()?;
                    let pin = parent_data
                        .pin
                        .as_ref()
                        .and_then(|s| self.pins.current(s))?;
                    unsafe { syncopen::open_inline_at(pin.fd(), name.as_ptr(), flags) }?
                }
            }
            libc::S_IFREG => {
                let (parent, name) = data.anchor.lock().unwrap().clone()?;
                let parent_data = self.inodes.read().unwrap().get(&parent).cloned()?;
                let pin = parent_data
                    .pin
                    .as_ref()
                    .and_then(|s| self.pins.current(s))?;
                unsafe { syncopen::open_inline_at(pin.fd(), name.as_ptr(), flags) }?
            }
            _ => return None,
        };
        if (st.st_dev as i64, st.st_ino) != (data.dev as i64, data.ino) {
            syncopen::clear_mtime();
            unsafe { libc::close(fd) };
            return None;
        }
        Some(fd)
    }

    fn open_pinned(&self, data: &InodeData, flags: i32) -> Option<RawFd> {
        if data.kind == libc::S_IFDIR {
            let pin = data.pin.as_ref().and_then(|s| self.pins.current(s))?;
            let fd = unsafe { libc::openat(pin.fd(), c".".as_ptr(), flags) };
            return (fd >= 0).then_some(fd);
        }
        if data.kind != libc::S_IFREG || flags & (libc::O_TRUNC | libc::O_CREAT) != 0 {
            return None;
        }
        let (parent, name) = data.anchor.lock().unwrap().clone()?;
        let parent_data = self.inodes.read().unwrap().get(&parent).cloned()?;
        let pin = parent_data
            .pin
            .as_ref()
            .and_then(|s| self.pins.current(s))?;
        // Non-blocking, in case the name has become a FIFO since.
        let fd = unsafe {
            libc::openat(
                pin.fd(),
                name.as_ptr(),
                flags | libc::O_NOFOLLOW | libc::O_NONBLOCK,
            )
        };
        if fd < 0 {
            return None;
        }
        let mut st = MaybeUninit::<bindings::stat64>::zeroed();
        let same = unsafe { libc::fstat(fd, st.as_mut_ptr()) } == 0 && {
            let st = unsafe { st.assume_init() };
            (st.st_dev, st.st_ino) == (data.dev, data.ino)
        };
        if !same {
            unsafe { libc::close(fd) };
            let mut anchor = data.anchor.lock().unwrap();
            if anchor
                .as_ref()
                .is_some_and(|(p, n)| *p == parent && *n == name)
            {
                *anchor = None;
            }
            return None;
        }
        if flags & libc::O_NONBLOCK == 0 {
            unsafe {
                let fl = libc::fcntl(fd, libc::F_GETFL);
                libc::fcntl(fd, libc::F_SETFL, fl & !libc::O_NONBLOCK);
            }
        }
        Some(fd)
    }

    /// The descriptor of the inode at `name` in `dir`, kept so the inode stays reachable after
    /// its last name goes, if the guest has it open. Inodes without open handles are not kept:
    /// the guest drops them with their name.
    fn keep_if_open(&self, dir: RawFd, name: &CStr) -> Option<RawFd> {
        let mut st = MaybeUninit::<bindings::stat64>::zeroed();
        let res = unsafe {
            libc::fstatat(
                dir,
                name.as_ptr(),
                st.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        if res < 0 {
            return None;
        }
        let st = unsafe { st.assume_init() };
        let altkey = InodeAltKey {
            ino: st.st_ino,
            dev: st.st_dev,
        };
        let open = self
            .inodes
            .read()
            .unwrap()
            .get_alt(&altkey)
            .is_some_and(|data| data.open_handles.load(Ordering::Acquire) > 0);
        if !open {
            return None;
        }
        match self.grab_unlinked_fd(dir, name) {
            Ok(fd) => Some(fd),
            Err(err) => {
                warn!(
                    "Couldn't grab a file descriptor for file \"{}\": {err}",
                    name.to_string_lossy()
                );
                None
            }
        }
    }

    fn inode_to_handle(&self, inode: Inode, supports_fd: bool) -> io::Result<InodeHandle> {
        debug!("inode_to_handle: inode={inode}");
        let data = self
            .inodes
            .read()
            .unwrap()
            .get(&inode)
            .cloned()
            .ok_or_else(ebadf)?;

        let cstr =
            CString::new(format!("/.vol/{}/{}", data.dev, data.ino)).map_err(|_| einval())?;
        debug!("inode_to_handle: path={}", cstr.to_string_lossy());

        if supports_fd {
            let unlinked_fd = data.unlinked_fd.load(Ordering::Acquire);
            if unlinked_fd >= 0 {
                return Ok(InodeHandle::Fd(unlinked_fd as RawFd));
            }
        }

        Ok(InodeHandle::Path(cstr))
    }

    fn name_to_path(&self, parent: Inode, name: &CStr) -> io::Result<CString> {
        debug!(
            "name_to_path: parent={} name={}",
            parent,
            name.to_string_lossy()
        );
        let data = self
            .inodes
            .read()
            .unwrap()
            .get(&parent)
            .cloned()
            .ok_or_else(ebadf)?;

        let cstr = CString::new(format!(
            "/.vol/{}/{}/{}",
            data.dev,
            data.ino,
            name.to_string_lossy()
        ))
        .map_err(|_| einval())?;
        debug!("name_to_path: path={}", cstr.to_string_lossy());
        Ok(cstr)
    }

    fn open_inode(&self, data: &InodeData, mut flags: i32) -> io::Result<File> {
        // When writeback caching is enabled, the kernel may send read requests even if the
        // userspace program opened the file write-only. So we need to ensure that we have opened
        // the file for reading as well as writing.
        let writeback = self.writeback.load(Ordering::Relaxed);
        if writeback && flags & libc::O_ACCMODE == libc::O_WRONLY {
            flags &= !libc::O_ACCMODE;
            flags |= libc::O_RDWR;
        }

        // When writeback caching is enabled the kernel is responsible for handling `O_APPEND`.
        // However, this breaks atomicity as the file may have changed on disk, invalidating the
        // cached copy of the data in the kernel and the offset that the kernel thinks is the end of
        // the file. Just allow this for now as it is the user's responsibility to enable writeback
        // caching only for directories that are not shared. It also means that we need to clear the
        // `O_APPEND` flag.
        if writeback && flags & libc::O_APPEND != 0 {
            flags &= !libc::O_APPEND;
        }

        let flags = (flags | libc::O_CLOEXEC) & (!libc::O_NOFOLLOW) & (!libc::O_EXLOCK);
        let open = || -> io::Result<RawFd> {
            // Check if we have recently unlinked the inode and kept open a file descriptor to it.
            let unlinked_fd = data.unlinked_fd.load(Ordering::Acquire);
            if unlinked_fd >= 0 {
                return Ok(unsafe { libc::dup(unlinked_fd as RawFd) });
            }
            if syncopen::in_bounded_scope() {
                if let Some(fd) = self.open_pinned_inline(data, flags) {
                    return Ok(fd);
                }
                return Ok(with_vol_path(data.dev, data.ino, |c_path| unsafe {
                    syncopen::open(c_path.as_ptr(), flags, 0)
                }));
            }
            if let Some(fd) = self.open_pinned(data, flags) {
                return Ok(fd);
            }
            Ok(with_vol_path(data.dev, data.ino, |c_path| unsafe {
                libc::open(c_path.as_ptr(), flags)
            }))
        };
        let open = || -> io::Result<RawFd> {
            let mut fd = open()?;
            if fd < 0 && is_fd_exhaustion(&io::Error::last_os_error()) {
                self.pins.relieve();
                fd = open()?;
            }
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(fd)
        };
        match open() {
            // Safe because we just opened this fd.
            Ok(fd) => Ok(unsafe { File::from_raw_fd(fd) }),
            // The host file behind the node is gone: removed, or replaced by a rename on the
            // host before the guest heard of it. ESTALE makes the guest walk the path again (with
            // LOOKUP_REVAL) and open whatever is there now.
            Err(e) if e.raw_os_error() == Some(libc::ENOENT) => {
                Err(linux_error(io::Error::from_raw_os_error(libc::ESTALE)))
            }
            Err(e) => Err(linux_error(e)),
        }
    }

    fn do_readdir<F>(
        &self,
        inode: Inode,
        handle: Handle,
        size: u32,
        mut offset: u64,
        need_attr: bool,
        mut add_entry: F,
    ) -> io::Result<()>
    where
        F: FnMut(DirEntry, &CachedDirEntry) -> io::Result<usize>,
    {
        if size == 0 {
            return Ok(());
        }

        let data = self.handle_data(inode, handle)?;

        let mut ds = data.dirstream.lock().unwrap();

        // We use offset == 0 as an indicator of this being either a fresh directory
        // stream or a stream that has been rewound. If that's the case, make sure
        // the cache will be refreshed.
        if offset == 0 && ds.ready {
            ds.entries.clear();
            ds.names.clear();
            ds.ready = false;
        }

        if !ds.ready {
            // Fill the cache on first call
            let file_guard = data.file.write().unwrap();
            let fd = file_guard.as_raw_fd();
            let use_bulk = self.bulk_readdir && self.is_apfs(data.node.dev, fd);
            let xattr_flags = need_attr && matches!(self.cfg.ownership(), Ownership::Xattr);
            if let Err(e) = ds.fill(fd, use_bulk, need_attr, xattr_flags) {
                if ds.entries.is_empty() {
                    return Err(e);
                }
                // If we got some valid entries before error happened,
                // treat this partial read as success and just log
                // the error.
                warn!("virtio-fs: error in readdir {}: {:?}", inode, e);
            }
            ds.ready = true;
        }

        while let Some((entry, cached)) = ds.get_entry(offset) {
            offset += 1;

            let name = entry.name;
            match add_entry(entry, cached) {
                Ok(size) => {
                    if size == 0 {
                        break;
                    }
                }
                Err(e) => {
                    warn!(
                        "virtio-fs: error adding entry {}: {:?}",
                        String::from_utf8_lossy(name),
                        e
                    );
                    break;
                }
            }
        }

        drop(ds);
        self.cfg.guest_cache.note_activity();
        Ok(())
    }

    /// Takes a lookup reference on the inode that `st` describes, adding it to the inode table
    /// when it is new, and returns its entry.
    fn register_entry(&self, parent_data: &InodeData, name: &CStr, st: bindings::stat64) -> Entry {
        let mut attr_flags: u32 = 0;

        if st.st_mode & libc::S_IFMT == libc::S_IFDIR
            && self.announce_submounts.load(Ordering::Relaxed)
            && (st.st_dev != parent_data.dev)
        {
            attr_flags |= fuse::ATTR_SUBMOUNT;
        }

        let altkey = InodeAltKey {
            ino: st.st_ino,
            dev: st.st_dev,
        };
        // The reference is taken under the table lock, so a concurrent `forget` cannot remove
        // the inode in between, and a new inode is added only after a second check under the
        // write lock, so two lookups of one file cannot each add (and evict) an inode.
        let take = |data: &Arc<InodeData>| {
            // Matches with the release store in `forget`.
            data.refcount.fetch_add(1, Ordering::Acquire);
            data.clone()
        };
        let mut dir_changed = false;
        let found = self.inodes.read().unwrap().get_alt(&altkey).map(take);
        let data = found.unwrap_or_else(|| {
            let mut inodes = self.inodes.write().unwrap();
            if let Some(data) = inodes.get_alt(&altkey) {
                return take(data);
            }
            let data = Arc::new(InodeData::new(self.inode_alloc.next(), &st, 1));
            inodes.insert(data.inode, altkey, data.clone());
            if u32::from(data.kind) == libc::S_IFDIR as u32 {
                dir_changed = true;
            }
            data
        });
        if data.kind != libc::S_IFDIR && st.st_nlink > 1 {
            data.hardlinked.store(true, Ordering::Release);
        }
        if data.set_anchor(parent_data.inode, name)
            && u32::from(data.kind) == libc::S_IFDIR as u32
        {
            dir_changed = true;
        }
        if dir_changed {
            self.cfg.guest_cache.note_dir_change();
        }
        self.cfg.guest_cache.note_activity();

        Entry {
            inode: data.inode,
            generation: 0,
            attr: st,
            attr_flags,
            attr_timeout: self.cfg.attr_timeout,
            entry_timeout: self.cfg.entry_timeout,
        }
    }

    /// The entry for a listed name whose host attributes came from getattrlistbulk, with the same
    /// ownership `lookup` would report.
    fn bulk_entry(
        &self,
        ctx: &Context,
        parent_data: &InodeData,
        name: &CStr,
        mut st: bindings::stat64,
        no_xattrs: bool,
    ) -> io::Result<Entry> {
        match self.cfg.ownership() {
            Ownership::Xattr if no_xattrs => {}
            Ownership::Xattr => {
                let c_path = self.name_to_path(parent_data.inode, name)?;
                let (uid, gid, mode) = get_xattr_lstat(&c_path, st)?;
                stat_xattr_common(&mut st, uid, gid, mode)?;
            }
            ownership => set_owner(ctx, ownership, &mut st),
        }
        Ok(self.register_entry(parent_data, name, st))
    }

    fn do_open(
        &self,
        ctx: &Context,
        inode: Inode,
        kill_priv: bool,
        flags: u32,
        directory: bool,
    ) -> io::Result<(Option<Handle>, OpenOptions)> {
        let mut flags = self.parse_open_flags(flags as i32);
        if directory {
            flags |= libc::O_DIRECTORY;
        }

        let data = self.node(inode)?;
        syncopen::clear_mtime();
        let file = self.open_file(&data, flags, directory)?;

        if !directory {
            let fd = file.read().unwrap().as_raw_fd();
            if syncopen::peek_hardlinked(fd) == Some(true) {
                data.hardlinked.store(true, Ordering::Release);
            }
        }

        // If O_TRUNC and kill_priv (OPEN_KILL_SUIDGID), clear security.capability and suid/sgid
        if (flags & libc::O_TRUNC) != 0 && kill_priv {
            let fd = file.read().unwrap().as_raw_fd();
            let ihandle = InodeHandle::Fd(fd);

            remove_security_capability(&ihandle);

            if let Ok(st) = fstat(ctx, self.cfg.ownership(), fd, false) {
                let new_mode = clear_suid_sgid(st.st_mode as u32);
                if new_mode != st.st_mode as u32
                    && let Err(err) = set_stat(
                        ctx,
                        self.cfg.semantics,
                        &ihandle,
                        Some(st),
                        None,
                        Some(new_mode),
                    )
                {
                    error!("Couldn't clear suid/sgid for inode {inode}: {err}");
                }
            }
        }

        let opts = match self.cfg.cache_policy {
            CachePolicy::Never if !directory => OpenOptions::DIRECT_IO,
            CachePolicy::Always if directory => OpenOptions::CACHE_DIR,
            CachePolicy::Always => OpenOptions::KEEP_CACHE,
            CachePolicy::Auto if directory => self.keep_dir_cache_if_unchanged(&data, &file),
            CachePolicy::Auto => self.keep_cache_if_unchanged(&data, &file),
            _ => OpenOptions::empty(),
        };

        let handle = self
            .handles
            .insert(Arc::new(HandleData::new(data, self.open_tracker.clone(), file)));

        Ok((Some(handle), opts))
    }

    /// KEEP_CACHE when the file's mtime is the one its previous open saw: nothing has written it
    /// since, so the pages the guest read then are still current.
    fn keep_cache_if_unchanged(&self, data: &InodeData, file: &RwLock<File>) -> OpenOptions {
        let fd = file.read().unwrap().as_raw_fd();
        let mtime = match syncopen::take_mtime(fd) {
            Some(mtime) => mtime,
            None => {
                let mut st = MaybeUninit::<libc::stat>::uninit();
                if unsafe { libc::fstat(fd, st.as_mut_ptr()) } < 0 {
                    return OpenOptions::empty();
                }
                let st = unsafe { st.assume_init_ref() };
                if st.st_nlink > 1 {
                    data.hardlinked.store(true, Ordering::Release);
                }
                syncopen::mtime_ns(st)
            }
        }
        .wrapping_add(1);
        if data.open_mtime.swap(mtime, Ordering::Relaxed) == mtime {
            OpenOptions::KEEP_CACHE
        } else {
            OpenOptions::empty()
        }
    }

    /// CACHE_DIR, plus KEEP_CACHE when the directory's stamp (mtime, ctime, entry count) matches
    /// its previous open: unchanged directories can be served straight from the guest's page cache.
    fn keep_dir_cache_if_unchanged(&self, data: &InodeData, file: &RwLock<File>) -> OpenOptions {
        let fd = file.read().unwrap().as_raw_fd();
        let stamp = match syncopen::take_dir_stamp(fd) {
            Some(stamp) => stamp,
            None => {
                let mut st = MaybeUninit::<libc::stat>::uninit();
                if unsafe { libc::fstat(fd, st.as_mut_ptr()) } < 0 {
                    return OpenOptions::empty();
                }
                syncopen::dir_stamp(unsafe { st.assume_init_ref() })
            }
        };
        if data.open_mtime.swap(stamp, Ordering::Relaxed) == stamp {
            OpenOptions::CACHE_DIR | OpenOptions::KEEP_CACHE
        } else {
            OpenOptions::CACHE_DIR
        }
    }

    fn open_file(
        &self,
        data: &InodeData,
        flags: i32,
        directory: bool,
    ) -> io::Result<Arc<RwLock<File>>> {
        if directory {
            let file = Arc::new(RwLock::new(self.open_inode(data, flags)?));
            if data.unlinked_fd.load(Ordering::Acquire) < 0 {
                *data.read_only_file.lock().unwrap() = Arc::downgrade(&file);
            }
            return Ok(file);
        }
        let shareable = flags & libc::O_ACCMODE == libc::O_RDONLY && flags & libc::O_TRUNC == 0;
        if !shareable {
            return Ok(Arc::new(RwLock::new(self.open_inode(data, flags)?)));
        }

        let mut shared = data.read_only_file.lock().unwrap();
        if let Some(file) = shared.upgrade() {
            return Ok(file);
        }
        let file = Arc::new(RwLock::new(self.open_inode(data, flags)?));
        *shared = Arc::downgrade(&file);
        Ok(file)
    }

    /// Opens `name` in the parent for CREATE. A file the call creates itself cannot be dataless
    /// or a FIFO, so it opens directly, through the parent's pin when it has one; a name that
    /// exists already (the guest did not ask for O_EXCL) gets a bounded open.
    fn create_file(
        &self,
        parent_data: &InodeData,
        name: &CStr,
        flags: i32,
        mode: u32,
    ) -> io::Result<File> {
        let flags = flags | libc::O_CREAT | libc::O_CLOEXEC | libc::O_NOFOLLOW;
        let excl = flags | libc::O_EXCL;
        let mut fd = match self.parent_pin(parent_data) {
            Some(pin) => unsafe {
                libc::openat(pin.fd(), name.as_ptr(), excl, mode as libc::c_uint)
            },
            None => {
                let c_path = self.name_to_path(parent_data.inode, name)?;
                unsafe { libc::open(c_path.as_ptr(), excl, mode as libc::c_uint) }
            }
        };
        if fd < 0
            && flags & libc::O_EXCL == 0
            && io::Error::last_os_error().raw_os_error() == Some(libc::EEXIST)
        {
            let c_path = self.name_to_path(parent_data.inode, name)?;
            fd = unsafe { syncopen::open(c_path.as_ptr(), flags, mode as libc::c_int) };
        }
        if fd < 0 {
            return Err(linux_error(io::Error::last_os_error()));
        }
        // Safe because we just opened this fd.
        Ok(unsafe { File::from_raw_fd(fd) })
    }

    fn handle_data(&self, inode: Inode, handle: Handle) -> io::Result<Arc<HandleData>> {
        self.handles
            .get(handle)
            .filter(|hd| hd.inode == inode)
            .ok_or_else(ebadf)
    }

    fn do_release(&self, inode: Inode, handle: Handle) -> io::Result<()> {
        // The file closes when the last `Arc` of the handle data drops.
        if self.handles.remove_if(handle, |hd| hd.inode == inode) {
            return Ok(());
        }
        Err(ebadf())
    }

    fn do_getattr(&self, ctx: &Context, inode: Inode) -> io::Result<(bindings::stat64, Duration)> {
        let data = self.node(inode)?;
        if !matches!(self.cfg.ownership(), Ownership::Xattr)
            && data.unlinked_fd.load(Ordering::Acquire) < 0
            && let Some(pin) = data.pin.as_ref().and_then(|s| self.pins.current(s))
        {
            let st = fstat(ctx, self.cfg.ownership(), pin.fd(), false)?;
            return Ok((st, self.cfg.attr_timeout));
        }
        let ihandle = self.inode_to_handle(inode, true)?;
        let st = match ihandle {
            InodeHandle::Path(c_path) => lstat(ctx, self.cfg.ownership(), &c_path, false)?,
            InodeHandle::Fd(fd) => fstat(ctx, self.cfg.ownership(), fd, false)?,
        };

        Ok((st, self.cfg.attr_timeout))
    }

    fn grab_unlinked_fd(&self, parent_fd: RawFd, name: &CStr) -> io::Result<RawFd> {
        let fd = unsafe {
            libc::openat(
                parent_fd,
                name.as_ptr(),
                libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(fd)
    }

    fn store_unlinked_fd(&self, ctx: &Context, unlinked_fd: RawFd) -> io::Result<bool> {
        let st = fstat(ctx, self.cfg.ownership(), unlinked_fd, true)?;
        let altkey = InodeAltKey {
            ino: st.st_ino,
            dev: st.st_dev,
        };
        // Hold the read lock across the swap: dropping it earlier would let a
        // concurrent `forget` remove this inode (closing its then-`-1`
        // `unlinked_fd`) between our lookup and swap, leaking the fd we store.
        let inodes = self.inodes.read().unwrap();
        if let Some(data) = inodes.get_alt(&altkey) {
            // Swap rather than store so that if this inode already had a
            // preserved fd (e.g. another hard link was unlinked/overwritten
            // earlier), we recover and close it instead of leaking it.
            let old_fd = data.unlinked_fd.swap(unlinked_fd as i64, Ordering::AcqRel);
            if old_fd >= 0 {
                unsafe { libc::close(old_fd as RawFd) };
            }
            // The tracked inode now owns `unlinked_fd` (closed in `forget_one`).
            Ok(true)
        } else {
            // No tracked inode for this (dev, ino): the caller keeps ownership
            // of `unlinked_fd` and must close it to avoid a leak.
            Ok(false)
        }
    }

    fn do_unlink(
        &self,
        ctx: Context,
        parent: Inode,
        name: &CStr,
        flags: libc::c_int,
    ) -> io::Result<()> {
        let parent_data = self.node(parent)?;
        let is_dir = (flags & libc::AT_REMOVEDIR) != 0;

        if !self.open_tracker.may_have_open_target(parent, is_dir) {
            // Fast path: no open handle can refer to this target, so skip keep_if_open (fstatat).
            if let Some(pin) = parent_data.pin.as_ref().and_then(|s| self.pins.current(s)) {
                let res = unsafe { libc::unlinkat(pin.fd(), name.as_ptr(), flags) };
                return if res == 0 {
                    Ok(())
                } else {
                    Err(linux_error(io::Error::last_os_error()))
                };
            }
            if let Some(dir_file) = parent_data.active_dir_file() {
                let guard = dir_file.read().unwrap();
                let res = unsafe { libc::unlinkat(guard.as_raw_fd(), name.as_ptr(), flags) };
                return if res == 0 {
                    Ok(())
                } else {
                    Err(linux_error(io::Error::last_os_error()))
                };
            }
            let unlinked_parent = parent_data.unlinked_fd.load(Ordering::Acquire);
            if unlinked_parent >= 0 {
                let res = unsafe { libc::unlinkat(unlinked_parent as RawFd, name.as_ptr(), flags) };
                return if res == 0 {
                    Ok(())
                } else {
                    Err(linux_error(io::Error::last_os_error()))
                };
            }
            if let Some(pin) = self.parent_pin(&parent_data) {
                let res = unsafe { libc::unlinkat(pin.fd(), name.as_ptr(), flags) };
                return if res == 0 {
                    Ok(())
                } else {
                    Err(linux_error(io::Error::last_os_error()))
                };
            }
            let res = with_vol_child_path(parent_data.dev, parent_data.ino, name, |c_path| unsafe {
                if is_dir {
                    libc::rmdir(c_path.as_ptr())
                } else {
                    libc::unlink(c_path.as_ptr())
                }
            })?;
            return if res == 0 {
                Ok(())
            } else {
                Err(linux_error(io::Error::last_os_error()))
            };
        }

        let dir = self.dir_fd(&parent_data)?;
        let fd = dir.fd();

        // After unlinking this inode, we can't keep relying on getting a "/.vol/..." path
        // to operate on it. Before unlinking the inode, grab a file descriptor so we can
        // still operate on it. This one will be closed on "forget_one".
        let unlinked_fd = self.keep_if_open(fd, name);

        // Safe because this doesn't modify any memory and we check the return value.
        let res = unsafe { libc::unlinkat(fd, name.as_ptr(), flags) };
        let err = io::Error::last_os_error();
        drop(dir);

        if res == 0 {
            if let Some(unlinked_fd) = unlinked_fd {
                match self.store_unlinked_fd(&ctx, unlinked_fd) {
                    // The tracked inode took ownership of the fd.
                    Ok(true) => {}
                    // No tracked inode: we still own the fd and must close it.
                    Ok(false) => unsafe {
                        libc::close(unlinked_fd);
                    },
                    Err(err) => {
                        unsafe { libc::close(unlinked_fd) };
                        warn!("Couldn't store unlinked fd \"{}\": {err}", unlinked_fd);
                    }
                }
            }
            Ok(())
        } else {
            if let Some(unlinked_fd) = unlinked_fd {
                unsafe { libc::close(unlinked_fd) };
            }
            Err(linux_error(err))
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn mknod_complete(
        &self,
        ctx: Context,
        parent: Inode,
        name: &CStr,
        mode: u32,
        _rdev: u32,
        umask: u32,
        extensions: Extensions,
    ) -> io::Result<Entry> {
        let c_path = self.name_to_path(parent, name)?;

        let fd = unsafe {
            libc::open(
                c_path.as_ptr(),
                libc::O_CREAT | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                0o600,
            )
        };
        if fd < 0 {
            Err(linux_error(io::Error::last_os_error()))
        } else {
            let ihandle = InodeHandle::Fd(fd);

            // Set security context
            if let Some(secctx) = extensions.secctx {
                set_secctx(&ihandle, secctx, false)?
            };

            // For mknod, we're forced to store the mode as xattr even in
            // simplified mode, since macOS doesn't allow unprivileged users
            // to create special files (such as sockets or fifos) using mknod.
            if let Err(e) = set_xattr_stat(
                &ctx,
                &ihandle,
                None,
                Some((ctx.uid, ctx.gid)),
                Some(mode & !umask),
            ) {
                unsafe { libc::close(fd) };
                return Err(e);
            }

            unsafe { libc::close(fd) };
            self.lookup(ctx, parent, name)
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn mknod_simplified(
        &self,
        ctx: Context,
        parent: Inode,
        name: &CStr,
        mode: u32,
        rdev: u32,
        umask: u32,
        extensions: Extensions,
    ) -> io::Result<Entry> {
        let c_path = self.name_to_path(parent, name)?;

        // macOS doesn't allow us to create UNIX sockets using macOS, so we
        // have to resort to actually creating the socket ourselves and
        // dropping it.
        if (mode as u16 & libc::S_IFMT) == libc::S_IFSOCK {
            let path = c_path.to_str().map_err(|_| einval())?;
            let listener = UnixListener::bind(Path::new(path)).map_err(|_| einval())?;
            // Explicitly drop the listener to make it clear we aren't going
            // to use it. UnixListener's Drop doesn't remove the socket it
            // created, so we can reuse for the guest.
            drop(listener);
        } else {
            let res = unsafe { libc::mknod(c_path.as_ptr(), (mode & !umask) as u16, rdev as i32) };
            if res < 0 {
                return Err(linux_error(io::Error::last_os_error()));
            }
        }

        // Set security context
        if let Some(secctx) = extensions.secctx {
            let ihandle = InodeHandle::Path(c_path);
            set_secctx(&ihandle, secctx, false)?
        };
        self.lookup(ctx, parent, name)
    }

    fn parse_open_flags(&self, flags: i32) -> i32 {
        let mut mflags: i32 = flags & 0b11;

        if (flags & bindings::LINUX_O_NONBLOCK) != 0 {
            mflags |= libc::O_NONBLOCK;
        }
        if (flags & bindings::LINUX_O_APPEND) != 0 {
            mflags |= libc::O_APPEND;
        }
        if (flags & bindings::LINUX_O_CREAT) != 0 {
            mflags |= libc::O_CREAT;
        }
        if (flags & bindings::LINUX_O_TRUNC) != 0 {
            mflags |= libc::O_TRUNC;
        }
        if (flags & bindings::LINUX_O_EXCL) != 0 {
            mflags |= libc::O_EXCL;
        }
        if (flags & bindings::LINUX_O_NOFOLLOW) != 0 {
            mflags |= libc::O_NOFOLLOW;
        }
        if (flags & bindings::LINUX_O_CLOEXEC) != 0 {
            mflags |= libc::O_CLOEXEC;
        }

        mflags
    }
}

fn do_set_secctx(file: &InodeHandle, secctx: &SecContext, options: i32) -> io::Result<()> {
    let ret = match file {
        InodeHandle::Path(path) => unsafe {
            libc::setxattr(
                path.as_ptr(),
                secctx.name.as_ptr(),
                secctx.secctx.as_ptr() as *const libc::c_void,
                secctx.secctx.len(),
                0,
                options,
            )
        },
        InodeHandle::Fd(fd) => unsafe {
            libc::fsetxattr(
                *fd,
                secctx.name.as_ptr(),
                secctx.secctx.as_ptr() as *const libc::c_void,
                secctx.secctx.len(),
                0,
                options,
            )
        },
    };

    if ret != 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn set_secctx(file: &InodeHandle, secctx: SecContext, symlink: bool) -> io::Result<()> {
    let options = if symlink { libc::XATTR_NOFOLLOW } else { 0 };

    match do_set_secctx(file, &secctx, options) {
        Ok(()) => return Ok(()),
        Err(err) => {
            let err_os = err.raw_os_error();
            if err_os != Some(libc::EACCES) && err_os != Some(libc::EPERM) {
                return Err(linux_error(err));
            }
        }
    }

    // The file mode doesn't allow setting xattrs. Temporarily grant the owner
    // write permission, set the attribute, then restore the original mode.
    let mut st = MaybeUninit::<bindings::stat64>::zeroed();
    let ret = match file {
        InodeHandle::Path(path) => unsafe { libc::lstat(path.as_ptr(), st.as_mut_ptr()) },
        InodeHandle::Fd(fd) => unsafe { libc::fstat(*fd, st.as_mut_ptr()) },
    };
    if ret < 0 {
        return Err(linux_error(io::Error::last_os_error()));
    }
    let st = unsafe { st.assume_init() };

    let tmp_mode = st.st_mode | libc::S_IWUSR;
    let ret = match file {
        InodeHandle::Path(path) => unsafe { libc::chmod(path.as_ptr(), tmp_mode) },
        InodeHandle::Fd(fd) => unsafe { libc::fchmod(*fd, tmp_mode) },
    };
    if ret < 0 {
        let err = io::Error::last_os_error();
        error!("set_secctx: chmod failed: {}", err);
        return Err(linux_error(err));
    }

    let secctx_ret = do_set_secctx(file, &secctx, options);

    let ret = match file {
        InodeHandle::Path(path) => unsafe { libc::chmod(path.as_ptr(), st.st_mode) },
        InodeHandle::Fd(fd) => unsafe { libc::fchmod(*fd, st.st_mode) },
    };
    if ret < 0 {
        error!(
            "set_secctx: chmod restore failed: {}",
            io::Error::last_os_error()
        );
    }

    match secctx_ret {
        Ok(()) => Ok(()),
        Err(err) => Err(linux_error(err)),
    }
}

/// Remove the security.capability extended attribute
fn remove_security_capability(file: &InodeHandle) {
    let ret = match file {
        InodeHandle::Path(path) => unsafe {
            libc::removexattr(path.as_ptr(), SECURITY_CAPABILITY.as_ptr() as *const i8, 0)
        },
        InodeHandle::Fd(fd) => unsafe {
            libc::fremovexattr(*fd, SECURITY_CAPABILITY.as_ptr() as *const i8, 0)
        },
    };

    // ENOATTR means the attribute didn't exist, which is fine
    if ret != 0 && io::Error::last_os_error().raw_os_error() != Some(libc::ENOATTR) {
        warn!("Error removing security.capability from file");
    }
}

/// Clear suid/sgid bits from mode.
/// sgid is cleared only if group executable bit is set.
fn clear_suid_sgid(mode: u32) -> u32 {
    let mut new_mode = mode;

    // Clear suid bit
    new_mode &= !libc::S_ISUID as u32;

    // Clear sgid bit only if group executable bit is set
    if (mode & libc::S_IXGRP as u32) != 0 {
        new_mode &= !libc::S_ISGID as u32;
    }

    new_mode
}

fn forget_one(
    inodes: &mut MultikeyBTreeMap<Inode, InodeAltKey, Arc<InodeData>>,
    inode: Inode,
    count: u64,
) -> Option<Arc<InodeData>> {
    if let Some(data) = inodes.get(&inode) {
        // Acquiring the write lock on the inode map prevents new lookups from incrementing the
        // refcount but there is the possibility that a previous lookup already acquired a
        // reference to the inode data and is in the process of updating the refcount so we need
        // to loop here until we can decrement successfully.
        loop {
            let refcount = data.refcount.load(Ordering::Relaxed);

            // Saturating sub because it doesn't make sense for a refcount to go below zero and
            // we don't want misbehaving clients to cause integer overflow.
            let new_count = refcount.saturating_sub(count);

            // Synchronizes with the acquire load in `lookup`.
            if data
                .refcount
                .compare_exchange(refcount, new_count, Ordering::Release, Ordering::Relaxed)
                .unwrap()
                == refcount
            {
                if new_count == 0 {
                    // Return the removed inode so the caller closes any unlinked_fd or pin
                    // after releasing the inode table write lock.
                    return inodes.remove(&inode);
                }
                break;
            }
        }
    }
    None
}

fn finish_forget(data: Arc<InodeData>) {
    let fd = data.unlinked_fd.swap(-1, Ordering::AcqRel);
    if fd >= 0 {
        unsafe { libc::close(fd as RawFd) };
    }
    drop(data);
}

impl FileSystem for PassthroughFs {
    type Inode = Inode;
    type Handle = Handle;

    fn init(&self, capable: FsOptions) -> io::Result<FsOptions> {
        let root = CString::new(self.cfg.root_dir.as_str()).expect("CString::new failed");

        // Safe because this doesn't modify any memory and we check the return value.
        // We use `O_PATH` because we just want this for traversing the directory tree
        // and not for actually reading the contents.
        let fd = unsafe {
            libc::openat(
                libc::AT_FDCWD,
                root.as_ptr(),
                libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }

        // Safe because we just opened this fd above.
        let f = unsafe { File::from_raw_fd(fd) };

        // Build a fake Context for fstat, it won't be using it anyways
        // as it'll be only looking at the host's bits.
        let ctx = Context {
            uid: 0,
            gid: 0,
            pid: 0,
        };
        let st = fstat(&ctx, self.cfg.ownership(), f.as_raw_fd(), true)?;
        let _ = self.is_apfs(st.st_dev, f.as_raw_fd());

        // Safe because this doesn't modify any memory and there is no need to check the return
        // value because this system call always succeeds. We need to clear the umask here because
        // we want the client to be able to set all the bits in the mode.
        unsafe { libc::umask(0o000) };

        {
            let mut inodes = self.inodes.write().unwrap();

            // Not sure why the root inode gets a refcount of 2 but that's what libfuse does.
            inodes.insert(
                fuse::ROOT_ID,
                InodeAltKey {
                    ino: st.st_ino,
                    dev: st.st_dev,
                },
                Arc::new(InodeData::new(fuse::ROOT_ID, &st, 2)),
            );
        }
        self.cfg.guest_cache.note_dir_change();

        let mut opts = FsOptions::empty();
        if self.cfg.writeback && capable.contains(FsOptions::WRITEBACK_CACHE) {
            opts |= FsOptions::WRITEBACK_CACHE;
            self.writeback.store(true, Ordering::Relaxed);
        }

        if capable.contains(FsOptions::SUBMOUNTS) {
            opts |= FsOptions::SUBMOUNTS;
            self.announce_submounts.store(true, Ordering::Relaxed);
        }

        // Every lookup costs a round trip through the VMM, so let the kernel fetch a directory's
        // attributes along with its entries, and look up names in one directory concurrently.
        // Neither `readdirplus` nor `lookup` takes a lock on the directory.
        opts |= capable
            & (FsOptions::DO_READDIRPLUS
                | FsOptions::READDIRPLUS_AUTO
                | FsOptions::PARALLEL_DIROPS);

        Ok(opts)
    }

    fn destroy(&self) {
        self.handles.clear();
        self.inodes.write().unwrap().clear();
        self.cfg.guest_cache.note_dir_change();
    }

    fn statfs(&self, _ctx: Context, inode: Inode) -> io::Result<bindings::statvfs64> {
        let mut out = MaybeUninit::<bindings::statvfs64>::zeroed();

        let res = match self.inode_to_handle(inode, true)? {
            InodeHandle::Path(c_path) => unsafe {
                bindings::statvfs64(c_path.as_ptr(), out.as_mut_ptr())
            },
            InodeHandle::Fd(fd) => unsafe { bindings::fstatvfs64(fd, out.as_mut_ptr()) },
        };
        if res == 0 {
            // Safe because the kernel guarantees that `out` has been initialized.
            Ok(unsafe { out.assume_init() })
        } else {
            Err(linux_error(io::Error::last_os_error()))
        }
    }

    fn lookup(&self, ctx: Context, parent: Inode, name: &CStr) -> io::Result<Entry> {
        let parent_data = self
            .inodes
            .read()
            .unwrap()
            .get(&parent)
            .cloned()
            .ok_or_else(ebadf)?;

        let st = match self.stat_child(&ctx, &parent_data, name) {
            Ok(st) => st,
            Err(e) => {
                if e.raw_os_error() == Some(libc::ENOENT) && !self.cfg.negative_timeout.is_zero() {
                    self.cfg.guest_cache.note_activity();
                }
                return Err(e);
            }
        };

        debug!(
            "lookup: parent={parent} name={} ino={}",
            name.to_string_lossy(),
            st.st_ino
        );

        Ok(self.register_entry(&parent_data, name, st))
    }

    fn negative_timeout(&self) -> Duration {
        self.cfg.negative_timeout
    }

    fn forget(&self, _ctx: Context, inode: Inode, count: u64) {
        let evicted = {
            let mut inodes = self.inodes.write().unwrap();
            forget_one(&mut inodes, inode, count)
        };
        if let Some(data) = evicted {
            let is_dir = u32::from(data.kind) == libc::S_IFDIR as u32;
            finish_forget(data);
            if is_dir {
                self.cfg.guest_cache.note_dir_change();
            }
        }
    }

    fn batch_forget(&self, _ctx: Context, requests: Vec<(Inode, u64)>) {
        let mut evicted = Vec::new();
        {
            let mut inodes = self.inodes.write().unwrap();
            for (inode, count) in requests {
                if let Some(data) = forget_one(&mut inodes, inode, count) {
                    evicted.push(data);
                }
            }
        }
        let mut dir_evicted = false;
        for data in evicted {
            if u32::from(data.kind) == libc::S_IFDIR as u32 {
                dir_evicted = true;
            }
            finish_forget(data);
        }
        if dir_evicted {
            self.cfg.guest_cache.note_dir_change();
        }
    }

    fn opendir(
        &self,
        ctx: Context,
        inode: Inode,
        flags: u32,
    ) -> io::Result<(Option<Handle>, OpenOptions)> {
        self.do_open(&ctx, inode, false, flags, true)
    }

    fn releasedir(
        &self,
        _ctx: Context,
        inode: Inode,
        _flags: u32,
        handle: Handle,
    ) -> io::Result<()> {
        self.do_release(inode, handle)
    }

    fn mkdir(
        &self,
        ctx: Context,
        parent: Inode,
        name: &CStr,
        mode: u32,
        umask: u32,
        extensions: Extensions,
    ) -> io::Result<Entry> {
        let c_path = self.name_to_path(parent, name)?;

        let (host_mode, complete) = match self.cfg.semantics {
            PermissionSemantics::LinuxComplete => (0o700, true),
            PermissionSemantics::LinuxSimplified
            | PermissionSemantics::LinuxSimplifiedFixedOwner => ((mode & !umask) as u16, false),
        };

        // Safe because this doesn't modify any memory and we check the return value.
        let res = unsafe { libc::mkdir(c_path.as_ptr(), host_mode) };
        if res == 0 {
            let ihandle = InodeHandle::Path(c_path);
            // Set security context
            if let Some(secctx) = extensions.secctx {
                set_secctx(&ihandle, secctx, false)?
            };

            if complete {
                set_stat(
                    &ctx,
                    self.cfg.semantics,
                    &ihandle,
                    None,
                    Some((ctx.uid, ctx.gid)),
                    Some(mode & !umask),
                )?;
            }
            self.lookup(ctx, parent, name)
        } else {
            Err(linux_error(io::Error::last_os_error()))
        }
    }

    fn rmdir(&self, ctx: Context, parent: Inode, name: &CStr) -> io::Result<()> {
        self.do_unlink(ctx, parent, name, libc::AT_REMOVEDIR)
    }

    fn readdir<F>(
        &self,
        _ctx: Context,
        inode: Inode,
        handle: Handle,
        size: u32,
        offset: u64,
        mut add_entry: F,
    ) -> io::Result<()>
    where
        F: FnMut(DirEntry) -> io::Result<usize>,
    {
        self.do_readdir(inode, handle, size, offset, false, |dir_entry, _| {
            add_entry(dir_entry)
        })
    }

    fn readdirplus<F>(
        &self,
        ctx: Context,
        inode: Inode,
        handle: Handle,
        size: u32,
        offset: u64,
        mut add_entry: F,
    ) -> io::Result<()>
    where
        F: FnMut(DirEntry, Entry) -> io::Result<usize>,
    {
        let parent_data = self
            .inodes
            .read()
            .unwrap()
            .get(&inode)
            .cloned()
            .ok_or_else(ebadf)?;
        self.do_readdir(inode, handle, size, offset, true, |dir_entry, cached| {
            // The directory stream keeps each name without a NUL, and without "." and "..".
            let looked_up = with_c_name(dir_entry.name, |name| match cached.attr {
                Some(st) => self.bulk_entry(&ctx, &parent_data, name, st, cached.no_xattrs),
                None => self.lookup(ctx, inode, name),
            });
            let entry = match looked_up {
                Ok(entry) => entry,
                // A name removed since the directory was read still lists, without attributes,
                // instead of ending the reply early.
                Err(e) if e.raw_os_error() == Some(libc::ENOENT) => {
                    return add_entry(dir_entry, Entry::without_attributes());
                }
                Err(e) => return Err(e),
            };
            let looked_up = entry.inode;
            match add_entry(dir_entry, entry) {
                // The entry did not fit, so the kernel never saw the reference this lookup took.
                Ok(0) => {
                    let evicted = forget_one(&mut self.inodes.write().unwrap(), looked_up, 1);
                    if let Some(data) = evicted {
                        let is_dir = u32::from(data.kind) == libc::S_IFDIR as u32;
                        finish_forget(data);
                        if is_dir {
                            self.cfg.guest_cache.note_dir_change();
                        }
                    }
                    Ok(0)
                }
                Err(e) => {
                    let evicted = forget_one(&mut self.inodes.write().unwrap(), looked_up, 1);
                    if let Some(data) = evicted {
                        let is_dir = u32::from(data.kind) == libc::S_IFDIR as u32;
                        finish_forget(data);
                        if is_dir {
                            self.cfg.guest_cache.note_dir_change();
                        }
                    }
                    Err(e)
                }
                added => added,
            }
        })
    }

    fn open(
        &self,
        ctx: Context,
        inode: Inode,
        kill_priv: bool,
        flags: u32,
    ) -> io::Result<(Option<Handle>, OpenOptions)> {
        self.do_open(&ctx, inode, kill_priv, flags, false)
    }

    fn release(
        &self,
        _ctx: Context,
        inode: Inode,
        _flags: u32,
        handle: Handle,
        _flush: bool,
        _flock_release: bool,
        _lock_owner: Option<u64>,
    ) -> io::Result<()> {
        self.do_release(inode, handle)
    }

    fn create(
        &self,
        ctx: Context,
        parent: Inode,
        name: &CStr,
        mode: u32,
        kill_priv: bool,
        flags: u32,
        umask: u32,
        extensions: Extensions,
    ) -> io::Result<(Entry, Option<Handle>, OpenOptions)> {
        let parent_data = self.node(parent)?;

        let flags = self.parse_open_flags(flags as i32);
        let (host_mode, complete) = match self.cfg.semantics {
            PermissionSemantics::LinuxComplete => {
                let mode = if (flags & libc::O_DIRECTORY) != 0 {
                    0o700
                } else {
                    0o600
                };
                (mode, true)
            }
            PermissionSemantics::LinuxSimplified
            | PermissionSemantics::LinuxSimplifiedFixedOwner => (mode & !(umask & 0o777), false),
        };

        let file = self.create_file(&parent_data, name, flags, host_mode)?;
        let fd = file.as_raw_fd();
        let ihandle = InodeHandle::Fd(fd);

        if complete {
            set_stat(
                &ctx,
                self.cfg.semantics,
                &ihandle,
                None,
                Some((ctx.uid, ctx.gid)),
                Some(libc::S_IFREG as u32 | (mode & !(umask & 0o777))),
            )?;
        }

        // Set security context
        if let Some(secctx) = extensions.secctx {
            set_secctx(&ihandle, secctx, false)?
        };

        // If O_TRUNC and kill_priv (OPEN_KILL_SUIDGID), clear security.capability.
        // We don't need to clear suid/sgid here because we've just updated them
        // unconditionally above.
        if (flags & libc::O_TRUNC) != 0 && kill_priv {
            remove_security_capability(&ihandle);
        }

        // The entry comes from the new descriptor: a lookup of the name would resolve the path
        // and stat the file a second time.
        let st = fstat(&ctx, self.cfg.ownership(), fd, false)?;
        let entry = self.register_entry(&parent_data, name, st);
        let file = Arc::new(RwLock::new(file));

        let handle = self.handles.insert(Arc::new(HandleData::new(
            self.node(entry.inode)?,
            self.open_tracker.clone(),
            file,
        )));

        let mut opts = OpenOptions::empty();
        match self.cfg.cache_policy {
            CachePolicy::Never => opts |= OpenOptions::DIRECT_IO,
            CachePolicy::Always => opts |= OpenOptions::KEEP_CACHE,
            _ => {}
        };

        Ok((entry, Some(handle), opts))
    }

    fn unlink(&self, ctx: Context, parent: Inode, name: &CStr) -> io::Result<()> {
        self.do_unlink(ctx, parent, name, 0)
    }

    fn read<W: io::Write + ZeroCopyWriter>(
        &self,
        _ctx: Context,
        inode: Inode,
        handle: Handle,
        mut w: W,
        size: u32,
        offset: u64,
        _lock_owner: Option<u64>,
        _flags: u32,
    ) -> io::Result<usize> {
        debug!("read: {inode:?}");
        let data = self.handle_data(inode, handle)?;

        // This is safe because write_from uses preadv64, so the underlying file descriptor
        // offset is not affected by this operation.
        let f = data.file.read().unwrap();
        w.write_from(&f, size as usize, offset)
    }

    fn write<R: io::Read + ZeroCopyReader>(
        &self,
        ctx: Context,
        inode: Inode,
        handle: Handle,
        mut r: R,
        size: u32,
        offset: u64,
        _lock_owner: Option<u64>,
        _delayed_write: bool,
        kill_priv: bool,
        _flags: u32,
    ) -> io::Result<usize> {
        let data = self.handle_data(inode, handle)?;

        // This is safe because read_to uses pwritev64, so the underlying file descriptor
        // offset is not affected by this operation.
        let f = data.file.read().unwrap();
        let result = r.read_to(&f, size as usize, offset);

        // If write succeeded and kill_priv is set, clear security.capability and suid/sgid
        if result.is_ok() && kill_priv {
            let fd = f.as_raw_fd();
            let ihandle = InodeHandle::Fd(fd);

            remove_security_capability(&ihandle);

            if let Ok(st) = fstat(&ctx, self.cfg.ownership(), fd, false) {
                let new_mode = clear_suid_sgid(st.st_mode as u32);
                if new_mode != st.st_mode as u32 {
                    // Update mode in xattr
                    if let Err(err) = set_stat(
                        &ctx,
                        self.cfg.semantics,
                        &ihandle,
                        Some(st),
                        None,
                        Some(new_mode),
                    ) {
                        error!("Couldn't clear suid/sgid for inode {inode}: {err}");
                    }
                }
            }
        }

        result
    }

    fn getattr(
        &self,
        ctx: Context,
        inode: Inode,
        _handle: Option<Handle>,
    ) -> io::Result<(bindings::stat64, Duration)> {
        self.do_getattr(&ctx, inode)
    }

    fn setattr(
        &self,
        ctx: Context,
        inode: Inode,
        attr: bindings::stat64,
        handle: Option<Handle>,
        valid: SetattrValid,
    ) -> io::Result<(bindings::stat64, Duration)> {
        // If we have a handle then use it otherwise get a new fd from the inode.
        let ihandle = if let Some(handle) = handle {
            let hd = self.handle_data(inode, handle)?;

            let fd = hd.file.write().unwrap().as_raw_fd();
            InodeHandle::Fd(fd)
        } else {
            self.inode_to_handle(inode, true)?
        };

        if valid.contains(SetattrValid::MODE) {
            set_stat(
                &ctx,
                self.cfg.semantics,
                &ihandle,
                None,
                None,
                Some(attr.st_mode as u32),
            )?
        }

        if valid.intersects(SetattrValid::UID | SetattrValid::GID) {
            let uid = if valid.contains(SetattrValid::UID) {
                attr.st_uid
            } else {
                // Cannot use -1 here because these are unsigned values.
                u32::MAX
            };
            let gid = if valid.contains(SetattrValid::GID) {
                attr.st_gid
            } else {
                // Cannot use -1 here because these are unsigned values.
                u32::MAX
            };

            remove_security_capability(&ihandle);
            let st = istat(&ctx, self.cfg.ownership(), &ihandle, false)?;

            // Clear suid/sgid if UID or GID is being changed
            let new_mode = clear_suid_sgid(st.st_mode as u32);
            let new_mode = if new_mode != st.st_mode as u32 {
                Some(new_mode)
            } else {
                None
            };
            set_stat(
                &ctx,
                self.cfg.semantics,
                &ihandle,
                Some(st),
                Some((uid, gid)),
                new_mode,
            )?;
        }

        if valid.contains(SetattrValid::SIZE) {
            // Safe because this doesn't modify any memory and we check the return value.
            match ihandle {
                InodeHandle::Fd(fd) => {
                    let res = unsafe { libc::ftruncate(fd, attr.st_size) };
                    if res < 0 {
                        return Err(linux_error(io::Error::last_os_error()));
                    }

                    // Clear security.capability on truncate unconditionally
                    remove_security_capability(&ihandle);
                    let st = fstat(&ctx, self.cfg.ownership(), fd, false)?;
                    let new_mode = clear_suid_sgid(st.st_mode as u32);
                    if new_mode != st.st_mode as u32 {
                        set_stat(
                            &ctx,
                            self.cfg.semantics,
                            &ihandle,
                            Some(st),
                            None,
                            Some(new_mode),
                        )?;
                    }
                }
                InodeHandle::Path(_) => {
                    // There is no `ftruncateat` so we need to get a new fd and truncate it.
                    let data = self.node(inode)?;
                    let f = self.open_inode(&data, libc::O_NONBLOCK | libc::O_RDWR)?;
                    let res = unsafe { libc::ftruncate(f.as_raw_fd(), attr.st_size) };
                    if res < 0 {
                        return Err(linux_error(io::Error::last_os_error()));
                    }

                    // Clear security.capability on truncate unconditionally
                    //
                    // Do this here even if it means duplicating the code above to be able to
                    // reuse the FD we just opened, thus reducing the number of syscalls.
                    let ihandle = InodeHandle::Fd(f.as_raw_fd());
                    remove_security_capability(&ihandle);
                    let st = istat(&ctx, self.cfg.ownership(), &ihandle, false)?;
                    let new_mode = clear_suid_sgid(st.st_mode as u32);
                    if new_mode != st.st_mode as u32 {
                        set_stat(
                            &ctx,
                            self.cfg.semantics,
                            &ihandle,
                            Some(st),
                            None,
                            Some(new_mode),
                        )?;
                    }
                }
            };
        }

        if valid.intersects(SetattrValid::ATIME | SetattrValid::MTIME) {
            let mut tvs = [
                libc::timespec {
                    tv_sec: 0,
                    tv_nsec: libc::UTIME_OMIT,
                },
                libc::timespec {
                    tv_sec: 0,
                    tv_nsec: libc::UTIME_OMIT,
                },
            ];

            if valid.contains(SetattrValid::ATIME_NOW) {
                tvs[0].tv_nsec = libc::UTIME_NOW;
            } else if valid.contains(SetattrValid::ATIME) {
                tvs[0].tv_sec = attr.st_atime;
                tvs[0].tv_nsec = attr.st_atime_nsec;
            }

            if valid.contains(SetattrValid::MTIME_NOW) {
                tvs[1].tv_nsec = libc::UTIME_NOW;
            } else if valid.contains(SetattrValid::MTIME) {
                tvs[1].tv_sec = attr.st_mtime;
                tvs[1].tv_nsec = attr.st_mtime_nsec;
            }

            // Safe because this doesn't modify any memory and we check the return value.
            let res = match ihandle {
                InodeHandle::Fd(fd) => unsafe { libc::futimens(fd, tvs.as_ptr()) },
                // One call instead of open, futimens and close; the times of a
                // symlink itself, as with O_SYMLINK.
                InodeHandle::Path(c_path) => unsafe {
                    libc::utimensat(
                        libc::AT_FDCWD,
                        c_path.as_ptr(),
                        tvs.as_ptr(),
                        libc::AT_SYMLINK_NOFOLLOW,
                    )
                },
            };
            if res < 0 {
                return Err(io::Error::last_os_error());
            }
        }

        self.do_getattr(&ctx, inode)
    }

    fn rename(
        &self,
        ctx: Context,
        olddir: Inode,
        oldname: &CStr,
        newdir: Inode,
        newname: &CStr,
        flags: u32,
    ) -> io::Result<()> {
        let mut mflags: u32 = 0;
        if ((flags as i32) & bindings::LINUX_RENAME_NOREPLACE) != 0 {
            mflags |= libc::RENAME_EXCL;
        }
        if ((flags as i32) & bindings::LINUX_RENAME_EXCHANGE) != 0 {
            mflags |= libc::RENAME_SWAP;
        }

        if ((flags as i32) & bindings::LINUX_RENAME_WHITEOUT) != 0
            && ((flags as i32) & bindings::LINUX_RENAME_EXCHANGE) != 0
        {
            return Err(linux_error(io::Error::from_raw_os_error(libc::EINVAL)));
        }

        let old_cpath = self.name_to_path(olddir, oldname)?;
        let new_cpath = self.name_to_path(newdir, newname)?;

        // macOS addresses inodes by their volfs path ("/.vol/{dev}/{ino}"),
        // which only resolves while the inode still has a directory entry. A
        // rename that REPLACES an existing target drops that target's last
        // link, so any inode the guest still holds open there would afterwards
        // resolve to a dangling volfs path and fail path-based ops
        // (getattr/open/setattr/...) with ENOENT (e.g. apt/dpkg's atomic
        // rewrite of /var/lib/dpkg/status, surfaced as
        // "close (2: No such file or directory)"). `do_unlink` already guards
        // the unlink case by stashing an fd to the doomed inode in
        // `InodeData.unlinked_fd`; mirror that for the overwritten target. Grab
        // it *before* the rename, while its entry still exists. RENAME_SWAP
        // keeps both inodes linked and RENAME_EXCL never overwrites, so skip
        // those; best-effort otherwise (a non-overwriting rename finds nothing).
        let doomed_fd = if (flags as i32)
            & (bindings::LINUX_RENAME_EXCHANGE | bindings::LINUX_RENAME_NOREPLACE)
            == 0
            && (self.open_tracker.may_have_open_target(newdir, false)
                || self.open_tracker.may_have_open_target(newdir, true))
        {
            self.node(newdir)
                .and_then(|data| self.dir_fd(&data))
                .ok()
                .and_then(|dir| self.keep_if_open(dir.fd(), newname))
        } else {
            None
        };

        let res = unsafe { libc::renamex_np(old_cpath.as_ptr(), new_cpath.as_ptr(), mflags) };
        if res == 0 {
            // If the rename overwrote a tracked inode, hand its preserved fd to
            // the inode store so later ops resolve by fd, not the vanished path.
            // `store_unlinked_fd` takes ownership only when that inode is
            // tracked; close the fd ourselves otherwise so it is never leaked.
            if let Some(fd) = doomed_fd {
                match self.store_unlinked_fd(&ctx, fd) {
                    Ok(true) => {}
                    Ok(false) | Err(_) => unsafe {
                        libc::close(fd);
                    },
                }
            }

            if ((flags as i32) & bindings::LINUX_RENAME_WHITEOUT) != 0 {
                let (host_mode, complete) = match self.cfg.semantics {
                    PermissionSemantics::LinuxComplete => (0o600, true),
                    PermissionSemantics::LinuxSimplified
                    | PermissionSemantics::LinuxSimplifiedFixedOwner => {
                        (((libc::S_IFCHR | 0o600) as u32), false)
                    }
                };
                let fd = unsafe {
                    libc::open(
                        old_cpath.as_ptr(),
                        libc::O_CREAT | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                        host_mode,
                    )
                };
                if fd > 0 {
                    if complete
                        && let Err(e) = set_stat(
                            &ctx,
                            self.cfg.semantics,
                            &InodeHandle::Fd(fd),
                            None,
                            None,
                            Some((libc::S_IFCHR | 0o600) as u32),
                        )
                    {
                        unsafe { libc::close(fd) };
                        return Err(e);
                    }

                    unsafe { libc::close(fd) };
                }
            }

            let entry = self.lookup(ctx, newdir, newname)?;
            self.forget(ctx, entry.inode, 1);

            Ok(())
        } else {
            if let Some(fd) = doomed_fd {
                // The rename failed; nothing was overwritten. Drop the fd.
                unsafe { libc::close(fd) };
            }
            Err(linux_error(io::Error::last_os_error()))
        }
    }

    fn mknod(
        &self,
        ctx: Context,
        parent: Inode,
        name: &CStr,
        mode: u32,
        rdev: u32,
        umask: u32,
        extensions: Extensions,
    ) -> io::Result<Entry> {
        match self.cfg.semantics {
            PermissionSemantics::LinuxComplete => {
                self.mknod_complete(ctx, parent, name, mode, rdev, umask, extensions)
            }
            PermissionSemantics::LinuxSimplified
            | PermissionSemantics::LinuxSimplifiedFixedOwner => {
                self.mknod_simplified(ctx, parent, name, mode, rdev, umask, extensions)
            }
        }
    }

    fn link(
        &self,
        ctx: Context,
        inode: Inode,
        newparent: Inode,
        newname: &CStr,
    ) -> io::Result<Entry> {
        let orig_c_path = match self.inode_to_handle(inode, false)? {
            InodeHandle::Path(c_path) => c_path,
            InodeHandle::Fd(_) => return Err(ebadf()),
        };
        let link_c_path = self.name_to_path(newparent, newname)?;

        // Safe because this doesn't modify any memory and we check the return value.
        let res = unsafe { libc::link(orig_c_path.as_ptr(), link_c_path.as_ptr()) };
        if res == 0 {
            if let Ok(node) = self.node(inode) {
                node.hardlinked.store(true, Ordering::Release);
            }
            self.lookup(ctx, newparent, newname)
        } else {
            Err(linux_error(io::Error::last_os_error()))
        }
    }

    fn symlink(
        &self,
        ctx: Context,
        linkname: &CStr,
        parent: Inode,
        name: &CStr,
        extensions: Extensions,
    ) -> io::Result<Entry> {
        let c_path = self.name_to_path(parent, name)?;

        // Safe because this doesn't modify any memory and we check the return value.
        let res = unsafe { libc::symlink(linkname.as_ptr(), c_path.as_ptr()) };
        if res == 0 {
            let ihandle = InodeHandle::Path(c_path);

            // Set security context
            if let Some(secctx) = extensions.secctx {
                set_secctx(&ihandle, secctx, true)?
            };

            let mut entry = self.lookup(ctx, parent, name)?;
            if matches!(self.cfg.semantics, PermissionSemantics::LinuxComplete) {
                let mode = libc::S_IFLNK | 0o777;
                set_stat(
                    &ctx,
                    self.cfg.semantics,
                    &ihandle,
                    None,
                    Some((ctx.uid, ctx.gid)),
                    Some(mode as u32),
                )?;
                entry.attr.st_uid = ctx.uid;
                entry.attr.st_gid = ctx.gid;
                entry.attr.st_mode = mode;
            }
            Ok(entry)
        } else {
            Err(linux_error(io::Error::last_os_error()))
        }
    }

    fn readlink(&self, _ctx: Context, inode: Inode) -> io::Result<Vec<u8>> {
        let mut buf = vec![0; libc::PATH_MAX as usize];

        let res = match self.inode_to_handle(inode, true)? {
            InodeHandle::Path(c_path) => unsafe {
                libc::readlink(
                    c_path.as_ptr(),
                    buf.as_mut_ptr() as *mut libc::c_char,
                    buf.len(),
                )
            },
            InodeHandle::Fd(fd) => unsafe {
                libc::freadlink(fd, buf.as_mut_ptr() as *mut libc::c_char, buf.len()) as isize
            },
        };
        if res < 0 {
            return Err(linux_error(io::Error::last_os_error()));
        }

        buf.resize(res as usize, 0);
        Ok(buf)
    }

    fn flush(
        &self,
        _ctx: Context,
        _inode: Inode,
        _handle: Handle,
        _lock_owner: u64,
    ) -> io::Result<()> {
        // Locks stay in the guest and closing a duplicate of the host file reports nothing, so a
        // FLUSH would only cost every close() a round trip. ENOSYS makes the guest stop sending
        // them; it still writes back dirty pages before it would have.
        Err(linux_error(io::Error::from_raw_os_error(libc::ENOSYS)))
    }

    fn fsync(
        &self,
        _ctx: Context,
        inode: Inode,
        _datasync: bool,
        handle: Handle,
    ) -> io::Result<()> {
        let data = self.handle_data(inode, handle)?;

        let fd = data.file.write().unwrap().as_raw_fd();

        fileops::fsync(fd, self.cfg.no_sync_io).map_err(linux_error)
    }

    fn fsyncdir(
        &self,
        ctx: Context,
        inode: Inode,
        datasync: bool,
        handle: Handle,
    ) -> io::Result<()> {
        self.fsync(ctx, inode, datasync, handle)
    }

    fn access(&self, ctx: Context, inode: Inode, mask: u32) -> io::Result<()> {
        let mut st = match self.inode_to_handle(inode, true)? {
            InodeHandle::Path(c_path) => lstat(&ctx, self.cfg.ownership(), &c_path, false)?,
            InodeHandle::Fd(fd) => fstat(&ctx, self.cfg.ownership(), fd, false)?,
        };
        if self.cfg.caller_owns {
            st.st_uid = ctx.uid;
            st.st_gid = ctx.gid;
        }

        let mode = mask as i32 & (libc::R_OK | libc::W_OK | libc::X_OK);

        if mode == libc::F_OK {
            // The file exists since we were able to call `stat(2)` on it.
            return Ok(());
        }

        // We use ctx.uid/ctx.gid for these checks, but when idmapped mounts
        // support is enabled on the guest side, it means that "default_permissions"
        // flag is set on virtiofs mount and FUSE_ACCESS request should never be
        // sent to the userspace. Please, refer to the kernel commit
        // ("fs/fuse: warn if fuse_access is called when idmapped mounts are allowed").
        // In case when idmapped mounts are not enabled we are good to rely on ctx.uid/ctx.gid values.

        if (mode & libc::R_OK) != 0
            && ctx.uid != 0
            && (st.st_uid != ctx.uid || st.st_mode & 0o400 == 0)
            && (st.st_gid != ctx.gid || st.st_mode & 0o040 == 0)
            && st.st_mode & 0o004 == 0
        {
            return Err(linux_error(io::Error::from_raw_os_error(libc::EACCES)));
        }

        if (mode & libc::W_OK) != 0
            && ctx.uid != 0
            && (st.st_uid != ctx.uid || st.st_mode & 0o200 == 0)
            && (st.st_gid != ctx.gid || st.st_mode & 0o020 == 0)
            && st.st_mode & 0o002 == 0
        {
            return Err(linux_error(io::Error::from_raw_os_error(libc::EACCES)));
        }

        // root can only execute something if it is executable by one of the owner, the group, or
        // everyone.
        if (mode & libc::X_OK) != 0
            && (ctx.uid != 0 || st.st_mode & 0o111 == 0)
            && (st.st_uid != ctx.uid || st.st_mode & 0o100 == 0)
            && (st.st_gid != ctx.gid || st.st_mode & 0o010 == 0)
            && st.st_mode & 0o001 == 0
        {
            return Err(linux_error(io::Error::from_raw_os_error(libc::EACCES)));
        }

        Ok(())
    }

    fn setxattr(
        &self,
        _ctx: Context,
        inode: Inode,
        name: &CStr,
        value: &[u8],
        flags: u32,
    ) -> io::Result<()> {
        debug!("setxattr: inode={inode} name={name:?} value={value:?}");

        if !self.cfg.xattr {
            return Err(linux_error(io::Error::from_raw_os_error(libc::ENOSYS)));
        }

        if name.to_bytes() == XATTR_KEY {
            return Err(linux_error(io::Error::from_raw_os_error(libc::EACCES)));
        }

        if name.to_bytes().starts_with(MACOS_XATTR_PREFIX) {
            return Err(linux_error(io::Error::from_raw_os_error(libc::EOPNOTSUPP)));
        }

        let mut mflags: i32 = 0;
        if (flags as i32) & bindings::LINUX_XATTR_CREATE != 0 {
            mflags |= libc::XATTR_CREATE;
        }
        if (flags as i32) & bindings::LINUX_XATTR_REPLACE != 0 {
            mflags |= libc::XATTR_REPLACE;
        }

        // Safe because this doesn't modify any memory and we check the return value.
        let res = match self.inode_to_handle(inode, true)? {
            InodeHandle::Path(c_path) => unsafe {
                libc::setxattr(
                    c_path.as_ptr(),
                    name.as_ptr(),
                    value.as_ptr() as *const libc::c_void,
                    value.len(),
                    0,
                    mflags as libc::c_int,
                )
            },
            InodeHandle::Fd(fd) => unsafe {
                libc::fsetxattr(
                    fd,
                    name.as_ptr(),
                    value.as_ptr() as *const libc::c_void,
                    value.len(),
                    0,
                    mflags as libc::c_int,
                )
            },
        };

        if res == 0 {
            Ok(())
        } else {
            Err(linux_error(io::Error::last_os_error()))
        }
    }

    fn getxattr(
        &self,
        _ctx: Context,
        inode: Inode,
        name: &CStr,
        size: u32,
    ) -> io::Result<GetxattrReply> {
        debug!("getxattr: inode={inode} name={name:?}, size={size}");

        if !self.cfg.xattr {
            return Err(linux_error(io::Error::from_raw_os_error(libc::ENOSYS)));
        }

        if name.to_bytes() == XATTR_KEY {
            return Err(linux_error(io::Error::from_raw_os_error(libc::EACCES)));
        }

        if name.to_bytes().starts_with(MACOS_XATTR_PREFIX) {
            return Err(linux_error(io::Error::from_raw_os_error(libc::ENODATA)));
        }

        let mut buf = vec![0; size as usize];

        // Safe because this will only modify the contents of `buf`
        let res = match self.inode_to_handle(inode, true)? {
            InodeHandle::Path(c_path) => unsafe {
                if size == 0 {
                    libc::getxattr(
                        c_path.as_ptr(),
                        name.as_ptr(),
                        std::ptr::null_mut(),
                        size as libc::size_t,
                        0,
                        0,
                    )
                } else {
                    libc::getxattr(
                        c_path.as_ptr(),
                        name.as_ptr(),
                        buf.as_mut_ptr() as *mut libc::c_void,
                        size as libc::size_t,
                        0,
                        0,
                    )
                }
            },
            InodeHandle::Fd(fd) => unsafe {
                if size == 0 {
                    libc::fgetxattr(
                        fd,
                        name.as_ptr(),
                        std::ptr::null_mut(),
                        size as libc::size_t,
                        0,
                        0,
                    )
                } else {
                    libc::fgetxattr(
                        fd,
                        name.as_ptr(),
                        buf.as_mut_ptr() as *mut libc::c_void,
                        size as libc::size_t,
                        0,
                        0,
                    )
                }
            },
        };
        if res < 0 {
            return Err(linux_error(io::Error::last_os_error()));
        }

        if size == 0 {
            Ok(GetxattrReply::Count(res as u32))
        } else {
            buf.resize(res as usize, 0);
            Ok(GetxattrReply::Value(buf))
        }
    }

    fn listxattr(&self, _ctx: Context, inode: Inode, size: u32) -> io::Result<ListxattrReply> {
        if !self.cfg.xattr {
            return Err(linux_error(io::Error::from_raw_os_error(libc::ENOSYS)));
        }

        let mut buf = vec![0; 512_usize];

        // Safe because this will only modify the contents of `buf`.
        let res = match self.inode_to_handle(inode, true)? {
            InodeHandle::Path(c_path) => unsafe {
                libc::listxattr(
                    c_path.as_ptr(),
                    buf.as_mut_ptr() as *mut libc::c_char,
                    512,
                    0,
                )
            },
            InodeHandle::Fd(fd) => unsafe {
                libc::flistxattr(fd, buf.as_mut_ptr() as *mut libc::c_char, 512, 0)
            },
        };
        if res < 0 {
            return Err(linux_error(io::Error::last_os_error()));
        }

        buf.truncate(res as usize);

        if size == 0 {
            let mut clean_size = res as usize;

            for attr in buf.split(|c| *c == 0) {
                if attr.starts_with(&XATTR_KEY[..XATTR_KEY.len() - 1]) {
                    clean_size -= XATTR_KEY.len();
                } else if attr.starts_with(MACOS_XATTR_PREFIX) {
                    // attr does not include the null terminator; add 1 for it.
                    clean_size -= attr.len() + 1;
                }
            }

            Ok(ListxattrReply::Count(clean_size as u32))
        } else {
            let mut clean_buf = Vec::new();

            for attr in buf.split(|c| *c == 0) {
                if attr.is_empty()
                    || attr.starts_with(&XATTR_KEY[..XATTR_KEY.len() - 1])
                    || attr.starts_with(MACOS_XATTR_PREFIX)
                {
                    continue;
                }

                clean_buf.extend_from_slice(attr);
                clean_buf.push(0);
            }

            clean_buf.shrink_to_fit();

            if clean_buf.len() > size as usize {
                Err(io::Error::from_raw_os_error(LINUX_ERANGE))
            } else {
                Ok(ListxattrReply::Names(clean_buf))
            }
        }
    }

    fn removexattr(&self, _ctx: Context, inode: Inode, name: &CStr) -> io::Result<()> {
        if !self.cfg.xattr {
            return Err(linux_error(io::Error::from_raw_os_error(libc::ENOSYS)));
        }

        if name.to_bytes() == XATTR_KEY {
            return Err(linux_error(io::Error::from_raw_os_error(
                bindings::LINUX_EACCES,
            )));
        }

        if name.to_bytes().starts_with(MACOS_XATTR_PREFIX) {
            return Err(linux_error(io::Error::from_raw_os_error(libc::ENODATA)));
        }

        // Safe because this doesn't modify any memory and we check the return value.
        let res = match self.inode_to_handle(inode, true)? {
            InodeHandle::Path(c_path) => unsafe {
                libc::removexattr(c_path.as_ptr(), name.as_ptr(), 0)
            },
            InodeHandle::Fd(fd) => unsafe { libc::fremovexattr(fd, name.as_ptr(), 0) },
        };
        if res == 0 {
            Ok(())
        } else {
            Err(linux_error(io::Error::last_os_error()))
        }
    }

    fn copyfilerange(
        &self,
        _ctx: Context,
        inode_in: Inode,
        handle_in: Handle,
        offset_in: u64,
        inode_out: Inode,
        handle_out: Handle,
        offset_out: u64,
        len: u64,
        flags: u64,
    ) -> io::Result<usize> {
        self.copy_range(
            (inode_in, handle_in, offset_in),
            (inode_out, handle_out, offset_out),
            len,
            flags,
        )
    }

    fn fallocate(
        &self,
        ctx: Context,
        inode: Inode,
        handle: Handle,
        mode: u32,
        offset: u64,
        length: u64,
    ) -> io::Result<()> {
        let data = self.handle_data(inode, handle)?;

        let fd = data.file.write().unwrap().as_raw_fd();

        const SUPPORTED_FLAGS: i32 = bindings::LINUX_FALLOC_FL_ALLOCATE_RANGE
            | bindings::LINUX_FALLOC_FL_KEEP_SIZE
            | bindings::LINUX_FALLOC_FL_PUNCH_HOLE;

        if mode as i32 & !SUPPORTED_FLAGS != 0 {
            return Err(linux_error(io::Error::from_raw_os_error(libc::EOPNOTSUPP)));
        }

        let keep_size = mode & bindings::LINUX_FALLOC_FL_KEEP_SIZE as u32 != 0;
        let mode = mode & !bindings::LINUX_FALLOC_FL_KEEP_SIZE as u32;

        match mode as i32 {
            bindings::LINUX_FALLOC_FL_ALLOCATE_RANGE => {
                // The closest thing we have on macOS to posix_fallocate is F_PREALLOCATE,
                // but this one doesn't allow us to allocate arbitrary ranges, only allocate
                // blocks to the file's end.
                //
                // The best thing we can do here is extend the file to (offset + length).
                // This doesn't adhere to the same semantics, but should work fine (albeit
                // less performant) for most guest applications.
                let st = fstat(&ctx, self.cfg.ownership(), fd, true)?;
                let new_length = (offset + length) as i64;

                if keep_size {
                    // Check the number of allocated blocks instead of the file size.
                    let disk_size = st.st_blocks * 512_i64;
                    if disk_size >= new_length {
                        return Ok(());
                    }
                    let mut fs = libc::fstore_t {
                        fst_flags: libc::F_ALLOCATEALL,
                        fst_posmode: libc::F_PEOFPOSMODE,
                        fst_offset: 0,
                        fst_length: new_length - disk_size,
                        fst_bytesalloc: 0,
                    };

                    let res = unsafe { libc::fcntl(fd, libc::F_PREALLOCATE, &mut fs as *mut _) };
                    if res < 0 {
                        return Err(linux_error(io::Error::last_os_error()));
                    }
                } else {
                    if st.st_size >= new_length {
                        return Ok(());
                    }
                    let res = unsafe { libc::ftruncate(fd, new_length) };
                    if res < 0 {
                        return Err(linux_error(io::Error::last_os_error()));
                    }
                }
            }
            bindings::LINUX_FALLOC_FL_PUNCH_HOLE => {
                if !keep_size {
                    // Linux forbids the use of PUNCH_HOLE without KEEP_SIZE.
                    return Err(linux_error(io::Error::from_raw_os_error(libc::EINVAL)));
                }

                let mut hole = libc::fpunchhole_t {
                    fp_offset: offset as i64,
                    fp_flags: 0,
                    reserved: 0,
                    fp_length: length as i64,
                };

                let res = unsafe { libc::fcntl(fd, libc::F_PUNCHHOLE, &mut hole as *mut _) };
                if res < 0 {
                    return Err(linux_error(io::Error::last_os_error()));
                }
            }
            _ => unreachable!(),
        }

        Ok(())
    }

    fn lseek(
        &self,
        _ctx: Context,
        inode: Inode,
        handle: Handle,
        offset: u64,
        whence: u32,
    ) -> io::Result<u64> {
        let data = self.handle_data(inode, handle)?;

        // SEEK_DATA and SEEK_HOLE have slightly different semantics
        // in Linux vs. macOS, which means we can't support them.
        let mwhence = if whence == 3 {
            // SEEK_DATA
            return Ok(offset);
        } else if whence == 4 {
            // SEEK_HOLE
            libc::SEEK_END
        } else {
            whence as i32
        };

        let fd = data.file.write().unwrap().as_raw_fd();

        // Safe because this doesn't modify any memory and we check the return value.
        let res = unsafe { libc::lseek(fd, offset as bindings::off64_t, mwhence as libc::c_int) };
        if res < 0 {
            Err(linux_error(io::Error::last_os_error()))
        } else {
            Ok(res as u64)
        }
    }

    fn setupmapping(
        &self,
        _ctx: Context,
        inode: Inode,
        _handle: Handle,
        foffset: u64,
        len: u64,
        flags: u64,
        moffset: u64,
        guest_shm_base: u64,
        shm_size: u64,
        map_sender: &Option<Sender<WorkerMessage>>,
    ) -> io::Result<()> {
        if map_sender.is_none() {
            return Err(linux_error(io::Error::from_raw_os_error(libc::ENOSYS)));
        }

        let open_flags = if (flags & fuse::SetupmappingFlags::WRITE.bits()) != 0 {
            libc::O_RDWR
        } else {
            libc::O_RDONLY
        };

        let prot_flags = if (flags & fuse::SetupmappingFlags::WRITE.bits()) != 0 {
            libc::PROT_READ | libc::PROT_WRITE
        } else {
            libc::PROT_READ
        };

        if (moffset + len) > shm_size {
            return Err(linux_error(io::Error::from_raw_os_error(libc::EINVAL)));
        }

        let guest_addr = guest_shm_base + moffset;

        debug!("setupmapping: ino {inode:?} guest_addr={guest_addr:x} len={len}");

        let data = self.node(inode)?;
        let file = self.open_inode(&data, open_flags)?;
        let fd = file.as_raw_fd();

        let host_addr = unsafe {
            libc::mmap(
                null_mut(),
                len as usize,
                prot_flags,
                libc::MAP_SHARED,
                fd,
                foffset as libc::off_t,
            )
        };
        if host_addr == libc::MAP_FAILED {
            return Err(linux_error(io::Error::last_os_error()));
        }

        drop(file);

        // We've checked that map_sender is something above.
        let sender = map_sender.as_ref().unwrap();
        let (reply_sender, reply_receiver) = unbounded();
        sender
            .send(WorkerMessage::GpuAddMapping(
                reply_sender,
                host_addr as u64,
                guest_addr,
                len,
            ))
            .unwrap();
        if !reply_receiver.recv().unwrap() {
            error!("Error requesting HVF the addition of a DAX window");
            unsafe { libc::munmap(host_addr, len as usize) };
            return Err(linux_error(io::Error::from_raw_os_error(libc::EINVAL)));
        }

        self.map_windows
            .lock()
            .unwrap()
            .insert(guest_addr, host_addr as u64);

        Ok(())
    }

    fn removemapping(
        &self,
        _ctx: Context,
        requests: Vec<fuse::RemovemappingOne>,
        guest_shm_base: u64,
        shm_size: u64,
        map_sender: &Option<Sender<WorkerMessage>>,
    ) -> io::Result<()> {
        if map_sender.is_none() {
            return Err(linux_error(io::Error::from_raw_os_error(libc::ENOSYS)));
        }

        for req in requests {
            let guest_addr = guest_shm_base + req.moffset;
            if (req.moffset + req.len) > shm_size {
                return Err(linux_error(io::Error::from_raw_os_error(libc::EINVAL)));
            }
            let host_addr = match self.map_windows.lock().unwrap().remove(&guest_addr) {
                Some(a) => a,
                None => return Err(linux_error(io::Error::from_raw_os_error(libc::EINVAL))),
            };
            debug!(
                "removemapping: guest_addr={:x} len={:?}",
                guest_addr, req.len
            );

            let sender = map_sender.as_ref().unwrap();
            let (reply_sender, reply_receiver) = unbounded();
            sender
                .send(WorkerMessage::GpuRemoveMapping(
                    reply_sender,
                    guest_addr,
                    req.len,
                ))
                .unwrap();
            if !reply_receiver.recv().unwrap() {
                error!("Error requesting HVF the removal of a DAX window");
                return Err(linux_error(io::Error::from_raw_os_error(libc::EINVAL)));
            }

            let ret = unsafe { libc::munmap(host_addr as *mut libc::c_void, req.len as usize) };
            if ret == -1 {
                error!("Error unmapping DAX window");
                return Err(linux_error(io::Error::last_os_error()));
            }
        }

        Ok(())
    }
}

// copy_file_range backed by APFS clones (see fileops.rs). A clone replaces the
// destination file, so its node and open handles are pointed at the clone.
impl PassthroughFs {
    fn copy_range(
        &self,
        (inode_in, handle_in, off_in): (Inode, Handle, u64),
        (inode_out, handle_out, off_out): (Inode, Handle, u64),
        len: u64,
        flags: u64,
    ) -> io::Result<usize> {
        if flags != 0 {
            return Err(linux_error(io::Error::from_raw_os_error(libc::EINVAL)));
        }
        let (src, dst) = (
            self.handle_data(inode_in, handle_in)?,
            self.handle_data(inode_out, handle_out)?,
        );
        let src_fd = src.file.read().unwrap().as_raw_fd();
        let dst_fd = dst.file.read().unwrap().as_raw_fd();
        match fileops::copy_file_range(src_fd, off_in, dst_fd, off_out, len).map_err(linux_error)? {
            fileops::Copied::Bytes(n) => Ok(n as usize),
            fileops::Copied::Cloned(file, n) => {
                self.retarget_inode(inode_out, file)?;
                Ok(n as usize)
            }
        }
    }

    fn retarget_inode(&self, inode: Inode, file: File) -> io::Result<()> {
        let ctx = Context {
            uid: 0,
            gid: 0,
            pid: 0,
        };
        let st = fstat(&ctx, self.cfg.ownership(), file.as_raw_fd(), true)?;
        let old = {
            let mut inodes = self.inodes.write().unwrap();
            let old = inodes.remove(&inode).ok_or_else(ebadf)?;
            let data = InodeData::new(inode, &st, old.refcount.load(Ordering::Acquire));
            data.open_handles
                .store(old.open_handles.load(Ordering::Acquire), Ordering::Release);
            data.anchor_parent
                .store(old.anchor_parent.load(Ordering::Acquire), Ordering::Release);
            if old.hardlinked.load(Ordering::Acquire) {
                data.hardlinked.store(true, Ordering::Release);
            }
            *data.anchor.lock().unwrap() = old.anchor.lock().unwrap().clone();
            inodes.insert(
                inode,
                InodeAltKey {
                    ino: st.st_ino,
                    dev: st.st_dev,
                },
                Arc::new(data),
            );
            old
        };
        let unlinked = old.unlinked_fd.swap(-1, Ordering::AcqRel);
        if unlinked >= 0 {
            unsafe { libc::close(unlinked as RawFd) };
        }
        for hd in self.handles.all() {
            if hd.inode == inode {
                *hd.file.write().unwrap() = file.try_clone()?;
            }
        }
        self.cfg.guest_cache.note_activity();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    use std::path::PathBuf;

    const CTX: Context = Context {
        uid: 0,
        gid: 0,
        pid: 0,
    };
    const LINUX_ARM64_O_DIRECT: i32 = 1 << 16;

    struct Share {
        dir: PathBuf,
        fs: PassthroughFs,
    }

    impl Share {
        fn with_file(name: &str) -> Self {
            Self::with_file_and_policy(name, CachePolicy::default())
        }

        fn with_file_and_policy(name: &str, cache_policy: CachePolicy) -> Self {
            let dir = std::env::temp_dir()
                .join(format!("libkrun-passthrough-{}-{name}", std::process::id()));
            fs::create_dir_all(&dir).unwrap();
            fs::write(dir.join(name), b"contents").unwrap();
            let cfg = Config {
                root_dir: dir.to_str().unwrap().to_string(),
                cache_policy,
                ..Default::default()
            };
            let fs = PassthroughFs::new(cfg, Arc::new(InodeAllocator::new())).unwrap();
            fs.init(FsOptions::empty()).unwrap();
            Share { dir, fs }
        }

        fn inode(&self, name: &str) -> Inode {
            let name = CString::new(name).unwrap();
            self.fs.lookup(CTX, fuse::ROOT_ID, &name).unwrap().inode
        }

        fn open(&self, inode: Inode, flags: i32) -> Handle {
            self.fs
                .open(CTX, inode, false, flags as u32)
                .unwrap()
                .0
                .unwrap()
        }

        fn fd(&self, handle: Handle) -> RawFd {
            self.fs
                .handles
                .get(handle)
                .unwrap()
                .file
                .read()
                .unwrap()
                .as_raw_fd()
        }
    }

    impl Drop for Share {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.dir);
        }
    }

    #[test]
    fn read_only_handles_of_one_inode_share_a_host_file() {
        let share = Share::with_file("shared");
        let inode = share.inode("shared");

        let first = share.open(inode, libc::O_RDONLY);
        let second = share.open(inode, libc::O_RDONLY);

        assert_eq!(share.fd(first), share.fd(second));
    }

    #[test]
    fn read_only_handles_with_other_flags_share_the_host_file() {
        let share = Share::with_file("flags");
        let inode = share.inode("flags");

        let plain = share.open(inode, libc::O_RDONLY);
        for flags in [
            bindings::LINUX_O_NONBLOCK,
            bindings::LINUX_O_SYNC,
            LINUX_ARM64_O_DIRECT,
        ] {
            let handle = share.open(inode, libc::O_RDONLY | flags);
            assert_eq!(share.fd(plain), share.fd(handle), "flags {flags:#x}");
        }
    }

    #[test]
    fn a_writable_handle_gets_its_own_host_file() {
        let share = Share::with_file("writable");
        let inode = share.inode("writable");

        let reader = share.open(inode, libc::O_RDONLY);
        let writer = share.open(inode, libc::O_RDWR);

        assert_ne!(share.fd(reader), share.fd(writer));
    }

    #[test]
    fn directory_handles_keep_their_own_host_file() {
        let share = Share::with_file("dirs");

        let flags = libc::O_RDONLY as u32;
        let first = share
            .fs
            .opendir(CTX, fuse::ROOT_ID, flags)
            .unwrap()
            .0
            .unwrap();
        let second = share
            .fs
            .opendir(CTX, fuse::ROOT_ID, flags)
            .unwrap()
            .0
            .unwrap();

        assert_ne!(share.fd(first), share.fd(second));
    }

    #[test]
    fn auto_policy_keeps_cache_of_unchanged_files() {
        let name = "auto";
        let share = Share::with_file_and_policy(name, CachePolicy::Auto);
        let inode = share.inode(name);
        let open = || share.fs.open(CTX, inode, false, libc::O_RDONLY as u32).unwrap().1;
        assert_eq!(open(), OpenOptions::empty(), "first open");
        assert_eq!(open(), OpenOptions::KEEP_CACHE, "unchanged");
        let file = fs::File::options().write(true).open(share.dir.join(name)).unwrap();
        file.set_modified(std::time::SystemTime::UNIX_EPOCH).unwrap();
        assert_eq!(open(), OpenOptions::empty(), "modified");
        assert_eq!(open(), OpenOptions::KEEP_CACHE, "unchanged again");
    }

    #[test]
    fn cache_policy_applies_file_and_directory_options() {
        for (name, policy, file_options, directory_options) in [
            (
                "uncached",
                CachePolicy::Never,
                OpenOptions::DIRECT_IO,
                OpenOptions::empty(),
            ),
            (
                "cached",
                CachePolicy::Always,
                OpenOptions::KEEP_CACHE,
                OpenOptions::CACHE_DIR,
            ),
        ] {
            let share = Share::with_file_and_policy(name, policy);
            let inode = share.inode(name);
            let flags = (libc::O_RDONLY | LINUX_ARM64_O_DIRECT) as u32;
            let (_, options) = share.fs.open(CTX, inode, false, flags).unwrap();
            assert_eq!(options, file_options, "{policy:?} file");

            let (_, options) = share
                .fs
                .opendir(CTX, fuse::ROOT_ID, libc::O_RDONLY as u32)
                .unwrap();
            assert_eq!(options, directory_options, "{policy:?} directory");
        }
    }

    #[test]
    fn the_shared_host_file_closes_with_the_last_handle() {
        let share = Share::with_file("released");
        let inode = share.inode("released");
        let first = share.open(inode, libc::O_RDONLY);
        let second = share.open(inode, libc::O_RDONLY);

        let shared = Arc::downgrade(&share.fs.handles.get(first).unwrap().file);

        share.fs.do_release(inode, first).unwrap();
        assert!(shared.upgrade().is_some());
        share.fs.do_release(inode, second).unwrap();

        assert!(shared.upgrade().is_none());
    }

    struct Listed {
        name: Vec<u8>,
        ino: u64,
        offset: u64,
        type_: u32,
        entry: Option<Entry>,
    }

    fn passthrough_at(root: &Path, bulk: bool, capable: FsOptions) -> PassthroughFs {
        let cfg = Config {
            root_dir: root.to_str().unwrap().to_string(),
            ..Default::default()
        };
        let mut fs = PassthroughFs::new(cfg, Arc::new(InodeAllocator::new())).unwrap();
        fs.bulk_readdir = bulk;
        fs.init(capable).unwrap();
        fs
    }

    /// Lists the root directory, taking at most `chunk` entries per request and resuming at the
    /// offset of the last entry taken, as the guest kernel does.
    fn list_root(fs: &PassthroughFs, plus: bool, chunk: usize) -> Vec<Listed> {
        let handle = fs
            .opendir(CTX, fuse::ROOT_ID, libc::O_RDONLY as u32)
            .unwrap()
            .0
            .unwrap();
        let mut listed: Vec<Listed> = Vec::new();
        loop {
            let offset = listed.last().map_or(0, |l| l.offset);
            let before = listed.len();
            let mut take = |d: DirEntry, entry: Option<Entry>| {
                if listed.len() - before == chunk {
                    return Ok(0);
                }
                listed.push(Listed {
                    name: d.name.to_vec(),
                    ino: d.ino,
                    offset: d.offset,
                    type_: d.type_,
                    entry,
                });
                Ok(1)
            };
            if plus {
                fs.readdirplus(CTX, fuse::ROOT_ID, handle, 4096, offset, |d, e| {
                    take(d, Some(e))
                })
                .unwrap();
            } else {
                fs.readdir(CTX, fuse::ROOT_ID, handle, 4096, offset, |d| take(d, None))
                    .unwrap();
            }
            if listed.len() == before {
                break;
            }
        }
        fs.releasedir(CTX, fuse::ROOT_ID, 0, handle).unwrap();
        listed.sort_by(|a, b| a.name.cmp(&b.name));
        listed
    }

    fn refcount(fs: &PassthroughFs, inode: Inode) -> u64 {
        fs.inodes
            .read()
            .unwrap()
            .get(&inode)
            .unwrap()
            .refcount
            .load(Ordering::Relaxed)
    }

    fn stat_fields(st: &bindings::stat64) -> Vec<(&'static str, i64)> {
        vec![
            ("dev", st.st_dev as i64),
            ("ino", st.st_ino as i64),
            ("mode", st.st_mode as i64),
            ("nlink", st.st_nlink as i64),
            ("uid", st.st_uid as i64),
            ("gid", st.st_gid as i64),
            ("rdev", st.st_rdev as i64),
            ("size", st.st_size),
            ("blocks", st.st_blocks),
            ("blksize", st.st_blksize as i64),
            ("flags", st.st_flags as i64),
            ("atime", st.st_atime),
            ("atime_nsec", st.st_atime_nsec),
            ("mtime", st.st_mtime),
            ("mtime_nsec", st.st_mtime_nsec),
            ("ctime", st.st_ctime),
            ("ctime_nsec", st.st_ctime_nsec),
        ]
    }

    fn set_xattr(path: &Path, name: &str, value: &[u8]) {
        let path = CString::new(path.to_str().unwrap()).unwrap();
        let name = CString::new(name).unwrap();
        let res = unsafe {
            libc::setxattr(
                path.as_ptr(),
                name.as_ptr(),
                value.as_ptr().cast(),
                value.len(),
                0,
                libc::XATTR_NOFOLLOW,
            )
        };
        assert_eq!(res, 0, "setxattr {name:?}: {}", io::Error::last_os_error());
    }

    /// A directory with one of each kind of entry the guest can meet on a share.
    fn mixed_tree(name: &str) -> PathBuf {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let dir =
            std::env::temp_dir().join(format!("libkrun-passthrough-{}-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("sub/deeper")).unwrap();
        fs::create_dir(dir.join("emptydir")).unwrap();
        fs::write(dir.join("sub/inner"), b"x").unwrap();
        fs::write(dir.join("regular"), vec![7u8; 100_000]).unwrap();
        fs::write(dir.join("empty"), b"").unwrap();
        fs::hard_link(dir.join("regular"), dir.join("hardlink")).unwrap();
        symlink("regular", dir.join("link")).unwrap();
        symlink("missing", dir.join("dangling")).unwrap();
        let fifo = CString::new(dir.join("fifo").to_str().unwrap()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o640) }, 0);
        std::mem::forget(UnixListener::bind(dir.join("socket")).unwrap());
        fs::write(dir.join("setuid"), b"#!/bin/sh\n").unwrap();
        fs::set_permissions(dir.join("setuid"), fs::Permissions::from_mode(0o4751)).unwrap();
        fs::write(dir.join("with-xattr"), b"x").unwrap();
        set_xattr(&dir.join("with-xattr"), "user.test", b"value");
        fs::write(dir.join("resource-fork"), b"data").unwrap();
        set_xattr(
            &dir.join("resource-fork"),
            "com.apple.ResourceFork",
            &[1u8; 20_000],
        );
        fs::write(dir.join("overridden"), b"x").unwrap();
        set_xattr(
            &dir.join("overridden"),
            "user.containers.override_stat",
            b"1000:1001:0100600",
        );
        fs::write(dir.join("\u{10d}\u{107}\u{17e} name with spaces"), b"x").unwrap();
        dir
    }

    #[test]
    fn bulk_attributes_match_lstat() {
        let dir = mixed_tree("bulk-lstat");
        let fd = File::open(&dir).unwrap();
        assert!(attrlist::supported(fd.as_raw_fd()));
        let mut entries = Vec::new();
        attrlist::read_dir(fd.as_raw_fd(), true, &mut entries).unwrap();

        let mut names: Vec<_> = entries.iter().map(|e| e.name.to_vec()).collect();
        names.sort();
        let mut expected: Vec<_> = fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_encoded_bytes())
            .collect();
        expected.sort();
        assert_eq!(names, expected);

        for e in &entries {
            let path =
                CString::new([dir.as_os_str().as_encoded_bytes(), b"/", &e.name].concat()).unwrap();
            let host = lstat(&CTX, Ownership::Xattr, &path, true).unwrap();
            let name = String::from_utf8_lossy(&e.name);
            let attr = e.attr.unwrap_or_else(|| panic!("{name}: no attributes"));
            assert_eq!(stat_fields(&attr), stat_fields(&host), "{name}");
            assert_eq!(e.ino, host.st_ino, "{name}");
            let dt = match host.st_mode & libc::S_IFMT {
                libc::S_IFREG => libc::DT_REG,
                libc::S_IFDIR => libc::DT_DIR,
                libc::S_IFLNK => libc::DT_LNK,
                libc::S_IFIFO => libc::DT_FIFO,
                libc::S_IFSOCK => libc::DT_SOCK,
                _ => unreachable!(),
            };
            assert_eq!(e.type_, dt, "{name}");
            // EF_NO_XATTRS may be missing (hard links never carry it), but it is never wrong.
            let xattrs_len =
                unsafe { libc::listxattr(path.as_ptr(), null_mut(), 0, libc::XATTR_NOFOLLOW) };
            if e.no_xattrs {
                assert_eq!(xattrs_len, 0, "{name}");
            }
            if ["with-xattr", "resource-fork", "overridden"].contains(&&*name) {
                assert!(!e.no_xattrs, "{name}");
            }
        }
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn bulk_and_legacy_listings_match() {
        let dir = mixed_tree("bulk-legacy");
        for plus in [false, true] {
            let bulk_fs = passthrough_at(&dir, true, FsOptions::empty());
            let legacy_fs = passthrough_at(&dir, false, FsOptions::empty());
            let bulk = list_root(&bulk_fs, plus, usize::MAX);
            let legacy = list_root(&legacy_fs, plus, usize::MAX);
            assert_eq!(bulk.len(), legacy.len());
            assert_eq!(bulk.len(), 14);

            for (b, l) in bulk.iter().zip(&legacy) {
                let name = String::from_utf8_lossy(&b.name);
                assert_eq!(b.name, l.name);
                assert_eq!((b.ino, b.type_), (l.ino, l.type_), "{name}");
                let (Some(be), Some(le)) = (&b.entry, &l.entry) else {
                    continue;
                };
                assert_ne!(be.inode, 0, "{name}");
                assert_eq!(stat_fields(&be.attr), stat_fields(&le.attr), "{name}");
                assert_eq!(be.attr_flags, le.attr_flags, "{name}");
                assert_eq!(be.attr_timeout, le.attr_timeout, "{name}");
                assert_eq!(be.entry_timeout, le.entry_timeout, "{name}");
                if name == "overridden" {
                    assert_eq!((be.attr.st_uid, be.attr.st_gid), (1000, 1001));
                    assert_eq!(be.attr.st_mode, libc::S_IFREG | 0o600);
                }

                // Each listed name took one lookup reference; a lookup finds the same inode.
                let names = bulk
                    .iter()
                    .filter(|o| o.entry.as_ref().unwrap().inode == be.inode)
                    .count() as u64;
                assert_eq!(refcount(&bulk_fs, be.inode), names, "{name}");
                assert_eq!(refcount(&legacy_fs, le.inode), names, "{name}");
                let c_name = CString::new(b.name.clone()).unwrap();
                let looked_up = bulk_fs.lookup(CTX, fuse::ROOT_ID, &c_name).unwrap();
                assert_eq!(looked_up.inode, be.inode, "{name}");
                assert_eq!(refcount(&bulk_fs, be.inode), names + 1, "{name}");
                bulk_fs.forget(CTX, be.inode, 1);
            }

            // Hard links share one inode.
            let inode_of = |name: &[u8]| {
                bulk.iter()
                    .find(|l| l.name == name)
                    .unwrap()
                    .entry
                    .as_ref()
                    .map(|e| e.inode)
            };
            assert_eq!(inode_of(b"regular"), inode_of(b"hardlink"));
        }
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn bulk_ownership_follows_the_permission_semantics() {
        let dir = mixed_tree("bulk-owner");
        let fixed = |bulk| {
            let cfg = Config {
                root_dir: dir.to_str().unwrap().to_string(),
                semantics: PermissionSemantics::LinuxSimplifiedFixedOwner,
                fixed_owner: Some((0, 0)),
                ..Default::default()
            };
            let mut fs = PassthroughFs::new(cfg, Arc::new(InodeAllocator::new())).unwrap();
            fs.bulk_readdir = bulk;
            fs.init(FsOptions::empty()).unwrap();
            list_root(&fs, true, usize::MAX)
        };
        let (bulk, legacy) = (fixed(true), fixed(false));
        for (b, l) in bulk.iter().zip(&legacy) {
            let (be, le) = (b.entry.as_ref().unwrap(), l.entry.as_ref().unwrap());
            assert_eq!((be.attr.st_uid, be.attr.st_gid), (0, 0));
            assert_eq!(stat_fields(&be.attr), stat_fields(&le.attr));
        }
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn bulk_listing_resumes_at_each_offset_and_refreshes_at_zero() {
        let dir = std::env::temp_dir().join(format!(
            "libkrun-passthrough-{}-bulk-large",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        // Enough entries for several 16 KiB getattrlistbulk batches.
        for i in 0..3000 {
            fs::write(dir.join(format!("file-with-a-long-name-{i:05}")), b"").unwrap();
        }
        let fs = passthrough_at(&dir, true, FsOptions::empty());

        let listed = list_root(&fs, true, 7);
        assert_eq!(listed.len(), 3000);
        let mut offsets: Vec<_> = listed.iter().map(|l| l.offset).collect();
        offsets.sort();
        assert_eq!(offsets, (1..=3000).collect::<Vec<_>>());
        assert!(listed.iter().all(|l| l.entry.as_ref().unwrap().inode != 0));
        assert!(listed.windows(2).all(|w| w[0].name != w[1].name));

        fs::write(dir.join("added"), b"").unwrap();
        let plain = list_root(&fs, false, 100);
        assert_eq!(plain.len(), 3001);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn mountpoints_and_firmlinks_list_like_lookup() {
        for root in ["/", "/System/Volumes"] {
            let bulk_fs = passthrough_at(Path::new(root), true, FsOptions::SUBMOUNTS);
            let legacy_fs = passthrough_at(Path::new(root), false, FsOptions::SUBMOUNTS);
            let bulk = list_root(&bulk_fs, true, usize::MAX);
            let legacy = list_root(&legacy_fs, true, usize::MAX);
            assert_eq!(
                bulk.iter().map(|l| &l.name).collect::<Vec<_>>(),
                legacy.iter().map(|l| &l.name).collect::<Vec<_>>()
            );
            let mut submounts = 0;
            for (b, l) in bulk.iter().zip(&legacy) {
                let name = String::from_utf8_lossy(&b.name);
                let (be, le) = (b.entry.as_ref().unwrap(), l.entry.as_ref().unwrap());
                let id = |e: &Entry| (e.attr.st_dev, e.attr.st_ino, e.attr.st_mode, e.attr_flags);
                assert_eq!(id(be), id(le), "{root} {name}");
                if be.attr_flags & fuse::ATTR_SUBMOUNT != 0 {
                    submounts += 1;
                }
            }
            if root == "/System/Volumes" {
                assert!(submounts > 0);
            }
        }
    }

    /// A share with katamaran's ownership policy, under which lookups resolve through pins.
    fn fixed_owner_at(root: &Path) -> PassthroughFs {
        let cfg = Config {
            root_dir: root.to_str().unwrap().to_string(),
            semantics: PermissionSemantics::LinuxSimplifiedFixedOwner,
            fixed_owner: Some((0, 0)),
            ..Default::default()
        };
        let fs = PassthroughFs::new(cfg, Arc::new(InodeAllocator::new())).unwrap();
        fs.init(FsOptions::empty()).unwrap();
        fs
    }

    fn fresh_dir(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("libkrun-passthrough-{}-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn lookup_in(fs: &PassthroughFs, parent: Inode, name: &str) -> io::Result<Entry> {
        fs.lookup(CTX, parent, &CString::new(name).unwrap())
    }

    fn open_handle(fs: &PassthroughFs, inode: Inode) -> io::Result<Handle> {
        Ok(fs
            .open(CTX, inode, false, libc::O_RDONLY as u32)?
            .0
            .unwrap())
    }

    fn read_handle(fs: &PassthroughFs, handle: Handle) -> Vec<u8> {
        let fd = fs
            .handles
            .get(handle)
            .unwrap()
            .file
            .read()
            .unwrap()
            .as_raw_fd();
        let mut buf = vec![0u8; 64];
        let n = unsafe { libc::pread(fd, buf.as_mut_ptr().cast(), buf.len(), 0) };
        assert!(n >= 0);
        buf.truncate(n as usize);
        buf
    }

    fn read_inode(fs: &PassthroughFs, inode: Inode) -> io::Result<Vec<u8>> {
        let handle = open_handle(fs, inode)?;
        let data = read_handle(fs, handle);
        fs.release(CTX, inode, 0, handle, false, false, None)?;
        Ok(data)
    }

    fn pin_root(fs: &PassthroughFs) {
        let root = fs.node(fuse::ROOT_ID).unwrap();
        while fs.parent_pin(&root).is_none() {}
    }

    /// With `owner_caller` the guest shows every file as owned by the caller and
    /// leaves permission checks to the host, so `access(2)` must agree with that
    /// owner: PHP's `is_writable()` on a 0755 directory, as a non-root user.
    #[test]
    fn access_treats_the_caller_as_owner_when_the_caller_owns_files() {
        let dir = fresh_dir("access-caller");
        fs::create_dir(dir.join("cache")).unwrap();
        fs::set_permissions(dir.join("cache"), fs::Permissions::from_mode(0o755)).unwrap();
        let user = Context {
            uid: 24128,
            gid: 24128,
            pid: 0,
        };
        let check = |caller_owns| {
            let mut fs = fixed_owner_at(&dir);
            fs.cfg.caller_owns = caller_owns;
            let inode = lookup_in(&fs, fuse::ROOT_ID, "cache").unwrap().inode;
            [libc::R_OK, libc::W_OK, libc::X_OK]
                .map(|m| fs.access(user, inode, m as u32).is_ok())
        };
        assert_eq!(check(true), [true, true, true]);
        assert_eq!(check(false), [true, false, true]);
        fs::set_permissions(dir.join("cache"), fs::Permissions::from_mode(0o555)).unwrap();
        assert_eq!(check(true), [true, false, true]);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn setattr_sets_times_without_following_symlinks() {
        use std::os::unix::fs::MetadataExt;
        let dir = fresh_dir("utimes");
        fs::write(dir.join("f"), "x").unwrap();
        std::os::unix::fs::symlink("f", dir.join("l")).unwrap();
        let fs = fixed_owner_at(&dir);
        let set = |name: &str, atime: i64, mtime: i64| {
            let mut attr: bindings::stat64 = unsafe { std::mem::zeroed() };
            attr.st_atime = atime;
            attr.st_mtime = mtime;
            let inode = lookup_in(&fs, fuse::ROOT_ID, name).unwrap().inode;
            fs.setattr(
                CTX,
                inode,
                attr,
                None,
                SetattrValid::ATIME | SetattrValid::MTIME,
            )
            .unwrap();
        };
        let times = |name: &str| {
            let md = fs::symlink_metadata(dir.join(name)).unwrap();
            (md.atime(), md.mtime())
        };
        set("f", 1_000_000, 2_000_000);
        set("l", 3_000_000, 4_000_000);
        assert_eq!(times("f"), (1_000_000, 2_000_000));
        assert_eq!(times("l"), (3_000_000, 4_000_000));
        fs::remove_dir_all(&dir).unwrap();
    }

    fn create_in(
        fs: &PassthroughFs,
        ctx: Context,
        name: &str,
        flags: i32,
    ) -> io::Result<(Entry, Handle)> {
        let (entry, handle, _) = fs.create(
            ctx,
            fuse::ROOT_ID,
            &CString::new(name).unwrap(),
            libc::S_IFREG as u32 | 0o640,
            false,
            (libc::O_RDWR | flags) as u32,
            0o022,
            Extensions::default(),
        )?;
        Ok((entry, handle.unwrap()))
    }

    /// CREATE builds the entry from the new descriptor instead of looking the name up again;
    /// the entry must still be what a lookup returns, with the root pinned or not.
    #[test]
    fn create_returns_the_entry_a_lookup_would() {
        let dir = fresh_dir("create-entry");
        for pinned in [false, true] {
            let fs = fixed_owner_at(&dir);
            if pinned {
                pin_root(&fs);
            }
            let name = format!("new-{pinned}");
            let (entry, handle) = create_in(&fs, CTX, &name, 0).unwrap();
            let meta = fs::symlink_metadata(dir.join(&name)).unwrap();
            assert_eq!(entry.attr.st_ino, meta.ino());
            assert_eq!(entry.attr.st_size, 0);
            assert_eq!(u32::from(entry.attr.st_mode), libc::S_IFREG as u32 | 0o640);
            assert_eq!((entry.attr.st_uid, entry.attr.st_gid), (0, 0));
            assert_eq!(meta.mode() & 0o777, 0o640);
            let looked_up = lookup_in(&fs, fuse::ROOT_ID, &name).unwrap();
            assert_eq!(looked_up.inode, entry.inode);
            assert_eq!(refcount(&fs, entry.inode), 2);
            let fd = fs
                .handles
                .get(handle)
                .unwrap()
                .file
                .read()
                .unwrap()
                .as_raw_fd();
            assert_eq!(
                unsafe { libc::pwrite(fd, b"data".as_ptr().cast(), 4, 0) },
                4
            );
            assert_eq!(fs::read(dir.join(&name)).unwrap(), b"data");
            fs.release(CTX, entry.inode, 0, handle, false, false, None)
                .unwrap();
        }
    }

    /// A CREATE for a name that exists on the host (the guest had not seen it) opens that file,
    /// unless the guest asked for O_EXCL.
    #[test]
    fn create_opens_an_existing_name_unless_exclusive() {
        let dir = fresh_dir("create-existing");
        fs::write(dir.join("old"), b"keep").unwrap();
        let fs = fixed_owner_at(&dir);
        let old = lookup_in(&fs, fuse::ROOT_ID, "old").unwrap().inode;

        let (entry, handle) = create_in(&fs, CTX, "old", 0).unwrap();
        assert_eq!(entry.inode, old);
        assert_eq!(read_handle(&fs, handle), b"keep");
        fs.release(CTX, old, 0, handle, false, false, None).unwrap();

        let err = create_in(&fs, CTX, "old", bindings::LINUX_O_EXCL).map(|_| ());
        assert_eq!(err.unwrap_err().raw_os_error(), Some(libc::EEXIST));
        assert_eq!(fs::read(dir.join("old")).unwrap(), b"keep");

        let (entry, handle) = create_in(&fs, CTX, "old", bindings::LINUX_O_TRUNC).unwrap();
        assert_eq!((entry.inode, entry.attr.st_size), (old, 0));
        assert!(fs::read(dir.join("old")).unwrap().is_empty());
        fs.release(CTX, old, 0, handle, false, false, None).unwrap();
    }

    /// With xattr semantics the entry of a new file shows the owner and mode just recorded for
    /// the guest, not the host's.
    #[test]
    fn create_reports_the_recorded_owner_with_xattr_semantics() {
        let dir = fresh_dir("create-xattr");
        let fs = passthrough_at(&dir, true, FsOptions::empty());
        let user = Context {
            uid: 1000,
            gid: 1001,
            pid: 0,
        };
        let (entry, handle) = create_in(&fs, user, "owned", 0).unwrap();
        assert_eq!((entry.attr.st_uid, entry.attr.st_gid), (1000, 1001));
        assert_eq!(u32::from(entry.attr.st_mode), libc::S_IFREG as u32 | 0o640);
        let looked_up = lookup_in(&fs, fuse::ROOT_ID, "owned").unwrap();
        assert_eq!(
            (
                looked_up.attr.st_uid,
                looked_up.attr.st_gid,
                looked_up.attr.st_mode
            ),
            (1000, 1001, entry.attr.st_mode)
        );
        fs.release(user, entry.inode, 0, handle, false, false, None)
            .unwrap();
    }

    #[test]
    fn lookups_pin_busy_directories_within_the_budget() {
        let dir = fresh_dir("pins");
        for d in 0..40 {
            fs::create_dir(dir.join(format!("d{d}"))).unwrap();
            for f in 0..6 {
                fs::write(dir.join(format!("d{d}/f{f}")), b"x").unwrap();
            }
        }
        let fs = fixed_owner_at(&dir);
        fs.pins.set_budget(10);

        let mut subdirs = Vec::new();
        for d in 0..40 {
            let sub = lookup_in(&fs, fuse::ROOT_ID, &format!("d{d}"))
                .unwrap()
                .inode;
            for f in 0..6 {
                let entry = lookup_in(&fs, sub, &format!("f{f}")).unwrap();
                let host = fs::symlink_metadata(dir.join(format!("d{d}/f{f}"))).unwrap();
                assert_eq!(entry.attr.st_ino, host.ino());
                assert!(fs.pins.count() <= 10);
            }
            subdirs.push(sub);
        }
        assert_eq!(fs.pins.count(), 10);

        let pinned = *subdirs
            .iter()
            .find(|i| {
                fs.node(**i)
                    .unwrap()
                    .pin
                    .as_ref()
                    .is_some_and(|p| p.is_pinned())
            })
            .unwrap();
        let (st, _) = fs.do_getattr(&CTX, pinned).unwrap();
        let host = fs::symlink_metadata(dir.join(format!(
            "d{}",
            subdirs.iter().position(|i| *i == pinned).unwrap()
        )))
        .unwrap();
        assert_eq!((st.st_ino, st.st_nlink as u64), (host.ino(), host.nlink()));

        fs.pins.demote(0);
        assert_eq!(fs.pins.count(), 0);
        assert!(lookup_in(&fs, subdirs[0], "f0").is_ok());
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn concurrent_lookups_and_forgets_keep_one_inode() {
        let dir = fresh_dir("refcount");
        fs::write(dir.join("x"), b"x").unwrap();
        let fs = fixed_owner_at(&dir);

        let inodes: Vec<Inode> = std::thread::scope(|s| {
            let workers: Vec<_> = (0..8)
                .map(|_| {
                    s.spawn(|| {
                        (0..500)
                            .map(|_| lookup_in(&fs, fuse::ROOT_ID, "x").unwrap().inode)
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            workers
                .into_iter()
                .flat_map(|w| w.join().unwrap())
                .collect()
        });
        assert!(inodes.iter().all(|i| *i == inodes[0]));
        assert_eq!(refcount(&fs, inodes[0]), 4000);

        std::thread::scope(|s| {
            for _ in 0..8 {
                s.spawn(|| {
                    for _ in 0..500 {
                        let inode = lookup_in(&fs, fuse::ROOT_ID, "x").unwrap().inode;
                        // The inode the lookup returned is still in the table.
                        assert!(fs.node(inode).is_ok());
                        fs.forget(CTX, inode, 1);
                        fs.forget(CTX, inode, 1);
                    }
                });
            }
        });
        assert!(fs.node(inodes[0]).is_err());
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_name_replaced_on_the_host_gets_a_new_inode() {
        let dir = fresh_dir("rename-over-host");
        fs::write(dir.join("a"), b"old").unwrap();
        let fs = fixed_owner_at(&dir);
        pin_root(&fs);

        let a = lookup_in(&fs, fuse::ROOT_ID, "a").unwrap().inode;
        let handle = open_handle(&fs, a).unwrap();
        assert_eq!(read_inode(&fs, a).unwrap(), b"old");

        fs::write(dir.join("a.tmp"), b"new").unwrap();
        fs::rename(dir.join("a.tmp"), dir.join("a")).unwrap();

        // The open handle stays on the replaced file, and the old inode never opens the new one.
        assert_eq!(read_handle(&fs, handle), b"old");
        assert_eq!(read_inode(&fs, a).unwrap(), b"old");
        fs.release(CTX, a, 0, handle, false, false, None).unwrap();
        // Without a shared open file, the anchor now names another file: the open must not
        // return it, and the stale anchor goes.
        assert_ne!(read_inode(&fs, a).ok(), Some(b"new".to_vec()));
        assert!(fs.node(a).unwrap().anchor.lock().unwrap().is_none());

        let b = lookup_in(&fs, fuse::ROOT_ID, "a").unwrap();
        assert_ne!(b.inode, a);
        assert_eq!(b.attr.st_ino, fs::metadata(dir.join("a")).unwrap().ino());
        assert_eq!(read_inode(&fs, b.inode).unwrap(), b"new");
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_rename_over_an_open_file_keeps_it_reachable() {
        let dir = fresh_dir("rename-over-guest");
        fs::write(dir.join("a"), b"old").unwrap();
        fs::write(dir.join("b"), b"new").unwrap();
        fs::write(dir.join("c"), b"unopened").unwrap();
        fs::write(dir.join("d"), b"d").unwrap();
        let fs = fixed_owner_at(&dir);
        pin_root(&fs);

        let a = lookup_in(&fs, fuse::ROOT_ID, "a").unwrap();
        let handle = open_handle(&fs, a.inode).unwrap();
        let c = lookup_in(&fs, fuse::ROOT_ID, "c").unwrap().inode;
        let name = |n: &str| CString::new(n).unwrap();
        fs.rename(CTX, fuse::ROOT_ID, &name("b"), fuse::ROOT_ID, &name("a"), 0)
            .unwrap();
        fs.rename(CTX, fuse::ROOT_ID, &name("d"), fuse::ROOT_ID, &name("c"), 0)
            .unwrap();

        let (st, _) = fs.do_getattr(&CTX, a.inode).unwrap();
        assert_eq!(st.st_ino, a.attr.st_ino);
        assert_eq!(read_handle(&fs, handle), b"old");
        assert_eq!(read_inode(&fs, a.inode).unwrap(), b"old");
        // Nothing had the overwritten "c" open, so no descriptor was kept for it.
        assert_eq!(fs.node(c).unwrap().unlinked_fd.load(Ordering::Acquire), -1);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_file_deleted_while_open_stays_usable() {
        let dir = fresh_dir("delete-open");
        fs::write(dir.join("f"), b"data").unwrap();
        fs::write(dir.join("g"), b"data").unwrap();
        let fs = fixed_owner_at(&dir);

        let f = lookup_in(&fs, fuse::ROOT_ID, "f").unwrap().inode;
        let handle = open_handle(&fs, f).unwrap();
        fs.unlink(CTX, fuse::ROOT_ID, &CString::new("f").unwrap())
            .unwrap();
        let (st, _) = fs.do_getattr(&CTX, f).unwrap();
        assert_eq!(st.st_nlink, 0);
        assert_eq!(read_handle(&fs, handle), b"data");
        assert_eq!(read_inode(&fs, f).unwrap(), b"data");

        let g = lookup_in(&fs, fuse::ROOT_ID, "g").unwrap().inode;
        fs.unlink(CTX, fuse::ROOT_ID, &CString::new("g").unwrap())
            .unwrap();
        assert_eq!(fs.node(g).unwrap().unlinked_fd.load(Ordering::Acquire), -1);
        assert_eq!(
            fs.do_getattr(&CTX, g).unwrap_err().raw_os_error(),
            Some(libc::ENOENT)
        );

        fs.release(CTX, f, 0, handle, false, false, None).unwrap();
        fs.forget(CTX, f, 1);
        assert!(fs.node(f).is_err());
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn git_checkout_swaps_inodes_behind_names() {
        let dir = fresh_dir("git-checkout");
        let git = |args: &[&str]| {
            let out = std::process::Command::new("git")
                .args(["-c", "user.name=t", "-c", "user.email=t@t", "-c"])
                .arg("init.defaultBranch=one")
                .args(args)
                .current_dir(&dir)
                .output();
            match out {
                Ok(out) => assert!(out.status.success(), "git {args:?}: {out:?}"),
                Err(e) => panic!("git: {e}"),
            }
        };
        git(&["init", "-q"]);
        fs::write(dir.join("a"), b"v1").unwrap();
        fs::write(dir.join("b"), b"b").unwrap();
        git(&["add", "."]);
        git(&["commit", "-qm", "one"]);
        git(&["checkout", "-qb", "two"]);
        fs::write(dir.join("a"), b"v2").unwrap();
        fs::remove_file(dir.join("b")).unwrap();
        fs::write(dir.join("c"), b"c").unwrap();
        git(&["add", "-A"]);
        git(&["commit", "-qm", "two"]);
        git(&["checkout", "-q", "one"]);

        let fs = fixed_owner_at(&dir);
        pin_root(&fs);
        let a = lookup_in(&fs, fuse::ROOT_ID, "a").unwrap();
        let handle = open_handle(&fs, a.inode).unwrap();
        assert_eq!(read_inode(&fs, a.inode).unwrap(), b"v1");

        git(&["checkout", "-q", "two"]);

        let a2 = lookup_in(&fs, fuse::ROOT_ID, "a").unwrap();
        assert_eq!(a2.attr.st_ino, fs::metadata(dir.join("a")).unwrap().ino());
        assert_eq!(read_inode(&fs, a2.inode).unwrap(), b"v2");
        assert_eq!(read_handle(&fs, handle), b"v1");
        if a2.attr.st_ino != a.attr.st_ino {
            assert_ne!(a2.inode, a.inode);
            assert_ne!(read_inode(&fs, a.inode).ok(), Some(b"v2".to_vec()));
        }
        assert_eq!(
            lookup_in(&fs, fuse::ROOT_ID, "b")
                .err()
                .and_then(|e| e.raw_os_error()),
            Some(libc::ENOENT)
        );
        assert!(lookup_in(&fs, fuse::ROOT_ID, "c").is_ok());
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn survives_a_low_descriptor_limit() {
        let out = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "virtio::fs::macos::passthrough::tests::low_descriptor_limit_child",
                "--exact",
                "--ignored",
                "--nocapture",
            ])
            .env("KAT_LOW_NOFILE_CHILD", "1")
            .output()
            .unwrap();
        assert!(
            out.status.success() && String::from_utf8_lossy(&out.stdout).contains("1 passed"),
            "{}\n{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// Runs in its own process (see `survives_a_low_descriptor_limit`) with RLIMIT_NOFILE 256.
    #[test]
    #[ignore]
    fn low_descriptor_limit_child() {
        if std::env::var("KAT_LOW_NOFILE_CHILD").is_err() {
            return;
        }
        let lim = libc::rlimit {
            rlim_cur: 256,
            rlim_max: 256,
        };
        assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &lim) }, 0);

        let dir = fresh_dir("low-nofile");
        for d in 0..300 {
            fs::create_dir(dir.join(format!("d{d}"))).unwrap();
            for f in 0..5 {
                fs::write(dir.join(format!("d{d}/f{f}")), b"x").unwrap();
            }
        }
        let fs = fixed_owner_at(&dir);

        let mut held = Vec::new();
        for d in 0..300 {
            let sub = lookup_in(&fs, fuse::ROOT_ID, &format!("d{d}"))
                .unwrap()
                .inode;
            let listed = list_dir(&fs, sub);
            assert_eq!(listed, 5);
            for f in 0..5 {
                let file = lookup_in(&fs, sub, &format!("f{f}")).unwrap().inode;
                if f == 0 && held.len() < 150 {
                    held.push((file, open_handle(&fs, file).unwrap()));
                } else {
                    assert_eq!(read_inode(&fs, file).unwrap(), b"x");
                }
            }
            assert!(fs.pins.count() <= 128);
        }
        assert_eq!(held.len(), 150);
        for (inode, handle) in held {
            fs.release(CTX, inode, 0, handle, false, false, None)
                .unwrap();
        }
        fs::remove_dir_all(&dir).unwrap();
    }

    fn list_dir(fs: &PassthroughFs, inode: Inode) -> usize {
        let handle = fs
            .opendir(CTX, inode, libc::O_RDONLY as u32)
            .unwrap()
            .0
            .unwrap();
        let mut n = 0;
        fs.readdirplus(CTX, inode, handle, 4096, 0, |_, e| {
            n += 1;
            fs.forget(CTX, e.inode, 1);
            Ok(1)
        })
        .unwrap();
        fs.releasedir(CTX, inode, 0, handle).unwrap();
        n
    }

    /// KAT_BENCH_TREE=<dir> KAT_BENCH_PINS=0|1: time a lookup of every entry under <dir> and an
    /// open and release of every regular file, through the server as the guest drives it.
    #[test]
    #[ignore]
    fn lookup_and_open_timing() {
        let root = PathBuf::from(std::env::var("KAT_BENCH_TREE").expect("KAT_BENCH_TREE"));
        let fs = fixed_owner_at(&root);
        if std::env::var("KAT_BENCH_PINS").as_deref() == Ok("0") {
            fs.pins.set_budget(0);
        }
        let mut files = Vec::new();
        let start = std::time::Instant::now();
        let mut lookups = 0;
        let mut stack = vec![(fuse::ROOT_ID, root.clone())];
        while let Some((inode, dir)) = stack.pop() {
            for e in fs::read_dir(&dir).unwrap() {
                let e = e.unwrap();
                let name = CString::new(e.file_name().into_encoded_bytes()).unwrap();
                let entry = fs.lookup(CTX, inode, &name).unwrap();
                lookups += 1;
                match entry.attr.st_mode & libc::S_IFMT {
                    libc::S_IFDIR => stack.push((entry.inode, e.path())),
                    libc::S_IFREG => files.push(entry.inode),
                    _ => {}
                }
            }
        }
        let walk = start.elapsed();
        let start = std::time::Instant::now();
        for inode in &files {
            let handle = open_handle(&fs, *inode).unwrap();
            fs.release(CTX, *inode, 0, handle, false, false, None)
                .unwrap();
        }
        let opens = start.elapsed();
        eprintln!(
            "pins={} walk incl. readdir: {} ns/lookup over {lookups}; open+release {} ns over {}",
            fs.pins.count(),
            walk.as_nanos() / lookups as u128,
            opens.as_nanos() / files.len() as u128,
            files.len()
        );
    }

    #[test]
    fn auto_policy_caches_unchanged_directories() {
        let share = Share::with_file_and_policy("a", CachePolicy::Auto);
        let opendir = || {
            let (h, opts) = share
                .fs
                .opendir(CTX, fuse::ROOT_ID, libc::O_RDONLY as u32)
                .unwrap();
            share
                .fs
                .releasedir(CTX, fuse::ROOT_ID, 0, h.unwrap())
                .unwrap();
            opts
        };
        assert_eq!(opendir(), OpenOptions::CACHE_DIR, "first opendir");
        assert_eq!(
            opendir(),
            OpenOptions::CACHE_DIR | OpenOptions::KEEP_CACHE,
            "unchanged directory"
        );
        fs::write(share.dir.join("b"), b"new").unwrap();
        assert_eq!(opendir(), OpenOptions::CACHE_DIR, "after entry added");
        assert_eq!(
            opendir(),
            OpenOptions::CACHE_DIR | OpenOptions::KEEP_CACHE,
            "unchanged again"
        );
    }

    #[test]
    fn unlink_and_rmdir_fast_path_and_open_hardlink() {
        let dir = fresh_dir("unlink-fast");
        fs::create_dir(dir.join("sub")).unwrap();
        fs::write(dir.join("sub/f1"), b"one").unwrap();
        fs::write(dir.join("sub/f2"), b"two").unwrap();
        fs::write(dir.join("hl1"), b"shared").unwrap();
        fs::hard_link(dir.join("hl1"), dir.join("hl2")).unwrap();

        let fs = fixed_owner_at(&dir);
        // Fast path: unlink inside an open directory and rmdir it afterwards.
        let sub = lookup_in(&fs, fuse::ROOT_ID, "sub").unwrap().inode;
        let dh = fs
            .opendir(CTX, sub, libc::O_RDONLY as u32)
            .unwrap()
            .0
            .unwrap();
        assert!(!fs.open_tracker.may_have_open_target(sub, false));
        fs.unlink(CTX, sub, &CString::new("f1").unwrap()).unwrap();
        fs.unlink(CTX, sub, &CString::new("f2").unwrap()).unwrap();
        fs.releasedir(CTX, sub, 0, dh).unwrap();
        assert!(!fs.open_tracker.may_have_open_target(fuse::ROOT_ID, true));
        fs.rmdir(CTX, fuse::ROOT_ID, &CString::new("sub").unwrap())
            .unwrap();
        assert!(!dir.join("sub").exists());

        // Hardlinked open file: unlinking both names keeps it usable via unlinked_fd.
        let hl1 = lookup_in(&fs, fuse::ROOT_ID, "hl1").unwrap().inode;
        let handle = open_handle(&fs, hl1).unwrap();
        assert!(fs.open_tracker.may_have_open_target(fuse::ROOT_ID, false));
        fs.unlink(CTX, fuse::ROOT_ID, &CString::new("hl1").unwrap())
            .unwrap();
        fs.unlink(CTX, fuse::ROOT_ID, &CString::new("hl2").unwrap())
            .unwrap();
        let (st, _) = fs.do_getattr(&CTX, hl1).unwrap();
        assert_eq!(st.st_nlink, 0);
        assert_eq!(read_handle(&fs, handle), b"shared");
        fs.release(CTX, hl1, 0, handle, false, false, None).unwrap();
        fs.forget(CTX, hl1, 1);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn plain_readdir_uses_lightweight_bulk() {
        let dir = fresh_dir("plain-readdir");
        fs::write(dir.join("file.txt"), b"hi").unwrap();
        fs::create_dir(dir.join("subdir")).unwrap();
        let fs = fixed_owner_at(&dir);
        let dh = fs
            .opendir(CTX, fuse::ROOT_ID, libc::O_RDONLY as u32)
            .unwrap()
            .0
            .unwrap();
        let mut names = Vec::new();
        fs.readdir(CTX, fuse::ROOT_ID, dh, 4096, 0, |e| {
            names.push((e.name.to_vec(), e.type_, e.ino));
            Ok(1)
        })
        .unwrap();
        fs.releasedir(CTX, fuse::ROOT_ID, 0, dh).unwrap();
        names.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(names.len(), 2);
        assert_eq!(names[0].0, b"file.txt");
        assert_eq!(names[0].1, u32::from(libc::DT_REG));
        assert_eq!(names[0].2, fs::metadata(dir.join("file.txt")).unwrap().ino());
        assert_eq!(names[1].0, b"subdir");
        assert_eq!(names[1].1, u32::from(libc::DT_DIR));
        assert_eq!(names[1].2, fs::metadata(dir.join("subdir")).unwrap().ino());
        fs::remove_dir_all(&dir).unwrap();
    }
}
