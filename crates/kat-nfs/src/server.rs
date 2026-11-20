//! NFSv3 (RFC 1813) and MOUNT v3 over the node table.
//!
//! The server runs as root in the guest and does not check permissions: the
//! socket it answers on is reachable only by the Mac's kernel NFS client,
//! which acts for the Mac user. Ownership is translated so the Mac user owns
//! what root owns in the guest: uid/gid 0 are shown as the caller, and a
//! chown to the caller is stored as 0. New files take their directory's
//! owner, so a file created in a database volume belongs to the database's
//! user.
//!
//! Writes go to the guest page cache and are answered `FILE_SYNC`, like a
//! Linux knfsd export with `async`: the data is as durable as any container
//! write, and the client never has to send COMMIT.

use std::collections::HashMap;
use std::io;
use std::os::fd::OwnedFd;
use std::sync::Mutex;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::fsops::{self, SetTime, Stat};
use crate::layout::Layout;
use crate::nodes::{HANDLE_LEN, HandleError, Id, Kind, Nodes, VDir, VFile};
use crate::rpc::{self, Cred};
use crate::stats;
use crate::xdr::{Garbage, Reader, Writer};

pub const PROG_NFS: u32 = 100_003;
pub const PROG_MOUNT: u32 = 100_005;
/// Largest READ or WRITE: matches `rsize`/`wsize` on the Mac side.
pub const MAX_IO: u32 = 1 << 20;
/// The export's filesystem id, `KATN`.
const FSID: u64 = 0x4b41_544e;

pub mod nfsstat {
    pub const OK: u32 = 0;
    pub const PERM: u32 = 1;
    pub const NOENT: u32 = 2;
    pub const IO: u32 = 5;
    pub const NXIO: u32 = 6;
    pub const ACCES: u32 = 13;
    pub const EXIST: u32 = 17;
    pub const XDEV: u32 = 18;
    pub const NODEV: u32 = 19;
    pub const NOTDIR: u32 = 20;
    pub const ISDIR: u32 = 21;
    pub const INVAL: u32 = 22;
    pub const FBIG: u32 = 27;
    pub const NOSPC: u32 = 28;
    pub const ROFS: u32 = 30;
    pub const MLINK: u32 = 31;
    pub const NAMETOOLONG: u32 = 63;
    pub const NOTEMPTY: u32 = 66;
    pub const DQUOT: u32 = 69;
    pub const STALE: u32 = 70;
    pub const BADHANDLE: u32 = 10001;
    pub const NOT_SYNC: u32 = 10002;
    pub const NOTSUPP: u32 = 10004;
    pub const TOOSMALL: u32 = 10005;
    pub const SERVERFAULT: u32 = 10006;
}
use nfsstat::*;

pub mod access {
    pub const READ: u32 = 0x01;
    pub const LOOKUP: u32 = 0x02;
    pub const MODIFY: u32 = 0x04;
    pub const EXTEND: u32 = 0x08;
    pub const DELETE: u32 = 0x10;
    pub const EXECUTE: u32 = 0x20;
}

const FILE_SYNC: u32 = 2;

type R<T> = Result<T, u32>;

pub fn errno_status(e: &io::Error) -> u32 {
    let Some(n) = e.raw_os_error() else {
        return IO;
    };
    match n {
        libc::EPERM => PERM,
        libc::ENOENT => NOENT,
        libc::ENXIO => NXIO,
        libc::EACCES => ACCES,
        libc::EEXIST => EXIST,
        libc::EXDEV => XDEV,
        libc::ENODEV => NODEV,
        libc::ENOTDIR => NOTDIR,
        libc::EISDIR => ISDIR,
        libc::EINVAL => INVAL,
        libc::EFBIG => FBIG,
        libc::ENOSPC => NOSPC,
        libc::EROFS => ROFS,
        libc::EMLINK => MLINK,
        libc::ENAMETOOLONG => NAMETOOLONG,
        libc::ENOTEMPTY => NOTEMPTY,
        libc::EDQUOT => DQUOT,
        libc::ESTALE => STALE,
        libc::ELOOP => STALE,
        libc::ENOTSUP => NOTSUPP,
        _ => IO,
    }
}

fn st_err(e: io::Error) -> u32 {
    errno_status(&e)
}

/// While walking to a node: a name that is gone, not a directory any more,
/// or replaced by a symlink means the handle no longer names that file.
fn walk_err(e: io::Error) -> u32 {
    match e.raw_os_error() {
        Some(libc::ENOENT | libc::ENOTDIR | libc::ELOOP) => STALE,
        _ => errno_status(&e),
    }
}

fn now() -> (i64, u32) {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    (d.as_secs() as i64, d.subsec_nanos())
}

/// fattr3.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fattr {
    pub ftype: u32,
    pub mode: u32,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub used: u64,
    pub rdev: (u32, u32),
    pub fileid: u64,
    pub atime: (i64, u32),
    pub mtime: (i64, u32),
    pub ctime: (i64, u32),
}

fn ftype(st: &Stat) -> u32 {
    match st.file_type() as libc::mode_t {
        libc::S_IFREG => 1,
        libc::S_IFDIR => 2,
        libc::S_IFBLK => 3,
        libc::S_IFCHR => 4,
        libc::S_IFLNK => 5,
        libc::S_IFSOCK => 6,
        libc::S_IFIFO => 7,
        _ => 1,
    }
}

fn shown(id: u32, caller: u32) -> u32 {
    if id == 0 { caller } else { id }
}

fn stored(id: u32, caller: u32) -> u32 {
    if id == caller { 0 } else { id }
}

fn fattr(st: &Stat, id: Id, cred: Cred) -> Fattr {
    Fattr {
        ftype: ftype(st),
        mode: st.mode & 0o7777,
        nlink: st.nlink,
        uid: shown(st.uid, cred.uid),
        gid: shown(st.gid, cred.gid),
        size: st.size,
        used: st.blocks * 512,
        rdev: st.rdev_parts(),
        fileid: id,
        atime: st.atime,
        mtime: st.mtime,
        ctime: st.ctime,
    }
}

fn put_time(w: &mut Writer, t: (i64, u32)) {
    w.u32(t.0.clamp(0, u32::MAX as i64) as u32).u32(t.1);
}

fn put_fattr(w: &mut Writer, a: &Fattr) {
    w.u32(a.ftype)
        .u32(a.mode)
        .u32(a.nlink)
        .u32(a.uid)
        .u32(a.gid);
    w.u64(a.size).u64(a.used).u32(a.rdev.0).u32(a.rdev.1);
    w.u64(FSID).u64(a.fileid);
    put_time(w, a.atime);
    put_time(w, a.mtime);
    put_time(w, a.ctime);
}

fn post_op(w: &mut Writer, a: Option<Fattr>) {
    match a {
        Some(a) => {
            w.bool(true);
            put_fattr(w, &a);
        }
        None => {
            w.bool(false);
        }
    }
}

/// wcc_data without pre-operation attributes.
fn wcc(w: &mut Writer, after: Option<Fattr>) {
    w.bool(false);
    post_op(w, after);
}

/// sattr3.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Sattr {
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub size: Option<u64>,
    pub atime: Option<SetTime>,
    pub mtime: Option<SetTime>,
}

fn get_time(a: &mut Reader) -> Result<Option<SetTime>, Garbage> {
    Ok(match a.u32()? {
        0 => None,
        1 => Some(SetTime::Now),
        2 => Some(SetTime::At(a.u32()? as i64, a.u32()?)),
        _ => return Err(Garbage),
    })
}

fn get_sattr(a: &mut Reader) -> Result<Sattr, Garbage> {
    let mut s = Sattr::default();
    if a.bool()? {
        s.mode = Some(a.u32()? & 0o7777);
    }
    if a.bool()? {
        s.uid = Some(a.u32()?);
    }
    if a.bool()? {
        s.gid = Some(a.u32()?);
    }
    if a.bool()? {
        s.size = Some(a.u64()?);
    }
    s.atime = get_time(a)?;
    s.mtime = get_time(a)?;
    Ok(s)
}

fn get_fh<'a>(a: &mut Reader<'a>) -> Result<&'a [u8], Garbage> {
    a.opaque(64)
}

fn get_name<'a>(a: &mut Reader<'a>) -> Result<&'a [u8], Garbage> {
    a.opaque(1024)
}

/// Where a real node is, ready for `*at` calls.
enum Loc {
    /// An anchor's own directory.
    Anchor(OwnedFd),
    /// A name in a directory.
    At(OwnedFd, Vec<u8>),
}

impl Loc {
    fn stat(&self) -> io::Result<Stat> {
        match self {
            Loc::Anchor(fd) => fsops::fstat(fd),
            Loc::At(d, n) => fsops::statat(d, n),
        }
    }

    /// The node itself as a directory to work in.
    fn dir(&self) -> io::Result<OwnedFd> {
        match self {
            Loc::Anchor(fd) => fd.try_clone(),
            Loc::At(d, n) => fsops::walk(d, n),
        }
    }

    fn open(&self, flags: libc::c_int) -> io::Result<OwnedFd> {
        match self {
            Loc::Anchor(fd) => fsops::reopen_dir(fd, flags),
            Loc::At(d, n) => fsops::openat(d, n, flags, 0),
        }
    }

    fn chown(&self, uid: Option<u32>, gid: Option<u32>) -> io::Result<()> {
        match self {
            Loc::Anchor(fd) => fsops::fchown(&fsops::reopen_dir(fd, libc::O_RDONLY)?, uid, gid),
            Loc::At(d, n) => fsops::chownat(d, n, uid, gid),
        }
    }

    fn chmod(&self, mode: u32) -> io::Result<()> {
        match self {
            Loc::Anchor(fd) => fsops::fchmod(&fsops::reopen_dir(fd, libc::O_RDONLY)?, mode),
            Loc::At(d, n) => fsops::chmodat(d, n, mode),
        }
    }

    fn utimes(&self, atime: SetTime, mtime: SetTime) -> io::Result<()> {
        match self {
            Loc::Anchor(fd) => {
                fsops::futimens(&fsops::reopen_dir(fd, libc::O_RDONLY)?, atime, mtime)
            }
            Loc::At(d, n) => fsops::utimensat(d, n, atime, mtime),
        }
    }
}

struct Real {
    loc: Loc,
    st: Stat,
}

struct Listing {
    verf: u64,
    names: Vec<Vec<u8>>,
    at: Instant,
}

/// Listings kept for paged READDIR(PLUS) calls.
const MAX_LISTINGS: usize = 64;

pub struct Server {
    layout: Layout,
    nodes: Mutex<Nodes>,
    listings: Mutex<HashMap<Id, Listing>>,
    /// Container names last listed, and when that list last changed: the
    /// `containers` directory's mtime.
    containers_seen: Mutex<(Vec<String>, (i64, u32))>,
    started: (i64, u32),
    /// Write verifier; changes when the server restarts.
    verf: u64,
    next_listing: Mutex<u64>,
}

impl Server {
    pub fn new(layout: Layout) -> Server {
        let t = now();
        let instance = (t.0 as u64) << 20 ^ t.1 as u64 ^ (std::process::id() as u64) << 44;
        Server {
            layout,
            nodes: Mutex::new(Nodes::new(instance)),
            listings: Mutex::new(HashMap::new()),
            containers_seen: Mutex::new((Vec::new(), t)),
            started: t,
            verf: instance,
            next_listing: Mutex::new(1),
        }
    }

    /// Answer one RPC record. `None` for records that get no reply. The
    /// reply starts with 4 bytes reserved for the record mark.
    pub fn handle(&self, record: &[u8]) -> Option<Vec<u8>> {
        self.handle_in(record, Vec::new())
    }

    /// [`Server::handle`], building the reply in `buf` to reuse its space.
    pub fn handle_in(&self, record: &[u8], buf: Vec<u8>) -> Option<Vec<u8>> {
        let call = match rpc::parse_call(record) {
            Ok(c) => c,
            Err(r) => return rpc::denied(&r).map(|w| w.buf),
        };
        let (xid, cred, mut args) = (call.xid, call.cred, call.args);
        let w = match (call.prog, call.vers) {
            (PROG_NFS, 3) | (PROG_MOUNT, 3) => {
                let mut w = rpc::accepted_in(buf, xid, rpc::SUCCESS);
                let done = if call.prog == PROG_NFS {
                    self.nfs(call.proc_, cred, &mut args, &mut w)
                } else {
                    self.mount(call.proc_, cred, &mut args, &mut w)
                };
                match done {
                    Ok(true) => w,
                    Ok(false) => rpc::accepted(xid, rpc::PROC_UNAVAIL),
                    Err(Garbage) => rpc::accepted(xid, rpc::GARBAGE_ARGS),
                }
            }
            (PROG_NFS, _) | (PROG_MOUNT, _) => rpc::prog_mismatch(xid, 3, 3),
            _ => rpc::accepted(xid, rpc::PROG_UNAVAIL),
        };
        Some(w.buf)
    }

    fn mount(&self, p: u32, cred: Cred, a: &mut Reader, w: &mut Writer) -> Result<bool, Garbage> {
        match p {
            0 => {}
            1 => {
                let path = a.opaque(1024)?;
                match self.mount_path(path, cred) {
                    Ok(id) => {
                        let fh = self.nodes.lock().unwrap().handle(id);
                        w.u32(0).opaque(&fh);
                        w.u32(1).u32(rpc::AUTH_SYS);
                    }
                    Err(s) => {
                        w.u32(s);
                    }
                }
            }
            // DUMP: nobody is listed.
            2 => {
                w.bool(false);
            }
            // UMNT, UMNTALL: nothing to forget.
            3 => {
                a.opaque(1024)?;
            }
            4 => {}
            // EXPORT: one export, "/", open to everyone who reaches it.
            5 => {
                w.bool(true).opaque(b"/").bool(false).bool(false);
            }
            _ => return Ok(false),
        }
        Ok(true)
    }

    fn mount_path(&self, path: &[u8], cred: Cred) -> R<Id> {
        let mut id = VDir::Root.id();
        for name in path.split(|&b| b == b'/').filter(|n| !n.is_empty()) {
            let (next, attr) = self.lookup(id, name, cred)?;
            if attr.ftype != 2 {
                return Err(NOTDIR);
            }
            id = next;
        }
        Ok(id)
    }

    fn nfs(&self, p: u32, cred: Cred, a: &mut Reader, w: &mut Writer) -> Result<bool, Garbage> {
        match p {
            0 => {}
            1 => {
                let fh = get_fh(a)?;
                match self.node(fh).and_then(|id| self.attr(id, cred)) {
                    Ok(attr) => {
                        w.u32(OK);
                        put_fattr(w, &attr);
                    }
                    Err(s) => {
                        w.u32(s);
                    }
                }
            }
            2 => {
                let fh = get_fh(a)?;
                let sattr = get_sattr(a)?;
                let guard = if a.bool()? {
                    Some((a.u32()? as i64, a.u32()?))
                } else {
                    None
                };
                let id = self.node(fh);
                let r = id.and_then(|id| self.setattr(id, sattr, guard, cred));
                w.u32(r.err().unwrap_or(OK));
                wcc(w, id.ok().and_then(|id| self.attr(id, cred).ok()));
            }
            3 => {
                let dir = get_fh(a)?;
                let name = get_name(a)?;
                let dir = self.node(dir);
                match dir.and_then(|d| self.lookup(d, name, cred)) {
                    Ok((id, attr)) => {
                        let fh = self.nodes.lock().unwrap().handle(id);
                        w.u32(OK).opaque(&fh);
                        post_op(w, Some(attr));
                    }
                    Err(s) => {
                        w.u32(s);
                    }
                }
                post_op(w, dir.ok().and_then(|d| self.attr(d, cred).ok()));
            }
            4 => {
                let fh = get_fh(a)?;
                let want = a.u32()?;
                let id = self.node(fh);
                match id.and_then(|id| self.access(id, want, cred)) {
                    Ok((attr, granted)) => {
                        w.u32(OK);
                        post_op(w, Some(attr));
                        w.u32(granted);
                    }
                    Err(s) => {
                        w.u32(s);
                        post_op(w, None);
                    }
                }
            }
            5 => {
                let fh = get_fh(a)?;
                let id = self.node(fh);
                match id.and_then(|id| self.readlink(id)) {
                    Ok(target) => {
                        w.u32(OK);
                        post_op(w, id.ok().and_then(|id| self.attr(id, cred).ok()));
                        w.opaque(&target);
                    }
                    Err(s) => {
                        w.u32(s);
                        post_op(w, None);
                    }
                }
            }
            6 => {
                let fh = get_fh(a)?;
                let (off, count) = (a.u64()?, a.u32()?);
                let id = self.node(fh);
                if let Err(s) = id.and_then(|id| self.read(id, off, count.min(MAX_IO), cred, w)) {
                    w.u32(s);
                    post_op(w, None);
                }
            }
            7 => {
                let fh = get_fh(a)?;
                let (off, _count, _stable) = (a.u64()?, a.u32()?, a.u32()?);
                let data = a.opaque(MAX_IO as usize)?;
                let id = self.node(fh);
                match id.and_then(|id| self.write(id, off, data, cred)) {
                    Ok(attr) => {
                        w.u32(OK);
                        wcc(w, Some(attr));
                        w.u32(data.len() as u32).u32(FILE_SYNC).u64(self.verf);
                    }
                    Err(s) => {
                        w.u32(s);
                        wcc(w, None);
                    }
                }
            }
            8 => {
                let dir = get_fh(a)?;
                let name = get_name(a)?;
                let how = match a.u32()? {
                    0 => Create::Unchecked(get_sattr(a)?),
                    1 => Create::Guarded(get_sattr(a)?),
                    2 => Create::Exclusive(a.fixed(8)?.try_into().unwrap()),
                    _ => return Err(Garbage),
                };
                let dir = self.node(dir);
                let r = dir.and_then(|d| self.create(d, name, how, cred));
                self.reply_new(w, dir, r, cred);
            }
            9 => {
                let dir = get_fh(a)?;
                let name = get_name(a)?;
                let sattr = get_sattr(a)?;
                let dir = self.node(dir);
                let r = dir.and_then(|d| self.mkdir(d, name, sattr, cred));
                self.reply_new(w, dir, r, cred);
            }
            10 => {
                let dir = get_fh(a)?;
                let name = get_name(a)?;
                let sattr = get_sattr(a)?;
                let target = a.opaque(4096)?;
                let dir = self.node(dir);
                let r = dir.and_then(|d| self.symlink(d, name, target, sattr, cred));
                self.reply_new(w, dir, r, cred);
            }
            11 => {
                let dir = get_fh(a)?;
                get_name(a)?;
                let dir = self.node(dir);
                w.u32(NOTSUPP);
                wcc(w, dir.ok().and_then(|d| self.attr(d, cred).ok()));
            }
            12 | 13 => {
                let dir = get_fh(a)?;
                let name = get_name(a)?;
                let dir = self.node(dir);
                let r = dir.and_then(|d| self.remove(d, name, p == 13));
                w.u32(r.err().unwrap_or(OK));
                wcc(w, dir.ok().and_then(|d| self.attr(d, cred).ok()));
            }
            14 => {
                let from = get_fh(a)?;
                let fname = get_name(a)?;
                let to = get_fh(a)?;
                let tname = get_name(a)?;
                let (from, to) = (self.node(from), self.node(to));
                let r = from.and_then(|f| to.and_then(|t| self.rename(f, fname, t, tname)));
                w.u32(r.err().unwrap_or(OK));
                wcc(w, from.ok().and_then(|d| self.attr(d, cred).ok()));
                wcc(w, to.ok().and_then(|d| self.attr(d, cred).ok()));
            }
            15 => {
                let file = get_fh(a)?;
                let dir = get_fh(a)?;
                let name = get_name(a)?;
                let (file, dir) = (self.node(file), self.node(dir));
                let r = file.and_then(|f| dir.and_then(|d| self.link(f, d, name)));
                w.u32(r.err().unwrap_or(OK));
                post_op(w, file.ok().and_then(|f| self.attr(f, cred).ok()));
                wcc(w, dir.ok().and_then(|d| self.attr(d, cred).ok()));
            }
            16 | 17 => {
                let fh = get_fh(a)?;
                let cookie = a.u64()?;
                let verf = u64::from_be_bytes(a.fixed(8)?.try_into().unwrap());
                let plus = p == 17;
                if plus {
                    a.u32()?; // dircount: maxcount alone bounds the reply
                }
                let max = a.u32()?;
                let id = self.node(fh);
                if let Err(s) = id.and_then(|id| self.readdir(id, cookie, verf, max, plus, cred, w))
                {
                    w.u32(s);
                    post_op(w, id.ok().and_then(|d| self.attr(d, cred).ok()));
                }
            }
            18 => {
                let fh = get_fh(a)?;
                let id = self.node(fh);
                match id.and_then(|id| self.fsstat(id)) {
                    Ok((total, free, avail, files, ffree)) => {
                        w.u32(OK);
                        post_op(w, id.ok().and_then(|i| self.attr(i, cred).ok()));
                        w.u64(total)
                            .u64(free)
                            .u64(avail)
                            .u64(files)
                            .u64(ffree)
                            .u64(ffree);
                        w.u32(0);
                    }
                    Err(s) => {
                        w.u32(s);
                        post_op(w, None);
                    }
                }
            }
            19 => {
                let fh = get_fh(a)?;
                let id = self.node(fh);
                match id.and_then(|id| self.attr(id, cred)) {
                    Ok(attr) => {
                        w.u32(OK);
                        post_op(w, Some(attr));
                        w.u32(MAX_IO).u32(MAX_IO).u32(4096);
                        w.u32(MAX_IO).u32(MAX_IO).u32(4096);
                        w.u32(32768).u64(i64::MAX as u64).u32(0).u32(1);
                        // LINK | SYMLINK | HOMOGENEOUS | CANSETTIME
                        w.u32(0x1b);
                    }
                    Err(s) => {
                        w.u32(s);
                        post_op(w, None);
                    }
                }
            }
            20 => {
                let fh = get_fh(a)?;
                let id = self.node(fh);
                match id.and_then(|id| self.attr(id, cred)) {
                    Ok(attr) => {
                        w.u32(OK);
                        post_op(w, Some(attr));
                        w.u32(65000).u32(255);
                        w.bool(true).bool(true).bool(false).bool(true);
                    }
                    Err(s) => {
                        w.u32(s);
                        post_op(w, None);
                    }
                }
            }
            21 => {
                let fh = get_fh(a)?;
                a.u64()?;
                a.u32()?;
                let id = self.node(fh);
                match id.and_then(|id| self.attr(id, cred)) {
                    Ok(attr) => {
                        w.u32(OK);
                        wcc(w, Some(attr));
                        w.u64(self.verf);
                    }
                    Err(s) => {
                        w.u32(s);
                        wcc(w, None);
                    }
                }
            }
            _ => return Ok(false),
        }
        Ok(true)
    }

    /// CREATE, MKDIR and SYMLINK replies.
    fn reply_new(&self, w: &mut Writer, dir: R<Id>, r: R<(Id, Fattr)>, cred: Cred) {
        match r {
            Ok((id, attr)) => {
                let fh = self.nodes.lock().unwrap().handle(id);
                w.u32(OK).bool(true).opaque(&fh);
                post_op(w, Some(attr));
            }
            Err(s) => {
                w.u32(s);
            }
        }
        wcc(w, dir.ok().and_then(|d| self.attr(d, cred).ok()));
    }

    fn node(&self, fh: &[u8]) -> R<Id> {
        self.nodes.lock().unwrap().resolve(fh).map_err(|e| match e {
            HandleError::Bad => BADHANDLE,
            HandleError::Stale => STALE,
        })
    }

    fn kind(&self, id: Id) -> R<Kind> {
        self.nodes.lock().unwrap().kind(id).ok_or(STALE)
    }

    fn locate(&self, id: Id) -> R<(Loc, (u64, u64))> {
        let (path, ident) = {
            let nodes = self.nodes.lock().unwrap();
            (nodes.path(id).ok_or(STALE)?, nodes.ident(id).ok_or(STALE)?)
        };
        let anchor = fsops::open_anchor(&path.base).map_err(walk_err)?;
        if fsops::fstat(&anchor).map_err(walk_err)?.ident() != path.anchor_ident {
            return Err(STALE);
        }
        let Some((last, dirs)) = path.names.split_last() else {
            return Ok((Loc::Anchor(anchor), ident));
        };
        let mut dir = anchor;
        for name in dirs {
            dir = fsops::walk(&dir, name).map_err(walk_err)?;
        }
        Ok((Loc::At(dir, last.clone()), ident))
    }

    fn real(&self, id: Id) -> R<Real> {
        let (loc, ident) = self.locate(id)?;
        let st = loc.stat().map_err(walk_err)?;
        if st.ident() != ident {
            return Err(STALE);
        }
        Ok(Real { loc, st })
    }

    /// A real directory to create in or remove from; virtual ones refuse.
    fn real_dir(&self, id: Id) -> R<(Real, OwnedFd)> {
        match self.kind(id)? {
            Kind::Virtual(_) => return Err(ACCES),
            Kind::VirtualFile(_) => return Err(NOTDIR),
            _ => {}
        }
        let r = self.real(id)?;
        if !r.st.is_dir() {
            return Err(NOTDIR);
        }
        let d = r.loc.dir().map_err(walk_err)?;
        Ok((r, d))
    }

    fn attr(&self, id: Id, cred: Cred) -> R<Fattr> {
        match self.kind(id)? {
            Kind::Virtual(v) => Ok(self.vattr(v, cred)),
            Kind::VirtualFile(f) => Ok(self.vfile_attr(f)),
            _ => Ok(fattr(&self.real(id)?.st, id, cred)),
        }
    }

    /// Empty, read-only and root's: only their names matter.
    fn vfile_attr(&self, f: VFile) -> Fattr {
        Fattr {
            ftype: 1,
            mode: 0o444,
            nlink: 1,
            uid: 0,
            gid: 0,
            size: 0,
            used: 0,
            rdev: (0, 0),
            fileid: f.id(),
            atime: self.started,
            mtime: self.started,
            ctime: self.started,
        }
    }

    /// Whether `name` in `dir` is one of the virtual files.
    fn is_vfile(&self, dir: Id, name: &[u8]) -> bool {
        match self.kind(dir) {
            Ok(Kind::Virtual(v)) => VFile::in_dir(v).iter().any(|f| f.name().as_bytes() == name),
            _ => false,
        }
    }

    fn vattr(&self, v: VDir, cred: Cred) -> Fattr {
        let mtime = match v {
            VDir::Volumes => {
                fsops::stat_path(&self.layout.volumes).map_or(self.started, |s| s.mtime)
            }
            VDir::Containers => self.containers_mtime(),
            VDir::Machines => {
                fsops::stat_path(&self.layout.machines).map_or(self.started, |s| s.mtime)
            }
            _ => self.started,
        };
        Fattr {
            ftype: 2,
            mode: 0o555,
            nlink: 2 + v.subdirs().len() as u32,
            uid: cred.uid,
            gid: cred.gid,
            size: 4096,
            used: 4096,
            rdev: (0, 0),
            fileid: v.id(),
            atime: mtime,
            mtime,
            ctime: mtime,
        }
    }

    /// The `containers` directory changes whenever the set of running
    /// containers does; its mtime says when that last happened.
    fn containers_mtime(&self) -> (i64, u32) {
        let names: Vec<String> = self
            .layout
            .anchors(VDir::Containers)
            .into_iter()
            .map(|(k, _)| k.name().to_string())
            .collect();
        let mut seen = self.containers_seen.lock().unwrap();
        if seen.0 != names {
            *seen = (names, now());
        }
        seen.1
    }

    fn lookup(&self, dir: Id, name: &[u8], cred: Cred) -> R<(Id, Fattr)> {
        if name == b"." || name == b".." {
            let id = if name == b"." {
                dir
            } else {
                self.nodes.lock().unwrap().parent(dir).ok_or(STALE)?
            };
            return Ok((id, self.attr(id, cred)?));
        }
        if !fsops::name_ok(name) {
            return Err(if name.len() > 255 { NAMETOOLONG } else { NOENT });
        }
        match self.kind(dir)? {
            Kind::Virtual(v) => {
                if let Some((_, sub)) = v.subdirs().iter().find(|(n, _)| n.as_bytes() == name) {
                    return Ok((sub.id(), self.vattr(*sub, cred)));
                }
                if let Some(f) = VFile::in_dir(v).iter().find(|f| f.name().as_bytes() == name) {
                    return Ok((f.id(), self.vfile_attr(*f)));
                }
                let name = std::str::from_utf8(name).map_err(|_| NOENT)?;
                let (key, base) = self.layout.find(v, name).ok_or(NOENT)?;
                self.anchor(key, base, cred)
            }
            Kind::VirtualFile(_) => Err(NOTDIR),
            _ => {
                let r = self.real(dir)?;
                if !r.st.is_dir() {
                    return Err(NOTDIR);
                }
                let d = r.loc.dir().map_err(walk_err)?;
                if fsops::is_pseudo_fs(&d) {
                    return Err(NOENT);
                }
                let st = fsops::statat(&d, name).map_err(st_err)?;
                let id = self
                    .nodes
                    .lock()
                    .unwrap()
                    .child(dir, name, st.ident())
                    .ok_or(STALE)?;
                Ok((id, fattr(&st, id, cred)))
            }
        }
    }

    fn anchor(
        &self,
        key: crate::nodes::AnchorKey,
        base: std::path::PathBuf,
        cred: Cred,
    ) -> R<(Id, Fattr)> {
        let fd = fsops::open_anchor(&base).map_err(|_| NOENT)?;
        let st = fsops::fstat(&fd).map_err(st_err)?;
        let id = self.nodes.lock().unwrap().anchor(key, base, st.ident());
        Ok((id, fattr(&st, id, cred)))
    }

    fn access(&self, id: Id, want: u32, cred: Cred) -> R<(Fattr, u32)> {
        use access::*;
        let attr = self.attr(id, cred)?;
        let granted = match self.kind(id)? {
            Kind::Virtual(_) => READ | LOOKUP | EXECUTE,
            Kind::VirtualFile(_) => READ,
            _ => match attr.ftype {
                2 => READ | LOOKUP | MODIFY | EXTEND | DELETE | EXECUTE,
                1 if attr.mode & 0o111 == 0 => READ | MODIFY | EXTEND | DELETE,
                1 => READ | MODIFY | EXTEND | DELETE | EXECUTE,
                _ => READ | MODIFY | EXTEND | DELETE,
            },
        };
        Ok((attr, want & granted))
    }

    fn setattr(&self, id: Id, s: Sattr, guard: Option<(i64, u32)>, cred: Cred) -> R<()> {
        if let Kind::Virtual(_) | Kind::VirtualFile(_) = self.kind(id)? {
            return Err(ACCES);
        }
        let r = self.real(id)?;
        if let Some(g) = guard
            && g != (r.st.ctime.0.clamp(0, u32::MAX as i64), r.st.ctime.1)
        {
            return Err(NOT_SYNC);
        }
        if let Some(size) = s.size {
            if r.st.is_dir() {
                return Err(ISDIR);
            }
            if !r.st.is_reg() {
                return Err(INVAL);
            }
            let fd = r
                .loc
                .open(libc::O_WRONLY | libc::O_NONBLOCK)
                .map_err(st_err)?;
            fsops::ftruncate(&fd, size).map_err(st_err)?;
        }
        if s.uid.is_some() || s.gid.is_some() {
            let uid = s.uid.map(|u| stored(u, cred.uid));
            let gid = s.gid.map(|g| stored(g, cred.gid));
            r.loc.chown(uid, gid).map_err(st_err)?;
        }
        if let Some(mode) = s.mode
            && !r.st.is_symlink()
        {
            r.loc.chmod(mode).map_err(st_err)?;
        }
        if s.atime.is_some() || s.mtime.is_some() {
            r.loc
                .utimes(
                    s.atime.unwrap_or(SetTime::Keep),
                    s.mtime.unwrap_or(SetTime::Keep),
                )
                .map_err(st_err)?;
        }
        Ok(())
    }

    fn readlink(&self, id: Id) -> R<Vec<u8>> {
        if let Kind::Virtual(_) | Kind::VirtualFile(_) = self.kind(id)? {
            return Err(INVAL);
        }
        let r = self.real(id)?;
        match &r.loc {
            Loc::At(d, n) if r.st.is_symlink() => fsops::readlinkat(d, n).map_err(st_err),
            _ => Err(INVAL),
        }
    }

    /// The opened regular file behind `id`, checked to still be that file.
    fn open_file(&self, id: Id, flags: libc::c_int) -> R<(OwnedFd, Stat)> {
        match self.kind(id)? {
            Kind::Virtual(_) => return Err(ISDIR),
            Kind::VirtualFile(_) => return Err(ACCES),
            _ => {}
        }
        let r = self.real(id)?;
        if r.st.is_dir() {
            return Err(ISDIR);
        }
        if !r.st.is_reg() {
            return Err(INVAL);
        }
        let fd = r.loc.open(flags | libc::O_NONBLOCK).map_err(walk_err)?;
        let st = fsops::fstat(&fd).map_err(st_err)?;
        if st.ident() != r.st.ident() {
            return Err(STALE);
        }
        Ok((fd, st))
    }

    fn read(&self, id: Id, off: u64, count: u32, cred: Cred, w: &mut Writer) -> R<()> {
        if let Kind::VirtualFile(f) = self.kind(id)? {
            w.u32(OK);
            post_op(w, Some(self.vfile_attr(f)));
            w.u32(0).bool(true).opaque(&[]);
            return Ok(());
        }
        let (fd, st) = self.open_file(id, libc::O_RDONLY)?;
        // Read straight into the reply: status, attributes, count, eof,
        // then the data as an opaque.
        let mark = w.len();
        w.u32(OK);
        post_op(w, Some(fattr(&st, id, cred)));
        let head = w.len();
        w.u32(0).u32(0).u32(0);
        let start = w.len();
        let t = Instant::now();
        let n = fsops::pread_append(&fd, &mut w.buf, count as usize, off);
        stats::io(stats::READ, t);
        let n = match n {
            Ok(n) => n,
            Err(e) => {
                w.buf.truncate(mark);
                return Err(st_err(e));
            }
        };
        w.buf.resize(start + n.div_ceil(4) * 4, 0);
        let eof = n < count as usize || off + n as u64 >= st.size;
        w.buf[head..head + 4].copy_from_slice(&(n as u32).to_be_bytes());
        w.buf[head + 4..head + 8].copy_from_slice(&(eof as u32).to_be_bytes());
        w.buf[head + 8..head + 12].copy_from_slice(&(n as u32).to_be_bytes());
        Ok(())
    }

    fn write(&self, id: Id, off: u64, data: &[u8], cred: Cred) -> R<Fattr> {
        let (fd, _) = self.open_file(id, libc::O_WRONLY)?;
        let t = Instant::now();
        let r = fsops::pwrite_all(&fd, data, off);
        stats::io(stats::WRITE, t);
        r.map_err(st_err)?;
        Ok(fattr(&fsops::fstat(&fd).map_err(st_err)?, id, cred))
    }

    /// Record a new entry and give it the directory's owner unless the
    /// client named one.
    fn adopt(
        &self,
        dir: Id,
        d: &OwnedFd,
        name: &[u8],
        parent: &Stat,
        s: &Sattr,
        cred: Cred,
    ) -> R<(Id, Fattr)> {
        let uid = s.uid.map_or(parent.uid, |u| stored(u, cred.uid));
        let gid = s.gid.map_or(parent.gid, |g| stored(g, cred.gid));
        fsops::chownat(d, name, Some(uid), Some(gid)).map_err(st_err)?;
        let st = fsops::statat(d, name).map_err(st_err)?;
        let id = self
            .nodes
            .lock()
            .unwrap()
            .child(dir, name, st.ident())
            .ok_or(STALE)?;
        Ok((id, fattr(&st, id, cred)))
    }

    fn new_name<'a>(&self, name: &'a [u8]) -> R<&'a [u8]> {
        if name.len() > 255 {
            return Err(NAMETOOLONG);
        }
        if !fsops::name_ok(name) || name == b"." || name == b".." {
            return Err(INVAL);
        }
        Ok(name)
    }

    fn create(&self, dir: Id, name: &[u8], how: Create, cred: Cred) -> R<(Id, Fattr)> {
        let name = self.new_name(name)?;
        if self.is_vfile(dir, name) {
            // UNCHECKED would open the file for writing.
            return Err(if let Create::Unchecked(_) = how {
                ACCES
            } else {
                EXIST
            });
        }
        let (parent, d) = self.real_dir(dir)?;
        let excl = libc::O_CREAT | libc::O_EXCL | libc::O_WRONLY | libc::O_NONBLOCK;
        match how {
            Create::Exclusive(verf) => {
                let (a, m) = (
                    u32::from_be_bytes(verf[..4].try_into().unwrap()),
                    u32::from_be_bytes(verf[4..].try_into().unwrap()),
                );
                match fsops::openat(&d, name, excl, 0o600) {
                    Ok(_) => {
                        let r = self.adopt(dir, &d, name, &parent.st, &Sattr::default(), cred)?;
                        let (at, mt) = (SetTime::At(a as i64, 0), SetTime::At(m as i64, 0));
                        fsops::utimensat(&d, name, at, mt).map_err(st_err)?;
                        let st = fsops::statat(&d, name).map_err(st_err)?;
                        Ok((r.0, fattr(&st, r.0, cred)))
                    }
                    Err(e) if e.raw_os_error() == Some(libc::EEXIST) => {
                        // A retransmission of a create that already worked.
                        let st = fsops::statat(&d, name).map_err(st_err)?;
                        if st.is_reg() && st.atime.0 == a as i64 && st.mtime.0 == m as i64 {
                            let id = self
                                .nodes
                                .lock()
                                .unwrap()
                                .child(dir, name, st.ident())
                                .ok_or(STALE)?;
                            Ok((id, fattr(&st, id, cred)))
                        } else {
                            Err(EXIST)
                        }
                    }
                    Err(e) => Err(st_err(e)),
                }
            }
            Create::Unchecked(s) | Create::Guarded(s) => {
                let guarded = matches!(how, Create::Guarded(_));
                let mode = s.mode.unwrap_or(0o644);
                let fd = match fsops::openat(&d, name, excl, mode) {
                    Ok(fd) => {
                        fsops::fchmod(&fd, mode).map_err(st_err)?;
                        self.adopt(dir, &d, name, &parent.st, &s, cred)?;
                        fd
                    }
                    Err(e) if e.raw_os_error() == Some(libc::EEXIST) && !guarded => {
                        fsops::openat(&d, name, libc::O_WRONLY | libc::O_NONBLOCK, 0)
                            .map_err(st_err)?
                    }
                    Err(e) => return Err(st_err(e)),
                };
                if let Some(size) = s.size {
                    fsops::ftruncate(&fd, size).map_err(st_err)?;
                }
                if s.atime.is_some() || s.mtime.is_some() {
                    let keep = SetTime::Keep;
                    fsops::futimens(&fd, s.atime.unwrap_or(keep), s.mtime.unwrap_or(keep))
                        .map_err(st_err)?;
                }
                let st = fsops::fstat(&fd).map_err(st_err)?;
                let id = self
                    .nodes
                    .lock()
                    .unwrap()
                    .child(dir, name, st.ident())
                    .ok_or(STALE)?;
                Ok((id, fattr(&st, id, cred)))
            }
        }
    }

    fn mkdir(&self, dir: Id, name: &[u8], s: Sattr, cred: Cred) -> R<(Id, Fattr)> {
        let name = self.new_name(name)?;
        if self.is_vfile(dir, name) {
            return Err(EXIST);
        }
        let (parent, d) = self.real_dir(dir)?;
        let mode = s.mode.unwrap_or(0o755);
        fsops::mkdirat(&d, name, mode).map_err(st_err)?;
        // Exactly the mode asked for, whatever the process umask.
        fsops::chmodat(&d, name, mode).map_err(st_err)?;
        self.adopt(dir, &d, name, &parent.st, &s, cred)
    }

    fn symlink(&self, dir: Id, name: &[u8], target: &[u8], s: Sattr, cred: Cred) -> R<(Id, Fattr)> {
        let name = self.new_name(name)?;
        if self.is_vfile(dir, name) {
            return Err(EXIST);
        }
        let (parent, d) = self.real_dir(dir)?;
        if target.is_empty() || target.contains(&0) {
            return Err(INVAL);
        }
        fsops::symlinkat(target, &d, name).map_err(st_err)?;
        self.adopt(dir, &d, name, &parent.st, &s, cred)
    }

    fn remove(&self, dir: Id, name: &[u8], is_dir: bool) -> R<()> {
        if name == b"." || name == b".." {
            return Err(INVAL);
        }
        let (_, d) = self.real_dir(dir)?;
        let st = fsops::statat(&d, name).map_err(st_err)?;
        if st.is_dir() != is_dir {
            return Err(if is_dir { NOTDIR } else { ISDIR });
        }
        fsops::unlinkat(&d, name, is_dir).map_err(st_err)?;
        if is_dir || st.nlink <= 1 {
            self.nodes.lock().unwrap().forget_ident(st.ident());
        }
        Ok(())
    }

    fn rename(&self, from: Id, fname: &[u8], to: Id, tname: &[u8]) -> R<()> {
        if fname == b"." || fname == b".." {
            return Err(INVAL);
        }
        let tname = self.new_name(tname)?;
        let (_, fd) = self.real_dir(from)?;
        let (_, td) = self.real_dir(to)?;
        let src = fsops::statat(&fd, fname).map_err(st_err)?;
        let dst = fsops::statat(&td, tname).ok();
        fsops::renameat(&fd, fname, &td, tname).map_err(st_err)?;
        let mut nodes = self.nodes.lock().unwrap();
        if let Some(dst) = dst
            && dst.ident() != src.ident()
            && (dst.is_dir() || dst.nlink <= 1)
        {
            nodes.forget_ident(dst.ident());
        }
        nodes.moved(src.ident(), to, tname);
        Ok(())
    }

    fn link(&self, file: Id, dir: Id, name: &[u8]) -> R<()> {
        let name = self.new_name(name)?;
        if self.is_vfile(dir, name) {
            return Err(EXIST);
        }
        if let Kind::Virtual(_) | Kind::VirtualFile(_) = self.kind(file)? {
            return Err(ACCES);
        }
        let r = self.real(file)?;
        let Loc::At(fdir, fname) = &r.loc else {
            return Err(ACCES);
        };
        if r.st.is_dir() {
            return Err(ISDIR);
        }
        let (_, d) = self.real_dir(dir)?;
        fsops::linkat(fdir, fname, &d, name).map_err(st_err)
    }

    fn fsstat(&self, id: Id) -> R<(u64, u64, u64, u64, u64)> {
        let fd = match self.kind(id)? {
            Kind::Virtual(_) | Kind::VirtualFile(_) => fsops::open_anchor(&self.layout.volumes)
                .or_else(|_| fsops::open_anchor(std::path::Path::new("/")))
                .map_err(st_err)?,
            _ => match self.real(id)?.loc {
                Loc::Anchor(fd) | Loc::At(fd, _) => fd,
            },
        };
        fsops::fsstat(&fd).map_err(st_err)
    }

    /// All names in a directory, `.` and `..` first. Paged calls reuse the
    /// listing their verifier names, so a page boundary never skips or
    /// repeats an entry while the directory changes.
    fn listing(&self, dir: Id, cookie: u64, verf: u64) -> R<(u64, Vec<Vec<u8>>)> {
        if cookie != 0
            && let Some(l) = self.listings.lock().unwrap().get(&dir)
            && l.verf == verf
        {
            return Ok((l.verf, l.names.clone()));
        }
        let mut names = vec![b".".to_vec(), b"..".to_vec()];
        match self.kind(dir)? {
            Kind::Virtual(v) => {
                names.extend(v.subdirs().iter().map(|(n, _)| n.as_bytes().to_vec()));
                names.extend(VFile::in_dir(v).iter().map(|f| f.name().as_bytes().to_vec()));
                names.extend(
                    self.layout
                        .anchors(v)
                        .into_iter()
                        .map(|(k, _)| k.name().as_bytes().to_vec()),
                );
            }
            _ => {
                let r = self.real(dir)?;
                if !r.st.is_dir() {
                    return Err(NOTDIR);
                }
                let d = r.loc.dir().map_err(walk_err)?;
                if !fsops::is_pseudo_fs(&d) {
                    names.extend(fsops::list(&d).map_err(st_err)?);
                }
            }
        }
        let verf = {
            let mut n = self.next_listing.lock().unwrap();
            *n += 1;
            self.verf ^ *n
        };
        let mut listings = self.listings.lock().unwrap();
        if listings.len() >= MAX_LISTINGS {
            let oldest = listings.iter().min_by_key(|(_, l)| l.at).map(|(k, _)| *k);
            if let Some(k) = oldest {
                listings.remove(&k);
            }
        }
        listings.insert(
            dir,
            Listing {
                verf,
                names: names.clone(),
                at: Instant::now(),
            },
        );
        Ok((verf, names))
    }

    /// One entry of a listing: its node and attributes. `None` if it went
    /// away since the listing was read.
    fn entry(&self, dir: Id, d: Option<&OwnedFd>, name: &[u8], cred: Cred) -> Option<(Id, Fattr)> {
        match d {
            Some(d) if name != b"." && name != b".." => {
                let st = fsops::statat(d, name).ok()?;
                let id = self.nodes.lock().unwrap().child(dir, name, st.ident())?;
                Some((id, fattr(&st, id, cred)))
            }
            _ => self.lookup(dir, name, cred).ok(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn readdir(
        &self,
        dir: Id,
        cookie: u64,
        verf: u64,
        max: u32,
        plus: bool,
        cred: Cred,
        w: &mut Writer,
    ) -> R<()> {
        let dattr = self.attr(dir, cred)?;
        if dattr.ftype != 2 {
            return Err(NOTDIR);
        }
        let (verf, names) = self.listing(dir, cookie, verf)?;
        let d = match self.kind(dir)? {
            Kind::Virtual(_) => None,
            _ => Some(self.real(dir)?.loc.dir().map_err(walk_err)?),
        };
        let mark = w.len();
        w.u32(OK);
        post_op(w, Some(dattr));
        w.u64(verf);
        // Room for the RPC header, the end marker and eof.
        let budget = (max as usize).saturating_sub(w.len() - mark + 32);
        let start = w.len();
        let mut eof = true;
        let mut wrote = 0;
        for (i, name) in names.iter().enumerate().skip(cookie as usize) {
            let Some((id, attr)) = self.entry(dir, d.as_ref(), name, cred) else {
                continue;
            };
            let size = 4
                + 8
                + 4
                + name.len().div_ceil(4) * 4
                + 8
                + if plus { 4 + 84 + 8 + HANDLE_LEN } else { 0 };
            if w.len() - start + size > budget {
                eof = false;
                break;
            }
            w.bool(true).u64(attr.fileid).opaque(name).u64(i as u64 + 1);
            if plus {
                if name == b".." {
                    w.bool(false).bool(false);
                } else {
                    post_op(w, Some(attr));
                    let fh = self.nodes.lock().unwrap().handle(id);
                    w.bool(true).opaque(&fh);
                }
            }
            wrote += 1;
        }
        if wrote == 0 && !eof {
            w.buf.truncate(mark);
            return Err(TOOSMALL);
        }
        w.bool(false).bool(eof);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum Create {
    Unchecked(Sattr),
    Guarded(Sattr),
    Exclusive([u8; 8]),
}

#[cfg(test)]
mod tests;
