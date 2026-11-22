//! The server driven through RPC records, on a temp directory laid out like
//! Docker's volumes plus one fake container root.

use std::path::PathBuf;

use super::*;
use crate::layout::FixedContainers;

const CRED: Cred = Cred { uid: 501, gid: 20 };

struct T {
    s: Server,
    dir: PathBuf,
    xid: u32,
}

#[derive(Debug)]
struct Ent {
    name: Vec<u8>,
    fileid: u64,
    cookie: u64,
    fh: Option<Vec<u8>>,
    attr: Option<Fattr>,
}

fn get_fattr(r: &mut Reader) -> Fattr {
    let mut f = || r.u32().unwrap();
    let (ftype, mode, nlink, uid, gid) = (f(), f(), f(), f(), f());
    let size = r.u64().unwrap();
    let used = r.u64().unwrap();
    let rdev = (r.u32().unwrap(), r.u32().unwrap());
    assert_eq!(r.u64().unwrap(), FSID);
    let fileid = r.u64().unwrap();
    let mut t = || (r.u32().unwrap() as i64, r.u32().unwrap());
    let (atime, mtime, ctime) = (t(), t(), t());
    Fattr {
        ftype,
        mode,
        nlink,
        uid,
        gid,
        size,
        used,
        rdev,
        fileid,
        atime,
        mtime,
        ctime,
    }
}

fn get_post_op(r: &mut Reader) -> Option<Fattr> {
    r.bool().unwrap().then(|| get_fattr(r))
}

fn skip_wcc(r: &mut Reader) {
    assert!(!r.bool().unwrap(), "no pre-op attributes are sent");
    get_post_op(r);
}

fn sattr(w: &mut Writer, mode: Option<u32>, size: Option<u64>) {
    match mode {
        Some(m) => w.bool(true).u32(m),
        None => w.bool(false),
    };
    w.bool(false).bool(false);
    match size {
        Some(s) => w.bool(true).u64(s),
        None => w.bool(false),
    };
    w.u32(0).u32(0);
}

impl T {
    fn new(tag: &str) -> T {
        let dir = std::env::temp_dir().join(format!("kat-nfs-srv-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("volumes/v1/_data")).unwrap();
        std::fs::create_dir_all(dir.join("volumes/v2/_data")).unwrap();
        std::fs::create_dir_all(dir.join("c1/etc")).unwrap();
        std::fs::write(dir.join("c1/etc/hostname"), b"c1\n").unwrap();
        let layout = Layout {
            volumes: dir.join("volumes"),
            containers: Box::new(FixedContainers(vec![("c1".into(), dir.join("c1"))])),
            machines: dir.join("machines"),
        };
        T {
            s: Server::new(layout),
            dir,
            xid: 0,
        }
    }

    fn data(&self) -> PathBuf {
        self.dir.join("volumes/v1/_data")
    }

    /// Call and return (accept status, results).
    fn raw(&mut self, prog: u32, vers: u32, proc_: u32, args: &[u8]) -> (u32, Vec<u8>) {
        self.xid += 1;
        let rec = rpc::call(self.xid, prog, vers, proc_, CRED, args);
        let reply = self.s.handle(&rec[4..]).expect("a reply");
        let (xid, stat, r) = rpc::parse_reply(&reply[4..]).unwrap();
        assert_eq!(xid, self.xid);
        let rest = reply.len() - r.remaining();
        (stat, reply[rest..].to_vec())
    }

    fn nfs(&mut self, proc_: u32, f: impl FnOnce(&mut Writer)) -> Vec<u8> {
        let mut w = Writer::default();
        f(&mut w);
        let (stat, res) = self.raw(PROG_NFS, 3, proc_, &w.buf);
        assert_eq!(stat, rpc::SUCCESS);
        res
    }

    fn root(&mut self) -> Vec<u8> {
        self.mnt("/").unwrap()
    }

    fn mnt(&mut self, path: &str) -> Result<Vec<u8>, u32> {
        let mut w = Writer::default();
        w.opaque(path.as_bytes());
        let (stat, res) = self.raw(PROG_MOUNT, 3, 1, &w.buf);
        assert_eq!(stat, rpc::SUCCESS);
        let mut r = Reader::new(&res);
        match r.u32().unwrap() {
            0 => {
                let fh = r.opaque(64).unwrap().to_vec();
                assert_eq!((r.u32(), r.u32()), (Ok(1), Ok(rpc::AUTH_SYS)));
                Ok(fh)
            }
            s => Err(s),
        }
    }

    fn getattr(&mut self, fh: &[u8]) -> Result<Fattr, u32> {
        let res = self.nfs(1, |w| {
            w.opaque(fh);
        });
        let mut r = Reader::new(&res);
        match r.u32().unwrap() {
            OK => Ok(get_fattr(&mut r)),
            s => Err(s),
        }
    }

    fn lookup(&mut self, dir: &[u8], name: &str) -> Result<(Vec<u8>, Fattr), u32> {
        let res = self.nfs(3, |w| {
            w.opaque(dir).opaque(name.as_bytes());
        });
        let mut r = Reader::new(&res);
        let s = r.u32().unwrap();
        let out = if s == OK {
            let fh = r.opaque(64).unwrap().to_vec();
            Ok((fh, get_post_op(&mut r).unwrap()))
        } else {
            Err(s)
        };
        get_post_op(&mut r);
        assert_eq!(r.remaining(), 0);
        out
    }

    fn path(&mut self, path: &str) -> Result<Vec<u8>, u32> {
        let mut fh = self.root();
        for n in path.split('/').filter(|n| !n.is_empty()) {
            fh = self.lookup(&fh, n)?.0;
        }
        Ok(fh)
    }

    fn new_reply(res: &[u8]) -> Result<Vec<u8>, u32> {
        let mut r = Reader::new(res);
        let s = r.u32().unwrap();
        let out = if s == OK {
            assert!(r.bool().unwrap());
            let fh = r.opaque(64).unwrap().to_vec();
            get_post_op(&mut r).unwrap();
            Ok(fh)
        } else {
            Err(s)
        };
        skip_wcc(&mut r);
        assert_eq!(r.remaining(), 0);
        out
    }

    fn create(&mut self, dir: &[u8], name: &str, mode: u32) -> Result<Vec<u8>, u32> {
        let res = self.nfs(8, |w| {
            w.opaque(dir).opaque(name.as_bytes()).u32(1);
            sattr(w, Some(mode), None);
        });
        Self::new_reply(&res)
    }

    fn create_excl(&mut self, dir: &[u8], name: &str, verf: [u8; 8]) -> Result<Vec<u8>, u32> {
        let res = self.nfs(8, |w| {
            w.opaque(dir).opaque(name.as_bytes()).u32(2).fixed(&verf);
        });
        Self::new_reply(&res)
    }

    fn mkdir(&mut self, dir: &[u8], name: &str) -> Result<Vec<u8>, u32> {
        let res = self.nfs(9, |w| {
            w.opaque(dir).opaque(name.as_bytes());
            sattr(w, Some(0o750), None);
        });
        Self::new_reply(&res)
    }

    fn symlink(&mut self, dir: &[u8], name: &str, target: &str) -> Result<Vec<u8>, u32> {
        let res = self.nfs(10, |w| {
            w.opaque(dir).opaque(name.as_bytes());
            sattr(w, None, None);
            w.opaque(target.as_bytes());
        });
        Self::new_reply(&res)
    }

    fn readlink(&mut self, fh: &[u8]) -> Result<Vec<u8>, u32> {
        let res = self.nfs(5, |w| {
            w.opaque(fh);
        });
        let mut r = Reader::new(&res);
        let s = r.u32().unwrap();
        get_post_op(&mut r);
        if s == OK {
            Ok(r.opaque(4096).unwrap().to_vec())
        } else {
            Err(s)
        }
    }

    fn write(&mut self, fh: &[u8], off: u64, data: &[u8]) -> Result<(), u32> {
        let res = self.nfs(7, |w| {
            w.opaque(fh)
                .u64(off)
                .u32(data.len() as u32)
                .u32(0)
                .opaque(data);
        });
        let mut r = Reader::new(&res);
        let s = r.u32().unwrap();
        skip_wcc(&mut r);
        if s != OK {
            return Err(s);
        }
        assert_eq!(r.u32().unwrap(), data.len() as u32);
        assert_eq!(r.u32().unwrap(), FILE_SYNC);
        r.u64().unwrap();
        Ok(())
    }

    fn read(&mut self, fh: &[u8], off: u64, count: u32) -> Result<(Vec<u8>, bool), u32> {
        let res = self.nfs(6, |w| {
            w.opaque(fh).u64(off).u32(count);
        });
        let mut r = Reader::new(&res);
        let s = r.u32().unwrap();
        get_post_op(&mut r);
        if s != OK {
            return Err(s);
        }
        let n = r.u32().unwrap();
        let eof = r.bool().unwrap();
        let data = r.opaque(MAX_IO as usize).unwrap().to_vec();
        assert_eq!(n as usize, data.len());
        assert_eq!(r.remaining(), 0);
        Ok((data, eof))
    }

    fn remove(&mut self, dir: &[u8], name: &str, rmdir: bool) -> Result<(), u32> {
        let res = self.nfs(if rmdir { 13 } else { 12 }, |w| {
            w.opaque(dir).opaque(name.as_bytes());
        });
        let mut r = Reader::new(&res);
        let s = r.u32().unwrap();
        skip_wcc(&mut r);
        if s == OK { Ok(()) } else { Err(s) }
    }

    fn rename(&mut self, from: &[u8], f: &str, to: &[u8], t: &str) -> Result<(), u32> {
        let res = self.nfs(14, |w| {
            w.opaque(from)
                .opaque(f.as_bytes())
                .opaque(to)
                .opaque(t.as_bytes());
        });
        let mut r = Reader::new(&res);
        let s = r.u32().unwrap();
        skip_wcc(&mut r);
        skip_wcc(&mut r);
        if s == OK { Ok(()) } else { Err(s) }
    }

    fn link(&mut self, file: &[u8], dir: &[u8], name: &str) -> Result<(), u32> {
        let res = self.nfs(15, |w| {
            w.opaque(file).opaque(dir).opaque(name.as_bytes());
        });
        let mut r = Reader::new(&res);
        let s = r.u32().unwrap();
        get_post_op(&mut r);
        skip_wcc(&mut r);
        assert_eq!(r.remaining(), 0);
        if s == OK { Ok(()) } else { Err(s) }
    }

    fn setattr(&mut self, fh: &[u8], f: impl FnOnce(&mut Writer)) -> u32 {
        let res = self.nfs(2, |w| {
            w.opaque(fh);
            f(w);
        });
        let mut r = Reader::new(&res);
        let s = r.u32().unwrap();
        skip_wcc(&mut r);
        s
    }

    fn access(&mut self, fh: &[u8], want: u32) -> Result<u32, u32> {
        let res = self.nfs(4, |w| {
            w.opaque(fh).u32(want);
        });
        let mut r = Reader::new(&res);
        let s = r.u32().unwrap();
        get_post_op(&mut r);
        if s == OK {
            Ok(r.u32().unwrap())
        } else {
            Err(s)
        }
    }

    /// Every entry, page by page, with READDIRPLUS (or READDIR).
    fn list(&mut self, dir: &[u8], max: u32, plus: bool) -> Result<Vec<Ent>, u32> {
        let (mut cookie, mut verf, mut out) = (0u64, [0u8; 8], Vec::new());
        loop {
            let res = self.nfs(if plus { 17 } else { 16 }, |w| {
                w.opaque(dir).u64(cookie).fixed(&verf);
                if plus {
                    w.u32(max);
                }
                w.u32(max);
            });
            assert!(res.len() <= max as usize);
            let mut r = Reader::new(&res);
            let s = r.u32().unwrap();
            get_post_op(&mut r);
            if s != OK {
                return Err(s);
            }
            verf = r.fixed(8).unwrap().try_into().unwrap();
            while r.bool().unwrap() {
                let fileid = r.u64().unwrap();
                let name = r.opaque(255).unwrap().to_vec();
                cookie = r.u64().unwrap();
                let (mut attr, mut fh) = (None, None);
                if plus {
                    attr = get_post_op(&mut r);
                    if r.bool().unwrap() {
                        fh = Some(r.opaque(64).unwrap().to_vec());
                    }
                }
                out.push(Ent {
                    name,
                    fileid,
                    cookie,
                    fh,
                    attr,
                });
            }
            if r.bool().unwrap() {
                return Ok(out);
            }
        }
    }

    fn names(&mut self, dir: &[u8]) -> Vec<String> {
        let mut n: Vec<String> = self
            .list(dir, 32768, true)
            .unwrap()
            .into_iter()
            .map(|e| String::from_utf8(e.name).unwrap())
            .collect();
        n.sort();
        n
    }
}

impl Drop for T {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[test]
fn mounts_and_shows_the_virtual_tree() {
    let mut t = T::new("tree");
    let root = t.root();
    assert_eq!(
        t.names(&root),
        [
            ".",
            "..",
            ".metadata-never-index",
            ".metadata_never_index",
            ".metadata_never_index_unless_rootfs",
            "docker",
            "machines"
        ]
    );
    let docker = t.lookup(&root, "docker").unwrap().0;
    assert_eq!(t.names(&docker), [".", "..", "containers", "volumes"]);
    let vols = t.path("docker/volumes").unwrap();
    assert_eq!(t.names(&vols), [".", "..", "v1", "v2"]);
    let cs = t.path("docker/containers").unwrap();
    assert_eq!(t.names(&cs), [".", "..", "c1"]);
    let m = t.path("machines").unwrap();
    assert_eq!(t.names(&m), [".", ".."]);
    // MNT of a deeper path, and of one that is not there.
    assert_eq!(t.mnt("/docker/volumes/v1"), t.path("docker/volumes/v1"));
    assert_eq!(t.mnt("/nope"), Err(NOENT));
    let a = t.getattr(&root).unwrap();
    assert_eq!(
        (a.ftype, a.mode, a.uid, a.gid, a.fileid),
        (2, 0o555, 501, 20, 1)
    );
    // `..` of a volume is the volumes directory; of the root, the root.
    let v1 = t.path("docker/volumes/v1").unwrap();
    assert_eq!(t.lookup(&v1, "..").unwrap().0, vols);
    assert_eq!(t.lookup(&root, "..").unwrap().0, root);
    // A container's files.
    let host = t.path("docker/containers/c1/etc/hostname").unwrap();
    assert_eq!(t.read(&host, 0, 100).unwrap(), (b"c1\n".to_vec(), true));
}

#[test]
fn reads_and_writes_files_in_a_volume() {
    let mut t = T::new("rw");
    let v1 = t.path("docker/volumes/v1").unwrap();
    let f = t.create(&v1, "hello.txt", 0o640).unwrap();
    t.write(&f, 0, b"hello ").unwrap();
    t.write(&f, 6, b"world").unwrap();
    assert_eq!(
        std::fs::read(t.data().join("hello.txt")).unwrap(),
        b"hello world"
    );
    assert_eq!(t.read(&f, 0, 5).unwrap(), (b"hello".to_vec(), false));
    assert_eq!(t.read(&f, 6, 100).unwrap(), (b"world".to_vec(), true));
    assert_eq!(t.read(&f, 50, 10).unwrap(), (Vec::new(), true));
    let a = t.getattr(&f).unwrap();
    assert_eq!((a.ftype, a.mode, a.size), (1, 0o640, 11));
    // GUARDED create of an existing name fails; the file is untouched.
    assert_eq!(t.create(&v1, "hello.txt", 0o600), Err(EXIST));
    assert_eq!(t.getattr(&f).unwrap().size, 11);

    let d = t.mkdir(&v1, "sub").unwrap();
    assert_eq!(t.getattr(&d).unwrap().mode, 0o750);
    t.rename(&v1, "hello.txt", &d, "moved.txt").unwrap();
    assert!(t.data().join("sub/moved.txt").is_file());
    // The old handle follows the file to its new name.
    assert_eq!(t.read(&f, 0, 5).unwrap().0, b"hello");
    assert_eq!(t.names(&d), [".", "..", "moved.txt"]);
    // Renaming the directory moves what is below it.
    t.rename(&v1, "sub", &v1, "sub2").unwrap();
    assert_eq!(t.read(&f, 0, 5).unwrap().0, b"hello");

    let l = t.symlink(&d, "link", "../elsewhere").unwrap();
    assert_eq!(t.readlink(&l).unwrap(), b"../elsewhere");
    assert_eq!(t.getattr(&l).unwrap().ftype, 5);
    assert_eq!(t.read(&l, 0, 10), Err(INVAL));

    assert_eq!(t.remove(&v1, "sub2", true), Err(NOTEMPTY));
    assert_eq!(t.remove(&v1, "sub2", false), Err(ISDIR));
    t.remove(&d, "moved.txt", false).unwrap();
    t.remove(&d, "link", false).unwrap();
    assert_eq!(t.getattr(&f), Err(STALE));
    t.remove(&v1, "sub2", true).unwrap();
    assert_eq!(t.names(&v1), [".", ".."]);
    assert!(std::fs::read_dir(t.data()).unwrap().next().is_none());
}

#[test]
fn sees_changes_made_inside_the_guest() {
    let mut t = T::new("ext");
    let v1 = t.path("docker/volumes/v1").unwrap();
    std::fs::write(t.data().join("a"), b"one").unwrap();
    let (a, _) = t.lookup(&v1, "a").unwrap();
    // Replaced behind our back: the handle names the old file.
    std::fs::remove_file(t.data().join("a")).unwrap();
    std::fs::write(t.data().join("tmp"), b"two").unwrap();
    std::fs::rename(t.data().join("tmp"), t.data().join("a")).unwrap();
    assert_eq!(t.getattr(&a), Err(STALE));
    let (a2, _) = t.lookup(&v1, "a").unwrap();
    assert_eq!(t.read(&a2, 0, 10).unwrap().0, b"two");
    // Moved by someone else, then found under its new name.
    std::fs::rename(t.data().join("a"), t.data().join("b")).unwrap();
    let (b, _) = t.lookup(&v1, "b").unwrap();
    assert_eq!(a2, b);
    // A volume that is removed takes its handles with it.
    std::fs::remove_dir_all(t.dir.join("volumes/v1")).unwrap();
    assert_eq!(t.getattr(&v1), Err(STALE));
    assert_eq!(t.getattr(&b), Err(STALE));
    let vols = t.path("docker/volumes").unwrap();
    assert_eq!(t.names(&vols), [".", "..", "v2"]);
}

#[test]
fn never_follows_symlinks_out_of_an_anchor() {
    let mut t = T::new("sym");
    let v1 = t.path("docker/volumes/v1").unwrap();
    std::os::unix::fs::symlink("/", t.data().join("esc")).unwrap();
    let (esc, attr) = t.lookup(&v1, "esc").unwrap();
    assert_eq!(attr.ftype, 5);
    assert_eq!(t.lookup(&esc, "etc"), Err(NOTDIR));
    assert_eq!(t.create(&esc, "x", 0o644), Err(NOTDIR));
    // A directory swapped for a symlink after it was looked up.
    std::fs::create_dir(t.data().join("d")).unwrap();
    let (d, _) = t.lookup(&v1, "d").unwrap();
    std::fs::remove_dir(t.data().join("d")).unwrap();
    std::os::unix::fs::symlink("/", t.data().join("d")).unwrap();
    assert_eq!(t.lookup(&d, "etc"), Err(STALE));
    assert_eq!(t.create(&d, "x", 0o644), Err(STALE));
    // Changing a symlink's mode leaves its target alone.
    let target = t.data().join("target");
    std::fs::write(&target, b"").unwrap();
    std::os::unix::fs::symlink(&target, t.data().join("l")).unwrap();
    let (l, _) = t.lookup(&v1, "l").unwrap();
    use std::os::unix::fs::PermissionsExt;
    let before = std::fs::metadata(&target).unwrap().permissions().mode();
    assert_eq!(
        t.setattr(&l, |w| {
            sattr(w, Some(0o777), None);
            w.bool(false);
        }),
        OK
    );
    assert_eq!(
        std::fs::metadata(&target).unwrap().permissions().mode(),
        before
    );
    // Names with slashes never reach the filesystem.
    assert_eq!(t.lookup(&v1, "../../.."), Err(NOENT));
    assert_eq!(t.create(&v1, "a/b", 0o644), Err(INVAL));
}

#[test]
fn virtual_directories_are_read_only() {
    let mut t = T::new("ro");
    let root = t.root();
    let vols = t.path("docker/volumes").unwrap();
    assert_eq!(t.create(&vols, "new", 0o644), Err(ACCES));
    assert_eq!(t.mkdir(&root, "x"), Err(ACCES));
    assert_eq!(t.remove(&vols, "v1", true), Err(ACCES));
    let v1 = t.path("docker/volumes/v1").unwrap();
    assert_eq!(t.rename(&vols, "v1", &v1, "x"), Err(ACCES));
    use access::*;
    let all = READ | LOOKUP | MODIFY | EXTEND | DELETE | EXECUTE;
    assert_eq!(t.access(&vols, all), Ok(READ | LOOKUP | EXECUTE));
    assert_eq!(t.access(&v1, all), Ok(all));
    let f = t.create(&v1, "f", 0o644).unwrap();
    assert_eq!(t.access(&f, all), Ok(READ | MODIFY | EXTEND | DELETE));
    let x = t.create(&v1, "x", 0o755).unwrap();
    assert_eq!(t.access(&x, all), Ok(all & !LOOKUP));
    assert_eq!(t.read(&vols, 0, 1), Err(ISDIR));
    assert_eq!(t.write(&vols, 0, b"x"), Err(ISDIR));
}

#[test]
fn setattr_truncates_chmods_and_sets_times() {
    let mut t = T::new("setattr");
    let v1 = t.path("docker/volumes/v1").unwrap();
    let f = t.create(&v1, "f", 0o644).unwrap();
    t.write(&f, 0, b"0123456789").unwrap();
    let s = t.setattr(&f, |w| {
        sattr(w, Some(0o600), Some(4));
        w.bool(false);
    });
    assert_eq!(s, OK);
    let a = t.getattr(&f).unwrap();
    assert_eq!((a.mode, a.size), (0o600, 4));
    let s = t.setattr(&f, |w| {
        w.bool(false).bool(false).bool(false).bool(false);
        w.u32(2).u32(1_000_000).u32(7).u32(2).u32(2_000_000).u32(9);
        w.bool(false);
    });
    assert_eq!(s, OK);
    let a = t.getattr(&f).unwrap();
    assert_eq!((a.atime, a.mtime), ((1_000_000, 7), (2_000_000, 9)));
    // A guard that does not match the file's ctime.
    let s = t.setattr(&f, |w| {
        sattr(w, Some(0o644), None);
        w.bool(true).u32(1).u32(2);
    });
    assert_eq!(s, NOT_SYNC);
    let d = t.mkdir(&v1, "d").unwrap();
    let s = t.setattr(&d, |w| {
        sattr(w, None, Some(0));
        w.bool(false);
    });
    assert_eq!(s, ISDIR);
    let vols = t.path("docker/volumes").unwrap();
    let s = t.setattr(&vols, |w| {
        sattr(w, Some(0o777), None);
        w.bool(false);
    });
    assert_eq!(s, ACCES);
}

#[test]
fn exclusive_create_survives_retransmission() {
    let mut t = T::new("excl");
    let v1 = t.path("docker/volumes/v1").unwrap();
    let verf = [0, 0, 0, 42, 0, 0, 0, 43];
    let a = t.create_excl(&v1, "e", verf).unwrap();
    assert_eq!(t.create_excl(&v1, "e", verf).unwrap(), a);
    assert_eq!(t.create_excl(&v1, "e", [9; 8]), Err(EXIST));
}

#[test]
fn pages_large_directories_without_losing_entries() {
    let mut t = T::new("page");
    for i in 0..300 {
        std::fs::write(t.data().join(format!("file-{i:03}")), b"").unwrap();
    }
    let v1 = t.path("docker/volumes/v1").unwrap();
    for plus in [true, false] {
        let list = t.list(&v1, 4096, plus).unwrap();
        assert_eq!(list.len(), 302);
        let mut names: Vec<_> = list.iter().map(|e| e.name.clone()).collect();
        names.sort();
        names.dedup();
        assert_eq!(names.len(), 302);
        // File ids are unique and match GETATTR.
        let mut ids: Vec<_> = list.iter().map(|e| e.fileid).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 302);
        if plus {
            let e = list.iter().find(|e| e.name == b"file-123").unwrap();
            let fh = e.fh.clone().unwrap();
            assert_eq!(t.getattr(&fh).unwrap(), e.attr.unwrap());
            assert_eq!(e.fileid, e.attr.unwrap().fileid);
        }
    }
    // A reply too small for even one entry.
    assert_eq!(t.list(&v1, 100, true).err(), Some(TOOSMALL));
}

#[test]
fn maps_root_ownership_to_the_caller() {
    let st = Stat {
        uid: 0,
        gid: 0,
        mode: libc::S_IFREG as u32 | 0o644,
        ..Stat::default()
    };
    let a = fattr(&st, 99, CRED);
    assert_eq!((a.uid, a.gid, a.fileid, a.ftype), (501, 20, 99, 1));
    let st = Stat {
        uid: 999,
        gid: 999,
        ..st
    };
    assert_eq!(fattr(&st, 99, CRED).uid, 999);
    assert_eq!((stored(501, 501), stored(999, 501)), (0, 999));
}

#[test]
fn answers_rpc_errors() {
    let mut t = T::new("rpcerr");
    assert_eq!(t.getattr(&[1, 2, 3]), Err(BADHANDLE));
    assert_eq!(t.raw(100_000, 2, 0, &[]).0, rpc::PROG_UNAVAIL);
    let (stat, res) = t.raw(PROG_NFS, 2, 0, &[]);
    assert_eq!(stat, rpc::PROG_MISMATCH);
    assert_eq!(res, [0, 0, 0, 3, 0, 0, 0, 3]);
    assert_eq!(t.raw(PROG_NFS, 3, 99, &[]).0, rpc::PROC_UNAVAIL);
    assert_eq!(t.raw(PROG_NFS, 3, 1, &[0, 0]).0, rpc::GARBAGE_ARGS);
    assert_eq!(t.raw(PROG_NFS, 3, 0, &[]), (rpc::SUCCESS, vec![]));
    // FSINFO advertises 1 MiB transfers.
    let root = t.root();
    let res = t.nfs(19, |w| {
        w.opaque(&root);
    });
    let mut r = Reader::new(&res);
    assert_eq!(r.u32(), Ok(OK));
    get_post_op(&mut r);
    assert_eq!(r.u32(), Ok(MAX_IO));
}

#[test]
fn serves_pipelined_requests_over_a_socket() {
    use std::io::Write;
    use std::os::unix::net::UnixStream;
    let t = T::new("pipe");
    std::fs::write(t.data().join("big"), vec![7u8; 3 << 20]).unwrap();
    let dir = t.dir.clone();
    let server = std::sync::Arc::new(Server::new(Layout {
        volumes: dir.join("volumes"),
        containers: Box::new(crate::layout::NoContainers),
        machines: dir.join("machines"),
    }));
    let (mut client, srv) = UnixStream::pair().unwrap();
    let h = std::thread::spawn(move || crate::serve(server, srv));
    let call = |xid: u32, prog: u32, proc_: u32, args: &[u8]| {
        let mut rec = rpc::call(xid, prog, 3, proc_, CRED, args);
        let len = (rec.len() - 4) as u32 | 1 << 31;
        rec[..4].copy_from_slice(&len.to_be_bytes());
        rec
    };
    let mut w = Writer::default();
    w.opaque(b"/docker/volumes/v1");
    client.write_all(&call(1, PROG_MOUNT, 1, &w.buf)).unwrap();
    let mut buf = Vec::new();
    let mut reader = std::io::BufReader::new(client.try_clone().unwrap());
    rpc::read_record(&mut reader, &mut buf).unwrap().unwrap();
    let (_, _, mut r) = rpc::parse_reply(&buf).unwrap();
    assert_eq!(r.u32(), Ok(0));
    let v1 = r.opaque(64).unwrap().to_vec();
    let mut w = Writer::default();
    w.opaque(&v1).opaque(b"big");
    client.write_all(&call(2, PROG_NFS, 3, &w.buf)).unwrap();
    rpc::read_record(&mut reader, &mut buf).unwrap().unwrap();
    let (_, _, mut r) = rpc::parse_reply(&buf).unwrap();
    assert_eq!(r.u32(), Ok(OK));
    let big = r.opaque(64).unwrap().to_vec();
    // Three 1 MiB reads in flight at once; replies may come in any order.
    for i in 0..3u32 {
        let mut w = Writer::default();
        w.opaque(&big).u64(u64::from(i) << 20).u32(MAX_IO);
        client
            .write_all(&call(10 + i, PROG_NFS, 6, &w.buf))
            .unwrap();
    }
    let mut seen = Vec::new();
    for _ in 0..3 {
        rpc::read_record(&mut reader, &mut buf).unwrap().unwrap();
        let (xid, _, mut r) = rpc::parse_reply(&buf).unwrap();
        assert_eq!(r.u32(), Ok(OK));
        get_post_op(&mut r);
        assert_eq!(r.u32(), Ok(MAX_IO));
        assert_eq!(r.bool(), Ok(xid == 12));
        assert!(r.opaque(MAX_IO as usize).unwrap().iter().all(|&b| b == 7));
        seen.push(xid);
    }
    seen.sort();
    assert_eq!(seen, [10, 11, 12]);
    drop(reader);
    client.shutdown(std::net::Shutdown::Both).unwrap();
    h.join().unwrap().unwrap();
}

const MARKERS: [&str; 3] = [
    ".metadata_never_index",
    ".metadata_never_index_unless_rootfs",
    ".metadata-never-index",
];

#[test]
fn root_lists_spotlight_markers_as_empty_files() {
    let mut t = T::new("noindex-list");
    let root = t.root();
    let want = [".", "..", "docker", "machines"]
        .into_iter()
        .chain(MARKERS)
        .collect::<Vec<_>>();
    // One page, and pages of an entry or two that resume at each cookie.
    for (plus, max) in [(true, 32768), (false, 32768), (true, 320), (false, 200)] {
        let list = t.list(&root, max, plus).unwrap();
        let names: Vec<_> = list
            .iter()
            .map(|e| String::from_utf8(e.name.clone()).unwrap())
            .collect();
        assert_eq!(names, want, "plus={plus} max={max}");
        let cookies: Vec<_> = list.iter().map(|e| e.cookie).collect();
        assert_eq!(cookies, (1..=want.len() as u64).collect::<Vec<_>>());
        for (e, id) in list[4..].iter().zip(6..) {
            assert_eq!(e.fileid, id);
            if plus {
                let a = e.attr.unwrap();
                assert_eq!(t.getattr(e.fh.as_ref().unwrap()).unwrap(), a);
            } else {
                assert!(e.fh.is_none());
            }
        }
    }
    // The directories keep their ids and handles.
    for (dir, id) in [("docker", 2), ("machines", 5)] {
        let fh = t.path(dir).unwrap();
        assert_eq!(t.getattr(&fh).unwrap().fileid, id);
    }
    assert_eq!(t.getattr(&root).unwrap().nlink, 4);
}

#[test]
fn spotlight_markers_look_up_and_read_empty() {
    let mut t = T::new("noindex-read");
    let root = t.root();
    let started = t.s.started;
    for (name, id) in MARKERS.into_iter().zip(6u64..) {
        let (fh, a) = t.lookup(&root, name).unwrap();
        assert_eq!(
            (a.ftype, a.mode, a.nlink, a.uid, a.gid, a.size, a.used),
            (1, 0o444, 1, 0, 0, 0, 0)
        );
        assert_eq!(
            (a.fileid, a.mtime, a.ctime, a.atime),
            (id, started, started, started)
        );
        assert_eq!(t.getattr(&fh).unwrap(), a);
        // Fixed handles, like the virtual directories': instance 0.
        assert_eq!(&fh[..8], &[0; 8]);
        assert_eq!(&fh[8..], &id.to_be_bytes());
        let other = Server::new(Layout {
            volumes: t.dir.join("volumes"),
            containers: Box::new(FixedContainers(Vec::new())),
            machines: t.dir.join("machines"),
        });
        assert_eq!(other.node(&fh), Ok(id));
        use access::*;
        let all = READ | LOOKUP | MODIFY | EXTEND | DELETE | EXECUTE;
        assert_eq!(t.access(&fh, all), Ok(READ));
        assert_eq!(t.read(&fh, 0, 4096).unwrap(), (Vec::new(), true));
        assert_eq!(t.read(&fh, 1 << 20, 10).unwrap(), (Vec::new(), true));
        assert_eq!(t.readlink(&fh), Err(INVAL));
        // A file, not a directory.
        assert_eq!(t.lookup(&fh, "x"), Err(NOTDIR));
        assert_eq!(t.list(&fh, 32768, true).err(), Some(NOTDIR));
        assert_eq!(t.lookup(&fh, "..").unwrap().0, root);
        assert_eq!(t.mnt(&format!("/{name}")), Err(NOTDIR));
    }
    // Only at the root.
    let docker = t.path("docker").unwrap();
    assert_eq!(t.lookup(&docker, MARKERS[0]), Err(NOENT));
}

#[test]
fn spotlight_markers_cannot_be_changed() {
    let mut t = T::new("noindex-ro");
    let root = t.root();
    let v1 = t.path("docker/volumes/v1").unwrap();
    let f = t.create(&v1, "f", 0o644).unwrap();
    for name in MARKERS {
        let (fh, before) = t.lookup(&root, name).unwrap();
        for (mode, size) in [(Some(0o666), None), (None, Some(0)), (None, Some(10))] {
            let s = t.setattr(&fh, |w| {
                sattr(w, mode, size);
                w.bool(false);
            });
            assert_eq!(s, ACCES);
        }
        assert_eq!(t.write(&fh, 0, b"x"), Err(ACCES));
        assert_eq!(t.remove(&root, name, false), Err(ACCES));
        assert_eq!(t.remove(&root, name, true), Err(ACCES));
        assert_eq!(t.rename(&root, name, &v1, "away"), Err(ACCES));
        assert_eq!(t.rename(&v1, "f", &root, name), Err(ACCES));
        // Taken names, whatever would be made there.
        assert_eq!(t.create(&root, name, 0o644), Err(EXIST));
        assert_eq!(t.create_excl(&root, name, [1; 8]), Err(EXIST));
        let res = t.nfs(8, |w| {
            w.opaque(&root).opaque(name.as_bytes()).u32(0);
            sattr(w, None, Some(0));
        });
        assert_eq!(T::new_reply(&res), Err(ACCES));
        assert_eq!(t.mkdir(&root, name), Err(EXIST));
        assert_eq!(t.symlink(&root, name, "x"), Err(EXIST));
        assert_eq!(t.link(&f, &root, name), Err(EXIST));
        assert_eq!(t.link(&fh, &v1, "copy"), Err(ACCES));
        // Not a directory to make things in.
        assert_eq!(t.create(&fh, "x", 0o644), Err(NOTDIR));
        assert_eq!(t.mkdir(&fh, "x"), Err(NOTDIR));
        assert_eq!(t.remove(&fh, "x", false), Err(NOTDIR));
        assert_eq!(t.rename(&v1, "f", &fh, "x"), Err(NOTDIR));
        assert_eq!(t.getattr(&fh).unwrap(), before);
    }
    assert!(t.data().join("f").is_file());
    assert!(!t.data().join("away").exists());
    assert!(!t.data().join("copy").exists());
}

#[test]
fn remove_rmdir_and_rename_reject_paths_with_slashes() {
    let mut t = T::new("path-escape");
    let v1 = t.path("docker/volumes/v1").unwrap();
    let outside = t.dir.join("outside.txt");
    std::fs::write(&outside, b"keep").unwrap();
    std::fs::create_dir(t.data().join("a")).unwrap();
    std::fs::write(t.data().join("a/b"), b"keep").unwrap();
    t.create(&v1, "ok", 0o644).unwrap();

    for bad in [
        "/etc/passwd",
        "../foo",
        "../../../outside.txt",
        "a/b",
        "",
        ".",
        "..",
        outside.to_str().unwrap(),
    ] {
        assert_eq!(t.remove(&v1, bad, false), Err(INVAL), "REMOVE {bad:?}");
        assert_eq!(t.remove(&v1, bad, true), Err(INVAL), "RMDIR {bad:?}");
        assert_eq!(
            t.rename(&v1, bad, &v1, "dst"),
            Err(INVAL),
            "RENAME from {bad:?}"
        );
        assert_eq!(
            t.rename(&v1, "ok", &v1, bad),
            Err(INVAL),
            "RENAME to {bad:?}"
        );
    }
    assert!(outside.is_file());
    assert!(t.data().join("a/b").is_file());
}

#[test]
fn setattr_truncate_fbig_and_write_count_and_unchecked_symlink() {
    let mut t = T::new("setattr-write-create");
    let v1 = t.path("docker/volumes/v1").unwrap();
    let f = t.create(&v1, "f", 0o644).unwrap();

    // SETATTR truncate > i64::MAX returns FBIG.
    let s = t.setattr(&f, |w| {
        sattr(w, None, Some(i64::MAX as u64 + 1));
        w.bool(false);
    });
    assert_eq!(s, FBIG);

    // WRITE with count > data.len() returns INVAL.
    let res = t.nfs(7, |w| {
        w.opaque(&f).u64(0).u32(10).u32(0).opaque(b"short");
    });
    let mut r = Reader::new(&res);
    assert_eq!(r.u32(), Ok(INVAL));

    // WRITE with count < data.len() writes only count bytes.
    let res = t.nfs(7, |w| {
        w.opaque(&f).u64(0).u32(3).u32(0).opaque(b"abcdef");
    });
    let mut r = Reader::new(&res);
    assert_eq!(r.u32(), Ok(OK));
    skip_wcc(&mut r);
    assert_eq!(r.u32(), Ok(3));
    assert_eq!(t.read(&f, 0, 16).unwrap(), (b"abc".to_vec(), true));

    // UNCHECKED create over an existing symlink returns EXIST, not STALE.
    t.symlink(&v1, "sym", "f").unwrap();
    let res = t.nfs(8, |w| {
        w.opaque(&v1).opaque(b"sym").u32(0);
        sattr(w, Some(0o644), None);
    });
    assert_eq!(T::new_reply(&res), Err(EXIST));
}

#[test]
fn readdir_bad_cookie_and_clamped_max() {
    let mut t = T::new("readdir-cookie");
    let v1 = t.path("docker/volumes/v1").unwrap();
    for i in 0..5 {
        t.create(&v1, &format!("f{i}"), 0o644).unwrap();
    }

    // First page gets a valid verifier.
    let res = t.nfs(16, |w| {
        w.opaque(&v1).u64(0).fixed(&[0; 8]).u32(4096);
    });
    let mut r = Reader::new(&res);
    assert_eq!(r.u32(), Ok(OK));
    get_post_op(&mut r);
    let verf: [u8; 8] = r.fixed(8).unwrap().try_into().unwrap();

    // Unknown verifier with non-zero cookie -> BAD_COOKIE.
    for proc_ in [16u32, 17] {
        let res = t.nfs(proc_, |w| {
            w.opaque(&v1).u64(1).fixed(&[0xaa; 8]);
            if proc_ == 17 {
                w.u32(4096);
            }
            w.u32(4096);
        });
        let mut r = Reader::new(&res);
        assert_eq!(r.u32(), Ok(BAD_COOKIE));
    }

    // Valid verifier with out-of-range cookie -> BAD_COOKIE.
    let res = t.nfs(16, |w| {
        w.opaque(&v1).u64(9999).fixed(&verf).u32(4096);
    });
    let mut r = Reader::new(&res);
    assert_eq!(r.u32(), Ok(BAD_COOKIE));

    // Huge max (u32::MAX) is clamped to MAX_IO so replies never overflow record marks.
    let mut big = T::new("readdir-max");
    let bv1 = big.path("docker/volumes/v1").unwrap();
    let pad = "a".repeat(200);
    for i in 0..3200 {
        std::fs::write(big.data().join(format!("{i:04}-{pad}")), b"").unwrap();
    }
    let res = big.nfs(17, |w| {
        w.opaque(&bv1)
            .u64(0)
            .fixed(&[0; 8])
            .u32(u32::MAX)
            .u32(u32::MAX);
    });
    assert!(
        res.len() <= MAX_IO as usize,
        "reply len {} exceeded MAX_IO",
        res.len()
    );
    let mut r = Reader::new(&res);
    assert_eq!(r.u32(), Ok(OK));
    get_post_op(&mut r);
    let _verf = r.fixed(8).unwrap();
    let mut count = 0;
    while r.bool().unwrap() {
        r.u64().unwrap();
        r.opaque(255).unwrap();
        r.u64().unwrap();
        get_post_op(&mut r);
        if r.bool().unwrap() {
            r.opaque(64).unwrap();
        }
        count += 1;
    }
    let eof = r.bool().unwrap();
    assert!(count > 0);
    assert!(
        !eof,
        "3200 long entries must be paged when max is clamped to MAX_IO"
    );
}

#[test]
fn readdir_returns_stale_when_dir_evicted_mid_listing() {
    let mut t = T::new("readdir-stale");
    let v1 = t.path("docker/volumes/v1").unwrap();
    let d = t.mkdir(&v1, "sub").unwrap();
    t.create(&d, "a", 0o644).unwrap();
    let dir_id = t.s.node(&d).unwrap();
    // Simulate `dir_id` being evicted from `nodes` while holding an open fd.
    let real_fd = t.s.real(dir_id).unwrap().loc.dir().unwrap();
    t.s.nodes.lock().unwrap().forget(dir_id);
    assert_eq!(t.s.entry(dir_id, Some(&real_fd), b"a", CRED), Err(STALE));
    assert_eq!(
        t.s.entry(dir_id, Some(&real_fd), b"missing", CRED),
        Ok(None)
    );
}
