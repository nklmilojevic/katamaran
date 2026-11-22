//! Host changes to the shares, dropped from the guest's caches, so the guest
//! can cache entries and attributes for long and still see
//! every change made on the Mac.
//!
//! One FSEvents stream covers every share. For each changed path this looks
//! up the path's directory and the path itself in the share's inode table:
//! the guest can only cache what it has looked up and not forgotten, so a
//! change to anything else costs the guest nothing. For what it does hold,
//! it sends the guest kernel (through the agent, to `/dev/kat_fsnotify`;
//! guest/kernel/patches/0004) the FUSE node ids to drop:
//!
//! - a node at a changed path loses its attributes and page cache;
//! - a name whose object may have changed (created, removed, replaced,
//!   renamed, or not the object the guest found there) is expired in its
//!   directory, and the directory's listing with it;
//! - after lost events, every node of the share.
//!
//! Everything is dropped lazily: the guest asks the host again only for
//! what it uses next. Each batch is looked at once more [`RECHECK`] later:
//! a lookup or directory listing that read the host before a change can
//! reach the inode table only after the first look. A change the guest made itself comes back from
//! FSEvents too; it costs the guest one more attribute fetch of that file,
//! and the name keeps its entry when the guest holds the same object by it.
//! (The host cannot tell from what it last sent whether the guest kept it:
//! the guest drops a reply that crossed an invalidation.)

use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::os::unix::fs::MetadataExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use kat_proto::fscache::{Inval, encode, write_frame};
use krun::{CachedNode, GuestCache, GuestCacheView};

use crate::fsevents::{self, Stream, flag};

/// How long the guest caches entries and attributes of a share whose host
/// changes are invalidated. Changes made on the Mac reach the guest through
/// FSEvents within tens of milliseconds, so this only bounds how long a
/// missed event could leave something stale.
pub const ENTRY_TIMEOUT: Duration = Duration::from_secs(3600);
pub const ATTR_TIMEOUT: Duration = Duration::from_secs(3600);
/// Failed lookups: a name the Mac creates expires its cached absence.
pub const NEGATIVE_TIMEOUT: Duration = Duration::from_secs(3600);

/// How long the guest may cache a share's entries, attributes and failed
/// lookups.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Timeouts {
    pub entry: Duration,
    pub attr: Duration,
    pub negative: Duration,
}

impl Timeouts {
    /// The long timeouts, unless KATAMARAN_FS_CACHE=0 turns invalidation off
    /// (and leaves libkrun's short ones). KATAMARAN_FS_TIMEOUT and
    /// KATAMARAN_FS_NEGATIVE (seconds) override them, for measuring.
    pub fn from_env() -> Option<Timeouts> {
        Self::parse(|k| std::env::var(k).ok())
    }

    fn parse(var: impl Fn(&str) -> Option<String>) -> Option<Timeouts> {
        if var("KATAMARAN_FS_CACHE").as_deref() == Some("0") {
            return None;
        }
        let secs = |k: &str| {
            var(k)
                .and_then(|v| v.parse::<f64>().ok())
                .and_then(|v| Duration::try_from_secs_f64(v).ok())
        };
        let timeout = secs("KATAMARAN_FS_TIMEOUT");
        Some(Timeouts {
            entry: timeout.unwrap_or(ENTRY_TIMEOUT),
            attr: timeout.unwrap_or(ATTR_TIMEOUT),
            negative: secs("KATAMARAN_FS_NEGATIVE").unwrap_or(NEGATIVE_TIMEOUT),
        })
    }
}

/// The socket the agent's connection to [`kat_proto::FSCACHE_PORT`] arrives
/// on, next to the control socket.
pub fn listen(control_socket: &Path) -> io::Result<(PathBuf, UnixListener)> {
    let path = control_socket.with_file_name("fscache.sock");
    let _ = std::fs::remove_file(&path);
    let listener = UnixListener::bind(&path)?;
    Ok((path, listener))
}

/// FSEvents' latency: changes within it arrive together.
const LATENCY: f64 = 0.01;
/// When a batch is looked at again. A directory listing reads the host's
/// attributes for all its entries at once and hands them out over its
/// following requests, so this covers listings that take that long.
pub const RECHECK: Duration = Duration::from_secs(2);
/// `kFSEventStreamEventFlagItemIsHardlink`: a name now links an existing object.
const ITEM_IS_HARDLINK: u32 = 0x10_0000;
/// A name that may now hold another object than the guest found there.
const ENTRY_FLAGS: u32 = flag::ITEM_REMOVED | flag::ITEM_RENAMED | ITEM_IS_HARDLINK;

/// The inodes the guest holds, by host identity.
pub trait Nodes {
    fn get(&self, dev: i32, ino: u64) -> Option<CachedNode>;
    fn all(&self) -> Vec<CachedNode>;
}

impl Nodes for GuestCacheView {
    fn get(&self, dev: i32, ino: u64) -> Option<CachedNode> {
        GuestCacheView::get(self, dev, ino)
    }

    fn all(&self) -> Vec<CachedNode> {
        GuestCacheView::all(self)
    }
}

fn node_at(nodes: &impl Nodes, path: &Path) -> Option<CachedNode> {
    let m = std::fs::symlink_metadata(path).ok()?;
    nodes.get(m.dev() as i32, m.ino())
}

/// What the guest must drop for a batch of FSEvents, for the share at `root`
/// (as FSEvents spells it). Paths outside it are ignored.
pub fn plan(nodes: &impl Nodes, share: u32, root: &Path, events: &[fsevents::Event]) -> Vec<Inval> {
    let mut out = Vec::new();
    let mut sent = HashSet::new();
    let mut push = |i: Inval| {
        if sent.insert(i.clone()) {
            out.push(i);
        }
    };
    // Directory path -> the node id the guest holds it by, once per batch.
    let mut dirs: HashMap<&Path, Option<u64>> = HashMap::new();
    for (p, flags) in events {
        if flags & (flag::USER_DROPPED | flag::KERNEL_DROPPED) != 0 {
            return everything(nodes, share);
        }
        let path = Path::new(p);
        if flags & flag::RESCAN != 0
            && (p.is_empty() || path.starts_with(root) || root.starts_with(path))
        {
            return everything(nodes, share);
        }
        let Ok(rel) = path.strip_prefix(root) else {
            continue;
        };
        // The guest holds a name only in a directory it holds.
        let parent = match (rel.as_os_str().is_empty(), path.parent(), path.file_name()) {
            (true, _, _) => None,
            (false, Some(dir), Some(name)) => {
                let held = *dirs
                    .entry(dir)
                    .or_insert_with(|| node_at(nodes, dir).map(|n| n.nodeid));
                match held {
                    Some(id) => Some((id, name.as_encoded_bytes())),
                    None => continue,
                }
            }
            _ => continue,
        };
        let mut entry_changed = true;
        if let Some(node) = node_at(nodes, path) {
            push(Inval::Inode {
                share,
                node: node.nodeid,
                data: !node.dir,
            });
            let anchored = match (&parent, &node.anchor) {
                (None, _) => true,
                (Some((dir, name)), Some((adir, aname))) => {
                    dir == adir && *name == aname.as_bytes()
                }
                (Some(_), None) => false,
            };
            entry_changed = !anchored || flags & ENTRY_FLAGS != 0;
        }
        if entry_changed && let Some((dir, name)) = parent {
            push(Inval::Entry {
                share,
                dir,
                name: name.to_vec(),
            });
        }
    }
    out
}

/// Everything the guest holds of a share, after events were lost.
pub fn everything(nodes: &impl Nodes, share: u32) -> Vec<Inval> {
    nodes
        .all()
        .into_iter()
        .map(|n| {
            if n.dir {
                Inval::ExpireDir {
                    share,
                    dir: n.nodeid,
                }
            } else {
                Inval::Inode {
                    share,
                    node: n.nodeid,
                    data: true,
                }
            }
        })
        .collect()
}

/// A share whose host changes the guest hears of.
pub struct Share {
    /// The shared host directory.
    pub root: PathBuf,
    pub cache: GuestCache,
}

/// Bounded channel depth so a paused or stalled VM cannot grow memory without limit.
const QUEUE_BOUND: usize = 1024;
/// Write timeout on the guest socket so a stalled/paused guest does not wedge `serve`.
const GUEST_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

enum Input {
    Guest(UnixStream),
    Fs(Vec<fsevents::Event>),
    Overflow,
}

/// Watch every share and send what the guest must drop to the agent, which
/// connects through `listener`. Runs for the life of the process.
pub fn start(shares: Vec<Share>, listener: UnixListener) -> io::Result<()> {
    let (tx, rx) = mpsc::sync_channel(QUEUE_BOUND);
    let overflow = Arc::new(AtomicBool::new(false));
    let roots: Vec<String> = shares
        .iter()
        .map(|s| {
            std::fs::canonicalize(&s.root)
                .unwrap_or_else(|_| s.root.clone())
                .display()
                .to_string()
        })
        .collect();
    let fs_tx = Mutex::new(tx.clone());
    let fs_overflow = Arc::clone(&overflow);
    let stream = Stream::start(&roots, &[], LATENCY, move |events| {
        let tx = fs_tx.lock().unwrap_or_else(|e| e.into_inner());
        if let Err(TrySendError::Full(_)) = tx.try_send(Input::Fs(events)) {
            fs_overflow.store(true, Ordering::Release);
            let _ = tx.try_send(Input::Overflow);
        }
    })?;
    thread::Builder::new()
        .name("fscache accept".into())
        .spawn(move || accept(&listener, &tx))?;
    let roots = roots.into_iter().map(PathBuf::from).collect();
    thread::Builder::new()
        .name("fscache".into())
        .spawn(move || {
            let _stream = stream;
            serve(&rx, &overflow, &shares, roots);
        })?;
    Ok(())
}

fn accept(listener: &UnixListener, tx: &SyncSender<Input>) {
    for conn in listener.incoming() {
        match conn {
            Ok(c) => {
                if tx.send(Input::Guest(c)).is_err() {
                    return;
                }
            }
            Err(e) => log::warn!("fscache: accept: {e}"),
        }
    }
}

/// With KATAMARAN_FSCACHE_DEBUG set, a per-batch trace on stderr.
fn debug_on() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var_os("KATAMARAN_FSCACHE_DEBUG").is_some())
}

fn serve(rx: &Receiver<Input>, overflow: &AtomicBool, shares: &[Share], roots: Vec<PathBuf>) {
    serve_with_recheck(rx, overflow, &roots, |i| shares[i].cache.view(), RECHECK);
}

fn serve_with_recheck<N: Nodes>(
    rx: &Receiver<Input>,
    overflow: &AtomicBool,
    roots: &[PathBuf],
    view_of: impl Fn(usize) -> Option<N>,
    recheck: Duration,
) {
    let mut guest: Option<UnixStream> = None;
    let mut again: VecDeque<(Instant, Vec<fsevents::Event>)> = VecDeque::new();
    let mut rescan_again: Option<Instant> = None;
    loop {
        let next_deadline = match (again.front().map(|(at, _)| *at), rescan_again) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        let first = match next_deadline {
            Some(at) => match rx.recv_timeout(at.saturating_duration_since(Instant::now())) {
                Ok(i) => Some(i),
                Err(RecvTimeoutError::Timeout) => None,
                Err(RecvTimeoutError::Disconnected) => return,
            },
            None => match rx.recv() {
                Ok(i) => Some(i),
                Err(_) => return,
            },
        };
        let mut events = Vec::new();
        let mut rescan = false;
        let mut schedule_rescan_recheck = false;
        for input in first.into_iter().chain(rx.try_iter()) {
            match input {
                Input::Guest(conn) => {
                    let _ = conn.set_write_timeout(Some(GUEST_WRITE_TIMEOUT));
                    // Whatever the guest cached before it connected may be stale.
                    guest = Some(conn);
                    rescan = true;
                    schedule_rescan_recheck = true;
                }
                Input::Fs(e) => events.extend(e),
                Input::Overflow => {
                    rescan = true;
                    schedule_rescan_recheck = true;
                }
            }
        }
        if overflow.swap(false, Ordering::AcqRel) {
            rescan = true;
            schedule_rescan_recheck = true;
            again.clear();
        }
        let now = Instant::now();
        if !rescan && !events.is_empty() {
            if again.len() >= QUEUE_BOUND {
                again.clear();
                rescan = true;
                schedule_rescan_recheck = true;
            } else {
                again.push_back((now + recheck, events.clone()));
            }
        }
        if let Some(at) = rescan_again
            && at <= now
        {
            rescan_again = None;
            rescan = true;
        }
        if schedule_rescan_recheck {
            again.clear();
            rescan_again = Some(now + recheck);
        }
        while let Some((_, due)) = again.pop_front_if(|(at, _)| *at <= now) {
            events.extend(due);
        }
        let Some(conn) = guest.as_mut() else {
            again.clear();
            rescan_again = None;
            continue;
        };
        let t = Instant::now();
        let mut invals = Vec::new();
        for (i, root) in roots.iter().enumerate() {
            let Some(view) = view_of(i) else {
                continue;
            };
            if rescan {
                invals.extend(everything(&view, i as u32));
            } else {
                invals.extend(plan(&view, i as u32, root, &events));
            }
        }
        if debug_on() {
            eprintln!(
                "fscache: {} events -> {} invalidations in {:?}",
                events.len(),
                invals.len(),
                t.elapsed()
            );
            for e in events.iter().take(20) {
                eprintln!("fscache:   event {e:?}");
            }
            for i in invals.iter().take(20) {
                eprintln!("fscache:   inval {i:?}");
            }
        }
        for chunk in encode(&invals) {
            if let Err(e) = write_frame(conn, &chunk) {
                log::warn!("fscache: guest gone: {e}");
                guest = None;
                again.clear();
                rescan_again = None;
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::fs;
    use std::sync::Arc;

    use super::*;

    /// A fake inode table: host (dev, ino) -> node.
    #[derive(Default)]
    struct Table(Arc<Mutex<HashMap<(i32, u64), CachedNode>>>);

    impl Nodes for Table {
        fn get(&self, dev: i32, ino: u64) -> Option<CachedNode> {
            self.0.lock().unwrap().get(&(dev, ino)).cloned()
        }

        fn all(&self) -> Vec<CachedNode> {
            let mut v: Vec<_> = self.0.lock().unwrap().values().cloned().collect();
            v.sort_by_key(|n| n.nodeid);
            v
        }
    }

    impl Table {
        /// The guest looked up `path` (found in `parent` by `name`) as `node`.
        fn hold(&self, path: &Path, node: u64, anchor: Option<(u64, &str)>) {
            let m = fs::symlink_metadata(path).unwrap();
            self.0.lock().unwrap().insert(
                (m.dev() as i32, m.ino()),
                CachedNode {
                    nodeid: node,
                    dir: m.is_dir(),
                    anchor: anchor.map(|(p, n)| (p, CString::new(n).unwrap())),
                },
            );
        }
    }

    struct Tmp(PathBuf);

    impl Tmp {
        fn new(tag: &str) -> Tmp {
            let p = std::env::temp_dir().join(format!("kat-fscache-{tag}-{}", std::process::id()));
            let _ = fs::remove_dir_all(&p);
            fs::create_dir_all(&p).unwrap();
            Tmp(fs::canonicalize(&p).unwrap())
        }
    }

    impl Drop for Tmp {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn ev(root: &Path, rel: &str, f: u32) -> fsevents::Event {
        let p = if rel.is_empty() {
            root.to_path_buf()
        } else {
            root.join(rel)
        };
        (p.display().to_string(), f)
    }

    fn inode(node: u64, data: bool) -> Inval {
        Inval::Inode {
            share: 0,
            node,
            data,
        }
    }

    fn entry(dir: u64, name: &str) -> Inval {
        Inval::Entry {
            share: 0,
            dir,
            name: name.into(),
        }
    }

    /// Root held as node 1, "d" as 2 and "d/f" as 3.
    fn setup(tag: &str) -> (Tmp, Table) {
        let t = Tmp::new(tag);
        fs::create_dir(t.0.join("d")).unwrap();
        fs::write(t.0.join("d/f"), "one").unwrap();
        let table = Table::default();
        table.hold(&t.0, 1, None);
        table.hold(&t.0.join("d"), 2, Some((1, "d")));
        table.hold(&t.0.join("d/f"), 3, Some((2, "f")));
        (t, table)
    }

    #[test]
    fn a_modified_cached_file_loses_attributes_and_data() {
        let (t, nodes) = setup("modify");
        fs::write(t.0.join("d/f"), "two!").unwrap();
        let out = plan(&nodes, 0, &t.0, &[ev(&t.0, "d/f", 0x1000)]);
        assert_eq!(out, [inode(3, true)]);
    }

    #[test]
    fn what_the_guest_does_not_hold_costs_nothing() {
        let (t, nodes) = setup("unheld");
        // The guest's own create, echoed back: it holds the object by that
        // name, so only its attributes go.
        fs::write(t.0.join("d/mine"), "x").unwrap();
        nodes.hold(&t.0.join("d/mine"), 4, Some((2, "mine")));
        let out = plan(&nodes, 0, &t.0, &[ev(&t.0, "d/mine", 0x1100)]);
        assert_eq!(out, [inode(4, true)]);
        // Anything in a directory the guest does not hold costs nothing.
        fs::create_dir(t.0.join("other")).unwrap();
        fs::write(t.0.join("other/x"), "x").unwrap();
        assert!(plan(&nodes, 0, &t.0, &[ev(&t.0, "other/x", 0x100)]).is_empty());
        // Nor does anything outside the share.
        assert!(plan(&nodes, 0, &t.0.join("d"), &[ev(&t.0, "other/x", 0x100)]).is_empty());
    }

    #[test]
    fn creates_and_deletes_expire_the_name() {
        let (t, nodes) = setup("create");
        fs::write(t.0.join("d/new"), "").unwrap();
        let out = plan(&nodes, 0, &t.0, &[ev(&t.0, "d/new", 0x100)]);
        assert_eq!(out, [entry(2, "new")]);

        fs::remove_file(t.0.join("d/f")).unwrap();
        let out = plan(&nodes, 0, &t.0, &[ev(&t.0, "d/f", 0x200)]);
        assert_eq!(out, [entry(2, "f")]);
    }

    #[test]
    fn an_atomic_save_expires_the_name() {
        let (t, nodes) = setup("save");
        fs::write(t.0.join("d/.f.tmp"), "new content").unwrap();
        fs::rename(t.0.join("d/.f.tmp"), t.0.join("d/f")).unwrap();
        let out = plan(
            &nodes,
            0,
            &t.0,
            &[ev(&t.0, "d/.f.tmp", 0x900), ev(&t.0, "d/f", 0x800)],
        );
        assert_eq!(out, [entry(2, ".f.tmp"), entry(2, "f")]);
    }

    #[test]
    fn a_renamed_cached_object_is_dropped_under_both_names() {
        let (t, nodes) = setup("rename");
        fs::rename(t.0.join("d/f"), t.0.join("d/g")).unwrap();
        let out = plan(
            &nodes,
            0,
            &t.0,
            &[ev(&t.0, "d/f", 0x800), ev(&t.0, "d/g", 0x800)],
        );
        assert_eq!(out, [entry(2, "f"), inode(3, true), entry(2, "g")]);

        // Renamed back to the name the guest found it by, which the guest
        // may cache as missing meanwhile.
        fs::rename(t.0.join("d/g"), t.0.join("d/f")).unwrap();
        let out = plan(&nodes, 0, &t.0, &[ev(&t.0, "d/f", 0x800)]);
        assert_eq!(out, [inode(3, true), entry(2, "f")]);
    }

    #[test]
    fn a_renamed_directory_and_the_root() {
        let (t, nodes) = setup("dir");
        fs::rename(t.0.join("d"), t.0.join("e")).unwrap();
        let out = plan(
            &nodes,
            0,
            &t.0,
            &[ev(&t.0, "d", 0x20800), ev(&t.0, "e", 0x20800)],
        );
        assert_eq!(out, [entry(1, "d"), inode(2, false), entry(1, "e")]);
        // An event on the root itself.
        fs::set_permissions(&t.0, std::os::unix::fs::PermissionsExt::from_mode(0o700)).unwrap();
        let out = plan(&nodes, 0, &t.0, &[ev(&t.0, "", 0x400)]);
        assert_eq!(out, [inode(1, false)]);
    }

    #[test]
    fn lost_events_drop_everything() {
        let (t, nodes) = setup("lost");
        let all = [
            Inval::ExpireDir { share: 0, dir: 1 },
            Inval::ExpireDir { share: 0, dir: 2 },
            inode(3, true),
        ];
        let out = plan(
            &nodes,
            0,
            &t.0,
            &[
                ev(&t.0, "d/f", 0x1000),
                ev(&t.0, "d", flag::MUST_SCAN_SUB_DIRS),
            ],
        );
        assert_eq!(out, all);

        // USER_DROPPED and KERNEL_DROPPED arrive with "" or "/" (outside root).
        for path in ["", "/", "/some/other/path"] {
            assert_eq!(
                plan(&nodes, 0, &t.0, &[(path.into(), flag::USER_DROPPED)]),
                all,
                "USER_DROPPED on {path:?}"
            );
            assert_eq!(
                plan(&nodes, 0, &t.0, &[(path.into(), flag::KERNEL_DROPPED)]),
                all,
                "KERNEL_DROPPED on {path:?}"
            );
        }

        // MUST_SCAN_SUB_DIRS on an ancestor of root (or empty path) drops everything.
        let ancestor = t.0.parent().unwrap().display().to_string();
        assert_eq!(
            plan(&nodes, 0, &t.0, &[(ancestor, flag::MUST_SCAN_SUB_DIRS)]),
            all
        );
        assert_eq!(
            plan(&nodes, 0, &t.0, &[("".into(), flag::MUST_SCAN_SUB_DIRS)]),
            all
        );

        // MUST_SCAN_SUB_DIRS on an unrelated sibling does not drop this share.
        let sibling =
            t.0.with_file_name("unrelated-sibling")
                .display()
                .to_string();
        assert!(plan(&nodes, 0, &t.0, &[(sibling, flag::MUST_SCAN_SUB_DIRS)]).is_empty());
    }

    #[test]
    fn timeouts_come_from_the_environment() {
        let env = |pairs: &'static [(&'static str, &'static str)]| {
            move |k: &str| {
                pairs
                    .iter()
                    .find(|(n, _)| *n == k)
                    .map(|(_, v)| v.to_string())
            }
        };
        let long = Timeouts {
            entry: ENTRY_TIMEOUT,
            attr: ATTR_TIMEOUT,
            negative: NEGATIVE_TIMEOUT,
        };
        assert_eq!(Timeouts::parse(env(&[])), Some(long));
        assert_eq!(Timeouts::parse(env(&[("KATAMARAN_FS_CACHE", "0")])), None);
        assert_eq!(
            Timeouts::parse(env(&[
                ("KATAMARAN_FS_TIMEOUT", "5"),
                ("KATAMARAN_FS_NEGATIVE", "0")
            ])),
            Some(Timeouts {
                entry: Duration::from_secs(5),
                attr: Duration::from_secs(5),
                negative: Duration::ZERO,
            })
        );
        // Invalid float values fall back to defaults without panicking.
        for bad in ["-1", "NaN", "inf", "-inf", "1e400"] {
            assert_eq!(
                Timeouts::parse(|k| match k {
                    "KATAMARAN_FS_TIMEOUT" | "KATAMARAN_FS_NEGATIVE" => Some(bad.to_string()),
                    _ => None,
                }),
                Some(long),
                "bad value {bad:?}"
            );
        }
    }

    #[test]
    fn overflow_rescan_rechecks_after_delay_without_more_events() {
        let (t, nodes) = setup("overflow-recheck");
        let (mut guest_rx, host_tx) = UnixStream::pair().unwrap();
        guest_rx
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let (tx, rx) = mpsc::sync_channel(8);
        let overflow = Arc::new(AtomicBool::new(false));
        let root = t.0.clone();
        let nodes_bg = Table(Arc::clone(&nodes.0));
        let overflow_bg = Arc::clone(&overflow);
        let h = thread::spawn(move || {
            serve_with_recheck(
                &rx,
                &overflow_bg,
                &[root],
                |_| Some(Table(Arc::clone(&nodes_bg.0))),
                Duration::from_millis(40),
            );
        });

        let mut buf = Vec::new();
        // Connect guest: receives initial full invalidation and its delayed recheck.
        tx.send(Input::Guest(host_tx)).unwrap();
        assert!(
            kat_proto::fscache::read_frame(&mut guest_rx, &mut buf)
                .unwrap()
                .is_some()
        );
        assert_eq!(buf, encode(&everything(&nodes, 0))[0]);
        assert!(
            kat_proto::fscache::read_frame(&mut guest_rx, &mut buf)
                .unwrap()
                .is_some()
        );
        assert_eq!(buf, encode(&everything(&nodes, 0))[0]);

        // Trigger overflow with no subsequent FSEvents.
        overflow.store(true, Ordering::Release);
        tx.send(Input::Overflow).unwrap();
        assert!(
            kat_proto::fscache::read_frame(&mut guest_rx, &mut buf)
                .unwrap()
                .is_some()
        );
        assert_eq!(buf, encode(&everything(&nodes, 0))[0]);

        // A concurrent guest lookup populates a new node right after the first invalidation.
        fs::write(t.0.join("d/late"), "v").unwrap();
        nodes.hold(&t.0.join("d/late"), 4, Some((2, "late")));

        // Without any more events, the delayed overflow recheck invalidates everything including node 4.
        assert!(
            kat_proto::fscache::read_frame(&mut guest_rx, &mut buf)
                .unwrap()
                .is_some()
        );
        assert_eq!(buf, encode(&everything(&nodes, 0))[0]);

        // No further frame should be sent after the delayed recheck completes.
        guest_rx
            .set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        let err = kat_proto::fscache::read_frame(&mut guest_rx, &mut buf).unwrap_err();
        assert!(
            matches!(
                err.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
            ),
            "unexpected error: {err:?}"
        );

        drop(tx);
        h.join().unwrap();
    }
}
