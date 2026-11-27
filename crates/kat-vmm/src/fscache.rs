//! Host changes to the shares, dropped from the guest's caches, so the guest
//! can cache entries and attributes for long and still see
//! every change made on the Mac.
//!
//! One FSEvents stream covers every share, started lazily only once the guest
//! actually looks up or lists inside a share, and torn down again once all
//! non-root inodes are forgotten and the share stays idle. High-churn trees
//! such as `~/Library`, `~/.Trash`, and Katamaran's own state directory are
//! excluded at the FSEvents level unless the guest actually looks up inside
//! them (in which case the exclusion is dropped immediately and the stream
//! restarted with zero gap).
//!
//! For each delivered FSEvent, an in-memory directory index built from
//! `GuestCache` filters out paths whose parent directory the guest does not
//! hold before making any `lstat` syscall, avoiding host metadata probes and
//! macOS Sandbox/TCC denials on unrelated directories. For what the guest does
//! hold, it sends the guest kernel (through the agent, to `/dev/kat_fsnotify`;
//! guest/kernel/patches/0005) the FUSE node ids to drop:
//!
//! - a node at a changed path loses its attributes and page cache;
//! - a name whose object may have changed (created, removed, replaced,
//!   renamed, or not the object the guest found there) is expired in its
//!   directory, and the directory's listing with it;
//! - after lost events affecting cached subtrees, every node of the share.
//!
//! Everything is dropped lazily: the guest asks the host again only for
//! what it uses next. Each batch is looked at once more [`RECHECK`] later if
//! the guest performed further lookups in the meantime: a lookup or directory
//! listing that read the host before a change can reach the inode table only
//! after the first look. A change the guest made itself comes back from
//! FSEvents too; it costs the guest one more attribute fetch of that file,
//! and the name keeps its entry when the guest holds the same object by it.
//! (The host cannot tell from what it last sent whether the guest kept it:
//! the guest drops a reply that crossed an invalidation.)

use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::os::unix::fs::MetadataExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Component, Path, PathBuf};
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
/// How long all shares must hold only their root inode with no activity before
/// the FSEvents stream is stopped.
const IDLE_STOP: Duration = Duration::from_secs(5);
/// `kFSEventStreamEventFlagItemIsHardlink`: a name now links an existing object.
const ITEM_IS_HARDLINK: u32 = 0x10_0000;
/// A name that may now hold another object than the guest found there.
const ENTRY_FLAGS: u32 = flag::ITEM_REMOVED | flag::ITEM_RENAMED | ITEM_IS_HARDLINK;

/// The inodes the guest holds, by host identity.
pub trait Nodes {
    fn get(&self, dev: i32, ino: u64) -> Option<CachedNode>;
    fn all(&self) -> Vec<CachedNode>;
    fn dirs(&self) -> Vec<CachedNode>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn activity(&self) -> u64;
    fn dir_seq(&self) -> u64;
}

impl Nodes for GuestCacheView {
    fn get(&self, dev: i32, ino: u64) -> Option<CachedNode> {
        GuestCacheView::get(self, dev, ino)
    }

    fn all(&self) -> Vec<CachedNode> {
        GuestCacheView::all(self)
    }

    fn dirs(&self) -> Vec<CachedNode> {
        GuestCacheView::dirs(self)
    }

    fn len(&self) -> usize {
        GuestCacheView::len(self)
    }

    fn activity(&self) -> u64 {
        GuestCacheView::activity(self)
    }

    fn dir_seq(&self) -> u64 {
        GuestCacheView::dir_seq(self)
    }
}

fn ascii_lower(bytes: &[u8]) -> Vec<u8> {
    bytes.iter().map(u8::to_ascii_lowercase).collect()
}

type AnchorKey = (u64, Vec<u8>);

/// In-memory index of the directory tree the guest currently holds in a share,
/// used to reject FSEvents for uncached paths without touching the host
/// filesystem.
#[derive(Default)]
pub struct DirIndex {
    dir_seq: Option<u64>,
    root_id: Option<u64>,
    dir_ids: HashSet<u64>,
    /// `nodeid -> Option<(parent_nodeid, ascii_lower(name))>` from `GuestCache`.
    anchors: HashMap<u64, Option<AnchorKey>>,
    /// `(parent_nodeid, ascii_lower(name)) -> child dir nodeids` from `GuestCache`.
    children: HashMap<AnchorKey, Vec<u64>>,
    /// Same, learned from host-side renames of cached directories before the
    /// guest re-looks them up and refreshes their anchor.
    aliases: HashMap<AnchorKey, Vec<u64>>,
    /// Directory nodeids whose cached children include a non-ASCII name;
    /// non-ASCII event components beneath them fall back to `lstat` so Unicode
    /// case-folding and NFC/NFD normalization never false-negative.
    non_ascii_parents: HashSet<u64>,
    /// True if any cached directory cannot be traced back to a single root.
    has_orphan: bool,
    /// Cached directories observed as renamed out of a cached parent and not
    /// yet seen at a new cached parent, along with their `GuestCache` anchor
    /// at that moment.
    moved_dirs: HashMap<u64, Option<AnchorKey>>,
}

impl DirIndex {
    /// Rebuild from `nodes` only when its directory sequence number has moved.
    pub fn refresh(&mut self, nodes: &impl Nodes) {
        let seq = nodes.dir_seq();
        if self.dir_seq == Some(seq) {
            return;
        }
        let dirs = nodes.dirs();
        self.dir_ids.clear();
        self.anchors.clear();
        self.children.clear();
        self.non_ascii_parents.clear();
        self.root_id = None;
        self.has_orphan = false;

        self.anchors.reserve(dirs.len());
        for n in &dirs {
            if !n.dir {
                continue;
            }
            self.dir_ids.insert(n.nodeid);
            match &n.anchor {
                None => {
                    if self.root_id.is_none() {
                        self.root_id = Some(n.nodeid);
                    } else {
                        self.has_orphan = true;
                    }
                    self.anchors.insert(n.nodeid, None);
                }
                Some((parent, name)) => {
                    let raw = name.as_bytes();
                    if !raw.is_ascii() {
                        self.non_ascii_parents.insert(*parent);
                    }
                    let lower = ascii_lower(raw);
                    self.children
                        .entry((*parent, lower.clone()))
                        .or_default()
                        .push(n.nodeid);
                    self.anchors.insert(n.nodeid, Some((*parent, lower)));
                }
            }
        }

        if let Some(root_id) = self.root_id {
            let max_depth = self.dir_ids.len();
            for &id in &self.dir_ids {
                let mut cur = id;
                let mut steps = 0;
                while cur != root_id {
                    steps += 1;
                    if steps > max_depth {
                        self.has_orphan = true;
                        break;
                    }
                    match self.anchors.get(&cur).and_then(|a| a.as_ref()) {
                        Some((parent, _)) if self.dir_ids.contains(parent) => cur = *parent,
                        _ => {
                            self.has_orphan = true;
                            break;
                        }
                    }
                }
                if self.has_orphan {
                    break;
                }
            }
        } else if !self.dir_ids.is_empty() {
            self.has_orphan = true;
        }

        let dir_ids = &self.dir_ids;
        let anchors = &self.anchors;
        self.aliases.retain(|(parent, name), ids| {
            if !dir_ids.contains(parent) {
                return false;
            }
            ids.retain(|id| {
                dir_ids.contains(id)
                    && anchors.get(id).and_then(|a| a.as_ref()) != Some(&(*parent, name.clone()))
            });
            !ids.is_empty()
        });
        for (parent, name) in self.aliases.keys() {
            if !name.is_ascii() {
                self.non_ascii_parents.insert(*parent);
            }
        }
        self.moved_dirs
            .retain(|id, old_anchor| dir_ids.contains(id) && anchors.get(id) == Some(old_anchor));
        self.dir_seq = Some(seq);
    }

    fn anchor_of(&self, id: u64) -> Option<AnchorKey> {
        self.anchors.get(&id).cloned().flatten()
    }

    fn root_id(&self) -> Option<u64> {
        self.root_id
    }

    /// True if `dir` could be a directory the guest currently holds in `root`.
    /// A `false` return guarantees the guest does not hold `dir`, so callers
    /// can skip `symlink_metadata(dir)` completely.
    pub fn may_hold_dir(&self, root: &Path, dir: &Path) -> bool {
        if self.has_orphan || !self.moved_dirs.is_empty() {
            return true;
        }
        let Some(root_id) = self.root_id else {
            return false;
        };
        let Ok(rel) = dir.strip_prefix(root) else {
            return false;
        };
        if rel.as_os_str().is_empty() {
            return true;
        }
        let mut cur = vec![root_id];
        for comp in rel.components() {
            match comp {
                Component::Normal(c) => {
                    let bytes = c.as_encoded_bytes();
                    if !bytes.is_ascii() && cur.iter().any(|id| self.non_ascii_parents.contains(id))
                    {
                        return true;
                    }
                    let key = ascii_lower(bytes);
                    let mut next = Vec::new();
                    for &parent in &cur {
                        if let Some(ids) = self.children.get(&(parent, key.clone())) {
                            for &id in ids {
                                if !next.contains(&id) {
                                    next.push(id);
                                }
                            }
                        }
                        if let Some(ids) = self.aliases.get(&(parent, key.clone())) {
                            for &id in ids {
                                if !next.contains(&id) {
                                    next.push(id);
                                }
                            }
                        }
                    }
                    if next.is_empty() {
                        return false;
                    }
                    cur = next;
                }
                Component::CurDir => {}
                Component::ParentDir | Component::RootDir | Component::Prefix(_) => return true,
            }
        }
        true
    }

    fn child_dirs(&self, parent: u64, name: &[u8]) -> Vec<u64> {
        let key = (parent, ascii_lower(name));
        let mut out = Vec::new();
        if let Some(ids) = self.children.get(&key) {
            out.extend_from_slice(ids);
        }
        if let Some(ids) = self.aliases.get(&key) {
            for &id in ids {
                if !out.contains(&id) {
                    out.push(id);
                }
            }
        }
        out
    }

    fn record_alias(&mut self, parent: u64, name: &[u8], nodeid: u64) {
        if !name.is_ascii() {
            self.non_ascii_parents.insert(parent);
        }
        let entry = self.aliases.entry((parent, ascii_lower(name))).or_default();
        if !entry.contains(&nodeid) {
            entry.push(nodeid);
        }
        self.moved_dirs.remove(&nodeid);
    }
}

fn node_at(nodes: &impl Nodes, path: &Path) -> Option<CachedNode> {
    let m = std::fs::symlink_metadata(path).ok()?;
    nodes.get(m.dev() as i32, m.ino())
}

/// What the guest must drop for a batch of FSEvents, for the share at `root`
/// (as FSEvents spells it). Paths outside it are ignored.
pub fn plan(nodes: &impl Nodes, share: u32, root: &Path, events: &[fsevents::Event]) -> Vec<Inval> {
    let mut index = DirIndex::default();
    plan_with_index(nodes, &mut index, share, root, events)
}

fn plan_with_index(
    nodes: &impl Nodes,
    index: &mut DirIndex,
    share: u32,
    root: &Path,
    events: &[fsevents::Event],
) -> Vec<Inval> {
    index.refresh(nodes);
    let mut out = Vec::new();
    let mut sent = HashSet::new();
    let mut push = |i: Inval| {
        if sent.insert(i.clone()) {
            out.push(i);
        }
    };
    // Directory path -> the node id the guest holds it by, once per batch.
    let mut dirs: HashMap<&Path, Option<u64>> = HashMap::new();
    let mut batch_moved_out: Vec<(u64, Option<AnchorKey>)> = Vec::new();
    let mut batch_relocated: HashSet<u64> = HashSet::new();
    for (p, flags) in events {
        if flags & (flag::USER_DROPPED | flag::KERNEL_DROPPED) != 0 {
            return everything(nodes, share);
        }
        let path = Path::new(p);
        if flags & flag::RESCAN != 0
            && (p.is_empty()
                || root.starts_with(path)
                || (path.starts_with(root) && index.may_hold_dir(root, path)))
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
                let held = *dirs.entry(dir).or_insert_with(|| {
                    if !index.may_hold_dir(root, dir) {
                        return None;
                    }
                    node_at(nodes, dir).map(|n| n.nodeid)
                });
                match held {
                    Some(id) => Some((id, name.as_encoded_bytes())),
                    None => continue,
                }
            }
            _ => continue,
        };
        let mut entry_changed = true;
        let node = node_at(nodes, path);
        if let Some(node) = &node {
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
            if node.dir
                && !anchored
                && let Some((dir, name)) = parent
            {
                index.record_alias(dir, name, node.nodeid);
                batch_relocated.insert(node.nodeid);
            }
            entry_changed = !anchored || flags & ENTRY_FLAGS != 0;
        }
        if let Some((dir, name)) = parent
            && flags & flag::ITEM_RENAMED != 0
        {
            for cid in index.child_dirs(dir, name) {
                if node.as_ref().is_none_or(|n| n.nodeid != cid) {
                    batch_moved_out.push((cid, index.anchor_of(cid)));
                }
            }
        }
        if entry_changed && let Some((dir, name)) = parent {
            push(Inval::Entry {
                share,
                dir,
                name: name.to_vec(),
            });
        }
    }
    for (cid, old_anchor) in batch_moved_out {
        if !batch_relocated.contains(&cid) {
            index.moved_dirs.insert(cid, old_anchor);
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

/// A candidate high-churn directory excluded from FSEvents until the guest
/// actually looks it up.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Exclusion {
    path: PathBuf,
    dev: i32,
    ino: u64,
}

/// Build the initial list of FSEvents exclusions strictly beneath `roots`
/// (high-churn macOS directories like `~/Library` and `~/.Trash`, plus
/// Katamaran's own state/disk directories). Exclusions are dropped dynamically
/// if the guest ever looks up inside one of them.
fn candidate_exclusions(
    roots: &[PathBuf],
    extra: &[PathBuf],
    home: Option<&Path>,
) -> Vec<Exclusion> {
    let home_canon = home.and_then(|h| std::fs::canonicalize(h).ok());
    let mut raw: Vec<PathBuf> = Vec::new();
    if let Some(h) = &home_canon {
        raw.push(h.join("Library"));
        raw.push(h.join(".Trash"));
    }
    for p in extra {
        if let Ok(canon) = std::fs::canonicalize(p)
            && home_canon.as_ref().is_none_or(|h| *h != canon)
        {
            raw.push(canon);
        }
    }

    let mut dirs: Vec<PathBuf> = raw
        .into_iter()
        .filter_map(|p| std::fs::canonicalize(p).ok())
        .filter(|p| {
            std::fs::symlink_metadata(p).is_ok_and(|m| m.is_dir())
                && roots.iter().any(|r| p.starts_with(r) && p != r)
        })
        .collect();
    // Shortest paths first so a parent exclusion subsumes its descendants.
    dirs.sort_by_key(|p| (p.as_os_str().len(), p.clone()));

    let mut out: Vec<Exclusion> = Vec::new();
    for path in dirs {
        if out.iter().any(|existing| {
            path.starts_with(&existing.path)
                || path
                    .as_os_str()
                    .as_encoded_bytes()
                    .eq_ignore_ascii_case(existing.path.as_os_str().as_encoded_bytes())
        }) {
            continue;
        }
        if let Ok(m) = std::fs::symlink_metadata(&path) {
            out.push(Exclusion {
                path,
                dev: m.dev() as i32,
                ino: m.ino(),
            });
            if out.len() >= fsevents::MAX_EXCLUSIONS {
                break;
            }
        }
    }
    out
}

/// Bounded channel depth so a paused or stalled VM cannot grow memory without limit.
const QUEUE_BOUND: usize = 1024;
/// Maximum total events retained across pending `RECHECK` batches before
/// collapsing to a full rescan.
const MAX_RECHECK_EVENTS: usize = 65_536;
/// Write timeout on the guest socket so a stalled/paused guest does not wedge `serve`.
const GUEST_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

enum Input {
    Guest(UnixStream),
    Fs(Vec<fsevents::Event>),
    Overflow,
    Wake,
}

trait CacheHooks {
    fn arm_activity(&self, share: usize);
    fn disarm_activity(&self, share: usize);
    fn arm_dir_change(&self, share: usize);
    fn disarm_dir_change(&self, share: usize);
}

struct RealHooks<'a>(&'a [Share]);

impl CacheHooks for RealHooks<'_> {
    fn arm_activity(&self, share: usize) {
        self.0[share].cache.arm_activity();
    }
    fn disarm_activity(&self, share: usize) {
        self.0[share].cache.disarm_activity();
    }
    fn arm_dir_change(&self, share: usize) {
        self.0[share].cache.arm_dir_change();
    }
    fn disarm_dir_change(&self, share: usize) {
        self.0[share].cache.disarm_dir_change();
    }
}

/// Watch every share lazily and send what the guest must drop to the agent,
/// which connects through `listener`. Runs for the life of the process.
pub fn start(
    shares: Vec<Share>,
    extra_exclusions: Vec<PathBuf>,
    listener: UnixListener,
) -> io::Result<()> {
    let (tx, rx) = mpsc::sync_channel(QUEUE_BOUND);
    let overflow = Arc::new(AtomicBool::new(false));
    let wake_pending = Arc::new(AtomicBool::new(false));
    let roots: Vec<PathBuf> = shares
        .iter()
        .map(|s| std::fs::canonicalize(&s.root).unwrap_or_else(|_| s.root.clone()))
        .collect();
    let home = std::env::var_os("HOME").map(PathBuf::from);
    let exclusions = candidate_exclusions(&roots, &extra_exclusions, home.as_deref());

    let wake_tx = Arc::new(Mutex::new(tx.clone()));
    for share in &shares {
        let tx = Arc::clone(&wake_tx);
        let pending = Arc::clone(&wake_pending);
        share.cache.set_notifier(move || {
            pending.store(true, Ordering::Release);
            let _ = tx
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .try_send(Input::Wake);
        });
    }

    let fs_tx = Arc::new(Mutex::new(tx.clone()));
    let fs_overflow = Arc::clone(&overflow);
    let start_stream = move |watch_roots: &[String], ex: &[String]| -> io::Result<Stream> {
        let tx = Arc::clone(&fs_tx);
        let ov = Arc::clone(&fs_overflow);
        Stream::start(watch_roots, ex, LATENCY, move |events| {
            let tx = tx.lock().unwrap_or_else(|e| e.into_inner());
            if let Err(TrySendError::Full(_)) = tx.try_send(Input::Fs(events)) {
                ov.store(true, Ordering::Release);
                let _ = tx.try_send(Input::Overflow);
            }
        })
    };

    thread::Builder::new()
        .name("fscache accept".into())
        .spawn(move || accept(&listener, &tx))?;
    thread::Builder::new()
        .name("fscache".into())
        .spawn(move || {
            let hooks = RealHooks(&shares);
            serve_with(
                &rx,
                &overflow,
                &wake_pending,
                &roots,
                &exclusions,
                |i| shares[i].cache.view(),
                &hooks,
                start_stream,
                RECHECK,
                IDLE_STOP,
            );
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

fn send_invals(conn: &mut UnixStream, invals: &[Inval]) -> bool {
    for chunk in encode(invals) {
        if let Err(e) = write_frame(conn, &chunk) {
            log::warn!("fscache: guest gone: {e}");
            return false;
        }
    }
    true
}

fn exclusion_accessed<N: Nodes>(
    views: &[Option<N>],
    dir_indices: &mut [DirIndex],
    roots: &[PathBuf],
    ex: &Exclusion,
) -> bool {
    for (i, view) in views.iter().enumerate() {
        let Some(v) = view else {
            continue;
        };
        if v.get(ex.dev, ex.ino).is_some() {
            return true;
        }
        dir_indices[i].refresh(v);
        if dir_indices[i].may_hold_dir(&roots[i], &ex.path) {
            return true;
        }
    }
    false
}

#[allow(clippy::too_many_arguments)]
fn serve_with<N: Nodes, S>(
    rx: &Receiver<Input>,
    overflow: &AtomicBool,
    wake_pending: &AtomicBool,
    roots: &[PathBuf],
    all_exclusions: &[Exclusion],
    view_of: impl Fn(usize) -> Option<N>,
    hooks: &impl CacheHooks,
    mut start_stream: impl FnMut(&[String], &[String]) -> io::Result<S>,
    recheck: Duration,
    idle_stop: Duration,
) {
    let root_strings: Vec<String> = roots.iter().map(|r| r.display().to_string()).collect();
    let n_shares = roots.len();
    let mut guest: Option<UnixStream> = None;
    let mut stream: Option<S> = None;
    let mut stream_retry_at: Option<Instant> = None;
    let mut active_exclusions: Vec<Exclusion> = Vec::new();
    let mut dir_indices: Vec<DirIndex> = (0..n_shares).map(|_| DirIndex::default()).collect();
    let mut last_activity: Vec<u64> = vec![0; n_shares];
    let mut last_dir_seq: Vec<u64> = vec![0; n_shares];
    let mut had_activity: Vec<bool> = vec![false; n_shares];
    let mut idle_since: Option<Instant> = None;
    let mut again: VecDeque<(Instant, Vec<u64>, Vec<fsevents::Event>)> = VecDeque::new();
    let mut again_events: usize = 0;
    let mut rescan_again: Option<Instant> = None;

    loop {
        let now = Instant::now();
        let idle_deadline = if stream.is_some() {
            Some(idle_since.map_or(now + idle_stop, |t| t + idle_stop))
        } else {
            stream_retry_at
        };
        let next_deadline = [
            again.front().map(|(at, _, _)| *at),
            rescan_again,
            idle_deadline,
        ]
        .into_iter()
        .flatten()
        .min();

        let first = if wake_pending.swap(false, Ordering::AcqRel) {
            None
        } else {
            match next_deadline {
                Some(at) => match rx.recv_timeout(at.saturating_duration_since(now)) {
                    Ok(i) => Some(i),
                    Err(RecvTimeoutError::Timeout) => None,
                    Err(RecvTimeoutError::Disconnected) => return,
                },
                None => match rx.recv() {
                    Ok(i) => Some(i),
                    Err(_) => return,
                },
            }
        };

        let mut events = Vec::new();
        let mut rescan = false;
        let mut schedule_rescan_recheck = false;
        let mut guest_connected = false;
        for input in first.into_iter().chain(rx.try_iter()) {
            match input {
                Input::Guest(conn) => {
                    let _ = conn.set_write_timeout(Some(GUEST_WRITE_TIMEOUT));
                    guest = Some(conn);
                    guest_connected = true;
                }
                Input::Fs(e) => events.extend(e),
                Input::Overflow => {
                    rescan = true;
                    schedule_rescan_recheck = true;
                }
                Input::Wake => {}
            }
        }
        wake_pending.store(false, Ordering::Release);
        if overflow.swap(false, Ordering::AcqRel) {
            rescan = true;
            schedule_rescan_recheck = true;
            again.clear();
            again_events = 0;
        }

        if guest.is_none() {
            stream = None;
            stream_retry_at = None;
            active_exclusions.clear();
            idle_since = None;
            again.clear();
            again_events = 0;
            rescan_again = None;
            for idx in &mut dir_indices {
                *idx = DirIndex::default();
            }
            for i in 0..n_shares {
                hooks.disarm_activity(i);
                hooks.disarm_dir_change(i);
            }
            continue;
        }

        let now = Instant::now();
        let views: Vec<Option<N>> = (0..n_shares).map(&view_of).collect();

        // Check whether any share currently holds non-root nodes or saw new activity.
        let mut any_active = false;
        for (i, view) in views.iter().enumerate() {
            if let Some(v) = view {
                let act = v.activity();
                if v.len() > 1 || act != last_activity[i] {
                    any_active = true;
                }
            }
        }

        if stream.is_none() {
            if !any_active {
                for i in 0..n_shares {
                    hooks.arm_activity(i);
                }
                // Dekker re-check after arming so activity racing with arming is never missed.
                for (i, view) in views.iter().enumerate() {
                    if let Some(v) = view
                        && (v.len() > 1 || v.activity() != last_activity[i])
                    {
                        any_active = true;
                        break;
                    }
                }
            }
            if !any_active {
                // Dormant: nothing cached in any share, no FSEvents stream running.
                stream_retry_at = None;
                if guest_connected && debug_on() {
                    eprintln!("fscache: guest connected, dormant (no cached share entries)");
                }
                continue;
            }
            for i in 0..n_shares {
                hooks.disarm_activity(i);
            }
            if stream_retry_at.is_none_or(|at| now >= at) {
                let desired: Vec<Exclusion> = all_exclusions
                    .iter()
                    .filter(|ex| !exclusion_accessed(&views, &mut dir_indices, roots, ex))
                    .cloned()
                    .collect();
                let ex_strings: Vec<String> = desired
                    .iter()
                    .map(|ex| ex.path.display().to_string())
                    .collect();
                match start_stream(&root_strings, &ex_strings) {
                    Ok(s) => {
                        stream = Some(s);
                        stream_retry_at = None;
                        active_exclusions = desired;
                        idle_since = None;
                        rescan = true;
                        schedule_rescan_recheck = true;
                        for (i, view) in views.iter().enumerate() {
                            if let Some(v) = view {
                                last_activity[i] = v.activity();
                                last_dir_seq[i] = v.dir_seq();
                                had_activity[i] = true;
                            }
                        }
                        if debug_on() {
                            eprintln!(
                                "fscache: started FSEvents stream (exclusions={:?})",
                                ex_strings
                            );
                        }
                    }
                    Err(e) => {
                        log::warn!("fscache: start FSEvents stream: {e}");
                        stream_retry_at = Some(now + idle_stop);
                        // Still invalidate whatever is currently cached so the guest doesn't stay stale.
                        rescan = true;
                        schedule_rescan_recheck = true;
                    }
                }
            }
        } else if guest_connected {
            rescan = true;
            schedule_rescan_recheck = true;
        }

        // While active, track idle time and drop exclusions if the guest looks up
        // an excluded directory.
        if stream.is_some() {
            let mut active_now = false;
            for (i, view) in views.iter().enumerate() {
                if let Some(v) = view {
                    let act = v.activity();
                    if act != last_activity[i] {
                        last_activity[i] = act;
                        had_activity[i] = true;
                        active_now = true;
                    }
                    if v.len() > 1 {
                        had_activity[i] = true;
                        active_now = true;
                    }
                }
            }
            if active_now {
                idle_since = None;
            } else {
                let since = *idle_since.get_or_insert(now);
                if now.duration_since(since) >= idle_stop && rescan_again.is_none() && !rescan {
                    // Drop any root-level negative dentries / cached directory listings
                    // and root attributes before tearing down the FSEvents stream.
                    let mut root_invals = Vec::new();
                    for (i, view) in views.iter().enumerate() {
                        if let Some(v) = view {
                            if had_activity[i] || !events.is_empty() {
                                dir_indices[i].refresh(v);
                                if let Some(root_id) = dir_indices[i].root_id() {
                                    root_invals.push(Inval::ExpireDir {
                                        share: i as u32,
                                        dir: root_id,
                                    });
                                }
                                had_activity[i] = false;
                            }
                            last_activity[i] = v.activity();
                        }
                    }
                    if !root_invals.is_empty()
                        && let Some(conn) = guest.as_mut()
                        && !send_invals(conn, &root_invals)
                    {
                        guest = None;
                    }
                    stream = None;
                    stream_retry_at = None;
                    active_exclusions.clear();
                    idle_since = None;
                    again.clear();
                    again_events = 0;
                    for idx in &mut dir_indices {
                        *idx = DirIndex::default();
                    }
                    for i in 0..n_shares {
                        hooks.disarm_dir_change(i);
                    }
                    if guest.is_some() {
                        for i in 0..n_shares {
                            hooks.arm_activity(i);
                        }
                        // Dekker re-check: if activity arrived during teardown, wake immediately.
                        let raced = views.iter().enumerate().any(|(i, view)| {
                            view.as_ref()
                                .is_some_and(|v| v.len() > 1 || v.activity() != last_activity[i])
                        });
                        if raced {
                            wake_pending.store(true, Ordering::Release);
                        }
                    }
                    if debug_on() {
                        eprintln!("fscache: stopped FSEvents stream (idle)");
                    }
                    continue;
                }
            }

            // Dynamically drop any exclusion that the guest has now looked up.
            if !active_exclusions.is_empty() {
                let mut recheck_exclusions = true;
                while recheck_exclusions && !active_exclusions.is_empty() {
                    recheck_exclusions = false;
                    for (i, view) in views.iter().enumerate() {
                        if let Some(v) = view {
                            last_dir_seq[i] = v.dir_seq();
                        }
                    }
                    let remaining: Vec<Exclusion> = active_exclusions
                        .iter()
                        .filter(|ex| !exclusion_accessed(&views, &mut dir_indices, roots, ex))
                        .cloned()
                        .collect();
                    if remaining.len() < active_exclusions.len() {
                        let ex_strings: Vec<String> = remaining
                            .iter()
                            .map(|ex| ex.path.display().to_string())
                            .collect();
                        // Start the replacement stream BEFORE dropping the old one so
                        // there is no coverage gap.
                        match start_stream(&root_strings, &ex_strings) {
                            Ok(new_stream) => {
                                let old_stream = stream.replace(new_stream);
                                drop(old_stream);
                                active_exclusions = remaining;
                                rescan = true;
                                schedule_rescan_recheck = true;
                                if debug_on() {
                                    eprintln!(
                                        "fscache: updated FSEvents exclusions={:?}",
                                        ex_strings
                                    );
                                }
                            }
                            Err(e) => {
                                log::warn!("fscache: restart FSEvents stream: {e}");
                                rescan = true;
                                schedule_rescan_recheck = true;
                                break;
                            }
                        }
                    }
                    if !active_exclusions.is_empty() {
                        for i in 0..n_shares {
                            hooks.arm_dir_change(i);
                        }
                        // Dekker re-check after arming.
                        if views.iter().enumerate().any(|(i, view)| {
                            view.as_ref()
                                .is_some_and(|v| v.dir_seq() != last_dir_seq[i])
                        }) {
                            for i in 0..n_shares {
                                hooks.disarm_dir_change(i);
                            }
                            recheck_exclusions = true;
                        }
                    } else {
                        for i in 0..n_shares {
                            hooks.disarm_dir_change(i);
                        }
                    }
                }
            }
        }

        let seq_before: Vec<u64> = views
            .iter()
            .map(|v| v.as_ref().map_or(0, Nodes::activity))
            .collect();

        if !rescan && !events.is_empty() {
            if again.len() >= QUEUE_BOUND
                || again_events.saturating_add(events.len()) > MAX_RECHECK_EVENTS
            {
                again.clear();
                again_events = 0;
                rescan = true;
                schedule_rescan_recheck = true;
            } else {
                again_events += events.len();
                again.push_back((now + recheck, seq_before.clone(), events.clone()));
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
            again_events = 0;
            rescan_again = Some(now + recheck);
        }
        while let Some((_, prev_seq, due)) = again.pop_front_if(|(at, _, _)| *at <= now) {
            again_events = again_events.saturating_sub(due.len());
            // Skip re-planning a batch if no share saw any lookup/readdir activity
            // since the first pass.
            let changed = views.iter().enumerate().any(|(i, view)| {
                view.as_ref()
                    .is_some_and(|v| v.activity() != prev_seq.get(i).copied().unwrap_or(0))
            });
            if changed {
                events.extend(due);
            }
        }

        if !rescan && events.is_empty() {
            continue;
        }

        let Some(conn) = guest.as_mut() else {
            continue;
        };
        let t = Instant::now();
        let mut invals = Vec::new();
        for (i, root) in roots.iter().enumerate() {
            let Some(view) = views[i].as_ref() else {
                continue;
            };
            if rescan {
                dir_indices[i].refresh(view);
                invals.extend(everything(view, i as u32));
            } else {
                invals.extend(plan_with_index(
                    view,
                    &mut dir_indices[i],
                    i as u32,
                    root,
                    &events,
                ));
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
        if !send_invals(conn, &invals) {
            guest = None;
            stream = None;
            stream_retry_at = None;
            active_exclusions.clear();
            idle_since = None;
            again.clear();
            again_events = 0;
            rescan_again = None;
            for idx in &mut dir_indices {
                *idx = DirIndex::default();
            }
            for i in 0..n_shares {
                hooks.disarm_activity(i);
                hooks.disarm_dir_change(i);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::fs;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;

    use super::*;

    /// A fake inode table: host (dev, ino) -> node, tracking `activity` and `dir_seq`.
    #[derive(Clone, Default)]
    struct Table {
        nodes: Arc<Mutex<HashMap<(i32, u64), CachedNode>>>,
        activity: Arc<AtomicU64>,
        dir_seq: Arc<AtomicU64>,
        activity_armed: Arc<AtomicBool>,
        dir_armed: Arc<AtomicBool>,
    }

    impl Nodes for Table {
        fn get(&self, dev: i32, ino: u64) -> Option<CachedNode> {
            self.nodes.lock().unwrap().get(&(dev, ino)).cloned()
        }

        fn all(&self) -> Vec<CachedNode> {
            let mut v: Vec<_> = self.nodes.lock().unwrap().values().cloned().collect();
            v.sort_by_key(|n| n.nodeid);
            v
        }

        fn dirs(&self) -> Vec<CachedNode> {
            let mut v: Vec<_> = self
                .nodes
                .lock()
                .unwrap()
                .values()
                .filter(|n| n.dir)
                .cloned()
                .collect();
            v.sort_by_key(|n| n.nodeid);
            v
        }

        fn len(&self) -> usize {
            self.nodes.lock().unwrap().len()
        }

        fn activity(&self) -> u64 {
            self.activity.load(Ordering::SeqCst)
        }

        fn dir_seq(&self) -> u64 {
            self.dir_seq.load(Ordering::SeqCst)
        }
    }

    impl CacheHooks for Table {
        fn arm_activity(&self, _share: usize) {
            self.activity_armed.store(true, Ordering::SeqCst);
        }
        fn disarm_activity(&self, _share: usize) {
            self.activity_armed.store(false, Ordering::Release);
        }
        fn arm_dir_change(&self, _share: usize) {
            self.dir_armed.store(true, Ordering::SeqCst);
        }
        fn disarm_dir_change(&self, _share: usize) {
            self.dir_armed.store(false, Ordering::Release);
        }
    }

    impl Table {
        /// Seed root without bumping `activity` (like `PassthroughFs::init`).
        fn hold_root(&self, path: &Path, node: u64) {
            let m = fs::symlink_metadata(path).unwrap();
            self.nodes.lock().unwrap().insert(
                (m.dev() as i32, m.ino()),
                CachedNode {
                    nodeid: node,
                    dir: m.is_dir(),
                    anchor: None,
                },
            );
            self.dir_seq.fetch_add(1, Ordering::SeqCst);
        }

        /// The guest looked up `path` (found in `parent` by `name`) as `node`.
        fn hold(&self, path: &Path, node: u64, anchor: Option<(u64, &str)>) {
            let m = fs::symlink_metadata(path).unwrap();
            let dir = m.is_dir();
            self.nodes.lock().unwrap().insert(
                (m.dev() as i32, m.ino()),
                CachedNode {
                    nodeid: node,
                    dir,
                    anchor: anchor.map(|(p, n)| (p, CString::new(n).unwrap())),
                },
            );
            self.activity.fetch_add(1, Ordering::SeqCst);
            if dir {
                self.dir_seq.fetch_add(1, Ordering::SeqCst);
            }
        }

        fn forget(&self, node: u64) {
            let mut g = self.nodes.lock().unwrap();
            let mut dir_removed = false;
            g.retain(|_, v| {
                if v.nodeid == node {
                    dir_removed |= v.dir;
                    false
                } else {
                    true
                }
            });
            drop(g);
            if dir_removed {
                self.dir_seq.fetch_add(1, Ordering::SeqCst);
            }
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
        table.hold_root(&t.0, 1);
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
    fn dir_index_rejects_uncached_subtrees_without_stat_and_tracks_renames() {
        let (t, nodes) = setup("dir-index");
        let mut idx = DirIndex::default();
        idx.refresh(&nodes);
        assert!(idx.may_hold_dir(&t.0, &t.0));
        assert!(idx.may_hold_dir(&t.0, &t.0.join("d")));
        // ASCII case-insensitive match on APFS.
        assert!(idx.may_hold_dir(&t.0, &t.0.join("D")));
        // Uncached trees are rejected in memory without calling lstat.
        assert!(!idx.may_hold_dir(&t.0, &t.0.join("Library")));
        assert!(!idx.may_hold_dir(&t.0, &t.0.join("Library/Caches/AppStore")));
        assert!(!idx.may_hold_dir(&t.0, &t.0.join("d/nested-uncached")));

        // Renaming a cached directory within a cached parent records an alias
        // so subsequent edits inside the renamed directory still invalidate.
        fs::rename(t.0.join("d"), t.0.join("e")).unwrap();
        let out = plan_with_index(
            &nodes,
            &mut idx,
            0,
            &t.0,
            &[ev(&t.0, "d", 0x20800), ev(&t.0, "e", 0x20800)],
        );
        assert_eq!(out, [entry(1, "d"), inode(2, false), entry(1, "e")]);
        assert!(idx.moved_dirs.is_empty(), "relocated in same batch");
        assert!(idx.may_hold_dir(&t.0, &t.0.join("e")));

        fs::write(t.0.join("e/f"), "edited after dir rename").unwrap();
        let out = plan_with_index(&nodes, &mut idx, 0, &t.0, &[ev(&t.0, "e/f", 0x1000)]);
        assert_eq!(out, [inode(3, true)]);
    }

    #[test]
    fn moved_dir_to_uncached_parent_falls_back_until_forgotten() {
        let (t, nodes) = setup("moved-out");
        fs::create_dir(t.0.join("other")).unwrap();
        let mut idx = DirIndex::default();

        // Host moves cached dir `d` into uncached parent `other/d2`.
        fs::rename(t.0.join("d"), t.0.join("other/d2")).unwrap();
        let out = plan_with_index(
            &nodes,
            &mut idx,
            0,
            &t.0,
            &[
                ev(&t.0, "d", flag::ITEM_RENAMED),
                ev(&t.0, "other/d2", flag::ITEM_RENAMED),
            ],
        );
        assert_eq!(out, [entry(1, "d")]);
        assert!(idx.moved_dirs.contains_key(&2));

        // While node 2 is still held by the guest at an unknown host path, edits
        // inside `other/d2` still invalidate node 3 via the fallback.
        fs::write(t.0.join("other/d2/f"), "still tracked").unwrap();
        let out = plan_with_index(&nodes, &mut idx, 0, &t.0, &[ev(&t.0, "other/d2/f", 0x1000)]);
        assert_eq!(out, [inode(3, true)]);

        // Once the guest forgets node 2, fallback clears and uncached subtrees
        // are rejected in memory again.
        nodes.forget(2);
        idx.refresh(&nodes);
        assert!(idx.moved_dirs.is_empty());
        assert!(!idx.may_hold_dir(&t.0, &t.0.join("other/d2")));
    }

    #[test]
    fn dir_index_falls_back_on_non_ascii_names_and_chained_renames() {
        let (t, nodes) = setup("non-ascii-chained");
        // NFC "café" cached, NFD "cafe\u{0301}" arrives in FSEvents -> must not false-negative.
        let nfc = "caf\u{00e9}";
        let nfd = "cafe\u{0301}";
        fs::create_dir(t.0.join(nfc)).unwrap();
        nodes.hold(&t.0.join(nfc), 10, Some((1, nfc)));
        let mut idx = DirIndex::default();
        idx.refresh(&nodes);
        assert!(idx.may_hold_dir(&t.0, &t.0.join(nfd)));
        assert!(!idx.may_hold_dir(&t.0, &t.0.join("ascii-uncached")));

        // Chained host rename: d -> e (alias), then e -> other/d2 (uncached parent).
        // Even after an unrelated dir_seq bump, moved_dirs keeps tracking node 2 because
        // it recorded node 2's real GuestCache anchor (1, "d"), not the transient alias (1, "e").
        fs::create_dir(t.0.join("other")).unwrap();
        fs::create_dir(t.0.join("unrelated")).unwrap();
        fs::rename(t.0.join("d"), t.0.join("e")).unwrap();
        let _ = plan_with_index(
            &nodes,
            &mut idx,
            0,
            &t.0,
            &[
                ev(&t.0, "d", flag::ITEM_RENAMED),
                ev(&t.0, "e", flag::ITEM_RENAMED),
            ],
        );
        assert!(idx.moved_dirs.is_empty());

        fs::rename(t.0.join("e"), t.0.join("other/d2")).unwrap();
        let _ = plan_with_index(
            &nodes,
            &mut idx,
            0,
            &t.0,
            &[
                ev(&t.0, "e", flag::ITEM_RENAMED | flag::ITEM_REMOVED),
                ev(&t.0, "other/d2", flag::ITEM_RENAMED),
            ],
        );
        assert!(idx.moved_dirs.contains_key(&2));

        // Unrelated directory lookup bumps dir_seq and refreshes DirIndex.
        nodes.hold(&t.0.join("unrelated"), 11, Some((1, "unrelated")));
        idx.refresh(&nodes);
        assert!(
            idx.moved_dirs.contains_key(&2),
            "moved_dirs must survive unrelated dir_seq refresh"
        );
        fs::write(t.0.join("other/d2/f"), "still seen").unwrap();
        let out = plan_with_index(&nodes, &mut idx, 0, &t.0, &[ev(&t.0, "other/d2/f", 0x1000)]);
        assert_eq!(out, [inode(3, true)]);
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
    fn lost_events_drop_everything_only_for_cached_subtrees() {
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

        // MUST_SCAN_SUB_DIRS deep inside an uncached subtree under root does NOT
        // blow away the cached tree.
        assert!(
            plan(
                &nodes,
                0,
                &t.0,
                &[ev(&t.0, "uncached/deep/dir", flag::MUST_SCAN_SUB_DIRS)]
            )
            .is_empty()
        );

        // MUST_SCAN_SUB_DIRS on an unrelated sibling does not drop this share.
        let sibling =
            t.0.with_file_name("unrelated-sibling")
                .display()
                .to_string();
        assert!(plan(&nodes, 0, &t.0, &[(sibling, flag::MUST_SCAN_SUB_DIRS)]).is_empty());
    }

    #[test]
    fn candidate_exclusions_filters_deduplicates_and_excludes_home_itself() {
        let t = Tmp::new("exclusions");
        let home = t.0.join("alice");
        fs::create_dir_all(home.join("Library/Caches")).unwrap();
        fs::create_dir_all(home.join(".Trash")).unwrap();
        fs::create_dir_all(home.join(".katamaran/data")).unwrap();
        let outside = std::env::temp_dir().join(format!("kat-fscache-out-{}", std::process::id()));
        let _ = fs::remove_dir_all(&outside);
        fs::create_dir_all(&outside).unwrap();

        let ex = candidate_exclusions(
            std::slice::from_ref(&t.0),
            &[
                home.clone(),                 // must not exclude HOME itself
                home.join(".katamaran"),      // kept
                home.join(".katamaran/data"), // subsumed by .katamaran
                home.join("Library/Caches"),  // subsumed by Library
                outside.clone(),              // outside share root
            ],
            Some(&home),
        );
        let paths: Vec<PathBuf> = ex.into_iter().map(|e| e.path).collect();
        assert_eq!(
            paths,
            vec![
                home.join(".Trash"),
                home.join("Library"),
                home.join(".katamaran"),
            ]
        );
        let _ = fs::remove_dir_all(&outside);
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
    fn lazy_stream_starts_on_activity_drops_accessed_exclusion_and_stops_when_idle() {
        let t = Tmp::new("lazy-lifecycle");
        let home = t.0.join("alice");
        fs::create_dir_all(home.join("Library")).unwrap();
        fs::create_dir_all(home.join("proj")).unwrap();
        fs::write(home.join("proj/a"), "1").unwrap();

        let table = Table::default();
        table.hold_root(&t.0, 1);

        let exclusions = candidate_exclusions(std::slice::from_ref(&t.0), &[], Some(&home));
        assert_eq!(exclusions.len(), 1);
        assert_eq!(exclusions[0].path, home.join("Library"));

        let (mut guest_rx, host_tx) = UnixStream::pair().unwrap();
        guest_rx
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let (tx, rx) = mpsc::sync_channel(16);
        let overflow = Arc::new(AtomicBool::new(false));
        let wake_pending = Arc::new(AtomicBool::new(false));

        #[derive(Clone, Debug, PartialEq, Eq)]
        enum StreamEvent {
            Started(Vec<String>),
            Dropped,
        }
        let log: Arc<Mutex<Vec<StreamEvent>>> = Arc::new(Mutex::new(Vec::new()));
        struct MockStream(Arc<Mutex<Vec<StreamEvent>>>);
        impl Drop for MockStream {
            fn drop(&mut self) {
                self.0.lock().unwrap().push(StreamEvent::Dropped);
            }
        }

        let root = t.0.clone();
        let table_bg = table.clone();
        let overflow_bg = Arc::clone(&overflow);
        let wake_bg = Arc::clone(&wake_pending);
        let log_bg = Arc::clone(&log);
        let ex_bg = exclusions.clone();
        let h = thread::spawn(move || {
            serve_with(
                &rx,
                &overflow_bg,
                &wake_bg,
                &[root],
                &ex_bg,
                |_| Some(table_bg.clone()),
                &table_bg,
                |_roots, ex| {
                    log_bg
                        .lock()
                        .unwrap()
                        .push(StreamEvent::Started(ex.to_vec()));
                    Ok(MockStream(Arc::clone(&log_bg)))
                },
                Duration::from_millis(40),
                Duration::from_millis(120),
            );
        });

        // 1. Guest connects while only root is cached -> stays dormant, no stream started.
        tx.send(Input::Guest(host_tx)).unwrap();
        thread::sleep(Duration::from_millis(50));
        assert!(
            log.lock().unwrap().is_empty(),
            "stream must not start when idle"
        );
        assert!(table.activity_armed.load(Ordering::SeqCst));

        // 2. Guest looks up alice/proj -> wakes watcher -> starts stream with Library excluded.
        table.hold(&home, 2, Some((1, "alice")));
        table.hold(&home.join("proj"), 3, Some((2, "proj")));
        tx.send(Input::Wake).unwrap();

        let mut buf = Vec::new();
        // Catch-up rescan + delayed recheck.
        assert!(
            kat_proto::fscache::read_frame(&mut guest_rx, &mut buf)
                .unwrap()
                .is_some()
        );
        assert_eq!(buf, encode(&everything(&table, 0))[0]);
        assert!(
            kat_proto::fscache::read_frame(&mut guest_rx, &mut buf)
                .unwrap()
                .is_some()
        );
        assert_eq!(buf, encode(&everything(&table, 0))[0]);
        assert_eq!(
            *log.lock().unwrap(),
            vec![StreamEvent::Started(vec![
                home.join("Library").display().to_string()
            ])]
        );
        assert!(table.dir_armed.load(Ordering::SeqCst));

        // 3. Guest looks up inside excluded Library -> replacement stream starts BEFORE old drops,
        //    with empty exclusions, followed by catch-up invalidation + recheck.
        table.hold(&home.join("Library"), 4, Some((2, "Library")));
        tx.send(Input::Wake).unwrap();
        assert!(
            kat_proto::fscache::read_frame(&mut guest_rx, &mut buf)
                .unwrap()
                .is_some()
        );
        assert_eq!(buf, encode(&everything(&table, 0))[0]);
        assert!(
            kat_proto::fscache::read_frame(&mut guest_rx, &mut buf)
                .unwrap()
                .is_some()
        );
        assert_eq!(buf, encode(&everything(&table, 0))[0]);
        assert_eq!(
            *log.lock().unwrap(),
            vec![
                StreamEvent::Started(vec![home.join("Library").display().to_string()]),
                StreamEvent::Started(vec![]),
                StreamEvent::Dropped,
            ]
        );

        // 4. Guest forgets all non-root nodes -> after idle_stop, root ExpireDir is sent and
        //    stream is dropped, returning to dormant state.
        table.forget(4);
        table.forget(3);
        table.forget(2);
        assert!(
            kat_proto::fscache::read_frame(&mut guest_rx, &mut buf)
                .unwrap()
                .is_some()
        );
        assert_eq!(buf, encode(&[Inval::ExpireDir { share: 0, dir: 1 }])[0]);
        thread::sleep(Duration::from_millis(30));
        assert_eq!(
            *log.lock().unwrap(),
            vec![
                StreamEvent::Started(vec![home.join("Library").display().to_string()]),
                StreamEvent::Started(vec![]),
                StreamEvent::Dropped,
                StreamEvent::Dropped,
            ]
        );
        assert!(table.activity_armed.load(Ordering::SeqCst));

        drop(tx);
        h.join().unwrap();
    }

    #[test]
    fn recheck_skips_batch_when_guest_had_no_activity() {
        let (t, table) = setup("skip-recheck");
        let (mut guest_rx, host_tx) = UnixStream::pair().unwrap();
        guest_rx
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let (tx, rx) = mpsc::sync_channel(16);
        let overflow = Arc::new(AtomicBool::new(false));
        let wake_pending = Arc::new(AtomicBool::new(false));

        let root = t.0.clone();
        let table_bg = table.clone();
        let overflow_bg = Arc::clone(&overflow);
        let wake_bg = Arc::clone(&wake_pending);
        let h = thread::spawn(move || {
            serve_with(
                &rx,
                &overflow_bg,
                &wake_bg,
                &[root],
                &[],
                |_| Some(table_bg.clone()),
                &table_bg,
                |_, _| Ok(()),
                Duration::from_millis(40),
                Duration::from_secs(60),
            );
        });

        let mut buf = Vec::new();
        tx.send(Input::Guest(host_tx)).unwrap();
        // Initial stream-start rescan + delayed recheck.
        assert!(
            kat_proto::fscache::read_frame(&mut guest_rx, &mut buf)
                .unwrap()
                .is_some()
        );
        assert!(
            kat_proto::fscache::read_frame(&mut guest_rx, &mut buf)
                .unwrap()
                .is_some()
        );

        // Host edits cached file while guest is quiet: first pass invalidates it,
        // and the 40ms recheck is skipped because `activity()` did not advance.
        fs::write(t.0.join("d/f"), "changed").unwrap();
        tx.send(Input::Fs(vec![ev(&t.0, "d/f", 0x1000)])).unwrap();
        assert!(
            kat_proto::fscache::read_frame(&mut guest_rx, &mut buf)
                .unwrap()
                .is_some()
        );
        assert_eq!(buf, encode(&[inode(3, true)])[0]);

        guest_rx
            .set_read_timeout(Some(Duration::from_millis(120)))
            .unwrap();
        let err = kat_proto::fscache::read_frame(&mut guest_rx, &mut buf).unwrap_err();
        assert!(
            matches!(
                err.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
            ),
            "recheck should have been skipped when idle, got {err:?}"
        );

        drop(tx);
        h.join().unwrap();
    }

    #[test]
    fn recheck_runs_when_guest_activity_advances_during_window() {
        let (t, table) = setup("run-recheck");
        let (mut guest_rx, host_tx) = UnixStream::pair().unwrap();
        guest_rx
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let (tx, rx) = mpsc::sync_channel(16);
        let overflow = Arc::new(AtomicBool::new(false));
        let wake_pending = Arc::new(AtomicBool::new(false));

        let root = t.0.clone();
        let table_bg = table.clone();
        let overflow_bg = Arc::clone(&overflow);
        let wake_bg = Arc::clone(&wake_pending);
        let h = thread::spawn(move || {
            serve_with(
                &rx,
                &overflow_bg,
                &wake_bg,
                &[root],
                &[],
                |_| Some(table_bg.clone()),
                &table_bg,
                |_, _| Ok(()),
                Duration::from_millis(50),
                Duration::from_secs(60),
            );
        });

        let mut buf = Vec::new();
        tx.send(Input::Guest(host_tx)).unwrap();
        assert!(
            kat_proto::fscache::read_frame(&mut guest_rx, &mut buf)
                .unwrap()
                .is_some()
        );
        assert!(
            kat_proto::fscache::read_frame(&mut guest_rx, &mut buf)
                .unwrap()
                .is_some()
        );

        // File is created on host and FSEvent arrives BEFORE the guest finishes registering it.
        fs::write(t.0.join("d/late"), "new").unwrap();
        tx.send(Input::Fs(vec![ev(&t.0, "d/late", flag::ITEM_CREATED)]))
            .unwrap();
        assert!(
            kat_proto::fscache::read_frame(&mut guest_rx, &mut buf)
                .unwrap()
                .is_some()
        );
        assert_eq!(buf, encode(&[entry(2, "late")])[0]);

        // Guest registers the node during the recheck window (bumping activity).
        table.hold(&t.0.join("d/late"), 4, Some((2, "late")));

        // Delayed recheck MUST run because activity advanced, invalidating node 4.
        assert!(
            kat_proto::fscache::read_frame(&mut guest_rx, &mut buf)
                .unwrap()
                .is_some()
        );
        assert_eq!(buf, encode(&[inode(4, true)])[0]);

        drop(tx);
        h.join().unwrap();
    }

    #[test]
    fn idle_stream_stops_even_with_background_uncached_host_events() {
        let t = Tmp::new("idle-bg-events");
        fs::create_dir_all(t.0.join("proj")).unwrap();
        let table = Table::default();
        table.hold_root(&t.0, 1);
        table.hold(&t.0.join("proj"), 2, Some((1, "proj")));

        let (mut guest_rx, host_tx) = UnixStream::pair().unwrap();
        guest_rx
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let (tx, rx) = mpsc::sync_channel(32);
        let overflow = Arc::new(AtomicBool::new(false));
        let wake_pending = Arc::new(AtomicBool::new(false));

        let active = Arc::new(AtomicBool::new(false));
        struct ActiveStream(Arc<AtomicBool>);
        impl Drop for ActiveStream {
            fn drop(&mut self) {
                self.0.store(false, Ordering::SeqCst);
            }
        }

        let root = t.0.clone();
        let table_bg = table.clone();
        let overflow_bg = Arc::clone(&overflow);
        let wake_bg = Arc::clone(&wake_pending);
        let active_bg = Arc::clone(&active);
        let h = thread::spawn(move || {
            serve_with(
                &rx,
                &overflow_bg,
                &wake_bg,
                &[root],
                &[],
                |_| Some(table_bg.clone()),
                &table_bg,
                |_, _| {
                    active_bg.store(true, Ordering::SeqCst);
                    Ok(ActiveStream(Arc::clone(&active_bg)))
                },
                Duration::from_millis(30),
                Duration::from_millis(120),
            );
        });

        let mut buf = Vec::new();
        tx.send(Input::Guest(host_tx)).unwrap();
        // Initial rescan + delayed recheck.
        assert!(
            kat_proto::fscache::read_frame(&mut guest_rx, &mut buf)
                .unwrap()
                .is_some()
        );
        assert!(
            kat_proto::fscache::read_frame(&mut guest_rx, &mut buf)
                .unwrap()
                .is_some()
        );
        assert!(active.load(Ordering::SeqCst));

        // Guest forgets non-root dir while unrelated host events keep arriving.
        table.forget(2);
        for _ in 0..8 {
            tx.send(Input::Fs(vec![ev(&t.0, "uncached/noise", 0x1000)]))
                .unwrap();
            thread::sleep(Duration::from_millis(25));
            if !active.load(Ordering::SeqCst) {
                break;
            }
        }
        assert!(
            kat_proto::fscache::read_frame(&mut guest_rx, &mut buf)
                .unwrap()
                .is_some()
        );
        assert_eq!(buf, encode(&[Inval::ExpireDir { share: 0, dir: 1 }])[0]);
        thread::sleep(Duration::from_millis(20));
        assert!(
            !active.load(Ordering::SeqCst),
            "background uncached events must not keep the stream alive"
        );

        drop(tx);
        h.join().unwrap();
    }

    #[test]
    fn exclusion_dropped_when_recreated_dir_accessed_by_path() {
        let t = Tmp::new("recreated-exclusion");
        let home = t.0.join("alice");
        fs::create_dir_all(home.join("Library")).unwrap();
        fs::create_dir_all(home.join("proj")).unwrap();

        let exclusions = candidate_exclusions(std::slice::from_ref(&t.0), &[], Some(&home));
        assert_eq!(exclusions.len(), 1);
        let old_ino = exclusions[0].ino;

        // Host deletes and recreates ~/Library with a new inode.
        fs::remove_dir(home.join("Library")).unwrap();
        fs::create_dir(home.join("Library")).unwrap();
        let new_ino = fs::symlink_metadata(home.join("Library")).unwrap().ino();
        assert_ne!(old_ino, new_ino);

        let table = Table::default();
        table.hold_root(&t.0, 1);
        table.hold(&home, 2, Some((1, "alice")));
        table.hold(&home.join("Library"), 3, Some((2, "Library")));

        let views = vec![Some(table)];
        let mut dir_indices = vec![DirIndex::default()];
        assert!(
            exclusion_accessed(
                &views,
                &mut dir_indices,
                std::slice::from_ref(&t.0),
                &exclusions[0]
            ),
            "path match in DirIndex must catch recreated excluded directories"
        );
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
        let wake_pending = Arc::new(AtomicBool::new(false));
        let root = t.0.clone();
        let nodes_bg = nodes.clone();
        let overflow_bg = Arc::clone(&overflow);
        let wake_bg = Arc::clone(&wake_pending);
        let h = thread::spawn(move || {
            serve_with(
                &rx,
                &overflow_bg,
                &wake_bg,
                &[root],
                &[],
                |_| Some(nodes_bg.clone()),
                &nodes_bg,
                |_, _| Ok(()),
                Duration::from_millis(40),
                Duration::from_secs(60),
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
