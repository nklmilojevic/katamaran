//! File changes on the Mac, sent to the guest for the paths guest processes
//! watch with inotify.
//!
//! The guest kernel reports each virtio-fs inode a guest process watches
//! (`FsWatch`, ref-counted in the kernel, so one id per watched inode).
//! This watches the same paths with FSEvents, one stream per directory of
//! the smallest set covering them all, keeps a snapshot (inode, size, times,
//! mode, owner) of each watched directory's entries and of each watched
//! path, and on every FSEvents batch compares the named entries with the
//! snapshot to tell what happened: FSEvents' own flags accumulate and only
//! say where to look. The result goes to the guest as `FsEvents`, by watch
//! id and child name; the guest kernel raises the inotify events and drops
//! its caches.
//!
//! Streams always start from now: a stream started from an older event id
//! replays FSEvents' history, which can take seconds. A change made between
//! the guest's watch and its stream going live is found instead by looking
//! at each newly watched path once more shortly after (see `Node::fresh`).
//!
//! An event can also come late: the path it names may already be renamed by
//! the time it is looked at, and FSEvents says so only in its next batch. A
//! path gone without a removed or renamed flag is held until its own event,
//! or its object turning up elsewhere, tells which it was (see `Held`).

use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::os::unix::fs::MetadataExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use kat_proto::{FsEvent, FsOp, Msg, write_msg};

use crate::fsevents::{self, Stream, flag};

/// FSEvents' latency: changes within it arrive together.
const LATENCY: f64 = 0.01;
/// A burst of watch changes updates the streams once, this long after the first.
const RESTART_DELAY: Duration = Duration::from_millis(5);
/// A newly watched path is looked at again this long after the watch, once
/// its stream is up and FSEvents has delivered what it saw before.
const RESCAN_DELAY: Duration = Duration::from_millis(30);
/// A path gone without saying why is reported deleted this long after, unless
/// FSEvents' following events show it renamed first. Also how long a rename
/// split across batches keeps its cookie for the destination.
const HOLD: Duration = Duration::from_millis(50);
/// Events per message: far below kat-proto's frame limit even for 255-byte names.
const BATCH: usize = 2000;
/// A guest watch is set this long before the host hears of it, at most.
const WATCH_SLACK: Duration = Duration::from_millis(50);
/// Noisy trees left out of the streams unless a guest process watches inside them.
const EXCLUDE: [&str; 2] = [".git/objects", "node_modules/.cache"];

pub struct Watcher {
    tx: Sender<Input>,
    thread: Option<JoinHandle<()>>,
}

enum Input {
    Watch { id: u64, dir: bool, path: String },
    Unwatch(u64),
    Reset,
    Fs(Vec<fsevents::Event>),
    Stop,
}

impl Watcher {
    /// `shares` maps guest paths to host paths. Events go out on `out`.
    pub fn start(shares: Vec<(String, PathBuf)>, out: UnixStream) -> Watcher {
        let (tx, rx) = mpsc::channel();
        let fs_tx = tx.clone();
        let thread = thread::Builder::new()
            .name("fswatch".into())
            .spawn(move || run(&rx, fs_tx, State::new(shares), out))
            .expect("spawn fswatch thread");
        Watcher {
            tx,
            thread: Some(thread),
        }
    }

    /// Take a guest watch message.
    pub fn handle(&self, msg: Msg) {
        let input = match msg {
            Msg::FsWatch { id, dir, path } => Input::Watch { id, dir, path },
            Msg::FsUnwatch { id } => Input::Unwatch(id),
            Msg::FsWatchReset => Input::Reset,
            _ => return,
        };
        let _ = self.tx.send(input);
    }

    /// Stop watching; no message is sent after this returns.
    pub fn stop(mut self) {
        let _ = self.tx.send(Input::Stop);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

fn run(rx: &Receiver<Input>, fs_tx: Sender<Input>, mut st: State, mut out: UnixStream) {
    let fs_tx = Arc::new(Mutex::new(fs_tx));
    let mut streams: HashMap<String, Stream> = HashMap::new();
    let mut update_at: Option<Instant> = None;
    let mut rescans: Vec<(Instant, PathBuf)> = Vec::new();
    loop {
        let now = Instant::now();
        let next = update_at
            .into_iter()
            .chain(rescans.iter().map(|r| r.0))
            .chain(st.held_until())
            .min();
        let mut inputs = Vec::new();
        let got = match next {
            Some(t) => rx.recv_timeout(t.saturating_duration_since(now)),
            None => rx.recv().map_err(|_| RecvTimeoutError::Disconnected),
        };
        match got {
            Ok(i) => inputs.push(i),
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return,
        }
        inputs.extend(rx.try_iter());

        let mut changed = false;
        let mut batch = Vec::new();
        for input in inputs {
            match input {
                Input::Stop => return,
                Input::Watch { id, dir, path } => {
                    debug(format_args!("watch {id} {path}"));
                    if let Some(new) = st.watch(id, dir, &path) {
                        rescans.push((Instant::now() + RESCAN_DELAY, new));
                        changed = true;
                    }
                }
                Input::Unwatch(id) => changed |= st.unwatch(id),
                Input::Reset => changed |= st.reset(),
                Input::Fs(events) => batch.extend(events),
            }
        }
        if changed {
            update_at.get_or_insert(Instant::now() + RESTART_DELAY);
        }

        let now = Instant::now();
        if update_at.is_some_and(|t| t <= now) {
            update_at = None;
            let roots = st.roots();
            // New streams first, so a tree moving to a new covering root is
            // never unwatched.
            for root in &roots {
                if streams.contains_key(root) {
                    continue;
                }
                let tx = fs_tx.clone();
                let excluded = st.exclusions(std::slice::from_ref(root));
                match Stream::start(std::slice::from_ref(root), &excluded, LATENCY, move |e| {
                    let _ = tx.lock().unwrap().send(Input::Fs(e));
                }) {
                    Ok(s) => {
                        debug(format_args!("stream {root}"));
                        streams.insert(root.clone(), s);
                    }
                    Err(e) => eprintln!("katamaran: cannot watch {root} for the guest: {e}"),
                }
            }
            streams.retain(|root, _| roots.contains(root));
        }

        let due: Vec<PathBuf> = rescans
            .extract_if(.., |(t, _)| *t <= now)
            .map(|(_, p)| p)
            .collect();
        let held_due = st.held_until().is_some_and(|t| t <= now);
        if !batch.is_empty() || !due.is_empty() || held_due {
            debug(format_args!("fsevents {batch:?} rescan {due:?}"));
            let events = st.process(batch, &due, now);
            debug(format_args!("guest events {events:?}"));
            for chunk in events.chunks(BATCH) {
                if write_msg(&mut out, &Msg::FsEvents(chunk.to_vec())).is_err() {
                    return;
                }
            }
        }
    }
}

/// With KATAMARAN_FSWATCH_DEBUG set, a timestamped trace on stderr.
fn debug(what: std::fmt::Arguments<'_>) {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    if *ON.get_or_init(|| std::env::var_os("KATAMARAN_FSWATCH_DEBUG").is_some()) {
        let (s, ns) = unix_time(SystemTime::now());
        eprintln!("fswatch {s}.{:03}: {what}", ns / 1_000_000);
    }
}

/// What a path was when last looked at: enough to tell what changed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Meta {
    ino: u64,
    dir: bool,
    file: bool,
    size: u64,
    mtime: (i64, i64),
    ctime: (i64, i64),
    mode: u32,
    owner: (u32, u32),
    born: (i64, i64),
}

fn unix_time(t: SystemTime) -> (i64, i64) {
    t.duration_since(UNIX_EPOCH).map_or((0, 0), |d| {
        (d.as_secs() as i64, i64::from(d.subsec_nanos()))
    })
}

impl Meta {
    fn of(m: &std::fs::Metadata) -> Meta {
        Meta {
            ino: m.ino(),
            dir: m.is_dir(),
            file: m.is_file(),
            size: m.size(),
            mtime: (m.mtime(), m.mtime_nsec()),
            ctime: (m.ctime(), m.ctime_nsec()),
            mode: m.mode(),
            owner: (m.uid(), m.gid()),
            born: m.created().map_or((0, 0), unix_time),
        }
    }

    fn at(path: &Path) -> Option<Meta> {
        std::fs::symlink_metadata(path).ok().map(|m| Meta::of(&m))
    }

    /// The change from `self` to `new`, the same object. A directory's times
    /// move with its entries, which Linux does not report on it.
    /// What `self`, first seen after the guest began watching at `since`,
    /// was before the changes made since then (None: it was not there).
    fn before(&self, since: (i64, i64)) -> Option<Option<Meta>> {
        if self.born >= since {
            Some(None)
        } else if self.mtime >= since {
            Some(Some(Meta {
                mtime: (0, 0),
                ..*self
            }))
        } else if self.ctime >= since {
            Some(Some(Meta {
                ctime: (0, 0),
                ..*self
            }))
        } else {
            None
        }
    }

    fn change(&self, new: &Meta) -> Option<FsOp> {
        let attrs = (self.mode, self.owner) != (new.mode, new.owner);
        if new.dir {
            attrs.then_some(FsOp::Attrib)
        } else if self.size != new.size || self.mtime != new.mtime {
            Some(FsOp::Modify)
        } else if attrs || self.ctime != new.ctime {
            Some(FsOp::Attrib)
        } else {
            None
        }
    }
}

/// A watched host path.
#[derive(Default)]
struct Node {
    /// Guest watch ids on it (normally one: the kernel keeps one per inode).
    ids: Vec<u64>,
    dir: bool,
    meta: Option<Meta>,
    /// A watched directory's entries.
    children: HashMap<OsString, Meta>,
    /// When the guest began watching (less the slack).
    since: (i64, i64),
    /// Entries (and the path itself) not looked at since the snapshot was
    /// taken: one may have changed after the guest's watch and before the
    /// snapshot, so the snapshot already shows the change.
    fresh: HashSet<OsString>,
    fresh_self: bool,
}

fn list(dir: &Path) -> HashMap<OsString, Meta> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return HashMap::new();
    };
    entries
        .flatten()
        .filter_map(|e| Some((e.file_name(), Meta::of(&e.metadata().ok()?))))
        .collect()
}

/// A changed path: what it was and is.
struct Change {
    /// The watched directory it is in, if the change is reported by name.
    parent: Option<PathBuf>,
    path: PathBuf,
    old: Option<Meta>,
    new: Option<Meta>,
    flags: u32,
    /// Until when a path gone without a removed or renamed flag may be held.
    hold: Option<Instant>,
}

/// A watched path that went without a removed or renamed flag: the event
/// that named it predates the change, and a rename's events may still come.
struct Held {
    parent: Option<PathBuf>,
    path: PathBuf,
    old: Meta,
    until: Instant,
}

impl Change {
    /// The object that left the path, if one did.
    fn left(&self) -> Option<Meta> {
        self.old.filter(|o| self.new.is_none_or(|n| n.ino != o.ino))
    }

    /// The object that arrived at the path, if one did.
    fn arrived(&self) -> Option<Meta> {
        self.new.filter(|n| self.old.is_none_or(|o| o.ino != n.ino))
    }
}

struct State {
    shares: Vec<(String, PathBuf)>,
    watches: HashMap<u64, PathBuf>,
    nodes: HashMap<PathBuf, Node>,
    cookie: u32,
    held: Vec<Held>,
    /// Objects just reported moved out with no destination seen: inode,
    /// cookie and until when a destination in a later batch gets the cookie.
    moved_out: Vec<(u64, u32, Instant)>,
}

impl State {
    fn new(shares: Vec<(String, PathBuf)>) -> State {
        State {
            shares,
            watches: HashMap::new(),
            nodes: HashMap::new(),
            cookie: 0,
            held: Vec::new(),
            moved_out: Vec::new(),
        }
    }

    /// When the first held path is due to be reported deleted.
    fn held_until(&self) -> Option<Instant> {
        self.held.iter().map(|h| h.until).min()
    }

    /// The host path of a guest path, as the host spells it.
    fn host_path(&self, guest: &str) -> Option<PathBuf> {
        self.shares.iter().find_map(|(g, h)| {
            let rest = guest.strip_prefix(g.trim_end_matches('/'))?;
            let rest = match rest {
                "" => "",
                r => r.strip_prefix('/')?,
            };
            let host = if rest.is_empty() {
                h.clone()
            } else {
                h.join(rest)
            };
            Some(std::fs::canonicalize(&host).unwrap_or(host))
        })
    }

    /// Returns the host path if it was not watched before.
    fn watch(&mut self, id: u64, dir: bool, guest: &str) -> Option<PathBuf> {
        let path = self.host_path(guest)?;
        self.unwatch(id);
        self.watches.insert(id, path.clone());
        if let Some(node) = self.nodes.get_mut(&path) {
            node.ids.push(id);
            return None;
        }
        let children = if dir { list(&path) } else { HashMap::new() };
        let node = Node {
            ids: vec![id],
            dir,
            meta: Meta::at(&path),
            since: unix_time(SystemTime::now() - WATCH_SLACK),
            fresh: children.keys().cloned().collect(),
            fresh_self: true,
            children,
        };
        self.nodes.insert(path.clone(), node);
        Some(path)
    }

    fn unwatch(&mut self, id: u64) -> bool {
        let Some(path) = self.watches.remove(&id) else {
            return false;
        };
        let Some(node) = self.nodes.get_mut(&path) else {
            return false;
        };
        node.ids.retain(|&i| i != id);
        if node.ids.is_empty() {
            self.nodes.remove(&path);
            return true;
        }
        false
    }

    fn reset(&mut self) -> bool {
        let had = !self.nodes.is_empty();
        self.watches.clear();
        self.nodes.clear();
        self.held.clear();
        self.moved_out.clear();
        had
    }

    /// The fewest directories whose trees hold every watched path.
    fn roots(&self) -> Vec<String> {
        let mut dirs: Vec<&Path> = self
            .nodes
            .iter()
            .filter_map(|(p, n)| if n.dir { Some(p.as_path()) } else { p.parent() })
            .collect();
        dirs.sort();
        dirs.dedup();
        let mut roots: Vec<&Path> = Vec::new();
        for d in dirs {
            if !roots.last().is_some_and(|r| d.starts_with(r)) {
                roots.push(d);
            }
        }
        roots.iter().map(|r| r.display().to_string()).collect()
    }

    /// Noisy trees under the roots that nothing watched is in.
    fn exclusions(&self, roots: &[String]) -> Vec<String> {
        roots
            .iter()
            .flat_map(|r| EXCLUDE.iter().map(move |e| Path::new(r).join(e)))
            .filter(|ex| ex.is_dir() && !self.nodes.keys().any(|p| p.starts_with(ex)))
            .map(|ex| ex.display().to_string())
            .take(fsevents::MAX_EXCLUSIONS)
            .collect()
    }

    fn next_cookie(&mut self) -> u32 {
        self.cookie = self.cookie.wrapping_add(1).max(1);
        self.cookie
    }

    /// The guest events for a batch of FSEvents, and for watched paths to
    /// look at again in full.
    fn process(
        &mut self,
        batch: Vec<fsevents::Event>,
        again: &[PathBuf],
        now: Instant,
    ) -> Vec<FsEvent> {
        // Entries to look at, by watched directory, and watched paths, in order.
        let mut named: Vec<(PathBuf, OsString)> = Vec::new();
        let mut selves: Vec<PathBuf> = Vec::new();
        let mut flags: HashMap<PathBuf, u32> = HashMap::new();
        let mut rescan: HashSet<PathBuf> = HashSet::new();
        for p in again {
            if let Some(node) = self.nodes.get(p) {
                if node.dir {
                    rescan.insert(p.clone());
                }
                selves.push(p.clone());
            }
        }
        for (p, f) in batch {
            let path = PathBuf::from(p);
            if f & flag::RESCAN != 0 {
                for (n, node) in &self.nodes {
                    if n.starts_with(&path) {
                        if node.dir {
                            rescan.insert(n.clone());
                        }
                        selves.push(n.clone());
                    }
                }
            }
            if let (Some(parent), Some(name)) = (path.parent(), path.file_name())
                && self.nodes.get(parent).is_some_and(|n| n.dir)
            {
                named.push((parent.to_path_buf(), name.to_os_string()));
            }
            if self.nodes.contains_key(&path) {
                selves.push(path.clone());
            }
            *flags.entry(path).or_default() |= f;
        }
        for dir in rescan {
            let node = &self.nodes[&dir];
            let mut names: HashSet<OsString> = node.children.keys().cloned().collect();
            if let Ok(entries) = std::fs::read_dir(&dir) {
                names.extend(entries.flatten().map(|e| e.file_name()));
            }
            named.extend(names.into_iter().map(|n| (dir.clone(), n)));
        }

        let mut changes = Vec::new();
        let mut seen = HashSet::new();
        for (dir, name) in named {
            let path = dir.join(&name);
            if !seen.insert((Some(dir.clone()), path.clone())) {
                continue;
            }
            let new = Meta::at(&path);
            let node = self.nodes.get_mut(&dir).unwrap();
            let fresh = node.fresh.remove(&name);
            let mut old = match new {
                Some(m) => node.children.insert(name, m),
                None => node.children.remove(&name),
            };
            if fresh && old == new {
                old = new.and_then(|n| n.before(node.since)).unwrap_or(old);
            } else if fresh
                && let (Some(o), Some(n)) = (old, new)
                && o.ino == n.ino
                && (o.size, o.mtime) == (n.size, n.mtime)
                && n.born >= node.since
            {
                // Made after the guest's watch, its attributes changed after
                // the snapshot (macOS adds an xattr to a new file just after).
                old = None;
            }
            if old != new {
                // Only a path an event named can have its event still to come.
                let named = flags.contains_key(&path);
                let flags = flags.get(&path).copied().unwrap_or(0);
                changes.push(Change {
                    parent: Some(dir),
                    path,
                    old,
                    new,
                    flags,
                    hold: named.then_some(now + HOLD),
                });
            }
        }
        for path in selves {
            if !seen.insert((None, path.clone())) {
                continue;
            }
            let new = Meta::at(&path);
            let node = self.nodes.get_mut(&path).unwrap();
            let mut old = std::mem::replace(&mut node.meta, new);
            if std::mem::take(&mut node.fresh_self) && old == new {
                old = new.and_then(|n| n.before(node.since)).unwrap_or(old);
            }
            if old != new {
                // Only a path an event named can have its event still to come.
                let named = flags.contains_key(&path);
                let flags = flags.get(&path).copied().unwrap_or(0);
                changes.push(Change {
                    parent: None,
                    path,
                    old,
                    new,
                    flags,
                    hold: named.then_some(now + HOLD),
                });
            }
        }
        self.sources(&mut changes, &seen);
        let changes = self.resume(changes, &flags, now);
        self.moved_out.retain(|m| m.2 > now);
        self.events(&changes, now)
    }

    /// Held paths explained by this batch go back in, before its changes:
    /// those named again, those whose object arrived somewhere, and those
    /// whose time is up.
    fn resume(
        &mut self,
        changes: Vec<Change>,
        flags: &HashMap<PathBuf, u32>,
        now: Instant,
    ) -> Vec<Change> {
        let arrived: HashSet<u64> = changes
            .iter()
            .filter(|c| c.parent.is_some())
            .filter_map(|c| c.arrived().map(|m| m.ino))
            .collect();
        let mut out = Vec::new();
        let due = self.held.extract_if(.., |h| {
            h.until <= now || flags.contains_key(&h.path) || arrived.contains(&h.old.ino)
        });
        for h in due {
            // Whether the path holds something again, as last looked at.
            let back = match &h.parent {
                Some(dir) => self
                    .nodes
                    .get(dir)
                    .map(|n| n.children.contains_key(h.path.file_name().unwrap())),
                None => self.nodes.get(&h.path).map(|n| n.meta.is_some()),
            };
            let Some(back) = back else {
                continue; // no longer watched
            };
            out.push(Change {
                flags: flags.get(&h.path).copied().unwrap_or(0),
                hold: (!back && h.until > now).then_some(h.until),
                parent: h.parent,
                path: h.path,
                old: Some(h.old),
                new: None,
            });
        }
        out.extend(changes);
        out
    }

    /// An object renamed into a watched directory from a watched path this
    /// batch does not name (FSEvents' event for the source is still to come)
    /// is found by inode in the snapshots, and the source taken as changed.
    fn sources(&mut self, changes: &mut Vec<Change>, seen: &HashSet<(Option<PathBuf>, PathBuf)>) {
        let left: HashSet<u64> = changes
            .iter()
            .filter(|c| c.parent.is_some())
            .filter_map(|c| c.left().map(|m| m.ino))
            .collect();
        let wanted: HashSet<u64> = changes
            .iter()
            .filter(|c| c.parent.is_some() && c.flags & flag::ITEM_RENAMED != 0)
            .filter_map(|c| c.arrived().map(|m| m.ino))
            .filter(|ino| !left.contains(ino) && !self.held.iter().any(|h| h.old.ino == *ino))
            .collect();
        if wanted.is_empty() {
            return;
        }
        let mut found = Vec::new();
        for (dir, node) in &self.nodes {
            if let Some(m) = node.meta.filter(|m| wanted.contains(&m.ino)) {
                found.push((None, dir.clone(), m));
            }
            for (name, m) in &node.children {
                if wanted.contains(&m.ino) {
                    found.push((Some(dir.clone()), dir.join(name), *m));
                }
            }
        }
        for (parent, path, old) in found {
            if seen.contains(&(parent.clone(), path.clone())) {
                continue;
            }
            let new = Meta::at(&path);
            if new.is_some_and(|n| n.ino == old.ino) {
                continue; // another hard link
            }
            match &parent {
                Some(dir) => {
                    let node = self.nodes.get_mut(dir).unwrap();
                    let name = path.file_name().unwrap();
                    node.fresh.remove(name);
                    match new {
                        Some(m) => node.children.insert(name.to_os_string(), m),
                        None => node.children.remove(name),
                    };
                }
                None => {
                    let node = self.nodes.get_mut(&path).unwrap();
                    node.fresh_self = false;
                    node.meta = new;
                }
            }
            changes.push(Change {
                parent,
                path,
                old: Some(old),
                new,
                flags: 0,
                hold: None,
            });
        }
    }

    fn events(&mut self, changes: &[Change], now: Instant) -> Vec<FsEvent> {
        // Renames: an object that left one watched entry and arrived at
        // another in the same batch. Moved-from goes out right before its
        // moved-to, at whichever of the two comes first.
        let mut left: HashMap<u64, usize> = HashMap::new();
        for (i, c) in changes.iter().enumerate() {
            if c.parent.is_some()
                && let Some(o) = c.left()
            {
                left.entry(o.ino).or_insert(i);
            }
        }
        // Source to destination and back; one change can be both, for
        // different objects (a file renamed over another that moves on).
        let mut to_of: HashMap<usize, usize> = HashMap::new();
        let mut from_of: HashMap<usize, usize> = HashMap::new();
        let mut moved_away: HashSet<u64> = HashSet::new();
        for (i, c) in changes.iter().enumerate() {
            if c.parent.is_some()
                && let Some(n) = c.arrived()
                && let Some(&j) = left.get(&n.ino)
                && j != i
                && !to_of.contains_key(&j)
            {
                to_of.insert(j, i);
                from_of.insert(i, j);
                moved_away.insert(n.ino);
            }
        }

        let mut out = Vec::new();
        // Sources whose rename went out.
        let mut done: HashSet<usize> = HashSet::new();
        for (i, c) in changes.iter().enumerate() {
            // A path keeps flags from its earlier events: a file renamed
            // into place and later removed says both.
            let removed = c.flags & flag::ITEM_REMOVED != 0;
            let renamed = c.flags & flag::ITEM_RENAMED != 0 && !removed;
            let created = c.flags & flag::ITEM_CREATED != 0;
            let Some(dir) = &c.parent else {
                // The watched path itself.
                let op = match (c.old, c.new) {
                    (Some(o), Some(n)) if o.ino == n.ino => o.change(&n).map(|op| (op, n)),
                    (Some(o), None) if moved_away.contains(&o.ino) || renamed => {
                        Some((FsOp::MovedFrom, o))
                    }
                    (Some(o), None) if !removed && c.hold.is_some() => {
                        self.hold(c, o);
                        None
                    }
                    (Some(o), _) => Some((FsOp::Delete, o)),
                    (None, _) => None,
                };
                if let Some((op, m)) = op {
                    self.emit(&mut out, &c.path, "", op, m, 0);
                }
                continue;
            };
            let name = c.path.file_name().unwrap().to_string_lossy();
            if let (Some(o), Some(n)) = (c.old, c.new)
                && o.ino == n.ino
            {
                if let Some(op) = o.change(&n) {
                    self.emit(&mut out, dir, &name, op, n, 0);
                }
                continue;
            }
            if let Some(o) = c.left() {
                match to_of.get(&i) {
                    Some(&j) => {
                        if done.insert(i) {
                            let cookie = self.next_cookie();
                            self.emit(&mut out, dir, &name, FsOp::MovedFrom, o, cookie);
                            let to = &changes[j];
                            let to_name = to.path.file_name().unwrap().to_string_lossy();
                            let to_dir = to.parent.as_ref().unwrap();
                            let n = to.arrived().unwrap();
                            self.emit(&mut out, to_dir, &to_name, FsOp::MovedTo, n, cookie);
                        }
                    }
                    None if c.new.is_none() && renamed => {
                        let cookie = self.next_cookie();
                        self.emit(&mut out, dir, &name, FsOp::MovedFrom, o, cookie);
                        self.moved_out.push((o.ino, cookie, now + HOLD));
                    }
                    None if c.new.is_none() && !removed && c.hold.is_some() => self.hold(c, o),
                    None if c.new.is_none() || !renamed || created => {
                        self.emit(&mut out, dir, &name, FsOp::Delete, o, 0);
                    }
                    None => {}
                }
            }
            if let Some(n) = c.arrived() {
                let moved_out = self.moved_out.iter().position(|m| m.0 == n.ino);
                match from_of.get(&i) {
                    Some(&j) => {
                        if done.insert(j) {
                            // The source comes later in the batch: emit both now.
                            let cookie = self.next_cookie();
                            let from = &changes[j];
                            let from_name = from.path.file_name().unwrap().to_string_lossy();
                            let from_dir = from.parent.as_ref().unwrap();
                            let o = from.left().unwrap();
                            self.emit(&mut out, from_dir, &from_name, FsOp::MovedFrom, o, cookie);
                            self.emit(&mut out, dir, &name, FsOp::MovedTo, n, cookie);
                        }
                    }
                    // The other half of a rename the last batch reported moved out.
                    None if moved_out.is_some() => {
                        let (_, cookie, _) = self.moved_out.swap_remove(moved_out.unwrap());
                        self.emit(&mut out, dir, &name, FsOp::MovedTo, n, cookie);
                    }
                    None if renamed && !created => {
                        let cookie = self.next_cookie();
                        self.emit(&mut out, dir, &name, FsOp::MovedTo, n, cookie);
                    }
                    None => {
                        self.emit(&mut out, dir, &name, FsOp::Create, n, 0);
                        if n.file && n.size > 0 {
                            self.emit(&mut out, dir, &name, FsOp::Modify, n, 0);
                        }
                    }
                }
            }
        }
        out
    }

    /// Wait for FSEvents to say why `c`'s path went.
    fn hold(&mut self, c: &Change, old: Meta) {
        debug(format_args!("hold {}", c.path.display()));
        self.held.push(Held {
            parent: c.parent.clone(),
            path: c.path.clone(),
            old,
            until: c.hold.unwrap(),
        });
    }

    fn emit(
        &self,
        out: &mut Vec<FsEvent>,
        node: &Path,
        name: &str,
        op: FsOp,
        m: Meta,
        cookie: u32,
    ) {
        for &id in self.nodes.get(node).map_or(&[][..], |n| &n.ids[..]) {
            out.push(FsEvent {
                id,
                ino: m.ino,
                cookie,
                op,
                dir: m.dir,
                name: name.to_owned(),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Write;

    use kat_proto::read_msg;

    use super::*;

    impl State {
        fn batch(&mut self, batch: Vec<fsevents::Event>) -> Vec<FsEvent> {
            self.process(batch, &[], Instant::now())
        }

        /// As if the hold ran out with no further events.
        fn later(&mut self) -> Vec<FsEvent> {
            self.process(vec![], &[], Instant::now() + HOLD)
        }
    }

    struct Tmp(PathBuf);

    impl Tmp {
        fn new(tag: &str) -> Tmp {
            let p = std::env::temp_dir().join(format!("kat-fswatch-{tag}-{}", std::process::id()));
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

    /// A state sharing `root` at /g in the guest, with /g watched as id 1.
    fn state(root: &Path) -> State {
        let mut st = State::new(vec![("/g".into(), root.to_path_buf())]);
        assert!(st.watch(1, true, "/g").is_some());
        st
    }

    fn ev(root: &Path, name: &str, f: u32) -> fsevents::Event {
        (root.join(name).display().to_string(), f)
    }

    fn ops(events: &[FsEvent]) -> Vec<(u64, String, FsOp)> {
        events
            .iter()
            .map(|e| (e.id, e.name.clone(), e.op))
            .collect()
    }

    fn op(id: u64, name: &str, op: FsOp) -> (u64, String, FsOp) {
        (id, name.into(), op)
    }

    #[test]
    fn create_modify_attrib_delete() {
        let t = Tmp::new("basic");
        let mut st = state(&t.0);
        fs::write(t.0.join("a"), "").unwrap();
        fs::write(t.0.join("b"), "x").unwrap();
        fs::create_dir(t.0.join("d")).unwrap();
        let out = st.batch(vec![
            ev(&t.0, "a", 0x100),
            ev(&t.0, "b", 0x100),
            ev(&t.0, "d", 0x100),
        ]);
        assert_eq!(
            ops(&out),
            [
                op(1, "a", FsOp::Create),
                op(1, "b", FsOp::Create),
                op(1, "b", FsOp::Modify),
                op(1, "d", FsOp::Create)
            ]
        );
        assert!(out[3].dir && !out[0].dir);
        assert_eq!(out[0].ino, fs::metadata(t.0.join("a")).unwrap().ino());

        // Repeated or stale events change nothing.
        assert!(st.batch(vec![ev(&t.0, "a", 0x100)]).is_empty());

        fs::OpenOptions::new()
            .append(true)
            .open(t.0.join("a"))
            .unwrap()
            .write_all(b"more")
            .unwrap();
        assert_eq!(
            ops(&st.batch(vec![ev(&t.0, "a", 0x1000)])),
            [op(1, "a", FsOp::Modify)]
        );

        fs::set_permissions(
            t.0.join("a"),
            std::os::unix::fs::PermissionsExt::from_mode(0o600),
        )
        .unwrap();
        assert_eq!(
            ops(&st.batch(vec![ev(&t.0, "a", 0x400)])),
            [op(1, "a", FsOp::Attrib)]
        );

        let ino = fs::metadata(t.0.join("a")).unwrap().ino();
        fs::remove_file(t.0.join("a")).unwrap();
        let out = st.batch(vec![ev(&t.0, "a", 0x200)]);
        assert_eq!(ops(&out), [op(1, "a", FsOp::Delete)]);
        assert_eq!(out[0].ino, ino);

        // Changes below a child directory are not the watched directory's.
        fs::write(t.0.join("d/deep"), "x").unwrap();
        assert!(st.batch(vec![ev(&t.0, "d/deep", 0x100)]).is_empty());
    }

    #[test]
    fn renames_pair_up() {
        let t = Tmp::new("rename");
        fs::write(t.0.join("old"), "x").unwrap();
        fs::write(t.0.join("target"), "old content").unwrap();
        let mut st = state(&t.0);

        fs::rename(t.0.join("old"), t.0.join("new")).unwrap();
        let out = st.batch(vec![ev(&t.0, "old", 0x800), ev(&t.0, "new", 0x800)]);
        assert_eq!(
            ops(&out),
            [op(1, "old", FsOp::MovedFrom), op(1, "new", FsOp::MovedTo)]
        );
        assert!(out[0].cookie != 0 && out[0].cookie == out[1].cookie);
        assert_eq!(out[0].ino, out[1].ino);

        // Destination named first: moved-from still comes first.
        fs::rename(t.0.join("new"), t.0.join("again")).unwrap();
        let out = st.batch(vec![ev(&t.0, "again", 0x800), ev(&t.0, "new", 0x800)]);
        assert_eq!(
            ops(&out),
            [op(1, "new", FsOp::MovedFrom), op(1, "again", FsOp::MovedTo)]
        );

        // Removed after a rename: the path still says renamed.
        fs::remove_file(t.0.join("again")).unwrap();
        let out = st.batch(vec![ev(&t.0, "again", 0xa00)]);
        assert_eq!(ops(&out), [op(1, "again", FsOp::Delete)]);

        // An editor's atomic save: a temporary file renamed over the target.
        fs::write(t.0.join(".target.swp"), "new content").unwrap();
        st.batch(vec![ev(&t.0, ".target.swp", 0x100)]);
        fs::rename(t.0.join(".target.swp"), t.0.join("target")).unwrap();
        let out = st.batch(vec![
            ev(&t.0, ".target.swp", 0x800),
            ev(&t.0, "target", 0x800),
        ]);
        assert_eq!(
            ops(&out),
            [
                op(1, ".target.swp", FsOp::MovedFrom),
                op(1, "target", FsOp::MovedTo)
            ]
        );

        // The temporary file never seen: the target is replaced.
        fs::write(t.0.join("tmp2"), "v3").unwrap();
        fs::rename(t.0.join("tmp2"), t.0.join("target")).unwrap();
        let out = st.batch(vec![ev(&t.0, "tmp2", 0x900), ev(&t.0, "target", 0x800)]);
        assert_eq!(ops(&out), [op(1, "target", FsOp::MovedTo)]);
    }

    /// Flags from a VM test run: an event from before a rename (a chmod)
    /// naming the source after it was renamed, then the rename's own events.
    const STALE: u32 = 0x1d500;
    const RENAMED_FROM: u32 = 0x1dd00;
    const RENAMED_TO: u32 = 0x10800;

    #[track_caller]
    fn moved(out: &[FsEvent], from: &str, to: &str) {
        assert_eq!(
            ops(out),
            [op(1, from, FsOp::MovedFrom), op(1, to, FsOp::MovedTo)]
        );
        assert!(out[0].cookie != 0 && out[0].cookie == out[1].cookie);
        assert_eq!(out[0].ino, out[1].ino);
    }

    #[test]
    fn a_late_event_for_a_renamed_path_waits_for_the_rename() {
        let t = Tmp::new("late");
        fs::write(t.0.join("a"), "x").unwrap();
        let mut st = state(&t.0);

        fs::rename(t.0.join("a"), t.0.join("b")).unwrap();
        assert!(st.batch(vec![ev(&t.0, "a", STALE)]).is_empty());
        assert!(st.held_until().is_some());
        let out = st.batch(vec![ev(&t.0, "a", RENAMED_FROM), ev(&t.0, "b", RENAMED_TO)]);
        moved(&out, "a", "b");
        assert!(st.held_until().is_none());
        assert!(st.later().is_empty());

        // The destination named first.
        fs::rename(t.0.join("b"), t.0.join("c")).unwrap();
        assert!(st.batch(vec![ev(&t.0, "b", STALE)]).is_empty());
        let out = st.batch(vec![ev(&t.0, "c", RENAMED_TO), ev(&t.0, "b", RENAMED_FROM)]);
        moved(&out, "b", "c");

        // The destination alone, the source's own event in a later batch.
        fs::rename(t.0.join("c"), t.0.join("d")).unwrap();
        assert!(st.batch(vec![ev(&t.0, "c", STALE)]).is_empty());
        moved(&st.batch(vec![ev(&t.0, "d", RENAMED_TO)]), "c", "d");
        assert!(st.batch(vec![ev(&t.0, "c", RENAMED_FROM)]).is_empty());

        // The source's own event alone, the destination in a later batch.
        fs::rename(t.0.join("d"), t.0.join("e")).unwrap();
        assert!(st.batch(vec![ev(&t.0, "d", STALE)]).is_empty());
        let from = st.batch(vec![ev(&t.0, "d", RENAMED_FROM)]);
        let to = st.batch(vec![ev(&t.0, "e", RENAMED_TO)]);
        moved(&[from, to].concat(), "d", "e");
        assert!(st.held_until().is_none());
        assert!(st.later().is_empty());
    }

    #[test]
    fn renames_split_across_batches_keep_their_cookie() {
        let t = Tmp::new("split");
        fs::write(t.0.join("a"), "x").unwrap();
        let mut st = state(&t.0);

        // Source in one batch, destination in the next.
        fs::rename(t.0.join("a"), t.0.join("b")).unwrap();
        let from = st.batch(vec![ev(&t.0, "a", RENAMED_FROM)]);
        let to = st.batch(vec![ev(&t.0, "b", RENAMED_TO)]);
        moved(&[from, to].concat(), "a", "b");

        // Destination first: the source is found by inode.
        fs::rename(t.0.join("b"), t.0.join("c")).unwrap();
        moved(&st.batch(vec![ev(&t.0, "c", RENAMED_TO)]), "b", "c");
        assert!(st.batch(vec![ev(&t.0, "b", RENAMED_FROM)]).is_empty());

        // Renamed over another file, the source's event still to come.
        fs::write(t.0.join("tmp"), "new").unwrap();
        st.batch(vec![ev(&t.0, "tmp", 0x10100)]);
        fs::rename(t.0.join("tmp"), t.0.join("c")).unwrap();
        moved(&st.batch(vec![ev(&t.0, "c", RENAMED_TO)]), "tmp", "c");
        assert!(st.batch(vec![ev(&t.0, "tmp", RENAMED_FROM)]).is_empty());

        // An atomic save with the target named before the temporary file.
        fs::write(t.0.join("tmp"), "newer").unwrap();
        st.batch(vec![ev(&t.0, "tmp", 0x10100)]);
        fs::rename(t.0.join("tmp"), t.0.join("c")).unwrap();
        let out = st.batch(vec![
            ev(&t.0, "c", RENAMED_TO),
            ev(&t.0, "tmp", RENAMED_FROM),
        ]);
        moved(&out, "tmp", "c");

        // A moved-out cookie is not kept forever.
        fs::rename(t.0.join("c"), t.0.join("f")).unwrap();
        let from = st.batch(vec![ev(&t.0, "c", RENAMED_FROM)]);
        assert_eq!(ops(&from), [op(1, "c", FsOp::MovedFrom)]);
        st.later();
        let to = st.process(vec![ev(&t.0, "f", RENAMED_TO)], &[], Instant::now() + HOLD);
        assert_eq!(ops(&to), [op(1, "f", FsOp::MovedTo)]);
        assert_ne!(from[0].cookie, to[0].cookie);
    }

    #[test]
    fn a_late_event_for_a_path_moved_out_or_deleted() {
        let t = Tmp::new("gone");
        let away = Tmp::new("gone-away");
        for f in ["a", "b", "c", "d"] {
            fs::write(t.0.join(f), "x").unwrap();
        }
        let mut st = state(&t.0);

        // Moved out of the watched tree.
        fs::rename(t.0.join("a"), away.0.join("a")).unwrap();
        assert!(st.batch(vec![ev(&t.0, "a", STALE)]).is_empty());
        let out = st.batch(vec![ev(&t.0, "a", RENAMED_FROM)]);
        assert_eq!(ops(&out), [op(1, "a", FsOp::MovedFrom)]);
        assert_ne!(out[0].cookie, 0);

        // Deleted: the following event says so.
        fs::remove_file(t.0.join("b")).unwrap();
        assert!(st.batch(vec![ev(&t.0, "b", STALE)]).is_empty());
        let out = st.batch(vec![ev(&t.0, "b", 0x1d700)]);
        assert_eq!(ops(&out), [op(1, "b", FsOp::Delete)]);

        // A delete that says so goes out at once.
        fs::remove_file(t.0.join("c")).unwrap();
        let out = st.batch(vec![ev(&t.0, "c", 0x10200)]);
        assert_eq!(ops(&out), [op(1, "c", FsOp::Delete)]);

        // Gone, and nothing ever says why: deleted once the hold is up.
        fs::remove_file(t.0.join("d")).unwrap();
        assert!(st.batch(vec![ev(&t.0, "d", STALE)]).is_empty());
        assert!(st.batch(vec![ev(&t.0, "d", STALE)]).is_empty());
        assert!(st.batch(vec![]).is_empty());
        assert_eq!(ops(&st.later()), [op(1, "d", FsOp::Delete)]);
        assert!(st.held_until().is_none());

        // Gone and back as another file before its next event.
        fs::write(t.0.join("e"), "x").unwrap();
        st.batch(vec![ev(&t.0, "e", 0x10100)]);
        fs::remove_file(t.0.join("e")).unwrap();
        assert!(st.batch(vec![ev(&t.0, "e", STALE)]).is_empty());
        fs::write(t.0.join("e"), "").unwrap();
        let out = st.batch(vec![ev(&t.0, "e", STALE)]);
        assert_eq!(
            ops(&out),
            [op(1, "e", FsOp::Delete), op(1, "e", FsOp::Create)]
        );
    }

    #[test]
    fn a_watched_file_with_a_late_event() {
        let t = Tmp::new("self-late");
        fs::write(t.0.join("f"), "x").unwrap();
        let mut st = state(&t.0);
        assert!(st.watch(2, false, "/g/f").is_some());

        fs::rename(t.0.join("f"), t.0.join("g")).unwrap();
        assert!(st.batch(vec![ev(&t.0, "f", STALE)]).is_empty());
        let out = st.batch(vec![ev(&t.0, "f", RENAMED_FROM), ev(&t.0, "g", RENAMED_TO)]);
        assert_eq!(
            ops(&out),
            [
                op(1, "f", FsOp::MovedFrom),
                op(1, "g", FsOp::MovedTo),
                op(2, "", FsOp::MovedFrom)
            ]
        );
        assert!(st.later().is_empty());
    }

    #[test]
    fn changes_between_the_guest_watch_and_the_snapshot_are_not_lost() {
        let t = Tmp::new("fresh");
        fs::write(t.0.join("old"), "x").unwrap();
        fs::write(t.0.join("touched"), "x").unwrap();
        std::thread::sleep(Duration::from_millis(100));
        let mut st = State::new(vec![("/g".into(), t.0.clone())]);
        // The guest watched just now; these happened before the host's snapshot.
        fs::write(t.0.join("new"), "").unwrap();
        fs::write(t.0.join("touched"), "xy").unwrap();
        st.watch(1, true, "/g");
        let out = st.batch(vec![
            ev(&t.0, "new", 0x100),
            ev(&t.0, "touched", 0x1000),
            ev(&t.0, "old", 0),
        ]);
        assert_eq!(
            ops(&out),
            [op(1, "new", FsOp::Create), op(1, "touched", FsOp::Modify)]
        );
        // Only the first look counts.
        assert!(st.batch(vec![ev(&t.0, "new", 0x100)]).is_empty());

        // Made after the watch, then changed after the snapshot: still new.
        let mut st = State::new(vec![("/g".into(), t.0.clone())]);
        fs::write(t.0.join("newer"), "").unwrap();
        st.watch(1, true, "/g");
        fs::set_permissions(
            t.0.join("newer"),
            std::os::unix::fs::PermissionsExt::from_mode(0o600),
        )
        .unwrap();
        let out = st.batch(vec![ev(&t.0, "newer", 0x18100)]);
        assert_eq!(ops(&out), [op(1, "newer", FsOp::Create)]);
    }

    #[test]
    fn a_new_watch_is_looked_at_again_without_events() {
        let t = Tmp::new("again");
        std::thread::sleep(Duration::from_millis(100));
        let mut st = State::new(vec![("/g".into(), t.0.clone())]);
        // Made after the guest's watch, before the host's snapshot, and
        // delivered by FSEvents before the stream was up: no event comes.
        fs::write(t.0.join("early"), "").unwrap();
        let path = st.watch(1, true, "/g").unwrap();
        fs::write(t.0.join("late"), "").unwrap();
        let mut out = ops(&st.process(vec![], &[path], Instant::now()));
        out.sort_by(|a, b| a.1.cmp(&b.1));
        assert_eq!(
            out,
            [op(1, "early", FsOp::Create), op(1, "late", FsOp::Create)]
        );
    }

    #[test]
    fn lost_events_rescan_the_directory() {
        let t = Tmp::new("rescan");
        fs::write(t.0.join("gone"), "x").unwrap();
        let mut st = state(&t.0);
        fs::remove_file(t.0.join("gone")).unwrap();
        fs::write(t.0.join("new"), "").unwrap();
        let mut out = ops(&st.batch(vec![(t.0.display().to_string(), flag::MUST_SCAN_SUB_DIRS)]));
        out.sort_by(|a, b| a.1.cmp(&b.1));
        assert_eq!(
            out,
            [op(1, "gone", FsOp::Delete), op(1, "new", FsOp::Create)]
        );
    }

    #[test]
    fn watched_files_get_their_own_events() {
        let t = Tmp::new("self");
        fs::write(t.0.join("f"), "x").unwrap();
        let mut st = state(&t.0);
        assert!(st.watch(2, false, "/g/f").is_some());
        assert_eq!(st.roots(), [t.0.display().to_string()]);

        fs::write(t.0.join("f"), "xy").unwrap();
        let out = ops(&st.batch(vec![ev(&t.0, "f", 0x1000)]));
        assert_eq!(out, [op(1, "f", FsOp::Modify), op(2, "", FsOp::Modify)]);

        fs::remove_file(t.0.join("f")).unwrap();
        let out = ops(&st.batch(vec![ev(&t.0, "f", 0x200)]));
        assert_eq!(out, [op(1, "f", FsOp::Delete), op(2, "", FsOp::Delete)]);

        assert!(st.unwatch(2));
        assert!(st.unwatch(1));
        assert!(st.roots().is_empty());
    }

    #[test]
    fn roots_cover_nested_watches_once() {
        let t = Tmp::new("roots");
        fs::create_dir_all(t.0.join("a/b")).unwrap();
        fs::create_dir_all(t.0.join("c")).unwrap();
        fs::write(t.0.join("c/file"), "").unwrap();
        let mut st = State::new(vec![("/g".into(), t.0.clone())]);
        st.watch(1, true, "/g/a/b");
        st.watch(2, true, "/g/a");
        st.watch(3, false, "/g/c/file");
        let root = |p: &str| t.0.join(p).display().to_string();
        assert_eq!(st.roots(), [root("a"), root("c")]);
        assert!(st.host_path("/elsewhere/x").is_none());
        assert!(st.host_path("/gx").is_none());
        // A re-sent watch id moves; a reset forgets everything.
        st.watch(2, true, "/g/c");
        assert_eq!(st.roots(), [root("a/b"), root("c")]);
        assert!(st.reset());
        assert!(st.roots().is_empty());
    }

    #[test]
    fn noisy_trees_are_excluded_unless_watched() {
        let t = Tmp::new("exclude");
        fs::create_dir_all(t.0.join(".git/objects")).unwrap();
        let mut st = state(&t.0);
        let roots = st.roots();
        assert_eq!(
            st.exclusions(&roots),
            [t.0.join(".git/objects").display().to_string()]
        );
        st.watch(2, true, "/g/.git/objects");
        assert!(st.exclusions(&roots).is_empty());
    }

    #[test]
    fn watcher_sends_events_from_fsevents() {
        let t = Tmp::new("watcher");
        let (host, mut guest) = UnixStream::pair().unwrap();
        let w = Watcher::start(vec![("/g".into(), t.0.clone())], host);
        w.handle(Msg::FsWatch {
            id: 9,
            dir: true,
            path: "/g".into(),
        });
        // The stream starts from the event id at the watch, so a change made
        // before the stream is up is not lost.
        std::thread::sleep(Duration::from_millis(5));
        fs::write(t.0.join("hello"), "").unwrap();
        guest
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let Some(Msg::FsEvents(events)) = read_msg(&mut guest).unwrap() else {
            panic!("expected FsEvents");
        };
        assert_eq!(ops(&events), [op(9, "hello", FsOp::Create)]);
        w.stop();
    }
}
