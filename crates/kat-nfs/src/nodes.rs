//! The node table: every file handle the server has handed out.
//!
//! The export is a small virtual tree (`/`, `docker`, `docker/volumes`,
//! `docker/containers`, `machines`) whose leaves are *anchors*: real guest
//! directories such as a volume's `_data` or a container's root. Below an
//! anchor, a node is its parent plus a name, so a path is rebuilt by walking
//! up to the anchor, and renaming a directory moves everything under it at
//! once. Each real node also remembers its `(dev, ino)`; an operation checks
//! that the path still leads to that inode and answers `STALE` if not.
//!
//! The root also holds a few empty read-only virtual files that keep
//! Spotlight out of the mount.
//!
//! A handle is 16 bytes: an instance number and a node id. Virtual
//! directories and files use instance 0 and fixed ids, so the mount's root
//! handle survives a server restart; every other handle names this instance
//! and goes stale when the server restarts or the table is reset.

use std::collections::HashMap;
use std::path::PathBuf;

pub type Id = u64;
pub type Ident = (u64, u64);

pub const HANDLE_LEN: usize = 16;
/// Nodes kept before the table starts over with a new instance.
pub const MAX_NODES: usize = 1 << 20;
/// Deepest path rebuilt; deeper (or cyclic) chains answer `STALE`.
const MAX_DEPTH: usize = 4096;
const FIRST_DYNAMIC: Id = 16;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum VDir {
    Root = 1,
    Docker = 2,
    Volumes = 3,
    Containers = 4,
    Machines = 5,
}

impl VDir {
    pub const ALL: [VDir; 5] = [
        VDir::Root,
        VDir::Docker,
        VDir::Volumes,
        VDir::Containers,
        VDir::Machines,
    ];

    pub fn id(self) -> Id {
        self as Id
    }

    pub fn from_id(id: Id) -> Option<VDir> {
        VDir::ALL.into_iter().find(|v| v.id() == id)
    }

    pub fn parent(self) -> VDir {
        match self {
            VDir::Root | VDir::Docker | VDir::Machines => VDir::Root,
            VDir::Volumes | VDir::Containers => VDir::Docker,
        }
    }

    /// Fixed subdirectories, by name.
    pub fn subdirs(self) -> &'static [(&'static str, VDir)] {
        match self {
            VDir::Root => &[("docker", VDir::Docker), ("machines", VDir::Machines)],
            VDir::Docker => &[("containers", VDir::Containers), ("volumes", VDir::Volumes)],
            _ => &[],
        }
    }
}

/// Empty read-only files in the root: their names tell Spotlight never to
/// index the mount.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum VFile {
    NeverIndex = 6,
    NeverIndexUnlessRootfs = 7,
    NeverIndexDash = 8,
}

impl VFile {
    pub const ALL: [VFile; 3] = [
        VFile::NeverIndex,
        VFile::NeverIndexUnlessRootfs,
        VFile::NeverIndexDash,
    ];

    pub fn id(self) -> Id {
        self as Id
    }

    pub fn from_id(id: Id) -> Option<VFile> {
        VFile::ALL.into_iter().find(|f| f.id() == id)
    }

    pub fn name(self) -> &'static str {
        match self {
            VFile::NeverIndex => ".metadata_never_index",
            VFile::NeverIndexUnlessRootfs => ".metadata_never_index_unless_rootfs",
            VFile::NeverIndexDash => ".metadata-never-index",
        }
    }

    /// The files in a virtual directory.
    pub fn in_dir(v: VDir) -> &'static [VFile] {
        match v {
            VDir::Root => &VFile::ALL,
            _ => &[],
        }
    }
}

/// Whether `id` is a fixed node, the same in every instance.
fn fixed(id: Id) -> bool {
    VDir::from_id(id).is_some() || VFile::from_id(id).is_some()
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum AnchorKey {
    Volume(String),
    Container(String),
    Machine(String),
}

impl AnchorKey {
    pub fn vdir(&self) -> VDir {
        match self {
            AnchorKey::Volume(_) => VDir::Volumes,
            AnchorKey::Container(_) => VDir::Containers,
            AnchorKey::Machine(_) => VDir::Machines,
        }
    }

    pub fn name(&self) -> &str {
        match self {
            AnchorKey::Volume(n) | AnchorKey::Container(n) | AnchorKey::Machine(n) => n,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Kind {
    Virtual(VDir),
    VirtualFile(VFile),
    Anchor { key: AnchorKey, base: PathBuf },
    Child { parent: Id, name: Vec<u8> },
}

#[derive(Clone, Debug)]
pub struct Node {
    pub kind: Kind,
    pub ident: Ident,
}

/// Where a real node lives: an anchor's directory and the names below it.
#[derive(Debug, PartialEq, Eq)]
pub struct Path {
    pub anchor: Id,
    pub base: PathBuf,
    pub anchor_ident: Ident,
    pub names: Vec<Vec<u8>>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum HandleError {
    /// Not one of ours at all.
    Bad,
    /// Ours, but from another instance or forgotten.
    Stale,
}

pub struct Nodes {
    instance: u64,
    nodes: HashMap<Id, Node>,
    by_ident: HashMap<Ident, Id>,
    anchors: HashMap<AnchorKey, Id>,
    next: Id,
    max: usize,
}

impl Nodes {
    pub fn new(instance: u64) -> Nodes {
        Nodes::with_capacity(instance, MAX_NODES)
    }

    pub fn with_capacity(instance: u64, max: usize) -> Nodes {
        Nodes {
            instance: instance.max(1),
            nodes: HashMap::new(),
            by_ident: HashMap::new(),
            anchors: HashMap::new(),
            next: FIRST_DYNAMIC,
            max,
        }
    }

    pub fn instance(&self) -> u64 {
        self.instance
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn handle(&self, id: Id) -> [u8; HANDLE_LEN] {
        let instance = if fixed(id) { 0 } else { self.instance };
        let mut h = [0u8; HANDLE_LEN];
        h[..8].copy_from_slice(&instance.to_be_bytes());
        h[8..].copy_from_slice(&id.to_be_bytes());
        h
    }

    pub fn resolve(&self, fh: &[u8]) -> Result<Id, HandleError> {
        let fh: &[u8; HANDLE_LEN] = fh.try_into().map_err(|_| HandleError::Bad)?;
        let instance = u64::from_be_bytes(fh[..8].try_into().unwrap());
        let id = u64::from_be_bytes(fh[8..].try_into().unwrap());
        if instance == 0 {
            return if fixed(id) {
                Ok(id)
            } else {
                Err(HandleError::Bad)
            };
        }
        if instance == self.instance && self.nodes.contains_key(&id) {
            Ok(id)
        } else {
            Err(HandleError::Stale)
        }
    }

    pub fn kind(&self, id: Id) -> Option<Kind> {
        if let Some(v) = VDir::from_id(id) {
            return Some(Kind::Virtual(v));
        }
        if let Some(f) = VFile::from_id(id) {
            return Some(Kind::VirtualFile(f));
        }
        self.nodes.get(&id).map(|n| n.kind.clone())
    }

    pub fn ident(&self, id: Id) -> Option<Ident> {
        self.nodes.get(&id).map(|n| n.ident)
    }

    /// Start over when full: every dynamic handle goes stale.
    fn make_room(&mut self) {
        if self.nodes.len() < self.max {
            return;
        }
        self.nodes.clear();
        self.by_ident.clear();
        self.anchors.clear();
        self.instance = self.instance.wrapping_add(1).max(1);
    }

    fn insert(&mut self, node: Node) -> Id {
        let id = self.next;
        self.next += 1;
        self.by_ident.insert(node.ident, id);
        self.nodes.insert(id, node);
        id
    }

    /// The node for an anchor, created or updated to `base` and `ident`.
    pub fn anchor(&mut self, key: AnchorKey, base: PathBuf, ident: Ident) -> Id {
        if let Some(&id) = self.anchors.get(&key) {
            let node = self.nodes.get_mut(&id).expect("anchor in table");
            if node.ident != ident {
                if self.by_ident.get(&node.ident) == Some(&id) {
                    self.by_ident.remove(&node.ident);
                }
                node.ident = ident;
            }
            node.kind = Kind::Anchor { key, base };
            self.by_ident.insert(ident, id);
            return id;
        }
        self.make_room();
        let id = self.insert(Node {
            kind: Kind::Anchor {
                key: key.clone(),
                base,
            },
            ident,
        });
        self.anchors.insert(key, id);
        id
    }

    /// The node for `name` in directory `parent`, whose inode is `ident`.
    /// An inode already known (a hard link, or the same directory reached
    /// through a bind mount) keeps its node; a plain node moves to the name
    /// it was last seen under. `None` if `parent` is no longer known.
    pub fn child(&mut self, parent: Id, name: &[u8], ident: Ident) -> Option<Id> {
        if VDir::from_id(parent).is_none() && !self.nodes.contains_key(&parent) {
            return None;
        }
        if let Some(&id) = self.by_ident.get(&ident) {
            let cyclic = self.is_ancestor(id, parent);
            let node = self.nodes.get_mut(&id).expect("indexed node in table");
            if let Kind::Child { .. } = node.kind
                && !cyclic
            {
                node.kind = Kind::Child {
                    parent,
                    name: name.to_vec(),
                };
            }
            return Some(id);
        }
        self.make_room();
        if !self.nodes.contains_key(&parent) && VDir::from_id(parent).is_none() {
            return None;
        }
        Some(self.insert(Node {
            kind: Kind::Child {
                parent,
                name: name.to_vec(),
            },
            ident,
        }))
    }

    /// Whether `a` is `b` or one of its ancestors.
    fn is_ancestor(&self, a: Id, mut b: Id) -> bool {
        for _ in 0..MAX_DEPTH {
            if a == b {
                return true;
            }
            match self.nodes.get(&b).map(|n| &n.kind) {
                Some(Kind::Child { parent, .. }) => b = *parent,
                _ => return false,
            }
        }
        true
    }

    pub fn forget(&mut self, id: Id) {
        if let Some(node) = self.nodes.get(&id)
            && matches!(node.kind, Kind::Child { .. })
        {
            let ident = node.ident;
            self.nodes.remove(&id);
            if self.by_ident.get(&ident) == Some(&id) {
                self.by_ident.remove(&ident);
            }
        }
    }

    /// Forget the node known under an inode, if it is a plain node.
    pub fn forget_ident(&mut self, ident: Ident) {
        if let Some(&id) = self.by_ident.get(&ident) {
            self.forget(id);
        }
    }

    /// After a rename: the node for `ident` now lives at `parent`/`name`.
    pub fn moved(&mut self, ident: Ident, parent: Id, name: &[u8]) {
        if let Some(&id) = self.by_ident.get(&ident)
            && !self.is_ancestor(id, parent)
            && let Some(node) = self.nodes.get_mut(&id)
            && let Kind::Child { .. } = node.kind
        {
            node.kind = Kind::Child {
                parent,
                name: name.to_vec(),
            };
        }
    }

    /// The parent directory, as `..` sees it.
    pub fn parent(&self, id: Id) -> Option<Id> {
        match self.kind(id)? {
            Kind::Virtual(v) => Some(v.parent().id()),
            Kind::VirtualFile(_) => Some(VDir::Root.id()),
            Kind::Anchor { key, .. } => Some(key.vdir().id()),
            Kind::Child { parent, .. } => Some(parent),
        }
    }

    /// Rebuild a real node's path. `None` for virtual nodes and for chains
    /// that no longer reach an anchor.
    pub fn path(&self, id: Id) -> Option<Path> {
        let mut names = Vec::new();
        let mut cur = id;
        for _ in 0..MAX_DEPTH {
            let node = self.nodes.get(&cur)?;
            match &node.kind {
                Kind::Child { parent, name } => {
                    names.push(name.clone());
                    cur = *parent;
                }
                Kind::Anchor { base, .. } => {
                    names.reverse();
                    return Some(Path {
                        anchor: cur,
                        base: base.clone(),
                        anchor_ident: node.ident,
                        names,
                    });
                }
                Kind::Virtual(_) | Kind::VirtualFile(_) => return None,
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vol(n: &str) -> AnchorKey {
        AnchorKey::Volume(n.into())
    }

    #[test]
    fn virtual_handles_survive_restarts() {
        let a = Nodes::new(7);
        let b = Nodes::new(8);
        let h = a.handle(VDir::Volumes.id());
        assert_eq!(&h[..8], &[0; 8]);
        assert_eq!(b.resolve(&h), Ok(VDir::Volumes.id()));
        assert_eq!(b.resolve(&[0; 15]), Err(HandleError::Bad));
        let mut bad = [0u8; 16];
        bad[15] = 99;
        assert_eq!(b.resolve(&bad), Err(HandleError::Bad));
    }

    #[test]
    fn dynamic_handles_are_per_instance() {
        let mut a = Nodes::new(7);
        let v = a.anchor(vol("db"), "/v/db/_data".into(), (1, 2));
        let h = a.handle(v);
        assert_eq!(a.resolve(&h), Ok(v));
        assert_eq!(Nodes::new(8).resolve(&h), Err(HandleError::Stale));
        a.forget(v); // anchors are not forgotten this way
        assert_eq!(a.resolve(&h), Ok(v));
    }

    #[test]
    fn paths_rebuild_through_parents() {
        let mut n = Nodes::new(1);
        let v = n.anchor(vol("db"), "/v/db/_data".into(), (1, 2));
        let d = n.child(v, b"dir", (1, 3)).unwrap();
        let f = n.child(d, b"file", (1, 4)).unwrap();
        let p = n.path(f).unwrap();
        assert_eq!(p.base, PathBuf::from("/v/db/_data"));
        assert_eq!(p.anchor, v);
        assert_eq!(p.names, vec![b"dir".to_vec(), b"file".to_vec()]);
        assert_eq!(n.path(v).unwrap().names, Vec::<Vec<u8>>::new());
        assert_eq!(n.path(VDir::Root.id()), None);
        assert_eq!(n.parent(f), Some(d));
        assert_eq!(n.parent(v), Some(VDir::Volumes.id()));
        assert_eq!(n.parent(VDir::Volumes.id()), Some(VDir::Docker.id()));
        assert_eq!(n.parent(VDir::Root.id()), Some(VDir::Root.id()));
    }

    #[test]
    fn same_inode_keeps_its_node_and_follows_renames() {
        let mut n = Nodes::new(1);
        let v = n.anchor(vol("a"), "/a".into(), (1, 1));
        let d1 = n.child(v, b"d1", (1, 2)).unwrap();
        let d2 = n.child(v, b"d2", (1, 3)).unwrap();
        let f = n.child(d1, b"f", (1, 4)).unwrap();
        // Renamed by someone else and looked up again under the new name.
        assert_eq!(n.child(d2, b"g", (1, 4)), Some(f));
        assert_eq!(
            n.path(f).unwrap().names,
            vec![b"d2".to_vec(), b"g".to_vec()]
        );
        // A rename we made moves the whole subtree.
        n.moved((1, 3), d1, b"inner");
        assert_eq!(
            n.path(f).unwrap().names,
            vec![b"d1".to_vec(), b"inner".to_vec(), b"g".to_vec()]
        );
        assert_eq!(n.parent(d2), Some(d1));
    }

    #[test]
    fn moves_that_would_make_cycles_are_ignored() {
        let mut n = Nodes::new(1);
        let v = n.anchor(vol("a"), "/a".into(), (1, 1));
        let d = n.child(v, b"d", (1, 2)).unwrap();
        let sub = n.child(d, b"sub", (1, 3)).unwrap();
        // `d` seen again inside its own subtree (a bind mount of itself).
        assert_eq!(n.child(sub, b"again", (1, 2)), Some(d));
        assert_eq!(n.path(d).unwrap().names, vec![b"d".to_vec()]);
        n.moved((1, 2), sub, b"x");
        assert_eq!(n.path(d).unwrap().names, vec![b"d".to_vec()]);
    }

    #[test]
    fn anchors_reached_twice_stay_anchors() {
        let mut n = Nodes::new(1);
        let v = n.anchor(vol("data"), "/v/data/_data".into(), (1, 9));
        let c = n.anchor(
            AnchorKey::Container("web".into()),
            "/proc/1/root".into(),
            (2, 1),
        );
        // The container mounts the volume at /data.
        assert_eq!(n.child(c, b"data", (1, 9)), Some(v));
        assert!(matches!(n.kind(v), Some(Kind::Anchor { .. })));
        // The volume was recreated under the same name.
        assert_eq!(n.anchor(vol("data"), "/v/data/_data".into(), (1, 10)), v);
        assert_eq!(n.ident(v), Some((1, 10)));
    }

    #[test]
    fn forgotten_and_orphaned_nodes() {
        let mut n = Nodes::new(1);
        let v = n.anchor(vol("a"), "/a".into(), (1, 1));
        let f = n.child(v, b"f", (1, 2)).unwrap();
        let h = n.handle(f);
        n.forget_ident((1, 2));
        assert_eq!(n.resolve(&h), Err(HandleError::Stale));
        assert_eq!(n.child(999, b"x", (1, 5)), None);
    }

    #[test]
    fn a_full_table_starts_over() {
        let mut n = Nodes::with_capacity(1, 3);
        let v = n.anchor(vol("a"), "/a".into(), (1, 1));
        let a = n.child(v, b"a", (1, 2)).unwrap();
        n.child(v, b"b", (1, 3)).unwrap();
        let h = n.handle(a);
        // The fourth node resets the table; the parent is gone with it.
        assert_eq!(n.child(v, b"c", (1, 4)), None);
        assert_eq!(n.resolve(&h), Err(HandleError::Stale));
        assert_ne!(n.instance(), 1);
        assert!(n.is_empty());
        assert_eq!(n.resolve(&n.handle(VDir::Root.id())), Ok(VDir::Root.id()));
    }
}
