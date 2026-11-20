//! What the export shows: `docker/volumes/<name>` is a volume's data,
//! `docker/containers/<name>` a running container's root filesystem, and
//! `machines/<name>` a Linux machine's root filesystem.

use std::path::PathBuf;

use crate::nodes::{AnchorKey, VDir};

/// Running containers, as (name, root directory) pairs.
pub trait Containers: Send + Sync {
    fn list(&self) -> Vec<(String, PathBuf)>;
}

/// No containers (tests, or no Docker).
pub struct NoContainers;

impl Containers for NoContainers {
    fn list(&self) -> Vec<(String, PathBuf)> {
        Vec::new()
    }
}

/// A fixed list (tests).
pub struct FixedContainers(pub Vec<(String, PathBuf)>);

impl Containers for FixedContainers {
    fn list(&self) -> Vec<(String, PathBuf)> {
        self.0.clone()
    }
}

pub struct Layout {
    /// Docker's volumes directory: `<name>/_data` per volume.
    pub volumes: PathBuf,
    pub containers: Box<dyn Containers>,
    /// Linux machines: `<name>/rootfs` per machine.
    pub machines: PathBuf,
}

/// A name usable as an export directory entry.
fn entry_name_ok(name: &str) -> bool {
    crate::fsops::name_ok(name.as_bytes()) && !name.starts_with('.')
}

impl Layout {
    /// The guest's Docker: `/var/lib/docker/volumes` and the containers the
    /// Docker API lists.
    pub fn docker() -> Layout {
        Layout {
            volumes: PathBuf::from("/var/lib/docker/volumes"),
            containers: Box::new(crate::docker::DockerContainers::default()),
            machines: PathBuf::from("/var/lib/katamaran/machines"),
        }
    }

    fn volume_base(&self, name: &str) -> Option<PathBuf> {
        if !entry_name_ok(name) {
            return None;
        }
        let base = self.volumes.join(name).join("_data");
        let meta = std::fs::symlink_metadata(&base).ok()?;
        meta.is_dir().then_some(base)
    }

    fn volumes(&self) -> Vec<(AnchorKey, PathBuf)> {
        let Ok(dir) = std::fs::read_dir(&self.volumes) else {
            return Vec::new();
        };
        let mut out: Vec<_> = dir
            .filter_map(|e| e.ok()?.file_name().into_string().ok())
            .filter_map(|n| Some((self.volume_base(&n)?, n)))
            .map(|(base, n)| (AnchorKey::Volume(n), base))
            .collect();
        out.sort_by(|a, b| a.0.name().cmp(b.0.name()));
        out
    }

    fn machine_base(&self, name: &str) -> Option<PathBuf> {
        if !entry_name_ok(name) {
            return None;
        }
        let base = self.machines.join(name).join("rootfs");
        let meta = std::fs::symlink_metadata(&base).ok()?;
        meta.is_dir().then_some(base)
    }

    fn machines(&self) -> Vec<(AnchorKey, PathBuf)> {
        let Ok(dir) = std::fs::read_dir(&self.machines) else {
            return Vec::new();
        };
        let mut out: Vec<_> = dir
            .filter_map(|e| e.ok()?.file_name().into_string().ok())
            .filter_map(|n| Some((self.machine_base(&n)?, n)))
            .map(|(base, n)| (AnchorKey::Machine(n), base))
            .collect();
        out.sort_by(|a, b| a.0.name().cmp(b.0.name()));
        out
    }

    fn containers(&self) -> Vec<(AnchorKey, PathBuf)> {
        let mut out: Vec<_> = self
            .containers
            .list()
            .into_iter()
            .filter(|(n, _)| entry_name_ok(n))
            .map(|(n, root)| (AnchorKey::Container(n), root))
            .collect();
        out.sort_by(|a, b| a.0.name().cmp(b.0.name()));
        out.dedup_by(|a, b| a.0 == b.0);
        out
    }

    /// The anchors listed in a virtual directory.
    pub fn anchors(&self, v: VDir) -> Vec<(AnchorKey, PathBuf)> {
        match v {
            VDir::Volumes => self.volumes(),
            VDir::Containers => self.containers(),
            VDir::Machines => self.machines(),
            _ => Vec::new(),
        }
    }

    /// One anchor by name.
    pub fn find(&self, v: VDir, name: &str) -> Option<(AnchorKey, PathBuf)> {
        match v {
            VDir::Volumes => self
                .volume_base(name)
                .map(|b| (AnchorKey::Volume(name.into()), b)),
            VDir::Containers => self
                .containers()
                .into_iter()
                .find(|(k, _)| k.name() == name),
            VDir::Machines => self
                .machine_base(name)
                .map(|b| (AnchorKey::Machine(name.into()), b)),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lists_volumes_with_data_only() {
        let d = std::env::temp_dir().join(format!("kat-nfs-layout-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        for v in ["b", "a", ".hidden"] {
            std::fs::create_dir_all(d.join(v).join("_data")).unwrap();
        }
        std::fs::create_dir_all(d.join("nodata")).unwrap();
        std::fs::write(d.join("metadata.db"), b"").unwrap();
        let l = Layout {
            volumes: d.clone(),
            containers: Box::new(FixedContainers(vec![
                ("web".into(), "/proc/7/root".into()),
                ("bad/name".into(), "/x".into()),
            ])),
            machines: d.join("machines"),
        };
        let names: Vec<_> = l
            .anchors(VDir::Volumes)
            .into_iter()
            .map(|(k, _)| k.name().to_string())
            .collect();
        assert_eq!(names, ["a", "b"]);
        assert_eq!(
            l.find(VDir::Volumes, "a"),
            Some((AnchorKey::Volume("a".into()), d.join("a/_data")))
        );
        assert_eq!(l.find(VDir::Volumes, "nodata"), None);
        assert_eq!(l.find(VDir::Volumes, ".."), None);
        assert_eq!(l.find(VDir::Volumes, ".hidden"), None);
        assert_eq!(
            l.anchors(VDir::Containers),
            vec![(AnchorKey::Container("web".into()), "/proc/7/root".into())]
        );
        assert_eq!(l.find(VDir::Containers, "bad/name"), None);
        assert!(l.anchors(VDir::Machines).is_empty());
        for m in ["dev", "half"] {
            std::fs::create_dir_all(d.join("machines").join(m)).unwrap();
        }
        std::fs::create_dir_all(d.join("machines/dev/rootfs/etc")).unwrap();
        std::fs::write(d.join("machines/.default"), b"dev\n").unwrap();
        assert_eq!(
            l.anchors(VDir::Machines),
            vec![(AnchorKey::Machine("dev".into()), d.join("machines/dev/rootfs"))]
        );
        assert_eq!(
            l.find(VDir::Machines, "dev"),
            Some((AnchorKey::Machine("dev".into()), d.join("machines/dev/rootfs")))
        );
        assert_eq!(l.find(VDir::Machines, "half"), None);
        assert_eq!(l.find(VDir::Machines, ".default"), None);
        std::fs::remove_dir_all(&d).unwrap();
    }
}
