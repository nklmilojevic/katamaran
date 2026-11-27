//! Runs the real `kat-init` binary in its `rmtree` helper mode, the same way
//! Kubernetes reset re-executes `/proc/self/exe rmtree …` under the reaper,
//! to verify argv dispatch in `main` and the helper's exit-status contract.
#![cfg(target_os = "linux")]

use std::fs;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn kat_init(args: &[&std::ffi::OsStr]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_kat-init"))
        .arg("rmtree")
        .args(args)
        .output()
        .expect("run kat-init rmtree")
}

struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        let p = std::env::temp_dir().join(format!("{tag}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&p);
        fs::create_dir(&p).unwrap();
        Self(p)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn rmtree_helper_removes_tree_without_following_symlinks() {
    let tmp = TempDir::new("kat-rmtree-helper");
    let base = &tmp.0;
    let outside = base.join("outside");
    fs::create_dir(&outside).unwrap();
    fs::write(outside.join("keep"), b"preserve").unwrap();

    let tree = base.join("tree");
    fs::create_dir_all(tree.join("a/b")).unwrap();
    fs::write(tree.join("a/b/f"), b"x").unwrap();
    symlink(&outside, tree.join("a/link")).unwrap();

    let out = kat_init(&[tree.as_os_str()]);
    assert!(out.status.success(), "{out:?}");
    assert!(fs::symlink_metadata(&tree).is_err());
    assert!(outside.join("keep").exists());

    // Already absent: success.
    let out = kat_init(&[tree.as_os_str()]);
    assert!(out.status.success(), "{out:?}");
}

#[test]
fn rmtree_helper_reclaims_only_stale_siblings() {
    let tmp = TempDir::new("kat-rmtree-stale");
    let parent = &tmp.0;
    for name in [".data.removing-1/x", ".data.removing-2", "data/server"] {
        fs::create_dir_all(parent.join(name)).unwrap();
    }
    fs::write(parent.join(".data.removing-1/x/f"), b"x").unwrap();
    fs::write(parent.join("data/server/state.db"), b"live").unwrap();

    let out = kat_init(&["--stale-in".as_ref(), parent.as_os_str()]);
    assert!(out.status.success(), "{out:?}");
    let mut left: Vec<String> = fs::read_dir(parent)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    left.sort();
    assert_eq!(left, vec!["data".to_string()]);
    assert!(Path::new(&parent.join("data/server/state.db")).exists());
}

#[test]
fn rmtree_helper_reports_usage_and_errors_via_exit_status() {
    let out = kat_init(&[]);
    assert_eq!(out.status.code(), Some(2), "{out:?}");
    assert!(String::from_utf8_lossy(&out.stderr).contains("usage"));

    let out = kat_init(&["/".as_ref()]);
    assert_eq!(out.status.code(), Some(1), "{out:?}");
    assert!(String::from_utf8_lossy(&out.stderr).contains("refusing to remove /"));
}
