//! The Mac user inside a new machine: same name and uid (so files on the
//! /Users share and in the machine agree), a group of the same id, a home
//! directory from /etc/skel, membership in the distro's admin group and
//! passwordless sudo. The account files are edited on the stopped rootfs, so
//! no distro tool is needed; only sudo itself is installed after the first
//! boot when the image lacks it.

use std::collections::VecDeque;
use std::ffi::OsString;
use std::fs;
use std::io;
use std::os::unix::fs::{MetadataExt, PermissionsExt, lchown};
use std::path::{Component, Path, PathBuf};

/// Groups that grant admin rights on some distro; the user joins the ones
/// the image has.
const ADMIN_GROUPS: [&str; 3] = ["sudo", "wheel", "adm"];
const MAX_SYMLINK_HOPS: usize = 40;

/// Resolve `rel` strictly inside `rootfs`, interpreting absolute symlinks
/// relative to `rootfs` (never PID 1's `/`) and clamping `..` at `rootfs`.
pub(super) fn resolve_in_rootfs(rootfs: &Path, rel: impl AsRef<Path>) -> io::Result<PathBuf> {
    let mut parts: Vec<OsString> = Vec::new();
    let mut pending: VecDeque<OsString> = VecDeque::new();
    let mut hops = 0usize;

    let push_front_path = |pending: &mut VecDeque<OsString>, p: &Path| {
        let comps: Vec<OsString> = p
            .components()
            .filter_map(|c| match c {
                Component::Normal(n) => Some(n.to_os_string()),
                Component::ParentDir => Some(OsString::from("..")),
                Component::RootDir | Component::CurDir | Component::Prefix(_) => None,
            })
            .collect();
        for c in comps.into_iter().rev() {
            pending.push_front(c);
        }
    };

    push_front_path(&mut pending, rel.as_ref());

    while let Some(comp) = pending.pop_front() {
        if comp == ".." {
            parts.pop();
            continue;
        }
        let mut candidate = rootfs.to_path_buf();
        for p in &parts {
            candidate.push(p);
        }
        candidate.push(&comp);
        match fs::symlink_metadata(&candidate) {
            Ok(meta) if meta.file_type().is_symlink() => {
                hops += 1;
                if hops > MAX_SYMLINK_HOPS {
                    return Err(io::Error::other(format!(
                        "too many symlinks resolving {}",
                        rel.as_ref().display()
                    )));
                }
                let target = fs::read_link(&candidate)?;
                if target.is_absolute() {
                    parts.clear();
                }
                push_front_path(&mut pending, &target);
            }
            Ok(_) => parts.push(comp),
            Err(e) if e.kind() == io::ErrorKind::NotFound => parts.push(comp),
            Err(e) => return Err(e),
        }
    }

    let mut out = rootfs.to_path_buf();
    for p in parts {
        out.push(p);
    }
    Ok(out)
}

pub fn add_user(rootfs: &Path, user: &str, uid: u32) -> io::Result<()> {
    let shell = ["bin/bash", "usr/bin/bash", "bin/zsh"]
        .iter()
        .find(|s| {
            resolve_in_rootfs(rootfs, s)
                .ok()
                .and_then(|p| fs::symlink_metadata(p).ok())
                .is_some_and(|m| m.is_file())
        })
        .map(|s| format!("/{s}"))
        .unwrap_or_else(|| "/bin/sh".into());
    let home = format!("/home/{user}");
    let passwd_path = resolve_in_rootfs(rootfs, "etc/passwd")?;
    let group_path = resolve_in_rootfs(rootfs, "etc/group")?;
    edit(&passwd_path, |t| passwd(t, user, uid, &home, &shell))?;
    edit(&group_path, |t| group(t, user, uid))?;
    let shadow_path = resolve_in_rootfs(rootfs, "etc/shadow")?;
    if fs::symlink_metadata(&shadow_path).is_ok() {
        edit(&shadow_path, |t| shadow(t, user))?;
    }
    let home_dir = resolve_in_rootfs(rootfs, home.trim_start_matches('/'))?;
    if fs::symlink_metadata(&home_dir).is_err() {
        fs::create_dir_all(&home_dir)?;
        let skel_dir = resolve_in_rootfs(rootfs, "etc/skel")?;
        copy_tree(&skel_dir, &home_dir, uid)?;
    }
    lchown(&home_dir, Some(uid), Some(uid))?;
    fs::set_permissions(&home_dir, fs::Permissions::from_mode(0o750))?;

    let sudoers = resolve_in_rootfs(rootfs, "etc/sudoers.d")?;
    fs::create_dir_all(&sudoers)?;
    let file = resolve_in_rootfs(rootfs, "etc/sudoers.d/katamaran")?;
    fs::write(&file, format!("{user} ALL=(ALL:ALL) NOPASSWD: ALL\n"))?;
    fs::set_permissions(&file, fs::Permissions::from_mode(0o440))?;
    Ok(())
}

fn edit(path: &Path, f: impl FnOnce(&str) -> String) -> io::Result<()> {
    let text = fs::read_to_string(path).unwrap_or_default();
    let meta = fs::symlink_metadata(path).ok();
    let mode = meta.as_ref().map_or(0o644, |m| m.permissions().mode());
    let owner = meta.as_ref().map(|m| (m.uid(), m.gid()));
    let tmp = path.with_extension("katamaran-tmp");
    fs::write(&tmp, f(&text))?;
    if let Some((uid, gid)) = owner {
        lchown(&tmp, Some(uid), Some(gid))?;
    }
    fs::set_permissions(&tmp, fs::Permissions::from_mode(mode))?;
    fs::rename(tmp, path)
}

fn without(text: &str, name: &str) -> String {
    text.lines()
        .filter(|l| l.split(':').next() != Some(name))
        .map(|l| format!("{l}\n"))
        .collect()
}

pub fn passwd(text: &str, user: &str, uid: u32, home: &str, shell: &str) -> String {
    let mut out = without(text, user);
    out.push_str(&format!("{user}:x:{uid}:{uid}:{user}:{home}:{shell}\n"));
    out
}

/// The user's own group, and the user added to the admin groups.
pub fn group(text: &str, user: &str, gid: u32) -> String {
    let mut out = String::new();
    for line in without(text, user).lines() {
        let f: Vec<&str> = line.splitn(4, ':').collect();
        if f.len() == 4 && ADMIN_GROUPS.contains(&f[0]) {
            let mut members: Vec<&str> = f[3].split(',').filter(|m| !m.is_empty()).collect();
            if !members.contains(&user) {
                members.push(user);
            }
            out.push_str(&format!(
                "{}:{}:{}:{}\n",
                f[0],
                f[1],
                f[2],
                members.join(",")
            ));
        } else {
            out.push_str(line);
            out.push('\n');
        }
    }
    // A gid taken by a distro group (on Debian 20 is dialout, not staff)
    // still gets the user's group: the name is what matters to tools.
    out.push_str(&format!("{user}:x:{gid}:\n"));
    out
}

/// No password: login is through katamaran (shell, ssh), sudo needs none.
pub fn shadow(text: &str, user: &str) -> String {
    let mut out = without(text, user);
    out.push_str(&format!("{user}:*:19000:0:99999:7:::\n"));
    out
}

fn copy_tree(from: &Path, to: &Path, uid: u32) -> io::Result<()> {
    let Ok(dir) = fs::read_dir(from) else {
        return Ok(());
    };
    for e in dir.flatten() {
        let (src, dst) = (e.path(), to.join(e.file_name()));
        let meta = fs::symlink_metadata(&src)?;
        if meta.is_dir() {
            fs::create_dir_all(&dst)?;
            copy_tree(&src, &dst, uid)?;
        } else if meta.file_type().is_symlink() {
            std::os::unix::fs::symlink(fs::read_link(&src)?, &dst)?;
        } else {
            fs::copy(&src, &dst)?;
        }
        lchown(&dst, Some(uid), Some(uid))?;
    }
    Ok(())
}

/// Run as root in the machine after its first boot: wait for the network,
/// then install sudo with the distro's package manager if it is missing.
pub const FIRST_BOOT: &str = r#"
has_route() {
    while read -r _ dst _; do [ "$dst" = 00000000 ] && return 0; done < /proc/net/route
    return 1
}
i=0
while [ $i -lt 100 ]; do
    has_route && break
    i=$((i + 1)); sleep 0.2
done
command -v sudo >/dev/null 2>&1 && exit 0
if command -v apk >/dev/null 2>&1; then
    apk add -q --no-progress sudo
elif command -v apt-get >/dev/null 2>&1; then
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -qq && apt-get install -y -qq --no-install-recommends sudo >/dev/null
elif command -v dnf >/dev/null 2>&1; then
    dnf install -y -q sudo
elif command -v pacman >/dev/null 2>&1; then
    (pacman-key --init && pacman-key --populate) >/dev/null 2>&1
    pacman -Sy --noconfirm --needed sudo >/dev/null
elif command -v zypper >/dev/null 2>&1; then
    zypper -q -n install sudo
else
    echo "no known package manager; sudo not installed" >&2; exit 1
fi
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adds_the_user_to_account_files() {
        let p = "root:x:0:0:root:/root:/bin/bash\nme:x:1000:1000::/home/me:/bin/sh\n";
        assert_eq!(
            passwd(p, "me", 501, "/home/me", "/bin/bash"),
            "root:x:0:0:root:/root:/bin/bash\nme:x:501:501:me:/home/me:/bin/bash\n"
        );
        let g = "root:x:0:\nsudo:x:27:\nwheel:x:10:root\nusers:x:100:\n";
        assert_eq!(
            group(g, "me", 501),
            "root:x:0:\nsudo:x:27:me\nwheel:x:10:root,me\nusers:x:100:\nme:x:501:\n"
        );
        assert_eq!(
            group("sudo:x:27:me\n", "me", 501),
            "sudo:x:27:me\nme:x:501:\n"
        );
        assert_eq!(
            shadow("root:*:1::::::\n", "me"),
            "root:*:1::::::\nme:*:19000:0:99999:7:::\n"
        );
    }

    #[test]
    fn sets_up_a_rootfs() {
        let d = std::env::temp_dir().join(format!("kat-provision-{}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        fs::create_dir_all(d.join("etc/skel")).unwrap();
        fs::create_dir_all(d.join("bin")).unwrap();
        fs::write(d.join("bin/bash"), "").unwrap();
        fs::write(d.join("etc/skel/.profile"), "x").unwrap();
        fs::write(d.join("etc/passwd"), "root:x:0:0::/root:/bin/sh\n").unwrap();
        fs::write(d.join("etc/group"), "root:x:0:\n").unwrap();
        let uid = unsafe { libc::getuid() };
        add_user(&d, "me", uid).unwrap();
        let p = fs::read_to_string(d.join("etc/passwd")).unwrap();
        assert!(
            p.ends_with(&format!("me:x:{uid}:{uid}:me:/home/me:/bin/bash\n")),
            "{p}"
        );
        assert!(d.join("home/me/.profile").is_file());
        assert!(
            fs::read_to_string(d.join("etc/sudoers.d/katamaran"))
                .unwrap()
                .contains("NOPASSWD")
        );
        fs::remove_dir_all(&d).unwrap();
    }

    #[test]
    fn resolves_symlinks_strictly_inside_rootfs() {
        let d = std::env::temp_dir().join(format!("kat-rootfs-{}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        fs::create_dir_all(d.join("usr/bin")).unwrap();
        fs::create_dir_all(d.join("etc")).unwrap();
        fs::write(d.join("usr/bin/bash"), "").unwrap();
        // Absolute symlink /bin -> /usr/bin stays inside rootfs.
        std::os::unix::fs::symlink("/usr/bin", d.join("bin")).unwrap();
        assert_eq!(
            resolve_in_rootfs(&d, "bin/bash").unwrap(),
            d.join("usr/bin/bash")
        );
        // `..` is clamped at rootfs.
        assert_eq!(
            resolve_in_rootfs(&d, "../../etc/passwd").unwrap(),
            d.join("etc/passwd")
        );
        // Self-referencing absolute symlink fails as a loop instead of escaping.
        std::os::unix::fs::symlink("/etc/hostname", d.join("etc/hostname")).unwrap();
        assert!(resolve_in_rootfs(&d, "etc/hostname").is_err());
        fs::remove_dir_all(&d).unwrap();
    }
}
