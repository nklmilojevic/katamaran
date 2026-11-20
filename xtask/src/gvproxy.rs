//! gvproxy from containers/gvisor-tap-vsock (Apache-2.0): the userspace
//! network stack behind the guest's virtio-net device. Built with the local Go
//! toolchain from the pinned release plus our patches in
//! vendor/patches/gvproxy, twice: the `gvproxy` binary, and `libgvproxy.dylib`
//! (`-buildmode=c-shared`) that the VM process loads to run it in-process.
//! Both need cgo: the DNS server asks the system resolver through dns_sd.
//! `go mod download` checks the module against the Go checksum database, so
//! the patches apply to the tagged source. Module and build caches live under
//! target/go, so nothing outside the checkout is written.

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, bail};

const MODULE: &str = "github.com/containers/gvisor-tap-vsock";
/// Tag v0.9.0 is commit 6dbb5d4fa0033356c72a201346f8350594229868 (2026-10-07).
const VERSION: &str = "v0.9.0";

pub fn build(root: &Path) -> anyhow::Result<()> {
    let bin_dir = root.join("target/release");
    let bin = bin_dir.join("gvproxy");
    let lib = bin_dir.join("libgvproxy.dylib");
    let stamp = bin_dir.join("gvproxy.version");
    let patches = patches(&root.join("vendor/patches/gvproxy"))?;
    let version = format!("{VERSION}-katamaran.{:016x}", fingerprint(&patches)?);
    if bin.is_file()
        && lib.is_file()
        && std::fs::read_to_string(&stamp).is_ok_and(|s| s.trim() == version)
    {
        return Ok(());
    }
    std::fs::create_dir_all(&bin_dir)?;
    let go = root.join("target/go");
    let go_env = |cmd: &mut Command| {
        cmd.env("GOMODCACHE", go.join("mod"))
            .env("GOCACHE", go.join("cache"))
            .env("GOFLAGS", "-modcacherw")
            .env("GOTOOLCHAIN", "local")
            .env("CGO_ENABLED", "1");
    };

    let mut download = Command::new("go");
    download
        .args(["mod", "download"])
        .arg(format!("{MODULE}@{VERSION}"));
    go_env(&mut download);
    super::run(&mut download).context("download gvproxy (needs the Go toolchain)")?;

    // A fresh copy of the pristine module, patched.
    let src = root.join("target/gvproxy-src");
    if src.exists() {
        std::fs::remove_dir_all(&src)?;
    }
    let pristine = go.join("mod").join(format!("{MODULE}@{VERSION}"));
    super::run(Command::new("cp").arg("-R").arg(&pristine).arg(&src))?;
    super::run(Command::new("chmod").args(["-R", "u+w"]).arg(&src))?;
    // Inside this checkout, `git apply` would find our repository and
    // silently skip every path outside target/gvproxy-src; stop discovery
    // above it so the patches apply to the copy. Reverse-checking each patch
    // right after it applied proves its changes are there.
    let git = |args: &[&str], p: &Path| {
        let mut cmd = Command::new("git");
        cmd.args(args)
            .arg(p)
            .current_dir(&src)
            .env("GIT_CEILING_DIRECTORIES", root.join("target"));
        cmd
    };
    for p in &patches {
        super::run(&mut git(&["apply"], p)).with_context(|| format!("apply {}", p.display()))?;
        super::run(&mut git(&["apply", "--reverse", "--check"], p))
            .with_context(|| format!("{} did not apply", p.display()))?;
    }

    let ldflags = format!("-ldflags=-s -w -X {MODULE}/pkg/types.gitVersion={version}");
    let mut build = Command::new("go");
    build
        .args(["build", "-trimpath", "-o"])
        .arg(&bin)
        .arg(&ldflags)
        .arg("./cmd/gvproxy")
        .current_dir(&src);
    go_env(&mut build);
    super::run(&mut build).context("build gvproxy (needs cgo, so Xcode's command line tools)")?;

    let mut build = Command::new("go");
    build
        .args(["build", "-buildmode=c-shared", "-trimpath", "-o"])
        .arg(&lib)
        .arg(&ldflags)
        .arg("./cmd/gvproxy")
        .current_dir(&src);
    go_env(&mut build);
    super::run(&mut build).context("build libgvproxy.dylib")?;
    // The C header cgo writes next to it; nothing includes it.
    let _ = std::fs::remove_file(lib.with_extension("h"));

    let out = Command::new(&bin).arg("-version").output()?;
    let reported = String::from_utf8_lossy(&out.stdout);
    if !reported.contains(&version) {
        bail!(
            "{} reports version {reported:?}, want {version}",
            bin.display()
        );
    }
    std::fs::write(&stamp, format!("{version}\n"))?;
    println!(
        "built {} and {} ({})",
        bin.display(),
        lib.display(),
        reported.trim()
    );
    Ok(())
}

/// The patch files in `dir`, in the order they apply.
fn patches(dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut out: Vec<PathBuf> = std::fs::read_dir(dir)
        .with_context(|| format!("read {}", dir.display()))?
        .map(|e| e.map(|e| e.path()))
        .collect::<Result<_, _>>()?;
    out.retain(|p| p.extension().is_some_and(|e| e == "patch"));
    out.sort();
    Ok(out)
}

/// FNV-1a over the patches' names and contents, so a changed series
/// rebuilds gvproxy.
fn fingerprint(patches: &[PathBuf]) -> anyhow::Result<u64> {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for p in patches {
        let name = p.file_name().unwrap_or_default().as_encoded_bytes();
        for b in name.iter().chain(&std::fs::read(p)?) {
            h = (h ^ u64::from(*b)).wrapping_mul(0x0100_0000_01b3);
        }
    }
    Ok(h)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn patches_are_sorted_and_fingerprinted() {
        let dir = std::env::temp_dir().join(format!("xtask-gvp-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        for (name, body) in [
            ("0002-b.patch", "b"),
            ("0001-a.patch", "a"),
            ("notes.txt", "x"),
        ] {
            std::fs::write(dir.join(name), body).unwrap();
        }
        let found = patches(&dir).unwrap();
        let names: Vec<_> = found.iter().map(|p| p.file_name().unwrap()).collect();
        assert_eq!(names, ["0001-a.patch", "0002-b.patch"]);

        let before = fingerprint(&found).unwrap();
        assert_eq!(before, fingerprint(&found).unwrap());
        std::fs::write(dir.join("0002-b.patch"), "b2").unwrap();
        assert_ne!(before, fingerprint(&found).unwrap());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn the_repository_patches_exist() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../vendor/patches/gvproxy");
        assert!(!patches(&dir).unwrap().is_empty());
    }
}
