"""Pinned datasets and their host-side preparation under bench/work/."""

import hashlib
import os
import shutil
import subprocess
import urllib.request
from pathlib import Path

BENCH = Path(__file__).resolve().parents[1]
WORK = BENCH / "work"
# Spotlight skips directories whose name ends in .noindex, which keeps mds out of the native timings.
DS = WORK / "ds.noindex"
IMAGE_DIR = BENCH / "image"

LINUX = {
    "repo": "https://git.kernel.org/pub/scm/linux/kernel/git/stable/linux.git",
    "tag": "v6.18.64",
    "commit": "fd7cfd39c88596fec816639ad83cbf3e9d316372",
}
LINUX_TARBALL = {
    "url": "https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-6.18.64.tar.xz",
    "sha256": "020d45510c539972e6f14c5c71566878d3b7ef56fb082b3e09134f2ebaade562",
    "bytes": 155382076,
}
JS_PROJECT = {
    "repo": "https://github.com/videojs/video.js.git",
    "tag": "v8.24.2",
    "commit": "3732efc5711a56dd86a52ed746aecce9b74c6fd0",
    "lockfile": "package-lock.json",
    "lockfile_sha256": "9059be321eadbdb20851856de8599ea645b708baa738dcee1c23bdc12de09291",
}
# pnpm-lock.yaml is produced by `pnpm import` from package-lock.json during preparation.
PNPM_LOCK_SHA256 = "1fffa1a586d12d69ba077b311ece21b7480988903c58fe402c92f73d7961624e"
RUST_CRATE = {
    "repo": "https://github.com/BurntSushi/ripgrep.git",
    "tag": "15.2.0",
    "commit": "e89fff89ac9af12e8d4ce9d5fd07beb408ca730f",
    "lockfile": "Cargo.lock",
    "lockfile_sha256": "7a7d39cda8a03930e578f1dbb724e055771901842eca239e03b01e19da946a64",
}


class Paths:
    linux = DS / "linux"
    linux_files = DS / "linux.files"
    tarball = DS / "linux-6.18.64.tar.xz"
    untar = DS / "untar"
    js_src = DS / "src" / "video.js"
    js_npm = DS / "js-npm"
    js_pnpm = DS / "js-pnpm"
    rust = DS / "ripgrep"
    fio = DS / "fio"
    inotify = DS / "inotify"
    native_bin = WORK / "native-bin"
    images = WORK / "images"


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def git_head(path):
    return subprocess.run(
        ["git", "-C", str(path), "rev-parse", "HEAD"], capture_output=True, text=True, check=True
    ).stdout.strip()


def clone_pinned(spec, dest, log):
    """Shallow-clone spec['tag'] into dest and verify the commit (and lockfile checksum, if any)."""
    if (dest / ".git").exists() and git_head(dest) == spec["commit"]:
        return
    if dest.exists():
        log(f"removing incomplete clone {dest}")
        shutil.rmtree(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    log(f"cloning {spec['repo']} {spec['tag']} -> {dest}")
    subprocess.run(
        ["git", "clone", "--quiet", "--depth", "1", "--single-branch", "--branch", spec["tag"],
         spec["repo"], str(dest)],
        check=True,
    )
    head = git_head(dest)
    if head != spec["commit"]:
        raise RuntimeError(f"{spec['repo']} {spec['tag']}: HEAD {head} != pinned {spec['commit']}")
    verify_lockfile(spec, dest)


def verify_lockfile(spec, dest):
    if "lockfile" not in spec:
        return
    got = sha256_file(dest / spec["lockfile"])
    if got != spec["lockfile_sha256"]:
        raise RuntimeError(f"{dest / spec['lockfile']}: sha256 {got} != pinned {spec['lockfile_sha256']}")


def download_verified(spec, dest, log):
    if dest.exists() and dest.stat().st_size == spec["bytes"] and sha256_file(dest) == spec["sha256"]:
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    log(f"downloading {spec['url']}")
    with urllib.request.urlopen(spec["url"]) as r, open(tmp, "wb") as f:
        shutil.copyfileobj(r, f, 1 << 20)
    got = sha256_file(tmp)
    if got != spec["sha256"]:
        tmp.unlink()
        raise RuntimeError(f"{spec['url']}: sha256 {got} != pinned {spec['sha256']}")
    tmp.rename(dest)


def write_file_list(tree, out):
    """Write the relative paths of all regular files in tree (excluding .git), sorted, one per line."""
    paths = []
    for root, dirs, files in os.walk(tree):
        dirs[:] = [d for d in dirs if d != ".git"]
        for name in files:
            full = os.path.join(root, name)
            if os.path.isfile(full) and not os.path.islink(full):
                paths.append(os.path.relpath(full, tree))
    paths.sort()
    tmp = out.with_suffix(".tmp")
    tmp.write_text("".join(p + "\n" for p in paths))
    tmp.rename(out)
    return len(paths)


def build_native_tools(log):
    """Compile kb_time, kb_stat and kb_open for macOS with the system compiler."""
    Paths.native_bin.mkdir(parents=True, exist_ok=True)
    src = IMAGE_DIR / "src"
    for tool in ("kb_time", "kb_stat", "kb_open"):
        out = Paths.native_bin / tool
        if out.exists() and out.stat().st_mtime >= max(p.stat().st_mtime for p in src.iterdir()):
            continue
        log(f"compiling native {tool}")
        subprocess.run(["cc", "-O2", "-Wall", "-Werror", "-o", str(out), str(src / f"{tool}.c")], check=True)


def prepare_host(log):
    """Fetch and verify every dataset. Idempotent; only missing or mismatched pieces are redone."""
    DS.mkdir(parents=True, exist_ok=True)
    clone_pinned(LINUX, Paths.linux, log)
    download_verified(LINUX_TARBALL, Paths.tarball, log)
    clone_pinned(JS_PROJECT, Paths.js_src, log)
    for dest in (Paths.js_npm, Paths.js_pnpm):
        if not (dest / "package.json").exists():
            log(f"copying {Paths.js_src} -> {dest}")
            shutil.copytree(Paths.js_src, dest, symlinks=True, dirs_exist_ok=True)
        verify_lockfile(JS_PROJECT, dest)
    clone_pinned(RUST_CRATE, Paths.rust, log)
    if not Paths.linux_files.exists():
        n = write_file_list(Paths.linux, Paths.linux_files)
        log(f"wrote {n} paths to {Paths.linux_files}")
    for d in (Paths.fio, Paths.inotify):
        d.mkdir(exist_ok=True)
    build_native_tools(log)


def pins():
    lock = Paths.js_pnpm / "pnpm-lock.yaml"
    return {
        "linux_git": LINUX,
        "linux_checkout_note": "checked out on the host; on case-insensitive APFS the 13 case-colliding "
                               "netfilter paths show as modified in git status",
        "linux_tarball": LINUX_TARBALL,
        "linux_file_count": sum(1 for _ in open(Paths.linux_files)) if Paths.linux_files.exists() else None,
        "js_project": JS_PROJECT,
        "pnpm_lock_sha256": sha256_file(lock) if lock.exists() else None,
        "pnpm_lock_pinned_sha256": PNPM_LOCK_SHA256,
        "rust_crate": RUST_CRATE,
    }
