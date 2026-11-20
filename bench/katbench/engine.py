"""Docker engine access (always through an explicit --context), the bench image and the run container."""

import hashlib
import json
import subprocess

from .datasets import IMAGE_DIR, Paths
from .parse import parse_kbtime


class BenchError(RuntimeError):
    pass


def _check(proc, what):
    if proc.returncode != 0:
        tail = (proc.stderr or proc.stdout or "")[-2000:]
        raise BenchError(f"{what} failed with exit {proc.returncode}:\n{tail}")
    return proc


class Docker:
    def __init__(self, context):
        self.context = context

    def argv(self, *args):
        return ["docker", "--context", self.context, *args]

    def run(self, *args, check=True, timeout=None):
        proc = subprocess.run(self.argv(*args), capture_output=True, text=True, timeout=timeout)
        return _check(proc, "docker " + " ".join(args[:3])) if check else proc

    def json(self, *args):
        return json.loads(self.run(*args).stdout)


def image_hash():
    """Hash of every file under bench/image, used as the image tag."""
    h = hashlib.sha256()
    for path in sorted(p for p in IMAGE_DIR.rglob("*") if p.is_file()):
        h.update(str(path.relative_to(IMAGE_DIR)).encode() + b"\0" + path.read_bytes() + b"\0")
    return h.hexdigest()[:12]


def ensure_image(docker, log):
    """Make katbench:<hash> available: reuse it, load the saved tarball, or build and save it.

    The saved tarball lets another engine run byte-identical tooling without rebuilding.
    """
    tag = f"katbench:{image_hash()}"
    if docker.run("image", "inspect", tag, check=False).returncode == 0:
        return tag
    tar = Paths.images / f"katbench-{image_hash()}.tar"
    if tar.exists():
        log(f"loading {tar}")
        docker.run("load", "-i", str(tar))
        return tag
    log(f"building {tag}")
    proc = subprocess.run(docker.argv("build", "-t", tag, str(IMAGE_DIR)), text=True)
    _check(proc, "docker build")
    Paths.images.mkdir(parents=True, exist_ok=True)
    log(f"saving {tag} -> {tar}")
    docker.run("save", "-o", str(tar), tag)
    return tag


def image_info(docker, tag):
    info = docker.json("image", "inspect", tag)[0]
    return {
        "tag": tag,
        "id": info["Id"],
        "repo_digests": info.get("RepoDigests") or [],
        "created": info.get("Created"),
        "size_bytes": info.get("Size"),
    }


CACHE_VOLUMES = {
    "katbench-npm-cache": "/cache/npm",
    "katbench-pnpm-store": "/cache/pnpm",
    "katbench-cargo-home": "/cache/cargo",
}


class Container:
    """A long-lived container with bench/work bind-mounted at the same path and caches on volumes."""

    def __init__(self, docker, image, name):
        self.docker, self.image, self.name = docker, image, name

    def start(self):
        self.docker.run("rm", "-f", self.name, check=False)
        args = ["run", "-d", "--name", self.name, "-v", f"{Paths.native_bin.parent}:{Paths.native_bin.parent}"]
        for vol, path in CACHE_VOLUMES.items():
            args += ["-v", f"{vol}:{path}"]
        self.docker.run(*args, self.image, "sleep", "infinity")
        self.sh("mkdir -p /vmdata")

    def remove(self):
        self.docker.run("rm", "-f", self.name, check=False)

    def exec_argv(self, argv, cwd=None):
        return self.docker.argv("exec", *(["-w", str(cwd)] if cwd else []), self.name, *argv)

    def sh(self, script, cwd=None, timeout=None):
        """Run a shell script untimed; return stdout. Raise on failure."""
        proc = subprocess.run(self.exec_argv(["sh", "-c", script], cwd), capture_output=True, text=True,
                              timeout=timeout)
        return _check(proc, f"[container] {script[:80]}").stdout

    def timed(self, script, cwd=None):
        """Run a shell script under kb_time; return (seconds, stdout)."""
        proc = subprocess.run(self.exec_argv(["kb_time", "sh", "-c", script], cwd), capture_output=True,
                              text=True)
        return _timed_result(proc, script)


class Native:
    """Runs the same scripts directly on macOS, timed by the natively compiled kb_time."""

    name = "native"

    def sh(self, script, cwd=None, timeout=None):
        proc = subprocess.run(["sh", "-c", script], cwd=cwd, capture_output=True, text=True, timeout=timeout)
        return _check(proc, f"[native] {script[:80]}").stdout

    def timed(self, script, cwd=None):
        proc = subprocess.run([str(Paths.native_bin / "kb_time"), "sh", "-c", script], cwd=cwd,
                              capture_output=True, text=True)
        return _timed_result(proc, script)


def _timed_result(proc, script):
    ns, code = parse_kbtime(proc.stderr)
    if code != 0 or proc.returncode != 0:
        raise BenchError(f"timed command failed (exit {code}): {script}\n{proc.stderr[-2000:]}")
    return ns / 1e9, proc.stdout
