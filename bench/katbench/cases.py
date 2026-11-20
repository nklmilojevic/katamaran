"""Benchmark cases. Each case runs at one location and returns a list of metric records.

Locations: "bind" (container, data in a bind mount of bench/work), "vm" (container, data copied into the
container's own filesystem) and "native" (macOS, same host directories as "bind").
"""

import shutil
import subprocess
import threading
import time

from .datasets import PNPM_LOCK_SHA256, Paths, sha256_file
from .engine import BenchError
from .parse import parse_fio, parse_inotify_line, parse_kbloop
from .stats import summarize

MICRO_OPS = 1_000_000
FIO_SIZE = "1G"
FIO_RUNTIME_S = 10
FIO_COMMON = (
    f"--name=job --filename=fio.dat --size={FIO_SIZE} --runtime={FIO_RUNTIME_S} --time_based "
    "--ioengine=psync --direct=0 --numjobs=1 --iodepth=1 --output-format=json"
)
FIO_JOBS = {
    "seq_read_1m": "--rw=read --bs=1M",
    "seq_write_1m": "--rw=write --bs=1M",
    "rand_read_4k": "--rw=randread --bs=4k",
    "rand_write_4k": "--rw=randwrite --bs=4k",
    "fsync_write_4k": "--rw=write --bs=4k --fsync=1",
}
INOTIFY_WARMUP, INOTIFY_EVENTS, INOTIFY_BURST, INOTIFY_FLOOR_EVENTS = 10, 200, 10_000, 50
INOTIFY_TIMEOUT_S, INOTIFY_GAP_S, INOTIFY_QUIET_S = 2.0, 0.02, 5.0

NPM_CI = "npm ci --prefer-offline --ignore-scripts --no-audit --no-fund --loglevel=error"
PNPM_INSTALL = "pnpm install --frozen-lockfile --ignore-scripts --store-dir /cache/pnpm --reporter=silent"
CARGO_BUILD = "cargo build --locked --quiet"


class Ctx:
    def __init__(self, container, native, runs, log):
        self.container, self.native, self.runs, self.log = container, native, runs, log
        self.vm_ready = set()

    def runner(self, loc):
        return self.native if loc == "native" else self.container

    def repeat(self, fn):
        """One untimed warm-up, then --runs measured calls."""
        fn()
        return [fn() for _ in range(self.runs)]


def record(case, loc, metric, unit, values, better="lower", **extra):
    return {"case": case, "location": loc, "metric": metric, "unit": unit, "better": better,
            "values": values, **summarize(values), "extra": extra}


def paths(loc):
    if loc == "vm":
        return {"linux": "/vmdata/linux", "node_modules": "/vmdata/node_modules",
                "tarball": "/vmdata/linux.tar.xz", "untar": "/vmdata/untar"}
    return {"linux": str(Paths.linux), "node_modules": str(Paths.js_npm / "node_modules"),
            "tarball": str(Paths.tarball), "untar": str(Paths.untar)}


def ensure_node_modules(ctx):
    if not (Paths.js_npm / "node_modules" / ".package-lock.json").exists():
        ctx.log("node_modules missing, running npm ci (untimed)")
        ctx.container.sh(NPM_CI, cwd=Paths.js_npm)


def ensure_vm(ctx, what):
    """Copy a dataset from the bind mount into the container's own filesystem (untimed, once)."""
    if what in ctx.vm_ready:
        return
    src = {"linux": Paths.linux, "node_modules": Paths.js_npm / "node_modules", "tarball": Paths.tarball}[what]
    dst = paths("vm")[what]
    ctx.log(f"copying {src} into the container filesystem at {dst} (untimed)")
    ctx.container.sh(f"rm -rf {dst} && cp -a {src} {dst}", timeout=3600)
    ctx.vm_ready.add(what)


def prepare_engine(ctx):
    """Warm the package caches on the engine's volumes (network allowed here, not in timed runs)."""
    c = ctx.container
    ctx.log("warming npm cache")
    c.sh("npm ci --ignore-scripts --no-audit --no-fund --loglevel=error", cwd=Paths.js_npm, timeout=3600)
    if not (Paths.js_pnpm / "pnpm-lock.yaml").exists():
        ctx.log("generating pnpm-lock.yaml with pnpm import")
        c.sh("pnpm import", cwd=Paths.js_pnpm, timeout=3600)
    lock_sha = sha256_file(Paths.js_pnpm / "pnpm-lock.yaml")
    if lock_sha != PNPM_LOCK_SHA256:
        raise BenchError(f"pnpm-lock.yaml sha256 {lock_sha} != pinned {PNPM_LOCK_SHA256}")
    ctx.log("warming pnpm store")
    # Without removing node_modules first, pnpm sees an up-to-date tree (left by another engine) and
    # fetches nothing, so a fresh engine's store stays empty and the --offline runs fail.
    c.sh("rm -rf node_modules && pnpm install --frozen-lockfile --ignore-scripts --store-dir /cache/pnpm "
         "--reporter=silent", cwd=Paths.js_pnpm, timeout=3600)
    ctx.log("fetching cargo dependencies")
    c.sh("cargo fetch --locked --quiet", cwd=Paths.rust, timeout=3600)


def git_status(ctx, loc):
    if loc == "vm":
        ensure_vm(ctx, "linux")
    r, p = ctx.runner(loc), paths(loc)
    values = ctx.repeat(lambda: r.timed("git status >/dev/null", cwd=p["linux"])[0])
    return [record("git_status", loc, "time", "s", values)]


def find_node_modules(ctx, loc):
    ensure_node_modules(ctx)
    if loc == "vm":
        ensure_vm(ctx, "node_modules")
    r, p = ctx.runner(loc), paths(loc)
    counts = []

    def run():
        secs, out = r.timed("find . -type f | wc -l", cwd=p["node_modules"])
        counts.append(int(out.strip()))
        return secs

    values = ctx.repeat(run)
    return [record("find_node_modules", loc, "time", "s", values, files=counts[-1])]


def untar(ctx, loc):
    if loc == "vm":
        ensure_vm(ctx, "tarball")
    r, p = ctx.runner(loc), paths(loc)
    dest = p["untar"]
    extract, remove = [], []

    def run():
        r.sh(f"rm -rf {dest} && mkdir -p {dest}")
        extract.append(r.timed(f"tar -xf {p['tarball']} -C {dest}")[0])
        remove.append(r.timed(f"rm -rf {dest}")[0])

    ctx.repeat(run)
    return [record("untar", loc, "extract", "s", extract[1:]),
            record("untar", loc, "rm_rf", "s", remove[1:])]


def _install(ctx, case, cwd, clean, command):
    r = ctx.container

    def run():
        r.sh(clean, cwd=cwd)
        return r.timed(command, cwd=cwd)[0]

    return [record(case, "bind", "time", "s", ctx.repeat(run), command=command)]


def npm_ci(ctx, loc):
    return _install(ctx, "npm_ci", Paths.js_npm, "rm -rf node_modules", NPM_CI)


def pnpm_install(ctx, loc):
    return _install(ctx, "pnpm_install", Paths.js_pnpm, "rm -rf node_modules", PNPM_INSTALL + " --offline")


def cargo_build(ctx, loc):
    return _install(ctx, "cargo_build", Paths.rust, "rm -rf target", CARGO_BUILD + " --offline")


def fio(ctx, loc):
    r, out = ctx.container, []
    try:
        for job, args in FIO_JOBS.items():
            runs = ctx.repeat(lambda: parse_fio(r.sh(f"fio {FIO_COMMON} {args}", cwd=Paths.fio)))
            out.append(record("fio", loc, f"{job}_mb_s", "MB/s", [x["mb_s"] for x in runs], "higher",
                              args=f"{FIO_COMMON} {args}"))
            out.append(record("fio", loc, f"{job}_iops", "IOPS", [x["iops"] for x in runs], "higher"))
    finally:
        (Paths.fio / "fio.dat").unlink(missing_ok=True)
    return out


def microbench(ctx, loc):
    if loc == "vm":
        ensure_vm(ctx, "linux")
    r, p, out = ctx.runner(loc), paths(loc), []
    for tool, metric in (("kb_stat", "stat"), ("kb_open", "open_close")):
        exe = str(Paths.native_bin / tool) if loc == "native" else tool
        cmd = f"{exe} {Paths.linux_files} {MICRO_OPS}"
        results = ctx.repeat(lambda: parse_kbloop(r.sh(cmd, cwd=p["linux"])))
        if any(x["errors"] for x in results):
            raise BenchError(f"{tool}: {results[-1]['errors']} failed calls at {loc}")
        out.append(record("microbench", loc, metric, "ns/op", [x["ns_per_op"] for x in results],
                          ops=MICRO_OPS, files=results[-1]["files"]))
    return out


class _Watcher:
    """inotifywait in the container; each event line is timestamped on the host clock when it arrives."""

    FLOOR_DIR = "/tmp/kbfloor"

    def __init__(self, ctx, directory):
        self.ctx = ctx
        ctx.container.sh(f"rm -rf {self.FLOOR_DIR} && mkdir -p {self.FLOOR_DIR}")
        self.seen, self.overflows = {}, 0
        self.cond, self.ready = threading.Condition(), threading.Event()
        argv = ctx.container.exec_argv(["stdbuf", "-oL", "inotifywait", "-m", "-e",
                                        "create,attrib,close_write,moved_to", "--format", "%f %e",
                                        str(directory), self.FLOOR_DIR])
        self.proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        threading.Thread(target=self._read_out, daemon=True).start()
        threading.Thread(target=self._read_err, daemon=True).start()
        if not self.ready.wait(30):
            self.stop()
            raise BenchError("inotifywait did not establish its watch within 30 s")

    def _read_out(self):
        for line in self.proc.stdout:
            now = time.monotonic_ns()
            parsed = parse_inotify_line(line)
            if not parsed:
                continue
            name, events = parsed
            with self.cond:
                if "Q_OVERFLOW" in events:
                    self.overflows += 1
                if name and name not in self.seen:
                    self.seen[name] = now
                self.cond.notify_all()

    def _read_err(self):
        for line in self.proc.stderr:
            if "Watches established" in line:
                self.ready.set()

    def wait_for(self, name, timeout):
        with self.cond:
            return self.cond.wait_for(lambda: name in self.seen, timeout=timeout)

    def count(self, names):
        with self.cond:
            return sum(1 for n in names if n in self.seen)

    def stop(self):
        self.ctx.container.sh("pkill -x inotifywait || true")
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()


def _floor(ctx, w):
    """Latency of the measuring pipeline itself: events created inside the container on its own
    filesystem, triggered over docker exec stdin and observed over docker exec stdout."""
    c = ctx.container
    toucher = subprocess.Popen(
        c.docker.argv("exec", "-i", c.name, "sh", "-c", f"while read n; do touch {w.FLOOR_DIR}/$n; done"),
        stdin=subprocess.PIPE, text=True)
    values = []
    try:
        for i in range(INOTIFY_WARMUP + INOTIFY_FLOOR_EVENTS):
            name = f"floor-{i:04d}"
            t0 = time.monotonic_ns()
            toucher.stdin.write(name + "\n")
            toucher.stdin.flush()
            if w.wait_for(name, INOTIFY_TIMEOUT_S) and i >= INOTIFY_WARMUP:
                values.append((w.seen[name] - t0) / 1e6)
            time.sleep(INOTIFY_GAP_S)
    finally:
        toucher.stdin.close()
        toucher.wait(timeout=10)
    return values


def _burst(w, d, prefix):
    """Create INOTIFY_BURST files as fast as possible; wait until all events arrived or none came for
    INOTIFY_QUIET_S. Returns (lost, seconds to create, seconds until the last event)."""
    names = [f"{prefix}-{i:05d}" for i in range(INOTIFY_BURST)]
    t0 = time.monotonic()
    for n in names:
        (d / n).touch()
    create_s = time.monotonic() - t0
    last, last_change = -1, time.monotonic()
    while True:
        n = w.count(names)
        now = time.monotonic()
        if n != last:
            last, last_change = n, now
        if n == len(names) or now - last_change >= INOTIFY_QUIET_S:
            break
        time.sleep(0.1)
    return len(names) - last, round(create_s, 3), round(last_change - t0, 3)


def inotify(ctx, loc):
    d = Paths.inotify
    shutil.rmtree(d, ignore_errors=True)
    d.mkdir(parents=True)
    w = _Watcher(ctx, d)
    try:
        floor = _floor(ctx, w)
        latencies, timeouts = [], 0
        for i in range(INOTIFY_WARMUP + INOTIFY_EVENTS):
            name = f"lat-{i:04d}"
            t0 = time.monotonic_ns()
            (d / name).touch()
            ok = w.wait_for(name, INOTIFY_TIMEOUT_S)
            if i >= INOTIFY_WARMUP:
                if ok:
                    latencies.append((w.seen[name] - t0) / 1e6)
                else:
                    timeouts += 1
            time.sleep(INOTIFY_GAP_S)

        bursts = [_burst(w, d, f"burst{k}") for k in range(ctx.runs)]
    finally:
        w.stop()
        shutil.rmtree(d, ignore_errors=True)
    if not latencies:
        raise BenchError(f"no inotify events arrived ({timeouts} timeouts)")
    method = "host create -> event line received through docker exec stdout, host monotonic clock"
    out = [record("inotify", loc, "latency", "ms", latencies, events=INOTIFY_EVENTS, timeouts=timeouts,
                  method=method)]
    if floor:
        out.append(record("inotify", loc, "pipeline_floor", "ms", floor, events=INOTIFY_FLOOR_EVENTS,
                          method="container-side touch via exec stdin -> event line via exec stdout"))
    return out + [
        record("inotify", loc, "latency_timeouts", "events", [timeouts], timeout_s=INOTIFY_TIMEOUT_S),
        record("inotify", loc, "burst_lost", "events", [b[0] for b in bursts], burst=INOTIFY_BURST,
               overflows=w.overflows, host_create_s=[b[1] for b in bursts],
               last_event_after_s=[b[2] for b in bursts]),
    ]


CASES = {
    "git_status": (git_status, ("bind", "vm", "native")),
    "find_node_modules": (find_node_modules, ("bind", "vm", "native")),
    "untar": (untar, ("bind", "vm", "native")),
    "npm_ci": (npm_ci, ("bind",)),
    "pnpm_install": (pnpm_install, ("bind",)),
    "cargo_build": (cargo_build, ("bind",)),
    "fio": (fio, ("bind",)),
    "microbench": (microbench, ("bind", "vm", "native")),
    "inotify": (inotify, ("bind",)),
}
