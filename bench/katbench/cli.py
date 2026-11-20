"""Command line: `bench --engine <context>`, `bench prepare`, `bench compare A B`, `bench list`."""

import argparse
import datetime as dt
import json
import os
import re
import subprocess
import sys
import time
import traceback
from pathlib import Path

from . import cases as C
from .datasets import BENCH, pins, prepare_host
from .engine import CACHE_VOLUMES, Container, Docker, Native, ensure_image, image_info
from .report import render, render_compare
from .sysinfo import docker_info, engine_identity, image_tools, loadavg_1m, machine

SUBCOMMANDS = {"run", "prepare", "compare", "list"}


def log(msg):
    print(f"[{dt.datetime.now():%H:%M:%S}] {msg}", file=sys.stderr, flush=True)


def busy_containers(context, prefix):
    """Names of running containers on `context` (None: the current context) that start with prefix
    ([] if the engine is down)."""
    argv = ["docker"] + (["--context", context] if context else []) + ["ps", "--format", "{{.Names}}"]
    try:
        proc = subprocess.run(argv, capture_output=True, text=True, timeout=30)
    except subprocess.TimeoutExpired:
        return []
    return [n for n in proc.stdout.split() if n.startswith(prefix)] if proc.returncode == 0 else []


def wait_quiet(args):
    """Block until the 1-minute load average is below --max-load and no build containers run.

    Returns (load average at return, seconds waited).
    """
    waited = 0
    while True:
        load = loadavg_1m()
        busy = busy_containers(args.quiet_context, args.quiet_prefix)
        if args.no_wait or (load < args.max_load and not busy):
            return load, waited
        if waited % 60 == 0:
            log(f"waiting for a quiet machine: load {load:.2f} (need < {args.max_load}), "
                f"{args.quiet_prefix}* running: {', '.join(busy) or 'none'}")
        time.sleep(15)
        waited += 15


def selected_cases(values):
    names = [n for v in values for n in v.split(",") if n]
    unknown = [n for n in names if n not in C.CASES]
    if unknown:
        raise SystemExit(f"unknown case(s): {', '.join(unknown)}; see `bench list`")
    return [n for n in C.CASES if n in names]


def result_path(args, engine_version, date):
    if args.output:
        return Path(args.output)
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", f"{args.engine}-{engine_version}-{date}")
    path = BENCH / "results" / f"{safe}.json"
    n = 2
    while path.exists():
        path = BENCH / "results" / f"{safe}-{n}.json"
        n += 1
    return path


def setup_engine(args):
    prepare_host(log)
    docker = Docker(args.engine)
    tag = ensure_image(docker, log)
    for vol in CACHE_VOLUMES:
        docker.run("volume", "create", vol)
    return docker, tag


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2) + "\n")
    tmp.rename(path)


def cmd_prepare(args):
    docker, tag = setup_engine(args)
    container = Container(docker, tag, f"katbench-prepare-{os.getpid()}")
    container.start()
    try:
        C.prepare_engine(C.Ctx(container, Native(), 0, log))
    finally:
        container.remove()
    log("prepared")


def cmd_run(args):
    started = time.time()
    names = selected_cases(args.cases)
    locations = set(args.locations.split(","))
    docker, tag = setup_engine(args)
    dinfo = docker_info(docker)
    engine = engine_identity(args.engine, dinfo["server_version"])
    if args.engine_version:
        engine["version"] = args.engine_version
    date = dt.date.today().isoformat()
    path = result_path(args, engine["version"], date)

    container = Container(docker, tag, f"katbench-run-{os.getpid()}")
    container.start()
    try:
        ctx = C.Ctx(container, Native(), args.runs, log)
        if not args.skip_prepare:
            C.prepare_engine(ctx)
        result = {
            "schema": 1,
            "date": date,
            "started_at": dt.datetime.fromtimestamp(started).astimezone().isoformat(timespec="seconds"),
            "engine": engine,
            "docker": dinfo,
            "machine": machine(),
            "image": {**image_info(docker, tag), "tools": image_tools(container)},
            "datasets": pins(),
            "runs": args.runs,
            "settings": {
                "max_load": args.max_load, "quiet_context": args.quiet_context,
                "quiet_prefix": args.quiet_prefix, "no_wait": args.no_wait,
                "micro_ops": C.MICRO_OPS, "fio_common": C.FIO_COMMON, "fio_jobs": C.FIO_JOBS,
                "inotify": {"warmup": C.INOTIFY_WARMUP, "events": C.INOTIFY_EVENTS,
                            "burst": C.INOTIFY_BURST, "timeout_s": C.INOTIFY_TIMEOUT_S,
                            "quiet_s": C.INOTIFY_QUIET_S},
                "commands": {"npm_ci": C.NPM_CI, "pnpm_install": C.PNPM_INSTALL, "cargo_build": C.CARGO_BUILD},
            },
            "results": [],
            "errors": [],
        }
        for name in names:
            fn, locs = C.CASES[name]
            for loc in locs:
                if loc not in locations:
                    continue
                load, waited = wait_quiet(args)
                log(f"{name} @ {loc} (load {load:.2f})")
                t0 = time.time()
                try:
                    records = fn(ctx, loc)
                except Exception as e:  # keep going; the failure is recorded in the result file
                    log(f"{name} @ {loc} failed: {e}")
                    result["errors"].append({"case": name, "location": loc, "error": str(e),
                                             "traceback": traceback.format_exc()})
                    continue
                for rec in records:
                    rec.update(loadavg_1m_start=load, waited_for_quiet_s=waited,
                               case_wall_s=round(time.time() - t0, 1))
                result["results"].extend(records)
                result["duration_s"] = round(time.time() - started, 1)
                write_json(path, result)
    finally:
        container.remove()

    result["finished_at"] = dt.datetime.now().astimezone().isoformat(timespec="seconds")
    result["duration_s"] = round(time.time() - started, 1)
    write_json(path, result)
    log(f"wrote {path} ({result['duration_s']:.0f} s)")
    print(render(result))


def cmd_compare(args):
    a, b = (json.loads(Path(p).read_text()) for p in (args.a, args.b))
    print(render_compare(a, b))


def cmd_list(args):
    for name, (_, locs) in C.CASES.items():
        print(f"{name:20} {','.join(locs)}")


def build_parser():
    p = argparse.ArgumentParser(prog="bench", description=__doc__)
    sub = p.add_subparsers(dest="command", required=True)

    for name, help_text in (("run", "run the benchmark suite (default)"), ("prepare", "only prepare datasets")):
        s = sub.add_parser(name, help=help_text)
        s.add_argument("--engine", required=True, help="Docker context name (e.g. katamaran, desktop-linux)")
        if name == "run":
            s.add_argument("--cases", nargs="+", default=[",".join(C.CASES)], help="case names (see `list`)")
            s.add_argument("--locations", default="bind,vm,native", help="subset of bind,vm,native")
            s.add_argument("--runs", type=int, default=5, help="measured runs per case (after 1 warm-up)")
            s.add_argument("--max-load", type=float, default=3.0, help="wait until the 1-min load is below this")
            s.add_argument("--quiet-context", default=None,
                           help="context checked for build containers (default: the current context)")
            s.add_argument("--quiet-prefix", default="katamaran-kbuild", help="build container name prefix")
            s.add_argument("--no-wait", action="store_true", help="do not wait for a quiet machine")
            s.add_argument("--skip-prepare", action="store_true", help="do not warm package caches first")
            s.add_argument("--engine-version", help="override the engine version used in the file name")
            s.add_argument("--output", help="result file path (default bench/results/<engine>-<ver>-<date>.json)")
        s.set_defaults(func=cmd_run if name == "run" else cmd_prepare)

    s = sub.add_parser("compare", help="render two result files side by side (B relative to A)")
    s.add_argument("a")
    s.add_argument("b")
    s.set_defaults(func=cmd_compare)

    s = sub.add_parser("list", help="list cases and their locations")
    s.set_defaults(func=cmd_list)
    return p


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] not in SUBCOMMANDS | {"-h", "--help"}:
        argv.insert(0, "run")
    args = build_parser().parse_args(argv)
    args.func(args)
