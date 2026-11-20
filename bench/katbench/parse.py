"""Parsers for the output of the tools the harness runs."""

import json
import re


class ParseError(ValueError):
    pass


def parse_kbtime(stderr):
    """Return (elapsed_ns, exit_code) from the last 'KBTIME <ns> <code>' line written by kb_time."""
    matches = re.findall(r"^KBTIME (\d+) (-?\d+)$", stderr, re.MULTILINE)
    if not matches:
        raise ParseError(f"no KBTIME line in output: {stderr[-500:]!r}")
    ns, code = matches[-1]
    return int(ns), int(code)


def parse_kbloop(stdout):
    """Parse the 'KBLOOP key=value ...' line printed by kb_stat and kb_open."""
    for line in stdout.splitlines():
        if line.startswith("KBLOOP "):
            fields = dict(kv.split("=", 1) for kv in line.split()[1:])
            return {
                "op": fields["op"],
                "ops": int(fields["ops"]),
                "files": int(fields["files"]),
                "errors": int(fields["errors"]),
                "ns_per_op": float(fields["ns_per_op"]),
            }
    raise ParseError(f"no KBLOOP line in output: {stdout[-500:]!r}")


def parse_fio(text):
    """Return {'mb_s', 'iops'} summed over read and write for a single-job fio JSON report."""
    # fio may print warnings before the JSON document.
    start = text.find("{")
    if start < 0:
        raise ParseError(f"no JSON in fio output: {text[-500:]!r}")
    data = json.loads(text[start:])
    jobs = data.get("jobs") or []
    if len(jobs) != 1:
        raise ParseError(f"expected 1 fio job, got {len(jobs)}")
    job = jobs[0]
    if job.get("error"):
        raise ParseError(f"fio job error {job['error']}")
    bw_bytes = sum(job[d]["bw_bytes"] for d in ("read", "write"))
    iops = sum(job[d]["iops"] for d in ("read", "write"))
    return {"mb_s": bw_bytes / 1e6, "iops": iops}


def parse_loadavg(text):
    """Parse the 1-minute value from `sysctl -n vm.loadavg` ('{ 1.23 1.10 0.98 }')."""
    m = re.search(r"([\d.]+)\s+([\d.]+)\s+([\d.]+)", text)
    if not m:
        raise ParseError(f"cannot parse load average: {text!r}")
    return float(m.group(1))


def parse_inotify_line(line):
    """Split an inotifywait '--format "%f %e"' line into (name, [events]).

    The name is empty for events on the watched directory itself and for Q_OVERFLOW.
    """
    line = line.rstrip("\n")
    if not line.strip():
        return None
    name, _, events = line.rpartition(" ")
    return name.strip(), events.split(",")
