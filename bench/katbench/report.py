"""Markdown rendering of one result file, and of two result files side by side."""

from .cases import CASES

LOCATIONS = (("bind", "Bind"), ("vm", "In-VM"), ("native", "Native"))


COUNT_UNITS = {"events"}


def fmt(v, unit=None):
    if v is None:
        return "–"
    a = abs(v)
    if a >= 100 or unit in COUNT_UNITS:
        return f"{v:,.0f}"
    if a >= 10:
        return f"{v:.1f}"
    if a >= 1:
        return f"{v:.2f}"
    return f"{v:.3f}"


def _order(rec):
    names = list(CASES)
    return (names.index(rec["case"]) if rec["case"] in names else len(names), rec["metric"])


def _index(result):
    """{(case, metric): {location: record}} in case order."""
    rows = {}
    for rec in sorted(result["results"], key=_order):
        rows.setdefault((rec["case"], rec["metric"]), {})[rec["location"]] = rec
    return rows


def title(result):
    e, m = result["engine"], result["machine"]
    return (f"{e['product']} {e['version']} (context `{result['docker']['context']}`), {result['date']}, "
            f"{m['model']} {m['cores']} cores {m['memory_bytes'] / 2**30:.0f} GiB, macOS {m['macos']}, "
            f"runs={result['runs']}")


def render(result):
    lines = [f"### {title(result)}", ""]
    header = ["Case", "Metric", "Unit"]
    for _, label in LOCATIONS:
        header += [f"{label} p50", f"{label} p95"]
    lines += ["| " + " | ".join(header) + " |", "|" + "---|" * 3 + "--:|" * (len(header) - 3)]
    for (case, metric), by_loc in _index(result).items():
        unit = next(iter(by_loc.values()))["unit"]
        cells = [case, metric, unit]
        for loc, _ in LOCATIONS:
            rec = by_loc.get(loc)
            cells += [fmt(rec["p50"], unit), fmt(rec["p95"], unit)] if rec else ["–", "–"]
        lines.append("| " + " | ".join(cells) + " |")
    lines += _problems(result)
    return "\n".join(lines)


def _problems(result):
    out = []
    for item in result.get("errors", []):
        out.append(f"- not measured: {item['case']} @ {item['location']}: {item['error'].splitlines()[0]}")
    return [""] + out if out else []


def ratio_text(a, b, better):
    """Describe B relative to A ('1.25× slower', '2.00× faster', 'same')."""
    if a is None or b is None:
        return "–", "–"
    if a == 0 or b == 0:
        return "–", "same" if a == b else "–"
    r = b / a
    worse = r > 1 if better == "lower" else r < 1
    factor = r if r >= 1 else 1 / r
    if abs(factor - 1) < 0.005:
        return f"{r:.2f}", "same"
    word = ("slower" if worse else "faster") if better == "lower" else ("worse" if worse else "better")
    return f"{r:.2f}", f"{factor:.2f}× {word}"


def render_compare(a, b):
    la = f"{a['engine']['product']} {a['engine']['version']}"
    lb = f"{b['engine']['product']} {b['engine']['version']}"
    lines = [f"### A = {title(a)}", f"### B = {title(b)}", "",
             f"| Case | Metric | Location | Unit | A p50 ({la}) | A p95 | B p50 ({lb}) | B p95 | B/A p50 | B vs A |",
             "|---|---|---|---|--:|--:|--:|--:|--:|---|"]
    ia, ib = _index(a), _index(b)
    keys = list(ia) + [k for k in ib if k not in ia]
    for key in keys:
        for loc, _ in LOCATIONS:
            ra, rb = ia.get(key, {}).get(loc), ib.get(key, {}).get(loc)
            if not ra and not rb:
                continue
            rec = ra or rb
            u = rec["unit"]
            r, verdict = ratio_text(ra and ra["p50"], rb and rb["p50"], rec["better"])
            lines.append("| " + " | ".join([
                key[0], key[1], loc, rec["unit"],
                fmt(ra and ra["p50"], u), fmt(ra and ra["p95"], u), fmt(rb and rb["p50"], u), fmt(rb and rb["p95"], u),
                r, verdict,
            ]) + " |")
    return "\n".join(lines)
