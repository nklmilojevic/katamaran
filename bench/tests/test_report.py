from katbench.cases import record
from katbench.report import fmt, ratio_text, render, render_compare


def _result(product, version, records):
    return {
        "engine": {"product": product, "version": version},
        "docker": {"context": product.lower()},
        "machine": {"model": "Mac15,10", "cores": 14, "memory_bytes": 36 * 2**30, "macos": "27.2.1"},
        "date": "2026-11-18",
        "runs": 3,
        "results": records,
        "errors": [{"case": "inotify", "location": "bind", "error": "no events\ntrace"}],
    }


def test_fmt():
    assert fmt(None) == "–"
    assert fmt(0, "events") == "0"
    assert fmt(2.0, "s") == "2.00"
    assert fmt(1234.56) == "1,235"
    assert fmt(12.345) == "12.3"
    assert fmt(1.2345) == "1.23"
    assert fmt(0.12345) == "0.123"


def test_ratio_text():
    assert ratio_text(2.0, 2.5, "lower") == ("1.25", "1.25× slower")
    assert ratio_text(2.0, 1.0, "lower") == ("0.50", "2.00× faster")
    assert ratio_text(100.0, 50.0, "higher") == ("0.50", "2.00× worse")
    assert ratio_text(1.0, 1.001, "lower") == ("1.00", "same")
    assert ratio_text(None, 1.0, "lower") == ("–", "–")
    assert ratio_text(0, 0, "lower") == ("–", "same")


def test_render_pivots_locations():
    recs = [
        record("git_status", "bind", "time", "s", [1.0, 2.0, 3.0]),
        record("git_status", "native", "time", "s", [0.5, 0.5, 0.5]),
        record("fio", "bind", "seq_read_1m_mb_s", "MB/s", [1000.0, 1100.0, 1200.0], "higher"),
    ]
    out = render(_result("Docker", "29.0.0", recs))
    lines = out.splitlines()
    assert lines[0].startswith("### Docker 29.0.0")
    row = next(l for l in lines if l.startswith("| git_status"))
    assert row == "| git_status | time | s | 2.00 | 2.90 | – | – | 0.500 | 0.500 |"
    # case order follows the case registry, not insertion order
    assert lines.index(row) < lines.index(next(l for l in lines if l.startswith("| fio")))
    assert "- not measured: inotify @ bind: no events" in out


def test_render_compare():
    a = _result("Docker", "29.0.0", [record("git_status", "bind", "time", "s", [1.0, 1.0, 1.0])])
    b = _result("katamaran", "0.1.0", [record("git_status", "bind", "time", "s", [2.0, 2.0, 2.0]),
                                       record("npm_ci", "bind", "time", "s", [9.0, 9.0, 9.0])])
    out = render_compare(a, b)
    assert "| git_status | time | bind | s | 1.00 | 1.00 | 2.00 | 2.00 | 2.00 | 2.00× slower |" in out
    assert "| npm_ci | time | bind | s | – | – | 9.00 | 9.00 | – | – |" in out
