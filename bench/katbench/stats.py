"""Percentiles and summaries for benchmark samples."""

import math


def percentile(values, q):
    """Return the q-th percentile (0..100) using linear interpolation between closest ranks."""
    if not values:
        raise ValueError("percentile of empty sequence")
    if not 0 <= q <= 100:
        raise ValueError(f"q must be in [0, 100], got {q}")
    s = sorted(values)
    pos = (len(s) - 1) * q / 100
    lo, hi = math.floor(pos), math.ceil(pos)
    return s[lo] + (s[hi] - s[lo]) * (pos - lo)


def summarize(values):
    """p50/p95/min/max/mean of a non-empty sample."""
    return {
        "n": len(values),
        "p50": percentile(values, 50),
        "p95": percentile(values, 95),
        "min": min(values),
        "max": max(values),
        "mean": sum(values) / len(values),
    }
