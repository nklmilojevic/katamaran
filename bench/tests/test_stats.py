import pytest

from katbench.stats import percentile, summarize


def test_percentile_single_value():
    assert percentile([7.0], 50) == 7.0
    assert percentile([7.0], 95) == 7.0


def test_percentile_interpolates_linearly():
    values = [5, 1, 4, 2, 3]  # sorted: 1..5, positions 0..4
    assert percentile(values, 0) == 1
    assert percentile(values, 50) == 3
    assert percentile(values, 95) == pytest.approx(4.8)
    assert percentile(values, 100) == 5


def test_percentile_even_count_median():
    assert percentile([1, 2, 3, 4], 50) == 2.5


def test_percentile_rejects_bad_input():
    with pytest.raises(ValueError):
        percentile([], 50)
    with pytest.raises(ValueError):
        percentile([1], 101)


def test_summarize():
    s = summarize([2.0, 4.0, 6.0])
    assert s == {"n": 3, "p50": 4.0, "p95": pytest.approx(5.8), "min": 2.0, "max": 6.0, "mean": 4.0}
