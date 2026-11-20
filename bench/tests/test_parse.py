import json

import pytest

from katbench.parse import (ParseError, parse_fio, parse_inotify_line, parse_kbloop, parse_kbtime,
                            parse_loadavg)


def test_kbtime_takes_last_line():
    stderr = "some noise\nKBTIME 100 0\nmore\nKBTIME 123456789 0\n"
    assert parse_kbtime(stderr) == (123456789, 0)


def test_kbtime_nonzero_exit():
    assert parse_kbtime("KBTIME 5 3\n") == (5, 3)


def test_kbtime_missing():
    with pytest.raises(ParseError):
        parse_kbtime("execvp: No such file or directory\n")


def test_kbloop():
    out = "KBLOOP op=stat ops=1000000 files=91159 errors=0 ns_total=812345678 ns_per_op=812.35\n"
    assert parse_kbloop(out) == {"op": "stat", "ops": 1000000, "files": 91159, "errors": 0,
                                 "ns_per_op": 812.35}


def test_kbloop_missing():
    with pytest.raises(ParseError):
        parse_kbloop("nothing here\n")


def _fio_json(read_bw=0, read_iops=0.0, write_bw=0, write_iops=0.0, error=0):
    job = {"jobname": "job", "error": error,
           "read": {"bw_bytes": read_bw, "iops": read_iops},
           "write": {"bw_bytes": write_bw, "iops": write_iops},
           "trim": {"bw_bytes": 0, "iops": 0.0}}
    return json.dumps({"fio version": "fio-3.39", "jobs": [job]})


def test_fio_read_job():
    assert parse_fio(_fio_json(read_bw=2_000_000_000, read_iops=1907.3)) == {"mb_s": 2000.0, "iops": 1907.3}


def test_fio_write_job_with_leading_warning():
    text = "fio: warning: something\n" + _fio_json(write_bw=4_096_000, write_iops=1000.0)
    assert parse_fio(text) == {"mb_s": 4.096, "iops": 1000.0}


def test_fio_job_error():
    with pytest.raises(ParseError):
        parse_fio(_fio_json(error=5))


def test_fio_no_json():
    with pytest.raises(ParseError):
        parse_fio("fio: failed to open file\n")


def test_loadavg():
    assert parse_loadavg("{ 4.42 4.15 4.02 }\n") == 4.42


def test_inotify_lines():
    assert parse_inotify_line("lat-0001 CREATE\n") == ("lat-0001", ["CREATE"])
    assert parse_inotify_line("burst-00001 ATTRIB,CLOSE_WRITE\n") == ("burst-00001", ["ATTRIB", "CLOSE_WRITE"])
    assert parse_inotify_line(" Q_OVERFLOW\n") == ("", ["Q_OVERFLOW"])
    assert parse_inotify_line("\n") is None
