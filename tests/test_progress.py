"""The progress protocol: a callable every long-running component accepts,
and the StatusWriter / read_status pair the console UI relies on."""

import io
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.progress import (
    StatusWriter, Sliced, log_path, null_progress, read_status, status_path,
)
from lux_pipeline.process.timing import PhaseTimer


class Recorder:
    """A Progress callable that remembers every call."""

    def __init__(self):
        self.calls = []

    def __call__(self, **kw):
        self.calls.append(kw)


# --- null_progress -----------------------------------------------------------


def test_null_progress_accepts_every_keyword_and_returns_none():
    assert null_progress() is None
    assert null_progress(advance=1, total=2, completed=3, description="x",
                         phase="y", done=True) is None


# --- paths -------------------------------------------------------------------


def test_path_convention():
    assert status_path("/logs/merge-20261023", 3) == "/logs/merge-20261023-w3.status"
    assert log_path("/logs/merge-20261023", 3) == "/logs/merge-20261023-w3.log"


# --- StatusWriter ------------------------------------------------------------


def test_unthrottled_writer_writes_after_every_call_and_round_trips(tmp_path):
    path = status_path(str(tmp_path / "load"), 2)
    sw = StatusWriter(path, worker=2, throttle=0)
    sw(phase="load", description="ycba", total=10)
    st = read_status(path)
    assert st["worker"] == 2
    assert st["phase"] == "load"
    assert st["description"] == "ycba"
    assert st["total"] == 10
    assert st["completed"] == 0
    assert st["done"] is False
    assert isinstance(st["updated"], float)

    sw(advance=3)
    sw(advance=4)
    assert read_status(path)["completed"] == 7

    sw(completed=9)
    assert read_status(path)["completed"] == 9

    sw(done=True)
    assert read_status(path)["done"] is True


def test_throttled_writer_only_writes_on_flush(tmp_path):
    path = status_path(str(tmp_path / "load"), 0)
    sw = StatusWriter(path, worker=0, throttle=10)
    sw(total=5)          # the first call writes: nothing was written before
    sw(advance=1)
    sw(advance=1)
    assert read_status(path)["completed"] == 0   # still the first snapshot
    sw.flush()
    assert read_status(path)["completed"] == 2


def test_reset_starts_a_second_pass_from_zero(tmp_path):
    """Reconcile's references pass starts after the records timer has
    reported done and a total; reset clears both, and total=None cannot."""
    path = str(tmp_path / "w.status")
    sw = StatusWriter(path, worker=1, throttle=0)
    sw(phase="reconcile", total=10, advance=10, done=True)
    sw(total=None)
    assert read_status(path)["total"] == 10
    sw(reset=True, phase="reconcile-refs", description="references")
    st = read_status(path)
    assert (st["completed"], st["total"], st["done"]) == (0, None, False)
    assert (st["phase"], st["description"]) == ("reconcile-refs", "references")
    sw(advance=3)
    assert read_status(path)["completed"] == 3


def test_writer_leaves_no_temp_file_behind(tmp_path):
    path = status_path(str(tmp_path / "x"), 0)
    sw = StatusWriter(path, worker=0, throttle=0)
    sw(advance=1)
    sw.flush()
    assert [p.name for p in tmp_path.iterdir()] == ["x-w0.status"]


def test_writer_survives_an_unwritable_path(tmp_path):
    sw = StatusWriter(str(tmp_path / "missing" / "dir" / "x.status"), worker=0, throttle=0)
    sw(advance=1)   # must not raise
    sw.flush()
    assert sw.completed == 1


# --- read_status -------------------------------------------------------------


def test_torn_file_reads_as_none(tmp_path):
    p = tmp_path / "w.status"
    p.write_text('{"wor')
    assert read_status(str(p)) is None


def test_missing_file_reads_as_none(tmp_path):
    assert read_status(str(tmp_path / "nope.status")) is None


def test_non_object_json_reads_as_none(tmp_path):
    p = tmp_path / "w.status"
    p.write_text("[1, 2]")
    assert read_status(str(p)) is None


# --- Sliced ------------------------------------------------------------------


def test_sliced_defaults_and_prepare():
    class Thing(Sliced):
        pass

    t = Thing()
    assert (t.my_slice, t.max_slice) == (-1, -1)
    assert t.progress(advance=1) is None       # null by default
    rec = Recorder()
    assert t.prepare(my_slice=2, max_slice=8, progress=rec) is t
    assert (t.my_slice, t.max_slice) == (2, 8)
    t.progress(advance=1)
    assert rec.calls == [{"advance": 1}]


# --- PhaseTimer --------------------------------------------------------------


def test_phase_timer_reports_through_progress():
    rec = Recorder()
    t = PhaseTimer("merge", slice_n=1, max_slice=4, total=100, report_every=1e9,
                   stream=io.StringIO(), progress=rec)
    assert {"phase": "merge"} in rec.calls
    assert {"total": 100} in rec.calls
    t.step()
    t.step(3)
    t.skip(2)
    t.mark("ycba")
    t.finish()
    assert {"advance": 1} in rec.calls
    assert {"advance": 3} in rec.calls
    assert {"advance": 2} in rec.calls
    assert {"description": "ycba"} in rec.calls
    assert rec.calls[-1] == {"completed": 6, "done": True}


def test_phase_timer_default_progress_is_null():
    t = PhaseTimer("merge", report_every=1e9, stream=io.StringIO())
    assert t.progress is null_progress
    t.step()
    t.finish()
