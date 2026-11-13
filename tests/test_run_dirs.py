"""A run is a directory: ``<log_dir>/<command>-<timestamp>/`` holding its
record (``run.json``), the driver's ``driver.log`` / ``driver.timing``, a
detached run's ``stdout.log`` and every worker's ``w<n>.*``. A ``lux
build``'s steps are runs in its directory.
"""

import os
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest

from lux_pipeline import progress
from lux_pipeline.cli import _handler
from lux_pipeline.progress import (RunRecord, StatusWriter, describe_run, driver_log_path,
                                   latest_prefix, list_runs, log_path, run_path, status_file_run,
                                   status_path, stdout_path, unfinished_workers)


# --- the layout ---------------------------------------------------------------


def test_every_file_of_a_run_is_in_its_directory(tmp_path):
    p = str(tmp_path / "reconcile-2026-11-13-101500")
    assert run_path(p) == f"{p}/run.json"
    assert driver_log_path(p) == f"{p}/driver.log"
    assert progress.driver_timing_path(p) == f"{p}/driver.timing"
    assert stdout_path(p) == f"{p}/stdout.log"
    assert (status_path(p, 3), log_path(p, 3)) == (f"{p}/w3.status", f"{p}/w3.log")
    assert progress.timing_log_path(p, 3) == f"{p}/w3.timing"
    assert progress.distance_log_path(p, 3) == f"{p}/w3.distance"
    assert progress.worker_out_path(p, 3) == f"{p}/w3.out"


def test_the_first_writer_makes_the_directory(tmp_path):
    p = str(tmp_path / "merge-2026-11-13-101500")
    StatusWriter(status_path(p, 0), 0, throttle=0)(advance=1)
    RunRecord(p, "merge", ["lux", "merge"]).start()
    assert sorted(os.listdir(p)) == ["run.json", "w0.status"]
    assert os.listdir(tmp_path) == ["merge-2026-11-13-101500"]


def test_status_file_run():
    assert status_file_run("/l/merge-1/w12.status") == ("/l/merge-1", 12)
    assert status_file_run("/l/merge-1-w12.status") is None
    assert status_file_run("/l/merge-1/w12.log") is None
    assert status_file_run("/l/merge-1/run.json") is None


# --- finding runs -------------------------------------------------------------


def test_a_builds_steps_are_runs_in_its_directory(tmp_path):
    b = str(tmp_path / "build-2026-11-13-101500")
    RunRecord(b, "build", ["lux", "build"]).start().finish(exit=0)
    step = os.path.join(b, "reconcile")
    RunRecord(step, "reconcile", ["lux", "reconcile"], parent=b).start().finish(exit=0)
    StatusWriter(status_path(step, 0), 0, throttle=0)(done=True)
    one_shot = os.path.join(b, "identify")
    RunRecord(one_shot, "identify", ["lux", "identify"], parent=b).start().finish(exit=0)
    runs = {r["prefix"]: r for r in list_runs(str(tmp_path))}
    assert set(runs) == {b, step, one_shot}
    assert runs[step]["parent"] == b and runs[step]["workers"] == 1
    assert latest_prefix(str(tmp_path)) == step


def test_files_outside_a_run_directory_are_not_runs(tmp_path):
    """The flat layout from before runs were directories is not read"""
    (tmp_path / "merge-2026-10-01-101500.run").write_text("{}")
    (tmp_path / "merge-2026-10-01-101500-w0.status").write_text('{"done": false}')
    assert list_runs(str(tmp_path)) == []
    assert unfinished_workers(str(tmp_path)) == []


def test_describe_run_names_a_step_by_its_record(tmp_path):
    step = str(tmp_path / "build-1" / "merge")
    RunRecord(step, "merge", ["lux", "merge"]).start()
    assert describe_run(step + "/")["command"] == "merge"


# --- the build guard and the prefix -------------------------------------------


def test_the_build_guard_sees_workers_in_run_directories(tmp_path):
    step = str(tmp_path / "build-1" / "merge")
    StatusWriter(status_path(step, 0), 0, throttle=0)(advance=1)
    assert [fn for fn, _ in unfinished_workers(str(tmp_path))] == [status_path(step, 0)]
    cfgs = SimpleNamespace(log_dir=str(tmp_path))
    with pytest.raises(RuntimeError, match="a build is running"):
        _handler.refuse_while_building(cfgs, "identify")
    assert _handler.refuse_while_building(cfgs, "identify", force=True) == [step]


def test_two_runs_in_the_same_second_get_their_own_directories(tmp_path, monkeypatch):
    monkeypatch.setattr(_handler, "default_prefix",
                        lambda cfgs, cmd: os.path.join(cfgs.log_dir, f"{cmd}-T"))
    cfgs = SimpleNamespace(log_dir=str(tmp_path))
    first = _handler.free_prefix(cfgs, "merge")
    os.makedirs(first)
    assert _handler.free_prefix(cfgs, "merge") == f"{first}-2"


def test_a_detached_runs_output_is_in_its_directory(tmp_path):
    p = str(tmp_path / "list-2026-11-13-101500")
    proc = _handler.start_detached([sys.executable, "-c", "print('hello')"], _handler.out_path(p))
    proc.wait()
    assert Path(p, "stdout.log").read_text() == "hello\n"
