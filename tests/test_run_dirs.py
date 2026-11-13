"""A run is a directory: ``<log_dir>/<command>-<timestamp>/`` holding its
record (``run.json``), the driver's ``driver.log`` / ``driver.timing``, a
detached run's ``stdout.log`` and every worker's ``w<n>.*``. A ``lux
build``'s steps are runs in its directory. Runs written before, flat in
``log_dir`` (``<prefix>.run``, ``<prefix>-w<n>.status``), are still read.
"""

import json
import os
import socket
import sys
import time
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest

from lux_pipeline import progress
from lux_pipeline.cli import _handler
from lux_pipeline.progress import (LogTailer, RunRecord, StatusWriter, count_workers, describe_run,
                                   driver_log_path, is_legacy, latest_prefix, list_runs, log_path,
                                   read_build, read_run, run_exists, run_path, status_file_run,
                                   status_path, stdout_path, unfinished_workers)


def legacy_run(log_dir, name="merge-2026-10-01-101500", workers=2, done=True):
    """A run as `lux` wrote them before runs were directories."""
    prefix = os.path.join(str(log_dir), name)
    for n in range(workers):
        Path(f"{prefix}-w{n}.status").write_text(json.dumps(
            {"worker": n, "phase": "merge", "description": f"s{n}", "completed": 5,
             "total": 5, "updated": time.time(), "done": done}))
        Path(f"{prefix}-w{n}.log").write_text(f"10:00:0{n} INFO     worker {n}\n")
    Path(f"{prefix}.log").write_text("10:00:09 INFO     driver line\n")
    Path(f"{prefix}.run").write_text(json.dumps(
        {"prefix": prefix, "command": "merge", "argv": ["lux", "merge"], "host": socket.gethostname(),
         "pid": None, "workers": workers, "started": 1000.0, "finished": 1001.0, "exit": 0}))
    Path(f"{prefix}.out").write_text("output\n")
    return prefix


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


def test_status_file_run_reads_both_layouts():
    assert status_file_run("/l/merge-1/w12.status") == ("/l/merge-1", 12)
    assert status_file_run("/l/merge-1-w12.status") == ("/l/merge-1", 12)
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


def test_a_run_from_before_is_still_read(tmp_path):
    p = legacy_run(tmp_path)
    assert is_legacy(p)
    assert run_path(p) == f"{p}.run" and status_path(p, 1) == f"{p}-w1.status"
    assert stdout_path(p) == f"{p}.out" and driver_log_path(p) == f"{p}.log"
    assert run_exists(p) and count_workers(p) == 2
    assert read_run(p)["command"] == "merge"
    assert read_build(p, 2)["done"] is True
    assert LogTailer(p, 2).read_new_lines() == [
        (0, "10:00:00 INFO     worker 0"), (1, "10:00:01 INFO     worker 1"),
        ("driver", "10:00:09 INFO     driver line")]
    [r] = list_runs(str(tmp_path))
    assert (r["prefix"], r["command"], r["state"], r["workers"]) == (p, "merge", "finished", 2)


def test_old_and_new_runs_are_listed_together(tmp_path):
    old = legacy_run(tmp_path)
    new = str(tmp_path / "merge-2026-11-13-101500")
    RunRecord(new, "merge", ["lux", "merge"]).start()
    assert {r["prefix"] for r in list_runs(str(tmp_path))} == {old, new}
    assert not is_legacy(new)


def test_a_new_run_is_never_written_flat(tmp_path):
    """No directory yet and nothing flat there: a new run, in a directory"""
    p = str(tmp_path / "export-2026-11-13-101500")
    assert not is_legacy(p)
    assert status_path(p, 0) == f"{p}/w0.status"


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
