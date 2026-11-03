"""Every phase writes logs, and a watcher passes from one to the next.

The identify step runs in the reconcile driver after the last worker has
finished, for hours, logging to stdout: every status file said done, no log
file moved, and `lux watch` ended ("finished") while the web UI's run page
sat on 100% with nothing new. The one-shot steps of a build (sheets, a
clear, portal) had no files at all. Now:

* a command given ``--prefix`` logs to ``<prefix>.log`` -- the driver's log,
  tailed beside the workers' as the worker named ``driver`` -- and, when it
  writes no record of its own, gets ``<prefix>.run`` from the entry point;
* ``lux build`` gives every step a prefix, the one-shot ones included;
* the console view is over when the run's record says so, not when the
  workers are, and grows rows as workers appear;
* ``lux watch <build>`` follows the build's steps in turn.
"""

import io
import json
import logging
import os
import sys
import threading
import time
import types
from pathlib import Path

import pytest
from rich.console import Console

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline import runtime
from lux_pipeline.cli import entry
from lux_pipeline.cli._handler import BaseHandler, CommandHandler, ensure_driver_log
from lux_pipeline.cli._view import BuildView
from lux_pipeline.process.tasks._task_manager import TaskManager, attach_worker_log_handler
from lux_pipeline.progress import (DRIVER, LogTailer, RunRecord, StatusWriter, current_step,
                                   driver_log_path, log_path, read_run, status_path)

LOGGER = logging.getLogger("lux_pipeline")


@pytest.fixture(autouse=True)
def _restore_logger():
    saved = (list(LOGGER.handlers), LOGGER.level)
    yield
    for h in list(LOGGER.handlers):
        LOGGER.removeHandler(h)
        if isinstance(h, logging.FileHandler):
            h.close()
    for h in saved[0]:
        LOGGER.addHandler(h)
    LOGGER.setLevel(saved[1])


def console():
    return Console(file=io.StringIO(), width=100, height=50, force_terminal=True)


# --- the driver's log file -------------------------------------------------------


def test_the_driver_log_is_in_the_workers_format(tmp_path):
    prefix = str(tmp_path / "reconcile-1")
    runtime.configure_logging("INFO", stream=io.StringIO())
    h = ensure_driver_log(prefix, "INFO")
    assert ensure_driver_log(prefix, "INFO") is h          # once per prefix
    LOGGER.info("Resolving the identity map")
    LOGGER.debug("not at INFO")
    h.flush()
    lines = Path(driver_log_path(prefix)).read_text().splitlines()
    assert len(lines) == 1
    assert lines[0][8:] == " INFO     Resolving the identity map"
    assert ensure_driver_log(None) is None


def test_the_tailer_reads_the_driver_as_its_own_worker(tmp_path):
    prefix = str(tmp_path / "x")
    Path(log_path(prefix, 0)).write_text("10:00:00 INFO     w0 line\n")
    Path(driver_log_path(prefix)).write_text("10:00:01 INFO     identify ...\n")
    t = LogTailer(prefix, 1)
    assert t.read_new_lines() == [(0, "10:00:00 INFO     w0 line"),
                                  (DRIVER, "10:00:01 INFO     identify ...")]
    assert t.offsets[DRIVER] == os.path.getsize(driver_log_path(prefix))
    assert LogTailer(prefix, 1, driver=False).read_new_lines() == [(0, "10:00:00 INFO     w0 line")]


# --- the view ----------------------------------------------------------------------


def two_workers(tmp_path, done=True):
    prefix = str(tmp_path / "reconcile-2026-11-03-101500")
    for n in range(2):
        StatusWriter(status_path(prefix, n), n, throttle=0)(total=10, completed=10, done=done)
        Path(log_path(prefix, n)).write_text(f"10:15:0{n} INFO     worker {n}\n")
    return prefix


def test_the_view_shows_the_driver_and_stays_until_the_record_is_finished(tmp_path):
    """Workers done, driver still identifying: not over."""
    prefix = two_workers(tmp_path)
    rec = RunRecord(prefix, "reconcile", ["lux", "reconcile"], workers=2).start()
    Path(driver_log_path(prefix)).write_text("10:20:00 INFO     Resolving the identity map\n")
    view = BuildView(prefix, 2, console=console(), auto_refresh=False)
    assert view.refresh() is False
    assert view.state == "running"
    assert "driver 10:20:00 INFO     Resolving the identity map" in view.log_panel.snapshot()
    rec.finish(exit=0)
    assert view.refresh() is True
    assert view.state == "finished"


def test_without_a_record_the_workers_decide(tmp_path):
    view = BuildView(two_workers(tmp_path), 2, console=console(), auto_refresh=False)
    assert view.refresh() is True


def test_a_failed_record_ends_the_view_as_failed(tmp_path):
    prefix = two_workers(tmp_path)
    RunRecord(prefix, "merge", ["lux", "merge"], workers=2).start().finish(exit=1, error="boom")
    view = BuildView(prefix, 2, console=console(), auto_refresh=False)
    assert view.refresh() is True and view.state == "failed"


def test_rows_appear_as_workers_do(tmp_path):
    prefix = str(tmp_path / "load-1")
    RunRecord(prefix, "load", ["lux", "load"], workers=0).start()
    view = BuildView(prefix, 0, console=console(), auto_refresh=False)
    assert view.refresh() is False and view.tasks == {}
    StatusWriter(status_path(prefix, 0), 0, throttle=0)(total=5, completed=1)
    StatusWriter(status_path(prefix, 1), 1, throttle=0)(total=5, completed=2)
    view.refresh()
    assert sorted(view.tasks) == [0, 1] and view.workers == 2
    assert view._task(view.tasks[1]).completed == 2
    assert view._task(view.all_task).description == "all 2 workers"


def test_the_view_leaves_the_driver_log_handler_in_place(tmp_path):
    prefix = two_workers(tmp_path)
    stream = runtime.configure_logging("INFO", stream=io.StringIO())
    h = ensure_driver_log(prefix, "INFO")
    with BuildView(prefix, 2, console=console(), auto_refresh=False) as view:
        assert stream not in LOGGER.handlers           # the console is the panel now
        assert h in LOGGER.handlers                    # the file stays
        LOGGER.info("from the driver")
    h.flush()
    assert "from the driver" in Path(driver_log_path(prefix)).read_text()
    assert any(l.endswith("from the driver") for l in view.log_panel.snapshot())
    assert stream in LOGGER.handlers and h in LOGGER.handlers


# --- entry: a one-shot command with a prefix is a run ---------------------------


class OneShot(BaseHandler):
    needs_configs = False
    fail = False

    def process(self, args, rest):
        super().process(args, rest)
        LOGGER.info("doing the one thing")
        if self.fail:
            raise ValueError("it broke")


@pytest.fixture
def one_shot(monkeypatch):
    OneShot.fail = False
    mod = types.ModuleType("lux_pipeline.cli.one_shot")
    mod.CommandHandler = OneShot
    monkeypatch.setitem(sys.modules, "lux_pipeline.cli.one_shot", mod)
    monkeypatch.setattr(runtime, "get_configs", lambda *a, **kw: None)
    return mod


def test_a_one_shot_command_with_a_prefix_has_a_record_and_a_log(tmp_path, one_shot, monkeypatch):
    prefix = str(tmp_path / "build-1-sheets")
    monkeypatch.setenv("LUX_PARENT_RUN", str(tmp_path / "build-1"))
    assert entry.run(["one-shot", "--prefix", prefix]) == 0
    rec = read_run(prefix)
    assert rec["command"] == "one-shot" and rec["workers"] == 0 and rec["exit"] == 0
    assert rec["parent"] == str(tmp_path / "build-1")
    assert rec["argv"] == ["lux", "one-shot", "--prefix", prefix]
    assert "doing the one thing" in Path(driver_log_path(prefix)).read_text()


def test_a_one_shot_failure_is_in_its_record(tmp_path, one_shot):
    OneShot.fail = True
    prefix = str(tmp_path / "sheets-1")
    assert entry.run(["one-shot", "--prefix", prefix]) == 1
    rec = read_run(prefix)
    assert rec["exit"] == 1 and rec["error"] == "ValueError: it broke"


def test_without_a_prefix_a_one_shot_command_leaves_no_files(tmp_path, one_shot, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert entry.run(["one-shot"]) == 0
    assert list(tmp_path.iterdir()) == []


def test_watch_takes_the_prefix_as_the_run_to_watch_not_its_own(tmp_path, monkeypatch, capsys):
    from lux_pipeline.cli import watch
    assert watch.CommandHandler.uses_prefix is False
    monkeypatch.setattr(runtime, "get_configs", lambda *a, **kw: None)
    prefix = str(tmp_path / "nothing")
    assert entry.run(["watch", "--prefix", prefix]) == 1
    assert not os.path.exists(f"{prefix}.run") and not os.path.exists(f"{prefix}.log")


def test_a_worker_command_keeps_its_own_record(tmp_path, monkeypatch):
    """CommandHandler writes the record with the manager's details; entry
    must not write a second one over it."""
    class Manager:
        max_workers = 2
        sources = ["aat"]

        def prepare_single(self, s):
            pass

        def process(self, engine, prefix, view):
            LOGGER.info("workers ran")

    class H(CommandHandler):
        needs_configs = False

        def make_manager(self, workers, args):
            return Manager()

        def resolve_sources(self, args):
            return ["aat"]

    mod = types.ModuleType("lux_pipeline.cli.fake_phase")
    mod.CommandHandler = H
    monkeypatch.setitem(sys.modules, "lux_pipeline.cli.fake_phase", mod)
    monkeypatch.setattr(runtime, "get_configs", lambda *a, **kw: None)
    prefix = str(tmp_path / "fake-phase-1")
    assert entry.run(["fake-phase", "--prefix", prefix, "--no-ui", "--max-workers", "2"]) == 0
    rec = read_run(prefix)
    assert rec["workers"] == 2 and rec["sources"] == ["aat"] and rec["exit"] == 0
    assert "workers ran" in Path(driver_log_path(prefix)).read_text()


# --- the null engine gives the driver its handlers back ---------------------------


class Quiet(TaskManager):
    needs_configs = False

    def _distributed(self, n, cfgs, progress):
        LOGGER.info(f"worker {n} working")
        return n


def test_the_null_engine_restores_the_drivers_handlers(tmp_path):
    prefix = str(tmp_path / "load-2")
    stream = runtime.configure_logging("INFO", stream=io.StringIO())
    h = ensure_driver_log(prefix, "INFO")
    m = Quiet(None, 1)
    m.process("null", prefix, None)
    LOGGER.info("the driver's tail")
    h.flush()
    assert stream in LOGGER.handlers and h in LOGGER.handlers
    assert "worker 0 working" in Path(log_path(prefix, 0)).read_text()
    text = Path(driver_log_path(prefix)).read_text()
    assert "the driver's tail" in text and "worker 0 working" not in text


# --- lux watch follows a build --------------------------------------------------


def test_current_step():
    assert current_step(None) is None
    assert current_step({"steps": []}) is None
    rec = {"steps": [{"phase": "sheets", "finished": 2}, {"phase": "reconcile", "finished": None},
                     {"phase": "merge", "finished": None}]}
    assert current_step(rec)["phase"] == "reconcile"


def run_lux(argv, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["lux", *argv])
    with pytest.raises(SystemExit) as ex:
        entry.main()
    return ex.value.code


def drive_a_build(logs, pause=0.05):
    """A fake `lux build` in a thread: a one-shot step (its record and log
    only), then a two-worker step whose driver goes on after its workers
    (reconcile's identify), then the build finishes."""
    build = str(logs / "build-2026-11-03-120000")
    rec = RunRecord(build, "build", ["lux", "build"], workers=0,
                    extra={"phases": ["sheets", "reconcile"], "current": None, "steps": []}).start()

    def go():
        time.sleep(pause)
        # step 1: sheets, a one-shot
        sheets = f"{build}-sheets"
        step = {"phase": "sheets", "command": "sheets", "argv": ["lux", "sheets"], "prefix": sheets,
                "started": time.time(), "finished": None, "exit": None}
        rec.data["steps"].append(step)
        rec.update(current="sheets")
        srec = RunRecord(sheets, "sheets", ["lux", "sheets"], workers=0, parent=build).start()
        Path(driver_log_path(sheets)).write_text("12:00:01 INFO     read 3 tabs\n")
        time.sleep(pause)
        srec.finish(exit=0)
        step["finished"], step["exit"] = time.time(), 0
        rec.update()
        time.sleep(pause)
        # step 2: reconcile, two workers, then the driver's tail
        recon = f"{build}-reconcile"
        step = {"phase": "reconcile", "command": "reconcile", "argv": ["lux", "reconcile"],
                "prefix": recon, "started": time.time(), "finished": None, "exit": None}
        rec.data["steps"].append(step)
        rec.update(current="reconcile")
        rrec = RunRecord(recon, "reconcile", ["lux", "reconcile"], workers=2, parent=build).start()
        time.sleep(pause)
        for n in range(2):
            StatusWriter(status_path(recon, n), n, throttle=0)(total=4, completed=4, done=True)
            Path(log_path(recon, n)).write_text(f"12:00:1{n} INFO     worker {n} done\n")
        time.sleep(pause)
        Path(driver_log_path(recon)).write_text("12:00:20 INFO     Resolving the identity map\n")
        time.sleep(pause * 3)
        with open(driver_log_path(recon), "a") as fh:
            fh.write("12:00:30 INFO     Done, ready to merge\n")
        rrec.finish(exit=0, workers=2)
        step["finished"], step["exit"] = time.time(), 0
        rec.update(current=None)
        time.sleep(pause)
        rec.finish(exit=0)

    t = threading.Thread(target=go, daemon=True)
    t.start()
    return build, t


def test_watch_follows_a_build_step_by_step(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(runtime, "get_configs", lambda *a, **kw: None)
    build, t = drive_a_build(tmp_path)
    assert run_lux(["watch", build, "--interval", "0.01"], monkeypatch) == 0
    t.join(5)
    out = capsys.readouterr().out
    # each step announced, each step's run reported, the build's end
    assert out.index("=== sheets: lux sheets ===") < out.index("=== reconcile: lux reconcile ===")
    assert f"{build}-sheets: 0 worker(s), finished" in out
    assert f"{build}-reconcile: 2 worker(s), finished" in out
    assert out.rstrip().endswith(f"{build}: 2 step(s), finished")
    # the driver's lines after the workers were done are what the watcher saw
    assert "Resolving the identity map" in out and "Done, ready to merge" in out
    assert "read 3 tabs" in out


def test_watch_latest_hands_a_phase_over_to_its_build(tmp_path, monkeypatch, capsys):
    class Cfgs:
        log_dir = str(tmp_path)
    monkeypatch.setattr(runtime, "get_configs", lambda instantiate=True: Cfgs())
    build, t = drive_a_build(tmp_path)
    time.sleep(0.2)          # into the first step
    assert run_lux(["watch", "--latest", "--interval", "0.01"], monkeypatch) == 0
    t.join(5)
    out = capsys.readouterr().out
    assert f"{build}: 2 step(s), finished" in out


def test_watch_a_finished_build_replays_its_steps(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(runtime, "get_configs", lambda *a, **kw: None)
    build, t = drive_a_build(tmp_path, pause=0.01)
    t.join(5)
    assert run_lux(["watch", build, "--interval", "0.01"], monkeypatch) == 0
    out = capsys.readouterr().out
    assert out.count("===") == 4 and "2 step(s), finished" in out


def test_watch_a_one_shot_run_with_no_workers(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(runtime, "get_configs", lambda *a, **kw: None)
    prefix = str(tmp_path / "sheets-1")
    RunRecord(prefix, "sheets", ["lux", "sheets"], workers=0).start().finish(exit=0)
    Path(driver_log_path(prefix)).write_text("12:00:01 INFO     read 3 tabs\n")
    assert run_lux(["watch", prefix, "--interval", "0.01"], monkeypatch) == 0
    out = capsys.readouterr().out
    assert f"{prefix}: 0 worker(s), finished" in out and "read 3 tabs" in out
