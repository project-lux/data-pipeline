"""Item W01: everything a run leaves on disk, read in one place
(lux_pipeline.progress): read_build, the run record (<prefix>.run) that
the driver writes, run_state and list_runs; and the record written by
the worker commands' handler and finished by the entry point."""

import json
import logging
import os
import socket
import sys
import time
import types
from argparse import Namespace
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline import progress, runtime
from lux_pipeline.cli import entry
from lux_pipeline.cli._handler import CommandHandler as CH
from lux_pipeline.process.tasks._task_manager import TaskManager
from lux_pipeline.progress import (
    LogTailer, RunRecord, StatusWriter, count_workers, latest_prefix, list_runs, pid_alive,
    read_build, read_run, run_path, run_state, status_path,
)
from webui_fixtures import dead_pid, write_fake_run

LOGGER = logging.getLogger("lux_pipeline")


@pytest.fixture(autouse=True)
def _restore_logger():
    saved = (list(LOGGER.handlers), LOGGER.level)
    yield
    for h in list(LOGGER.handlers):
        LOGGER.removeHandler(h)
    for h in saved[0]:
        LOGGER.addHandler(h)
    LOGGER.setLevel(saved[1])


# --- the moved readers ---------------------------------------------------------


def test_the_readers_are_still_importable_where_they_were():
    from lux_pipeline.cli import _handler, _view, watch
    assert _view.LogTailer is LogTailer
    assert watch.count_workers is count_workers and watch.latest_prefix is latest_prefix
    assert _handler.RUNNING_WINDOW == progress.RUNNING_WINDOW == 3600
    assert _handler._WORKER_RE is progress.STATUS_RE


# --- read_build ------------------------------------------------------------------


def test_read_build_reads_every_worker_and_adds_up_the_visible_ones(tmp_path):
    prefix = write_fake_run(tmp_path, "reconcile", 3, done=(True, False, False), totals=(100, 50, 10))
    b = read_build(prefix, 3)
    assert b["seen"] == 3 and b["done"] is False
    assert (b["completed"], b["total"]) == (120, 160)
    assert b["labels"] == {0: "source0", 1: "source1", 2: "source2"}
    hidden = read_build(prefix, 3, visible={0, 1})
    assert (hidden["completed"], hidden["total"]) == (80, 150)


def test_read_build_waits_for_workers_without_files_and_totals(tmp_path):
    prefix = write_fake_run(tmp_path, "merge", 2, done=True, totals=(100, None))
    b = read_build(prefix, 3)
    assert b["workers"][2] is None and b["labels"][2] == "waiting"
    assert b["seen"] == 2 and b["done"] is False        # w2 never reported
    assert b["total"] is None                            # w1 has none yet
    assert read_build(prefix, 2)["done"] is True
    empty = read_build(str(tmp_path / "nothing"), 2)
    assert (empty["seen"], empty["done"], empty["completed"], empty["total"]) == (0, False, 0, None)


def test_read_build_labels_follow_the_console_view(tmp_path):
    prefix = str(tmp_path / "x")
    StatusWriter(status_path(prefix, 0), 0, throttle=0)(phase="references")
    StatusWriter(status_path(prefix, 1), 1, throttle=0)(advance=1)
    assert read_build(prefix, 2)["labels"] == {0: "references", 1: "running"}


# --- the record ------------------------------------------------------------------


def test_record_start_update_finish_round_trip(tmp_path):
    prefix = str(tmp_path / "reconcile-2026-11-02-141500")
    rec = RunRecord(prefix, "reconcile", ["lux", "reconcile", "--source", "ycba"], engine="mp",
                    workers=4, visible_workers=[0, 1, 2, 3], sources=["ycba"], parent="/p",
                    extra={"note": "x"}).start()
    got = read_run(prefix)
    assert got["pid"] == os.getpid() and got["host"] == socket.gethostname()
    assert got["started"] and got["finished"] is None and got["exit"] is None
    assert (got["command"], got["argv"][1], got["sources"], got["parent"], got["note"]) == \
        ("reconcile", "reconcile", ["ycba"], "/p", "x")
    rec.update(current="merge")
    assert read_run(prefix)["current"] == "merge"
    rec.finish(exit=1, error="ValueError: no", workers=2)
    got = read_run(prefix)
    assert (got["exit"], got["error"], got["workers"], got["visible_workers"]) == \
        (1, "ValueError: no", 2, [0, 1])
    assert got["finished"] >= got["started"]
    assert [p.name for p in tmp_path.iterdir()] == ["reconcile-2026-11-02-141500.run"]


def test_record_finish_keeps_the_planned_count_when_no_files_say_otherwise(tmp_path):
    rec = RunRecord(str(tmp_path / "x"), "export", ["lux"], workers=4).start()
    rec.finish(exit=0, workers=0)
    assert read_run(str(tmp_path / "x"))["workers"] == 4


def test_a_record_that_cannot_be_written_does_not_raise(tmp_path):
    RunRecord(str(tmp_path / "missing" / "x"), "merge", ["lux"]).start().finish(exit=0)
    assert read_run(str(tmp_path / "missing" / "x")) is None


def test_pid_alive():
    assert pid_alive(os.getpid())
    assert not pid_alive(dead_pid())
    assert not pid_alive(None) and not pid_alive(0) and not pid_alive("nonsense")


# --- run_state ---------------------------------------------------------------------


def state(tmp_path, **kw):
    prefix = write_fake_run(tmp_path, "merge", 2, **kw)
    return run_state(read_run(prefix), read_build(prefix, 2))


@pytest.mark.parametrize("exit, expected", [(0, "finished"), (1, "failed"), (130, "interrupted"),
                                            (-15, "interrupted"), (3, "failed")])
def test_state_from_the_records_exit(tmp_path, exit, expected):
    assert state(tmp_path, exit=exit, done=True) == expected


def test_state_while_the_driver_runs_and_after_it_was_killed(tmp_path):
    assert state(tmp_path / "a") == "running"
    # killed: not finished, its process gone, however recent the status files
    assert state(tmp_path / "b", pid=dead_pid()) == "killed"


def test_a_record_from_another_machine_falls_back_to_the_files(tmp_path):
    prefix = write_fake_run(tmp_path, "merge", 1, done=True, extra={"host": "elsewhere", "pid": 1})
    assert run_state(read_run(prefix), read_build(prefix, 1)) == "finished"


def test_state_without_a_record(tmp_path):
    assert state(tmp_path / "a", record=False, done=True) == "finished"
    assert state(tmp_path / "b", record=False) == "running"
    assert state(tmp_path / "c", record=False, updated=time.time() - 7200) == "unknown"


# --- list_runs ------------------------------------------------------------------------


def test_list_runs_finds_every_kind_of_run_newest_first(tmp_path):
    logs = tmp_path / "logs"
    t = time.time()
    running = write_fake_run(logs, "reconcile", 2, ts="2026-11-02-090000", started=t - 30)
    killed = write_fake_run(logs, "merge", 2, ts="2026-11-02-080000", started=t - 60, pid=dead_pid())
    shell = write_fake_run(logs, "export", 3, ts="2026-11-01-120000", record=False, done=True)
    build = str(logs / "build-2026-11-02-070000")
    RunRecord(build, "build", ["lux", "build"], extra={"phases": ["merge"], "steps": []}).start()
    # a build runs no workers itself: its record was written before the others
    rec = json.loads(Path(run_path(build)).read_text())
    rec["started"] = t - 90
    Path(run_path(build)).write_text(json.dumps(rec))
    runs = list_runs(str(logs))
    assert [r["prefix"] for r in runs] == [running, killed, build, shell]
    by = {r["prefix"]: r for r in runs}
    assert by[running]["state"] == "running" and by[killed]["state"] == "killed"
    assert by[shell]["state"] == "finished" and by[shell]["record"] is None
    assert by[shell]["command"] == "export"                 # from the prefix
    assert by[shell]["started"] == pytest.approx(
        time.mktime(time.strptime("2026-11-01-120000", "%Y-%m-%d-%H%M%S")))
    assert by[build]["workers"] == 0 and by[build]["state"] == "running"
    assert by[running]["progress"]["completed"] == 80


def test_list_runs_counts_the_workers_the_files_show(tmp_path):
    # the record planned 4; the manager ran 2 (download, export, index-load)
    prefix = write_fake_run(tmp_path, "download", 2, done=True, exit=0, visible=[0, 1, 2, 3])
    rec = json.loads(Path(run_path(prefix)).read_text())
    rec["workers"] = 4
    Path(run_path(prefix)).write_text(json.dumps(rec))
    (run,) = list_runs(str(tmp_path))
    assert run["workers"] == 2 and run["progress"]["done"] is True
    assert run["state"] == "finished"


def test_list_runs_on_nothing():
    assert list_runs(None) == [] and list_runs("/nonexistent/logs") == []


# --- the handler writes the record --------------------------------------------------


class QuietManager(TaskManager):
    """Each worker reports one unit of work and returns its number."""

    needs_configs = False

    def __init__(self, configs, max_workers=0, args=None, behaviour=None):
        super().__init__(configs, max_workers, args)
        self.behaviour = behaviour

    def _distributed(self, n, cfgs, progress):
        progress(phase="quiet", total=1, completed=1)
        if self.behaviour == "fail":
            raise ValueError("told to fail")
        if self.behaviour == "interrupt":
            raise KeyboardInterrupt()
        return n


class Cfgs:
    max_workers = 3
    internal = {"ycba": {"name": "ycba"}}
    external = {"aat": {"name": "aat"}}
    results = {}

    def __init__(self, log_dir):
        self.log_dir = str(log_dir)


def handler(cfgs, behaviour=None, after=None):
    class Handler(CH):
        def make_manager(self, wks, args):
            return QuietManager(self.configs, wks, args, behaviour=behaviour)

        def process(self, args, rest):
            manager = super().process(args, rest)
            if after:
                raise RuntimeError(after)
            return manager

    return Handler(cfgs)


def parse(argv):
    args, rest = entry.build_parser().parse_known_args(argv)
    args.cli_argv = list(argv)
    if args.my_worker > -1:
        args.engine = "null"
    return args, rest


def run_handler(tmp_path, argv, behaviour=None):
    h = handler(Cfgs(tmp_path / "logs"), behaviour)
    args, rest = parse(["quiet", "--engine", "null", "--no-ui", "--prefix",
                        str(tmp_path / "logs" / "quiet-1"), *argv])
    (tmp_path / "logs").mkdir(exist_ok=True)
    return h, args, rest


def test_a_worker_command_writes_and_finishes_its_record(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("LUX_PARENT_RUN", "/logs/build-1")
    h, args, rest = run_handler(tmp_path, ["--source", "ycba,aat", "--max-workers", "3"])
    h.process(args, rest)
    rec = read_run(str(tmp_path / "logs" / "quiet-1"))
    assert rec["command"] == "quiet" and rec["exit"] == 0 and rec["error"] is None
    assert rec["argv"][:2] == ["lux", "quiet"] and "--source" in rec["argv"]
    assert rec["sources"] == ["ycba", "aat"] and rec["parent"] == "/logs/build-1"
    assert rec["engine"] == "null" and rec["pid"] == os.getpid()
    # the null engine ran worker 0 only, and the files say so
    assert rec["workers"] == 1
    assert "status/log prefix:" in capsys.readouterr().out


def test_a_failed_worker_is_recorded(tmp_path):
    h, args, rest = run_handler(tmp_path, ["--source", "ycba"], behaviour="fail")
    with pytest.raises(Exception, match="told to fail"):
        h.process(args, rest)
    rec = read_run(str(tmp_path / "logs" / "quiet-1"))
    assert rec["exit"] == 1 and "told to fail" in rec["error"]


def test_an_interrupt_is_recorded(tmp_path):
    h, args, rest = run_handler(tmp_path, ["--source", "ycba"], behaviour="interrupt")
    with pytest.raises(KeyboardInterrupt):
        h.process(args, rest)
    rec = read_run(str(tmp_path / "logs" / "quiet-1"))
    assert (rec["exit"], rec["error"]) == (130, "interrupted")
    assert run_state(rec, read_build(rec["prefix"], 1)) == "interrupted"


def test_whoever_starts_a_command_can_add_to_its_record(tmp_path, monkeypatch):
    """LUX_RUN_EXTRA (the web UI's runner): its fields go into the record;
    a value that is not a JSON object is ignored, never an error."""
    monkeypatch.setenv("LUX_RUN_EXTRA", '{"web": true, "stdout": "/x/run.out"}')
    h, args, rest = run_handler(tmp_path, ["--source", "ycba"])
    h.process(args, rest)
    rec = read_run(args.prefix)
    assert rec["web"] is True and rec["stdout"] == "/x/run.out" and rec["exit"] == 0
    monkeypatch.setenv("LUX_RUN_EXTRA", "[1, 2")
    from lux_pipeline.cli._handler import run_extra
    assert run_extra() == {}


def test_one_slice_of_many_writes_no_record(tmp_path):
    h, args, rest = run_handler(tmp_path, ["--source", "ycba", "--my-worker", "1",
                                           "--max-workers", "3"])
    h.process(args, rest)
    assert not Path(run_path(str(tmp_path / "logs" / "quiet-1"))).exists()
    assert Path(status_path(str(tmp_path / "logs" / "quiet-1"), 1)).exists()


def test_idle_workers_are_recorded_as_hidden(tmp_path):
    class Idle(QuietManager):
        hide_idle_workers = True

    class Handler(CH):
        def make_manager(self, wks, args):
            return Idle(self.configs, wks, args)

    h = Handler(Cfgs(tmp_path / "logs"))
    args, rest = parse(["quiet", "--engine", "mp", "--no-ui", "--source", "ycba",
                        "--max-workers", "3", "--prefix", str(tmp_path / "logs" / "q")])
    # no sources yet: every worker is idle
    rec = h.start_record(args.prefix, args, Idle(h.configs, 3, args), 3)
    assert rec.data["visible_workers"] == []
    m = Idle(h.configs, 3, args)
    m.prepare_single("ycba")
    rec = h.start_record(args.prefix, args, m, 3)
    assert rec.data["workers"] == 3 and rec.data["visible_workers"] == [0]


# --- the entry point re-finishes a command that failed after its workers -----------


@pytest.fixture
def fake_command(tmp_path, monkeypatch):
    """`lux quiet-cmd`, a worker command whose process raises after its
    workers succeeded, as harvest and validate --strict do."""
    mod = types.ModuleType("lux_pipeline.cli.quiet_cmd")
    mod.CommandHandler = lambda cfgs: handler(cfgs, after="2 harvest(s) failed")
    monkeypatch.setitem(sys.modules, "lux_pipeline.cli.quiet_cmd", mod)
    (tmp_path / "logs").mkdir()
    monkeypatch.setattr(runtime, "get_configs", lambda *a, **kw: Cfgs(tmp_path / "logs"))
    return mod


def test_entry_records_how_the_command_ended(tmp_path, fake_command, capsys):
    prefix = str(tmp_path / "logs" / "q")
    code = entry.run(["quiet-cmd", "--engine", "null", "--no-ui", "--source", "ycba",
                      "--prefix", prefix])
    assert code == 1
    rec = read_run(prefix)
    assert rec["exit"] == 1 and "2 harvest(s) failed" in rec["error"]
    assert rec["argv"] == ["lux", "quiet-cmd", "--engine", "null", "--no-ui", "--source", "ycba",
                           "--prefix", prefix]
