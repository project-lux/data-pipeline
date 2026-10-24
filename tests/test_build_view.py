"""BuildView renders a build from its status and log files, and lux watch
attaches to one."""

import io
import logging
import os
import sys
import time
from pathlib import Path

import pytest
from rich.console import Console

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline import runtime
from lux_pipeline.cli import entry
from lux_pipeline.cli._view import BuildView, LogPanel, LogTailer, clamp_level
from lux_pipeline.cli.watch import count_workers, latest_prefix
from lux_pipeline.progress import StatusWriter, log_path, status_path

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


def console(height=50):
    return Console(file=io.StringIO(), width=100, height=height, force_terminal=True)


def make_build(tmp_path, done=(False, False)):
    """Two workers with status and log files under tmp_path."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    prefix = str(tmp_path / "reconcile-2026-10-24-101500")
    w0 = StatusWriter(status_path(prefix, 0), 0, throttle=0)
    w0(phase="reconcile", description="ycba", total=100, completed=40, done=done[0])
    w1 = StatusWriter(status_path(prefix, 1), 1, throttle=0)
    w1(phase="references", total=50, completed=50, done=done[1])
    Path(log_path(prefix, 0)).write_text("10:15:01 INFO     starting ycba\n10:15:02 INFO     40 done\n")
    Path(log_path(prefix, 1)).write_text("10:15:01 INFO     references\n")
    return prefix


# --- refresh from files ------------------------------------------------------


def test_refresh_reads_status_into_one_task_per_worker(tmp_path):
    prefix = make_build(tmp_path)
    view = BuildView(prefix, 2, console=console(), auto_refresh=False)
    assert view.refresh() is False
    t0, t1 = (view._task(view.tasks[n]) for n in (0, 1))
    assert (t0.completed, t0.total) == (40, 100)
    assert t0.description == "w0 ycba"
    assert (t1.completed, t1.total) == (50, 50)
    assert t1.description == "w1 references"       # phase when no description
    assert t0.started and t1.started
    all_task = view._task(view.all_task)
    assert (all_task.completed, all_task.total) == (90, 150)


def test_worker_lines_land_in_the_panel_prefixed(tmp_path):
    prefix = make_build(tmp_path)
    view = BuildView(prefix, 2, console=console(), auto_refresh=False)
    view.refresh()
    lines = view.log_panel.snapshot()
    assert "w0 10:15:01 INFO     starting ycba" in lines
    assert "w1 10:15:01 INFO     references" in lines
    # only new lines next time
    view.refresh()
    assert len(view.log_panel.snapshot()) == 3
    with open(log_path(prefix, 1), "a") as fh:
        fh.write("10:15:09 INFO     more\npartial line without newline")
    view.refresh()
    lines = view.log_panel.snapshot()
    assert lines[-1] == "w1 10:15:09 INFO     more"


def test_refresh_is_true_only_when_every_worker_is_done(tmp_path):
    assert BuildView(make_build(tmp_path, (True, False)), 2, console=console(),
                     auto_refresh=False).refresh() is False
    assert BuildView(make_build(tmp_path / "b", (True, True)), 2, console=console(),
                     auto_refresh=False).refresh() is True


def test_missing_status_shows_waiting_and_is_not_done(tmp_path):
    prefix = str(tmp_path / "x")
    view = BuildView(prefix, 2, console=console(), auto_refresh=False)
    assert view.refresh() is False
    assert view._task(view.tasks[0]).description == "w0 waiting"
    assert view._task(view.tasks[0]).total is None
    assert not view._task(view.tasks[0]).started


def test_torn_status_file_is_tolerated(tmp_path):
    prefix = make_build(tmp_path)
    Path(status_path(prefix, 1)).write_text('{"wor')
    view = BuildView(prefix, 2, console=console(), auto_refresh=False)
    assert view.refresh() is False
    assert view._task(view.tasks[0]).completed == 40


def test_all_workers_total_is_unknown_until_every_total_is(tmp_path):
    prefix = str(tmp_path / "x")
    StatusWriter(status_path(prefix, 0), 0, throttle=0)(completed=5, total=10)
    StatusWriter(status_path(prefix, 1), 1, throttle=0)(completed=3)
    view = BuildView(prefix, 2, console=console(), auto_refresh=False)
    view.refresh()
    all_task = view._task(view.all_task)
    assert (all_task.completed, all_task.total) == (8, None)


def test_set_visible_hides_a_worker_and_its_share_of_the_total(tmp_path):
    prefix = make_build(tmp_path)
    view = BuildView(prefix, 2, console=console(), auto_refresh=False)
    view.set_visible(1, False)
    view.refresh()
    assert view._task(view.tasks[1]).visible is False
    assert view._task(view.all_task).total == 100


# --- the live context --------------------------------------------------------


def test_context_routes_parent_logs_to_the_panel_and_restores_handlers(tmp_path):
    prefix = make_build(tmp_path)
    stream = runtime.configure_logging("INFO", stream=io.StringIO())
    view = BuildView(prefix, 2, log_level="INFO", console=console(), auto_refresh=False)
    with view:
        assert LOGGER.handlers == [view.handler]
        LOGGER.info("hello from the parent")
        LOGGER.debug("too quiet")
    lines = view.log_panel.snapshot()
    assert any(l.endswith("INFO     hello from the parent") for l in lines)
    assert not any("too quiet" in l for l in lines)
    assert LOGGER.handlers == [stream]
    assert view.live is None


def test_exit_refreshes_the_final_state(tmp_path):
    prefix = make_build(tmp_path, (True, True))
    view = BuildView(prefix, 2, console=console(), auto_refresh=False)
    with view:
        pass
    assert view.done is True


def test_auto_refresh_polls_the_files(tmp_path):
    prefix = make_build(tmp_path, (True, True))
    view = BuildView(prefix, 2, console=console(), interval=0.02)
    with view:
        deadline = time.time() + 2
        while not view.done and time.time() < deadline:
            time.sleep(0.02)
    assert view.done is True


def test_small_terminal_prints_a_warning_once(tmp_path):
    prefix = make_build(tmp_path)
    c = console(height=6)
    with BuildView(prefix, 2, console=c, auto_refresh=False):
        pass
    assert "use --no-ui" in c.file.getvalue()


def test_level_is_clamped_to_a_valid_name():
    assert clamp_level("debug") == "DEBUG"
    assert clamp_level("LOUD") == "INFO"
    assert clamp_level(None) == "INFO"


def test_log_panel_is_bounded():
    p = LogPanel(max_lines=3)
    for i in range(10):
        p.add_line(f"line {i}")
    assert p.snapshot() == ["line 7", "line 8", "line 9"]


def test_tailer_replaces_undecodable_bytes(tmp_path):
    prefix = str(tmp_path / "x")
    Path(log_path(prefix, 0)).write_bytes(b"ok \xff bad\n")
    assert LogTailer(prefix, 1).read_new_lines() == [(0, "ok � bad")]


# --- lux watch ---------------------------------------------------------------


def test_count_workers_and_latest_prefix(tmp_path):
    a = make_build(tmp_path / "logs")
    assert count_workers(a) == 2
    assert count_workers(str(tmp_path / "logs" / "nothing")) == 0
    time.sleep(0.01)
    b = str(tmp_path / "logs" / "merge-2026-10-24-120000")
    StatusWriter(status_path(b, 0), 0, throttle=0)(done=True)
    assert latest_prefix(str(tmp_path / "logs")) == b
    assert latest_prefix(str(tmp_path / "empty")) is None


def run(argv, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["lux", *argv])
    with pytest.raises(SystemExit) as ex:
        entry.main()
    return ex.value.code


def test_watch_renders_a_finished_build_without_a_config(tmp_path, monkeypatch, capsys):
    def boom(*a, **kw):
        raise AssertionError("watch must not load the configuration")
    monkeypatch.setattr(runtime, "get_configs", boom)
    prefix = make_build(tmp_path, (True, True))
    assert run(["watch", prefix, "--interval", "0.01"], monkeypatch) == 0
    out = capsys.readouterr().out
    assert f"{prefix}: 2 worker(s), finished" in out
    assert "w0 ycba" in out


def test_watch_latest_uses_log_dir_from_a_cheap_config_load(tmp_path, monkeypatch, capsys):
    prefix = make_build(tmp_path, (True, True))

    class Cfgs:
        log_dir = str(tmp_path)
    monkeypatch.setattr(runtime, "get_configs", lambda instantiate=True: Cfgs())
    assert run(["watch", "--latest", "--interval", "0.01"], monkeypatch) == 0
    assert f"{prefix}: 2 worker(s), finished" in capsys.readouterr().out


def test_watch_without_files_fails_clearly(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(runtime, "get_configs", lambda *a, **kw: None)
    assert run(["watch", str(tmp_path / "nope")], monkeypatch) == 1
    assert "no status files found" in capsys.readouterr().err


def test_watch_needs_a_prefix_or_latest(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(runtime, "get_configs", lambda *a, **kw: None)
    assert run(["watch"], monkeypatch) == 1
    assert "--latest" in capsys.readouterr().err
