"""The web UI's runner (W10): starting a command as its own process with
its record from the first moment, finishing the record when the process
ends without having done so, cancelling a whole process group, picking
runs up again after a restart, the build guard, and `lux build --dry-run`.

The commands are a fake `lux` (`FAKE`, below; its behaviour from
``FAKE_LUX`` in the environment the runner passes on) except where a test
says it runs the real one."""

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

pytest.importorskip("fastapi")

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

from webui_fixtures import dead_pid, use_installation, write_fake_run  # noqa: E402

from lux_pipeline.progress import describe_run, pid_alive, read_run  # noqa: E402
from lux_pipeline.web.runner import CANCEL_ERROR, Runner, finish_file, last_line  # noqa: E402
from lux_pipeline.web.settings import Settings  # noqa: E402
from lux_pipeline.web.state import AppState  # noqa: E402

FAKE = r'''
import json, os, subprocess, sys, time
mode = os.environ.get("FAKE_LUX", "ok")
argv = sys.argv[1:]
prefix = argv[argv.index("--prefix") + 1]
print("fake lux", " ".join(argv), flush=True)
if mode == "ok":
    print("all done", flush=True)
elif mode == "exit3":
    print("lux list: ValueError: something broke", flush=True)
    sys.exit(3)
elif mode == "driver":
    # a worker command: its driver replaces the runner's record with its own
    from lux_pipeline.cli._handler import run_extra
    from lux_pipeline.progress import RunRecord
    rec = RunRecord(prefix, argv[0], ["lux", *argv], engine="mp", workers=2, extra=run_extra()).start()
    time.sleep(0.3)
    rec.finish(exit=0)
elif mode == "sleep":
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    with open(prefix + ".child", "w") as fh:
        fh.write(str(child.pid))
    time.sleep(60)
'''


def wait_for(cond, timeout=15.0, step=0.05):
    end = time.time() + timeout
    while time.time() < end:
        v = cond()
        if v:
            return v
        time.sleep(step)
    raise AssertionError("timed out")


@pytest.fixture
def runner(tmp_path, monkeypatch):
    root = use_installation(monkeypatch, tmp_path / "inst")
    state = AppState(Settings())
    r = Runner(state)
    fake = tmp_path / "fake_lux.py"
    fake.write_text(FAKE)
    r.base_argv = lambda: [sys.executable, str(fake)]
    r.resets = []
    monkeypatch.setattr(state, "reset", lambda: r.resets.append(1))
    os.makedirs(state.configs().log_dir, exist_ok=True)
    yield r
    for h in list(r.handles.values()):
        if h.proc is not None and h.proc.poll() is None:
            os.killpg(os.getpgid(h.pid), signal.SIGKILL)


def finished(prefix):
    return lambda: (read_run(prefix) or {}).get("exit") is not None


# --- command lines -----------------------------------------------------------


def test_argv_adds_no_ui_and_the_prefix_once(runner):
    argv = runner.argv_for("reconcile", ["--source", "ycba", "--no-ui"], "/l/p")
    assert argv[-6:] == ["reconcile", "--source", "ycba", "--no-ui", "--prefix", "/l/p"]
    assert argv.count("--no-ui") == 1
    plain = Runner(runner.state).argv_for("merge", [], "/l/m")
    assert plain == [sys.executable, "-m", "lux_pipeline.cli.entry", "merge", "--no-ui", "--prefix", "/l/m"]


@pytest.mark.parametrize("bad", ["--my-worker", "--my_worker=2", "--debug", "--prefix", "--env"])
def test_what_the_runner_sets_cannot_come_from_a_form(runner, bad):
    with pytest.raises(ValueError, match="set by the runner"):
        runner.argv_for("reconcile", [bad, "1"], "/l/p")


def test_a_second_run_in_the_same_second_gets_its_own_prefix(runner, monkeypatch):
    from lux_pipeline.cli import _handler as mod
    monkeypatch.setattr(mod, "default_prefix", lambda cfgs, cmd: os.path.join(cfgs.log_dir, f"{cmd}-T"))
    cfgs = runner.state.configs()
    first = runner.new_prefix(cfgs, "list")
    Path(first).mkdir()                       # a run's directory is there
    assert runner.new_prefix(cfgs, "list") == f"{first}-2"


# --- starting and reaping ----------------------------------------------------


def test_a_run_has_a_record_and_output_from_the_start(runner, monkeypatch):
    monkeypatch.setenv("FAKE_LUX", "ok")
    h = runner.start("list", ["--maps"])
    rec = read_run(h.prefix)
    assert rec["web"] is True and rec["stdout"] == os.path.join(h.prefix, "stdout.log")
    assert rec["argv"] == ["lux", "list", "--maps"] and rec["command"] == "list"
    assert rec["pid"] == h.pid != os.getpid()
    assert os.path.dirname(h.prefix) == runner.state.configs().log_dir
    wait_for(finished(h.prefix))
    rec = read_run(h.prefix)
    assert rec["exit"] == 0 and rec["error"] is None
    out = Path(h.prefix, "stdout.log").read_text()
    assert "--maps --no-ui --prefix " + h.prefix in out and "all done" in out
    wait_for(lambda: h.prefix not in runner.handles)
    assert runner.resets


def test_a_failure_is_recorded_with_its_last_line(runner, monkeypatch):
    monkeypatch.setenv("FAKE_LUX", "exit3")
    h = runner.start("list", [])
    wait_for(finished(h.prefix))
    rec = read_run(h.prefix)
    assert rec["exit"] == 3 and rec["error"] == "lux list: ValueError: something broke"
    assert describe_run(h.prefix)["state"] == "failed"


def test_a_driver_keeps_the_web_fields_in_its_own_record(runner, monkeypatch):
    monkeypatch.setenv("FAKE_LUX", "driver")
    h = runner.start("list", [])
    wait_for(finished(h.prefix))
    rec = read_run(h.prefix)
    # the driver's record (engine, workers), with the runner's fields from LUX_RUN_EXTRA
    assert rec["engine"] == "mp" and rec["workers"] == 2 and rec["exit"] == 0
    assert rec["web"] is True and rec["stdout"] == os.path.join(h.prefix, "stdout.log")
    wait_for(lambda: h.prefix not in runner.handles)
    assert read_run(h.prefix)["exit"] == 0           # the reaper did not finish it again


def test_finish_file_leaves_a_finished_record_alone(tmp_path):
    prefix = write_fake_run(tmp_path, "merge", 1, exit=0)
    before = read_run(prefix)
    assert finish_file(prefix, 1, "late") is False
    assert read_run(prefix) == before
    Path(f"{prefix}.out").write_text("one\n\ntwo\n  \n")
    assert last_line(f"{prefix}.out") == "two" and last_line(str(tmp_path / "nope")) is None


# --- the guard ---------------------------------------------------------------


def test_a_phase_is_refused_while_a_build_runs(runner, monkeypatch):
    log_dir = runner.state.configs().log_dir
    prefix = write_fake_run(log_dir, "merge", 2, updated=time.time())
    with pytest.raises(RuntimeError, match="a build is running") as e:
        runner.start("reconcile", [])
    assert os.path.basename(prefix) in str(e.value)
    # identify has --force; a phase does not, so forcing it changes nothing
    monkeypatch.setenv("FAKE_LUX", "ok")
    h = runner.start("identify", ["--force"], force=True)
    assert h.prefix
    with pytest.raises(RuntimeError):
        runner.start("reconcile", [], force=True)
    # a run that rewrites nothing is not held up
    assert runner.start("pg-maintain", ["--bloat"], guarded=False).prefix


def test_two_phases_from_here_never_run_at_once(runner, monkeypatch):
    monkeypatch.setenv("FAKE_LUX", "sleep")
    h = runner.start("merge", [])
    with pytest.raises(RuntimeError, match="still running"):
        runner.start("export", [])
    runner.cancel(h.prefix, grace=0.5)
    wait_for(finished(h.prefix))


def test_a_command_the_web_does_not_run(runner):
    with pytest.raises(ValueError, match="not run from the web"):
        runner.start("serve", [])


# --- cancelling --------------------------------------------------------------


def test_cancel_ends_the_whole_process_group(runner, monkeypatch):
    monkeypatch.setenv("FAKE_LUX", "sleep")
    h = runner.start("list", [])
    child = int(wait_for(lambda: Path(f"{h.prefix}.child").exists()
                         and Path(f"{h.prefix}.child").read_text()))
    assert runner.cancellable(read_run(h.prefix)) is None
    assert runner.cancel(h.prefix, grace=0.5) is True
    wait_for(finished(h.prefix))
    rec = read_run(h.prefix)
    assert rec["error"] == CANCEL_ERROR and rec["exit"] < 0
    assert describe_run(h.prefix)["state"] == "interrupted"
    wait_for(lambda: not pid_alive(child))            # the worker went too


def test_only_a_running_web_run_can_be_cancelled(runner, tmp_path):
    log_dir = runner.state.configs().log_dir
    shell = write_fake_run(log_dir, "merge", 1, ts="2026-11-02-100000")
    assert "started elsewhere" in runner.cancellable(read_run(shell))
    with pytest.raises(RuntimeError, match="started elsewhere"):
        runner.cancel(shell)
    build = write_fake_run(log_dir, "build", 0, ts="2026-11-02-110000", extra={"web": True})
    phase = write_fake_run(log_dir, "merge", 1, ts="2026-11-02-110001", extra={"parent": build})
    assert "cancel the build" in runner.cancellable(read_run(phase))
    done = write_fake_run(log_dir, "export", 1, ts="2026-11-02-120000", exit=0, extra={"web": True})
    assert runner.cancellable(read_run(done)) == "it is not running"
    gone = write_fake_run(log_dir, "export", 1, ts="2026-11-02-130000", pid=dead_pid(),
                          extra={"web": True})
    assert runner.cancellable(read_run(gone)) == "it is not running"
    assert runner.cancellable(None) == "it has no record"


# --- after a restart ---------------------------------------------------------


def test_adopt_watches_web_runs_still_running(runner):
    log_dir = runner.state.configs().log_dir
    sleeper = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        mine = write_fake_run(log_dir, "merge", 1, ts="2026-11-02-090000", pid=sleeper.pid,
                              updated=time.time(), extra={"web": True})
        write_fake_run(log_dir, "export", 1, ts="2026-11-02-090100", updated=time.time())  # a shell's
        assert runner.adopt() == 1 and list(runner.handles) == [mine]
        assert runner.handles[mine].proc is None
        sleeper.kill()
        sleeper.wait()
        wait_for(lambda: mine not in runner.handles)
    finally:
        if sleeper.poll() is None:
            sleeper.kill()
    # not this server's child: no exit code to record; the record says the run was killed
    assert read_run(mine)["exit"] is None


def test_adopted_and_cancelled_is_finished_as_cancelled(runner):
    log_dir = runner.state.configs().log_dir
    sleeper = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"],
                               start_new_session=True)
    try:
        prefix = write_fake_run(log_dir, "merge", 1, ts="2026-11-02-080000", pid=sleeper.pid,
                                updated=time.time(), extra={"web": True})
        runner.cancel(prefix, grace=0.5)
        sleeper.wait(timeout=10)
        wait_for(finished(prefix))
        rec = read_run(prefix)
        assert rec["exit"] == -signal.SIGTERM and rec["error"] == CANCEL_ERROR
    finally:
        if sleeper.poll() is None:
            sleeper.kill()


# --- the real lux ------------------------------------------------------------


def test_the_real_lux_list_runs_and_is_finished(runner):
    real = Runner(runner.state)
    h = real.start("list", ["--maps"])
    wait_for(finished(h.prefix), timeout=60)
    rec = read_run(h.prefix)
    out = Path(h.prefix, "stdout.log").read_text()
    assert rec["exit"] == 0, out
    assert "Maps" in out and "idmap" in out


def test_the_real_dry_run_lists_the_build_steps(runner):
    real = Runner(runner.state)
    steps = real.dry_run(["--skip", "sheets,portal", "--source", "ycba"])
    assert steps and all(s.startswith("[") for s in steps)
    text = "\n".join(steps)
    assert "lux reconcile" in text and "lux merge" in text and "lux sheets" not in text
    assert sys.executable not in text
    with pytest.raises(RuntimeError):
        real.dry_run(["--from", "nowhere"])
