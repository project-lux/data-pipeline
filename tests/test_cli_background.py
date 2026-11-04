"""`lux --background`: the command started as a process of its own, the
starter returning at once (or with the child's status when it fails at
once), and the run's files what `lux watch` and the web UI read."""

import os
import signal
import sys
import time
from argparse import Namespace
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).parent))

from lux_pipeline.cli import _background, entry  # noqa: E402
from lux_pipeline.cli.introspect import HIDDEN  # noqa: E402
from lux_pipeline.progress import pid_alive, read_build, read_run, run_state  # noqa: E402
from webui_fixtures import use_installation  # noqa: E402

SLEEPER = '''
import logging, os, subprocess, sys, time
from lux_pipeline.cli._handler import BaseHandler
logger = logging.getLogger("lux_pipeline")

class CommandHandler(BaseHandler):
    needs_configs = False

    def add_args(self, ap):
        ap.add_argument("--until", help="run until this file exists")
        ap.add_argument("--pidfile", help="start a worker and write its pid here")

    def process(self, args, rest):
        super().process(args, rest)
        logger.info("sleeper started")
        if args.pidfile:
            kid = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"])
            with open(args.pidfile + ".tmp", "w") as fh:
                fh.write(str(kid.pid))
            os.replace(args.pidfile + ".tmp", args.pidfile)
        print("sleeper says hello", flush=True)
        while args.until and not os.path.exists(args.until):
            time.sleep(0.05)
        logger.info("sleeper done")
'''


def wait_until(cond, timeout=30.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if cond():
            return True
        time.sleep(0.05)
    return False


@pytest.fixture
def install(monkeypatch, tmp_path):
    root = use_installation(monkeypatch, tmp_path / "inst")
    pkg = tmp_path / "ext" / "bgtests"
    pkg.mkdir(parents=True)
    (pkg / "sleeper.py").write_text(SLEEPER)
    # the starter resolves the command; the child imports it again
    monkeypatch.syspath_prepend(str(tmp_path / "ext"))
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join([str(tmp_path / "ext"), str(ROOT)]))
    monkeypatch.delenv(_background.CHILD_ENV, raising=False)
    monkeypatch.delenv("LUX_PARENT_RUN", raising=False)
    monkeypatch.delenv("LUX_RUN_EXTRA", raising=False)
    return root


def runs(root):
    return sorted(p[:-4] for p in os.listdir(root / "data" / "logs") if p.endswith(".run"))


# --- the command line ---------------------------------------------------------


@pytest.mark.parametrize("token,flag", [("-b", True), ("--background", True), ("--backg", True),
                                        ("--b", True), ("--baseline", False), ("-bh", False),
                                        ("--", False), ("build", False)])
def test_what_counts_as_the_flag(token, flag):
    assert _background.is_flag(token) is flag


def test_the_child_gets_no_flag_and_its_own_prefix_and_no_view():
    args = Namespace(no_ui=False, prefix=None)
    assert _background.child_args(["build", "-b", "--from", "merge"], args, "/l/build-1") == \
        ["build", "--from", "merge", "--no-ui", "--prefix", "/l/build-1"]
    args = Namespace(no_ui=True, prefix="/l/mine")
    assert _background.child_args(["--background", "merge", "--no-ui", "--prefix", "/l/mine"],
                                  args, "/l/mine") == ["merge", "--no-ui", "--prefix", "/l/mine"]


def test_the_flag_is_global_and_not_on_a_form():
    ns = entry.build_parser().parse_known_args(["reconcile", "-b", "--baseline"])
    assert ns[0].background is True and ns[1] == ["--baseline"]
    assert "background" in HIDDEN


def test_commands_that_are_not_runs_refuse_it(install, capsys):
    assert entry.run(["watch", "--background"]) == 2
    assert "--background does not apply" in capsys.readouterr().err
    assert runs(install) == []


# --- starting -----------------------------------------------------------------


def test_a_quick_command_finishes_and_its_output_is_kept(install, capsys):
    assert entry.run(["list", "-b"]) == 0
    out = capsys.readouterr().out
    [prefix] = runs(install)
    prefix = str(install / "data" / "logs" / prefix)
    assert wait_until(lambda: (read_run(prefix) or {}).get("exit") is not None)
    rec = read_run(prefix)
    assert rec["exit"] == 0 and rec["background"] is True and "provisional" not in rec
    assert rec["stdout"] == f"{prefix}.out" and os.path.getsize(f"{prefix}.out") > 0
    assert prefix in out


def test_a_mistake_is_reported_with_the_childs_status(install, capsys):
    assert entry.run(["counts", "--nonsense", "--background"]) == 2
    err = capsys.readouterr().err
    assert "exit status 2" in err and "unrecognized arguments: --nonsense" in err
    [prefix] = runs(install)
    assert read_run(str(install / "data" / "logs" / prefix))["exit"] == 2


def test_a_long_run_carries_on_and_is_followed_like_any_other(install, tmp_path, capsys):
    release = tmp_path / "release"
    assert entry.run(["bgtests.sleeper", "--until", str(release), "-b"]) == 0
    out = capsys.readouterr().out
    assert "running in the background" in out
    [prefix] = runs(install)
    prefix = str(install / "data" / "logs" / prefix)
    assert f"lux watch {prefix}" in out
    rec = read_run(prefix)
    assert rec["exit"] is None and not rec.get("provisional") and pid_alive(rec["pid"])
    assert run_state(rec, read_build(prefix, 0)) == "running"
    assert wait_until(lambda: "sleeper started" in Path(f"{prefix}.log").read_text()
                      if os.path.exists(f"{prefix}.log") else False)

    release.touch()
    assert wait_until(lambda: (read_run(prefix) or {}).get("exit") is not None)
    assert read_run(prefix)["exit"] == 0
    os.waitpid(rec["pid"], 0)       # our child here; the shell's would be init's
    assert "sleeper says hello" in Path(f"{prefix}.out").read_text()
    assert "sleeper done" in Path(f"{prefix}.log").read_text()


def test_stopping_the_process_group_stops_its_workers(install, tmp_path, capsys):
    pidfile = tmp_path / "worker.pid"
    assert entry.run(["bgtests.sleeper", "--until", str(tmp_path / "never"),
                      "--pidfile", str(pidfile), "-b"]) == 0
    stop = [l for l in capsys.readouterr().out.splitlines() if "kill -- -" in l]
    pid = int(stop[0].rsplit("-", 1)[1])
    assert wait_until(pidfile.exists)
    worker = int(pidfile.read_text())
    assert os.getpgid(worker) == pid       # in the run's own group, not ours
    assert os.getpgid(pid) != os.getpgid(0)

    os.killpg(pid, signal.SIGTERM)
    os.waitpid(pid, 0)
    assert wait_until(lambda: not pid_alive(worker))
    [prefix] = runs(install)
    prefix = str(install / "data" / "logs" / prefix)
    assert run_state(read_run(prefix), read_build(prefix, 0)) == "killed"


def test_a_slice_writes_no_record_of_its_own(install, capsys):
    prefix = str(install / "data" / "logs" / "slices")
    assert entry.run(["bgtests.sleeper", "--max-workers", "2", "--my-worker", "1",
                      "--prefix", prefix, "-b"]) == 0
    assert wait_until(lambda: "sleeper done" in Path(f"{prefix}-w1.out").read_text()
                      if os.path.exists(f"{prefix}-w1.out") else False)
    rec = read_run(prefix)
    # the one-shot command's own record (entry.start_record), not a provisional one
    assert rec is None or not rec.get("provisional")


def test_the_background_process_never_starts_itself_again(install, monkeypatch, capsys):
    monkeypatch.setenv(_background.CHILD_ENV, "1")
    prefix = str(install / "data" / "logs" / "inproc")
    assert entry.run(["bgtests.sleeper", "--prefix", prefix, "-b"]) == 0
    assert read_run(prefix)["exit"] == 0 and not os.path.exists(f"{prefix}.out")
