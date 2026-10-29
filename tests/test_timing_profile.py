"""cProfile inside PhaseTimer, and timing-report.py --profile (item 41)."""

import importlib.util
import io
import json
import os
import pstats
import sys
from argparse import ArgumentParser
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest

from lux_pipeline.cli._handler import add_profile_arg, enable_profiling
from lux_pipeline.process import timing
from lux_pipeline.process.timing import PhaseTimer, profiling_requested

ROOT = Path(__file__).parent.parent


def busy(n=2000):
    return sum(i * i for i in range(n))


def run(out_dir, phase="reconcile", slice_n=0, **kw):
    t = PhaseTimer(phase, slice_n=slice_n, max_slice=2, out_dir=str(out_dir),
                   stream=io.StringIO(), **kw)
    for _ in range(5):
        with t.stage("busy"):
            busy()
        t.step()
    t.finish()
    return t


def functions(path):
    return {f[2] for f in pstats.Stats(str(path)).stats}


@pytest.fixture(autouse=True)
def no_env(monkeypatch):
    monkeypatch.delenv("LUX_PROFILE", raising=False)
    yield
    # a test that fails part way must not leave a profiler running
    if timing._profiling is not None:
        timing._profiling._stop_profile()


def test_a_profiled_phase_writes_its_stats_beside_the_timing(tmp_path):
    t = run(tmp_path, profile=True)
    prof = tmp_path / "profile-reconcile-0.prof"
    assert prof.exists() and (tmp_path / "timing-reconcile-0.json").exists()
    assert "busy" in functions(prof)
    assert json.loads((tmp_path / "timing-reconcile-0.json").read_text())["profile"] == prof.name
    # the phase ran to the end; nothing went to the stream about profiling
    assert t.complete and "profil" not in t.stream.getvalue()
    assert timing._profiling is None


def test_off_by_default(tmp_path):
    run(tmp_path)
    assert not list(tmp_path.glob("*.prof"))
    assert json.loads((tmp_path / "timing-reconcile-0.json").read_text())["profile"] is None


@pytest.mark.parametrize("value, on", [("1", True), ("yes", True), ("0", False),
                                       ("false", False), ("", False)])
def test_lux_profile_switches_it_on(tmp_path, monkeypatch, value, on):
    monkeypatch.setenv("LUX_PROFILE", value)
    assert profiling_requested() is on
    run(tmp_path)
    assert (tmp_path / "profile-reconcile-0.prof").exists() is on


def test_the_second_timer_in_a_process_gets_its_own_file(tmp_path):
    """Reconcile's references pass is a second timer after the first finished."""
    run(tmp_path, profile=True)
    run(tmp_path, phase="reconcile-refs", profile=True)
    assert sorted(p.name for p in tmp_path.glob("*.prof")) == [
        "profile-reconcile-0.prof", "profile-reconcile-refs-0.prof"]


def test_a_timer_that_never_finished_is_stopped_by_the_next(tmp_path):
    """Python allows one profiler at a time."""
    first = PhaseTimer("merge", slice_n=1, max_slice=2, out_dir=str(tmp_path),
                       stream=io.StringIO(), profile=True)
    busy()
    run(tmp_path, phase="merge-refs", slice_n=1, profile=True)
    assert (tmp_path / "profile-merge-1.prof").exists()
    assert (tmp_path / "profile-merge-refs-1.prof").exists()
    assert first._profiler is None


def test_an_unfinished_phase_writes_its_profile_on_exit(tmp_path):
    t = PhaseTimer("export", out_dir=str(tmp_path), stream=io.StringIO(), profile=True)
    busy()
    t._at_exit()
    assert (tmp_path / "profile-export-all.prof").exists()


def test_without_a_directory_it_says_so_and_carries_on(tmp_path):
    t = PhaseTimer("export", out_dir=None, stream=io.StringIO(), profile=True)
    t.finish()
    assert "not profiling export: no directory" in t.stream.getvalue()
    assert t.complete


def test_another_profiler_running_is_not_fatal(tmp_path):
    import cProfile
    other = cProfile.Profile()
    other.enable()
    try:
        t = run(tmp_path, profile=True)
    finally:
        other.disable()
    assert "not profiling reconcile[0/2]" in t.stream.getvalue()
    assert not list(tmp_path.glob("*.prof"))


# --- the command line ---------------------------------------------------------


def test_profile_flag_sets_the_environment(monkeypatch):
    ap = ArgumentParser()
    add_profile_arg(ap)
    enable_profiling(ap.parse_args([]))
    assert "LUX_PROFILE" not in os.environ
    enable_profiling(ap.parse_args(["--profile"]))
    assert os.environ["LUX_PROFILE"] == "1"


@pytest.mark.parametrize("command", ["reconcile", "merge", "export"])
def test_the_phase_commands_take_profile(command, monkeypatch):
    mod = importlib.import_module(f"lux_pipeline.cli.{command}")
    h = object.__new__(mod.CommandHandler)
    ap = ArgumentParser()
    h.add_args(ap)
    args = ap.parse_args(["--profile"])
    seen = {}
    monkeypatch.setattr(mod, {"reconcile": "ReconcileManager", "merge": "MergeManager",
                              "export": "ExportManager"}[command],
                        lambda *a: seen.setdefault("env", os.environ.get("LUX_PROFILE")))
    h.configs = None
    h.make_manager([], args)
    assert seen["env"] == "1"


# --- timing-report.py --profile -------------------------------------------------


def report_module():
    spec = importlib.util.spec_from_file_location("timing_report", ROOT / "timing-report.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_the_report_adds_up_the_slices(tmp_path, capsys, monkeypatch):
    run(tmp_path, slice_n=0, profile=True)
    run(tmp_path, slice_n=1, profile=True)
    run(tmp_path, phase="reconcile-refs", slice_n=0, profile=True)
    rep = report_module()
    profiles = rep.load_profiles(str(tmp_path))
    assert {k: len(v) for k, v in profiles.items()} == {"reconcile": 2, "reconcile-refs": 1}
    one = rep.combine(profiles["reconcile"][:1])
    both = rep.combine(profiles["reconcile"])
    busy_key = next(f for f in both.stats if f[2] == "busy")
    assert both.stats[busy_key][1] == 2 * one.stats[busy_key][1] == 10

    monkeypatch.setattr(sys, "argv", ["timing-report.py", "--dir", str(tmp_path),
                                      "--phase", "reconcile", "--profile", "5"])
    rep.main()
    out = capsys.readouterr().out
    assert "=== reconcile profile  (2 slices," in out
    assert "top 5 by cumulative" in out and "top 5 by internal" in out
    # the "busy" stage matched the function of that name, and its callees
    assert "\n  busy: busy (tests/test_timing_profile.py:" in out
    assert "<genexpr>" in out
    assert "reconcile-refs profile" not in out


def test_the_report_without_profiles_says_how_to_get_them(tmp_path, capsys, monkeypatch):
    run(tmp_path)
    rep = report_module()
    monkeypatch.setattr(sys, "argv", ["timing-report.py", "--dir", str(tmp_path), "--profile"])
    rep.main()
    assert "no profile-*.prof found" in capsys.readouterr().out
