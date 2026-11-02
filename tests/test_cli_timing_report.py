"""`lux timing-report`: the per-slice timing JSON added up per phase, the
side-by-side comparison, the profiles, log_dir from the configuration when
no --dir is given, the Rich view (the default) and the plain text
(``--no-ui``), which the root timing-report.py prints too.
Synthetic timing files written by hand plus real PhaseTimer output; no
database and no installed configuration."""

import io
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from lux_pipeline import runtime
from lux_pipeline.cli import _timing_view, entry
from lux_pipeline.cli.help import list_commands
from lux_pipeline.process import timing_report
from lux_pipeline.process.timing import PhaseTimer


def write_timing(d, phase, slice_n, records, seconds, cpu, stages, complete=True, marks=None):
    d.mkdir(parents=True, exist_ok=True)
    js = {"phase": phase, "slice": slice_n, "records": records, "seconds": seconds,
          "cpu_seconds": cpu, "complete": complete,
          "records_per_second": records / seconds,
          "stages": {k: {"calls": c, "seconds": s} for k, (c, s) in stages.items()}}
    if marks:
        js["marks"] = marks
    (d / f"timing-{phase}-{slice_n}.json").write_text(json.dumps(js))


def merge_run(d, slow_seconds=3600):
    """Three merge slices, the last one slow, and a nested stage."""
    for n, secs in enumerate((3600, 3600, slow_seconds)):
        write_timing(d, "merge", n, 36000, secs, secs / 2,
                     {"merge": (36000, secs / 2), "merge.fetch": (36000, secs / 4),
                      "write": (36000, secs / 4)})


def lux(*argv):
    """The plain-text report (``--no-ui``), as the root script prints it."""
    return entry.run(["--no-ui", "timing-report", *argv])


def lux_rich(*argv):
    """The default, Rich-rendered report."""
    return entry.run(["timing-report", *argv])


@pytest.fixture
def wide(monkeypatch):
    # Rich sizes a console that is not a terminal from COLUMNS
    monkeypatch.setenv("COLUMNS", "160")


# --- the report -------------------------------------------------------------


def test_summary_adds_the_slices_up(tmp_path):
    merge_run(tmp_path, slow_seconds=7200)
    s = timing_report.summarise(timing_report.load(str(tmp_path))["merge"])
    assert (s["slices"], s["complete"], s["records"]) == (3, 3, 108000)
    assert s["wall"] == 7200                        # the slowest slice
    assert s["worker_seconds"] == 14400
    assert s["cpu_percent"] == 50
    # merge.fetch is nested inside merge: listed, never subtracted twice
    assert set(s["top_stages"]) == {"merge", "write"}
    assert s["unattributed"] == pytest.approx(14400 * 0.25)


def test_lux_prints_every_phase(tmp_path, capsys):
    merge_run(tmp_path, slow_seconds=7200)
    write_timing(tmp_path, "reconcile", 0, 10, 10, 9, {}, complete=False,
                 marks=[["ycba", 120]])
    assert lux("--dir", str(tmp_path)) == 0
    out = capsys.readouterr().out
    assert out.startswith(f"# {tmp_path}\n")
    assert "\n=== merge  (3/3 finished)" in out
    assert "108,000 records in 120.0 min wall" in out
    assert "workers are off-cpu 50% of the time" in out
    assert "inside merge -- already counted above, not additional:" in out
    assert "slow slices (>20% under median): 2" in out
    assert "\n=== reconcile  (0/1 finished)" in out
    assert "source boundaries (slice 0): ycba at 2m" in out
    # phases in name order
    assert out.index("=== merge") < out.index("=== reconcile")


def test_phase_restricts_the_report(tmp_path, capsys):
    merge_run(tmp_path)
    write_timing(tmp_path, "reconcile", 0, 10, 10, 9, {})
    assert lux("--dir", str(tmp_path), "--phase", "reconcile") == 0
    out = capsys.readouterr().out
    assert "=== reconcile" in out and "=== merge" not in out


def test_no_timing_files_fails(tmp_path, capsys):
    assert lux("--dir", str(tmp_path)) == 1
    err = capsys.readouterr().err
    assert "lux timing-report:" in err and "no timing-*.json in" in err
    merge_run(tmp_path)
    assert lux("--dir", str(tmp_path), "--phase", "export") == 1
    assert "no timing-export-*.json" in capsys.readouterr().err


def test_missing_directory_fails(tmp_path, capsys):
    assert lux("--dir", str(tmp_path / "nope")) == 1
    assert "no such directory" in capsys.readouterr().err
    assert lux("--compare", str(tmp_path), str(tmp_path / "nope")) == 1
    assert "no such directory" in capsys.readouterr().err


def test_unreadable_file_is_skipped(tmp_path, capsys):
    merge_run(tmp_path)
    (tmp_path / "timing-broken-0.json").write_text("{not json")
    assert lux("--dir", str(tmp_path)) == 0
    out = capsys.readouterr().out
    assert "=== merge" in out and "=== broken" not in out
    assert "skipping timing-broken-0.json" in out    # logged as a warning


def test_compare(tmp_path, capsys):
    merge_run(tmp_path / "old", slow_seconds=7200)
    merge_run(tmp_path / "new")
    write_timing(tmp_path / "new", "export", 0, 10, 10, 9, {})
    assert lux("--compare", str(tmp_path / "old"), str(tmp_path / "new")) == 0
    out = capsys.readouterr().out
    assert f"=== export: only in {tmp_path / 'new'}" in out
    assert "=== merge\n" in out
    assert "wall minutes" in out and "-50.0%" in out


def test_log_dir_from_the_configuration(tmp_path, capsys, monkeypatch):
    merge_run(tmp_path)
    calls = []

    def get_configs(*a, **kw):
        calls.append(kw)
        return SimpleNamespace(log_dir=str(tmp_path))

    monkeypatch.setattr(runtime, "get_configs", get_configs)
    assert lux() == 0
    assert "=== merge" in capsys.readouterr().out
    # read, never instantiated: no idmap, no database
    assert calls == [{"instantiate": False}]


def test_dir_and_compare_never_load_the_configuration(tmp_path, capsys, monkeypatch):
    merge_run(tmp_path)

    def boom(*a, **kw):
        raise AssertionError("configuration loaded")

    monkeypatch.setattr(runtime, "get_configs", boom)
    assert lux("--dir", str(tmp_path)) == 0
    assert lux("--compare", str(tmp_path), str(tmp_path)) == 0


# --- profiles ---------------------------------------------------------------


def busy(n=2000):
    return sum(i * i for i in range(n))


def profiled_run(out_dir, slice_n):
    t = PhaseTimer("reconcile", slice_n=slice_n, max_slice=2, out_dir=str(out_dir),
                   stream=io.StringIO(), profile=True)
    for _ in range(5):
        with t.stage("busy"):
            busy()
        t.step()
    t.finish()


def test_profile(tmp_path, capsys):
    profiled_run(tmp_path, 0)
    profiled_run(tmp_path, 1)
    assert lux("--dir", str(tmp_path), "--profile", "5") == 0
    out = capsys.readouterr().out
    assert "=== reconcile profile  (2 slices," in out
    assert "top 5 by cumulative" in out and "top 5 by internal" in out
    assert "\n  busy: busy (tests/test_cli_timing_report.py:" in out


def test_profile_default_and_none_found(tmp_path, capsys):
    merge_run(tmp_path)
    assert lux("--dir", str(tmp_path), "--profile") == 0
    assert "no profile-*.prof found" in capsys.readouterr().out
    profiled_run(tmp_path, 0)
    assert lux("--dir", str(tmp_path), "--phase", "reconcile", "--profile") == 0
    assert f"top {timing_report.DEFAULT_TOP} by cumulative" in capsys.readouterr().out


# --- the command and the legacy script -------------------------------------


def test_listed_and_documented():
    assert "timing-report" in dict(list_commands())
    text, err = entry.command_help("timing-report")
    assert err is None
    for opt in ("--dir", "--phase", "--compare", "--profile"):
        assert opt in text


def test_legacy_script_prints_the_same_report(tmp_path, capsys):
    merge_run(tmp_path, slow_seconds=7200)
    assert lux("--dir", str(tmp_path)) == 0
    ours = capsys.readouterr().out
    res = subprocess.run([sys.executable, str(ROOT / "timing-report.py"), "--dir", str(tmp_path)],
                         capture_output=True, text=True, cwd=ROOT)
    assert res.returncode == 0, res.stderr
    assert res.stdout == ours


def test_legacy_script_exits_1_without_timing(tmp_path):
    res = subprocess.run([sys.executable, str(ROOT / "timing-report.py"), "--dir", str(tmp_path)],
                         capture_output=True, text=True, cwd=ROOT)
    assert res.returncode == 1
    assert "no timing-*.json" in res.stdout


# --- the Rich view ----------------------------------------------------------


def test_rich_report(tmp_path, capsys, wide):
    merge_run(tmp_path, slow_seconds=7200)
    write_timing(tmp_path, "reconcile", 0, 10, 10, 9, {}, complete=False,
                 marks=[["ycba", 120]])
    assert lux_rich("--dir", str(tmp_path)) == 0
    out = capsys.readouterr().out
    assert "timing report" in out and str(tmp_path)[:20] in out
    assert "╭─ merge " in out and "3/3 finished" in out
    assert "0/1 finished · running or stopped" in out
    assert "108,000" in out and "120.0 min" in out
    assert "workers are off-cpu 50% of the time" in out
    assert "(unattributed)" in out and "█" in out
    assert "inside merge  already counted above, not additional" in out
    assert "slow (>20% under median): 2" in out
    assert "ycba at 2m" in out
    assert out.index("─ merge ") < out.index("─ reconcile ")
    # not the plain text
    assert "=== merge" not in out


def test_rich_profile(tmp_path, capsys, wide):
    profiled_run(tmp_path, 0)
    profiled_run(tmp_path, 1)
    assert lux_rich("--dir", str(tmp_path), "--profile", "5") == 0
    out = capsys.readouterr().out
    assert "╭─ reconcile profile " in out and "2 slices ·" in out
    assert "Top 5 by cumulative" in out and "Top 5 by internal" in out
    assert "busy: busy  tests/test_cli_timing_report.py:" in out
    assert "<genexpr>" in out


def test_rich_no_profiles(tmp_path, capsys, wide):
    merge_run(tmp_path)
    assert lux_rich("--dir", str(tmp_path), "--profile") == 0
    assert "no profile-*.prof found" in capsys.readouterr().out


def test_rich_compare(tmp_path, capsys, wide):
    merge_run(tmp_path / "old", slow_seconds=7200)
    merge_run(tmp_path / "new")
    write_timing(tmp_path / "new", "export", 0, 10, 10, 9, {"pack": (1, 5)})
    write_timing(tmp_path / "old", "export", 0, 10, 10, 9, {})
    assert lux_rich("--compare", str(tmp_path / "old"), str(tmp_path / "new")) == 0
    out = capsys.readouterr().out
    assert "timing comparison" in out
    assert "╭─ merge " in out and "-50.0%" in out
    assert "new" in out.split("─ export ")[1]


def test_rich_errors_exit_1(tmp_path, capsys, monkeypatch):
    assert lux_rich("--dir", str(tmp_path)) == 1
    assert "no timing-*.json in" in capsys.readouterr().err
    assert lux_rich("--compare", str(tmp_path), str(tmp_path / "nope")) == 1
    assert "no such directory" in capsys.readouterr().err

    def boom(*a, **kw):
        raise AssertionError("configuration loaded")

    monkeypatch.setattr(runtime, "get_configs", boom)
    merge_run(tmp_path)
    assert lux_rich("--dir", str(tmp_path)) == 0


def test_change_colour_says_better_or_worse():
    assert _timing_view._change(-10, lower_better=True).style == "green"
    assert _timing_view._change(10, lower_better=True).style == "red"
    assert _timing_view._change(10, lower_better=False).style == "green"
    assert _timing_view._change(10, lower_better=None).style == "dim"
    assert _timing_view._change(1, lower_better=True).style == "dim"   # under 2%
    assert str(_timing_view._change(12.34)) == "+12.3%"


def test_bar_is_clamped():
    assert _timing_view._bar(150, "cyan").end == 100
    assert _timing_view._bar(-20, "cyan").end == 0


def test_the_view_shares_the_numbers_with_the_text(tmp_path):
    """Both renderers read the same helpers: the straggler and overrun
    figures are computed once, in lux_pipeline.process.timing_report."""
    merge_run(tmp_path, slow_seconds=7200)
    s = timing_report.summarise(timing_report.load(str(tmp_path))["merge"])
    sr = timing_report.slice_rates(s)
    assert sr["slow"] == [2] and sr["fast"] == [] and not sr["even"]
    assert timing_report.slice_rates({**s, "slices": 2}) is None
    assert timing_report.nested_overrun(s, "merge") == 0
