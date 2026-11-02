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


def write_timing(d, phase, slice_n, records, seconds, cpu, stages, complete=True, marks=None,
                 started_at=None, max_slice=None):
    d.mkdir(parents=True, exist_ok=True)
    js = {"phase": phase, "slice": slice_n, "records": records, "seconds": seconds,
          "cpu_seconds": cpu, "complete": complete,
          "records_per_second": records / seconds,
          "stages": {k: {"calls": c, "seconds": s} for k, (c, s) in stages.items()}}
    if marks:
        js["marks"] = marks
    if max_slice is not None:
        js["max_slice"] = max_slice
    if started_at is not None:
        js["started_at"] = started_at
        js["updated_at"] = started_at + seconds
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
    # phases in build order: reconcile before merge
    assert out.index("=== reconcile") < out.index("=== merge")


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
    assert out.index("─ reconcile ") < out.index("─ merge ")
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


# --- build order and the summary of every phase ----------------------------

T0 = 1793520000.0   # 2026-11-01 morning, UTC


def build(d, gap=60):
    """A small build: reconcile (one slow slice), identify, merge (idmap
    bound), export, `gap` seconds apart."""
    t = T0
    for n, secs in enumerate((5400, 5500, 7900)):
        write_timing(d, "reconcile", n, 1000, secs, secs / 2,
                     {"acquire": (1000, secs * 0.5), "acquire.map": (1000, secs * 0.2)},
                     started_at=t)
    t += 7900 + gap
    write_timing(d, "identify", "all", 1000, 2700, 2400, {"resolve": (1, 2000)}, started_at=t)
    t += 2700 + gap
    for n in range(3):
        write_timing(d, "merge", n, 1000, 10800, 1080,
                     {"fetch_idmap": (1000, 5400), "merge": (1000, 3240)}, started_at=t)
    t += 10800 + gap
    write_timing(d, "export", 0, 1000, 2400, 1700, {"write": (1000, 1200)}, started_at=t)
    return t + 2400


def test_phase_order_is_the_builds():
    names = ["export", "zeta", "merge-refs", "identify", "reconcile-refs", "alpha",
             "reconcile", "merge", "sheets", "export-nt"]
    assert sorted(names, key=timing_report.phase_key) == [
        "sheets", "reconcile", "reconcile-refs", "identify", "merge", "merge-refs",
        "export", "export-nt", "alpha", "zeta"]


def test_compare_in_build_order(tmp_path, capsys):
    for d in ("old", "new"):
        write_timing(tmp_path / d, "merge", 0, 10, 10, 9, {})
        write_timing(tmp_path / d, "identify", "all", 10, 10, 9, {})
        write_timing(tmp_path / d, "reconcile", 0, 10, 10, 9, {})
    assert lux("--compare", str(tmp_path / "old"), str(tmp_path / "new")) == 0
    out = capsys.readouterr().out
    assert out.index("=== reconcile") < out.index("=== identify") < out.index("=== merge")


def test_phase_timer_records_clock_times(tmp_path):
    t = PhaseTimer("merge", slice_n=0, max_slice=1, out_dir=str(tmp_path), stream=io.StringIO())
    t.step()
    t.finish()
    js = json.loads((tmp_path / "timing-merge-0.json").read_text())
    assert js["started_at"] <= js["updated_at"]
    assert abs(js["updated_at"] - __import__("time").time()) < 60


def test_old_files_take_their_times_from_the_mtime(tmp_path):
    write_timing(tmp_path, "merge", 0, 10, 600, 300, {})
    fn = tmp_path / "timing-merge-0.json"
    import os
    os.utime(fn, (T0, T0))
    s = timing_report.summarise(timing_report.load(str(tmp_path))["merge"])
    assert (s["started_at"], s["ended_at"], s["times_estimated"]) == (T0 - 600, T0, True)


def test_overall_summary(tmp_path):
    end = build(tmp_path)
    phases, _ = timing_report.directory_data(str(tmp_path))
    assert [p[0] for p in phases] == ["reconcile", "identify", "merge", "export"]
    t = timing_report.overall_summary(phases)
    assert t["phases"] == 4
    assert (t["started_at"], t["ended_at"], t["elapsed"]) == (T0, end, end - T0)
    assert t["between"] == 180 and t["in_phases"] == end - T0 - 180
    assert t["gaps"] == []
    assert [r[0] for r in t["timeline"]] == ["reconcile", "identify", "merge", "export"]
    assert t["wall"] == 7900 + 2700 + 10800 + 2400
    # merge's wall shared out by worker time: half of it waiting on the idmap
    ph, name, secs, share = t["bottlenecks"][0]
    assert (ph, name, secs) == ("merge", "fetch_idmap", 5400)
    assert share == pytest.approx(5400 / t["wall"])
    assert ("merge", "merge", 3240, pytest.approx(3240 / t["wall"])) in t["bottlenecks"]
    assert len(t["bottlenecks"]) == timing_report.TOP_BOTTLENECKS
    # nested stages never compete with their parent
    assert all(name != "acquire.map" for _, name, _, _ in t["bottlenecks"])
    # reconcile's slowest slice ran 2400s past the median
    assert t["stragglers"] == [("reconcile", 2400, [2])]
    assert t["incomplete"] == [] and not t["times_estimated"]


def test_one_phase_has_no_summary(tmp_path, capsys):
    merge_run(tmp_path)
    phases, _ = timing_report.directory_data(str(tmp_path))
    assert timing_report.overall_summary(phases) is None
    assert lux("--dir", str(tmp_path)) == 0
    assert "=== all" not in capsys.readouterr().out


def test_far_apart_phases_are_flagged(tmp_path):
    build(tmp_path, gap=7 * 3600)
    t = timing_report.overall_summary(timing_report.directory_data(str(tmp_path))[0])
    assert [(a, b) for a, b, _ in t["gaps"]] == [("reconcile", "identify"), ("identify", "merge"),
                                                ("merge", "export")]


def test_summary_at_the_bottom_plain(tmp_path, capsys):
    build(tmp_path)
    assert lux("--dir", str(tmp_path)) == 0
    out = capsys.readouterr().out
    tail = out[out.index("=== all 4 phases"):]
    assert out.index("=== export") < out.index("=== all 4 phases")
    assert "start to end" in tail and "3m 00s between phases" in tail
    assert "merge / fetch_idmap" in tail and "1h 30m" in tail
    assert "reconcile waited 40m 00s on its slowest slice (slow: 2)" in tail


def test_summary_at_the_bottom_rich(tmp_path, capsys, wide):
    build(tmp_path, gap=7 * 3600)
    assert lux_rich("--dir", str(tmp_path)) == 0
    out = capsys.readouterr().out
    tail = out[out.index("─ all 4 phases "):]
    assert out.index("─ export ") < out.index("─ all 4 phases ")
    assert "start to end" in tail and "Timeline" in tail and "Biggest bottlenecks" in tail
    assert "fetch_idmap" in tail and "1h 30m" in tail
    assert "probably not the same build" in tail
    assert "reconcile waited 40m 00s on its slowest slice" in tail


def test_durations():
    f = timing_report.fmt_duration
    assert (f(45), f(870), f(7500), f(-3)) == ("45s", "14m 30s", "2h 05m", "0s")
    assert f(4 * 86400 + 8100) == "4d 02h"


# --- files left by an earlier run -------------------------------------------

OLD = T0 - 4 * 86400


def mixed_reconcile(d):
    """Today's 4-way reconcile over the files of an 8-way one four days ago
    (slices 4-7 survive), and reconcile-refs over an unsliced run's -all."""
    for n in range(8):
        write_timing(d, "reconcile", n, 1000, 9000, 4500, {"acquire": (1000, 4500)},
                     started_at=OLD, max_slice=8)
    for n in range(4):
        write_timing(d, "reconcile", n, 1000, 8100, 4000, {"acquire": (1000, 4000)},
                     started_at=T0, max_slice=4)
    write_timing(d, "reconcile-refs", "all", 10, 600, 300, {}, started_at=OLD, max_slice=-1)
    for n in range(4):
        write_timing(d, "reconcile-refs", n, 100, 3000, 1500, {}, started_at=T0 + 8200,
                     max_slice=4)


def test_an_earlier_runs_slices_are_left_out(tmp_path):
    mixed_reconcile(tmp_path)
    stale = {}
    phases = timing_report.load(str(tmp_path), stale=stale)
    assert sorted(r["slice"] for r in phases["reconcile"]) == [0, 1, 2, 3]
    assert len(phases["reconcile-refs"]) == 4
    assert stale == {"reconcile": [f"timing-reconcile-{n}.json" for n in range(4, 8)],
                     "reconcile-refs": ["timing-reconcile-refs-all.json"]}
    s = timing_report.summarise(phases["reconcile"])
    assert (s["started_at"], s["wall"], s["slices"]) == (T0, 8100, 4)


def test_the_timeline_starts_with_the_latest_run(tmp_path):
    mixed_reconcile(tmp_path)
    t = timing_report.overall_summary(timing_report.directory_data(str(tmp_path))[0])
    assert t["started_at"] == T0
    assert [(r[0], r[1]) for r in t["timeline"]] == [("reconcile", T0),
                                                    ("reconcile-refs", T0 + 8200)]
    assert t["spread_out"] == [] and t["gaps"] == []


def test_left_out_files_are_named(tmp_path, capsys, wide):
    mixed_reconcile(tmp_path)
    assert lux("--dir", str(tmp_path)) == 0
    out = capsys.readouterr().out
    assert ("ignored 4 files of an earlier run with a different number of slices: "
            "timing-reconcile-4.json, timing-reconcile-5.json") in out
    assert "ignored 1 file of an earlier run" in out and "timing-reconcile-refs-all.json" in out
    assert lux_rich("--dir", str(tmp_path)) == 0
    assert "ignored 4 files of an earlier run" in capsys.readouterr().out


def test_the_latest_run_wins_even_unsliced(tmp_path):
    for n in range(4):
        write_timing(tmp_path, "reconcile", n, 1000, 8100, 4000, {}, started_at=OLD, max_slice=4)
    write_timing(tmp_path, "reconcile", "all", 1, 60, 30, {}, started_at=T0, max_slice=-1)
    stale = {}
    phases = timing_report.load(str(tmp_path), stale=stale)
    assert [r["file"] for r in phases["reconcile"]] == ["timing-reconcile-all.json"]
    assert len(stale["reconcile"]) == 4


def test_slices_spread_over_days_are_flagged(tmp_path, capsys):
    """Same number of slices, so nothing to tell them apart by -- but a
    phase whose slices span days against a wall clock of hours is shown."""
    write_timing(tmp_path, "reconcile", 0, 1000, 8100, 4000, {}, started_at=OLD, max_slice=2)
    write_timing(tmp_path, "reconcile", 1, 1000, 8100, 4000, {}, started_at=T0, max_slice=2)
    write_timing(tmp_path, "merge", 0, 1000, 3600, 1000, {}, started_at=T0 + 9000)
    t = timing_report.overall_summary(timing_report.directory_data(str(tmp_path))[0])
    assert [(ph, w) for ph, _, w in t["spread_out"]] == [("reconcile", 8100)]
    assert lux("--dir", str(tmp_path)) == 0
    assert ("reconcile's slices span 4d 02h but its wall clock is 2h 15m -- slices from "
            "more than one run?") in capsys.readouterr().out
