"""Postgres's and the machine's cpu beside the workers' own: the counters
(`lux_pipeline.process.host_cpu`) from a fake /proc and from a stub psutil,
the difference between two readings, PhaseTimer recording them, and the
report taking a phase's earliest and latest readings rather than adding
slices up."""

import io
import json
import sys
from collections import namedtuple
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.cli import entry
from lux_pipeline.process import host_cpu, timing_report
from lux_pipeline.process.timing import PhaseTimer

TICKS = 100


@pytest.fixture(autouse=True)
def no_cache(monkeypatch):
    monkeypatch.setattr(host_cpu, "CACHE_SECONDS", 0)


def fake_proc(root, procs, stat="cpu  1000 0 500 8000 500 0 0 0 0 0", cpus=4):
    """`procs`: ``{pid: (comm, utime, stime, cutime, cstime)}`` in ticks."""
    root.mkdir(parents=True, exist_ok=True)
    lines = [stat] + [f"cpu{i} 0 0 0 0 0 0 0 0 0 0" for i in range(cpus)] + ["intr 1 2 3"]
    (root / "stat").write_text("\n".join(lines) + "\n")
    for pid, (comm, ut, st, cut, cst) in procs.items():
        (root / str(pid)).mkdir(exist_ok=True)
        # pid (comm) state ppid pgrp session tty tpgid flags minflt cminflt majflt cmajflt
        # utime stime cutime cstime ...
        (root / str(pid) / "stat").write_text(
            f"{pid} ({comm}) S 1 1 1 0 -1 0 0 0 0 0 {ut} {st} {cut} {cst} 20 0 1 0 0\n")
    return str(root)


# --- /proc ------------------------------------------------------------------


def test_postgres_from_proc(tmp_path):
    proc = fake_proc(tmp_path / "proc", {
        100: ("postgres", 1000, 500, 20000, 4000),    # postmaster: reaped backends in cutime/cstime
        101: ("postgres", 300, 200, 0, 0),
        102: ("python3", 99999, 99999, 0, 0),
        103: ("post gres) x", 5, 5, 0, 0),             # a comm with spaces and a paren
    })
    (tmp_path / "proc" / "self").mkdir()               # not a pid
    assert host_cpu.postgres_seconds(proc, TICKS) == (1500 + 24000 + 500) / TICKS


def test_no_postgres_is_none(tmp_path):
    proc = fake_proc(tmp_path / "proc", {102: ("python3", 1, 1, 0, 0)})
    assert host_cpu.postgres_seconds(proc, TICKS) is None


def test_host_from_proc_stat(tmp_path):
    # user nice system idle iowait irq softirq steal guest guest_nice
    proc = fake_proc(tmp_path / "proc", {}, stat="cpu  1000 100 500 8000 400 10 20 30 7 0")
    h = host_cpu.host_seconds(proc, TICKS)
    assert h == {"busy": (1000 + 100 + 500 + 10 + 20 + 30) / TICKS, "iowait": 4.0,
                 "idle": 80.0, "cpus": 4}


# --- psutil (macOS) ---------------------------------------------------------

PCpu = namedtuple("pcputimes", "user system children_user children_system")
HostCpu = namedtuple("scputimes", "user nice system idle")


def stub_psutil(procs, host):
    def process_iter(attrs):
        for name, ct in procs:
            def cpu_times(ct=ct):
                if ct is None:
                    raise PermissionError("access denied")
                return ct
            yield SimpleNamespace(info={"name": name}, cpu_times=cpu_times)
    return SimpleNamespace(process_iter=process_iter, cpu_times=lambda: host,
                           cpu_count=lambda: 18)


def test_psutil_without_proc(tmp_path, monkeypatch):
    ps = stub_psutil([("postgres", PCpu(70.0, 6.0, 0.0, 0.0)), ("postgres", PCpu(1.5, 0.5, 0, 0)),
                      ("postgres", None),                  # another user's: access denied
                      ("Finder", PCpu(500.0, 500.0, 0, 0))],
                     HostCpu(user=200.0, nice=0.0, system=100.0, idle=700.0))
    monkeypatch.setattr(host_cpu, "_psutil", lambda: ps)
    missing = str(tmp_path / "no-proc")
    assert host_cpu.postgres_seconds(missing) == 78.0
    # macOS has no iowait
    assert host_cpu.host_seconds(missing) == {"busy": 300.0, "iowait": None, "idle": 700.0,
                                              "cpus": 18}


def test_nothing_to_read_with(tmp_path, monkeypatch):
    monkeypatch.setattr(host_cpu, "_psutil", lambda: None)
    r = host_cpu.read_counters(str(tmp_path / "no-proc"))
    assert r["postgres"] is None and r["host"] is None and r["at"] > 0
    assert host_cpu.delta(r, {**r, "at": r["at"] + 10}) is None


def test_this_machine_can_be_read():
    """Whatever this machine is (Linux /proc, or psutil on macOS), the host
    counters come back."""
    r = host_cpu.read_counters()
    assert r["host"] is not None and r["host"]["busy"] > 0 and r["host"]["cpus"]


# --- delta ------------------------------------------------------------------


def reading(at, pg, busy, iowait, idle, cpus=4):
    host = None if busy is None else {"busy": busy, "iowait": iowait, "idle": idle, "cpus": cpus}
    return {"at": at, "postgres": pg, "host": host}


def test_delta():
    d = host_cpu.delta(reading(0, 100, 1000, 200, 3000), reading(100, 300, 1200, 300, 3100))
    assert d["seconds"] == 100 and d["postgres"] == 200 and d["postgres_cores"] == 2
    assert d["busy_percent"] == pytest.approx(50) and d["iowait_percent"] == pytest.approx(25)
    assert d["cpus"] == 4


def test_delta_without_iowait():
    d = host_cpu.delta(reading(0, None, 1000, None, 3000), reading(10, None, 1010, None, 3030))
    assert d["postgres"] is None and d["busy_percent"] == 25 and d["iowait_percent"] is None


def test_delta_counter_went_backwards():
    # postgres restarted: its total fell; the host counters still compare
    d = host_cpu.delta(reading(0, 500, 10, 0, 10), reading(10, 20, 20, 0, 20))
    assert d["postgres"] is None and d["busy_percent"] == 50
    assert host_cpu.delta(reading(10, 1, 1, 1, 1), reading(10, 2, 2, 2, 2)) is None


# --- PhaseTimer and the report ---------------------------------------------


def test_phase_timer_records_the_counters(tmp_path):
    t = PhaseTimer("merge", slice_n=0, max_slice=1, out_dir=str(tmp_path), stream=io.StringIO())
    t.step()
    t.finish()
    js = json.loads((tmp_path / "timing-merge-0.json").read_text())
    start, now = js["host_cpu"]["start"], js["host_cpu"]["now"]
    assert start["at"] <= now["at"]
    assert now["host"] is not None


def write(d, phase, n, start, end, started_at, cpu=100.0, max_slice=3):
    d.mkdir(parents=True, exist_ok=True)
    js = {"phase": phase, "slice": n, "max_slice": max_slice, "records": 100, "seconds": 3600,
          "cpu_seconds": cpu, "complete": True, "records_per_second": 100 / 3600,
          "started_at": started_at, "updated_at": started_at + 3600, "stages": {},
          "host_cpu": {"start": start, "now": end}}
    (d / f"timing-{phase}-{n}.json").write_text(json.dumps(js))


def test_a_phase_takes_its_earliest_and_latest_readings(tmp_path):
    # three slices read the same machine: subtracting per slice and adding
    # up would count postgres three times
    write(tmp_path, "merge", 0, reading(0, 1000, 0, 0, 0), reading(3600, 8200, 3600, 7200, 3600), 0)
    write(tmp_path, "merge", 1, reading(5, 1001, 1, 1, 1), reading(3590, 8100, 3590, 7100, 3590), 5)
    write(tmp_path, "merge", 2, reading(9, 1002, 2, 2, 2), reading(3700, 8300, 3700, 7300, 3700), 9)
    s = timing_report.summarise(timing_report.load(str(tmp_path))["merge"])
    h = s["host"]
    assert h["seconds"] == 3700 and h["postgres"] == 7300
    assert h["postgres_cores"] == pytest.approx(7300 / 3700)
    # slice 0's start to slice 2's end: 7300 of 14700 cpu-seconds waiting on io
    assert h["iowait_percent"] == pytest.approx(7300 / 14700 * 100)
    lines = "\n".join(timing_report.report_lines("merge", s))
    assert "postgres 2.0 cpu-hours (2.0 cores) beside the workers' 0.1" in lines
    assert "machine 25% busy, 50% iowait over 4 cpus" in lines
    assert "(postgres kept 2.0 cores busy meanwhile)" in lines


def test_the_summary_measures_the_whole_span(tmp_path, capsys, monkeypatch):
    monkeypatch.setenv("COLUMNS", "160")
    write(tmp_path, "reconcile", 0, reading(0, 0, 0, 0, 0), reading(3600, 3600, 3600, 0, 3600), 0)
    # merge starts an hour after reconcile ends
    write(tmp_path, "merge", 0, reading(7200, 3600, 7200, 0, 7200),
          reading(10800, 18000, 7200, 3600, 7200), 7200)
    t = timing_report.overall_summary(timing_report.directory_data(str(tmp_path))[0])
    assert t["host"]["seconds"] == 10800 and t["host"]["postgres"] == 18000
    assert entry.run(["--no-ui", "timing-report", "--dir", str(tmp_path)]) == 0
    assert "start to end: postgres 5.0 cpu-hours" in capsys.readouterr().out
    assert entry.run(["timing-report", "--dir", str(tmp_path)]) == 0
    out = capsys.readouterr().out
    assert "postgres cpu-h" in out and "5.0  (1.7 cores)" in out and "iowait" in out


def test_macos_readings_show_no_iowait(tmp_path, capsys, monkeypatch):
    monkeypatch.setenv("COLUMNS", "160")
    write(tmp_path, "merge", 0, reading(0, 10, 100, None, 900),
          reading(60, 70, 150, None, 1450), 0, max_slice=1)
    s = timing_report.summarise(timing_report.load(str(tmp_path))["merge"])
    assert "machine 8% busy over 4 cpus" in "\n".join(timing_report.report_lines("merge", s))
    assert entry.run(["timing-report", "--dir", str(tmp_path)]) == 0
    assert "n/a" in capsys.readouterr().out


def test_old_files_have_no_host_line(tmp_path):
    d = tmp_path
    js = {"phase": "merge", "slice": 0, "records": 1, "seconds": 10, "cpu_seconds": 5,
          "complete": True, "records_per_second": 0.1, "stages": {}}
    (d / "timing-merge-0.json").write_text(json.dumps(js))
    s = timing_report.summarise(timing_report.load(str(d))["merge"])
    assert s["host"] is None
    assert not any("postgres" in l for l in timing_report.report_lines("merge", s))


def test_postgres_reading_is_reused_briefly(tmp_path, monkeypatch):
    monkeypatch.setattr(host_cpu, "CACHE_SECONDS", 60)
    proc = tmp_path / "proc"
    fake_proc(proc, {100: ("postgres", 100, 0, 0, 0)})
    assert host_cpu.postgres_seconds(str(proc), TICKS) == 1.0
    fake_proc(proc, {100: ("postgres", 500, 0, 0, 0)})
    assert host_cpu.postgres_seconds(str(proc), TICKS) == 1.0      # cached
    monkeypatch.setattr(host_cpu, "CACHE_SECONDS", 0)
    assert host_cpu.postgres_seconds(str(proc), TICKS) == 5.0
