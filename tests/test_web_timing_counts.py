"""Web UI item W11: the timing report, counts and maintenance pages, from
the functions `lux timing-report`, `lux counts`, `lux pg-tune` and `lux
pg-maintain --bloat` print from. On the W00 installation; Postgres is
stubbed, never reached."""

import io
import json
import re
import sys
import time
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.process.utils import timing_report as tr
from lux_pipeline.process.utils.timing import PhaseTimer
from lux_pipeline.storage import pg_tune
from web_client import make_client
from webui_fixtures import make_configs, seed_records


@pytest.fixture
def web(tmp_path, monkeypatch):
    client, state = make_client(tmp_path, monkeypatch, seed=False)
    seed_records(make_configs(Path(state.configs().config_dir), instantiate=True))
    state.reset()
    return client, state


def write_timing(d, phase, n, records, seconds, cpu, stages, complete=True, started_at=None):
    d.mkdir(parents=True, exist_ok=True)
    js = {"phase": phase, "slice": n, "records": records, "seconds": seconds, "cpu_seconds": cpu,
          "complete": complete, "records_per_second": records / seconds, "max_slice": 3,
          "stages": {k: {"calls": c, "seconds": s} for k, (c, s) in stages.items()}}
    if started_at is not None:
        js["started_at"], js["updated_at"] = started_at, started_at + seconds
    (d / f"timing-{phase}-{n}.json").write_text(json.dumps(js))


def build(d, slow=3600, t0=None):
    """Three merge slices, the last `slow` seconds, with a nested stage; a
    reconcile phase before it."""
    t0 = t0 or time.time() - 20000
    for n in range(3):
        write_timing(d, "reconcile", n, 1000, 600, 500, {"reconcile": (1000, 400)}, started_at=t0)
    for n, secs in enumerate((3600, 3600, slow)):
        write_timing(d, "merge", n, 36000, secs, secs / 2,
                     {"merge": (36000, secs / 2), "merge.fetch": (36000, secs / 4),
                      "write": (36000, secs / 4)}, started_at=t0 + 1000)


# --- timing ------------------------------------------------------------------------------


def test_the_timing_page_is_the_report(web):
    client, state = web
    logs = Path(state.configs().log_dir)
    build(logs, slow=7200)
    d = client.get("/api/timing").json()
    phases, _ = tr.directory_data(str(logs))
    assert [p["phase"] for p in d["phases"]] == [ph for ph, _, _ in phases] == ["reconcile", "merge"]
    merge = d["phases"][1]
    s = phases[1][1]
    assert merge["records"] == s["records"] == 108000 and merge["wall"] == s["wall"] == 7200
    assert [st["name"] for st in merge["stages"]][:2] == ["merge", "write"]
    assert merge["stages"][-1]["name"] == "(unattributed)"
    assert merge["nested"][0]["parent"] == "merge" and merge["nested"][0]["rows"][0]["name"] == "merge.fetch"
    assert merge["rates"]["slow"] == [2] and [r["class"] for r in merge["slice_rows"]] == ["", "", "slow"]
    t = d["summary"]
    assert t["phases"] == 2 and [r["phase"] for r in t["timeline"]] == ["reconcile", "merge"]
    assert all(0 <= r["x"] <= 1000 and r["x"] + r["w"] <= 1000.1 for r in t["timeline"])
    html = client.get("/timing").text
    assert "inside merge" in html and "Slow slices: 2" in html and "<svg" in html
    assert '<tr class="slow">' in html and "2h 00m" in html
    assert " style=" not in html


def test_one_phase_and_no_timing_at_all(web):
    client, state = web
    assert "no timing-*.json" in client.get("/api/timing").json()["error"]
    assert client.get("/timing").status_code == 200
    build(Path(state.configs().log_dir))
    d = client.get("/api/timing?phase=reconcile").json()
    assert [p["phase"] for p in d["phases"]] == ["reconcile"] and d["summary"] is None


def test_profiles(web):
    client, state = web
    logs = Path(state.configs().log_dir)
    for n in (0, 1):
        t = PhaseTimer("reconcile", slice_n=n, max_slice=2, out_dir=str(logs), stream=io.StringIO(),
                       profile=True)
        for _ in range(3):
            with t.stage("busy"):
                sum(i * i for i in range(2000))
            t.step()
        t.finish()
    p = client.get("/api/timing?profile=5").json()["phases"][0]["profile"]
    assert p["slices"] == 2 and len(p["top"]["cumtime"]) <= 5 and p["top"]["tottime"]
    assert "cumtime" in p["titles"]
    assert "Profile" in client.get("/timing?profile=5").text
    build(logs)
    assert client.get("/api/timing?phase=merge&profile=5").json()["no_profiles"]


def test_directories_must_be_inside_the_installation(web, tmp_path):
    client, state = web
    elsewhere = tmp_path / "elsewhere"
    build(elsewhere)
    assert client.get("/timing", params={"dir": str(elsewhere)}).status_code == 404
    assert client.get("/timing", params={"dir": "/etc"}).status_code == 404
    inside = Path(state.configs().data_dir) / "old-logs"
    build(inside)
    assert client.get("/api/timing", params={"dir": str(inside)}).json()["phases"]


def test_each_phase_links_to_the_run_it_comes_from(web):
    client, state = web
    logs = Path(state.configs().log_dir)
    build(logs / "build-1" / "steps")
    d = client.get("/api/timing").json()
    assert [(p["dir"], p["run"]) for p in d["phases"]] == [("build-1/steps", "build-1/steps")] * 2
    assert 'href="/runs/build-1/steps"' in client.get("/timing").text
    # a run's own directory: a flat report, no run link
    one = client.get("/api/timing", params={"dir": str(logs / "build-1" / "steps")}).json()
    assert [(p["dir"], p["run"]) for p in one["phases"]] == [(None, None)] * 2


def test_compare(web):
    client, state = web
    data = Path(state.configs().data_dir)
    build(data / "before", slow=3600)
    build(data / "after", slow=7200)
    d = client.get("/api/timing/compare", params={"before": str(data / "before"),
                                                  "after": str(data / "after")}).json()
    merge = next(p for p in d["phases"] if p["phase"] == "merge")
    wall = next(m for m in merge["metrics"] if m["label"] == "wall minutes")
    assert wall["change"] == pytest.approx(100.0) and wall["better"] is False
    html = client.get("/timing/compare", params={"before": str(data / "before"),
                                                 "after": str(data / "after")}).text
    assert "+100.0%" in html and "state-fail" in html


# --- counts ------------------------------------------------------------------------------


def test_counts_are_lux_counts(web):
    client, state = web
    d = client.get("/api/counts?source=aat,ycba&cache=datacache,recordcache&map=all&index=all").json()
    caches = [r["cells"] for r in d["rows"] if r["section"] == "caches"]
    assert caches == [["external", "aat", "datacache", "=1"], ["external", "aat", "recordcache", "=1"],
                      ["internal", "ycba", "datacache", "=1"], ["internal", "ycba", "recordcache", "=1"]]
    assert {r["cells"][0] for r in d["rows"] if r["section"] == "maps"} == set(state.configs().map_stores)
    assert d["totals"]["caches"] == "~4"
    assert state.last_counts["rows"] == d["rows"]


def test_the_counts_page_counts_only_when_asked(web):
    client, state = web
    html = client.get("/counts").text
    assert "Not counted since the server started." in html
    html = client.get("/counts?go=1&source=aat").text
    assert "<td>aat</td>" in html and "Counted" in html
    assert "Counted" in client.get("/counts").text                 # the last count is kept
    r = client.get("/counts?go=1&source=nope")
    assert r.status_code == 400 and "Unknown source: nope" in r.text


def test_counts_stream_row_by_row(web):
    client, _ = web
    with client.stream("GET", "/api/counts/events?source=aat&cache=recordcache") as s:
        lines = [l for l in s.iter_lines() if l.startswith(("event:", "data:"))]
    events = [l[7:] for l in lines if l.startswith("event:")]
    assert events == ["row", "done"]
    done = json.loads(lines[-1][6:])
    assert done["totals"] == {"caches": "~1"}
    assert client.get("/api/counts/events?source=nope").status_code == 400


# --- maintenance -----------------------------------------------------------------------------


MACHINE = {"cpus": {"logical": 8, "physical": 4, "limited_by": None},
           "ram": {"total": 32 * 1024 ** 3, "limited_by": None},
           "storage": {"kind": "ssd", "why": "test"}}


def test_tuning_without_a_server(web, monkeypatch):
    client, _ = web
    monkeypatch.setattr(pg_tune, "analyse", lambda db, no_db, table: (MACHINE, None, "connection refused"))
    d = client.get("/api/maintenance/tune").json()
    assert d["server"] is None and d["why"] == "connection refused" and d["pending"] is None
    assert d["machine"]["cpus"] == 8 and d["machine"]["ram_gb"] == 32.0 and d["workers"] == 2
    html = client.get("/maintenance").text
    assert "not read: connection refused" in html and "The full report and the SQL" in html


def test_tuning_with_settings_to_change(web, monkeypatch):
    client, _ = web
    monkeypatch.setattr(pg_tune, "analyse",
                        lambda db, no_db, table: (MACHINE, {"version": "PostgreSQL 18", "data_dir": "/pg"}, None))
    monkeypatch.setattr(pg_tune, "report", lambda m, srv, why, args: (["ALTER SYSTEM SET x = 1;"], 0))
    monkeypatch.setattr(pg_tune, "pending_changes",
                        lambda m, srv, workers: [("shared_buffers", "128MB", "8GB")])
    d = client.get("/api/maintenance/tune?workers=24").json()
    assert d["workers"] == 24 and d["pending"] == [
        {"name": "shared_buffers", "current": "128MB", "recommended": "8GB"}]
    html = client.get("/maintenance?workers=24").text
    assert "1 setting(s) to change" in html and "ALTER SYSTEM SET x = 1;" in html


def test_a_tuning_failure_is_a_message(web, monkeypatch):
    client, _ = web

    def boom(*a):
        raise OSError("no /proc")

    monkeypatch.setattr(pg_tune, "analyse", boom)
    r = client.get("/maintenance")
    assert r.status_code == 200 and "OSError: no /proc" in r.text


def test_bloat(web, monkeypatch):
    client, state = web
    monkeypatch.setattr(pg_tune, "analyse", lambda db, no_db, table: (MACHINE, None, "stub"))
    d = client.get("/api/maintenance/bloat?source=aat").json()
    assert d["rows"] == [] and "aat datacache" in d["skipped"]          # filesystem caches
    ended = []

    class PgLike:
        def bloat(self):
            return {"name": "aat_data_cache", "live": 100, "dead": 80, "heap": 2048, "indexes": 1024,
                    "toast": 10 * 1024 ** 2, "total": 10 * 1024 ** 2 + 3072, "free_percent": 45.0}

        def end_read(self):
            ended.append(1)

    state.source("aat")["datacache"] = PgLike()
    d = client.get("/api/maintenance/bloat?source=aat&cache=datacache").json()
    (row,) = d["rows"]
    assert row["cells"][:4] == ["aat", "datacache", "100", "10MB"] and row["cells"][7] == "45%"
    assert row["rewrite"] and "lux pg-maintain --rewrite --source aat --cache datacache" in row["cells"][8]
    assert ended == [1]
    with client.stream("GET", "/api/maintenance/bloat/events?source=aat") as s:
        events = [l[7:] for l in s.iter_lines() if l.startswith("event:")]
    assert events == ["row", "done"]
    assert "10MB" in client.get("/maintenance?bloat=1&source=aat").text


def test_the_pages_are_in_the_navigation(web, monkeypatch):
    client, _ = web
    monkeypatch.setattr(pg_tune, "analyse", lambda db, no_db, table: (MACHINE, None, "stub"))
    html = client.get("/").text
    for label, href in (("Counts", "/counts"), ("Timing", "/timing"), ("Maintenance", "/maintenance")):
        assert f'<a href="{href}">{label}</a>' in html
    for path in ("/counts", "/timing", "/maintenance"):
        page = client.get(path).text
        for tag in re.findall(r"<script[^>]*>", page):
            assert 'src="/static/' in tag or 'type="application/json"' in tag
        assert " style=" not in page
