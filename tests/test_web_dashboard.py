"""Web UI item W06: the dashboard and the health page, on the W00
installation with seeded records and fake runs."""

import os
import sys
import time
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.web.api.health import start_health
from web_client import csrf, make_client
from webui_fixtures import write_fake_run


@pytest.fixture
def web(tmp_path, monkeypatch):
    return make_client(tmp_path, monkeypatch)


def logs(state):
    return Path(state.configs().log_dir)


def test_dashboard_data(web):
    client, state = web
    write_fake_run(logs(state), "merge", 2, ts="2026-11-01-100000", done=True, exit=0)
    (logs(state) / "pipeline_process_status.txt").write_text(
        "Sun Nov 01 10:00:00 2026 [Error] Error in merge\nMon Nov 02 10:00:00 2026 [Success] Build was successful\n")
    d = client.get("/api/dashboard").json()
    i = d["installation"]
    assert i["base_dir"] == str(state.configs().base_dir) and i["max_workers"] == 2
    assert i["postgres"].startswith("local socket") and i["base_record"] == "base"
    assert d["sources"] == {"internal": ["ycba"], "external": ["aat"], "results": ["merged"]}
    assert d["running"] == [] and [r["command"] for r in d["recent"]] == ["merge"]
    assert d["recent"][0]["state"] == "finished" and d["recent"][0]["total"] == 200
    assert d["last_build"][-1].endswith("[Success] Build was successful")
    assert d["health"] is None


def test_the_dashboard_page(web):
    client, state = web
    html = client.get("/").text
    assert str(state.configs().base_dir) in html and "ycba" in html and "aat" in html
    assert "Nothing is running." in html and "No runs under" in html


def test_a_running_build_shows(web):
    client, state = web
    prefix = write_fake_run(logs(state), "reconcile", 2, totals=(100, None))
    running = client.get("/api/dashboard/running").json()
    assert [(r["command"], r["state"], r["completed"], r["total"]) for r in running] == [
        ("reconcile", "running", 80, None)]
    assert running[0]["prefix"] == prefix and running[0]["labels"] == ["source0", "source1"]
    html = client.get("/").text
    assert "Running now" in html and '<progress class="bar" title="80 so far' in html
    assert 'id="running-badge"' in html          # app.js shows it from /api/dashboard/running


def test_health_runs_every_check_and_says_why(web):
    client, state = web
    r = client.post("/health/run", data={"_csrf": csrf(client)})
    assert r.status_code == 200 and r.url.path == "/health"
    deadline = time.time() + 30
    while state.health["running"] and time.time() < deadline:
        time.sleep(0.05)
    h = client.get("/api/health").json()
    by = {x["label"]: x for x in h["results"]}
    assert by[".env"]["state"] == "OK" and by["LUX_BASEPATH"]["state"] == "OK"
    assert by["postgres reachable"]["detail"] == "PostgreSQL (stub)"
    assert by["validator schemas present"]["state"] == "FAIL"
    assert "worker processes spawn" not in by and "postgres tuned for the build" not in by
    assert h["finished"] and not h["running"]
    page = client.get("/health").text
    assert "validator schemas present" in page and "Checked" in page
    assert client.get("/api/dashboard").json()["health"]["results"]


def test_health_with_postgres_down_still_runs_every_check(tmp_path, monkeypatch):
    client, state = make_client(tmp_path, monkeypatch, seed=False,
                                postgres=ConnectionError("no server"))
    results = start_health(state, wait=True)
    by = {x["label"]: x for x in results}
    assert by["postgres reachable"]["state"] == "FAIL"
    assert by["postgres reachable"]["detail"] == "ConnectionError: no server"
    assert by["idmap reachable"]["state"] == "OK"


def test_health_streams_its_results_then_done(web):
    client, state = web
    start_health(state, wait=True)
    with client.stream("GET", "/api/health/events") as s:
        events = [l.split(": ", 1)[1] for l in s.iter_lines() if l.startswith("event:")]
    assert events[-1] == "done" and events.count("result") == len(state.health["results"])


def test_one_health_run_at_a_time(web):
    client, state = web
    state.health["running"] = True
    r = client.post("/api/health/run", headers={"X-CSRF-Token": csrf(client)})
    assert r.status_code == 409 and "already running" in r.json()["error"]
