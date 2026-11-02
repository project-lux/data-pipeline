"""Web UI item W09: the run list and the run page, live, from the files a
run leaves (lux_pipeline.progress, W01), with the console view's rules.
On the W00 installation with fake runs; no database, no browser."""

import io
import json
import os
import sys
import time
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.progress import RunRecord, StatusWriter, log_path, status_path
from lux_pipeline.web.api import runs as runs_api
from lux_pipeline.web.api.runs import parse_line, tail
from lux_pipeline.web.settings import Settings
from web_client import LiveServer, make_client
from webui_fixtures import dead_pid, write_fake_run


@pytest.fixture
def web(tmp_path, monkeypatch):
    client, state = make_client(tmp_path, monkeypatch, seed=False,
                                settings=Settings(open_browser=False, token=None, refresh=0.05))
    return client, state, Path(state.configs().log_dir)


def rel(prefix):
    return os.path.basename(prefix)


# --- the list --------------------------------------------------------------------------


def test_the_list_newest_first_with_states_and_filters(web):
    client, _, logs = web
    t = time.time()
    a = write_fake_run(logs, "reconcile", 2, ts="2026-11-02-090000", started=t - 10)
    b = write_fake_run(logs, "merge", 2, ts="2026-11-02-080000", started=t - 20, pid=dead_pid())
    c = write_fake_run(logs, "export", 1, ts="2026-11-01-120000", record=False, done=True)
    d = client.get("/api/runs").json()
    assert [r["rel"] for r in d["runs"]] == [rel(a), rel(b), rel(c)]
    assert [r["state"] for r in d["runs"]] == ["running", "killed", "finished"]
    assert d["commands"] == ["export", "merge", "reconcile"]
    assert [r["rel"] for r in client.get("/api/runs?state=killed").json()["runs"]] == [rel(b)]
    assert [r["rel"] for r in client.get("/api/runs?command=export").json()["runs"]] == [rel(c)]
    html = client.get("/runs").text
    assert f'<a href="/runs/{rel(a)}">' in html and "killed" in html


def test_latest_goes_to_the_newest_run(web):
    client, _, logs = web
    assert client.get("/runs/_latest").url.path == "/runs"
    p = write_fake_run(logs, "merge", 1)
    assert client.get("/runs/_latest").url.path == f"/runs/{rel(p)}"


# --- one run -------------------------------------------------------------------------------


def test_worker_rows_follow_the_console_view(web):
    client, _, logs = web
    p = write_fake_run(logs, "reconcile", 3, totals=(100, None, 50), visible=[0, 1])
    os.remove(status_path(p, 1))                          # w1 has not reported yet
    rec = json.loads(Path(p + ".run").read_text())
    rec["workers"] = 3
    Path(p + ".run").write_text(json.dumps(rec))
    d = client.get(f"/api/runs/{rel(p)}").json()
    rows = {r["n"]: r for r in d["rows"]}
    assert rows[1]["label"] == "waiting" and rows[1]["waiting"] and rows[1]["elapsed"] is None
    assert rows[0]["percent"] == 40.0 and rows[0]["label"] == "source0"
    assert rows[2]["visible"] is False                    # idle: left out of the totals
    assert d["all"]["completed"] == 40 and d["all"]["total"] is None and d["all"]["workers"] == 2
    assert d["state"] == "running" and rows[0]["eta"] is not None and rows[0]["eta"] > 0


def test_a_finished_run_and_its_numbers(web):
    client, _, logs = web
    p = write_fake_run(logs, "merge", 2, done=True, exit=0, started=time.time() - 120, totals=(100, 100),
                       completed=100)
    d = client.get(f"/api/runs/{rel(p)}").json()
    assert d["state"] == "finished" and d["exit"] == 0 and d["all"]["percent"] == 100.0
    assert d["all"]["eta"] is None and 100 <= d["all"]["elapsed"] <= 130
    html = client.get(f"/runs/{rel(p)}").text
    assert "finished" in html and "lux merge" in html and '"live": false' in html


def test_the_page_matches_the_console_view(web, tmp_path):
    from rich.console import Console
    from lux_pipeline.cli._view import BuildView
    client, _, logs = web
    p = write_fake_run(logs, "reconcile", 3, totals=(100, None, 50), done=(True, False, False))
    view = BuildView(p, 3, console=Console(file=io.StringIO()), auto_refresh=False)
    view.refresh()
    task = view._task(view.all_task)
    d = client.get(f"/api/runs/{rel(p)}").json()
    assert (d["all"]["completed"], d["all"]["total"]) == (task.completed, task.total)
    for n, tid in view.tasks.items():
        assert view._task(tid).description == f"w{n} {d['rows'][n]['label']}"


def test_names_that_leave_log_dir_are_not_runs(web):
    client, _, logs = web
    write_fake_run(logs, "merge", 1)
    outside = logs.parent / "elsewhere-w0.status"
    outside.write_text("{}")
    for path in ("/api/runs/..%2Felsewhere", "/api/runs/%2Fetc%2Fpasswd", "/runs/nope",
                 "/api/runs/..%2F..%2Fx/log", "/api/runs/nope/events"):
        assert client.get(path).status_code == 404, path


def test_a_run_started_from_a_shell(web):
    client, _, logs = web
    p = write_fake_run(logs, "export", 2, record=False, done=True)
    html = client.get(f"/runs/{rel(p)}").text
    assert "started slice by slice from a shell" in html
    d = client.get(f"/api/runs/{rel(p)}").json()
    assert d["argv"] is None and d["state"] == "finished" and d["command"] == "export"


def test_a_build_shows_its_phases_and_its_phases_their_build(web):
    client, _, logs = web
    child = write_fake_run(logs, "reconcile", 1, extra={"parent": str(logs / "build-2026-11-02-130000")})
    b = str(logs / "build-2026-11-02-130000")
    RunRecord(b, "build", ["lux", "build"], extra={
        "phases": ["sheets", "reconcile", "merge"], "current": "reconcile",
        "steps": [{"phase": "sheets", "command": "sheets", "argv": ["lux", "sheets"], "prefix": None,
                   "started": 1, "finished": 2, "exit": 0},
                  {"phase": "reconcile", "command": "reconcile", "argv": ["lux", "reconcile"],
                   "prefix": child, "started": 3, "finished": None, "exit": None}]}).start()
    d = client.get(f"/api/runs/{rel(b)}").json()
    assert [(p["phase"], p["state"]) for p in d["phases"]] == [
        ("sheets", "finished"), ("reconcile", "running"), ("merge", "pending")]
    assert d["phases"][1]["run"]["rel"] == rel(child)
    assert [c["run"]["rel"] for c in d["children"]] == [rel(child)]
    html = client.get(f"/runs/{rel(b)}").text
    assert f'<a href="/runs/{rel(child)}">reconcile</a>' in html
    page = client.get(f"/runs/{rel(child)}").text
    assert f'<a href="/runs/{rel(b)}">part of {rel(b)}</a>' in page


# --- logs ---------------------------------------------------------------------------------------


@pytest.mark.parametrize("line,expected", [
    ("14:15:01 INFO     started ycba", {"time": "14:15:01", "level": "INFO", "text": "started ycba"}),
    ("09:00:00 CRITICAL boom", {"time": "09:00:00", "level": "CRITICAL", "text": "boom"}),
    ("Traceback (most recent call last):", {"time": None, "level": "ERROR",
                                            "text": "Traceback (most recent call last):"}),
    ("ok � bad", {"time": None, "level": "ERROR", "text": "ok � bad"}),
])
def test_parse_line(line, expected):
    got = parse_line(3, line, {"level": "ERROR"})
    assert {k: got[k] for k in expected} == expected and got["worker"] == 3


def test_tail_pages_backwards_without_gaps(tmp_path):
    path = tmp_path / "big.log"
    path.write_text("".join(f"12:00:00 INFO     line {i}\n" for i in range(10_000)))
    seen, before = [], None
    while True:
        lines, start, end = tail(str(path), 200, before)
        if not lines:
            break
        seen = lines + seen
        assert before is None or end == before
        before = start
        if start == 0:
            break
    assert seen == [f"12:00:00 INFO     line {i}" for i in range(10_000)]
    lines, start, end = tail(str(path), 200)
    assert lines[-1].endswith("line 9999") and end == path.stat().st_size and len(lines) == 200


def test_tail_ignores_a_line_still_being_written(tmp_path):
    path = tmp_path / "w.log"
    path.write_text("a\nb\npartial")
    assert tail(str(path), 10)[0] == ["a", "b"]


def test_the_log_interleaves_workers_and_keeps_tracebacks_together(web):
    client, _, logs = web
    p = write_fake_run(logs, "reconcile", 2)
    with open(log_path(p, 0), "a") as fh:
        fh.write("14:16:00 ERROR    boom\nTraceback (most recent call last):\n  File x\n")
    with open(log_path(p, 1), "a") as fh:
        fh.write("14:15:30 WARNING  slow\n")
    d = client.get(f"/api/runs/{rel(p)}/log").json()
    assert [(l["worker"], l["level"], l["text"][:9]) for l in d["lines"]] == [
        (0, "INFO", "worker 0 "), (1, "INFO", "worker 1 "), (1, "WARNING", "slow"),
        (0, "ERROR", "boom"), (0, "ERROR", "Traceback"), (0, "ERROR", "  File x")]
    assert d["offsets"] == {"0": os.path.getsize(log_path(p, 0)), "1": os.path.getsize(log_path(p, 1))}
    only = client.get(f"/api/runs/{rel(p)}/log?worker=1&level=WARNING").json()
    assert [l["text"] for l in only["lines"]] == ["slow"]


def test_the_commands_own_output(web):
    client, _, logs = web
    p = write_fake_run(logs, "merge", 1)
    Path(p + ".out").write_text("status/log prefix: x\n")
    assert client.get(f"/api/runs/{rel(p)}/stdout").json()["lines"] == ["status/log prefix: x"]
    assert "own output" in client.get(f"/runs/{rel(p)}").text


# --- live ---------------------------------------------------------------------------------------


def events(stream):
    """(event, data) pairs from a server-sent event stream."""
    name = None
    for line in stream.iter_lines():
        if line.startswith("event: "):
            name = line[7:]
        elif line.startswith("data: "):
            yield name, json.loads(line[6:])


def test_the_page_follows_a_run_until_it_ends(web):
    client, state, logs = web
    p = write_fake_run(logs, "reconcile", 2, totals=(100, 100), completed=10)
    offsets = client.get(f"/api/runs/{rel(p)}/log").json()["offsets"]
    query = ",".join(f"{n}:{o}" for n, o in offsets.items())
    seen = []
    # a real server: the test client would hold the stream until it ended
    with LiveServer(client.app) as server:
        step = 0
        for name, data in server.events(f"/api/runs/{rel(p)}/events?offsets={query}"):
            seen.append(name)
            if name == "status" and step == 0:
                assert data["state"] == "running" and data["all"]["completed"] == 20
                with open(log_path(p, 1), "a") as fh:
                    fh.write("14:20:00 WARNING  disk filling\n")
                StatusWriter(status_path(p, 0), 0, throttle=0)(completed=55, total=100)
                step = 1
            elif name == "log":
                assert [(l["worker"], l["level"], l["text"]) for l in data] == [
                    (1, "WARNING", "disk filling")]
                for n in (0, 1):
                    StatusWriter(status_path(p, n), n, throttle=0)(completed=100, total=100, done=True)
                rec = json.loads(Path(p + ".run").read_text())
                rec.update(finished=time.time(), exit=0)
                Path(p + ".run").write_text(json.dumps(rec))
                step = 2
            elif name == "done":
                assert data["state"] == "finished" and data["all"]["completed"] == 200
                break
    assert seen[0] == "status" and "log" in seen and seen[-1] == "done"
    assert seen.count("log") == 1                          # only what was new; nothing twice


def test_a_finished_run_streams_once_and_ends(web):
    client, _, logs = web
    p = write_fake_run(logs, "merge", 1, done=True, exit=0)
    with client.stream("GET", f"/api/runs/{rel(p)}/events") as s:
        names = [n for n, _ in events(s)]
    # no offsets given: the whole log is new to the stream
    assert names == ["status", "log", "done"]


def test_too_many_live_pages(web, monkeypatch):
    client, _, logs = web
    p = write_fake_run(logs, "merge", 1)
    monkeypatch.setitem(runs_api._streams, "count", runs_api.STREAM_LIMIT)
    assert client.get(f"/api/runs/{rel(p)}/events").status_code == 429


def test_the_dashboard_links_to_the_run(web):
    client, _, logs = web
    p = write_fake_run(logs, "reconcile", 1)
    assert client.get("/api/dashboard/running").json()[0]["rel"] == rel(p)
    assert f'<a href="/runs/{rel(p)}">reconcile</a>' in client.get("/").text
