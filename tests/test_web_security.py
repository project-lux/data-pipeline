"""Web UI item W13: what keeps the server safe to leave running. The
token and the sign-in page, the CSRF check on every route that changes
something, the response headers, no inline scripts or styles anywhere,
paths from URLs kept inside their roots, one slow report at a time, and
the action log."""

import html
import json
import os
import re
import sys
import time
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
sys.path.insert(0, str(Path(__file__).parent))

from fastapi.routing import APIRoute  # noqa: E402
from web_client import csrf, make_client  # noqa: E402
from webui_fixtures import write_fake_run  # noqa: E402

from lux_pipeline.web import security  # noqa: E402
from lux_pipeline.web.actions import ACTIONS_FILE, read_actions  # noqa: E402
from lux_pipeline.web.settings import Settings  # noqa: E402

WEB = Path(__file__).parent.parent / "lux_pipeline" / "web"
MUTATING = {"POST", "PUT", "PATCH", "DELETE"}


@pytest.fixture
def web(tmp_path, monkeypatch):
    return make_client(tmp_path, monkeypatch, seed=False)


def with_token(tmp_path, monkeypatch, token="s3cret"):
    return make_client(tmp_path, monkeypatch, seed=False,
                       settings=Settings(open_browser=False, token=token))


# --- the token -------------------------------------------------------------------


def test_the_token(tmp_path, monkeypatch):
    client, _ = with_token(tmp_path, monkeypatch)
    r = client.get("/")
    assert r.status_code == 401 and 'action="/login"' in r.text and 'name="next" value="/"' in r.text
    assert client.get("/api/dashboard").status_code == 401
    assert client.get("/api/dashboard").json()["error"] == "this server needs its token"
    assert client.get("/", headers={"Authorization": "Bearer s3cret"}).status_code == 200
    assert client.get("/", headers={"Authorization": "Bearer nope"}).status_code == 401
    # what needs no token: a monitor's check, the sign-in page and what it loads
    for path in ("/healthz", "/login", "/static/lux.css", "/static/app.js"):
        assert client.get(path).status_code == 200, path


def test_signing_in(tmp_path, monkeypatch):
    client, _ = with_token(tmp_path, monkeypatch)
    r = client.get("/login?token=wrong", follow_redirects=False)
    assert r.status_code == 401 and "not this server's token" in html.unescape(r.text) and "lux_token" not in r.cookies
    r = client.get("/login", params={"token": "s3cret", "next": "/runs?state=running"},
                   follow_redirects=False)
    assert r.status_code == 303 and r.headers["location"] == "/runs?state=running"
    assert client.cookies.get("lux_token") == "s3cret"
    assert client.get("/config").status_code == 200
    # only a path on this server to go on to
    for bad in ("https://evil.example/", "//evil.example/", "/\\\\evil"):
        r = client.get("/login", params={"token": "s3cret", "next": bad}, follow_redirects=False)
        assert r.headers["location"] == "/", bad
    # a page asked for without the token comes back to itself after signing in
    client.cookies.clear()
    assert 'name="next" value="/records?q=x"' in client.get("/records?q=x").text


def test_a_redirect_after_a_post_stays_on_this_server(web):
    client, _ = web
    for back in ("/\\evil.example/", "//evil.example/", "https://evil.example/"):
        r = client.post("/config/_reload", data={"_csrf": csrf(client), "back": back},
                        follow_redirects=False)
        assert r.status_code == 303 and r.headers["location"].startswith("/?flash="), back
    r = client.post("/config/_reload", data={"_csrf": csrf(client), "back": "/config/aat"},
                    follow_redirects=False)
    assert r.headers["location"].startswith("/config/aat?flash=")


def test_without_a_token_login_goes_home(web):
    client, _ = web
    r = client.get("/login", follow_redirects=False)
    assert r.status_code == 303 and r.headers["location"] == "/"


def test_the_token_comes_from_the_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("LUX_WEB_TOKEN", "from-env")
    assert Settings().token == "from-env"
    with pytest.raises(ValueError, match="give --token"):
        Settings(host="0.0.0.0", token=None).check()
    Settings(host="0.0.0.0", token="t").check()


def test_serve_reads_the_dotenv_files_before_its_settings(tmp_path, monkeypatch):
    """LUX_WEB_TOKEN may be in ./.env, as LUX_BASEPATH is."""
    from argparse import Namespace

    from lux_pipeline.cli import serve
    monkeypatch.delenv("LUX_WEB_TOKEN", raising=False)
    monkeypatch.delenv("LUX_BASEPATH", raising=False)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text("LUX_WEB_TOKEN=dotenv-token\n")
    from lux_pipeline import runtime
    monkeypatch.setattr(runtime, "get_configs", lambda **kw: (_ for _ in ()).throw(RuntimeError("no")))
    h = serve.CommandHandler(None)
    s = h.settings(Namespace(host="0.0.0.0", port=None, token=None, no_open=True, debug=False))
    assert s.token == "dotenv-token"


# --- CSRF ------------------------------------------------------------------------


def fill(path):
    return re.sub(r"\{[^}]+\}", "x", path)


def test_every_mutating_route_checks_csrf_first(web):
    client, _ = web
    routes = [r for r in client.app.routes if isinstance(r, APIRoute) and r.methods & MUTATING]
    assert len(routes) >= 12
    for route in routes:
        calls = [d.call for d in route.dependant.dependencies]
        assert security.require_token in calls and security.require_csrf in calls, route.path
        for method in route.methods & MUTATING:
            for kw in ({}, {"data": {"_csrf": "wrong"}}, {"headers": {"X-CSRF-Token": "wrong"}}):
                r = client.request(method, fill(route.path), **kw)
                assert r.status_code == 403, (method, route.path, kw, r.status_code)


# --- headers, and nothing inline -------------------------------------------------


@pytest.mark.parametrize("path", ["/", "/api/dashboard", "/static/lux.css", "/nowhere", "/healthz"])
def test_the_security_headers(web, path):
    client, _ = web
    h = client.get(path).headers
    assert h["content-security-policy"].startswith("default-src 'self'")
    assert "frame-ancestors 'none'" in h["content-security-policy"]
    assert h["x-content-type-options"] == "nosniff" and h["referrer-policy"] == "same-origin"
    assert h["x-frame-options"] == "DENY"


def test_the_api_docs_page_may_load_its_scripts(web):
    client, _ = web
    r = client.get("/api/docs")
    assert r.status_code == 200 and "content-security-policy" not in r.headers
    assert r.headers["x-content-type-options"] == "nosniff"


def test_no_template_has_inline_script_style_or_handler():
    found = []
    for path in sorted((WEB / "templates").rglob("*.html")):
        text = path.read_text()
        for tag in re.findall(r"<script\b[^>]*>", text):
            if 'src="/static/' not in tag and 'type="application/json"' not in tag:
                found.append(f"{path.name}: {tag}")
        for bad in re.findall(r"""\s(on[a-z]+\s*=|style\s*=)|<style\b|javascript:""", text, re.I):
            found.append(f"{path.name}: {bad}")
    assert found == []


def test_no_script_writes_html_from_data():
    """The modules build elements and set textContent; nothing assigns
    innerHTML or evaluates text."""
    found = []
    for path in sorted((WEB / "static").glob("*.js")):
        text = path.read_text()
        for bad in re.findall(r"\.innerHTML\s*=|\.outerHTML\s*=|insertAdjacentHTML|\beval\(|new Function\(|document\.write", text):
            found.append(f"{path.name}: {bad}")
    assert found == []


# --- paths from URLs -------------------------------------------------------------


@pytest.mark.parametrize("root,path,inside", [
    ("/a/b", "/a/b", True), ("/a/b", "/a/b/c/d", True), ("/a/b", "/a/b/../c", False),
    ("/a/b", "/a/bc", False), ("/a/b", "/etc/passwd", False), ("/a/b", "", False),
    ("", "/a/b", False), ("/a/b", "/a/b/./c/../d", True),
])
def test_under(root, path, inside):
    assert security.under(root, path) is inside


def test_under_follows_symbolic_links(tmp_path):
    (tmp_path / "root").mkdir()
    (tmp_path / "outside").mkdir()
    os.symlink(tmp_path / "outside", tmp_path / "root" / "link")
    assert not security.under(str(tmp_path / "root"), str(tmp_path / "root" / "link"))


def test_run_names_stay_in_log_dir(web):
    client, state = web
    log_dir = state.configs().log_dir
    write_fake_run(log_dir, "merge", 1)
    outside = Path(log_dir).parent / "outside"
    write_fake_run(outside, "merge", 1)
    for rel in ("../outside/merge-2026-11-02-141500", "%2E%2E/outside/merge-2026-11-02-141500",
                "..%2Foutside%2Fmerge-2026-11-02-141500", "%2Fetc%2Fpasswd", "/etc/passwd"):
        for path in (f"/runs/{rel}", f"/api/runs/{rel}", f"/api/runs/{rel}/log",
                     f"/api/runs/{rel}/stdout"):
            assert client.get(path).status_code == 404, path
    assert client.get("/api/runs/merge-2026-11-02-141500").status_code == 200


def test_timing_directories_stay_in_the_installation(web, tmp_path):
    client, state = web
    for d in ("/etc", "../../..", str(tmp_path), os.path.join(state.configs().log_dir, "..", "..", "..")):
        assert client.get("/api/timing", params={"dir": d}).status_code == 404, d
        assert client.get("/timing/compare", params={"before": d, "after": d}).status_code in (400, 404), d


def test_record_keys_name_no_path(web):
    client, state = web
    cfgs = state.configs()
    for key in ("..%2Fbase", "%2Fetc%2Fpasswd", "../base"):
        assert client.get(f"/api/config/{key}").status_code in (400, 404), key
    before = set(Path(cfgs.config_dir).parent.rglob("*.json"))
    for key in ("../evil", "/tmp/evil", "a/b", ".hidden"):
        r = client.post("/config/_new", data={"_csrf": csrf(client), "key": key, "type": "external",
                                              "text": json.dumps({"type": "external", "name": key})})
        assert r.status_code == 400 and "not a usable record key" in r.text, key
    assert set(Path(cfgs.config_dir).parent.rglob("*.json")) == before
    assert not Path("/tmp/evil.json").exists()


# --- confirmations (each also has its own test in its area's file) ----------------


def test_confirmations_are_checked_by_the_server(web, monkeypatch):
    client, state = web
    t = csrf(client)
    r = client.post("/config/aat/delete", data={"_csrf": t, "confirm": ""})
    assert r.status_code == 400 and Path(state.configs().record_origins["aat"]["path"]).exists()
    started = []
    monkeypatch.setattr(client.app.state.runner, "start", lambda *a, **k: started.append(a))
    r = client.post("/commands/clear", data={"_csrf": t, "source": "aat"}, follow_redirects=False)
    assert r.status_code == 400 and not started
    prefix = write_fake_run(state.configs().log_dir, "merge", 1, updated=time.time(), extra={"web": True})
    cancelled = []
    monkeypatch.setattr(client.app.state.runner, "cancel", lambda p, grace=10.0: cancelled.append(p))
    client.post(f"/runs/{Path(prefix).name}/cancel", data={"_csrf": t, "confirm": "Cancel!"})
    assert not cancelled


# --- one slow report at a time ---------------------------------------------------


def test_one_count_and_one_bloat_report_at_a_time(web):
    client, state = web
    state.busy.add("counts")
    assert client.get("/api/counts?source=aat").status_code == 409
    assert "already running" in client.get("/counts?go=1&source=aat").text
    events = client.get("/api/counts/events?source=aat").text
    assert "event: failed" in events and "already running" in events
    state.busy.clear()
    assert client.get("/api/counts?source=aat").status_code == 200 and not state.busy
    assert "event: done" in client.get("/api/counts/events?source=aat").text and not state.busy
    state.busy.add("bloat")
    assert client.get("/api/maintenance/bloat?source=aat").status_code == 409
    assert "already running" in client.get("/api/maintenance/bloat/events?source=aat").text
    state.busy.clear()
    assert client.get("/api/maintenance/bloat?source=aat").status_code == 200 and not state.busy


def test_the_health_checks_run_one_at_a_time(web):
    client, state = web
    state.health["running"] = True
    r = client.post("/api/health/run", headers={"X-CSRF-Token": csrf(client)})
    assert r.status_code == 409 and "already running" in r.json()["error"]


# --- the action log --------------------------------------------------------------


def test_the_action_log(web, monkeypatch):
    client, state = web
    t = csrf(client)
    cfgs = state.configs()
    log = Path(cfgs.log_dir) / ACTIONS_FILE
    r = client.post("/config/aat", data={"_csrf": t, "mode": "text",
                                         "text": Path(cfgs.record_origins["aat"]["path"]).read_text()
                                         + " "}, follow_redirects=False)
    assert r.status_code == 303
    from lux_pipeline.web.runner import RunHandle
    monkeypatch.setattr(client.app.state.runner, "start",
                        lambda c, tail, **k: RunHandle(os.path.join(cfgs.log_dir, "list-1"), c,
                                                       ["lux", c, *tail], 4242, time.time(), "x.out"))
    r = client.post("/commands/list", data={"_csrf": t, "maps": "1"}, follow_redirects=False)
    assert r.status_code == 303
    client.post("/config/_reload", data={"_csrf": t})
    lines = [json.loads(line) for line in log.read_text().splitlines()]
    assert [e["action"] for e in lines] == ["config.write", "command.start", "config.reload"]
    write, start, _ = lines
    assert write["key"] == "aat" and write["path"] == cfgs.record_origins["aat"]["path"]
    assert write["remote"] == "testclient" and write["time"] > 0
    assert start["argv"] == ["lux", "list", "--maps"] and start["pid"] == 4242
    assert [e["action"] for e in read_actions(state)] == ["config.reload", "command.start", "config.write"]
    page = client.get("/actions").text
    assert "config.write" in page and "command.start" in page and "<code>lux list --maps</code>" in page
    assert client.get("/api/actions?limit=1").json()[0]["action"] == "config.reload"


def test_the_action_log_reads_back_its_tail(tmp_path, monkeypatch):
    client, state = make_client(tmp_path, monkeypatch, seed=False)
    log = Path(state.configs().log_dir) / ACTIONS_FILE
    with open(log, "w") as fh:
        for n in range(5000):
            fh.write(json.dumps({"time": n, "action": f"a{n}", "pad": "x" * 40}) + "\n")
        fh.write("not json\n")
    got = read_actions(state, limit=200)
    assert len(got) == 200 and got[0]["action"] == "a4999" and got[-1]["action"] == "a4800"
