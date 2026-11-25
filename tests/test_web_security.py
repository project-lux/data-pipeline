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

from web_client import api_routes, csrf, make_client  # noqa: E402
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


# --- the Host header (DNS rebinding) ---------------------------------------------


@pytest.mark.parametrize("host", ["attacker.example", "attacker.example:8321", "127.0.0.1.nip.io",
                                  "192.168.1.5:8321", "", "[::2]:8321"])
@pytest.mark.parametrize("path", ["/", "/config", "/api/dashboard", "/healthz", "/static/app.js",
                                  "/login"])
def test_without_a_token_only_this_machines_names_are_answered(web, host, path):
    client, _ = web
    r = client.get(path, headers={"host": host})
    assert r.status_code == 400 and "csrf" not in r.text.lower(), (host, path)


def test_a_rebound_post_is_refused_before_it_reaches_a_route(web, monkeypatch):
    client, _ = web
    started = []
    monkeypatch.setattr(client.app.state.runner, "start", lambda *a, **k: started.append(a))
    r = client.post("/commands/counts", headers={"host": "attacker.example:8321"},
                    data={"_csrf": csrf(client)})
    assert r.status_code == 400 and started == []


@pytest.mark.parametrize("host", ["127.0.0.1", "127.0.0.1:8321", "localhost:8321", "LOCALHOST",
                                  "[::1]:8321", "::1"])
def test_without_a_token_loopback_names_are_answered(web, host):
    client, _ = web
    assert client.get("/", headers={"host": host}).status_code == 200, host


def test_with_a_token_any_host_name_will_do(tmp_path, monkeypatch):
    # the browser sends the cookie to the name it was set for, so a rebound
    # page has no token; the server can be reached by the machine's name
    client, _ = with_token(tmp_path, monkeypatch)
    auth = {"Authorization": "Bearer s3cret"}
    assert client.get("/", headers={**auth, "host": "build.example.org:8321"}).status_code == 200
    assert client.get("/", headers={"host": "attacker.example"}).status_code == 401


@pytest.mark.parametrize("header,name", [("127.0.0.1:8321", "127.0.0.1"), ("[::1]:8321", "::1"),
                                         ("::1", "::1"), ("LocalHost", "localhost"), ("", ""),
                                         ("[::1", "")])
def test_host_name(header, name):
    assert security.host_name(header) == name


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


def test_every_mutating_route_checks_csrf_first(web, tmp_path, monkeypatch):
    client, _ = web
    routes = [r for r in api_routes(client.app.routes) if r.methods & MUTATING]
    assert len(routes) >= 12
    for route in routes:
        for method in route.methods & MUTATING:
            for kw in ({}, {"data": {"_csrf": "wrong"}}, {"headers": {"X-CSRF-Token": "wrong"}}):
                r = client.request(method, fill(route.path), **kw)
                assert r.status_code == 403, (method, route.path, kw, r.status_code)
    # and the token before that, when the server has one
    (tmp_path / "token").mkdir()
    client, _ = with_token(tmp_path / "token", monkeypatch)
    for route in routes:
        for method in route.methods & MUTATING:
            r = client.request(method, fill(route.path), data={"_csrf": csrf(client)})
            assert r.status_code == 401, (method, route.path, r.status_code)
            r = client.request(method, fill(route.path), headers={"Authorization": "Bearer s3cret"})
            assert r.status_code == 403, (method, route.path, r.status_code)


# --- headers, and nothing inline -------------------------------------------------


@pytest.mark.parametrize("path", ["/", "/api/dashboard", "/static/lux.css", "/nowhere", "/healthz"])
def test_the_security_headers(web, path):
    client, _ = web
    h = client.get(path).headers
    assert h["content-security-policy"].startswith("default-src 'self'")
    assert "frame-ancestors 'none'" in h["content-security-policy"]
    assert h["x-content-type-options"] == "nosniff" and h["referrer-policy"] == "same-origin"
    assert h["x-frame-options"] == "DENY"


def test_there_are_no_api_docs(tmp_path, monkeypatch):
    # FastAPI served them outside the token check, with a CDN's scripts
    client, _ = with_token(tmp_path, monkeypatch)
    for path in ("/api/docs", "/api/openapi.json", "/api/redoc", "/api/docs/oauth2-redirect"):
        assert client.get(path).status_code == 404, path


# --- command names --------------------------------------------------------------


@pytest.mark.parametrize("name", ["this.x", "antigravity.x", "lux_pipeline.cli.counts", "os.path",
                                  "_handler", "nope"])
def test_only_listed_commands_are_looked_up(web, monkeypatch, name):
    # a dotted name is an extension command to `lux`, imported as it stands
    client, _ = web
    started = []
    monkeypatch.setattr(client.app.state.runner, "start", lambda *a, **k: started.append(a))
    before = set(sys.modules)
    assert client.get(f"/api/commands/{name}").status_code == 400
    assert client.get(f"/commands/{name}").status_code == 400
    assert client.post(f"/commands/{name}", data={"_csrf": csrf(client)}).status_code == 400
    assert client.post(f"/api/commands/{name}/preview",
                       data={"_csrf": csrf(client)}).status_code == 400
    assert set(sys.modules) - before == set() and started == []
    assert client.get("/api/commands/counts").status_code == 200


# --- requests from other sites' pages (Fetch Metadata) --------------------------

IMAGE = {"sec-fetch-site": "cross-site", "sec-fetch-mode": "no-cors", "sec-fetch-dest": "image"}
FETCH = {"sec-fetch-site": "cross-site", "sec-fetch-mode": "cors", "sec-fetch-dest": "empty"}
LINK = {"sec-fetch-site": "cross-site", "sec-fetch-mode": "navigate", "sec-fetch-dest": "document"}
OWN_LINK = {**LINK, "sec-fetch-site": "same-origin"}
OWN_FETCH = {**FETCH, "sec-fetch-site": "same-origin"}
TYPED = {**LINK, "sec-fetch-site": "none"}


@pytest.mark.parametrize("site", ["cross-site", "same-site"])
@pytest.mark.parametrize("path", ["/", "/api/dashboard", "/static/app.js", "/healthz", "/config",
                                  "/records/browse?source=aat"])
def test_another_sites_page_cannot_load_anything(web, site, path):
    client, _ = web
    for h in (IMAGE, FETCH, {**LINK, "sec-fetch-dest": "iframe"}):
        assert client.get(path, headers={**h, "sec-fetch-site": site}).status_code == 403, (h, path)


def test_another_sites_form_cannot_post(web, monkeypatch):
    client, _ = web
    started = []
    monkeypatch.setattr(client.app.state.runner, "start", lambda *a, **k: started.append(a))
    r = client.post("/commands/counts", headers={**LINK, "sec-fetch-dest": "document"},
                    data={"_csrf": csrf(client)})
    assert r.status_code == 403 and started == []


def test_links_in_and_this_servers_own_requests_are_answered(web):
    client, _ = web
    for h in (LINK, OWN_LINK, OWN_FETCH, TYPED, {}):
        assert client.get("/", headers=h).status_code == 200, h
        assert client.get("/records/show?q=aat:300033618", headers=h).status_code == 200, h
    assert client.get("/api/dashboard", headers=OWN_FETCH).status_code == 200


STARTS_WORK = ["/counts?go=1", "/api/counts", "/api/counts/events", "/maintenance?bloat=1",
               "/api/maintenance/bloat", "/api/maintenance/bloat/events",
               "/graph?a=aat:300033618&b=wd:Q1", "/api/graph?a=aat:300033618&b=wd:Q1",
               "/records/show?q=aat:300033618&mapped=1",
               "/api/records/journey?q=aat:300033618&mapped=1",
               "/records/browse?source=aat&type=Type", "/api/records/sample?source=aat&type=Type",
               "/commands/build?preview_steps=1"]


@pytest.mark.parametrize("path", STARTS_WORK)
def test_a_link_from_another_site_starts_no_work(web, monkeypatch, path):
    client, state = web
    from lux_pipeline.web import runner
    monkeypatch.setattr(runner.Runner, "dry_run", lambda self, tail: pytest.fail("dry run"))
    for name in ("build_all", "source", "idmap"):
        monkeypatch.setattr(state, name, lambda *a, **k: pytest.fail(f"{name} called"))
    r = client.get(path, headers=LINK)
    assert r.status_code == 403, path
    assert read_actions(state) == []
    if path.startswith("/api/"):
        assert r.json()["error"] == "This link came from another site, so it was not run."
    else:
        # one click on this server's page runs it
        assert "This link came from another site" in r.text
        assert f'<a href="{html.escape(path)}">Run it</a>' in r.text


def test_the_same_pages_without_starting_anything_follow_links(web):
    client, _ = web
    for path in ("/counts", "/maintenance", "/graph", "/records/browse?source=aat",
                 "/commands/build", "/api/records/sample?source=aat"):
        assert client.get(path, headers=LINK).status_code == 200, path


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
