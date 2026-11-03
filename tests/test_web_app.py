"""Web UI item W06: the application itself: /healthz, the error pages, the
navigation, static files, CSRF and the token. On the W00 installation;
no database, no browser."""

import sys
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.web.app import AREAS, create_app
from lux_pipeline.web.settings import Settings
from web_client import csrf, make_client


def test_healthz_answers_without_a_configuration(tmp_path, monkeypatch):
    client, _ = make_client(tmp_path, monkeypatch, root=None)
    r = client.get("/healthz")
    assert r.status_code == 200 and r.json()["ok"] is True and r.json()["basepath"] is None


def test_without_an_installation_pages_say_what_to_do(tmp_path, monkeypatch):
    client, _ = make_client(tmp_path, monkeypatch, root=None)
    r = client.get("/")
    assert r.status_code == 503 and "LUX_BASEPATH is not set" in r.text and "lux initialize" in r.text
    r = client.get("/api/dashboard")
    assert r.status_code == 503 and r.json()["hint"].startswith("Use 'lux initialize")


def test_a_broken_configuration_is_a_page_not_a_crash(tmp_path, monkeypatch):
    client, state = make_client(tmp_path, monkeypatch, seed=False)
    (Path(state.configs().config_dir) / "config_cache" / "aat.json").write_text('{"no type": 1}')
    state.reset()
    r = client.get("/")
    assert r.status_code == 503 and "does not build" in r.text


def test_every_area_is_in_the_navigation(tmp_path, monkeypatch):
    client, _ = make_client(tmp_path, monkeypatch, seed=False)
    html = client.get("/health").text
    assert '<a href="/">Dashboard</a>' in html
    assert '<a href="/health" class="active" aria-current="page">Health</a>' in html
    assert len(client.app.state.nav) == len(AREAS)


def test_unknown_paths_and_static_files(tmp_path, monkeypatch):
    client, _ = make_client(tmp_path, monkeypatch, seed=False)
    assert client.get("/nope").status_code == 404
    assert client.get("/api/nope").json()["status"] == 404
    for name, kind in (("lux.css", "text/css"), ("app.js", "javascript"), ("sse.js", "javascript"),
                       ("health.js", "javascript"), ("favicon.svg", "image/svg")):
        r = client.get(f"/static/{name}")
        assert r.status_code == 200 and kind in r.headers["content-type"], name


def test_every_change_needs_the_csrf_token(tmp_path, monkeypatch):
    client, _ = make_client(tmp_path, monkeypatch, seed=False)
    posts = [r.path for r in client.app.routes if "POST" in getattr(r, "methods", ())]
    assert posts, "there are POST routes to check"
    for path in posts:
        assert client.post(path).status_code == 403, path
        assert client.post(path, data={"_csrf": "wrong"}).status_code == 403, path
    token = csrf(client)
    assert f'<meta name="csrf-token" content="{token}">' in client.get("/health").text
    assert client.post("/api/health/run", headers={"X-CSRF-Token": token}).status_code == 200


def test_a_token_keeps_others_out(tmp_path, monkeypatch):
    client, _ = make_client(tmp_path, monkeypatch, seed=False,
                            settings=Settings(open_browser=False, token="s3cret"))
    assert client.get("/").status_code == 401
    assert client.get("/healthz").status_code == 200
    assert client.get("/", headers={"Authorization": "Bearer s3cret"}).status_code == 200
    r = client.get("/?token=s3cret")
    assert r.status_code == 200 and client.cookies.get("lux_token") == "s3cret"
    assert client.get("/api/dashboard").status_code == 200       # the cookie now
    client.cookies.clear()
    assert client.get("/", headers={"Authorization": "Bearer wrong"}).status_code == 401


def test_templates_escape_and_load_no_inline_script(tmp_path, monkeypatch):
    client, state = make_client(tmp_path, monkeypatch, seed=False)
    state.health["results"] = [{"label": "<b>x</b>", "state": "FAIL",
                                "detail": "<script>alert(1)</script>", "required": True}]
    html = client.get("/health").text
    assert "<script>alert(1)</script>" not in html and "&lt;script&gt;alert(1)&lt;/script&gt;" in html
    import re
    for tag in re.findall(r"<script[^>]*>", html):
        assert 'src="/static/' in tag or 'type="application/json"' in tag, tag
    assert " onclick=" not in html and " style=" not in html


def test_settings_from_the_configuration_and_options(tmp_path, monkeypatch):
    class Cfgs:
        web = {"host": "127.0.0.1", "port": 9000}

    assert Settings.from_config(Cfgs()).port == 9000
    assert Settings.from_config(Cfgs(), port=9100).port == 9100
    assert Settings.from_config(None).port == 8321
    with pytest.raises(ValueError, match="give --token"):
        Settings(host="0.0.0.0", token=None).check()
    Settings(host="0.0.0.0", token="t").check()
    monkeypatch.setenv("LUX_WEB_TOKEN", "from-env")
    assert Settings().token == "from-env"
    assert Settings(host="::1").url == "http://[::1]:8321/"
