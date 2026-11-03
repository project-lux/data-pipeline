"""The command pages (W10): the catalogue, a form per command from its
real options, the command line it makes as it is filled in, starting a
run (with the typed confirmation and the build guard), the build page,
cancelling, "run again", and the maintenance page's launcher links.

Starting is the runner's (tests/test_web_runner.py); here it is recorded
rather than run, except where a test says so."""

import json
import re
import sys
import time
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit

import pytest

pytest.importorskip("fastapi")

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

from starlette.datastructures import FormData, QueryParams  # noqa: E402
from web_client import csrf, make_client  # noqa: E402
from webui_fixtures import write_fake_run  # noqa: E402

from lux_pipeline.cli.help import list_commands  # noqa: E402
from lux_pipeline.cli.introspect import (HIDDEN, all_command_specs, argv_from_values,  # noqa: E402
                                         command_spec)
from lux_pipeline.progress import read_run  # noqa: E402
from lux_pipeline.web.api.commands import build_values, form_values, run_again_url  # noqa: E402
from lux_pipeline.web.runner import RunHandle  # noqa: E402


@pytest.fixture
def web(tmp_path, monkeypatch):
    client, state = make_client(tmp_path, monkeypatch)
    return client, state


@pytest.fixture
def started(web, monkeypatch):
    """The runner's start, recorded: the run gets a record (so its page
    exists) and nothing is started."""
    client, state = web
    calls = []
    runner = client.app.state.runner

    def start(command, tail, *, force=False, guarded=None):
        spec = command_spec(command)
        runner.check(command, spec, force, guarded)
        prefix = runner.new_prefix(state.configs(), command)
        from lux_pipeline.progress import RunRecord
        RunRecord(prefix, command, ["lux", command, *tail], extra={"web": True}).start()
        calls.append({"command": command, "tail": list(tail), "force": force, "guarded": guarded,
                      "prefix": prefix})
        return RunHandle(prefix, command, ["lux", command, *tail], 0, time.time(), f"{prefix}.out")

    monkeypatch.setattr(runner, "start", start)
    return calls


def post(client, url, data):
    return client.post(url, data={"_csrf": csrf(client), **data}, follow_redirects=False)


def names_in(html):
    return set(re.findall(r'name="([^"]+)"', html))


# --- the catalogue -----------------------------------------------------------


def test_the_catalogue_has_every_command_in_its_group(web):
    client, _ = web
    data = client.get("/api/commands").json()
    listed = [c["name"] for g in data for c in g["commands"]]
    assert sorted(listed) == sorted(n for n, _ in list_commands())
    html = client.get("/commands").text
    for g in data:
        assert g["label"] in html
    assert 'href="/commands/reconcile"' in html and 'href="/commands/build"' in html
    row = re.search(r'href="/commands/serve".*?</tr>', html, re.S).group(0)
    assert "not run from here" in row
    assert client.get("/api/commands/merge").json()["name"] == "merge"


def test_the_catalogue_says_what_is_running(web):
    client, state = web
    write_fake_run(state.configs().log_dir, "merge", 1, updated=time.time())
    html = client.get("/commands").text
    assert "Running now" in html and 'href="/runs/merge-2026-11-02-141500"' in html


# --- forms -------------------------------------------------------------------


def test_the_reconcile_form(web):
    client, _ = web
    html = client.get("/commands/reconcile").text
    names = names_in(html)
    assert {"recid", "no_refs", "source", "max_workers", "max_records", "engine", "log"} <= names
    assert not names & {"my_worker", "no_ui", "prefix", "debug", "env", "help"}
    assert re.search(r'type="checkbox" name="no_refs"', html)
    assert re.search(r'<select id="f-engine" name="engine">', html)
    assert re.search(r'type="number" name="max_workers"', html)
    assert 'list="source-names"' in html and '<option value="ycba">' in html
    assert 'data-preview="/api/commands/reconcile/preview"' in html


def test_every_kind_of_option_has_its_input(web):
    client, _ = web
    html = client.get("/commands/timing-report").text
    assert len(re.findall(r'type="text" name="compare"', html)) == 2       # two values
    assert 'name="profile__bare"' in html                                  # an optional value
    html = client.get("/commands/validate").text
    assert re.search(r'<textarea id="f-ignore" name="ignore"', html)       # repeated
    html = client.get("/commands/show").text
    assert 'name="ref"' in html                                            # a positional


def test_every_runnable_command_has_a_form(web):
    client, _ = web
    for spec in all_command_specs():
        r = client.get(f"/commands/{spec['name']}")
        assert r.status_code == 200, spec["name"]
        if spec["runnable"]:
            assert f'action="/commands/{spec["name"]}"' in r.text
        else:
            assert "is not run from the web UI" in r.text and "<form method=\"post\"" not in r.text


def test_a_form_reads_back_as_the_values_the_command_line_has(tmp_path):
    """Every option of every command: a form filled in as a run page's
    "run again" link fills it gives the same command line."""
    from test_cli_introspect import sample
    for spec in all_command_specs():
        if not spec["runnable"] or spec["name"] == "build":
            continue
        for o in spec["options"]:
            if o["dest"] in HIDDEN:
                continue
            required = {r["dest"]: sample(r, tmp_path) for r in spec["options"]
                        if r["required"] and r["dest"] != o["dest"]}
            values = {**required, o["dest"]: sample(o, tmp_path)}
            argv = argv_from_values(spec, values)
            url = run_again_url(spec["name"], ["lux", *argv, "--no-ui", "--prefix", "/x/p"])
            query = QueryParams(urlsplit(url).query)
            assert url, (spec["name"], o["dest"], argv)
            assert argv_from_values(spec, form_values(spec, query)) == argv, (spec["name"], o["dest"])


# --- the command line as it is typed in --------------------------------------


def test_the_preview_is_the_command_line(web):
    client, _ = web
    r = client.post("/api/commands/reconcile/preview", headers={"X-CSRF-Token": csrf(client)},
                    data={"source": "ycba", "max_workers": "4", "recid": "obj/1", "no_refs": "1"})
    assert r.json() == {"command_line": "lux reconcile --source ycba --max-workers 4 --recid obj/1 "
                                        "--no-refs --no-ui --prefix <log_dir>/reconcile-<timestamp>",
                        "confirm": None, "error": None}
    r = client.post("/api/commands/reconcile/preview", headers={"X-CSRF-Token": csrf(client)},
                    data={"max_workers": "four"})
    assert "--max-workers" in r.json()["error"] and r.json()["command_line"] is None
    r = client.post("/api/commands/clear/preview", headers={"X-CSRF-Token": csrf(client)},
                    data={"source": "ycba"})
    assert r.json()["confirm"].startswith("deletes")


def test_the_json_preview(web):
    """The W02 example; a value for an option the command lacks is a 400."""
    client, _ = web
    r = client.post("/api/commands/reconcile/preview", headers={"X-CSRF-Token": csrf(client)},
                    json={"values": {"source": "ycba", "max_workers": "4", "recid": "obj/1",
                                     "no_refs": "on"}})
    assert r.json()["argv"] == ["reconcile", "--source", "ycba", "--max-workers", "4",
                                "--recid", "obj/1", "--no-refs"]
    r = client.post("/api/commands/reconcile/preview", headers={"X-CSRF-Token": csrf(client)},
                    json={"values": {"nonsense": "1"}})
    assert r.status_code == 400 and "no option 'nonsense'" in r.json()["error"]


def test_the_preview_needs_the_csrf_token(web):
    client, _ = web
    r = client.post("/api/commands/reconcile/preview", data={"source": "ycba"})
    assert r.status_code == 403


# --- starting ----------------------------------------------------------------


def test_a_destructive_run_needs_its_name_typed(web, started):
    client, _ = web
    r = post(client, "/commands/clear", {"source": "ycba"})
    assert r.status_code == 400 and "deletes the selected caches" in r.text and "type clear" in r.text
    r = post(client, "/commands/clear", {"source": "ycba", "confirm": "yes"})
    assert r.status_code == 400 and not started
    r = post(client, "/commands/clear", {"source": "ycba", "force": "1", "confirm": "clear"})
    assert r.status_code == 303
    assert started[0]["tail"] == ["--source", "ycba", "--force"] and started[0]["force"] is True
    assert started[0]["guarded"] is True
    assert r.headers["location"] == "/runs/" + Path(started[0]["prefix"]).name
    assert client.get(r.headers["location"]).status_code == 200


def test_a_plain_run_starts_at_once(web, started):
    client, _ = web
    r = post(client, "/commands/reconcile", {"source": "ycba", "no_refs": "1"})
    assert r.status_code == 303
    assert started[0]["command"] == "reconcile" and started[0]["tail"] == ["--source", "ycba", "--no-refs"]
    assert started[0]["guarded"] is False


def test_a_bad_value_shows_the_form_again(web, started):
    client, _ = web
    r = post(client, "/commands/reconcile", {"max_workers": "many"})
    # the form comes back as it was sent, to be put right
    assert r.status_code == 400 and "is not an integer" in r.text and 'value="many"' in r.text
    assert not started
    r = post(client, "/commands/serve", {})
    assert r.status_code == 400 and "not run from the web UI" in r.text


def test_refused_while_a_build_runs(web, started):
    client, state = web
    prefix = write_fake_run(state.configs().log_dir, "merge", 2, updated=time.time())
    r = post(client, "/commands/reconcile", {"source": "ycba"})
    assert r.status_code == 409 and Path(prefix).name in r.text and not started
    # identify has --force: ticking it passes it and lets the run through
    r = post(client, "/commands/identify", {"force": "1", "confirm": "identify"})
    assert r.status_code == 303 and started[0]["tail"] == ["--force"]


def test_a_real_run_from_the_form(web):
    """No stubs: lux list started from its form, its record and output in
    log_dir, the run page showing it finished."""
    client, state = web
    r = post(client, "/commands/list", {"maps": "1"})
    assert r.status_code == 303
    rel = r.headers["location"].removeprefix("/runs/")
    prefix = str(Path(state.configs().log_dir) / rel)
    end = time.time() + 60
    while (read_run(prefix) or {}).get("exit") is None and time.time() < end:
        time.sleep(0.1)
    rec = read_run(prefix)
    assert rec["exit"] == 0 and rec["web"] is True
    assert "idmap" in Path(f"{prefix}.out").read_text()
    html = client.get(r.headers["location"]).text
    assert "finished" in html and "Run again" in html and 'href="/commands/list?maps=1"' in html


# --- the build page ----------------------------------------------------------


def test_the_build_page(web, monkeypatch):
    client, _ = web
    html = client.get("/commands/build").text
    for p in ("sheets", "reconcile", "merge", "portal", "export"):
        assert f'name="phase" value="{p}" checked' in html
    assert 'name="clear"' in html and 'name="start"' in html and 'name="stop"' in html
    assert 'name="skip"' not in html and 'name="dry_run"' not in html
    seen = []
    monkeypatch.setattr(client.app.state.runner, "dry_run",
                        lambda tail: seen.append(tail) or ["[1] lux sheets", "[2] lux merge", "[3] lux export"])
    html = client.get("/commands/build?phases_shown=1&phase=merge&phase=export&preview_steps=1").text
    assert "<code>[2] lux merge</code>" in html and seen == [["--skip", "sheets,reconcile,portal"]]
    assert 'value="reconcile" checked' not in html and 'value="merge" checked' in html
    r = client.post("/api/commands/build/dry-run", headers={"X-CSRF-Token": csrf(client)},
                    data={"phases_shown": "1", "phase": ["reconcile", "merge"]})
    assert r.json()["steps"][0] == "[1] lux sheets"
    assert seen[-1] == ["--skip", "sheets,portal,export"]


def test_starting_a_build(web, started):
    client, _ = web
    form = {"phases_shown": "1", "phase": ["reconcile", "merge", "portal", "export"], "start": "merge"}
    r = post(client, "/commands/build", form)
    assert r.status_code == 303
    assert started[0]["command"] == "build" and started[0]["tail"] == ["--from", "merge", "--skip", "sheets"]
    r = post(client, "/commands/build", {**form, "clear": "1"})
    assert r.status_code == 400 and "type build to confirm" in r.text and len(started) == 1
    r = post(client, "/commands/build", {**form, "clear": "1", "confirm": "build"})
    assert r.status_code == 303 and started[1]["tail"][0] == "--clear"


def test_build_values_from_the_phase_boxes():
    form = FormData([("phases_shown", "1"), ("phase", "reconcile"), ("phase", "merge"),
                     ("skip", "ignored"), ("dry_run", "1")])
    assert build_values(form) == {"skip": "sheets,portal,export"}
    every = FormData([("phases_shown", "1")] + [("phase", p) for p in
                                                ("sheets", "reconcile", "merge", "portal", "export")])
    assert build_values(every) == {}


def test_run_again_for_a_build_ticks_its_phases():
    url = run_again_url("build", ["lux", "build", "--skip", "sheets,portal", "--source", "ycba",
                                  "--prefix", "/l/b"])
    query = dict(parse_qsl(urlsplit(url).query))
    assert url.startswith("/commands/build?") and query["source"] == "ycba"
    assert [v for k, v in parse_qsl(urlsplit(url).query) if k == "phase"] == ["reconcile", "merge", "export"]
    assert run_again_url("serve", ["lux", "serve"]) is None


# --- cancelling and run pages ------------------------------------------------


def test_cancel_needs_the_word_typed(web, monkeypatch):
    client, state = web
    prefix = write_fake_run(state.configs().log_dir, "merge", 1, updated=time.time(),
                            extra={"web": True})
    runner = client.app.state.runner
    cancelled = []
    monkeypatch.setattr(runner, "cancel", lambda p, grace=10.0: cancelled.append(p) or True)
    rel = Path(prefix).name
    html = client.get(f"/runs/{rel}").text
    assert f'action="/runs/{rel}/cancel"' in html
    r = post(client, f"/runs/{rel}/cancel", {"confirm": "stop"})
    assert r.status_code == 303 and not cancelled
    assert "Not cancelled" in client.get(r.headers["location"]).text
    r = post(client, f"/runs/{rel}/cancel", {"confirm": "cancel"})
    assert r.status_code == 303 and cancelled == [prefix]
    assert "Cancelling" in client.get(r.headers["location"]).text


def test_a_shell_run_cannot_be_cancelled_here(web):
    client, state = web
    prefix = write_fake_run(state.configs().log_dir, "merge", 1, updated=time.time())
    rel = Path(prefix).name
    html = client.get(f"/runs/{rel}").text
    assert "/cancel" not in html and "started elsewhere" in html
    r = post(client, f"/runs/{rel}/cancel", {"confirm": "cancel"})
    assert "started elsewhere" in client.get(r.headers["location"]).text


def test_a_run_page_links_to_run_it_again(web):
    client, state = web
    prefix = write_fake_run(state.configs().log_dir, "reconcile", 1, exit=0)
    rec = read_run(prefix)
    rec["argv"] = ["lux", "reconcile", "--source", "ycba", "--no-refs", "--no-ui", "--prefix", prefix]
    Path(f"{prefix}.run").write_text(json.dumps(rec))
    html = client.get(f"/runs/{Path(prefix).name}").text
    m = re.search(r'href="(/commands/reconcile\?[^"]+)">Run again', html)
    assert m
    form = client.get(m.group(1).replace("&amp;", "&")).text
    assert 'name="source" list="source-names" value="ycba"' in form
    assert re.search(r'name="no_refs" value="1" checked', form)


# --- elsewhere ---------------------------------------------------------------


def test_the_maintenance_commands_open_the_launcher(web):
    client, _ = web
    html = client.get("/maintenance").text
    assert 'href="/commands/pg-maintain?vacuum=1&amp;source=all"' in html
    form = client.get("/commands/pg-maintain?vacuum=1&source=all").text
    assert re.search(r'name="vacuum" value="1" checked', form)


def test_the_catalogue_without_an_installation(tmp_path, monkeypatch):
    client, _ = make_client(tmp_path, monkeypatch, root=None)
    assert client.get("/commands").status_code == 200
    assert client.get("/commands/reconcile").status_code == 200
