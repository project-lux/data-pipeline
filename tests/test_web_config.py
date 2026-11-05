"""Web UI item W07: the configuration pages. On the W00 installation with
a sub-config store (as pipeline-configs has) and a hand-formatted
record; files are written as `lux config` writes them."""

import json
import re
import sys
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
sys.path.insert(0, str(Path(__file__).parent.parent))

from web_client import csrf, make_client
from webui_fixtures import add_subconfig_store, write_fake_run

HAND = ('{\n    "name": "_caches",\n    "type": "caches",\n    "base_dir": BASE,\n\n'
        '    "port": 5432,\n    "overwrite": true\n}\n')


@pytest.fixture
def web(tmp_path, monkeypatch):
    client, state = make_client(tmp_path, monkeypatch, seed=False)
    root = Path(state.configs().config_dir)
    add_subconfig_store(root, "sources", move=("aat",))
    caches = json.loads((root / "config_cache" / "caches.json").read_text())
    (root / "config_cache" / "caches.json").write_text(HAND.replace("BASE", json.dumps(caches["base_dir"])))
    state.reset()
    return client, state, root


def aat(root):
    return root / "subs" / "sources_cache" / "aat.json"


def form(client, **values):
    return {"_csrf": csrf(client), **values}


# --- the list and a record --------------------------------------------------------------


def test_the_index_lists_every_record_by_type_with_its_file(web):
    client, state, root = web
    d = client.get("/api/config").json()
    rows = {r["key"]: (g["type"], r) for g in d["groups"] for r in g["records"]}
    assert rows["aat"][0] == "external" and rows["aat"][1]["store"] == "sources"
    assert rows["aat"][1]["path"] == str(aat(root))
    assert rows["map_idmap"][1]["summary"] == "IdMap"
    assert rows["caches"][1]["summary"] == "local socket, database (user)"
    assert d["count"] == len(state.configs().record_origins) and d["stores"] == ["sources"]
    assert [g["type"] for g in d["groups"]][:2] == ["base", "caches"]
    assert {s["source"] for s in d["sources"]} == {"ycba", "aat", "merged"}
    html = client.get("/config").text
    assert '<a href="/config/aat">aat</a>' in html and str(aat(root)) in html


def test_a_records_data(web):
    client, _, root = web
    d = client.get("/api/config/aat").json()
    assert d["raw"] == json.loads(aat(root).read_text()) and d["text"] == aat(root).read_text()
    f = {x["key"]: x for x in d["fields"]}
    assert f["mapperClass"]["class_state"] == {"state": "ok", "detail": ""}
    assert f["merge_order"]["input"] == "10" and f["matches"]["input"].startswith("[")
    # a class key with a default shows even when absent
    assert f["loaderClass"]["present"] is False and f["loaderClass"]["default"] == "process.base.loader.Loader"
    assert d["problems"] == [] and d["store"] == "sources"
    assert client.get("/api/config/nope").status_code == 404


def test_an_old_class_path_is_shown_as_such(web):
    client, _, root = web
    data = json.loads(aat(root).read_text())
    data["mapperClass"] = "pipeline.sources.authorities.getty.mapper.AatMapper"
    aat(root).write_text(json.dumps(data, indent=2) + "\n")
    f = {x["key"]: x for x in client.get("/api/config/aat").json()["fields"]}
    assert f["mapperClass"]["class_state"]["state"] == "legacy"
    assert "lux_pipeline.sources.getty.mapper.AatMapper" in f["mapperClass"]["class_state"]["detail"]
    page = client.get("/config/aat").text
    assert '<li class="warning">mapperClass uses the old path' in page


def test_paths_are_shown_resolved(web):
    client, state, root = web
    f = {x["key"]: x for x in client.get("/api/config/base").json()["fields"]}
    assert f["log_dir"]["resolved"] == {"path": state.configs().log_dir, "exists": True}


# --- checking and diffing ----------------------------------------------------------------


def test_validate_and_diff(web):
    client, _, root = web
    hdr = {"X-CSRF-Token": csrf(client)}
    good = aat(root).read_text()
    r = client.post("/api/config/aat/validate", json={"text": good}, headers=hdr).json()
    assert (r["json_error"], r["problems"], r["errors"]) == (None, [], 0)
    bad = good.replace("sources.getty.mapper.AatMapper", "sources.getty.mapper.Nope")
    r = client.post("/api/config/aat/validate", json={"text": bad}, headers=hdr).json()
    assert r["errors"] == 1 and "has no Nope" in r["problems"][0]
    r = client.post("/api/config/aat/validate", json={"text": "{nope"}, headers=hdr).json()
    assert r["json_error"].startswith("line 1, column 2")
    diff = client.post("/api/config/aat/diff", json={"text": good.replace(": 10,", ": 11,")},
                       headers=hdr).json()["diff"]
    assert '-  "merge_order": 10,\n+  "merge_order": 11,\n' in diff
    data = json.loads(good)
    data["merge_order"] = 12
    assert '+  "merge_order": 12,' in client.post("/api/config/aat/diff", json={"data": data},
                                                  headers=hdr).json()["diff"]
    assert client.post("/api/config/aat/validate", json={"text": good}).status_code == 403


# --- saving -----------------------------------------------------------------------------


def test_saving_a_field_changes_one_line_and_reloads(web):
    client, state, root = web
    before = aat(root).read_text()
    r = client.post("/config/aat", data=form(client, mode="fields", **{"field:merge_order": "11"}))
    assert r.status_code == 200 and r.url.path == "/config/aat"
    assert f"Wrote {aat(root)}; the configuration was reloaded" in r.text
    assert aat(root).read_text() == before.replace('"merge_order": 10', '"merge_order": 11')
    assert client.get("/api/config/aat").json()["raw"]["merge_order"] == 11
    assert state.configs().external["aat"]["merge_order"] == 11


def test_the_fields_form_as_the_page_sends_it_changes_nothing_else(web):
    client, _, root = web
    before = aat(root).read_text()
    fields = client.get("/api/config/aat").json()["fields"]
    sent = {f"field:{f['key']}": f["input"] for f in fields}
    r = client.post("/config/aat", data=form(client, mode="fields", **sent))
    assert "No change" in r.text and aat(root).read_text() == before


def test_fields_keep_an_empty_string_and_remove_on_request(web):
    client, _, root = web
    caches = root / "config_cache" / "caches.json"
    client.post("/config/caches", data=form(client, mode="fields", **{"field:host": "", "field:port": "5433"}))
    data = json.loads(caches.read_text())
    assert data["port"] == 5433 and "host" not in data            # was absent, left empty: still absent
    assert caches.read_text() == json.dumps(data, indent=2) + "\n"   # written normalized
    client.post("/config/caches", data=form(client, mode="fields", **{"field:dbname": "lux"}))
    assert json.loads(caches.read_text())["dbname"] == "lux"
    client.post("/config/caches", data=form(client, mode="fields",
                                            **{"field:dbname": "lux", "unset:dbname": "1"}))
    assert "dbname" not in json.loads(caches.read_text())


def test_a_bad_field_is_refused_and_kept(web):
    client, _, root = web
    before = aat(root).read_text()
    r = client.post("/config/aat", data=form(client, mode="fields", **{"field:matches": "[broken"}))
    assert r.status_code == 400 and "matches: not JSON" in r.text and "[broken" in r.text
    r = client.post("/config/aat", data=form(client, mode="fields", **{"field:merge_order": "ten"}))
    assert r.status_code == 400 and "is not a whole number" in r.text
    assert aat(root).read_text() == before


def test_saving_the_text_stores_it_normalized(web):
    client, _, root = web
    text = aat(root).read_text().replace('"merge_order": 10', '"merge_order":7')
    r = client.post("/config/aat", data=form(client, mode="text", text=text))
    assert r.status_code == 200 and r.url.params.get("tab") == "text"
    assert aat(root).read_text() == json.dumps(json.loads(text), indent=2) + "\n"


def test_text_that_is_not_json_or_not_valid_is_refused_and_kept(web):
    client, _, root = web
    before = aat(root).read_text()
    r = client.post("/config/aat", data=form(client, mode="text", text='{"name": "aat", oops'))
    assert r.status_code == 400 and "Not JSON: line 1" in r.text and "oops" in r.text
    bad = before.replace('"name": "aat"', '"name": "getty"')
    r = client.post("/config/aat", data=form(client, mode="text", text=bad))
    assert r.status_code == 400 and "a source&#39;s name must be its key" in r.text
    assert "getty" in r.text and aat(root).read_text() == before


def test_saving_while_a_build_runs_needs_force(web):
    client, state, root = web
    write_fake_run(Path(state.configs().log_dir), "reconcile", 1)
    assert "a build is running" in client.get("/config/aat").text
    r = client.post("/config/aat", data=form(client, mode="fields", **{"field:merge_order": "12"}))
    assert r.status_code == 409 and "a build is running" in r.text
    assert 'value="12"' in r.text                                   # what was typed is still there
    r = client.post("/config/aat", data=form(client, mode="fields", force="1", **{"field:merge_order": "12"}))
    assert r.status_code == 200 and json.loads(aat(root).read_text())["merge_order"] == 12


# --- new, delete, reload --------------------------------------------------------------------


def test_new_then_delete(web):
    client, _, root = web
    import html
    page = html.unescape(client.get("/config/_new?type=external&key=ulan").text)
    assert '"name": "ulan"' in page and "sub-config store sources" in page
    rec = {"name": "ulan", "type": "external", "namespace": "http://vocab.getty.edu/ulan/",
           "matches": ["vocab.getty.edu/ulan/"]}
    r = client.post("/config/_new", data=form(client, type="external", key="ulan", store="sources",
                                              text=json.dumps(rec)))
    assert r.status_code == 200 and r.url.path == "/config/ulan" and "Wrote" in r.text
    path = root / "subs" / "sources_cache" / "ulan.json"
    assert path.read_text().startswith('{\n  "name": "ulan"')      # the store's own indent
    assert "ulan" in {r["key"] for g in client.get("/api/config").json()["groups"] for r in g["records"]}
    r = client.post("/config/_new", data=form(client, type="external", key="ulan", text=json.dumps(rec)))
    assert r.status_code == 400 and "already exists" in r.text
    r = client.post("/config/ulan/delete", data=form(client, confirm="nope"))
    assert r.status_code == 400 and path.exists()
    r = client.post("/config/ulan/delete", data=form(client, confirm="ulan"))
    assert r.status_code == 200 and r.url.path == "/config" and "Deleted ulan" in r.text
    assert not path.exists()


def test_a_new_record_with_what_a_person_must_fill_in_left_empty(web):
    client, _, root = web
    r = client.post("/config/_new", data=form(client, type="map", key="map_x",
                                              text='{"name": "x", "type": "map", "storeClass": ""}'))
    assert r.status_code == 400 and "storeClass is empty" in r.text
    assert not (root / "config_cache" / "map_x.json").exists()


def test_reload(web):
    client, state, _ = web
    before = state.configs()
    r = client.post("/config/_reload", data=form(client, back="/config"))
    assert r.status_code == 200 and "Reloaded the configuration:" in r.text
    assert state.configs() is not before
    r = client.post("/config/_reload", data=form(client, back="https://elsewhere.example/"))
    assert r.url.path == "/"                                           # never off this server


def test_every_post_needs_the_token(web):
    client, _, _ = web
    for path in ("/config/aat", "/config/aat/delete", "/config/_new", "/config/_reload"):
        assert client.post(path, data={"mode": "text", "text": "{}"}).status_code == 403, path


def test_the_page_passes_data_to_scripts_safely(web):
    client, _, root = web
    html = client.get("/config/aat").text
    for block in re.findall(r'<script type="application/json"[^>]*>(.*?)</script>', html, re.S):
        assert "&#34;" not in block and json.loads(block) == {"key": "aat"}
    assert "[object Object]" not in html
    for tag in re.findall(r"<script[^>]*>", html):
        assert 'src="/static/' in tag or 'type="application/json"' in tag
    assert " style=" not in html and " onclick=" not in html
