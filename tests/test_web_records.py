"""Web UI item W08: the records pages: look anything up and see where it
is; sample a cache; validate. On the W00 installation with seeded records;
nothing is fetched or stored."""

import html
import json
import re
import sys
from pathlib import Path
from urllib.parse import quote, urlencode

import pytest

pytest.importorskip("fastapi")
sys.path.insert(0, str(Path(__file__).parent.parent))

from web_client import make_client
from webui_fixtures import make_configs, seed_records


@pytest.fixture
def web(tmp_path, monkeypatch):
    client, state = make_client(tmp_path, monkeypatch, seed=False)
    seeded = seed_records(make_configs(Path(state.configs().config_dir), instantiate=True))
    state.reset()
    return client, state, seeded


def show(client, q, **params):
    return client.get("/records/show?" + urlencode({"q": q, **params}))


# --- resolving and the journey ------------------------------------------------------------


def test_resolve(web):
    client, _, s = web
    r = client.get("/api/records/resolve?" + urlencode({"q": s["aat"]["uri"]})).json()
    assert (r["kind"], r["source"], r["identifier"]) == ("uri", "aat", "300033618")
    assert client.get("/api/records/resolve?q=aat:300033618").json()["kind"] == "curie"
    r = client.get("/api/records/resolve?q=nonsense")
    assert r.status_code == 400 and "give the source for a bare identifier" in r.json()["error"]


def test_journey_of_an_external_record(web):
    client, _, s = web
    j = client.get("/api/records/journey?q=aat:300033618").json()
    st = j["stages"]
    assert st["datacache"]["found"] and st["datacache"]["key"] == "300033618"
    assert st["recordcache"]["key"] == "300033618##quaType"
    assert isinstance(st["recordcache"]["meta"]["record_time"], str)       # times as text
    assert st["recordcache"]["size"] > 0 and st["recordcache"]["data"]["type"] == "Type"
    assert st["identity"]["yuid"] == s["aat"]["yuid"]
    assert st["merged"]["found"] and st["merged"]["key"] == s["aat"]["uuid"]
    assert (st["reconciled"]["applies"], st["reconciled"]["found"]) == (True, False)
    assert st["mapped"]["applies"] is False and st["marklogic"]["applies"] is False
    assert "recordcache: found under the typed key 300033618##quaType" in j["notes"]


def test_the_page_for_every_kind_of_reference(web):
    client, _, s = web
    for q, params in ((s["aat"]["uri"], {}), ("aat:300033618", {}), ("object/1", {"source": "ycba"}),
                      (s["ycba"]["uri"] + "##quaHumanMadeObject", {}), (s["ycba"]["uuid"], {})):
        r = show(client, q, **params)
        assert r.status_code == 200, q
        assert html.escape(q) in r.text
    page = show(client, s["ycba"]["uuid"]).text
    assert "A YUID has no datacache" in page and s["ycba"]["yuid"] in page


def test_a_reference_that_is_not_one_stays_on_the_search_page(web):
    client, _, _ = web
    r = show(client, "nope:1")
    assert r.status_code == 400 and "give the source for a bare identifier" in r.text
    assert 'value="nope:1"' in r.text                       # what was typed stays in the box


def test_the_identity_lists_members_as_links(web):
    client, _, s = web
    page = show(client, s["aat"]["uri"]).text
    member = s["aat"]["uri"] + "##quaType"
    assert f'href="/records/show?{urlencode({"q": member})}"' in html.unescape(page).replace("&amp;", "&") \
        or f'/records/show?{urlencode({"q": member})}' in page
    assert "this record" in page


def test_record_data_reaches_scripts_safely(web):
    client, state, s = web
    cfg = state.source("aat")
    cfg["recordcache"]["300033618##quaType"] = {"id": s["aat"]["uri"], "type": "Type",
                                                "_label": "</script><script>alert(1)</script>"}
    page = show(client, "aat:300033618").text
    assert "<script>alert(1)</script>" not in page
    blocks = re.findall(r'<script type="application/json" class="json-data">(.*?)</script>', page, re.S)
    assert blocks and any(json.loads(b).get("_label", "").startswith("</script>") for b in blocks)
    for tag in re.findall(r"<script[^>]*>", page):
        assert 'src="/static/' in tag or 'type="application/json"' in tag
    assert " style=" not in page


def test_mapping_stores_nothing(web, monkeypatch):
    client, state, _ = web
    state.build_all()
    cfg = state.source("ycba")

    def transform(row, rectype, reference=False):
        return {"data": dict(row["data"], _label="mapped now"), "identifier": row["identifier"]}

    monkeypatch.setattr(cfg["mapper"], "transform", transform)
    writes = []
    monkeypatch.setattr(cfg["recordcache"], "set", lambda *a, **k: writes.append(a))
    j = client.get("/api/records/journey?q=ycba:object/1&mapped=1").json()
    assert j["mapped"] is True and j["stages"]["mapped"]["records"][0]["data"]["_label"] == "mapped now"
    assert writes == []
    page = show(client, "ycba:object/1", mapped=1, tab="mapped").text
    assert "mapped now" in page and "Map it now" not in page


def test_build_all_builds_each_source_once(web, monkeypatch):
    _, state, _ = web
    from lux_pipeline.config import Config
    calls = []
    real = Config.instantiate
    monkeypatch.setattr(Config, "instantiate",
                        lambda self, name, which=None: (calls.append(name), real(self, name, which))[1])
    state.build_all()
    state.build_all()
    cfgs = state.configs()
    assert sorted(calls) == sorted([*cfgs.internal, *cfgs.external, *cfgs.results])


# --- raw, validate, browse -------------------------------------------------------------------


def test_raw_and_download(web):
    client, _, s = web
    r = client.get("/records/raw?" + urlencode({"q": "aat:300033618", "stage": "merged", "download": 1}))
    assert r.status_code == 200 and r.json()["id"] == s["aat"]["yuid"]
    assert r.headers["content-disposition"].startswith('attachment; filename="aat_300033618-merged')
    assert client.get("/records/raw?q=aat:300033618&stage=reconciled").status_code == 404
    ident = client.get("/records/raw?q=aat:300033618&stage=identity").json()
    assert ident["members"] == [s["aat"]["uri"] + "##quaType"]


def test_validate_without_schemas_says_so(web):
    client, _, _ = web
    page = show(client, "aat:300033618", validate="recordcache").text
    assert "Cannot validate" in page and "core.json" in page
    r = client.get("/api/records/validate?q=aat:300033618&stage=recordcache")
    assert r.status_code == 409 and "lux initialize" in r.json()["error"]


def test_validate_reports_errors(web, monkeypatch):
    client, state, _ = web

    class Err:
        absolute_path = ["identified_by", 0]
        message = "is not valid"

    monkeypatch.setattr(state.configs().validator, "validate", lambda rec: [Err()])
    page = show(client, "aat:300033618", validate="recordcache").text
    assert "1 validation error(s)" in page and "/identified_by/0 --&gt; is not valid" in page


def test_browse(web):
    client, _, s = web
    d = client.get("/api/records/sample?source=aat&cache=recordcache&type=Type").json()
    assert [k["key"] for k in d["results"]] == ["300033618##quaType"] and d["cost"]
    assert d["results"][0]["source"] == "aat"
    page = client.get("/records/browse?source=aat&cache=recordcache&type=Type").text
    assert "300033618##quaType" in page and "reading every record" in page
    merged = client.get("/api/records/sample?source=merged&cache=recordcache").json()
    assert {k["source"] for k in merged["results"]} == {None}       # a YUID needs no source
    assert client.get("/records/browse?source=aat&cache=nope").status_code == 400
    assert client.get("/records/browse?source=aat&since=yesterday").status_code == 400


def test_recent_lookups_and_links_from_elsewhere(web):
    client, _, _ = web
    show(client, "aat:300033618")
    show(client, "object/1", source="ycba")
    page = client.get("/records").text
    recent = page[page.index("Looked up lately"):]                   # (the introduction has an example)
    assert recent.index("object/1") < recent.index("aat:300033618")  # newest first
    assert '/records/browse?source=aat' in client.get("/").text
    assert '/records/browse?source=aat' in client.get("/config").text
