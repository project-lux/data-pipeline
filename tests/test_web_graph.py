"""Web UI item W12: the identity graph page, `lux viz` in the browser.
The replay (`records.identity_graph`) is patched to a four-node graph
except where a test says otherwise."""

import html
import re
import sys
from pathlib import Path
from urllib.parse import urlencode

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("networkx")
sys.path.insert(0, str(Path(__file__).parent))

from web_client import make_client  # noqa: E402
from webui_fixtures import make_configs, seed_records  # noqa: E402

from lux_pipeline import records  # noqa: E402
from lux_pipeline.web.api import graph as graph_api  # noqa: E402

A = "http://vocab.getty.edu/aat/300033618"
B = "http://www.wikidata.org/entity/Q3305213"


def four(cfgs, a, b):
    """What viz.graph_data returns: aat:0 -> wd:1 -> lcsh:2 -> ycba:3, and
    aat:0 -> lcsh:2 by name; the path aat:0, lcsh:2 ... wd:1."""
    nodes = [("aat:0", A, "aat"), ("wd:1", B, "wd"),
             ("lcsh:2", "http://id.loc.gov/authorities/subjects/sh1", "lcsh"),
             ("ycba:3", "https://ycba.example/obj/1", "ycba")]
    return {"yuid": "https://lux.example/data/concept/u1", "from": a, "to": b, "graph": {},
            "labels": {u: n for n, u, _ in nodes},
            "nodes": [{"id": n, "uri": u, "source": s} for n, u, s in nodes],
            "edges": [{"from": "aat:0", "to": "wd:1", "kind": "eq"},
                      {"from": "wd:1", "to": "lcsh:2", "kind": "uri"},
                      {"from": "lcsh:2", "to": "ycba:3", "kind": "eq"},
                      {"from": "aat:0", "to": "lcsh:2", "kind": "nm"}],
            "ends": ["aat:0", "wd:1"], "path": ["aat:0", "wd:1"]}


@pytest.fixture
def web(tmp_path, monkeypatch):
    client, state = make_client(tmp_path, monkeypatch, seed=False)
    calls = []
    monkeypatch.setattr(records, "identity_graph", lambda c, a, b: calls.append((a, b)) or four(c, a, b))
    monkeypatch.setattr(state, "build_all", lambda: state.configs())
    return client, state, calls


def get_graph(client, a=A, b=B, api=False):
    return client.get(("/api/graph?" if api else "/graph?") + urlencode({"a": a, "b": b}))


def test_the_drawing_as_data(web):
    client, _, calls = web
    d = get_graph(client, api=True).json()
    assert calls == [(A, B)]
    assert len(d["nodes"]) == 4 and len(d["edges"]) == 4
    for n in d["nodes"]:
        assert 40 <= n["x"] <= 960 and 40 <= n["y"] <= 660, n
    on = {n["id"] for n in d["nodes"] if n["on_path"]}
    assert on == {"aat:0", "wd:1"}
    assert {n["id"] for n in d["nodes"] if n["is_endpoint"]} == {"aat:0", "wd:1"}
    assert [(e["from"], e["to"]) for e in d["edges"] if e["on_path"]] == [("aat:0", "wd:1")]
    assert d["path"] == ["aat:0", "wd:1"] and d["notes"] == []
    # the same graph is drawn the same way every time
    again = get_graph(client, api=True).json()
    assert [(n["x"], n["y"]) for n in again["nodes"]] == [(n["x"], n["y"]) for n in d["nodes"]]
    # a source keeps its colour
    assert graph_api.colour("aat") == graph_api.colour("aat") and 0 <= graph_api.colour("wd") < 10


def test_the_page(web):
    client, _, _ = web
    r = get_graph(client)
    assert r.status_code == 200
    page = r.text
    assert len(re.findall(r"<circle ", page)) == 4 and len(re.findall(r"<line ", page)) == 4
    assert page.count('class="edge kind-nm') == 1 and "kind-eq on-path" in page
    rows = re.findall(r"<tr(?: class=\"on-path\")?><td><code>([^<]+)</code>", page)
    assert rows == ["aat:0", "wd:1", "lcsh:2", "ycba:3"]
    assert f'href="/records/show?{urlencode({"q": B})}"' in page
    assert f"<title>{B}</title>" in page
    assert "<code>aat:0</code> → <code>wd:1</code>" in page
    assert " style=" not in page and "<script>" not in page


def test_one_uri_is_the_form_only(web):
    client, _, calls = web
    r = client.get("/graph?" + urlencode({"a": A}))
    assert r.status_code == 200 and "<svg" not in r.text and 'name="b"' in r.text
    assert f'value="{A}"' in r.text and calls == []
    assert client.get("/graph").status_code == 200


def test_without_networkx(web, monkeypatch):
    client, _, calls = web
    monkeypatch.setitem(sys.modules, "networkx", None)
    r = get_graph(client)
    assert r.status_code == 409 and "pip install -e .[ui-extras]" in html.unescape(r.text)
    r = get_graph(client, api=True)
    assert r.status_code == 409 and "ui-extras" in r.json()["error"] and calls == []


def test_not_one_identity(web, monkeypatch):
    client, _, _ = web

    def apart(cfgs, a, b):
        raise ValueError(f"{a} is u1 but {b} is u2: they are not the same identity, so nothing connects them")

    monkeypatch.setattr(records, "identity_graph", apart)
    r = get_graph(client)
    assert r.status_code == 400 and "not the same identity" in r.text and "<svg" not in r.text
    assert get_graph(client, api=True).status_code == 400


def test_too_slow(web, monkeypatch):
    client, _, _ = web
    import time
    monkeypatch.setattr(records, "identity_graph", lambda c, a, b: time.sleep(1) or four(c, a, b))
    monkeypatch.setattr(graph_api, "TIMEOUT", 0.1)
    r = get_graph(client)
    assert r.status_code == 504 and "try lux viz at a shell" in r.text
    assert get_graph(client, api=True).status_code == 504
    time.sleep(1.2)                          # let the replays finish before the next test


def test_notes_for_what_the_graph_lacks(web, monkeypatch):
    client, _, _ = web
    lonely = lambda c, a, b: dict(four(c, a, b), ends=["aat:0", None], path=None)   # noqa: E731
    monkeypatch.setattr(records, "identity_graph", lonely)
    d = get_graph(client, api=True).json()
    assert d["notes"] == [f"{B} is not in the graph."] and not any(n["on_path"] for n in d["nodes"])
    empty = lambda c, a, b: dict(four(c, a, b), nodes=[], edges=[], ends=[None, None], path=None)  # noqa: E731
    monkeypatch.setattr(records, "identity_graph", empty)
    d = get_graph(client, api=True).json()
    assert d["notes"] == ["Reconciling the identity's records recorded no connections."]
    assert "recorded no connections" in get_graph(client).text


def test_the_source_reconcilers_debug_is_put_back(web, monkeypatch):
    """viz.enable_debug turns debug on in the shared source reconcilers;
    after a replay they are as they were."""
    client, state, _ = web
    cfgs = state.configs()

    class R:
        debug = False
        debug_graph = {}

    r = R()
    monkeypatch.setitem(cfgs.external["aat"], "reconciler", r)

    def replaying(c, a, b):
        r.debug, r.debug_graph = True, {"x": [("y", "eq")]}
        return four(c, a, b)

    monkeypatch.setattr(records, "identity_graph", replaying)
    assert get_graph(client, api=True).status_code == 200
    assert r.debug is False and r.debug_graph == {}


def test_the_real_replay_on_the_installation(tmp_path, monkeypatch):
    """No stubs (but typed keys: the installation cannot fetch a record to
    find its type): two URIs not of one identity say so; a URI not in the
    idmap says so."""
    client, state = make_client(tmp_path, monkeypatch, seed=False)
    s = seed_records(make_configs(Path(state.configs().config_dir), instantiate=True))
    state.reset()
    aat, ycba = s["aat"]["uri"] + "##quaType", s["ycba"]["uri"] + "##quaHumanMadeObject"
    r = get_graph(client, aat, ycba, api=True)
    assert r.status_code == 400 and "not the same identity" in r.json()["error"]
    r = get_graph(client, aat, "http://vocab.getty.edu/aat/300000000##quaType", api=True)
    assert r.status_code == 400 and "is not in the idmap" in r.json()["error"]


def test_the_records_page_links_each_other_member(tmp_path, monkeypatch):
    client, state = make_client(tmp_path, monkeypatch, seed=False)
    s = seed_records(make_configs(Path(state.configs().config_dir), instantiate=True))
    state.reset()
    idmap = state.idmap()
    other = "http://www.wikidata.org/entity/Q3305213##quaType"
    idmap.data[other] = s["aat"]["yuid"]          # a second member, both ways
    idmap.data[s["aat"]["yuid"]].append(other)
    page = client.get("/records/show?" + urlencode({"q": s["aat"]["uri"]})).text
    me = s["aat"]["uri"] + "##quaType"
    assert f'href="/graph?{urlencode({"a": me, "b": other})}"' in page.replace("&amp;", "&")
    assert f'href="/graph?{urlencode({"a": me, "b": me})}"' not in page.replace("&amp;", "&")


def test_the_page_is_in_the_navigation(web):
    client, _, _ = web
    assert 'href="/graph"' in client.get("/").text
