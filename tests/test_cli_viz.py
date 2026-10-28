"""lux viz and the collector's part of the reconciler's debug graph."""

from argparse import Namespace
from types import SimpleNamespace

import pytest

from lux_pipeline.cli import viz
from lux_pipeline.process.reconciler import Reconciler

pytest.importorskip("networkx")
pytest.importorskip("netext")

NS = {"aat": "http://vocab.getty.edu/aat/", "wd": "http://www.wikidata.org/entity/",
      "ycba": "https://ycba.example/obj/"}


class Acquirer:
    def __init__(self, ns, kind="Type"):
        self.ns = ns
        self.kind = kind
        self.calls = []

    def acquire(self, ident, rectype=None, store=True, **kw):
        self.calls.append((ident, store))
        if ident == "missing":
            return None
        return {"data": {"id": f"{self.ns}{ident}", "type": self.kind}, "identifier": ident,
                "source": "x"}


class Mapper:
    def get_reference(self, ident):
        return None if ident == "gone" else SimpleNamespace(type="Type")


def source(name, kind, size):
    return {"name": name, "namespace": NS[name], "type": kind, "acquirer": Acquirer(NS[name]),
            "mapper": Mapper(), "datacache": SimpleNamespace(len_estimate=lambda: size)}


class Idmap(dict):
    """None for an unknown key, as the real idmaps return."""

    def __missing__(self, key):
        return None


class Cfgs:
    def __init__(self, idmap=None):
        self.sources = {"aat": source("aat", "external", 50), "wd": source("wd", "external", 10),
                        "ycba": source("ycba", "internal", 1000)}
        self.idmap = Idmap(idmap or {})

    def is_qua(self, u):
        return "##qua" in u

    def split_qua(self, u):
        return u.split("##qua")

    def make_qua(self, u, t):
        return f"{u}##qua{t}"

    def canonicalize(self, u):
        return u

    def split_uri(self, u):
        for s in self.sources.values():
            if u.startswith(s["namespace"]):
                return s, u[len(s["namespace"]):]
        return None

    def get_idmap(self):
        return self.idmap

    def instantiate_map(self, name):
        return {"store": {}}


class FakeReconciler:
    """Each record 'finds' what `links` says, recording it in debug_graph."""

    links = {}

    def __init__(self, cfgs, idmap, networkmap):
        self.reconcilers = [SimpleNamespace(debug=False, debug_graph={"stale": []})]
        self.collector = SimpleNamespace(debug=False, debug_graph={"stale": []},
                                         acquirer_should_store=True)
        self.debug = False
        self.debug_graph = {"stale": []}
        self.order = []

    def reconcile(self, rec):
        rid = rec["data"]["id"]
        self.order.append(rid)
        found = self.links.get(rid, [])
        rec["data"]["equivalent"] = [{"id": u} for u, _ in found]
        if found:
            self.debug_graph.setdefault(rid, []).extend(found)
        return rec


def test_typed_uri():
    cfgs = Cfgs()
    assert viz.typed_uri(cfgs, NS["aat"] + "1") == NS["aat"] + "1##quaType"
    assert viz.typed_uri(cfgs, NS["aat"] + "1##quaPlace") == NS["aat"] + "1##quaPlace"
    with pytest.raises(ValueError, match="Unknown URI"):
        viz.typed_uri(cfgs, "http://nowhere/1")
    with pytest.raises(ValueError, match="Could not fetch"):
        viz.typed_uri(cfgs, NS["aat"] + "gone")


def test_enable_debug_reaches_every_part():
    r = FakeReconciler(None, None, None)
    viz.enable_debug(r)
    assert r.debug and r.collector.debug and all(x.debug for x in r.reconcilers)
    assert r.debug_graph == {} and r.collector.debug_graph == {} and r.reconcilers[0].debug_graph == {}
    assert r.collector.acquirer_should_store is False


def test_replay_order_and_skips():
    cfgs = Cfgs()
    r = FakeReconciler(None, None, None)
    r.debug_graph = {}
    r.links = {NS["ycba"] + "7": [(NS["wd"] + "Q1", "uri")]}
    members = ["__20261028__", NS["aat"] + "1##quaType", NS["wd"] + "Q1##quaType",
               NS["ycba"] + "7##quaType", NS["aat"] + "missing##quaType", "http://nowhere/x##quaType"]
    graph = viz.replay(cfgs, r, members)
    # internal first, then by datacache size; wd Q1 was found by ycba 7, so
    # it is not reconciled again
    assert r.order == [NS["ycba"] + "7", NS["aat"] + "1"]
    assert graph == {NS["ycba"] + "7": [(NS["wd"] + "Q1", "uri")]}
    # replay's own fetches are not stored
    assert all(store is False for _, store in cfgs.sources["aat"]["acquirer"].calls)


def test_labels_edges_and_path():
    cfgs = Cfgs()
    a, w, y = NS["aat"] + "1", NS["wd"] + "Q1", NS["ycba"] + "7"
    graph = {y: [(w, "uri"), ("http://id.worldcat.org/fast/9", "eq"), (w, "eq")],
             w: [(a, "nm"), ("http://junk.example/z", "eq")]}
    labels = viz.node_labels(cfgs, graph)
    assert labels == {y: "ycba:0", w: "wd:1", "http://id.worldcat.org/fast/9": "fast:2",
                      a: "aat:3", "http://junk.example/z": "???:4"}
    edges = viz.graph_edges(graph, labels)
    assert edges == [("ycba:0", "wd:1", "uri"), ("ycba:0", "fast:2", "eq"),
                     ("wd:1", "aat:3", "nm"), ("wd:1", "???:4", "eq")]
    # either direction
    assert viz.shortest_path(edges, "aat:3", "ycba:0") == ["aat:3", "wd:1", "ycba:0"]
    assert viz.shortest_path(edges + [("x:9", "x:10", "eq")], "aat:3", "x:9") is None
    assert viz.shortest_path(edges, "aat:3", "nope") is None


def test_labels_do_not_run_out():
    cfgs = Cfgs()
    graph = {NS["aat"] + "0": [(NS["aat"] + str(i), "eq") for i in range(1, 200)]}
    labels = viz.node_labels(cfgs, graph)
    assert len(set(labels.values())) == 200
    assert labels[NS["aat"] + "199"] == "aat:199"


def handler(cfgs):
    return viz.CommandHandler(cfgs)


def run(cfgs, recid, monkeypatch):
    import lux_pipeline.process.reconciler as rmod
    monkeypatch.setattr(rmod, "Reconciler", FakeReconciler)
    return handler(cfgs).process(Namespace(command="viz"), ["--recid", recid])


def test_process_draws_and_prints_the_path(monkeypatch, capsys):
    a, w, y = NS["aat"] + "1", NS["wd"] + "Q1", NS["ycba"] + "7"
    idmap = {f"{a}##quaType": "yuid1", f"{y}##quaType": "yuid1",
             "yuid1": [f"{y}##quaType", f"{a}##quaType", f"{w}##quaType"]}
    # ycba 7 (internal, first) finds wd Q1, which is then not reconciled
    # again; aat 1 finds wd Q1 by name, which joins the two ends
    monkeypatch.setattr(FakeReconciler, "links", {y: [(w, "uri")], a: [(w, "nm")]})
    graph = run(Cfgs(idmap), f"{a},{y}", monkeypatch)
    out = capsys.readouterr().out
    assert graph == {y: [(w, "uri")], a: [(w, "nm")]}
    assert "ycba:0" in out and "wd:1" in out and "aat:2" in out
    assert "yuid1: 3 URIs, 2 connections" in out
    assert "aat:2 --> wd:1 --> ycba:0" in out


def test_process_refuses_two_identities(monkeypatch):
    a, y = NS["aat"] + "1", NS["ycba"] + "7"
    idmap = {f"{a}##quaType": "yuid1", f"{y}##quaType": "yuid2"}
    with pytest.raises(ValueError, match="not the same identity"):
        run(Cfgs(idmap), f"{a},{y}", monkeypatch)


def test_process_refuses_unknown_and_bad_recid(monkeypatch):
    a, y = NS["aat"] + "1", NS["ycba"] + "7"
    with pytest.raises(ValueError, match="not in the idmap"):
        run(Cfgs({f"{a}##quaType": "yuid1"}), f"{a},{y}", monkeypatch)
    with pytest.raises(ValueError, match="two URIs"):
        run(Cfgs(), a, monkeypatch)


def test_process_without_netext(monkeypatch):
    import builtins
    real = builtins.__import__

    def fake(name, *a, **kw):
        if name == "netext":
            raise ImportError("no netext", name="netext")
        return real(name, *a, **kw)

    monkeypatch.setattr(builtins, "__import__", fake)
    with pytest.raises(RuntimeError, match=r"pip install -e \.\[ui-extras\]"):
        handler(Cfgs()).process(Namespace(command="viz"), ["--recid", "a,b"])


# ---------------------------------------------- the reconciler's merge


class StubGlobal:
    def reconcile(self, eq, reconcileType="uri"):
        return set()


class StubCollector:
    def __init__(self, debug):
        self.debug = debug
        self.debug_graph = {}
        self.calls = 0

    def collect(self, record):
        self.calls += 1
        if self.calls == 1:
            record["data"]["equivalent"].append({"id": "http://found/1", "type": "Type"})
            if self.debug:
                self.debug_graph["http://rec/x"] = [("http://found/1", "eq")]


def process_reconciler(debug):
    r = object.__new__(Reconciler)
    r.reconcilers = []
    r.global_reconciler = StubGlobal()
    r.debug = debug
    r.debug_graph = {"http://rec/x": [("http://rec/y", "eq")]} if debug else {}
    r.collector = StubCollector(debug)
    return r


def record():
    return {"data": {"id": "http://rec/x", "type": "Type", "_label": "x",
                     "equivalent": [{"id": "http://rec/x", "type": "Type"}]},
            "source": "src", "identifier": "x"}


def test_reconcile_uris_merges_the_collector_graph_when_debugging():
    r = process_reconciler(True)
    rec = record()
    r.reconcile_uris(rec)
    assert r.debug_graph == {"http://rec/x": [("http://rec/y", "eq"), ("http://found/1", "eq")]}
    assert r.collector.debug_graph == {}


def test_reconcile_uris_unchanged_without_debug():
    on, off = process_reconciler(True), process_reconciler(False)
    rec_on, rec_off = record(), record()
    on.reconcile_uris(rec_on)
    off.reconcile_uris(rec_off)
    assert rec_on == rec_off
    assert off.debug_graph == {}
