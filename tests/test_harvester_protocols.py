"""Tests for the harvester protocol / driver split (refactor item 16)."""

import io
import json
import logging
import os
from types import SimpleNamespace

import pytest

import lux_pipeline.process.base.harvester as harvester_mod
from lux_pipeline.process.base.harvester import (
    ASHarvester,
    ASProtocol,
    Harvester,
    HarvestProtocol,
    PmhHarvester,
    PmhProtocol,
)
from lux_pipeline.process.utils.io import count_lines
from lux_pipeline.sources.getty.harvester import GettyHarvester, GettyProtocol
from lux_pipeline.sources.lc.harvester import LCHarvester, LCProtocol


# --- stubs ------------------------------------------------------------------


class FakeSession:
    """Serves canned bodies by URI and remembers what was asked for."""

    def __init__(self, pages):
        self.pages = pages
        self.requested = []
        self.headers = {}

    def get(self, uri):
        self.requested.append(uri)
        body = self.pages[uri]
        return SimpleNamespace(text=body if isinstance(body, str) else json.dumps(body))


class Recorder:
    def __init__(self):
        self.calls = []

    def __call__(self, **kw):
        self.calls.append(kw)

    def advanced(self):
        return sum(c.get("advance", 0) for c in self.calls)

    def descriptions(self):
        return [c["description"] for c in self.calls if c.get("description")]

    def last_total(self):
        totals = [c["total"] for c in self.calls if c.get("total") is not None]
        return totals[-1] if totals else None


class DictCache(dict):
    """The slice of the datacache API the harvester touches."""

    def __init__(self, *a, latest="2000-01-01T00:00:00", **kw):
        super().__init__(*a, **kw)
        self._latest = latest
        self.sets = []
        self.insert_times = {}

    def __setitem__(self, key, value):
        # a real cache serialises, so the stored copy is independent
        super().__setitem__(key, {"data": json.loads(json.dumps(value))})

    def __getitem__(self, key):
        return super().get(key)

    def set(self, data, identifier=None, record_time=None):
        self.sets.append((identifier, record_time))
        self[identifier] = data

    def metadata(self, key, field="insert_time"):
        if key in self.insert_times:
            return {field: self.insert_times[key]}
        return None

    def latest(self):
        return self._latest


NS = "http://example.org/data/"
COLL = "https://example.org/stream/collection"
P1 = "https://example.org/stream/page/1"
P2 = "https://example.org/stream/page/2"
T1, T2, T3, T4 = ("2024-12-01T00:00:00", "2025-01-02T00:00:00", "2025-01-03T00:00:00", "2025-01-04T00:00:00")


def item(typ, when, ident):
    return {"type": typ, "endTime": when, "object": {"id": f"https://example.org/data/{ident}"}}


def record(ident):
    return {"id": f"{NS}{ident}", "type": "Person", "_label": ident}


def as_pages():
    return {
        COLL: {"id": COLL, "first": {"id": P1}, "last": {"id": P2}},
        P2: {"id": P2, "prev": {"id": P1}, "orderedItems": [item("Update", T2, "a"), item("Delete", T3, "b"), item("Create", T4, "c")]},
        P1: {"id": P1, "orderedItems": [item("Update", T1, "d")]},
        f"{NS}a": record("a"),
        f"{NS}c": record("c"),
        f"{NS}d": record("d"),
    }


def make_configs(tmp_path=None, **extra):
    cfgs = SimpleNamespace(
        internal={}, external={}, results={},
        data_dir=str(tmp_path) if tmp_path else "", temp_dir=str(tmp_path) if tmp_path else "",
        make_qua=lambda uri, typ: f"{uri}##qua{typ}",
        get_idmap=lambda: {},
    )
    for k, v in extra.items():
        setattr(cfgs, k, v)
    return cfgs


def make_config(configs=None, **keys):
    cfg = {"name": "src", "namespace": NS, "type": "internal", "activitystreams": [COLL],
           "all_configs": configs or make_configs()}
    cfg.update(keys)
    return cfg


def make_as(pages=None, progress=None, **keys):
    proto = ASProtocol(make_config(**keys))
    proto.session = FakeSession(pages if pages is not None else as_pages())
    if progress is not None:
        proto.progress = progress
    return proto


# --- ASProtocol -------------------------------------------------------------


def test_as_walks_pages_newest_first_and_yields_tuples():
    proto = make_as()
    got = list(proto.crawl())
    assert got == [
        ("create", "c", record("c"), T4),
        ("delete", "b", {}, ""),
        ("update", "a", record("a"), T2),
        ("update", "d", record("d"), T1),
    ]
    # collection, last page, items, then the prev page
    assert proto.session.requested[:2] == [COLL, P2]
    assert P1 in proto.session.requested
    assert proto.session.requested.index(P1) > proto.session.requested.index(f"{NS}a")


def test_as_last_harvest_stops_the_walk():
    proto = make_as()
    got = list(proto.crawl(last_harvest="2025-01-01T00:00:00"))
    assert [g[1] for g in got] == ["c", "b", "a"]
    assert f"{NS}d" not in proto.session.requested
    assert proto.page is None


def test_as_last_harvest_from_config():
    proto = make_as(last_harvest=T3)
    assert [g[1] for g in proto.crawl()] == ["c", "b"]


def test_as_refsonly_fetches_no_records():
    proto = make_as()
    got = list(proto.crawl(refsonly=True))
    assert got == [("create", "c", {}, T4), ("delete", "b", {}, T3), ("update", "a", {}, T2), ("update", "d", {}, T1)]
    assert not [u for u in proto.session.requested if u.startswith(NS)]


def test_as_dedupes_and_delete_shadows_older_changes():
    pages = as_pages()
    pages[P1]["orderedItems"] = [item("Update", T1, "b"), item("Update", T1, "c"), item("Update", T1, "d")]
    proto = make_as(pages)
    got = [(g[0], g[1]) for g in proto.crawl()]
    assert got == [("create", "c"), ("delete", "b"), ("update", "a"), ("update", "d")]


def test_as_unknown_type_is_update_and_https_is_smushed_to_namespace():
    pages = as_pages()
    pages[P2]["orderedItems"] = [{"type": "Frobnicate", "endTime": T2, "object": {"id": "https://example.org/data/a"}}]
    pages[P1]["orderedItems"] = []
    proto = make_as(pages)
    assert list(proto.crawl()) == [("update", "a", record("a"), T2)]
    assert f"{NS}a" in proto.session.requested


def test_as_harvest_from_skips_newer_changes():
    proto = make_as(harvest_from=T2)
    assert [g[1] for g in proto.crawl()] == ["a", "d"]


def test_as_refresh_marker_stops(caplog):
    pages = as_pages()
    pages[P2]["orderedItems"].insert(0, {"type": "Refresh", "endTime": T1, "object": {"id": f"{NS}x"}})
    proto = make_as(pages)
    # reversed: c, b, a, then the refresh marker stops the walk before page 1
    assert [g[1] for g in proto.crawl()] == ["c", "b", "a"]
    assert P1 not in proto.session.requested


def test_as_skips_newer_cached_record_and_empty_fetch(caplog):
    pages = as_pages()
    pages[f"{NS}c"] = {}
    cache = DictCache()
    cache.insert_times["a"] = SimpleNamespace(isoformat=lambda: "2026-01-01T00:00:00")
    proto = make_as(pages, datacache=cache)
    got = [g[1] for g in proto.crawl()]
    assert got == ["b", "d"]
    assert f"{NS}a" not in proto.session.requested
    assert "got {} from c" in caplog.text


def test_as_uses_fetcher_when_configured():
    class Fetcher:
        enabled = False

        def fetch(self, ident):
            return {"fetched": ident}

    proto = make_as(fetcher=Fetcher())
    got = list(proto.crawl())
    assert got[0] == ("create", "c", {"fetched": "c"}, T4)
    assert proto.fetcher.enabled is True
    assert not [u for u in proto.session.requested if u.startswith(NS)]


def test_as_reports_progress():
    rec = Recorder()
    proto = make_as(progress=rec)
    list(proto.crawl())
    assert rec.descriptions() == ["src/collection"]
    assert rec.last_total() == 4
    assert rec.advanced() == 4


def test_as_without_progress_reports_nowhere():
    proto = make_as()
    assert proto.progress is harvester_mod.null_progress
    assert len(list(proto.crawl())) == 4


def test_as_start_page_skips_the_collection():
    proto = make_as(start_page=P1)
    assert [g[1] for g in proto.crawl()] == ["d"]
    assert COLL not in proto.session.requested


def test_as_multiple_collections_share_seen():
    pages = as_pages()
    COLL2 = "https://example.org/stream2/collection"
    Q1 = "https://example.org/stream2/page/1"
    pages[COLL2] = {"id": COLL2, "first": {"id": Q1}, "last": {"id": Q1}}
    pages[Q1] = {"id": Q1, "orderedItems": [item("Update", T1, "a"), item("Update", T1, "e")]}
    pages[f"{NS}e"] = record("e")
    rec = Recorder()
    proto = make_as(pages, progress=rec, activitystreams=[COLL, COLL2])
    assert [g[1] for g in proto.crawl()] == ["c", "b", "a", "d", "e"]
    assert rec.descriptions() == ["src/collection", "src/collection"]


def test_as_crawl_single_resets_state():
    proto = make_as()
    assert [g[1] for g in proto.crawl_single(COLL)] == ["c", "b", "a", "d"]
    assert [g[1] for g in proto.crawl_single(COLL)] == ["c", "b", "a", "d"]


# --- page cache ---------------------------------------------------------------


def test_as_page_cache_fills_then_serves_pages():
    cache = DictCache()
    proto = make_as()
    proto.page_cache = cache
    assert [g[1] for g in proto.crawl()] == ["c", "b", "a", "d"]
    assert set(cache) == {P1, P2}
    assert cache[P2]["data"]["orderedItems"][0]["object"]["id"].endswith("/a")  # stored unreversed

    # Second walk: the last page is fetched and matches, the rest come from the cache
    before = len(proto.session.requested)
    assert [g[1] for g in proto.crawl()] == ["c", "b", "a", "d"]
    fetched = proto.session.requested[before:]
    assert P2 in fetched and P1 not in fetched
    assert proto.cache_okay is True


def test_set_page_cache_from_source_key_and_base_config(caplog):
    cache = DictCache()
    cfgs = make_configs(external={"activitystreams": {"datacache": cache}})
    proto = ASProtocol(make_config(cfgs, activitystreams_cache_source="activitystreams"))
    proto.set_page_cache()
    assert proto.page_cache is cache

    cfgs.activitystreams_cache_source = "activitystreams"
    proto = ASProtocol(make_config(cfgs))
    proto.set_page_cache()
    assert proto.page_cache is cache

    # explicit assignment wins; a missing source warns and leaves it unset
    proto = ASProtocol(make_config(cfgs))
    proto.page_cache = "mine"
    proto.set_page_cache()
    assert proto.page_cache == "mine"
    proto = ASProtocol(make_config(make_configs(), activitystreams_cache_source="nope"))
    proto.set_page_cache()
    assert proto.page_cache is None
    assert "not a configured source" in caplog.text


def test_harvest_pages_fetches_this_slice_into_the_cache():
    pages = as_pages()
    pages[COLL] = {"id": COLL, "first": {"id": P1}, "last": {"id": P2}}
    cache = DictCache()
    rec = Recorder()
    proto = make_as(pages, progress=rec)
    proto.page_cache = cache
    proto.harvest_pages(0, 2)
    assert set(cache) == {P1}
    proto.harvest_pages(1, 2)
    assert set(cache) == {P1, P2}
    assert rec.descriptions() == ["src/pages", "src/pages"]
    assert rec.advanced() == 2


# --- GettyProtocol ------------------------------------------------------------


def test_getty_rewrites_vocab_uris_and_filters_other_vocabularies():
    gns = "http://vocab.getty.edu/aat/"
    pages = {
        COLL: {"id": COLL, "last": {"id": P1}},
        P1: {"id": P1, "orderedItems": [
            {"type": "Update", "endTime": T1, "object": {"id": "https://data.getty.edu/vocab/ulan/500000001"}},
            {"type": "Update", "endTime": T2, "object": {"id": "https://data.getty.edu/vocab/aat/300404670"}},
        ]},
        f"{gns}300404670": {"id": f"{gns}300404670", "type": "Type"},
    }
    rec = Recorder()
    proto = GettyProtocol(make_config(name="aat", namespace=gns))
    proto.session = FakeSession(pages)
    proto.progress = rec
    got = list(proto.crawl())
    assert got == [("update", "300404670", {"id": f"{gns}300404670", "type": "Type"}, T2)]
    assert rec.advanced() == 2  # one filtered out, one processed


# --- PmhProtocol --------------------------------------------------------------


def pmh_page(headers, token=None):
    hs = "".join(f"<header><identifier>{i}</identifier><datestamp>{d}</datestamp></header>" for i, d in headers)
    tk = f"<resumptionToken>{token}</resumptionToken>" if token else ""
    return f'<?xml version="1.0"?><OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"><ListIdentifiers>{hs}{tk}</ListIdentifiers></OAI-PMH>'


def test_pmh_walks_pages_with_resumption_tokens_and_stops_at_last_harvest():
    class Fetcher:
        enabled = False

        def fetch(self, ident):
            return {"id": ident}

    endpoint = "https://pmh.example.org/oai"
    pages = {
        f"{endpoint}?verb=ListIdentifiers&metadataPrefix=lido": pmh_page([("oai:1", T4), ("oai:2", T3)], token="tok"),
        f"{endpoint}?verb=ListIdentifiers&resumptionToken=tok": pmh_page([("oai:3", T2), ("oai:4", T1)]),
    }
    rec = Recorder()
    proto = PmhProtocol(make_config(activitystreams=None, pmhEndpoint=endpoint, pmhMetadataPrefix="lido", fetcher=Fetcher()))
    proto.session = FakeSession(pages)
    proto.progress = rec
    assert list(proto.crawl()) == [
        ("update", "oai:1", {"id": "oai:1"}, T4),
        ("update", "oai:2", {"id": "oai:2"}, T3),
        ("update", "oai:3", {"id": "oai:3"}, T2),
        ("update", "oai:4", {"id": "oai:4"}, T1),
    ]
    assert rec.advanced() == 4
    assert [g[1] for g in proto.crawl(last_harvest=T4)] == ["oai:1"]
    assert len(proto.session.requested) == 3  # the second walk stopped on page one


# --- drivers ----------------------------------------------------------------


def test_driver_creates_its_protocol_in_init():
    h = ASHarvester(make_config())
    assert type(h.protocol) is ASProtocol
    assert h.protocol.progress is harvester_mod.null_progress
    assert type(ASHarvester(make_config(activitystreams=None, pmhEndpoint="https://x/oai")).protocol) is PmhProtocol
    assert type(PmhHarvester(make_config(pmhEndpoint="https://x/oai")).protocol) is PmhProtocol
    assert type(GettyHarvester(make_config()).protocol) is GettyProtocol
    assert type(LCHarvester(make_config()).protocol) is LCProtocol
    assert Harvester is HarvestProtocol
    assert issubclass(GettyHarvester, ASHarvester) and issubclass(PmhHarvester, ASHarvester)


def test_driver_honours_harvest_protocol_class(monkeypatch):
    class MyProtocol(ASProtocol):
        pass

    monkeypatch.setattr(harvester_mod, "importObject", lambda p: MyProtocol if p == "x.MyProtocol" else None)
    assert type(ASHarvester(make_config(harvestProtocolClass="x.MyProtocol")).protocol) is MyProtocol


def test_driver_pass_throughs_reach_the_protocol():
    h = ASHarvester(make_config())
    h.last_harvest = T2
    h.harvest_from = T4
    h.fetcher = "f"
    h.page_cache = "pc"
    assert (h.protocol.last_harvest, h.protocol.harvest_from, h.protocol.fetcher, h.protocol.page_cache) == (T2, T4, "f", "pc")
    assert (h.last_harvest, h.harvest_from, h.fetcher, h.page_cache) == (T2, T4, "f", "pc")
    h.protocol.session = FakeSession(as_pages())
    h.fetcher = None
    h.page_cache = None
    assert [g[1] for g in h.crawl(last_harvest=T3, refsonly=True)] == ["c", "b"]


def test_driver_harvest_pages_pass_through():
    h = ASHarvester(make_config())
    h.protocol.session = FakeSession(as_pages())
    h.page_cache = DictCache()
    h.harvest_pages(0, 1)
    assert set(h.page_cache) == {P1, P2}


def test_prepare_resolves_last_harvest_and_hands_progress_down():
    cache = DictCache(latest="2025-06-01T00:00:00")
    rec = Recorder()
    h = ASHarvester(make_config(datacache=cache)).prepare(0, 1, progress=rec)
    assert h.last_harvest == "2025-06-01T00:00:00"
    assert h.protocol.progress is rec
    assert h.my_slice == 0 and h.max_slice == 1

    h = ASHarvester(make_config(datacache=cache, last_harvest=T2)).prepare()
    assert h.last_harvest == T2

    h = ASHarvester(make_config(datacache=cache)).prepare(until=T3)
    assert h.last_harvest == T3

    h = ASHarvester(make_config(datacache=cache))
    h.until = T4
    assert h.prepare().last_harvest == T4

    # no datacache: the default stands
    assert ASHarvester(make_config()).prepare().last_harvest == harvester_mod.DEFAULT_LAST_HARVEST


class RecordFetcher:
    """Returns records the way the real Fetcher does: a dict with ``data``."""

    enabled = False

    def fetch(self, ident):
        return {"identifier": ident, "data": record(ident)}


def test_process_applies_changes_through_update_manager(tmp_path):
    datacache = DictCache()
    datacache["b"] = {"id": f"{NS}b", "type": "Person"}
    recordcache = DictCache()
    recordcache["b"] = {"id": "yuid-b"}
    idmap = {f"{NS}b##quaPerson": "urn:yuid:1", "urn:yuid:1": [f"{NS}b##quaPerson", f"{NS}z##quaPerson"]}
    cfgs = make_configs(tmp_path, get_idmap=lambda: idmap)
    cfg = make_config(cfgs, datacache=datacache, recordcache=recordcache, fetcher=RecordFetcher())
    cfgs.internal["src"] = cfg
    h = ASHarvester(cfg).prepare(0, 1)
    h.protocol.session = FakeSession(as_pages())

    assert h.process() == 4
    assert [s[0] for s in datacache.sets] == ["c", "a", "d"]
    assert datacache.sets[0] == ("c", T4)
    assert datacache["c"]["data"] == record("c")
    assert "b" not in datacache and "b" not in recordcache
    assert f"{NS}b##quaPerson" not in idmap
    entries = [json.loads(l) for l in (tmp_path / "pending_deletes.jsonl").read_text().splitlines()]
    assert entries == [{
        "action": "rebuild", "source": "src", "identifier": "b", "qua": f"{NS}b##quaPerson",
        "yuid": "urn:yuid:1", "remaining": [f"{NS}z##quaPerson"], "has_internal": True, "changeTime": "",
    }]
    assert [c[1] for c in h.changed] == ["c", "a", "d"]
    assert isinstance(h.update_manager, harvester_mod.UpdateManager)


def test_process_overwrite_false_keeps_existing(tmp_path):
    datacache = DictCache()
    datacache["a"] = {"old": True}
    cfg = make_config(make_configs(tmp_path), datacache=datacache, recordcache=DictCache(),
                      fetcher=RecordFetcher(), harvest_overwrite=False)
    h = ASHarvester(cfg).prepare(idmap={})
    h.protocol.session = FakeSession(as_pages())
    h.process()
    assert datacache["a"]["data"] == {"old": True}
    assert [s[0] for s in datacache.sets] == ["c", "d"]


def test_get_record_list_writes_sorted_lists(tmp_path):
    pages = as_pages()
    pages[P1]["orderedItems"] = [item("Update", T1, "d"), item("Delete", T1, "c"), item("Update", T1, "b")]
    cfg = make_config(make_configs(tmp_path))
    h = ASHarvester(cfg)
    h.protocol.session = FakeSession(pages)
    records, deleted = h.get_record_list()
    assert records == {"a": T2, "c": T4, "d": T1}
    assert deleted == {"b": T3}
    assert (tmp_path / "all_src_uris.txt").read_text() == f"a\t{T2}\nc\t{T4}\nd\t{T1}\n"
    assert (tmp_path / "deleted_src_uris.txt").read_text() == f"b\t{T3}\n"
    assert not [u for u in h.protocol.session.requested if u.startswith(NS)]

    h2 = ASHarvester(cfg)
    h2.protocol.session = FakeSession(pages)
    records, deleted = h2.get_record_list(until=T3)
    assert records == {"c": T4} and deleted == {"b": T3}


def test_harvest_from_list_takes_this_workers_share(tmp_path):
    class Fetcher:
        enabled = False

        def __init__(self):
            self.asked = []

        def fetch(self, ident):
            self.asked.append(ident)
            return None if ident == "gone" else {"id": ident}

    lines = [("a", T1), ("b", T2), ("c", T3), ("d", T4), ("gone", T4), ("fresh", T1)]
    (tmp_path / "all_src_uris.txt").write_text("".join(f"{i}\t{t}\n" for i, t in lines) + "\n")
    storage = DictCache()
    storage.insert_times["fresh"] = SimpleNamespace(isoformat=lambda: "2026-01-01T00:00:00")
    fetcher = Fetcher()
    cfg = make_config(make_configs(tmp_path), datacache=storage, fetcher=fetcher)
    rec = Recorder()

    h = ASHarvester(cfg).prepare(1, 2, progress=rec)
    assert h.harvest_from_list() == 2
    assert fetcher.asked == ["b", "d", "fresh"][:2] or fetcher.asked == ["b", "d"]
    assert sorted(storage) == ["b", "d"]
    assert fetcher.enabled is True
    assert rec.last_total() == 3  # 7 lines (one blank) // 2
    assert rec.advanced() == 3
    assert rec.descriptions() == ["src/list"]

    h = ASHarvester(cfg).prepare(0, 2)
    assert h.harvest_from_list() == 2
    assert sorted(storage) == ["a", "b", "c", "d"]
    assert "gone" in fetcher.asked and "fresh" not in fetcher.asked

    # explicit slices and a single worker
    storage.clear()
    assert ASHarvester(cfg).harvest_from_list(0, 1) == 4

    # missing list / no datacache / no fetcher are logged, not raised
    os.remove(tmp_path / "all_src_uris.txt")
    assert ASHarvester(cfg).harvest_from_list(0, 1) == 0
    assert ASHarvester(make_config(make_configs(tmp_path), fetcher=fetcher)).harvest_from_list(0, 1) == 0
    assert ASHarvester(make_config(make_configs(tmp_path), datacache=storage)).harvest_from_list(0, 1) == 0


# --- count_lines ----------------------------------------------------------------


def test_count_lines(tmp_path):
    p = tmp_path / "f.txt"
    p.write_text("a\nb\nc\n")
    with open(p) as fh:
        fh.readline()
        assert count_lines(fh) == 3
        assert fh.tell() == 0
    with open(p, "rb") as fh:
        assert count_lines(fh, limit=4) == -1

    class NoSeek(io.IOBase):
        def seek(self, *a):
            raise OSError("no")

    assert count_lines(NoSeek()) == -1


# --- run-harvest.py entry points on UpdateManager still work ---------------------


def test_update_manager_entry_points_drive_the_new_harvester(tmp_path):
    from lux_pipeline.process.update_manager import UpdateManager

    datacache = DictCache(latest="2025-01-02T12:00:00")
    cfgs = make_configs(tmp_path, external={"activitystreams": {"datacache": DictCache()}})
    cfg = make_config(cfgs, datacache=datacache, recordcache=DictCache(), fetcher=RecordFetcher())
    cfgs.internal["src"] = cfg
    cfg["harvester"] = ASHarvester(cfg)
    cfg["harvester"].protocol.session = FakeSession(as_pages())
    mgr = UpdateManager(cfgs, {})

    # run-harvest.py: assign the page cache, then harvest_single -> harvest()
    cfg["harvester"].page_cache = cfgs.external["activitystreams"]["datacache"]
    mgr.harvest_single("src")
    assert cfg["harvester"].last_harvest == "2025-01-02T12:00:00"   # from datacache.latest()
    assert [s[0] for s in datacache.sets] == ["c"]
    assert set(cfgs.external["activitystreams"]["datacache"]) == {P2}

    # --pages mode
    mgr.harvest_pages(cfg, 0, 1)
    assert set(cfgs.external["activitystreams"]["datacache"]) == {P1, P2}

    # two-phase through the manager
    records, deleted = mgr.get_record_list(cfg)
    assert sorted(records) == ["a", "c", "d"] and sorted(deleted) == ["b"]
    datacache.sets.clear()
    mgr.harvest_from_list(cfg, 0, 1)
    assert sorted(datacache) == ["a", "c", "d"]


# --- set_endpoint (lux harvest --endpoint) ------------------------------------


def test_as_set_endpoint_walks_only_that_collection_ignoring_start_page():
    other = "https://other.org/as/collection.json"
    proto = make_as(start_page="https://example.org/as/page/1")
    proto.set_endpoint(other)
    assert proto.collections == [other]
    proto.prepare()
    assert proto.page is None
    pages = as_pages()
    pages[other] = pages[COLL]  # the same stream, served from the other URL
    proto.session = FakeSession(pages)
    got = list(proto.crawl())
    assert proto.session.requested[0] == other
    assert COLL not in proto.session.requested
    assert [ident for _, ident, _, _ in got] == ["c", "b", "a", "d"]


def test_pmh_set_endpoint():
    proto = PmhProtocol({"name": "p", "pmhEndpoint": "https://a.org/oai"})
    proto.set_endpoint("https://b.org/oai")
    assert proto.make_pmh_uri("ListIdentifiers").startswith("https://b.org/oai?verb=ListIdentifiers")


def test_driver_set_endpoint_reaches_the_protocol(caplog):
    h = ASHarvester(make_config())
    with caplog.at_level("INFO", logger="lux_pipeline"):
        h.set_endpoint("https://other.org/as/collection.json")
    assert h.protocol.collections == ["https://other.org/as/collection.json"]
    assert "instead of the configured feed" in caplog.text


def test_a_protocol_without_a_feed_url_says_so():
    with pytest.raises(NotImplementedError, match="cannot take a feed URL"):
        HarvestProtocol({"name": "x"}).set_endpoint("https://x.org")
