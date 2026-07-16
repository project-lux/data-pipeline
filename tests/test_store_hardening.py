"""Regression tests for the flagged (previously unfixed) audit items:
atomic idmap operations, reference bookkeeping, cache hardening, and the
repaired storage modules. Redis behavior is exercised with fakeredis;
everything else is pure or filesystem-tmpdir level.

See docs/determinism-audit.md.
"""

import json
import os
import sys
import threading
from pathlib import Path

import fakeredis
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.storage.idmap import redis as idmap_redis
from pipeline.storage.idmap.lmdb import TabLmdb
from pipeline.storage.idmap.memory import IdMap as MemoryIdMap
from pipeline.process.reference_manager import ReferenceManager


# ---------------------------------------------------------------------------
# Helpers: build IdMap / ReferenceMap instances backed by fakeredis
# ---------------------------------------------------------------------------

class StubConfigs:
    internal_uri = "https://lux.test/data/"
    ok_record_types = {"Type": "concept"}
    external = {}

    def is_qua(self, recid):
        return "##qua" in recid

    def make_qua(self, recid, typ):
        if "##qua" in recid:
            return recid
        return f"{recid}##qua{typ}"


def make_idmap(tmp_path):
    m = object.__new__(idmap_redis.IdMap)
    m.configs = StubConfigs()
    m.conn = fakeredis.FakeStrictRedis(decode_responses=True)
    m._restoring_data_state = False
    m.prefix_map_in = {}
    m.prefix_map_out = {"yuid": StubConfigs.internal_uri}
    m.memory_cache_enabled = False
    m.memory_cache = {}
    m.clean_on_remove = False
    m.update_token = "__20260713__"
    return m


def make_refmap():
    r = object.__new__(idmap_redis.ReferenceMap)
    r.conn = fakeredis.FakeStrictRedis(decode_responses=True)
    r._restoring_data_state = False
    r.prefix_map_in = {}
    r.prefix_map_out = {}
    return r


# ---------------------------------------------------------------------------
# IdMap.set: atomic rekey, no silently dropped mappings
# ---------------------------------------------------------------------------

def test_idmap_set_and_rekey(tmp_path):
    m = make_idmap(tmp_path)
    y1 = m.mint("http://ext/a##quaType", "concept")
    y2 = m.mint("http://ext/b##quaType", "concept")
    # repoint a onto y2: every member of y1 must follow, y1 must vanish
    m.set("http://ext/a##quaType", y2)
    assert m.get("http://ext/a##quaType") == y2
    members = m.get(y2)
    assert "http://ext/a##quaType" in members
    assert m.get(y1) is None


def test_idmap_set_concurrent_rekeys_lose_nothing(tmp_path):
    # Two threads repointing different members between the same two yuids;
    # with the old non-atomic read-modify-write one thread's members could
    # be deleted by the other's stale smembers+delete.
    m = make_idmap(tmp_path)
    y1 = m.mint("http://ext/a##quaType", "concept")
    for i in range(20):
        m.set(f"http://ext/m{i}##quaType", y1)
    y2 = m.mint("http://ext/b##quaType", "concept")

    errs = []

    def repoint(names):
        try:
            mm = make_idmap(tmp_path)
            mm.conn = m.conn
            for n in names:
                mm.set(n, y2)
        except Exception as e:  # pragma: no cover
            errs.append(e)

    t1 = threading.Thread(target=repoint,
                          args=([f"http://ext/m{i}##quaType" for i in range(0, 10)],))
    t2 = threading.Thread(target=repoint,
                          args=([f"http://ext/m{i}##quaType" for i in range(10, 20)],))
    t1.start(); t2.start(); t1.join(); t2.join()
    assert not errs
    # every member must map to y2 and be present in y2's member set
    members = m.get(y2)
    for i in range(20):
        n = f"http://ext/m{i}##quaType"
        assert m.get(n) == y2, n
        assert n in members, n


# ---------------------------------------------------------------------------
# popitem: atomic claim -- no duplicates, no lost items, no (key, None)
# ---------------------------------------------------------------------------

def test_refmap_popitem_drains_exactly_once():
    r = make_refmap()
    for i in range(200):
        r.set(f"http://ref/{i}", {"dist": 1, "type": ""})

    seen = []
    lock = threading.Lock()

    def drain():
        rr = make_refmap()
        rr.conn = r.conn
        while True:
            item = rr.popitem()
            if item is None:
                return
            with lock:
                seen.append(item[0])

    threads = [threading.Thread(target=drain) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(seen) == 200, f"popped {len(seen)} (dupes or losses)"
    assert len(set(seen)) == 200
    assert len(r) == 0


def test_idmap_popitem_empty_returns_none(tmp_path):
    m = make_idmap(tmp_path)
    assert m.popitem() is None


# ---------------------------------------------------------------------------
# ReferenceMap.merge_ref: min-distance, type-if-unset
# ---------------------------------------------------------------------------

def test_merge_ref_keeps_minimum_distance():
    r = make_refmap()
    # whichever order the workers apply, min wins
    r.merge_ref("http://ref/x", 3, "Type")
    r.merge_ref("http://ref/x", 1, "")
    r.merge_ref("http://ref/x", 2, "")
    got = r.get("http://ref/x")
    assert got["dist"] == 1
    assert got["type"] == "Type"


def test_add_ref_requeue_adds_before_deleting():
    # A ref already done at distance 3, re-found at distance 1: it must be
    # back in all_refs at 1. The old order (delete-from-done first) could
    # lose it entirely on a crash between the two operations.
    rm = object.__new__(ReferenceManager)
    rm.ref_cache = {}
    rm.all_refs = make_refmap()
    rm.done_refs = make_refmap()
    rm.done_refs.set("http://ref/x", {"dist": 3, "type": ""})

    rm.add_ref("http://ref/x", {}, 1, "Type")
    assert rm.all_refs.get("http://ref/x")["dist"] == 1
    assert rm.done_refs.get("http://ref/x") is None


# ---------------------------------------------------------------------------
# NetworkOperationMap: cached errors expire, successes persist
# ---------------------------------------------------------------------------

def test_networkmap_errors_get_ttl():
    n = object.__new__(idmap_redis.NetworkOperationMap)
    n.conn = fakeredis.FakeStrictRedis(decode_responses=True)
    n._restoring_data_state = False
    n.prefix_map_in = {}
    n.prefix_map_out = {}

    n.set("http://fetch/fail", 404)
    n.set("http://fetch/err", None)
    n.set("http://fetch/ok", "http://redirected/to")

    assert n.conn.ttl(n._manage_key_in("http://fetch/fail")) > 0
    assert n.conn.ttl(n._manage_key_in("http://fetch/err")) > 0
    assert n.conn.ttl(n._manage_key_in("http://fetch/ok")) == -1  # no expiry


# ---------------------------------------------------------------------------
# TabLmdb: tab-containing members rejected; long keys reported not swallowed
# ---------------------------------------------------------------------------

def test_tablmdb_rejects_tab_members(tmp_path):
    db = TabLmdb.open(str(tmp_path / "t.lmdb"), "c")
    db["k"] = ["http://a", "http://b"]
    assert db["k"] == ["http://a", "http://b"]
    db["s"] = "http://single"
    assert db["s"] == "http://single"  # single member round-trips as str
    with pytest.raises(ValueError):
        db["bad"] = ["http://a\thttp://phantom"]
    with pytest.raises(ValueError):
        db["bad2"] = "with\ttab"


def test_tablmdb_long_key_contains_false(tmp_path):
    db = TabLmdb.open(str(tmp_path / "t2.lmdb"), "c")
    long_key = "http://very/" + "x" * 600
    assert long_key not in db  # reported absent, not crashed


# ---------------------------------------------------------------------------
# Repaired modules: memory idmap, filesystem caches
# ---------------------------------------------------------------------------

def test_memory_idmap_roundtrip():
    m = MemoryIdMap()
    y = m.mint("http://ext/a##quaType", "concept")
    assert m.get("http://ext/a##quaType") == y
    assert "http://ext/a##quaType" in m.get(y)
    m.set("http://ext/b##quaType", y)
    assert m.get("http://ext/b##quaType") == y
    m.delete("http://ext/b##quaType")
    assert m.get("http://ext/b##quaType") is None


def test_filesystem_caches_use_distinct_directories(tmp_path):
    from pipeline.storage.cache import filesystem as fscache

    made = {}
    for cls in (fscache.InternalRecordCache, fscache.ExternalRecordCache,
                fscache.RecordCache):
        inst = cls({"name": "src", "base_dir": str(tmp_path)})
        made[cls.__name__] = inst.directory
    # Internal, External and yuid-keyed record caches previously all
    # defaulted to "src_record_cache" and overwrote each other on disk
    dirs = list(made.values())
    assert len(set(dirs)) == len(dirs), f"colliding cache dirs: {made}"


def test_filesystem_cache_set_get_delete(tmp_path):
    from pipeline.storage.cache import filesystem as fscache

    c = fscache.DataCache({"name": "src", "base_dir": str(tmp_path)})
    c["some/ident"] = {"data": {"id": "x"}, "identifier": "some/ident"}
    assert "some/ident" in c
    del c["some/ident"]  # was AttributeError: no delete()
    assert "some/ident" not in c


# ---------------------------------------------------------------------------
# pending-deletes consumer: delete_yuid + process_pending_deletes
# ---------------------------------------------------------------------------

def test_delete_yuid_only_when_token_only(tmp_path):
    m = make_idmap(tmp_path)
    y = m.mint("http://ext/a##quaType", "concept")
    m.conn.sadd(m._manage_value_in(y), "__20260713__")
    # real member present -> refuse
    with pytest.raises(ValueError):
        m.delete_yuid(y)
    # remove the member; token-only set now deletable
    m.conn.srem(m._manage_value_in(y), "http://ext/a##quaType")
    assert m.delete_yuid(y) is True
    assert m.get(y) is None


def test_process_pending_deletes(tmp_path):
    from pipeline.process.update_manager import UpdateManager

    idmap = make_idmap(tmp_path)
    y = idmap.mint("http://ext/gone##quaType", "concept")
    uu = y.rsplit("/", 1)[-1]
    idmap.conn.srem(idmap._manage_value_in(y),
                          "http://ext/gone##quaType")

    merged = {uu: {"data": 1}}
    ml = {uu: {"data": 1}}

    class Cfg(StubConfigs):
        data_dir = str(tmp_path)
        internal = {}
        results = {"merged": {"recordcache": merged},
                   "marklogic": {"recordcache": ml}}

    mgr = object.__new__(UpdateManager)
    mgr.configs = Cfg()
    mgr.idmap = idmap

    fn = tmp_path / "pending_deletes.jsonl"
    fn.write_text(json.dumps({"action": "delete-merged", "yuid": y,
                              "identifier": "x", "qua": "q",
                              "remaining": []}) + "\n")
    stats = mgr.process_pending_deletes(str(fn))
    assert stats["processed"] == 1
    assert uu not in merged and uu not in ml
    assert idmap.get(y) is None
    # file rotated: source empty, .done has the entry
    assert fn.read_text() == ""
    assert "delete-merged" in (tmp_path / "pending_deletes.done.jsonl").read_text()
