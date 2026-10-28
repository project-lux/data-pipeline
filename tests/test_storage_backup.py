"""lux_pipeline.storage.backup: the protocol, the portable formats and the
native copies.

Real files throughout for the portable format (memory idmap, filesystem
cache and map, LMDB) and for LMDB's native copy. Postgres is a fake
connection that records every statement, answers the catalog queries and
holds each table's COPY text, so the native COPY path, the drop / truncate
/ load / rebuild order of a restore and its rollback are pinned without a
server. Also: what is refused and why, the manifest, and verify."""

import datetime
import gzip
import json
import os
import random
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.storage import backup as bk
from lux_pipeline.storage.cache.filesystem import FsCache
from lux_pipeline.storage.idmap import postgres as pgidmap
from lux_pipeline.storage.idmap.filesystem import FsMap
from lux_pipeline.storage.idmap.lmdb import JsonLmdb, TabLmdb
from lux_pipeline.storage.idmap.memory import IdMap as MemIdMap

LUX = "https://lux.collections.yale.edu/data/"


# --- fake postgres -------------------------------------------------------------


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.result = []
        self.rowcount = -1

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.conn.log.append(sql.split()[0] if "pg_" in sql else sql)
        table = params[0] if params else None
        if "FROM pg_attribute" in sql:
            self.result = [(c,) for c in self.conn.columns[table]]
        elif "FROM pg_index" in sql:
            self.result = list(self.conn.indexes.get(table, []))
        elif "FROM pg_constraint" in sql:
            self.result = list(self.conn.constraints.get(table, []))
        elif "pg_total_relation_size" in sql:
            self.result = [(4096 * len(params[0]),)]
        else:
            self.result = []

    def fetchall(self):
        return self.result

    def fetchone(self):
        return self.result[0] if self.result else None

    def copy_expert(self, sql, fh):
        self.conn.log.append(sql)
        table = sql.split()[1]
        if "TO STDOUT" in sql:
            text = self.conn.tables[table]
            fh.write(text.encode("utf-8"))
            self.rowcount = text.count("\n")
        else:
            data = fh.read()
            if self.conn.fail_on == table:
                raise RuntimeError("COPY failed")
            self.conn.loaded[table] = data.decode("utf-8")
            self.rowcount = data.count(b"\n")


class FakeConn:
    def __init__(self, tables=None, columns=None, constraints=None, indexes=None):
        self.tables = tables or {}
        self.columns = columns or {}
        self.constraints = constraints or {}
        self.indexes = indexes or {}
        self.loaded = {}
        self.log = []
        self.autocommit = True
        self.session = None
        self.fail_on = None
        self.closed = False

    def cursor(self, name=None):
        return FakeCursor(self)

    def set_session(self, **kw):
        self.session = kw

    def commit(self):
        self.log.append("COMMIT")

    def rollback(self):
        self.log.append("ROLLBACK")

    def close(self):
        self.closed = True


IDMAP_TABLES = {"idmap": "lcnaf:n1##quaPerson\tyuid:aaa\nlcnaf:n2##quaPerson\tyuid:aaa\n",
                "idmap_yuid": "yuid:aaa\t__20261027__\t2026-10-27 10:00:00\nyuid:bbb\t\\N\t\\N\n"}
IDMAP_COLUMNS = {"idmap": ["uri", "yuid"], "idmap_yuid": ["yuid", "token", "minted"],
                 "scratch": ["uri", "yuid"], "scratch_yuid": ["yuid", "token", "minted"]}
IDMAP_CONSTRAINTS = {"idmap": [("idmap_pkey", "PRIMARY KEY (uri)")],
                     "idmap_yuid": [("idmap_yuid_pkey", "PRIMARY KEY (yuid)")]}
IDMAP_INDEXES = {"idmap": [("idmap_yuid_uri_idx",
                            "CREATE INDEX idmap_yuid_uri_idx ON public.idmap USING btree (yuid, uri)")]}


def fake_idmap(conn, table="idmap"):
    """A postgres IdMap without __init__ (no server, no token file)."""
    m = object.__new__(pgidmap.IdMap)
    m.table = table
    m.yuid_table = f"{table}_yuid"
    m.prefix_map_out = {"yuid": LUX, "lcnaf": "http://id.loc.gov/authorities/names/"}
    m.prefix_map_in = {v: k for k, v in m.prefix_map_out.items()}
    m._conn_kw = {}
    m.memory_cache = set()          # anything with clear()
    m.memory_cache.add("stale")
    return m


@pytest.fixture
def pg(monkeypatch):
    conn = FakeConn(dict(IDMAP_TABLES), dict(IDMAP_COLUMNS), dict(IDMAP_CONSTRAINTS),
                    dict(IDMAP_INDEXES))
    monkeypatch.setattr(pgidmap.psycopg2, "connect", lambda **kw: conn)
    return conn


# --- postgres native -------------------------------------------------------------


def test_idmap_backup_copies_both_tables_in_one_snapshot(tmp_path, pg):
    m = fake_idmap(pg)
    entry = bk.backup(m, "map", "idmap", str(tmp_path))
    assert entry["format"] == bk.PG_COPY
    assert entry["store_class"] == "storage.idmap.postgres.IdMap"
    assert entry["files"] == ["idmap.idmap.copy.gz", "idmap.idmap_yuid.copy.gz"]
    assert entry["rows"] == 4
    assert [t["rows"] for t in entry["extra"]["tables"]] == [2, 2]
    assert entry["extra"]["tables"][1]["columns"] == ["yuid", "token", "minted"]
    assert pg.session["readonly"] is True and pg.autocommit is False
    copies = [s for s in pg.log if s.startswith("COPY")]
    assert copies == ["COPY idmap (uri, yuid) TO STDOUT", "COPY idmap_yuid (yuid, token, minted) TO STDOUT"]
    assert pg.log[-1] == "COMMIT"
    with gzip.open(tmp_path / "idmap.idmap_yuid.copy.gz", "rt") as fh:
        assert fh.read() == IDMAP_TABLES["idmap_yuid"]
    assert pg.closed


def test_idmap_restore_drops_truncates_loads_rebuilds_in_one_transaction(tmp_path, pg):
    entry = bk.backup(fake_idmap(pg), "map", "idmap", str(tmp_path))
    pg.log.clear()
    m = fake_idmap(pg)
    assert bk.restore(m, str(tmp_path), entry) == 4
    log = [s for s in pg.log if s != "SELECT"]
    assert log == [
        'DROP INDEX "idmap_yuid_uri_idx"',
        'ALTER TABLE idmap DROP CONSTRAINT "idmap_pkey"',
        'ALTER TABLE idmap_yuid DROP CONSTRAINT "idmap_yuid_pkey"',
        "TRUNCATE idmap, idmap_yuid",
        "COPY idmap (uri, yuid) FROM STDIN",
        "COPY idmap_yuid (yuid, token, minted) FROM STDIN",
        'ALTER TABLE idmap ADD CONSTRAINT "idmap_pkey" PRIMARY KEY (uri)',
        "CREATE INDEX idmap_yuid_uri_idx ON public.idmap USING btree (yuid, uri)",
        'ALTER TABLE idmap_yuid ADD CONSTRAINT "idmap_yuid_pkey" PRIMARY KEY (yuid)',
        "COMMIT",
    ]
    assert pg.loaded == IDMAP_TABLES
    assert not m.memory_cache


def test_a_failed_restore_rolls_back(tmp_path, pg):
    entry = bk.backup(fake_idmap(pg), "map", "idmap", str(tmp_path))
    pg.log.clear()
    pg.fail_on = "idmap_yuid"
    with pytest.raises(RuntimeError, match="COPY failed"):
        bk.restore(fake_idmap(pg), str(tmp_path), entry)
    assert pg.log[-1] == "ROLLBACK" and "COMMIT" not in pg.log


def test_restore_into_a_scratch_table(tmp_path, pg):
    entry = bk.backup(fake_idmap(pg), "map", "idmap", str(tmp_path))
    bk.restore(fake_idmap(pg, table="scratch"), str(tmp_path), entry)
    assert pg.loaded == {"scratch": IDMAP_TABLES["idmap"], "scratch_yuid": IDMAP_TABLES["idmap_yuid"]}


def test_restore_refuses_other_columns(tmp_path, pg):
    entry = bk.backup(fake_idmap(pg), "map", "idmap", str(tmp_path))
    pg.columns["idmap"] = ["uri", "yuid", "extra"]
    with pytest.raises(ValueError, match="has columns"):
        bk.restore(fake_idmap(pg), str(tmp_path), entry)
    assert not pg.loaded


def test_native_entries_restore_only_into_their_class(tmp_path, pg):
    entry = bk.backup(fake_idmap(pg), "map", "idmap", str(tmp_path))
    with pytest.raises(bk.BackupRefused, match="restore it from a portable backup"):
        bk.restore(MemIdMap(), str(tmp_path), entry)


def test_idmap_portable_backup_keeps_tokens_and_memberless_yuids(tmp_path, monkeypatch):
    m = fake_idmap(None)
    monkeypatch.setattr(m, "iter_members", lambda: iter([
        ("http://id.loc.gov/authorities/names/n1##quaPerson", f"{LUX}aaa")]))
    monkeypatch.setattr(m, "iter_yuid_registry", lambda: iter([
        (f"{LUX}aaa", "__20261027__", "2026-10-27T10:00:00"), (f"{LUX}bbb", None, None)]))
    entry = bk.backup(m, "map", "idmap", str(tmp_path), portable=True)
    assert entry["format"] == bk.IDMAP_JSONL
    assert entry["extra"] == {"members": 1, "yuids": 2}
    assert bk.verify(None, str(tmp_path), entry) == []

    # ... restored into the memory idmap
    mem = MemIdMap()
    assert bk.restore(mem, str(tmp_path), entry) == 1
    assert mem["http://id.loc.gov/authorities/names/n1##quaPerson"] == f"{LUX}aaa"
    assert f"{LUX}bbb" in mem
    assert list(mem.iter_yuid_registry()) == [(f"{LUX}aaa", "__20261027__", None),
                                              (f"{LUX}bbb", None, None)]


def test_idmap_load_portable_into_postgres_is_one_load_in_short_form(tmp_path, pg):
    members = [("http://id.loc.gov/authorities/names/n1##quaPerson", f"{LUX}aaa"),
               ("http://example.org/a\tb", f"{LUX}aaa")]
    registry = [(f"{LUX}aaa", "__20261027__", "2026-10-27T10:00:00"), (f"{LUX}bbb", None, None)]
    m = fake_idmap(pg)
    assert m.load_portable(iter(members), iter(registry)) == 2
    assert pg.loaded["idmap_yuid"] == ("yuid:aaa\t__20261027__\t2026-10-27T10:00:00\n"
                                       "yuid:bbb\t\\N\t\\N\n")
    assert pg.loaded["idmap"] == ("lcnaf:n1##quaPerson\tyuid:aaa\n"
                                  "http://example.org/a\\tb\tyuid:aaa\n")
    assert "TRUNCATE idmap_yuid, idmap" in pg.log and pg.log[-1] == "COMMIT"


def test_memory_idmap_portable_round_trip(tmp_path):
    src = MemIdMap()
    y = src.mint("http://test.example.com/1##quaPerson")
    src["http://test.example.com/2##quaPerson"] = y
    src._add(y, "__20261028__")
    entry = bk.backup(src, "map", "idmap", str(tmp_path))
    assert entry["format"] == bk.IDMAP_JSONL and entry["rows"] == 2
    dst = MemIdMap()
    dst.mint("http://test.example.com/old##quaPerson")
    bk.restore(dst, str(tmp_path), entry)
    assert dst.data == src.data


def test_a_postgres_cache_copies_its_own_table(tmp_path, monkeypatch):
    from lux_pipeline.storage.cache import postgres as pgcache
    conn = FakeConn({"aat_data_cache": "300001\t{}\n"},
                    {"aat_data_cache": ["identifier", "data"]},
                    {"aat_data_cache": [("aat_data_cache_pkey", "PRIMARY KEY (identifier)")]})
    monkeypatch.setattr(pgcache.psycopg2, "connect", lambda **kw: conn)
    c = object.__new__(pgcache.DataCache)
    c.name = "aat_data_cache"
    c.config = {"host": "", "user": "u", "dbname": "d"}
    entry = bk.backup(c, "cache", "aat", str(tmp_path), cache="datacache")
    assert (entry["format"], entry["rows"], entry["cache"]) == (bk.PG_COPY, 1, "datacache")
    assert entry["store_class"] == "storage.cache.postgres.DataCache"
    assert bk.restore(c, str(tmp_path), entry) == 1
    assert conn.loaded == {"aat_data_cache": "300001\t{}\n"}
    assert c.backup_size() == 4096


def test_a_postgres_reference_map_copies_its_own_table(tmp_path, pg):
    pg.tables["all_refs"] = "http://a\t1\tPerson\n"
    pg.columns["all_refs"] = ["uri", "dist", "ctype"]
    r = object.__new__(pgidmap.ReferenceMap)
    r.table = "all_refs"
    r._conn_kw = {}
    entry = bk.backup(r, "map", "all_refs", str(tmp_path))
    assert entry["format"] == bk.PG_COPY and entry["rows"] == 1
    bk.restore(r, str(tmp_path), entry)
    assert pg.loaded["all_refs"] == "http://a\t1\tPerson\n"


def store_classes():
    """Every concrete store class under lux_pipeline/storage (cache and idmap)."""
    import importlib
    import inspect
    out = []
    for mod in ("cache.postgres", "cache.redis", "cache.filesystem", "idmap.postgres",
                "idmap.redis", "idmap.memory", "idmap.filesystem", "idmap.lmdb",
                "idmap.subsidiary"):
        m = importlib.import_module(f"lux_pipeline.storage.{mod}")
        for name, cls in inspect.getmembers(m, inspect.isclass):
            if cls.__module__ != m.__name__ or name in ("PoolManager", "RedisDictValue",
                                                       "ReadOnlyMaster", "RedisCache"):
                continue
            if issubclass(cls, Exception):
                continue
            out.append(cls)
    return out


@pytest.mark.parametrize("cls", store_classes(), ids=lambda c: f"{c.__module__.split('.', 2)[2]}.{c.__name__}")
def test_every_store_class_backs_up_or_says_why_not(cls):
    if hasattr(cls, "backup_refused"):
        return          # says why (tested above for the two that do)
    readable = any(hasattr(cls, m) for m in ("backup", "iter_items", "items", "iter_keys"))
    writable = (hasattr(cls, "restore") or hasattr(cls, "load_portable")
                or (hasattr(cls, "clear") and (hasattr(cls, "set") or hasattr(cls, "update"))))
    assert readable and writable


# --- refusals ----------------------------------------------------------------------


def test_the_subsidiary_idmap_is_refused_naming_its_master(tmp_path):
    from lux_pipeline.storage.idmap.subsidiary import IdMap as Sub
    s = object.__new__(Sub)
    s.master_name = "idmap"
    with pytest.raises(bk.BackupRefused, match="back up the master map \\('idmap'\\)"):
        bk.backup(s, "map", "sub", str(tmp_path))
    with pytest.raises(bk.BackupRefused, match="master"):
        bk.check_restorable(s, {"format": bk.PG_COPY, "name": "sub"})


def test_the_redis_idmap_is_refused(tmp_path):
    from lux_pipeline.storage.idmap.redis import IdMap as RedisIdMap
    r = object.__new__(RedisIdMap)
    with pytest.raises(bk.BackupRefused, match="migrate-idmap.py"):
        bk.backup(r, "map", "idmap", str(tmp_path), portable=True)


def test_unknown_kind():
    with pytest.raises(ValueError, match="kind is one of"):
        bk.backup(MemIdMap(), "table", "x", "/nowhere")


# --- portable: caches and maps ------------------------------------------------------


def fs_cache(tmp_path, name):
    return FsCache({"name": name, "tabletype": "datacache", "base_dir": str(tmp_path)})


def test_cache_portable_round_trip_on_the_filesystem(tmp_path):
    (tmp_path / "a").mkdir()
    src = fs_cache(tmp_path / "a", "aat")
    for i in range(3):
        src.set({"id": f"aat:{i}", "type": "Type"}, identifier=f"30000{i}/x")
    entry = bk.backup(src, "cache", "aat", str(tmp_path / "bk"), cache="datacache")
    assert (entry["format"], entry["rows"], entry["cache"]) == (bk.JSONL, 3, "datacache")
    first = next(bk.read_jsonl(str(tmp_path / "bk" / entry["files"][0])))
    assert first == {"k": "300000/x", "v": {"data": {"id": "aat:0", "type": "Type"},
                                            "identifier": "300000/x"}}

    (tmp_path / "b").mkdir()
    dst = fs_cache(tmp_path / "b", "aat")
    dst.set({"id": "stale"}, identifier="stale")
    seen = []
    assert bk.restore(dst, str(tmp_path / "bk"), entry, progress=lambda **kw: seen.append(kw)) == 3
    assert sorted(dst.iter_keys()) == ["300000/x", "300001/x", "300002/x"]
    assert dst["300002/x"]["data"] == {"id": "aat:2", "type": "Type"}
    assert {"advance": 3} in seen and seen[-1] == {"done": True}
    assert bk.verify(dst, str(tmp_path / "bk"), entry) == []


class RefsSource:
    """The shape of either ReferenceMap's iter_items: key -> {dist, type}."""

    def iter_items(self):
        yield "http://a", {"dist": 1, "type": "Person"}
        yield "http://b", {"dist": 0, "type": None}


class Recorder:
    def __init__(self):
        self.calls = []
        self.cleared = False

    def clear(self):
        self.cleared = True

    def set(self, k, v):
        self.calls.append((k, v))


def test_a_reference_map_restores_into_another_backend(tmp_path):
    entry = bk.backup(RefsSource(), "map", "all_refs", str(tmp_path), portable=True)
    dst = Recorder()
    assert bk.restore(dst, str(tmp_path), entry) == 2
    assert dst.cleared
    assert dst.calls == [("http://a", {"dist": 1, "type": "Person"}),
                         ("http://b", {"dist": 0, "type": None})]


class MultiSource:
    """A set-valued map, as the redis MultiMap answers: keys and get()."""

    data = {"http://x": {"http://z", "http://y"}}

    def iter_keys(self):
        return iter(self.data)

    def get(self, k):
        return self.data[k]


def test_a_set_valued_map_is_written_sorted_and_restored_member_by_member(tmp_path):
    entry = bk.backup(MultiSource(), "map", "record_refs", str(tmp_path))
    assert next(bk.read_jsonl(str(tmp_path / entry["files"][0]))) == \
        {"k": "http://x", "v": ["http://y", "http://z"]}
    dst = Recorder()
    bk.restore(dst, str(tmp_path), entry)
    assert dst.calls == [("http://x", "http://y"), ("http://x", "http://z")]


def test_fs_map_portable_round_trip(tmp_path):
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    src = FsMap({"base_dir": str(tmp_path / "a"), "name": "redirects", "all_configs": None})
    src["http://a"] = "http://b"
    entry = bk.backup(src, "map", "redirects", str(tmp_path / "bk"))
    dst = FsMap({"base_dir": str(tmp_path / "b"), "name": "redirects", "all_configs": None})
    dst["old"] = "x"
    bk.restore(dst, str(tmp_path / "bk"), entry)
    assert dst.data == {"http://a": "http://b"}


# --- lmdb ----------------------------------------------------------------------------


def make_index(path, n=5):
    idx = TabLmdb.open(str(path), "c", map_size=2**22)
    idx.update({f"label {i}": [f"http://x/{i}", "Person"] for i in range(n)})
    return idx


def test_lmdb_native_round_trip(tmp_path):
    path = tmp_path / "idx" / "label_to_aat.lmdb"
    path.parent.mkdir()
    idx = make_index(path)
    entry = bk.backup(idx, "index", "aat", str(tmp_path / "bk"), stem="aat.reconcile_label",
                      index="reconcile_label")
    assert (entry["format"], entry["rows"], entry["files"]) == \
        (bk.LMDB_COPY, 5, ["aat.reconcile_label.lmdb"])
    assert entry["store_class"] == "storage.idmap.lmdb.TabLmdb"
    assert bk.verify(None, str(tmp_path / "bk"), entry) == []

    idx.clear()
    idx["other"] = "x"
    assert bk.restore(idx, str(tmp_path / "bk"), entry) == 5
    again = TabLmdb.open(str(path), "r")
    assert dict(again.items()) == {f"label {i}": [f"http://x/{i}", "Person"] for i in range(5)}
    again.close()


def test_lmdb_portable_restores_into_another_lmdb_class(tmp_path):
    idx = make_index(tmp_path / "a.lmdb", n=3)
    entry = bk.backup(idx, "index", "aat", str(tmp_path / "bk"), portable=True)
    assert entry["format"] == bk.JSONL and entry["rows"] == 3
    idx.close()
    dst = JsonLmdb.open(str(tmp_path / "b.lmdb"), "c", map_size=2**22)
    bk.restore(dst, str(tmp_path / "bk"), entry)
    assert dst["label 1"] == ["http://x/1", "Person"]
    assert len(dst) == 3
    dst.close()


# --- manifest and verify ----------------------------------------------------------


def test_manifest_round_trip_and_refusals(tmp_path):
    m = bk.new_manifest("before-clear")
    assert m["version"] == 1 and m["label"] == "before-clear" and m["entries"] == []
    m["entries"].append({"name": "x"})
    bk.write_manifest(str(tmp_path), m)
    assert bk.read_manifest(str(tmp_path)) == json.loads(json.dumps(m))
    with pytest.raises(ValueError, match="did not finish"):
        bk.read_manifest(str(tmp_path / "nothing"))
    m["version"] = 99
    bk.write_manifest(str(tmp_path), m)
    with pytest.raises(ValueError, match="version 99"):
        bk.read_manifest(str(tmp_path))


def test_verify_finds_missing_and_short_files_and_changed_values(tmp_path, pg):
    entry = bk.backup(fake_idmap(pg), "map", "idmap", str(tmp_path))
    entry["extra"]["tables"][0]["rows"] = 3
    assert bk.verify(None, str(tmp_path), entry) == ["idmap.idmap.copy.gz: 2 rows, manifest says 3"]
    os.remove(tmp_path / "idmap.idmap_yuid.copy.gz")
    assert bk.verify(None, str(tmp_path), entry) == ["idmap.idmap_yuid.copy.gz is missing"]

    src = FsMap({"base_dir": str(tmp_path), "name": "m", "all_configs": None})
    src["a"] = "1"
    src["b"] = "2"
    entry = bk.backup(src, "map", "m", str(tmp_path / "bk"))
    assert bk.verify(src, str(tmp_path / "bk"), entry) == []
    src["b"] = "3"
    assert bk.verify(src, str(tmp_path / "bk"), entry, rng=random.Random(1)) == \
        ["b: the store no longer matches the backup"]


def test_values_are_written_as_json(tmp_path):
    t = datetime.datetime(2026, 10, 28, 9, 30)
    assert bk.dumps({"t": t, "s": {"b", "a"}}) == '{"t":"2026-10-28T09:30:00","s":["a","b"]}'
    assert bk.copy_text(None) == "\\N"
    assert bk.copy_text("a\\b\tc\nd") == "a\\\\b\\tc\\nd"


def test_line_reader_reads_across_lines():
    r = bk.LineReader(iter(["ab\n", "cde\n", "f\n"]))
    assert r.read(3) == b"ab\n"
    assert r.readline() == b"cde\n"
    assert r.read() == b"f\n"
    assert r.read(5) == b""
