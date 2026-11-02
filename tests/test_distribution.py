"""LMDB distribution stores (lux_pipeline.storage.distribution): sorted runs,
the in-order merge, both key forms and their index blocks, the description
in ``meta``, a failed build leaving the old store, and the loader reading a
text-keyed store back."""

import json
import os
import random
import sys
import uuid
import zlib
from pathlib import Path

import lmdb
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.storage import distribution as D


def uuids(n, seed=1):
    rnd = random.Random(seed)
    return [str(uuid.UUID(int=rnd.getrandbits(128), version=4)) for _ in range(n)]


def write_runs(runs, records, workers=3, key_format="uuid", source="merged", run_bytes=4000):
    """`records`: ``[(identifier, class or None)]``, shared out round robin
    as workers would get hash slices: in no particular order."""
    for w in range(workers):
        with D.RunWriter(str(runs), w, key_format, run_bytes=run_bytes) as rw:
            for ident, cls in records[w::workers]:
                rec = {"id": ident, **({"type": cls} if cls else {})}
                rw.add(source, ident, json.dumps(rec))


def store_keys(path):
    env = lmdb.open(str(path), max_dbs=3, readonly=True, lock=False)
    with env.begin() as txn:
        keys = [bytes(k) for k in txn.cursor(db=env.open_db(D.DATA_DB, txn=txn)).iternext(values=False)]
    env.close()
    return keys


# --- keys --------------------------------------------------------------------


def test_uuid_keys_take_the_last_path_segment():
    u = "4f0c6a8e-1d2b-4c3d-9e8f-0a1b2c3d4e5f"
    assert D.uuid_key(u) == uuid.UUID(u).bytes
    assert D.uuid_key(f"https://lux.collections.yale.edu/data/person/{u}") == uuid.UUID(u).bytes
    with pytest.raises(ValueError, match="not a UUID.*use text keys"):
        D.uuid_key("sh85000001")


def test_text_keys_and_decoding():
    k = D.make_key("text", "lcsh", "sh85000001##quaType")
    assert k == b"lcsh:sh85000001##quaType"
    assert D.decode_key("text", k) == "lcsh:sh85000001##quaType"
    u = uuids(1)[0]
    assert D.decode_key("uuid", D.make_key("uuid", "merged", u)) == u
    with pytest.raises(ValueError, match="unknown key format"):
        D.make_key("int", "x", "1")


def test_record_class():
    assert D.record_class('{"type": "Person"}') == "Person"
    assert D.record_class('{"type": ["Person"]}') is None
    assert D.record_class('{"id": 1}') is None
    assert D.record_class("not json") is None


# --- runs --------------------------------------------------------------------


def test_runs_are_sorted_and_spill_by_size(tmp_path):
    ids = uuids(200)
    with D.RunWriter(str(tmp_path), 0, "uuid", run_bytes=3000) as rw:
        for u in ids:
            rw.add("merged", u, {"id": u, "type": "Person"})      # a dict is serialised
    assert rw.stats["records"] == 200 and rw.stats["runs"] > 1
    total = 0
    assert json.loads((tmp_path / "w0000.done").read_text())["records"] == 200
    for fn in sorted(f for f in os.listdir(tmp_path) if f.endswith(".run")):
        assert fn.startswith("w0000-")
        rows = list(D.read_run(tmp_path / fn))
        keys = [r[0] for r in rows]
        assert keys == sorted(keys)
        assert all(r[1] == b"Person" for r in rows)
        assert json.loads(zlib.decompress(rows[0][2]))["type"] == "Person"
        total += len(rows)
    assert total == 200


def test_a_rerun_slice_replaces_only_its_own_runs(tmp_path):
    write_runs(tmp_path, [(u, None) for u in uuids(60)], workers=2, run_bytes=500)
    others = D.worker_runs(str(tmp_path), 1)
    with D.RunWriter(str(tmp_path), 0, "uuid") as rw:
        rw.add("merged", uuids(1, seed=9)[0], "{}")
    assert D.worker_runs(str(tmp_path), 0) == ["w0000-00000.run"]
    assert D.missing_workers(str(tmp_path), 3) == [2]
    assert D.worker_runs(str(tmp_path), 1) == others


def test_text_keys_too_long_are_skipped(tmp_path):
    with D.RunWriter(str(tmp_path), 0, "text") as rw:
        assert rw.add("src", "x" * 600, "{}") is False
        assert rw.add("src", "ok", "{}") is True
    assert rw.stats["too_long"] == 1 and rw.stats["records"] == 1


# --- building ------------------------------------------------------------------


CLASSES = ["Person", "Place", "Type", None]


def test_uuid_store_from_unordered_slices(tmp_path):
    rnd = random.Random(3)
    records = [(u, rnd.choice(CLASSES)) for u in uuids(3000)]
    write_runs(tmp_path / "runs", records)
    st = D.build_store(str(tmp_path / "merged.lmdb"), str(tmp_path / "runs"), "uuid",
                       meta={"export": "merged", "sources": ["merged"]})
    assert (st["records"], st["duplicates"]) == (3000, 0)
    assert not (tmp_path / "runs").exists() and not (tmp_path / "merged.lmdb.tmp").exists()
    # every record, in key order
    keys = store_keys(tmp_path / "merged.lmdb")
    assert keys == sorted(D.uuid_key(u) for u, _ in records)
    with D.DistributionStore(str(tmp_path / "merged.lmdb")) as s:
        m = s.meta
        assert (m["format"], m["keys"], m["compression"], m["export"]) == (1, "uuid", "zlib", "merged")
        assert m["index"] == {"block_keys": 30, "key_bytes": 16}
        assert m["unindexed"] == sum(1 for _, c in records if c is None)
        for cls in ("Person", "Place", "Type"):
            want = sorted(D.uuid_key(u) for u, c in records if c == cls)
            assert list(s.keys(cls)) == want and m["classes"][cls] == len(want)
        # blocks of at most 30 keys
        with s.env.begin() as txn:
            cur = txn.cursor(db=s.index)
            assert cur.set_key(b"Person")
            sizes = [len(b) for b in cur.iternext_dup()]
        assert max(sizes) == 30 * 16 and all(n % 16 == 0 for n in sizes)
        u, cls = records[0]
        assert s.get(u) == {"id": u, **({"type": cls} if cls else {})}
        assert s.get(D.uuid_key(u))["id"] == u
        k, rec = next(s.records("Place"))
        assert rec["type"] == "Place" and s.get(k) == rec


def test_text_store_blocks_stay_under_the_limit_and_in_order(tmp_path):
    # Q1 and Q10: a key that is a prefix of the next still sorts first,
    # inside a block and between blocks, because NUL is the lowest byte
    ids = [f"Q{n}##quaPerson" for n in range(1, 400)]
    random.Random(5).shuffle(ids)
    write_runs(tmp_path / "runs", [(i, "Person") for i in ids], workers=4, key_format="text",
               source="wikidata")
    D.build_store(str(tmp_path / "s.lmdb"), str(tmp_path / "runs"), "text")
    with D.DistributionStore(str(tmp_path / "s.lmdb")) as s:
        assert s.meta["index"] == {"block_bytes": 500, "separator": "\\u0000"}
        keys = list(s.keys("Person"))
        assert keys == sorted(f"wikidata:{i}".encode() for i in ids)
        with s.env.begin() as txn:
            cur = txn.cursor(db=s.index)
            cur.set_key(b"Person")
            blocks = [bytes(b) for b in cur.iternext_dup()]
        assert all(len(b) <= 500 for b in blocks) and len(blocks) > 1
        assert s.get("wikidata:Q10##quaPerson")["id"] == "Q10##quaPerson"


def test_duplicate_keys_keep_the_first(tmp_path, caplog):
    u = uuids(1)[0]
    write_runs(tmp_path / "runs", [(u, "Person"), (u, "Person")], workers=2)
    with caplog.at_level("WARNING", logger="lux_pipeline"):
        st = D.build_store(str(tmp_path / "s.lmdb"), str(tmp_path / "runs"), "uuid")
    assert (st["records"], st["duplicates"]) == (1, 1)
    assert "1 duplicate keys" in caplog.text
    with D.DistributionStore(str(tmp_path / "s.lmdb")) as s:
        assert len(list(s.keys("Person"))) == 1


def test_a_failed_build_leaves_the_old_store(tmp_path, monkeypatch):
    write_runs(tmp_path / "runs", [(u, "Person") for u in uuids(50)])
    D.build_store(str(tmp_path / "s.lmdb"), str(tmp_path / "runs"), "uuid")
    write_runs(tmp_path / "runs", [(u, "Place") for u in uuids(50, seed=2)])

    def broken(path):
        yield from []
        raise OSError("disk full")
    monkeypatch.setattr(D, "read_run", broken)
    with pytest.raises(OSError, match="disk full"):
        D.build_store(str(tmp_path / "s.lmdb"), str(tmp_path / "runs"), "uuid")
    assert not (tmp_path / "s.lmdb.tmp").exists()
    assert (tmp_path / "runs").exists()              # kept, to merge again
    with D.DistributionStore(str(tmp_path / "s.lmdb")) as s:
        assert s.meta["classes"] == {"Person": 50}


def test_a_full_map_says_how_to_retry(tmp_path, monkeypatch):
    write_runs(tmp_path / "runs", [(u, "Person") for u in uuids(3000)])
    monkeypatch.setenv("LUX_LMDB_MAP_GB", "0.0001")
    with pytest.raises(RuntimeError, match="set LUX_LMDB_MAP_GB higher.*--merge-only"):
        D.build_store(str(tmp_path / "s.lmdb"), str(tmp_path / "runs"), "uuid")
    assert not (tmp_path / "s.lmdb").exists() and (tmp_path / "runs").exists()


# --- the loader reads a text store back -------------------------------------


def test_the_loader_reads_a_text_store(tmp_path):
    from lux_pipeline.process.base.loader import Loader
    ids = ["000025p04", "05dxps055", "013fj3d42"]
    with D.RunWriter(str(tmp_path / "runs"), 0, "text") as rw:
        for i in ids:
            rw.add("ror", i, {"id": f"https://ror.org/{i}"})
        rw.add("other", "x", {"id": "not ror"})
    D.build_store(str(tmp_path / "s.lmdb"), str(tmp_path / "runs"), "text")
    ldr = Loader.__new__(Loader)
    ldr.name = "ror"
    ldr._lmdb_envs = {}
    got = {}
    for ptr in ldr.iterate_lmdb(str(tmp_path / "s.lmdb"), None, []):
        with ldr.file_opener(ptr, "z") as fh:
            got[ptr.get_name()] = json.loads(fh.read())
    assert got == {i: {"id": f"https://ror.org/{i}"} for i in ids}
