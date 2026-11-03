"""AbstractCache, the interface every record cache implements, and FsCache
honouring all of it (item 38)."""

import datetime
import inspect
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest

from lux_pipeline.storage.cache.abstract import AbstractCache
from lux_pipeline.storage.cache.filesystem import FsCache, MergedRecordCache
from lux_pipeline.storage.cache.postgres import PooledCache

from test_cache_raw_and_fresh import cache as pg_cache


def required():
    """The methods AbstractCache leaves to the backend."""
    return [n for n, f in vars(AbstractCache).items()
            if callable(f) and "raise NotImplementedError()" in inspect.getsource(f)]


@pytest.mark.parametrize("cls", [PooledCache, FsCache])
def test_every_backend_provides_the_required_methods(cls):
    assert len(required()) > 10
    missing = [n for n in required() if getattr(cls, n) is getattr(AbstractCache, n)]
    assert missing == []


@pytest.mark.parametrize("name", ["backup", "restore", "backup_size", "get_like", "optimize",
                                  "bloat", "rewrite", "drop_time_index"])
def test_optional_capabilities_are_not_in_the_interface(name):
    """storage/backup.py and others find these with getattr; a stub here
    would make FsCache look like it had native backup."""
    assert not hasattr(AbstractCache, name)
    assert not hasattr(FsCache, name)


def test_postgres_set_still_checks_its_arguments():
    c = pg_cache()
    with pytest.raises(ValueError, match="Must give YUID or Identifier"):
        c.set({"a": 1})
    with pytest.raises(ValueError, match="Data must be a dict"):
        c.set("text", identifier="x")


def test_postgres_set_writes_the_same_row():
    c = pg_cache()
    c.conn.autocommit = True
    c.set({"a": 1}, yuid=12345)
    qry, params = c.conn.queries[-1]
    assert qry.startswith("INSERT INTO marklogic_merged_record_cache "
                          "(data,yuid,insert_time,record_time,refresh_time)")
    # the yuid became a string; record_time defaults to insert_time
    assert params[1] == "12345" and params[2] == params[3]
    assert params[4] == "9999-12-31T00:00:00Z"


# --- FsCache ---------------------------------------------------------------


@pytest.fixture
def fs(tmp_path):
    return FsCache({"name": "aat", "tabletype": "datacache", "base_dir": str(tmp_path)})


def fill(c, n=6):
    for i in range(n):
        c.set({"id": f"aat:{i}", "type": "Type" if i % 2 else "Place"}, identifier=f"3000{i}/x")


def at(c, key, when):
    c.set_metadata(key, "record_time", when)


def test_reads_and_writes(fs):
    fill(fs, 3)
    assert len(fs) == 3 and fs.len_estimate() == 3
    row = fs.get("30001/x")
    assert row["data"] == {"id": "aat:1", "type": "Type"}
    assert row["identifier"] == "30001/x" and row["source"] == "aat"
    assert isinstance(row["insert_time"], datetime.datetime)
    assert fs.get("30001/x", raw=True)["data"] == '{"id":"aat:1","type":"Type"}'
    assert "30001/x" in fs and "nope" not in fs
    fs["30009/x"] = {"data": {"id": "aat:9"}, "identifier": "30009/x"}
    assert fs["30009/x"]["data"] == {"id": "aat:9"}
    with pytest.raises(ValueError, match="different"):
        fs["30009/x"] = {"data": {}, "identifier": "other"}
    del fs["30009/x"]
    assert fs.get("30009/x") is None


def test_a_miss_is_not_a_warning(fs, caplog):
    assert fs.get("nope") is None
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_batch_reads(fs):
    fill(fs, 3)
    assert set(fs.get_multi(["30000/x", "gone", "30002/x"])) == {"30000/x", "30002/x"}
    assert fs.get_multi(["30000/x"], raw=True)["30000/x"]["data"].startswith("{")
    assert fs.has_multi(["30000/x", "gone"]) == {"30000/x"}


def test_times_come_from_the_file(fs):
    assert fs.latest() == "0000-01-01T00:00:00"
    fill(fs, 3)
    at(fs, "30000/x", "2020-01-01T00:00:00")
    at(fs, "30001/x", datetime.datetime(2021, 1, 1))
    at(fs, "30002/x", "2022-01-01T00:00:00Z")
    assert fs.metadata("30001/x", "record_time") == {"record_time": datetime.datetime(2021, 1, 1)}
    assert fs.metadata("30001/x")["insert_time"] == datetime.datetime(2021, 1, 1)
    assert fs.metadata("gone") is None
    # a UTC time becomes the local time a file keeps
    utc = datetime.datetime(2022, 1, 1, tzinfo=datetime.timezone.utc)
    assert fs.latest() == utc.astimezone().replace(tzinfo=None).isoformat()
    assert fs.has_item("30001/x", timestamp="2020-06-01")
    assert not fs.has_item("30000/x", timestamp="2020-06-01")
    # newest first, as postgres orders them
    assert list(fs.iter_keys_since("2020-06-01")) == ["30002/x", "30001/x"]
    assert [r["identifier"] for r in fs.iter_records_since()] == ["30002/x", "30001/x", "30000/x"]
    assert fs.get_fresh("30001/x", since=datetime.datetime(2020, 6, 1))["data"]["id"] == "aat:1"
    assert fs.get_fresh("30000/x", since=datetime.datetime(2020, 6, 1)) is None
    assert fs.get_fresh("30000/x")["data"]["id"] == "aat:0"


def test_set_with_a_record_time(fs):
    fs.set({"id": "aat:0"}, identifier="a", record_time="2019-05-01T12:00:00")
    assert fs.metadata("a", "record_time")["record_time"] == datetime.datetime(2019, 5, 1, 12)


def test_the_other_metadata_lives_beside_the_record(fs, tmp_path):
    fill(fs, 2)
    assert fs.metadata("30000/x", "change") == {"change": None}
    fs.set_metadata("30000/x", "change", "portal")
    fs.set_metadata("30000/x", "valid", True)
    assert fs.metadata("30000/x", "change") == {"change": "portal"}
    row = fs.get("30000/x")
    assert row["change"] == "portal" and row["valid"] is True
    # not a record of its own
    assert len(fs) == 2 and sorted(fs.iter_keys()) == ["30000/x", "30001/x"]
    # a later set() replaces it
    fs.set({"id": "aat:0"}, identifier="30000/x", change="update")
    assert fs.get("30000/x")["change"] == "update" and "valid" not in fs.get("30000/x")
    fs.set({"id": "aat:0"}, identifier="30000/x")
    assert not os.path.exists(fs._path("30000/x") + ".meta")
    fs.set({"id": "aat:1"}, identifier="30001/x", valid=False)
    fs.delete("30001/x")
    assert not os.path.exists(fs._path("30001/x") + ".meta")
    fs.set({"id": "aat:1"}, identifier="30001/x", valid=False)
    fs.clear()
    assert os.listdir(fs.directory) == []


def test_metadata_fields_are_checked(fs):
    fill(fs, 1)
    with pytest.raises(ValueError, match="Unknown metadata field"):
        fs.metadata("30000/x", "colour")
    with pytest.raises(ValueError, match="unsettable"):
        fs.set_metadata("30000/x", "insert_time", "2020-01-01")
    assert fs.set_metadata("gone", "change", "x") is None
    assert not os.path.exists(fs._path("gone") + ".meta")


def test_slices_partition_the_records(fs):
    fill(fs, 7)
    keys = [list(fs.iter_keys_slice(n, 3)) for n in range(3)]
    assert sorted(sum(keys, [])) == sorted(fs.iter_keys())
    assert all(set(a).isdisjoint(b) for a in keys for b in keys if a is not b)
    assert [r["identifier"] for r in fs.iter_records_slice(1, 3)] == keys[1]
    assert sorted(sum((list(fs.iter_keys_slice_mem(n, 3)) for n in range(3)), [])) == \
        sorted(fs.iter_keys())
    with pytest.raises(ValueError):
        list(fs.iter_keys_slice(3, 3))


def test_records_by_type(fs):
    fill(fs, 6)
    fs.set({"id": "aat:9", "type": "Language"}, identifier="lang")
    assert sorted(fs.iter_keys_type("Place")) == ["30000/x", "30002/x", "30004/x"]
    assert sorted(fs.iter_keys_type("Concept")) == ["30001/x", "30003/x", "30005/x", "lang"]
    assert [r["identifier"] for r in fs.iter_records_type("Language")] == ["lang"]
    parts = [list(fs.iter_keys_type_slice("Place", n, 2)) for n in range(2)]
    assert sorted(sum(parts, [])) == ["30000/x", "30002/x", "30004/x"]


def test_bulk_and_batching_are_plain_writes(fs):
    fs.defer_commits(every=10, batch=5)
    fs.start_bulk()
    fs.set_bulk({"id": "aat:0"}, identifier="b")
    fs.end_bulk()
    fs.checkpoint()
    fs.flush()
    fs.resume_commits()
    fs.commit()
    fs.shutdown()
    assert fs["b"]["data"] == {"id": "aat:0"}
    assert dict(fs.iter_items())["b"]["data"] == {"id": "aat:0"}


def test_a_yuid_keyed_cache(tmp_path):
    c = MergedRecordCache({"name": "merged", "base_dir": str(tmp_path)})
    y = "0123456789abcdef0123456789abcdef0123"
    c[y] = {"id": "x"}
    row = c.get(y)
    assert (row["yuid"], row["data"]) == (y, {"id": "x"})
    assert list(c.iter_items()) == [(y, c.get(y))]
