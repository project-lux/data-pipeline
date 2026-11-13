"""The networkmap on Postgres: what a fetch got for a URL, with errors that
expire, as storage.idmap.redis.NetworkOperationMap keeps it in Redis. An
installation without Redis (``lux initialize --no-redis``) uses it."""

import datetime
import sys
from argparse import Namespace
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest

from lux_pipeline.config import importObject
from lux_pipeline.storage.idmap import postgres as pgmap
from lux_pipeline.storage.idmap.postgres import NETWORK_ERROR_TTL, NetworkOperationMap

from test_idmap_indexes import Conn


@pytest.fixture
def conn(monkeypatch):
    c = Conn()
    monkeypatch.setattr(pgmap, "_connect_kwargs", lambda config, cfgs: {})
    monkeypatch.setattr(pgmap, "_connect", lambda kw: c)
    return c


def make(**record):
    return NetworkOperationMap({"all_configs": Namespace(), "name": "networkmap", **record})


def test_the_table_is_created_and_named_for_the_map(conn):
    make()
    assert conn.executed[0].startswith("CREATE TABLE IF NOT EXISTS networkmap (")
    m = make(tableName="netmap_scratch")
    assert m.table == "netmap_scratch" and "netmap_scratch" in conn.executed[-1]


@pytest.mark.parametrize("value, stored, error", [
    (0, "000", True), (None, "000", True), (404, "404", True), ("500", "500", True),
    (200, "200", False), ("Q42", "Q42", False), ("300404670", "300404670", False),
])
def test_values_as_the_redis_store_writes_them(conn, value, stored, error):
    m = make()
    before = datetime.datetime.now(datetime.timezone.utc)
    m["http://example.org/x"] = value
    sql, (key, got, expires) = conn.executed[-1], conn.params[-1]
    assert sql.startswith("INSERT INTO networkmap (key, value, expires)") and "ON CONFLICT (key)" in sql
    assert (key, got) == ("http://example.org/x", stored)
    if error:
        assert expires - before >= datetime.timedelta(seconds=NETWORK_ERROR_TTL - 5)
    else:
        assert expires is None


def test_reads_skip_expired_rows(conn):
    m = make()
    conn.answers = [("404",), None, (1,), (7,)]
    assert m["http://a"] == "404"
    assert m.get("http://b") is None
    assert "http://a" in m
    assert len(m) == 7
    reads = conn.executed[1:]
    assert all("(expires IS NULL OR expires > now())" in q for q in reads)


def test_delete_and_clear(conn):
    m = make()
    del m["http://a"]
    m.clear()
    assert conn.executed[1:] == ["DELETE FROM networkmap WHERE key = %s", "TRUNCATE networkmap"]


def test_the_class_resolves_from_a_map_record():
    assert importObject("storage.idmap.postgres.NetworkOperationMap") is NetworkOperationMap
