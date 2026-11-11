"""Identifier columns are text, not VARCHAR(n).

The width bought nothing in Postgres and cost records: the datacache took
identifiers up to 256 characters, its recordcache 120 (less the ##qua
suffix it adds), so one in between loaded and then failed to store. New
tables declare text; `identifier_to_text` (``lux pg-maintain
--identifier-text``) changes an old one in place.

No live postgres -- the connection is stubbed.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.storage.cache import postgres
from lux_pipeline.storage.cache.postgres import PooledCache, PoolManager

CACHE_CLASSES = ("DataCache", "InternalRecordCache", "ExternalRecordCache",
                 "ExternalReconciledRecordCache", "RecordCache")


class Cursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass

    def execute(self, qry, params=None):
        self.conn.executed.append(" ".join(qry.split()))

    def fetchone(self):
        return self.conn.column


class Conn:
    closed = False

    def __init__(self, column):
        self.autocommit = True
        self.column = column
        self.executed = []

    def cursor(self, **kw):
        return Cursor(self)

    def commit(self):
        pass


def cache(column):
    c = object.__new__(PooledCache)
    c.name, c.key = "ycba_data_cache", "identifier"
    c.config = {"name": "ycba"}
    c._batch = {}
    c._col_types = {"identifier": "character varying"}
    c.pools = PoolManager()
    c.pool_name = "p"
    c.conn = Conn(column)
    c.pools.conn = c.conn
    return c


def alters(c):
    return [q for q in c.conn.executed if q.startswith("ALTER")]


@pytest.mark.parametrize("name", CACHE_CLASSES)
def test_new_tables_declare_the_identifier_text(name, monkeypatch):
    # the classes set pk_defn before PooledCache.__init__ connects
    seen = {}
    monkeypatch.setattr(PooledCache, "__init__",
                        lambda self, config: seen.setdefault("pk", self.pk_defn))
    getattr(postgres, name)({})
    assert "identifier TEXT" in seen["pk"]
    assert "VARCHAR" not in seen["pk"].upper()


def test_a_varchar_identifier_is_altered_to_text():
    c = cache({"data_type": "character varying", "width": 120})
    assert c.identifier_to_text() is True
    assert alters(c) == ["ALTER TABLE ycba_data_cache ALTER COLUMN identifier TYPE text"]
    # the remembered column type is dropped, not left saying varchar
    assert "identifier" not in c._col_types


def test_a_text_identifier_is_left_alone():
    c = cache({"data_type": "text", "width": None})
    assert c.identifier_to_text() is False
    assert alters(c) == []


def test_a_table_without_an_identifier_is_left_alone():
    # the merged record cache keys on yuid alone
    c = cache(None)
    assert c.identifier_to_text() is False
    assert alters(c) == []


def test_it_runs_outside_a_transaction_and_puts_autocommit_back():
    c = cache({"data_type": "character varying", "width": 256})
    c.conn.autocommit = False
    c.identifier_to_text()
    assert c.conn.autocommit is False
