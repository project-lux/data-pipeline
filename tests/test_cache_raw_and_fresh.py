"""PooledCache.get_fresh() and the raw= (unparsed JSON) select contract.

export used to ask three or four questions per record -- has_item, metadata,
has_item again, get -- to decide whether its cached MarkLogic document was
still current, then parse the answer out of jsonb and re-serialise it to the
same bytes. get_fresh(raw=True) answers all of it in one query and hands back
the text postgres already holds.

The risk in raw= is silence: a caller that asks for text and gets parsed
objects will json.loads() a dict. These tests pin the generated SQL and the
failure mode. No live postgres -- the cursor is stubbed.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest

from pipeline.storage.cache.postgres import PoolManager, PooledCache

COLUMNS = ["yuid", "insert_time", "record_time", "refresh_time", "valid", "change", "data"]


class Cursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass

    def execute(self, qry, params=None):
        self.conn.queries.append((" ".join(qry.split()), params))

    def fetchall(self):
        return [{"column_name": c} for c in COLUMNS]

    def fetchone(self):
        return self.conn.row

    def __iter__(self):
        return iter(self.conn.rows)


class Conn:
    def __init__(self):
        self.queries = []
        self.row = None
        self.rows = []

    def cursor(self, **kw):
        return Cursor(self)

    def commit(self):
        pass

    def rollback(self):
        pass


def cache():
    c = object.__new__(PooledCache)
    c.name, c.key = "marklogic_merged_record_cache", "yuid"
    c.config = {"name": "marklogic", "overwrite": True, "cursor_size": 1000}
    c._cols = None
    c.pools = PoolManager()
    c.pool_name = "p"
    c.conn = Conn()
    c.pools.conn = c.conn
    # one Conn for both roles so every query lands in the same log
    c.iterating_conn = c.conn
    c.pools.iterating_conn = c.conn
    return c


YUID = "0123456789abcdef0123456789abcdef0123"   # 36 chars, as the guard expects


def test_raw_select_replaces_only_the_data_column():
    c = cache()
    cols = c._select_list(raw=True)
    assert "data::text AS data" in cols
    # the bare column must not also be selected: postgres would send the
    # jsonb too and psycopg2 would parse it, which is the cost being avoided
    assert ", data," not in f", {cols},"
    for col in COLUMNS:
        assert col in cols


def test_plain_select_costs_no_column_lookup():
    c = cache()
    assert c._select_list() == "*"
    assert c.conn.queries == []


def test_column_list_is_read_once():
    c = cache()
    c._select_list(raw=True)
    c._select_list(raw=True)
    lookups = [q for q, _ in c.conn.queries if "information_schema" in q]
    assert len(lookups) == 1


def test_raw_fails_loudly_rather_than_returning_parsed_data():
    # a caller that asked for text is about to json.loads() the result;
    # silently handing back objects would be a confusing TypeError far away
    c = cache()
    c._cols = []
    with pytest.raises(ValueError, match="raw select"):
        c._select_list(raw=True)


def test_get_fresh_is_one_query_carrying_the_freshness_test():
    c = cache()
    c.conn.row = {"yuid": YUID, "data": '{"json":1}'}
    got = c.get_fresh(YUID, since="2026-07-30T00:00:00", raw=True)
    qs = [q for q, _ in c.conn.queries if "information_schema" not in q]
    assert len(qs) == 1
    assert "insert_time >= %s" in qs[0]
    assert "data::text AS data" in qs[0]
    assert got["data"] == '{"json":1}'
    assert got["source"] == "marklogic"


def test_get_fresh_without_since_accepts_any_cached_row():
    c = cache()
    c.conn.row = {"yuid": YUID, "data": '{"json":1}'}
    c.get_fresh(YUID, since=None)
    qs = [q for q, _ in c.conn.queries if "information_schema" not in q]
    assert "insert_time" not in qs[0]


def test_get_fresh_returns_none_when_stale_or_absent():
    c = cache()
    c.conn.row = None      # no row matched the key + freshness predicate
    assert c.get_fresh(YUID, since="2026-07-30T00:00:00") is None


def test_iter_records_slice_passes_raw_through():
    c = cache()
    list(c.iter_records_slice(3, 24, raw=True))
    qs = [q for q, _ in c.conn.queries if "information_schema" not in q]
    assert "data::text AS data" in qs[0]
    assert "hashtext" in qs[0]
