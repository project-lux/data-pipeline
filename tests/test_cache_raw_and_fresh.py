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
        if self.conn.rows_all is not None:
            return self.conn.rows_all
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
        # None means "answer the column-name lookup"; a list is what the
        # query under test should see
        self.rows_all = None

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


# --- has_multi ---------------------------------------------------------------
#
# merge's claim_member() walked every internal member of a cluster to find the
# smallest one that still exists, one has_item() round trip per candidate:
# 1.2M calls at 28ms each over a 100.7M record merge, the worst per-call cost
# in the phase by a factor of seven. One query answers all of them.

def test_has_multi_asks_once_for_every_key():
    c = cache()
    c.conn.rows_all = [{"yuid": YUID}]
    got = c.has_multi([YUID, YUID.replace("0", "1")])
    assert got == {YUID}
    (qry, params) = c.conn.queries[-1]
    assert qry == f"SELECT yuid FROM {c.name} WHERE yuid = ANY(%s)"
    assert len(params[0]) == 2


def test_has_multi_selects_no_payload():
    """A cluster's members can be large records; presence must not drag the
    data column back with it."""
    c = cache()
    c.conn.rows_all = []
    c.has_multi([YUID])
    (qry, _) = c.conn.queries[-1]
    assert "data" not in qry
    assert "SELECT yuid" in qry


def test_has_multi_keeps_has_items_key_guard():
    """yuid keys that are not 36 characters never match a row, and get_multi
    drops them rather than sending them."""
    c = cache()
    c.conn.rows_all = []
    assert c.has_multi(["too-short"]) == set()
    assert c.conn.queries == []          # nothing worth asking


def test_has_multi_of_nothing_asks_nothing():
    c = cache()
    assert c.has_multi([]) == set()
    assert c.conn.queries == []


def test_has_multi_can_be_asked_about_another_column():
    c = cache()
    c.conn.rows_all = [{"identifier": "abc"}]
    assert c.has_multi(["abc", "def"], _key_type="identifier") == {"abc"}
    (qry, _) = c.conn.queries[-1]
    assert qry == f"SELECT identifier FROM {c.name} WHERE identifier = ANY(%s)"


# --- typed parameters for ANY() ---------------------------------------------
#
# `col = %s` works on a uuid column because postgres resolves a single
# unknown literal to the column's type. `col = ANY(%s)` does not: psycopg2
# sends a text[] and there is no `uuid = text` operator. This shipped, and
# merge died on the first batch with
#
#   psycopg2.errors.UndefinedFunction: operator does not exist: uuid = text
#   LINE 1: ...yuid FROM merged_merged_record_cache WHERE yuid = ANY(ARRA...
#
# The stubbed cursor cannot catch a type mismatch, so what these pin is the
# generated SQL. The cast goes on the *parameter*: `col::text = ANY(...)`
# would answer correctly and sequential-scan the table to do it.

def uuid_keyed(c):
    """Make the stubbed information_schema lookup report a uuid column."""
    c.conn.row = {"data_type": "uuid"}
    c.conn.rows_all = []
    return c


def test_has_multi_casts_the_parameter_for_a_uuid_key():
    c = uuid_keyed(cache())
    c.has_multi([YUID])
    (qry, _) = c.conn.queries[-1]
    assert "= ANY(%s::uuid[])" in qry


def test_get_multi_casts_it_too():
    """Same ANY(), same latent break -- it had just never been called on a
    uuid-keyed cache."""
    c = uuid_keyed(cache())
    c.get_multi([YUID])
    (qry, _) = c.conn.queries[-1]
    assert "= ANY(%s::uuid[])" in qry


def test_a_text_key_is_not_cast():
    c = cache()
    c.conn.row = {"data_type": "text"}
    c.conn.rows_all = []
    c.has_multi(["abc"], _key_type="identifier")
    (qry, _) = c.conn.queries[-1]
    assert "= ANY(%s)" in qry
    assert "uuid" not in qry


def test_an_unknown_column_type_is_left_uncast():
    """Guessing a cast would be worse than sending what we always sent."""
    c = cache()
    c.conn.row = None
    c.conn.rows_all = []
    c.has_multi(["abc"], _key_type="identifier")
    assert "= ANY(%s)" in c.conn.queries[-1][0]


def test_the_column_type_is_read_once_per_column():
    c = uuid_keyed(cache())
    for _ in range(4):
        c.has_multi([YUID])
    looked_up = [q for q, _ in c.conn.queries if "information_schema" in q]
    assert len(looked_up) == 1


def test_the_cast_is_not_shared_between_caches():
    """_col_types is a class attribute defaulting to None so an instance
    built without __init__ still works; it must not become a dict on the
    class and leak one table's column types into another's."""
    from pipeline.storage.cache.postgres import PooledCache
    a, b = uuid_keyed(cache()), cache()
    b.conn.row = {"data_type": "text"}
    b.conn.rows_all = []
    a.has_multi([YUID])
    b.has_multi(["abc"], _key_type="identifier")
    assert "uuid" not in b.conn.queries[-1][0]
    assert PooledCache._col_types is None
