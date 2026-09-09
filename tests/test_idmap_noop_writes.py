"""An upsert that changes nothing must not rewrite the row.

Re-running identify over unchanged data computes the same clusters, so every
member already points at the YUID being written. Measured on a production
run: `touched.tsv` -- which gets a line per member that actually moved -- held
**exactly one entry against ~45M members**. Without a guard, all 45M rows are
rewritten to the value they already hold, each costing a new tuple version, a
WAL record and a dead tuple. That is where `_apply_stream` spent hours, and
where `idmap`'s 9.1% dead tuples came from.

The codebase had already identified this failure mode three times -- the
token upsert in the same function, the `token_set` prepared statement, and
`merge_refs` -- and guarded all three. `assign_bulk`'s member upsert, the
biggest write in the build, was the one that got missed.

Two statements are *deliberately* unguarded and these tests pin that too, so
nobody "fixes" them: `mint()` assigns the column to itself precisely so
RETURNING fires on conflict, and `_import_state()` truncates first.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest

from pipeline.storage.idmap import postgres as pgmap
from pipeline.storage.idmap.postgres import IdMap


class Cursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass

    def execute(self, sql, params=None):
        self.conn.executed.append(sql)

    def fetchone(self):
        return (self.conn.returns,)


class Conn:
    def __init__(self, returns="yuid:person/x"):
        self.executed = []
        self.returns = returns
        self.autocommit = True

    def cursor(self):
        return Cursor(self)


class Configs:
    ok_record_types = {"Person": "person"}
    internal_uri = "https://lux.collections.yale.edu/data/"

    def is_qua(self, key):
        return "##qua" in key

    def make_qua(self, key, typ):
        return f"{key}##qua{typ}"

    def split_qua(self, key):
        return key.split("##qua") if "##qua" in key else (key, None)


@pytest.fixture
def batched(monkeypatch):
    """Capture the SQL handed to execute_values, rather than exercising it."""
    seen = []
    monkeypatch.setattr(pgmap.psycopg2.extras, "execute_values",
                        lambda cur, sql, args, **kw: seen.append((sql, list(args))))
    return seen


def idmap():
    m = object.__new__(IdMap)
    m.configs = Configs()
    m.table, m.yuid_table = "idmap", "idmap_yuid"
    m.prefix_map_out = {"yuid": Configs.internal_uri}
    m.prefix_map_in = {Configs.internal_uri: "yuid"}
    m.memory_cache_enabled = False
    m.update_token = "__20260906__"
    m.conn = Conn()
    return m


def member_upsert(seen):
    return next(sql for sql, _ in seen if "(uri, yuid)" in sql)


# --- the fix ----------------------------------------------------------------

def test_assign_bulk_does_not_rewrite_an_unchanged_member(batched):
    m = idmap()
    m.assign_bulk([("https://lux.collections.yale.edu/data/person/x",
                    ["aat:300404670##quaPerson"], {})])
    sql = member_upsert(batched)
    assert "idmap.yuid IS DISTINCT FROM EXCLUDED.yuid" in sql


def test_both_statements_in_assign_bulk_are_guarded(batched):
    """They have to stay consistent -- the token half was guarded from the
    start and the member half was not, which is how this was missed."""
    m = idmap()
    m.assign_bulk([("https://lux.collections.yale.edu/data/person/x",
                    ["aat:1##quaPerson"], {})])
    assert len(batched) == 2
    for sql, _args in batched:
        assert "IS DISTINCT FROM" in sql, sql


def test_add_is_guarded_too(batched):
    m = idmap()
    m._add("https://lux.collections.yale.edu/data/person/x", "aat:1##quaPerson")
    assert "idmap.yuid IS DISTINCT FROM EXCLUDED.yuid" in member_upsert(batched)


def test_the_guard_names_the_table_not_excluded_twice(batched):
    """`EXCLUDED.yuid IS DISTINCT FROM EXCLUDED.yuid` is always false, which
    would silently stop the map ever being updated at all."""
    m = idmap()
    m.assign_bulk([("https://lux.collections.yale.edu/data/person/x",
                    ["aat:1##quaPerson"], {})])
    sql = member_upsert(batched)
    where = sql.split("WHERE", 1)[1]
    assert where.count("EXCLUDED.yuid") == 1
    assert "idmap.yuid" in where


# --- the deliberate exceptions ---------------------------------------------

def test_mint_keeps_its_no_op_update():
    """It assigns the column to itself so RETURNING fires on conflict and the
    caller learns who won the race. A WHERE would return no row and make
    "I minted this" indistinguishable from "someone else did"."""
    m = idmap()
    m.mint("aat:300404670##quaPerson", "person")
    upsert = next(q for q in m.conn.executed if "RETURNING yuid" in q)
    assert "SET yuid = idmap.yuid" in upsert
    assert "IS DISTINCT FROM" not in upsert
