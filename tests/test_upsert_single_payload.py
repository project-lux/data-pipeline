"""An overwrite upsert sends its document once.

`SET (cols) = (%s, ...)` with the parameter list passed twice is the obvious
way to write this and it doubles the cost of every write in the pipeline:
psycopg2's Json adapter re-runs dumps() on each adaptation (getquoted() does
no caching), so the record is serialised twice client-side, crosses the socket
twice, and is parsed into jsonb twice by postgres -- and the WAL those writes
generate doubles with it. Measured share of the merge phase before the fix:
write_merged and write_rewritten together were 40% of worker time.

The property is easy to reintroduce while editing set(), so it is pinned here
along with the overwrite semantics it must not break: every column the INSERT
names still has to be assigned on conflict, or an overwrite would leave a
stale value behind.

No live postgres -- the cursor is stubbed.
"""

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from psycopg2.extras import Json

from pipeline.storage.cache.postgres import PooledCache, PoolManager


class Cursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass

    def execute(self, qry, params=None):
        self.conn.executed.append((qry, params))


class Conn:
    def __init__(self):
        self.executed = []

    def cursor(self, **kw):
        return Cursor(self)

    def commit(self):
        pass

    def rollback(self):
        pass


def cache(key="yuid"):
    c = object.__new__(PooledCache)
    c.name, c.key = "merged_merged_record_cache", key
    c.config = {"name": "merged", "overwrite": True, "cursor_size": 1000}
    c._cols = None
    c.pools = PoolManager()
    c.pool_name = "p"
    c.conn = Conn()
    c.pools.conn = c.conn
    return c


def columns(qry):
    return re.search(r"INSERT INTO \S+ \(([^)]*)\)", qry).group(1).split(",")


def assignments(qry):
    return qry.split("DO UPDATE SET ", 1)[1].split(",")


def test_payload_crosses_once():
    c = cache()
    c.set({"id": "x", "type": "Person"}, yuid="0" * 36)
    (qry, params) = c.conn.executed[-1]
    # one parameter per column, not two
    assert len(params) == len(columns(qry))
    # and the document is adapted once, which is what dumps() costs follow
    assert sum(isinstance(p, Json) for p in params) == 1


def test_conflict_clause_takes_no_parameters():
    c = cache()
    c.set({"id": "x", "type": "Person"}, yuid="0" * 36)
    (qry, _params) = c.conn.executed[-1]
    assert "EXCLUDED" in qry
    assert "%s" not in qry.split("DO UPDATE SET ", 1)[1]


def test_every_inserted_column_is_still_overwritten():
    """Otherwise re-merging a record would leave stale columns behind."""
    c = cache(key="identifier")
    c.set({"id": "y", "type": "Group"}, identifier="ils:123",
          yuid="1" * 36, change="update")
    (qry, _params) = c.conn.executed[-1]
    assert assignments(qry) == [f"{col} = EXCLUDED.{col}" for col in columns(qry)]
