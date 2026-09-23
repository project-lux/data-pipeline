"""The map's indexes: who builds them, when, and with what lock.

Two separate hazards.

`_ensure_schema` runs in every process that opens the map, and merge opens 24
within a few seconds. `CREATE INDEX IF NOT EXISTS` is a cheap catalog check
when the index exists -- but when it does not, on a 50M row table, it is a
ShareLock and a full build attempted by all 24 at once. So indexes are built
only for a table this call just created.

`ensure_covering_indexes` is the migration for a populated map: (yuid, uri)
in place of (yuid), which makes get_cluster()'s member scan index-only
instead of a random heap fetch per member -- 3.84ms per call and 107 of a
merge's 299 worker hours. CONCURRENTLY, because merge holds ACCESS SHARE on
the table for as long as it runs.
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
        self.conn.executed.append(" ".join(sql.split()))
        self.conn.params.append(params)

    def fetchone(self):
        return self.conn.answers.pop(0) if self.conn.answers else None

    def fetchall(self):
        return self.conn.rows.pop(0) if self.conn.rows else []


class Conn:
    def __init__(self, answers=(), rows=()):
        self.executed = []
        self.params = []
        self.answers = list(answers)
        self.rows = [list(r) for r in rows]
        self.autocommit = True

    def cursor(self):
        return Cursor(self)


def idmap(conn):
    m = object.__new__(IdMap)
    m.table, m.yuid_table = "idmap", "idmap_yuid"
    m.conn = conn
    return m


def ddl(conn):
    return [q for q in conn.executed
            if q.startswith(("CREATE INDEX", "DROP INDEX", "CREATE TABLE"))]


# --- _ensure_schema ---------------------------------------------------------

def test_a_new_table_gets_its_indexes_immediately():
    """Nothing can be waiting on a build against an empty table."""
    conn = Conn(answers=[(None,)])            # to_regclass: does not exist
    idmap(conn)._ensure_schema()
    assert any(q.startswith("CREATE TABLE") for q in ddl(conn))
    assert any("idmap_yuid_uri_idx" in q for q in ddl(conn))


def test_an_existing_table_is_left_alone():
    """The 24-worker hazard: a missing index here would be built 24 times
    over, non-concurrently, with 23 of them queued on the lock."""
    conn = Conn(answers=[("idmap",), (1,)])   # exists; and has an index
    idmap(conn)._ensure_schema()
    assert not any(q.startswith("CREATE INDEX") for q in ddl(conn))
    # tables are still asserted -- they are genuine no-ops
    assert any(q.startswith("CREATE TABLE") for q in ddl(conn))


def test_a_map_with_no_yuid_index_says_so_rather_than_building_one(capsys):
    conn = Conn(answers=[("idmap",), None])   # exists; no non-pkey index
    idmap(conn)._ensure_schema()
    out = capsys.readouterr().out
    assert "no index on yuid" in out
    assert "--idmap-indexes" in out
    assert not any(q.startswith("CREATE INDEX") for q in ddl(conn))


def test_the_schema_index_is_the_covering_one():
    """(yuid) alone is what made the member scan hit the heap."""
    assert "(yuid, uri)" in pgmap.INDEXES
    assert "ON {idmap} (yuid);" not in pgmap.INDEXES


# --- ensure_covering_indexes ------------------------------------------------

def sizes(*names):
    """One index_sizes() answer per call the method makes."""
    return [[(n, s) for (n, s) in batch] for batch in names]


def test_it_builds_concurrently_and_drops_the_old_one_after():
    conn = Conn(rows=sizes(
        [("idmap_pkey", 5_900_000_000), ("idmap_yuid_idx", 3_200_000_000)],
        [("idmap_yuid_idx", 3_200_000_000)],      # the drop_old existence check
        [("idmap_pkey", 5_900_000_000), ("idmap_yuid_uri_idx", 7_000_000_000)],
    ))
    m = idmap(conn)
    assert m.ensure_covering_indexes() is True
    built = [q for q in conn.executed if q.startswith("CREATE INDEX")]
    dropped = [q for q in conn.executed if q.startswith("DROP INDEX")]
    assert built == ["CREATE INDEX CONCURRENTLY IF NOT EXISTS "
                     "idmap_yuid_uri_idx ON idmap (yuid, uri)"]
    assert dropped == ["DROP INDEX CONCURRENTLY IF EXISTS idmap_yuid_idx"]
    # order matters: dropping first would leave yuid lookups on a seq scan
    # if the build failed
    assert conn.executed.index(built[0]) < conn.executed.index(dropped[0])


def test_it_can_keep_the_old_index():
    """For the operator who wants to watch plans move over first."""
    conn = Conn(rows=sizes(
        [("idmap_yuid_idx", 3_200_000_000)],
        [("idmap_yuid_idx", 3_200_000_000)],
    ))
    idmap(conn).ensure_covering_indexes(drop_old=False)
    assert not any(q.startswith("DROP INDEX") for q in conn.executed)


def test_a_map_already_migrated_builds_nothing():
    conn = Conn(rows=sizes(
        [("idmap_pkey", 1), ("idmap_yuid_uri_idx", 1)],
        [("idmap_pkey", 1), ("idmap_yuid_uri_idx", 1)],   # no old index to drop
        [("idmap_pkey", 1), ("idmap_yuid_uri_idx", 1)],
    ))
    idmap(conn).ensure_covering_indexes()
    assert not any(q.startswith("CREATE INDEX") for q in conn.executed)
    assert not any(q.startswith("DROP INDEX") for q in conn.executed)


def test_it_refuses_outside_autocommit():
    """CONCURRENTLY cannot run in a transaction, and failing over to a plain
    CREATE INDEX would take the lock merge is holding."""
    conn = Conn()
    conn.autocommit = False
    with pytest.raises(RuntimeError, match="autocommit"):
        idmap(conn).ensure_covering_indexes()
    assert conn.executed == []
