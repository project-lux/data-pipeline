"""Multi-row upserts while deferring.

Deferring already removed the fsync per write; what was left was one round
trip and one parse per record. `defer_commits(batch=N)` sends N records per
statement instead. The properties that make it safe are the ones pinned here:

*   rows go out in key order, because ON CONFLICT locks conflicting rows in
    the order the VALUES list gives them and two transactions overlapping in
    opposite orders deadlock -- the same reason merge_refs sorts;
*   a key written twice in one batch folds, because two rows for one key in a
    single ON CONFLICT statement is an error, not an upsert;
*   a commit covers whole records, so every cache in the process lands its
    buffer before the connection commits;
*   a read sees this process's own buffered writes, which an executed-but-
    uncommitted statement gave for free and buffering would otherwise lose.

Batching is off unless asked for, so everything that does not pass `batch` is
unaffected -- see test_deferred_commits.py, which still exercises the
one-statement-per-record path.

No live postgres -- the cursor is stubbed.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import psycopg2
from psycopg2.extras import Json

from pipeline.storage.cache.postgres import PooledCache, PoolManager


class Deadlock(psycopg2.extensions.TransactionRollbackError):
    pass


class Cursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass

    def execute(self, qry, params=None):
        self.conn.executed.append((qry, params))
        if self.conn.fail_once:
            self.conn.fail_once = False
            raise Deadlock("deadlock detected")
        self.conn.applied.append((qry, params))

    def fetchone(self):
        return None

    def fetchall(self):
        return []


class Conn:
    # the real write connection is in autocommit unless a caller has
    # taken control with defer_commits(); these tests are about the
    # paths where it has
    autocommit = False
    closed = False

    def __init__(self):
        self.executed = []
        self.applied = []
        self.committed = []
        self.fail_once = False

    def cursor(self, **kw):
        return Cursor(self)

    def commit(self):
        self.committed.extend(self.applied)
        self.applied = []

    def rollback(self):
        self.applied = []


def cache(pools=None, name="merged_merged_record_cache", key="yuid"):
    c = object.__new__(PooledCache)
    c.name, c.key = name, key
    c.config = {"name": "merged", "overwrite": True, "cursor_size": 1000}
    c._cols = None
    c._batch = {}
    c.pools = pools or PoolManager()
    c.pool_name = "p"
    c.conn = c.pools.conn or Conn()
    c.pools.conn = c.conn
    return c


def write(c, n):
    c.set({"id": n}, yuid=f"{n:036d}")


def writes(qry):
    """How many placeholder groups one statement carries."""
    return qry.split("VALUES ", 1)[1].split(" ON CONFLICT", 1)[0].count("(")


# --- batching happens, and only when asked for ------------------------------

def test_off_by_default_one_statement_per_record():
    c = cache()
    c.defer_commits(every=500)
    for i in range(10):
        write(c, i)
        c.checkpoint()
    assert len(c.conn.executed) == 10
    assert all(writes(q) == 1 for q, _ in c.conn.executed)


def test_batch_of_25_sends_one_statement_per_25_records():
    c = cache()
    c.defer_commits(every=500, batch=25)
    for i in range(50):
        write(c, i)
        c.checkpoint()
    assert len(c.conn.executed) == 2
    assert [writes(q) for q, _ in c.conn.executed] == [25, 25]


def test_partial_batch_lands_on_flush():
    c = cache()
    c.defer_commits(every=500, batch=25)
    for i in range(30):
        write(c, i)
        c.checkpoint()
    assert len(c.conn.executed) == 1        # 25 out, 5 still held
    c.flush()
    assert [writes(q) for q, _ in c.conn.executed] == [25, 5]
    assert sum(writes(q) for q, _ in c.conn.committed) == 30


def test_resume_commits_lands_everything():
    c = cache()
    c.defer_commits(every=500, batch=25)
    for i in range(7):
        write(c, i)
    c.resume_commits()
    assert sum(writes(q) for q, _ in c.conn.executed) == 7
    assert c.pools.batch_rows == 0


# --- the properties that keep it safe ---------------------------------------

def test_rows_go_out_in_key_order():
    c = cache()
    c.defer_commits(every=500, batch=3)
    for n in (7, 2, 5):
        write(c, n)
    (qry, params) = c.conn.executed[-1]
    # one row is (data, yuid, insert_time, record_time, refresh_time)
    yuids = [p for p in params if isinstance(p, str) and p.isdigit()]
    assert yuids == sorted(yuids), yuids


def test_a_key_written_twice_folds_to_the_last_version():
    """Two rows for one key in one ON CONFLICT statement is an error."""
    c = cache()
    c.defer_commits(every=500, batch=10)
    c.set({"v": "first"}, yuid="0" * 36)
    c.set({"v": "second"}, yuid="0" * 36)
    c.flush()
    (qry, params) = c.conn.executed[-1]
    assert writes(qry) == 1
    assert [p.adapted for p in params if isinstance(p, Json)] == [{"v": "second"}]


def test_the_document_still_crosses_once_per_row():
    """The §3.1 property has to survive batching."""
    c = cache()
    c.defer_commits(every=500, batch=4)
    for i in range(4):
        write(c, i)
    (_qry, params) = c.conn.executed[-1]
    assert sum(isinstance(p, Json) for p in params) == 4


def test_commit_every_still_counts_records_not_statements():
    """Buffered rows count towards the threshold. Counting emitted statements
    instead would let batch=25 quietly turn every=10 into every=250, so far
    more would ride on one commit than the caller asked for."""
    c = cache()
    c.defer_commits(every=10, batch=25)
    for i in range(9):
        write(c, i)
        c.checkpoint()
    assert c.conn.committed == []           # nine records: below the threshold
    write(c, 9)
    c.checkpoint()
    # the tenth record trips it, and the commit carries all ten
    assert len(c.conn.committed) == 1       # ...as a single statement
    assert sum(writes(q) for q, _ in c.conn.committed) == 10


def test_every_cache_lands_before_the_connection_commits():
    """A commit has to cover the merged row and its rewritten rows together."""
    pools = PoolManager()
    merged = cache(pools, name="merged_merged_record_cache")
    rewritten = cache(pools, name="ils_rewritten_record_cache")
    merged.defer_commits(every=500, batch=25)
    merged.set({"id": 1}, yuid="1" * 36)
    rewritten.set({"id": 1}, yuid="1" * 36)
    assert merged.conn.executed == []       # both still buffered
    merged.flush()
    assert sum(writes(q) for q, _ in merged.conn.executed) == 2
    assert sum(writes(q) for q, _ in merged.conn.committed) == 2


def test_a_read_sees_this_processs_own_buffered_writes():
    c = cache()
    c.defer_commits(every=500, batch=25)
    write(c, 1)
    assert c.conn.executed == []
    c.get("0" * 36)
    # the buffered upsert went out ahead of the SELECT
    assert len(c.conn.executed) == 2
    assert "INSERT INTO" in c.conn.executed[0][0]
    assert "SELECT" in c.conn.executed[1][0]


def test_a_batched_statement_is_replayed_after_a_deadlock():
    """A deadlock aborts the whole transaction, so the batch has to go back."""
    c = cache()
    c.defer_commits(every=500, batch=2)
    write(c, 1)
    write(c, 2)                      # emits a 2-row statement
    assert len(c.conn.applied) == 1
    c.conn.fail_once = True
    write(c, 3)
    write(c, 4)                      # deadlocks, replays, retries
    c.flush()
    landed = sum(writes(q) for q, _ in c.conn.committed)
    assert landed >= 4, c.conn.committed


def test_a_delete_is_not_undone_by_a_buffered_write():
    """commit_all() lands buffers, so without a barrier of its own the DELETE
    would execute first and the buffered INSERT would resurrect the row."""
    c = cache()
    c.defer_commits(every=500, batch=25)
    write(c, 1)
    c.delete("0" * 36)
    kinds = [q.split()[0] for q, _ in c.conn.executed]
    assert kinds == ["INSERT", "DELETE"], kinds


def test_a_clear_is_not_undone_by_a_buffered_write():
    c = cache()
    c.defer_commits(every=500, batch=25)
    write(c, 1)
    c.clear()
    kinds = [q.split()[0] for q, _ in c.conn.executed]
    assert kinds == ["INSERT", "TRUNCATE"], kinds
