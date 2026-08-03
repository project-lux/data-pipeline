"""Deferred commits, and what happens when one of them deadlocks.

Merge and export stop committing inside every set() and batch instead --
roughly five fsyncs per merged record, times 24 workers, is the thing being
avoided. The cost is that a transaction now holds many writes at once, and
postgres aborts the WHOLE transaction when it picks one as a deadlock victim,
not just the statement that lost. So the batch has to be replayed rather than
mourned; these tests pin that, and the bookkeeping that goes with it (the
buffer is bounded by `every`, and it is empty again after a commit).

No live postgres -- the cursor is stubbed.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import psycopg2
import pytest

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
        self.conn.attempts.append(params)
        if self.conn.fail_on and self.conn.attempts[-1] in self.conn.fail_on:
            self.conn.fail_on.remove(params)
            raise Deadlock("deadlock detected")
        self.conn.applied.append(params)


class Conn:
    def __init__(self):
        self.attempts = []   # every execute, including ones that blew up
        self.applied = []    # the ones that got through
        self.fail_on = []    # params to deadlock on, once each
        self.committed = []  # applied rows, snapshotted per commit
        self.rollbacks = 0

    def cursor(self, **kw):
        return Cursor(self)

    def commit(self):
        self.committed.extend(self.applied)
        self.applied = []

    def rollback(self):
        self.rollbacks += 1
        # an aborted transaction loses everything not yet committed
        self.applied = []


def cache():
    c = object.__new__(PooledCache)
    c.name, c.key = "merged_merged_record_cache", "yuid"
    c.config = {"name": "merged", "overwrite": True, "cursor_size": 1000}
    c._cols = None
    c.pools = PoolManager()
    c.pool_name = "p"
    c.conn = Conn()
    c.pools.conn = c.conn
    return c


def write(c, n):
    c.set({"id": n}, yuid=f"{n:036d}")


def test_deferred_writes_land_only_on_flush():
    c = cache()
    c.defer_commits(every=500)
    for i in range(10):
        write(c, i)
        c.checkpoint()

    assert c.conn.committed == [], "nothing may commit before the batch fills"
    assert c.pools.pending_writes == 10
    assert len(c.pools.deferred_stmts) == 10

    c.flush()
    assert len(c.conn.committed) == 10
    assert c.pools.deferred_stmts == [], "a commit clears the replay buffer"
    assert c.pools.pending_writes == 0


def test_checkpoint_commits_once_the_batch_is_full():
    c = cache()
    c.defer_commits(every=3)
    for i in range(3):
        write(c, i)
        c.checkpoint()
    assert len(c.conn.committed) == 3
    assert c.pools.deferred_stmts == []


def test_deadlock_while_deferring_replays_the_batch():
    """The whole point: a deadlock aborts writes 0..3 along with write 4, so
    all five have to be re-executed, not just the one that lost."""
    c = cache()
    c.defer_commits(every=500)
    for i in range(4):
        write(c, i)
        c.checkpoint()

    # deadlock on the next statement executed
    original = Cursor.execute

    def fail_once(self, qry, params=None):
        self.conn.attempts.append(params)
        if len(self.conn.attempts) == 5:
            raise Deadlock("deadlock detected")
        self.conn.applied.append(params)

    Cursor.execute = fail_once
    try:
        write(c, 4)
    finally:
        Cursor.execute = original

    assert c.conn.rollbacks == 1
    c.flush()
    ids = [p[0].adapted["id"] for p in c.conn.committed]
    assert ids == [0, 1, 2, 3, 4], "no write may be lost to the rollback"
    assert c.pools.deferred_stmts == []


def test_deadlock_without_deferring_retries_just_the_statement():
    c = cache()
    calls = []
    original = Cursor.execute

    def fail_first(self, qry, params=None):
        calls.append(params)
        if len(calls) == 1:
            raise Deadlock("deadlock detected")
        self.conn.applied.append(params)

    Cursor.execute = fail_first
    try:
        write(c, 1)
    finally:
        Cursor.execute = original

    assert len(calls) == 2, "the losing statement is retried"
    assert len(c.conn.committed) == 1


def test_persistent_deadlock_gives_up_and_clears_the_buffer():
    c = cache()
    c.defer_commits(every=500)
    write(c, 0)
    original = Cursor.execute

    def always_fail(self, qry, params=None):
        raise Deadlock("deadlock detected")

    Cursor.execute = always_fail
    try:
        with pytest.raises(psycopg2.extensions.TransactionRollbackError):
            write(c, 1)
    finally:
        Cursor.execute = original

    # it failed loudly rather than pretending the batch is still pending
    assert c.pools.deferred_stmts == []
    assert c.pools.pending_writes == 0


def test_non_overwrite_insert_lands_the_batch_first():
    """A UniqueViolation on a plain insert is survivable, but its rollback
    would take the open transaction with it -- so deferred writes commit
    before the insert runs."""
    c = cache()
    c.defer_commits(every=500)
    write(c, 0)
    assert len(c.pools.deferred_stmts) == 1

    # a second cache on the same connection, this one not an overwrite cache
    c.config["overwrite"] = False
    write(c, 1)
    assert c.pools.deferred_stmts == []
    assert len(c.conn.committed) >= 1
