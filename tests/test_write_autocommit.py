"""The write connection commits per statement unless a caller says otherwise.

psycopg2 with autocommit off issues BEGIN as its own command when the
connection is idle, and commit() sends COMMIT. A phase that commits per write
-- reconcile -- therefore pays two round trips per record that do no work. At
227us for a single-row indexed SELECT in that phase, over 43.8M records, that
is ~5.5 of its 31.1 worker-hours.

It does not lengthen a row lock, which is what run-reconcile.py's "DO NOT
defer commits here" comment is protecting: in autocommit the lock lives from
the statement to its implicit commit, rather than until an explicitly-issued
COMMIT arrives a round trip later.

Deferring still needs a transaction it controls, so defer_commits() turns it
off and resume_commits() turns it back on. These pin that handover, including
the two places that used to assume one mode or the other.

No live postgres -- the connection is stubbed.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import psycopg2

from pipeline.storage.cache.postgres import PooledCache, PoolManager


class Cursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass

    def execute(self, qry, params=None):
        self.conn.executed.append(qry)

    def fetchone(self):
        return None

    def fetchall(self):
        return []


class Conn:
    closed = False

    def __init__(self, autocommit=False):
        self.autocommit = autocommit
        self.executed = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self, **kw):
        return Cursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def cache(autocommit=True):
    c = object.__new__(PooledCache)
    c.name, c.key = "ils_record_cache", "identifier"
    c.config = {"name": "ils", "overwrite": True, "cursor_size": 1000}
    c._cols = None
    c._batch = {}
    c.pools = PoolManager()
    c.pool_name = "p"
    c.conn = Conn(autocommit=autocommit)
    c.pools.conn = c.conn
    c.pools.write_autocommit = autocommit
    return c


# --- the write path ---------------------------------------------------------

def test_a_write_in_autocommit_sends_no_separate_commit():
    c = cache(autocommit=True)
    c.set({"id": "x"}, identifier="ils:1")
    assert len(c.conn.executed) == 1
    assert c.conn.executed[0].startswith("INSERT INTO")
    assert c.conn.commits == 0, "COMMIT is a round trip doing nothing here"


def test_a_write_outside_autocommit_still_commits():
    """The old behaviour, for anything that turns autocommit off."""
    c = cache(autocommit=False)
    c.set({"id": "x"}, identifier="ils:1")
    assert c.conn.commits == 1


# --- the handover -----------------------------------------------------------

def test_defer_commits_takes_control_and_resume_gives_it_back():
    c = cache(autocommit=True)
    c.defer_commits(every=500)
    assert c.conn.autocommit is False
    assert c.pools.write_autocommit is False
    c.resume_commits()
    assert c.conn.autocommit is True
    assert c.pools.write_autocommit is True


def test_returning_to_autocommit_commits_first():
    """psycopg2 refuses to switch inside a transaction, and a read leaves
    one open."""
    c = cache(autocommit=False)
    c.pools.set_autocommit(True)
    assert c.conn.commits >= 1
    assert c.conn.autocommit is True


def test_switching_to_the_mode_it_is_already_in_does_nothing():
    c = cache(autocommit=True)
    c.pools.set_autocommit(True)
    assert c.conn.commits == 0


def test_the_setting_survives_a_connection_made_later():
    """make_pool() runs when the first cache is built, which may be after a
    caller has already said which mode it wants."""
    made = []

    def fake_connect(**kw):
        made.append(Conn())
        return made[-1]

    pools = PoolManager()
    pools.write_autocommit = False
    real, psycopg2.connect = psycopg2.connect, fake_connect
    try:
        pools.make_pool("localsocket", user="u", dbname="d")
    finally:
        psycopg2.connect = real
    assert made[0].autocommit is False           # write connection
    assert made[1].autocommit is False           # iterating: never touched
    assert pools.iterating_conn is made[1]


def test_the_iterating_connection_is_never_put_in_autocommit():
    """Server-side cursors need a transaction to live in."""
    made = []

    def fake_connect(**kw):
        made.append(Conn())
        return made[-1]

    pools = PoolManager()
    real, psycopg2.connect = psycopg2.connect, fake_connect
    try:
        pools.make_pool("localsocket", user="u", dbname="d")
    finally:
        psycopg2.connect = real
    assert pools.conn.autocommit is True
    assert pools.iterating_conn.autocommit is False


# --- the two places that assumed a mode ------------------------------------

def test_end_read_is_a_noop_in_autocommit():
    """A read there leaves no transaction, so there is nothing to end -- and
    a ROLLBACK would be another pointless round trip."""
    c = cache(autocommit=True)
    c.end_read()
    assert c.conn.rollbacks == 0

    c = cache(autocommit=False)
    c.end_read()
    assert c.conn.rollbacks == 1


def test_maintenance_hands_the_connection_back_in_the_mode_it_took_it():
    """It is shared with every other cache in the process: left in the wrong
    mode it makes defer_commits() a silent no-op, or silently reintroduces a
    COMMIT per write, for the rest of the run."""
    for started_in in (True, False):
        c = cache(autocommit=started_in)
        with c._maintenance() as cur:
            assert c.conn.autocommit is True     # VACUUM needs its own txn
            assert any("maintenance_work_mem" in q for q in c.conn.executed)
        assert c.conn.autocommit is started_in
