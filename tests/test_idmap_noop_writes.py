"""An upsert that changes nothing must not rewrite the row -- or be sent.

Re-running identify over unchanged data computes the same clusters, so every
member already points at the YUID being written. Measured on a production
run: `touched.tsv` -- which gets a line per member that actually moved -- held
**exactly one entry against ~45M members**. Without a guard, all 45M rows are
rewritten to the value they already hold, each costing a new tuple version, a
WAL record and a dead tuple. That is where `_apply_stream` spent hours, and
where `idmap`'s 9.1% dead tuples came from.

The guard alone was not enough: every one of those rows was still serialized,
sent and probed against idmap_pkey for the server to find nothing to do, so a
member whose prior already is the YUID is now never sent at all. And the
statement next to it -- the registry upsert that stamped every cluster's YUID
with the build token -- had the same shape with no cure at all: the token is
new every build, so its guard never fired and 45M registry rows were rewritten
per build to say nothing had changed. `assign_bulk` now registers only the
YUIDs identify minted; which YUIDs a build saw is the registry diffed against
the clusters once, afterwards (`record_presence`, tests/test_idmap_presence.py).

Two statements are *deliberately* unguarded and these tests pin that too, so
nobody "fixes" them: `mint()` assigns the column to itself precisely so
RETURNING fires on conflict, and `_import_state()` truncates first.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest

from lux_pipeline.storage.idmap import postgres as pgmap
from lux_pipeline.storage.idmap.postgres import IdMap


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


def member_rows(seen):
    return next(args for sql, args in seen if "(uri, yuid)" in sql)


def registry_inserts(seen):
    return [(sql, args) for sql, args in seen if "(yuid, token)" in sql]


PERSON_X = "https://lux.collections.yale.edu/data/person/x"
PERSON_Y = "https://lux.collections.yale.edu/data/person/y"


# --- the member upsert's guard ---------------------------------------------

def test_assign_bulk_does_not_rewrite_an_unchanged_member(batched):
    m = idmap()
    m.assign_bulk([(PERSON_X, ["aat:300404670##quaPerson"], {})])
    sql = member_upsert(batched)
    assert "idmap.yuid IS DISTINCT FROM EXCLUDED.yuid" in sql


def test_add_is_guarded_too(batched):
    m = idmap()
    m._add(PERSON_X, "aat:1##quaPerson")
    assert "idmap.yuid IS DISTINCT FROM EXCLUDED.yuid" in member_upsert(batched)


def test_the_guard_names_the_table_not_excluded_twice(batched):
    """`EXCLUDED.yuid IS DISTINCT FROM EXCLUDED.yuid` is always false, which
    would silently stop the map ever being updated at all."""
    m = idmap()
    m.assign_bulk([(PERSON_X, ["aat:1##quaPerson"], {})])
    sql = member_upsert(batched)
    where = sql.split("WHERE", 1)[1]
    assert where.count("EXCLUDED.yuid") == 1
    assert "idmap.yuid" in where


# --- not sending it at all --------------------------------------------------

def test_a_member_already_pointing_at_the_yuid_is_never_sent(batched):
    """The guard keeps the server from writing the row. It does not keep the
    row from being serialized, sent and probed against idmap_pkey (5.9GB) --
    ~0.7ms of random btree descent each, which on a measured run was 10.7 of
    identify's 14.1 hours. prior already says what the row holds."""
    m = idmap()
    got = m.assign_bulk([(PERSON_X, ["aat:1##quaPerson"],
                          {"aat:1##quaPerson": PERSON_X})])
    assert got["unchanged"] == 1
    assert got["set"] == 1        # still counts the member
    assert got["moved"] == 0      # it did not move; it did not go anywhere


def test_an_all_unchanged_cluster_sends_nothing_at_all(batched):
    """Its members are where they were and its YUID is registered (a member
    held it). The build token used to be stamped on the registry row here,
    for every cluster, every build: 45M rows rewritten to record that nothing
    changed. Presence is recorded by the diff afterwards instead."""
    m = idmap()
    got = m.assign_bulk([(PERSON_X, ["aat:1##quaPerson"],
                          {"aat:1##quaPerson": PERSON_X})])
    assert got["clusters"] == 1
    assert batched == []
    assert m.conn.executed == []


def test_a_minted_cluster_registers_its_yuid_without_stamping(batched):
    """No member held the YUID, so identify minted it and merge will ask
    `yuid in idmap`: a registry row, DO NOTHING, no token guard needed."""
    m = idmap()
    m.assign_bulk([(PERSON_X, ["aat:1##quaPerson", "aat:2##quaPerson"],
                    {"aat:2##quaPerson": PERSON_Y})])
    [(sql, args)] = registry_inserts(batched)
    assert "ON CONFLICT (yuid) DO NOTHING" in sql
    assert "DO UPDATE" not in sql
    assert args == [("yuid:person/x", "__20260906__")]
    assert len(member_rows(batched)) == 2


def test_a_cluster_that_won_its_yuid_is_not_re_registered(batched):
    """One voter is enough to prove the registry row exists: a cluster keeps
    a YUID only when a member held it."""
    m = idmap()
    m.assign_bulk([(PERSON_X,
                    ["aat:1##quaPerson", "aat:2##quaPerson", "aat:3##quaPerson"],
                    {"aat:1##quaPerson": PERSON_X, "aat:2##quaPerson": PERSON_Y})])
    assert registry_inserts(batched) == []
    assert len(member_rows(batched)) == 2


def test_a_moved_member_is_still_sent(batched):
    m = idmap()
    got = m.assign_bulk([(PERSON_X, ["aat:1##quaPerson"],
                          {"aat:1##quaPerson": PERSON_Y})])
    assert got["moved"] == 1
    assert got["unchanged"] == 0
    assert len(member_rows(batched)) == 1


def test_a_member_with_no_prior_is_still_sent(batched):
    """A blank prior means no row at all, not a row that matches."""
    m = idmap()
    got = m.assign_bulk([(PERSON_X, ["aat:1##quaPerson"], {})])
    assert got["unchanged"] == 0
    assert len(member_rows(batched)) == 1


def test_a_mixed_cluster_sends_only_what_changed(batched):
    m = idmap()
    got = m.assign_bulk([(PERSON_X,
                          ["aat:1##quaPerson", "aat:2##quaPerson",
                           "aat:3##quaPerson"],
                          {"aat:1##quaPerson": PERSON_X,     # unchanged
                           "aat:2##quaPerson": PERSON_Y})])  # moved
    assert got == {"set": 3, "moved": 1, "clusters": 1, "unchanged": 1}
    rows = member_rows(batched)
    assert len(rows) == 2
    assert all("aat:1##quaPerson" not in r[0] for r in rows)


def test_nothing_at_all_still_does_nothing(batched):
    m = idmap()
    assert m.assign_bulk([])["clusters"] == 0
    assert batched == []


# --- the token statements that remain ---------------------------------------

def test_add_update_token_only_writes_when_something_changes():
    """A current YUID is a no-op; a missing one is cleared; a new one is
    registered. Nothing rewrites a row to the value it holds."""
    m = idmap()
    sql = IdMap.PREPARED["token_set"].format(t="idmap", y="idmap_yuid")
    assert "ON CONFLICT (yuid) DO UPDATE SET missing_since = NULL" in sql
    assert "WHERE idmap_yuid.missing_since IS NOT NULL" in sql


def test_has_update_token_reads_presence_not_the_stamp():
    """With no per-build stamp, "seen by this build" is: registered by it,
    or current as of the last identify."""
    sql = IdMap.PREPARED["token_is"].format(t="idmap", y="idmap_yuid")
    assert "token = $2 OR missing_since IS NULL" in sql


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
