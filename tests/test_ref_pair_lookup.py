"""all_refs and done_refs answer in one round trip.

resolve_refs asks the same question of both maps for every record. Sampled
during a reconcile those two SELECTs were 16.3% and 10.3% of everything
postgres was doing -- and done_refs holds no rows at all during the main loop,
because did_ref() only runs in the references loop afterwards. So half of that
was a round trip to a table that could not answer.

The paired form has to keep the two results apart, fall back cleanly when the
maps are not on one connection, and leave the redis backend alone.

No live postgres -- the cursor is stubbed.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.storage.idmap.postgres import ReferenceMap


class Cursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass

    def execute(self, sql, params=None):
        self.conn.executed.append((sql, params))

    def fetchall(self):
        return self.conn.rows


class Conn:
    def __init__(self, rows=()):
        self.executed = []
        self.rows = list(rows)

    def cursor(self):
        return Cursor(self)


def refmap(table, conn):
    m = object.__new__(ReferenceMap)
    m.table = table
    m.conn = conn
    m._conn_kw = {}
    return m


def maps(rows=()):
    conn = Conn(rows)
    return refmap("all_refs", conn), refmap("done_refs", conn), conn


def test_one_statement_covers_both_tables():
    a, d, conn = maps()
    a.get_multi_pair(d, ["u1", "u2"])
    assert len(conn.executed) == 1
    sql = conn.executed[0][0]
    assert "all_refs" in sql and "done_refs" in sql
    assert "UNION ALL" in sql


def test_the_two_results_are_kept_apart():
    #        src, uri,  dist, ctype
    rows = [(0, "u1", 2, "Person"),
            (0, "u2", 5, ""),
            (1, "u2", 3, "Place")]
    a, d, _conn = maps(rows)
    mine, theirs = a.get_multi_pair(d, ["u1", "u2"])
    assert mine == {"u1": {"dist": 2, "type": "Person"},
                    "u2": {"dist": 5, "type": ""}}
    assert theirs == {"u2": {"dist": 3, "type": "Place"}}


def test_a_key_in_neither_is_simply_absent():
    a, d, _ = maps()
    mine, theirs = a.get_multi_pair(d, ["u1"])
    assert mine == {} and theirs == {}


def test_it_chunks_like_get_multi_does():
    a, d, conn = maps()
    a.get_multi_pair(d, [f"u{i}" for i in range(2500)], chunk=1000)
    assert len(conn.executed) == 3
    assert [len(p[0]) for _s, p in conn.executed] == [1000, 1000, 500]


def test_maps_on_different_connections_fall_back_to_two_queries():
    """_refs_connection keeps them apart when their session settings differ;
    no single statement can span two connections."""
    ca, cd = Conn(), Conn()
    a, d = refmap("all_refs", ca), refmap("done_refs", cd)
    a.get_multi_pair(d, ["u1"])
    assert len(ca.executed) == 1 and len(cd.executed) == 1
    assert "UNION" not in ca.executed[0][0]


def test_the_manager_falls_back_when_the_backend_has_no_paired_form():
    """The redis ReferenceMap has no get_multi_pair; resolve_refs must still
    work against it."""
    from pipeline.process.reference_manager import ReferenceManager

    class Plain:
        def __init__(self):
            self.asked = []

        def get_multi(self, keys):
            self.asked.append(list(keys))
            return {}

        def merge_refs(self, items):
            pass

        def delete_multi(self, keys):
            pass

    mgr = object.__new__(ReferenceManager)
    mgr.all_refs, mgr.done_refs = Plain(), Plain()
    mgr.ref_cache = {}
    out = mgr.resolve_refs({"u1": [1, "Person"]})
    assert mgr.all_refs.asked == [["u1"]]
    assert mgr.done_refs.asked == [["u1"]]
    assert out == {"u1": {"dist": 1, "type": "Person"}}


def test_the_manager_uses_the_paired_form_when_it_exists():
    from pipeline.process.reference_manager import ReferenceManager

    a, d, conn = maps()
    calls = []
    real = a.get_multi_pair
    a.get_multi_pair = lambda other, keys: (calls.append(keys) or real(other, keys))
    a.merge_refs = lambda items: None
    d.delete_multi = lambda keys: None

    mgr = object.__new__(ReferenceManager)
    mgr.all_refs, mgr.done_refs = a, d
    mgr.ref_cache = {}
    mgr.resolve_refs({"u1": [1, "Person"]})
    assert calls == [["u1"]]
    assert len(conn.executed) == 1
