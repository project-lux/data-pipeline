"""One round trip for a key's YUID and that YUID's class.

`idmap[uri]` then `idmap[yuid]` is two sequential single-row lookups for what
is one answer, and merge did it 43.8M times a build -- 9.35 worker-hours
between the two stages. Membership is derived from the yuid column, so one
statement resolves the forward pointer and returns the class it points at.

What has to hold, beyond returning the right answer: the member set must be
left in the memory cache under the YUID's own key. Merge reads it straight
back (`idmap_equivs`, 12.7us -- a memory hit) and so does the reidentifier's
prefetch, so a version of this that skipped that caching would move the cost
rather than remove it.

No live postgres -- the cursor is stubbed.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import psycopg2

from pipeline.storage.idmap.postgres import IdMap
from pipeline.storage.uricache import URICache

INTERNAL = "https://lux.collections.yale.edu/data/"
AAT = "http://vocab.getty.edu/aat/"

YUID_OUT = f"{INTERNAL}person/0123"
YUID_IN = "yuid:person/0123"
KEY_OUT = f"{AAT}300404670##quaPerson"
KEY_IN = "aat:300404670##quaPerson"
SIBLING_IN = "aat:300111999##quaPerson"
SIBLING_OUT = f"{AAT}300111999##quaPerson"


class Configs:
    ok_record_types = {"Person": "person"}

    def is_qua(self, key):
        return "##qua" in key

    def make_qua(self, key, typ):
        return f"{key}##qua{typ}"


class HotCursor:
    def __init__(self, rows):
        self.rows = rows            # {"cluster"|"fwd"|"rev": [tuples]}
        self.calls = []
        self.fail_on = set()
        self._out = []

    def execute(self, sql, params=None):
        which = next(k for k in ("cluster", "fwd", "rev") if f"idmap_{k}_" in sql)
        self.calls.append(which)
        if which in self.fail_on:
            raise psycopg2.Error("statement blew up")
        self._out = list(self.rows.get(which, []))

    def fetchall(self):
        return self._out

    def fetchone(self):
        return self._out[0] if self._out else None


class Conn:
    def __init__(self):
        self.rollbacks = 0

    def rollback(self):
        self.rollbacks += 1


def idmap(rows, memory=True):
    m = object.__new__(IdMap)
    m.configs = Configs()
    m.table = "idmap"
    m.yuid_table = "idmap_yuid"
    m.prefix_map_out = {"yuid": INTERNAL, "aat": AAT}
    m.prefix_map_in = {v: k for (k, v) in m.prefix_map_out.items()}
    m.memory_cache = URICache(capacity=100)
    m.memory_cache_enabled = memory
    m.conn = Conn()
    m._hot = HotCursor(rows)
    m._stmt = {n: f"idmap_{n}_ab" for n in ("cluster", "fwd", "rev")}
    return m


CLUSTER = {"cluster": [(YUID_IN, KEY_IN), (YUID_IN, SIBLING_IN)]}


def test_one_round_trip_returns_the_yuid_and_its_members():
    m = idmap(CLUSTER)
    (yuid, members) = m.get_cluster(KEY_OUT)
    assert yuid == YUID_OUT
    assert members == {KEY_OUT, SIBLING_OUT}
    assert m._hot.calls == ["cluster"], "should be one statement, not two"


def test_an_unknown_key_is_not_an_empty_class():
    """A key is always a member of its own class, so no rows means the
    forward lookup missed."""
    m = idmap({"cluster": []})
    assert m.get_cluster(KEY_OUT) == (None, None)


def test_the_member_set_is_left_cached_under_the_yuid():
    """This is what keeps merge's next lookup a memory hit."""
    m = idmap(CLUSTER)
    m.get_cluster(KEY_OUT)
    assert m.memory_cache[YUID_IN] == {KEY_OUT, SIBLING_OUT}
    assert m.memory_cache[KEY_IN] == YUID_OUT
    # ...so reading the class straight back costs no further statement
    m._hot.calls.clear()
    assert m.get(YUID_OUT) == {KEY_OUT, SIBLING_OUT}
    assert m._hot.calls == []


def test_a_fully_cached_cluster_issues_no_statement():
    m = idmap(CLUSTER)
    m.get_cluster(KEY_OUT)
    m._hot.calls.clear()
    assert m.get_cluster(KEY_OUT) == (YUID_OUT, {KEY_OUT, SIBLING_OUT})
    assert m._hot.calls == []


def test_memory_cache_off_still_works():
    m = idmap(CLUSTER, memory=False)
    (yuid, members) = m.get_cluster(KEY_OUT)
    assert (yuid, members) == (YUID_OUT, {KEY_OUT, SIBLING_OUT})


def test_a_failed_statement_falls_back_rather_than_reporting_no_yuid():
    """Returning (None, None) on an error would make merge skip the record
    and print a missing-YUID line for one it has."""
    m = idmap({"cluster": [], "fwd": [(YUID_IN,)], "rev": [(KEY_IN,), (SIBLING_IN,)]},
              memory=False)
    m._hot.fail_on.add("cluster")
    (yuid, members) = m.get_cluster(KEY_OUT)
    assert yuid == YUID_OUT
    assert members == {KEY_OUT, SIBLING_OUT}
    assert m._hot.calls == ["cluster", "fwd", "rev"]
    assert m.conn.rollbacks == 1
