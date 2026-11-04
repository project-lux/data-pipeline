"""A subsidiary map takes identities from a master and never writes to it.

What has to hold, and is checked here:

  * the master is read-only, enforced rather than assumed;
  * the copy works against a redis-shaped master (get only, update tokens as
    pseudo-members of the YUID set) and a postgres-shaped one (get_cluster,
    token in a column) without the caller choosing;
  * a copy never lands on top of a local decision -- the hydrating inserts
    are ON CONFLICT DO NOTHING;
  * the whole equivalence class comes across, not the row asked for, so the
    reverse direction cannot disagree with the forward one;
  * the origin snapshot is what stops a local change being undone by a later
    fallback to the master, including for keys the master had nothing for;
  * a master that errors records nothing, so the miss is retried rather than
    frozen in as an answer.

No live postgres -- the cursor is a small interpreter over dicts.
"""

import sys
from contextlib import contextmanager
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

import psycopg2.extras

from lux_pipeline.storage.idmap import subsidiary
from lux_pipeline.storage.idmap.subsidiary import (IdMap, MasterUnavailable,
                                               ReadOnlyMaster,
                                               ReadOnlyMasterError)
from lux_pipeline.storage.uricache import URICache

INTERNAL = "https://lux.collections.yale.edu/data/"
AAT = "http://vocab.getty.edu/aat/"
TOKEN = "__20260101__"

YUID = f"{INTERNAL}person/0123"
YUID_IN = "yuid:person/0123"
OTHER = f"{INTERNAL}person/9999"
OTHER_IN = "yuid:person/9999"

KEY = f"{AAT}300404670##quaPerson"
KEY_IN = "aat:300404670##quaPerson"
SIBLING = f"{AAT}300111999##quaPerson"
SIBLING_IN = "aat:300111999##quaPerson"
STRANGER = f"{AAT}300222888##quaPerson"
STRANGER_IN = "aat:300222888##quaPerson"


class Configs:
    ok_record_types = {"Person": "person"}

    def is_qua(self, key):
        return "##qua" in key

    def make_qua(self, key, typ):
        return f"{key}##qua{typ}"


class RedisShapedMaster:
    """What the redis backend offers: get(), and tokens are set members."""

    def __init__(self, fwd, sets, fail=False):
        self.fwd = fwd
        self.sets = sets
        self.fail = fail
        self.calls = []

    def get(self, key, typ=""):
        self.calls.append(key)
        if self.fail:
            raise RuntimeError("master is loading its dump")
        if key.startswith(INTERNAL):
            return set(self.sets.get(key, set())) or None
        return self.fwd.get(key)

    def get_multi(self, keys, chunk=1000):
        return {k: self.get(k) for k in keys}

    # Present so the read-only proxy has something to refuse.
    def set(self, key, value, typ=""):
        raise AssertionError("the master was written to")

    def mint(self, key, slug, typ=""):
        raise AssertionError("the master was written to")

    def assign_bulk(self, items):
        raise AssertionError("the master was written to")


class PgShapedMaster(RedisShapedMaster):
    """What the postgres backend adds: the class in one call, token in a
    column rather than in the member set."""

    def __init__(self, fwd, sets, tokens=(), fail=False):
        super().__init__(fwd, sets, fail=fail)
        self.tokens = set(tokens)

    def get_cluster(self, key, typ=""):
        self.calls.append(f"cluster:{key}")
        if self.fail:
            raise RuntimeError("connection reset")
        yuid = self.fwd.get(key)
        if not yuid:
            return (None, None)
        return (yuid, set(self.sets.get(yuid, set())))

    def has_update_token(self, key):
        self.calls.append(f"token:{key}")
        return key in self.tokens


class FakeCursor:
    """Just enough SQL to exercise the copy path, over plain dicts."""

    def __init__(self, state):
        self.state = state
        self._out = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        sql = " ".join(sql.split())
        self.state["sql"].append((sql, params))
        pairs = params if isinstance(params, list) else [params]
        if sql.startswith("INSERT INTO sub_yuid"):
            for row in pairs:
                self.state["yuids"].setdefault(row[0], row[1] if len(row) > 1 else None)
        elif sql.startswith("INSERT INTO sub (uri, yuid)"):
            clobber = "DO UPDATE" in sql
            for (uri, yuid) in pairs:
                if clobber or uri not in self.state["rows"]:
                    self.state["rows"][uri] = yuid
        elif sql.startswith("SELECT uri, yuid FROM sub WHERE uri = ANY"):
            self._out = [(u, self.state["rows"][u]) for u in params[0] if u in self.state["rows"]]
        elif sql.startswith("SELECT key FROM sub_origin WHERE key = ANY"):
            self._out = [(key,) for key in params[0] if key in self.state["origin"]]
        elif sql.startswith("SELECT uri FROM sub WHERE yuid"):
            (yuid, skip) = params
            self._out = [(u,) for u, y in sorted(self.state["rows"].items())
                         if y == yuid and u != skip]
        else:
            raise AssertionError(f"unexpected statement: {sql}")

    def fetchall(self):
        return self._out

    def fetchone(self):
        return self._out[0] if self._out else None


def submap(master, rows=None, origin=None):
    """A subsidiary map with the SQL replaced by dicts.

    Only what touches a cursor directly is stubbed -- _db_get, _run,
    _is_hydrated, _mark_origin and _log_many. _hydrate, _absorb,
    _master_cluster and the inherited has_item/count are the real ones, which
    is the point.
    """
    m = object.__new__(IdMap)
    m.configs = Configs()
    m.table = "sub"
    m.yuid_table = "sub_yuid"
    m.origin_table = "sub_origin"
    m.change_table = "sub_changes"
    m.prefix_map_out = {"yuid": INTERNAL, "aat": AAT}
    m.prefix_map_in = {v: k for (k, v) in m.prefix_map_out.items()}
    m.memory_cache = URICache(capacity=100)
    m.memory_cache_enabled = False
    m.update_token = TOKEN
    m.copy_update_token = True
    m.log_changes = True
    m.master_name = "master"
    m.master = ReadOnlyMaster(master, "master")
    m._master_has_cluster = hasattr(m.master, "get_cluster")
    m._master_has_multi = hasattr(m.master, "get_multi")

    state = {"rows": dict(rows or {}), "yuids": {}, "origin": dict(origin or {}),
             "sql": [], "log": []}
    m.state = state

    @contextmanager
    def cursor():
        yield FakeCursor(state)

    def db_get(ikey):
        if ikey.startswith("yuid:"):
            members = {u for u, y in state["rows"].items() if y == ikey}
            if not members:
                return None
            return {m._manage_value_out(x) for x in members}
        value = state["rows"].get(ikey)
        return m._manage_value_out(value) if value else None

    def run(name, params):
        """The parent's single-row prepared statements, over the same dicts."""
        cur = FakeCursor(state)
        if name == "has_uri":
            cur._out = [(1,)] if params[0] in state["rows"] else []
        elif name == "has_yuid":
            cur._out = [(1,)] if params[0] in state["yuids"] else []
        elif name == "member_count":
            cur._out = [(sum(1 for y in state["rows"].values()
                             if y == params[0]),)]
        else:
            raise AssertionError(f"unexpected prepared statement: {name}")
        return cur

    m._cursor = cursor
    m._db_get = db_get
    m._run = run
    m._is_hydrated = lambda ikey: ikey in state["origin"]
    m._mark_origin = lambda rws: [
        state["origin"].setdefault(r[0], r) for r in rws]
    m._log_many = lambda rws: state["log"].extend(rws)
    return m


@pytest.fixture(autouse=True)
def _no_real_execute_values(monkeypatch):
    """execute_values needs a live cursor to mogrify against; hand the whole
    page to the fake cursor instead."""
    monkeypatch.setattr(psycopg2.extras, "execute_values",
                        lambda cur, sql, rows, page_size=100, **kw:
                        cur.execute(sql, list(rows)))


def redis_master(**kw):
    return RedisShapedMaster({KEY: YUID, SIBLING: YUID},
                             {YUID: {KEY, SIBLING, TOKEN}}, **kw)


def pg_master(**kw):
    return PgShapedMaster({KEY: YUID, SIBLING: YUID},
                          {YUID: {KEY, SIBLING}}, tokens={YUID}, **kw)


# ------------------------------------------------------------- the master is safe

def test_master_reads_are_allowed_writes_are_not():
    m = ReadOnlyMaster(redis_master(), "idmap")
    assert m.get(KEY) == YUID
    assert m[KEY] == YUID
    for call in ("set", "mint", "assign_bulk"):
        with pytest.raises(ReadOnlyMasterError):
            getattr(m, call)
    with pytest.raises(ReadOnlyMasterError):
        m[KEY] = YUID
    with pytest.raises(ReadOnlyMasterError):
        del m[KEY]


def test_unknown_master_methods_are_still_attribute_errors():
    """hasattr() has to work, because the backend is probed for get_cluster."""
    assert not hasattr(ReadOnlyMaster(redis_master(), "idmap"), "get_cluster")
    assert hasattr(ReadOnlyMaster(pg_master(), "idmap"), "get_cluster")


# --------------------------------------------------------------- copying a class

def test_a_redis_shaped_master_needs_two_lookups_and_loses_the_token():
    master = redis_master()
    m = submap(master)
    assert m.get(KEY) == YUID
    # forward pointer, then the member set -- get_cluster does not exist here
    assert master.calls == [KEY, YUID]
    assert m.state["rows"] == {KEY_IN: YUID_IN, SIBLING_IN: YUID_IN}
    # the token was a member on the way in and is a column on the way out
    assert m.state["yuids"] == {YUID_IN: TOKEN}


def test_a_postgres_shaped_master_answers_in_one_call():
    master = pg_master()
    m = submap(master)
    assert m.get(KEY) == YUID
    assert master.calls == [f"cluster:{KEY}", f"token:{YUID}"]
    assert m.state["rows"] == {KEY_IN: YUID_IN, SIBLING_IN: YUID_IN}
    assert m.state["yuids"] == {YUID_IN: TOKEN}


def test_the_whole_class_comes_across_not_just_the_key_asked_for():
    m = submap(pg_master())
    m.get(KEY)
    # the sibling was never asked for, but the reverse direction needs it
    assert m.get(YUID) == {KEY, SIBLING}
    assert m.state["origin"][SIBLING_IN][2] == YUID_IN


def test_copying_never_overwrites_a_local_decision():
    """The key has been moved here already, and the class is then copied in
    from the other end -- via a sibling nobody had asked about yet."""
    m = submap(pg_master(), rows={KEY_IN: OTHER_IN})
    assert m.get(SIBLING) == YUID
    assert m.state["rows"][KEY_IN] == OTHER_IN, \
        "the master overwrote a decision made in the subsidiary"
    assert m.state["rows"][SIBLING_IN] == YUID_IN


def test_every_hydrating_insert_says_do_nothing():
    """The guarantee the test above depends on, asserted on the SQL itself."""
    m = submap(pg_master())
    m.get(KEY)
    inserts = [sql for (sql, _) in m.state["sql"] if sql.startswith("INSERT")]
    assert inserts and all("DO NOTHING" in s for s in inserts), inserts


# ------------------------------------------------------ the origin snapshot

def test_a_miss_is_recorded_and_the_master_is_not_asked_twice():
    master = pg_master()
    m = submap(master)
    assert m.get(STRANGER) is None
    assert m.state["origin"][STRANGER_IN] == (STRANGER_IN, "u", None, None)
    assert m.get(STRANGER) is None
    assert master.calls == [f"cluster:{STRANGER}"], \
        "a recorded miss must not go back to the master"


def test_a_local_deletion_is_not_resurrected_from_the_master():
    master = pg_master()
    # copied in once, then deleted here: origin remembers, the row is gone
    m = submap(master, origin={KEY_IN: (KEY_IN, "u", YUID_IN, None),
                               YUID_IN: (YUID_IN, "y", None, 2)})
    assert m.get(KEY) is None
    assert master.calls == []


def test_the_master_failing_records_nothing_so_it_is_retried():
    master = pg_master(fail=True)
    m = submap(master)
    assert m.get(KEY) is None
    assert m.state["origin"] == {}, \
        "an unreachable master must not be recorded as having no mapping"
    assert m.get(KEY) is None
    assert len(master.calls) == 2


def test_master_failure_is_distinguished_from_a_miss():
    m = submap(pg_master(fail=True))
    with pytest.raises(MasterUnavailable):
        m._master_cluster(KEY)


# ------------------------------------------------------------------- has_item

def test_has_item_copies_before_answering_and_then_stops_asking():
    master = pg_master()
    m = submap(master)
    assert m.has_item(KEY) is True
    assert m.has_item(YUID) is True
    assert master.calls == [f"cluster:{KEY}", f"token:{YUID}"]


def test_has_item_is_false_for_a_key_neither_side_has():
    m = submap(pg_master())
    assert m.has_item(STRANGER) is False


# -------------------------------------------------------------- configuration

def test_tablename_is_required():
    with pytest.raises(ValueError, match="tableName"):
        IdMap({"name": "sub", "masterMap": "idmap"})


def test_mastermap_is_required():
    with pytest.raises(ValueError, match="masterMap"):
        IdMap({"name": "sub", "tableName": "sub"})


def test_a_map_cannot_be_its_own_master():
    with pytest.raises(ValueError, match="own master"):
        IdMap({"name": "idmap", "masterMap": "idmap", "tableName": "sub"})


def test_sharing_the_masters_tables_is_refused():
    """The failure mode the class exists to prevent: a postgres master in the
    same database, and a subsidiary pointed at its tables."""
    master = object.__new__(subsidiary.postgres.IdMap)
    master.table = "idmap"
    master.yuid_table = "idmap_yuid"
    master._conn_kw = {"dbname": "lux"}

    m = object.__new__(IdMap)
    m.master_name = "idmap"
    m.table = "idmap"
    m.yuid_table = "idmap_yuid"
    m.origin_table = "idmap_origin"
    m.change_table = "idmap_changes"
    m._conn_kw = {"dbname": "lux"}
    with pytest.raises(ValueError, match="collide"):
        m._guard_distinct(master)


def test_a_master_on_another_server_does_not_trip_the_guard():
    master = object.__new__(subsidiary.postgres.IdMap)
    master.table = "idmap"
    master.yuid_table = "idmap_yuid"
    master._conn_kw = {"dbname": "lux", "host": "other"}

    m = object.__new__(IdMap)
    m.master_name = "idmap"
    m.table = "idmap"
    m.yuid_table = "idmap_yuid"
    m.origin_table = "idmap_origin"
    m.change_table = "idmap_changes"
    m._conn_kw = {"dbname": "lux"}
    m._guard_distinct(master)


# ----------------------------------------------------- identify against a subsidiary

def k(i):
    return f"{AAT}30000{i}##quaPerson"


def k_in(i):
    return f"aat:30000{i}##quaPerson"


def y(i):
    return f"{INTERNAL}person/{i}"


def y_in(i):
    return f"yuid:person/{i}"


class PgMultiMaster(PgShapedMaster):
    """A postgres master that answers the token question for a batch."""

    def has_update_token_multi(self, keys, chunk=1000):
        keys = list(keys)
        self.calls.append(f"tokens:{len(keys)}")
        return {key for key in keys if key in self.tokens}


def three_classes(cls=PgMultiMaster):
    return cls({k(1): y(1), k(2): y(1), k(3): y(2), k(4): y(3)},
               {y(1): {k(1), k(2)}, y(2): {k(3)}, y(3): {k(4)}},
               tokens={y(1), y(3)})


def inserts(m, table):
    return [s for (s, _) in m.state["sql"]
            if s.startswith(f"INSERT INTO {table} ")]


def test_hydrating_a_batch_is_one_insert_per_table_not_per_class():
    """identify hydrates every member of every cluster before it writes: one
    round trip per class was days for a first run against a full master."""
    master = three_classes()
    m = submap(master)
    asked = m._hydrate_multi([k(1), k(3), k(4)])
    assert sorted(asked) == sorted([k(1), k(3), k(4)])
    assert m.state["rows"] == {k_in(1): y_in(1), k_in(2): y_in(1),
                               k_in(3): y_in(2), k_in(4): y_in(3)}
    assert len(inserts(m, "sub_yuid")) == 1
    assert len(inserts(m, "sub")) == 1
    # the tokens in one question, not one per class
    assert [c for c in master.calls if c.startswith("token")] == ["tokens:3"]
    assert m.state["yuids"] == {y_in(1): TOKEN, y_in(2): None, y_in(3): TOKEN}
    # every member and every class has its baseline
    assert m.state["origin"][k_in(2)][2] == y_in(1)
    assert m.state["origin"][y_in(1)][3] == 2


def test_a_master_without_the_batch_token_question_is_asked_per_class():
    master = three_classes(PgShapedMaster)
    m = submap(master)
    m._hydrate_multi([k(1), k(3)])
    assert sorted(c for c in master.calls if c.startswith("token")) == \
        [f"token:{y(1)}", f"token:{y(2)}"]
    assert m.state["yuids"] == {y_in(1): TOKEN, y_in(2): None}


def test_a_batch_copy_never_overwrites_a_local_decision():
    m = submap(three_classes(), rows={k_in(1): OTHER_IN})
    m._hydrate_multi([k(2)])
    assert m.state["rows"][k_in(1)] == OTHER_IN
    assert m.state["rows"][k_in(2)] == y_in(1)


def test_a_member_pointing_at_a_class_that_does_not_list_it_joins_it():
    """The union _master_cluster does, now grouped in one pass."""
    master = three_classes()
    master.fwd[k(5)] = y(2)     # forward pointer only, not in y(2)'s set
    m = submap(master)
    m._hydrate_multi([k(5), k(4)])
    assert m.state["rows"][k_in(5)] == y_in(2)
    assert m.state["rows"][k_in(3)] == y_in(2)
    assert k_in(1) not in m.state["rows"]


def test_misses_are_recorded_in_a_batch_too():
    master = three_classes()
    m = submap(master)
    m._hydrate_multi([STRANGER, OTHER])
    assert m.state["origin"][STRANGER_IN][2] is None
    assert m.state["origin"][OTHER_IN][1] == "y"
    assert m.state["rows"] == {}


def pg_store(**kw):
    store = object.__new__(subsidiary.postgres.IdMap)
    store.table = "idmap"
    store.yuid_table = "idmap_yuid"
    store._conn_kw = {"dbname": "lux"}
    store.prefix_map_in = {INTERNAL: "yuid", AAT: "aat"}
    for (a, v) in kw.items():
        setattr(store, a, v)
    return store


def dump_submap(store):
    m = submap(store)
    m._conn_kw = {"dbname": "lux"}
    return m


def test_the_member_dump_is_offered_with_a_postgres_master_on_the_same_database():
    """identify's fetch_prior takes one scan instead of a probe per member
    when the map can be dumped. A subsidiary can, as the overlay of its own
    rows on the master's, when one query can read both."""
    assert subsidiary.postgres.IdMap.supports_member_dump is True
    assert dump_submap(pg_store()).supports_member_dump is True


@pytest.mark.parametrize("why, store", [
    ("another server", lambda: pg_store(_conn_kw={"dbname": "lux", "host": "other"})),
    ("other curies", lambda: pg_store(prefix_map_in={INTERNAL: "yuid"})),
    ("a subsidiary master", lambda: object.__new__(IdMap)),
    ("not postgres", lambda: pg_master()),
])
def test_the_member_dump_is_declined_otherwise(why, store):
    s = store()
    if isinstance(s, IdMap):
        s._conn_kw = {"dbname": "lux"}
        s.prefix_map_in = {INTERNAL: "yuid", AAT: "aat"}
    m = dump_submap(s)
    assert m.supports_member_dump is False, why
    with pytest.raises(NotImplementedError):
        m.dump_members(None)


def test_the_member_dump_overlays_local_rows_on_the_master():
    m = dump_submap(pg_store())
    seen = {}

    class Cur:
        rowcount = 7

        def copy_expert(self, sql, fh):
            seen["sql"] = " ".join(sql.split())
            seen["fh"] = fh

    @contextmanager
    def cursor():
        yield Cur()

    m._cursor = cursor
    assert m.dump_members("FH") == 7
    sql = seen["sql"]
    assert seen["fh"] == "FH"
    assert sql.startswith("COPY ( SELECT uri, yuid FROM sub UNION ALL")
    assert "FROM idmap m" in sql
    assert "NOT EXISTS (SELECT 1 FROM sub l WHERE l.uri = m.uri)" in sql
    assert "NOT EXISTS (SELECT 1 FROM sub_origin o WHERE o.key = m.uri)" in sql


def test_has_update_token_multi_answers_in_full_form_per_chunk():
    store = pg_store(update_token=TOKEN)
    store.prefix_map_out = {v: key for (key, v) in store.prefix_map_in.items()}
    sent = []

    class Cur:
        def execute(self, sql, params):
            sent.append(params)
            self.rows = [(ik,) for ik in params[0] if ik != y_in(2)]

        def fetchall(self):
            return self.rows

    @contextmanager
    def cursor():
        yield Cur()

    store._cursor = cursor
    got = store.has_update_token_multi([y(1), y(2), y(3)], chunk=2)
    assert got == {y(1), y(3)}
    assert [p[0] for p in sent] == [[y_in(1), y_in(2)], [y_in(3)]]
    assert all(p[1] == TOKEN for p in sent)


# ------------------------------------------------- apply: only what is written

def asked(master):
    """The keys the master was asked about, tokens aside."""
    return {c for c in master.calls if not c.startswith("token")}


def test_assign_bulk_hydrates_only_what_it_writes():
    """identify passes every member's prior. A member already on its YUID is
    not written, so it needs no baseline: hydrating those copied the whole
    master into the subsidiary, one random read at a time."""
    master = three_classes()
    m = submap(master)
    items = [
        # unchanged: both members already on y(1)
        (y(1), [k(1), k(2)], {k(1): y(1), k(2): y(1)}),
        # k(4) leaves y(3) for y(2); k(3) stays
        (y(2), [k(3), k(4)], {k(3): y(2), k(4): y(3)}),
    ]
    m.assign_bulk(items)
    # asked about the mover and both classes it touches, nothing of y(1)
    assert asked(master) == {k(4), y(2), y(3)}
    assert k_in(1) not in m.state["origin"] and y_in(1) not in m.state["origin"]
    assert m.state["rows"] == {k_in(3): y_in(2), k_in(4): y_in(2)}
    # the old class's baseline is recorded before the move
    assert m.state["origin"][k_in(4)][2] == y_in(3)
    assert [r[1:] for r in m.state["log"]] == [(k_in(4), y_in(3), y_in(2))]


def test_a_new_member_hydrates_its_target_class():
    """No prior: it is written, and the class it lands in is copied whole
    first, so the YUID's members here are not just the newcomer."""
    master = three_classes()
    m = submap(master)
    m.assign_bulk([(y(1), [k(1), k(2), STRANGER], {k(1): y(1), k(2): y(1)})])
    assert asked(master) == {STRANGER, y(1)}
    assert m.state["rows"] == {k_in(1): y_in(1), k_in(2): y_in(1), STRANGER_IN: y_in(1)}
    assert m.state["origin"][STRANGER_IN][2] is None


def test_without_priors_everything_is_hydrated_as_before():
    master = three_classes()
    m = submap(master)
    m.assign_bulk([(y(1), [k(1), k(2)], None)])
    assert {k(1), k(2), y(1)} <= asked(master)
    assert m.state["origin"][k_in(2)][2] == y_in(1)


def test_nothing_changed_asks_the_master_nothing():
    master = three_classes()
    m = submap(master)
    m.assign_bulk([(y(1), [k(1), k(2)], {k(1): y(1), k(2): y(1)}),
                   (y(3), [k(4)], {k(4): y(3)})])
    assert master.calls == [] and m.state["origin"] == {} and m.state["log"] == []


def copy_cursor(seen, rowcount=5):
    class Cur:
        def copy_expert(self, sql, fh):
            seen.append(" ".join(sql.split()))
            self.rowcount = rowcount

    @contextmanager
    def cursor():
        yield Cur()
    return cursor


def test_the_registry_dump_overlays_the_masters():
    """identify's presence step diffs the registry against every YUID it
    assigned. The local registry alone holds only hydrated classes, so each
    YUID kept from the master would come out "new" and be registered here."""
    m = dump_submap(pg_store())
    seen = []
    m._cursor = copy_cursor(seen)
    assert m.dump_registry(None) == 5
    sql = seen[0]
    assert sql.startswith("COPY ( SELECT yuid, coalesce(missing_since, '') FROM sub_yuid UNION ALL")
    assert "FROM idmap_yuid m" in sql
    assert "NOT EXISTS (SELECT 1 FROM sub_yuid l WHERE l.yuid = m.yuid)" in sql
    assert "NOT EXISTS (SELECT 1 FROM sub_origin o WHERE o.key = m.yuid)" in sql


def test_the_registry_dump_is_local_only_without_a_dumpable_master():
    m = dump_submap(pg_master())
    seen = []
    m._cursor = copy_cursor(seen)
    m.dump_registry(None)
    assert seen == ["COPY (SELECT yuid, coalesce(missing_since, '') FROM sub_yuid) TO STDOUT"]
