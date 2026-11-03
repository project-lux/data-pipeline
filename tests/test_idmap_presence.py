"""Which YUIDs a build saw, recorded as the ones it did not.

The registry used to carry "the build that last saw this YUID" as a token
column, stamped by identify for every cluster, every build: 45.5M rows
serialized, sent, probed and -- the token being new every build -- rewritten,
to record that nothing had changed. The information is wanted (a YUID whose
records are deleted at the source should outlive them for a while, in case
they come back, then go), so it is kept as the complement: `missing_since`,
the first build that did not see the YUID, NULL while it is current. Identify
dumps the registry, diffs it against the clusters it just applied, and hands
over only the differences; `lux pg-maintain --expire-yuids DAYS` deletes
what has been missing long enough.

No live postgres: the cursor is stubbed and the resolver is driven against a
dict-shaped fake, as tests/test_identity_streaming.py does.
"""

import datetime
import io
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest

from lux_pipeline.process.identity_resolver import IdentityResolver
from lux_pipeline.storage.idmap import postgres as pgmap
from lux_pipeline.storage.idmap.postgres import IdMap

from test_identity_streaming import FakeIdMap, StubConfigs, make_clusters, qua, write_assertions


# --- the diff itself ----------------------------------------------------------

def diff(tmp_path, registry, seen):
    r = object.__new__(IdentityResolver)
    reg = tmp_path / "registry.sorted"
    reg.write_text("".join(f"{y}\t{m}\n" for y, m in sorted(registry)))
    sn = tmp_path / "seen.sorted"
    sn.write_text("".join(f"{y}\n" for y in sorted(set(seen))))
    return list(r._diff_presence(str(reg), str(sn)))


def test_the_three_kinds_of_difference(tmp_path):
    got = diff(tmp_path,
               registry=[("yuid:a", ""), ("yuid:b", ""), ("yuid:c", "__20260901__"),
                         ("yuid:d", "__20260901__")],
               seen=["yuid:a", "yuid:c", "yuid:e"])
    assert got == [("missing", "yuid:b"),       # registered, current, not seen
                   ("returned", "yuid:c"),      # seen, was missing
                   ("new", "yuid:e")]           # seen, not registered
    # d: already missing and still not seen -- nothing to write
    assert not any(y == "yuid:d" for _, y in got)
    # a: current and seen -- nothing to write
    assert not any(y == "yuid:a" for _, y in got)


def test_nothing_changed_yields_nothing(tmp_path):
    """The whole point: a build that saw what the last one saw writes no
    registry row at all."""
    registry = [(f"yuid:{i}", "") for i in range(1000)]
    assert diff(tmp_path, registry, [y for y, _ in registry]) == []


def test_seen_beyond_the_registry_and_an_empty_registry(tmp_path):
    assert diff(tmp_path, [], ["yuid:a", "yuid:b"]) == [("new", "yuid:a"), ("new", "yuid:b")]
    assert diff(tmp_path, [("yuid:a", "")], []) == [("missing", "yuid:a")]
    assert diff(tmp_path, [("yuid:a", ""), ("yuid:z", "")], ["yuid:m"]) == [
        ("missing", "yuid:a"), ("new", "yuid:m"), ("missing", "yuid:z")]


# --- through resolve_identity ----------------------------------------------------

class PresenceFakeIdMap(FakeIdMap):
    """The streaming fake plus the registry's presence surface."""

    def __init__(self, configs, token="__testtok__"):
        super().__init__(configs, token)
        self.missing = {}       # shortened yuid -> missing_since token
        self.presence_calls = []

    def dump_registry(self, fh):
        for y in self.yuids:
            fh.write(f"{y}\t{self.missing.get(y, '')}\n")
        return len(self.yuids)

    def record_presence(self, missing=(), returned=(), new=()):
        counts = {"missing": 0, "returned": 0, "new": 0}
        for y in missing:
            iy = self._manage_value_in(y)
            assert iy in self.yuids
            if iy not in self.missing:
                self.missing[iy] = self.update_token
                counts["missing"] += 1
        for y in returned:
            iy = self._manage_value_in(y)
            if self.missing.pop(iy, None) is not None:
                counts["returned"] += 1
        for y in new:
            iy = self._manage_value_in(y)
            if iy not in self.yuids:
                self.yuids[iy] = self.update_token
                counts["new"] += 1
        self.presence_calls.append(counts)
        return counts


def resolve(cfgs, idmap, work):
    r = IdentityResolver(cfgs, idmap)
    return r.resolve_identity(conflicts_file=str(Path(work) / "conf.jsonl"), work_dir=str(work))


def test_a_vanished_record_leaves_its_yuid_marked_missing_and_kept(tmp_path):
    """The record deleted at its source: its cluster is not asserted, its
    YUID is marked missing as of this build, and its member row stays so
    the identity is still there to claim if the record returns."""
    rng = random.Random(1)
    cfgs = StubConfigs(temp_dir=tmp_path)
    kept = "http://vocab.getty.edu/aat/1"
    gone = "http://vocab.getty.edu/aat/2"
    write_assertions(cfgs, [[kept]], 1, rng)
    idmap = PresenceFakeIdMap(cfgs, token="__20261106__")
    y_kept = f"{cfgs.internal_uri}concept/KEPT"
    y_gone = f"{cfgs.internal_uri}concept/GONE"
    idmap.seed(qua(kept), y_kept)
    idmap.seed(qua(gone), y_gone)

    stats = resolve(cfgs, idmap, tmp_path)

    assert stats["yuids_missing"] == 1 and stats["yuids_returned"] == 0
    assert idmap.missing == {idmap._manage_value_in(y_gone): "__20261106__"}
    assert idmap.forward()[qua(gone)] == y_gone        # member row kept
    assert idmap._manage_value_in(y_gone) in idmap.yuids


def test_a_returning_record_gets_its_yuid_back_and_is_current_again(tmp_path):
    rng = random.Random(2)
    cfgs = StubConfigs(temp_dir=tmp_path)
    back = "http://vocab.getty.edu/aat/3"
    write_assertions(cfgs, [[back]], 1, rng)
    idmap = PresenceFakeIdMap(cfgs, token="__20261110__")
    y_back = f"{cfgs.internal_uri}concept/BACK"
    idmap.seed(qua(back), y_back)
    idmap.missing[idmap._manage_value_in(y_back)] = "__20261106__"

    stats = resolve(cfgs, idmap, tmp_path)

    assert stats["yuids_returned"] == 1 and stats["yuids_missing"] == 0
    assert idmap.missing == {}
    assert idmap.forward()[qua(back)] == y_back        # the same identity


def test_an_unchanged_build_writes_no_presence_at_all(tmp_path):
    rng = random.Random(3)
    cfgs = StubConfigs(temp_dir=tmp_path)
    clusters = make_clusters(rng, 30)
    write_assertions(cfgs, clusters, 2, rng)
    idmap = PresenceFakeIdMap(cfgs)
    stats = resolve(cfgs, idmap, tmp_path)
    # first build: everything minted, registered by assign_bulk, nothing
    # to diff
    assert stats["yuids_new"] == 0 and stats["yuids_missing"] == 0
    before = dict(idmap.yuids)

    write_assertions(cfgs, clusters, 3, random.Random(4))
    stats = resolve(cfgs, idmap, tmp_path)
    assert (stats["yuids_missing"], stats["yuids_returned"], stats["yuids_new"]) == (0, 0, 0)
    assert idmap.yuids == before
    assert idmap.missing == {}


def test_a_yuid_the_sweep_deleted_is_not_marked_missing(tmp_path):
    """The dead-set sweep runs first: a YUID whose members all moved away is
    deleted outright, not kept as missing -- there is no member row left
    for a returning record to find it through."""
    rng = random.Random(5)
    cfgs = StubConfigs(temp_dir=tmp_path)
    members = [f"http://vocab.getty.edu/aat/{i}" for i in (10, 11, 12)]
    write_assertions(cfgs, [members], 1, rng)
    idmap = PresenceFakeIdMap(cfgs)
    ya = f"{cfgs.internal_uri}concept/AAA"
    yb = f"{cfgs.internal_uri}concept/BBB"
    idmap.seed(qua(members[0]), ya)
    idmap.seed(qua(members[1]), ya)
    idmap.seed(qua(members[2]), yb)
    stats = resolve(cfgs, idmap, tmp_path)
    assert stats["deleted_yuids"] == 1
    assert stats["yuids_missing"] == 0
    assert idmap.missing == {}


def test_a_backend_without_a_registry_dump_is_left_alone(tmp_path):
    """redis-shaped maps stamp tokens in assign_bulk as they always did."""
    rng = random.Random(6)
    cfgs = StubConfigs(temp_dir=tmp_path)
    write_assertions(cfgs, make_clusters(rng, 5), 1, rng)
    idmap = FakeIdMap(cfgs)
    stats = resolve(cfgs, idmap, tmp_path)
    assert (stats["yuids_missing"], stats["yuids_returned"], stats["yuids_new"]) == (0, 0, 0)


# --- the postgres side ----------------------------------------------------------

class Cursor:
    def __init__(self, conn):
        self.conn = conn
        self.rowcount = 0
        self.itersize = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass

    def execute(self, sql, params=None):
        self.conn.executed.append((sql, params))
        self.rowcount = self.conn.rowcount
        self.result = self.conn.results.pop(0) if self.conn.results else []

    def fetchone(self):
        return self.result[0] if self.result else None

    def __iter__(self):
        return iter(self.result)

    def close(self):
        pass

    def copy_expert(self, sql, fh):
        self.conn.executed.append((sql, None))
        fh.write(self.conn.dump)
        self.rowcount = self.conn.dump.count("\n")


class Conn:
    def __init__(self, results=(), rowcount=3, dump=""):
        self.executed = []
        self.results = list(results)
        self.rowcount = rowcount
        self.dump = dump
        self.autocommit = True
        self.closed = False

    def cursor(self, name=None):
        return Cursor(self)

    def commit(self):
        pass

    def close(self):
        self.closed = True


class Configs:
    internal_uri = "https://lux.collections.yale.edu/data/"


def idmap(**kw):
    m = object.__new__(IdMap)
    m.configs = Configs()
    m.table, m.yuid_table = "idmap", "idmap_yuid"
    m.prefix_map_out = {"yuid": Configs.internal_uri}
    m.prefix_map_in = {Configs.internal_uri: "yuid"}
    m.memory_cache_enabled = False
    m.memory_cache = set(["stale"])
    m.update_token = "__20261106__"
    m.conn = Conn(**kw)
    m._conn_kw = {}
    return m


def sql_of(conn):
    return [s for s, _ in conn.executed]


def test_record_presence_writes_each_kind_once_guarded(monkeypatch):
    batched = []
    monkeypatch.setattr(pgmap.psycopg2.extras, "execute_values",
                        lambda cur, sql, args, **kw: batched.append((sql, list(args))))
    m = idmap(rowcount=2)
    got = m.record_presence(missing=[f"{Configs.internal_uri}concept/A", f"{Configs.internal_uri}concept/B"],
                            returned=[f"{Configs.internal_uri}concept/C"],
                            new=[f"{Configs.internal_uri}concept/D"])
    (s_missing, p_missing), (s_returned, p_returned) = m.conn.executed
    assert "SET missing_since = %s WHERE yuid = ANY(%s) AND missing_since IS NULL" in s_missing
    assert p_missing == ("__20261106__", ["yuid:concept/A", "yuid:concept/B"])
    assert "SET missing_since = NULL WHERE yuid = ANY(%s) AND missing_since IS NOT NULL" in s_returned
    assert p_returned == (["yuid:concept/C"],)
    [(s_new, rows)] = batched
    assert "ON CONFLICT (yuid) DO NOTHING" in s_new
    assert rows == [("yuid:concept/D", "__20261106__")]
    assert got == {"missing": 2, "returned": 2, "new": 0}


def test_record_presence_with_nothing_sends_nothing(monkeypatch):
    monkeypatch.setattr(pgmap.psycopg2.extras, "execute_values",
                        lambda *a, **kw: pytest.fail("nothing to insert"))
    m = idmap()
    assert m.record_presence() == {"missing": 0, "returned": 0, "new": 0}
    assert m.conn.executed == []


def test_record_presence_chunks_the_lists(monkeypatch):
    monkeypatch.setattr(pgmap.psycopg2.extras, "execute_values", lambda *a, **kw: None)
    m = idmap(rowcount=1)
    m.record_presence(missing=(f"{Configs.internal_uri}concept/{i}" for i in range(2500)), chunk=1000)
    assert [len(p[1]) for _, p in m.conn.executed] == [1000, 1000, 500]


def test_dump_registry_is_one_copy_with_blank_for_current():
    m = idmap(dump="yuid:concept/A\t\nyuid:concept/B\t__20260901__\n")
    fh = io.StringIO()
    assert m.dump_registry(fh) == 2
    [(sql, _)] = m.conn.executed
    assert sql == "COPY (SELECT yuid, coalesce(missing_since, '') FROM idmap_yuid) TO STDOUT"
    assert fh.getvalue().splitlines() == ["yuid:concept/A\t", "yuid:concept/B\t__20260901__"]


def test_expiry_token_is_a_build_token_days_ago():
    assert IdMap.expiry_token(180, now=datetime.date(2026, 11, 6)) == "__20260510__"
    assert IdMap.expiry_token(0, now=datetime.date(2026, 1, 1)) == "__20260101__"
    # tokens compare as strings: a same-day letter token is not before the day
    assert "__20260510a__" > "__20260510__"
    assert "__20260509b__" < "__20260510__"


def test_expire_yuids_deletes_members_then_registry_in_chunks(monkeypatch):
    m = idmap(results=[[("yuid:concept/A",), ("yuid:concept/B",), ("yuid:concept/C",)]], rowcount=2)
    # the list is read on a connection of its own (a named cursor)
    monkeypatch.setattr(pgmap.psycopg2, "connect", lambda **kw: m.conn)
    got = m.expire_yuids("__20260510__", chunk=2)
    assert got == {"yuids": 4, "members": 4, "cutoff": "__20260510__"}
    sql = sql_of(m.conn)
    assert sql[0] == "SELECT yuid FROM idmap_yuid WHERE missing_since < %s"
    assert m.conn.executed[0][1] == ("__20260510__",)
    assert sql[1:] == ["DELETE FROM idmap WHERE yuid = ANY(%s)",
                       "DELETE FROM idmap_yuid WHERE yuid = ANY(%s)"] * 2
    assert m.conn.executed[1][1] == (["yuid:concept/A", "yuid:concept/B"],)
    assert m.conn.executed[3][1] == (["yuid:concept/C"],)
    assert not m.memory_cache


def test_expire_yuids_dry_run_only_counts():
    m = idmap(results=[[(7,)], [(21,)]])
    got = m.expire_yuids("__20260510__", dry_run=True)
    assert got == {"yuids": 7, "members": 21, "cutoff": "__20260510__"}
    assert all(s.startswith("SELECT count(*)") for s in sql_of(m.conn))
    assert m.memory_cache == {"stale"}


def test_missing_summary():
    m = idmap(results=[[(12, "__20260901__")]])
    assert m.missing_summary() == (12, "__20260901__")


def test_an_old_map_gets_the_column_once():
    """_ensure_schema adds missing_since only when the catalog says the
    table lacks it: ALTER TABLE takes the exclusive lock even when IF NOT
    EXISTS turns it into a no-op, and 24 workers open the map at once."""
    m = idmap(results=[[("idmap",)], [], [], [(1,)]])   # exists; no column; (no index row)
    m._ensure_schema()
    sql = sql_of(m.conn)
    assert any(s.startswith("ALTER TABLE idmap_yuid ADD COLUMN IF NOT EXISTS missing_since") for s in sql)

    m = idmap(results=[[("idmap",)], [(1,)], [(1,)]])   # exists; column there
    m._ensure_schema()
    assert not any(s.startswith("ALTER TABLE") for s in sql_of(m.conn))


def test_new_tables_carry_the_column():
    assert "missing_since TEXT" in pgmap.TABLES


def test_the_registry_backs_up_and_restores_with_the_column(monkeypatch):
    """Portable rows gain a fourth element; a three-element row (a backup
    from before) still loads."""
    loads = []
    monkeypatch.setattr(pgmap, "pg_load", lambda conn, specs: loads.append(
        [(t, cols, reader.read().decode()) for t, cols, reader in specs]))
    monkeypatch.setattr(pgmap.psycopg2, "connect", lambda **kw: Conn())
    m = idmap()
    m.load_portable([], [(f"{Configs.internal_uri}concept/A", "__20260901__", None, "__20261001__"),
                         (f"{Configs.internal_uri}concept/B", "__20260901__", None)])
    [(table, cols, text), _members] = loads[0]
    assert table == "idmap_yuid" and cols == ["yuid", "token", "minted", "missing_since"]
    assert text.splitlines() == ["yuid:concept/A\t__20260901__\t\\N\t__20261001__",
                                 "yuid:concept/B\t__20260901__\t\\N\t\\N"]
