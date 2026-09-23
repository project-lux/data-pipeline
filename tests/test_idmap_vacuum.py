"""The identity map vacuums itself at the end of identify.

Nothing else does. The table carries no storage parameters and autovacuum's
default 20% scale factor means a 50M-row map needs ~10M dead tuples before it
fires -- sampled mid-build it had 5.1M dead, 9.1%, and **autovacuums = 0**.
Every build adds a fresh crop, because assign_bulk() is INSERT ... ON CONFLICT
DO UPDATE and leaves a dead tuple for every member it moves.

Identify is the only writer and does not care. Merge is the phase that pays,
probing idmap_pkey (5.9GB) and idmap_yuid_idx (3.2GB) tens of millions of
times against a 9% bloated index.

No live postgres -- the cursor is stubbed.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.storage.idmap.postgres import IdMap


class Cursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass

    def execute(self, sql, params=None):
        self.conn.executed.append(sql)
        # the table is the last token, so fail_on="idmap" fails that table
        # and not idmap_yuid, which a substring match would also catch
        if sql.startswith("VACUUM") and sql.split()[-1] == self.conn.fail_on:
            raise RuntimeError("could not get lock")

    def fetchall(self):
        return self.conn.stats


class Conn:
    def __init__(self, stats=(), fail_on="\0"):
        self.executed = []
        self.stats = list(stats)
        self.fail_on = fail_on

    def cursor(self):
        return Cursor(self)


def idmap(stats=(), fail_on="\0"):
    m = object.__new__(IdMap)
    m.table, m.yuid_table = "idmap", "idmap_yuid"
    m.conn = Conn(stats, fail_on)
    return m


def vacuums(conn):
    return [q for q in conn.executed if q.startswith("VACUUM")]


def test_both_tables_are_vacuumed():
    m = idmap()
    m.optimize(report=False)
    assert vacuums(m.conn) == ["VACUUM (ANALYZE, FREEZE) idmap",
                               "VACUUM (ANALYZE, FREEZE) idmap_yuid"]


def test_both_options_can_be_turned_off():
    """`VACUUM () idmap` is a syntax error -- with neither option the
    statement has to come out as the plain vacuum that was asked for."""
    m = idmap()
    m.optimize(analyze=False, freeze=False, report=False)
    assert vacuums(m.conn) == ["VACUUM idmap", "VACUUM idmap_yuid"]


def test_freeze_is_on_by_default():
    """Without it a vacuum can only advance relfrozenxid to the current XID
    minus vacuum_freeze_min_age, so the map keeps ageing towards
    autovacuum_freeze_max_age and eventually trips a forced anti-wraparound
    autovacuum that will not yield its lock to anything."""
    m = idmap()
    m.optimize(report=False)
    assert all("FREEZE" in q for q in vacuums(m.conn))


def test_freeze_can_be_turned_off_on_its_own():
    """The first FREEZE reads the whole table; a caller that only wants the
    planner statistics refreshed should not have to pay for that."""
    m = idmap()
    m.optimize(freeze=False, report=False)
    assert vacuums(m.conn) == ["VACUUM (ANALYZE) idmap",
                               "VACUUM (ANALYZE) idmap_yuid"]


def test_a_failure_on_one_table_does_not_skip_the_other():
    """A VACUUM that cannot get its lock should not cost you the second
    table, or leave identify looking like it succeeded."""
    m = idmap(fail_on="idmap")
    m.optimize(report=False)
    assert "VACUUM (ANALYZE, FREEZE) idmap_yuid" in m.conn.executed


def test_the_report_reads_the_stats_before_vacuuming():
    """VACUUM zeroes n_dead_tup, so the numbers have to come first."""
    m = idmap(stats=[("idmap", 50_761_975, 5_101_830, None)])
    before = m.optimize(report=True)
    assert before == {"idmap": (50_761_975, 5_101_830, None)}
    assert m.conn.executed[0].startswith("SELECT relname")
    assert m.conn.executed[1].startswith("VACUUM")


def test_never_autovacuumed_is_called_out(capsys):
    m = idmap(stats=[("idmap", 50_761_975, 5_101_830, None)])
    m.optimize()
    out = capsys.readouterr().out
    assert "9.1%" in out
    assert "never autovacuumed" in out


def test_stats_failure_does_not_stop_the_vacuum():
    """Reporting is a nicety; the vacuum is the point."""
    class Broken(Conn):
        first = True

        def cursor(self):
            if self.first:            # only the stats query
                self.first = False
                raise RuntimeError("stats view unavailable")
            return Cursor(self)

    m = idmap()
    m.conn = Broken()
    m.optimize(report=True)
    assert vacuums(m.conn) == ["VACUUM (ANALYZE, FREEZE) idmap",
                               "VACUUM (ANALYZE, FREEZE) idmap_yuid"]
