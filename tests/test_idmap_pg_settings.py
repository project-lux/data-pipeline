"""Where the Postgres maps connect, and what happens when the table they find
is not an idmap.

The maps take their database from the cache defaults, as the caches do. They
used to read only the single `caches` record, so an instance whose defaults
are in default_cache.json (pipeline-configs, `lux initialize`) sent the idmap
to $USER's database over the local socket while the caches went to the
configured one; an old `idmap` table there survived CREATE TABLE IF NOT
EXISTS and every prepared statement failed with `column "uri" does not exist`.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import psycopg2
import pytest

from lux_pipeline.storage.idmap.postgres import IdMap, _connect_kwargs


class Cfgs:
    def __init__(self, caches=None, default_cache=None):
        self.caches = caches or {}
        self.defaults = {"cache": default_cache or {}, "map": {}, "marklogic": {}}


PG = {"host": "db.example", "port": 5433, "user": "lux", "dbname": "luxdb", "password": "pw"}


def test_the_cache_defaults_record_is_used():
    kw = _connect_kwargs({"host": "localhost", "port": 6379}, Cfgs(default_cache=PG))
    assert (kw["host"], kw["port"], kw["user"], kw["dbname"], kw["password"]) == \
        ("db.example", 5433, "lux", "luxdb", "pw")


def test_a_caches_record_alone_still_works():
    kw = _connect_kwargs({}, Cfgs(caches=PG))
    assert (kw["host"], kw["dbname"]) == ("db.example", "luxdb")


def test_the_map_records_redis_host_and_port_are_never_used():
    kw = _connect_kwargs({"host": "redis.example", "port": 6379}, Cfgs(default_cache=PG))
    assert kw["host"] == "db.example" and kw["port"] == 5433


def test_per_map_overrides_win():
    kw = _connect_kwargs({"pgHost": "other", "pgPort": 6000, "pgDbname": "x", "pgUser": "u"},
                         Cfgs(default_cache=PG))
    assert (kw["host"], kw["port"], kw["dbname"], kw["user"]) == ("other", 6000, "x", "u")


def test_no_settings_is_the_local_socket_as_the_user(monkeypatch):
    monkeypatch.setenv("USER", "someone")
    kw = _connect_kwargs({}, Cfgs())
    assert "host" not in kw and kw["user"] == "someone" and kw["dbname"] == "someone"


class Hot:
    def __init__(self, error):
        self.error = error
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append(sql)
        if self.error is not None:
            raise self.error


class Conn:
    def __init__(self):
        self.rolled_back = 0

    def rollback(self):
        self.rolled_back += 1


def bare_idmap(error):
    m = IdMap.__new__(IdMap)
    m.table = "idmap"
    m.yuid_table = "idmap_yuid"
    m._conn_kw = {"dbname": "rs2668", "user": "rs2668"}
    m._hot = Hot(error)
    m.conn = Conn()
    return m


@pytest.mark.parametrize("cls", [psycopg2.errors.UndefinedColumn, psycopg2.errors.UndefinedTable])
def test_a_table_that_is_not_an_idmap_stops_with_the_database_named(cls):
    m = bare_idmap(cls('column "uri" does not exist\nLINE 1: PREPARE idmap_fwd_f080 AS ...'))
    with pytest.raises(ValueError) as e:
        m._prepare()
    msg = str(e.value)
    assert "'idmap'" in msg and "rs2668@local socket" in msg and 'column "uri" does not exist' in msg
    assert "LINE 1" not in msg


def test_other_prepare_errors_still_fall_back_to_plain_sql(caplog):
    m = bare_idmap(psycopg2.errors.SyntaxError("syntax error"))
    m._prepare()
    assert m._stmt == {} and m.conn.rolled_back == len(IdMap.PREPARED)


def test_a_good_table_prepares_everything():
    m = bare_idmap(None)
    m._prepare()
    assert set(m._stmt) == set(IdMap.PREPARED)
