"""The reference queues take a connection of their own only when it buys
something.

They have always had one, for a single reason: they run with
`synchronous_commit = off`, and that must not leak onto the identity map
sharing the same database. When the server already defaults to off -- which
it does on the build box -- the SET is a no-op, the separation protects
nothing, and the connection is one of four per worker, 96 across a 24-way
build.

So the tag is claimed only when the session setting would actually differ
from the server's. The separation has to come back on its own if anyone turns
synchronous commit back on, which is what most of these check.

No live postgres -- psycopg2.connect is stubbed.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest

from pipeline.storage.idmap import postgres as pgmap


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
        return (self.conn.server_setting,)


class Conn:
    closed = False

    def __init__(self, server_setting):
        self.autocommit = False
        self.executed = []
        self.server_setting = server_setting

    def cursor(self):
        return Cursor(self)


@pytest.fixture
def server(monkeypatch):
    """A stubbed server whose synchronous_commit the test chooses."""
    made = []

    class Factory:
        setting = "off"

        def connect(self, **kw):
            made.append(Conn(self.setting))
            return made[-1]

    f = Factory()
    monkeypatch.setattr(pgmap.psycopg2, "connect", lambda **kw: f.connect(**kw))
    monkeypatch.setattr(pgmap, "_SHARED_CONNECTIONS", {})
    monkeypatch.setattr(pgmap, "_SERVER_SYNC_COMMIT", {})
    monkeypatch.setattr(pgmap, "_SHARE_NOTED", set())
    f.made = made
    return f


KW = {"user": "u", "dbname": "d"}


def test_no_second_connection_when_the_server_is_already_off(server):
    server.setting = "off"
    conn = pgmap._refs_connection(KW, {})
    # one connection total: the untagged one, asked and then reused
    assert len(server.made) == 1
    assert conn is server.made[0]
    assert ("refs" not in [k[1] for k in pgmap._SHARED_CONNECTIONS])


def test_the_separation_comes_back_when_the_server_is_durable(server):
    server.setting = "on"
    conn = pgmap._refs_connection(KW, {})
    # one to ask on, one for the queues
    assert len(server.made) == 2
    assert conn is server.made[1]
    assert "SET synchronous_commit = off" in conn.executed
    assert "refs" in [k[1] for k in pgmap._SHARED_CONNECTIONS]


def test_local_is_treated_as_durable(server):
    """synchronous_commit has five values; only 'off' skips the WAL wait."""
    server.setting = "local"
    pgmap._refs_connection(KW, {})
    assert len(server.made) == 2


def test_asyncCommit_false_never_wants_its_own_connection(server):
    """It asks for no session setting, so it has nothing to keep apart."""
    server.setting = "on"
    conn = pgmap._refs_connection(KW, {"asyncCommit": False})
    assert len(server.made) == 1
    assert conn.executed == []


def test_the_server_is_asked_once_per_target(server):
    server.setting = "off"
    for _ in range(5):
        pgmap._refs_connection(KW, {})
    shows = [q for c in server.made for q in c.executed if "SHOW" in q]
    assert len(shows) == 1


def test_both_reference_maps_land_on_the_same_connection(server):
    """all_refs and done_refs share, as they always have."""
    server.setting = "off"
    a = pgmap._refs_connection(KW, {})
    b = pgmap._refs_connection(KW, {})
    assert a is b


def test_an_unreadable_setting_keeps_the_separate_connection(server, capsys):
    """The conservative answer: assume the server wants durability."""
    def boom(**kw):
        raise RuntimeError("no server")
    server.connect = boom
    with pytest.raises(Exception):
        pgmap._connect(KW)
    # the helper swallows it and assumes 'on'
    assert pgmap._server_sync_commit(KW) == "on"
    assert "assuming 'on'" in capsys.readouterr().out
