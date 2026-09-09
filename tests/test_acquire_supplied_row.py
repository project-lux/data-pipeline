"""A caller streaming the datacache shouldn't pay to read it again.

run-reconcile used to walk `iter_keys_slice()` and then hand each key to
acquire(), which SELECTed that same row straight back -- a round trip and a
large jsonb parse per record, 43.8M of them in a full build. It now streams
`iter_records_slice()` and passes the row it already holds, the way run-merge
does. These pin the contract that makes that safe: a supplied row replaces the
datacache read and nothing else, and `refetch` still means the network.

No live postgres -- the caches are stubbed.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.process.base.acquirer import Acquirer


class Cache:
    def __init__(self, rows=None):
        self.rows = rows or {}
        self.reads = 0
        self.writes = {}

    def __getitem__(self, key):
        self.reads += 1
        return self.rows.get(key)

    def __setitem__(self, key, value):
        self.writes[key] = value

    def get_multi(self, keys):
        self.reads += 1
        return {k: self.rows[k] for k in keys if k in self.rows}


class Fetcher:
    enabled = True

    def __init__(self):
        self.fetched = []

    def validate_identifier(self, identifier):
        return True

    def fetch(self, identifier):
        self.fetched.append(identifier)
        return {"identifier": identifier, "data": {"id": identifier, "type": "Person"},
                "source": "network"}


class Mapper:
    def returns_multiple(self, record=None):
        return False

    def transform(self, rec, rectype, reference=False):
        # carry the input through so a test can see which row was mapped
        return {"identifier": rec["identifier"], "data": rec["data"],
                "seen_source": rec.get("source")}

    def post_mapping(self, rec, rectype):
        return rec


class Configs:
    ok_record_types = {"Person": "person", "Group": "group"}

    def is_qua(self, key):
        return "##" in key

    def make_qua(self, key, typ):
        return f"{key}##qua{typ}"

    def split_qua(self, key):
        return key.split("##qua") if "##qua" in key else (key, None)


def row(identifier, source="ils"):
    """What iter_records_slice yields, plus the source run-reconcile stamps."""
    return {"identifier": identifier, "data": {"id": identifier, "type": "Person"},
            "source": source}


def acquirer(cached=None):
    a = object.__new__(Acquirer)
    a.config = {"type": "internal", "name": "ils"}
    a.configs = Configs()
    a.datacache = Cache({identifier: row(identifier) for identifier in (cached or [])})
    a.recordcache = Cache()
    a.mapper = Mapper()
    a.fetcher = Fetcher()
    a.debug = 0
    a.name = "ils"
    a.validate = False
    a.raise_on_error = False
    a.validator = None
    a.ignore_sources = []
    return a


def test_supplied_row_is_used_instead_of_reading_the_datacache():
    a = acquirer(cached=["ils:1"])
    got = a.acquire("ils:1", data=row("ils:1"))
    assert got is not None
    assert a.datacache.reads == 0, "the row was already in hand"
    assert a.fetcher.fetched == [], "and nothing had to be fetched"
    # still stored under the row's identifier, as before
    assert "ils:1" in a.recordcache.writes


def test_without_a_row_the_datacache_is_still_read():
    """The --recid path, and every other caller, must be unaffected."""
    a = acquirer(cached=["ils:1"])
    got = a.acquire("ils:1")
    assert got is not None
    assert a.datacache.reads == 1


def test_the_supplied_row_is_what_gets_mapped():
    a = acquirer(cached=["ils:1"])
    supplied = row("ils:1", source="stamped-by-run-reconcile")
    got = a.acquire("ils:1", data=supplied)
    # the mappers and the reconciler read rec["source"], which get() used to
    # stamp and the iterators do not -- so the caller's value has to survive
    assert got["seen_source"] == "stamped-by-run-reconcile"


def test_refetch_ignores_a_supplied_row():
    """refetch means "go to the network"; a cached row cannot satisfy it."""
    a = acquirer(cached=["ils:1"])
    a.acquire("ils:1", data=row("ils:1"), refetch=True)
    assert a.fetcher.fetched == ["ils:1"]
    assert a.datacache.reads == 0


def test_a_missing_row_falls_through_to_the_fetcher():
    """data=None for a key the datacache doesn't have: unchanged behaviour."""
    a = acquirer(cached=[])
    got = a.acquire("ils:9")
    assert got is not None
    assert a.datacache.reads == 1
    assert a.fetcher.fetched == ["ils:9"]
