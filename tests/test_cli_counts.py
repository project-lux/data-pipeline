"""`lux counts`: cache, map and index counts from a stubbed configuration."""

import sys
from argparse import Namespace
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from lux_pipeline.cli import counts
from lux_pipeline.cli.counts import CommandHandler
from lux_pipeline.storage.idmap.lmdb import TabLmdb


# --- stubs -------------------------------------------------------------------


class StubCache:
    """A cache like the postgres one: a cheap estimate plus an exact len."""

    def __init__(self, estimate, exact=None):
        self.estimate = estimate
        self.exact = estimate if exact is None else exact
        self.len_calls = 0

    def len_estimate(self):
        return self.estimate

    def __len__(self):
        self.len_calls += 1
        return self.exact


class PlainCache:
    """A cache with only ``__len__`` (filesystem, redis)."""

    def __init__(self, n):
        self.n = n

    def __len__(self):
        return self.n


class StubStore:
    def __init__(self, n):
        self.n = n

    def __len__(self):
        return self.n


class StubConfigs:
    def __init__(self, tmp=None):
        self.internal = {
            "ycba": {"name": "ycba",
                     "datacache": StubCache(12),
                     "recordcache": StubCache(7),
                     "recordcache2": None},
        }
        self.external = {
            "aat": {"name": "aat",
                    "datacache": StubCache(250_000, exact=123),
                    "recordcache": PlainCache(5),
                    "reconciledRecordcache": StubCache(99_999, exact=99_998),
                    "recordcache2": StubCache(3),
                    "reconcileDbPath": str(tmp / "label_to_aat.lmdb") if tmp else "",
                    "inverseEquivDbPath": str(tmp / "aat_equivs.lmdb") if tmp else ""},
            "wikidata": {"name": "wikidata",
                         "datacache": StubCache(1),
                         "reconcileDbPath": str(tmp / "nope.lmdb") if tmp else ""},
        }
        self.results = {
            "merged": {"name": "merged",
                       "recordcache": StubCache(40),
                       "recordcache2": StubCache(41)},
        }
        self.map_stores = {
            "idmap": {"name": "idmap", "store": StubStore(1_000_000)},
            "all_refs": {"name": "all_refs"},
            "done_refs": {"name": "done_refs", "store": StubStore(9)},
        }
        self.instantiated = []

    def instantiate_map(self, key):
        self.instantiated.append(key)
        cfg = self.map_stores[key]
        cfg["store"] = StubStore(17)
        return cfg


def run(cfgs, rest=(), source=None):
    """process() with the global options `entry` would have parsed."""
    args = Namespace(command="counts", source=source)
    CommandHandler(cfgs).process(args, list(rest))


def lines(capsys):
    return capsys.readouterr().out.strip().split("\n")


# --- caches ------------------------------------------------------------------


def test_default_counts_every_cache_of_every_group(capsys):
    cfgs = StubConfigs()
    run(cfgs)
    out = lines(capsys)
    assert out == [
        "ycba datacache: =12",
        "ycba recordcache: =7",
        "aat datacache: ~250,000",
        "aat recordcache: =5",
        "aat reconciledRecordcache: =99,998",
        "aat recordcache2: =3",
        "wikidata datacache: =1",
        "merged recordcache: =40",
        "merged recordcache2: =41",
        f"Total cache records: ~{12 + 7 + 250_000 + 5 + 99_998 + 3 + 1 + 40 + 41:,}",
    ]


def test_estimate_replaced_by_exact_len_only_under_threshold():
    cfgs = StubConfigs()
    big = cfgs.external["aat"]["datacache"]
    small = cfgs.external["aat"]["reconciledRecordcache"]
    assert counts._count(big) == (250_000, False)
    assert big.len_calls == 0
    assert counts._count(small) == (99_998, True)
    assert small.len_calls == 1
    assert counts._count(PlainCache(5)) == (5, True)


def test_source_and_cache_filters(capsys):
    run(StubConfigs(), ["--cache", "datacache,recordcache2"], source="aat,ycba")
    assert lines(capsys) == [
        "aat datacache: ~250,000",
        "aat recordcache2: =3",
        "ycba datacache: =12",
        "Total cache records: ~250,015",
    ]


def test_source_group(capsys):
    run(StubConfigs(), source="results")
    assert lines(capsys) == [
        "merged recordcache: =40",
        "merged recordcache2: =41",
        "Total cache records: ~81",
    ]


def test_unknown_source_is_an_error():
    with pytest.raises(ValueError, match="Unknown source: nope"):
        run(StubConfigs(), source="aat,nope")


# --- maps --------------------------------------------------------------------


def test_map_all_instantiates_missing_stores_and_has_its_own_total(capsys):
    cfgs = StubConfigs()
    run(cfgs, ["--map", "all"])
    # no --source with --map: caches are not counted, so the map total is
    # only the maps (the source handler added them onto the cache total)
    assert lines(capsys) == [
        "idmap: ~1,000,000",
        "all_refs: ~17",
        "done_refs: ~9",
        "Total map records: ~1,000,026",
    ]
    assert cfgs.instantiated == ["all_refs"]


def test_named_maps_and_caches_together(capsys):
    run(StubConfigs(), ["--map", "done_refs"], source="ycba")
    assert lines(capsys) == [
        "ycba datacache: =12",
        "ycba recordcache: =7",
        "Total cache records: ~19",
        "done_refs: ~9",
        "Total map records: ~9",
    ]


def test_unknown_map_is_an_error():
    with pytest.raises(ValueError, match="Unknown map: nope"):
        run(StubConfigs(), ["--map", "nope"])


# --- indexes -----------------------------------------------------------------


@pytest.fixture
def indexed(tmp_path):
    cfgs = StubConfigs(tmp_path)
    for path, n in [(cfgs.external["aat"]["reconcileDbPath"], 3),
                    (cfgs.external["aat"]["inverseEquivDbPath"], 2)]:
        db = TabLmdb.open(path, "c")
        for i in range(n):
            db[f"key{i}"] = f"value{i}"
        db.commit()
        db.close()
    # wikidata's reconcileDbPath points at a file that was never built
    return cfgs


def test_index_all_opens_each_configured_path_read_only(indexed, capsys):
    run(indexed, ["--index", "all"])
    assert lines(capsys) == [
        "aat name index: =3",
        "aat id index: =2",
        "wikidata name index: missing",
        "Total index records: =5",
    ]


def test_index_by_source_name(indexed, capsys):
    run(indexed, ["--index", "aat"])
    assert lines(capsys) == [
        "aat name index: =3",
        "aat id index: =2",
        "Total index records: =5",
    ]


def test_index_unknown_source_is_an_error(indexed):
    with pytest.raises(ValueError, match="Unknown source: nope"):
        run(indexed, ["--index", "nope"])


def test_sources_without_index_paths_contribute_nothing(indexed, capsys):
    run(indexed, ["--index", "ycba,merged"])
    assert lines(capsys) == ["Total index records: =0"]


# --- listing -----------------------------------------------------------------


def test_help_lists_counts_with_its_docstring():
    from lux_pipeline.cli.help import list_commands
    assert ("counts", "Count records in caches, maps and indexes.") in list_commands()
