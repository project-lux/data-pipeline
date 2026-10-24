"""`lux list`: the sources, maps and indexes tables from a stubbed
configuration, rendered to a wide console and checked as text."""

import sys
from argparse import Namespace
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from lux_pipeline.cli import list as list_cmd
from lux_pipeline.cli.list import CommandHandler


class StubConfigs:
    internal = {
        "ycba": {"name": "ycba", "namespace": "https://ycba-lux.yale.edu/data/",
                 "merge_order": 1,
                 "mapperClass": "sources.yale.ycba.mapper.YcbaMapper",
                 "harvesterClass": "process.base.harvester.ASHarvester",
                 "fetcherClass": "process.base.fetcher.Fetcher"},
    }
    external = {
        "aat": {"name": "aat", "namespace": "http://vocab.getty.edu/aat/",
                "merge_order": 10,
                "mapperClass": "sources.getty.mapper.AatMapper",
                "downloaderClass": "process.base.downloader.Downloader",
                "loaderClass": "sources.getty.loader.AatLoader",
                "acquirerClass": "process.base.acquirer.Acquirer",
                "reconcilerClass": "sources.getty.reconciler.AatReconciler",
                "indexLoaderClass": "process.base.index_loader.LmdbIndexLoader",
                "reconcileDbPath": "/data/indexes/label_to_aat.lmdb",
                "inverseEquivDbPath": "/data/indexes/aat_equivs.lmdb"},
        "wikidata": {"name": "wikidata",
                     "namespace": "http://www.wikidata.org/entity/this/is/a/very/long/namespace/path/",
                     "merge_order": 20,
                     "mapperClass": "sources.wikidata.mapper.WdMapper",
                     "reconcileDbPath": "/data/indexes/label_to_wd.lmdb"},
    }
    results = {
        # acquirerClass: null is how the live merged.json switches the
        # acquirer off; a class object is what a config built in code holds
        "merged": {"name": "merged", "namespace": "https://lux.collections.yale.edu/data/",
                   "mapperClass": "sources.lux.merged.mapper.MergedMapper",
                   "acquirerClass": None,
                   "reconcilerClass": ""},
    }
    map_stores = {
        "idmap": {"name": "idmap", "storeClass": "storage.idmap.postgres.IdMap",
                  "host": "localhost", "port": 6379, "db": 0},
        "all_refs": {"name": "all_refs", "storeClass": "storage.idmap.postgres.ReferenceMap",
                     "tableName": "all_refs_tbl"},
        "redirects": {"name": "redirects", "storeClass": "storage.idmap.redis.TransitiveMultiMap",
                      "host": "redis-host"},
    }


@pytest.fixture(autouse=True)
def _wide(monkeypatch):
    """Rich reads COLUMNS when stdout is not a tty; keep rows on one line."""
    monkeypatch.setenv("COLUMNS", "200")


def run(rest=(), source=None):
    args = Namespace(command="list", source=source)
    CommandHandler(StubConfigs()).process(args, list(rest))


def out(capsys):
    return capsys.readouterr().out


# --- sources -----------------------------------------------------------------


def test_default_lists_every_group(capsys):
    run()
    text = out(capsys)
    for title in ("Internal sources", "External sources", "Result sources"):
        assert title in text
    for name in ("ycba", "aat", "wikidata", "merged"):
        assert name in text
    assert "Maps" not in text and "Indexes" not in text


def test_rows_show_namespace_order_and_short_class_names(capsys):
    run(source="external")
    text = out(capsys)
    assert "Internal sources" not in text
    assert "vocab.getty.edu/aat" in text
    assert "10" in text and "20" in text
    for cls in ("Mapper: AatMapper", "Downloader: Downloader", "Loader: AatLoader",
                "Acquirer: Acquirer", "Reconciler: AatReconciler",
                "IndexLoader: LmdbIndexLoader"):
        assert cls in text
    # full dotted paths are not shown
    assert "sources.getty.mapper" not in text


def test_long_namespace_is_trimmed():
    ns = list_cmd._namespace_display("http://www.wikidata.org/entity/this/is/a/very/long/namespace/path/")
    assert ns.endswith("…") and len(ns) == 40
    assert list_cmd._namespace_display("https://ycba-lux.yale.edu/data/") == "ycba-lux.yale.edu/data"
    assert list_cmd._namespace_display("") == ""


def test_named_sources_are_binned_by_group(capsys):
    run(source="merged,ycba")
    text = out(capsys)
    assert "Internal sources" in text and "Result sources" in text
    assert "External sources" not in text
    assert "ycba" in text and "merged" in text and "aat" not in text


def test_unknown_named_source_is_reported(capsys):
    run(source="aat,nope")
    text = out(capsys)
    assert "Unknown source:" in text and "nope" in text
    assert "AatMapper" in text


def test_null_and_empty_class_keys_are_skipped(capsys):
    run(source="results")
    text = out(capsys)
    assert "Mapper: MergedMapper" in text
    assert "Acquirer" not in text and "Reconciler" not in text and "None" not in text


def test_short_class_accepts_class_objects():
    class ThingMapper:
        pass
    assert list_cmd._short_class(ThingMapper) == "ThingMapper"
    assert list_cmd._short_class("sources.a.b.C") == "C"
    assert list_cmd._short_class("C") == "C"


def test_class_keys_include_the_ones_added_for_main():
    keys = [k for k, _ in list_cmd._CLASS_KEYS]
    assert "downloaderClass" in keys and "indexLoaderClass" in keys


# --- maps and indexes --------------------------------------------------------


def test_maps_table_alone(capsys):
    run(["--maps"])
    text = out(capsys)
    assert "Maps" in text
    assert "Internal sources" not in text
    assert "idmap" in text and "storage.idmap.postgres.IdMap" in text
    # the stale redis keys on a postgres map are not shown; its table is
    assert "table idmap" in text and "localhost:6379" not in text
    assert "table all_refs_tbl" in text
    assert "redis-host" in text


def test_indexes_table_alone_lists_every_path(capsys):
    run(["--indexes"])
    text = out(capsys)
    assert "Indexes" in text and "Internal sources" not in text
    assert "/data/indexes/label_to_aat.lmdb" in text
    assert "/data/indexes/aat_equivs.lmdb" in text
    assert "/data/indexes/label_to_wd.lmdb" in text
    assert "ycba" not in text and "merged" not in text


def test_indexes_follow_source_selection(capsys):
    run(["--indexes"], source="wikidata")
    text = out(capsys)
    # with --source the sources table is shown too, filtered the same way
    assert "External sources" in text and "WdMapper" in text
    assert "label_to_wd.lmdb" in text and "label_to_aat.lmdb" not in text


def test_index_rows():
    h = CommandHandler(StubConfigs())
    assert h.index_rows("all") == [
        ("external", "aat", "name", "/data/indexes/label_to_aat.lmdb"),
        ("external", "aat", "id", "/data/indexes/aat_equivs.lmdb"),
        ("external", "wikidata", "name", "/data/indexes/label_to_wd.lmdb"),
    ]
    assert h.index_rows("internal") == []
    assert h.index_rows("aat") == h.index_rows("all")[:2]


def test_everything_together(capsys):
    run(["--maps", "--indexes"], source="all")
    text = out(capsys)
    for title in ("Internal sources", "External sources", "Result sources", "Maps", "Indexes"):
        assert title in text


# --- listing -----------------------------------------------------------------


def test_help_lists_list_with_its_docstring():
    from lux_pipeline.cli.help import list_commands
    assert ("list", "List the configured sources, maps and indexes.") in list_commands()
