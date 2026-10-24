"""`lux clear`: caches, maps and index files from a stubbed configuration,
with stub stores that record their clear() calls."""

import os
import sys
from argparse import Namespace
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from lux_pipeline.cli import clear
from lux_pipeline.cli.clear import CommandHandler
from lux_pipeline.storage.idmap.lmdb import TabLmdb


# --- stubs -------------------------------------------------------------------


class StubStore:
    def __init__(self, label):
        self.label = label
        self.cleared = 0

    def clear(self):
        self.cleared += 1


class NoClear:
    """A store without clear(): skipped, never an error."""


class StubConfigs:
    def __init__(self, tmp=None):
        def idx(fn):
            return str(tmp / fn) if tmp else ""
        self.internal = {
            "ycba": {"name": "ycba",
                     "datacache": StubStore("ycba.datacache"),
                     "recordcache": StubStore("ycba.recordcache"),
                     "recordcache2": StubStore("ycba.recordcache2")},
            "ypm": {"name": "ypm",
                    "datacache": StubStore("ypm.datacache"),
                    "recordcache": StubStore("ypm.recordcache"),
                    "recordcache2": None},
        }
        self.external = {
            "aat": {"name": "aat",
                    "datacache": StubStore("aat.datacache"),
                    "recordcache": StubStore("aat.recordcache"),
                    "reconciledRecordcache": StubStore("aat.reconciledRecordcache"),
                    "recordcache2": NoClear(),
                    "reconcileDbPath": idx("label_to_aat.lmdb"),
                    "inverseEquivDbPath": idx("aat_equivs.lmdb")},
            "lcnaf": {"name": "lcnaf",
                      "recordcache": StubStore("lcnaf.recordcache"),
                      "reconcileDbPath": idx("label_to_lcnaf.lmdb")},
        }
        self.results = {
            "merged": {"name": "merged",
                       "recordcache": StubStore("merged.recordcache"),
                       "recordcache2": StubStore("merged.recordcache2")},
        }
        self.map_stores = {
            "idmap": {"name": "idmap", "store": StubStore("idmap")},
            "all_refs": {"name": "all_refs"},
            "done_refs": {"name": "done_refs", "store": StubStore("done_refs")},
            "networkmap": {"name": "networkmap", "store": StubStore("networkmap")},
        }
        self.instantiated = []

    def instantiate_map(self, key):
        self.instantiated.append(key)
        cfg = self.map_stores[key]
        cfg["store"] = StubStore(key)
        return cfg

    def cleared(self):
        """Labels of every store that was cleared, in config order."""
        out = []
        for group in ("internal", "external", "results"):
            for cfg in getattr(self, group).values():
                for v in cfg.values():
                    if isinstance(v, StubStore) and v.cleared:
                        out.append(v.label)
        for cfg in self.map_stores.values():
            store = cfg.get("store")
            if isinstance(store, StubStore) and store.cleared:
                out.append(store.label)
        return out


def run(cfgs, rest=(), source=None):
    args = Namespace(command="clear", source=source)
    return CommandHandler(cfgs).process(args, list(rest))


@pytest.fixture(autouse=True)
def _not_a_tty(monkeypatch):
    """pytest's stdin is not a tty anyway; make it explicit so the
    confirmation tests are the only ones that opt in."""
    monkeypatch.setattr(clear.sys.stdin, "isatty", lambda: False, raising=False)


# --- caches ------------------------------------------------------------------


def test_default_caches_exclude_datacache(capsys):
    cfgs = StubConfigs()
    assert run(cfgs, source="ycba") is True
    assert cfgs.cleared() == ["ycba.recordcache", "ycba.recordcache2"]
    assert capsys.readouterr().out.split("\n")[:2] == [
        "Clearing ycba recordcache", "Clearing ycba recordcache2"]


def test_datacache_must_be_named():
    cfgs = StubConfigs()
    run(cfgs, ["--cache", "datacache"], source="ycba")
    assert cfgs.cleared() == ["ycba.datacache"]


def test_comma_lists_of_sources_and_caches():
    cfgs = StubConfigs()
    run(cfgs, ["--cache", "datacache,recordcache", "--force"], source="ycba,aat")
    assert cfgs.cleared() == ["ycba.datacache", "ycba.recordcache",
                              "aat.datacache", "aat.recordcache"]


def test_group_clears_only_computed_caches_like_manage_data():
    cfgs = StubConfigs()
    run(cfgs, ["--force"], source="external")
    # None and a store without clear() are skipped
    assert cfgs.cleared() == ["aat.recordcache", "aat.reconciledRecordcache",
                              "lcnaf.recordcache"]


def test_source_all_is_every_group_but_not_maps():
    cfgs = StubConfigs()
    run(cfgs, ["--force"], source="all")
    assert cfgs.cleared() == [
        "ycba.recordcache", "ycba.recordcache2", "ypm.recordcache",
        "aat.recordcache", "aat.reconciledRecordcache", "lcnaf.recordcache",
        "merged.recordcache", "merged.recordcache2"]


def test_unknown_source_clears_nothing():
    cfgs = StubConfigs()
    with pytest.raises(ValueError, match="Unknown source: nope"):
        run(cfgs, ["--force"], source="ycba,nope")
    assert cfgs.cleared() == []


def test_nothing_to_clear_is_an_error():
    cfgs = StubConfigs()
    with pytest.raises(ValueError, match="Nothing to clear"):
        run(cfgs)
    assert cfgs.cleared() == []


# --- confirmation ------------------------------------------------------------


def test_wide_clear_asks_on_a_tty_and_no_clears_nothing(monkeypatch, capsys):
    cfgs = StubConfigs()
    monkeypatch.setattr(clear.sys.stdin, "isatty", lambda: True, raising=False)
    asked = []
    monkeypatch.setattr("builtins.input", lambda prompt: asked.append(prompt) or "n")
    assert run(cfgs, source="all") is False
    assert cfgs.cleared() == []
    assert len(asked) == 1 and "5 sources" in asked[0]
    assert "Nothing cleared" in capsys.readouterr().out


def test_wide_clear_yes_proceeds(monkeypatch):
    cfgs = StubConfigs()
    monkeypatch.setattr(clear.sys.stdin, "isatty", lambda: True, raising=False)
    monkeypatch.setattr("builtins.input", lambda prompt: "y")
    assert run(cfgs, source="ycba,ypm") is True
    assert cfgs.cleared() == ["ycba.recordcache", "ycba.recordcache2", "ypm.recordcache"]


def test_force_skips_the_question_and_single_source_never_asks(monkeypatch):
    cfgs = StubConfigs()
    monkeypatch.setattr(clear.sys.stdin, "isatty", lambda: True, raising=False)
    monkeypatch.setattr("builtins.input", lambda prompt: pytest.fail("asked"))
    run(cfgs, ["--force"], source="internal")
    run(cfgs, source="merged")
    assert "merged.recordcache" in cfgs.cleared() and "ypm.recordcache" in cfgs.cleared()


def test_not_a_tty_proceeds_without_asking(monkeypatch):
    cfgs = StubConfigs()
    monkeypatch.setattr("builtins.input", lambda prompt: pytest.fail("asked"))
    run(cfgs, source="all")
    assert "merged.recordcache2" in cfgs.cleared()


# --- maps --------------------------------------------------------------------


def test_idmap_refuses_without_force():
    cfgs = StubConfigs()
    with pytest.raises(ValueError, match="idmap without --force"):
        run(cfgs, ["--map", "idmap"])
    assert cfgs.cleared() == []


def test_idmap_with_force():
    cfgs = StubConfigs()
    run(cfgs, ["--map", "idmap", "--force"])
    assert cfgs.cleared() == ["idmap"]


def test_map_comma_list_instantiates_missing_stores():
    cfgs = StubConfigs()
    run(cfgs, ["--map", "all_refs,networkmap"])
    assert cfgs.cleared() == ["all_refs", "networkmap"]
    assert cfgs.instantiated == ["all_refs"]


def test_refs_is_an_alias_for_both_queues(capsys):
    cfgs = StubConfigs()
    run(cfgs, ["--refs"])
    assert cfgs.cleared() == ["all_refs", "done_refs"]
    assert capsys.readouterr().out == "Clearing map all_refs\nClearing map done_refs\n"


def test_refs_with_map_does_not_double_clear():
    cfgs = StubConfigs()
    run(cfgs, ["--refs", "--map", "done_refs"])
    assert cfgs.map_stores["done_refs"]["store"].cleared == 1


def test_unknown_map_clears_nothing():
    cfgs = StubConfigs()
    with pytest.raises(ValueError, match="Unknown map: nope"):
        run(cfgs, ["--map", "all_refs,nope"])
    assert cfgs.cleared() == []


def test_clear_all_equivalent_is_source_all_plus_refs():
    cfgs = StubConfigs()
    run(cfgs, ["--refs", "--force"], source="all")
    got = cfgs.cleared()
    assert "all_refs" in got and "done_refs" in got and "merged.recordcache" in got
    assert "idmap" not in got and "networkmap" not in got


# --- indexes -----------------------------------------------------------------


@pytest.fixture
def indexed(tmp_path):
    cfgs = StubConfigs(tmp_path)
    for path in (cfgs.external["aat"]["reconcileDbPath"],
                 cfgs.external["aat"]["inverseEquivDbPath"],
                 cfgs.external["lcnaf"]["reconcileDbPath"]):
        db = TabLmdb.open(path, "c")
        db["k"] = "v"
        db.commit()
        db.close()
        assert os.path.exists(os.path.join(path, "data.mdb"))
    return cfgs


def test_index_removes_the_lmdb_files_for_a_source(indexed, capsys):
    run(indexed, ["--index", "lcnaf"])
    assert not os.path.exists(indexed.external["lcnaf"]["reconcileDbPath"])
    assert os.path.exists(indexed.external["aat"]["reconcileDbPath"])
    assert indexed.cleared() == []
    assert "Removing index" in capsys.readouterr().out


def test_index_all_and_missing_files_are_fine(indexed):
    run(indexed, ["--index", "all"])
    for cfg in (indexed.external["aat"], indexed.external["lcnaf"]):
        for key in ("reconcileDbPath", "inverseEquivDbPath"):
            if cfg.get(key):
                assert not os.path.exists(cfg[key])
    # a second run has nothing left to delete and does not fail
    run(indexed, ["--index", "all"])


def test_index_unknown_source_is_an_error(indexed):
    with pytest.raises(ValueError, match="Unknown source: nope"):
        run(indexed, ["--index", "nope"])
    assert os.path.exists(indexed.external["aat"]["reconcileDbPath"])


# --- listing -----------------------------------------------------------------


def test_help_lists_clear_with_its_docstring():
    from lux_pipeline.cli.help import list_commands
    assert ("clear", "Clear caches, maps and reconcile indexes.") in list_commands()
