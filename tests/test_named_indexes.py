"""The named `indexes` config block (item 34): canonical names and aliases,
Config.resolve_indexes() normalising both forms into one, lazy IndexSpec
objects, the readers (reconciler, index loader, identity resolver, counts,
clear, list) seeing the block, and the pipeline-configs repository's
records resolving."""

import json
import shutil
import sys
from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

import lux_pipeline.config as config_mod
from lux_pipeline.config import (
    Config, IndexSpec, INDEX_LABELS, canonical_index_name, index_path, source_indexes,
)
from lux_pipeline.process.base.index_loader import LmdbIndexLoader
from lux_pipeline.process.base.reconciler import LmdbReconciler
from lux_pipeline.storage.idmap.lmdb import TabLmdb

PIPELINE_CONFIGS = Path(__file__).parent.parent.parent / "pipeline-configs" / "configs"


@pytest.fixture(autouse=True)
def _fresh_legacy_warnings(monkeypatch):
    monkeypatch.setattr(config_mod, "_warned_legacy_paths", set())


def make_config(tmp_path, records=None):
    """A Config from a temp dir with a system record whose indexes_dir is
    <tmp>/indexes; nothing instantiated."""
    cc = tmp_path / "config_cache"
    cc.mkdir(exist_ok=True)
    system = {"type": "system", "base_dir": str(tmp_path), "data_dir": "data", "exports_dir": "out",
              "indexes_dir": "indexes", "ok_record_types": {}, "parent_record_types": {},
              "path_types": {"reconcileDbPath": "indexes_dir", "inverseEquivDbPath": "indexes_dir"},
              "idmap_name": "idmap"}
    (cc / "system.json").write_text(json.dumps(system))
    for k, rec in (records or {}).items():
        (cc / f"{k}.json").write_text(json.dumps(rec))
    return Config(basepath=str(tmp_path))


# --- names -------------------------------------------------------------------


def test_canonical_names_and_aliases(caplog):
    assert canonical_index_name("reconcile_label") == "reconcile_label"
    with caplog.at_level("WARNING", logger="lux_pipeline"):
        assert canonical_index_name("reconcile_labels") == "reconcile_label"
        assert canonical_index_name("reconcile_ids") == "reconcile_id"
        assert canonical_index_name("equivalents") == "reconcile_id"
        assert canonical_index_name("diffs") == "differents"
    assert "Config refers to 'index reconcile_labels'; update it to 'index reconcile_label'" in caplog.text
    assert canonical_index_name("headings") == "headings"     # a custom index is kept
    assert INDEX_LABELS == {"reconcile_label": "name", "reconcile_id": "id", "differents": "differents"}


def test_index_path_prefers_the_block_then_the_legacy_key():
    cfg = {"indexes": {"reconcile_label": {"path": "/i/block.lmdb"}}, "reconcileDbPath": "/i/legacy.lmdb",
           "inverseEquivDbPath": "/i/ids.lmdb"}
    assert index_path(cfg, "reconcile_label") == "/i/block.lmdb"
    assert index_path(cfg, "reconcile_labels") == "/i/block.lmdb"
    assert index_path(cfg, "reconcile_id") == "/i/ids.lmdb"
    assert index_path(cfg, "differents", "") == ""
    # an alias used as the block's key is found by its canonical name
    assert index_path({"indexes": {"equivalents": {"path": "/i/e.lmdb"}}}, "reconcile_id") == "/i/e.lmdb"
    assert index_path({}, "reconcile_label") is None


def test_source_indexes_from_either_form_in_canonical_order():
    legacy = {"inverseEquivDbPath": "/i/ids.lmdb", "reconcileDbPath": "/i/names.lmdb"}
    assert source_indexes(legacy) == {"reconcile_label": "/i/names.lmdb", "reconcile_id": "/i/ids.lmdb"}
    block = {"indexes": {"headings": {"path": "/i/h.lmdb"}, "reconcile_ids": {"path": "/i/ids.lmdb"},
                         "differents": {"path": "/i/d.lmdb"}, "nopath": {}}}
    assert list(source_indexes(block).items()) == [
        ("reconcile_id", "/i/ids.lmdb"), ("differents", "/i/d.lmdb"), ("headings", "/i/h.lmdb")]
    assert source_indexes({}) == {}


# --- Config.resolve_indexes --------------------------------------------------


def test_block_paths_resolve_under_indexes_dir_and_fill_the_legacy_keys(tmp_path):
    cfgs = make_config(tmp_path)
    cfg = {"name": "aat", "indexes": {"reconcile_label": {"path": "label_to_aat.lmdb"},
                                      "reconcile_id": {"path": "/abs/id_to_aat.lmdb", "mapExponent": 24}}}
    block = cfgs.resolve_indexes(cfg)
    assert block is cfg["indexes"]
    assert block["reconcile_label"]["path"] == str(tmp_path / "indexes" / "label_to_aat.lmdb")
    assert block["reconcile_id"]["path"] == "/abs/id_to_aat.lmdb"
    assert cfg["reconcileDbPath"] == block["reconcile_label"]["path"]
    assert cfg["inverseEquivDbPath"] == "/abs/id_to_aat.lmdb"
    assert "differentDbPath" not in cfg
    spec = block["reconcile_label"]["index"]
    assert isinstance(spec, IndexSpec) and spec.cls is TabLmdb and spec.name == "reconcile_label"
    assert block["reconcile_id"]["index"].map_size == 2 ** 24
    assert block["reconcile_label"]["indexClass"] == "storage.idmap.lmdb.TabLmdb"
    assert not spec.exists()


def test_legacy_keys_alone_produce_the_block(tmp_path):
    cfgs = make_config(tmp_path)
    cfg = {"name": "lcsh", "reconcileDbPath": "/i/label_to_lcsh.lmdb", "mapSizeExponent": 25}
    block = cfgs.resolve_indexes(cfg)
    assert list(block) == ["reconcile_label"]
    assert block["reconcile_label"]["path"] == "/i/label_to_lcsh.lmdb"
    assert block["reconcile_label"]["index"].map_size == 2 ** 25


def test_block_wins_when_both_forms_are_given_and_aliases_are_canonicalised(tmp_path, caplog):
    cfgs = make_config(tmp_path)
    cfg = {"name": "lcsh", "reconcileDbPath": "/legacy/label.lmdb",
           "indexes": {"reconcile_labels": {"path": "/block/label.lmdb",
                                            "indexClass": "storage.index.lmdb.LmdbIndex"}}}
    with caplog.at_level("WARNING", logger="lux_pipeline"):
        block = cfgs.resolve_indexes(cfg)
    assert list(block) == ["reconcile_label"]
    assert cfg["reconcileDbPath"] == "/block/label.lmdb"
    assert block["reconcile_label"]["indexClass"] == "storage.idmap.lmdb.TabLmdb"
    assert block["reconcile_label"]["index"].cls is TabLmdb
    assert "update it to 'index reconcile_label'" in caplog.text
    assert "Config refers to 'storage.index.lmdb.LmdbIndex'; update it to 'storage.idmap.lmdb.TabLmdb'" in caplog.text


def test_differents_on_merged_maps_to_differentDbPath(tmp_path):
    cfgs = make_config(tmp_path)
    cfg = {"name": "merged", "indexes": {"differents": {"path": "global_diffs.lmdb"},
                                         "reconcile_id": {"path": "global_equivs.lmdb"}}}
    cfgs.resolve_indexes(cfg)
    assert cfg["differentDbPath"] == str(tmp_path / "indexes" / "global_diffs.lmdb")
    assert cfg["inverseEquivDbPath"] == str(tmp_path / "indexes" / "global_equivs.lmdb")


def test_duplicate_names_keep_the_first_and_resolve_is_idempotent(tmp_path, caplog):
    cfgs = make_config(tmp_path)
    cfg = {"name": "x", "indexes": {"reconcile_id": {"path": "/a.lmdb"}, "equivalents": {"path": "/b.lmdb"}}}
    with caplog.at_level("WARNING", logger="lux_pipeline"):
        cfgs.resolve_indexes(cfg)
    assert cfg["indexes"]["reconcile_id"]["path"] == "/a.lmdb"
    assert "duplicates 'reconcile_id'" in caplog.text
    spec = cfg["indexes"]["reconcile_id"]["index"]
    cfgs.resolve_indexes(cfg)
    assert cfg["indexes"]["reconcile_id"]["index"] is spec


def test_instantiate_resolves_the_block(tmp_path, monkeypatch):
    class Recording:
        def __init__(self, config):
            self.config = config

    stubs = {k: "x.Recording" for k in ("datacacheClass", "recordcacheClass", "recordcacheReconciledClass",
                                         "mapperClass", "loaderClass", "recordcache2Class", "acquirerClass",
                                         "downloaderClass")}
    cfgs = make_config(tmp_path, {"aat": {"type": "external", "name": "aat", **stubs,
                                         "indexes": {"reconcile_label": {"path": "label_to_aat.lmdb"}}}})
    monkeypatch.setattr(config_mod, "importObject", lambda path: Recording if path == "x.Recording" else TabLmdb)
    cfg = cfgs.instantiate("aat")
    assert cfg["reconcileDbPath"] == str(tmp_path / "indexes" / "label_to_aat.lmdb")
    assert cfg["indexes"]["reconcile_label"]["index"].cls is TabLmdb


# --- IndexSpec ---------------------------------------------------------------


def test_index_spec_opens_lazily(tmp_path):
    path = str(tmp_path / "names.lmdb")
    spec = IndexSpec("reconcile_label", path, TabLmdb, 2 ** 20)
    assert not spec.exists()
    db = spec.open("c")
    db["gold"] = ["1", "Material"]
    db.close()
    assert spec.exists()
    assert spec.open("r")["gold"] == ["1", "Material"]
    assert "reconcile_label" in repr(spec) and "TabLmdb" in repr(spec)


# --- the readers -------------------------------------------------------------


def reader_config(tmp_path, **extra):
    cfg = {"name": "aat", "namespace": "http://vocab.getty.edu/aat/",
           "all_configs": SimpleNamespace(debug_reconciliation=False, data_dir=str(tmp_path)),
           "datacache": None}
    cfg.update(extra)
    return cfg


def test_reconciler_and_index_loader_read_the_block(tmp_path):
    cfg = reader_config(tmp_path, indexes={"reconcile_label": {"path": "/i/names.lmdb"},
                                           "reconcile_id": {"path": "/i/ids.lmdb"}})
    r = LmdbReconciler(cfg)
    assert r._index_paths == {"name": "/i/names.lmdb", "id": "/i/ids.lmdb"}
    ldr = LmdbIndexLoader(cfg)
    assert (ldr.out_path, ldr.inverse_path) == ("/i/names.lmdb", "/i/ids.lmdb")
    # legacy keys still work on their own
    cfg = reader_config(tmp_path, reconcileDbPath="/i/legacy.lmdb")
    assert LmdbReconciler(cfg)._index_paths == {"name": "/i/legacy.lmdb", "id": ""}
    assert LmdbIndexLoader(cfg).inverse_path is None


def test_global_loader_and_reconciler_read_differents_from_the_block(tmp_path):
    from lux_pipeline.sources.lux.final.index_loader import GlobalIndexLoader
    from lux_pipeline.sources.lux.final.reconciler import GlobalReconciler
    cfg = reader_config(tmp_path, name="merged",
                        indexes={"differents": {"path": str(tmp_path / "diffs.lmdb")},
                                 "reconcile_id": {"path": str(tmp_path / "equivs.lmdb")}})
    diff, eq = GlobalIndexLoader(cfg).get_storage()
    assert diff is not None and eq is not None
    diff.close(); eq.close()
    r = GlobalReconciler(cfg)
    assert r.diff_index is not None
    assert r._index_paths["id"] == str(tmp_path / "equivs.lmdb")


def test_identity_resolver_reads_differents_from_the_block(tmp_path, caplog):
    from lux_pipeline.process import identity_resolver as ir
    path = str(tmp_path / "global_diffs.lmdb")
    db = TabLmdb.open(path, "c")
    db["http://a.org/1"] = ["http://b.org/2"]
    db.commit(); db.close()
    obj = ir.IdentityResolver.__new__(ir.IdentityResolver)
    obj.prefix_in = {}          # no curie shortening
    # the block alone, no differentDbPath: the index is found and applied
    obj.configs = SimpleNamespace(results={"merged": {"name": "merged",
                                                      "indexes": {"differents": {"path": path}}}})
    assert obj.load_diff_pairs({"http://a.org/1", "http://b.org/2"}) != set()
    # nothing configured: the documented warning, no diffs
    obj.configs = SimpleNamespace(results={"merged": {"name": "merged"}})
    with caplog.at_level("WARNING", logger="lux_pipeline"):
        assert obj.load_diff_pairs({"http://a.org/1"}) == set()
    assert "No differentDbPath configured" in caplog.text


# --- the commands ------------------------------------------------------------


class CmdConfigs:
    """One source with a block, one with legacy keys, one with neither."""

    def __init__(self, tmp_path):
        self.internal = {"ycba": {"name": "ycba"}}
        self.external = {
            "aat": {"name": "aat", "indexes": {"reconcile_label": {"path": str(tmp_path / "label_to_aat.lmdb")},
                                               "equivalents": {"path": str(tmp_path / "aat_equivs.lmdb")}}},
            "lcsh": {"name": "lcsh", "reconcileDbPath": str(tmp_path / "label_to_lcsh.lmdb")},
        }
        self.results = {}
        self.map_stores = {}


def test_counts_clear_and_list_see_both_forms(tmp_path, capsys, monkeypatch):
    from lux_pipeline.cli import clear as clear_mod
    from lux_pipeline.cli.clear import CommandHandler as Clear
    from lux_pipeline.cli.counts import CommandHandler as Counts
    from lux_pipeline.cli.list import CommandHandler as List
    cfgs = CmdConfigs(tmp_path)
    for fn, n in (("label_to_aat.lmdb", 2), ("aat_equivs.lmdb", 1)):
        db = TabLmdb.open(str(tmp_path / fn), "c")
        for i in range(n):
            db[f"k{i}"] = f"v{i}"
        db.commit(); db.close()

    rows = Counts(cfgs).resolve_indexes(Namespace(index="all"))
    assert rows == [("external", "aat", "name", str(tmp_path / "label_to_aat.lmdb")),
                    ("external", "aat", "id", str(tmp_path / "aat_equivs.lmdb")),
                    ("external", "lcsh", "name", str(tmp_path / "label_to_lcsh.lmdb"))]
    Counts(cfgs).process(Namespace(command="counts", source=None), ["--index", "aat"])
    out = capsys.readouterr().out.splitlines()
    assert "aat name index: =2" in out and "aat id index: =1" in out

    assert List(cfgs).index_rows("all") == [
        ("external", "aat", "name", str(tmp_path / "label_to_aat.lmdb")),
        ("external", "aat", "id", str(tmp_path / "aat_equivs.lmdb")),
        ("external", "lcsh", "name", str(tmp_path / "label_to_lcsh.lmdb"))]

    monkeypatch.setattr(clear_mod.sys.stdin, "isatty", lambda: False, raising=False)
    assert Clear(cfgs).resolve_indexes(Namespace(index="aat,lcsh")) == [
        ("aat", str(tmp_path / "label_to_aat.lmdb")), ("aat", str(tmp_path / "aat_equivs.lmdb")),
        ("lcsh", str(tmp_path / "label_to_lcsh.lmdb"))]
    Clear(cfgs).process(Namespace(command="clear", source=None), ["--index", "aat"])
    assert not (tmp_path / "label_to_aat.lmdb").exists() and not (tmp_path / "aat_equivs.lmdb").exists()


# --- the pipeline-configs repository ----------------------------------------


@pytest.mark.skipif(not PIPELINE_CONFIGS.is_dir(), reason="pipeline-configs checkout not beside this repo")
def test_pipeline_configs_index_blocks_resolve(tmp_path, caplog):
    shutil.copytree(PIPELINE_CONFIGS, tmp_path / "configs")
    cc = tmp_path / "configs" / "system" / "config_cache"
    for fn in ("sources", "maps", "mls"):
        rec = json.loads((cc / f"{fn}.json").read_text())
        rec["base_dir"] = "../subs"
        (cc / f"{fn}.json").write_text(json.dumps(rec))
    cfgs = Config(basepath=str(tmp_path / "configs" / "system"))
    with caplog.at_level("WARNING", logger="lux_pipeline"):
        for name in ("aat", "ulan", "lcnaf", "viaf", "wikidata", "lcsh"):
            cfgs.resolve_indexes(cfgs.external[name])
        cfgs.resolve_indexes(cfgs.results["merged"])
    for name in ("aat", "ulan", "lcnaf", "lcsh"):
        assert cfgs.external[name]["reconcileDbPath"].startswith(cfgs.indexes_dir)
    for name in ("lcnaf", "viaf", "wikidata", "lcsh"):
        assert cfgs.external[name]["inverseEquivDbPath"].startswith(cfgs.indexes_dir)
    merged = cfgs.results["merged"]
    assert merged["differentDbPath"].endswith("global_diffs.lmdb")
    assert merged["inverseEquivDbPath"].endswith("global_equivs.lmdb")
    assert "update it to 'index reconcile_label'" in caplog.text
    assert "update it to 'storage.idmap.lmdb.TabLmdb'" in caplog.text
