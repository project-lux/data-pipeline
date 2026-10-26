"""The reconcilers open their LMDB / sqlite indexes on first use, not in
__init__, so a source can be instantiated before its index is built
(`lux load-index` on a fresh machine); a missing configured index still
fails loudly, at first access."""

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.process.base import reconciler as rmod
from lux_pipeline.process.base.reconciler import LmdbReconciler, SqliteReconciler
from lux_pipeline.sources.lux.final.reconciler import GlobalReconciler
from lux_pipeline.storage.idmap.lmdb import TabLmdb


def config(tmp_path, name_path="", id_path="", **extra):
    cfg = {"name": "aat", "namespace": "http://vocab.getty.edu/aat/",
           "all_configs": SimpleNamespace(debug_reconciliation=False),
           "reconcileDbPath": name_path, "inverseEquivDbPath": id_path}
    cfg.update(extra)
    return cfg


@pytest.fixture(autouse=True)
def _no_retry_sleep(monkeypatch):
    monkeypatch.setattr(rmod.time, "sleep", lambda s: None)


def test_missing_index_does_not_block_construction_but_fails_loudly_on_use(tmp_path):
    missing = str(tmp_path / "label_to_aat.lmdb")
    r = LmdbReconciler(config(tmp_path, name_path=missing))
    assert r.id_index is None                      # not configured
    with pytest.raises(RuntimeError, match="Could not open name index .*label_to_aat.lmdb for aat") as info:
        r.name_index
    assert "Reconciliation would silently be disabled" in str(info.value)
    # should_reconcile by name reaches the same loud failure, never a silent False
    with pytest.raises(RuntimeError):
        r.should_reconcile({"data": {"type": "Type", "identified_by": []}}, "name")


def test_index_opens_once_on_first_use_and_is_read(tmp_path):
    path = str(tmp_path / "label_to_aat.lmdb")
    idx = TabLmdb.open(path, "c", map_size=2**20, readahead=False, writemap=True)
    idx["gold"] = ["300011021", "Material"]
    idx.close()
    r = LmdbReconciler(config(tmp_path, name_path=path))
    first = r.name_index
    assert first is r.name_index
    assert r.name_index.get("gold") == ["300011021", "Material"]
    assert r.should_reconcile({"data": {"type": "Material", "identified_by": []}}, "name") is True


def test_indexes_stay_assignable(tmp_path):
    r = LmdbReconciler(config(tmp_path, name_path=str(tmp_path / "nope.lmdb")))
    r.name_index = {"x": ["1", "Type"]}
    r.id_index = {}
    assert r.name_index == {"x": ["1", "Type"]} and r.id_index == {}
    # built without __init__ (as the determinism tests do) and assigned
    r = object.__new__(LmdbReconciler)
    r.name_index = {"y": ["2", "Type"]}
    assert r.name_index == {"y": ["2", "Type"]} and r.id_index is None


def test_sqlite_reconciler_is_lazy_too(tmp_path):
    r = SqliteReconciler(config(tmp_path, id_path=str(tmp_path / "missing.sqlite")))
    assert r.name_index is None
    with pytest.raises(RuntimeError, match="Could not open id index"):
        r.id_index


def test_global_reconciler_builds_before_its_indexes_exist(tmp_path, capsys):
    cfg = config(tmp_path, id_path=str(tmp_path / "global_equivs.lmdb"),
                 differentDbPath=str(tmp_path / "global_diffs.lmdb"))
    cfg["name"] = "merged"
    r = GlobalReconciler(cfg)
    assert r.diff_index is None
    with pytest.raises(RuntimeError, match="for merged"):
        r.id_index
    cfg = config(tmp_path)
    GlobalReconciler(cfg)
    assert "Could not find global equivalents index" in capsys.readouterr().out
