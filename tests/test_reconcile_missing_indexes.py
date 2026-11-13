"""Reconciliation indexes not built yet. On a fresh installation with only
aat loaded, `lux reconcile --baseline` failed in every worker on its first
record: lcnaf's id index did not exist. The failure is deliberately loud --
a reconciler without its index emits nothing -- so the driver now names
every missing index before a worker starts, and `--skip-missing-indexes`
runs a partial build without them."""

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.process.base import reconciler as rmod
from lux_pipeline.process.base.reconciler import LmdbReconciler, missing_indexes, skip_missing_indexes
from lux_pipeline.storage.idmap.lmdb import TabLmdb


@pytest.fixture(autouse=True)
def _no_retry_sleep(monkeypatch):
    monkeypatch.setattr(rmod.time, "sleep", lambda s: None)


def built(path):
    idx = TabLmdb.open(str(path), "c", map_size=2**20, readahead=False, writemap=True)
    idx["x"] = ["1", "Type"]
    idx.close()
    return str(path)


def test_missing_indexes_are_read_from_the_configuration(tmp_path):
    built(tmp_path / "label_to_aat.lmdb")
    cfgs = SimpleNamespace(indexes_dir=str(tmp_path), external={
        "aat": {"name": "aat", "reconcilerClass": "lux_pipeline.sources.getty.reconciler.AatReconciler",
                "indexes": {"reconcile_label": {"path": "label_to_aat.lmdb"}}},
        "lcnaf": {"name": "lcnaf", "reconcilerClass": "sources.lc.reconciler.LcnafReconciler",
                  "indexes": {"reconcile_label": {"path": "label_to_lcnaf.lmdb"},
                              "reconcile_id": {"path": str(tmp_path / "id_to_lcnaf.lmdb")}}},
        "ror": {"name": "ror", "indexes": {"reconcile_label": {"path": "nowhere.lmdb"}}},   # no reconciler
    }, results={
        "merged": {"name": "merged", "reconcilerClass": "sources.lux.final.reconciler.GlobalReconciler",
                   "indexes": {"reconcile_id": {"path": "global_equivs.lmdb"}}},
    })
    assert missing_indexes(cfgs) == [
        ("lcnaf", "name", str(tmp_path / "label_to_lcnaf.lmdb")),
        ("lcnaf", "id", str(tmp_path / "id_to_lcnaf.lmdb")),
        ("merged", "id", str(tmp_path / "global_equivs.lmdb")),
    ]


def test_skipping_turns_only_the_missing_index_off(tmp_path):
    id_path = built(tmp_path / "id_to_aat.lmdb")
    name_path = str(tmp_path / "label_to_aat.lmdb")
    r = LmdbReconciler({"name": "aat", "namespace": "http://vocab.getty.edu/aat/",
                        "all_configs": SimpleNamespace(debug_reconciliation=False),
                        "reconcileDbPath": name_path, "inverseEquivDbPath": id_path})
    assert skip_missing_indexes([r, object()]) == [("aat", "name", name_path)]
    assert r.name_index is None
    assert r.should_reconcile({"data": {"type": "Type", "identified_by": []}}, "name") is False
    assert r.id_index.get("x") == ["1", "Type"]          # the built one still opens
    # without the option the missing one still fails loudly
    r2 = LmdbReconciler({"name": "aat", "namespace": "http://vocab.getty.edu/aat/",
                         "all_configs": SimpleNamespace(debug_reconciliation=False),
                         "reconcileDbPath": name_path})
    with pytest.raises(RuntimeError, match="Could not open name index"):
        r2.name_index
