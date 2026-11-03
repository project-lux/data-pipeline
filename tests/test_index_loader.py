"""`IndexLoader` as a Sliced component (item 18): collision rule, byte-length
drop, progress, the single-writer rule, overwrite, the legacy load() alias,
export, a real LMDB round trip, and the subclasses still importing."""

import csv
import importlib
import logging
import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.process.base import index_loader as il
from lux_pipeline.process.base.index_loader import IndexLoader, LmdbIndexLoader, MAX_KEY_BYTES
from lux_pipeline.progress import null_progress
from lux_pipeline.storage.idmap.lmdb import TabLmdb


# --- stubs -------------------------------------------------------------------


class InCache:
    def __init__(self, records):
        self.records = records

    def len_estimate(self):
        return len(self.records)

    def iter_records(self):
        for r in self.records:
            yield {"identifier": r["id"], "data": r}


class Reconciler:
    def __init__(self, uris_forbidden=False, names_forbidden=False):
        self.uris_forbidden = uris_forbidden
        self.names_forbidden = names_forbidden

    def extract_names(self, rec):
        assert not self.names_forbidden, "extract_names called for a source without a name index"
        return {n: 1 for n in rec.get("names", [])}

    def extract_uris(self, rec):
        assert not self.uris_forbidden, "extract_uris called for a source without an equivalents index"
        return list(rec.get("uris", []))


class Acquirer:
    """Returns the cached record; None for identifiers in `missing`."""

    def __init__(self, cache, missing=()):
        self.cache = cache
        self.missing = set(missing)

    def acquire(self, recid, store=False):
        assert store is False
        if recid in self.missing:
            return None
        for r in self.cache.records:
            if r["id"] == recid:
                return {"data": r}
        return None


class Mapper:
    def guess_type(self, data):
        return "Guessed"


class Recorder:
    def __init__(self):
        self.calls = []

    def __call__(self, **kw):
        self.calls.append(kw)


class DictIndexLoader(IndexLoader):
    """Storage as two plain dicts (or None when the path is unset)."""

    def __init__(self, config):
        super().__init__(config)
        self.names_index = {} if self.out_path else None
        self.uris_index = {} if self.inverse_path else None
        self.opened = []

    def get_storage(self):
        return self.names_index, self.uris_index

    def open_index_readonly(self, path):
        self.opened.append(path)
        return self.names_index if path == self.out_path else self.uris_index


def make_loader(records, names=True, uris=True, missing=(), reconciler=None, cls=DictIndexLoader,
                tmp_path=None, **extra):
    cache = InCache(records)
    cfg = {"name": "src", "all_configs": SimpleNamespace(data_dir=str(tmp_path) if tmp_path else "/data"),
           "datacache": cache, "namespace": "http://x.org/",
           "reconciler": reconciler or Reconciler(uris_forbidden=not uris, names_forbidden=not names),
           "acquirer": Acquirer(cache, missing), "mapper": Mapper()}
    if names:
        cfg["reconcileDbPath"] = str(tmp_path / "names.lmdb") if tmp_path else "/idx/names.lmdb"
    if uris:
        cfg["inverseEquivDbPath"] = str(tmp_path / "uris.lmdb") if tmp_path else "/idx/uris.lmdb"
    cfg.update(extra)
    return cls(cfg)


RECORDS = [
    {"id": "b", "type": "Person", "names": ["Smith, John", "J. Smith"], "uris": ["http://viaf.org/1"]},
    {"id": "a", "type": "Person", "names": ["smith, john"], "uris": ["http://viaf.org/1", "http://wd.org/Q1"]},
    {"id": "c", "type": "Group", "names": ["Acme"], "uris": []},
]


# --- the build ---------------------------------------------------------------


def test_names_and_uris_keyed_with_smallest_recid_on_collision():
    ldr = make_loader(RECORDS)
    assert ldr.prepare(0, 1).process() == {"records": 3, "names": 3, "uris": 2}
    assert ldr.names_index == {"smith, john": ["a", "Person"], "j. smith": ["b", "Person"],
                               "acme": ["c", "Group"]}
    assert ldr.uris_index == {"http://viaf.org/1": ["a", "Person"], "http://wd.org/Q1": ["a", "Person"]}


def test_order_of_records_does_not_change_the_winner():
    ldr = make_loader(list(reversed(RECORDS)))
    ldr.process()
    assert ldr.names_index["smith, john"] == ["a", "Person"]
    assert ldr.uris_index["http://viaf.org/1"] == ["a", "Person"]


def test_long_names_are_dropped_by_byte_length(caplog):
    cjk = "名" * 200                  # 200 chars, 600 bytes: over the limit
    ascii_ok = "x" * (MAX_KEY_BYTES - 1)
    recs = [{"id": "a", "type": "Type", "names": [cjk, ascii_ok, ""], "uris": [""]}]
    ldr = make_loader(recs)
    with caplog.at_level("WARNING", logger="lux_pipeline"):
        assert ldr.process() == {"records": 1, "names": 1, "uris": 0}
    assert list(ldr.names_index) == [ascii_ok]
    assert "Dropping name as too long (200>500)" in caplog.text


def test_unacquirable_and_untyped_records_are_skipped_and_types_are_guessed():
    recs = [{"id": "a", "names": ["No Type"], "uris": []},            # type guessed
            {"id": "b", "type": "Person", "names": ["Gone"], "uris": []},   # acquirer returns None
            {"id": "", "type": "Person", "names": ["No Id"], "uris": []}]
    ldr = make_loader(recs, missing=["b"])
    ldr.process()
    assert ldr.names_index == {"no type": ["a", "Guessed"]}


def test_only_the_configured_indexes_are_extracted():
    ldr = make_loader(RECORDS, uris=False)
    assert ldr.process() == {"records": 3, "names": 3, "uris": 0}
    assert ldr.uris_index is None
    ldr = make_loader(RECORDS, names=False)
    assert ldr.process() == {"records": 3, "names": 0, "uris": 2}


def test_no_index_configured_warns_and_returns_none(caplog):
    ldr = make_loader(RECORDS, names=False, uris=False)
    with caplog.at_level("WARNING", logger="lux_pipeline"):
        assert ldr.process() is None
    assert "src has no indexes configured" in caplog.text


def test_overwrite_clears_first_and_no_overwrite_merges():
    ldr = make_loader(RECORDS)
    ldr.names_index["stale"] = ["z", "Type"]
    ldr.process()
    assert "stale" not in ldr.names_index
    ldr.names_index["stale"] = ["z", "Type"]
    ldr.process(overwrite=False)
    assert ldr.names_index["stale"] == ["z", "Type"] and "acme" in ldr.names_index


def test_components_fall_back_to_the_config_at_process_time():
    ldr = make_loader(RECORDS)
    ldr.reconciler = ldr.mapper = ldr.acquirer = None
    ldr.process()
    assert ldr.reconciler is ldr.config["reconciler"] and ldr.acquirer is ldr.config["acquirer"]


# --- managed component -------------------------------------------------------


def test_prepare_refuses_more_than_one_slice():
    ldr = make_loader(RECORDS)
    with pytest.raises(ValueError, match="single writer"):
        ldr.prepare(0, 2)
    assert ldr.prepare(0, 1) is ldr
    assert ldr.prepare(0, -1).max_slice == -1


def test_progress_sees_the_total_the_description_and_every_record():
    rec = Recorder()
    ldr = make_loader(RECORDS).prepare(0, 1, progress=rec)
    ldr.process()
    assert rec.calls[0] == {"total": 3, "completed": 0, "description": "src index"}
    assert sum(c.get("advance", 0) for c in rec.calls) == 3


def test_without_prepare_progress_reports_nowhere():
    ldr = make_loader(RECORDS)
    assert ldr.progress is null_progress
    ldr.process()


def test_load_is_prepare_then_process():
    ldr = make_loader(RECORDS)
    assert ldr.load() == {"records": 3, "names": 3, "uris": 2}


def test_periodic_log_line(caplog, monkeypatch):
    monkeypatch.setattr(il, "LOG_EVERY", 2)
    ldr = make_loader(RECORDS)
    with caplog.at_level("INFO", logger="lux_pipeline"):
        ldr.process()
    assert "2 of 3 in" in caplog.text


# --- export ------------------------------------------------------------------


def test_export_writes_one_csv_per_index(tmp_path):
    ldr = make_loader(RECORDS, tmp_path=tmp_path)
    ldr.process()
    ldr.export()
    assert ldr.opened == [str(tmp_path / "names.lmdb"), str(tmp_path / "uris.lmdb")]
    rows = list(csv.reader(open(tmp_path / "names.csv")))
    assert ["smith, john", "a\tPerson"] in rows
    assert (tmp_path / "uris.csv").exists()


# --- real LMDB ---------------------------------------------------------------


def test_lmdb_loader_round_trip_and_export(tmp_path):
    ldr = make_loader(RECORDS, cls=LmdbIndexLoader, tmp_path=tmp_path, mapSizeExponent=24)
    assert ldr.load() == {"records": 3, "names": 3, "uris": 2}
    # the loader closed its handles, so the files open again in this process
    names = TabLmdb.open(str(tmp_path / "names.lmdb"), "r", readahead=False, writemap=True)
    assert names["smith, john"] == ["a", "Person"]
    assert len(list(names.items())) == 3
    uris = TabLmdb.open(str(tmp_path / "uris.lmdb"), "r", readahead=False, writemap=True)
    assert uris["http://wd.org/Q1"] == ["a", "Person"]
    names.close()
    uris.close()
    ldr.export()
    rows = list(csv.reader(open(tmp_path / "uris.csv")))
    assert ["http://viaf.org/1", "a\tPerson"] in rows


def test_indexes_are_closed_after_the_build():
    class Closable(dict):
        def __init__(self):
            super().__init__()
            self.committed = self.closed = 0

        def commit(self):
            self.committed += 1

        def close(self):
            self.closed += 1

    ldr = make_loader(RECORDS)
    ldr.names_index, ldr.uris_index = Closable(), Closable()
    ldr.process()
    assert (ldr.names_index.committed, ldr.names_index.closed) == (1, 1)
    assert (ldr.uris_index.committed, ldr.uris_index.closed) == (1, 1)


def test_base_class_has_no_storage():
    with pytest.raises(NotImplementedError):
        IndexLoader({"name": "x"}).get_storage()


@pytest.mark.parametrize("module, cls", [
    ("lux_pipeline.sources.lc.index_loader", "LCIndexLoader"),
    ("lux_pipeline.sources.oclc.index_loader", "ViafIndexLoader"),
    ("lux_pipeline.sources.wikidata.index_loader", "WdFileIndexLoader"),
    ("lux_pipeline.sources.yale.library.index_loader", "YulIndexLoader"),
    ("lux_pipeline.sources.lux.final.index_loader", "GlobalIndexLoader"),
])
def test_subclasses_import_and_build_from_a_config(module, cls):
    klass = getattr(importlib.import_module(module), cls)
    ldr = klass({"name": "x", "all_configs": SimpleNamespace(data_dir="/d"), "datacache": None,
                 "namespace": "http://x.org/"})
    assert isinstance(ldr, LmdbIndexLoader) and ldr.name == "x"
