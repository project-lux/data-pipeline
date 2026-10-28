"""Config reads both configuration layouts: today's base.json + caches.json,
and the refactor's system.json + {"type": "default"} records.

Built from a temporary directory through FsCache, exactly as
Config(basepath=...) does. Nothing is instantiated, so no Postgres."""

import json
import logging
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.config import Config

BASE = {
    "base_dir": None,              # filled in per test
    "data_dir": "data/files",
    "exports_dir": "data/output",
    "indexes_dir": "data/processing",
    "ok_record_types": {"Person": "person", "Type": "concept"},
    "parent_record_types": {},
    "path_types": {"reconcileDbPath": "indexes_dir"},
    "idmap_name": "idmap",
}

CACHE_SETTINGS = {"host": "", "port": 5432, "user": "u", "dbname": "d", "cursor_size": 1000}


def write_configs(tmp_path, records):
    """records: {key: dict}. Returns the basepath to hand to Config."""
    cc = tmp_path / "config_cache"
    cc.mkdir()
    for k, rec in records.items():
        (cc / f"{k}.json").write_text(json.dumps(rec))
    return str(tmp_path)


def base_record(tmp_path, typ="base", **extra):
    rec = dict(BASE, type=typ, base_dir=str(tmp_path))
    rec.update(extra)
    return rec


def todays_layout(tmp_path, **extra):
    return write_configs(tmp_path, {
        "base": base_record(tmp_path, **extra),
        "caches": dict(CACHE_SETTINGS, name="_caches", type="caches"),
        "map_idmap": {"type": "map", "name": "idmap", "storeClass": "storage.idmap.memory.IdMap"},
        "lcnaf": {"type": "external", "name": "lcnaf", "namespace": "http://id.loc.gov/authorities/names/",
                  "matches": ["id.loc.gov/authorities/names/"]},
    })


def refactor_layout(tmp_path, **extra):
    return write_configs(tmp_path, {
        "system": base_record(tmp_path, typ="system", **extra),
        "default_cache": dict(CACHE_SETTINGS, name="cache", type="default"),
        "default_map": {"type": "default", "name": "map", "host": "localhost", "port": 6379},
        "default_ml": {"type": "default", "name": "ml", "user": "admin"},
        "map_idmap": {"type": "map", "name": "idmap", "storeClass": "storage.idmap.memory.IdMap"},
        "lcnaf": {"type": "external", "name": "lcnaf", "namespace": "http://id.loc.gov/authorities/names/",
                  "matches": ["id.loc.gov/authorities/names/"]},
    })


# --- both layouts load -------------------------------------------------------


def test_todays_layout_loads(tmp_path):
    cfgs = Config(basepath=todays_layout(tmp_path))
    assert "lcnaf" in cfgs.external
    assert "idmap" in cfgs.map_stores
    assert cfgs.caches["user"] == "u"
    # the single caches record is the cache default
    assert cfgs.defaults["cache"] is cfgs.caches
    assert cfgs.defaults["map"] == {}
    assert cfgs.defaults["marklogic"] == {}


def test_refactor_layout_loads(tmp_path):
    cfgs = Config(basepath=refactor_layout(tmp_path))
    assert "lcnaf" in cfgs.external
    assert cfgs.defaults["cache"]["user"] == "u"
    assert cfgs.defaults["map"]["port"] == 6379
    # "ml" is accepted as the name of the marklogic defaults
    assert cfgs.defaults["marklogic"]["user"] == "admin"
    assert cfgs.caches == {}


def test_system_record_wins_over_base(tmp_path):
    bp = write_configs(tmp_path, {
        "system": base_record(tmp_path, typ="system", internal_uri="from-system"),
        "base": base_record(tmp_path, internal_uri="from-base"),
    })
    assert Config(basepath=bp).internal_uri == "from-system"


def test_missing_base_and_system_is_a_clear_error(tmp_path):
    bp = write_configs(tmp_path, {"globals": {"type": "globals"}})
    with pytest.raises(ValueError, match="system.json or base.json"):
        Config(basepath=bp)


# --- new settings with defaults ---------------------------------------------


@pytest.mark.parametrize("layout", [todays_layout, refactor_layout])
def test_max_workers_temp_dir_indexes_dir_exist_on_every_load(tmp_path, layout):
    cfgs = Config(basepath=layout(tmp_path))
    assert cfgs.max_workers >= 1
    assert os.path.isabs(cfgs.temp_dir)
    assert cfgs.temp_dir == os.path.join(str(tmp_path), "tmp")
    assert os.path.isabs(cfgs.indexes_dir)
    assert cfgs.indexes_dir == os.path.join(str(tmp_path), "data/processing")


def test_explicit_max_workers_and_temp_dir_are_kept(tmp_path):
    cfgs = Config(basepath=todays_layout(tmp_path, max_workers=24, temp_dir="/scratch"))
    assert cfgs.max_workers == 24
    assert cfgs.temp_dir == "/scratch"


def test_absolute_indexes_dir_is_left_alone(tmp_path):
    cfgs = Config(basepath=todays_layout(tmp_path, indexes_dir="/data-io2/processing"))
    assert cfgs.indexes_dir == "/data-io2/processing"


def test_indexes_dir_defaults_to_data_dir(tmp_path):
    recs = {"base": base_record(tmp_path)}
    del recs["base"]["indexes_dir"]
    cfgs = Config(basepath=write_configs(tmp_path, recs))
    assert cfgs.indexes_dir == cfgs.data_dir


# --- record handling ---------------------------------------------------------


@pytest.mark.parametrize("value, expected", [
    (None, "data/output"),
    ("data/nt", "data/nt"),
    ("/elsewhere/nt", "/elsewhere/nt"),
])
def test_nt_dir_is_optional_and_falls_back_to_exports_dir(tmp_path, value, expected):
    extra = {} if value is None else {"nt_dir": value}
    cfgs = Config(basepath=todays_layout(tmp_path, **extra))
    assert cfgs.nt_dir == os.path.join(str(tmp_path), expected)


def test_unknown_record_type_is_logged_not_raised(tmp_path, caplog):
    bp = write_configs(tmp_path, {
        "base": base_record(tmp_path),
        "odd": {"type": "banana", "name": "odd"},
    })
    with caplog.at_level(logging.CRITICAL, logger="lux_pipeline"):
        cfgs = Config(basepath=bp)
    assert cfgs.base_dir == str(tmp_path)
    assert any("banana" in r.getMessage() and r.levelno == logging.CRITICAL for r in caplog.records)


def test_default_for_an_unknown_kind_raises(tmp_path):
    bp = write_configs(tmp_path, {
        "base": base_record(tmp_path),
        "default_x": {"type": "default", "name": "elephant"},
    })
    with pytest.raises(ValueError, match="elephant"):
        Config(basepath=bp)


def test_handle_config_record_rejects_bad_records(tmp_path):
    cfgs = Config(basepath=todays_layout(tmp_path))
    with pytest.raises(ValueError, match="not a dict"):
        cfgs.handle_config_record("x", "just a string")
    with pytest.raises(ValueError, match="no 'type'"):
        cfgs.handle_config_record("x", {"name": "x"})


def test_handle_config_record_files_each_type(tmp_path):
    cfgs = Config(basepath=todays_layout(tmp_path))
    cfgs.handle_config_record("ycba", {"type": "internal", "name": "ycba"})
    cfgs.handle_config_record("merged", {"type": "results", "name": "merged"})
    cfgs.handle_config_record("ml", {"type": "marklogic", "name": "ml_dev"})
    cfgs.handle_config_record("map2", {"type": "map", "name": "redirects"})
    cfgs.handle_config_record("globals", {"type": "globals", "english": "300388277"})
    assert "ycba" in cfgs.internal
    assert "merged" in cfgs.results
    assert "ml_dev" in cfgs.marklogic_stores
    assert "redirects" in cfgs.map_stores
    assert cfgs.globals_cfg == {"english": "300388277"}


# --- defaults are merged under stores ----------------------------------------


class RecordingStore:
    def __init__(self, config):
        self.config = config


def test_map_and_marklogic_defaults_are_merged_under_the_store_config(tmp_path, monkeypatch):
    bp = write_configs(tmp_path, {
        "system": base_record(tmp_path, typ="system"),
        "default_map": {"type": "default", "name": "map", "host": "redis-host", "port": 6379},
        "default_ml": {"type": "default", "name": "marklogic", "user": "admin", "port": 8000},
        "map_idmap": {"type": "map", "name": "idmap", "port": 6380},
        "ml_dev": {"type": "marklogic", "name": "ml_dev", "user": "dev"},
    })
    import lux_pipeline.config as config_mod
    monkeypatch.setattr(config_mod, "importObject", lambda path: RecordingStore)
    cfgs = Config(basepath=bp)
    idmap = cfgs.instantiate_map("idmap")["store"]
    assert idmap.config["host"] == "redis-host"      # from the default
    assert idmap.config["port"] == 6380              # the store's own value wins
    ml = cfgs.instantiate_ml("ml_dev")["store"]
    assert ml.config["port"] == 8000
    assert ml.config["user"] == "dev"


def test_downloader_class_defaults_to_the_base_downloader(tmp_path, monkeypatch):
    bp = write_configs(tmp_path, {
        "base": base_record(tmp_path),
        "caches": dict(CACHE_SETTINGS, name="_caches", type="caches"),
        "with": {"type": "external", "name": "with", "downloaderClass": "x.Recording",
                 "datacacheClass": "x.Recording", "recordcacheClass": "x.Recording",
                 "recordcacheReconciledClass": "x.Recording", "mapperClass": "x.Recording",
                 "loaderClass": "x.Recording", "recordcache2Class": "x.Recording",
                 "acquirerClass": "x.Recording"},
        "without": {"type": "external", "name": "without",
                    "datacacheClass": "x.Recording", "recordcacheClass": "x.Recording",
                    "recordcacheReconciledClass": "x.Recording", "mapperClass": "x.Recording",
                    "loaderClass": "x.Recording", "recordcache2Class": "x.Recording",
                    "acquirerClass": "x.Recording"},
        "opted_out": {"type": "external", "name": "opted_out", "downloaderClass": None,
                      "datacacheClass": "x.Recording", "recordcacheClass": "x.Recording",
                      "recordcacheReconciledClass": "x.Recording", "mapperClass": "x.Recording",
                      "loaderClass": "x.Recording", "recordcache2Class": "x.Recording",
                      "acquirerClass": "x.Recording"},
    })
    import lux_pipeline.config as config_mod
    requested = []

    def fake_import(path):
        requested.append(path)
        return RecordingStore

    monkeypatch.setattr(config_mod, "importObject", fake_import)
    cfgs = Config(basepath=bp)
    assert isinstance(cfgs.instantiate("with")["downloader"], RecordingStore)
    assert isinstance(cfgs.instantiate("without")["downloader"], RecordingStore)
    assert "process.base.downloader.BaseDownloader" in requested
    assert cfgs.instantiate("opted_out")["downloader"] is None
