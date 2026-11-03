"""Sub-config stores (item 35): a {"type": "config"} record in the
configuration directory names another cache whose records are more
configuration. Built from temporary directories through FsCache, as
Config(basepath=...) does; nothing is instantiated, so no Postgres."""

import json
import os
import shutil
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.config import Config
from lux_pipeline.storage.cache.filesystem import FsCache

PIPELINE_CONFIGS = Path(__file__).parent.parent.parent / "pipeline-configs" / "configs"

SYSTEM = {
    "type": "system",
    "data_dir": "data/files",
    "exports_dir": "data/output",
    "indexes_dir": "data/processing",
    "ok_record_types": {"Person": "person"},
    "parent_record_types": {},
    "path_types": {"reconcileDbPath": "indexes_dir"},
    "idmap_name": "idmap",
}

LCNAF = {"type": "external", "name": "lcnaf", "namespace": "http://id.loc.gov/authorities/names/",
         "matches": ["id.loc.gov/authorities/names/"]}
YCBA = {"type": "internal", "name": "ycba", "namespace": "https://ycba-lux.s3.amazonaws.com/v3/",
        "matches": ["ycba-lux.s3.amazonaws.com/v3/"]}
IDMAP = {"type": "map", "name": "idmap", "storeClass": "storage.idmap.memory.IdMap"}
ML = {"type": "marklogic", "name": "ml_local", "rest": "http://localhost:8000/LATEST/documents"}


def write(directory, records):
    directory.mkdir(parents=True, exist_ok=True)
    for k, rec in records.items():
        (directory / f"{k}.json").write_text(json.dumps(rec))
    return directory


def layout(tmp_path, system_extra=None, config_records=None, subs=None):
    """``<tmp>/system/config_cache/`` with system.json plus `config_records`
    (the "type": "config" ones) and ``<tmp>/subs/<name>_cache/`` for each
    entry of `subs` ({name: {key: record}}). Returns the basepath."""
    sysdir = tmp_path / "system"
    records = {"system": {**SYSTEM, "base_dir": str(tmp_path), **(system_extra or {})}}
    records.update(config_records or {})
    write(sysdir / "config_cache", records)
    for name, recs in (subs or {}).items():
        write(tmp_path / "subs" / f"{name}_cache", recs)
    return str(sysdir)


def store(name, base_dir="../subs", **extra):
    return dict({"name": name, "type": "config", "base_dir": base_dir}, **extra)


# --- loading -----------------------------------------------------------------


def test_sub_store_records_land_where_the_main_directory_s_would(tmp_path, caplog):
    bp = layout(tmp_path,
                config_records={"sources": store("sources"), "maps": store("maps"), "mls": store("mls")},
                subs={"sources": {"lcnaf": LCNAF, "ycba": YCBA},
                      "maps": {"map_idmap": IDMAP},
                      "mls": {"ml_local": ML}})
    with caplog.at_level("DEBUG", logger="lux_pipeline"):
        cfgs = Config(basepath=bp)
    assert cfgs.external["lcnaf"] is not None and cfgs.external["lcnaf"]["name"] == "lcnaf"
    assert cfgs.internal["ycba"]["namespace"] == YCBA["namespace"]
    assert cfgs.map_stores["idmap"]["storeClass"] == IDMAP["storeClass"]
    assert cfgs.marklogic_stores["ml_local"]["rest"] == ML["rest"]
    assert sorted(cfgs.subconfig_stores) == ["maps", "mls", "sources"]
    # at DEBUG only: nothing is written to the console while a configuration loads
    assert "Read 2 configuration records from sub-config store sources" in caplog.text
    assert all(r.levelname == "DEBUG" for r in caplog.records if "configuration records" in r.getMessage())


def test_relative_base_dir_resolves_against_the_configuration_directory(tmp_path):
    bp = layout(tmp_path, config_records={"sources": store("sources")}, subs={"sources": {"lcnaf": LCNAF}})
    cfgs = Config(basepath=bp)
    cfg = cfgs.subconfig_stores["sources"]
    assert cfg["base_dir"] == str(tmp_path / "subs")
    assert cfgs.config_dir == str(tmp_path / "system")
    assert isinstance(cfg["datacache"], FsCache)
    assert cfg["datacache"].directory == str(tmp_path / "subs" / "sources_cache")
    assert cfg["tabletype"] == "cache"
    assert cfg["all_configs"] is cfgs


def test_absolute_base_dir_is_left_alone(tmp_path):
    shared = tmp_path / "elsewhere"
    write(shared / "sources_cache", {"lcnaf": LCNAF})
    bp = layout(tmp_path, config_records={"sources": store("sources", base_dir=str(shared))})
    cfgs = Config(basepath=bp)
    assert cfgs.subconfig_stores["sources"]["base_dir"] == str(shared)
    assert "lcnaf" in cfgs.external


def test_the_data_base_dir_is_not_what_relative_paths_resolve_against(tmp_path):
    """The system record's base_dir is the data directory, often a
    placeholder in a shared checkout; it must play no part."""
    bp = layout(tmp_path, system_extra={"base_dir": "path-to-where-files-should-live"},
                config_records={"sources": store("sources")}, subs={"sources": {"lcnaf": LCNAF}})
    cfgs = Config(basepath=bp)
    assert "lcnaf" in cfgs.external


def test_missing_store_directory_is_an_error(tmp_path):
    bp = layout(tmp_path, config_records={"sources": store("sources")})
    with pytest.raises(ValueError, match="does not exist") as info:
        Config(basepath=bp)
    assert str(tmp_path / "subs" / "sources_cache") in str(info.value)
    assert not (tmp_path / "subs" / "sources_cache").exists()


def test_store_without_a_name_is_an_error(tmp_path):
    bp = layout(tmp_path, config_records={"sources": {"type": "config", "base_dir": "../subs"}})
    with pytest.raises(ValueError, match="no 'name'"):
        Config(basepath=bp)


def test_unknown_store_key(tmp_path):
    cfgs = Config(basepath=layout(tmp_path))
    with pytest.raises(ValueError, match="No such sub-config store"):
        cfgs.instantiate_config("nope")


# --- precedence and defaults -------------------------------------------------


def test_a_sub_store_record_replaces_a_main_record_with_the_same_key(tmp_path):
    bp = layout(tmp_path, config_records={"sources": store("sources"), "lcnaf": dict(LCNAF, merge_order=1)},
                subs={"sources": {"lcnaf": dict(LCNAF, merge_order=22)}})
    cfgs = Config(basepath=bp)
    assert cfgs.external["lcnaf"]["merge_order"] == 22


def test_defaults_in_a_sub_store_apply(tmp_path):
    bp = layout(tmp_path, config_records={"sources": store("sources")},
                subs={"sources": {"default_cache": {"type": "default", "name": "cache", "user": "u", "host": ""},
                                  "lcnaf": LCNAF}})
    cfgs = Config(basepath=bp)
    assert cfgs.defaults["cache"]["user"] == "u"


def test_caches_record_fallback_still_applies_after_sub_stores(tmp_path):
    bp = layout(tmp_path, config_records={"sources": store("sources"),
                                          "caches": {"type": "caches", "name": "_caches", "user": "today"}},
                subs={"sources": {"lcnaf": LCNAF}})
    cfgs = Config(basepath=bp)
    assert cfgs.defaults["cache"] is cfgs.caches


def test_nested_stores_load_once_and_a_cycle_terminates(tmp_path):
    bp = layout(tmp_path, config_records={"sources": store("sources")},
                subs={"sources": {"lcnaf": LCNAF, "more": store("more")},
                      "more": {"ycba": YCBA, "sources": store("sources")}})
    cfgs = Config(basepath=bp)
    assert "lcnaf" in cfgs.external and "ycba" in cfgs.internal
    assert sorted(cfgs.subconfig_stores) == ["more", "sources"]


def test_a_bad_record_in_a_sub_store_raises(tmp_path):
    bp = layout(tmp_path, config_records={"sources": store("sources")},
                subs={"sources": {"broken": {"name": "broken"}}})
    with pytest.raises(ValueError, match="has no 'type'"):
        Config(basepath=bp)


def test_instantiate_config_is_idempotent(tmp_path):
    bp = layout(tmp_path, config_records={"sources": store("sources")}, subs={"sources": {"lcnaf": LCNAF}})
    cfgs = Config(basepath=bp)
    cache = cfgs.subconfig_stores["sources"]["datacache"]
    assert cfgs.instantiate_config("sources")["datacache"] is cache


# --- the pipeline-configs repository ----------------------------------------


@pytest.mark.skipif(not PIPELINE_CONFIGS.is_dir(), reason="pipeline-configs checkout not beside this repo")
def test_pipeline_configs_repository_loads_from_a_copy(tmp_path):
    """The sibling repository's files, copied, with its three store records
    pointed at ``../subs`` (the one edit the repository needs). No
    instantiation: that is what item 34 and the per-machine values are for."""
    shutil.copytree(PIPELINE_CONFIGS, tmp_path / "configs")
    cc = tmp_path / "configs" / "system" / "config_cache"
    for fn in ("sources", "maps", "mls"):
        rec = json.loads((cc / f"{fn}.json").read_text())
        rec["base_dir"] = "../subs"
        (cc / f"{fn}.json").write_text(json.dumps(rec))
    cfgs = Config(basepath=str(tmp_path / "configs" / "system"))
    assert {"ror", "wikidata", "lcnaf", "aat"} <= set(cfgs.external)
    assert {"ycba", "ypm", "ils"} <= set(cfgs.internal)
    assert {"merged", "marklogic"} <= set(cfgs.results)
    assert {"idmap", "all_refs", "done_refs", "networkmap", "record_refs", "redirects"} <= set(cfgs.map_stores)
    assert "ml_local" in cfgs.marklogic_stores
    assert cfgs.defaults["cache"]["cursor_size"] == 1000
    assert cfgs.defaults["marklogic"]["database"] == "lux-content"
    assert cfgs.globals_cfg["primaryName"] == "300404670"
