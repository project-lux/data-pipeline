"""Item W00: the database-free installation in tests/webui_fixtures.py
builds a real Config, instantiates every component on the filesystem
stores, and round-trips the records seed_records puts in it."""

import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline import runtime
from lux_pipeline.config import _modernize_object_type, importObject
from lux_pipeline.storage.cache.filesystem import FsCache
from lux_pipeline.storage.idmap.filesystem import FsMap, IdMap
from webui_fixtures import (DEFAULT_SOURCES, SOURCES, installation_records, make_configs,
                            make_installation, seed_records, use_installation)

CACHES = ("datacache", "recordcache", "reconciledRecordcache", "recordcache2")


@pytest.fixture
def elsewhere(tmp_path, monkeypatch):
    """An empty working directory, so a test can tell nothing was written
    into it."""
    d = tmp_path / "cwd"
    d.mkdir()
    monkeypatch.chdir(d)
    return d


def test_the_installation_builds_and_instantiates(tmp_path, elsewhere):
    root = tmp_path / "inst"
    cfgs = make_configs(root, instantiate=True)
    assert list(cfgs.internal) == ["ycba"] and list(cfgs.external) == ["aat"]
    assert list(cfgs.results) == ["merged"]
    assert set(cfgs.map_stores) == {"idmap", "networkmap", "all_refs", "done_refs", "redirects"}
    files = root / "data" / "files"
    for group in ("internal", "external", "results"):
        for name, cfg in getattr(cfgs, group).items():
            for c in CACHES:
                cache = cfg.get(c)
                if cache is None:
                    continue
                assert isinstance(cache, FsCache), (name, c)
                assert Path(cache.directory).parent == files, (name, c)
    assert isinstance(cfgs.get_idmap(), IdMap)
    assert all(isinstance(cfgs.map_stores[m]["store"], FsMap) for m in ("networkmap",))
    assert cfgs.max_workers == 2 and cfgs.log_dir == str(root / "data" / "logs")
    assert cfgs.temp_dir == str(root / "data" / "tmp")
    # nothing outside the installation
    assert list(elsewhere.iterdir()) == []


def test_every_source_in_the_catalogue_builds(tmp_path):
    cfgs = make_configs(tmp_path, instantiate=True, sources=tuple(SOURCES))
    assert set(cfgs.internal) == {n for n, s in SOURCES.items() if s[0] == "internal"}
    assert set(cfgs.external) == {n for n, s in SOURCES.items() if s[0] == "external"}
    for cfg in [*cfgs.internal.values(), *cfgs.external.values()]:
        assert cfg["mapper"] is not None and cfg["acquirer"] is not None


def test_the_records_name_classes_that_exist_by_their_current_paths(tmp_path):
    for key, rec in installation_records(tmp_path, tuple(SOURCES)).items():
        for k, v in rec.items():
            if k.endswith("Class") and isinstance(v, str):
                assert _modernize_object_type(v) == v, (key, k, v)
                assert importObject(v) is not None, (key, k, v)


def test_make_installation_writes_the_files(tmp_path):
    root = make_installation(tmp_path)
    assert (root / ".env").read_text() == f"LUX_BASEPATH={root}\n"
    assert (root / "data" / "files" / "idmap_update_token.txt").read_text().strip() == "__20260101__"
    names = sorted(p.name for p in (root / "config_cache").iterdir())
    assert names == sorted(f"{k}.json" for k in installation_records(root))
    base = json.loads((root / "config_cache" / "base.json").read_text())
    assert base["base_dir"] == str(root) and base["idmap_name"] == "idmap"


def test_seeded_records_read_back_as_the_pipeline_stores_them(tmp_path):
    cfgs = make_configs(tmp_path, instantiate=True)
    seeded = seed_records(cfgs)
    assert set(seeded) == {"ycba", "aat"}
    idmap = cfgs.get_idmap()
    merged = cfgs.results["merged"]["recordcache"]
    for name, s in seeded.items():
        cfg = cfgs.internal.get(name) or cfgs.external.get(name)
        assert cfg["datacache"][s["identifier"]]["data"]["id"] == s["uri"]
        assert cfg["recordcache"][s["key"]]["data"]["type"] == s["type"]
        typed = cfgs.make_qua(s["uri"], s["type"])
        assert idmap[typed] == s["yuid"]
        assert idmap[s["yuid"]] == [typed]
        assert merged[s["uuid"]]["data"]["id"] == s["yuid"]
    # internal records under the bare identifier, external under the typed key
    assert seeded["ycba"]["key"] == "object/1"
    assert seeded["aat"]["key"] == "300033618##quaType"
    # the slash in an identifier survives the filesystem cache's escaping
    assert list(cfgs.internal["ycba"]["datacache"].iter_keys()) == ["object/1"]


def test_seeding_survives_a_rebuild_of_the_configuration(tmp_path):
    seeded = seed_records(make_configs(tmp_path, instantiate=True))
    again = make_configs(tmp_path, instantiate=True)
    assert again.get_idmap()[again.make_qua(seeded["aat"]["uri"], "Type")] == seeded["aat"]["yuid"]
    assert len(again.results["merged"]["recordcache"]) == 2


def test_uris_resolve_to_their_sources(tmp_path):
    cfgs = make_configs(tmp_path, instantiate=True)
    src, ident = cfgs.split_uri("http://vocab.getty.edu/aat/300033618")
    assert (src["name"], ident) == ("aat", "300033618")
    src, ident = cfgs.split_uri("https://ycba-lux.s3.amazonaws.com/v3/object/1")
    assert (src["name"], ident) == ("ycba", "object/1")


def test_the_validator_is_configured_and_its_schemas_are_absent(tmp_path):
    cfgs = make_configs(tmp_path)
    assert cfgs.validator is not None
    assert "core.json" in cfgs.validator.missing()


def test_use_installation_points_runtime_at_each_installation(tmp_path, monkeypatch):
    a = use_installation(monkeypatch, tmp_path / "a")
    first = runtime.get_configs()
    assert first.base_dir == str(a) and list(first.internal) == list(DEFAULT_SOURCES[:1])
    b = use_installation(monkeypatch, tmp_path / "b", sources=("yuag", "ulan"))
    second = runtime.get_configs()
    assert second is not first and second.base_dir == str(b)
    assert list(second.internal) == ["yuag"] and list(second.external) == ["ulan"]
    assert os.environ["LUX_BASEPATH"] == str(b)
