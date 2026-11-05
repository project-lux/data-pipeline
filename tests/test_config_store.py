"""Configuration records read, checked and written through the caches they
came from (lux_pipeline.config_store: each a ConfigCache, an FsCache),
written as normalized JSON and checked against the schema generated from
the catalogue. Built on the W00 installation, with a sub-config store
added; no database."""

import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline import config_store as store
from lux_pipeline.config import Config
from lux_pipeline.config_store import ConfigError
from webui_fixtures import add_subconfig_store, make_installation

HAND = ('{\n    "name": "_caches",\n    "type": "caches",\n\n    "host": "",\n    "port": 5432,\n\n'
        '    "connect": true,\n    "overwrite": true\n}\n')


def installation(tmp_path):
    """The W00 installation with aat moved into a sub-config store
    ``sources`` (2-space indent, as pipeline-configs writes them) and the
    caches record hand-formatted with blank lines."""
    root = make_installation(tmp_path / "inst")
    cc = root / "config_cache"
    add_subconfig_store(root, "sources", move=("aat",))
    caches = json.loads((cc / "caches.json").read_text())
    text = HAND.replace('"_caches",', '"_caches",\n    "base_dir": ' + json.dumps(caches["base_dir"]) + ",")
    (cc / "caches.json").write_text(text)
    return root


@pytest.fixture
def root(tmp_path):
    return installation(tmp_path)


@pytest.fixture
def cfgs(root):
    return Config(basepath=str(root))


# --- where records came from -------------------------------------------------------------


def test_config_remembers_each_records_file_and_store(root, cfgs):
    cc = root / "config_cache"
    assert cfgs.record_origins["ycba"] == {"store": None, "path": str(cc / "ycba.json")}
    assert cfgs.record_origins["aat"] == {"store": "sources",
                                          "path": str(root / "subs" / "sources_cache" / "aat.json")}
    assert cfgs.record_origins["base"]["path"] == str(cc / "base.json")
    assert cfgs.base_record_key == "base"
    assert set(cfgs.record_origins) >= {"base", "caches", "globals", "ycba", "aat", "merged",
                                        "sources", "map_idmap"}


def test_read_record_is_the_file_not_the_live_dict(root, cfgs):
    data = store.find(cfgs, "aat").read("aat")
    assert data == json.loads((root / "subs" / "sources_cache" / "aat.json").read_text())
    assert data is not cfgs.external["aat"] and "all_configs" not in data
    with pytest.raises(ConfigError, match="no configuration record 'aatt'.*did you mean aat"):
        store.find(cfgs, "aatt").read("aatt")


def test_all_records_lists_type_name_and_store(cfgs):
    rows = {r["key"]: r for r in store.records(cfgs)}
    assert (rows["aat"]["type"], rows["aat"]["name"], rows["aat"]["store"]) == ("external", "aat", "sources")
    assert (rows["map_idmap"]["type"], rows["map_idmap"]["name"]) == ("map", "idmap")
    assert rows["caches"]["store"] is None and rows["caches"]["error"] is None


# --- checking ---------------------------------------------------------------------------------


def test_the_installation_has_no_problems(cfgs):
    for key in cfgs.record_origins:
        assert store.find(cfgs, key).check(key) == [], key


def with_(cfgs, key, **changes):
    data = store.find(cfgs, key).read(key)
    data.update(changes)
    return store.find(cfgs, key).validate(key, data)


@pytest.mark.parametrize("key,changes,expect", [
    ("aat", {"name": "getty"}, "a source's name must be its key 'aat'"),
    ("aat", {"type": "authority"}, "type 'authority' is not one of"),
    ("aat", {"mapperClass": "sources.getty.mapper.NoSuchMapper"}, "mapperClass: lux_pipeline.sources.getty.mapper has no NoSuchMapper"),
    ("aat", {"mapperClass": "sources.nosuch.mapper.M"}, "mapperClass: no module"),
    ("aat", {"mapperClass": ""}, "mapperClass is empty (null switches the component off)"),
    ("aat", {"merge_order": "10"}, "merge_order should be int, not str"),
    ("aat", {"matches": "vocab.getty.edu"}, "matches should be list, not str"),
    ("aat", {"namespace": ""}, "namespace is empty"),
    ("aat", {"indexes": {"reconcile_label": {"path": "x.lmdb", "indexClass": "storage.nope.Index"}}}, "indexes: "),
    ("base", {"max_workers": "24"}, "max_workers should be int, not str ('24')"),
    ("base", {"max_workers": 0}, "max_workers should be at least 1, not 0"),
    ("base", {"path_types": {"dumpFilePath": "dumps_dir", "x": "nowhere_dir"}}, "path_types maps x to nowhere_dir"),
    ("map_networkmap", {"name": "idmap"}, "name 'idmap' is also the name of map_idmap"),
    ("map_idmap", {"storeClass": None}, "storeClass is empty"),
    ("caches", {"port": "5432"}, "port should be int, not str"),
])
def test_validate_reports(cfgs, key, changes, expect):
    problems = with_(cfgs, key, **changes)
    assert any(expect in p for p in problems), problems
    assert all(not p.startswith("warning:") for p in problems)


def test_null_switches_a_component_off_and_unknown_keys_are_fine(cfgs):
    assert with_(cfgs, "aat", acquirerClass=None, loaderClass=None, myMapperSetting=[1, "x"]) == []


def test_an_old_class_path_is_only_a_warning(cfgs):
    problems = with_(cfgs, "aat", mapperClass="pipeline.sources.authorities.getty.mapper.AatMapper")
    assert problems == ["warning: mapperClass uses the old path pipeline.sources.authorities.getty."
                        "mapper.AatMapper; write lux_pipeline.sources.getty.mapper.AatMapper"]


def test_globals_values_are_terms_even_one_called_class(cfgs):
    assert with_(cfgs, "globals", Class="Q37517", primaryName="300404670") == []
    assert with_(cfgs, "globals", primaryName=300404670) == [
        "primaryName should be str, not int (300404670)"]


def test_default_records_and_new_keys(cfgs):
    here = store.store_cache(cfgs)
    assert "a default record's name is one of" in here.validate(
        "default_x", {"type": "default", "name": "caches"})[0]
    assert here.validate("default_cache", {"type": "default", "name": "cache", "port": 5432}) == []
    aat = store.find(cfgs, "aat").read("aat")
    assert any("already exists in store sources" in p for p in here.validate("aat", aat, new=True))
    assert any("not a usable record key" in p for p in here.validate("a/b", {"type": "globals"}))


def test_a_key_written_twice_is_a_warning_from_the_file(root, cfgs):
    path = Path(cfgs.record_origins["globals"]["path"])
    path.write_text('{\n    "type": "globals",\n    "Subgenus": "Q3238261",\n    "Subgenus": "Q3238261"\n}\n')
    assert store.find(cfgs, "globals").check("globals") == [
        "warning: Subgenus appears twice in the file; only the last value counts"]


@pytest.mark.parametrize("path,ok,modern", [
    ("sources.getty.mapper.AatMapper", True, None),
    ("lux_pipeline.sources.getty.mapper.AatMapper", True, None),
    ("pipeline.sources.authorities.getty.mapper.AatMapper", True,
     "lux_pipeline.sources.getty.mapper.AatMapper"),
    ("sources.getty.mapper.Nope", False, None),
    ("nosuch.module.Thing", False, None),
    ("NoDots", False, None),
])
def test_resolve_class(path, ok, modern):
    cls, error, new = store.resolve_class(path)
    assert (cls is not None, error is None, new) == (ok, ok, modern)


# --- writing: normalized JSON, through the record's own cache ---------------------------------


def normalized(data):
    return json.dumps(data, indent=2, ensure_ascii=False) + "\n"


def test_write_normalizes_the_file_and_config_sees_it(root, cfgs):
    path = Path(cfgs.record_origins["caches"]["path"])
    assert path.read_text().startswith('{\n    "name"')            # hand-formatted, 4-space
    data = store.find(cfgs, "caches").read("caches")
    data["port"] = 5433
    assert store.find(cfgs, "caches").write("caches", data) == (str(path), [])
    assert path.read_text() == normalized(data)
    assert sorted(p.name for p in path.parent.iterdir() if p.name.startswith("caches")) == ["caches.json"]
    assert Config(basepath=str(root)).caches["port"] == 5433


def test_a_store_record_keeps_its_slashes_and_is_one_line_changed(root, cfgs):
    path = Path(cfgs.record_origins["aat"]["path"])
    before = path.read_text()
    data = store.find(cfgs, "aat").read("aat")
    data["merge_order"] = 11
    store.find(cfgs, "aat").write("aat", data)
    after = path.read_text()
    assert "\\/" not in after and "http://vocab.getty.edu/aat/" in after
    assert changed_lines(before, after) == ['-  "merge_order": 10,', '+  "merge_order": 11,']


def changed_lines(a, b):
    import difflib
    return [l for l in difflib.unified_diff(a.splitlines(), b.splitlines(), lineterm="", n=0)
            if l[:1] in "+-" and not l.startswith(("+++", "---"))]


def test_a_no_op_write_leaves_the_file_alone(cfgs):
    data = store.find(cfgs, "aat").read("aat")
    path = Path(cfgs.record_origins["aat"]["path"])
    os.utime(path, (1, 1))
    store.find(cfgs, "aat").write("aat", data)
    assert path.stat().st_mtime == 1
    assert store.find(cfgs, "aat").diff("aat", data) == ""


def test_an_invalid_record_is_not_written(cfgs):
    path = Path(cfgs.record_origins["base"]["path"])
    before = path.read_text()
    data = store.find(cfgs, "base").read("base")
    data["max_workers"] = "lots"
    with pytest.raises(ConfigError, match="not writing base: max_workers should be int") as e:
        store.find(cfgs, "base").write("base", data)
    assert e.value.problems and path.read_text() == before


def test_warnings_are_returned_and_do_not_block(cfgs):
    data = store.find(cfgs, "aat").read("aat")
    data["mapperClass"] = "pipeline.sources.authorities.getty.mapper.AatMapper"
    path, warnings = store.find(cfgs, "aat").write("aat", data)
    assert warnings and warnings[0].startswith("warning: mapperClass uses the old path")


def test_diff_is_against_what_would_be_written(cfgs):
    data = store.find(cfgs, "caches").read("caches")
    diff = store.find(cfgs, "caches").diff("caches", data)
    path = cfgs.record_origins["caches"]["path"]
    assert diff.startswith(f"--- {path}\n+++ {path}\n")
    # the hand-formatted file would be normalized even with no value changed
    assert '-    "port": 5432,\n' in diff and '+  "port": 5432,\n' in diff


def test_write_text_is_stored_normalized(cfgs):
    text = '{"name":"aat","type":"external","namespace":"http://vocab.getty.edu/aat/","merge_order":3}'
    path, warnings = store.find(cfgs, "aat").write_text("aat", text)
    assert Path(path).read_text() == normalized(json.loads(text)) and warnings == []
    path, warnings = store.find(cfgs, "aat").write_text("aat", text[:-1] + ',"merge_order":4}')
    assert json.loads(Path(path).read_text())["merge_order"] == 4
    assert warnings == ["warning: merge_order appears twice in the file; only the last value counts"]
    with pytest.raises(ConfigError, match="not JSON"):
        store.find(cfgs, "aat").write_text("aat", "{nope")


def test_create_in_the_directory_and_in_a_store(root, cfgs):
    ulan = {"name": "ulan", "type": "external", "namespace": "http://vocab.getty.edu/ulan/",
            "matches": ["vocab.getty.edu/ulan/"]}
    path, _ = store.store_cache(cfgs, "sources").create("ulan", ulan)
    assert path == str(root / "subs" / "sources_cache" / "ulan.json")
    assert Path(path).read_text() == normalized(ulan)
    path2, _ = store.store_cache(cfgs).create("map_extra", {"name": "extra", "type": "map",
                                                           "storeClass": "storage.idmap.filesystem.FsMap",
                                                           "base_dir": str(root / "data" / "files")})
    assert path2 == str(root / "config_cache" / "map_extra.json")
    rebuilt = Config(basepath=str(root))
    assert "ulan" in rebuilt.external and "extra" in rebuilt.map_stores
    assert rebuilt.record_origins["ulan"]["store"] == "sources"
    with pytest.raises(ConfigError, match="already exists"):
        store.store_cache(cfgs, "sources").create("ycba", store.find(cfgs, "ycba").read("ycba"))
    with pytest.raises(ConfigError, match="no sub-config store 'nope'"):
        store.store_cache(cfgs, "nope").create("x", {"type": "globals"})


def test_delete(root, cfgs):
    path = store.find(cfgs, "aat").delete("aat")
    assert not os.path.exists(path)
    assert "aat" not in Config(basepath=str(root)).external


# --- the stores in order, and a record in more than one ---------------------------------------


def test_stores_and_locations(root, cfgs):
    assert store.stores(cfgs) == [None, "sources"]
    assert store.locations(cfgs, "aat") == ["sources"]
    assert store.find(cfgs, "aat") is store.store_cache(cfgs, "sources")
    assert store.locations(cfgs, "ycba") == [None] and store.find(cfgs, "ycba") is cfgs.configcache
    # a copy left in the configuration directory is replaced by the store's
    shadow = root / "config_cache" / "aat.json"
    shadow.write_text((root / "subs" / "sources_cache" / "aat.json").read_text())
    cfgs = Config(basepath=str(root))
    assert store.locations(cfgs, "aat") == [None, "sources"]
    assert cfgs.record_origins["aat"]["store"] == "sources"
    assert store.find(cfgs, "aat").check("aat") == [
        "warning: aat in store sources replaces the record of the same key in the "
        "configuration directory"]


# --- the stores check what is written against the catalogue's schema --------------------------


def test_config_caches_carry_the_catalogue_schema_and_warn_on_set(cfgs, caplog):
    from lux_pipeline import config_schema
    for cache in (cfgs.configcache, store.store_cache(cfgs, "sources")):
        assert isinstance(cache, store.ConfigCache)
        assert cache.config["json_schema"] is config_schema.json_schema()
        assert cache.config["json_format"] == "normalized"
    with caplog.at_level("WARNING", logger="lux_pipeline"):
        cfgs.configcache.set({"type": "map", "name": "x", "storeClass": 3}, identifier="map_x")
    assert "map_x: storeClass: 3 is not of type 'string', 'null'" in caplog.text


def test_a_store_record_can_name_its_own_format_and_schema(root):
    sources = root / "config_cache" / "sources.json"
    rec = json.loads(sources.read_text())
    (root / "subs" / "only-names.json").write_text(json.dumps(
        {"type": "object", "required": ["name", "type"]}))
    rec.update(json_format="compact", json_schema="only-names.json")
    sources.write_text(json.dumps(rec))
    cfgs = Config(basepath=str(root))
    cache = store.store_cache(cfgs, "sources")
    assert cache.dumps({"a": "http://x/"}) == '{"a":"http://x/"}'
    assert cache.record_problems({"type": "external"}) == ["record: 'name' is a required property"]
    # the store's schema adds to the catalogue's, it does not replace it
    data = store.find(cfgs, "aat").read("aat")
    data["namespace"] = ""
    assert any("namespace is empty" in p for p in store.find(cfgs, "aat").validate("aat", data))


def test_set_unset_and_parse_value():
    data = {"a": 1}
    assert store.set_key(data, "indexes.reconcile_label.path", "x") == \
        {"a": 1, "indexes": {"reconcile_label": {"path": "x"}}}
    assert store.unset_key(data, "indexes.reconcile_label") == {"a": 1, "indexes": {}}
    with pytest.raises(ConfigError, match="no b in the record"):
        store.unset_key(data, "b")
    with pytest.raises(ConfigError, match="a in a.b is a int"):
        store.set_key(data, "a.b", 1)
    assert [store.parse_value(v) for v in ("12", "true", "null", '["a"]', '"12"', "plain text")] == \
        [12, True, None, ["a"], "12", "plain text"]
