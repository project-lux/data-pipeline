"""Web UI item W04: configuration records read, checked and written in the
files they came from (lux_pipeline.config_store), with Config remembering
where each came from. Built on the W00 installation, with a sub-config
store added; no database."""

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
    data = store.read_record(cfgs, "aat")
    assert data == json.loads((root / "subs" / "sources_cache" / "aat.json").read_text())
    assert data is not cfgs.external["aat"] and "all_configs" not in data
    with pytest.raises(ConfigError, match="no configuration record 'aatt'.*did you mean aat"):
        store.read_record(cfgs, "aatt")


def test_all_records_lists_type_name_and_store(cfgs):
    rows = {r["key"]: r for r in store.all_records(cfgs)}
    assert (rows["aat"]["type"], rows["aat"]["name"], rows["aat"]["store"]) == ("external", "aat", "sources")
    assert (rows["map_idmap"]["type"], rows["map_idmap"]["name"]) == ("map", "idmap")
    assert rows["caches"]["store"] is None and rows["caches"]["error"] is None


# --- checking ---------------------------------------------------------------------------------


def test_the_installation_has_no_problems(cfgs):
    for key in cfgs.record_origins:
        assert store.check_record(cfgs, key) == [], key


def with_(cfgs, key, **changes):
    data = store.read_record(cfgs, key)
    data.update(changes)
    return store.validate_record(cfgs, key, data)


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
    assert "a default record's name is one of" in store.validate_record(
        cfgs, "default_x", {"type": "default", "name": "caches"})[0]
    assert store.validate_record(cfgs, "default_cache", {"type": "default", "name": "cache",
                                                         "port": 5432}) == []
    assert any("already exists in store sources" in p for p in store.validate_record(
        cfgs, "aat", store.read_record(cfgs, "aat"), new=True))
    assert any("not a usable record key" in p for p in store.validate_record(
        cfgs, "a/b", {"type": "globals"}))


def test_a_key_written_twice_is_a_warning_from_the_file(root, cfgs):
    path = Path(cfgs.record_origins["globals"]["path"])
    path.write_text('{\n    "type": "globals",\n    "Subgenus": "Q3238261",\n    "Subgenus": "Q3238261"\n}\n')
    assert store.check_record(cfgs, "globals") == [
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


# --- editing the text in place ------------------------------------------------------------------

STYLES = {
    "indent2": '{\n  "name": "aat",\n  "type": "external",\n  "merge_order": 10,\n  "matches": [\n    "x"\n  ]\n}\n',
    "indent4": json.dumps({"name": "aat", "type": "external", "merge_order": 10, "matches": ["x"]}, indent=4) + "\n",
    "tabs": '{\n\t"name": "aat",\n\t"type": "external",\n\t"merge_order": 10,\n\t"matches": [\n\t\t"x"\n\t]\n}',
    "hand": HAND,
    "compact": '{"name": "aat", "type": "external", "merge_order": 10}',
}


def changed_lines(a, b):
    import difflib
    return [l for l in difflib.unified_diff(a.splitlines(), b.splitlines(), lineterm="", n=0)
            if l[:1] in "+-" and not l.startswith(("+++", "---"))]


@pytest.mark.parametrize("style", sorted(STYLES))
def test_edits_touch_only_what_changed_in_every_style(style):
    text = STYLES[style]
    old = json.loads(text)
    keys = list(old)
    cases = {
        "same": dict(old),
        "change": {**old, keys[-1]: "changed"},
        "add": {**old, "added": {"k": [1, 2]}},
        "drop first": {k: v for k, v in old.items() if k != keys[0]},
        "drop middle": {k: v for k, v in old.items() if k != keys[1]},
        "drop last": {k: v for k, v in old.items() if k != keys[-1]},
    }
    for label, new in cases.items():
        out = store.edit_text(text, new)
        assert json.loads(out) == new, (style, label)
        assert out.endswith("\n") == text.endswith("\n"), (style, label)
        diff = changed_lines(text, out)
        if label == "same":
            assert out == text
        elif style != "compact" and label == "change" and not isinstance(old[keys[-1]], (list, dict)):
            # one line out, one line in: the rest is untouched
            assert len(diff) == 2, (style, diff)
    if style == "hand":
        out = store.edit_text(text, cases["drop middle"])
        # the blank line separating the groups survives
        assert out.count("\n\n") == text.count("\n\n")


def test_added_members_use_the_files_indent():
    out = store.edit_text(STYLES["tabs"], {**json.loads(STYLES["tabs"]), "indexes": {"a": {"path": "p"}}})
    assert '\t"indexes": {\n\t\t"a": {\n\t\t\t"path": "p"\n\t\t}\n\t}' in out


def test_a_file_with_a_key_twice_is_rewritten_whole_in_its_style():
    text = '{\n  "type": "globals",\n  "a": "1",\n  "a": "1"\n}\n'
    out = store.edit_text(text, {"type": "globals", "a": "2"})
    assert out == '{\n  "type": "globals",\n  "a": "2"\n}\n'


def test_file_style():
    assert store.file_style(STYLES["indent2"]) == {"indent": "  ", "newline": True}
    assert store.file_style(STYLES["tabs"]) == {"indent": "\t", "newline": False}
    assert store.file_style(STYLES["compact"])["indent"] == "    "


# --- writing --------------------------------------------------------------------------------


def test_write_record_changes_one_line_and_config_sees_it(root, cfgs):
    path = Path(cfgs.record_origins["aat"]["path"])
    before = path.read_text()
    data = store.read_record(cfgs, "aat")
    data["merge_order"] = 11
    assert store.write_record(cfgs, "aat", data) == (str(path), [])
    after = path.read_text()
    assert changed_lines(before, after) == ['-  "merge_order": 10,', '+  "merge_order": 11,']
    assert [p.name for p in path.parent.iterdir()] == ["aat.json"]     # no temp file left
    assert Config(basepath=str(root)).external["aat"]["merge_order"] == 11


def test_a_no_op_write_leaves_the_file_alone(cfgs):
    path = Path(cfgs.record_origins["caches"]["path"])
    os.utime(path, (1, 1))
    store.write_record(cfgs, "caches", store.read_record(cfgs, "caches"))
    assert path.stat().st_mtime == 1
    assert store.diff_record(cfgs, "caches", store.read_record(cfgs, "caches")) == ""


def test_an_invalid_record_is_not_written(cfgs):
    path = Path(cfgs.record_origins["base"]["path"])
    before = path.read_text()
    data = store.read_record(cfgs, "base")
    data["max_workers"] = "lots"
    with pytest.raises(ConfigError, match="not writing base: max_workers should be int") as e:
        store.write_record(cfgs, "base", data)
    assert e.value.problems and path.read_text() == before


def test_warnings_are_returned_and_do_not_block(cfgs):
    data = store.read_record(cfgs, "aat")
    data["mapperClass"] = "pipeline.sources.authorities.getty.mapper.AatMapper"
    path, warnings = store.write_record(cfgs, "aat", data)
    assert warnings and warnings[0].startswith("warning: mapperClass uses the old path")


def test_diff_record(cfgs):
    data = store.read_record(cfgs, "caches")
    data["port"] = 5433
    diff = store.diff_record(cfgs, "caches", data)
    path = cfgs.record_origins["caches"]["path"]
    assert diff.startswith(f"--- {path}\n+++ {path}\n")
    assert '-    "port": 5432,\n+    "port": 5433,\n' in diff


def test_write_text_keeps_the_editors_layout(cfgs):
    text = '{"name":"aat","type":"external","namespace":"http://vocab.getty.edu/aat/","merge_order":3}'
    path, _ = store.write_text(cfgs, "aat", text)
    assert Path(path).read_text() == text + "\n"
    with pytest.raises(ConfigError, match="not JSON"):
        store.write_text(cfgs, "aat", "{nope")


def test_new_record_in_the_directory_and_in_a_store(root, cfgs):
    path, _ = store.new_record(cfgs, "ulan", {"name": "ulan", "type": "external",
                                              "namespace": "http://vocab.getty.edu/ulan/",
                                              "matches": ["vocab.getty.edu/ulan/"]}, store="sources")
    assert path == str(root / "subs" / "sources_cache" / "ulan.json")
    assert Path(path).read_text().startswith('{\n  "name": "ulan"')     # the store's 2-space indent
    path2, _ = store.new_record(cfgs, "map_extra", {"name": "extra", "type": "map",
                                                    "storeClass": "storage.idmap.filesystem.FsMap",
                                                    "base_dir": str(root / "data" / "files")})
    assert path2 == str(root / "config_cache" / "map_extra.json")
    rebuilt = Config(basepath=str(root))
    assert "ulan" in rebuilt.external and "extra" in rebuilt.map_stores
    assert rebuilt.record_origins["ulan"]["store"] == "sources"
    with pytest.raises(ConfigError, match="already exists"):
        store.new_record(cfgs, "ycba", store.read_record(cfgs, "ycba"))
    with pytest.raises(ConfigError, match="no sub-config store 'nope'"):
        store.new_record(cfgs, "x", {"type": "globals"}, store="nope")


def test_delete_record(root, cfgs):
    path = store.delete_record(cfgs, "aat")
    assert not os.path.exists(path)
    assert "aat" not in Config(basepath=str(root)).external


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
