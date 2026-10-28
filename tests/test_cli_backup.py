"""`lux backup` and `lux restore` end to end against a configuration of real
filesystem caches, memory and filesystem maps and LMDB indexes in
`tmp_path`: target resolution, the files and manifest written, --all,
--portable, --verify, the free-space check, refusals (unknown names, the
subsidiary idmap, an existing label, a build running, the idmap or a
non-empty store without --force, a backup with no manifest), --list, and
a full round trip."""

import json
import os
import sys
from argparse import Namespace
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.cli import backup as backup_cmd
from lux_pipeline.cli import restore as restore_cmd
from lux_pipeline.storage import backup as bk
from lux_pipeline.storage.cache.filesystem import FsCache
from lux_pipeline.storage.idmap.filesystem import FsMap
from lux_pipeline.storage.idmap.lmdb import TabLmdb
from lux_pipeline.storage.idmap.memory import IdMap as MemIdMap


def fs_cache(root, name, tabletype):
    os.makedirs(root, exist_ok=True)
    return FsCache({"name": name, "tabletype": tabletype, "base_dir": str(root)})


class StubConfigs:
    def __init__(self, tmp):
        self.data_dir = str(tmp / "data")
        self.log_dir = str(tmp / "logs")
        self.backup_dir = str(tmp / "backups")
        os.makedirs(self.log_dir, exist_ok=True)
        caches = tmp / "caches"
        self.internal = {"ycba": {"name": "ycba",
                                  "datacache": fs_cache(caches, "ycba", "datacache"),
                                  "recordcache": fs_cache(caches, "ycba", "recordcache")}}
        self.external = {"aat": {"name": "aat",
                                 "datacache": fs_cache(caches, "aat", "datacache"),
                                 "reconcileDbPath": str(tmp / "idx" / "label_to_aat.lmdb"),
                                 "inverseEquivDbPath": str(tmp / "idx" / "aat_equivs.lmdb")}}
        self.results = {"merged": {"name": "merged",
                                   "recordcache": fs_cache(caches, "merged", "recordcache")}}
        os.makedirs(tmp / "maps", exist_ok=True)
        idmap = MemIdMap()
        self.map_stores = {
            "idmap": {"name": "idmap", "store": idmap},
            "redirects": {"name": "redirects",
                          "store": FsMap({"base_dir": str(tmp / "maps"), "name": "redirects",
                                          "all_configs": None})},
        }


def populate(cfgs, tmp):
    cfgs.internal["ycba"]["datacache"].set({"id": "ycba:1"}, identifier="obj/1")
    cfgs.external["aat"]["datacache"].set({"id": "aat:1"}, identifier="300001")
    cfgs.external["aat"]["datacache"].set({"id": "aat:2"}, identifier="300002")
    cfgs.results["merged"]["recordcache"].set({"id": "lux:1"}, identifier="abc")
    y = cfgs.map_stores["idmap"]["store"].mint("http://test.example.com/1##quaPerson")
    cfgs.map_stores["idmap"]["store"]._add(y, "__20261028__")
    cfgs.map_stores["redirects"]["store"]["http://a"] = "http://b"
    os.makedirs(tmp / "idx", exist_ok=True)
    idx = TabLmdb.open(cfgs.external["aat"]["reconcileDbPath"], "c", map_size=2**22)
    idx.update({"label one": ["http://x/1", "Type"], "label two": "http://x/2"})
    idx.close()


@pytest.fixture
def cfgs(tmp_path):
    c = StubConfigs(tmp_path)
    populate(c, tmp_path)
    return c


def args_for(command, rest_source=None, **kw):
    return Namespace(command=command, source=rest_source, **kw)


def run_backup(cfgs, rest, source=None):
    return backup_cmd.CommandHandler(cfgs).process(args_for("backup", source), list(rest))


def run_restore(cfgs, rest, source=None):
    return restore_cmd.CommandHandler(cfgs).process(args_for("restore", source), list(rest))


def only(manifest, **match):
    got = [e for e in manifest["entries"] if all(e.get(k) == v for k, v in match.items())]
    assert len(got) == 1, got
    return got[0]


# --- backup -------------------------------------------------------------------


def test_one_source_one_cache(cfgs, tmp_path, capsys):
    m = run_backup(cfgs, ["--cache", "datacache", "--label", "one"], source="aat")
    out = tmp_path / "backups" / "one"
    assert [(e["kind"], e["name"], e["cache"], e["rows"]) for e in m["entries"]] == \
        [("cache", "aat", "datacache", 2)]
    assert sorted(os.listdir(out)) == ["aat.datacache.jsonl.gz", "manifest.json"]
    assert bk.read_manifest(str(out))["label"] == "one"
    text = capsys.readouterr().out
    assert "cache aat datacache: 2 rows, jsonl" in text and f"backup in {out}" in text


def test_a_source_without_cache_is_every_cache_it_has(cfgs):
    m = run_backup(cfgs, ["--label", "x"], source="ycba")
    assert sorted(e["cache"] for e in m["entries"]) == ["datacache", "recordcache"]


def test_all_is_every_cache_map_and_index(cfgs, capsys):
    m = run_backup(cfgs, ["--all", "--label", "all"])
    kinds = sorted((e["kind"], e["name"], e.get("cache") or e.get("index") or "") for e in m["entries"])
    assert kinds == [("cache", "aat", "datacache"), ("cache", "merged", "recordcache"),
                     ("cache", "ycba", "datacache"), ("cache", "ycba", "recordcache"),
                     ("index", "aat", "reconcile_label"),
                     ("map", "idmap", ""), ("map", "redirects", "")]
    assert only(m, kind="index")["format"] == bk.LMDB_COPY
    assert only(m, kind="map", name="idmap")["format"] == bk.IDMAP_JSONL
    # the equivalents index was never built: reported, not a failure
    assert "index aat reconcile_id: not built" in capsys.readouterr().out


def test_portable_and_verify(cfgs, capsys):
    m = run_backup(cfgs, ["--index", "aat", "--portable", "--verify", "--label", "p"])
    assert only(m, kind="index")["format"] == bk.JSONL
    assert "verify:" not in capsys.readouterr().out


def test_unknown_names_back_up_nothing(cfgs, tmp_path):
    with pytest.raises(ValueError, match="Unknown source: nope"):
        run_backup(cfgs, ["--label", "x"], source="nope")
    with pytest.raises(ValueError, match="Unknown map: nope"):
        run_backup(cfgs, ["--map", "nope", "--label", "x"])
    with pytest.raises(ValueError, match="Nothing to back up"):
        run_backup(cfgs, [])
    assert not (tmp_path / "backups").exists()


def test_an_existing_label_is_refused(cfgs):
    run_backup(cfgs, ["--map", "redirects", "--label", "same"])
    with pytest.raises(ValueError, match="already holds a backup"):
        run_backup(cfgs, ["--map", "redirects", "--label", "same"])


def test_a_refused_store_is_reported_and_the_rest_backed_up(cfgs, tmp_path, capsys):
    from lux_pipeline.storage.idmap.subsidiary import IdMap as Sub
    sub = object.__new__(Sub)
    sub.master_name = "idmap"
    cfgs.map_stores["sub"] = {"name": "sub", "store": sub}
    with pytest.raises(RuntimeError, match="1 of 2 stores not backed up"):
        run_backup(cfgs, ["--map", "sub,redirects", "--label", "r"])
    m = bk.read_manifest(str(tmp_path / "backups" / "r"))
    assert [e["name"] for e in m["entries"]] == ["redirects"]
    assert "map sub: not backed up: a subsidiary idmap" in capsys.readouterr().out


def test_not_enough_space_writes_nothing(cfgs, tmp_path, monkeypatch):
    class Big(FsMap):
        def backup_size(self):
            return 10**15
    big = Big({"base_dir": str(tmp_path / "maps"), "name": "big", "all_configs": None})
    cfgs.map_stores["big"] = {"name": "big", "store": big}
    with pytest.raises(RuntimeError, match="the stores need about 1000000.0 GB"):
        run_backup(cfgs, ["--map", "big", "--label", "s"])
    assert not (tmp_path / "backups" / "s").exists()


def test_both_commands_refuse_while_building(cfgs, monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(backup_cmd, "refuse_while_building",
                        lambda c, cmd, force: calls.append((cmd, force)))
    monkeypatch.setattr(restore_cmd, "refuse_while_building",
                        lambda c, cmd, force: calls.append((cmd, force)))
    run_backup(cfgs, ["--map", "redirects", "--label", "b", "--force"])
    run_restore(cfgs, [str(tmp_path / "backups" / "b"), "--force"])
    assert calls == [("backup", True), ("restore", True)]


# --- restore ------------------------------------------------------------------


def test_list_shows_the_manifest(cfgs, tmp_path, capsys):
    run_backup(cfgs, ["--map", "redirects", "--label", "l"])
    capsys.readouterr()
    run_restore(cfgs, [str(tmp_path / "backups" / "l"), "--list"])
    out = capsys.readouterr().out
    assert "backup l in" in out and "map redirects" in out and "jsonl" in out


def test_no_manifest_is_refused(cfgs, tmp_path):
    (tmp_path / "half").mkdir()
    with pytest.raises(ValueError, match="did not finish"):
        run_restore(cfgs, [str(tmp_path / "half")])


def test_the_idmap_needs_force_even_when_empty(cfgs, tmp_path):
    run_backup(cfgs, ["--map", "idmap", "--label", "i"])
    idmap = cfgs.map_stores["idmap"]["store"]
    before = dict(idmap.data)
    idmap.data = {}
    with pytest.raises(ValueError, match="refusing to replace the idmap without --force"):
        run_restore(cfgs, [str(tmp_path / "backups" / "i")])
    assert idmap.data == {}
    run_restore(cfgs, [str(tmp_path / "backups" / "i"), "--force"])
    assert idmap.data == before


def test_a_store_that_is_not_empty_needs_force(cfgs, tmp_path):
    run_backup(cfgs, ["--cache", "datacache", "--label", "c"], source="aat")
    dc = cfgs.external["aat"]["datacache"]
    dc.set({"id": "new"}, identifier="999")
    with pytest.raises(ValueError, match="cache aat datacache: not empty; --force replaces it"):
        run_restore(cfgs, [str(tmp_path / "backups" / "c")])
    assert "999" in dc
    dc.clear()
    run_restore(cfgs, [str(tmp_path / "backups" / "c")])
    assert sorted(dc.iter_keys()) == ["300001", "300002"]


def test_selecting_what_the_backup_does_not_hold(cfgs, tmp_path):
    run_backup(cfgs, ["--map", "redirects", "--label", "s"])
    with pytest.raises(ValueError, match="the backup has no map idmap"):
        run_restore(cfgs, [str(tmp_path / "backups" / "s"), "--map", "idmap", "--force"])
    with pytest.raises(ValueError, match="the backup has no caches for --source aat"):
        run_restore(cfgs, [str(tmp_path / "backups" / "s"), "--force"], source="aat")


def test_a_native_entry_into_another_class_is_refused(cfgs, tmp_path):
    run_backup(cfgs, ["--index", "aat", "--label", "n"])
    path = tmp_path / "backups" / "n" / bk.MANIFEST
    m = json.loads(path.read_text())
    m["entries"][0]["store_class"] = "storage.idmap.lmdb.JsonLmdb"
    path.write_text(json.dumps(m))
    with pytest.raises(ValueError, match="restore it from a portable backup"):
        run_restore(cfgs, [str(tmp_path / "backups" / "n"), "--force"])


def test_round_trip_of_everything(cfgs, tmp_path, capsys):
    run_backup(cfgs, ["--all", "--label", "rt"])
    snapshot = {
        "aat": sorted(cfgs.external["aat"]["datacache"].iter_keys()),
        "merged": sorted(cfgs.results["merged"]["recordcache"].iter_keys()),
        "idmap": dict(cfgs.map_stores["idmap"]["store"].data),
        "redirects": dict(cfgs.map_stores["redirects"]["store"].data),
    }
    for g in ("internal", "external", "results"):
        for cfg in getattr(cfgs, g).values():
            for c in ("datacache", "recordcache"):
                if cfg.get(c) is not None:
                    cfg[c].clear()
    cfgs.map_stores["idmap"]["store"].data = {}
    cfgs.map_stores["redirects"]["store"].clear()
    path = cfgs.external["aat"]["reconcileDbPath"]
    idx = TabLmdb.open(path, "w")
    idx.clear()
    idx.close()

    run_restore(cfgs, [str(tmp_path / "backups" / "rt"), "--force"])
    assert sorted(cfgs.external["aat"]["datacache"].iter_keys()) == snapshot["aat"]
    assert sorted(cfgs.results["merged"]["recordcache"].iter_keys()) == snapshot["merged"]
    assert cfgs.map_stores["idmap"]["store"].data == snapshot["idmap"]
    assert cfgs.map_stores["redirects"]["store"].data == snapshot["redirects"]
    idx = TabLmdb.open(path, "r")
    assert dict(idx.items()) == {"label one": ["http://x/1", "Type"], "label two": "http://x/2"}
    idx.close()
    assert "index aat reconcile_label: 2 rows restored (lmdb-copy)" in capsys.readouterr().out


def test_commands_are_listed_by_help():
    from lux_pipeline.cli.help import list_commands
    names = dict(list_commands())
    assert names["backup"].startswith("Back up caches, maps and reconcile indexes")
    assert names["restore"] == "Restore caches, maps and reconcile indexes from a `lux backup`."
