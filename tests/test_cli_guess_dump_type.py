"""`lux guess-dump-type`: the printed block, pasted into a config, loads the
directory it describes; unplaceable files exit 1; --source resolves the
dumps directory the way the loader does; nothing is written."""

import json
import os
import sqlite3
import sys
import zipfile
from argparse import Namespace
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from lux_pipeline import runtime
from lux_pipeline.cli import entry
from lux_pipeline.cli.guess_dump_type import CommandHandler
from lux_pipeline.process.base.loader import Loader, Pointer

BASE = ROOT / "tests" / "loader_tests"


class StubConfigs:
    def __init__(self, dumps_dir, internal=None, external=None):
        self.dumps_dir = str(dumps_dir)
        self.temp_dir = str(dumps_dir)
        self.internal = internal or {}
        self.external = external or {}
        self.results = {}


class JsonMembersLoader(Loader):
    """What the ignored-member note asks for (as the ROR loader does)."""

    def should_process_item(self, child):
        return not isinstance(child, Pointer) or child.get_name().endswith(".json")


def run(rest=(), source=None, cfgs=None):
    args = Namespace(command="guess-dump-type", source=source)
    return CommandHandler(cfgs).process(args, list(rest))


def block_of(stdout):
    """The printed fragment as the dict a config would hold."""
    return json.loads("{" + stdout + "}")["input_files"]


def snapshot(path):
    return {str(p): p.stat().st_mtime_ns for p in Path(path).rglob("*")}


def make_dumps(tmp_path):
    d = tmp_path / "dumps"
    d.mkdir()
    (d / "a.jsonl").write_text('{"id": "1"}\n{"id": "2"}\n')
    (d / "b.json").write_text('[\n{"id": "3"},\n{"id": "4"}\n]\n')
    with zipfile.ZipFile(d / "c.zip", "w") as zf:
        zf.writestr("c.json", json.dumps({"x": {"id": "5"}, "y": {"id": "6"}}))
        zf.writestr("c.csv", "id\n5\n6\n")
    (d / "recs").mkdir()
    (d / "recs" / "7.json").write_text(json.dumps({"id": "7"}))
    return d


def test_round_trip_the_printed_block_loads_the_directory(tmp_path, capsys):
    d = make_dumps(tmp_path)
    run(["--path", str(d)])
    out = capsys.readouterr()
    assert out.out.startswith('"input_files": {')
    block = block_of(out.out)
    assert block == {"records": [
        {"path": "a.jsonl", "type": "lines/json"},
        {"path": "b.json", "type": "arraylines/json"},
        {"path": "c.zip", "type": "zip/dict/raw"},
        {"path": "recs", "type": "dir/json"},
    ]}
    assert "c.zip: ignored member c.csv" in out.err
    assert "should_process_item" in out.err

    # pasted into a config: bare paths resolve against the dumps directory
    ldr = JsonMembersLoader({"name": "rt", "all_configs": StubConfigs(d), "datacache": {},
                  "input_files": block})
    ldr.prepare(0, 1)
    ldr.process()
    assert sorted(ldr.out_cache) == ["1", "2", "3", "4", "5", "6", "7"]


def test_round_trip_every_checked_in_fixture_directory(capsys):
    for sub in sorted(p for p in BASE.iterdir() if p.is_dir()):
        run(["--path", str(sub)])
        block = block_of(capsys.readouterr().out)
        ldr = Loader({"name": sub.name, "all_configs": StubConfigs(sub), "datacache": {},
                      "input_files": block})
        ldr.prepare(0, 1)
        ldr.process()
        assert len(ldr.out_cache) >= 1, sub.name
        assert not ldr.failed_files, (sub.name, ldr.failed_files)


def test_unplaceable_files_go_to_stderr_and_exit_1(tmp_path, capsys):
    d = make_dumps(tmp_path)
    con = sqlite3.connect(d / "latest_wof.db")
    con.execute("create table t (x)")
    con.commit()
    con.close()
    (d / "names.tsv").write_text("id\tname\n")
    with pytest.raises(SystemExit) as e:
        run(["--path", str(d)])
    assert e.value.code == 1
    out = capsys.readouterr()
    assert "latest_wof.db: cannot place: SQLite database" in out.err
    assert "names.tsv: cannot place: delimited" in out.err
    # the placeable files are still printed
    assert len(block_of(out.out)["records"]) == 4


def test_type_and_explain(tmp_path, capsys):
    d = make_dumps(tmp_path)
    run(["--path", str(d / "a.jsonl"), "--type", "export", "--explain"])
    out = capsys.readouterr()
    assert block_of(out.out) == {"export": [{"path": "a.jsonl", "type": "lines/json"}]}
    line = next(x for x in out.err.splitlines() if x.startswith("a.jsonl: lines/json"))
    assert "magic=7b 22" in line and "head=" in line and "keys=['id']" in line


def test_nothing_is_written(tmp_path, capsys):
    d = make_dumps(tmp_path)
    before = snapshot(tmp_path)
    run(["--path", str(d)])
    cfgs = StubConfigs(d.parent, external={"src": {"name": "src", "dumps_dir": "dumps"}})
    run([], source="src", cfgs=cfgs)
    assert snapshot(tmp_path) == before


def test_needs_one_of_source_or_path(capsys):
    with pytest.raises(SystemExit) as e:
        run([])
    assert e.value.code == 2
    with pytest.raises(SystemExit):
        run(["--path", "x"], source="ror", cfgs=StubConfigs("."))
    with pytest.raises(SystemExit) as e:
        run(["--path", "/no/such/dir"])
    assert e.value.code == 2


# --- --source ----------------------------------------------------------------


def test_source_uses_its_own_dumps_dir(tmp_path, capsys):
    d = make_dumps(tmp_path)
    cfgs = StubConfigs(tmp_path, external={"src": {"name": "src", "dumps_dir": "dumps"}})
    run([], source="src", cfgs=cfgs)
    block = block_of(capsys.readouterr().out)
    assert [e["path"] for e in block["records"]] == ["a.jsonl", "b.json", "c.zip", "recs"]
    # and the loader resolves them against the same directory
    ldr = Loader({"name": "src", "dumps_dir": "dumps", "all_configs": cfgs, "datacache": {},
                  "input_files": block})
    ldr.prepare(0, 1)
    assert ldr.my_files[0]["path"] == str(d / "a.jsonl")


def test_source_falls_back_to_the_dump_file_path_directory(tmp_path, capsys):
    make_dumps(tmp_path)
    cfgs = StubConfigs(tmp_path / "elsewhere",
                       internal={"u": {"name": "u", "dumpFilePath": str(tmp_path / "dumps" / "a.jsonl")}})
    run([], source="u", cfgs=cfgs)
    block = block_of(capsys.readouterr().out)
    # outside the loader's directory, so absolute
    assert block["records"][0] == {"path": str(tmp_path / "dumps" / "a.jsonl"), "type": "lines/json"}


def test_source_in_the_shared_dumps_dir_scans_only_its_own_file(tmp_path, capsys):
    d = make_dumps(tmp_path)
    cfgs = StubConfigs(d, external={"s": {"name": "s", "dumpFilePath": "b.json"}})
    run([], source="s", cfgs=cfgs)
    assert block_of(capsys.readouterr().out) == {
        "records": [{"path": "b.json", "type": "arraylines/json"}]}


def test_all_sources_print_an_object_keyed_by_source_and_skip_the_rest(tmp_path, capsys):
    make_dumps(tmp_path)
    (tmp_path / "other").mkdir()
    (tmp_path / "other" / "r.json").write_text(json.dumps({"id": "1"}))
    cfgs = StubConfigs(tmp_path,
                       internal={"one": {"name": "one", "dumps_dir": "dumps"}},
                       external={"two": {"name": "two", "dumps_dir": "other"},
                                 "none": {"name": "none"},
                                 "gone": {"name": "gone", "dumps_dir": "missing"}})
    run([], source="all", cfgs=cfgs)
    out = capsys.readouterr()
    data = json.loads(out.out)
    assert list(data) == ["one", "two"]
    assert data["two"] == {"input_files": {"records": [{"path": "r.json", "type": "json"}]}}
    assert "none: no dumps directory" in out.err
    assert "gone: no dumps directory" in out.err


def test_unknown_source_raises(tmp_path):
    with pytest.raises(ValueError, match="Unknown source: nope"):
        run([], source="nope", cfgs=StubConfigs(tmp_path))


# --- through the lux entry point ---------------------------------------------


def test_path_runs_without_loading_a_configuration(tmp_path, capsys, monkeypatch):
    def no_configs():
        raise AssertionError("--path must not load the configuration")

    monkeypatch.setattr(runtime, "get_configs", no_configs)
    d = make_dumps(tmp_path)
    assert entry.run(["guess-dump-type", "--path", str(d)]) == 0
    assert len(block_of(capsys.readouterr().out)["records"]) == 4
    (d / "x.tsv").write_text("a\tb\n")
    with pytest.raises(SystemExit) as e:
        entry.run(["guess-dump-type", "--path", str(d)])
    assert e.value.code == 1


def test_source_loads_the_configuration(tmp_path, capsys, monkeypatch):
    make_dumps(tmp_path)
    cfgs = StubConfigs(tmp_path, external={"src": {"name": "src", "dumps_dir": "dumps"}})
    monkeypatch.setattr(runtime, "get_configs", lambda: cfgs)
    assert entry.run(["guess-dump-type", "--source", "src"]) == 0
    assert len(block_of(capsys.readouterr().out)["records"]) == 4
