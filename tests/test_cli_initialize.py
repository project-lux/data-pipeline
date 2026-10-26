"""`lux initialize` and `lux testinstall`: the layout, the configuration
copies, the .env, the checkout mode, and the step-by-step checks."""

import json
import os
import sys
from argparse import Namespace
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from lux_pipeline import runtime
from lux_pipeline.cli import initialize, testinstall
from lux_pipeline.config import Config

PIPELINE_CONFIGS = ROOT.parent / "pipeline-configs" / "configs"


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    """initialize fetches the Linked Art schemas; answer with a tiny one."""
    class Resp:
        text = '{"$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object"}'

        def raise_for_status(self):
            pass

    monkeypatch.setattr(initialize.requests, "get", lambda url, **kw: Resp())


def init(rest):
    return initialize.CommandHandler(None).process(Namespace(command="initialize"), list(rest))


def test_samples_layout_config_and_dotenv(tmp_path, capsys):
    root = tmp_path / "inst"
    bp = init([str(root)])
    assert bp == root
    cc = root / "config_cache"
    assert {p.name for p in cc.iterdir()} == {p.name for p in initialize.SAMPLES.glob("*.json")}
    assert not (cc / "example-dotenv.txt").exists()
    base = json.load(open(cc / "base.json"))
    assert base["base_dir"] == str(root)
    for key in ("data_dir", "dumps_dir", "exports_dir", "indexes_dir", "log_dir", "temp_dir", "tests_dir"):
        assert (root / base[key]).is_dir(), key
    assert (root / base["log_dir"] / "flags").is_dir()
    assert (root / ".env").read_text() == f"LUX_BASEPATH={root}\n"
    out = capsys.readouterr().out
    assert f"Installed at {root}" in out and "lux testinstall" in out
    assert "schemas: 11 fetched" in out
    assert sorted(p.name for p in (root / base["data_dir"] / "schema").iterdir()) == sorted(
        ["core.json", "digital.json", "group.json", "image.json", "object.json", "person.json",
         "place.json", "set.json", "text.json", "concept.json", "event.json"])
    # and the result is a configuration main can read
    cfgs = Config(basepath=str(root))
    assert "ror" in cfgs.external and cfgs.base_dir == str(root)
    assert cfgs.dumps_dir == str(root / "data" / "input")


def test_no_schemas_and_fetch_failures(tmp_path, monkeypatch, capsys):
    root = tmp_path / "a"
    init([str(root), "--no-schemas"])
    assert not (root / "data" / "files" / "schema").exists()
    assert "schemas:" not in capsys.readouterr().out

    def flaky(url, **kw):
        if url.endswith("person.json"):
            raise ConnectionError("down")
        return type("R", (), {"text": '{"$schema": "https://json-schema.org/draft/2020-12/schema"}',
                              "raise_for_status": lambda self: None})()

    monkeypatch.setattr(initialize.requests, "get", flaky)
    root = tmp_path / "b"
    init([str(root)])
    out = capsys.readouterr().out
    assert "schemas: 10 fetched" in out and "FAILED: person.json" in out
    assert not (root / "data" / "files" / "schema" / "person.json").exists()


def test_refuses_an_existing_installation(tmp_path):
    root = tmp_path / "inst"
    init([str(root)])
    with pytest.raises(ValueError, match="refusing to overwrite"):
        init([str(root)])
    other = tmp_path / "other"
    (other / "config_cache").mkdir(parents=True)
    with pytest.raises(ValueError, match="config_cache already exists"):
        init([str(other)])


def test_default_directory_is_the_current_one(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert init([]) == tmp_path
    assert (tmp_path / "config_cache" / "base.json").exists()


def fake_checkout(where):
    """A pipeline-configs-shaped tree: system/config_cache with a system
    record and a sub-config store pointing at subs/sources_cache."""
    cc = where / "configs" / "system" / "config_cache"
    cc.mkdir(parents=True)
    json.dump({"type": "system", "base_dir": "path-to-where-files-should-live", "data_dir": "files",
               "dumps_dir": "input", "exports_dir": "output", "indexes_dir": "indexes", "log_dir": "logs",
               "temp_dir": "temp", "tests_dir": "tests", "ok_record_types": {}, "parent_record_types": {},
               "path_types": {}, "idmap_name": "idmap"}, open(cc / "system.json", "w"))
    json.dump({"name": "sources", "type": "config", "base_dir": "pipeline-configs/configs/subs",
               "datacacheClass": "storage.cache.filesystem.FsCache", "tabletype": "cache"},
              open(cc / "sources.json", "w"))
    sc = where / "configs" / "subs" / "sources_cache"
    sc.mkdir(parents=True)
    json.dump({"name": "aat", "type": "external", "namespace": "http://vocab.getty.edu/aat/",
               "matches": ["vocab.getty.edu/aat/"]}, open(sc / "aat.json", "w"))
    return where


def test_configs_checkout_is_copied_and_wired(tmp_path):
    src = fake_checkout(tmp_path / "checkout")
    root = tmp_path / "inst"
    bp = init([str(root), "--configs", str(src)])
    assert bp == root / "pipeline-configs" / "configs" / "system"
    system = json.load(open(bp / "config_cache" / "system.json"))
    assert system["base_dir"] == str(root)
    store = json.load(open(bp / "config_cache" / "sources.json"))
    assert store["base_dir"] == "../subs"
    assert (root / "input").is_dir() and (root / "logs" / "flags").is_dir()
    assert (root / ".env").read_text() == f"LUX_BASEPATH={bp}\n"
    # the source checkout is untouched
    assert json.load(open(src / "configs" / "system" / "config_cache" / "sources.json"))["base_dir"] == "pipeline-configs/configs/subs"
    # and it loads, sub-store included (item 35)
    cfgs = Config(basepath=str(bp))
    assert "aat" in cfgs.external and cfgs.base_dir == str(root)


def test_configs_accepts_the_configs_dir_itself_and_rejects_other_dirs(tmp_path):
    src = fake_checkout(tmp_path / "checkout")
    bp = init([str(tmp_path / "a"), "--configs", str(src / "configs")])
    assert (bp / "config_cache" / "system.json").exists()
    with pytest.raises(ValueError, match="not a pipeline-configs checkout"):
        init([str(tmp_path / "b"), "--configs", str(tmp_path)])


def test_clone_runs_git_and_wires_the_result(tmp_path, monkeypatch):
    calls = []

    def fake_run(cmd, **kw):
        calls.append(cmd)
        fake_checkout(Path(cmd[-1]))
        return type("R", (), {"returncode": 0, "stderr": "", "stdout": ""})()

    monkeypatch.setattr(initialize.subprocess, "run", fake_run)
    root = tmp_path / "inst"
    bp = init([str(root), "--clone"])
    assert calls == [["git", "clone", initialize.PIPELINE_CONFIGS_URL, str(root / "pipeline-configs")]]
    assert bp == root / "pipeline-configs" / "configs" / "system"
    assert json.load(open(bp / "config_cache" / "sources.json"))["base_dir"] == "../subs"


def test_clone_failure_is_reported(tmp_path, monkeypatch):
    monkeypatch.setattr(initialize.subprocess, "run",
                        lambda cmd, **kw: type("R", (), {"returncode": 128, "stderr": "fatal: nope", "stdout": ""})())
    with pytest.raises(RuntimeError, match="git clone .* failed: fatal: nope"):
        init([str(tmp_path / "inst"), "--clone", "https://example.org/x.git"])


@pytest.mark.skipif(not PIPELINE_CONFIGS.is_dir(), reason="pipeline-configs checkout not beside this repo")
def test_the_real_pipeline_configs_checkout_installs_and_loads(tmp_path):
    root = tmp_path / "inst"
    bp = init([str(root), "--configs", str(PIPELINE_CONFIGS.parent)])
    cfgs = Config(basepath=str(bp))
    assert {"ror", "wikidata", "aat"} <= set(cfgs.external) and "ycba" in cfgs.internal
    assert cfgs.base_dir == str(root) and (root / "input").is_dir()


# --- testinstall ---------------------------------------------------------------


def run_testinstall(rest=(), cwd=None, monkeypatch=None):
    if cwd is not None:
        monkeypatch.chdir(cwd)
    runtime.reset_configs()
    return testinstall.CommandHandler(None).process(Namespace(command="testinstall"), list(rest))


def test_testinstall_stops_at_the_first_failure_without_dotenv(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("LUX_BASEPATH", raising=False)
    (tmp_path / "empty").mkdir()
    with pytest.raises(SystemExit) as info:
        run_testinstall(cwd=tmp_path / "empty", monkeypatch=monkeypatch)
    assert info.value.code == 1
    out = capsys.readouterr().out.splitlines()
    assert out[0].startswith("FAIL  .env:") and "Stopped at the first failure" in out[-1]


def test_testinstall_reports_a_broken_configuration(tmp_path, monkeypatch, capsys):
    root = tmp_path / "inst"
    init([str(root)])
    capsys.readouterr()
    (root / "config_cache" / "base.json").write_text("{not json")
    monkeypatch.delenv("LUX_BASEPATH", raising=False)
    with pytest.raises(SystemExit):
        run_testinstall(cwd=root, monkeypatch=monkeypatch)
    lines = capsys.readouterr().out.splitlines()
    assert [l[:4] for l in lines[:4]] == ["OK  ", "OK  ", "OK  ", "FAIL"]
    assert lines[3].startswith("FAIL  configuration builds")


def test_testinstall_reports_the_database_as_the_first_failure_without_one(tmp_path, monkeypatch, capsys):
    """The acceptance case: a fresh install on a machine without Postgres."""
    root = tmp_path / "inst"
    init([str(root)])
    capsys.readouterr()
    monkeypatch.delenv("LUX_BASEPATH", raising=False)
    monkeypatch.setattr(Config, "get_idmap", lambda self: (_ for _ in ()).throw(ConnectionError("connection refused")))
    with pytest.raises(SystemExit):
        run_testinstall(cwd=root, monkeypatch=monkeypatch)
    lines = capsys.readouterr().out.splitlines()
    assert [l[:4] for l in lines[:5]] == ["OK  ", "OK  ", "OK  ", "OK  ", "FAIL"]
    assert lines[4].startswith("FAIL  idmap reachable: ConnectionError: connection refused")
    assert "max_workers" in lines[3]


def test_testinstall_all_ok_with_stubbed_databases_and_real_workers(tmp_path, monkeypatch, capsys):
    root = tmp_path / "inst"
    init([str(root)])
    capsys.readouterr()
    monkeypatch.delenv("LUX_BASEPATH", raising=False)

    class FakeIdmap(dict):
        pass

    class FakeCache:
        def len_estimate(self):
            return 42

    monkeypatch.setattr(Config, "get_idmap", lambda self: FakeIdmap(a=1))
    monkeypatch.setattr(Config, "cache_globals", lambda self, idmap=None: setattr(self, "globals", {"g": 1}))

    def fake_instantiate_all(self):
        for cfg in self.external.values():
            cfg["datacache"] = FakeCache()

    monkeypatch.setattr(Config, "instantiate_all", fake_instantiate_all)
    assert run_testinstall(["--workers", "2"], cwd=root, monkeypatch=monkeypatch) is True
    lines = capsys.readouterr().out.splitlines()
    assert all(l.startswith("OK    ") for l in lines[:-1]) and lines[-1] == "All checks passed."
    assert "1 entries" in lines[4] and "1 globals" in lines[5] and "~42 records" in lines[6]
    assert lines[8].startswith("OK    validator schemas present: 11 schemas")
    assert "2 workers spawned and reported" in lines[9]
    assert len(list((root / "tmp").glob("testinstall-*/spawn-w*.status"))) == 2


def test_testinstall_skip_workers(tmp_path, monkeypatch, capsys):
    root = tmp_path / "inst"
    init([str(root)])
    capsys.readouterr()
    monkeypatch.delenv("LUX_BASEPATH", raising=False)
    monkeypatch.setattr(Config, "get_idmap", lambda self: {})
    monkeypatch.setattr(Config, "cache_globals", lambda self, idmap=None: None)
    monkeypatch.setattr(Config, "instantiate_all", lambda self: None)
    assert run_testinstall(["--skip-workers"], cwd=root, monkeypatch=monkeypatch) is True
    assert "worker processes" not in capsys.readouterr().out


def test_both_commands_are_listed_and_need_no_config():
    from lux_pipeline.cli.help import list_commands
    names = dict(list_commands())
    assert names["initialize"].startswith("Create a new installation")
    assert names["testinstall"].startswith("Check an installation step by step")
    assert initialize.CommandHandler.needs_configs is False
    assert testinstall.CommandHandler.needs_configs is False
