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


from lux_pipeline.cli import _services


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    """initialize fetches the Linked Art schemas and probes the servers;
    answer with a tiny schema and reachable servers."""
    class Resp:
        text = '{"$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object"}'

        def raise_for_status(self):
            pass

    monkeypatch.setattr(initialize.requests, "get", lambda url, **kw: Resp())
    monkeypatch.setattr(_services, "probe_postgres", lambda s, timeout=5: f"PostgreSQL 16 at {_services.describe_postgres(s)}")
    monkeypatch.setattr(_services, "probe_redis", lambda h, p, timeout=5: f"redis 7 at {h}:{p}")
    # the database offer: not a tty, so nothing is asked or created unless a test says so
    monkeypatch.setattr(_services, "postgres_database_status", lambda s, name, timeout=5: (False, True))
    monkeypatch.setattr(initialize.sys.stdin, "isatty", lambda: False, raising=False)


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
    assert f"Installation at {root}" in out and "lux testinstall" in out
    assert "schemas: 0 present, 11 fetched" in out
    assert ".env: written" in out and "directories: 7 created, 0 existing" in out
    user = os.getenv("USER")
    assert f"postgres: OK: PostgreSQL 16 at local socket, database {user}, user {user}" in out
    assert "redis: OK: redis 7 at localhost:6379" in out
    caches = json.load(open(cc / "caches.json"))
    assert (caches["user"], caches["password"], caches["dbname"]) == (user, "", user)
    assert "database: skipped (rerun with --create-db to create 'lux')" in out
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
    assert "0 present, 10 fetched" in out and "FAILED: person.json" in out
    assert not (root / "data" / "files" / "schema" / "person.json").exists()


def test_unreachable_services_are_reported_not_fatal(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(_services, "probe_postgres", lambda s, timeout=5: (_ for _ in ()).throw(OSError("connection refused")))
    monkeypatch.setattr(_services, "probe_redis", lambda h, p, timeout=5: (_ for _ in ()).throw(ConnectionError("no route")))
    root = tmp_path / "inst"
    assert init([str(root)]) == root
    out = capsys.readouterr().out
    assert "postgres: NOT REACHABLE: local socket" in out and "OSError: connection refused" in out
    assert "redis: NOT REACHABLE: localhost:6379: ConnectionError: no route" in out
    assert "start or configure postgres and redis" in out


class RecordingIdmap(dict):
    pass


@pytest.fixture
def db_setup(monkeypatch):
    """Stub the parts that touch Postgres: the create and the idmap build."""
    created = []
    monkeypatch.setattr(_services, "create_database", lambda s, name, timeout=5: created.append((s["dbname"], name)) or True)
    monkeypatch.setattr(Config, "get_idmap", lambda self: RecordingIdmap())
    return created


def test_create_db_configures_the_database_and_an_empty_idmap(tmp_path, monkeypatch, capsys, db_setup):
    root = tmp_path / "inst"
    init([str(root), "--create-db"])
    out = capsys.readouterr().out
    assert db_setup == [(os.getenv("USER"), "lux")]
    assert "database: created 'lux'" in out
    cc = root / "config_cache"
    assert json.load(open(cc / "caches.json"))["dbname"] == "lux"
    assert json.load(open(cc / "map_idmap.json"))["storeClass"] == "storage.idmap.postgres.IdMap"
    token = (root / "data" / "files" / "idmap_update_token.txt").read_text().strip()
    assert len(token) == 12 and token.startswith("__") and token.endswith("__") and token[2:10].isdigit()
    assert "idmap tables: created in 'lux' (RecordingIdmap, 0 entries)" in out
    assert "update token:" in out and "idmap: map_idmap.json -> storage.idmap.postgres.IdMap" in out


def test_prompt_is_asked_at_a_tty_and_a_no_skips(tmp_path, monkeypatch, capsys, db_setup):
    monkeypatch.setattr(initialize.sys.stdin, "isatty", lambda: True, raising=False)
    asked = []
    monkeypatch.setattr("builtins.input", lambda q: asked.append(q) or "n")
    root = tmp_path / "a"
    init([str(root)])
    assert asked == ["Create a Postgres database 'lux' for the pipeline and set up an empty idmap in it? [y/N] "]
    assert db_setup == [] and json.load(open(root / "config_cache" / "map_idmap.json"))["storeClass"].endswith("redis.IdMap")
    assert "database: skipped" in capsys.readouterr().out
    monkeypatch.setattr("builtins.input", lambda q: "yes")
    init([str(tmp_path / "b"), "--dbname", "luxtest"])
    assert db_setup == [(os.getenv("USER"), "luxtest")]
    assert json.load(open(tmp_path / "b" / "config_cache" / "caches.json"))["dbname"] == "luxtest"


def test_existing_database_is_adopted_and_no_create_db_never_asks(tmp_path, monkeypatch, capsys, db_setup):
    monkeypatch.setattr(_services, "postgres_database_status", lambda s, name, timeout=5: (True, False))
    init([str(tmp_path / "a"), "--create-db"])
    out = capsys.readouterr().out
    assert "database: using existing 'lux'" in out and db_setup == []
    assert json.load(open(tmp_path / "a" / "config_cache" / "caches.json"))["dbname"] == "lux"
    monkeypatch.setattr(_services, "postgres_database_status", lambda s, name, timeout=5: (False, False))
    init([str(tmp_path / "b"), "--create-db"])
    assert "may not create databases" in capsys.readouterr().out
    monkeypatch.setattr(_services, "postgres_database_status", lambda s, name, timeout=5: (_ for _ in ()).throw(AssertionError("should not be called")))
    init([str(tmp_path / "c"), "--no-create-db"])
    assert "database:" not in capsys.readouterr().out


def test_create_db_in_a_checkout_edits_the_sub_store_map_record(tmp_path, monkeypatch, capsys, db_setup):
    src = fake_checkout(tmp_path / "checkout")
    cc = src / "configs" / "system" / "config_cache"
    json.dump({"name": "cache", "type": "default", "host": "", "port": 5432, "user": "", "password": "", "dbname": ""},
              open(cc / "default_cache.json", "w"))
    json.dump({"name": "maps", "type": "config", "base_dir": "pipeline-configs/configs/subs",
               "datacacheClass": "storage.cache.filesystem.FsCache", "tabletype": "cache"}, open(cc / "maps.json", "w"))
    mc = src / "configs" / "subs" / "maps_cache"
    mc.mkdir()
    json.dump({"name": "idmap", "type": "map", "storeClass": "storage.idmap.redis.IdMap", "db": 0}, open(mc / "map_idmap.json", "w"))
    root = tmp_path / "inst"
    bp = init([str(root), "--configs", str(src), "--create-db"])
    assert db_setup == [(os.getenv("USER"), "lux")]
    assert json.load(open(bp / "config_cache" / "default_cache.json"))["dbname"] == "lux"
    installed_map = root / "pipeline-configs" / "configs" / "subs" / "maps_cache" / "map_idmap.json"
    assert json.load(open(installed_map))["storeClass"] == "storage.idmap.postgres.IdMap"
    assert json.load(open(mc / "map_idmap.json"))["storeClass"].endswith("redis.IdMap")   # the source untouched
    assert (root / "files" / "idmap_update_token.txt").exists()


def test_database_setup_failure_is_reported_not_fatal(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(_services, "create_database", lambda s, name, timeout=5: (_ for _ in ()).throw(OSError("permission denied")))
    root = tmp_path / "inst"
    assert init([str(root), "--create-db"]) == root
    out = capsys.readouterr().out
    assert "database: FAILED: OSError: permission denied" in out and "start or configure the database" in out


def test_services_module_settings():
    s = _services.postgres_settings({"host": "db.example.org", "port": "5433", "user": "USERNAME",
                                     "password": "PASSWORD", "dbname": "lux"})
    assert s == {"host": "db.example.org", "port": 5433, "user": os.getenv("USER"), "password": "", "dbname": "lux"}
    assert _services.describe_postgres(s) == "db.example.org:5433, database lux, user " + os.getenv("USER")
    assert _services.redis_settings({"db": 3}, {"host": "cache", "port": 6380}) == {"host": "cache", "port": 6380, "db": 3}
    maps = [{"storeClass": "storage.idmap.redis.IdMap", "host": "a"}, {"storeClass": "storage.idmap.postgres.IdMap"},
            {"storeClass": "storage.idmap.redis.MultiMap", "host": "a", "db": 8}, {"host": "b"}]
    assert _services.redis_targets(maps, {"port": 6379}) == [("a", 6379)]
    assert _services.redis_targets(maps[3:], {"storeClass": "storage.idmap.redis.IdMap"}) == [("b", 6379)]
    assert _services.redis_targets([{"storeClass": "storage.idmap.postgres.IdMap"}]) == []


def test_rerun_keeps_everything_and_reports_it(tmp_path, capsys):
    root = tmp_path / "inst"
    init([str(root)])
    cc = root / "config_cache"
    # the user edited a record and a stray sample vanished; a directory was removed; .env too
    caches = json.load(open(cc / "caches.json"))
    caches["dbname"] = "mine"
    json.dump(caches, open(cc / "caches.json", "w"))
    (cc / "ror.json").unlink()
    (root / "data" / "input").rmdir()
    (root / ".env").unlink()
    capsys.readouterr()
    init([str(root)])
    out = capsys.readouterr().out
    assert "configuration:" in out and "exists; added the missing samples ror.json" in out
    assert json.load(open(cc / "caches.json"))["dbname"] == "mine"           # the edit survives
    assert (cc / "ror.json").exists() and (root / "data" / "input").is_dir()
    assert "directories: 1 created, 6 existing (data/input)" in out
    assert ".env: written" in out and (root / ".env").read_text() == f"LUX_BASEPATH={root}\n"
    assert "schemas: 11 present, 0 fetched" in out
    capsys.readouterr()
    init([str(root)])
    out = capsys.readouterr().out
    assert "exists, kept" in out and ".env: kept" in out and "directories: 0 created, 7 existing" in out
    assert "base_dir:" not in out and "placeholders" not in out


def test_rerun_after_services_were_down_completes_the_database(tmp_path, monkeypatch, capsys, db_setup):
    """The case Rob raised: Postgres was not up the first time."""
    monkeypatch.setattr(_services, "probe_postgres", lambda s, timeout=5: (_ for _ in ()).throw(OSError("connection refused")))
    root = tmp_path / "inst"
    init([str(root), "--create-db"])
    out = capsys.readouterr().out
    assert "postgres: NOT REACHABLE" in out and "run lux initialize" in out and "carries on from here" in out
    assert db_setup == [] and json.load(open(root / "config_cache" / "caches.json"))["dbname"] != "lux"
    # Postgres is up now
    monkeypatch.setattr(_services, "probe_postgres", lambda s, timeout=5: "PostgreSQL 16")
    init([str(root), "--create-db"])
    out = capsys.readouterr().out
    assert "configuration:" in out and "exists, kept" in out
    assert db_setup == [(os.getenv("USER"), "lux")] and "database: created 'lux'" in out
    assert json.load(open(root / "config_cache" / "caches.json"))["dbname"] == "lux"
    assert json.load(open(root / "config_cache" / "map_idmap.json"))["storeClass"] == "storage.idmap.postgres.IdMap"
    # and a third run only verifies
    monkeypatch.setattr(_services, "postgres_database_status", lambda s, name, timeout=5: (True, True))
    init([str(root), "--create-db"])
    out = capsys.readouterr().out
    assert "database: 'lux' already configured" in out and "idmap tables: verified in 'lux'" in out
    assert "cache defaults:" not in out and "idmap: map_idmap.json" not in out
    assert db_setup == [(os.getenv("USER"), "lux")]


def test_already_configured_database_asks_nothing(tmp_path, monkeypatch, capsys, db_setup):
    root = tmp_path / "inst"
    init([str(root), "--create-db"])
    monkeypatch.setattr(_services, "postgres_database_status", lambda s, name, timeout=5: (True, True))
    monkeypatch.setattr(initialize.sys.stdin, "isatty", lambda: True, raising=False)
    monkeypatch.setattr("builtins.input", lambda q: (_ for _ in ()).throw(AssertionError("asked")))
    capsys.readouterr()
    init([str(root)])
    assert "database: 'lux' already configured" in capsys.readouterr().out


def test_partial_layout_is_completed(tmp_path, capsys):
    """config_cache was created by hand (or a crashed run): no .env, no dirs."""
    root = tmp_path / "inst"
    cc = root / "config_cache"
    cc.mkdir(parents=True)
    for fn in initialize.SAMPLES.glob("*.json"):
        (cc / fn.name).write_text(fn.read_text())
    init([str(root)])
    out = capsys.readouterr().out
    assert "exists, kept" in out and f"base_dir: set to {root}" in out
    assert "directories: 7 created" in out and ".env: written" in out
    assert json.load(open(cc / "base.json"))["base_dir"] == str(root)


def test_dotenv_pointing_at_another_valid_install_is_kept_a_stale_one_rewritten(tmp_path, capsys):
    other = tmp_path / "other"
    init([str(other)])
    root = tmp_path / "inst"
    root.mkdir()
    (root / ".env").write_text(f"LUX_BASEPATH={other}\n")
    capsys.readouterr()
    init([str(root)])
    out = capsys.readouterr().out
    assert f".env: kept; it points at {other}, not {root}" in out
    assert (root / ".env").read_text() == f"LUX_BASEPATH={other}\n"
    (root / ".env").write_text("LUX_BASEPATH=/nowhere\n")
    init([str(root)])
    assert ".env: rewritten (was LUX_BASEPATH=/nowhere" in capsys.readouterr().out
    assert (root / ".env").read_text() == f"LUX_BASEPATH={root}\n"


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
    # a rerun, with the flag or without, finds the installed checkout and keeps an edit in it
    rec = json.load(open(bp / "config_cache" / "system.json"))
    rec["max_workers"] = 3
    json.dump(rec, open(bp / "config_cache" / "system.json", "w"))
    assert init([str(root), "--configs", str(src)]) == bp
    assert init([str(root)]) == bp
    assert json.load(open(bp / "config_cache" / "system.json"))["max_workers"] == 3


def test_configs_accepts_the_configs_dir_itself_and_rejects_other_dirs(tmp_path):
    src = fake_checkout(tmp_path / "checkout")
    bp = init([str(tmp_path / "a"), "--configs", str(src / "configs")])
    assert (bp / "config_cache" / "system.json").exists()
    with pytest.raises(ValueError, match="not a pipeline-configs checkout"):
        init([str(tmp_path / "b"), "--configs", str(tmp_path)])
    # a checkout already at <root>/pipeline-configs is used in place
    root = tmp_path / "c"
    fake_checkout(root / "pipeline-configs")
    bp = init([str(root), "--configs", str(root / "pipeline-configs")])
    assert bp == root / "pipeline-configs" / "configs" / "system"
    assert json.load(open(bp / "config_cache" / "sources.json"))["base_dir"] == "../subs"


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
    # a rerun does not clone again
    assert init([str(root), "--clone"]) == bp
    assert len(calls) == 1


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


def test_testinstall_reports_postgres_as_the_first_failure_without_one(tmp_path, monkeypatch, capsys):
    """The acceptance case: a fresh install on a machine without Postgres."""
    root = tmp_path / "inst"
    init([str(root)])
    capsys.readouterr()
    monkeypatch.delenv("LUX_BASEPATH", raising=False)
    monkeypatch.setattr(_services, "probe_postgres", lambda s, timeout=5: (_ for _ in ()).throw(OSError("connection refused")))
    with pytest.raises(SystemExit):
        run_testinstall(cwd=root, monkeypatch=monkeypatch)
    lines = capsys.readouterr().out.splitlines()
    assert [l[:4] for l in lines[:5]] == ["OK  ", "OK  ", "OK  ", "OK  ", "FAIL"]
    assert lines[4].startswith("FAIL  postgres reachable: OSError: connection refused")
    assert "max_workers" in lines[3]


def test_testinstall_reports_an_unreachable_idmap_after_the_servers(tmp_path, monkeypatch, capsys):
    root = tmp_path / "inst"
    init([str(root)])
    capsys.readouterr()
    monkeypatch.delenv("LUX_BASEPATH", raising=False)
    monkeypatch.setattr(Config, "get_idmap", lambda self: (_ for _ in ()).throw(ConnectionError("no idmap")))
    with pytest.raises(SystemExit):
        run_testinstall(cwd=root, monkeypatch=monkeypatch)
    lines = capsys.readouterr().out.splitlines()
    assert lines[4].startswith("OK    postgres reachable: PostgreSQL 16")
    assert lines[5].startswith("OK    redis reachable: redis 7 at localhost:6379")
    assert lines[6].startswith("FAIL  idmap reachable: ConnectionError: no idmap")


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
    assert lines[4].startswith("OK    postgres reachable") and lines[5].startswith("OK    redis reachable")
    assert "1 entries" in lines[6] and "1 globals" in lines[7] and "~42 records" in lines[8]
    assert lines[10].startswith("OK    validator schemas present: 11 schemas")
    assert "2 workers spawned and reported" in lines[11]
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
    assert names["initialize"].startswith("Create or complete an installation")
    assert names["testinstall"].startswith("Check an installation step by step")
    assert initialize.CommandHandler.needs_configs is False
    assert testinstall.CommandHandler.needs_configs is False
