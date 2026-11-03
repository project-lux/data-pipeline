"""A source's name starts its Postgres table names, and a map without a
``tableName`` is a table of its name, written into SQL unquoted: a name
Postgres cannot read that way (``my-source``) stops the configuration
loading, says which record and file, and `lux testinstall`'s
"configuration builds" fails on it. The record validator (``lux config
check``, the web editor) refuses one before it is written."""

import json
import sys
from argparse import Namespace
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline import config_store
from lux_pipeline.cli import testinstall
from lux_pipeline.config import MAX_STORE_NAME, Config, store_name_problem
from webui_fixtures import make_configs, make_installation, use_installation


@pytest.mark.parametrize("name", ["aat", "getty_museum", "_x", "lcnaf2", "a" * MAX_STORE_NAME])
def test_names_postgres_reads_unquoted(name):
    assert store_name_problem(name) is None


@pytest.mark.parametrize("name, says", [
    ("my-source", "'-'"),
    ("My_source", "'M'"),
    ("two words", "' '"),
    ("café", "'é'"),
    ("9lives", "starts with a digit"),
    ("", "missing"),
    (None, "missing"),
    ("a" * (MAX_STORE_NAME + 1), f"at most {MAX_STORE_NAME}"),
])
def test_names_that_break_the_sql(name, says):
    problem = store_name_problem(name)
    assert problem and says in problem


def rename(root, key, **changes):
    fn = Path(root) / "config_cache" / f"{key}.json"
    rec = json.loads(fn.read_text())
    rec.update(changes)
    fn.write_text(json.dumps(rec, indent=4))
    return fn


def test_the_configuration_refuses_to_load_and_names_the_file(tmp_path):
    root = make_installation(tmp_path)
    fn = rename(root, "aat", name="getty-aat")
    with pytest.raises(ValueError) as e:
        Config(basepath=str(root))
    assert "Config record aat" in str(e.value) and str(fn) in str(e.value)
    assert "'getty-aat' cannot name a Postgres table: it has '-'" in str(e.value)


def test_a_map_name_or_table_name_too(tmp_path):
    root = make_installation(tmp_path)
    maps = [p.stem for p in (root / "config_cache").glob("*.json")
            if json.loads(p.read_text()).get("type") == "map"]
    rename(root, maps[0], tableName="id-map")
    with pytest.raises(ValueError, match="tableName 'id-map' cannot name a Postgres table"):
        Config(basepath=str(root))


def test_handle_config_record_checks_the_name_given_or_the_key(tmp_path):
    cfgs = make_configs(tmp_path)
    with pytest.raises(ValueError, match="'new-source'"):
        cfgs.handle_config_record("new-source", {"type": "external"})
    with pytest.raises(ValueError, match="'Upper'"):
        cfgs.handle_config_record("x", {"type": "internal", "name": "Upper"})
    assert "Upper" not in cfgs.internal and "new-source" not in cfgs.external
    # a MarkLogic store's name is not a table
    cfgs.handle_config_record("ml", {"type": "marklogic", "name": "ml-dev"})


def test_testinstall_fails_on_it(tmp_path, monkeypatch):
    root = make_installation(tmp_path / "inst")
    rename(root, "ycba", name="ycba-test")
    use_installation(monkeypatch, root)
    monkeypatch.chdir(root)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    results = testinstall.CommandHandler(None).run_checks(
        Namespace(skip_workers=True, skip_pg_tune=True, workers=0), stop_on_fail=True)
    last = results[-1]
    assert last["label"] == "configuration builds" and last["state"] == "FAIL"
    assert "'ycba-test' cannot name a Postgres table" in last["detail"]


def test_the_validator_refuses_it_before_writing(tmp_path):
    cfgs = make_configs(tmp_path)
    rec = dict(json.loads((Path(tmp_path) / "config_cache" / "aat.json").read_text()))
    problems = config_store.validate_record(cfgs, "aat", {**rec, "name": "a-at"})
    assert any("'a-at' cannot name a Postgres table" in p for p in problems)
    assert not [p for p in config_store.validate_record(cfgs, "aat", rec) if "Postgres" in p]
    new = config_store.validate_record(cfgs, "my-src", {**rec, "name": "my-src"}, new=True)
    assert any("'my-src' cannot name a Postgres table" in p for p in new)
