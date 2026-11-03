"""Web UI item W03: lux testinstall's checks as data. `run_checks` returns
a row per check; without stopping at the first failure every check gets a
row, and one that needs what an earlier failure left missing says so
instead of running. Built on the database-free installation (W00); the
Postgres probe is stubbed, so no server is needed (or used)."""

import io
import os
import sys
from argparse import Namespace
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline import runtime
from lux_pipeline.cli import testinstall
from webui_fixtures import make_configs, make_installation, use_installation

SKIPS = Namespace(skip_workers=True, skip_pg_tune=True, workers=0)


@pytest.fixture
def installed(tmp_path, monkeypatch):
    """The W00 installation, the working directory in it, the real
    ~/.lux/.env out of reach, and Postgres answering."""
    root = make_installation(tmp_path / "inst")
    use_installation(monkeypatch, root)
    monkeypatch.chdir(root)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setattr(testinstall._services, "probe_postgres", lambda settings: "PostgreSQL (stub)")
    return root


def handler():
    return testinstall.CommandHandler(None)


def states(results):
    return [(r["label"], r["state"]) for r in results]


def test_every_check_gets_a_row_with_the_servers_configuration(installed, monkeypatch, capsys):
    cfgs = make_configs(installed)

    def no_rebuild(*a, **kw):
        raise AssertionError("must check the configuration it was given, not build one")

    monkeypatch.setattr(runtime, "get_configs", no_rebuild)
    h = handler()
    results = h.run_checks(SKIPS, out=None, stop_on_fail=False, cfgs=cfgs)
    assert [r["label"] for r in results] == [label for label, *_ in h.checks(SKIPS)]
    by = dict(states(results))
    assert all(state == "OK" for label, state in by.items() if label != "validator schemas present")
    assert by["validator schemas present"] == "FAIL"           # the fixture has no schemas
    config = next(r for r in results if r["label"] == "configuration builds")
    assert config["detail"].startswith("1 internal, 1 external, 1 results, 5 maps")
    assert capsys.readouterr().out == ""                        # out=None writes nowhere
    assert all(set(r) == {"label", "state", "detail", "required"} for r in results)


def test_stopping_at_the_first_failure_or_carrying_on(installed, monkeypatch):
    def down(settings):
        raise ConnectionError("no server")

    monkeypatch.setattr(testinstall._services, "probe_postgres", down)
    stopped = handler().run_checks(SKIPS, stop_on_fail=True)
    assert states(stopped)[-1] == ("postgres reachable", "FAIL")
    assert len(stopped) == 5
    every = handler().run_checks(SKIPS, stop_on_fail=False)
    assert len(every) == len(handler().checks(SKIPS))
    by = dict(states(every))
    assert by["postgres reachable"] == "FAIL" and by["idmap reachable"] == "OK"
    pg = next(r for r in every if r["label"] == "postgres reachable")
    assert pg["detail"] == "ConnectionError: no server"


def test_checks_that_need_what_failed_say_so_instead_of_running(tmp_path, monkeypatch):
    monkeypatch.delenv("LUX_BASEPATH", raising=False)
    monkeypatch.delenv("LUX_ENV_FILE", raising=False)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.chdir(tmp_path)
    runtime.reset_configs()
    results = handler().run_checks(SKIPS, stop_on_fail=False)
    by = {r["label"]: r for r in results}
    assert by[".env"]["state"] == "FAIL" and by["LUX_BASEPATH"]["state"] == "FAIL"
    for label in ("configuration directory", "configuration builds"):
        assert (by[label]["state"], by[label]["detail"]) == ("FAIL", "LUX_BASEPATH was not found")
    assert (by["idmap reachable"]["state"], by["idmap reachable"]["detail"]) == \
        ("FAIL", "the configuration did not build")
    # not required: a warning, still saying why
    assert (by["redis reachable"]["state"], by["redis reachable"]["detail"]) == \
        ("WARN", "the configuration did not build")


def test_the_lines_written_are_the_commands(installed):
    out = io.StringIO()
    handler().run_checks(SKIPS, out=out, stop_on_fail=False, cfgs=make_configs(installed))
    lines = out.getvalue().splitlines()
    assert lines[0].startswith("OK    .env: ")
    assert lines[4] == "OK    postgres reachable: PostgreSQL (stub)"
    assert lines[-1].startswith("FAIL  validator schemas present: FileNotFoundError: core.json")


def test_the_command_still_stops_and_exits_one(installed, capsys):
    with pytest.raises(SystemExit) as ex:
        handler().process(Namespace(command="testinstall"), ["--skip-workers", "--skip-pg-tune"])
    assert ex.value.code == 1
    out = capsys.readouterr().out.splitlines()
    assert out[-2].startswith("FAIL  validator schemas present")
    assert out[-1] == "Stopped at the first failure. Fix it and run lux testinstall again."
