"""Web UI item W05: `lux show`, through the entry point, on the W00
installation with seeded records."""

import json
import logging
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline import runtime
from lux_pipeline.cli import entry, show
from lux_pipeline.config import Config
from webui_fixtures import make_configs, seed_records, use_installation

LOGGER = logging.getLogger("lux_pipeline")


@pytest.fixture(autouse=True)
def _restore_logger():
    saved = (list(LOGGER.handlers), LOGGER.level)
    yield
    for h in list(LOGGER.handlers):
        LOGGER.removeHandler(h)
    for h in saved[0]:
        LOGGER.addHandler(h)
    LOGGER.setLevel(saved[1])


@pytest.fixture
def seeded(tmp_path, monkeypatch):
    root = tmp_path / "inst"
    s = seed_records(make_configs(root, instantiate=True))
    use_installation(monkeypatch, root)
    monkeypatch.chdir(tmp_path)
    return s


def lux(capsys, *argv):
    runtime.reset_configs()
    code = entry.run(["show", *argv])
    out = capsys.readouterr()
    return code, out.out, out.err


def test_a_uri_shows_every_stage(seeded, capsys):
    s = seeded["aat"]
    code, out, _ = lux(capsys, s["uri"])
    lines = out.splitlines()
    assert code == 0
    assert lines[0] == f"{s['uri']}: aat (external) 300033618  {s['uri']}"
    assert lines[1].startswith("  datacache    300033618  record_time ")
    assert lines[2].startswith("  recordcache  300033618##quaType")
    assert lines[3] == "  reconciled   -"
    assert lines[4] == f"  identity     {s['yuid']}  (1 member(s))"
    assert lines[5].strip() == s["uri"] + "##quaType"
    assert lines[-1] == "  note: recordcache: found under the typed key 300033618##quaType"


def test_a_bare_identifier_with_the_global_source_and_its_record(seeded, capsys):
    code, out, _ = lux(capsys, "object/1", "--source", "ycba", "--cache", "recordcache")
    assert code == 0 and out.splitlines()[0].startswith("object/1: ycba (internal) object/1")
    record = json.loads(out[out.index("{"):])
    assert record["type"] == "HumanMadeObject" and record["_label"] == "Portrait of a Lady"


def test_a_yuid(seeded, capsys):
    s = seeded["ycba"]
    code, out, _ = lux(capsys, s["uuid"])
    assert code == 0 and out.splitlines()[0] == f"{s['uuid']}: YUID {s['uuid']}"
    assert f"  merged       {s['uuid']}" in out and s["yuid"] in out


def test_raw_is_json(seeded, capsys):
    code, out, _ = lux(capsys, "aat:300033618", "--raw")
    j = json.loads(out)
    assert code == 0 and j["identity"]["yuid"] == seeded["aat"]["yuid"]
    assert isinstance(j["recordcache"]["row"]["record_time"], str)


def test_validate_without_schemas_says_which(seeded, capsys):
    code, _, err = lux(capsys, "aat:300033618", "--validate")
    assert code == 1 and "core.json" in err


def test_a_stage_that_is_empty(seeded, capsys):
    code, _, err = lux(capsys, "aat:300033618", "--cache", "reconciled")
    assert code == 1 and "nothing in reconciled" in err


def test_sample(seeded, capsys):
    code, out, _ = lux(capsys, "--sample", "aat", "--cache", "recordcache", "--type", "Type")
    assert (code, out) == (0, "300033618##quaType\n")
    code, out, _ = lux(capsys, "--sample", "ycba", "--cache", "datacache", "-n", "5")
    assert out == "object/1\n"
    code, _, err = lux(capsys, "--sample", "aat", "--cache", "merged")
    assert code == 1 and "--sample lists one of" in err


def test_errors(seeded, capsys):
    assert lux(capsys)[0] == 1
    code, _, err = lux(capsys, "http://nowhere.example/1")
    assert code == 1 and "no configured source matches" in err


def test_map_builds_every_source_first(seeded, capsys, monkeypatch):
    built = []
    real = Config.instantiate_all
    monkeypatch.setattr(Config, "instantiate_all", lambda self: (built.append(1), real(self)))
    code, out, _ = lux(capsys, "ycba:object/1", "--map")
    assert code == 0 and built == [1] and "  mapped       " in out


def test_only_what_is_looked_at_is_built(seeded, capsys, monkeypatch):
    built = []
    real = Config.instantiate
    monkeypatch.setattr(Config, "instantiate",
                        lambda self, name, which=None: (built.append(name), real(self, name, which))[1])
    assert lux(capsys, "aat:300033618")[0] == 0
    assert sorted(built) == ["aat", "merged"]
