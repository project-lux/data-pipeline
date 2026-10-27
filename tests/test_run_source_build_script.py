"""run-source-build.py, the legacy wrapper: its argv handling and the
directories it has always written to, run against the script file itself
with the configuration and the phase function stubbed."""

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))


class StubConfigs:
    def __init__(self, basepath=None):
        self.basepath = basepath
        self.calls = []

    def get_idmap(self):
        self.calls.append("get_idmap")
        return "idmap"

    def cache_globals(self):
        self.calls.append("cache_globals")

    def instantiate_all(self):
        self.calls.append("instantiate_all")


@pytest.fixture
def script(monkeypatch):
    spec = importlib.util.spec_from_file_location("run_source_build", ROOT / "run-source-build.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    seen = {}

    def export_sources_slice(cfgs, my_slice, max_slice, **kw):
        seen["slice"] = dict(cfgs=cfgs, my_slice=my_slice, max_slice=max_slice, **kw)
        return {}

    monkeypatch.setattr(mod, "Config", StubConfigs)
    monkeypatch.setattr(mod, "export_sources_slice", export_sources_slice)
    monkeypatch.setattr(mod, "load_dotenv", lambda: None)

    def run(*argv):
        monkeypatch.setattr(sys, "argv", ["run-source-build.py", *argv])
        mod.main()
        return seen

    return run


def test_sliced_run_passes_the_slice_and_the_old_directories(script):
    s = script("3", "24")["slice"]
    assert (s["my_slice"], s["max_slice"]) == (3, 24)
    assert s["out_dir"] == "/data-export/output/lux/latest"
    assert s["nt_dir"] == "/data-export/output/lux/nt"
    assert "sources" not in s
    assert s["cfgs"].calls == ["get_idmap", "cache_globals", "instantiate_all"]


def test_no_arguments_is_unsliced(script):
    s = script()["slice"]
    assert (s["my_slice"], s["max_slice"]) == (-1, -1)


def test_non_numeric_arguments_are_ignored(script):
    s = script("--all")["slice"]
    assert (s["my_slice"], s["max_slice"]) == (-1, -1)
