"""run-export.py, the legacy wrapper: its argv and environment handling, run
against the script file itself with the configuration and the phase
function stubbed. The shell scripts (run-all.sh, export_parallel.sh) depend
on exactly this: `N M`, `--gz`, `--no-ml`, LUX_EXPORT_CHUNK and
LUX_EXPORT_GZIP."""

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
    """The script as a module, with Config and the phase function replaced
    by recorders; returns a runner taking argv."""
    spec = importlib.util.spec_from_file_location("run_export", ROOT / "run-export.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    seen = {}

    def export_slice(cfgs, my_slice, max_slice, **kw):
        seen["slice"] = dict(cfgs=cfgs, my_slice=my_slice, max_slice=max_slice, **kw)
        return {}

    monkeypatch.setattr(mod, "Config", StubConfigs)
    monkeypatch.setattr(mod, "export_slice", export_slice)
    monkeypatch.setattr(mod, "load_dotenv", lambda: None)
    monkeypatch.delenv("LUX_EXPORT_CHUNK", raising=False)
    monkeypatch.delenv("LUX_EXPORT_GZIP", raising=False)

    def run(*argv, **env):
        for k, v in env.items():
            monkeypatch.setenv(k, v)
        monkeypatch.setattr(sys, "argv", ["run-export.py", *argv])
        mod.main()
        return seen

    return run


def test_sliced_marklogic_export(script):
    seen = script("3", "24")
    s = seen["slice"]
    assert (s["my_slice"], s["max_slice"]) == (3, 24)
    assert s["no_ml"] is False and s["gz"] is False
    assert (s["chunk_size"], s["gzip_level"]) == (1000, 9)
    assert isinstance(s["cfgs"], StubConfigs)
    assert s["cfgs"].calls == ["get_idmap", "cache_globals", "instantiate_all"]


def test_gz_no_ml_and_profile(script):
    seen = script("0", "24", "--gz", "--no-ml", "--profile")
    s = seen["slice"]
    assert (s["my_slice"], s["max_slice"]) == (0, 24)
    assert s["no_ml"] is True and s["gz"] is True
    assert "--profile" not in sys.argv


def test_no_slice_arguments_means_the_whole_table(script):
    s = script()["slice"]
    assert (s["my_slice"], s["max_slice"]) == (0, 1)


def test_flags_alone_do_not_read_as_slices(script):
    s = script("--no-ml", "--gz")["slice"]
    assert (s["my_slice"], s["max_slice"]) == (0, 1)
    assert s["no_ml"] is True and s["gz"] is True


def test_chunk_and_gzip_level_come_from_the_environment(script):
    s = script("0", "2", LUX_EXPORT_CHUNK="50", LUX_EXPORT_GZIP="3")["slice"]
    assert (s["chunk_size"], s["gzip_level"]) == (50, 3)
