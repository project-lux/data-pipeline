"""run-reconcile.py, the legacy wrapper: its argv handling, run against the
script file itself with the configuration and the phase functions stubbed.
The shell scripts (run-all.sh, reconcile_parallel.sh) depend on exactly
this: `N M --all`, `--<src>`, `--recid`, `--norefs`, `--baseline`."""

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))


class StubConfigs:
    def __init__(self, basepath=None):
        self.basepath = basepath
        self.internal = {"ycba": {"name": "ycba"}, "ypm": {"name": "ypm"}}
        self.external = {"aat": {"name": "aat"}}
        self.calls = []

    def get_idmap(self):
        self.calls.append("get_idmap")
        return "idmap"

    def instantiate_map(self, name):
        self.calls.append(f"instantiate_map:{name}")
        return {"store": "networkmap"}

    def cache_globals(self):
        self.calls.append("cache_globals")

    def instantiate_all(self):
        self.calls.append("instantiate_all")


@pytest.fixture
def script(monkeypatch):
    """The script as a module, with Config and the phase functions replaced
    by recorders; returns a runner taking argv."""
    spec = importlib.util.spec_from_file_location("run_reconcile", ROOT / "run-reconcile.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    seen = {}

    def select_sources(cfgs, names=(), all_internal=False, baseline=False, recids=()):
        seen["select"] = dict(names=list(names), all_internal=all_internal,
                              baseline=baseline, recids=list(recids))
        if not names and not all_internal and not baseline:
            raise ValueError("No source selected")
        if recids and (all_internal or len(names) > 1):
            raise ValueError("Can only build individual records from a single source")
        return ["to_do"]

    def reconcile_slice(cfgs, idmap, networkmap, to_do, **kw):
        seen["slice"] = dict(cfgs=cfgs, idmap=idmap, networkmap=networkmap, to_do=to_do, **kw)
        return {}

    monkeypatch.setattr(mod, "Config", StubConfigs)
    monkeypatch.setattr(mod, "select_sources", select_sources)
    monkeypatch.setattr(mod, "reconcile_slice", reconcile_slice)
    monkeypatch.setattr(mod, "load_dotenv", lambda: None)

    def run(*argv):
        monkeypatch.setattr(sys, "argv", ["run-reconcile.py", *argv])
        mod.main()
        return seen

    return run


def test_sliced_all(script):
    seen = script("3", "24", "--all")
    assert seen["select"] == dict(names=[], all_internal=True, baseline=False, recids=[])
    s = seen["slice"]
    assert (s["my_slice"], s["max_slice"], s["do_references"]) == (3, 24, True)
    assert (s["idmap"], s["networkmap"], s["to_do"]) == ("idmap", "networkmap", ["to_do"])
    assert s["cfgs"].calls == ["get_idmap", "instantiate_map:networkmap", "cache_globals",
                               "instantiate_all"]


def test_named_source_with_recids_and_norefs(script):
    seen = script("--aat", "--recid", "300010001", "--recid", "300010002", "--norefs")
    assert seen["select"] == dict(names=["aat"], all_internal=False, baseline=False,
                                  recids=["300010001", "300010002"])
    s = seen["slice"]
    assert (s["my_slice"], s["max_slice"], s["do_references"]) == (-1, -1, False)


def test_unknown_flags_and_profile_are_ignored(script):
    seen = script("--profile", "0", "2", "--ycba", "--typo", "--ypm")
    assert seen["select"]["names"] == ["ycba", "ypm"]
    assert (seen["slice"]["my_slice"], seen["slice"]["max_slice"]) == (0, 2)


def test_slices_are_only_read_from_the_first_two_arguments(script):
    seen = script("--all", "0", "2")
    assert (seen["slice"]["my_slice"], seen["slice"]["max_slice"]) == (-1, -1)


def test_baseline_ignores_slices_and_recids(script):
    seen = script("0", "24", "--baseline", "--recid", "x")
    assert seen["select"] == dict(names=[], all_internal=False, baseline=True, recids=[])
    assert (seen["slice"]["my_slice"], seen["slice"]["max_slice"]) == (-1, -1)


def test_selection_errors_print_and_exit_1(script, capsys):
    with pytest.raises(SystemExit) as e:
        script("--aat", "--ycba", "--recid", "1")
    assert e.value.code == 1
    assert "single source" in capsys.readouterr().out
    with pytest.raises(SystemExit) as e:
        script("0", "24")
    assert e.value.code == 1
    assert "No source selected" in capsys.readouterr().out
