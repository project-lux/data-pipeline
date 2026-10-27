"""run-merge.py, the legacy wrapper: its argv handling, run against the
script file itself with the configuration and the phase functions stubbed.
The shell scripts (run-all.sh, merge_parallel.sh) depend on exactly this:
`N M --all`, `--<src>`, `--recid`, `--norefs`, `--resume`, `--onlyrefs`."""

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

    def cache_globals(self):
        self.calls.append("cache_globals")

    def instantiate_all(self):
        self.calls.append("instantiate_all")


@pytest.fixture
def script(monkeypatch):
    """The script as a module, with Config and the phase functions replaced
    by recorders; returns a runner taking argv."""
    spec = importlib.util.spec_from_file_location("run_merge", ROOT / "run-merge.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    seen = {"order": [], "bad_token": False}

    def read_update_token(cfgs):
        seen["order"].append("token")
        if seen["bad_token"]:
            raise ValueError("Idmap Update Token is badly formed")
        return "__20261027__"

    def select_sources(cfgs, names=(), all_internal=False, only_refs=False, recids=()):
        seen["order"].append("select")
        seen["select"] = dict(names=list(names), all_internal=all_internal,
                              only_refs=only_refs, recids=list(recids))
        if not names and not all_internal and not only_refs:
            raise ValueError("No source selected")
        if recids and (all_internal or len(names) > 1):
            raise ValueError("Can only build individual records from a single source")
        return ["to_do"]

    def merge_slice(cfgs, idmap, to_do, **kw):
        seen["order"].append("merge")
        seen["slice"] = dict(cfgs=cfgs, idmap=idmap, to_do=to_do, **kw)
        return {}

    monkeypatch.setattr(mod, "Config", StubConfigs)
    monkeypatch.setattr(mod, "read_update_token", read_update_token)
    monkeypatch.setattr(mod, "select_sources", select_sources)
    monkeypatch.setattr(mod, "merge_slice", merge_slice)
    monkeypatch.setattr(mod, "load_dotenv", lambda: None)

    def run(*argv, bad_token=False):
        seen["bad_token"] = bad_token
        monkeypatch.setattr(sys, "argv", ["run-merge.py", *argv])
        mod.main()
        return seen

    return run


def test_sliced_all(script):
    seen = script("3", "24", "--all")
    assert seen["select"] == dict(names=[], all_internal=True, only_refs=False, recids=[])
    s = seen["slice"]
    assert (s["my_slice"], s["max_slice"]) == (3, 24)
    assert (s["recids"], s["do_references"], s["resume"]) == ([], True, False)
    assert (s["idmap"], s["to_do"]) == ("idmap", ["to_do"])
    assert s["cfgs"].calls == ["get_idmap", "cache_globals", "instantiate_all"]
    assert seen["order"] == ["token", "select", "merge"]


def test_named_source_with_recids_norefs_and_resume(script):
    seen = script("--ycba", "--recid", "obj/1", "--recid", "obj/2", "--norefs", "--resume")
    assert seen["select"] == dict(names=["ycba"], all_internal=False, only_refs=False,
                                  recids=["obj/1", "obj/2"])
    s = seen["slice"]
    assert (s["my_slice"], s["max_slice"]) == (-1, -1)
    assert (s["recids"], s["do_references"], s["resume"]) == (["obj/1", "obj/2"], False, True)


def test_only_refs(script):
    seen = script("0", "24", "--onlyrefs")
    assert seen["select"] == dict(names=[], all_internal=False, only_refs=True, recids=[])
    assert (seen["slice"]["my_slice"], seen["slice"]["max_slice"]) == (0, 24)


def test_unknown_flags_and_profile_are_ignored(script):
    seen = script("--profile", "0", "2", "--ypm", "--typo", "--ycba")
    assert seen["select"]["names"] == ["ypm", "ycba"]     # select_sources puts them in config order
    assert (seen["slice"]["my_slice"], seen["slice"]["max_slice"]) == (0, 2)


def test_slices_are_only_read_from_the_first_two_arguments(script):
    seen = script("--all", "0", "2")
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


def test_a_bad_token_prints_and_exits_1_before_anything_runs(script, capsys):
    with pytest.raises(SystemExit) as e:
        seen = script("0", "24", "--all", bad_token=True)
    assert e.value.code == 1
    assert "badly formed" in capsys.readouterr().out
