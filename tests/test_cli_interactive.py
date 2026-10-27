"""`lux interactive`: piped statements run against the loaded configuration
without prompts; setup_for builds a one-worker manager with the command's
options; idmap failures and the lazy ReferenceManager; readline set-up."""

import io
import sys
from argparse import Namespace
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from lux_pipeline import runtime
from lux_pipeline.cli import entry
from lux_pipeline.cli import interactive
from lux_pipeline.cli.interactive import CommandHandler
from lux_pipeline.process.base.loader import Loader
from lux_pipeline.process.tasks.load_task import LoadManager


class StubConfigs:
    def __init__(self, base_dir, idmap="the-idmap"):
        self.base_dir = str(base_dir)
        self.max_workers = 8
        self.internal = {}
        self.external = {"ror": {"name": "ror", "loader": Loader({"name": "ror", "datacache": {}})},
                         "aat": {"name": "aat"}}
        self.results = {}
        self._idmap = idmap

    def get_idmap(self):
        if isinstance(self._idmap, Exception):
            raise self._idmap
        return self._idmap


def run(cfgs, stdin, monkeypatch):
    monkeypatch.setattr(sys, "stdin", io.StringIO(stdin))
    CommandHandler(cfgs).process(Namespace(command="interactive"), [])


def test_piped_statements_run_without_prompts(tmp_path, monkeypatch, capsys):
    run(StubConfigs(tmp_path), "print(len(cfgs.external))\nprint(idmap)\n", monkeypatch)
    out = capsys.readouterr()
    assert out.out.split() == ["2", "the-idmap"]
    assert ">>>" not in out.out and ">>>" not in out.err
    # no readline set-up without a terminal, so no history file
    assert not (tmp_path / ".python_history").exists()


def test_through_the_lux_entry_point(tmp_path, monkeypatch, capsys):
    cfgs = StubConfigs(tmp_path)
    monkeypatch.setattr(runtime, "get_configs", lambda: cfgs)
    monkeypatch.setattr(sys, "stdin", io.StringIO("print(len(cfgs.external))\n"))
    assert entry.run(["interactive"]) == 0
    assert capsys.readouterr().out.strip() == "2"


def test_idmap_unavailable_is_none_with_a_warning(tmp_path, monkeypatch, capsys, caplog):
    run(StubConfigs(tmp_path, idmap=ConnectionError("no postgres")), "print(idmap)\n", monkeypatch)
    assert capsys.readouterr().out.strip() == "None"
    assert "idmap unavailable: no postgres" in caplog.text


def test_ref_mgr_is_built_once_on_first_use(tmp_path, monkeypatch):
    built = []

    class FakeRefMgr:
        def __init__(self, cfgs, idmap):
            built.append((cfgs, idmap))

    import lux_pipeline.process.reference_manager as rm
    monkeypatch.setattr(rm, "ReferenceManager", FakeRefMgr)
    cfgs = StubConfigs(tmp_path)
    ns = CommandHandler(cfgs).namespace()
    assert built == []
    first = ns["ref_mgr"]()
    assert ns["ref_mgr"]() is first
    assert built == [(cfgs, "the-idmap")]


# --- setup_for ---------------------------------------------------------------


def test_setup_for_returns_a_one_worker_manager_with_sources(tmp_path):
    mgr = CommandHandler(StubConfigs(tmp_path)).setup_for("load", "ror", "aat")
    assert isinstance(mgr, LoadManager)
    assert mgr.max_workers == 1
    # aat has no loader, so LoadManager.maybe_add leaves it out
    assert mgr.sources == [("external", "ror")]
    # the command's defaults are applied
    assert (mgr.load_type, mgr.overwrite, mgr.clear) == ("records", True, False)


def test_setup_for_options_override_the_defaults(tmp_path):
    mgr = CommandHandler(StubConfigs(tmp_path)).setup_for("load", type="export", overwrite=False)
    assert (mgr.load_type, mgr.overwrite) == ("export", False)


def test_setup_for_a_command_without_a_manager(tmp_path, capsys):
    h = CommandHandler(StubConfigs(tmp_path))
    assert h.setup_for("list") is None
    assert "'list' does not run a manager" in capsys.readouterr().out
    assert h.setup_for("no-such-command") is None
    assert "Unknown command" in capsys.readouterr().out


def test_setup_for_unknown_source_raises(tmp_path):
    with pytest.raises(ValueError, match="Unknown source: nope"):
        CommandHandler(StubConfigs(tmp_path)).setup_for("load", "nope")


# --- readline ----------------------------------------------------------------


class FakeReadline:
    __doc__ = "Importing this module enables command line editing using libedit readline."

    def __init__(self):
        self.read, self.written, self.bindings, self.completer = [], [], [], None

    def read_history_file(self, fn):
        self.read.append(fn)

    def write_history_file(self, fn):
        self.written.append(fn)

    def set_completer(self, fn):
        self.completer = fn

    def parse_and_bind(self, s):
        self.bindings.append(s)


def test_readline_history_and_completion(tmp_path, monkeypatch):
    rl = FakeReadline()
    at_exit = []
    monkeypatch.setattr(interactive, "readline", rl)
    monkeypatch.setattr(interactive.atexit, "register", at_exit.append)
    hist = tmp_path / ".python_history"

    interactive.enable_readline({"cfgs": 1}, str(hist))
    assert rl.read == []          # nothing to read yet
    assert rl.bindings == ["bind ^I rl_complete"]
    assert rl.completer("cf", 0) == "cfgs"
    at_exit[0]()
    assert rl.written == [str(hist)]

    hist.write_text("")
    rl.__doc__ = "GNU readline"
    interactive.enable_readline({}, str(hist))
    assert rl.read == [str(hist)]
    assert rl.bindings[-1] == "tab: complete"
