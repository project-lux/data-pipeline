"""The `lux` entry point: global options, command lookup, config handling,
exit codes; the handler base; and that packaging metadata agrees."""

import logging
import re
import sys
import types
from argparse import Namespace
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from lux_pipeline import runtime
from lux_pipeline.cli import entry
from lux_pipeline.cli._handler import BaseHandler, CommandHandler, default_prefix
from lux_pipeline.cli.help import list_commands


# --- fixtures ----------------------------------------------------------------


@pytest.fixture(autouse=True)
def _no_real_config(monkeypatch):
    """No installation: get_configs raises, as on a fresh machine."""
    def boom(*a, **kw):
        raise RuntimeError("LUX_BASEPATH is not set (no .env found)")
    monkeypatch.setattr(runtime, "get_configs", boom)
    yield


@pytest.fixture(autouse=True)
def _restore_logger():
    logger = logging.getLogger("lux_pipeline")
    saved = (list(logger.handlers), logger.level)
    yield
    for h in list(logger.handlers):
        logger.removeHandler(h)
    for h in saved[0]:
        logger.addHandler(h)
    logger.setLevel(saved[1])


class Recorder:
    """A CommandHandler that remembers how it was called."""
    calls = []

    def __init__(self, cfgs):
        self.cfgs = cfgs

    def process(self, args, rest):
        Recorder.calls.append((self.cfgs, args, rest))


@pytest.fixture
def fake_command(monkeypatch):
    """A built-in command `fake-cmd` -> module lux_pipeline.cli.fake_cmd."""
    Recorder.calls = []
    mod = types.ModuleType("lux_pipeline.cli.fake_cmd")
    mod.CommandHandler = Recorder
    monkeypatch.setitem(sys.modules, "lux_pipeline.cli.fake_cmd", mod)
    return mod


def run(argv, monkeypatch):
    """main() with argv, returning (exit code, stdout, stderr)."""
    monkeypatch.setattr(sys, "argv", ["lux", *argv])
    with pytest.raises(SystemExit) as ex:
        entry.main()
    return ex.value.code


# --- help --------------------------------------------------------------------


def test_help_runs_without_a_config_and_lists_help(monkeypatch, capsys):
    assert run(["help"], monkeypatch) == 0
    out = capsys.readouterr().out
    assert re.search(r"^\s+help\s+List the available lux commands", out, re.M)
    assert "--my-worker" in out


def test_help_log_debug_sets_the_logger_level(monkeypatch, capsys):
    assert run(["help", "--log", "DEBUG"], monkeypatch) == 0
    logger = logging.getLogger("lux_pipeline")
    assert logger.level == logging.DEBUG
    assert logger.handlers[0].level == logging.DEBUG
    # the debug line in help.py reaches the stream handler
    assert "listing the command modules" in capsys.readouterr().out


def test_list_commands_skips_private_modules_and_entry():
    names = [n for n, _ in list_commands()]
    assert "help" in names
    assert "entry" not in names
    assert not any(n.startswith("_") for n in names)


# --- command lookup ----------------------------------------------------------


def test_unknown_command_exits_nonzero_with_a_clear_message(monkeypatch, capsys):
    assert run(["nosuch"], monkeypatch) == 2
    assert "Unknown command 'nosuch'" in capsys.readouterr().err


def test_private_module_is_not_a_command(monkeypatch, capsys):
    assert run(["_handler"], monkeypatch) == 2
    assert "Unknown command" in capsys.readouterr().err


def test_dash_maps_to_underscore(monkeypatch, fake_command):
    monkeypatch.setattr(runtime, "get_configs", lambda *a, **kw: "CFGS")
    assert run(["fake-cmd"], monkeypatch) == 0
    cfgs, args, rest = Recorder.calls[0]
    assert cfgs == "CFGS"
    assert args.command == "fake-cmd"


def test_extension_command_by_dotted_name(monkeypatch):
    Recorder.calls = []
    pkg = types.ModuleType("myext")
    pkg.__path__ = []
    mod = types.ModuleType("myext.private_cmd")
    mod.CommandHandler = Recorder
    monkeypatch.setitem(sys.modules, "myext", pkg)
    monkeypatch.setitem(sys.modules, "myext.private_cmd", mod)
    monkeypatch.setattr(runtime, "get_configs", lambda *a, **kw: "CFGS")
    assert run(["myext.private-cmd"], monkeypatch) == 0
    assert Recorder.calls[0][1].command == "myext.private-cmd"


def test_extension_without_a_handler_is_rejected(monkeypatch, capsys):
    mod = types.ModuleType("myext2.nothing")
    monkeypatch.setitem(sys.modules, "myext2.nothing", mod)
    assert run(["myext2.nothing"], monkeypatch) == 2
    assert "no CommandHandler" in capsys.readouterr().err


def test_a_broken_command_module_reports_the_import_error(monkeypatch, capsys):
    monkeypatch.setattr(entry.importlib, "import_module",
                        lambda name: (_ for _ in ()).throw(ImportError("bad module")))
    assert run(["help"], monkeypatch) == 2
    assert "Failed to import command 'help': bad module" in capsys.readouterr().err


# --- global options ----------------------------------------------------------


def test_default_engine_is_mp(monkeypatch, fake_command):
    monkeypatch.setattr(runtime, "get_configs", lambda *a, **kw: None)
    run(["fake-cmd"], monkeypatch)
    assert Recorder.calls[0][1].engine == "mp"


def test_my_worker_forces_the_null_engine(monkeypatch, fake_command):
    monkeypatch.setattr(runtime, "get_configs", lambda *a, **kw: None)
    run(["fake-cmd", "--my-worker", "1", "--max-workers", "4", "--engine", "ray"], monkeypatch)
    args = Recorder.calls[0][1]
    assert args.engine == "null"
    assert (args.my_worker, args.max_workers) == (1, 4)


@pytest.mark.parametrize("argv", [
    ["help", "--my-worker", "3", "--max-workers", "2"],
    ["help", "--my-worker", "2", "--max-workers", "2"],
    ["help", "--my_worker", "0"],                       # max_workers unset
])
def test_my_worker_must_be_below_max_workers(monkeypatch, capsys, argv):
    assert run(argv, monkeypatch) == 2
    assert "must be lower than max_workers" in capsys.readouterr().err


def test_both_spellings_of_the_worker_options(monkeypatch, fake_command):
    monkeypatch.setattr(runtime, "get_configs", lambda *a, **kw: None)
    run(["fake-cmd", "--max_workers", "6", "--max_records", "10", "--no_ui"], monkeypatch)
    args = Recorder.calls[0][1]
    assert (args.max_workers, args.max_records, args.no_ui) == (6, 10, True)


def test_unknown_log_level_exits_2(monkeypatch, capsys):
    assert run(["help", "--log", "LOUD"], monkeypatch) == 2
    assert "unknown log level" in capsys.readouterr().err


def test_debug_implies_log_debug(monkeypatch, fake_command):
    monkeypatch.setattr(runtime, "get_configs", lambda *a, **kw: None)
    run(["fake-cmd", "--debug"], monkeypatch)
    assert Recorder.calls[0][1].log == "DEBUG"


def test_rest_is_passed_through_to_the_handler(monkeypatch, fake_command):
    monkeypatch.setattr(runtime, "get_configs", lambda *a, **kw: None)
    run(["fake-cmd", "--their-option", "x"], monkeypatch)
    assert Recorder.calls[0][2] == ["--their-option", "x"]


# --- config handling ---------------------------------------------------------


def test_config_failure_stops_a_normal_command(monkeypatch, fake_command, capsys):
    assert run(["fake-cmd"], monkeypatch) == 1
    err = capsys.readouterr().err
    assert "could not load the pipeline configuration" in err
    assert "LUX_BASEPATH" in err
    assert Recorder.calls == []


@pytest.mark.parametrize("cmd", ["initialize", "testinstall"])
def test_config_failure_lets_setup_commands_continue(monkeypatch, cmd):
    Recorder.calls = []
    mod = types.ModuleType(f"lux_pipeline.cli.{cmd}")
    mod.CommandHandler = Recorder
    monkeypatch.setitem(sys.modules, f"lux_pipeline.cli.{cmd}", mod)
    assert run([cmd], monkeypatch) == 0
    assert Recorder.calls[0][0] is None


# --- handler failures --------------------------------------------------------


class Exploding:
    def __init__(self, cfgs):
        pass

    def process(self, args, rest):
        raise KeyError("boom")


@pytest.fixture
def exploding_command(monkeypatch):
    mod = types.ModuleType("lux_pipeline.cli.explode")
    mod.CommandHandler = Exploding
    monkeypatch.setitem(sys.modules, "lux_pipeline.cli.explode", mod)
    monkeypatch.setattr(runtime, "get_configs", lambda *a, **kw: None)


def test_handler_exception_exits_1(monkeypatch, exploding_command, capsys):
    assert run(["explode"], monkeypatch) == 1
    assert "lux explode: KeyError: 'boom'" in capsys.readouterr().err


def test_handler_exception_reraises_under_debug(monkeypatch, exploding_command):
    monkeypatch.setattr(sys, "argv", ["lux", "explode", "--debug"])
    with pytest.raises(KeyError):
        entry.main()


# --- handler base ------------------------------------------------------------


class StubConfigs:
    internal = {"ycba": {}, "yuag": {}}
    external = {"lcnaf": {}, "aat": {}}
    results = {"merged": {}}
    max_workers = 7
    log_dir = "/var/log/lux"


def make_args(**kw):
    base = dict(command="load", source=None, max_workers=0, engine="mp",
                no_ui=True, prefix=None, log="INFO")
    base.update(kw)
    return Namespace(**base)


@pytest.mark.parametrize("source, expected", [
    (None, ["ycba", "yuag", "lcnaf", "aat"]),
    ("all", ["ycba", "yuag", "lcnaf", "aat"]),
    ("internal", ["ycba", "yuag"]),
    ("external", ["lcnaf", "aat"]),
    ("results", ["merged"]),
    ("aat,ycba", ["aat", "ycba"]),
    ("merged", ["merged"]),
])
def test_resolve_sources(source, expected):
    h = CommandHandler(StubConfigs())
    assert h.resolve_sources(make_args(source=source)) == expected


def test_resolve_sources_names_the_unknown_source():
    h = CommandHandler(StubConfigs())
    with pytest.raises(ValueError, match="Unknown source: nope"):
        h.resolve_sources(make_args(source="aat,nope"))


def test_base_handler_always_parses_its_own_options():
    class H(BaseHandler):
        def add_args(self, ap):
            ap.add_argument("--recid", default="none")
    args = make_args()
    H(None).process(args, [])
    assert args.recid == "none"
    H(None).process(args, ["--recid", "300010001"])
    assert args.recid == "300010001"


def test_command_handler_without_a_manager_reports_the_sources(capsys):
    args = make_args(source="internal", max_workers=3)
    assert CommandHandler(StubConfigs()).process(args, []) is None
    out = capsys.readouterr().out
    assert "2 source(s) over 3 worker(s)" in out
    assert "ycba, yuag" in out


def test_command_handler_runs_a_manager_with_the_prefix(capsys):
    events = []

    class Manager:
        def prepare_single(self, s):
            events.append(("prepare", s))

        def process(self, engine, prefix, view):
            events.append(("process", engine, prefix, view))

    class H(CommandHandler):
        def make_manager(self, workers, args):
            events.append(("make", workers))
            return Manager()

    args = make_args(source="aat", prefix="/tmp/run-1", engine="null")
    H(StubConfigs()).process(args, [])
    assert events == [("make", 7), ("prepare", "aat"), ("process", "null", "/tmp/run-1", None)]
    assert "status/log prefix: /tmp/run-1" in capsys.readouterr().out


def test_default_prefix_is_under_the_log_dir():
    p = default_prefix(StubConfigs(), "merge")
    assert p.startswith("/var/log/lux/merge-")
    assert re.search(r"merge-\d{4}-\d{2}-\d{2}-\d{6}$", p)


# --- packaging ---------------------------------------------------------------


def test_requirements_and_pyproject_agree():
    tomllib = pytest.importorskip("tomllib")
    reqs = {l.strip() for l in (ROOT / "requirements.txt").read_text().splitlines()
            if l.strip() and not l.startswith("#")}
    with open(ROOT / "pyproject.toml", "rb") as fh:
        proj = tomllib.load(fh)
    deps = set(proj["project"]["dependencies"])
    assert deps == reqs
    assert proj["project"]["scripts"]["lux"] == "lux_pipeline.cli.entry:main"
    assert "ray" in proj["project"]["optional-dependencies"]
