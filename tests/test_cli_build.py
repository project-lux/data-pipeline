"""`lux build`: the phases run-all.sh runs, in its order, each as its own
process. The plan from --clear / --from / --to / --skip, the global options
each step is given (every one a lux command), stopping at the first
failure and the status-file lines -- with the steps stubbed, so
nothing is started."""

import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from lux_pipeline.cli import build, entry
from lux_pipeline.cli._handler import BaseHandler


class Cfgs:
    def __init__(self, tmp_path):
        self.data_dir = str(tmp_path / "data")
        self.exports_dir = str(tmp_path / "exports")
        self.log_dir = str(tmp_path / "logs")
        os.makedirs(self.exports_dir)
        os.makedirs(self.log_dir)
        self.internal = {"ycba": {}, "ypm": {}}
        self.external = {"aat": {}}


def parse(argv):
    """The args `entry.run` hands the handler, and the handler's rest."""
    args, rest = entry.build_parser().parse_known_args(["build", *argv])
    if args.my_worker > -1:
        args.engine = "null"
    return args, rest


def make(tmp_path, results=None):
    """A handler whose steps are recorded rather than run; `results` maps a
    command name to its exit status."""
    hdlr = build.CommandHandler(Cfgs(tmp_path))
    hdlr.ran = []

    def run_step(argv):
        hdlr.ran.append(argv)
        return (results or {}).get(name_of(argv), 0)

    hdlr.run_step = run_step
    return hdlr


def name_of(argv):
    assert argv[1:3] == ["-m", "lux_pipeline.cli.entry"], "every step is a lux command"
    return argv[3]


def run(hdlr, argv):
    args, rest = parse(argv)
    return hdlr.process(args, rest)


# --- the plan ----------------------------------------------------------------


def test_default_plan_is_run_all_order(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    hdlr = make(tmp_path)
    run(hdlr, [])
    assert [name_of(a) for a in hdlr.ran] == ["sheets", "reconcile", "merge", "portal", "export"]
    reconcile = next(a for a in hdlr.ran if name_of(a) == "reconcile")
    assert reconcile[4:6] == ["--source", "all"]
    # reconcile runs its finishing steps: identify, metatypes, write-refs
    assert "--no-finish" not in reconcile
    export = next(a for a in hdlr.ran if name_of(a) == "export")
    assert "--source" not in export


def test_clear_runs_first(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    hdlr = make(tmp_path)
    run(hdlr, ["--clear", "--to", "reconcile", "--skip", "sheets"])
    assert [a[3:] for a in hdlr.ran[:3]] == [
        ["clear", "--source", "all", "--force"], ["clear", "--refs", "--force"], ["new-token"]]
    assert [name_of(a) for a in hdlr.ran[3:]] == ["reconcile"]


@pytest.mark.parametrize("argv,phases", [
    (["--from", "merge"], ["merge", "portal", "export"]),
    (["--to", "reconcile"], ["sheets", "reconcile"]),
    (["--from", "merge", "--to", "merge"], ["merge"]),
    (["--skip", "sheets,portal"], ["reconcile", "merge", "export"]),
    (["--clear"], list(build.PHASES)),
])
def test_phase_selection(tmp_path, argv, phases):
    hdlr = make(tmp_path)
    args, rest = parse(argv)
    BaseHandler.process(hdlr, args, rest)  # the command's own options only
    assert hdlr.phases(args) == phases


@pytest.mark.parametrize("argv,msg", [
    (["--skip", "nonesuch"], "Unknown phase"),
    (["--from", "export", "--to", "merge"], "comes after"),
    (["--clear", "--from", "merge"], "--clear"),
    (["--source", "ycbx"], "Unknown source: ycbx"),
    (["--my-worker", "0", "--max-workers", "2"], "--my-worker"),
])
def test_bad_options_run_nothing(tmp_path, argv, msg):
    hdlr = make(tmp_path)
    with pytest.raises(ValueError, match=msg):
        run(hdlr, argv)
    assert hdlr.ran == []


def test_global_options_go_to_the_phases(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    hdlr = make(tmp_path)
    run(hdlr, ["--clear", "--source", "ycba,aat", "--max-workers", "8", "--engine", "ray",
               "--no-ui", "--prefix", "/logs/b1", "--log", "DEBUG", "--profile",
               "--skip", "sheets"])
    by = {}
    for a in hdlr.ran:
        by.setdefault(name_of(a), a)
    rec = by["reconcile"]
    assert rec[4:6] == ["--source", "ycba,aat"]
    for opt in (["--max-workers", "8"], ["--engine", "ray"], ["--prefix", "/logs/b1-reconcile"],
                ["--log", "DEBUG"]):
        i = rec.index(opt[0])
        assert rec[i:i + 2] == opt
    assert "--no-ui" in rec and "--profile" in rec
    assert by["merge"][by["merge"].index("--prefix") + 1] == "/logs/b1-merge"
    assert by["export"][by["export"].index("--prefix") + 1] == "/logs/b1-export"
    # the one-shot steps take no worker options
    assert "--max-workers" not in by["clear"] and "--prefix" not in by["new-token"]
    assert by["clear"][by["clear"].index("--log") + 1] == "DEBUG"
    # portal runs its own pool: the worker count, nothing else
    assert by["portal"][4:] == ["--log", "DEBUG", "--max-workers", "8"]


# --- housekeeping, failure, status -------------------------------------------


def test_failure_stops_and_is_recorded(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    hdlr = make(tmp_path, {"merge": 3})
    with pytest.raises(RuntimeError, match=r"build stopped in merge: .* exited with 3"):
        run(hdlr, ["--skip", "sheets"])
    assert [name_of(a) for a in hdlr.ran] == ["reconcile", "merge"]
    status = Path(hdlr.configs.log_dir, "pipeline_process_status.txt").read_text()
    assert status.strip().endswith("[Error] Error in merge")


def test_success_is_recorded_in_given_status_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    hdlr = make(tmp_path)
    status = tmp_path / "status.txt"
    run(hdlr, ["--skip", "sheets", "--status-file", str(status)])
    assert status.read_text().strip().endswith("[Success] Build was successful")


def test_dry_run_runs_nothing(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    hdlr = make(tmp_path)
    run(hdlr, ["--dry-run", "--clear"])
    out = capsys.readouterr().out
    assert hdlr.ran == []
    assert "[clear]" in out and "new-token" in out and "[portal]" in out
    assert not Path(hdlr.configs.log_dir, "pipeline_process_status.txt").exists()


def test_refuses_while_a_build_runs(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    hdlr = make(tmp_path)
    monkeypatch.setattr(build, "refuse_while_building",
                        lambda cfgs, cmd, force: (_ for _ in ()).throw(RuntimeError("running")))
    with pytest.raises(RuntimeError, match="running"):
        run(hdlr, [])
    assert hdlr.ran == []


def test_listed_by_help():
    from lux_pipeline.cli.help import list_commands
    assert "build" in dict(list_commands())
