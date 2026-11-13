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
from lux_pipeline.progress import read_run
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
    assert [name_of(a) for a in hdlr.ran] == ["sheets", "reconcile", "identify", "metatypes",
                                              "write-refs", "merge", "portal", "export"]
    reconcile = next(a for a in hdlr.ran if name_of(a) == "reconcile")
    assert reconcile[4:6] == ["--source", "all"]
    # identify, metatypes and write-refs are the identify phase's, not
    # reconcile's tail
    assert "--no-finish" in reconcile
    export = next(a for a in hdlr.ran if name_of(a) == "export")
    assert "--source" not in export


def test_clear_runs_first(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    hdlr = make(tmp_path)
    run(hdlr, ["--clear", "--to", "reconcile", "--skip", "sheets"])
    base = hdlr.build_prefix
    assert [a[3:] for a in hdlr.ran[:3]] == [
        ["clear", "--source", "all", "--force", "--prefix", os.path.join(base, "clear")],
        ["clear", "--refs", "--force", "--prefix", os.path.join(base, "clear-refs")],
        ["new-token", "--prefix", os.path.join(base, "new-token")]]
    assert [name_of(a) for a in hdlr.ran[3:]] == ["reconcile"]


@pytest.mark.parametrize("argv,phases", [
    (["--from", "merge"], ["merge", "portal", "export"]),
    (["--to", "reconcile"], ["sheets", "reconcile"]),
    (["--from", "identify"], ["identify", "merge", "portal", "export"]),
    (["--from", "merge", "--to", "merge"], ["merge"]),
    (["--skip", "sheets,portal"], ["reconcile", "identify", "merge", "export"]),
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
    for opt in (["--max-workers", "8"], ["--engine", "ray"], ["--prefix", "/logs/b1/reconcile"],
                ["--log", "DEBUG"]):
        i = rec.index(opt[0])
        assert rec[i:i + 2] == opt
    assert "--no-ui" in rec and "--profile" in rec
    assert by["merge"][by["merge"].index("--prefix") + 1] == "/logs/b1/merge"
    assert by["export"][by["export"].index("--prefix") + 1] == "/logs/b1/export"
    # the one-shot steps take no worker options, but a prefix of their own
    assert "--max-workers" not in by["clear"] and "--engine" not in by["new-token"]
    assert by["new-token"][by["new-token"].index("--prefix") + 1] == "/logs/b1/new-token"
    assert by["clear"][by["clear"].index("--log") + 1] == "DEBUG"
    # portal runs its own pool: the worker count and its prefix, nothing else
    assert by["portal"][4:] == ["--log", "DEBUG", "--max-workers", "8", "--prefix", "/logs/b1/portal"]


# --- housekeeping, failure, status -------------------------------------------


def test_failure_stops_and_is_recorded(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    hdlr = make(tmp_path, {"merge": 3})
    with pytest.raises(RuntimeError, match=r"build stopped in merge: .* exited with 3"):
        run(hdlr, ["--skip", "sheets"])
    assert [name_of(a) for a in hdlr.ran] == ["reconcile", "identify", "metatypes", "write-refs",
                                              "merge"]
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


# --- the build's run record (<prefix>/run.json) ----------------------------------


def test_the_build_records_its_phases_and_steps(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    hdlr = make(tmp_path)
    run(hdlr, ["--skip", "sheets,portal", "--prefix", str(tmp_path / "logs" / "b1")])
    rec = read_run(str(tmp_path / "logs" / "b1"))
    assert rec["command"] == "build" and rec["exit"] == 0 and rec["workers"] == 0
    assert rec["phases"] == ["reconcile", "identify", "merge", "export"] and rec["current"] == "export"
    assert [(s["phase"], s["command"], s["exit"]) for s in rec["steps"]] == [
        ("reconcile", "reconcile", 0), ("identify", "identify", 0), ("identify", "metatypes", 0),
        ("identify", "write-refs", 0), ("merge", "merge", 0), ("export", "export", 0)]
    assert rec["steps"][4]["prefix"] == str(tmp_path / "logs" / "b1" / "merge")
    assert rec["steps"][4]["argv"][:2] == ["lux", "merge"]
    assert all(s["started"] <= s["finished"] for s in rec["steps"])


def test_a_failed_step_is_in_the_record(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    hdlr = make(tmp_path, {"merge": 3})
    with pytest.raises(RuntimeError):
        run(hdlr, ["--skip", "sheets", "--prefix", str(tmp_path / "logs" / "b2")])
    rec = read_run(str(tmp_path / "logs" / "b2"))
    assert rec["exit"] == 1 and "build stopped in merge" in rec["error"]
    assert [(s["phase"], s["exit"]) for s in rec["steps"]] == [
        ("reconcile", 0), ("identify", 0), ("identify", 0), ("identify", 0), ("merge", 3)]
    assert rec["current"] == "merge"


def test_every_worker_phase_gets_a_prefix_without_one_given(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    hdlr = make(tmp_path)
    run(hdlr, ["--clear"])
    base = hdlr.build_prefix
    assert base.startswith(os.path.join(hdlr.configs.log_dir, "build-"))
    assert read_run(base)["exit"] == 0
    by = {}
    for a in hdlr.ran:
        by.setdefault(name_of(a), a)
    for phase in ("reconcile", "merge", "export", "sheets", "portal"):
        assert by[phase][by[phase].index("--prefix") + 1] == os.path.join(base, phase)
    # every step has a prefix, so `lux watch <build>` can follow each in
    # turn; the clear phase's three steps each get their own
    clears = [s for s in read_run(base)["steps"] if s["phase"] == "clear"]
    assert [s["command"] for s in clears] == ["clear", "clear", "new-token"]
    assert [s["prefix"] for s in clears] == [os.path.join(base, "clear"), os.path.join(base, "clear-refs"), os.path.join(base, "new-token")]
    assert all(s["prefix"] for s in read_run(base)["steps"])
    # so do the identify phase's, and the build's record says identify
    # while they run, not reconcile
    idents = [s for s in read_run(base)["steps"] if s["phase"] == "identify"]
    assert [s["command"] for s in idents] == ["identify", "metatypes", "write-refs"]
    assert [s["prefix"] for s in idents] == [os.path.join(base, "identify"), os.path.join(base, "metatypes"),
                                             os.path.join(base, "write-refs")]
    # and the build logs to its own file
    assert os.path.exists(os.path.join(base, "driver.log"))
    assert "running:" in Path(base, "driver.log").read_text()


def test_each_step_is_told_its_parent(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    hdlr = make(tmp_path)
    seen = []
    hdlr.run_step = lambda argv: seen.append(hdlr.step_env().get("LUX_PARENT_RUN")) or 0
    run(hdlr, ["--to", "reconcile", "--prefix", str(tmp_path / "logs" / "b3")])
    assert seen == [str(tmp_path / "logs" / "b3")] * 2


def test_the_web_tags_the_build_not_its_phases(tmp_path, monkeypatch):
    """LUX_RUN_EXTRA (the web UI's runner) goes into the build's record;
    the phases are not given it, so only the build reads as web-started."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("LUX_RUN_EXTRA", '{"web": true, "stdout": "x.out"}')
    hdlr = make(tmp_path)
    seen = []
    hdlr.run_step = lambda argv: seen.append(hdlr.step_env().get("LUX_RUN_EXTRA")) or 0
    run(hdlr, ["--to", "reconcile", "--prefix", str(tmp_path / "logs" / "b4")])
    rec = read_run(str(tmp_path / "logs" / "b4"))
    assert rec["web"] is True and rec["stdout"] == "x.out" and rec["phases"] == ["sheets", "reconcile"]
    assert seen == [None, None]


def test_a_dry_run_writes_no_record(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    hdlr = make(tmp_path)
    run(hdlr, ["--dry-run"])
    assert list(Path(hdlr.configs.log_dir).rglob("run.json")) == []


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


def test_the_identify_phase_steps_are_one_shot(tmp_path, monkeypatch):
    """identify, metatypes and write-refs run in one process each: the log
    level and a prefix, no worker options."""
    monkeypatch.chdir(tmp_path)
    hdlr = make(tmp_path)
    run(hdlr, ["--from", "identify", "--to", "identify", "--max-workers", "8", "--engine", "mp",
               "--no-ui", "--prefix", "/logs/b2", "--log", "DEBUG"])
    assert [a[3:] for a in hdlr.ran] == [
        ["identify", "--log", "DEBUG", "--prefix", "/logs/b2/identify"],
        ["metatypes", "--log", "DEBUG", "--prefix", "/logs/b2/metatypes"],
        ["write-refs", "--log", "DEBUG", "--prefix", "/logs/b2/write-refs"]]


def test_a_failed_identify_stops_before_metatypes(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    hdlr = make(tmp_path, {"identify": 1})
    with pytest.raises(RuntimeError, match=r"build stopped in identify"):
        run(hdlr, ["--skip", "sheets"])
    assert [name_of(a) for a in hdlr.ran] == ["reconcile", "identify"]
