"""`lux build`: the phases run-all.sh runs, in its order, each as its own
process. The plan from --clear / --from / --to / --skip, the global options
each step is given, the housekeeping run-all.sh did with rm, stopping at
the first failure and the status-file lines -- with the steps stubbed, so
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
    command or script name to its exit status."""
    hdlr = build.CommandHandler(Cfgs(tmp_path))
    hdlr.ran = []

    def run_step(argv):
        hdlr.ran.append(argv)
        return (results or {}).get(name_of(argv), 0)

    hdlr.run_step = run_step
    return hdlr


def name_of(argv):
    if argv[1] == "-m":
        return argv[3]
    return os.path.basename(argv[1])


def run(hdlr, argv):
    args, rest = parse(argv)
    return hdlr.process(args, rest)


# --- the plan ----------------------------------------------------------------


def test_default_plan_is_run_all_order(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    hdlr = make(tmp_path)
    run(hdlr, [])
    assert [name_of(a) for a in hdlr.ran] == [
        "google-sames-diffs.py", "load-csv-map2.py", "load-csv-map2.py",
        "reconcile", "merge", "post-build-portal.py", "export"]
    reconcile = next(a for a in hdlr.ran if name_of(a) == "reconcile")
    assert reconcile[4:6] == ["--source", "all"]
    # reconcile runs its finishing steps: identify, metatypes, write-refs
    assert "--no-finish" not in reconcile
    export = next(a for a in hdlr.ran if name_of(a) == "export")
    assert "--source" not in export
    same = hdlr.ran[1]
    assert same[2:] == ["--same", os.path.join(hdlr.configs.data_dir, "sameAs", "google.csv")]
    assert all(os.path.exists(a[1]) for a in hdlr.ran if a[1] != "-m"), \
        "the legacy scripts are found in the repository"


def test_clear_runs_first(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    hdlr = make(tmp_path)
    run(hdlr, ["--clear", "--to", "reconcile", "--skip", "google"])
    assert [a[3:] for a in hdlr.ran[:3]] == [
        ["clear", "--source", "all", "--force"], ["clear", "--refs", "--force"], ["new-token"]]
    assert [name_of(a) for a in hdlr.ran[3:]] == ["reconcile"]


@pytest.mark.parametrize("argv,phases", [
    (["--from", "merge"], ["merge", "portal", "export"]),
    (["--to", "reconcile"], ["google", "reconcile"]),
    (["--from", "merge", "--to", "merge"], ["merge"]),
    (["--skip", "google,portal"], ["reconcile", "merge", "export"]),
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
               "--skip", "google,portal"])
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


# --- housekeeping, failure, status -------------------------------------------


def test_removes_stale_slice_files(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    hdlr = make(tmp_path)
    (tmp_path / "metatypes-3.json").write_text("{}")
    (tmp_path / "metatypes.json").write_text("{}")
    exports = Path(hdlr.configs.exports_dir)
    for fn in ("export_full_0.jsonl", "export_full_23.jsonl.gz", "export_merged_0.jsonl", "keep.txt"):
        (exports / fn).write_text("")
    run(hdlr, ["--skip", "google,portal"])
    assert not (tmp_path / "metatypes-3.json").exists()
    assert (tmp_path / "metatypes.json").exists()
    assert sorted(os.listdir(exports)) == ["export_merged_0.jsonl", "keep.txt"]


def test_failure_stops_and_is_recorded(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    hdlr = make(tmp_path, {"merge": 3})
    with pytest.raises(RuntimeError, match=r"build stopped in merge: .* exited with 3"):
        run(hdlr, ["--skip", "google"])
    assert [name_of(a) for a in hdlr.ran] == ["reconcile", "merge"]
    status = Path(hdlr.configs.log_dir, "pipeline_process_status.txt").read_text()
    assert status.strip().endswith("[Error] Error in merge")


def test_success_is_recorded_in_given_status_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    hdlr = make(tmp_path)
    status = tmp_path / "status.txt"
    run(hdlr, ["--skip", "google", "--status-file", str(status)])
    assert status.read_text().strip().endswith("[Success] Build was successful")


def test_dry_run_runs_and_removes_nothing(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    hdlr = make(tmp_path)
    (tmp_path / "metatypes-0.json").write_text("{}")
    run(hdlr, ["--dry-run", "--clear"])
    out = capsys.readouterr().out
    assert hdlr.ran == []
    assert (tmp_path / "metatypes-0.json").exists()
    assert "[clear]" in out and "new-token" in out and "[export] remove export_full_" in out
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
