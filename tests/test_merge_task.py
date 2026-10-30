"""`MergeManager` and `lux merge`: what each worker asks the phase to do
(its own slice, the same source list, an explicit record list unsliced on
one worker with no references pass), the options, the selection rules and
the checks made in the driver before any worker starts, and the command
end to end on the null engine. `merge_slice` is a recorder throughout."""

import sys
from argparse import Namespace
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from lux_pipeline.cli.merge import CommandHandler
from lux_pipeline.process.tasks import _task_manager, merge_task
from lux_pipeline.process.tasks._task_manager import WorkerFailed
from lux_pipeline.process.tasks.merge_task import MergeManager
from lux_pipeline.progress import StatusWriter, null_progress


# --- stubs -------------------------------------------------------------------


class StubConfigs:
    """Two internal and two external sources (the order is the
    configuration's), the directories, a token file and an idmap."""

    max_workers = 4

    def __init__(self, tmp_path, token="__20261027__", metatypes=True):
        self.data_dir = str(tmp_path / "data")
        self.log_dir = str(tmp_path / "logs")
        Path(self.data_dir).mkdir(parents=True, exist_ok=True)
        Path(self.log_dir).mkdir(parents=True, exist_ok=True)
        if token is not None:
            (Path(self.data_dir) / "idmap_update_token.txt").write_text(f"{token}\n")
        if metatypes:
            (Path(self.data_dir) / "metatypes.json").write_text("{}")
        self.internal = {n: {"name": n} for n in ("ypm", "ycba")}
        self.external = {n: {"name": n} for n in ("aat", "wikidata")}
        self.results = {"merged": {"name": "merged"}}
        self.idmap = object()

    def get_idmap(self):
        return self.idmap


@pytest.fixture
def harness(tmp_path, monkeypatch):
    """A configuration, `merge_slice` replaced by a recorder, the working
    directory a fresh one holding ``reference_uris.txt``, and
    `runtime.get_configs` pointed at the configuration so the null engine's
    worker finds it."""
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    monkeypatch.chdir(cwd)
    (cwd / "reference_uris.txt").write_text("0|yuid|uri\n")

    class Harness:
        def __init__(self):
            self.cwd = cwd
            self.cfgs = None
            self.calls = []
            self.fail = False

        def configure(self, **kw):
            self.cfgs = StubConfigs(tmp_path, **kw)
            monkeypatch.setattr(_task_manager.runtime, "get_configs", lambda: self.cfgs)
            return self.cfgs

        def patch(self):
            def merge_slice(cfgs, idmap, to_do, my_slice=-1, max_slice=-1, recids=(),
                            do_references=True, resume=False, progress=null_progress,
                            write_flag=True):
                self.calls.append(dict(cfgs=cfgs, idmap=idmap, to_do=to_do, my_slice=my_slice,
                                       max_slice=max_slice, recids=list(recids),
                                       do_references=do_references, resume=resume,
                                       progress=progress, write_flag=write_flag))
                if self.fail:
                    raise RuntimeError("final transform raised")
                return {"records": {"records": 3}, "references": None}

            monkeypatch.setattr(merge_task, "merge_slice", merge_slice)

        def manager(self, sources=("ypm", "ycba"), workers=4, **options):
            if self.cfgs is None:
                self.configure()
            self.patch()
            m = MergeManager(self.cfgs, workers, Namespace(**options))
            for s in sources:
                m.prepare_single(s)
            return m

        def todo(self, i=0):
            return [name for name, _cfg in self.calls[i]["to_do"]]

        def slices(self):
            return [(c["my_slice"], c["max_slice"]) for c in self.calls]

    return Harness()


# --- the worker --------------------------------------------------------------


def test_run_single_calls_the_phase_with_slice_0_of_1(harness):
    h = harness
    m = h.manager(workers=1)
    result = m.run_single()
    call = h.calls[0]
    assert (call["my_slice"], call["max_slice"]) == (0, 1)
    assert h.todo() == ["ypm", "ycba"]                 # configuration order
    assert call["cfgs"] is h.cfgs
    assert call["idmap"] is h.cfgs.idmap
    assert call["recids"] == []
    assert (call["do_references"], call["resume"]) == (True, False)
    assert call["progress"] is null_progress
    assert call["write_flag"] is True
    assert result == {"records": {"records": 3}, "references": None}


def test_every_worker_gets_its_own_slice_of_the_same_sources(harness):
    h = harness
    m = h.manager(workers=4)
    for n in range(4):
        m._distributed(n, h.cfgs, null_progress)
    assert h.slices() == [(0, 4), (1, 4), (2, 4), (3, 4)]
    assert all(h.todo(i) == h.todo(0) for i in range(4))
    assert all(c["do_references"] for c in h.calls)


def test_process_keeps_the_results(harness):
    h = harness
    m = h.manager(workers=1)
    results = m.process("null")
    assert results == m.results == [{"records": {"records": 3}, "references": None}]


def test_my_worker_runs_only_that_slice(harness):
    h = harness
    m = h.manager(workers=4, my_worker=2)
    m.process("null")
    assert h.slices() == [(2, 4)]


def test_resume_and_no_refs_reach_the_phase(harness):
    h = harness
    m = h.manager(workers=1, resume=True, no_refs=True)
    m.run_single()
    assert (h.calls[0]["resume"], h.calls[0]["do_references"]) == (True, False)


def test_a_named_external_source(harness):
    h = harness
    m = h.manager(sources=("aat",), workers=1)
    m.run_single()
    assert h.todo() == ["aat"]


def test_only_refs_selects_no_source_and_keeps_the_slices(harness):
    h = harness
    m = h.manager(workers=2, only_refs=True)
    for n in range(2):
        m._distributed(n, h.cfgs, null_progress)
    assert h.slices() == [(0, 2), (1, 2)]
    assert all(c["to_do"] == [] and c["do_references"] for c in h.calls)


def test_the_results_group_is_rejected(harness):
    h = harness
    m = h.manager(sources=())
    with pytest.raises(ValueError, match="not merged"):
        m.prepare_single("merged")


def test_a_source_is_added_once():
    m = MergeManager.__new__(MergeManager)
    m.sources = []
    m.maybe_add("internal", {"name": "ycba"})
    m.maybe_add("internal", {"name": "ycba"})
    assert m.sources == [("internal", "ycba")]


def test_a_worker_failure_is_raised(harness):
    h = harness
    m = h.manager(workers=1)
    h.fail = True
    with pytest.raises(WorkerFailed):
        m.process("null")


# --- explicit record lists ---------------------------------------------------


def test_recids_run_one_worker_unsliced_without_references(harness, caplog):
    h = harness
    with caplog.at_level("INFO", logger="lux_pipeline"):
        m = h.manager(sources=("ycba",), workers=4, recid=" obj/1, obj/2 ")
        assert m.max_workers == 1
        assert m.recids == ["obj/1", "obj/2"]
        assert m.hide_idle_workers
        m.process("null")
    assert "one worker for --recid" in caplog.text
    assert "Not running the references pass for --recid" in caplog.text
    assert h.slices() == [(-1, -1)]
    call = h.calls[0]
    assert h.todo() == ["ycba"]
    assert call["recids"] == ["obj/1", "obj/2"]
    assert call["do_references"] is False


def test_recids_need_no_references_file(harness):
    h = harness
    (h.cwd / "reference_uris.txt").unlink()
    m = h.manager(sources=("ycba",), workers=1, recid="obj/1")
    m.process("null")
    assert len(h.calls) == 1


# --- checks in the driver ----------------------------------------------------


@pytest.mark.parametrize("sources, options, match", [
    (("ycba", "ypm"), dict(recid="obj/1"), "single source"),
    ((), {}, "No source"),
    (("ycba",), dict(only_refs=True, recid="obj/1"), "merge nothing"),
    (("ycba",), dict(only_refs=True, no_refs=True), "merge nothing"),
])
def test_bad_selections_are_rejected_before_any_worker(harness, sources, options, match):
    h = harness
    m = h.manager(sources=sources, workers=1, **options)
    with pytest.raises(ValueError, match=match):
        m.process("null")
    assert h.calls == []


@pytest.mark.parametrize("token, match", [(None, "No update token"),
                                          ("20261027", "badly formed")])
def test_an_unusable_token_is_rejected_before_any_worker(harness, token, match):
    h = harness
    h.configure(token=token)
    m = h.manager(workers=1)
    with pytest.raises(ValueError, match=match):
        m.process("null")
    assert h.calls == []


def test_no_references_file_is_rejected_before_any_worker(harness):
    h = harness
    (h.cwd / "reference_uris.txt").unlink()
    m = h.manager(workers=2)
    with pytest.raises(ValueError, match="No reference_uris.txt in .*lux write-refs"):
        m.process("null")
    assert h.calls == []
    m = h.manager(workers=2, only_refs=True)
    with pytest.raises(ValueError, match="No reference_uris.txt"):
        m.process("null")


def test_no_references_file_is_fine_without_the_references_pass(harness):
    h = harness
    (h.cwd / "reference_uris.txt").unlink()
    m = h.manager(workers=1, no_refs=True)
    m.process("null")
    assert len(h.calls) == 1


def test_no_metatypes_is_only_a_warning(harness, caplog):
    h = harness
    h.configure(metatypes=False)
    m = h.manager(workers=1)
    with caplog.at_level("WARNING", logger="lux_pipeline"):
        m.process("null")
    assert "metatypes.json: the merged records will have no metatypes" in caplog.text
    assert len(h.calls) == 1


# --- the command -------------------------------------------------------------


def run_command(cfgs, rest, tmp_path, source="all", workers=1, my_worker=-1):
    args = Namespace(command="merge", source=source, max_workers=workers,
                     engine="null", no_ui=True, prefix=str(tmp_path / "merge"),
                     log="INFO", max_records=-1, my_worker=my_worker)
    return CommandHandler(cfgs).process(args, list(rest))


def test_source_all_is_every_internal_source(tmp_path):
    cfgs = StubConfigs(tmp_path)
    h = CommandHandler(cfgs)
    assert h.resolve_sources(Namespace(source="all")) == ["ypm", "ycba"]
    assert h.resolve_sources(Namespace(source=None)) == ["ypm", "ycba"]
    assert h.resolve_sources(Namespace(source="external")) == ["aat", "wikidata"]
    assert h.resolve_sources(Namespace(source="aat,ycba")) == ["aat", "ycba"]
    with pytest.raises(ValueError, match="Unknown source: nope"):
        h.resolve_sources(Namespace(source="nope"))


def test_command_full_run(harness, tmp_path, capsys):
    h = harness
    h.configure()
    h.patch()
    m = run_command(h.cfgs, [], tmp_path, workers=1)
    assert isinstance(m, MergeManager)
    assert m.sources == [("internal", "ypm"), ("internal", "ycba")]
    call = h.calls[0]
    assert (call["my_slice"], call["max_slice"], call["do_references"]) == (0, 1, True)
    assert isinstance(call["progress"], StatusWriter)
    assert "status/log prefix:" in capsys.readouterr().out
    assert (tmp_path / "merge-w0.status").exists()


def test_command_options(harness, tmp_path):
    h = harness
    h.configure()
    h.patch()
    m = run_command(h.cfgs, ["--norefs", "--resume"], tmp_path, workers=2)
    assert (m.no_refs, m.resume, m.only_refs) == (True, True, False)
    assert all(c["do_references"] is False and c["resume"] for c in h.calls)


def test_command_only_refs(harness, tmp_path):
    h = harness
    h.configure()
    h.patch()
    m = run_command(h.cfgs, ["--onlyrefs"], tmp_path, workers=2)
    assert m.only_refs
    assert all(c["to_do"] == [] for c in h.calls)


def test_command_recid_run(harness, tmp_path):
    h = harness
    h.configure()
    h.patch()
    m = run_command(h.cfgs, ["--recid", "obj/1,obj/2"], tmp_path, source="ycba", workers=4)
    assert m.recids == ["obj/1", "obj/2"] and m.max_workers == 1
    assert h.slices() == [(-1, -1)]
    assert h.calls[0]["do_references"] is False


def test_command_recid_with_two_sources_fails(harness, tmp_path):
    h = harness
    h.configure()
    h.patch()
    with pytest.raises(ValueError, match="single source"):
        run_command(h.cfgs, ["--recid", "obj/1"], tmp_path, source="ycba,ypm")
    assert h.calls == []


def test_command_my_worker(harness, tmp_path):
    h = harness
    h.configure()
    h.patch()
    run_command(h.cfgs, [], tmp_path, workers=24, my_worker=3)
    assert h.slices() == [(3, 24)]


def test_command_is_listed_by_help():
    from lux_pipeline.cli.help import list_commands
    assert ("merge", "Merge the sources' records into one record per identity, then the "
                     "references.") in list_commands()


# --- what an earlier run left ------------------------------------------------


@pytest.mark.parametrize("options,cleared", [({}, True), ({"my_worker": 1}, False),
                                             ({"recid": "a"}, False)])
def test_done_flags_are_cleared_by_a_full_run(harness, options, cleared):
    h = harness
    cfgs = h.configure()
    flags = Path(cfgs.log_dir) / "flags"
    flags.mkdir()
    for fn in ("merge_is_done-0.txt", "export_is_done-0.txt"):
        (flags / fn).write_text("1")
    m = h.manager(sources=["ycba"], workers=1 if "recid" in options else 2, **options)
    m.clear_stale()
    assert (flags / "merge_is_done-0.txt").exists() is not cleared
    assert (flags / "export_is_done-0.txt").exists()
