"""`ReconcileManager` and `lux reconcile`: what each worker asks the phase
to do (its own slice, the same source list, the explicit record list
unsliced on one worker), the selection rules, the token minted before the
workers, the driver tail in run-all.sh's order and every reason it is
skipped, `merge_metatypes` and `new_update_token` for real, and the
command end to end on the null engine."""

import json
import sys
from argparse import Namespace
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from lux_pipeline.cli.reconcile import CommandHandler
from lux_pipeline.process.phases import identify as identify_phase
from lux_pipeline.process.phases import reconcile as phase
from lux_pipeline.process.reference_manager import ReferenceManager
from lux_pipeline.process.tasks import _task_manager, reconcile_task
from lux_pipeline.process.tasks._task_manager import WorkerFailed
from lux_pipeline.process.tasks.reconcile_task import ReconcileManager
from lux_pipeline.progress import StatusWriter, null_progress

AAT = "http://vocab.getty.edu/aat/"


# --- stubs -------------------------------------------------------------------


class Datacache:
    def __init__(self, size):
        self.size = size

    def len_estimate(self):
        return self.size


class Idmap:
    def __init__(self, log, token="__20260101__", optimize=True):
        self.log = log
        self.update_token = token
        if optimize:
            self.optimize = lambda: self.log.append(("optimize",))


class Store:
    def __init__(self, log, name):
        self.log = log
        self.name = name

    def clear(self):
        self.log.append(("clear", self.name))


class Mapper:
    process_langs = {}


class StubConfigs:
    """Two internal and two external sources with sized datacaches, the
    directories, an idmap, the maps the phase and the tail ask for."""

    max_workers = 4
    log_dir = None
    globals_cfg = {"collection": "300025976", "human": "Q5"}

    def __init__(self, tmp_path, log=None, sizes=None, token="__20260101__", optimize=True):
        self.log = [] if log is None else log
        sizes = sizes or {}
        self.data_dir = str(tmp_path / "data")
        self.temp_dir = str(tmp_path / "temp")
        Path(self.data_dir).mkdir(parents=True, exist_ok=True)
        self.internal, self.external, self.results = {}, {}, {}
        for name in ("ycba", "ypm"):
            self.internal[name] = {"name": name, "datacache": Datacache(sizes.get(name, 10)),
                                   "mapper": Mapper()}
        for name in ("aat", "wikidata"):
            self.external[name] = {"name": name, "datacache": Datacache(sizes.get(name, 100)),
                                   "mapper": Mapper()}
        self.results["merged"] = {"name": "merged"}
        self.idmap = Idmap(self.log, token, optimize)
        self.stores = {"networkmap": Store(self.log, "networkmap"),
                       "done_refs": Store(self.log, "done_refs"),
                       "all_refs": Store(self.log, "all_refs")}

    def get_idmap(self):
        return self.idmap

    def instantiate_map(self, name):
        return {"store": self.stores[name]}


def make_tail(log, stats=None):
    """Stub resolver and reference manager for the driver tail, recording
    their calls in `log`."""

    class Resolver:
        def __init__(self, cfgs, idmap, my_slice=-1, max_slice=-1):
            log.append(("IdentityResolver", idmap.update_token))

        def resolve_identity(self):
            log.append(("resolve_identity",))
            return dict(stats or {"nodes": 5, "pairs": 4, "diff_pairs": 1, "clusters": 2,
                                  "conflicts": 0})

    class RefMgr:
        def __init__(self, cfgs, idmap, workers=1):
            log.append(("ReferenceManager",))
            self.done_refs = cfgs.stores["done_refs"]

        def merge_metatypes(self):
            log.append(("merge_metatypes",))
            return {"a": [1], "b": [2]}

        def write_done_refs(self):
            log.append(("write_done_refs",))

    return Resolver, RefMgr


@pytest.fixture
def harness(tmp_path, monkeypatch):
    """A configuration, `reconcile_slice` and the tail replaced by
    recorders, and `runtime.get_configs` pointed at the configuration so
    the null engine's worker finds it."""

    class Harness:
        def __init__(self):
            self.log = []
            self.cfgs = None
            self.slice_calls = []
            self.fail = False

        def configure(self, **kw):
            self.cfgs = StubConfigs(tmp_path, self.log, **kw)
            monkeypatch.setattr(_task_manager.runtime, "get_configs", lambda: self.cfgs)
            return self.cfgs

        def patch(self):
            def reconcile_slice(cfgs, idmap, networkmap, to_do, my_slice=-1, max_slice=-1,
                                do_references=True, progress=null_progress, write_flag=True):
                self.log.append(("reconcile_slice", my_slice, max_slice))
                self.slice_calls.append(dict(cfgs=cfgs, idmap=idmap, networkmap=networkmap,
                                             to_do=to_do, my_slice=my_slice, max_slice=max_slice,
                                             do_references=do_references, progress=progress,
                                             token=idmap.update_token))
                if self.fail:
                    raise RuntimeError("acquire failed")
                return {"records": {"records": 3}, "references": None}

            Resolver, RefMgr = make_tail(self.log)
            monkeypatch.setattr(reconcile_task, "reconcile_slice", reconcile_slice)
            monkeypatch.setattr(identify_phase, "IdentityResolver", Resolver)
            monkeypatch.setattr(reconcile_task, "ReferenceManager", RefMgr)

        def manager(self, sources=("ycba", "ypm"), workers=4, **options):
            if self.cfgs is None:
                self.configure()
            self.patch()
            m = ReconcileManager(self.cfgs, workers, Namespace(**options))
            for s in sources:
                m.prepare_single(s)
            return m

        def names(self):
            return [c[0] for c in self.log]

        def todo(self, i=0):
            return [(name, recids) for name, _cfg, recids in self.slice_calls[i]["to_do"]]

    return Harness()


# --- the worker --------------------------------------------------------------


def test_run_single_calls_the_phase_with_slice_0_of_1_the_sources_in_size_order_and_progress(harness):
    h = harness
    h.configure(sizes={"ycba": 50, "ypm": 5})
    m = h.manager(workers=1)
    result = m.run_single()

    call = h.slice_calls[0]
    assert (call["my_slice"], call["max_slice"]) == (0, 1)
    assert h.todo() == [("ypm", []), ("ycba", [])]       # smallest datacache first
    assert call["cfgs"] is h.cfgs
    assert call["idmap"] is h.cfgs.idmap
    assert call["networkmap"] is h.cfgs.stores["networkmap"]
    assert call["do_references"] is True
    assert call["progress"] is null_progress
    assert result == {"records": {"records": 3}, "references": None}


def test_every_worker_gets_its_own_slice_of_the_same_sources(harness):
    h = harness
    m = h.manager(workers=4)
    for n in range(4):
        m._distributed(n, h.cfgs, null_progress)
    assert [(c["my_slice"], c["max_slice"]) for c in h.slice_calls] == [(0, 4), (1, 4), (2, 4), (3, 4)]
    assert all(h.todo(i) == h.todo(0) for i in range(4))


def test_my_worker_runs_only_that_slice_under_the_null_engine(harness):
    h = harness
    m = h.manager(workers=4, my_worker=2)
    m.process("null")
    assert [(c["my_slice"], c["max_slice"]) for c in h.slice_calls] == [(2, 4)]


def test_named_external_source_and_no_refs(harness):
    h = harness
    m = h.manager(sources=("aat",), workers=1, no_refs=True)
    m.run_single()
    assert h.todo() == [("aat", [])]
    assert h.slice_calls[0]["do_references"] is False


def test_the_results_group_is_rejected(harness):
    h = harness
    m = h.manager(sources=())
    with pytest.raises(ValueError, match="not merged"):
        m.prepare_single("merged")


def test_a_source_is_added_once():
    m = ReconcileManager.__new__(ReconcileManager)
    m.sources = []
    m.maybe_add("internal", {"name": "ycba"})
    m.maybe_add("internal", {"name": "ycba"})
    assert m.sources == [("internal", "ycba")]


# --- explicit record lists ---------------------------------------------------


def test_recids_run_one_worker_unsliced(harness, caplog):
    h = harness
    with caplog.at_level("INFO", logger="lux_pipeline"):
        m = h.manager(sources=("ycba",), workers=4, recid=" obj/1, obj/2 ")
    assert m.max_workers == 1
    assert m.recids == ["obj/1", "obj/2"]
    assert m.hide_idle_workers
    assert "one worker for --recid" in caplog.text
    m.process("null")
    assert [(c["my_slice"], c["max_slice"]) for c in h.slice_calls] == [(-1, -1)]
    assert h.todo() == [("ycba", ["obj/1", "obj/2"])]


def test_recids_with_two_sources_are_rejected_before_any_worker(harness):
    h = harness
    m = h.manager(sources=("ycba", "ypm"), workers=1, recid="obj/1")
    with pytest.raises(ValueError, match="single source"):
        m.process("null")
    assert h.slice_calls == []


def test_no_source_is_rejected_before_any_worker(harness):
    h = harness
    m = h.manager(sources=(), workers=1)
    with pytest.raises(ValueError, match="No source"):
        m.process("null")
    assert h.slice_calls == []


def test_baseline_ignores_the_sources_and_runs_unsliced(harness):
    h = harness
    m = h.manager(sources=("ycba", "ypm"), workers=4, baseline=True, recid="obj/1")
    assert m.max_workers == 1
    m.process("null")
    assert [(c["my_slice"], c["max_slice"]) for c in h.slice_calls] == [(-1, -1)]
    assert h.todo() == [("aat", ["300025976"]), ("wikidata", ["Q5"])]


# --- the token ---------------------------------------------------------------


def test_new_token_is_minted_before_the_workers_and_seen_by_them(harness, monkeypatch):
    h = harness
    h.configure(token="__20260101__")

    def new_update_token(cfgs, idmap):
        h.log.append(("new_token",))
        idmap.update_token = "__20261027__"
        return idmap.update_token

    monkeypatch.setattr(reconcile_task, "new_update_token", new_update_token)
    m = h.manager(workers=1, new_token=True, no_finish=True)
    m.process("null")
    assert h.names()[:2] == ["new_token", "reconcile_slice"]
    assert h.slice_calls[0]["token"] == "__20261027__"


class FixedDatetime:
    """`datetime.datetime` with `now()` pinned."""

    def __init__(self, year, month, day):
        self.ymd = (year, month, day)

    @property
    def datetime(self):
        ymd = self.ymd

        class _dt:
            @staticmethod
            def now():
                return Namespace(year=ymd[0], month=ymd[1], day=ymd[2])

        return _dt


@pytest.mark.parametrize("today, current, expected", [
    ((2026, 10, 27), "__20260101__", "__20261027__"),      # first build of the day
    ((2026, 10, 27), "__20261027__", "__20261027a__"),     # second
    ((2026, 10, 27), "__20261027a__", "__20261027b__"),    # third
    ((2026, 3, 5), "__20260305b__", "__20260305c__"),      # zero-padded month and day
    ((2026, 3, 5), "__20260101__", "__20260305__"),
])
def test_new_update_token_writes_the_file_and_updates_the_idmap(tmp_path, monkeypatch, caplog,
                                                                 today, current, expected):
    cfgs = StubConfigs(tmp_path, token=current)
    monkeypatch.setattr(phase, "datetime", FixedDatetime(*today))
    with caplog.at_level("INFO", logger="lux_pipeline"):
        tok = phase.new_update_token(cfgs, cfgs.idmap)
    assert tok == expected
    assert (Path(cfgs.data_dir) / "idmap_update_token.txt").read_text() == f"{expected}\n"
    assert cfgs.idmap.update_token == expected
    assert f"New update token: {expected}" in caplog.text


# --- the driver tail ---------------------------------------------------------


def test_the_tail_runs_in_order_after_the_workers(harness, caplog):
    h = harness
    m = h.manager(workers=2)
    with caplog.at_level("INFO", logger="lux_pipeline"):
        results = m.process("null")
    assert h.names() == ["reconcile_slice", "IdentityResolver", "resolve_identity", "optimize",
                         "ReferenceManager", "merge_metatypes", "write_done_refs", "clear"]
    assert h.log[-1] == ("clear", "done_refs")
    assert m.finish_stats["metatypes"] == 2
    assert m.finish_stats["identify"]["clusters"] == 2
    assert "seconds" in m.finish_stats["identify"]
    assert "nodes=5 pairs=4 diff_pairs=1 clusters=2" in caplog.text
    assert "conflicts=0" in caplog.text
    assert results == m.results


def test_the_tail_stamps_identity_with_the_new_token(harness, monkeypatch):
    h = harness
    h.configure(token="__20260101__")
    monkeypatch.setattr(reconcile_task, "new_update_token",
                        lambda cfgs, idmap: setattr(idmap, "update_token", "__20261027__"))
    m = h.manager(workers=1, new_token=True)
    m.process("null")
    assert ("IdentityResolver", "__20261027__") in h.log


def test_an_idmap_without_optimize_is_fine(harness):
    h = harness
    h.configure(optimize=False)
    m = h.manager(workers=1)
    m.process("null")
    assert "optimize" not in h.names()
    assert "merge_metatypes" in h.names()


@pytest.mark.parametrize("options, reason", [
    (dict(no_finish=True), "--no-finish"),
    (dict(my_worker=1), "--my-worker 1 is one slice of 4"),
    (dict(recid="obj/1"), "--recid"),
    (dict(baseline=True), "--baseline"),
])
def test_the_tail_is_skipped_for_a_partial_run(harness, caplog, options, reason):
    h = harness
    sources = ("ycba",) if "recid" in options else ("ycba", "ypm")
    m = h.manager(sources=sources, workers=4, **options)
    with caplog.at_level("INFO", logger="lux_pipeline"):
        m.process("null")
    assert "reconcile_slice" in h.names()
    assert not {"IdentityResolver", "merge_metatypes", "write_done_refs"} & set(h.names())
    assert m.finish_stats is None
    assert f"Not finishing the phase ({reason}" in caplog.text


def test_the_tail_does_not_run_after_a_worker_failed(harness):
    h = harness
    m = h.manager(workers=1)
    h.fail = True
    with pytest.raises(WorkerFailed):
        m.process("null")
    assert h.names() == ["reconcile_slice"]
    assert m.finish_stats is None


def test_finish_can_be_called_on_its_own(harness):
    h = harness
    m = h.manager(workers=1)
    stats = m.finish()
    assert h.names() == ["IdentityResolver", "resolve_identity", "optimize", "ReferenceManager",
                         "merge_metatypes", "write_done_refs", "clear"]
    assert stats is m.finish_stats


# --- merge_metatypes, for real -----------------------------------------------


def ref_mgr_for(cfgs):
    """A ReferenceManager without its constructor: merge_metatypes only
    needs the configuration."""
    rm = ReferenceManager.__new__(ReferenceManager)
    rm.configs = cfgs
    return rm


def test_merge_metatypes_unions_the_slices_in_sorted_order_and_deletes_them(tmp_path, monkeypatch):
    cfgs = StubConfigs(tmp_path)
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    monkeypatch.chdir(cwd)
    (cwd / "metatypes-1.json").write_text(json.dumps({f"{AAT}1": ["t1", "t2"], f"{AAT}2": ["t3"]}))
    (cwd / "metatypes-0.json").write_text(json.dumps({f"{AAT}1": ["t2", "t9"]}))
    (cwd / "metatypes-10.json").write_text(json.dumps({f"{AAT}1": ["t0"], f"{AAT}3": ["t4"]}))
    (cwd / "metatypes-single.json").write_text(json.dumps({f"{AAT}4": ["t5"]}))
    (cwd / "other.json").write_text("{}")

    got = ref_mgr_for(cfgs).merge_metatypes()

    # metatypes-0, -1, -10, -single: the first file's lists, then the others'
    # new entries in order of arrival
    assert got == {f"{AAT}1": ["t2", "t9", "t1", "t0"], f"{AAT}2": ["t3"], f"{AAT}3": ["t4"],
                   f"{AAT}4": ["t5"]}
    out = Path(cfgs.data_dir) / "metatypes.json"
    assert json.loads(out.read_text()) == got
    assert sorted(p.name for p in cwd.iterdir()) == ["other.json"]


def test_merge_metatypes_with_no_slices_writes_an_empty_map(tmp_path, monkeypatch):
    cfgs = StubConfigs(tmp_path)
    monkeypatch.chdir(tmp_path)
    assert ref_mgr_for(cfgs).merge_metatypes() == {}
    assert json.loads((Path(cfgs.data_dir) / "metatypes.json").read_text()) == {}


# --- the command -------------------------------------------------------------


def run_command(cfgs, rest, tmp_path, source="all", workers=1, my_worker=-1):
    args = Namespace(command="reconcile", source=source, max_workers=workers,
                     engine="null", no_ui=True, prefix=str(tmp_path / "reconcile"),
                     log="INFO", max_records=-1, my_worker=my_worker)
    return CommandHandler(cfgs).process(args, list(rest))


def test_source_all_is_every_internal_source(tmp_path):
    cfgs = StubConfigs(tmp_path)
    h = CommandHandler(cfgs)
    assert h.resolve_sources(Namespace(source="all")) == ["ycba", "ypm"]
    assert h.resolve_sources(Namespace(source=None)) == ["ycba", "ypm"]
    assert h.resolve_sources(Namespace(source="internal")) == ["ycba", "ypm"]
    assert h.resolve_sources(Namespace(source="external")) == ["aat", "wikidata"]
    assert h.resolve_sources(Namespace(source="aat,ycba")) == ["aat", "ycba"]
    with pytest.raises(ValueError, match="Unknown source: nope"):
        h.resolve_sources(Namespace(source="nope"))


def test_command_full_run_with_the_tail(harness, tmp_path, capsys):
    h = harness
    h.configure()
    h.patch()
    m = run_command(h.cfgs, [], tmp_path, workers=1)
    assert isinstance(m, ReconcileManager)
    assert m.sources == [("internal", "ycba"), ("internal", "ypm")]
    call = h.slice_calls[0]
    assert (call["my_slice"], call["max_slice"], call["do_references"]) == (0, 1, True)
    assert isinstance(call["progress"], StatusWriter)
    assert h.names()[-1] == "clear"
    assert m.finish_stats["metatypes"] == 2
    assert "status/log prefix:" in capsys.readouterr().out
    assert (tmp_path / "reconcile-w0.status").exists()


def test_command_options(harness, tmp_path):
    h = harness
    h.configure()
    h.patch()
    m = run_command(h.cfgs, ["--norefs", "--new-token", "--no-finish"], tmp_path, workers=2)
    assert (m.no_refs, m.new_token, m.no_finish, m.baseline) == (True, True, True, False)
    assert h.slice_calls[0]["do_references"] is False
    assert "merge_metatypes" not in h.names()


def test_command_recid_run(harness, tmp_path):
    h = harness
    h.configure()
    h.patch()
    m = run_command(h.cfgs, ["--recid", "obj/1,obj/2"], tmp_path, source="ycba", workers=4)
    assert m.recids == ["obj/1", "obj/2"]
    assert m.max_workers == 1
    assert [(c["my_slice"], c["max_slice"]) for c in h.slice_calls] == [(-1, -1)]
    assert "merge_metatypes" not in h.names()


def test_command_recid_with_two_sources_fails(harness, tmp_path):
    h = harness
    h.configure()
    h.patch()
    with pytest.raises(ValueError, match="single source"):
        run_command(h.cfgs, ["--recid", "obj/1"], tmp_path, source="ycba,ypm")


def test_command_baseline(harness, tmp_path):
    h = harness
    h.configure()
    h.patch()
    m = run_command(h.cfgs, ["--baseline"], tmp_path, workers=3)
    assert m.baseline and m.max_workers == 1
    assert h.todo() == [("aat", ["300025976"]), ("wikidata", ["Q5"])]


def test_command_is_listed_by_help():
    from lux_pipeline.cli.help import list_commands
    assert ("reconcile", "Reconcile the sources' records and the references they lead to.") \
        in list_commands()
