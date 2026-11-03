"""`LoadManager` and `lux load`: the dispatch rules per loader kind (DSL,
sliced legacy, plain legacy, export), the options, --clear in the driver,
failure handling, and the command end to end on the null engine."""

import sys
from argparse import Namespace
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from lux_pipeline.cli.load import CommandHandler
from lux_pipeline.process.base.loader import Loader
from lux_pipeline.process.tasks import _task_manager
from lux_pipeline.process.tasks._task_manager import WorkerFailed
from lux_pipeline.process.tasks.load_task import LOAD_TYPES, LoadManager, accepts_slices
from lux_pipeline.progress import StatusWriter, null_progress


# --- stubs -------------------------------------------------------------------


class StubCache:
    def __init__(self):
        self.cleared = 0

    def clear(self):
        self.cleared += 1


class FakeLoader(Loader):
    """Records every call. Like most real source loaders it does not call
    Loader.__init__. `dsl` says whether prepare() finds configured files;
    `export_files` whether it finds them for --type export."""

    def __init__(self, name, dsl=False, export_files=False, fail=False):
        self.name = name
        self.dsl = dsl
        self.export_files = export_files
        self.fail = fail
        self.calls = []
        self.uses_dsl = False
        self.my_files = []

    def prepare(self, my_slice=-1, max_slice=-1, load_type="records", max_records=-1,
                progress=null_progress, path=None, path_type=None):
        self.calls.append(("prepare", my_slice, max_slice, load_type, max_records, progress))
        self.path_args = (path, path_type)
        self.uses_dsl = self.dsl
        if self.dsl and (load_type == "records" or self.export_files):
            self.my_files = [{"path": f"{self.name}.{load_type}", "fmt": [["json"]]}]
        else:
            self.my_files = []
        return self

    def process(self, overwrite=True):
        if self.fail:
            raise OSError(f"{self.name} dump unreadable")
        self.calls.append(("process", overwrite))
        return 1

    def load_export2(self):
        self.calls.append(("load_export2",))

    def load(self):
        if self.fail:
            raise OSError(f"{self.name} dump unreadable")
        self.calls.append(("load",))

    def names(self):
        return [c[0] for c in self.calls]


class SlicedLoader(FakeLoader):
    def load(self, slicen=None, maxSlice=None):
        self.calls.append(("load", slicen, maxSlice))


class RequiredSlicesLoader(FakeLoader):
    def load(self, slice, max_slice):
        self.calls.append(("load", slice, max_slice))


class StubConfigs:
    max_workers = 4
    log_dir = None

    def __init__(self, **loaders):
        """``name=loader``; names starting with ``y`` are internal. A
        source given ``None`` has no loader; ``"junk"`` a non-Loader one."""
        self.internal, self.external, self.results = {}, {}, {}
        for name, ldr in loaders.items():
            if ldr == "junk":
                ldr = object()
            cfg = {"name": name, "datacache": StubCache()}
            if ldr is not None:
                cfg["loader"] = ldr
            (self.internal if name.startswith("y") else self.external)[name] = cfg

    def loader(self, name):
        for group in (self.internal, self.external):
            if name in group:
                return group[name]["loader"]
        raise KeyError(name)

    def cache(self, name):
        for group in (self.internal, self.external):
            if name in group:
                return group[name]["datacache"]
        raise KeyError(name)


def manager(cfgs, sources, workers=4, **options):
    args = Namespace(**options)
    m = LoadManager(cfgs, workers, args)
    for s in sources:
        m.prepare_single(s)
    return m


@pytest.fixture
def stub_runtime(monkeypatch):
    """Workers under an engine build their Config through runtime; hand
    them the stub instead."""
    def use(cfgs):
        monkeypatch.setattr(_task_manager.runtime, "get_configs", lambda: cfgs)
        return cfgs
    return use


# --- which sources -----------------------------------------------------------


def test_only_loader_instances_are_added_and_not_twice():
    cfgs = StubConfigs(ror=FakeLoader("ror"), aat=None, tgn="junk", ycba=FakeLoader("ycba"))
    m = manager(cfgs, ["ror", "aat", "tgn", "ycba", "ror"])
    assert m.sources == [("external", "ror"), ("internal", "ycba")]


def test_prepare_all_takes_every_loader():
    cfgs = StubConfigs(ror=FakeLoader("ror"), aat=None, ycba=FakeLoader("ycba"))
    m = LoadManager(cfgs, 1)
    m.prepare_all()
    assert sorted(m.sources) == [("external", "ror"), ("internal", "ycba")]


# --- dispatch ----------------------------------------------------------------


def test_dsl_loader_every_worker_prepares_its_slice_and_processes():
    cfgs = StubConfigs(ror=FakeLoader("ror", dsl=True))
    m = manager(cfgs, ["ror"], workers=4)
    assert m.run_single(2) == ["ror"]
    ldr = cfgs.loader("ror")
    assert ldr.calls == [("prepare", 2, 4, "records", -1, null_progress), ("process", True)]
    m.run_single(0)
    assert ldr.names() == ["prepare", "process", "prepare", "process"]


def test_no_overwrite_reaches_process():
    cfgs = StubConfigs(ror=FakeLoader("ror", dsl=True))
    m = manager(cfgs, ["ror"], overwrite=False)
    m.run_single(0)
    assert ("process", False) in cfgs.loader("ror").calls


def test_max_records_reaches_prepare():
    cfgs = StubConfigs(ror=FakeLoader("ror", dsl=True))
    m = manager(cfgs, ["ror"], max_records=50)
    m.run_single(0)
    assert cfgs.loader("ror").calls[0][4] == 50


def test_sliced_legacy_loader_gets_this_workers_slice():
    cfgs = StubConfigs(wikidata=SlicedLoader("wikidata"))
    m = manager(cfgs, ["wikidata"], workers=4)
    assert m.run_single(3) == ["wikidata"]
    assert cfgs.loader("wikidata").calls[-1] == ("load", 3, 4)
    m.run_single(0)
    assert cfgs.loader("wikidata").calls[-1] == ("load", 0, 4)


def test_sliced_legacy_loader_with_one_worker_is_slice_0_of_1():
    """A loader whose slice arguments are required (japan) still works."""
    cfgs = StubConfigs(japan=RequiredSlicesLoader("japan"))
    m = manager(cfgs, ["japan"], workers=1)
    m.run_single(0)
    assert cfgs.loader("japan").calls[-1] == ("load", 0, 1)


def test_plain_legacy_loader_runs_on_worker_0_only():
    cfgs = StubConfigs(ror=FakeLoader("ror"))
    m = manager(cfgs, ["ror"], workers=4)
    assert m.run_single(1) == []
    assert m.run_single(2) == []
    assert cfgs.loader("ror").names() == ["prepare", "prepare"]
    assert m.run_single(0) == ["ror"]
    assert cfgs.loader("ror").names() == ["prepare", "prepare", "prepare", "load"]


def test_export_uses_load_export2_on_worker_0_only():
    cfgs = StubConfigs(ycba=FakeLoader("ycba", dsl=True), ror=FakeLoader("ror"))
    m = manager(cfgs, ["ycba", "ror"], workers=2, type="export")
    assert m.run_single(1) == []
    assert m.run_single(0) == ["ycba", "ror"]
    for name in ("ycba", "ror"):
        ldr = cfgs.loader(name)
        assert ldr.names() == ["prepare", "prepare", "load_export2"]
        assert ldr.calls[0][3] == "export"


def test_export_described_in_input_files_goes_through_the_dsl():
    cfgs = StubConfigs(ycba=FakeLoader("ycba", dsl=True, export_files=True))
    m = manager(cfgs, ["ycba"], workers=2, type="export")
    assert m.run_single(1) == ["ycba"]
    assert cfgs.loader("ycba").names() == ["prepare", "process"]


def test_lmdb_type_reaches_prepare():
    cfgs = StubConfigs(wikidata=FakeLoader("wikidata", dsl=True))
    m = manager(cfgs, ["wikidata"], type="lmdb")
    m.run_single(0)
    assert cfgs.loader("wikidata").calls[0][3] == "lmdb"


def test_several_sources_in_order_with_progress_description():
    seen = []

    def progress(**kw):
        seen.append(kw)

    cfgs = StubConfigs(ror=FakeLoader("ror", dsl=True), ycba=SlicedLoader("ycba"))
    m = manager(cfgs, ["ror", "ycba"], workers=2)
    assert m._distributed(1, cfgs, progress) == ["ror", "ycba"]
    assert [kw["description"] for kw in seen if "description" in kw] == ["ror", "ycba"]
    # the loaders were handed the same progress callable
    assert cfgs.loader("ror").calls[0][5] is progress


# --- options -----------------------------------------------------------------


def test_defaults_without_args():
    m = LoadManager(StubConfigs(), 2)
    assert (m.load_type, m.overwrite, m.clear, m.max_records) == ("records", True, False, -1)


def test_unknown_type_is_rejected():
    with pytest.raises(ValueError, match="Unknown load type 'tsv'"):
        LoadManager(StubConfigs(), 1, Namespace(type="tsv"))
    assert LOAD_TYPES == ("records", "export", "lmdb")


@pytest.mark.parametrize("fn, expected", [
    (FakeLoader("a").load, False),
    (SlicedLoader("b").load, True),
    (RequiredSlicesLoader("c").load, True),
    (Loader.__new__(Loader).load, False),
    (len, False),
])
def test_accepts_slices(fn, expected):
    assert accepts_slices(fn) is expected


# --- clearing ----------------------------------------------------------------


def test_clear_empties_datacaches_in_the_driver_before_the_workers(stub_runtime):
    cfgs = stub_runtime(StubConfigs(ror=FakeLoader("ror"), ycba=FakeLoader("ycba", dsl=True),
                                    aat=FakeLoader("aat")))
    m = manager(cfgs, ["ror", "ycba"], workers=1, clear=True)
    assert m.process("null") == [["ror", "ycba"]]
    assert (cfgs.cache("ror").cleared, cfgs.cache("ycba").cleared) == (1, 1)
    assert cfgs.cache("aat").cleared == 0


def test_no_clear_by_default_and_never_from_run_single(stub_runtime):
    cfgs = stub_runtime(StubConfigs(ror=FakeLoader("ror")))
    m = manager(cfgs, ["ror"], workers=1)
    m.process("null")
    assert cfgs.cache("ror").cleared == 0
    manager(cfgs, ["ror"], workers=1, clear=True).run_single(0)
    assert cfgs.cache("ror").cleared == 0


def test_clear_skips_a_source_without_a_datacache(caplog):
    cfgs = StubConfigs(ror=FakeLoader("ror"))
    del cfgs.external["ror"]["datacache"]
    m = manager(cfgs, ["ror"], workers=1, clear=True)
    with caplog.at_level("WARNING", logger="lux_pipeline"):
        assert m.clear_datacaches() == []
    assert "ror has no datacache to clear" in caplog.text


# --- failures ----------------------------------------------------------------


def test_a_failing_source_does_not_stop_the_others_and_the_worker_fails(caplog):
    cfgs = StubConfigs(ror=FakeLoader("ror", fail=True), ycba=FakeLoader("ycba", dsl=True))
    m = manager(cfgs, ["ror", "ycba"], workers=1)
    with caplog.at_level("CRITICAL", logger="lux_pipeline"):
        with pytest.raises(RuntimeError, match="load failed for ror in slice 0") as info:
            m.run_single(0)
    assert isinstance(info.value.__cause__, OSError)
    assert cfgs.loader("ycba").names() == ["prepare", "process"]
    assert "Failed to load ror in slice 0: OSError: ror dump unreadable" in caplog.text


def test_failure_surfaces_as_worker_failed_under_an_engine(stub_runtime):
    cfgs = stub_runtime(StubConfigs(ror=FakeLoader("ror", fail=True)))
    m = manager(cfgs, ["ror"], workers=1)
    with pytest.raises(WorkerFailed):
        m.process("null")


# --- the command -------------------------------------------------------------


def run_command(cfgs, rest, tmp_path, source="all", workers=1):
    args = Namespace(command="load", source=source, max_workers=workers,
                     engine="null", no_ui=True, prefix=str(tmp_path / "load"),
                     log="INFO", max_records=-1, my_worker=-1)
    return CommandHandler(cfgs).process(args, list(rest))


def test_command_defaults(tmp_path, stub_runtime, capsys):
    cfgs = stub_runtime(StubConfigs(ror=FakeLoader("ror", dsl=True)))
    m = run_command(cfgs, [], tmp_path, source="ror")
    assert isinstance(m, LoadManager)
    assert (m.load_type, m.overwrite, m.clear) == ("records", True, False)
    calls = cfgs.loader("ror").calls
    assert calls[0][:5] == ("prepare", 0, 1, "records", -1)
    assert isinstance(calls[0][5], StatusWriter)   # the worker's status file
    assert calls[1] == ("process", True)
    assert cfgs.cache("ror").cleared == 0
    assert "status/log prefix:" in capsys.readouterr().out
    assert (tmp_path / "load-w0.status").exists()


def test_command_options(tmp_path, stub_runtime):
    cfgs = stub_runtime(StubConfigs(ror=FakeLoader("ror", dsl=True), ycba=FakeLoader("ycba")))
    m = run_command(cfgs, ["--no-overwrite", "--clear", "--type", "lmdb"], tmp_path)
    assert (m.load_type, m.overwrite, m.clear) == ("lmdb", False, True)
    assert m.sources == [("internal", "ycba"), ("external", "ror")]
    assert cfgs.cache("ror").cleared == 1 and cfgs.cache("ycba").cleared == 1
    assert ("process", False) in cfgs.loader("ror").calls


def test_command_rejects_an_unknown_type(tmp_path):
    with pytest.raises(SystemExit):
        run_command(StubConfigs(ror=FakeLoader("ror")), ["--type", "tsv"], tmp_path)


def test_command_is_listed_by_help():
    from lux_pipeline.cli.help import list_commands
    assert ("load", "Load source dumps into their datacaches.") in list_commands()


def test_real_loader_signatures():
    """The sliced legacy loaders on main are recognised; ror's is not."""
    from lux_pipeline.sources.ror.loader import RorLoader
    from lux_pipeline.sources.wikidata.loader import WdLoader
    assert accepts_slices(WdLoader.__new__(WdLoader).load)
    assert not accepts_slices(RorLoader.__new__(RorLoader).load)


# --- --file / --file-type ----------------------------------------------------


def test_file_reaches_prepare(tmp_path):
    cfgs = StubConfigs(ror=FakeLoader("ror", dsl=True))
    m = manager(cfgs, ["ror"], workers=2, file="/dumps/ror.zip", file_type="zip/json")
    m.run_single()
    assert cfgs.loader("ror").path_args == ("/dumps/ror.zip", "zip/json")


def test_file_needs_exactly_one_source():
    cfgs = StubConfigs(ror=FakeLoader("ror", dsl=True), ycba=FakeLoader("ycba"))
    m = manager(cfgs, ["ror", "ycba"], file="/dumps/x.zip")
    with pytest.raises(ValueError, match="exactly one --source.*ror, ycba"):
        m.process("null")
    assert cfgs.loader("ror").calls == [] and cfgs.loader("ycba").calls == []


def test_file_type_needs_file():
    with pytest.raises(ValueError, match="--file-type needs --file"):
        manager(StubConfigs(), [], file_type="zip/json")


def test_command_file(tmp_path, stub_runtime):
    dump = tmp_path / "ror.zip"
    dump.write_text("")
    cfgs = stub_runtime(StubConfigs(ror=FakeLoader("ror", dsl=True)))
    m = run_command(cfgs, ["--file", str(dump), "--file-type", "zip/json"], tmp_path, source="ror")
    assert (m.file, m.file_type) == (str(dump), "zip/json")
    assert cfgs.loader("ror").path_args == (str(dump), "zip/json")
    with pytest.raises(SystemExit):
        run_command(cfgs, ["--file", str(tmp_path / "missing.zip")], tmp_path, source="ror")
