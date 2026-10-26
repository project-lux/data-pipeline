"""`IndexLoadManager` and `lux load-index`: one worker per source, the
legacy load() dispatch, failure counting, and the command end to end on
the null engine."""

import sys
from argparse import Namespace
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from lux_pipeline.cli.load_index import CommandHandler
from lux_pipeline.process.base.index_loader import IndexLoader
from lux_pipeline.process.tasks import _task_manager
from lux_pipeline.process.tasks.index_load_task import (
    IndexLoadManager, overrides_load, takes_no_arguments,
)
from lux_pipeline.progress import null_progress


# --- stubs -------------------------------------------------------------------


class FakeIndexLoader(IndexLoader):
    """Uses the base load(); records prepare/process."""

    def __init__(self, name, fail=False, names=5, uris=2):
        super().__init__({"name": name})
        self.fail = fail
        self.counts = {"records": 10, "names": names, "uris": uris}
        self.calls = []

    def prepare(self, my_slice=-1, max_slice=-1, progress=null_progress):
        self.calls.append(("prepare", my_slice, max_slice, progress))
        return super().prepare(my_slice, max_slice, progress)

    def process(self, overwrite=True):
        if self.fail:
            raise OSError(f"{self.name}: lmdb full")
        self.calls.append(("process", overwrite))
        return dict(self.counts)

    def names(self):
        return [c[0] for c in self.calls]


class LegacyIndexLoader(FakeIndexLoader):
    """Overrides load() with no arguments, like viaf and wikidata."""

    def load(self):
        self.calls.append(("load",))
        return None


class NeedsArgsIndexLoader(FakeIndexLoader):
    """Overrides load() with required arguments, like the global loader."""

    def load(self, filename, which="equivs"):
        self.calls.append(("load", filename, which))


class StubConfigs:
    max_workers = 4
    log_dir = None

    def __init__(self, **loaders):
        """``name=loader``; ``None`` for no indexLoader; ``"junk"`` for a
        non-IndexLoader value. Names starting with ``y`` are internal."""
        self.internal, self.external, self.results = {}, {}, {}
        for name, ldr in loaders.items():
            cfg = {"name": name, "indexLoader": object() if ldr == "junk" else ldr}
            (self.internal if name.startswith("y") else self.external)[name] = cfg

    def ldr(self, name):
        for group in (self.internal, self.external):
            if name in group:
                return group[name]["indexLoader"]
        raise KeyError(name)


def manager(cfgs, sources, workers=2, **options):
    m = IndexLoadManager(cfgs, workers, Namespace(**options))
    for s in sources:
        m.prepare_single(s)
    return m


@pytest.fixture
def stub_runtime(monkeypatch):
    def use(cfgs):
        monkeypatch.setattr(_task_manager.runtime, "get_configs", lambda: cfgs)
        return cfgs
    return use


# --- which sources -----------------------------------------------------------


def test_only_index_loader_instances_are_added_once():
    cfgs = StubConfigs(aat=FakeIndexLoader("aat"), tgn=None, ror="junk", ils=FakeIndexLoader("ils"))
    m = manager(cfgs, ["aat", "tgn", "ror", "ils", "aat"])
    assert m.sources == [("external", "aat"), ("external", "ils")]


# --- distribution and dispatch -----------------------------------------------


def test_each_worker_takes_every_max_workers_th_source():
    cfgs = StubConfigs(aat=FakeIndexLoader("aat"), ulan=FakeIndexLoader("ulan"), lcnaf=FakeIndexLoader("lcnaf"))
    m = manager(cfgs, ["aat", "ulan", "lcnaf"], workers=2)
    assert m.run_single(0) == {"sources": 2, "names": 10, "uris": 4, "failed": 0}
    assert cfgs.ldr("aat").calls == [("prepare", 0, 1, null_progress), ("process", True)]
    assert cfgs.ldr("lcnaf").names() == ["prepare", "process"]
    assert cfgs.ldr("ulan").calls == []
    assert m.run_single(1) == {"sources": 1, "names": 5, "uris": 2, "failed": 0}
    assert cfgs.ldr("ulan").names() == ["prepare", "process"]


def test_prepare_gets_the_progress_callable_and_a_single_slice():
    seen = []

    def progress(**kw):
        seen.append(kw)

    cfgs = StubConfigs(aat=FakeIndexLoader("aat"))
    manager(cfgs, ["aat"], workers=3)._distributed(0, cfgs, progress)
    assert cfgs.ldr("aat").calls[0] == ("prepare", 0, 1, progress)
    assert seen[0] == {"phase": "index", "description": "aat"}


def test_no_overwrite_reaches_process():
    cfgs = StubConfigs(aat=FakeIndexLoader("aat"))
    manager(cfgs, ["aat"], workers=1, overwrite=False).run_single(0)
    assert ("process", False) in cfgs.ldr("aat").calls


def test_legacy_load_override_is_called_instead_of_process():
    cfgs = StubConfigs(viaf=LegacyIndexLoader("viaf"))
    m = manager(cfgs, ["viaf"], workers=1)
    assert m.run_single(0) == {"sources": 1, "names": 0, "uris": 0, "failed": 0}
    assert cfgs.ldr("viaf").names() == ["prepare", "load"]


def test_a_load_that_needs_arguments_is_a_counted_failure(caplog):
    cfgs = StubConfigs(merged=NeedsArgsIndexLoader("merged"), aat=FakeIndexLoader("aat"))
    m = manager(cfgs, ["merged", "aat"], workers=1)
    with caplog.at_level("ERROR", logger="lux_pipeline"):
        assert m.run_single(0) == {"sources": 1, "names": 5, "uris": 2, "failed": 1}
    assert "merged: index build failed in worker 0: TypeError: NeedsArgsIndexLoader.load() needs arguments" in caplog.text
    assert cfgs.ldr("aat").names() == ["prepare", "process"]


def test_overrides_load_and_takes_no_arguments():
    assert not overrides_load(FakeIndexLoader("a"))
    assert overrides_load(LegacyIndexLoader("b"))
    assert takes_no_arguments(LegacyIndexLoader("b").load)
    assert not takes_no_arguments(NeedsArgsIndexLoader("c").load)
    assert not takes_no_arguments(len)


def test_a_failing_source_is_counted_and_the_rest_still_build(caplog):
    cfgs = StubConfigs(aat=FakeIndexLoader("aat", fail=True), ulan=FakeIndexLoader("ulan"))
    m = manager(cfgs, ["aat", "ulan"], workers=1)
    with caplog.at_level("ERROR", logger="lux_pipeline"):
        assert m.run_single(0) == {"sources": 1, "names": 5, "uris": 2, "failed": 1}
    assert "aat: index build failed in worker 0: OSError: aat: lmdb full" in caplog.text
    assert cfgs.ldr("ulan").names() == ["prepare", "process"]


def test_an_idle_worker_reports_zero():
    cfgs = StubConfigs(aat=FakeIndexLoader("aat"))
    m = manager(cfgs, ["aat"], workers=4)
    assert m.run_single(3) == {"sources": 0, "names": 0, "uris": 0, "failed": 0}
    assert list(m.idle_workers()) == [1, 2, 3]
    assert IndexLoadManager.hide_idle_workers is True


# --- the driver --------------------------------------------------------------


def test_process_caps_workers_at_the_number_of_sources(stub_runtime, caplog):
    cfgs = stub_runtime(StubConfigs(aat=FakeIndexLoader("aat")))
    m = manager(cfgs, ["aat"], workers=4)
    with caplog.at_level("INFO", logger="lux_pipeline"):
        results = m.process("null")
    assert m.max_workers == 1
    assert results == m.results == [{"sources": 1, "names": 5, "uris": 2, "failed": 0}]
    assert m.summary() == {"sources": 1, "names": 5, "uris": 2, "failed": 0}
    assert "1 source(s) to index: using 1 of 4 workers" in caplog.text


def test_process_refuses_with_nothing_to_index():
    with pytest.raises(RuntimeError, match="nothing to index"):
        manager(StubConfigs(tgn=None), ["tgn"]).process("null")


def test_defaults_without_args():
    m = IndexLoadManager(StubConfigs(), 2)
    assert (m.overwrite, m.results) == (True, None)


# --- the command -------------------------------------------------------------


def run_command(cfgs, rest, tmp_path, source="all", workers=1):
    args = Namespace(command="load-index", source=source, max_workers=workers,
                     engine="null", no_ui=True, prefix=str(tmp_path / "load-index"),
                     log="INFO", max_records=-1, my_worker=-1)
    return CommandHandler(cfgs).process(args, list(rest))


def test_command_builds_and_logs_a_summary(tmp_path, stub_runtime, caplog):
    cfgs = stub_runtime(StubConfigs(aat=FakeIndexLoader("aat"), ulan=FakeIndexLoader("ulan", names=1, uris=0)))
    with caplog.at_level("INFO", logger="lux_pipeline"):
        m = run_command(cfgs, ["--no-overwrite"], tmp_path)
    assert isinstance(m, IndexLoadManager) and m.overwrite is False
    assert m.summary() == {"sources": 2, "names": 6, "uris": 2, "failed": 0}
    assert "indexed 2 source(s): 6 names, 2 uris, 0 failed" in caplog.text
    assert (tmp_path / "load-index-w0.status").exists()


def test_command_fails_at_the_end_when_a_source_failed(tmp_path, stub_runtime):
    cfgs = stub_runtime(StubConfigs(aat=FakeIndexLoader("aat", fail=True), ulan=FakeIndexLoader("ulan")))
    with pytest.raises(RuntimeError, match="1 index build\\(s\\) failed"):
        run_command(cfgs, [], tmp_path)
    assert cfgs.ldr("ulan").names() == ["prepare", "process"]


def test_command_with_nothing_to_index_is_an_error(tmp_path):
    with pytest.raises(RuntimeError, match="nothing to index"):
        run_command(StubConfigs(tgn=None), [], tmp_path)


def test_command_is_listed_by_help_with_a_dash():
    from lux_pipeline.cli.help import list_commands
    assert ("load-index", "Build each source's reconciliation indexes from its datacache.") in list_commands()
