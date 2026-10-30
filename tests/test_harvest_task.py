"""`HarvestManager` and `lux harvest`: single, two-phase and pages modes
over a fake driver, the idle-worker rules, failure counting, and the
command end to end on the null engine."""

import logging
import sys
from argparse import Namespace
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from lux_pipeline.cli.harvest import CommandHandler, iso8601
from lux_pipeline.process.tasks import _task_manager
from lux_pipeline.process.tasks.harvest_task import HarvestManager
from lux_pipeline.progress import null_progress

IDMAP = {"an": "idmap"}


# --- stubs -------------------------------------------------------------------


class FakeProtocol:
    overwrite = True


class FakeHarvester:
    """Records every call; `fail` names the entry points that raise;
    `records` is what each entry point reports."""

    def __init__(self, name, fail=(), records=3):
        self.name = name
        self.fail = set(fail)
        self.records = records
        self.protocol = FakeProtocol()
        self.calls = []

    def _hit(self, what):
        if what in self.fail:
            raise ConnectionError(f"{self.name}: {what} broke")

    def prepare(self, my_slice=-1, max_slice=-1, progress=null_progress, until=None, idmap=None):
        self.calls.append(("prepare", my_slice, max_slice, progress, until, idmap))
        return self

    def process(self, refsonly=False):
        self._hit("process")
        self.calls.append(("process",))
        return self.records

    def get_record_list(self, until=None):
        self._hit("list")
        self.calls.append(("list",))
        return {f"r{i}": "t" for i in range(self.records)}, {}

    def harvest_from_list(self, my_slice=None, max_slice=None):
        self._hit("fetch")
        self.calls.append(("fetch", my_slice, max_slice))
        return self.records

    def set_endpoint(self, url):
        self.calls.append(("endpoint", url))

    def harvest_pages(self, my_slice, max_slice):
        self._hit("pages")
        self.calls.append(("pages", my_slice, max_slice))
        return None

    def names(self):
        return [c[0] for c in self.calls]


class StubConfigs:
    max_workers = 4
    log_dir = None
    temp_dir = "/tmp"

    def __init__(self, **sources):
        """``name=harvester`` (activitystreams feed); ``name=("pmh", h)``
        for an OAI-PMH feed; ``name=("nofeed", h)`` for a harvester without
        a feed; ``None`` for no harvester. Names starting with ``y`` are
        internal."""
        self.internal, self.external, self.results = {}, {}, {}
        self.idmap_calls = 0
        for name, spec in sources.items():
            cfg = {"name": name}
            if isinstance(spec, tuple):
                kind, h = spec
                cfg["harvester"] = h
                if kind == "pmh":
                    cfg["pmhEndpoint"] = "https://x.org/oai"
            else:
                cfg["harvester"] = spec
                cfg["activitystreams"] = ["https://x.org/as"] if spec is not None else []
            (self.internal if name.startswith("y") else self.external)[name] = cfg

    def get_idmap(self):
        self.idmap_calls += 1
        return IDMAP

    def h(self, name):
        for group in (self.internal, self.external):
            if name in group:
                return group[name]["harvester"]
        raise KeyError(name)


class FakeView:
    def __init__(self):
        self.visible = []

    def refresh(self):
        return False

    def set_visible(self, n, visible):
        self.visible.append((n, visible))


def manager(cfgs, sources, workers=2, **options):
    m = HarvestManager(cfgs, workers, Namespace(**options))
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


def test_only_harvesters_with_a_feed_are_added():
    cfgs = StubConfigs(ycba=FakeHarvester("ycba"), aat=None,
                       lcnaf=("nofeed", FakeHarvester("lcnaf")), oai=("pmh", FakeHarvester("oai")))
    m = manager(cfgs, ["ycba", "aat", "lcnaf", "oai", "ycba"])
    assert m.sources == [("internal", "ycba"), ("external", "oai")]


# --- single mode -------------------------------------------------------------


def test_single_mode_each_worker_takes_every_max_workers_th_source():
    cfgs = StubConfigs(ycba=FakeHarvester("ycba"), ypm=FakeHarvester("ypm"), yuag=FakeHarvester("yuag"))
    m = manager(cfgs, ["ycba", "ypm", "yuag"], workers=2, until="2025-01-01T00:00:00")
    assert m.run_single(0) == {"sources": 2, "records": 6, "failed": 0}
    assert cfgs.h("ycba").calls == [("prepare", 0, 2, null_progress, "2025-01-01T00:00:00", IDMAP), ("process",)]
    assert cfgs.h("yuag").names() == ["prepare", "process"]
    assert cfgs.h("ypm").calls == []
    assert m.run_single(1) == {"sources": 1, "records": 3, "failed": 0}
    assert cfgs.h("ypm").names() == ["prepare", "process"]
    # one idmap per worker run, shared by its drivers
    assert cfgs.idmap_calls == 2


def test_no_overwrite_reaches_the_protocol():
    cfgs = StubConfigs(ycba=FakeHarvester("ycba"))
    manager(cfgs, ["ycba"], workers=1, overwrite=False).run_single(0)
    assert cfgs.h("ycba").protocol.overwrite is False
    manager(cfgs, ["ycba"], workers=1).run_single(0)
    assert cfgs.h("ycba").protocol.overwrite is False   # never set back to True by the manager


def test_an_idle_worker_in_single_mode(caplog):
    cfgs = StubConfigs(ycba=FakeHarvester("ycba"))
    m = manager(cfgs, ["ycba"], workers=4)
    assert m.run_single(2) == {"sources": 0, "records": 0, "failed": 0}
    assert list(m.idle_workers()) == [1, 2, 3]
    assert m.hide_idle_workers is True
    assert cfgs.idmap_calls == 0


def test_progress_gets_the_phase_and_source():
    seen = []
    cfgs = StubConfigs(ycba=FakeHarvester("ycba"))
    manager(cfgs, ["ycba"], workers=1)._distributed(0, cfgs, lambda **kw: seen.append(kw))
    assert seen[0] == {"phase": "harvest", "description": "ycba"}


# --- pages mode --------------------------------------------------------------


def test_pages_mode_every_worker_takes_its_share_of_every_source():
    cfgs = StubConfigs(ycba=FakeHarvester("ycba"), ypm=FakeHarvester("ypm"))
    m = manager(cfgs, ["ycba", "ypm"], workers=3, pages=True)
    assert m.mode == "pages" and m.hide_idle_workers is False
    assert m.run_single(2) == {"sources": 2, "records": 0, "failed": 0}
    assert cfgs.h("ycba").calls == [("prepare", 2, 3, null_progress, None, None), ("pages", 2, 3)]
    assert cfgs.h("ypm").calls[-1] == ("pages", 2, 3)
    assert cfgs.idmap_calls == 0


# --- two-phase ---------------------------------------------------------------


def test_two_phase_lists_once_per_source_then_fetches_in_every_worker(stub_runtime, caplog):
    cfgs = stub_runtime(StubConfigs(ycba=FakeHarvester("ycba"), ypm=FakeHarvester("ypm")))
    m = manager(cfgs, ["ycba", "ypm"], workers=4, two_phase=True)
    assert m.mode == "two-phase" and m.phases == (1, 2) and m.hide_idle_workers is True
    view = FakeView()
    with caplog.at_level("INFO", logger="lux_pipeline"):
        results = m.process("null", None, view)
    # the null engine runs worker 0 of each phase
    assert results == [{"sources": 1, "records": 3, "failed": 0},
                       {"sources": 2, "records": 6, "failed": 0}]
    assert m.results == results and m.max_workers == 4
    assert cfgs.h("ycba").calls == [("prepare", 0, 2, null_progress, None, None), ("list",),
                                    ("prepare", 0, 4, null_progress, None, None), ("fetch", 0, 4)]
    assert cfgs.h("ypm").calls == [("prepare", 0, 4, null_progress, None, None), ("fetch", 0, 4)]
    # phase 1 hid the two spare workers, phase 2 showed them again
    assert view.visible == [(2, False), (3, False), (2, True), (3, True)]
    assert "Phase 1: listing records from 2 source(s) with 2 worker(s)" in caplog.text
    assert "Phase 2: fetching listed records with 4 worker(s)" in caplog.text
    assert cfgs.idmap_calls == 0


def test_phase_2_worker_takes_its_share_of_every_source():
    cfgs = StubConfigs(ycba=FakeHarvester("ycba"), ypm=FakeHarvester("ypm"))
    m = manager(cfgs, ["ycba", "ypm"], workers=4, phase=2)
    assert m.phases == (2,) and m.hide_idle_workers is False
    assert m.run_single(3) == {"sources": 2, "records": 6, "failed": 0}
    assert cfgs.h("ycba").calls[-1] == ("fetch", 3, 4)
    assert cfgs.h("ypm").calls[-1] == ("fetch", 3, 4)


def test_phase_1_alone_uses_one_worker_per_source(stub_runtime):
    cfgs = stub_runtime(StubConfigs(ycba=FakeHarvester("ycba")))
    m = manager(cfgs, ["ycba"], workers=4, phase=1)
    assert m.phases == (1,) and m.hide_idle_workers is True
    assert m.process("null") == [{"sources": 1, "records": 3, "failed": 0}]
    assert cfgs.h("ycba").names() == ["prepare", "list"]
    assert m.max_workers == 4


def test_bad_phase_and_pages_with_two_phase_are_rejected():
    with pytest.raises(ValueError, match="phase must be 1 or 2"):
        HarvestManager(StubConfigs(), 1, Namespace(phase=3))
    with pytest.raises(ValueError, match="cannot be combined"):
        HarvestManager(StubConfigs(), 1, Namespace(pages=True, two_phase=True))


# --- failures and the driver -------------------------------------------------


def test_a_failing_source_is_counted_and_the_rest_still_run(caplog):
    cfgs = StubConfigs(ycba=FakeHarvester("ycba", fail=["process"]), ypm=FakeHarvester("ypm"))
    m = manager(cfgs, ["ycba", "ypm"], workers=1)
    with caplog.at_level("ERROR", logger="lux_pipeline"):
        assert m.run_single(0) == {"sources": 1, "records": 3, "failed": 1}
    assert cfgs.h("ypm").names() == ["prepare", "process"]
    assert "ycba: harvest (harvest) failed in worker 0: ConnectionError: ycba: process broke" in caplog.text


def test_process_refuses_with_nothing_to_harvest():
    with pytest.raises(RuntimeError, match="nothing to harvest"):
        manager(StubConfigs(aat=None), ["aat"]).process("null")


def test_summary_sums_over_workers_and_phases():
    m = HarvestManager(StubConfigs(), 2)
    m.results = [{"sources": 1, "records": 5, "failed": 0}, None, {"sources": 2, "records": 1, "failed": 2}]
    assert m.summary() == {"sources": 3, "records": 6, "failed": 2}


def test_defaults_without_args():
    m = HarvestManager(StubConfigs(), 2)
    assert (m.mode, m.phases, m.pages, m.until, m.overwrite) == ("single", (), False, None, True)


# --- the command -------------------------------------------------------------


def run_command(cfgs, rest, tmp_path, source="all", workers=1):
    args = Namespace(command="harvest", source=source, max_workers=workers,
                     engine="null", no_ui=True, prefix=str(tmp_path / "harvest"),
                     log="INFO", max_records=-1, my_worker=-1)
    return CommandHandler(cfgs).process(args, list(rest))


def test_command_single_mode_and_summary(tmp_path, stub_runtime, caplog):
    cfgs = stub_runtime(StubConfigs(ycba=FakeHarvester("ycba"), ypm=FakeHarvester("ypm", records=2)))
    with caplog.at_level("INFO", logger="lux_pipeline"):
        m = run_command(cfgs, ["--until", "2025-01-01T00:00:00", "--no-overwrite"], tmp_path)
    assert isinstance(m, HarvestManager)
    assert (m.mode, m.until, m.overwrite) == ("single", "2025-01-01T00:00:00", False)
    assert m.summary() == {"sources": 2, "records": 5, "failed": 0}
    assert "harvested 5 record(s) over 2 source run(s), 0 failed" in caplog.text
    assert (tmp_path / "harvest-w0.status").exists()


def test_command_two_phase_and_pages_options(tmp_path, stub_runtime):
    cfgs = stub_runtime(StubConfigs(ycba=FakeHarvester("ycba")))
    m = run_command(cfgs, ["--two-phase"], tmp_path, workers=2)
    assert m.phases == (1, 2)
    assert cfgs.h("ycba").names() == ["prepare", "list", "prepare", "fetch"]
    cfgs = stub_runtime(StubConfigs(ycba=FakeHarvester("ycba")))
    m = run_command(cfgs, ["--pages"], tmp_path, workers=2)
    assert m.mode == "pages"
    assert cfgs.h("ycba").calls[-1] == ("pages", 0, 2)


def test_command_fails_at_the_end_when_a_source_failed(tmp_path, stub_runtime):
    cfgs = stub_runtime(StubConfigs(ycba=FakeHarvester("ycba", fail=["process"]), ypm=FakeHarvester("ypm")))
    with pytest.raises(RuntimeError, match="1 harvest\\(s\\) failed"):
        run_command(cfgs, [], tmp_path)
    assert cfgs.h("ypm").names() == ["prepare", "process"]


def test_command_rejects_bad_until_and_bad_phase(tmp_path):
    cfgs = StubConfigs(ycba=FakeHarvester("ycba"))
    with pytest.raises(SystemExit):
        run_command(cfgs, ["--until", "yesterday"], tmp_path)
    with pytest.raises(SystemExit):
        run_command(cfgs, ["--phase", "3"], tmp_path)
    assert iso8601("2025-01-01T00:00:00") == "2025-01-01T00:00:00"


def test_command_is_listed_by_help():
    from lux_pipeline.cli.help import list_commands
    assert ("harvest", "Harvest each source's change feed into its caches.") in list_commands()


# --- --endpoint ----------------------------------------------------------------

FEED = "https://media.art.yale.edu/discovery/lux-full/collection.json"


@pytest.mark.parametrize("options,expected", [
    ({}, ["endpoint", "prepare", "process"]),
    ({"two_phase": True}, ["endpoint", "prepare", "list", "endpoint", "prepare", "fetch"]),
    ({"pages": True}, ["endpoint", "prepare", "pages"]),
])
def test_endpoint_is_set_before_every_entry_point(stub_runtime, options, expected):
    cfgs = stub_runtime(StubConfigs(yuag=FakeHarvester("yuag")))
    m = manager(cfgs, ["yuag"], workers=1, endpoint=FEED, **options)
    m.process("null")
    h = cfgs.h("yuag")
    assert h.names() == expected
    assert all(c[1] == FEED for c in h.calls if c[0] == "endpoint")


def test_endpoint_gives_a_feed_to_a_source_without_one():
    cfgs = StubConfigs(lcnaf=("nofeed", FakeHarvester("lcnaf")), aat=None)
    assert manager(cfgs, ["lcnaf", "aat"], endpoint=FEED).sources == [("external", "lcnaf")]


def test_endpoint_needs_exactly_one_source(stub_runtime):
    cfgs = stub_runtime(StubConfigs(ycba=FakeHarvester("ycba"), yuag=FakeHarvester("yuag")))
    m = manager(cfgs, ["ycba", "yuag"], endpoint=FEED)
    with pytest.raises(ValueError, match="exactly one --source.*ycba, yuag"):
        m.process("null")
    assert cfgs.h("ycba").calls == [] and cfgs.h("yuag").calls == []


def test_command_endpoint(tmp_path, stub_runtime):
    cfgs = stub_runtime(StubConfigs(yuag=FakeHarvester("yuag")))
    m = run_command(cfgs, ["--two-phase", "--endpoint", FEED], tmp_path, source="yuag", workers=2)
    assert m.endpoint == FEED
    assert ("endpoint", FEED) in cfgs.h("yuag").calls
    with pytest.raises(SystemExit):
        run_command(cfgs, ["--endpoint", "media.art.yale.edu/collection.json"], tmp_path,
                    source="yuag")
