"""`lux differents` (DifferentsManager): the wikidata process_equivs rules
(P1889 and disambiguation pages) written as full URI pairs, sliced parts
merged sorted and unique into <data_dir>/differentFrom/<source>.csv, a
missing part refused, --my-worker writing only its part, --load adding the
CSV to the merged index (rows the real merged loader accepts), and sources
without process_equivs skipped -- against stub configurations."""

import csv
import os
import sys
from argparse import Namespace
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.process.tasks._task_manager import TaskManager
from lux_pipeline.process.tasks.differents_task import DifferentsManager
from lux_pipeline.progress import null_progress
from lux_pipeline.sources.wikidata.base import WdConfigManager

NS = "http://www.wikidata.org/entity/"


class Cache:
    def __init__(self, recs):
        self.recs = recs

    def iter_records(self):
        return iter(self.recs)

    def iter_records_slice(self, mySlice=0, maxSlice=10):
        return (r for i, r in enumerate(self.recs) if i % maxSlice == mySlice)

    def len_estimate(self):
        return len(self.recs)


class WdEquivs(WdConfigManager):
    """The real process_equivs, without the property map the configs give."""

    def __init__(self):
        self.pref_map = {}
        self.different_prop = "P1889"
        self.instance_of_prop = "P31"
        self.disambiguations = ["Q4167410"]


class Loader:
    def __init__(self):
        self.loaded = []

    def load(self, path, which):
        with open(path) as fh:
            self.loaded.append((which, list(csv.reader(fh))))


def rec(qid, **props):
    return {"identifier": qid, "data": {"id": qid, **props}}


RECORDS = [
    rec("Q1", P1889=["Q2", "Q3"]),
    rec("Q2", P1889="Q1"),                         # a string, not a list
    rec("Q3"),
    rec("Q4", P31=["Q4167410"], P1889=["Q5", "Q6"]),  # disambiguation page
    rec("Q7", P1889=["Q2"]),
    {"identifier": "Q8", "data": None},           # broken record: skipped
]


class Configs:
    max_workers = 2

    def __init__(self, tmp):
        self.temp_dir = str(tmp / "temp")
        self.data_dir = str(tmp / "data")
        self.internal = {"ycba": {"name": "ycba", "datacache": Cache([]),
                                  "namespace": "https://x/", "mapper": object()}}
        self.external = {"wikidata": {"name": "wikidata", "namespace": NS,
                                      "datacache": Cache(RECORDS), "mapper": WdEquivs()}}
        self.results = {"merged": {"name": "merged", "indexLoader": Loader()}}


def manager(cfgs, sources, workers=2, **opts):
    m = DifferentsManager(cfgs, workers, Namespace(**opts))
    for s in sources:
        m.prepare_single(s)
    return m


@pytest.fixture(autouse=True)
def in_process(monkeypatch):
    """TaskManager.process runs the manager's slices here: ``my_slice``'s,
    or every one."""
    def fake(self, engine="mp", prefix=None, view=None):
        ns = [self.my_slice] if self.my_slice > -1 else range(self.max_workers)
        return [self._distributed(n, self._cfgs, null_progress) for n in ns]
    monkeypatch.setattr(TaskManager, "process", fake)


def run(m, cfgs):
    m._cfgs = cfgs
    return m.process("mp")


def read(path):
    with open(path) as fh:
        return list(csv.reader(fh))


EXPECTED = sorted([
    [NS + "Q1", NS + "Q2"], [NS + "Q2", NS + "Q1"], [NS + "Q3", NS + "Q1"],
    [NS + "Q5", NS + "Q6"], [NS + "Q6", NS + "Q5"], [NS + "Q2", NS + "Q7"],
])


def test_writes_uri_pairs_sorted_unique(tmp_path):
    cfgs = Configs(tmp_path)
    m = manager(cfgs, ["wikidata"])
    run(m, cfgs)
    out = os.path.join(cfgs.data_dir, "differentFrom", "wikidata.csv")
    assert read(out) == EXPECTED
    assert m.written["wikidata"] == (out, len(EXPECTED))
    s = m.summary()["sources"]["wikidata"]
    assert s["records"] == len(RECORDS) and s["with"] == 4
    # parts are removed once merged
    assert os.listdir(os.path.join(cfgs.temp_dir, "differents")) == []


def test_the_same_pairs_with_one_worker(tmp_path):
    cfgs = Configs(tmp_path)
    run(manager(cfgs, ["wikidata"], workers=1), cfgs)
    assert read(os.path.join(cfgs.data_dir, "differentFrom", "wikidata.csv")) == EXPECTED


def test_load_adds_rows_the_merged_loader_accepts(tmp_path):
    cfgs = Configs(tmp_path)
    m = manager(cfgs, ["wikidata"], load=True, out_dir=str(tmp_path / "out"))
    run(m, cfgs)
    [(which, rows)] = cfgs.results["merged"]["indexLoader"].loaded
    assert which == "diffs" and rows == EXPECTED
    assert all(a.startswith("http") and b.startswith("http") for a, b in rows)
    assert m.loaded == [str(tmp_path / "out" / "wikidata.csv")]


def test_my_worker_writes_its_part_then_merge_only(tmp_path):
    cfgs = Configs(tmp_path)
    out = os.path.join(cfgs.data_dir, "differentFrom", "wikidata.csv")
    for n in range(2):
        run(manager(cfgs, ["wikidata"], my_worker=n), cfgs)
        assert not os.path.exists(out)
    m = manager(cfgs, ["wikidata"], merge_only=True, load=True)
    m.process("mp")
    assert read(out) == EXPECTED
    assert cfgs.results["merged"]["indexLoader"].loaded[0][0] == "diffs"


def test_a_missing_part_writes_nothing(tmp_path):
    cfgs = Configs(tmp_path)
    out = Path(cfgs.data_dir) / "differentFrom" / "wikidata.csv"
    out.parent.mkdir(parents=True)
    out.write_text("previous\n")
    run(manager(cfgs, ["wikidata"], my_worker=0), cfgs)
    with pytest.raises(FileNotFoundError, match="1 of 2 parts missing"):
        manager(cfgs, ["wikidata"], merge_only=True).process("mp")
    assert out.read_text() == "previous\n"


def test_sources_without_process_equivs_are_skipped(tmp_path):
    cfgs = Configs(tmp_path)
    m = manager(cfgs, ["ycba", "wikidata"])
    assert m.sources == [("external", "wikidata")] and m.skipped == ["ycba"]
    with pytest.raises(ValueError, match="skipped: ycba"):
        manager(cfgs, ["ycba"]).check()


def test_cli_lists_and_parses(tmp_path):
    from lux_pipeline.cli import differents as cli
    from lux_pipeline.cli.help import list_commands
    assert "differents" in dict(list_commands())
    from argparse import ArgumentParser
    ap = ArgumentParser()
    cli.CommandHandler(None).add_args(ap)
    a = ap.parse_args(["--load", "--merge-only", "--out-dir", "x"])
    assert (a.load, a.merge_only, a.out_dir, a.force) == (True, True, "x", False)
