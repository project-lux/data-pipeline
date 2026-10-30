"""`lux google` and `lux portal`: google-sames-diffs.py / load-csv-map2.py
and post-build-portal.py as commands. The sheet is a stub of the Sheets
API, the caches are dicts and the process pools run in this process, so
nothing is fetched, started or written outside tmp_path."""

import csv
import json
import os
import sys
from argparse import Namespace
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from lux_pipeline.cli import google as google_cli
from lux_pipeline.cli import portal as portal_cli
from lux_pipeline.process import google_sheet
from lux_pipeline.process.phases import portal

# --- google ------------------------------------------------------------------


class Sheet:
    """`spreadsheets()`: ``values().get(spreadsheetId, range).execute()``
    over {sheet name: rows from row 2}, recording the ranges asked for."""

    def __init__(self, sheets):
        self.sheets = sheets
        self.ranges = []

    def values(self):
        return self

    def get(self, spreadsheetId, range):
        assert spreadsheetId == "SHEET-ID"
        self.ranges.append(range)
        name, cells = range.split("!")
        first, last = cells.split(":")
        start, end = int(first[1:]), int(last[1:])
        rows = self.sheets.get(name, [])[start - 2:end - 1]
        return Namespace(execute=lambda: {"values": rows} if rows else {})


class GCfgs:
    def __init__(self, tmp_path, loader=None):
        self.data_dir = str(tmp_path)
        (tmp_path / "google_sheet_id.txt").write_text("SHEET-ID\n")
        self.results = {"merged": {"indexLoader": loader}}
        self.log_dir = str(tmp_path / "logs")

    def canonicalize(self, uri):
        return None if "bad" in uri else uri.replace("http:", "https:")


class Loader:
    def __init__(self):
        self.loaded = []

    def load(self, fn, which):
        self.loaded.append((which, os.path.basename(os.path.dirname(fn))))


def read_csv(path):
    with open(path) as fh:
        return list(csv.reader(fh))


def test_fetch_writes_the_three_files(tmp_path):
    sheet = Sheet({
        "Same As": [["http://a/1", "http://b/1"], [], ["http://bad/2", "http://b/2"], ["only-one"]],
        "Different From": [["http://a/3", " http://b/3 "]],
        "Fixes": [["ycba", "obj/1", "HumanMadeObject", "", "$.x", "delete"],
                  ["ypm", "obj/2", "Place", "eq", "$.y", "set", " val "],
                  ["short", "row"]],
    })
    counts = google_sheet.fetch(GCfgs(tmp_path), sheet)
    assert counts == {"equivs": 1, "diffs": 1, "fixes": 2}
    assert read_csv(tmp_path / "sameAs" / "google.csv") == [["https://a/1", "https://b/1"]]
    assert read_csv(tmp_path / "differentFrom" / "google.csv") == [["https://a/3", "https://b/3"]]
    fixes = json.loads((tmp_path / "xpath_fixes.json").read_text())
    assert fixes[0]["argument"] == "" and fixes[0]["operation"] == "delete"
    assert fixes[1] == {"source": "ypm", "identifier": "obj/2", "class": "Place", "equivalent": "eq",
                        "path": "$.y", "operation": "set", "argument": "val"}


def test_fetch_pages_do_not_overlap(tmp_path):
    rows = [[f"http://a/{n}", f"http://b/{n}"] for n in range(1200)]
    sheet = Sheet({"Same As": rows})
    counts = google_sheet.fetch(GCfgs(tmp_path), sheet)
    assert counts["equivs"] == 1200
    same = [r for r in sheet.ranges if r.startswith("Same As")]
    assert same == ["Same As!A2:B501", "Same As!A502:B1001", "Same As!A1002:B1501",
                    "Same As!A1502:B2001"]


def test_empty_sheet_keeps_previous_csv(tmp_path):
    (tmp_path / "sameAs").mkdir()
    (tmp_path / "sameAs" / "google.csv").write_text("https://old,https://older\n")
    google_sheet.fetch(GCfgs(tmp_path), Sheet({}))
    assert read_csv(tmp_path / "sameAs" / "google.csv") == [["https://old", "https://older"]]
    assert json.loads((tmp_path / "xpath_fixes.json").read_text()) == []


def test_api_error_writes_nothing(tmp_path):
    class Broken(Sheet):
        def get(self, spreadsheetId, range):
            raise RuntimeError("HttpError 503")

    with pytest.raises(RuntimeError, match="503"):
        google_sheet.fetch(GCfgs(tmp_path), Broken({}))
    assert not (tmp_path / "xpath_fixes.json").exists()


def test_load_adds_both_csvs(tmp_path):
    loader = Loader()
    cfgs = GCfgs(tmp_path, loader)
    for d in ("sameAs", "differentFrom"):
        (tmp_path / d).mkdir()
        (tmp_path / d / "google.csv").write_text("")
    assert len(google_sheet.load(cfgs)) == 2
    assert loader.loaded == [("equivs", "sameAs"), ("diffs", "differentFrom")]


def test_load_without_a_loader(tmp_path):
    with pytest.raises(RuntimeError, match="indexLoader"):
        google_sheet.load(GCfgs(tmp_path))


def test_missing_client_libraries_say_how_to_install(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "googleapiclient.discovery", None)
    with pytest.raises(RuntimeError, match=r"\.\[google\]"):
        google_sheet.sheet_client(str(tmp_path))


@pytest.mark.parametrize("opts,fetched,loaded", [
    ([], True, True), (["--fetch-only"], True, False), (["--load-only"], False, True)])
def test_google_command(tmp_path, monkeypatch, opts, fetched, loaded):
    calls = []
    monkeypatch.setattr(google_sheet, "fetch",
                        lambda cfgs: calls.append("fetch") or {"equivs": 1, "diffs": 2, "fixes": 3})
    monkeypatch.setattr(google_sheet, "load", lambda cfgs: calls.append("load") or [])
    monkeypatch.setattr(google_cli, "refuse_while_building", lambda *a: calls.append("guard"))
    hdlr = google_cli.CommandHandler(GCfgs(tmp_path))
    hdlr.process(Namespace(command="google"), opts)
    assert calls == ["guard"] + ["fetch"] * fetched + ["load"] * loaded


def test_google_command_only_options_exclusive(tmp_path):
    hdlr = google_cli.CommandHandler(GCfgs(tmp_path))
    with pytest.raises(SystemExit):
        hdlr.process(Namespace(command="google"), ["--fetch-only", "--load-only"])


# --- portal ------------------------------------------------------------------


class Merged(dict):
    def __init__(self, recs):
        super().__init__(recs)
        self.meta = {}

    def set_metadata(self, key, field, value):
        if key == "unsettable":
            raise ValueError("no")
        self.meta[key] = (field, value)


class Keys:
    def __init__(self, keys):
        self.keys = keys

    def iter_keys(self):
        return iter(self.keys)


def ref(yuid, **more):
    return {"id": f"https://lux/data/x/{yuid}", **more}


def chain(merged, start, length):
    """start -> start1 -> start2 ... a reference chain of `length` hops."""
    prev = start
    for n in range(1, length + 1):
        merged[prev] = {"data": {"id": f"https://lux/data/x/{prev}", "about": ref(f"{start}{n}")}}
        prev = f"{start}{n}"
    merged[prev] = {"data": {"id": f"https://lux/data/x/{prev}"}}


@pytest.fixture
def pool(monkeypatch):
    """The process pools as plain loops, the workers' merged cache a dict."""
    merged = Merged({})
    monkeypatch.setattr(portal, "_merged", lambda: merged)
    monkeypatch.setattr(portal, "_parallel",
                        lambda fn, slices, workers, *extra: (fn(s, *extra) for s in slices if s))
    return merged


def test_walk_skips_equivalents_and_blank_nodes():
    rec = {"id": "https://lux/data/x/top", "equivalent": [ref("eq")],
           "member_of": [ref("set1"), {"id": "_:b1", "part": ref("inner")}],
           "access_point": [ref("ap")], "conforms_to": [ref("ct")]}
    distances, added = {}, {}
    portal.walk_for_refs(rec, 1, distances, added, top=True)
    assert distances == {"set1": 1, "inner": 1}
    assert added == distances


def test_walk_keeps_the_shortest_distance():
    distances, added = {"near": 1}, {}
    portal.walk_for_refs({"a": ref("near"), "b": ref("new")}, 3, distances, added, top=True)
    assert distances == {"near": 1, "new": 3}
    assert added == {"new": 3}


def test_chunks_cover_everything():
    assert portal.chunks(list(range(7)), 3) == [[0, 1], [2, 3], [4, 5, 6]]
    assert portal.chunks([1], 4) == [[], [], [], [1]]


def test_tag_portal(pool):
    chain(pool, "p", 8)
    pool["q"] = None  # a YPM record with no merged record
    pool["unsettable"] = {"data": {}}
    cfgs = Namespace(internal={"ypm": {"recordcache2": Keys(["p", "q", "missing", "unsettable"])}},
                     results={"merged": {"recordcache": pool}})
    reached, tagged = portal.tag_portal(cfgs, workers=2)
    # p is 0, p1 is 1 ...; references are followed from records up to
    # distance 4, so p5 is the last one reached
    assert set(pool.meta) == {"p", "p1", "p2", "p3", "p4", "p5", "q", "missing"}
    assert all(v == ("change", "ypm") for v in pool.meta.values())
    assert (reached, tagged) == (9, 8)


def test_portal_command(monkeypatch):
    calls = []
    monkeypatch.setattr(portal_cli, "tag_portal", lambda cfgs, workers: calls.append(workers) or (5, 4))
    monkeypatch.setattr(portal_cli, "refuse_while_building", lambda *a: calls.append("guard"))
    hdlr = portal_cli.CommandHandler(Namespace())
    assert hdlr.process(Namespace(command="portal", max_workers=0), []) == (5, 4)
    hdlr.process(Namespace(command="portal", max_workers=6), [])
    assert calls == ["guard", None, "guard", 6]
