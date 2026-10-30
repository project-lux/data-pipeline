"""`lux sheets` and `lux portal`: google-sames-diffs.py / load-csv-map2.py
and post-build-portal.py as commands. The spreadsheet process against an
in-memory format, then each format: Google through a stub of the Sheets
API, xlsx through a stub of openpyxl, and a real CSV directory. The caches
are dicts and the process pools run in this process, so nothing is
fetched, started or written outside tmp_path."""

import csv
import json
import os
import sys
from argparse import Namespace
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from lux_pipeline.cli import sheets as sheets_cli
from lux_pipeline.cli import portal as portal_cli
from lux_pipeline.process import sheets
from lux_pipeline.process.phases import portal

# --- the spreadsheet process ------------------------------------------------

TABS = {
    "Same As": [["http://a/1", "http://b/1"], [], ["http://bad/2", "http://b/2"], ["only-one"]],
    "Different From": [["http://a/3", " http://b/3 "]],
    "Fixes": [["ycba", "obj/1", "HumanMadeObject", "", "$.x", "delete"],
              ["ypm", "obj/2", "Place", "eq", "$.y", "set", " val "],
              ["short", "row"]],
}


class Cfgs:
    def __init__(self, tmp_path, loader=None):
        self.data_dir = str(tmp_path / "data")
        os.makedirs(self.data_dir)
        Path(self.data_dir, "google_sheet_id.txt").write_text("SHEET-ID\n")
        self.results = {"merged": {"indexLoader": loader}}
        self.log_dir = str(tmp_path / "logs")

    def canonicalize(self, uri):
        return None if "bad" in uri else uri.replace("http:", "https:")


class Loader:
    def __init__(self):
        self.loaded = []

    def load(self, fn, which):
        self.loaded.append((which, os.path.basename(os.path.dirname(fn))))


class Memory(sheets.SheetImport):
    """The smallest format: tabs held in a dict."""

    def __init__(self, cfgs, tabs):
        super().__init__(cfgs)
        self.tabs = tabs
        self.events = []

    def open(self):
        self.events.append("open")

    def close(self):
        self.events.append("close")

    def read_rows(self, name, last_col):
        self.events.append(name)
        rows = self.tabs.get(name)
        return None if rows is None else [r[:sheets.width(last_col)] for r in rows]


def read_csv(path):
    with open(path) as fh:
        return list(csv.reader(fh))


def data(cfgs, *parts):
    return Path(cfgs.data_dir, *parts)


def assert_written(cfgs):
    assert read_csv(data(cfgs, "sameAs", "google.csv")) == [["https://a/1", "https://b/1"]]
    assert read_csv(data(cfgs, "differentFrom", "google.csv")) == [["https://a/3", "https://b/3"]]
    fixes = json.loads(data(cfgs, "xpath_fixes.json").read_text())
    assert fixes[0]["argument"] == "" and fixes[0]["operation"] == "delete"
    assert fixes[1] == {"source": "ypm", "identifier": "obj/2", "class": "Place",
                        "equivalent": "eq", "path": "$.y", "operation": "set", "argument": "val"}


def test_the_process_writes_the_three_files(tmp_path):
    cfgs = Cfgs(tmp_path)
    imp = Memory(cfgs, TABS)
    assert imp.fetch() == {"equivs": 1, "diffs": 1, "fixes": 2}
    assert_written(cfgs)
    assert imp.events == ["open", "Different From", "Same As", "Fixes", "close"]


def test_a_read_error_writes_nothing_and_closes(tmp_path):
    cfgs = Cfgs(tmp_path)

    class Failing(Memory):
        def read_rows(self, name, last_col):
            if name == "Fixes":
                raise RuntimeError("HttpError 503")
            return super().read_rows(name, last_col)

    imp = Failing(cfgs, TABS)
    with pytest.raises(RuntimeError, match="503"):
        imp.fetch()
    assert imp.events[-1] == "close"
    assert sorted(os.listdir(cfgs.data_dir)) == ["google_sheet_id.txt"]


def test_empty_or_missing_tabs_keep_previous_files(tmp_path):
    cfgs = Cfgs(tmp_path)
    data(cfgs, "sameAs").mkdir()
    data(cfgs, "sameAs", "google.csv").write_text("https://old,https://older\n")
    data(cfgs, "xpath_fixes.json").write_text("[1]")
    counts = Memory(cfgs, {"Same As": [], "Different From": []}).fetch()
    assert counts == {"equivs": 0, "diffs": 0, "fixes": None}
    assert read_csv(data(cfgs, "sameAs", "google.csv")) == [["https://old", "https://older"]]
    assert data(cfgs, "xpath_fixes.json").read_text() == "[1]"
    # an empty Fixes tab is an empty list of fixes, as the script wrote
    Memory(cfgs, {"Fixes": []}).fetch()
    assert data(cfgs, "xpath_fixes.json").read_text() == "[]"


def test_load_adds_both_csvs(tmp_path):
    loader = Loader()
    cfgs = Cfgs(tmp_path, loader)
    for d in ("sameAs", "differentFrom"):
        data(cfgs, d).mkdir()
        data(cfgs, d, "google.csv").write_text("")
    assert len(Memory(cfgs, {}).load()) == 2
    assert loader.loaded == [("equivs", "sameAs"), ("diffs", "differentFrom")]


def test_load_without_a_loader(tmp_path):
    with pytest.raises(RuntimeError, match="indexLoader"):
        Memory(Cfgs(tmp_path), {}).load()


def test_the_base_class_needs_a_format(tmp_path):
    with pytest.raises(NotImplementedError):
        sheets.SheetImport(Cfgs(tmp_path)).fetch()


@pytest.mark.parametrize("cells,last_col,row", [
    (["a", "b", "c"], "B", ["a", "b"]),
    ([None, "x", None, None], "G", ["", "x"]),
    ([12.0, 1.5, 7, "s"], "G", ["12", "1.5", "7", "s"]),
    ((), "B", []),
])
def test_tidy_row(cells, last_col, row):
    assert sheets.tidy_row(cells, last_col) == row


# --- Google ------------------------------------------------------------------


class Sheet:
    """`spreadsheets()`: ``values().get(spreadsheetId, range).execute()``
    over {tab name: rows from row 2}, recording the ranges asked for."""

    def __init__(self, tabs):
        self.tabs = tabs
        self.ranges = []

    def values(self):
        return self

    def get(self, spreadsheetId, range):
        assert spreadsheetId == "SHEET-ID"
        self.ranges.append(range)
        name, cells = range.split("!")
        first, last = cells.split(":")
        start, end = int(first[1:]), int(last[1:])
        rows = self.tabs.get(name, [])[start - 2:end - 1]
        return Namespace(execute=lambda: {"values": rows} if rows else {})


def test_google(tmp_path):
    cfgs = Cfgs(tmp_path)
    imp = sheets.GoogleSheetImport(cfgs, Sheet(TABS))
    assert imp.fetch() == {"equivs": 1, "diffs": 1, "fixes": 2}
    assert_written(cfgs)
    assert imp.describe() == "Google Sheet SHEET-ID"


def test_google_pages_do_not_overlap(tmp_path):
    sheet = Sheet({"Same As": [[f"http://a/{n}", f"http://b/{n}"] for n in range(1200)]})
    assert sheets.GoogleSheetImport(Cfgs(tmp_path), sheet).fetch()["equivs"] == 1200
    assert [r for r in sheet.ranges if r.startswith("Same As")] == [
        "Same As!A2:B501", "Same As!A502:B1001", "Same As!A1002:B1501", "Same As!A1502:B2001"]


def test_google_without_its_libraries_says_how_to_install(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "googleapiclient.discovery", None)
    with pytest.raises(RuntimeError, match=r"\.\[google\]"):
        sheets.GoogleSheetImport(Cfgs(tmp_path)).fetch()


# --- CSV directory -----------------------------------------------------------


def write_tab(path, rows, header=("A", "B")):
    with open(path, "w", newline="", encoding="utf-8-sig") as fh:
        csv.writer(fh).writerows([list(header), *rows])


def test_csv_directory(tmp_path):
    cfgs = Cfgs(tmp_path)
    d = tmp_path / "export"
    d.mkdir()
    for name, rows in TABS.items():
        write_tab(d / f"{name}.csv", rows)
    imp = sheets.sheet_import(cfgs, str(d))
    assert isinstance(imp, sheets.CsvSheetImport)
    assert imp.fetch() == {"equivs": 1, "diffs": 1, "fixes": 2}
    assert_written(cfgs)


def test_csv_directory_without_a_tab(tmp_path):
    cfgs = Cfgs(tmp_path)
    d = tmp_path / "export"
    d.mkdir()
    write_tab(d / "Same As.csv", TABS["Same As"])
    assert sheets.CsvSheetImport(cfgs, str(d)).fetch() == {"equivs": 1, "diffs": None,
                                                          "fixes": None}


# --- xlsx --------------------------------------------------------------------


class FakeWorkbook:
    """What `openpyxl.load_workbook(read_only=True, data_only=True)` gives:
    sheetnames, worksheets whose iter_rows yields value tuples."""

    def __init__(self, tabs):
        self.tabs = tabs
        self.sheetnames = list(tabs)
        self.closed = False

    def __getitem__(self, name):
        rows = [("header", "row")] + [tuple(r) for r in self.tabs[name]]

        def iter_rows(min_row, max_col, values_only):
            assert values_only
            for r in rows[min_row - 1:]:
                yield tuple(r[:max_col]) + (None,) * (max_col - len(r[:max_col]))

        return Namespace(iter_rows=iter_rows)

    def close(self):
        self.closed = True


def test_xlsx(tmp_path, monkeypatch):
    cfgs = Cfgs(tmp_path)
    book = tmp_path / "curation.xlsx"
    book.write_text("")
    wb = FakeWorkbook({**TABS, "Fixes": [r + [None] * (7 - len(r)) for r in TABS["Fixes"]]})
    opened = []
    monkeypatch.setitem(sys.modules, "openpyxl", Namespace(
        load_workbook=lambda path, read_only, data_only: opened.append(path) or wb))
    imp = sheets.sheet_import(cfgs, str(book))
    assert isinstance(imp, sheets.XlsxSheetImport)
    assert imp.fetch() == {"equivs": 1, "diffs": 1, "fixes": 2}
    assert_written(cfgs)
    assert opened == [str(book)] and wb.closed


def test_xlsx_without_openpyxl_says_how_to_install(tmp_path, monkeypatch):
    book = tmp_path / "c.xlsx"
    book.write_text("")
    monkeypatch.setitem(sys.modules, "openpyxl", None)
    with pytest.raises(RuntimeError, match=r"\.\[xlsx\]"):
        sheets.XlsxSheetImport(Cfgs(tmp_path), str(book)).fetch()


@pytest.mark.parametrize("name,match", [("missing.xlsx", "does not exist"),
                                        ("notes.ods", "an .xlsx workbook or a directory")])
def test_sheet_import_rejects(tmp_path, name, match):
    with pytest.raises(ValueError, match=match):
        sheets.sheet_import(Cfgs(tmp_path), str(tmp_path / name))


def test_sheet_import_defaults_to_google(tmp_path):
    assert isinstance(sheets.sheet_import(Cfgs(tmp_path)), sheets.GoogleSheetImport)


# --- the command -------------------------------------------------------------


@pytest.mark.parametrize("opts,fetched,loaded", [
    ([], True, True), (["--fetch-only"], True, False), (["--load-only"], False, True)])
def test_sheets_command(tmp_path, monkeypatch, opts, fetched, loaded):
    calls = []

    class Recorder(Memory):
        def fetch(self):
            calls.append("fetch")
            return {"equivs": 1, "diffs": None, "fixes": 3}

        def load(self):
            calls.append("load")
            return []

    monkeypatch.setattr(sheets, "sheet_import",
                        lambda cfgs, path: calls.append(path) or Recorder(cfgs, {}))
    monkeypatch.setattr(sheets_cli, "refuse_while_building", lambda *a: calls.append("guard"))
    hdlr = sheets_cli.CommandHandler(Cfgs(tmp_path))
    hdlr.process(Namespace(command="sheets"), opts + ["--file", "x.xlsx"])
    assert calls == ["x.xlsx", "guard"] + ["fetch"] * fetched + ["load"] * loaded


def test_sheets_command_only_options_exclusive(tmp_path):
    hdlr = sheets_cli.CommandHandler(Cfgs(tmp_path))
    with pytest.raises(SystemExit):
        hdlr.process(Namespace(command="sheets"), ["--fetch-only", "--load-only"])


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
