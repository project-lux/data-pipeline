"""Loaders with their own load() (wikidata, viaf, orcid, geonames, the LC
sources through Loader.load) drive the progress bar: their slice's share of
totalRecords as the total, a step per record read. They used to log a
timing line every 10,000 records and leave the bar empty."""

import gzip
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.process.base.loader import Loader
from lux_pipeline.sources.wikidata.loader import WdLoader


class Recorder:
    def __init__(self):
        self.calls = []

    def __call__(self, **kw):
        self.calls.append(kw)

    @property
    def total(self):
        return [c["total"] for c in self.calls if "total" in c]

    @property
    def advanced(self):
        return sum(c.get("advance", 0) for c in self.calls)


class Cache(dict):
    def set(self, value, identifier=None):
        self[identifier] = value

    def checkpoint(self):
        pass

    def commit(self):
        pass


def test_start_progress_is_this_slices_share_and_skips_an_unknown_total():
    ldr = object.__new__(Loader)
    ldr.progress, ldr.total = Recorder(), 1000
    ldr.start_progress(slices=4)
    ldr.start_progress()
    ldr.start_progress(total=-1)
    assert ldr.progress.total == [250, 1000]


def test_wikidata_reports_its_slice_of_the_dump(tmp_path):
    lines = ['{"type":"item","id":"Q1"},', '{"type":"property","id":"P1"},', '{"type":"item","id":"Q2"},',
             '{"type":"item","id":"Q3"},', '{"type":"item","id":"Q4"}']
    dump = tmp_path / "latest-all.json.gz"
    with gzip.open(dump, "wt") as fh:
        fh.write("[\n" + "\n".join(lines) + "\n")
    ldr = object.__new__(WdLoader)
    ldr.in_path, ldr.total, ldr.out_cache = str(dump), 100, Cache()
    ldr.configs = SimpleNamespace(temp_dir=str(tmp_path))
    ldr.progress = Recorder()
    ldr.post_process_json = lambda js, what: js
    ldr.process_equivs = lambda rec: ([], [])
    ldr._load_dump(0, 2)                     # lines 0, 2, 4 of the dump: Q1, Q2, Q4
    assert ldr.progress.total == [50]
    assert ldr.progress.advanced == 3
    assert sorted(ldr.out_cache) == ["Q1", "Q2", "Q4"]


def test_the_base_load_counts_every_line_read_skipped_or_stored(tmp_path):
    dump = tmp_path / "names.jsonl.gz"
    with gzip.open(dump, "wt") as fh:
        fh.write('{"id": "a"}\n{"id": "b"}\n{"id": "c"}\n')
    ldr = object.__new__(Loader)
    ldr.in_path, ldr.total, ldr.name = str(dump), 3, "lcsh"
    ldr.out_cache = Cache(b="already")
    ldr.progress = Recorder()
    ldr.get_identifier_raw = lambda l: l.decode().split('"')[3]
    ldr.post_process_json = lambda js: js
    ldr.load()
    assert ldr.progress.total == [3] and ldr.progress.advanced == 3
    assert ldr.out_cache == {"a": {"id": "a"}, "b": "already", "c": {"id": "c"}}
