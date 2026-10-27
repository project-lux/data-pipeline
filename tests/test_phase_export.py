"""The three export functions pinned against what the scripts did, with the
caches, the mappers and the pool stubbed. `export_slice`: every branch of
the freshness check, the fresh document written as the exact text the cache
holds, the stale one transformed and stored back, the chunked cache lookups
and checkpoints, --no-ml never touching the marklogic cache, --gz, the
mapper error, the file names, the flag, progress and the return value.
`export_sources_slice` and `export_nt_slice`: each record's path to the
files, sliced and unsliced iteration, the record the mapper rejects, the
one the qlever mapper cannot express, the commit calls and the tidy-up."""

import datetime
import gzip
import logging
import os
import sys
from pathlib import Path

import pytest
import ujson

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.process.phases import export as phase
from lux_pipeline.process.phases.export import export_nt_slice, export_slice, export_sources_slice

LUX = "https://lux.collections.yale.edu/data/"
AAT = "http://vocab.getty.edu/aat/"
ULAN = "http://vocab.getty.edu/ulan/"
T0 = datetime.datetime(2026, 10, 1, 12, 0, 0)
T1 = datetime.datetime(2026, 10, 2, 12, 0, 0)


def merged_row(yuid, insert_time=T1):
    """A merged row as iter_records_slice(raw=True) hands it out: the data
    column is the JSON text postgres stored, not a dict."""
    data = {"id": f"{LUX}{yuid}", "type": "Person", "_label": f"person {yuid}"}
    return {"yuid": yuid, "identifier": yuid, "data": ujson.dumps(data), "insert_time": insert_time}


def ml_row(yuid, insert_time=T1, text=None):
    """A marklogic cache row as get_multi(raw=True) hands it out."""
    if text is None:
        text = ujson.dumps({"json": {"id": f"{LUX}{yuid}", "cached": True}})
    return {"yuid": yuid, "data": text, "insert_time": insert_time}


def nt_row(yuid, rectype="Person"):
    """A merged row as the plain iterators hand it out: data parsed."""
    return {"yuid": yuid, "identifier": yuid, "data": {"id": f"{LUX}{yuid}", "type": rectype}}


def src_row(ns, ident, rectype="Type"):
    """A datacache row of an external source."""
    return {"identifier": ident, "data": {"id": f"{ns}{ident}", "type": rectype}}


# --- stubs -------------------------------------------------------------------


class Recorder:
    def __init__(self):
        self.calls = []

    def __call__(self, **kw):
        self.calls.append(kw)


class Idmap:
    update_token = "__20261027__"

    def __init__(self, log):
        self.log = log

    def enable_memory_cache(self):
        self.log.append(("memory_cache", True))

    def disable_memory_cache(self):
        self.log.append(("memory_cache", False))


class Cache:
    """A record cache: the iterators, the batch fetch, the deferred-commit
    calls and the writes, all recorded. `rows` is what the iterators and
    get_multi answer from; `stored` is what was written."""

    def __init__(self, log, name, rows=(), estimate=None):
        self.log = log
        self.name = name
        self.config = {"name": name}
        self.rows = list(rows)
        self.estimate = estimate
        self.stored = {}

    def len_estimate(self):
        return len(self.rows) if self.estimate is None else self.estimate

    def iter_records(self, raw=False):
        self.log.append(("iter_records", self.name, raw))
        for r in self.rows:
            yield dict(r)

    def iter_records_slice(self, my_slice, max_slice, raw=False):
        self.log.append(("iter_records_slice", self.name, my_slice, max_slice, raw))
        for i, r in enumerate(self.rows):
            if i % max_slice == my_slice:
                yield dict(r)

    def get_multi(self, keys, raw=False):
        self.log.append(("get_multi", self.name, list(keys), raw))
        by = {r["yuid"]: r for r in self.rows}
        return {k: dict(by[k]) for k in keys if k in by}

    def defer_commits(self, every=100, batch=0):
        self.log.append(("defer_commits", self.name, every, batch))

    def checkpoint(self):
        self.log.append(("checkpoint", self.name))

    def flush(self):
        self.log.append(("flush", self.name))

    def resume_commits(self):
        self.log.append(("resume_commits", self.name))

    def __setitem__(self, key, value):
        self.log.append(("store", self.name, key))
        self.stored[key] = value


class NoEstimateCache(Cache):
    """A backend without the postgres estimate."""
    len_estimate = None


class MlMapper:
    """The marklogic mapper: transform(record, type) -> the document, or an
    exception for the yuids in `fail`. Records what shape the data had."""

    def __init__(self, log, fail=()):
        self.log = log
        self.fail = set(fail)

    def transform(self, record, rectype=None, reference=False):
        self.log.append(("transform", "marklogic", record["yuid"], rectype,
                         type(record["data"]).__name__))
        if record["yuid"] in self.fail:
            raise ValueError(f"cannot map {record['yuid']}")
        return {"data": {"id": record["data"]["id"], "type": rectype, "mapped": True},
                "yuid": record["yuid"]}


class SourceMapper:
    """An external source's mapper: transform(record, None) -> the record
    the recordcache gets, or None for the identifiers in `reject`."""

    def __init__(self, log, name, reject=()):
        self.log = log
        self.name = name
        self.reject = set(reject)

    def transform(self, record, rectype=None, reference=False):
        self.log.append(("transform", self.name, record["identifier"]))
        if record["identifier"] in self.reject:
            return None
        return {"identifier": record["identifier"], "yuid": f"yuid-{record['identifier']}",
                "data": dict(record["data"])}


def make_qlever(log, fail=(), empty=()):
    """mapper2's QleverMapper: built per source from its configuration,
    transform(record) -> the record's triples."""

    class QleverMapper:
        def __init__(self, config):
            log.append(("QleverMapper", config["name"]))

        def transform(self, record, rectype=None, reference=False):
            rid = record["data"]["id"]
            log.append(("qlever", rid))
            if rid in fail:
                raise ValueError(f"not an IRI: {rid}")
            if rid in empty:
                return []
            return [f"<{rid}> <urn:type> <urn:{record['data']['type']}> .",
                    f'<{rid}> <urn:label> "{rid}" .']

    return QleverMapper


def make_pool(log):
    class PoolManager:
        @classmethod
        def get_instance(cls):
            return cls()

        def put_all(self, name):
            log.append(("put_all", name))

    return PoolManager


class StubConfigs:
    """The configuration the phase reads: the directories, the idmap, the
    merged and marklogic results, and the external sources given as
    name -> datacache rows (every one with an empty recordcache)."""

    def __init__(self, tmp_path, log, merged=(), ml=(), fail=(), estimate=None,
                 sources=None, reject=()):
        self.log = log
        self.data_dir = str(tmp_path / "data")
        self.log_dir = str(tmp_path / "logs")
        # left for the phase to create
        self.exports_dir = str(tmp_path / "exports")
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.log_dir, exist_ok=True)
        self.idmap = Idmap(log)
        self.results = {
            "merged": {"name": "merged",
                       "recordcache": Cache(log, "merged", merged, estimate)},
            "marklogic": {"name": "marklogic",
                          "recordcache": Cache(log, "ml", ml),
                          "mapper": MlMapper(log, fail)},
        }
        self.external = {}
        for name, rows in (sources or {}).items():
            self.external[name] = {"name": name,
                                   "datacache": Cache(log, f"{name}-data", rows),
                                   "recordcache": Cache(log, f"{name}-rec"),
                                   "mapper": SourceMapper(log, name, reject)}

    def get_idmap(self):
        return self.idmap

    def make_qua(self, uri, typ):
        return f"{uri}##qua{typ}"


@pytest.fixture
def harness(tmp_path, monkeypatch):
    """Build a configuration and the stubs, patch the pool and the qlever
    mapper into the phase module, and run one of the three functions; the
    shared `log` is the record of what happened."""

    class Harness:
        def __init__(self):
            self.log = []
            self.cfgs = None

        def configure(self, qlever_fail=(), qlever_empty=(), **kw):
            self.cfgs = StubConfigs(tmp_path, self.log, **kw)
            monkeypatch.setattr(phase, "PoolManager", make_pool(self.log))
            monkeypatch.setattr(phase, "QleverMapper",
                                make_qlever(self.log, qlever_fail, qlever_empty))
            return self.cfgs

        def export(self, my_slice=0, max_slice=1, **kw):
            if self.cfgs is None:
                self.configure()
            return export_slice(self.cfgs, my_slice, max_slice, **kw)

        def sources(self, my_slice=-1, max_slice=-1, **kw):
            if self.cfgs is None:
                self.configure()
            return export_sources_slice(self.cfgs, my_slice, max_slice, **kw)

        def nt(self, my_slice=-1, max_slice=-1, **kw):
            if self.cfgs is None:
                self.configure()
            return export_nt_slice(self.cfgs, my_slice, max_slice, **kw)

        def names(self):
            return [c[0] for c in self.log]

        def only(self, *kinds):
            return [c for c in self.log if c[0] in kinds]

        def lines(self, path):
            opener = gzip.open if path.endswith(".gz") else open
            with opener(path, "rt") as fh:
                return fh.read().splitlines()

        def flag(self, n):
            return Path(self.cfgs.log_dir) / "flags" / f"export_is_done-{n}.txt"

        def ml(self):
            return self.cfgs.results["marklogic"]["recordcache"]

        def merged(self):
            return self.cfgs.results["merged"]["recordcache"]

    return Harness()


# --- export_slice: the marklogic documents ------------------------------------


def test_fresh_row_is_written_as_is_and_stale_row_is_transformed_and_stored(harness):
    h = harness
    h.configure(merged=[merged_row("Y1", T0), merged_row("Y2", T1)],
                ml=[ml_row("Y1", T1, text='{"json": {"cached": "Y1"}}')])
    result = h.export()

    lines = h.lines(result["path"])
    # the cached document, newer than the merged record, is the exact text
    # the cache holds, neither parsed nor re-serialised
    assert lines[0] == '{"json": {"cached": "Y1"}}'
    # the record the cache has not seen is transformed, from parsed data
    assert ujson.loads(lines[1]) == {"data": {"id": f"{LUX}Y2", "type": "Person", "mapped": True},
                                     "yuid": "Y2"}
    assert h.only("transform") == [("transform", "marklogic", "Y2", "Person", "dict")]
    # ...and stored back under its yuid
    assert list(h.ml().stored) == ["Y2"]
    assert h.ml().stored["Y2"]["data"]["mapped"] is True
    assert result["written"] == 2
    assert result["path"] == os.path.join(h.cfgs.exports_dir, "export_full_0.jsonl")


@pytest.mark.parametrize("since, cached, fresh", [
    (T0, T1, True),      # the document is newer than the merged record
    (T0, T0, True),      # the same time
    (T1, T0, False),     # the document is older: the merged record changed
    (None, T0, True),    # a merged record without a time: any document will do
    (None, None, True),
    (T0, None, False),   # a document without a time cannot be trusted
])
def test_freshness_decides_whether_the_cached_document_is_reused(harness, since, cached, fresh):
    h = harness
    h.configure(merged=[merged_row("Y1", since)], ml=[ml_row("Y1", cached, text='{"cached": 1}')])
    result = h.export()
    assert (h.lines(result["path"]) == ['{"cached": 1}']) is fresh
    assert bool(h.only("transform")) is not fresh
    assert ("Y1" in h.ml().stored) is not fresh


def test_a_record_the_cache_has_never_seen_is_transformed(harness):
    h = harness
    h.configure(merged=[merged_row("Y1", None)], ml=[])
    result = h.export()
    assert h.only("transform") == [("transform", "marklogic", "Y1", "Person", "dict")]
    assert list(h.ml().stored) == ["Y1"]
    assert result["written"] == 1


def test_a_record_the_mapper_cannot_transform_is_logged_and_left_out(harness, caplog):
    h = harness
    h.configure(merged=[merged_row("Y1"), merged_row("Y2")], ml=[], fail=["Y1"])
    with caplog.at_level(logging.WARNING, logger="lux_pipeline"):
        result = h.export()
    assert "Y1 errored in marklogic mapper: cannot map Y1" in caplog.text
    assert [ujson.loads(l)["yuid"] for l in h.lines(result["path"])] == ["Y2"]
    assert "Y1" not in h.ml().stored
    assert result["written"] == 1
    # the timer counts the chunk, so the summary still says two records
    assert result["records"]["records"] == 2


def test_the_cache_is_asked_once_per_chunk_and_checkpointed_after_each(harness):
    h = harness
    h.configure(merged=[merged_row(f"Y{i}") for i in range(5)], ml=[])
    result = h.export(chunk_size=2)
    assert h.only("get_multi") == [("get_multi", "ml", ["Y0", "Y1"], True),
                                   ("get_multi", "ml", ["Y2", "Y3"], True),
                                   ("get_multi", "ml", ["Y4"], True)]
    assert h.only("defer_commits", "checkpoint", "resume_commits") == [
        ("defer_commits", "ml", 500, 0),
        ("checkpoint", "ml"), ("checkpoint", "ml"), ("checkpoint", "ml"),
        ("resume_commits", "ml")]
    assert result["written"] == 5


def test_the_transformed_document_keeps_its_slashes(harness):
    h = harness
    h.configure(merged=[merged_row("Y1")], ml=[])
    result = h.export()
    line = h.lines(result["path"])[0]
    assert f"{LUX}Y1" in line
    assert "\\/" not in line


def test_no_ml_writes_the_stored_text_and_never_touches_the_marklogic_cache(harness):
    h = harness
    h.configure(merged=[merged_row("Y1"), merged_row("Y2")], ml=[ml_row("Y1")])
    result = h.export(no_ml=True)
    assert h.lines(result["path"]) == [r["data"] for r in h.merged().rows]
    assert not h.only("get_multi", "transform", "store", "defer_commits", "checkpoint",
                      "resume_commits")
    assert result["path"].endswith("export_merged_0.jsonl")
    assert result["written"] == 2


def test_gz_writes_the_same_lines_compressed(harness):
    h = harness
    h.configure(merged=[merged_row("Y1", T0), merged_row("Y2")], ml=[ml_row("Y1")])
    plain = h.export()
    packed = h.export(gz=True, gzip_level=1)
    assert packed["path"].endswith("export_full_0.jsonl.gz")
    with gzip.open(packed["path"], "rb") as fh:
        fh.read(1)   # really gzip
    assert h.lines(packed["path"]) == h.lines(plain["path"])


def test_a_sliced_run_reads_its_slice_and_names_its_file_and_flag_after_it(harness):
    h = harness
    h.configure(merged=[merged_row(f"Y{i}") for i in range(4)], ml=[])
    result = h.export(my_slice=1, max_slice=2)
    assert h.only("iter_records_slice") == [("iter_records_slice", "merged", 1, 2, True)]
    assert [ujson.loads(l)["yuid"] for l in h.lines(result["path"])] == ["Y1", "Y3"]
    assert result["path"].endswith("export_full_1.jsonl")
    assert h.flag(1).read_text() == "1\n"
    assert not h.flag(0).exists()
    assert (result["records"]["slice"], result["records"]["max_slice"]) == (1, 2)


def test_an_unsliced_run_still_writes_flag_zero(harness):
    h = harness
    h.configure(merged=[merged_row("Y1")], ml=[])
    h.export()
    assert h.flag(0).read_text() == "1\n"


def test_write_flag_false_writes_no_flag(harness):
    h = harness
    h.configure(merged=[merged_row("Y1")], ml=[])
    h.export(write_flag=False)
    assert not h.flag(0).exists()


def test_out_dir_defaults_to_the_exports_dir_and_is_created(harness):
    h = harness
    h.configure(merged=[merged_row("Y1")], ml=[])
    assert not os.path.exists(h.cfgs.exports_dir)
    result = h.export()
    assert os.path.dirname(result["path"]) == h.cfgs.exports_dir


def test_out_dir_is_used_when_given(harness, tmp_path):
    h = harness
    h.configure(merged=[merged_row("Y1")], ml=[])
    out = tmp_path / "elsewhere" / "deeper"
    result = h.export(out_dir=str(out))
    assert Path(result["path"]).parent == out
    assert not os.path.exists(h.cfgs.exports_dir)


def test_progress_memory_cache_and_tidy_up(harness):
    h = harness
    p = Recorder()
    h.configure(merged=[merged_row(f"Y{i}") for i in range(6)], ml=[], estimate=60)
    result = h.export(my_slice=0, max_slice=3, chunk_size=2, progress=p)

    assert [c["phase"] for c in p.calls if "phase" in c] == ["export"]
    assert [c["total"] for c in p.calls if "total" in c] == [20]
    assert [c["advance"] for c in p.calls if "advance" in c] == [2]
    assert p.calls[-1] == {"completed": 2, "done": True}
    assert h.only("memory_cache") == [("memory_cache", True)]
    assert h.only("resume_commits", "put_all") == [("resume_commits", "ml"),
                                                   ("put_all", "localsocket")]
    assert set(result) == {"records", "written", "path"}
    assert result["records"]["phase"] == "export"
    assert result["records"]["complete"] is True
    assert (Path(h.cfgs.log_dir) / "timing-export-0.json").exists()


def test_a_cache_without_an_estimate_gives_no_total(harness):
    h = harness
    p = Recorder()
    cfgs = h.configure(merged=[merged_row("Y1")], ml=[])
    cfgs.results["merged"]["recordcache"] = NoEstimateCache(h.log, "merged", [merged_row("Y1")])
    h.export(progress=p)
    assert [c["total"] for c in p.calls if "total" in c] == [0]


# --- export_sources_slice: run-source-build.py --------------------------------


def test_each_mapped_record_is_stored_qua_and_written_as_json_and_triples(harness):
    h = harness
    h.configure(sources={"aat": [src_row(AAT, "1"), src_row(AAT, "2")]})
    result = h.sources(sources=("aat",))

    jsonl, nt = result["paths"]
    rec_cache = h.cfgs.external["aat"]["recordcache"]
    # stored under the qua'd identifier, which the record now carries
    assert list(rec_cache.stored) == ["1##quaType", "2##quaType"]
    assert rec_cache.stored["1##quaType"]["identifier"] == "1##quaType"
    # the JSON line is the data as ujson writes it by default: slashes escaped
    assert h.lines(jsonl) == [ujson.dumps(src_row(AAT, "1")["data"]),
                              ujson.dumps(src_row(AAT, "2")["data"])]
    assert "\\/" in h.lines(jsonl)[0]
    assert h.lines(nt) == [f"<{AAT}1> <urn:type> <urn:Type> .", f'<{AAT}1> <urn:label> "{AAT}1" .',
                           f"<{AAT}2> <urn:type> <urn:Type> .", f'<{AAT}2> <urn:label> "{AAT}2" .']
    # per record: transform, store, checkpoint, then the triples
    per_record = [c[0] for c in h.only("transform", "store", "checkpoint", "qlever")]
    assert per_record == ["transform", "store", "checkpoint", "qlever"] * 2
    assert result["written"] == 2
    assert jsonl == os.path.join(h.cfgs.exports_dir, "sources_-1.jsonl.gz")
    assert nt == os.path.join(h.cfgs.exports_dir, "sources_-1.nt.gz")


def test_a_record_the_mapper_rejects_is_skipped(harness):
    h = harness
    h.configure(sources={"aat": [src_row(AAT, "1"), src_row(AAT, "2")]}, reject=["1"])
    result = h.sources(sources=("aat",))
    assert list(h.cfgs.external["aat"]["recordcache"].stored) == ["2##quaType"]
    assert len(h.lines(result["paths"][0])) == 1
    assert [c[1] for c in h.only("qlever")] == [f"{AAT}2"]
    assert result["written"] == 1
    assert result["records"]["skipped"] == 1


def test_a_record_the_qlever_mapper_cannot_express_is_json_only_and_logged(harness, caplog):
    h = harness
    h.configure(sources={"aat": [src_row(AAT, "1"), src_row(AAT, "2")]}, qlever_fail=[f"{AAT}1"])
    with caplog.at_level(logging.WARNING, logger="lux_pipeline"):
        result = h.sources(sources=("aat",))
    assert f"*** yuid-1 failed in the qlever mapper: not an IRI: {AAT}1" in caplog.text
    jsonl, nt = result["paths"]
    assert len(h.lines(jsonl)) == 2
    assert all(f"{AAT}2" in line for line in h.lines(nt)) and len(h.lines(nt)) == 2
    assert result["written"] == 2


def test_empty_triples_write_nothing(harness):
    h = harness
    h.configure(sources={"aat": [src_row(AAT, "1")]}, qlever_empty=[f"{AAT}1"])
    result = h.sources(sources=("aat",))
    assert h.lines(result["paths"][1]) == []
    assert len(h.lines(result["paths"][0])) == 1
    assert result["written"] == 1


def test_a_sliced_run_reads_its_slice_without_the_memory_cache(harness):
    h = harness
    h.configure(sources={"aat": [src_row(AAT, "1"), src_row(AAT, "2"), src_row(AAT, "3")]})
    result = h.sources(my_slice=0, max_slice=2, sources=("aat",))
    assert h.only("memory_cache") == [("memory_cache", False)]
    assert h.only("iter_records_slice") == [("iter_records_slice", "aat-data", 0, 2, False)]
    assert list(h.cfgs.external["aat"]["recordcache"].stored) == ["1##quaType", "3##quaType"]
    assert [os.path.basename(p) for p in result["paths"]] == ["sources_0.jsonl.gz", "sources_0.nt.gz"]
    assert result["records"]["slice"] == 0


def test_an_unsliced_run_reads_everything_with_the_memory_cache(harness):
    h = harness
    h.configure(sources={"aat": [src_row(AAT, "1"), src_row(AAT, "2")]})
    result = h.sources(sources=("aat",))
    assert h.only("memory_cache") == [("memory_cache", True)]
    assert h.only("iter_records") == [("iter_records", "aat-data", False)]
    assert result["written"] == 2


def test_the_default_sources_in_order_each_with_their_own_mapper_and_commits(harness):
    h = harness
    p = Recorder()
    h.configure(sources={"aat": [src_row(AAT, "1")], "ulan": [src_row(ULAN, "5"), src_row(ULAN, "6")],
                         "lcsh": [], "tgn": []})
    result = h.sources(progress=p)

    assert h.only("QleverMapper") == [("QleverMapper", "aat"), ("QleverMapper", "ulan"),
                                      ("QleverMapper", "lcsh"), ("QleverMapper", "tgn")]
    commits = h.only("defer_commits", "flush", "resume_commits")
    assert commits == [c for name in ("aat", "ulan", "lcsh", "tgn")
                       for c in (("defer_commits", f"{name}-rec", 1000, 0),
                                 ("flush", f"{name}-rec"), ("resume_commits", f"{name}-rec"))]
    assert [c["description"] for c in p.calls if "description" in c] == ["aat", "ulan", "lcsh", "tgn"]
    # running total across sources, each estimate over max(max_slice, 1)
    assert [c["total"] for c in p.calls if "total" in c] == [1, 3, 3, 3]
    assert [c["phase"] for c in p.calls if "phase" in c] == ["export-sources"]
    assert p.calls[-1] == {"completed": 3, "done": True}
    assert result["records"]["marks"][0][0] == "aat"
    assert h.log[-1] == ("put_all", "localsocket")
    assert result["written"] == 3
    assert (Path(h.cfgs.log_dir) / "timing-export-sources-all.json").exists()


def test_the_directories_default_to_the_exports_dir_and_nt_to_out(harness, tmp_path):
    h = harness
    h.configure(sources={"aat": [src_row(AAT, "1")]})
    out = tmp_path / "out"
    nt = tmp_path / "nt"
    result = h.sources(sources=("aat",), out_dir=str(out), nt_dir=str(nt))
    assert result["paths"] == [str(out / "sources_-1.jsonl.gz"), str(nt / "sources_-1.nt.gz")]

    h.configure(sources={"aat": [src_row(AAT, "1")]})
    result = h.sources(sources=("aat",), out_dir=str(out))
    assert result["paths"] == [str(out / "sources_-1.jsonl.gz"), str(out / "sources_-1.nt.gz")]


def test_the_update_token_is_logged(harness, caplog):
    h = harness
    h.configure(sources={"aat": []})
    with caplog.at_level(logging.INFO, logger="lux_pipeline"):
        h.sources(sources=("aat",))
    assert "Update token is: __20261027__" in caplog.text


# --- export_nt_slice: manage-data.py --nt -------------------------------------


def test_each_merged_record_becomes_its_triples(harness):
    h = harness
    h.configure(merged=[nt_row("Y1"), nt_row("Y2", "Place")])
    result = h.nt()
    assert h.only("QleverMapper") == [("QleverMapper", "marklogic")]
    assert h.only("iter_records") == [("iter_records", "merged", False)]
    assert h.lines(result["path"]) == [
        f"<{LUX}Y1> <urn:type> <urn:Person> .", f'<{LUX}Y1> <urn:label> "{LUX}Y1" .',
        f"<{LUX}Y2> <urn:type> <urn:Place> .", f'<{LUX}Y2> <urn:label> "{LUX}Y2" .']
    assert result["path"] == os.path.join(h.cfgs.exports_dir, "lux_-1.nt.gz")
    assert result["written"] == 2


def test_a_sliced_nt_run_reads_its_slice(harness):
    h = harness
    h.configure(merged=[nt_row(f"Y{i}") for i in range(4)])
    result = h.nt(my_slice=1, max_slice=2)
    assert h.only("iter_records_slice") == [("iter_records_slice", "merged", 1, 2, False)]
    assert [c[1] for c in h.only("qlever")] == [f"{LUX}Y1", f"{LUX}Y3"]
    assert result["path"].endswith("lux_1.nt.gz")
    assert result["records"]["slice"] == 1


def test_a_record_the_qlever_mapper_cannot_express_is_logged_and_skipped(harness, caplog):
    h = harness
    h.configure(merged=[nt_row("Y1"), nt_row("Y2")], qlever_fail=[f"{LUX}Y1"])
    with caplog.at_level(logging.WARNING, logger="lux_pipeline"):
        result = h.nt()
    assert f"*** Y1 failed in the qlever mapper: not an IRI: {LUX}Y1" in caplog.text
    assert all(f"{LUX}Y2" in line for line in h.lines(result["path"]))
    assert len(h.lines(result["path"])) == 2
    assert result["written"] == 1
    assert result["records"]["skipped"] == 1


def test_empty_triples_are_counted_but_write_nothing(harness):
    h = harness
    h.configure(merged=[nt_row("Y1")], qlever_empty=[f"{LUX}Y1"])
    result = h.nt()
    assert h.lines(result["path"]) == []
    assert result["written"] == 1


def test_nt_out_dir_progress_and_tidy_up(harness, tmp_path):
    h = harness
    p = Recorder()
    h.configure(merged=[nt_row(f"Y{i}") for i in range(4)], estimate=40)
    out = tmp_path / "nt"
    result = h.nt(my_slice=0, max_slice=2, out_dir=str(out), progress=p)
    assert Path(result["path"]).parent == out
    assert [c["phase"] for c in p.calls if "phase" in c] == ["export-nt"]
    assert [c["total"] for c in p.calls if "total" in c] == [20]
    assert [c["advance"] for c in p.calls if "advance" in c] == [1, 1]
    assert p.calls[-1] == {"completed": 2, "done": True}
    assert h.log[-1] == ("put_all", "localsocket")
    assert set(result) == {"records", "written", "path"}
    assert (Path(h.cfgs.log_dir) / "timing-export-nt-0.json").exists()
