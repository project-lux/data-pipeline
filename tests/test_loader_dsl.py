"""The loader micro-DSL: every fixture case from the stale
rob_refactor:tests/test-loader.py as a pytest, plus slicing, max_records,
the legacy fallback, the post_process_json shim, deferred commits, index
TSVs and the lmdb container."""

import gzip
import os
import sys
import zlib
from pathlib import Path

import lmdb
import pytest
import ujson as json

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from lux_pipeline.process.base import loader as loader_mod
from lux_pipeline.process.base.loader import Loader, Pointer

BASE = ROOT / "tests" / "loader_tests"


# --- stubs -------------------------------------------------------------------


class StubConfigs:
    def __init__(self, dumps_dir, temp_dir="."):
        self.dumps_dir = str(dumps_dir)
        self.temp_dir = str(temp_dir)


class Recorder:
    """A progress callable that remembers every call."""

    def __init__(self):
        self.calls = []

    def __call__(self, **kw):
        self.calls.append(kw)

    @property
    def advances(self):
        return sum(c.get("advance", 0) for c in self.calls)

    @property
    def totals(self):
        return [c["total"] for c in self.calls if "total" in c]

    @property
    def descriptions(self):
        return [c["description"] for c in self.calls if "description" in c]


class CommittingCache(dict):
    """What the legacy load() needs: a commit()."""

    def commit(self):
        pass


class DeferringCache(dict):
    """A cache with the postgres commit API, recording the calls."""

    def __init__(self):
        super().__init__()
        self.events = []

    def defer_commits(self, every=100, batch=0):
        self.events.append(("defer", every))

    def checkpoint(self):
        self.events.append("checkpoint")

    def resume_commits(self):
        self.events.append("resume")

    def commit(self):
        self.events.append("commit")


def make_loader(path, typ, name="test", cls=Loader, cache=None, tmp=None, **extra):
    cfg = {"name": name, "all_configs": StubConfigs(BASE, tmp or BASE),
           "datacache": {} if cache is None else cache,
           "input_files": {"records": [{"path": str(path), "type": typ}]}}
    cfg.update(extra)
    return cls(cfg)


def load(path, typ, my_slice=0, max_slice=1, **kw):
    rec = Recorder()
    ldr = make_loader(path, typ, **kw)
    ldr.prepare(my_slice, max_slice, progress=rec)
    ldr.process()
    return ldr, rec


# --- the fixture cases -------------------------------------------------------

CASES = [
    ("json", "json/record.json", "json"),
    ("json.gz", "json/record.json.gz", "json.gz"),
    ("other", "other/record.xml", "other"),
    ("zip_json", "zip_json/records.zip", "zip/json"),
    ("tar", "tar/records.tar", "tar/json"),
    ("tgz", "tar/records.tgz", "tar.gz/json"),
    ("tgz_l", "tar/records.tgz", "tar.gz/lines/json"),
    ("tbz", "tar/records.tar.bz2", "tar.bz2/json"),
    ("jsonl", "lines/records.jsonl", "lines/json"),
    ("jsonl.gz", "lines/records.jsonl.gz", "lines.gz/json"),
    ("zip_jsonl", "zip_json/records.zip", "zip/lines/json"),
    ("dir_zip_jsonl", "zip_json", "dir/zip/lines/json"),
    ("dir_jsonl", "dir", "dir/lines/json"),
    ("dir_json", "dir", "dir/json"),
    ("array_json", "array/records.json", "array/raw"),
    ("dict_json", "dict/records.json", "dict/raw"),
    ("tgz_lgz", "tar/records-gz.tar.gz", "tar.gz/lines.gz/json"),
]


@pytest.mark.parametrize("name,rel,typ", CASES, ids=[c[0] for c in CASES])
def test_single_record_fixture(name, rel, typ):
    ldr, rec = load(BASE / rel, typ, name=name)
    assert len(ldr.out_cache) == 1, ldr.out_cache
    assert ldr.uses_dsl is True
    assert rec.advances == 1
    assert rec.descriptions  # one per file
    (data,) = ldr.out_cache.values()
    if typ == "other":
        assert data == {"data": "<record>\n<id>1</id>\n</record>"}
    else:
        assert data == {"id": "1"}


def test_identifier_from_member_name_or_from_the_record():
    # a named file / member: the name without extensions is the identifier
    ldr, _ = load(BASE / "zip_json/records.zip", "zip/json")
    assert list(ldr.out_cache) == ["record"]
    # a line has no name: the record's id is used
    ldr, _ = load(BASE / "lines/records.jsonl", "lines/json")
    assert list(ldr.out_cache) == ["1"]
    # array / dict items with the json format are accepted already parsed
    ldr, _ = load(BASE / "array/records.json", "array/json")
    assert list(ldr.out_cache) == ["1"]


# --- the multi-record file ---------------------------------------------------

MULTI = BASE / "multi/records.jsonl"


def test_multi_all_records_with_counted_total():
    ldr, rec = load(MULTI, "lines/json")
    assert len(ldr.out_cache) == 20
    assert rec.advances == 20
    assert rec.totals == [20]
    assert ldr.seen == 20 and ldr.handled == 20 and ldr.stored == 20


def test_slicing_at_the_format_step():
    ldr, rec = load(MULTI, "lines/json", my_slice=0, max_slice=5)
    assert len(ldr.out_cache) == 4
    assert "5" in ldr.out_cache
    assert set(ldr.out_cache) == {"5", "10", "15", "20"}
    assert ldr.seen == 20 and ldr.handled == 4
    # the counted total is this slice's share
    assert rec.totals == [4]
    assert rec.advances == 4


def test_every_slice_together_covers_the_file_once():
    got = []
    for s in range(5):
        ldr, _ = load(MULTI, "lines/json", my_slice=s, max_slice=5)
        got.extend(ldr.out_cache)
    assert sorted(got, key=int) == [str(i) for i in range(1, 21)]


def test_max_records_stops_the_whole_load():
    rec = Recorder()
    ldr = make_loader(MULTI, "lines/json")
    ldr.prepare(0, 1, max_records=3, progress=rec)
    assert ldr.process() == 3
    assert set(ldr.out_cache) == {"1", "2", "3"}


def test_max_records_counts_this_slices_records():
    ldr = make_loader(MULTI, "lines/json")
    ldr.prepare(1, 2, max_records=3)
    ldr.process()
    assert list(ldr.out_cache) == ["1", "3", "5"]


def test_configured_total_is_reported_per_slice():
    rec = Recorder()
    ldr = make_loader(MULTI, "lines/json", totalRecords=20)
    ldr.prepare(0, 4, progress=rec)
    ldr.process()
    assert rec.totals == [5]


def test_overwrite_false_skips_records_already_in_the_cache():
    cache = {"1": {"id": "old"}, "2": {"id": "old"}}
    ldr = make_loader(MULTI, "lines/json", cache=cache)
    ldr.prepare(0, 1)
    assert ldr.process(overwrite=False) == 18
    assert cache["1"] == {"id": "old"}
    assert cache["3"] == {"id": "3"}


# --- the spec ----------------------------------------------------------------


def test_process_fmt_parses_and_rejects():
    ldr = make_loader("x", "json")
    assert ldr.process_fmt("tar.gz/lines.bz2/json") == [["tar", "gz"], ["lines", "bz2"], ["json"]]
    assert ldr.process_fmt("dir/zip/lines/json") == [["dir"], ["zip"], ["lines"], ["json"]]
    assert ldr.process_fmt("lmdb/json.z") == [["lmdb"], ["json", "z"]]
    assert ldr.process_fmt("dirs/json") == [["dirs"], ["json"]]
    with pytest.raises(ValueError, match="container type 'blob'"):
        ldr.process_fmt("blob/json")
    with pytest.raises(ValueError, match="compression type 'rar'"):
        ldr.process_fmt("tar.rar/json")
    with pytest.raises(ValueError, match="format type 'csv'"):
        ldr.process_fmt("lines/csv")
    with pytest.raises(ValueError, match="compression type 'bz'"):
        ldr.process_fmt("json.bz")
    with pytest.raises(ValueError, match="Badly specified"):
        ldr.process_fmt("tar.gz.bz2/json")


def test_guess_fmt_from_names():
    ldr = make_loader("x", "json")
    assert ldr.guess_fmt("a/b.jsonl.gz") == [["lines", "gz"], ["json"]]
    assert ldr.guess_fmt("a/b.json") == [["json"]]
    assert ldr.guess_fmt("a/b.json.bz2") == [["json", "bz2"]]
    assert ldr.guess_fmt("a/b.tgz") == [["tar", "gz"], ["json"]]
    assert ldr.guess_fmt("a/b.tar.bz2") == [["tar", "bz2"], ["json"]]
    assert ldr.guess_fmt("a/b.zip") == [["zip"], ["json"]]
    assert ldr.guess_fmt(str(BASE / "dir")) == [["dir"], ["json"]]
    with pytest.raises(ValueError, match="set 'type'"):
        ldr.guess_fmt("a/b.csv")


def test_entry_without_type_is_guessed_and_loaded():
    cfg = {"name": "g", "all_configs": StubConfigs(BASE), "datacache": {},
           "input_files": {"records": [{"path": str(BASE / "lines/records.jsonl.gz")}]}}
    ldr = Loader(cfg)
    ldr.prepare(0, 1)
    assert ldr.my_files[0]["fmt"] == [["lines", "gz"], ["json"]]
    ldr.process()
    assert list(ldr.out_cache) == ["1"]


# --- prepare: files, dumps_dir, legacy fallback ------------------------------


def test_bare_filename_is_resolved_against_dumps_dir():
    cfg = {"name": "r", "all_configs": StubConfigs(BASE), "datacache": {},
           "input_files": {"records": [{"path": "records.jsonl", "type": "lines/json"}]},
           "dumps_dir": "multi"}
    ldr = Loader(cfg)
    ldr.prepare(0, 1)
    assert ldr.my_files == [{"path": str(BASE / "multi" / "records.jsonl"), "fmt": [["lines"], ["json"]]}]
    ldr.process()
    assert len(ldr.out_cache) == 20


def test_url_without_path_uses_its_last_component():
    cfg = {"name": "u", "all_configs": StubConfigs(BASE / "lines"), "datacache": {},
           "input_files": {"records": [{"url": "https://example.org/dumps/records.jsonl",
                                        "type": "lines/json"}]}}
    ldr = Loader(cfg)
    ldr.prepare(0, 1)
    assert ldr.my_files[0]["path"] == str(BASE / "lines" / "records.jsonl")


def test_legacy_config_does_not_use_the_dsl():
    cfg = {"name": "old", "all_configs": StubConfigs(BASE), "datacache": {},
           "dumpFilePath": str(BASE / "lines/records.jsonl.gz")}
    ldr = Loader(cfg)
    assert ldr.in_path.endswith("records.jsonl.gz")
    ldr.prepare(0, 1)
    assert ldr.uses_dsl is False
    assert ldr.my_files == []
    assert ldr.process() == 0


def test_dump_file_type_makes_a_legacy_path_a_dsl_source():
    cfg = {"name": "typed", "all_configs": StubConfigs(BASE), "datacache": {},
           "dumpFilePath": str(BASE / "lines/records.jsonl.gz"), "dumpFileType": "lines.gz/json"}
    ldr = Loader(cfg)
    ldr.prepare(0, 1)
    assert ldr.uses_dsl is True
    ldr.process()
    assert list(ldr.out_cache) == ["1"]


def test_unknown_load_type_loads_nothing_but_is_still_dsl(caplog):
    ldr = make_loader(MULTI, "lines/json")
    ldr.prepare(0, 1, load_type="export")
    assert ldr.uses_dsl is True and ldr.my_files == []


class OldStyleLoader(Loader):
    """A source loader as several on main are written: its own __init__
    that never calls the base, and its own load()."""

    def __init__(self, config):
        self.in_path = config["dumpFilePath"]
        self.out_cache = config["datacache"]
        self.total = config.get("totalRecords", -1)

    def get_identifier_json(self, js):
        return f"old-{js['id']}"


def test_subclass_that_skips_base_init_still_prepares():
    ldr = OldStyleLoader({"dumpFilePath": str(BASE / "lines/records.jsonl.gz"),
                          "datacache": CommittingCache()})
    ldr.prepare(0, 1)
    assert ldr.uses_dsl is False
    # and its legacy load() is main's, untouched
    ldr.load()
    assert list(ldr.out_cache) == ["old-1"]


def test_legacy_load_default_gzipped_jsonl():
    class L(Loader):
        def get_identifier_json(self, js):
            return js["id"]
    ldr = L({"name": "l", "datacache": CommittingCache(),
             "dumpFilePath": str(BASE / "lines/records.jsonl.gz")})
    ldr.load()
    assert ldr.out_cache == {"1": {"id": "1"}}


# --- hooks -------------------------------------------------------------------


class OneArg(Loader):
    def post_process_json(self, js):
        js["seen_by"] = "one"
        return js


class TwoArg(Loader):
    def post_process_json(self, js, identifier):
        js["seen_by"] = f"two:{identifier}"
        return js


class Dropper(Loader):
    def post_process_json(self, js):
        return None if js["id"] in ("2", "3") else js


def test_post_process_json_shim_handles_both_signatures():
    ldr, _ = load(BASE / "zip_json/records.zip", "zip/json", cls=OneArg)
    assert ldr.out_cache["record"]["seen_by"] == "one"
    ldr, _ = load(BASE / "zip_json/records.zip", "zip/json", cls=TwoArg)
    assert ldr.out_cache["record"]["seen_by"] == "two:record"
    ldr, _ = load(BASE / "lines/records.jsonl", "lines/json", cls=TwoArg)
    assert ldr.out_cache["1"]["seen_by"] == "two:None"


def test_post_process_json_returning_none_drops_the_record():
    ldr, rec = load(MULTI, "lines/json", cls=Dropper)
    assert len(ldr.out_cache) == 18 and "2" not in ldr.out_cache
    assert rec.advances == 18


class SkipGz(Loader):
    def should_process_item(self, child):
        name = child.get_name() if isinstance(child, Pointer) else str(child)
        return not name.endswith(".gz")


def test_should_process_item_filters_children():
    ldr, _ = load(BASE / "tar/records-gz.tar.gz", "tar.gz/lines.gz/json", cls=SkipGz)
    assert ldr.out_cache == {}


class SubLoader(Loader):
    def extract_identifier(self, data):
        return f"sub-{data['id']}"


def test_extract_identifier_override():
    ldr, _ = load(BASE / "dict/records.json", "dict/raw", cls=SubLoader)
    assert list(ldr.out_cache) == ["sub-1"]


# --- commits, temp files, failures ------------------------------------------


def test_commits_are_deferred_and_checkpointed_per_record():
    cache = DeferringCache()
    ldr, _ = load(MULTI, "lines/json", cache=cache)
    assert len(cache) == 20
    assert cache.events[0] == ("defer", 1000)
    assert cache.events.count("checkpoint") == 20
    assert cache.events[-1] == "resume"
    assert "commit" not in cache.events


def test_plain_dict_cache_needs_no_commit_api():
    ldr, _ = load(MULTI, "lines/json", cache={})
    assert len(ldr.out_cache) == 20


class IndexingMapper:
    def extract_index_data(self, record):
        d = record["data"]
        return {"label": [(f"name {d['id']}", d["id"])],
                "equiv": [(d["id"], f"http://x/{d['id']}")],
                "ignored": [("a", "b")]}


def test_index_tsvs_are_written_per_slice(tmp_path):
    ldr = make_loader(MULTI, "lines/json", tmp=tmp_path, mapper=IndexingMapper(),
                      reconcileDbPath="/idx/labels.lmdb", inverseEquivDbPath="/idx/equivs.lmdb")
    ldr.prepare(1, 2)
    ldr.process()
    labels = (tmp_path / "test_labels_1.tsv").read_text().splitlines()
    equivs = (tmp_path / "test_equivs_1.tsv").read_text().splitlines()
    assert len(labels) == 10 and labels[0] == "name 1\t1"
    assert len(equivs) == 10 and equivs[-1] == "19\thttp://x/19"
    assert not (tmp_path / "test_diffs_1.tsv").exists()
    assert ldr.temp_file_handles == {}


def test_no_temp_files_without_a_mapper(tmp_path):
    ldr = make_loader(MULTI, "lines/json", tmp=tmp_path, reconcileDbPath="/idx/labels.lmdb")
    ldr.prepare(0, 1)
    ldr.process()
    assert list(tmp_path.iterdir()) == []


def test_a_failing_file_is_logged_and_the_rest_still_load(tmp_path, caplog):
    cfg = {"name": "f", "all_configs": StubConfigs(BASE), "datacache": {},
           "input_files": {"records": [
               {"path": str(tmp_path / "missing.jsonl"), "type": "lines/json"},
               {"path": str(MULTI), "type": "lines/json"}]}}
    ldr = Loader(cfg)
    ldr.prepare(0, 1)
    with pytest.raises(RuntimeError, match="1 file\\(s\\) failed"):
        ldr.process()
    assert len(ldr.out_cache) == 20
    assert "Failed to load" in caplog.text


# --- containers that need built fixtures -------------------------------------


def test_dirs_recurses_in_sorted_order(tmp_path):
    for sub, ids in (("b", ["3", "4"]), ("a/aa", ["1"]), ("a", ["2"])):
        d = tmp_path / sub
        d.mkdir(parents=True, exist_ok=True)
        for i in ids:
            (d / f"{i}.json").write_text(json.dumps({"id": i}))
    rec = Recorder()
    ldr = make_loader(tmp_path, "dirs/json")
    ldr.prepare(0, 1, progress=rec)
    ldr.process()
    assert list(ldr.out_cache) == ["2", "1", "3", "4"]
    assert rec.totals[-1] == 4


def test_arraylines(tmp_path):
    fn = tmp_path / "arr.json"
    fn.write_text('[\n{"id": "1"},\n{"id": "2"},\n{"id": "3"}\n]\n')
    ldr, _ = load(fn, "arraylines/json")
    assert list(ldr.out_cache) == ["1", "2", "3"]


def test_xmls(tmp_path):
    fn = tmp_path / "recs.xml"
    fn.write_text("<root><rec><id>a</id></rec><rec><id>b</id></rec></root>")

    class XmlLoader(Loader):
        def post_process_other(self, data):
            return {"id": data.split("<id>")[1].split("</id>")[0], "xml": data}

    ldr, rec = load(fn, "xmls/other", cls=XmlLoader)
    assert list(ldr.out_cache) == ["a", "b"]
    assert ldr.out_cache["a"]["xml"].startswith("<rec>")
    assert rec.totals == [2]


def test_lmdb_container_with_zlib_records(tmp_path):
    path = str(tmp_path / "dist.lmdb")
    env = lmdb.open(path, max_dbs=3, map_size=2**24)
    db = env.open_db(b"data")
    with env.begin(write=True, db=db) as txn:
        for name, ident in (("aat", "300000001"), ("aat", "300000002"), ("ulan", "500000001")):
            txn.put(f"{name}:{ident}".encode(), zlib.compress(json.dumps({"id": ident}).encode()))
    env.close()

    rec = Recorder()
    ldr = make_loader(path, "lmdb/json.z", name="aat")
    ldr.prepare(0, 1, load_type="records", progress=rec)
    ldr.process()
    assert list(ldr.out_cache) == ["300000001", "300000002"]
    assert ldr._lmdb_envs == {}  # closed after the load
    assert ldr.lmdb_count_entries(path) == 2


def test_lmdb_load_type_counts_for_the_total(tmp_path):
    path = str(tmp_path / "dist.lmdb")
    env = lmdb.open(path, max_dbs=3, map_size=2**24)
    db = env.open_db(b"data")
    with env.begin(write=True, db=db) as txn:
        for i in range(6):
            txn.put(f"src:{i}".encode(), json.dumps({"id": str(i)}).encode())
    env.close()
    cfg = {"name": "src", "all_configs": StubConfigs(BASE), "datacache": {},
           "input_files": {"lmdb": [{"path": path, "type": "lmdb/json"}]}}
    rec = Recorder()
    ldr = Loader(cfg)
    ldr.prepare(0, 2, load_type="lmdb", progress=rec)
    assert ldr.total == 6
    ldr.process()
    assert rec.totals == [3]
    assert set(ldr.out_cache) == {"1", "3", "5"}



# --- prepare(path=..., path_type=...): lux load --file / --file-type ------------


def test_file_override_keeps_the_configured_type():
    # configured: one jsonl.gz; the override is another file of that shape
    ldr = make_loader(BASE / "missing.jsonl.gz", "lines.gz/json")
    ldr.prepare(0, 1, path=str(BASE / "lines/records.jsonl.gz"))
    assert ldr.my_files == [{"path": str(BASE / "lines/records.jsonl.gz"),
                             "fmt": [["lines", "gz"], ["json"]]}]
    ldr.process()
    assert len(ldr.out_cache) == 1


def test_file_type_overrides_the_configured_type():
    ldr = make_loader(BASE / "missing.jsonl.gz", "lines.gz/json")
    ldr.prepare(0, 1, path=str(BASE / "zip_json/records.zip"), path_type="zip/json")
    ldr.process()
    assert list(ldr.out_cache) == ["record"]


def test_file_override_guesses_without_a_single_configured_type():
    cfg = {"name": "t", "all_configs": StubConfigs(BASE), "datacache": {},
           "input_files": {"records": [{"path": "a.jsonl"}, {"path": "b.jsonl"}]}}
    ldr = Loader(cfg)
    ldr.prepare(0, 1, path=str(BASE / "lines/records.jsonl"))
    assert [f["path"] for f in ldr.my_files] == [str(BASE / "lines/records.jsonl")]
    ldr.process()
    assert len(ldr.out_cache) == 1


def test_file_override_for_a_legacy_loader_replaces_in_path():
    class Legacy(Loader):
        def __init__(self, config):
            self.config = config
            self.in_path = config["dumpFilePath"]

        def load(self):
            return self.in_path

    ldr = Legacy({"name": "old", "dumpFilePath": "/configured.gz", "all_configs": StubConfigs(BASE)})
    ldr.prepare(0, 1, path="/elsewhere.gz")
    assert ldr.uses_dsl is False and ldr.load() == "/elsewhere.gz"
    with pytest.raises(ValueError, match="reads its file its own way"):
        ldr.prepare(0, 1, path="/elsewhere.gz", path_type="lines.gz/json")
    with pytest.raises(ValueError, match="give --file-type"):
        ldr.prepare(0, 1, load_type="lmdb", path="/elsewhere.lmdb")


def test_file_override_with_a_type_puts_a_plain_dump_loader_on_the_dsl():
    # base Loader, dumpFilePath and no type: load() would read in_path; a
    # type sends it through the DSL instead
    cfg = {"name": "plain", "dumpFilePath": "/configured.jsonl.gz", "datacache": {},
           "all_configs": StubConfigs(BASE)}
    ldr = Loader(cfg)
    ldr.prepare(0, 1, path=str(BASE / "lines/records.jsonl"), path_type="lines/json")
    assert ldr.uses_dsl is True
    ldr.process()
    assert len(ldr.out_cache) == 1


def write_tab_export(fn, rows, gz=True):
    opener = gzip.open if gz else open
    with opener(fn, "wt") as fh:
        for line in rows:
            fh.write(line + "\n")


def test_file_override_for_an_export_goes_through_the_dsl(tmp_path):
    fn = tmp_path / "export_test.jsonl.gz"
    write_tab_export(fn, ['r1\t{"data": {"id": "1"}}', 'r2\t{"data": {"id": "2"}}'])
    ldr = make_loader(BASE / "lines/records.jsonl", "lines/json")
    ldr.prepare(0, 1, load_type="export", path=str(fn))
    # no export file configured, but its content says what it is
    assert ldr.my_files == [{"path": str(fn), "fmt": [["lines", "gz"], ["tabjson"]]}]
    ldr.process()
    assert ldr.out_cache == {"r1": {"id": "1"}, "r2": {"id": "2"}}


def test_file_override_with_unknown_content_and_no_export_configured_uses_load_export2(tmp_path):
    fn = tmp_path / "export_test.bin"
    fn.write_bytes(b"\x00\x01 not anything")
    cache = DeferringCache()
    ldr = make_loader(BASE / "lines/records.jsonl", "lines/json", cache=cache)
    ldr.prepare(0, 1, load_type="export", path=str(fn))
    assert ldr.my_files == [] and ldr.export_path == str(fn)
    # the next run without an override goes back to the default path
    ldr.prepare(0, 1, load_type="export")
    assert ldr.export_path is None


def test_file_override_content_beats_the_configured_type(tmp_path):
    # aat: its configured export is a zip of bz2 json; the file given is a
    # tab-prefixed export, which must not be opened as a zip
    fn = tmp_path / "export_aat.jsonl.gz"
    write_tab_export(fn, ['300025962\t{"id": "http://vocab.getty.edu/aat/300025962"}'])
    cfg = {"name": "aat", "all_configs": StubConfigs(BASE), "datacache": {},
           "input_files": {"export": [{"path": "aat/aat.zip", "type": "zip.bz2/json"}]}}
    ldr = Loader(cfg)
    ldr.prepare(0, 1, load_type="export", path=str(fn))
    assert ldr.my_files[0]["fmt"] == [["lines", "gz"], ["tabjson"]]
    ldr.process()
    assert ldr.out_cache == {"300025962": {"id": "http://vocab.getty.edu/aat/300025962"}}


def test_file_override_falls_back_to_the_configured_type_when_content_is_unknown(tmp_path):
    fn = tmp_path / "odd.dat"
    fn.write_bytes(b"\x00\x01 not anything")
    ldr = make_loader(BASE / "missing.jsonl.gz", "lines.gz/json")
    ldr.prepare(0, 1, path=str(fn))
    assert ldr.my_files[0]["fmt"] == [["lines", "gz"], ["json"]]


# --- tabjson -----------------------------------------------------------------


def test_tabjson_takes_the_identifier_from_the_prefix(tmp_path):
    fn = tmp_path / "ids.jsonl"
    write_tab_export(fn, ['a1\t{"id": "http://x.org/other"}',
                          'a2\t{"data": {"id": "http://x.org/2"}}',
                          'a3\t{"data": {"id": "3"}, "extra": 1}'], gz=False)
    ldr, _ = load(fn, "lines/tabjson")
    # the prefix wins over the record's id; a bare {"data": ...} is unwrapped
    assert ldr.out_cache == {"a1": {"id": "http://x.org/other"}, "a2": {"id": "http://x.org/2"},
                             "a3": {"data": {"id": "3"}, "extra": 1}}


def test_tabjson_is_sliced_like_any_format(tmp_path):
    fn = tmp_path / "ids.jsonl.gz"
    write_tab_export(fn, [f'r{i}\t{{"id": "{i}"}}' for i in range(5)])
    got = []
    for n in range(2):
        ldr, _ = load(fn, "lines.gz/tabjson", my_slice=n, max_slice=2)
        got.append(sorted(ldr.out_cache))
    # the same split as every format: together exactly once
    assert sorted(got[0] + got[1]) == [f"r{i}" for i in range(5)]
    assert not set(got[0]) & set(got[1]) and got[0] and got[1]


def test_tabjson_post_processes_with_the_identifier(tmp_path):
    class Upper(Loader):
        def post_process_json(self, data, identifier):
            return {**data, "seen_as": identifier}

    fn = tmp_path / "ids.jsonl"
    write_tab_export(fn, ['k\t{"id": "1"}'], gz=False)
    ldr, _ = load(fn, "lines/tabjson", cls=Upper)
    assert ldr.out_cache == {"k": {"id": "1", "seen_as": "k"}}


def test_tabjson_line_without_a_prefix_fails_the_file(tmp_path):
    fn = tmp_path / "ids.jsonl"
    write_tab_export(fn, ['{"id": "1"}'], gz=False)
    ldr = make_loader(fn, "lines/tabjson")
    ldr.prepare(0, 1)
    with pytest.raises(RuntimeError, match="1 file\\(s\\) failed"):
        ldr.process()
