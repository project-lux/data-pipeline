"""The dump sniffer (lux_pipeline/process/utils/sniff.py): every checked-in
loader fixture sniffs to a spec that loads it, built fixtures cover the
shapes the checked-in set lacks, and Loader.guess_fmt() asks the sniffer."""

import bz2
import gzip
import io
import logging
import os
import sqlite3
import sys
import tarfile
import zipfile
import zlib
from pathlib import Path

import lmdb
import pytest
import ujson as json

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from lux_pipeline.process.base.loader import Loader
from lux_pipeline.process.utils import sniff
from lux_pipeline.process.utils.sniff import (discover_input_files, sniff_compression,
                                              sniff_container, sniff_spec)

BASE = ROOT / "tests" / "loader_tests"


class StubConfigs:
    def __init__(self, dumps_dir):
        self.dumps_dir = str(dumps_dir)
        self.temp_dir = str(dumps_dir)


def load_with(path, spec, name="t"):
    cfg = {"name": name, "all_configs": StubConfigs(BASE), "datacache": {},
           "input_files": {"records": [{"path": str(path), "type": spec}]}}
    ldr = Loader(cfg)
    ldr.prepare(0, 1)
    ldr.process()
    return ldr.out_cache


def spec_of(path):
    found = sniff_spec(str(path))
    assert found.placed, found.reason
    return found.spec


# --- the checked-in fixtures -------------------------------------------------

FIXTURE_FILES = sorted(str(p.relative_to(BASE)) for p in BASE.glob("*/*"))
FIXTURE_DIRS = sorted(p.name for p in BASE.iterdir() if p.is_dir())


@pytest.mark.parametrize("rel", FIXTURE_FILES)
def test_every_fixture_file_sniffs_to_a_spec_that_loads_it(rel):
    spec = spec_of(BASE / rel)
    records = load_with(BASE / rel, spec)
    assert records, f"{rel} as {spec} loaded nothing"
    if spec.startswith("other") or "/other" in spec:
        assert all("data" in r for r in records.values())
    else:
        assert all(r.get("id") for r in records.values())


@pytest.mark.parametrize("rel", FIXTURE_DIRS)
def test_every_fixture_directory_is_described_by_entries_that_load_it(rel):
    found = discover_input_files(str(BASE / rel))
    assert [f.path for f in found] == sorted(os.listdir(BASE / rel))
    for f in found:
        assert load_with(BASE / rel / f.path, f.spec), f.path


@pytest.mark.parametrize("rel", ["dir", "multi", "zip_json", "array"])
def test_a_uniform_fixture_directory_is_one_dir_spec(rel):
    spec = spec_of(BASE / rel)
    assert spec.startswith("dir/")
    assert load_with(BASE / rel, spec)


def test_a_mixed_directory_takes_the_majority_and_says_so():
    found = sniff_spec(str(BASE / "tar"))
    assert found.spec.startswith("dir/tar")
    assert any("members disagree" in n for n in found.notes)


def test_fixture_specs_are_the_item_12_conventions():
    expected = {
        "array/records.json": "array/raw",
        "dict/records.json": "dict/raw",
        "json/record.json": "json",
        "json/record.json.gz": "json.gz",
        "multi/records.jsonl": "lines/json",
        "lines/records.jsonl.gz": "lines.gz/json",
        "other/record.xml": "other",
        "tar/records.tar": "tar/json",
        "tar/records.tar.bz2": "tar.bz2/json",
        "zip_json/records.zip": "zip/json",
    }
    for rel, spec in expected.items():
        assert spec_of(BASE / rel) == spec, rel
    # one record per member: tar.gz/lines/json would load it too
    assert spec_of(BASE / "tar/records.tgz") in ("tar.gz/json", "tar.gz/lines/json")
    assert spec_of(BASE / "tar/records-gz.tar.gz") in ("tar.gz/json.gz", "tar.gz/lines.gz/json")


# --- bytes -------------------------------------------------------------------


def test_compression_and_container_from_bytes(tmp_path):
    assert sniff_compression(gzip.compress(b"{}")) == "gz"
    assert sniff_compression(bz2.compress(b"{}")) == "bz2"
    assert sniff_compression(zlib.compress(b"{}")) == "z"
    assert sniff_compression(b"BZhello") is None   # BZh then a block-size digit
    assert sniff_compression(b'{"id": 1}') is None
    assert sniff_container(str(BASE / "tar/records.tgz")) == ("tar", "gz")
    assert sniff_container(str(BASE / "tar/records.tar")) == ("tar", None)
    assert sniff_container(str(BASE / "zip_json/records.zip")) == ("zip", None)
    assert sniff_container(str(BASE / "lines/records.jsonl.gz")) == (None, "gz")
    assert sniff_container(str(BASE / "dir")) == ("dir", None)
    (tmp_path / "empty").mkdir()
    assert sniff_container(str(tmp_path / "empty")) == (None, None)


# --- built fixtures: what the checked-in set lacks ---------------------------


def write(path, text):
    path.write_text(text)
    return path


def test_arraylines(tmp_path):
    fn = write(tmp_path / "a.json", '[\n{"id": "1"},\n{"id": "2"},\n{"id": "3"}\n]\n')
    assert spec_of(fn) == "arraylines/json"
    assert list(load_with(fn, "arraylines/json")) == ["1", "2", "3"]
    # the first record on the bracket's line
    fn = write(tmp_path / "b.json", '[{"id": "1"},\n{"id": "2"}]\n')
    assert spec_of(fn) == "arraylines/json"
    assert list(load_with(fn, "arraylines/json")) == ["1", "2"]


def test_array_on_one_line_or_pretty_printed_is_parsed_whole(tmp_path):
    fn = write(tmp_path / "one.json", '[{"id": "1"}, {"id": "2"}]')
    assert spec_of(fn) == "array/raw"
    fn = write(tmp_path / "pretty.json", json.dumps([{"id": "1"}, {"id": "2"}], indent=2))
    assert spec_of(fn) == "array/raw"
    assert list(load_with(fn, "array/raw")) == ["1", "2"]


def test_large_array_is_noted(tmp_path, monkeypatch):
    monkeypatch.setattr(sniff, "LARGE_ARRAY", 10)
    found = sniff_spec(str(write(tmp_path / "big.json", '[{"id": "1"}, {"id": "2"}]')))
    assert found.spec == "array/raw"
    assert any("parses all" in n for n in found.notes)


def test_dict_of_records_versus_a_single_record(tmp_path):
    recs = write(tmp_path / "d.json", json.dumps({"a": {"id": "1"}, "b": {"id": "2"}}, indent=2))
    assert spec_of(recs) == "dict/raw"
    one = write(tmp_path / "r.json", json.dumps({"id": "1", "part": {"x": 1}}, indent=2))
    assert spec_of(one) == "json"
    assert sniff_spec(str(one)).evidence["keys"] == ["id"]


def test_large_single_object_is_json_with_a_note(tmp_path, monkeypatch):
    monkeypatch.setattr(sniff, "WHOLE_OBJECT_BELOW", 10)
    found = sniff_spec(str(write(tmp_path / "r.json", json.dumps({"id": "1", "x": "y" * 40}))))
    assert found.spec == "json"
    assert any("verify" in n for n in found.notes)


def test_json_lines_with_a_first_line_longer_than_the_sample(tmp_path, monkeypatch):
    monkeypatch.setattr(sniff, "SAMPLE_BYTES", 50)
    lines = [json.dumps({"id": str(i), "pad": "x" * 100}) for i in range(3)]
    fn = write(tmp_path / "r.json", "\n".join(lines) + "\n")
    assert spec_of(fn) == "lines/json"


def test_one_line_object_is_lines_only_when_the_name_says_so(tmp_path):
    assert spec_of(write(tmp_path / "a.jsonl", '{"id": "1"}\n')) == "lines/json"
    assert spec_of(write(tmp_path / "a.ndjson", '{"id": "1"}\n')) == "lines/json"
    assert spec_of(write(tmp_path / "a.json", '{"id": "1"}\n')) == "json"


def test_compressed_content_puts_the_compression_on_the_first_step(tmp_path):
    fn = tmp_path / "a.json.bz2"
    fn.write_bytes(bz2.compress(b'[\n{"id": "1"},\n{"id": "2"}\n]\n'))
    assert spec_of(fn) == "arraylines.bz2/json"
    assert list(load_with(fn, "arraylines.bz2/json")) == ["1", "2"]


def test_xml_collection_single_record_and_lines(tmp_path):
    coll = write(tmp_path / "c.xml", "<?xml version='1.0'?>\n<root>\n<rec><id>a</id></rec>\n"
                                     "<rec><id>b</id></rec>\n</root>\n")
    assert spec_of(coll) == "xmls/other"
    one = write(tmp_path / "o.xml", "<record><id>1</id><title>t</title></record>")
    assert spec_of(one) == "other"
    lines = write(tmp_path / "l.xml", "<rec><id>a</id></rec>\n<rec><id>b</id></rec>\n")
    assert spec_of(lines) == "lines/other"


def test_concatenated_xml_documents_are_unplaceable(tmp_path):
    fn = write(tmp_path / "cat.xml", "<rec><id>a</id>\n</rec><rec><id>b</id>\n</rec>")
    found = sniff_spec(str(fn))
    assert not found.placed and "several XML documents" in found.reason


def test_lmdb_directory(tmp_path):
    path = tmp_path / "dist.lmdb"
    env = lmdb.open(str(path), max_dbs=3, map_size=2**24)
    db = env.open_db(b"data")
    with env.begin(write=True, db=db) as txn:
        txn.put(b"aat:1", zlib.compress(json.dumps({"id": "1"}).encode()))
    env.close()
    found = sniff_spec(str(path))
    assert found.spec == "lmdb/json.z"
    assert found.evidence["member"] == "aat:1"
    assert load_with(path, found.spec, name="aat") == {"1": {"id": "1"}}


def test_zip_with_a_csv_to_ignore(tmp_path):
    """The ROR shape: the records array beside a CSV of the same data."""
    fn = tmp_path / "v1-ror-data.zip"
    with zipfile.ZipFile(fn, "w") as zf:
        zf.writestr("v1-ror-data.csv", "id,name\n1,x\n")
        zf.writestr("v1-ror-data.json", json.dumps([{"id": "1"}, {"id": "2"}]))
        zf.writestr("__MACOSX/._v1-ror-data.json", "junk")
        zf.writestr("sub/", "")
    found = sniff_spec(str(fn))
    assert found.spec == "zip/array/raw"
    assert found.ignored == ["v1-ror-data.csv", "__MACOSX/._v1-ror-data.json"]
    assert any("should_process_item" in n for n in found.notes)
    assert found.evidence["member"] == "v1-ror-data.json"


def test_zip_with_only_ignored_members_is_unplaceable(tmp_path):
    fn = tmp_path / "x.zip"
    with zipfile.ZipFile(fn, "w") as zf:
        zf.writestr("data.csv", "a,b\n")
    found = sniff_spec(str(fn))
    assert not found.placed and found.reason == "every member is ignored"


def test_mixed_member_archive_takes_the_majority_and_says_so(tmp_path):
    fn = tmp_path / "mixed.tar.gz"
    with tarfile.open(fn, "w:gz") as tf:
        for name, body in (("a.json", b'{"id": "1"}\n{"id": "2"}\n'),
                           ("b.json", b'{"id": "3"}\n{"id": "4"}\n'),
                           ("c.json", b'[{"id": "5"}]')):
            ti = tarfile.TarInfo(name)
            ti.size = len(body)
            tf.addfile(ti, io.BytesIO(body))
    found = sniff_spec(str(fn))
    assert found.spec == "tar.gz/lines/json"
    assert any("members disagree" in n for n in found.notes)


def test_archive_inside_an_archive_is_not_placed(tmp_path):
    inner = io.BytesIO()
    with zipfile.ZipFile(inner, "w") as zf:
        zf.writestr("r.json", '{"id": "1"}')
    fn = tmp_path / "outer.zip"
    with zipfile.ZipFile(fn, "w") as zf:
        zf.writestr("inner.json", inner.getvalue())
    found = sniff_spec(str(fn))
    assert not found.placed and "inside an archive" in found.reason


@pytest.mark.parametrize("name,body,reason", [
    ("tsv", "id\tname\n1\tx\n", "delimited"),
    ("csv", "id,name\n1,x\n", "delimited"),
    ("nt", '<http://a> <http://b> "c" .\n', "N-Triples"),
    ("bnode", '_:b0 <http://b> "c" .\n', "N-Triples"),
    ("empty", "", "empty file"),
    ("blank", "\n\n  \n", "empty file"),
    ("text", "hello world\n", "unrecognised content"),
])
def test_unplaceable_text(tmp_path, name, body, reason):
    found = sniff_spec(str(write(tmp_path / name, body)))
    assert not found.placed and reason in found.reason


def test_sqlite_is_unplaceable(tmp_path):
    fn = tmp_path / "latest_wof.db"
    con = sqlite3.connect(fn)
    con.execute("create table t (x)")
    con.commit()
    con.close()
    found = sniff_spec(str(fn))
    assert not found.placed and found.reason == "SQLite database"


def test_missing_path_is_unplaceable(tmp_path):
    assert sniff_spec(str(tmp_path / "nope")).reason == "no such file or directory"


# --- directories -------------------------------------------------------------


def test_dirs_and_dir(tmp_path):
    for sub, i in (("a", "1"), ("b/c", "2")):
        (tmp_path / "tree" / sub).mkdir(parents=True)
        write(tmp_path / "tree" / sub / f"{i}.json", json.dumps({"id": i}))
    assert spec_of(tmp_path / "tree") == "dirs/json"
    assert sorted(load_with(tmp_path / "tree", "dirs/json")) == ["1", "2"]

    flat = tmp_path / "flat"
    flat.mkdir()
    write(flat / "1.json", json.dumps({"id": "1"}))
    (flat / "sub").mkdir()
    found = sniff_spec(str(flat))
    assert found.spec == "dir/json"
    assert any("not read" in n for n in found.notes)


def test_discover_one_entry_per_file_relative_and_sorted(tmp_path):
    write(tmp_path / "b.jsonl", '{"id": "1"}\n{"id": "2"}\n')
    write(tmp_path / "a.json", json.dumps({"id": "3"}))
    write(tmp_path / ".hidden", "x")
    write(tmp_path / "c.tsv", "a\tb\n")
    (tmp_path / "recs").mkdir()
    write(tmp_path / "recs" / "r.json", json.dumps({"id": "4"}))
    found = discover_input_files(str(tmp_path))
    assert [(f.path, f.spec) for f in found] == [
        ("a.json", "json"), ("b.jsonl", "lines/json"), ("c.tsv", None), ("recs", "dir/json")]
    assert "delimited" in found[2].reason


def test_discover_a_whole_directory_and_paths_outside(tmp_path):
    (tmp_path / "x" / "a").mkdir(parents=True)
    write(tmp_path / "x" / "a" / "1.json", json.dumps({"id": "1"}))
    (found,) = discover_input_files(str(tmp_path / "x"))
    assert (found.path, found.spec) == (".", "dirs/json")
    # relative to somewhere it is not under: absolute
    (found,) = discover_input_files(str(tmp_path / "x"), relative_to=str(tmp_path / "other"))
    assert found.path == str(tmp_path / "x")
    # a file on its own
    (found,) = discover_input_files(str(tmp_path / "x" / "a" / "1.json"))
    assert (found.path, found.spec) == ("1.json", "json")


# --- Loader.guess_fmt --------------------------------------------------------


def make_loader():
    return Loader({"name": "src", "all_configs": StubConfigs(BASE), "datacache": {}})


def test_guess_fmt_sniffs_content_and_logs_it(tmp_path, caplog):
    fn = write(tmp_path / "records.json", '[\n{"id": "1"},\n{"id": "2"}\n]\n')
    with caplog.at_level(logging.INFO, logger="lux_pipeline"):
        assert make_loader().guess_fmt(str(fn)) == [["arraylines"], ["json"]]
    assert f"src: guessed arraylines/json for {fn}" in caplog.text


def test_guess_fmt_unplaceable_says_set_type(tmp_path):
    fn = write(tmp_path / "data.json", "id\tname\n")
    with pytest.raises(ValueError, match=r"delimited.*set 'type'"):
        make_loader().guess_fmt(str(fn))


def test_entry_without_type_loads_through_the_sniffer(tmp_path):
    fn = write(tmp_path / "records.json", json.dumps({"a": {"id": "1"}, "b": {"id": "2"}}))
    cfg = {"name": "g", "all_configs": StubConfigs(tmp_path), "datacache": {},
           "input_files": {"records": [{"path": "records.json"}]}}
    ldr = Loader(cfg)
    ldr.prepare(0, 1)
    assert ldr.my_files[0]["fmt"] == [["dict"], ["raw"]]
    ldr.process()
    assert sorted(ldr.out_cache) == ["1", "2"]


# --- identifier<TAB>JSON lines (lux export --type external) -----------------


@pytest.mark.parametrize("gz,spec", [(False, "lines/tabjson"), (True, "lines.gz/tabjson")])
def test_tab_prefixed_json_lines(tmp_path, gz, spec):
    fn = tmp_path / ("export_aat.jsonl" + (".gz" if gz else ""))
    body = '300025962\t{"data": {"id": "1"}}\n300025963\t{"data": {"id": "2"}}\n'
    fn.write_bytes(gzip.compress(body.encode()) if gz else body.encode())
    found = sniff.sniff_spec(str(fn))
    assert found.spec == spec
    assert sorted(load_with(fn, spec)) == ["300025962", "300025963"]


def test_a_tab_prefixed_first_line_longer_than_the_sample(tmp_path, monkeypatch):
    monkeypatch.setattr(sniff, "SAMPLE_BYTES", 64)
    fn = tmp_path / "big.jsonl"
    fn.write_text('k\t{"id": "' + "x" * 200 + '"}\n')
    found = sniff.sniff_spec(str(fn))
    assert found.spec == "lines/tabjson"
    assert any("longer than the sample" in n for n in found.notes)


@pytest.mark.parametrize("line", ['a b\t{"id": 1}', 'a\tb\tc', '\t{"id": 1}', 'a\t{not json'])
def test_other_tab_lines_are_not_tabjson(tmp_path, line):
    fn = tmp_path / "x.txt"
    fn.write_text(line + "\n" + line + "\n")
    assert sniff.sniff_spec(str(fn)).spec != "lines/tabjson"
