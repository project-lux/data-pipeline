"""``"slice": "files"`` in input_files: each worker reads whole files (its
share of a directory, or of the entries) instead of every worker reading
every file; and the library loader (YulLoader) on the DSL with it."""

import gzip
import sys
from pathlib import Path

import pytest
import ujson as json

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from lux_pipeline.process.base.loader import Loader
from lux_pipeline.sources.yale.library.loader import NAMESPACE, YulLoader


class StubConfigs:
    def __init__(self, dumps_dir):
        self.dumps_dir = str(dumps_dir)
        self.temp_dir = str(dumps_dir)


class Opening(Loader):
    """Remembers every file it opens."""

    def file_opener(self, path, comp):
        if isinstance(path, str):
            self.opened.append(Path(path).name)
        return super().file_opener(path, comp)


def write_dump(dirpath, files=4, per_file=3, ns="http://x.org/", prefix="f"):
    """`files` gzipped JSON Lines files of `per_file` records each; the
    record identifiers by file name."""
    dirpath.mkdir(parents=True, exist_ok=True)
    expected = {}
    for f in range(files):
        name = f"part-{f:02d}.jsonl.gz"
        ids = [f"{prefix}{f}r{r}" for r in range(per_file)]
        with gzip.open(dirpath / name, "wt") as fh:
            for i in ids:
                fh.write(json.dumps({"id": ns + i}) + "\n")
        expected[name] = ids
    return expected


def make(cls, tmp_path, entries, **extra):
    cfg = {"name": "test", "all_configs": StubConfigs(tmp_path), "datacache": {},
           "input_files": {"records": entries}}
    cfg.update(extra)
    ldr = cls(cfg)
    ldr.opened = []
    return ldr


def run_slices(cls, tmp_path, entries, max_slice, **extra):
    out = []
    for n in range(max_slice):
        ldr = make(cls, tmp_path, entries, **extra)
        ldr.prepare(n, max_slice)
        ldr.process()
        out.append(ldr)
    return out


def test_each_worker_reads_only_its_own_files(tmp_path):
    expected = write_dump(tmp_path / "dump", files=5)
    entry = {"path": "dump", "type": "dir/lines.gz/json", "slice": "files"}
    ldrs = run_slices(Opening, tmp_path, [entry], 2)
    names = sorted(expected)
    assert [ldr.opened for ldr in ldrs] == [names[0::2], names[1::2]]
    # and every record of a worker's files, none of the others'
    for ldr in ldrs:
        assert sorted(ldr.out_cache) == sorted(i for f in ldr.opened for i in expected[f])


def test_file_slices_together_cover_every_record_once(tmp_path):
    expected = write_dump(tmp_path / "dump", files=7, per_file=2)
    entry = {"path": "dump", "type": "dir/lines.gz/json", "slice": "files"}
    ldrs = run_slices(Loader, tmp_path, [entry], 3)
    got = [k for ldr in ldrs for k in ldr.out_cache]
    assert sorted(got) == sorted(i for ids in expected.values() for i in ids)
    assert all(ldr.out_cache for ldr in ldrs)


def test_record_slicing_is_still_the_default(tmp_path):
    write_dump(tmp_path / "dump", files=2)
    entry = {"path": "dump", "type": "dir/lines.gz/json"}
    ldrs = run_slices(Opening, tmp_path, [entry], 2)
    # every worker opens every file and keeps every other record
    assert all(ldr.opened == ["part-00.jsonl.gz", "part-01.jsonl.gz"] for ldr in ldrs)
    assert [len(ldr.out_cache) for ldr in ldrs] == [3, 3]


def test_single_worker_reads_every_file(tmp_path):
    expected = write_dump(tmp_path / "dump", files=3)
    entry = {"path": "dump", "type": "dir/lines.gz/json", "slice": "files"}
    (ldr,) = run_slices(Loader, tmp_path, [entry], 1)
    assert len(ldr.out_cache) == sum(len(v) for v in expected.values())


def test_files_in_a_hierarchy_are_sliced_by_file(tmp_path):
    a = write_dump(tmp_path / "dump" / "a", files=2)
    b = write_dump(tmp_path / "dump" / "b", files=2, prefix="g")
    entry = {"path": "dump", "type": "dirs/lines.gz/json", "slice": "files"}
    ldrs = run_slices(Opening, tmp_path, [entry], 2)
    # a/part-00, a/part-01, b/part-00, b/part-01 in that order
    assert [ldr.opened for ldr in ldrs] == [["part-00.jsonl.gz"] * 2, ["part-01.jsonl.gz"] * 2]
    assert sorted(ldrs[0].out_cache) == sorted(a["part-00.jsonl.gz"] + b["part-00.jsonl.gz"])
    assert sorted(ldrs[1].out_cache) == sorted(a["part-01.jsonl.gz"] + b["part-01.jsonl.gz"])


def test_entries_without_a_directory_are_each_a_unit(tmp_path):
    expected = write_dump(tmp_path / "dump", files=3)
    entries = [{"path": f"dump/{n}", "type": "lines.gz/json", "slice": "files"}
               for n in sorted(expected)]
    ldrs = run_slices(Opening, tmp_path, entries, 2)
    assert [ldr.opened for ldr in ldrs] == [["part-00.jsonl.gz", "part-02.jsonl.gz"],
                                            ["part-01.jsonl.gz"]]
    assert sorted(ldrs[1].out_cache) == expected["part-01.jsonl.gz"]


def test_refused_files_are_not_counted(tmp_path):
    expected = write_dump(tmp_path / "dump", files=4)
    (tmp_path / "dump" / "part-00.md5").write_text("x")
    (tmp_path / "dump" / "part-01.md5").write_text("x")

    class SkipMd5(Opening):
        def should_process_item(self, child):
            return not str(child).endswith(".md5")

    entry = {"path": "dump", "type": "dir/lines.gz/json", "slice": "files"}
    ldrs = run_slices(SkipMd5, tmp_path, [entry], 2)
    names = sorted(expected)
    assert [ldr.opened for ldr in ldrs] == [names[0::2], names[1::2]]


def test_max_records_with_file_slicing(tmp_path):
    write_dump(tmp_path / "dump", files=4)
    entry = {"path": "dump", "type": "dir/lines.gz/json", "slice": "files"}
    ldr = make(Loader, tmp_path, [entry])
    ldr.prepare(1, 2, max_records=4)
    ldr.process()
    assert sorted(ldr.out_cache) == ["f1r0", "f1r1", "f1r2", "f3r0"]


def test_unknown_slice_is_refused(tmp_path):
    entry = {"path": "dump", "type": "dir/lines.gz/json", "slice": "lines"}
    ldr = make(Loader, tmp_path, [entry])
    with pytest.raises(ValueError, match="Unknown slice 'lines'"):
        ldr.prepare(0, 2)


def test_file_override_keeps_file_slicing_for_a_directory(tmp_path):
    write_dump(tmp_path / "other", files=2)
    entry = {"path": "dump", "type": "dir/lines.gz/json", "slice": "files"}
    ldr = make(Loader, tmp_path, [entry])
    ldr.prepare(0, 2, path=str(tmp_path / "other"), path_type="dir/lines.gz/json")
    assert ldr.my_files[0]["slice"] == "files"
    ldr.process()
    assert sorted(ldr.out_cache) == ["f0r0", "f0r1", "f0r2"]


def test_file_override_with_one_file_is_sliced_by_record(tmp_path):
    write_dump(tmp_path / "dump", files=1)
    entry = {"path": "dump", "type": "dir/lines.gz/json", "slice": "files"}
    ldr = make(Loader, tmp_path, [entry])
    ldr.prepare(1, 2, path=str(tmp_path / "dump" / "part-00.jsonl.gz"))
    assert "slice" not in ldr.my_files[0]
    ldr.process()
    # the one file is shared: slice 1 takes the 1st and 3rd records
    assert sorted(ldr.out_cache) == ["f0r0", "f0r2"]


# --- the library loader ------------------------------------------------------


def yul(tmp_path, **cfg):
    base = {"name": "ils", "all_configs": StubConfigs(tmp_path), "datacache": {}}
    base.update(cfg)
    ldr = YulLoader(base)
    ldr.opened = []
    return ldr


def test_yul_without_input_files_reads_dump_file_path_by_file(tmp_path):
    expected = write_dump(tmp_path / "yul", files=4, ns=NAMESPACE)
    (tmp_path / "yul" / "manifest.txt").write_text("not records")
    got = []
    for n in range(2):
        ldr = yul(tmp_path, dumpFilePath=str(tmp_path / "yul"))
        ldr.prepare(n, 2)
        assert ldr.uses_dsl and ldr.my_files[0]["slice"] == "files"
        ldr.process()
        got.append(sorted(ldr.out_cache))
    names = sorted(expected)
    assert got == [sorted(expected[names[0]] + expected[names[2]]),
                   sorted(expected[names[1]] + expected[names[3]])]


def test_yul_identifier_strips_the_namespace(tmp_path):
    write_dump(tmp_path / "yul", files=1, per_file=1, ns=NAMESPACE)
    ldr = yul(tmp_path, input_files={"records": [
        {"path": "yul", "type": "dir/lines.gz/json", "slice": "files"}]})
    ldr.prepare(0, 1)
    ldr.process()
    assert ldr.out_cache == {"f0r0": {"id": NAMESPACE + "f0r0"}}


def test_yul_default_does_not_change_the_source_config(tmp_path):
    cfg = {"name": "ils", "all_configs": StubConfigs(tmp_path), "datacache": {},
           "dumpFilePath": str(tmp_path / "yul")}
    (tmp_path / "yul").mkdir()
    YulLoader(cfg).prepare(0, 1)
    assert "input_files" not in cfg


def test_yul_legacy_load_entry_point_loads_its_slice(tmp_path):
    expected = write_dump(tmp_path / "yul", files=2, ns=NAMESPACE)
    ldr = yul(tmp_path, dumpFilePath=str(tmp_path / "yul"))
    assert ldr.load(1, 2) == 3
    assert sorted(ldr.out_cache) == expected["part-01.jsonl.gz"]
