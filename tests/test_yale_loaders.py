"""The Yale unit loaders on the DSL (item 29): each reproduces the
datacache keys and tolerance of main's hand-written loader over archives
built here -- ycba, ypm, yuag, pmc as tar/zip members, ipch as JSON
lines."""

import io
import json
import sys
import tarfile
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.config import importObject
from lux_pipeline.process.base.loader import Loader
from lux_pipeline.sources.yale.archive_loader import ArchiveJsonLoader, MIN_RECORD_BYTES
from lux_pipeline.sources.yale.ipch.loader import IpchLoader
from lux_pipeline.sources.yale.pmc.loader import PmcLoader
from lux_pipeline.sources.yale.ycba.loader import YcbaLoader
from lux_pipeline.sources.yale.ypm.loader import YpmLoader
from lux_pipeline.sources.yale.yuag.loader import YuagLoader

ROOT = Path(__file__).parent.parent


def record(ns, ident, n):
    return json.dumps({"id": f"{ns}{ident}", "type": "HumanMadeObject", "_label": f"Record {n} " + "x" * 20}).encode()


def write_tar(path, members):
    with tarfile.open(path, "w:gz") as tf:
        for name, data in members:
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))


def write_zip(path, members):
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zh:
        for name, data in members:
            zh.writestr(name, data)


def make(cls, tmp_path, name, ns, **cfg):
    config = {"name": name, "namespace": ns, "datacache": {},
              "all_configs": SimpleNamespace(dumps_dir=str(tmp_path), temp_dir=str(tmp_path))}
    config.update(cfg)
    ldr = cls(config)
    ldr.prepare(0, 1)
    assert ldr.uses_dsl is True
    return ldr


# --- ycba --------------------------------------------------------------------

YCBA_NS = "https://ycba-lux.s3.amazonaws.com/v3/"


@pytest.fixture
def ycba_tar(tmp_path):
    path = tmp_path / "linked_art.tar.gz"
    write_tar(path, [
        ("linked_art/object/ab/ab12.json", record(YCBA_NS, "object/ab/ab12", 1)),
        ("linked_art/person/cd/cd34.json", record(YCBA_NS, "person/cd/cd34", 2)),
        ("linked_art/object/ef/ef56.json", record(YCBA_NS, "object/ef/ef56", 3)),
        ("linked_art/object/gh/gh78.json", b""),                       # deleted
        ("linked_art/object/ij/ij90.json", b"{this is not json at all, sorry}"),   # broken
        ("linked_art/README.txt", b"not a record, no slash-json name either......"),
        ("toplevel.json", record(YCBA_NS, "x", 4)),                    # no "/" in the name
    ])
    return path


def test_ycba_keys_are_the_last_three_path_components(ycba_tar, tmp_path, caplog):
    ldr = make(YcbaLoader, tmp_path, "ycba", YCBA_NS, dumpFilePath=str(ycba_tar), dumpFileType="tar.gz/json")
    with caplog.at_level("WARNING", logger="lux_pipeline"):
        assert ldr.process() == 3
    assert set(ldr.out_cache) == {"object/ab/ab12.json", "person/cd/cd34.json", "object/ef/ef56.json"}
    assert ldr.out_cache["person/cd/cd34.json"]["id"] == f"{YCBA_NS}person/cd/cd34"
    assert "ycba: broken record linked_art/object/ij/ij90.json" in caplog.text


def test_ycba_slices_split_the_members(ycba_tar, tmp_path):
    seen = set()
    for n in range(2):
        ldr = YcbaLoader({"name": "ycba", "namespace": YCBA_NS, "datacache": {},
                          "all_configs": SimpleNamespace(dumps_dir=str(tmp_path), temp_dir=str(tmp_path)),
                          "dumpFilePath": str(ycba_tar), "dumpFileType": "tar.gz/json"})
        ldr.prepare(n, 2)
        ldr.process()
        seen |= set(ldr.out_cache)
    assert seen == {"object/ab/ab12.json", "person/cd/cd34.json", "object/ef/ef56.json"}


# --- ypm ---------------------------------------------------------------------

YPM_NS = "https://images.peabody.yale.edu/data/"


def test_ypm_keys_prefix_the_archive_name_and_repair_bare_values(tmp_path):
    d = tmp_path / "ypm"
    d.mkdir()
    write_tar(d / "agent.tar.gz", [
        ("agent/1.json", record(YPM_NS, "agent/1", 1)),
        ("agent/2.json", b'{"id": "' + YPM_NS.encode() + b'agent/2", "dimension": {"value": .}, "_label": "pad pad pad pad"}'),
    ])
    write_tar(d / "object.tar.gz", [("object/9.json", record(YPM_NS, "object/9", 3))])
    ldr = make(YpmLoader, tmp_path, "ypm", YPM_NS, dumps_dir="ypm",
               input_files={"records": [{"path": "agent.tar.gz", "type": "tar.gz/json"},
                                        {"path": "object.tar.gz", "type": "tar.gz/json"}]})
    assert ldr.process() == 3
    assert set(ldr.out_cache) == {"agent/agent/1.json", "agent/agent/2.json", "object/object/9.json"}
    assert ldr.out_cache["agent/agent/2.json"]["dimension"] == {"value": 0.0}


# --- yuag --------------------------------------------------------------------

YUAG_NS = "https://media.art.yale.edu/content/lux/"


def test_yuag_keys_are_the_zip_member_names(tmp_path):
    path = tmp_path / "luxjson.zip"
    write_zip(path, [("lux/objects/1.json", record(YUAG_NS, "objects/1", 1)),
                     ("lux/objects/2.json", record(YUAG_NS, "objects/2", 2)),
                     ("lux/objects/3.json", b""),
                     ("lux/manifest.csv", b"id,name\n1,x\n")])
    ldr = make(YuagLoader, tmp_path, "yuag", YUAG_NS, dumpFilePath=str(path), dumpFileType="zip/json")
    assert ldr.process() == 2
    assert set(ldr.out_cache) == {"lux/objects/1.json", "lux/objects/2.json"}


# --- pmc ---------------------------------------------------------------------

PMC_NS = "https://data.paul-mellon-centre.ac.uk/"


def test_pmc_keys_drop_the_json_extension(tmp_path):
    path = tmp_path / "pmc-export.tgz"
    write_tar(path, [("pmc/object/1.json", record(PMC_NS, "object/1", 1)),
                     ("pmc/person/2.json", record(PMC_NS, "person/2", 2))])
    ldr = make(PmcLoader, tmp_path, "pmc", PMC_NS, dumpFilePath=str(path), dumpFileType="tar.gz/json")
    assert ldr.process() == 2
    assert set(ldr.out_cache) == {"pmc/object/1", "pmc/person/2"}


# --- ipch --------------------------------------------------------------------

IPCH_NS = "https://paperbase.xyz/records/"


def test_ipch_keys_strip_the_namespace_from_the_id(tmp_path):
    path = tmp_path / "all_records.jsonl"
    path.write_bytes(b"\n".join(record(IPCH_NS, f"r{i}", i) for i in range(3)) + b"\n")
    ldr = make(IpchLoader, tmp_path, "ipch", IPCH_NS, dumpFilePath=str(path), dumpFileType="lines/json")
    assert ldr.process() == 3
    assert set(ldr.out_cache) == {"r0", "r1", "r2"}


# --- shared base and configs -------------------------------------------------


def test_archive_base_filters_and_sizes(tmp_path):
    path = tmp_path / "a.tar.gz"
    write_tar(path, [("d/ok.json", b"x" * MIN_RECORD_BYTES), ("d/small.json", b"x" * (MIN_RECORD_BYTES - 1)),
                     ("d/notes.txt", b"x" * 50), ("flat.json", b"x" * 50)])
    ldr = ArchiveJsonLoader({"name": "a", "datacache": {},
                             "all_configs": SimpleNamespace(dumps_dir=str(tmp_path), temp_dir=str(tmp_path))})
    with tarfile.open(path) as tf:
        from lux_pipeline.process.base.loader import TarPointer
        kept = [m.name for m in tf.getmembers() if ldr.should_process_item(TarPointer(tf, m))]
        assert kept == ["d/ok.json"]
        assert ldr.archive_name(TarPointer(tf, tf.getmembers()[0])) == "a"
    assert ldr.should_process_item({"not": "a pointer"}) is True


@pytest.mark.parametrize("cls", [YcbaLoader, YpmLoader, YuagLoader, PmcLoader, IpchLoader])
def test_the_legacy_load_is_gone(cls):
    assert cls.load is Loader.load


@pytest.mark.parametrize("sample", ["ycba.json", "ypm.json", "yuag.json", "ipch.json", "pmc.json"])
def test_samples_parse_and_resolve(sample):
    d = json.load(open(ROOT / "docs" / "sample_config" / sample))
    assert d.get("dumpFileType") or d.get("input_files")
    for k, v in d.items():
        if k.endswith("Class") and v:
            assert importObject(v) is not None
