"""RorLoader on the DSL (item 29, ror): the Zenodo zip with its JSON and
CSV members, identifiers from the ROR id, slicing, and the extracted
JSON form."""

import io
import json
import sys
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.process.base.loader import Loader
from lux_pipeline.sources.ror.loader import RorLoader

ORGS = [{"id": "https://ror.org/01abc", "name": "Alpha", "types": ["education"]},
        {"id": "https://ror.org/02def", "name": "Beta", "types": ["company"]},
        {"id": "https://ror.org/03ghi", "name": "Gamma", "types": ["facility"]}]


@pytest.fixture
def ror_zip(tmp_path):
    path = tmp_path / "ror" / "ror-data.zip"
    path.parent.mkdir()
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zh:
        zh.writestr("v2.15-2026-10-20-ror-data.csv", "id,name\nhttps://ror.org/01abc,Alpha\n")
        zh.writestr("v2.15-2026-10-20-ror-data.json", json.dumps(ORGS))
    return path


def make(tmp_path, path, fmt):
    cfg = {"name": "ror", "all_configs": SimpleNamespace(dumps_dir=str(tmp_path), temp_dir=str(tmp_path)),
           "datacache": {}, "dumpFilePath": str(path), "dumpFileType": fmt}
    return RorLoader(cfg)


def test_zip_json_member_is_loaded_and_the_csv_skipped(ror_zip, tmp_path):
    ldr = make(tmp_path, ror_zip, "zip/array/raw")
    ldr.prepare(0, 1)
    assert ldr.uses_dsl is True
    assert ldr.process() == 3
    assert set(ldr.out_cache) == {"01abc", "02def", "03ghi"}
    assert ldr.out_cache["02def"]["name"] == "Beta"


def test_slices_split_the_organisations(ror_zip, tmp_path):
    seen = set()
    for n in range(2):
        ldr = make(tmp_path, ror_zip, "zip/array/raw")
        ldr.prepare(n, 2)
        ldr.process()
        seen |= set(ldr.out_cache)
        assert 0 < len(ldr.out_cache) < 3
    assert seen == {"01abc", "02def", "03ghi"}


def test_extracted_json_with_array_raw(tmp_path):
    path = tmp_path / "ror-data.json"
    path.write_text(json.dumps(ORGS))
    ldr = make(tmp_path, path, "array/raw")
    ldr.prepare(0, 1)
    assert ldr.process() == 3
    assert set(ldr.out_cache) == {"01abc", "02def", "03ghi"}


def test_the_legacy_load_is_gone():
    assert RorLoader.load is Loader.load


def test_should_process_item_only_filters_archive_members():
    ldr = RorLoader({"name": "ror", "datacache": {}})
    assert ldr.should_process_item({"id": "x"}) is True
