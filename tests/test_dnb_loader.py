"""DnbLoader on the DSL (item 29): entityfacts as arraylines, the
Sachbegriff file through its own container, keys and the stored shape
as main's hand-written loader produced them."""

import gzip
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.config import importObject
from lux_pipeline.process.base.loader import Loader
from lux_pipeline.sources.dnb.loader import DnbLoader

ROOT = Path(__file__).parent.parent
GND = "https://d-nb.info/gnd/"


def entity(i, extra=None):
    d = {"@id": f"{GND}{i}", "@type": "person", "preferredName": f"Person {i}"}
    d.update(extra or {})
    return d


def subject(i):
    return {"@id": f"{GND}{i}", "preferredName": f"Subject {i}", "@type": "SubjectHeading"}


@pytest.fixture
def dumps(tmp_path):
    d = tmp_path / "dnb"
    d.mkdir()
    ef = d / "authorities-gnd_entityfacts.jsonld.gz"
    with gzip.open(ef, "wt") as fh:
        fh.write("[\n" + ",\n".join(json.dumps(entity(i)) for i in ("1", "2", "shared")) + "\n]\n")
    sb = d / "authorities-gnd-sachbegriff_lds.jsonld.gz"
    weird = [subject("s1"), [subject("s2"), {"@id": f"{GND}s3/about"}, "junk"], subject("shared"),
             {"@id": "https://example.org/not-gnd/x"}, 42]
    with gzip.open(sb, "wt") as fh:
        fh.write(json.dumps(weird))
    return tmp_path


def make(tmp_path, records, workers=(0, 1)):
    ldr = DnbLoader({"name": "dnb", "namespace": GND, "datacache": {}, "dumps_dir": "dnb",
                     "all_configs": SimpleNamespace(dumps_dir=str(tmp_path), temp_dir=str(tmp_path)),
                     "input_files": {"records": records}})
    ldr.prepare(*workers)
    assert ldr.uses_dsl is True
    return ldr


EF = {"path": "authorities-gnd_entityfacts.jsonld.gz", "type": "arraylines.gz/json"}
SB = {"path": "authorities-gnd-sachbegriff_lds.jsonld.gz", "type": "sachbegriff.gz/json"}


def test_entityfacts_keys_are_the_gnd_ids(dumps):
    ldr = make(dumps, [EF])
    assert ldr.process() == 3
    assert set(ldr.out_cache) == {"1", "2", "shared"}
    assert ldr.out_cache["1"]["preferredName"] == "Person 1"


def test_sachbegriff_records_are_flattened_filtered_and_wrapped(dumps):
    ldr = make(dumps, [SB])
    assert ldr.process() == 3
    assert set(ldr.out_cache) == {"s1", "s2", "shared"}      # no about node, no non-GND id, no junk
    assert ldr.out_cache["s2"] == {"list": [subject("s2")]}


def test_both_files_in_order_subject_form_wins_for_a_shared_id(dumps):
    ldr = make(dumps, [EF, SB])
    assert ldr.process() == 6
    assert set(ldr.out_cache) == {"1", "2", "shared", "s1", "s2"}
    assert ldr.out_cache["shared"] == {"list": [subject("shared")]}
    assert ldr.out_cache["2"]["@type"] == "person"


def test_slices_split_both_files(dumps):
    seen = set()
    for n in range(2):
        ldr = make(dumps, [EF, SB], workers=(n, 2))
        ldr.process()
        seen |= set(ldr.out_cache)
    assert seen == {"1", "2", "shared", "s1", "s2"}


def test_extract_identifier_handles_both_shapes():
    ldr = DnbLoader({"name": "dnb", "datacache": {}})
    assert ldr.extract_identifier(entity("7")) == "7"
    assert ldr.extract_identifier({"list": [subject("9")]}) == "9"
    assert ldr.extract_identifier({"no": "id"}) is None


def test_the_container_is_only_known_to_dnb():
    assert DnbLoader.extra_containers == ("sachbegriff",)
    assert DnbLoader.load is Loader.load
    with pytest.raises(ValueError, match="container type 'sachbegriff'"):
        Loader({"name": "x", "datacache": {}}).process_fmt("sachbegriff.gz/json")
    assert DnbLoader({"name": "dnb", "datacache": {}}).process_fmt("sachbegriff.gz/json") == [["sachbegriff", "gz"], ["json"]]


def test_sample_parses_and_resolves():
    d = json.load(open(ROOT / "docs" / "sample_config" / "dnb.json"))
    assert [e["type"] for e in d["input_files"]["records"]] == ["arraylines.gz/json", "sachbegriff.gz/json"]
    for k, v in d.items():
        if k.endswith("Class") and v:
            assert importObject(v) is not None
