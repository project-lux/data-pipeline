"""The validator reads its schemas on first validate(), so a Config
builds on a machine without them; the missing files are named."""

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.process.validator import SCHEMA_BASE_URL, Validator, schema_file_names


def configs(tmp_path):
    return SimpleNamespace(data_dir=str(tmp_path), validate_allow_underscore_properties=True)


def test_construction_needs_no_files_and_names_what_is_missing(tmp_path):
    v = Validator(configs(tmp_path))
    assert v.schema_dir == str(tmp_path / "schema")
    assert v.missing() == schema_file_names() and len(v.missing()) == 11
    with pytest.raises(FileNotFoundError, match="core.json.*linked.art.*lux initialize"):
        v.validate({"data": {"type": "Person"}})


def test_validates_once_the_schemas_are_present(tmp_path):
    sdir = tmp_path / "schema"
    sdir.mkdir()
    draft = "https://json-schema.org/draft/2020-12/schema"
    (sdir / "core.json").write_text(json.dumps({"$schema": draft, "$id": SCHEMA_BASE_URL + "core.json", "$defs": {}}))
    for fn in schema_file_names()[1:]:
        (sdir / fn).write_text(json.dumps({"$schema": draft, "type": "object", "required": ["type"],
                                           "properties": {"type": {"type": "string"}},
                                           "additionalProperties": False}))
    v = Validator(configs(tmp_path))
    assert v.missing() == []
    assert v.validate({"data": {"type": "Person", "_local": 1}}) == []           # underscore allowed
    errs = v.validate({"data": {"type": "Material", "bogus": 1}})               # alias of Type
    assert len(errs) == 1 and errs[0].validator == "additionalProperties"
    with pytest.raises(ValueError, match="Cannot validate record with class"):
        v.validate({"data": {"type": "Nope"}})
    assert v._loaded is True
