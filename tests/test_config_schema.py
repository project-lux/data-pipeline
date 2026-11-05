"""Web UI item W04: the catalogue of keys each kind of configuration
record carries (lux_pipeline.config_schema)."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline import config_schema as schema
from lux_pipeline import config_store as store
from lux_pipeline.config import Config
from webui_fixtures import make_installation


def test_every_record_type_has_a_catalogue_and_every_key_a_known_type():
    for typ in schema.RECORD_TYPES:
        keys = schema.keys_for({"type": typ, "name": "cache"})
        assert "type" in keys, typ
        for k, spec in keys.items():
            assert spec.type in schema.TYPES, (typ, k)
            assert spec.help, (typ, k)


@pytest.mark.parametrize("typ", ["internal", "external", "results", "map", "marklogic", "config"])
def test_every_default_class_imports(typ):
    for key, path in schema.class_defaults(typ).items():
        cls, error, legacy = store.resolve_class(path)
        assert error is None and legacy is None, (typ, key, error)


def test_class_defaults_depend_on_the_role():
    assert schema.class_defaults("internal")["harvesterClass"] == "process.base.harvester.ASHarvester"
    assert "harvesterClass" not in schema.class_defaults("external")
    assert schema.class_defaults("external")["recordcacheClass"].endswith("ExternalRecordCache")
    assert schema.class_defaults("internal")["recordcacheClass"].endswith("InternalRecordCache")
    assert "recordcacheReconciledClass" not in schema.class_defaults("internal")


def test_default_records_get_the_keys_of_their_kind_none_required():
    keys = schema.keys_for({"type": "default", "name": "ml"})
    assert "rest" in keys and not keys["rest"].required
    assert "host" in schema.keys_for({"type": "default", "name": "cache"})


def test_globals_take_any_name():
    assert schema.known({"type": "globals"}, "Subgenus").type == "str"
    assert schema.known({"type": "external"}, "someMapperKnob") is None


def test_unused_and_planned_keys_are_marked():
    assert schema.BASE["subconfigs"].unused and schema.CACHES["refresh_delta"].unused
    assert schema.BASE["web"].planned


@pytest.mark.parametrize("typ", sorted(schema.TEMPLATES))
def test_templates_ask_only_for_what_a_person_must_fill_in(typ, tmp_path):
    cfgs = Config(basepath=str(make_installation(tmp_path)))
    # the record's key is its file name; a source's name must equal it, a
    # default record's name is the kind it is for
    name = {"default": "cache"}.get(typ, "newone")
    data = schema.template(typ, name)
    assert data["type"] == typ
    problems = cfgs.config_store.validate("newone", data, new=True)
    assert all(("is empty" in p or "is missing" in p) for p in problems), problems
    with pytest.raises(ValueError):
        schema.template("base", "base")


def test_the_catalogue_as_a_json_schema(tmp_path):
    from jsonschema import Draft202012Validator
    js = schema.json_schema()
    Draft202012Validator.check_schema(js)
    v = Draft202012Validator(js)
    cfgs = Config(basepath=str(make_installation(tmp_path / "inst")))
    for key in cfgs.record_origins:
        assert list(v.iter_errors(cfgs.config_store.read(key))) == [], key
    errors = lambda r: sorted((list(e.path), e.validator) for e in v.iter_errors(r))
    assert errors({"type": "map", "name": "m", "storeClass": None}) == [(["storeClass"], "not")]
    assert errors({"type": "external", "name": "x", "merge_order": "1", "extra": 1}) == [
        ([], "required"), (["merge_order"], "type")]                 # extra keys are allowed
    assert errors({"type": "globals", "Class": 3}) == [(["Class"], "type")]
    assert errors({"type": "default", "name": "nope"}) == [(["name"], "enum")]
    assert errors({"type": "default", "name": "cache", "port": "1"}) == [(["port"], "type")]
