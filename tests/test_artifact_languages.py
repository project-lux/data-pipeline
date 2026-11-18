"""The type_overrides artifact (docs/artifacts.md, item 2): `LanguageTypes`
asks Wikidata for the items with an ISO 639 code and their AAT, LC and LC
vocabulary identifiers, keeps the AAT ones Getty has under its languages
hierarchy, and writes {uri: "Language"}; the mappers read it through
`Config.artifact_path`. Both SPARQL endpoints are stubbed."""

import json
import logging
import sys
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlparse

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.artifacts import base as artifact_base
from lux_pipeline.artifacts import cache as artifact_cache
from lux_pipeline.artifacts.base import ArtifactError
from lux_pipeline.process.base.mapper import read_type_overrides
from webui_fixtures import make_configs, make_installation

WD = "http://www.wikidata.org/entity/"
AAT = "http://vocab.getty.edu/aat/"
ESPERANTO, FRENCH, JIBU = WD + "Q143", WD + "Q150", WD + "Q3914448"
RECORD = {"name": "type_overrides", "type": "artifact",
          "generatorClass": "lux_pipeline.artifacts.languages.LanguageTypes",
          "files": ["type_overrides.json"], "maxAgeDays": 180}

# what the stubbed endpoints know
ITEMS = [ESPERANTO, FRENCH, JIBU]
IDENTIFIERS = {
    "P1014": [(ESPERANTO, "300388282"), (FRENCH, "300388306"),
              (JIBU, "300016098"),                       # Jibu, the people: not a language
              (FRENCH, "6000320089674"),                 # malformed
              (ESPERANTO, "300389334")],                 # replaced by a language
    "P244": [(ESPERANTO, "sh85044883"), (FRENCH, "sh85051829"), (FRENCH, "n79021783")],
    "P4801": [(ESPERANTO, "languages/epo"), (FRENCH, "languages/fre"), (FRENCH, "iso639-1/fr")],
}
AAT_LANGUAGES = [AAT + "300388282", AAT + "300388306", AAT + "300388607"]
AAT_REPLACED = [(AAT + "300389334", AAT + "300388607")]


def bindings(rows):
    return json.dumps({"head": {}, "results": {"bindings": rows}})


def uri(v):
    return {"type": "uri", "value": v}


def literal(v):
    return {"type": "literal", "value": v}


class Answer:
    def __init__(self, text, status=200):
        self.text, self.status_code = text, status


def endpoints(monkeypatch, items=ITEMS, identifiers=IDENTIFIERS, aat=AAT_LANGUAGES,
              replaced=AAT_REPLACED):
    """Stub requests.get: Wikidata answers the items and identifier queries,
    Getty the hierarchy and replaced-ids queries. Returns the queries
    asked, as (host, query)."""
    asked = []

    def get(url, params=None, timeout=None, headers=None):
        query = params["query"]
        host = urlparse(url).netloc
        asked.append((host, query))
        if host == "query.wikidata.org":
            if "?value" not in query:
                return Answer(bindings([{"item": uri(i)} for i in items]))
            prop = query.rsplit("wdt:", 1)[1].split()[0]
            return Answer(bindings([{"item": uri(i), "value": literal(v)}
                                    for i, v in identifiers.get(prop, [])]))
        if host == "vocab.getty.edu":
            if "isReplacedBy" in query:
                return Answer(bindings([{"old": uri(o), "new": uri(n)} for o, n in replaced]))
            return Answer(bindings([{"s": uri(s)} for s in aat]))
        raise AssertionError(url)
    monkeypatch.setattr(artifact_base.requests, "get", get)
    return asked


@pytest.fixture(autouse=True)
def _fresh(monkeypatch):
    monkeypatch.setattr(artifact_cache, "_warned", set())
    monkeypatch.setattr(artifact_base.time, "sleep", lambda s: None)


def install(tmp_path, record=RECORD):
    root = make_installation(tmp_path)
    (root / "config_cache" / "type_overrides.json").write_text(json.dumps(record))
    return make_configs(root)


def test_the_overrides_are_the_languages_and_their_identifiers(tmp_path, monkeypatch):
    cfgs = install(tmp_path)
    asked = endpoints(monkeypatch)
    entry = cfgs.artifacts_cache.make("type_overrides")
    data = json.loads(Path(cfgs.artifacts_dir, "type_overrides.json").read_text())
    assert data == {u: "Language" for u in [
        ESPERANTO, FRENCH, JIBU,                         # every item with a code
        AAT + "300388282", AAT + "300388306",            # AAT languages
        AAT + "300389334", AAT + "300388607",            # a replaced id and its replacement
        "http://id.loc.gov/authorities/subjects/sh85044883",
        "http://id.loc.gov/authorities/subjects/sh85051829",
        "http://id.loc.gov/vocabulary/languages/epo",
        "http://id.loc.gov/vocabulary/languages/fre"]}
    assert list(data) == sorted(data)
    assert entry["files"][0]["rows"] == 11
    # no source needed: only the two endpoints were asked, the items first
    assert [h for h, _ in asked] == ["query.wikidata.org", "vocab.getty.edu", "vocab.getty.edu",
                                     "query.wikidata.org", "query.wikidata.org",
                                     "query.wikidata.org"]
    assert "?item wdt:P218|wdt:P219|wdt:P220 []" in asked[0][1]
    assert "gvp:broaderExtended aat:300389739" in asked[1][1]


def test_the_records_settings_are_used(tmp_path, monkeypatch):
    cfgs = install(tmp_path, {**RECORD, "codeProperties": ["P220"], "aatRoot": "300000000",
                              "lcVocabularies": ["languages", "iso639-1"]})
    asked = endpoints(monkeypatch)
    cfgs.artifacts_cache.make("type_overrides")
    data = json.loads(Path(cfgs.artifacts_dir, "type_overrides.json").read_text())
    assert "http://id.loc.gov/vocabulary/iso639-1/fr" in data
    assert "?item wdt:P220 []" in asked[0][1]
    assert "aat:300000000" in asked[1][1]


def test_counts_are_logged(tmp_path, monkeypatch, caplog):
    cfgs = install(tmp_path)
    endpoints(monkeypatch)
    with caplog.at_level(logging.INFO, logger="lux_pipeline"):
        cfgs.artifacts_cache.make("type_overrides")
    assert ("type_overrides: 11 identifiers of languages: wikidata 3, aat 2, aat replaced 1, "
            "aat not a language 2, lcsh 2, lc not a subject 1, lc vocabulary 2, "
            "lc vocabulary other 1") in caplog.text


@pytest.mark.parametrize("kwargs,message", [
    ({"items": []}, "no item has any of P218, P219, P220"),
    ({"aat": []}, "nothing under aat:300389739"),
])
def test_an_empty_answer_makes_nothing(tmp_path, monkeypatch, kwargs, message):
    cfgs = install(tmp_path)
    endpoints(monkeypatch, **kwargs)
    with pytest.raises(ArtifactError, match=message):
        cfgs.artifacts_cache.make("type_overrides")
    assert cfgs.artifacts_cache.entries() == {}


def test_a_much_smaller_answer_is_refused(tmp_path, monkeypatch):
    cfgs = install(tmp_path)
    endpoints(monkeypatch)
    cfgs.artifacts_cache.make("type_overrides")
    endpoints(monkeypatch, items=[ESPERANTO], identifiers={}, replaced=[])
    with pytest.raises(ArtifactError, match="down from 11"):
        cfgs.artifacts_cache.make("type_overrides")


def test_the_mapper_reads_the_artifact(tmp_path, monkeypatch, caplog):
    cfgs = install(tmp_path)
    # before it is made: the old file in data_dir, with a warning
    Path(cfgs.data_dir, "type_overrides.json").write_text(json.dumps({FRENCH: "Language"}))
    with caplog.at_level(logging.WARNING, logger="lux_pipeline"):
        assert read_type_overrides(cfgs) == {FRENCH: "Language"}
    assert "artifact type_overrides has not been made" in caplog.text
    endpoints(monkeypatch)
    cfgs.artifacts_cache.make("type_overrides")
    got = read_type_overrides(cfgs)
    assert len(got) == 11 and got[ESPERANTO] == "Language"


def test_the_mapper_without_artifacts(tmp_path):
    stub = SimpleNamespace(data_dir=str(tmp_path))
    assert read_type_overrides(stub) == {}
    (tmp_path / "type_overrides.json").write_text(json.dumps({FRENCH: "Language"}))
    assert read_type_overrides(stub) == {FRENCH: "Language"}
    cfgs = make_configs(tmp_path / "inst")
    assert read_type_overrides(cfgs) == {}            # no record, no file


def test_post_reconcile_uses_what_the_mapper_read(tmp_path, monkeypatch):
    """The applying code is unchanged: a Type whose equivalent is a
    language becomes a Language."""
    from lux_pipeline.process.base.mapper import Mapper
    cfgs = install(tmp_path)
    endpoints(monkeypatch)
    cfgs.artifacts_cache.make("type_overrides")
    mapper = Mapper.__new__(Mapper)
    mapper.type_overrides = read_type_overrides(cfgs)
    rec = {"data": {"type": "Type", "equivalent": [{"id": AAT + "300388306"}]}}
    mapper.post_reconcile(rec)
    assert rec["data"]["type"] == "Language"
    culture = {"data": {"type": "Type", "equivalent": [{"id": AAT + "300016098"}]}}
    mapper.post_reconcile(culture)
    assert culture["data"]["type"] == "Type"


def test_the_record_checks_clean(tmp_path):
    from lux_pipeline import config_store
    cfgs = install(tmp_path)
    assert config_store.artifact_problems("type_overrides", RECORD, cfgs) == []
    assert config_store.artifact_problems(
        "type_overrides", {**RECORD, "aatroot": "1"}, cfgs) == [
        "warning: aatroot is not a key of LanguageTypes or of every artifact: a typo is ignored"]
