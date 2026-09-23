"""Counting identifier vs substantive statements in the wikidata datacache.

The record shape under test is the one WdFetcher.post_process writes, not raw
Wikidata JSON: `id`, `prefLabel`/`altLabel`/`description` as language maps,
an optional `sitelinks`, and one key per property whose value is the list of
statements that survived its filtering.

The point of the script is a percentage, so what these pin is the
denominator. P31 and the labels are on essentially every record and belong
outside it; getting that wrong moves the headline number without changing
any of the data.
"""

import importlib.util
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest

_spec = importlib.util.spec_from_file_location(
    "wd_id_share", Path(__file__).parent.parent / "wikidata-identifier-share.py")
wd = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(wd)


IDENT = {"P214", "P356"}                       # VIAF, DOI
OTHER = {"commons-link": {"P373"},             # Commons category
         "ontology-mapping": {"P1709"}}


def tally(*records):
    t = wd.Tally(IDENT, OTHER)
    for r in records:
        t.add(r)
    return t


def test_statements_split_four_ways():
    t = tally({
        "id": "Q42",
        "prefLabel": {"en": "Douglas Adams", "fr": "Douglas Adams"},
        "altLabel": {"en": ["Adams", "D. Adams"]},
        "description": {"en": "author"},
        "sitelinks": {"enwiki": {"title": "Douglas Adams"}},
        "P31": ["Q5"],
        "P214": ["113230702"],
        "P356": ["10.1000/1", "10.1000/2"],
        "P373": ["Douglas Adams"],
        "P106": ["Q36180", "Q214917"],
        "P569": [{"time": "+1952-03-11T00:00:00Z", "precision": 11}],
    })
    assert t.records == 1
    assert t.statements == 8          # P-statements only
    assert t.identifiers == 3         # one VIAF + two DOI
    assert t.other_strip == 1         # the commons category
    assert t.instance_of == 1
    assert t.substantive == 3         # two occupations + one birth date
    assert t.identifiers + t.other_strip + t.instance_of + t.substantive \
        == t.statements


def test_labels_are_not_statements():
    """Two languages of prefLabel, two aliases and a description is five
    label values and zero statements -- they must not reach the denominator
    the identifier share is quoted against."""
    t = tally({
        "id": "Q1",
        "prefLabel": {"en": "a", "de": "a"},
        "altLabel": {"en": ["b", "c"]},
        "description": {"en": "d"},
    })
    assert t.statements == 0
    assert t.labels == 5
    assert t.sitelinks == 0


def test_sitelinks_counted_apart_from_both():
    t = tally({"id": "Q1", "sitelinks": {"enwiki": {"title": "x"}},
               "P106": ["Q1"]})
    assert t.sitelinks == 1
    assert t.statements == 1
    assert t.labels == 0


def test_a_record_of_nothing_but_identifiers_is_flagged():
    """The question behind the script: how many records would survive the
    strip with nothing a reader could use."""
    t = tally(
        {"id": "Q1", "P31": ["Q5"], "P214": ["1"], "prefLabel": {"en": "a"}},
        {"id": "Q2", "P31": ["Q5"], "P106": ["Q1"]},
    )
    assert t.empty_records == 1
    assert t.no_substantive_but_labels == 1


def test_an_empty_statement_list_is_not_a_statement():
    """post_process writes `new[prop] = newvals` unconditionally, so a
    property whose every value was deprecated lands as []."""
    t = tally({"id": "Q1", "P214": [], "P106": ["Q1"]})
    assert t.statements == 1
    assert t.per_property == {"P106": 1}
    assert t.records_with["P214"] == 0


def test_p31_is_never_also_counted_as_substantive():
    t = tally({"id": "Q1", "P31": ["Q5", "Q215627"]})
    assert t.instance_of == 2
    assert t.substantive == 0
    assert t.by_category["instance-of"] == 2


def test_unknown_keys_do_not_become_statements():
    """A future fetcher key must not be counted as a property."""
    t = tally({"id": "Q1", "someNewKey": {"a": 1}, "P106": ["Q1"]})
    assert t.statements == 1
    assert t.by_category["not-a-property"] == 1


def test_merge_sums_slices():
    a = tally({"id": "Q1", "P214": ["1"], "P106": ["Q1"]})
    b = tally({"id": "Q2", "P356": ["2"], "P31": ["Q5"]})
    rolled = wd.Tally(IDENT, OTHER)
    rolled.merge(a.to_json())
    rolled.merge(b.to_json())
    assert rolled.records == 2
    assert rolled.statements == a.statements + b.statements
    assert rolled.identifiers == 2
    assert rolled.per_property["P214"] == 1
    assert rolled.per_property["P356"] == 1


# --- the property list itself -----------------------------------------------

def test_the_real_property_list_splits_into_identifiers_and_the_rest():
    """The shipped JSON has strip_by_category, so `identifier` must mean the
    external-identifier category alone -- not the whole strip set, which is
    the number the percentages would otherwise be quoted against."""
    path = Path(__file__).parent.parent / "wikidata-identifier-properties.json"
    ident, other = wd.load_properties(path)
    js = json.loads(path.read_text())
    assert len(ident) == js["counts"]["external-identifier"]
    assert ident | set().union(*other.values()) == set(js["strip"])
    assert "P356" in ident                      # DOI
    assert "P373" not in ident                  # commons category
    assert "P31" not in ident


def test_a_list_without_categories_still_runs(tmp_path):
    """Falling back to the union keeps an older or hand-made list usable."""
    p = tmp_path / "props.json"
    p.write_text(json.dumps({"strip": ["P214", "P356"]}))
    ident, other = wd.load_properties(p)
    assert ident == {"P214", "P356"}
    assert other == {}
