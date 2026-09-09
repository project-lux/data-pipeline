"""QleverMapper (mapper2) -- the n-triples export for the qlever index.

This runs over every merged record in the build and emits tens of triples for
each, so it is the innermost loop of `manage-data.py --nt`. The tests here do
two jobs:

  * pin the exact triples for a record of each prefix, so the hot loop can be
    rewritten (str.format -> f-strings) and proved output-identical, and
  * cover the shapes that used to raise, since one exception used to abort the
    whole slice's export.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest

from pipeline.sources.lux.qlever.mapper2 import QleverMapper

GLOBALS = {"primaryName": "urn:PN", "sortName": "urn:SN", "gender": "urn:GEN",
           "nationality": "urn:NAT", "occupation": "urn:OCC"}
IDMAP = {"https://vocab.getty.edu/aat/300456575##quaType": "urn:SORTID",
         "http://vocab.getty.edu/aat/300055644##quaType": "urn:H",
         "http://vocab.getty.edu/aat/300055647##quaType": "urn:W",
         "http://vocab.getty.edu/aat/300072633##quaType": "urn:D",
         "http://vocab.getty.edu/aat/300056240##quaType": "urn:WT"}


class StubConfigs:
    globals = GLOBALS
    data_dir = "/nonexistent"
    results = {"merged": {}}

    def get_idmap(self):
        return IDMAP


@pytest.fixture(scope="module")
def mapper():
    return QleverMapper({"all_configs": StubConfigs(), "namespace": "urn:ns/",
                         "name": "qlever"})


LUX = "https://lux.collections.yale.edu/data/"
NS = "https://lux.collections.yale.edu/ns/"


def rec(data, **kw):
    out = {"data": data}
    out.update(kw)
    return out


# --- output is pinned, so the hot loop can be rewritten safely --------------

def test_person_triples(mapper):
    got = mapper.transform(rec({
        "id": f"{LUX}person/p1",
        "type": "Person",
        "identified_by": [
            {"type": "Name", "content": "Ada Lovelace",
             "classified_as": [{"id": "urn:PN"}]},
            {"type": "Name", "content": "Lovelace, Ada",
             "classified_as": [{"id": "urn:SN"}]},
            {"type": "Identifier", "content": "12345",
             "classified_as": [{"id": "urn:SORTID"}]},
        ],
        "classified_as": [
            {"id": "urn:brit", "classified_as": [{"id": "urn:NAT"}]},
            {"id": "urn:math", "classified_as": [{"id": "urn:OCC"}]},
        ],
        "born": {"timespan": {"begin_of_the_begin": "1815-12-10T00:00:00"}},
        "died": {"timespan": {"end_of_the_end": "1852-11-27T23:59:59"}},
        "member_of": [{"id": "urn:grp", "type": "Group"}],
    }, change="ils|yuag"))

    assert got == [
        f"<{LUX}person/p1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <{NS}Agent> .",
        f"<{LUX}person/p1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://linked.art/ns/terms/Person> .",
        f"<{LUX}person/p1> <{NS}source> <{NS}ILS> .",
        f"<{LUX}person/p1> <{NS}source> <{NS}YUAG> .",
        f'<{LUX}person/p1> <{NS}agentPrimaryName> "ada lovelace" .',
        f'<{LUX}person/p1> <{NS}primaryName> "ada lovelace" .',
        f'<{LUX}person/p1> <{NS}agentName> "ada lovelace" .',
        f'<{LUX}person/p1> <{NS}name> "ada lovelace" .',
        f'<{LUX}person/p1> <{NS}agentSortName> "lovelace, ada" .',
        f'<{LUX}person/p1> <{NS}agentName> "lovelace, ada" .',
        f'<{LUX}person/p1> <{NS}name> "lovelace, ada" .',
        f'<{LUX}person/p1> <{NS}sortIdentifier> "12345" .',
        f"<{LUX}person/p1> <{NS}agentClassification> <urn:brit> .",
        f"<{LUX}person/p1> <{NS}agentAny> <urn:brit> .",
        f"<{LUX}person/p1> <{NS}agentClassification> <urn:math> .",
        f"<{LUX}person/p1> <{NS}agentAny> <urn:math> .",
        f'<{LUX}person/p1> <{NS}startOfAgentBeginning> "1815-12-10T00:00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime> .',
        f'<{LUX}person/p1> <{NS}endOfAgentEnding> "1852-11-27T23:59:59"^^<http://www.w3.org/2001/XMLSchema#dateTime> .',
        f"<{LUX}person/p1> <{NS}agentMemberOfGroup> <urn:grp> .",
        f"<{LUX}person/p1> <{NS}agentAny> <urn:grp> .",
        f"<{LUX}person/p1> <{NS}nationality> <urn:brit> .",
        f"<{LUX}person/p1> <{NS}occupation> <urn:math> .",
        f'<{LUX}person/p1> <{NS}agentRecordText> "ada lovelace lovelace, ada 12345" .',
    ]


def test_item_triples(mapper):
    got = mapper.transform(rec({
        "id": f"{LUX}object/o1",
        "type": "HumanMadeObject",
        "identified_by": [{"type": "Name", "content": "A Pot"}],
        "representation": [{"digitally_shown_by": [
            {"access_point": [{"id": "https://img/1.jpg"}]}]}],
        "made_of": [{"id": "urn:clay"}],
        "dimension": [{"value": 12.5, "classified_as": [{"id": "urn:H"}]}],
        "carries": [{"id": "urn:txt"}],
    }))

    assert got == [
        f"<{LUX}object/o1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <{NS}Item> .",
        f"<{LUX}object/o1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://linked.art/ns/terms/HumanMadeObject> .",
        f'<{LUX}object/o1> <{NS}itemName> "a pot" .',
        f'<{LUX}object/o1> <{NS}name> "a pot" .',
        f'<{LUX}object/o1> <{NS}itemHasDigitalImage> "1"^^<http://www.w3.org/2001/XMLSchema#decimal> .',
        f'<{LUX}object/o1> <{NS}itemIsOnline> "1"^^<http://www.w3.org/2001/XMLSchema#decimal> .',
        f"<{LUX}object/o1> <{NS}carries> <urn:txt> .",
        f"<{LUX}object/o1> <{NS}itemAny> <urn:txt> .",
        f"<{LUX}object/o1> <{NS}material> <urn:clay> .",
        f"<{LUX}object/o1> <{NS}itemAny> <urn:clay> .",
        f'<{LUX}object/o1> <{NS}height> "12.5"^^<http://www.w3.org/2001/XMLSchema#decimal> .',
        f'<{LUX}object/o1> <{NS}dimension> "12.5"^^<http://www.w3.org/2001/XMLSchema#decimal> .',
        f'<{LUX}object/o1> <{NS}itemRecordText> "a pot" .',
    ]


def test_place_wkt_is_validated(mapper):
    base = {"id": f"{LUX}place/x", "type": "Place", "identified_by": []}
    good = mapper.transform(rec(dict(base, defined_by="POINT (10 20)")))
    assert any("placeWKT" in t for t in good)
    # latitude out of range, and unparsable: both dropped, neither raises
    for bad in ["POINT (10 200)", "NOT WKT AT ALL"]:
        out = mapper.transform(rec(dict(base, defined_by=bad)))
        assert not any("placeWKT" in t for t in out), bad


def test_work_subjects_and_public_domain(mapper):
    got = mapper.transform(rec({
        "id": f"{LUX}text/t1",
        "type": "LinguisticObject",
        "identified_by": [],
        "about": [{"id": "urn:sub", "type": "Type"}],
        "represents": [{"id": "urn:per", "type": "Person"}],
        "subject_to": [{"classified_as": [
            {"id": "http://creativecommons.org/publicdomain/zero/1.0/"}]}],
    }))
    assert f"<{LUX}text/t1> <{NS}workAboutConcept> <urn:sub> ." in got
    assert f"<{LUX}text/t1> <{NS}workAboutAgent> <urn:per> ." in got
    assert f'<{LUX}text/t1> <{NS}workIsPublicDomain> "1"^^<http://www.w3.org/2001/XMLSchema#decimal> .' in got


# --- html stripping in statements ------------------------------------------

@pytest.mark.parametrize("html,expected", [
    ("<p>Hello world</p>", "hello world"),
    ("<b>bold</b> and tail", "bold and tail"),
    ("plain text, no markup", "plain text, no markup"),
    ("<p>caf&eacute;</p>", "café"),
    ("<p>a &amp; b</p>", "a & b"),
    ("<p>unclosed", "unclosed"),
    # script/style must not be spliced into the searchable text -- lxml's
    # text_content() includes them where bs4's get_text() does not
    ("<p>text</p><script>var x=1;</script>", "text"),
    ("<style>.a{color:red}</style><p>hi</p>", "hi"),
    # starts with '<' but isn't markup: left alone rather than raising
    ("<<<", "<<<"),
    ("<>", "<>"),
])
def test_statement_html_is_reduced_to_text(mapper, html, expected):
    got = mapper.transform(rec({
        "id": f"{LUX}text/h", "type": "LinguisticObject",
        "identified_by": [],
        "referred_to_by": [{"content": html}],
    }))
    text = [t for t in got if "workRecordText" in t]
    assert text == [f'<{LUX}text/h> <{NS}workRecordText> "{expected}" .']


# --- shapes that used to abort the export ----------------------------------

def test_record_without_identified_by_does_not_raise(mapper):
    """data["identified_by"] was the one unguarded access in the whole
    mapper. manage-data.py --nt has no error handling around transform, so a
    single such record ended the slice's export."""
    got = mapper.transform(rec({"id": f"{LUX}concept/c1", "type": "Type"}))
    assert any("conceptRecordText" in t for t in got)


def test_unknown_type_raises_rather_than_emitting_junk(mapper):
    """Deliberate: an unmapped type would silently produce `other`-prefixed
    predicates that nothing queries. The caller catches it per record."""
    with pytest.raises(ValueError):
        mapper.transform(rec({"id": f"{LUX}x/1", "type": "Frobnicator",
                              "identified_by": []}))


# --- the mapper must not mutate the record it is given ----------------------

def test_transform_does_not_mutate_the_source_record(mapper):
    """Several blocks did `x = data.get(k, []); x.extend(data.get(k2, []))`,
    which appends to the record's OWN list. Nothing is written back in --nt so
    it caused no corruption there, but it silently grows the input."""
    data = {
        "id": f"{LUX}text/t2",
        "type": "LinguisticObject",
        "identified_by": [],
        "about": [{"id": "urn:a", "type": "Type"}],
        "represents": [{"id": "urn:b", "type": "Person"}],
        "created_by": {"classified_as": [{"id": "urn:c"}],
                       "technique": [{"id": "urn:t"}],
                       "influenced_by": [{"id": "urn:i", "type": "Person"}],
                       "used_specific_object": [{"id": "urn:u", "type": "Type"}]},
    }
    import copy
    before = copy.deepcopy(data)
    mapper.transform(rec(data))
    assert data == before


def test_item_carries_does_not_mutate(mapper):
    data = {
        "id": f"{LUX}object/o2",
        "type": "HumanMadeObject",
        "identified_by": [],
        "carries": [{"id": "urn:1"}],
        "shows": [{"id": "urn:2"}],
        "digitally_carries": [{"id": "urn:3"}],
    }
    import copy
    before = copy.deepcopy(data)
    mapper.transform(rec(data))
    assert data == before


def test_classification_without_id_is_not_treated_as_a_match(mapper):
    """cxns was built with x.get("id", None), so it contained None for any
    classification lacking an id. If the aat lookup behind sortIdentifier /
    primaryName is itself missing from the idmap -- also None -- then
    `None in cxns` matched and mislabelled the field."""
    m = QleverMapper({"all_configs": StubConfigs(), "namespace": "urn:ns/",
                      "name": "qlever"})
    m.sortIdentifier = None          # aat term absent from the idmap
    got = m.transform(rec({
        "id": f"{LUX}object/o3",
        "type": "HumanMadeObject",
        "identified_by": [{"type": "Identifier", "content": "abc",
                           "classified_as": [{"_label": "no id here"}]}],
    }))
    assert f'<{LUX}object/o3> <{NS}itemIdentifier> "abc" .' in got
    assert not any("sortIdentifier" in t for t in got)
