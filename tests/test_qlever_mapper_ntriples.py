"""QleverMapper (mapper2) must never write a line that isn't valid N-Triples.

The mapper builds every triple with an f-string and copies record values
straight into it, so whatever is in the record ends up in the .nt file. Most
values come from upstream sources we don't control, and a bad one does real
damage. Probing qlever-index v0.5.50 showed:

  * `"`, `\\`, `<` or a tab inside <...> is a hard parse error. The index
    build fails.
  * a `"` inside a "..." literal is worse. The serial parser logs "Parsing of
    line has Failed" and EXITS 0, and every triple after that line in the
    slice is silently dropped. The parallel parser fails the build instead.
  * `<https:/>`, `<>`, `<None>`, relative IRIs, and spaces and `{}|^``
    inside IRIs are all ACCEPTED (a space only gets a WARN). None of them are
    valid N-Triples, and each one becomes a junk node in the graph.

So these tests check the output against the W3C N-Triples grammar instead of
just "qlever didn't crash". qlever is laxer than the spec today, and other
consumers (rdflib, riot, a future qlever) may not be.

Raising is an acceptable outcome for a bad record: manage-data.py --nt catches
the exception per record and skips it. Writing a bad line is not acceptable.

If qlever-index is on the PATH, the last test also builds a real index from
the mapper output.
"""

import logging
import math
import re
import shutil
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest

from lux_pipeline.sources.lux.qlever.mapper2 import QleverMapper

GLOBALS = {"primaryName": "urn:PN", "sortName": "urn:SN", "gender": "urn:GEN",
           "nationality": "urn:NAT", "occupation": "urn:OCC"}
IDMAP = {"http://vocab.getty.edu/aat/300456575##quaType": "urn:SORTID",
         "http://vocab.getty.edu/aat/300025976##quaType": "urn:COLLECTION",
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
XSD = "http://www.w3.org/2001/XMLSchema#"
GOOD = "https://example.org/good"


# --- a strict N-Triples line validator (W3C RDF 1.1 N-Triples, sec. 7) -------

_UCHAR = r"\\u[0-9A-Fa-f]{4}|\\U[0-9A-Fa-f]{8}"
_IRIREF = rf'<((?:[^\x00-\x20<>"{{}}|^`\\]|{_UCHAR})*)>'
_STRING = rf'"((?:[^"\\\n\r]|\\[tbnrf"\'\\]|{_UCHAR})*)"'
_LANGTAG = r"@[a-zA-Z]+(?:-[a-zA-Z0-9]+)*"
_LITERAL = rf"{_STRING}(?:\^\^{_IRIREF}|{_LANGTAG})?"
# the mapper never writes blank nodes, so they are rejected too
_TRIPLE = re.compile(
    rf"{_IRIREF}[ \t]+{_IRIREF}[ \t]+(?:{_IRIREF}|{_LITERAL})[ \t]*\.[ \t]*")
# N-Triples forbids relative IRIs: there must be a scheme
_ABSOLUTE = re.compile(r"[A-Za-z][A-Za-z0-9+.\-]*:.")
# The grammar allows `https:/` (scheme `https`, path `/`). It is still junk:
# an http(s) IRI needs an authority.
_HOLLOW_HTTP = re.compile(r"https?:(?!//[^/?#\s]+)", re.I)

_DECIMAL = re.compile(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)")
_DATETIME = re.compile(
    r"-?(?:[1-9]\d{4,}|\d{4})-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?"
    r"(?:Z|[+-]\d\d:\d\d)?")


def ntriples_problems(line):
    """Why `line` is not valid N-Triples, or [] if it's fine."""
    m = _TRIPLE.fullmatch(line)
    if not m:
        return [f"not an N-Triples statement: {line!r}"]
    s, p, o, lit, dtype = m.groups()
    problems = []
    for iri in (s, p, o, dtype):
        if iri is None:
            continue
        if not _ABSOLUTE.match(iri):
            problems.append(f"relative or empty IRI <{iri}> in {line!r}")
        elif _HOLLOW_HTTP.match(iri):
            problems.append(f"http(s) IRI with no host <{iri}> in {line!r}")
    if dtype == f"{XSD}decimal" and not _DECIMAL.fullmatch(lit):
        problems.append(f"bad xsd:decimal {lit!r} in {line!r}")
    if dtype == f"{XSD}dateTime" and not _DATETIME.fullmatch(lit):
        problems.append(f"bad xsd:dateTime {lit!r} in {line!r}")
    return problems


def assert_valid_or_raises(mapper, data):
    """Transform `data`. It may raise, since the caller skips the record. If
    it doesn't raise, every line it returns must be valid N-Triples."""
    try:
        lines = mapper.transform({"data": data})
    except Exception:
        return
    problems = [p for line in lines for p in ntriples_problems(line)]
    assert not problems, "\n".join(problems)


# --- the validator itself ----------------------------------------------------

@pytest.mark.parametrize("line", [
    f"<{GOOD}> <{GOOD}> <{GOOD}> .",
    f'<{GOOD}> <{GOOD}> "plain" .',
    f'<{GOOD}> <{GOOD}> "esc \\" \\\\ \\n" .',
    f'<{GOOD}> <{GOOD}> "café ünïcode" .',
    f'<{GOOD}> <{GOOD}> "12.5"^^<{XSD}decimal> .',
    f'<{GOOD}> <{GOOD}> "1"^^<{XSD}decimal> .',
    f'<{GOOD}> <{GOOD}> "1815-12-10T00:00:00"^^<{XSD}dateTime> .',
    f'<{GOOD}> <{GOOD}> "-10000-01-01T00:00:00Z"^^<{XSD}dateTime> .',
    f"<{GOOD}> <{GOOD}> <urn:x> .",
])
def test_validator_accepts(line):
    assert ntriples_problems(line) == []


@pytest.mark.parametrize("line", [
    f"<{GOOD}> <{GOOD}> <https:/> .",
    f"<{GOOD}> <{GOOD}> <https://> .",
    f"<{GOOD}> <{GOOD}> <> .",
    f"<{GOOD}> <{GOOD}> <None> .",
    f"<{GOOD}> <{GOOD}> <relative/path> .",
    f"<{GOOD}> <{GOOD}> <https://x.org/a b> .",
    f'<{GOOD}> <{GOOD}> <https://x.org/a"b> .',
    f"<{GOOD}> <{GOOD}> <https://x.org/{{a}}> .",
    f'<{GOOD}> <{GOOD}> "a"b" .',
    f'<{GOOD}> <{GOOD}> "a\nb" .',
    f'<{GOOD}> <{GOOD}> "a\\q" .',      # \q is not an ECHAR
    f'<{GOOD}> <{GOOD}> "nan"^^<{XSD}decimal> .',
    f'<{GOOD}> <{GOOD}> "1e+30"^^<{XSD}decimal> .',
    f'<{GOOD}> <{GOOD}> "1815"^^<{XSD}dateTime> .',
    f"<{GOOD}> <{GOOD}> <{GOOD}>",
])
def test_validator_rejects(line):
    assert ntriples_problems(line)


# --- bad values in every IRI slot the mapper writes ------------------------

BAD_IRIS = [
    "https:/",                 # the case seen in the wild
    "https:",
    "https://",
    "http:/example.org/x",
    "",
    "relative/path",
    "https://example.org/a b",
    "https://example.org/a\tb",
    "https://example.org/a\nb",
    'https://example.org/a"b',
    "https://example.org/a>b",
    "https://example.org/a<b",
    "https://example.org/a\\b",
    "https://example.org/{a}",
    "https://example.org/a|b",
    "https://example.org/a^b",
    "https://example.org/a`b",
    None,                      # interpolates as <None>
    12345,                     # interpolates as <12345>
]


def _ref(v, typ="Type"):
    return {"id": v, "type": typ}


# (label, record type, function from a bad IRI to the record data). Together
# these reach every `<{...}>` the mapper interpolates from the record.
IRI_SLOTS = [
    ("subject id", "Type", lambda v: {"id": v}),
    ("equivalent", "Type", lambda v: {"equivalent": [_ref(v)]}),
    ("classified_as", "Type", lambda v: {"classified_as": [_ref(v)]}),
    ("member_of", "Type", lambda v: {"member_of": [_ref(v, "Set")]}),
    ("part_of", "Place", lambda v: {"part_of": [_ref(v, "Place")]}),
    ("broader", "Type", lambda v: {"broader": [_ref(v)]}),
    ("agent nationality", "Person", lambda v: {"classified_as": [
        {"id": v, "classified_as": [{"id": "urn:NAT"}]}]}),
    ("work language", "LinguisticObject", lambda v: {"language": [_ref(v, "Language")]}),
    ("work about", "LinguisticObject", lambda v: {"about": [_ref(v, "Person")]}),
    ("work represents", "VisualItem", lambda v: {"represents": [_ref(v, "Place")]}),
    ("work created_by agent", "LinguisticObject",
     lambda v: {"created_by": {"carried_out_by": [_ref(v, "Person")]}}),
    ("work created_by place", "LinguisticObject",
     lambda v: {"created_by": {"took_place_at": [_ref(v, "Place")]}}),
    ("work created_by technique", "LinguisticObject",
     lambda v: {"created_by": {"technique": [_ref(v)]}}),
    ("work created_by cause", "LinguisticObject",
     lambda v: {"created_by": {"caused_by": [_ref(v, "Event")]}}),
    ("work created_by influence", "LinguisticObject",
     lambda v: {"created_by": {"influenced_by": [_ref(v, "Person")]}}),
    ("work created_by part agent", "LinguisticObject",
     lambda v: {"created_by": {"part": [{"carried_out_by": [_ref(v, "Person")]}]}}),
    ("set curated_by", "Set",
     lambda v: {"used_for": [{"carried_out_by": [_ref(v, "Group")]}]}),
    ("item carries", "HumanMadeObject", lambda v: {"carries": [_ref(v, "LinguisticObject")]}),
    ("item digitally_shows", "DigitalObject",
     lambda v: {"digitally_shows": [_ref(v, "VisualItem")]}),
    ("item made_of", "HumanMadeObject", lambda v: {"made_of": [_ref(v, "Material")]}),
    ("item produced_by agent", "HumanMadeObject",
     lambda v: {"produced_by": {"carried_out_by": [_ref(v, "Person")]}}),
    ("person born place", "Person",
     lambda v: {"born": {"took_place_at": [_ref(v, "Place")]}}),
    ("group formed_by agent", "Group",
     lambda v: {"formed_by": {"carried_out_by": [_ref(v, "Person")]}}),
    ("event carried_out_by", "Activity", lambda v: {"carried_out_by": [_ref(v, "Person")]}),
    ("event took_place_at", "Event", lambda v: {"took_place_at": [_ref(v, "Place")]}),
    ("event caused_by", "Period", lambda v: {"caused_by": [_ref(v, "Event")]}),
    ("event used_specific_object", "Activity",
     lambda v: {"used_specific_object": [_ref(v, "HumanMadeObject")]}),
]


def _record(rectype, extra):
    data = {"id": f"{LUX}x/1", "type": rectype,
            "identified_by": [{"type": "Name", "content": "a name"}]}
    data.update(extra)
    return data


@pytest.mark.parametrize("bad", BAD_IRIS, ids=repr)
@pytest.mark.parametrize("label,rectype,build", IRI_SLOTS, ids=[s[0] for s in IRI_SLOTS])
def test_bad_iri_never_reaches_output(mapper, label, rectype, build, bad):
    assert_valid_or_raises(mapper, _record(rectype, build(bad)))


@pytest.mark.parametrize("label,rectype,build", IRI_SLOTS, ids=[s[0] for s in IRI_SLOTS])
def test_good_iri_in_each_slot_is_written(mapper, label, rectype, build):
    """Guard for the test above: each slot really is written to the output,
    so a pass there isn't just because the slot was never reached."""
    lines = mapper.transform({"data": _record(rectype, build(GOOD))})
    if label == "subject id":
        assert all(line.startswith(f"<{GOOD}> ") for line in lines)
    else:
        assert any(line.endswith(f"<{GOOD}> .") for line in lines), label
    for line in lines:
        assert ntriples_problems(line) == [], line


# --- record values that are interpolated into predicates --------------------

@pytest.mark.parametrize("bad_type", ["Set Thing", 'Se"t', "Set>", "", "Set\n"], ids=repr)
def test_member_of_type_is_not_spliced_raw_into_predicate(mapper, bad_type):
    """member_of[].type goes into the predicate IRI: `...MemberOf{type}`."""
    assert_valid_or_raises(mapper, _record(
        "HumanMadeObject", {"member_of": [{"id": GOOD, "type": bad_type}]}))


# --- record values that are interpolated into literals ----------------------

BAD_DATES = [
    "1815",                          # year only
    "circa 1800",
    "1815-12-10",                    # date with no time
    '1815-12-10T00:00:00"',          # would close the literal early
    "1815-12-10T00:00:00\n",
    "1815-12-10T00:00:00\\",
    " 1815-12-10T00:00:00",
]


@pytest.mark.parametrize("bad", BAD_DATES, ids=repr)
@pytest.mark.parametrize("rectype,wrap", [
    ("Person", lambda ts: {"born": {"timespan": ts}}),
    ("LinguisticObject", lambda ts: {"created_by": {"timespan": ts}}),
    ("Event", lambda ts: {"timespan": ts}),
], ids=["person born", "work created", "event"])
@pytest.mark.parametrize("key", ["begin_of_the_begin", "end_of_the_end"])
def test_bad_timespan_value(mapper, rectype, wrap, key, bad):
    assert_valid_or_raises(mapper, _record(rectype, wrap({key: bad})))


@pytest.mark.parametrize("bad", [
    "nan", "inf", "-inf", "1e400",   # float() accepts all of these
    "1e30", 1e30, 1e-7,              # str(float) writes exponent notation
    math.nan, math.inf,
], ids=repr)
def test_bad_dimension_value(mapper, bad):
    assert_valid_or_raises(mapper, _record("HumanMadeObject", {"dimension": [
        {"value": bad, "classified_as": [{"id": "urn:H"}]}]}))


@pytest.mark.parametrize("wkt", [
    "POINT (10 20)\n",               # shapely accepts it; the \n is copied out
    "POINT (10 20)\r\n",
    "POLYGON ((0 0, 1 0, 1 1, 0 0))\n",
], ids=repr)
def test_wkt_is_written_as_a_single_line_literal(mapper, wkt):
    assert_valid_or_raises(mapper, _record("Place", {"defined_by": wkt}))


@pytest.mark.parametrize("wkt", [
    "LINESTRING (0 0, 1 1)",
    "MULTIPOINT ((0 0), (1 1))",
    "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 0)))",
], ids=repr)
def test_non_polygon_wkt_does_not_raise(mapper, wkt):
    """The bounds check reads `geom.exterior`, which only Polygons have, so
    any other geometry raises AttributeError and the caller drops the whole
    Place record (names, part_of and so on), not just the WKT."""
    lines = mapper.transform({"data": _record("Place", {"defined_by": wkt})})
    for line in lines:
        assert ntriples_problems(line) == [], line


@pytest.mark.parametrize("content", [
    'say "hello"', "back\\slash", "line\nbreak", "cr\rreturn", "tab\there",
    "trailing\\", '"', "\\", "\u2028sep", "\x0bvt", "\x00nul",
], ids=repr)
def test_name_and_statement_text_is_escaped(mapper, content):
    """identified_by and referred_to_by go through sanitize_string, which
    should make any text safe to put in a literal."""
    assert_valid_or_raises(mapper, _record("Type", {
        "identified_by": [{"type": "Name", "content": content},
                          {"type": "Identifier", "content": content}],
        "referred_to_by": [{"content": content}, {"content": f"<p>{content}</p>"}],
    }))


# --- end to end through the real qlever parser ------------------------------

@pytest.mark.skipif(not shutil.which("qlever-index"), reason="qlever-index not installed")
def test_qlever_index_parses_every_line(mapper, tmp_path):
    """Build a real index from the mapper's output for every bad-input case
    above. Use the serial parser, because it fails quietly: on a bad literal
    it logs "Parsing of line has Failed", exits 0, and drops the rest of the
    file. So check the parsed-triple count, not just the exit code."""
    records = [_record(rt, build(bad)) for _, rt, build in IRI_SLOTS for bad in BAD_IRIS]
    records += [_record("Person", {"born": {"timespan": {"begin_of_the_begin": d}}})
                for d in BAD_DATES]
    records += [_record("Place", {"defined_by": "POINT (10 20)\n"})]
    lines = []
    for i, data in enumerate(records):
        if data["id"] == f"{LUX}x/1":
            data["id"] = f"{LUX}x/{i}"     # keep subjects distinct
        try:
            lines.extend(mapper.transform({"data": data}))
        except Exception:
            pass
    # a sentinel last, so a truncated parse is visible in the count
    lines.append(f'<{GOOD}> <{GOOD}> "sentinel" .')
    nt = tmp_path / "in.nt"
    nt.write_text("\n".join(lines) + "\n", encoding="utf-8")

    r = subprocess.run(
        ["qlever-index", "-i", str(tmp_path / "idx"), "-f", str(nt), "-F", "nt",
         "-p", "false"],
        capture_output=True, text=True, timeout=300)
    log = r.stdout + r.stderr
    assert r.returncode == 0, log[-2000:]
    assert "Parsing of line has Failed" not in log, log[-2000:]
    parsed = re.search(r"Triples parsed: ([\d,]+)", log)
    assert parsed and int(parsed.group(1).replace(",", "")) == len(lines), log[-2000:]


# --- what the mapper does with bad values: repair or drop -------------------

from lux_pipeline.sources.lux.qlever.mapper2 import clean_datetime, clean_iri, format_decimal


@pytest.mark.parametrize("value,expected", [
    (GOOD, GOOD),
    ("urn:uuid:1234", "urn:uuid:1234"),
    ("HTTPS://example.org/x", "HTTPS://example.org/x"),
    # forbidden characters are percent-encoded, as sanitize_uri did for spaces
    ("https://example.org/a b", "https://example.org/a%20b"),
    ('https://example.org/a"b', "https://example.org/a%22b"),
    ("https://example.org/{a}", "https://example.org/%7Ba%7D"),
    ("https://example.org/a\\b", "https://example.org/a%5Cb"),
    # line breaks are wrapping, so they are deleted, as are outer spaces
    ("https://example.org/a\nb", "https://example.org/ab"),
    (" https://example.org/x ", "https://example.org/x"),
    # nothing to repair: dropped
    ("https:/", None), ("https:", None), ("https://", None),
    ("http:/example.org/x", None), ("", None), ("relative/path", None),
    (None, None), (12345, None),
])
def test_clean_iri(value, expected):
    assert clean_iri(value) == expected


@pytest.mark.parametrize("value,expected", [
    ("1815-12-10T00:00:00", "1815-12-10T00:00:00"),
    (" 1815-12-10T00:00:00\n", "1815-12-10T00:00:00"),
    ("-10000-01-01T00:00:00Z", "-10000-01-01T00:00:00Z"),
    ("1815", None), ("1815-12-10", None), ('1815-12-10T00:00:00"', None),
    (None, None), (1815, None),
])
def test_clean_datetime(value, expected):
    assert clean_datetime(value) == expected


@pytest.mark.parametrize("value,expected", [
    (12.5, "12.5"), (1.0, "1.0"), (-0.0, "-0.0"),
    (1e30, "1000000000000000000000000000000"),
    (1e-7, "0.0000001"),
    (math.nan, None), (math.inf, None), (-math.inf, None),
])
def test_format_decimal(value, expected):
    assert format_decimal(value) == expected


def test_bad_subject_raises_so_the_record_is_skipped(mapper):
    with pytest.raises(ValueError):
        mapper.transform({"data": _record("Type", {"id": "https:/"})})


def test_bad_reference_is_dropped_and_the_rest_kept(mapper):
    lines = mapper.transform({"data": _record("Type", {
        "classified_as": [_ref("https:/"), _ref(GOOD)]})})
    assert f"<{LUX}x/1> <https://lux.collections.yale.edu/ns/conceptClassification> <{GOOD}> ." in lines
    assert not any("https:/>" in line for line in lines)


def test_non_polygon_wkt_is_written(mapper):
    lines = mapper.transform({"data": _record("Place", {
        "defined_by": "LINESTRING (0 0,\n 1 1)"})})
    assert (f'<{LUX}x/1> <https://lux.collections.yale.edu/ns/placeWKT> '
            f'"LINESTRING (0 0, 1 1)"^^<http://www.opengis.net/ont/geosparql#wktLiteral> .') in lines


# --- partial dates are padded out to the start or end of their period -------

from lux_pipeline.sources.lux.qlever.mapper2 import pad_datetime


@pytest.mark.parametrize("value,end,expected", [
    ("1815", False, "1815-01-01T00:00:00"),
    ("1815", True, "1815-12-31T23:59:59"),
    ("1815-06", False, "1815-06-01T00:00:00"),
    ("1815-06", True, "1815-06-30T23:59:59"),
    ("1815-12-10", False, "1815-12-10T00:00:00"),
    ("1815-12-10", True, "1815-12-10T23:59:59"),
    (" 1815 ", False, "1815-01-01T00:00:00"),
    # leap years, proleptic Gregorian
    ("2000-02", True, "2000-02-29T23:59:59"),
    ("1900-02", True, "1900-02-28T23:59:59"),
    ("2024-02", True, "2024-02-29T23:59:59"),
    ("-0004-02", True, "-0004-02-29T23:59:59"),
    ("-10000", False, "-10000-01-01T00:00:00"),
    ("-10000", True, "-10000-12-31T23:59:59"),
    # not a partial date, or not a real one
    ("1815-13", False, None), ("1815-00", True, None),
    ("1815-02-30", False, None), ("1815-04-31", True, None),
    ("181", False, None), ("circa 1800", False, None),
    ("1815-12-10T00:00:00", False, None), (None, False, None), (1815, False, None),
])
def test_pad_datetime(value, end, expected):
    assert pad_datetime(value, end) == expected


def test_partial_dates_are_padded_and_logged(mapper, caplog):
    caplog.set_level(logging.DEBUG, logger="lux_pipeline")
    lines = mapper.transform({"data": _record("Person", {
        "born": {"timespan": {"begin_of_the_begin": "1815",
                              "end_of_the_end": "1815-12-10"}},
        "died": {"timespan": {"begin_of_the_begin": "1852-11-27T00:00:00",
                              "end_of_the_end": "1852"}}})})
    ns = "https://lux.collections.yale.edu/ns/"
    dt = "^^<http://www.w3.org/2001/XMLSchema#dateTime>"
    me = f"{LUX}x/1"
    assert f'<{me}> <{ns}startOfAgentBeginning> "1815-01-01T00:00:00"{dt} .' in lines
    assert f'<{me}> <{ns}endOfAgentBeginning> "1815-12-10T23:59:59"{dt} .' in lines
    assert f'<{me}> <{ns}startOfAgentEnding> "1852-11-27T00:00:00"{dt} .' in lines
    assert f'<{me}> <{ns}endOfAgentEnding> "1852-12-31T23:59:59"{dt} .' in lines
    # one line per padded value, naming the record; complete dates aren't logged
    assert [r.getMessage() for r in caplog.records if "padded" in r.getMessage()] == [
        f"*** padded begin date '1815' to 1815-01-01T00:00:00 in {me} ***",
        f"*** padded end date '1815-12-10' to 1815-12-10T23:59:59 in {me} ***",
        f"*** padded end date '1852' to 1852-12-31T23:59:59 in {me} ***",
    ]
