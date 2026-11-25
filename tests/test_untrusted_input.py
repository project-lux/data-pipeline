"""What records from outside cannot do: XML that loads files through its
own DTD, URIs that name another source's records from inside a path, and
identifiers with control characters that split the assertion files into
lines nobody wrote."""

import logging
import re
import subprocess
import sys
from pathlib import Path

import pytest
from lxml import etree

sys.path.insert(0, str(Path(__file__).parent))

from test_assertion_log_lifecycle import StubConfigs, StubIdmap  # noqa: E402
from webui_fixtures import make_configs  # noqa: E402

from lux_pipeline.config import CONTROL_CHARS, match_at_start  # noqa: E402
from lux_pipeline.process.identity_resolver import IdentityResolver  # noqa: E402
from lux_pipeline.process.utils import safe_xml  # noqa: E402

ROOT = Path(__file__).parent.parent / "lux_pipeline"
YCBA = "https://ycba-lux.s3.amazonaws.com/v3/object/12345"


# --- XML ---------------------------------------------------------------------------


def evil_dtd(tmp_path):
    dtd = tmp_path / "evil.dtd"
    dtd.write_text('<!ENTITY leak "SECRET">')
    return f'<!DOCTYPE r [<!ENTITY % x SYSTEM "{dtd.as_uri()}"> %x;]><r>&leak;</r>'.encode()


def test_lxml_alone_loads_a_parameter_entity(tmp_path):
    # what safe_xml is for: the default parser reads the file the DTD names
    assert etree.fromstring(evil_dtd(tmp_path)).text == "SECRET"


@pytest.mark.parametrize("call", ["XML", "fromstring", "parse", "iterparse", "XMLPullParser"])
def test_safe_xml_loads_nothing(tmp_path, call):
    doc = evil_dtd(tmp_path)
    src = tmp_path / "doc.xml"
    src.write_bytes(doc)
    if call == "parse":
        root = safe_xml.parse(str(src)).getroot()
    elif call == "iterparse":
        root = [el for _, el in safe_xml.iterparse(str(src), events=("end",))][-1]
    elif call == "XMLPullParser":
        p = safe_xml.XMLPullParser(events=("end",))
        p.feed(doc)
        root = p.close()
    else:
        root = getattr(safe_xml, call)(doc)
    assert "SECRET" not in etree.tostring(root, encoding="unicode")


def test_safe_xml_reads_ordinary_documents_as_before():
    doc = b'<r a="&amp;&lt;"><x>&#233;&gt; t</x><!-- c --></r>'
    assert etree.tostring(safe_xml.XML(doc)) == etree.tostring(etree.XML(doc))
    assert safe_xml.XML(doc, remove_blank_text=True).find("x").text == "é> t"


def test_a_parameter_entity_on_dev_zero_does_not_hang():
    # the default parser reads /dev/zero for ever
    code = ("from lux_pipeline.process.utils import safe_xml\n"
            "safe_xml.fromstring(b'<!DOCTYPE r [<!ENTITY % x SYSTEM \"file:///dev/zero\">%x;]><r/>')")
    subprocess.run([sys.executable, "-c", code], timeout=20, check=True,
                   cwd=str(ROOT.parent))


def test_nothing_parses_xml_with_lxml_directly():
    calls = re.compile(r"\b(?:etree|ET)\.(?:XML|fromstring|parse|iterparse|XMLPullParser|XMLParser)\(")
    found = []
    for path in ROOT.rglob("*.py"):
        if path.name == "safe_xml.py":
            continue
        for n, line in enumerate(path.read_text().splitlines(), 1):
            if calls.search(line) and 'etree.XML("<record/>")' not in line:
                found.append(f"{path.relative_to(ROOT)}:{n}: {line.strip()}")
    assert found == [], "parse XML from outside with process/utils/safe_xml:\n" + "\n".join(found)


# --- where a source's matches entry may be found -------------------------------------


@pytest.mark.parametrize("uri,m,ok", [
    ("http://vocab.getty.edu/aat/1", "vocab.getty.edu/aat/", True),
    ("vocab.getty.edu/aat/1", "vocab.getty.edu/aat/", True),
    ("https://sws.geonames.org/1/", "geonames.org/", True),
    ("http://en.wikipedia.org/wiki/X", "wikipedia.org/wiki/", True),
    ("//www.wikidata.org/entity/Q1", "wikidata.org/entity/", True),
    ("https://orcid.org/0000-0001", "/orcid.org/", True),
    ("https://media.art.yale.edu/content/lux/1", "https://media.art.yale.edu/content/lux/", True),
    ("http://vocab.getty.edu/ulan/1/" + YCBA, "ycba-lux.s3.amazonaws.com/v3/", False),
    ("https://example.org/?u=vocab.getty.edu/aat/1", "vocab.getty.edu/aat/", False),
    ("https://example.org/vocab.getty.edu/aat/1", "vocab.getty.edu/aat/", False),
    ("http://geonames.org.example/geonames.org/1", "geonames.org/", False),
    ("http://vocab.getty.edu/aat/1", "aat/", False),
])
def test_match_at_start(uri, m, ok):
    assert match_at_start(uri, m) is ok


@pytest.fixture
def cfgs(tmp_path):
    # ycba internal, aat external
    return make_configs(tmp_path)


def test_a_uri_inside_another_sources_path_names_nothing_of_it(cfgs):
    # a Wikidata ULAN ID "1/https://ycba-lux..." used to name the Yale record
    got = cfgs.canonicalize("http://vocab.getty.edu/aat/1/" + YCBA)
    assert got is None or not got.startswith(cfgs.internal["ycba"]["namespace"]), got
    assert cfgs.canonicalize("https://example.org/r?u=" + YCBA) is None


def test_another_sources_uri_wrapped_in_a_namespace_names_nothing(cfgs):
    # "{ulan namespace}{ULAN ID}" with the ID a Yale object's URI
    assert cfgs.split_uri("http://vocab.getty.edu/aat/" + YCBA) is None
    assert cfgs.canonicalize("http://vocab.getty.edu/aat/" + YCBA) is None


def test_a_source_wrapped_in_its_own_namespace_is_still_unwrapped(cfgs):
    cfgs.external["aat"]["matches"] = ["vocab.getty.edu/aat/", "aat.example.org/"]
    cfgs.build_match_index()
    src, ident = cfgs.split_uri("http://vocab.getty.edu/aat/https://aat.example.org/300033618")
    assert src["name"] == "aat" and ident == "300033618"


def test_ordinary_uris_canonicalize_as_before(cfgs):
    assert cfgs.canonicalize("https://vocab.getty.edu/aat/300033618") == "http://vocab.getty.edu/aat/300033618"
    assert cfgs.canonicalize(YCBA) == YCBA
    assert cfgs.canonicalize("http://vocab.getty.edu/aat/300033618/") == "http://vocab.getty.edu/aat/300033618"


# --- control characters ------------------------------------------------------------


@pytest.mark.parametrize("bad", ["\t", "\n", "\r", "\x00", "\x1b", "\x7f"])
def test_an_identifier_with_a_control_character_names_nothing(cfgs, bad):
    assert cfgs.canonicalize(f"http://vocab.getty.edu/aat/300{bad}033618") is None
    assert cfgs.fix_identifier(cfgs.external["aat"], f"300{bad}1") is None
    # at the ends it is only whitespace, stripped as before
    if bad in "\t\n\r":
        assert cfgs.canonicalize(f"http://vocab.getty.edu/aat/300033618{bad}") == \
            "http://vocab.getty.edu/aat/300033618"


def test_the_assertions_file_gets_no_line_nobody_wrote(tmp_path, caplog):
    stub = StubConfigs(tmp_path)
    w = IdentityResolver(stub, StubIdmap(stub))
    forged = ("http://vocab.getty.edu/aat/1\nhttp://example.org/A##quaType\t"
              "http://example.org/B##quaType\tF1\nX")
    w.write_record({"data": {"id": "http://example.org/rec/1", "type": "Type",
                             "equivalent": [{"id": forged}, {"id": "http://vocab.getty.edu/aat/2"}]}})
    w.write_record({"data": {"id": "http://example.org/rec/\t2", "type": "Type", "equivalent": []}})
    with caplog.at_level(logging.WARNING, logger="lux_pipeline"):
        w.close()
    lines = Path(w.filename).read_text().splitlines()
    assert len(lines) == 1 and all(len(line.split("\t")) == 3 for line in lines), lines
    assert "aat:2" in lines[0] or "vocab.getty.edu/aat/2" in lines[0]
    assert "2 assertion(s) not written: a URI with a control character" in caplog.text


def test_control_chars():
    assert CONTROL_CHARS.search("a\tb") and not CONTROL_CHARS.search("http://x.org/a bé")
