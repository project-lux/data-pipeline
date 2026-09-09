"""MlMapper: html text extraction and reference collection.

Both were rewritten for speed -- BeautifulSoup replaced with lxml directly
(~10x, it was the largest single cost in transform()), and the reference
dedupe moved off O(n^2) list membership. Neither is allowed to change what
comes out, so these pin the behaviour that is easy to break:

  * script/style contents stay OUT of the extracted text. bs4's get_text()
    excluded them; lxml's text_content() does not, so a naive swap would
    splice css and javascript into descriptions.
  * refs keep insertion order and first-occurrence dedupe, because that
    order is the order the triples are emitted in.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.sources.lux.marklogic.mapper import MlMapper

IGNORE = {"identified_by", "referred_to_by", "equivalent", "subject_of", "language"}
INTERNAL = "https://lux.collections.yale.edu/data/"


class StubConfigs:
    internal_uri = INTERNAL


def mapper():
    m = object.__new__(MlMapper)
    m.configs = StubConfigs()
    m.ignore_props = IGNORE
    return m


def uri(n):
    return f"{INTERNAL}person/{n}"


# --- html -----------------------------------------------------------------

def clean(content):
    part = {"content": content}
    mapper().do_bs_html(part)
    return part


def test_markup_is_stripped_and_original_kept():
    part = clean("<p>Some <b>marked up</b> text.</p>")
    assert part["content"] == "Some marked up text."
    assert part["_content_html"] == "<p>Some <b>marked up</b> text.</p>"


def test_script_and_style_contents_are_not_text():
    # lxml's text_content() would include both; bs4's get_text() did not
    assert clean("<script>var x = 1;</script><p>after</p>")["content"] == "after"
    assert clean("<style>.a{color:red}</style><p>styled</p>")["content"] == "styled"


def test_entities_and_tails_and_malformed_markup():
    assert clean("<p>a &amp; b</p>")["content"] == "a & b"
    assert clean("<div><p>x</p>tail</div>")["content"] == "xtail"
    assert clean("<p>Unclosed <b>bold")["content"] == "Unclosed bold"


def test_non_markup_is_left_alone():
    # doesn't start with '<': untouched
    assert clean("plain text") == {"content": "plain text"}


def test_content_yielding_no_text_is_kept_as_is():
    # parses but has no text -- the original content must survive
    part = clean("<p></p>")
    assert part["content"] == "<p></p>"
    assert "_content_html" not in part

    # a comment is the one input where the two implementations reach the
    # same place differently: bs4 extracted "" (falsy, so no change), lxml
    # raises ParserError on a document with no elements. Both leave it alone.
    part = clean("<!-- just a comment -->")
    assert part["content"] == "<!-- just a comment -->"
    assert "_content_html" not in part


def test_bare_angle_bracket_is_treated_as_text():
    # both implementations parse this to the literal "<", which is non-empty,
    # so it is written back as cleaned content -- odd, but pre-existing
    part = clean("<")
    assert part["content"] == "<"
    assert part["_content_html"] == "<"


# --- references -----------------------------------------------------------

def test_refs_keep_first_occurrence_order_and_dedupe():
    data = {"id": uri("top"), "type": "Person",
            "member_of": [{"id": uri("b")}, {"id": uri("a")}, {"id": uri("b")}],
            "about": [{"id": uri("a")}, {"id": uri("c")}]}
    refs, all_refs = mapper().find_named_refs(data)
    assert refs == [uri("b"), uri("a"), uri("c")]
    assert all_refs == refs


def test_top_node_is_not_its_own_reference():
    data = {"id": uri("top"), "type": "Person", "member_of": [{"id": uri("a")}]}
    refs, all_refs = mapper().find_named_refs(data)
    assert uri("top") not in all_refs


def test_ignored_props_reach_all_refs_but_not_refs():
    data = {"id": uri("top"), "type": "Person",
            "member_of": [{"id": uri("keep")}],
            "referred_to_by": [{"id": uri("skip")}]}     # an ignore_props key
    refs, all_refs = mapper().find_named_refs(data)
    assert refs == [uri("keep")]
    assert all_refs == [uri("keep"), uri("skip")]


def test_ignore_does_not_leak_to_later_siblings():
    # the walk sets `ignore` per key and restores it; a leak here would
    # silently drop references from every property after an ignored one
    data = {"id": uri("top"), "type": "Person",
            "referred_to_by": [{"id": uri("skip")}],
            "member_of": [{"id": uri("keep")}]}
    refs, _ = mapper().find_named_refs(data)
    assert refs == [uri("keep")]


def test_external_uris_are_not_collected():
    data = {"id": uri("top"), "type": "Person",
            "member_of": [{"id": "http://vocab.getty.edu/aat/300404670"}]}
    refs, all_refs = mapper().find_named_refs(data)
    assert refs == [] and all_refs == []


def test_nesting_below_an_ignored_key_stays_ignored():
    data = {"id": uri("top"), "type": "Person",
            "referred_to_by": [{"type": "LinguisticObject",
                                "about": [{"id": uri("deep")}]}]}
    refs, all_refs = mapper().find_named_refs(data)
    assert refs == []
    assert all_refs == [uri("deep")]
