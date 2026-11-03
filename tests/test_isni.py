"""The ISNI source: the downloader's rewrite of the published JSON-LD into
one ``<ISNI><TAB><node>`` line per record (array and record-separator
forms, control characters, reads split anywhere), the loader reading that
as ``lines.gz/tabjson``, and the mapper. No network: the HTTP download is
the base class's and is replaced here."""

import gzip
import io
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.config import importObject
from lux_pipeline.process.base.downloader import BaseDownloader
from lux_pipeline.process.base.loader import Loader
from lux_pipeline.sources.isni import downloader as D
from lux_pipeline.sources.isni.mapper import IsniMapper, format_isni, literals

CONTEXT = {"schema": "http://schema.org/", "owl": "http://www.w3.org/2002/07/owl#",
           "madsrdf": "http://www.loc.gov/mads/rdf/v1#"}
PERSON = {"@id": "https://isni.org/isni/0000000121032683", "@type": "schema:Person",
          "schema:alternateName": ["Twain, Mark", {"@value": "Марк Твен", "@language": "ru"},
                                   "Twain, Mark"],
          "schema:birthDate": "1835-11-30", "schema:deathDate": "1910",
          "owl:sameAs": ["http://viaf.org/viaf/50566653", {"@id": "http://www.wikidata.org/entity/Q7245"}],
          "madsrdf:isIdentifiedByAuthority": [{"@id": "http://id.loc.gov/authorities/names/n79021164"},
                                              {"@id": "http://viaf.org/viaf/50566653"}]}
GROUP = {"@id": "https://isni.org/isni/000000012150090X", "@type": ["schema:Organization"],
         "schema:alternateName": "Yale University", "schema:foundingDate": "1701",
         "owl:sameAs": "https://ror.org/03v76x132"}


def doc(*nodes):
    return {"@id": "ISNI JSON-LD", "@context": CONTEXT, "@graph": list(nodes)}


def array_dump(docs):
    """The array form, with the control characters the dump has."""
    text = "[" + ",\n".join(json.dumps(d, ensure_ascii=False) for d in docs) + "]"
    return text.replace('"@graph"', '\x1e"@graph"').replace("Yale", "Ya\x1fle").encode("utf-8")


def seq_dump(docs):
    """The record-separator form (application/json-seq)."""
    return b"".join(b"\x1e" + json.dumps(d, ensure_ascii=False).encode("utf-8") + b"\n" for d in docs)


# --- the rewrite ---------------------------------------------------------------


def test_isni_number():
    assert D.isni_number("https://isni.org/isni/0000000121032683") == "0000000121032683"
    assert D.isni_number("http://isni.org/isni/000000012150090x/") == "000000012150090X"
    assert D.isni_number("http://viaf.org/viaf/50566653") is None
    assert D.isni_number("https://isni.org/isni/123") is None
    assert D.isni_number(None) is None


@pytest.mark.parametrize("form", [array_dump, seq_dump])
@pytest.mark.parametrize("read_bytes", [5, 64, D.READ_BYTES])
def test_documents_come_out_whatever_the_form_and_wherever_a_read_ends(form, read_bytes):
    docs = [doc(PERSON), doc(GROUP), doc()]
    got = list(D.iter_documents(io.BytesIO(form(docs)), read_bytes=read_bytes))
    assert [d["@graph"] for d in got] == [[PERSON], [GROUP], []]


def test_an_unparsable_document_says_where():
    with pytest.raises(ValueError, match="unparsable ISNI document before byte"):
        list(D.iter_documents(io.BytesIO(b'[{"@id": "x"}, {"@id": ]'), read_bytes=4))


def test_flatten_writes_one_line_per_isni_node(tmp_path):
    other = {"@id": "_:b0", "schema:name": "a blank node"}
    src = tmp_path / "data.jsonld"
    src.write_bytes(array_dump([doc(PERSON, other), doc(GROUP)]))
    seen = []
    stats = D.flatten(str(src), str(tmp_path / "out.jsonl.gz"), progress=seen.append)
    assert stats == {"documents": 2, "records": 2, "skipped": 1}
    assert sum(seen) == src.stat().st_size
    lines = gzip.open(tmp_path / "out.jsonl.gz", "rt", encoding="utf-8").read().splitlines()
    assert [ln.split("\t", 1)[0] for ln in lines] == ["0000000121032683", "000000012150090X"]
    assert json.loads(lines[0].split("\t", 1)[1]) == PERSON
    assert json.loads(lines[1].split("\t", 1)[1])["schema:alternateName"] == "Yale University"
    assert not (tmp_path / "out.jsonl.gz.part").exists()


def test_a_failed_flatten_leaves_nothing(tmp_path):
    src = tmp_path / "data.jsonld"
    src.write_bytes(b'[{"@id": "ISNI JSON-LD", "@graph": [')
    with pytest.raises(ValueError):
        D.flatten(str(src), str(tmp_path / "out.jsonl.gz"))
    assert not (tmp_path / "out.jsonl.gz").exists() and not (tmp_path / "out.jsonl.gz.part").exists()


# --- the downloader ------------------------------------------------------------


def downloader(tmp_path, entries):
    return D.IsniDownloader({"name": "isni", "dumps_dir": "isni",
                             "all_configs": SimpleNamespace(dumps_dir=str(tmp_path)),
                             "input_files": {"records": entries}})


def test_both_dumps_get_their_own_file(tmp_path):
    dl = downloader(tmp_path, [{"url": "https://isni.org/isni/data-person/data.jsonld"},
                               {"url": "https://isni.org/isni/data-organization/data.jsonld",
                                "path": "orgs.jsonl.gz"},
                               {"path": "local.jsonl.gz"}])
    assert dl.get_urls() == [
        {"url": "https://isni.org/isni/data-person/data.jsonld",
         "path": str(tmp_path / "isni" / "isni-person.jsonl.gz")},
        {"url": "https://isni.org/isni/data-organization/data.jsonld",
         "path": str(tmp_path / "isni" / "orgs.jsonl.gz")}]
    assert dl.get_urls("export") == []


def test_process_downloads_rewrites_and_removes_the_original(tmp_path, monkeypatch):
    fetched = []

    def fake_fetch(self, download, overwrite=True):
        fetched.append(download["path"])
        Path(download["path"]).parent.mkdir(parents=True, exist_ok=True)
        Path(download["path"]).write_bytes(seq_dump([doc(PERSON), doc(GROUP)]))
        return 1
    monkeypatch.setattr(BaseDownloader, "process", fake_fetch)
    dl = downloader(tmp_path, [{"url": "https://isni.org/isni/data-person/data.jsonld"}])
    (entry,) = dl.get_urls()
    assert dl.process(entry) == 1
    assert fetched == [entry["path"] + ".jsonld"]
    assert not Path(entry["path"] + ".jsonld").exists()
    with gzip.open(entry["path"], "rt") as fh:
        assert len(fh.read().splitlines()) == 2
    # an existing file is kept when told not to overwrite
    assert dl.process(entry, overwrite=False) == 0
    assert len(fetched) == 1


def test_the_rewritten_file_loads_as_tabjson(tmp_path):
    src = tmp_path / "data.jsonld"
    src.write_bytes(array_dump([doc(PERSON), doc(GROUP)]))
    out = tmp_path / "isni-person.jsonl.gz"
    D.flatten(str(src), str(out))
    ldr = Loader({"name": "isni", "datacache": {},
                  "all_configs": SimpleNamespace(dumps_dir=str(tmp_path), temp_dir=str(tmp_path)),
                  "input_files": {"records": [{"path": str(out), "type": "lines.gz/tabjson"}]}})
    ldr.prepare(0, 1)
    ldr.process()
    assert ldr.out_cache == {"0000000121032683": PERSON, "000000012150090X":
                             {**GROUP, "schema:alternateName": "Yale University"}}


# --- the mapper ----------------------------------------------------------------


@pytest.fixture
def mapper(tmp_path):
    cfgs = SimpleNamespace(internal={}, external={}, results={"merged": {}}, data_dir=str(tmp_path),
                           globals={}, get_idmap=lambda: {})
    return IsniMapper({"name": "isni", "all_configs": cfgs, "namespace": "http://isni.org/isni/"})


def ids(things):
    return [t.get("id") for t in things]


def test_a_person(mapper):
    out = mapper.transform({"identifier": "0000000121032683", "data": PERSON}, "Person")
    assert out["identifier"] == "0000000121032683" and out["source"] == "isni"
    rec = out["data"]
    assert rec["type"] == "Person" and rec["id"] == "http://isni.org/isni/0000000121032683"
    assert rec["_label"] == "Twain, Mark"
    names = [i for i in rec["identified_by"] if i["type"] == "Name"]
    assert [n["content"] for n in names] == ["Twain, Mark", "Марк Твен"]     # the repeat dropped
    assert names[0]["classified_as"][0]["_label"].startswith("Primary")
    assert any(lang.get("_label", "").lower().startswith("russian") for lang in names[1].get("language", []))
    number = [i for i in rec["identified_by"] if i["type"] == "Identifier"][0]
    assert number["content"] == "0000 0001 2103 2683"
    assert ids(rec["equivalent"]) == ["http://viaf.org/viaf/50566653", "http://www.wikidata.org/entity/Q7245",
                                      "http://id.loc.gov/authorities/names/n79021164"]
    born = rec["born"]["timespan"]
    assert born["begin_of_the_begin"].startswith("1835-11-30") and born["identified_by"][0]["content"] == "1835-11-30"
    assert rec["died"]["timespan"]["end_of_the_end"].startswith("1910-12-31")


def test_a_group(mapper):
    rec = mapper.transform({"identifier": "000000012150090X", "data": GROUP})["data"]
    assert rec["type"] == "Group" and rec["_label"] == "Yale University"
    assert ids(rec["equivalent"]) == ["https://ror.org/03v76x132"]
    assert rec["formed_by"]["timespan"]["begin_of_the_begin"].startswith("1701-01-01")
    assert "born" not in rec


def test_guess_type_returns_the_class(mapper):
    from cromulent import model
    assert mapper.guess_type(PERSON) is model.Person
    assert mapper.guess_type(GROUP) is model.Group
    assert mapper.guess_type({"@type": "schema:Thing"}) is model.Person


def test_the_wrong_type_or_no_isni_maps_to_nothing(mapper):
    from cromulent import model
    assert mapper.transform({"identifier": "x", "data": PERSON}, "Group") is None
    assert mapper.transform({"identifier": "x", "data": PERSON}, model.Group) is None
    assert mapper.transform({"identifier": "x", "data": {"@id": "_:b1"}}) is None


def test_an_unparsable_date_keeps_its_text(mapper):
    rec = mapper.transform({"data": {**PERSON, "schema:birthDate": "flourished long ago"}})["data"]
    ts = rec["born"]["timespan"]
    assert "begin_of_the_begin" not in ts and ts["identified_by"][0]["content"] == "flourished long ago"


def test_helpers():
    assert format_isni("0000000121032683") == "0000 0001 2103 2683"
    assert format_isni("123") == "123"
    assert literals([" a ", {"@value": "b", "@language": "FR"}, {"@id": "x"}, 1835, ""]) == [
        ("a", None), ("b", "fr"), ("1835", None)]


def test_the_classes_resolve():
    assert importObject("sources.isni.downloader.IsniDownloader") is D.IsniDownloader
    assert importObject("sources.isni.mapper.IsniMapper") is IsniMapper
