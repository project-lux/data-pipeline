"""The last three item 29 loaders: bne (JSON-LD zips), bnf (RDF/XML
tarballs grouped per entity) and pleiades (a JSON-LD @graph of places
and a Turtle vocabulary), over dumps built in the test."""

import gzip
import io
import json
import sys
import tarfile
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest
from lxml import etree

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.config import importObject
from lux_pipeline.process.base.loader import CONTAINERS, Loader
from lux_pipeline.sources.bne.loader import BneLoader
from lux_pipeline.sources.bnf.loader import BnfLoader
from lux_pipeline.sources.pleiades.loader import PleiadesLoader

ROOT = Path(__file__).parent.parent


def make(cls, tmp_path, name, records, **extra):
    cfg = {"name": name, "datacache": {}, "dumps_dir": name,
           "all_configs": SimpleNamespace(dumps_dir=str(tmp_path), temp_dir=str(tmp_path)),
           "input_files": {"records": records}}
    cfg.update(extra)
    ldr = cls(cfg)
    ldr.prepare(0, 1)
    assert ldr.uses_dsl is True
    return ldr


# --- bne ---------------------------------------------------------------------

BNE = "https://datos.bne.es/resource/"


def test_bne_arraylines_and_array_files_keyed_by_id_variants(tmp_path):
    d = tmp_path / "bne"
    d.mkdir()
    persons = [{"@id": f"{BNE}XX1", "@type": "https://datos.bne.es/def/C1005", "label": "Uno"},
               {"id": f"{BNE}XX2", "label": "Dos"},
               {"idBNE": "XX3", "label": "Tres"},
               {"label": "sin id"}]
    with zipfile.ZipFile(d / "persona-JSON.zip", "w") as zh:
        zh.writestr("persona.json", "[\n" + ",\n".join(json.dumps(p) for p in persons) + "\n]\n")
    with zipfile.ZipFile(d / "materia-JSON.zip", "w") as zh:
        zh.writestr("materia.json", json.dumps([{"@id": f"{BNE}XX9", "@type": "http://www.w3.org/2004/02/skos/core#Concept"}]))
    ldr = make(BneLoader, tmp_path, "bne", [{"path": "persona-JSON.zip", "type": "zip/arraylines/json"},
                                           {"path": "materia-JSON.zip", "type": "zip/array/raw"}])
    with pytest.raises(RuntimeError):
        # the record without any id cannot be keyed: the file is reported failed...
        ldr.process()
    assert set(ldr.out_cache) >= {"XX1", "XX2", "XX3"}            # ...after the others were stored
    assert ldr.out_cache["XX1"]["label"] == "Uno"


def test_bne_extract_identifier_order():
    ldr = BneLoader({"name": "bne", "datacache": {}})
    assert ldr.extract_identifier({"id": f"{BNE}A", "@id": f"{BNE}B", "idBNE": "C"}) == "A"
    assert ldr.extract_identifier({"@id": f"{BNE}B", "idBNE": "C"}) == "B"
    assert ldr.extract_identifier({"idBNE": "C"}) == "C"
    assert ldr.extract_identifier({}) is None and ldr.extract_identifier("x") is None


# --- bnf ---------------------------------------------------------------------

RDF = """<?xml version="1.0" encoding="utf-8"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:foaf="http://xmlns.com/foaf/0.1/" xmlns:skos="http://www.w3.org/2004/02/skos/core#">
  <rdf:Description rdf:about="http://data.bnf.fr/ark:/12148/cb111#about"><foaf:name>Alpha</foaf:name></rdf:Description>
  <rdf:Description rdf:about="http://data.bnf.fr/ark:/12148/cb111"><skos:prefLabel>Alpha, A.</skos:prefLabel></rdf:Description>
  <rdf:Description rdf:about="http://data.bnf.fr/ark:/12148/cb222#about"><foaf:name>Beta</foaf:name></rdf:Description>
  <rdf:Description rdf:about="http://data.bnf.fr/pages/not-an-ark"><foaf:name>page</foaf:name></rdf:Description>
  <rdf:Description rdf:about="http://data.bnf.fr/ark:/12148/cb222"><skos:prefLabel>Beta, B.</skos:prefLabel></rdf:Description>
</rdf:RDF>
"""


def test_bnf_groups_descriptions_per_entity_as_the_mapper_expects(tmp_path):
    d = tmp_path / "bnf"
    d.mkdir()
    with tarfile.open(d / "1.tar.gz", "w:gz") as tf:
        data = RDF.encode()
        info = tarfile.TarInfo("databnf_person_authors__rdf_1.xml")
        info.size = len(data)
        tf.addfile(info, io.BytesIO(data))
    ldr = make(BnfLoader, tmp_path, "bnf", [{"path": "1.tar.gz", "type": "tar.gz/entities/raw"}])
    assert ldr.process() == 2
    assert set(ldr.out_cache) == {"cb111", "cb222"}
    xml = ldr.out_cache["cb111"]["xml"]
    dom = etree.XML(xml.encode())
    nss = {"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"}
    # both nodes of the entity, under https, in one rdf:RDF document: what BnfXmlMapper.transform xpaths for
    assert dom.xpath("./rdf:Description[@rdf:about='https://data.bnf.fr/ark:/12148/cb111#about']", namespaces=nss)
    assert dom.xpath("./rdf:Description[@rdf:about='https://data.bnf.fr/ark:/12148/cb111']", namespaces=nss)
    assert "cb222" not in xml and "not-an-ark" not in xml


def test_bnf_extract_identifier_and_bad_xml(tmp_path, caplog):
    ldr = BnfLoader({"name": "bnf", "datacache": {}})
    assert ldr.extract_identifier({"xml": '<rdf:RDF><rdf:Description rdf:about="https://data.bnf.fr/ark:/12148/cb9#about"/></rdf:RDF>'}) == "cb9"
    assert ldr.extract_identifier({"xml": "<x/>"}) is None
    bad = tmp_path / "bad.xml"
    bad.write_text("<rdf:RDF")
    with caplog.at_level("ERROR", logger="lux_pipeline"):
        assert list(ldr.iterate_entities(str(bad), None, ["raw"])) == []
    assert "Error parsing XML" in caplog.text


# --- pleiades ----------------------------------------------------------------

TTL = """# Pleiades RDF Dump
# Contents: Pleiades Vocabulary 'place-types'

@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .

<https://pleiades.stoa.org/vocabularies/abbey> a <http://www.w3.org/2004/02/skos/core#Concept>;
    owl:sameAs <http://pleiades.stoa.org/vocabularies/abbey>,
        <http://vocab.getty.edu/aat/300000642>;
    skos:prefLabel "abbey (monastery)"@en;
    skos:scopeNote "An abbey."@en .

<https://pleiades.stoa.org/vocabularies/abbey-church> a <http://www.w3.org/2004/02/skos/core#Concept>;
    skos:prefLabel "abbey church"@en .
"""

PLACES = {"@context": {"uri": "@id"}, "@graph": [
    {"id": "48210385", "uri": "https://pleiades.stoa.org/places/48210385", "title": "Alba Fucens", "features": []},
    {"uri": "https://pleiades.stoa.org/places/579885", "title": "Athenae"},
]}


def test_pleiades_types_and_places(tmp_path):
    d = tmp_path / "pleiades"
    d.mkdir()
    (d / "place-types.ttl").write_text(TTL)
    with gzip.open(d / "pleiades-places-latest.json.gz", "wt") as fh:
        json.dump(PLACES, fh)
    ldr = make(PleiadesLoader, tmp_path, "pleiades",
               [{"path": "place-types.ttl", "type": "ttl/other"},
                {"path": "pleiades-places-latest.json.gz", "type": "graph.gz/raw"}])
    assert ldr.process() == 4
    assert set(ldr.out_cache) == {"abbey", "abbey-church", "48210385", "579885"}
    block = ldr.out_cache["abbey"]["data"]
    assert block.startswith("<https://pleiades.stoa.org/vocabularies/abbey>") and block.rstrip().endswith(".")
    assert 'skos:scopeNote "An abbey."@en .' in block and "abbey-church" not in block
    assert ldr.out_cache["579885"]["title"] == "Athenae"


def test_graph_is_a_base_container():
    assert "graph" in CONTAINERS
    ldr = Loader({"name": "g", "datacache": {}}).prepare(0, 1)
    assert ldr.process_fmt("graph.gz/raw") == [["graph", "gz"], ["raw"]]
    assert list(ldr.iterate_graph(io.BytesIO(b'{"@graph": [1, 2]}'), None, ["raw"])) == [1, 2]
    assert list(ldr.iterate_graph(io.BytesIO(b'[1]'), None, ["raw"])) == []


@pytest.mark.parametrize("cls", [BneLoader, BnfLoader, PleiadesLoader])
def test_no_legacy_load(cls):
    assert cls.load is Loader.load


@pytest.mark.parametrize("sample", ["bne.json", "bnf.json", "pleiades.json"])
def test_samples_parse_and_resolve(sample):
    d = json.load(open(ROOT / "docs" / "sample_config" / sample))
    assert d["loaderClass"].endswith("Loader")
    for k, v in d.items():
        if k.endswith("Class") and v:
            assert importObject(v) is not None
    ldr = importObject(d["loaderClass"])({"name": d["name"], "datacache": {}})
    for e in d["input_files"]["records"]:
        assert ldr.process_fmt(e["type"])
