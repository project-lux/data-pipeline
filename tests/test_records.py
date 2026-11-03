"""Web UI item W05: any reference resolved and followed through every
cache and the identity map (lux_pipeline.records), on the W00
installation with seeded records; nothing stored, nothing fetched."""

import datetime
import logging
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline import records
from lux_pipeline.records import Ref, journey, record_keys, resolve
from webui_fixtures import INTERNAL_URI, make_configs, seed_records

AAT = "http://vocab.getty.edu/aat/"
YCBA = "https://ycba-lux.s3.amazonaws.com/v3/"


@pytest.fixture
def built(tmp_path):
    cfgs = make_configs(tmp_path / "inst", instantiate=True)
    return cfgs, seed_records(cfgs)


@pytest.fixture
def plain(tmp_path):
    """A configuration read but not built: no source has components."""
    return make_configs(tmp_path / "inst")


# --- resolve ---------------------------------------------------------------------------


@pytest.mark.parametrize("text,kind,source,ident", [
    (AAT + "300033618", "uri", "aat", "300033618"),
    ("aat:300033618", "curie", "aat", "300033618"),
    (YCBA + "object/1", "uri", "ycba", "object/1"),
    (YCBA + "object/1/", "uri", "ycba", "object/1"),
    ("ycba:object/1", "curie", "ycba", "object/1"),
])
def test_resolve_uris_and_curies(plain, text, kind, source, ident):
    ref = resolve(plain, text)
    assert (ref.kind, ref.source, ref.identifier) == (kind, source, ident)
    assert ref.group == ("external" if source == "aat" else "internal")
    assert ref.uri == plain.internal.get(source, plain.external.get(source))["namespace"] + ident


def test_resolve_a_typed_uri_keeps_the_type(plain):
    ref = resolve(plain, AAT + "300033618##quaType")
    assert (ref.source, ref.identifier, ref.rectype) == ("aat", "300033618", "Type")
    with pytest.raises(ValueError, match="'Nope' is not a record type"):
        resolve(plain, AAT + "300033618##quaNope")


@pytest.mark.parametrize("text", ["0e59009d-ed65-45f8-be83-46be0984fd12",
                                  "0e59009ded6545f8be8346be0984fd12",
                                  INTERNAL_URI + "concept/0e59009d-ed65-45f8-be83-46be0984fd12",
                                  "merged:0e59009d-ed65-45f8-be83-46be0984fd12",
                                  "yuid:0e59009d-ed65-45f8-be83-46be0984fd12"])
def test_resolve_yuids(plain, text):
    ref = resolve(plain, text)
    assert ref.kind == "yuid" and ref.source is None
    assert ref.uuid.replace("-", "") == "0e59009ded6545f8be8346be0984fd12"


def test_resolve_bare_identifiers_need_a_source(plain):
    ref = resolve(plain, "object/1", source="ycba")
    assert (ref.kind, ref.source, ref.uri) == ("identifier", "ycba", YCBA + "object/1")
    with pytest.raises(ValueError, match="give the source for a bare identifier"):
        resolve(plain, "object/1")
    with pytest.raises(ValueError, match="Unknown source: nope"):
        resolve(plain, "object/1", source="nope")


def test_resolve_refuses_what_it_cannot_place(plain):
    with pytest.raises(ValueError, match="no configured source matches this URI"):
        resolve(plain, "http://nowhere.example/1")
    with pytest.raises(ValueError, match="is not a YUID"):
        resolve(plain, INTERNAL_URI + "concept/not-a-uuid")
    with pytest.raises(ValueError, match="nothing to look up"):
        resolve(plain, "  ")


def test_resolve_on_an_unbuilt_configuration_logs_nothing(plain, caplog):
    with caplog.at_level(logging.DEBUG, logger="lux_pipeline"):
        resolve(plain, YCBA + "object/1.html")
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_record_keys_put_the_likely_form_first(plain):
    ref = Ref(kind="curie", text="x", source="aat", group="external", identifier="1")
    assert record_keys(plain, ref, "external", "Type") == ["1##quaType", "1"]
    assert record_keys(plain, ref, "internal", "Type") == ["1", "1##quaType"]
    every = record_keys(plain, ref, "external")
    assert every[-1] == "1" and "1##quaPerson" in every and "1##quaMaterial" not in every


# --- journey ---------------------------------------------------------------------------------


def test_an_internal_records_journey(built):
    cfgs, seeded = built
    s = seeded["ycba"]
    j = journey(cfgs, resolve(cfgs, s["uri"]))
    assert j["datacache"]["key"] == "object/1" and j["recordcache"]["key"] == "object/1"
    assert j["datacache"]["row"]["data"]["id"] == s["uri"]
    assert j["reconciled"] is None and j["mapped"] is None
    assert j["identity"] == {"yuid": s["yuid"], "uuid": s["uuid"],
                             "key": s["uri"] + "##quaHumanMadeObject",
                             "members": [s["uri"] + "##quaHumanMadeObject"]}
    assert j["merged"]["key"] == s["uuid"] and j["merged"]["row"]["data"]["id"] == s["yuid"]
    assert j["notes"] == [] and j["ref"]["source"] == "ycba"


def test_an_external_record_is_found_under_its_typed_key(built):
    cfgs, seeded = built
    j = journey(cfgs, resolve(cfgs, "aat:300033618"))
    assert j["recordcache"]["key"] == "300033618##quaType"
    assert "recordcache: found under the typed key 300033618##quaType" in j["notes"]
    assert j["identity"]["yuid"] == seeded["aat"]["yuid"]


def test_a_yuid_finds_its_merged_record_and_members(built):
    cfgs, seeded = built
    s = seeded["aat"]
    for text in (s["uuid"], s["yuid"], f"merged:{s['uuid']}"):
        j = journey(cfgs, resolve(cfgs, text))
        assert j["merged"]["key"] == s["uuid"]
        assert j["identity"]["yuid"] == s["yuid"]
        assert j["identity"]["members"] == [s["uri"] + "##quaType"]
        assert j["datacache"] is None and j["recordcache"] is None


def test_a_record_that_is_nowhere(built):
    cfgs, _ = built
    j = journey(cfgs, resolve(cfgs, "aat:999"))
    assert all(j[k] is None for k in ("datacache", "recordcache", "reconciled", "identity",
                                      "merged", "rewritten", "marklogic"))
    assert j["notes"] == ["identity: not in the identity map"]
    j = journey(cfgs, resolve(cfgs, "0e59009d-ed65-45f8-be83-46be0984fd12"))
    assert j["merged"] is None and j["identity"] is None


def test_a_store_that_fails_is_a_note(built):
    cfgs, _ = built

    class Broken:
        def get(self, key):
            raise ConnectionError("server gone")

    cfgs.external["aat"]["datacache"] = Broken()
    j = journey(cfgs, resolve(cfgs, "aat:300033618"))
    assert j["datacache"] is None
    assert "datacache: ConnectionError: server gone" in j["notes"]
    assert j["recordcache"] is not None


def test_mapping_stores_nothing_and_leaves_the_row_alone(built, monkeypatch):
    cfgs, _ = built
    cfg = cfgs.internal["ycba"]
    before = cfg["datacache"]["object/1"]["data"]
    seen = {}

    def transform(row, rectype, reference=False):
        seen["rectype"] = rectype
        row["data"]["_label"] = "mapped"      # a mapper may change what it is given
        return {"data": row["data"], "identifier": row["identifier"]}

    stored = []
    monkeypatch.setattr(cfg["mapper"], "transform", transform)
    monkeypatch.setattr(cfg["recordcache"], "set", lambda *a, **kw: stored.append(a))
    j = journey(cfgs, resolve(cfgs, "ycba:object/1"), mapped=True)
    assert [r["data"]["_label"] for r in j["mapped"]["records"]] == ["mapped"]
    assert seen["rectype"] == "HumanMadeObject"
    assert stored == []
    assert cfg["datacache"]["object/1"]["data"] == before


def test_metadata_and_sample(built):
    cfgs, _ = built
    cache = cfgs.external["aat"]["recordcache"]
    md = records.metadata(cache, "300033618##quaType")
    assert isinstance(md["record_time"], datetime.datetime)
    ended = []
    cache.end_read = lambda: ended.append(True)
    assert records.sample(cache) == ["300033618##quaType"]
    assert records.sample(cache, typ="Type") == ["300033618##quaType"]
    assert records.sample(cache, typ="Person") == []
    assert records.sample(cache, since=datetime.datetime(2999, 1, 1)) == []
    assert records.sample(cache, n=0) == []
    assert len(ended) == 5


def test_sample_leaves_no_iteration_open():
    """A Postgres cache iterates on a second connection; stopping early must
    close the cursor and end that connection's transaction."""
    log = []

    class Conn:
        autocommit = False

        def rollback(self):
            log.append("rollback")

    class Cache:
        iterating_conn = Conn()

        def iter_keys(self):
            try:
                for i in range(100):
                    yield f"k{i}"
            finally:
                log.append("cursor closed")

        def end_read(self):
            log.append("end_read")

    assert records.sample(Cache(), n=3) == ["k0", "k1", "k2"]
    assert log == ["cursor closed", "end_read", "rollback"]


def test_validate(built, monkeypatch):
    cfgs, _ = built
    row = cfgs.internal["ycba"]["recordcache"]["object/1"]
    with pytest.raises(FileNotFoundError, match="core.json"):
        records.validate(cfgs, row)

    class Err:
        absolute_path = ["identified_by", 0]
        message = "is not valid"

    monkeypatch.setattr(cfgs.validator, "validate", lambda rec: [Err()])
    assert records.validate(cfgs, row["data"]) == ["/identified_by/0 --> is not valid"]


def test_instantiate_source_builds_once(plain):
    cfg = records.instantiate_source(plain, "aat")
    cache = cfg["datacache"]
    assert records.instantiate_source(plain, "aat")["datacache"] is cache
    with pytest.raises(ValueError, match="Unknown source: nope"):
        records.instantiate_source(plain, "nope")


def test_identity_graph_is_lux_viz_graph_data(plain, monkeypatch):
    from lux_pipeline.cli import viz
    monkeypatch.setattr(viz, "graph_data", lambda cfgs, a, b: {"from": a, "to": b})
    assert records.identity_graph(plain, "a", "b") == {"from": "a", "to": "b"}
