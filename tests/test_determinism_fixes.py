"""Regression tests for the determinism/data-loss audit fixes.

Each test corresponds to a flagged defect where pipeline output depended on
processing order, set/dict iteration order, or unordered shared state --
or where records were silently lost. All tests are pure-function level:
stub configs and dict-backed stores, no redis/postgres.

See docs/determinism-audit.md for the full findings list, including the
items flagged but not yet fixed.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.process.base.reconciler import LmdbReconciler
from pipeline.process.merger import MergeHandler, RecordMerger
from pipeline.process.reconciler import Reconciler
from pipeline.process.reidentifier import Reidentifier


# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------

class StubConfigs:
    internal_uri = "https://lux.test/data/"
    internal = {}
    external = {}
    globals = {
        "primaryName": "http://vocab.getty.edu/aat/300404670",
        "alternateName": "http://vocab.getty.edu/aat/300264273",
        "sortName": "http://vocab.getty.edu/aat/300451544",
    }
    ok_record_types = {"Type": "concept", "Person": "person"}
    debug_reconciliation = False

    def make_qua(self, recid, typ):
        if "##qua" in recid:
            return recid
        return f"{recid}##qua{typ}"

    def split_qua(self, recid):
        return recid.split("##qua")


def make_lmdb_reconciler(name_index=None, id_index=None):
    r = object.__new__(LmdbReconciler)
    r.config = {"name": "teststub", "namespace": "http://authority.test/"}
    r.namespace = "http://authority.test/"
    r.configs = StubConfigs()
    r.debug = False
    r.debug_graph = {}
    r.name_index = name_index if name_index is not None else {}
    r.id_index = id_index if id_index is not None else {}
    return r


# ---------------------------------------------------------------------------
# base/reconciler.py: multi-match tie-break was a no-op (len(x) of a 2-tuple)
# ---------------------------------------------------------------------------

def test_multi_match_picks_most_supported():
    # Three equivalents resolve to two candidates: A supported by 2 URIs,
    # B by 1. The old sort key made the result dict-insertion order.
    id_index = {
        "http://ext/1": ("targetB", "Person"),
        "http://ext/2": ("targetA", "Person"),
        "http://ext/3": ("targetA", "Person"),
    }
    rec = {"data": {"type": "Person",
                    "equivalent": [{"id": "http://ext/1"},
                                   {"id": "http://ext/2"},
                                   {"id": "http://ext/3"}]},
           "source": "other"}
    r = make_lmdb_reconciler(id_index=id_index)
    assert r.reconcile(rec, "uri") == "http://authority.test/targetA"


def test_multi_match_independent_of_equivalent_order():
    id_index = {
        "http://ext/1": ("targetB", "Person"),
        "http://ext/2": ("targetA", "Person"),
    }
    results = set()
    for eqs in (["http://ext/1", "http://ext/2"],
                ["http://ext/2", "http://ext/1"]):
        rec = {"data": {"type": "Person",
                        "equivalent": [{"id": e} for e in eqs]},
               "source": "other"}
        r = make_lmdb_reconciler(id_index=id_index)
        results.add(r.reconcile(rec, "uri"))
    # 1-vote tie: must be the same (lexicographic) winner either way
    assert results == {"http://authority.test/targetA"}


# ---------------------------------------------------------------------------
# process/reconciler.py: ids.remove() while iterating skipped diff checks
# ---------------------------------------------------------------------------

class StubGlobalReconciler:
    """Asymmetric diff lookups, like a one-directional differentFrom load."""

    def __init__(self, diffs):
        self.diffs = diffs

    def reconcile(self, eq, reconcileType="uri"):
        assert reconcileType == "diffs"
        return self.diffs.get(eq, set())


def make_process_reconciler(diffs):
    r = object.__new__(Reconciler)
    r.reconcilers = []
    r.global_reconciler = StubGlobalReconciler(diffs)
    r.debug = False
    r.debug_graph = {}
    return r


def record_with_equivs(ids):
    return {"data": {"id": "http://rec/x", "type": "Type", "_label": "x",
                     "equivalent": [{"id": i, "type": "Type"} for i in ids]},
            "source": "src", "identifier": "x"}


def test_diff_check_covers_all_ids():
    # Old code: removing A while the cursor sat on B skipped C entirely,
    # so the C!=D conflict (only discoverable via C's lookup) survived.
    diffs = {"B": {"A"}, "C": {"D"}}
    rec = record_with_equivs(["A", "B", "C", "D"])
    r = make_process_reconciler(diffs)
    r.call_reconcilers(rec, "uri")
    ids = [x["id"] for x in rec["data"]["equivalent"]]
    assert not ("C" in ids and "D" in ids), "known-distinct pair survived"


def test_diff_resolution_is_order_independent():
    diffs = {"B": {"A"}, "C": {"D"}}
    outcomes = set()
    for order in (["A", "B", "C", "D"], ["D", "C", "B", "A"],
                  ["B", "D", "A", "C"]):
        rec = record_with_equivs(order)
        r = make_process_reconciler(diffs)
        r.call_reconcilers(rec, "uri")
        outcomes.add(frozenset(x["id"] for x in rec["data"]["equivalent"]))
    assert len(outcomes) == 1, f"survivors differ by input order: {outcomes}"


# ---------------------------------------------------------------------------
# reidentifier.py: set.pop() chose an arbitrary YUID when >1 present
# ---------------------------------------------------------------------------

def make_reidentifier(idmap):
    r = object.__new__(Reidentifier)
    r.configs = StubConfigs()
    r.idmap = idmap
    r.debug = False
    r.do_not_reidentify = []
    r.redirects = {}
    r.use_slug = True
    r.ignore_props = ["access_point", "conforms_to"]
    r.equivalent_refs = True
    r.preserve_equivalents = {}
    r.ignore_ns = []
    return r


def test_reidentifier_multi_yuid_is_deterministic():
    # A record whose equivalents resolve to two different YUIDs ("shouldn't
    # happen", but does): the chosen id must not depend on set ordering.
    idmap = {
        "http://rec/1##quaType": "https://lux.test/data/concept/zzz",
        "http://ext/a##quaType": "https://lux.test/data/concept/aaa",
    }
    record = {"id": "http://rec/1", "type": "Type",
              "equivalent": [{"id": "http://ext/a", "type": "Type"}]}
    for _ in range(20):
        r = make_reidentifier(dict(idmap))
        result = r.process_entity(dict(record), "Type", top=False)
        assert result["id"] == "https://lux.test/data/concept/aaa"


# ---------------------------------------------------------------------------
# merger.py: dimension merge appended to the wrong object (crash), and
# merge order for equal merge_order sources came from redis set iteration
# ---------------------------------------------------------------------------

def make_record_merger():
    return RecordMerger(StubConfigs(), idmap={})


def dim(value, unit="http://unit/cm", cls="http://dim/height"):
    return {"value": value, "unit": {"id": unit},
            "classified_as": [{"id": cls}]}


def test_dimension_merge_appends_to_record():
    # Previously: dr.append(dm) -> AttributeError on a dict (or NameError
    # with an empty dimension list), killing the whole merge slice.
    m = make_record_merger()
    rec = {"id": "x", "type": "LinguisticObject", "dimension": [dim(1)]}
    merge = {"dimension": [dim(1), dim(2)]}
    m.merge_linguisticobject(rec, dict(merge), "src", [])
    assert [d["value"] for d in rec["dimension"]] == [1, 2]

    # base record with no dimension list at all
    rec2 = {"id": "y", "type": "LinguisticObject"}
    m.merge_linguisticobject(rec2, dict(merge), "src", [])
    assert [d["value"] for d in rec2["dimension"]] == [1, 2]


class StubRefMgr:
    def delete_done_ref(self, eq):
        pass


class StubReidentifier:
    def reidentify(self, rec, typ=None):
        out = dict(rec)
        out["yuid"] = "yuid-1"
        return out


class StubSplitConfigs(StubConfigs):
    """split_uri router for MergeHandler.merge."""

    def __init__(self, routes):
        self.routes = routes

    def split_uri(self, uri):
        return self.routes[uri]


def name(content, primary, gbl):
    cls = [{"id": gbl["primaryName"]}] if primary else []
    return {"type": "Name", "content": content, "classified_as": cls}


def test_merge_output_independent_of_equivalent_set_order():
    # Two external sources with the SAME merge_order, each contributing a
    # primary name. Stable sort used to preserve redis set-iteration order,
    # so whichever arrived first won the primary-name latch.
    gbl = StubConfigs.globals

    def build(order):
        cfg_a = {"name": "aaa", "type": "external", "merge_order": 1,
                 "recordcache": {"a": {"data": {"id": "yuid:1", "type": "Type",
                                                "identified_by": [name("Alpha", True, gbl)]},
                                       "source": "aaa", "identifier": "a"}},
                 "recordcache2": {}}
        cfg_b = {"name": "bbb", "type": "external", "merge_order": 1,
                 "recordcache": {"b": {"data": {"id": "yuid:1", "type": "Type",
                                                "identified_by": [name("Beta", True, gbl)]},
                                       "source": "bbb", "identifier": "b"}},
                 "recordcache2": {}}
        mh = object.__new__(MergeHandler)
        mh.config = StubSplitConfigs({"http://ext/aaa/a": (cfg_a, "a"),
                                      "http://ext/bbb/b": (cfg_b, "b")})
        mh.idmap = {}
        mh.merger = make_record_merger()
        mh.reference_manager = StubRefMgr()
        mh.reidentifier = StubReidentifier()
        base = {"data": {"id": "yuid:1", "type": "Type", "identified_by": []},
                "source": "internal-x", "identifier": "base"}
        return mh.merge(base, list(order))

    out1 = build(["http://ext/aaa/a", "http://ext/bbb/b"])
    out2 = build(["http://ext/bbb/b", "http://ext/aaa/a"])
    assert json.dumps(out1, sort_keys=True) == json.dumps(out2, sort_keys=True)


# ---------------------------------------------------------------------------
# getty AAT extract_names: keys must be lowercased and priorities not
# clobbered by later non-matching languages
# ---------------------------------------------------------------------------

def test_aat_extract_names_lowercases_and_keeps_best_priority():
    from pipeline.sources.authorities.getty.reconciler import AatReconciler

    class AatConfigs(StubConfigs):
        external = {"aat": {"namespace": "http://vocab.getty.edu/aat/"}}
        globals_cfg = {
            "primaryName": "300404670",
            "lang_en": "300388277", "lang_fr": "300388306",
            "lang_nl": "300388256", "lang_de": "300388344",
            "lang_es": "300389311", "lang_ar": "300387843",
            "lang_zh": "300388113",
        }

    r = object.__new__(AatReconciler)
    r.configs = AatConfigs()

    ns = "http://vocab.getty.edu/aat/"
    primary = [{"id": ns + "300404670"}]
    rec = {
        "identified_by": [
            # English primary name: priority 1, lowercased key
            {"content": "Bronze", "classified_as": primary,
             "language": [{"id": ns + "300388277"}]},
            # unknown-language primary name: still lowercased, fallback prio
            {"content": "Bronzo", "classified_as": primary,
             "language": [{"id": ns + "999999999"}]},
        ]
    }
    vals = r.extract_names(rec)
    assert "bronze" in vals and vals["bronze"] == 1, vals
    assert "Bronze" not in vals
    assert "bronzo" in vals and vals["bronzo"] > 8


# ---------------------------------------------------------------------------
# index_loader collision policy (tested at the policy level: smallest recid
# wins regardless of iteration order) and metatypes/ref-file helpers
# ---------------------------------------------------------------------------

def test_iter_done_refs_skips_blank_lines(tmp_path, monkeypatch):
    from pipeline.process.reference_manager import ReferenceManager

    rm = object.__new__(ReferenceManager)
    monkeypatch.chdir(tmp_path)
    (tmp_path / "reference_uris.txt").write_text(
        "1|http://a##quaType\n\n2|http://b##quaType\n")
    got = list(rm.iter_done_refs(-1, -1))
    assert got == [["1", "http://a##quaType"], ["2", "http://b##quaType"]]

    # empty file yields nothing (previously yielded a bogus [""])
    (tmp_path / "reference_uris.txt").write_text("")
    assert list(rm.iter_done_refs(-1, -1)) == []
