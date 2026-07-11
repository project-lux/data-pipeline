"""Unit tests for pipeline.process.identity (deterministic identity
resolution). Pure-function coverage: clustering semantics, differentFrom
constraints, voted conflict resolution, YUID assignment/reuse, and the
assertion log round-trip. No redis required."""

import random
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.process.identity import (AssertionWriter, assign, cluster,
                                       load_assertions, mint_yuid)


class StubConfigs:
    internal_uri = "https://lux.collections.yale.edu/data/"
    ok_record_types = {"Type": "concept", "Person": "person",
                       "Material": "concept"}
    parent_record_types = {"Material": "Type"}

    def is_qua(self, recid):
        return "##qua" in recid

    def make_qua(self, recid, typ):
        if "##qua" in recid:
            return recid
        typ = self.parent_record_types.get(typ, typ)
        return f"{recid}##qua{typ}"

    def split_qua(self, recid):
        return recid.split("##qua")


CFGS = StubConfigs()


def edges_from(assertions):
    edges = defaultdict(set)
    for asserter, a, b in assertions:
        if a > b:
            a, b = b, a
        edges[(a, b)].add(asserter)
    return edges


def roots_of(clusters):
    return {m: k for k, ms in clusters.items() for m in ms}


BASE = [
    ("1", "1", "A"), ("1", "1", "B"), ("1", "A", "B"),
    ("2", "2", "C"), ("2", "2", "D"), ("2", "C", "D"),
    ("3", "3", "B"), ("3", "3", "C"), ("3", "B", "C"),
]


def test_diff_keeps_clusters_separate():
    clusters, conflicts = cluster(edges_from(BASE), {("B", "C")})
    roots = roots_of(clusters)
    assert roots["B"] != roots["C"]
    # the disputed record is still assigned to one side, not dropped
    assert roots["3"] in (roots["B"], roots["C"])
    # refused link is reported with its asserters
    assert [c for c in conflicts if c["pair"] == ["B", "C"]]
    assert all(c["asserters"] for c in conflicts)


def test_voting_moves_disputed_record():
    # A second, independent assertion of 3=C outweighs the single 3=B
    clusters, _ = cluster(edges_from(BASE + [("4", "3", "C")]), {("B", "C")})
    roots = roots_of(clusters)
    assert roots["3"] == roots["C"]
    assert roots["3"] != roots["B"]


def test_diff_applies_cluster_wide():
    # chain A~B, B~C, C~D with diff(A,D): whichever link would connect
    # A and D transitively is refused, regardless of who asserted it
    clusters, conflicts = cluster(edges_from([
        ("1", "A", "B"), ("2", "C", "D"), ("3", "B", "C"),
    ]), {("A", "D")})
    roots = roots_of(clusters)
    assert roots["A"] != roots["D"]
    assert len(conflicts) == 1


def test_deterministic_under_input_order():
    diffs = {("B", "C")}
    expected = None
    for seed in range(5):
        shuffled = BASE.copy()
        random.Random(seed).shuffle(shuffled)
        clusters, conflicts = cluster(edges_from(shuffled), diffs)
        result = (sorted(map(tuple, clusters.values())),
                  [tuple(c["pair"]) for c in conflicts])
        if expected is None:
            expected = result
        assert result == expected


def test_prior_yuids_reused():
    prior = {"A": "yuid-1", "B": "yuid-1", "C": "yuid-2", "D": "yuid-2"}
    clusters, _ = cluster(edges_from(BASE), {("B", "C")})
    yuids = assign(clusters, prior, CFGS)
    roots = roots_of(clusters)
    assert yuids[roots["A"]] == "yuid-1"
    assert yuids[roots["C"]] == "yuid-2"


def test_new_uri_does_not_change_yuid():
    prior = {"A": "yuid-1", "B": "yuid-1"}
    clusters, _ = cluster(edges_from(BASE + [("1", "A", "NEW")]), {("B", "C")})
    yuids = assign(clusters, prior, CFGS)
    assert yuids[roots_of(clusters)["NEW"]] == "yuid-1"


def test_split_majority_keeps_minority_mints():
    split = [("1", "A", "B"), ("1", "B", "E"), ("2", "C", "C")]
    prior = {"A": "yuid-9", "B": "yuid-9", "E": "yuid-9", "C": "yuid-9"}
    clusters, _ = cluster(edges_from(split), set())
    yuids = assign(clusters, prior, CFGS)
    roots = roots_of(clusters)
    assert yuids[roots["A"]] == "yuid-9"
    minted = yuids[roots["C"]]
    assert minted != "yuid-9"
    assert minted.startswith(CFGS.internal_uri)


def test_mint_is_deterministic_and_slugged():
    key = "http://vocab.getty.edu/aat/300055647##quaType"
    y1 = mint_yuid(CFGS, key)
    y2 = mint_yuid(CFGS, key)
    assert y1 == y2
    assert y1.startswith(CFGS.internal_uri + "concept/")
    assert mint_yuid(CFGS, "http://example.org/p/1##quaPerson") != y1


def test_assertion_writer_round_trip(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    w = AssertionWriter(CFGS, 3)
    w.write_record({"data": {
        "id": "http://example.org/rec/1", "type": "Material",
        "equivalent": [{"id": "http://vocab.getty.edu/aat/1", "type": "Type"},
                       {"id": "http://www.wikidata.org/entity/Q1", "type": "Type"}],
    }})
    # no equivalents -> self assertion so the record still gets a YUID
    w.write_record({"data": {"id": "http://example.org/rec/2", "type": "Person"}})
    w.close()

    edges = load_assertions([tmp_path / "assertions-3.tsv"])
    rec1 = "http://example.org/rec/1##quaType"  # Material quas to Type
    pairs = set(edges)
    assert (sorted([rec1, "http://vocab.getty.edu/aat/1##quaType"])[0],
            sorted([rec1, "http://vocab.getty.edu/aat/1##quaType"])[1]) in pairs
    self_pair = ("http://example.org/rec/2##quaPerson",
                 "http://example.org/rec/2##quaPerson")
    assert self_pair in pairs
    # self assertions register the node but never merge anything
    clusters, _ = cluster(edges, set())
    assert len(clusters) == 2
