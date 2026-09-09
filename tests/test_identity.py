"""Unit tests for pipeline.process.identity_resolver (deterministic identity
resolution). Pure-function coverage: clustering semantics, differentFrom
constraints, voted conflict resolution, YUID assignment/reuse, and the
assertion log round-trip. No redis or postgres required.

Ported from the module-level API of the old `pipeline.process.identity`,
which no longer exists -- everything here is now a method on
`IdentityResolver`, and the prefix maps it uses are built by the idmap rather
than by a `build_prefix_maps()` helper. The assertions are unchanged in
substance; only the call shape moved.
"""

import random
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.process.identity_resolver import IdentityResolver


class StubConfigs:
    internal_uri = "https://lux.collections.yale.edu/data/"
    ok_record_types = {"Type": "concept", "Person": "person",
                       "Material": "concept"}
    parent_record_types = {"Material": "Type"}
    external = {
        "aat": {"name": "aat", "namespace": "http://vocab.getty.edu/aat/"},
        "wikidata": {"name": "wikidata",
                     "namespace": "http://www.wikidata.org/entity/"},
    }

    def __init__(self, temp_dir="."):
        self.temp_dir = str(temp_dir)

    def is_qua(self, recid):
        return "##qua" in recid

    def make_qua(self, recid, typ):
        if "##qua" in recid:
            return recid
        typ = self.parent_record_types.get(typ, typ)
        return f"{recid}##qua{typ}"

    def split_qua(self, recid):
        return recid.split("##qua")


class StubIdmap:
    """Only the two prefix maps are read off the idmap by the resolver.

    Built exactly as `storage.idmap.postgres.IdMap` builds them, which is
    where `build_prefix_maps()` went when it stopped being a free function."""

    def __init__(self, cfgs):
        self.prefix_map_out = {"yuid": cfgs.internal_uri}
        for cf in cfgs.external.values():
            self.prefix_map_out[cf["name"]] = cf["namespace"]
        self.prefix_map_in = {v: k for (k, v) in self.prefix_map_out.items()}


CFGS = StubConfigs()


def resolver(cfgs=None):
    """A resolver usable for the pure parts, with no file side effects.

    The constructor no longer opens the assertion log -- that happens on the
    first write -- so building one of these creates nothing on disk."""
    cfgs = cfgs or CFGS
    return IdentityResolver(cfgs, StubIdmap(cfgs))


R = resolver()


def q(name):
    """Production cluster keys are qua'd curies, and mint_yuid needs the qua
    to find a type slug (it raises without one), so the fixtures carry one."""
    return f"{name}##quaPerson"


def edges_from(assertions):
    edges = defaultdict(set)
    for asserter, a, b in assertions:
        a, b = q(a), q(b)
        if a > b:
            a, b = b, a
        edges[(a, b)].add(q(asserter))
    return edges


def roots_of(clusters):
    return {m: k for k, ms in clusters.items() for m in ms}


BASE = [
    ("1", "1", "A"), ("1", "1", "B"), ("1", "A", "B"),
    ("2", "2", "C"), ("2", "2", "D"), ("2", "C", "D"),
    ("3", "3", "B"), ("3", "3", "C"), ("3", "B", "C"),
]

DIFF_BC = {(q("B"), q("C"))}


def test_diff_keeps_clusters_separate():
    clusters, conflicts = R.cluster(edges_from(BASE), DIFF_BC)
    roots = roots_of(clusters)
    assert roots[q("B")] != roots[q("C")]
    # the disputed record is still assigned to one side, not dropped
    assert roots[q("3")] in (roots[q("B")], roots[q("C")])
    # refused link is reported with its asserters
    assert [c for c in conflicts if c["pair"] == [q("B"), q("C")]]
    assert all(c["asserters"] for c in conflicts)


def test_voting_moves_disputed_record():
    # A second, independent assertion of 3=C outweighs the single 3=B
    clusters, _ = R.cluster(edges_from(BASE + [("4", "3", "C")]), DIFF_BC)
    roots = roots_of(clusters)
    assert roots[q("3")] == roots[q("C")]
    assert roots[q("3")] != roots[q("B")]


def test_diff_applies_cluster_wide():
    # chain A~B, B~C, C~D with diff(A,D): whichever link would connect
    # A and D transitively is refused, regardless of who asserted it
    clusters, conflicts = R.cluster(edges_from([
        ("1", "A", "B"), ("2", "C", "D"), ("3", "B", "C"),
    ]), {(q("A"), q("D"))})
    roots = roots_of(clusters)
    assert roots[q("A")] != roots[q("D")]
    assert len(conflicts) == 1


def test_deterministic_under_input_order():
    expected = None
    for seed in range(5):
        shuffled = BASE.copy()
        random.Random(seed).shuffle(shuffled)
        clusters, conflicts = R.cluster(edges_from(shuffled), DIFF_BC)
        result = (sorted(map(tuple, clusters.values())),
                  [tuple(c["pair"]) for c in conflicts])
        if expected is None:
            expected = result
        assert result == expected


def test_prior_yuids_reused():
    prior = {q("A"): "yuid-1", q("B"): "yuid-1",
             q("C"): "yuid-2", q("D"): "yuid-2"}
    clusters, _ = R.cluster(edges_from(BASE), DIFF_BC)
    yuids = R.assign(clusters, prior)
    roots = roots_of(clusters)
    assert yuids[roots[q("A")]] == "yuid-1"
    assert yuids[roots[q("C")]] == "yuid-2"


def test_new_uri_does_not_change_yuid():
    prior = {q("A"): "yuid-1", q("B"): "yuid-1"}
    clusters, _ = R.cluster(edges_from(BASE + [("1", "A", "NEW")]), DIFF_BC)
    yuids = R.assign(clusters, prior)
    assert yuids[roots_of(clusters)[q("NEW")]] == "yuid-1"


def test_split_majority_keeps_minority_mints():
    split = [("1", "A", "B"), ("1", "B", "E"), ("2", "C", "C")]
    prior = {q("A"): "yuid-9", q("B"): "yuid-9",
             q("E"): "yuid-9", q("C"): "yuid-9"}
    clusters, _ = R.cluster(edges_from(split), set())
    yuids = R.assign(clusters, prior)
    roots = roots_of(clusters)
    assert yuids[roots[q("A")]] == "yuid-9"
    minted = yuids[roots[q("C")]]
    assert minted != "yuid-9"
    assert minted.startswith(CFGS.internal_uri)


def test_mint_is_deterministic_and_slugged():
    key = "http://vocab.getty.edu/aat/300055647##quaType"
    y1 = R.mint_yuid(key)
    y2 = R.mint_yuid(key)
    assert y1 == y2
    assert y1.startswith(CFGS.internal_uri + "concept/")
    assert R.mint_yuid("http://example.org/p/1##quaPerson") != y1


def test_shorten_expand_round_trip():
    for full in ("http://vocab.getty.edu/aat/300055647##quaType",
                 "http://www.wikidata.org/entity/Q1##quaType",
                 "https://lux.collections.yale.edu/data/concept/x##quaType",
                 # no matching namespace -> passes through unchanged
                 "http://example.org/rec/1##quaType"):
        assert R.expand(R.shorten(full)) == full
    # the qua suffix survives shortening
    assert R.shorten("http://vocab.getty.edu/aat/1##quaType") == "aat:1##quaType"


def load_assertions(paths):
    """Parse assertion TSVs into {(lo, hi): {asserter}}.

    The production reader is now a streaming external sort feeding
    `_aggregate_edges`, which yields vote counts rather than asserter sets.
    This keeps the writer's on-disk contract under test in the form
    `cluster()` consumes."""
    edges = defaultdict(set)
    for path in paths:
        with open(path) as fh:
            for line in fh:
                lo, hi, asserter = line.rstrip("\n").split("\t")
                edges[(lo, hi)].add(asserter)
    return edges


def test_assertion_writer_round_trip(tmp_path):
    cfgs = StubConfigs(temp_dir=tmp_path)
    w = IdentityResolver(cfgs, StubIdmap(cfgs), 3)
    w.write_record({"data": {
        "id": "http://example.org/rec/1", "type": "Material",
        "equivalent": [{"id": "http://vocab.getty.edu/aat/1", "type": "Type"},
                       {"id": "http://www.wikidata.org/entity/Q1", "type": "Type"}],
    }})
    # no equivalents -> self assertion so the record still gets a YUID
    w.write_record({"data": {"id": "http://example.org/rec/2", "type": "Person"}})
    w.close()

    edges = load_assertions([tmp_path / "assertions-3.tsv"])
    pairs = set(edges)
    # URIs are stored in shortened (curie) form; the record id has no
    # matching namespace so it stays full
    rec1 = "http://example.org/rec/1##quaType"  # Material quas to Type
    aat1 = "aat:1##quaType"
    assert tuple(sorted([rec1, aat1])) in pairs
    self_pair = ("http://example.org/rec/2##quaPerson",
                 "http://example.org/rec/2##quaPerson")
    assert self_pair in pairs
    # asserter (col3) is always the record's own id
    assert all(a == rec1 for a in edges[tuple(sorted([rec1, aat1]))])
    # self assertions register the node but never merge anything
    clusters, _ = w.cluster(edges, set())
    assert len(clusters) == 2
