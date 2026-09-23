"""claim_member() asks each source once, not once per candidate.

Which worker builds a cluster's merged record is decided by the
lexicographically smallest internal member that still exists in its
recordcache, so the walk has to be in order and has to stop at the first
survivor. What it does not have to do is spend a round trip per candidate:
measured over a merge of 100.7M records, 1.2M calls at 28ms each -- the worst
per-call cost in the phase by a factor of seven.

run-merge.py is a script, not a module, so the function is rebuilt here
against the same contract rather than imported.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest


class Cache:
    """A recordcache that counts how often it is asked anything."""

    def __init__(self, present):
        self.present = set(present)
        self.calls = []

    def has_multi(self, keys):
        self.calls.append(list(keys))
        return {k for k in keys if k in self.present}

    def has_item(self, key):
        self.calls.append([key])
        return key in self.present

    def __contains__(self, key):
        return self.has_item(key)


NS = {"ycba": "https://ycba.example/", "ypm": "https://ypm.example/"}


def make_claim_member(sources):
    """run-merge.py's claim_member, with its two module-level dependencies
    (cfgs and internal_namespaces) passed in."""
    internal_namespaces = tuple(NS[n] for n in sources)

    def split_qua(uri):
        return uri.split("##qua")

    def split_uri(uri):
        for name, ns in NS.items():
            if uri.startswith(ns) and name in sources:
                return (sources[name], uri[len(ns):])
        raise ValueError(uri)

    def claim_member(cluster, present=()):
        cands = []
        for cand in sorted(m for m in cluster
                           if not m.startswith("__")
                           and m.startswith(internal_namespaces)):
            try:
                (csrc, crecid) = split_uri(split_qua(cand)[0])
            except Exception:
                continue
            cands.append((cand, csrc, crecid))

        asked = {}
        for cand, csrc, crecid in cands:
            if cand in present:
                return (cand, csrc)
            name = csrc["name"]
            if name not in asked:
                mine = [c for (_u, s, c) in cands if s["name"] == name]
                asked[name] = csrc["recordcache"].has_multi(mine)
            if crecid in asked[name]:
                return (cand, csrc)
        return (None, None)

    return claim_member


def sources(**present):
    return {name: {"name": name, "recordcache": Cache(keys)}
            for name, keys in present.items()}


def uri(src, ident, typ="HMO"):
    return f"{NS[src]}{ident}##qua{typ}"


# --- the batching ------------------------------------------------------------

def test_one_query_per_source_not_per_candidate():
    srcs = sources(ycba=["z"])
    claim = make_claim_member(srcs)
    cluster = {uri("ycba", c) for c in "abcdefz"}
    (claimed, src) = claim(cluster)
    assert claimed == uri("ycba", "z")
    assert src["name"] == "ycba"
    # one call, carrying every candidate that source holds
    assert len(srcs["ycba"]["recordcache"].calls) == 1
    assert sorted(srcs["ycba"]["recordcache"].calls[0]) == list("abcdefz")


def test_a_second_source_is_only_asked_when_reached():
    """Candidate order decides, so a source whose candidates all sort after
    a survivor is never queried at all."""
    srcs = sources(ycba=["a"], ypm=["b"])
    claim = make_claim_member(srcs)
    # ycba's namespace sorts before ypm's, and ycba/a exists
    (claimed, _) = claim({uri("ycba", "a"), uri("ypm", "b")})
    assert claimed == uri("ycba", "a")
    assert len(srcs["ycba"]["recordcache"].calls) == 1
    assert srcs["ypm"]["recordcache"].calls == []


def test_a_present_member_costs_no_query_at_all():
    """The main loop hands in its own record, which it knows exists."""
    srcs = sources(ycba=[])
    claim = make_claim_member(srcs)
    mine = uri("ycba", "a")
    (claimed, _) = claim({mine}, present=(mine,))
    assert claimed == mine
    assert srcs["ycba"]["recordcache"].calls == []


# --- behaviour the batching must not change ---------------------------------

def test_the_smallest_surviving_member_still_wins():
    """Not the first one asked about, and not one from an earlier source:
    two workers building the same cluster deadlock over its rows, so every
    worker has to reach the same answer."""
    srcs = sources(ycba=["m"], ypm=["b"])
    claim = make_claim_member(srcs)
    cluster = {uri("ycba", "m"), uri("ypm", "b")}
    assert claim(cluster)[0] == uri("ycba", "m")


def test_a_missing_smaller_member_is_skipped():
    srcs = sources(ycba=["b"])
    claim = make_claim_member(srcs)
    assert claim({uri("ycba", "a"), uri("ycba", "b")})[0] == uri("ycba", "b")


def test_present_does_not_beat_a_smaller_survivor():
    """`present` says "this one exists", not "this one wins"."""
    srcs = sources(ycba=["a"])
    claim = make_claim_member(srcs)
    mine = uri("ycba", "b")
    assert claim({uri("ycba", "a"), mine}, present=(mine,))[0] == uri("ycba", "a")


def test_no_internal_member_means_the_reference_pass_owns_it():
    srcs = sources(ycba=[])
    claim = make_claim_member(srcs)
    assert claim({"http://vocab.getty.edu/aat/300404670##quaType"}) == (None, None)


def test_the_update_token_is_not_a_candidate():
    srcs = sources(ycba=["a"])
    claim = make_claim_member(srcs)
    assert claim({"__20260921__", uri("ycba", "a")})[0] == uri("ycba", "a")


def test_nothing_exists_anywhere():
    srcs = sources(ycba=[], ypm=[])
    claim = make_claim_member(srcs)
    assert claim({uri("ycba", "a"), uri("ypm", "b")}) == (None, None)
    # asked each source exactly once despite finding nothing
    assert len(srcs["ycba"]["recordcache"].calls) == 1
    assert len(srcs["ypm"]["recordcache"].calls) == 1


def test_an_unsplittable_member_is_skipped_not_fatal():
    srcs = sources(ycba=["a"])
    claim = make_claim_member(srcs)
    bad = f"{NS['ycba']}##qua"      # no identifier
    assert claim({bad, uri("ycba", "a")})[0] == uri("ycba", "a")
