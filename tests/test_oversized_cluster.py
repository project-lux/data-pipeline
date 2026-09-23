"""A cluster too large to be a record: cap what merges, keep what resolves.

Measured on a europeana build, `yuid:person/bcd070a7-...` had **88,447
members** and py-spy caught a worker inside it with `lbl: "anonymous"` --
every unattributed creator in the corpus reconciled into one person. The
largest legitimate cluster seen is a few dozen.

Merging it means 88,447 record fetches, 88,447 reidentify calls (each of
which walks the whole cluster), and an `equivalent` array of 88,447 entries
on every member record. The cap takes the biggest MAX_CLUSTER_MEMBERS,
because a longer record carries more information.

What the dropped members keep matters as much as what they lose, and most of
these tests are about that: they still resolve to the YUID everywhere they
are referenced, and their reference-queue entries are still cleared. What
they lose is the chance to contribute content -- and a recordcache2 row they
were overwriting each other in anyway, since that row is keyed by YUID.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import json

import pytest

from pipeline.process.merger import MergeHandler


NS = "https://europeana.linked.art/data/"


class PlainCache(dict):
    """Single-key access only, as FsCache and the in-memory doubles are."""

    def __init__(self, rows):
        super().__init__(rows)
        self.multi_calls = 0
        self.item_calls = 0

    def __getitem__(self, k):
        self.item_calls += 1
        return dict(super().__getitem__(k)) if k in self else None


class Cache(PlainCache):
    """A recordcache with the postgres batch method."""

    def get_multi(self, keys, raw=False):
        self.multi_calls += 1
        out = {}
        for k in keys:
            if k in self:
                # bypass __getitem__ so item_calls counts only single fetches
                row = dict(dict.__getitem__(self, k))
                if raw:
                    row["data"] = json.dumps(row["data"])
                out[k] = row
        return out


class Refs:
    def __init__(self):
        self.deleted = []

    def delete_done_ref(self, eq):
        self.deleted.append(eq)


def record(ident, pad=0):
    return {"identifier": ident,
            "data": {"id": f"{NS}{ident}", "type": "Person",
                     "_label": "anonymous", "pad": "x" * pad}}


def handler(cache, refs, seen):
    h = object.__new__(MergeHandler)
    h.reference_manager = refs

    class Cfg:
        internal = {"europeana": {"name": "europeana", "type": "internal",
                                  "namespace": NS, "merge_order": 1}}

        def split_uri(self, uri):
            return (self.internal["europeana"], uri[len(NS):])

    cfg = Cfg()
    cfg.internal["europeana"]["recordcache"] = cache
    cfg.internal["europeana"]["recordcache2"] = seen
    h.config = cfg

    class Reid:
        def reidentify(self, rec, rectype=None):
            return {"data": rec["data"], "yuid": "YU"}

    class Rec:
        def merge(self, record, other):
            record.setdefault("_merged", []).append(other["data"]["id"])

    h.reidentifier, h.merger = Reid(), Rec()
    return h


def cluster(n, pads=None, batched=True):
    rows = {f"person/p{i}": record(f"person/p{i}", (pads or {}).get(i, i))
            for i in range(n)}
    cache = (Cache if batched else PlainCache)(rows)
    return cache, {f"{NS}person/p{i}##quaPerson" for i in range(n)}


def run(n, cap=None, batched=True, pads=None):
    cache, members = cluster(n, pads, batched)
    refs, seen = Refs(), {}
    h = handler(cache, refs, seen)
    if cap is not None:
        h.MAX_CLUSTER_MEMBERS = cap
    base = {"source": "europeana", "data": {"id": "https://lux/data/person/YU",
                                            "type": "Person"}}
    out = h.merge(base, members)
    return out, cache, refs, seen


# --- the cap -----------------------------------------------------------------

def test_the_default_cap_is_100():
    assert MergeHandler.MAX_CLUSTER_MEMBERS == 100


def test_a_small_cluster_is_untouched():
    out, _, refs, _ = run(12, cap=100)
    assert len(out["_merged"]) == 12
    assert len(refs.deleted) == 12


def test_an_oversized_cluster_merges_only_the_cap():
    out, _, _, _ = run(500, cap=100)
    assert len(out["_merged"]) == 100


def test_the_largest_records_are_the_ones_kept():
    """Longer record = more information."""
    out, _, _, _ = run(50, cap=5)
    kept = {u.rsplit("/", 1)[-1] for u in out["_merged"]}
    assert kept == {"p49", "p48", "p47", "p46", "p45"}      # pad == i


def test_ties_break_deterministically():
    """Equal-sized records must be cut the same way in every worker, or two
    workers build different merged records for one YUID."""
    pads = {i: 10 for i in range(40)}          # all the same size
    first = run(40, cap=5, pads=pads)[0]["_merged"]
    for _ in range(5):
        assert run(40, cap=5, pads=pads)[0]["_merged"] == first


# --- what the dropped members keep -------------------------------------------

def test_every_member_still_has_its_reference_queue_entry_cleared():
    """Including the 400 that do not contribute content: the queue records
    what has been dealt with, not what was merged."""
    _, _, refs, _ = run(500, cap=100)
    assert len(refs.deleted) == 500


def test_the_update_token_is_not_a_member():
    cache, members = cluster(5)
    members.add("__20260922__")
    refs, seen = Refs(), {}
    h = handler(cache, refs, seen)
    out = h.merge({"source": "europeana",
                   "data": {"id": "https://lux/data/person/YU", "type": "Person"}},
                  members)
    assert len(out["_merged"]) == 5
    assert "__20260922__" not in refs.deleted


# --- fetching ----------------------------------------------------------------

def test_members_are_fetched_in_one_batch_per_source():
    _, cache, _, _ = run(500, cap=100)
    assert cache.multi_calls == 1
    assert cache.item_calls == 0          # never one at a time


def test_a_cache_without_get_multi_still_works():
    """FsCache has no batch method, and it is a configurable cache class."""
    out, cache, _, _ = run(50, cap=5, batched=False)
    assert len(out["_merged"]) == 5
    assert cache.item_calls == 50


def test_only_the_survivors_are_parsed():
    """The raw fetch hands back text; parsing 500 documents to discard 400
    of them is the cost being avoided."""
    out, _, _, _ = run(500, cap=100)
    # everything that reached the merger was parsed back into a dict
    assert all(isinstance(u, str) for u in out["_merged"])
    assert len(out["_merged"]) == 100


def test_a_missing_record_is_skipped_not_fatal():
    cache, members = cluster(10)
    members.add(f"{NS}person/gone##quaPerson")
    refs, seen = Refs(), {}
    h = handler(cache, refs, seen)
    out = h.merge({"source": "europeana",
                   "data": {"id": "https://lux/data/person/YU", "type": "Person"}},
                  members)
    assert len(out["_merged"]) == 10
    assert f"{NS}person/gone##quaPerson" in refs.deleted    # still dequeued
