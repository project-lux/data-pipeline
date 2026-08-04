"""Every row a merge writes must be keyed by the cluster it is building.

This is the invariant that makes 24 parallel merge workers safe with commits
deferred: they own disjoint YUIDs, so they never contend for a row. It is not
enough to partition the YUIDs -- a write has to actually STAY inside its
cluster, and one path did not.

reidentify() resolved a record to min() over {the record's own YUID} plus
{one per equivalent}. run-identify refuses assertions that violate a
differentFrom (identity_conflicts.jsonl), so a record's equivalents really do
span clusters, and when an equivalent's YUID sorted lower it won. merge then
wrote that record's rewritten row under a YUID belonging to another worker.
Two workers upserting one primary key in <source>_rewritten_record_cache is
what postgres reported as `deadlock detected`.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.process.merger import MergeHandler

X = "https://lux.test/data/person/" + "1" * 32   # the cluster being built
Y = "https://lux.test/data/person/" + "0" * 32   # a DIFFERENT cluster


class Cache(dict):
    """recordcache2 stand-in that records what key each row was written to."""

    def __init__(self, *a):
        super().__init__(*a)
        self.written = []

    def __setitem__(self, key, value):
        self.written.append(key)
        super().__setitem__(key, value)


class StubConfigs:
    internal = {}

    def __init__(self, sources):
        self.sources = sources

    def split_uri(self, uri):
        for name, src in self.sources.items():
            if src["namespace"] in uri:
                return (src, uri.rsplit(src["namespace"], 1)[1])
        raise ValueError(uri)


class StubReidentifier:
    """Returns whatever YUID it is told to -- standing in for the equivalents
    disagreeing with the record's own assignment."""

    def __init__(self, yuid):
        self.yuid = yuid

    def reidentify(self, record, rectype=None):
        return {"data": dict(record["data"]), "yuid": self.yuid,
                "identifier": record.get("identifier", "")}


class StubMerger:
    def __init__(self):
        self.merged = []

    def merge(self, record, other):
        self.merged.append(other["yuid"])
        return record


class StubRefMgr:
    def delete_done_ref(self, eq):
        pass


def handler(resolves_to):
    dnb = {"name": "dnb", "type": "external", "merge_order": 1,
           "namespace": "https://d-nb.info/gnd/",
           "recordcache": {"118540238##quaPerson":
                           {"identifier": "118540238##quaPerson",
                            "data": {"id": "https://d-nb.info/gnd/118540238",
                                     "type": "Person"}}},
           "recordcache2": Cache()}
    h = object.__new__(MergeHandler)
    h.config = StubConfigs({"dnb": dnb})
    h.idmap = {}
    h.merger = StubMerger()
    h.reference_manager = StubRefMgr()
    h.reidentifier = StubReidentifier(resolves_to)
    return h, dnb


def cluster_record():
    return {"data": {"id": X, "type": "Person"}, "source": "yuag"}


EQUIVS = ["https://d-nb.info/gnd/118540238##quaPerson"]


def test_member_row_is_written_under_the_cluster_yuid():
    h, dnb = handler(X.rsplit("/", 1)[-1])
    h.merge(cluster_record(), list(EQUIVS))
    assert dnb["recordcache2"].written == [X.rsplit("/", 1)[-1]]


def test_row_escaping_the_cluster_is_not_written():
    """The DNB record was handed to us as a member of X, but resolves to Y.
    Y's rows belong to whichever worker owns Y -- do not touch them."""
    h, dnb = handler(Y.rsplit("/", 1)[-1])
    h.merge(cluster_record(), list(EQUIVS))
    assert dnb["recordcache2"].written == [], \
        "wrote into a YUID this worker does not own"


def test_escaping_record_still_contributes_its_content():
    """Skipping the row must not silently drop the record from the merge --
    the data is still a legitimate member of this cluster."""
    h, dnb = handler(Y.rsplit("/", 1)[-1])
    h.merge(cluster_record(), list(EQUIVS))
    assert h.merger.merged == [Y.rsplit("/", 1)[-1]], \
        "content should still be merged even when its row is not written"
