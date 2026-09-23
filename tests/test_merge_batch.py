"""run-merge resolves a slice against the idmap a batch at a time.

Asked one record at a time, the two lookups merge needs -- what YUID is this
in, has that YUID already been built -- were the phase. Measured on a resumed
run of 20.7M records: `idmap_cluster` 22.64 worker-hours at 3,930us/call and
`resume_check` 3.98 at 690us/call, 85% of everything, with **97.4% of the
records already built** so nearly all of it discarded.

These tests are about what must not change. The batch is an optimisation
sitting in front of the loop, so every record must come out exactly once, in
order, with the same answers it would have got one at a time -- and a batch
that fails must degrade to asking per record rather than silently skipping
500 records.
"""

import sys
from contextlib import contextmanager
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest

from pipeline.process.merge_batch import built_yuids, in_batches, prefetched


NS = "https://europeana.linked.art/data/"
INT = "https://lux.collections.yale.edu/data/"


class Timer:
    def __init__(self):
        self.steps = 0
        self.stages = []

    def step(self):
        self.steps += 1

    @contextmanager
    def stage(self, name):
        self.stages.append(name)
        yield


class Cfgs:
    def make_qua(self, uri, typ):
        return f"{uri}##qua{typ}"


class IdMap:
    """The idmap's batch and single-key reads, counted."""

    def __init__(self, fwd, members=None, fail_batch=False):
        self.fwd = fwd
        self.members = members or {}
        self.fail_batch = fail_batch
        self.multi = []          # the key lists it was asked for
        self.singles = []

    def get_multi(self, keys):
        keys = list(keys)
        self.multi.append(keys)
        if self.fail_batch:
            # get_multi swallows its own errors and returns None per key
            return {k: None for k in keys}
        out = {}
        for k in keys:
            out[k] = self.members.get(k) if k.startswith(INT) else self.fwd.get(k)
        return out

    def __getitem__(self, key):
        self.singles.append(key)
        return self.fwd.get(key)


class PlainMerged:
    """Single-key only, as a filesystem-backed merged cache is."""

    def __init__(self, built):
        self.built = set(built)
        self.multi = []
        self.items = []

    def has_item(self, y):
        self.items.append(y)
        return y in self.built


class Merged(PlainMerged):
    def has_multi(self, yuids):
        self.multi.append(list(yuids))
        return {y for y in yuids if y in self.built}


def rec(i):
    return {"identifier": f"person/p{i}", "data": {"type": "Person"}}


def qua(i):
    return f"{NS}person/p{i}##quaPerson"


def setup(n, yuid_of=None, built=(), batch=10, resume=True,
          fail_batch=False, merged_batched=True):
    yuid_of = yuid_of or {i: f"{INT}person/y{i}" for i in range(n)}
    fwd = {qua(i): y for i, y in yuid_of.items() if y}
    members = {y: {qua(i)} for i, y in yuid_of.items() if y}
    idmap = IdMap(fwd, members, fail_batch=fail_batch)
    merged = (Merged if merged_batched else PlainMerged)(built)
    timer = Timer()
    src = {"name": "europeana", "namespace": NS}
    out = list(prefetched((rec(i) for i in range(n)), src, Cfgs(), idmap,
                          merged, timer, resume, batch))
    return out, idmap, merged, timer


# --- in_batches --------------------------------------------------------------

@pytest.mark.parametrize("n,size,expect", [
    (0, 5, []), (1, 5, [1]), (5, 5, [5]), (6, 5, [5, 1]), (12, 5, [5, 5, 2]),
])
def test_in_batches_covers_everything_without_overlap(n, size, expect):
    got = list(in_batches(range(n), size))
    assert [len(b) for b in got] == expect
    assert [x for b in got for x in b] == list(range(n))


def test_in_batches_does_not_read_the_whole_iterator():
    """The records come off a server-side cursor; buffering the slice would
    defeat the point of iterating it."""
    seen = []

    def gen():
        for i in range(100):
            seen.append(i)
            yield i

    it = in_batches(gen(), 10)
    next(it)
    assert len(seen) == 10


# --- every record comes out, once, in order ----------------------------------

@pytest.mark.parametrize("n,batch", [(0, 10), (1, 10), (9, 10), (10, 10),
                                     (11, 10), (25, 10), (100, 7)])
def test_every_record_is_yielded_once_in_order(n, batch):
    out, _, _, timer = setup(n, batch=batch)
    assert [o[1] for o in out] == [f"person/p{i}" for i in range(n)]
    assert timer.steps == n


def test_the_record_dict_is_the_one_that_came_in_with_source_stamped():
    out, _, _, _ = setup(3)
    for i, (r, recid, recuri, qrecid, full, built) in enumerate(out):
        assert r["source"] == "europeana"
        assert recuri == f"{NS}person/p{i}"
        assert qrecid == qua(i)


# --- the answers -------------------------------------------------------------

def test_a_record_not_in_the_map_yields_no_yuid():
    out, _, _, _ = setup(3, yuid_of={0: f"{INT}person/y0", 1: None,
                                     2: f"{INT}person/y2"})
    assert [bool(o[4]) for o in out] == [True, False, True]


def test_already_built_is_set_from_the_merged_cache():
    out, _, merged, _ = setup(4, built={"y1", "y3"})
    assert [o[5] for o in out] == [False, True, False, True]
    assert len(merged.multi) == 1          # one check for the batch


def test_nothing_is_already_built_without_resume():
    out, _, merged, _ = setup(4, built={"y1", "y3"}, resume=False)
    assert [o[5] for o in out] == [False] * 4
    assert merged.multi == []              # not asked at all


def test_a_missing_record_is_never_already_built():
    out, _, _, _ = setup(2, yuid_of={0: None, 1: f"{INT}person/y1"},
                         built={"y1"})
    assert out[0][5] is False
    assert out[1][5] is True


# --- the round trips ---------------------------------------------------------

def test_one_idmap_batch_per_chunk_not_per_record():
    _, idmap, _, _ = setup(25, batch=10)
    forward = [c for c in idmap.multi if c and c[0].startswith(NS)]
    assert [len(c) for c in forward] == [10, 10, 5]
    assert idmap.singles == []


def test_member_sets_are_fetched_only_for_records_that_will_be_built():
    """The member scan is the expensive half; on a skipped record it is
    pure waste, which is why the resume check comes first."""
    _, idmap, _, _ = setup(10, built={f"y{i}" for i in range(10)}, batch=10)
    member_calls = [c for c in idmap.multi if c and c[0].startswith(INT)]
    assert member_calls == []              # all built: nothing prefetched


def test_member_sets_are_fetched_for_the_survivors():
    _, idmap, _, _ = setup(10, built={f"y{i}" for i in range(8)}, batch=10)
    member_calls = [c for c in idmap.multi if c and c[0].startswith(INT)]
    assert len(member_calls) == 1
    assert sorted(member_calls[0]) == [f"{INT}person/y8", f"{INT}person/y9"]


def test_the_resume_check_is_one_call_per_batch():
    _, _, merged, _ = setup(25, batch=10)
    assert [len(c) for c in merged.multi] == [10, 10, 5]


# --- degrading -------------------------------------------------------------

def test_a_failed_batch_falls_back_to_asking_per_record():
    """get_multi swallows its own errors and returns None for every key. If
    that were taken at face value, one failure would skip 500 records."""
    out, idmap, _, _ = setup(5, fail_batch=True, batch=10)
    assert idmap.singles == [qua(i) for i in range(5)]
    assert [bool(o[4]) for o in out] == [True] * 5


def test_only_the_unresolved_keys_are_asked_for_singly():
    out, idmap, _, _ = setup(4, yuid_of={0: f"{INT}person/y0", 1: None,
                                         2: f"{INT}person/y2", 3: None})
    assert idmap.singles == [qua(1), qua(3)]


def test_a_merged_cache_without_has_multi_still_works():
    """FsCache has neither has_multi nor metadata."""
    out, _, merged, _ = setup(4, built={"y1"}, merged_batched=False)
    assert [o[5] for o in out] == [False, True, False, False]
    assert len(merged.items) == 4


def test_built_yuids_prefers_the_batch_call():
    class C:
        def has_multi(self, ys):
            return {"a"}

        def has_item(self, y):
            raise AssertionError("should not be reached")

    assert built_yuids(C(), ["a", "b"]) == {"a"}


# --- the stages the timing report will show ---------------------------------

def test_the_stages_are_named_for_the_report():
    _, _, _, timer = setup(10, built={"y0"}, batch=10)
    assert timer.stages == ["prefetch_yuid", "resume_check", "prefetch_cluster"]


def test_no_empty_batches_are_issued():
    """A chunk where everything is already built must not issue a member
    prefetch, and an empty slice must issue nothing at all."""
    _, idmap, merged, timer = setup(0)
    assert idmap.multi == [] and merged.multi == [] and timer.stages == []
