"""ReferenceManager: one record's references resolved in one batch.

add_ref used to issue 7-11 redis round trips per reference node (EXISTS +
HGET + HGET against all_refs, EXISTS + HGET against done_refs, then a
WATCH/MULTI merge), so a record with 200 references cost over 2000. The walk
now collects into a pending dict and resolve_refs() does the work in three
pipelined round trips regardless of how many references there are.

The decision table it replaces is subtle -- whether a reference is in
all_refs, in done_refs, and at what distance, decides between re-queueing,
merging, and doing nothing -- so these tests pin every branch of it.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.process.reference_manager import ReferenceManager
from pipeline.storage.idmap.redis import ReferenceMap

AAT = "http://vocab.getty.edu/aat/"
LUX = "https://lux.collections.yale.edu/data/"
REF = f"{AAT}300404670##quaType"


# --- a hash store standing in for redis, counting network round trips ------

class Script:
    def __init__(self, store):
        self.store = store

    def __call__(self, keys=None, args=None, client=None):
        h = self.store.setdefault(keys[0], {})
        dist = int(args[0])
        cur = h.get("dist")
        if cur is None or int(cur) > dist:
            h["dist"] = str(dist)
        h.setdefault("type", args[1])


class Pipe:
    def __init__(self, conn):
        self.conn = conn
        self.queue = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass

    def hgetall(self, key):
        self.queue.append(("hgetall", key))

    def delete(self, key):
        self.queue.append(("delete", key))

    def execute(self):
        self.conn.round_trips += 1
        out = []
        for op in self.queue:
            if op[0] == "hgetall":
                out.append(dict(self.conn.store.get(op[1], {})))
            else:
                self.conn.store.pop(op[1], None)
                out.append(1)
        self.queue = []
        return out


class Conn:
    def __init__(self):
        self.store = {}
        self.round_trips = 0

    def pipeline(self, transaction=True):
        return Pipe(self)

    def register_script(self, lua):
        return Script(self.store)


def refmap(seed=None):
    m = object.__new__(ReferenceMap)
    m.conn = Conn()
    m.prefix_map_in = {}
    m.prefix_map_out = {}
    m.configs = None
    m._merge_script = None
    m._scripting = True
    for k, v in (seed or {}).items():
        m.conn.store[k] = {kk: str(vv) for kk, vv in v.items()}
    return m


class StubConfigs:
    internal_uri = LUX
    internal = {}
    parent_record_types = {"Type": "Type", "Material": "Type", "Language": "Type"}

    def make_qua(self, recid, typ):
        return recid if "##qua" in recid else f"{recid}##qua{typ}"


def manager(all_seed=None, done_seed=None):
    rm = object.__new__(ReferenceManager)
    rm.configs = StubConfigs()
    rm.metatypes_seen = {}
    rm.all_refs = refmap(all_seed)
    rm.done_refs = refmap(done_seed)
    rm.idmap = None
    rm.debug = False
    rm.internal_uris = [LUX]
    rm.internal_uris_t = (LUX,)
    rm.redirects = {}
    rm.ref_cache = {}
    return rm


def dist_of(rm, ref):
    return rm.all_refs.conn.store.get(ref, {}).get("dist")


# --- the decision table ----------------------------------------------------

def test_unknown_reference_is_added_and_returned():
    rm = manager()
    got = rm.resolve_refs({REF: [2, "Type"]})
    assert dist_of(rm, REF) == "2"
    assert got[REF] == {"dist": 2, "type": "Type"}


def test_known_reference_keeps_the_shorter_distance():
    rm = manager({REF: {"dist": 5, "type": "Type"}})
    rm.resolve_refs({REF: [2, "Type"]})
    assert dist_of(rm, REF) == "2"

    rm = manager({REF: {"dist": 2, "type": "Type"}})
    rm.resolve_refs({REF: [5, "Type"]})
    assert dist_of(rm, REF) == "2"


def test_done_reference_is_requeued_only_when_now_closer():
    # closer than when it was done: back into all_refs, out of done_refs
    rm = manager(None, {REF: {"dist": 5}})
    rm.resolve_refs({REF: [1, "Type"]})
    assert dist_of(rm, REF) == "1"
    assert REF not in rm.done_refs.conn.store

    # no closer: left alone, still done
    rm = manager(None, {REF: {"dist": 1}})
    got = rm.resolve_refs({REF: [5, "Type"]})
    assert REF not in rm.all_refs.conn.store
    assert rm.done_refs.conn.store[REF]["dist"] == "1"
    assert got == {}


def test_already_done_reference_is_not_returned_as_new():
    rm = manager(None, {REF: {"dist": 5}})
    assert rm.resolve_refs({REF: [1, "Type"]}) == {}


def test_type_is_set_once_and_not_overwritten():
    rm = manager({REF: {"dist": 5, "type": "Type"}})
    rm.resolve_refs({REF: [1, "Material"]})
    assert rm.all_refs.conn.store[REF]["type"] == "Type"


def test_aat_at_distance_one_is_locally_cached():
    rm = manager()
    rm.resolve_refs({REF: [1, "Type"]})
    assert rm.ref_cache[REF] == 1
    # a cached ref is skipped entirely on the next collect
    pending = {}
    rm.collect_ref(REF, pending, 1, "Type")
    assert pending == {}


# --- collection ------------------------------------------------------------

def test_collect_keeps_shortest_distance_and_first_type():
    rm = manager()
    pending = {}
    rm.collect_ref(REF, pending, 3, "Type")
    rm.collect_ref(REF, pending, 1, "")
    rm.collect_ref(REF, pending, 2, "Material")
    assert pending[REF] == [1, "Type"]


def test_collect_fills_in_a_type_that_was_missing():
    rm = manager()
    pending = {}
    rm.collect_ref(REF, pending, 3, "")
    rm.collect_ref(REF, pending, 3, "Type")
    assert pending[REF] == [3, "Type"]


# --- the walk --------------------------------------------------------------

def record(n_refs):
    return {
        "id": "http://vocab.getty.edu/ulan/500012345",
        "type": "Person",
        "classified_as": [{"id": f"{AAT}30040{i:04d}", "type": "Type"} for i in range(n_refs)],
        "member_of": [{"id": f"{LUX}set/abc", "type": "Set"}],        # internal
        "produced_by": {"id": "_:b1", "type": "Activity"},            # bnode
        "access_point": [{"id": f"{AAT}999999", "type": "Type"}],     # skipped prop
        "equivalent": [{"id": "http://www.wikidata.org/entity/Q1", "type": "Person"}],
    }


def test_whole_record_costs_a_fixed_number_of_round_trips():
    for n in (5, 50, 200):
        rm = manager()
        rm.walk_top_for_refs(record(n), 0)
        trips = rm.all_refs.conn.round_trips + rm.done_refs.conn.round_trips
        assert trips == 3, f"{n} refs took {trips} round trips"


def test_walk_records_the_right_references():
    rm = manager()
    rm.walk_top_for_refs(record(3), 0)
    keys = rm.all_refs.conn.store
    assert not any(k.startswith(LUX) for k in keys), "internal URIs are built anyway"
    assert not any("_:b1" in k for k in keys), "bnodes are not references"
    assert f"{AAT}999999##quaType" not in keys, "access_point is not followed"
    assert f"{AAT}300400000##quaType" in keys


def test_equivalents_are_closer_than_the_walk():
    # the walk runs at distance+1, equivalents at distance
    rm = manager()
    rm.walk_top_for_refs(record(1), 0)
    assert dist_of(rm, "http://www.wikidata.org/entity/Q1##quaPerson") == "0"
    assert dist_of(rm, f"{AAT}300400000##quaType") == "1"


def test_metatypes_dedupe():
    rm = manager()
    rm.walk_top_for_refs({
        "id": "http://x/1", "type": "Person",
        "referred_to_by": [{"id": f"{AAT}300404670", "type": "Type", "classified_as": [
            {"id": f"{AAT}300435443", "type": "Type"},
            {"id": f"{AAT}300435443", "type": "Type"},
        ]}]}, 0)
    assert rm.metatypes_seen[f"{AAT}300404670"] == {f"{AAT}300435443"}
