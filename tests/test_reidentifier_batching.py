"""Reidentifier two-pass batching.

process_entity used to issue one blocking idmap lookup per node. It now reads
from a batch that prefetch() resolves in two pipelined round trips. The risk
in that split is drift: _node_keys/_collect_keys must predict exactly what
process_entity will ask for. These tests pin both halves together --

  * output must be byte-identical to the unbatched walk, and
  * no lookup may fall through to the live idmap (batch_misses == 0)

-- over a record shaped to hit every short-circuit in process_entity:
do_not_reidentify, redirects, bnodes, dict-valued properties, ignore_props,
a node with no type (pruned), and a node with equivalents but no id.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.process.reidentifier import Reidentifier


class StubConfigs:
    internal_uri = "https://lux.test/data/"
    internal = {}
    external = {}
    globals = {"primaryName": "http://vocab.getty.edu/aat/300404670"}
    globals_cfg = {"primaryName": "300404670"}
    ok_record_types = {
        "Type": "concept", "Person": "person", "Place": "place",
        "Activity": "activity", "Right": "right", "HumanMadeObject": "object",
    }
    debug_reconciliation = False

    def is_qua(self, recid):
        return "##qua" in recid

    def make_qua(self, recid, typ):
        if "##qua" in recid:
            return recid
        if not typ in self.ok_record_types:
            raise ValueError(f"Unknown type: {typ}")
        return f"{recid}##qua{typ}"

    def split_qua(self, recid):
        return recid.split("##qua")


class CountingIdMap:
    """Dict-backed idmap that separates live single lookups from batched ones."""

    def __init__(self, data):
        self.data = data
        self.gets = 0
        self.multis = 0

    def __getitem__(self, key):
        self.gets += 1
        return self.data.get(key)

    def get_multi(self, keys, chunk=1000):
        self.multis += 1
        return {k: self.data.get(k) for k in keys}


YUID = "https://lux.test/data/person/p1"
TOKEN = "__20260101__"

IDMAP = {
    "http://int/p1##quaPerson": YUID,
    "http://ext/viaf1##quaPerson": YUID,
    YUID: {"http://int/p1##quaPerson", "http://ext/viaf1##quaPerson", TOKEN},
    "http://vocab.getty.edu/aat/300404670##quaType": "https://lux.test/data/concept/pn",
    "http://ext/place1##quaPlace": "https://lux.test/data/place/pl1",
    "http://ext/agent1##quaPerson": "https://lux.test/data/person/ag1",
    "http://ext/moved/1##quaPerson": "https://lux.test/data/person/moved",
}


def a_record():
    return {
        "id": "http://int/p1",
        "type": "Person",
        "_label": "Somebody",
        "equivalent": [{"id": "http://ext/viaf1", "type": "Person"}],
        # dict-valued property, not a list
        "born": {
            "type": "Birth",
            "took_place_at": [{"id": "http://ext/place1", "type": "Place"}],
        },
        # bnode: gets qua'd and looked up today, resolves to nothing
        "produced_by": {
            "id": "_:b1",
            "type": "Activity",
            "carried_out_by": [{"id": "http://ext/agent1", "type": "Person"}],
        },
        "identified_by": [
            {
                "type": "Name",
                "content": "Somebody",
                "classified_as": [
                    {"id": "http://vocab.getty.edu/aat/300404670", "type": "Type"}
                ],
            }
        ],
        # do_not_reidentify: returns early, but children are still walked
        "subject_to": [
            {
                "id": "https://creativecommons.org/licenses/x",
                "type": "Right",
                "classified_as": [
                    {"id": "http://vocab.getty.edu/aat/300404670", "type": "Type"}
                ],
            }
        ],
        # no type: process_entity returns None and the node is dropped
        "member_of": [{"id": "http://ext/untyped/1"}],
        # equivalents but no id: preserved as-is, no lookups
        "current_owner": [
            {"type": "Person", "equivalent": [{"id": "http://ext/owner1", "type": "Person"}]}
        ],
        # ignore_props: copied across without recursion
        "access_point": [{"id": "http://ap/1", "type": "Person"}],
        # redirected before qua'ing
        "residence": [{"id": "http://ext/old/1", "type": "Person"}],
    }


def make_reidentifier(idmap):
    r = object.__new__(Reidentifier)
    r.configs = StubConfigs()
    r.idmap = idmap
    r.debug = False
    r.do_not_reidentify = ["creativecommons.org"]
    r.redirects = {"http://ext/old/1": "http://ext/moved/1"}
    r.use_slug = True
    r.ignore_props = ["access_point", "conforms_to"]
    r.equivalent_refs = True
    r.preserve_equivalents = {}
    r.ignore_ns = []
    r.batch = {}
    r.batch_misses = 0
    return r


class UnbatchedIdMap(CountingIdMap):
    """No get_multi, so prefetch() declines and every lookup goes live."""
    get_multi = None


def test_batched_output_matches_unbatched():
    batched = make_reidentifier(CountingIdMap(IDMAP))
    unbatched = make_reidentifier(UnbatchedIdMap(IDMAP))
    assert batched._reidentify(a_record(), "Person", True) == \
        unbatched._reidentify(a_record(), "Person", True)


def test_every_lookup_comes_from_the_batch():
    idmap = CountingIdMap(IDMAP)
    r = make_reidentifier(idmap)
    r._reidentify(a_record(), "Person", True)
    # _node_keys predicted everything process_entity asked for
    assert r.batch_misses == 0
    assert idmap.gets == 0
    # one request for the string keys, one for the resolved YUID's member set
    assert idmap.multis == 2


def test_collected_keys_are_exactly_what_the_walk_requests():
    """Belt and braces: compare the predicted key set against the keys the
    real walk actually asks for, so a divergence names the difference rather
    than just showing a miss count."""
    idmap = CountingIdMap(IDMAP)
    asked = []

    class Recording(CountingIdMap):
        def __getitem__(self, key):
            asked.append(key)
            return super().__getitem__(key)
    recording = Recording(IDMAP)
    recording.get_multi = None      # force the live path so we see every key

    make_reidentifier(recording)._reidentify(a_record(), "Person", True)
    predicted = make_reidentifier(idmap)._collect_keys(
        a_record(), "Person", set(), top=True)
    # the YUID member-set key is resolved in prefetch's second hop, not by
    # _collect_keys, so add it before comparing
    predicted.add(YUID)
    assert predicted == set(asked)


def test_prefetch_failure_falls_back_to_live_lookups():
    class Broken(CountingIdMap):
        def get_multi(self, keys, chunk=1000):
            raise RuntimeError("redis is down")

    idmap = Broken(IDMAP)
    r = make_reidentifier(idmap)
    result = r._reidentify(a_record(), "Person", True)
    assert result["id"] == YUID
    assert idmap.gets > 0
