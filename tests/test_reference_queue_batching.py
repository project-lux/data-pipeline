"""Claiming and draining the reference queue in batches.

popitem() spent four round trips per reference (RANDOMKEY, WATCH, HGETALL,
MULTI/EXEC -- redis-py runs a watched pipeline in immediate mode, so each is
its own trip), and write_done_refs() spent three, because iter_keys() hands
back a live reference into redis and the loop read ['dist'] off it three
times.

The batched versions must keep two guarantees that matter more than speed:

  * a reference is claimed by exactly one worker, and
  * an empty result means the queue really is empty -- returning [] early
    ends the caller's loop and silently drops the rest of the build.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import redis

from pipeline.process.reference_manager import ReferenceManager
from pipeline.storage.idmap.redis import ReferenceMap

AAT = "http://vocab.getty.edu/aat/"


class Store(dict):
    """Keyspace with a stable ordering, so SCAN can be emulated the way redis
    behaves: a cursor walks a fixed order and tolerates deletion mid-scan."""

    def __init__(self, n, dist="1"):
        super().__init__({f"{AAT}{i:07d}##quaType": {"dist": dist, "type": "Type"}
                          for i in range(n)})
        self.order = list(self.keys())

    def add(self, key, dist="1"):
        self[key] = {"dist": dist, "type": "Type"}
        self.order.append(key)


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

    def execute(self):
        self.conn.round_trips += 1
        out = []
        for op, key in self.queue:
            if op == "pop":
                # the Lua script: read and delete as one atomic step
                d = self.conn.store.pop(key, None)
                out.append([x for kv in (d or {}).items() for x in kv] if d else [])
            else:
                out.append(dict(self.conn.store.get(key, {})))
        self.queue = []
        return out


class PopScript:
    def __init__(self, conn):
        self.conn = conn

    def __call__(self, keys=None, args=None, client=None):
        client.queue.append(("pop", keys[0]))


class Conn:
    def __init__(self, store):
        self.store = store
        self.round_trips = 0

    def pipeline(self, transaction=True):
        return Pipe(self)

    def register_script(self, lua):
        return PopScript(self)

    def scan(self, cursor=0, count=10):
        self.round_trips += 1
        order = self.store.order
        if cursor >= len(order):
            return (0, [])
        window = order[cursor:cursor + count]
        nxt = cursor + count
        return (nxt if nxt < len(order) else 0,
                [k for k in window if k in self.store])

    def scan_iter(self, count=1000, **kw):
        self.round_trips += 1
        return list(self.store.keys())


def refmap(store):
    m = object.__new__(ReferenceMap)
    m.conn = Conn(store)
    m.prefix_map_in = {}
    m.prefix_map_out = {}
    m.configs = None
    m._merge_script = None
    m._pop_script = None
    m._scripting = True
    m._scan_cursor = 0
    return m


def drain(m, batch=50):
    out = []
    while True:
        got = m.popitems(batch)
        if not got:
            return out
        out.extend(got)


# --- claiming ---------------------------------------------------------------

def test_every_reference_is_claimed_exactly_once():
    store = Store(500)
    got = drain(refmap(store))
    assert sorted(k for k, _ in got) == sorted(store.order)
    assert len(got) == 500


def test_values_come_back_intact():
    store = Store(5)
    got = drain(refmap(store))
    assert all(v == {"dist": 1, "type": "Type"} for _, v in got)


def test_two_workers_never_claim_the_same_reference():
    store = Store(400)
    a = refmap(store)
    b = refmap(store)
    b.conn = a.conn                      # one keyspace, two claimants
    got = []
    while True:
        ba, bb = a.popitems(30), b.popitems(30)
        got.extend(ba)
        got.extend(bb)
        if not ba and not bb:
            break
    keys = [k for k, _ in got]
    assert len(keys) == len(set(keys)), "a reference was claimed twice"
    assert sorted(keys) == sorted(store.order), "a reference was never claimed"


def test_reference_added_while_draining_is_still_claimed():
    store = Store(100)
    m = refmap(store)
    got = []
    added = False
    while True:
        batch = m.popitems(25)
        if not batch:
            break
        got.extend(batch)
        if not added:                 # a peer discovers a new reference
            store.add(f"{AAT}9999999##quaType")
            added = True
    assert any(k.endswith("9999999##quaType") for k, _ in got)


def test_batch_size_is_respected():
    # everything a scan returns is claimed, so a batch can overshoot -- but
    # not without bound: claimed-but-unprocessed work is lost if a worker dies
    store = Store(1000)
    m = refmap(store)
    first = m.popitems(25)
    assert 25 <= len(first) < 60, f"batch of 25 returned {len(first)}"


def test_empty_only_when_actually_empty():
    # a false empty ends the caller's loop and drops the rest of the build
    assert refmap(Store(0)).popitems(10) == []
    m = refmap(Store(3))
    assert len(m.popitems(10)) == 3
    assert m.popitems(10) == []


def test_costs_far_fewer_round_trips_than_one_at_a_time():
    store = Store(1000)
    m = refmap(store)
    drain(m, batch=50)
    # the per-item path was 4 round trips each, i.e. 4000
    assert m.conn.round_trips < 100


def test_falls_back_when_scripting_is_unavailable():
    store = Store(10)
    m = refmap(store)

    def boom():
        raise redis.ResponseError("unknown command 'EVALSHA'")

    real_pipeline = m.conn.pipeline

    def bad_pipeline(transaction=True):
        p = real_pipeline(transaction)
        if transaction:               # popitem()'s WATCH path still works
            p.watch = lambda k: None
            p.hgetall = lambda k: dict(m.conn.store.get(k, {}))
            p.multi = lambda: None
            p.unwatch = lambda: None
            p.delete = lambda k: p.queue.append(("pop", k))
            return p
        p.execute = boom
        return p

    m.conn.pipeline = bad_pipeline
    m.conn.randomkey = lambda: next(iter(m.conn.store), None)
    got = m.popitems(10)
    assert len(got) == 10
    assert m._scripting is False


# --- pop_ref keeps handing out one at a time --------------------------------

def test_pop_ref_yields_one_at_a_time_from_a_batch():
    store = Store(120)
    rm = object.__new__(ReferenceManager)
    rm.all_refs = refmap(store)
    rm.ref_batch = 50
    rm._ref_buffer = []
    seen = []
    while True:
        item = rm.pop_ref()
        if item is None:
            break
        seen.append(item)
    assert len(seen) == 120
    assert sorted(k for k, _ in seen) == sorted(store.order)


# --- write_done_refs --------------------------------------------------------

class StubConfigs:
    max_distance = 3


def test_write_done_refs_batches_and_filters(tmp_path, monkeypatch):
    store = Store(100)
    for i, k in enumerate(store.order):
        store[k] = {"dist": "9" if i < 10 else "1", "type": "Type"}
    rm = object.__new__(ReferenceManager)
    rm.configs = StubConfigs()
    rm.done_refs = refmap(store)

    monkeypatch.chdir(tmp_path)
    rm.write_done_refs()

    lines = (tmp_path / "reference_uris.txt").read_text().splitlines()
    assert len(lines) == 90, "references past max_distance must be excluded"
    assert all(line.startswith("1|") for line in lines)
    # was three round trips per reference
    assert rm.done_refs.conn.round_trips < 10
