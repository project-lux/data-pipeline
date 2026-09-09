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
import time
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

    def dbsize(self):
        # all_refs has a redis db to itself, so this is the queue length
        return len(self.store)


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

def popper(store, workers=1, idle_timeout=0, idle_poll=0):
    rm = object.__new__(ReferenceManager)
    rm.all_refs = refmap(store)
    rm.ref_batch = 50
    rm._ref_buffer = []
    rm.ref_workers = workers
    rm.ref_idle_timeout = idle_timeout
    rm.ref_idle_poll = idle_poll
    return rm


def test_pop_ref_yields_one_at_a_time_from_a_batch():
    store = Store(120)
    rm = popper(store)
    seen = []
    while True:
        item = rm.pop_ref()
        if item is None:
            break
        seen.append(item)
    assert len(seen) == 120
    assert sorted(k for k, _ in seen) == sorted(store.order)


# --- the queue going empty is not the phase being over ----------------------

def test_pop_ref_waits_for_work_another_worker_is_still_generating():
    """Processing a reference enqueues the references it finds, so an empty
    read while other workers are still expanding is transient. Quitting on it
    left whichever worker claimed the last batch to do the entire remaining
    expansion alone -- 23 of 24 workers exited within the same minute and the
    survivor ran on for 21 more."""
    store = Store(1)
    rm = popper(store, workers=24, idle_timeout=5, idle_poll=0)

    # drain what's there, then have a "peer" enqueue more mid-wait
    assert rm.pop_ref() is not None
    polls = []
    real_popitems = rm.all_refs.popitems

    def refill_on_second_poll(count=100):
        polls.append(count)
        if len(polls) == 2:
            store.add(f"{AAT}9999999##quaType")
        return real_popitems(count)

    rm.all_refs.popitems = refill_on_second_poll
    item = rm.pop_ref()
    assert item is not None, "gave up while a peer was still producing work"
    assert item[0] == f"{AAT}9999999##quaType"


def test_pop_ref_gives_up_once_the_queue_stays_empty():
    """The wait is bounded, so a finished phase still ends and no shared
    state is needed to decide that."""
    rm = popper(Store(0), workers=24, idle_timeout=0.05, idle_poll=0.01)
    start = time.time()
    assert rm.pop_ref() is None
    assert time.time() - start < 5, "idle wait should be bounded by the timeout"


def test_a_single_worker_does_not_wait_at_all():
    """With one process nothing else can enqueue, so an empty queue really is
    the end -- don't make single-source runs sit through the idle timeout."""
    rm = popper(Store(0), workers=1, idle_timeout=30, idle_poll=30)
    start = time.time()
    assert rm.pop_ref() is None
    assert time.time() - start < 1


# --- claim size shrinks as the queue drains ---------------------------------

def test_claim_size_spreads_the_tail_across_workers():
    rm = popper(Store(48), workers=24)
    # 48 left over 24 workers: take 2, not all 48
    assert rm._claim_size() == 2
    # plenty of work: the flat batch is still the right answer
    rm.all_refs = refmap(Store(100000))
    assert rm._claim_size() == 50
    # never zero, or the worker would claim nothing and spin
    rm.all_refs = refmap(Store(1))
    assert rm._claim_size() == 1


def test_claim_size_is_unchanged_for_a_single_worker():
    rm = popper(Store(48), workers=1)
    assert rm._claim_size() == 50


# --- write_done_refs --------------------------------------------------------

class StubConfigs:
    max_distance = 3

    def is_qua(self, recid):
        return "##qua" in recid


class StubIdMap:
    """Resolves each reference URI to a YUID. By default every URI gets its
    own, so nothing dedupes; `shared` maps URIs onto one YUID."""

    def __init__(self, shared=None):
        self.shared = shared or {}
        self.calls = 0

    def get_multi(self, keys, chunk=1000):
        self.calls += 1
        out = {}
        for k in keys:
            if k in self.shared:
                out[k] = self.shared[k]
            else:
                digest = f"{abs(hash(k)):032x}"[:32]
                out[k] = f"https://lux.collections.yale.edu/data/concept/{digest}"
        return out


def make_rm(store, idmap=None):
    rm = object.__new__(ReferenceManager)
    rm.configs = StubConfigs()
    rm.done_refs = refmap(store)
    rm.idmap = idmap if idmap is not None else StubIdMap()
    return rm


def test_write_done_refs_batches_and_filters(tmp_path, monkeypatch):
    store = Store(100)
    for i, k in enumerate(store.order):
        store[k] = {"dist": "9" if i < 10 else "1", "type": "Type"}
    rm = make_rm(store)

    monkeypatch.chdir(tmp_path)
    rm.write_done_refs()

    lines = (tmp_path / "reference_uris.txt").read_text().splitlines()
    assert len(lines) == 90, "references past max_distance must be excluded"
    assert all(line.startswith("1|") for line in lines)
    # was three round trips per reference
    assert rm.done_refs.conn.round_trips < 10


def test_write_done_refs_dedupes_by_yuid(tmp_path, monkeypatch):
    """Sibling URIs in one identity cluster must produce ONE line.

    iter_done_refs() slices this file by line number, so two lines for the
    same YUID go to different merge workers, which then build the same
    merged record and upsert the same rows in the same tables -- the
    cross-worker deadlock the merge phase hit once commits were deferred."""
    store = Store(10)
    yuid = "https://lux.collections.yale.edu/data/concept/" + "a" * 32
    cluster = {store.order[0]: yuid, store.order[3]: yuid, store.order[7]: yuid}
    rm = make_rm(store, StubIdMap(shared=cluster))

    monkeypatch.chdir(tmp_path)
    rm.write_done_refs()

    lines = [line.split("|", 2) for line in
             (tmp_path / "reference_uris.txt").read_text().splitlines()]
    assert len(lines) == 8, "the three-member cluster must collapse to one line"
    uris = [uri for (_, _, uri) in lines]
    assert len(set(uris) & set(cluster)) == 1
    # the surviving line carries the cluster's YUID, so merge doesn't have to
    # resolve it again in all 24 workers
    assert [y for (_, y, uri) in lines if uri in cluster] == [yuid]


def test_write_done_refs_keeps_refs_with_no_yuid(tmp_path, monkeypatch):
    """A reference the idmap doesn't know can't collide with anything, and
    run-merge reports it -- keep the line so the gap stays visible."""
    store = Store(5)
    idmap = StubIdMap()
    idmap.get_multi = lambda keys, chunk=1000: {k: None for k in keys}
    rm = make_rm(store, idmap)

    monkeypatch.chdir(tmp_path)
    rm.write_done_refs()

    lines = (tmp_path / "reference_uris.txt").read_text().splitlines()
    assert len(lines) == 5
