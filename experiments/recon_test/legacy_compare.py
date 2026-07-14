"""Comparative identity-phase build: legacy vs deterministic.

Replays all production concept clusters through BOTH identity mechanisms,
consuming byte-identical input records:

  legacy - the actual ReferenceManager.manage_identifiers code, called
           per record in (seeded) arrival order against a redis-shaped
           idmap, exactly as run-reconcile.py drove it before the
           deterministic refactor. uuid4 minting, first-wins merging,
           update-token rebuild pruning.
  new    - pipeline.process.identity: aggregate assertions -> voted
           constrained union-find -> prior-YUID-reuse assignment ->
           bulk apply to the same redis-shaped idmap.

Both run against fakeredis (in-process). Wall-times therefore EXCLUDE
network round-trips; redis command counts are reported so production
cost can be estimated (legacy pays per-record round trips, new pays
bulk pipelines).

Usage:
  python legacy_compare.py run --mode legacy --seed 1 --out /tmp/L1 \
      [--prior] [--limit 50000]
  (same for --mode new; then compare the mapping.tsv files)
"""

import argparse
import hashlib
import json
import random
import sys
import time
import uuid
import zlib
from collections import defaultdict
from pathlib import Path

import fakeredis
import lmdb
import orjson

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from pipeline.process.reference_manager import ReferenceManager
from pipeline.process import identity as det_identity
from pipeline.storage.idmap import redis as idmap_redis

DB_PATH = "/Users/wjm55/lux-concepts-scc/concepts.lmdb"
CONFLICT_SEED = 42
N_CONFLICTS = 1000


# ---------------------------------------------------------------------------
# Shared input: records derived from the production clusters
# ---------------------------------------------------------------------------

def iter_concepts(db_path, limit=0):
    env = lmdb.open(str(db_path), max_dbs=2, readonly=True, lock=False,
                    readahead=False, subdir=True)
    db = env.open_db(b"data")
    n = 0
    with env.begin(buffers=True, db=db) as txn:
        for key, val in txn.cursor():
            rec = orjson.loads(zlib.decompress(bytes(val)))
            yield str(uuid.UUID(bytes=bytes(key))), rec
            n += 1
            if limit and n >= limit:
                break
    env.close()


def is_internal(uri):
    return ".yale.edu" in uri


def cluster_records(members):
    """The same simulated per-source records as the earlier experiments:
    a chain of sorted members plus an internal hub re-asserting each link.
    Returns {record_id: [equivalent_ids]}."""
    recs = defaultdict(list)
    if len(members) == 1:
        recs[members[0]] = []
        return recs
    for i in range(len(members) - 1):
        recs[members[i]].append(members[i + 1])
    internal = [m for m in members if is_internal(m)]
    if internal:
        hub = internal[0]
        for m in members:
            if m != hub and m not in recs[hub]:
                recs[hub].append(m)
    return recs


def build_input(db_path, limit=0):
    """records: list of (record_uri, [equiv_uris]); prior: member->yuid;
    conflicts: [{cr, b, c}] with diff pairs (b, c)."""
    records = []
    prior = {}
    keys = []
    for rec_uuid, rec in iter_concepts(db_path, limit):
        members = set()
        for eq in rec.get("equivalent", []):
            if eq.get("id"):
                members.add(eq["id"].strip())
        members.discard(rec.get("id", ""))
        members = sorted(members)
        if not members:
            continue
        yuid = rec.get("id", "")
        for m in members:
            prior[m] = yuid
        for rid, eqs in cluster_records(members).items():
            records.append((rid, eqs))
        keys.append((rec_uuid, members))

    # conflict scenarios: record CR says b=c although b != c; a helper
    # record seconds the CR=c link so the C side has more votes
    keys.sort()
    rng = random.Random(CONFLICT_SEED)
    conflicts = []
    for i in range(N_CONFLICTS):
        (k1, m1), (k2, m2) = rng.sample(keys, 2)
        b, c = m1[-1], m2[0]
        if b == c:
            continue
        cr = f"test://conflict/{i}"
        helper = f"test://conflict-helper/{i}"
        records.append((cr, [b, c]))
        records.append((helper, [cr, c]))
        conflicts.append({"cr": cr, "b": b, "c": c})
    return records, prior, conflicts


# ---------------------------------------------------------------------------
# The idmap (fakeredis-backed, command-counted)
# ---------------------------------------------------------------------------

class CountingPipeline:
    """Counts each command queued/executed on a pipeline as one redis op
    (they still share round trips; round_trips counts the executes)."""

    def __init__(self, pipe, counter):
        self._pipe = pipe
        self._counter = counter

    def __enter__(self):
        self._pipe.__enter__()
        return self

    def __exit__(self, *a):
        return self._pipe.__exit__(*a)

    def __getattr__(self, name):
        attr = getattr(self._pipe, name)
        if callable(attr) and name not in ("reset",):
            def wrapper(*a, **kw):
                self._counter.ops += 1
                if name == "execute":
                    self._counter.round_trips += 1
                return attr(*a, **kw)
            return wrapper
        return attr


class CountingRedis:
    def __init__(self, conn):
        self._conn = conn
        self.ops = 0
        self.round_trips = 0

    def pipeline(self, *a, **kw):
        return CountingPipeline(self._conn.pipeline(*a, **kw), self)

    def __getattr__(self, name):
        attr = getattr(self._conn, name)
        if callable(attr):
            def wrapper(*a, **kw):
                self.ops += 1
                self.round_trips += 1
                return attr(*a, **kw)
            return wrapper
        return attr


class StubConfigs:
    internal_uri = "https://lux.collections.yale.edu/data/"
    ok_record_types = {"Type": "concept"}
    external = {}
    internal = {}
    debug_reconciliation = False

    def is_qua(self, recid):
        return "##qua" in recid

    def make_qua(self, recid, typ):
        if "##qua" in recid:
            return recid
        return f"{recid}##quaType"

    def split_qua(self, recid):
        return recid.split("##qua")


def make_idmap(token="__20260714__"):
    m = object.__new__(idmap_redis.IdMap)
    m.configs = StubConfigs()
    m.conn = CountingRedis(fakeredis.FakeStrictRedis(decode_responses=True))
    m._restoring_data_state = False
    m.prefix_map_in = {}
    m.prefix_map_out = {"yuid": StubConfigs.internal_uri}
    m.memory_cache_enabled = False
    m.memory_cache = {}
    m.clean_on_remove = False
    m.update_token = token
    return m


def load_prior(idmap, prior):
    """Preload last build's state: member##quaType -> yuid, yuid set of
    members WITHOUT the current update token (so legacy rebuild pruning
    engages, as it would with --new-token)."""
    conn = idmap.conn._conn
    pipe = conn.pipeline(transaction=False)
    n = 0
    for m, y in prior.items():
        qm = f"{m}##quaType"
        pipe.set(qm, y)
        pipe.sadd(y, qm, "__old__")
        n += 2
        if n % 10000 == 0:
            pipe.execute()
            pipe = conn.pipeline(transaction=False)
    pipe.execute()


def dump_mapping(idmap, out_file):
    """member (qua-stripped) -> yuid, sorted; only string keys."""
    conn = idmap.conn._conn
    rows = []
    for k in conn.scan_iter(count=10000):
        if "##qua" in k:
            v = conn.get(k)
            if v:
                rows.append(f"{k.split('##qua')[0]}\t{v}\n")
    rows.sort()
    with open(out_file, "w") as fh:
        fh.writelines(rows)
    return hashlib.sha256("".join(rows).encode()).hexdigest()


# ---------------------------------------------------------------------------
# Legacy mode: the real manage_identifiers, per record, arrival order
# ---------------------------------------------------------------------------

def run_legacy(records, idmap, seed):
    rm = object.__new__(ReferenceManager)
    rm.configs = idmap.configs
    rm.idmap = idmap
    rm.debug = False

    order = list(records)
    random.Random(seed).shuffle(order)
    t0 = time.time()
    for rid, eqs in order:
        rec = {"data": {"id": rid, "type": "Type", "_label": "",
                        "equivalent": [{"id": e, "type": "Type"} for e in eqs]}}
        rm.manage_identifiers(rec)
    return time.time() - t0


# ---------------------------------------------------------------------------
# New mode: assertions -> cluster -> assign -> bulk apply
# ---------------------------------------------------------------------------

def run_new(records, idmap, seed, diffs):
    order = list(records)
    random.Random(seed).shuffle(order)
    t0 = time.time()
    edges = defaultdict(set)
    for rid, eqs in order:
        qrid = f"{rid}##quaType"
        if not eqs:
            edges[(qrid, qrid)].add(qrid)
        for e in eqs:
            qe = f"{e}##quaType"
            a, b = (qrid, qe) if qrid < qe else (qe, qrid)
            edges[(a, b)].add(qrid)

    qdiffs = set()
    for (a, b) in diffs:
        qa, qb = f"{a}##quaType", f"{b}##quaType"
        qdiffs.add((qa, qb) if qa < qb else (qb, qa))

    clusters, conflicts = det_identity.cluster(edges, qdiffs)
    nodes = set()
    for (a, b) in edges:
        nodes.add(a)
        nodes.add(b)
    prior = det_identity.fetch_prior(idmap, nodes)
    yuids = det_identity.assign(clusters, prior, idmap.configs)
    stats = det_identity.apply_assignments(idmap, clusters, yuids, prior)
    stats["conflicts_refused"] = len(conflicts)
    return time.time() - t0, stats


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["legacy", "new"], required=True)
    ap.add_argument("--db", default=DB_PATH)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--out", required=True)
    ap.add_argument("--prior", action="store_true",
                    help="preload production idmap state (rebuild scenario)")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    records, prior, conflicts = build_input(args.db, args.limit)
    prep_s = time.time() - t0

    idmap = make_idmap()
    if args.prior:
        load_prior(idmap, prior)
    idmap.conn.ops = 0
    idmap.conn.round_trips = 0

    diffs = [(c["b"], c["c"]) for c in conflicts]
    if args.mode == "legacy":
        wall = run_legacy(records, idmap, args.seed)
        extra = {}
    else:
        wall, extra = run_new(records, idmap, args.seed, diffs)

    sha = dump_mapping(idmap, out / "mapping.tsv")
    stats = {
        "mode": args.mode, "seed": args.seed, "prior": args.prior,
        "records": len(records), "prep_s": round(prep_s, 1),
        "identity_wall_s": round(wall, 1),
        "redis_ops": idmap.conn.ops,
        "redis_round_trips": idmap.conn.round_trips,
        "mapping_sha256": sha,
        **extra,
    }
    with open(out / "stats.json", "w") as fh:
        json.dump(stats, fh, indent=2)
    with open(out / "conflicts_expected.json", "w") as fh:
        json.dump(conflicts, fh)
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
