# Moving the identity map from Redis to Postgres

The identity map — the `uri -> yuid` / `yuid -> {uris}` union-find behind
`cfgs.get_idmap()` — has moved from Redis to Postgres. This is the runbook,
the reasoning, and the measurements, including the ideas that were tried and
discarded.

## Why

One Redis process held 97.5M keys in 23.9 GB, and it persists by `fork()`.
Under a build with parallel writers most of the keyspace is touched during the
save, so peak resident memory approached twice the dataset — at the moment
Postgres was also asking for memory. The stock save policy (`3600 1 300 100 60
10000`) meant *every 60 seconds in which 10,000 keys changed*, which during a
build is continuously true.

Four of the five maps on that instance are rebuildable scratch data. The fifth
— the identity map — is the one thing in the pipeline that cannot be
regenerated, because YUIDs must stay stable or every published URI changes. So
persistence was being paid for on everything in order to protect the one map
with a natural relational shape.

## Result

One merge slice (`run-merge.py 0 48`, ~983k records), same machine, same data:

| run | idmap | shared_buffers | wall | ms/record |
|---|---|---|---|---|
| 1 | redis | 128 MB | 53m 59s | 3.30 |
| 2 | postgres | 128 MB | 57m 59s | 3.54 |
| 3 | postgres + LMDB tier | 32 GB | 45m 29s | 2.78 |
| 4 | **postgres** | **32 GB** | **38m 00s** | **2.32** |

**30% faster per slice, Redis deleted from the architecture, 24 GB returned.**

Two findings worth keeping:

* The backend swap and the server tuning were never separable. Postgres looked
  *worse* than Redis (run 2) until it was configured to use the machine it runs
  on. 128 MB of buffers for a 25 GB identity map was always going to lose to an
  in-memory store.
* The LMDB read tier (run 3) is a **net cost** once the buffer pool is right —
  7.5 minutes a slice, plus 5.4 minutes to build it. It has been removed. See
  "Discarded" below.

## Schema

```sql
CREATE TABLE idmap (
    uri   TEXT PRIMARY KEY,
    yuid  TEXT NOT NULL
);
CREATE INDEX idmap_yuid_idx ON idmap (yuid);

CREATE TABLE idmap_yuid (
    yuid    TEXT PRIMARY KEY,
    token   TEXT,            -- was a __YYYYMMDD__ pseudo-member of the set
    minted  TIMESTAMP DEFAULT now()
);
```

The reverse direction is **not stored**. A YUID's members are the rows carrying
it, so the forward pointer and the member set cannot disagree, and the union
that Redis needs a fifty-retry `WATCH`/`MULTI` loop for is one statement:

```sql
UPDATE idmap SET yuid = $new WHERE yuid = $old;
```

The `idmap_yuid` table keeps `value in idmap` meaningful for a YUID with no
members yet, which `set()` checks before assigning.

## Migration

Steps 1–3 need no config change: every script takes its targets explicitly, so
the whole thing is validated while Redis is still live and authoritative.

```bash
# 1. Copy redis -> postgres and verify.  ~17 min for 97.5M keys.
python migrate-idmap.py --verify-pct 0.1

# 2. Prove the backends agree, on real keys, reads and writes.
python compare-idmap.py --keys 20000 --writes

# 3. Measure, if you want your own numbers.
python bench-idmap-workload.py --source yuag --records 3000 \
    --stages reidentify,merge --backends redis,postgres
```

Step 4 is the switch — one line in `config_cache/map_idmap.json`:

```json
"storeClass": "storage.idmap.postgres.IdMap",
```

Connection details are **not** read from that file. `instantiate_map()` passes
`map_idmap.json`, whose `host`/`port` are Redis's `localhost:6379`; the backend
takes its connection from the caches config instead, and honours
`pgHost`/`pgPort`/`pgUser`/`pgDbname` overrides if the map ever lives elsewhere.

Rollback is putting `storage.idmap.redis.IdMap` back — instant, until the first
build writes to Postgres, after which Redis is stale. For a parallel
comparison of a whole build, snapshot Redis before switching.

## Server settings

**These matter more than the backend choice.** Postgres.app ships defaults sized
for a laptop; the identity map is a 25 GB table (8.5 GB heap, 5.9 GB primary
key, 3.2 GB reverse index).

```sql
ALTER SYSTEM SET shared_buffers = '32GB';        -- was 128MB
ALTER SYSTEM SET effective_cache_size = '96GB';  -- planner hint, not an allocation
ALTER SYSTEM SET work_mem = '32MB';              -- was 4MB
ALTER SYSTEM SET maintenance_work_mem = '2GB';   -- was 64MB
ALTER SYSTEM SET max_connections = 200;          -- was 100
```

`shared_buffers` and `max_connections` need a full restart, not a reload.

`max_connections` is not an optimisation, it is a correctness requirement:
each worker opens **three** connections (two from `PoolManager`, one for the
idmap), so 48 workers need 144. With Redis it was 96, just under the old limit
of 100 — which is why this never bit before.

After a restart the buffer pool is empty, so prewarm rather than paying for it
during the first run:

```sql
CREATE EXTENSION IF NOT EXISTS pg_prewarm;
SELECT pg_prewarm('idmap_pkey'), pg_prewarm('idmap_yuid_idx'), pg_prewarm('idmap');
```

## Cost model, if you are changing the backend

Redis and Postgres have very different cost profiles behind the *same*
interface, and the traps are where Redis is O(1) and Postgres is O(n) — because
every caller was written against Redis's costs and nobody wrote that down.

The one that bit: `base Mapper.__init__` does

```python
idmap_has_data = len(idmap)
```

a truthiness test. On Redis that is `DBSIZE`, O(1). Implemented literally on
Postgres it is `count(*)` over 97M rows — **7.5 seconds**, and
`instantiate_all()` builds a mapper per source, so **3.7 minutes of startup per
process** before any work began. `__len__` now answers from `pg_class.reltuples`
(3 ms), falling back to an existence check when the estimate is non-positive.

`keys()` has the same shape and is expensive on both, but at least obviously so
at the call site.

## Discarded

**An LMDB read tier.** A frozen `uri -> yuid` / `yuid -> members` snapshot built
from Postgres after identity resolution, read during merge and export, with
writes forbidden while enabled. Per-lookup it was genuinely faster — 2.1 µs
against Postgres's 18.6 µs on merge-body lookups, 100% hit rate. It still lost:

* 5.4 minutes to build 97M entries (20 GB), serial, every run.
* Once `shared_buffers` was raised it made the slice **7.5 minutes slower**,
  because it moved lookups off a warm 32 GB buffer pool and onto a 20 GB
  memory-mapped file the OS had to fault in.

The tier and a correctly sized buffer pool solve the same problem. The buffer
pool wins, and costs nothing to run.

**Scoping the snapshot by record type.** Objects and works are effectively
immutable — measured at production scale, 0.05% of their classes have more than
one member, against 49.5% for people, places and concepts. That made them the
safe subset to hold in a file that outlives a run. But it also meant the tier
served only 7.9% of the merge read path, because a record's *references* are
people and concepts whatever the record is.

**Maintaining the snapshot incrementally** rather than rebuilding — applying the
adds, moves and dead YUIDs that `assign_bulk` already computes, in one atomic
LMDB transaction. Sound design; moot once the tier itself lost.

## Related changes

`IdentityResolver` used to reach past the interface into `idmap.conn` with Redis
pipelines in five places, so `run-identify.py` could not run on any other
backend. Those now go through `get_multi()`, `assign_bulk()` and
`delete_empty_yuids()`, implemented on all three backends. Verified to produce
identical map state and identical stats on Redis and Postgres.

The Postgres backend uses its own connection in autocommit, deliberately not
joining the record caches' deferred batches: identity has to be visible to other
workers the moment it is assigned, and a read must not leave a transaction open
holding a lock. Hot single-row statements are server-side prepared.

## Tools

| script | what it does |
|---|---|
| `migrate-idmap.py` | Redis -> Postgres, with verification |
| `compare-idmap.py` | asks both backends the same questions on real keys; `--writes` exercises mint/merge/token/delete |
| `idmap-survey.py` | read-only survey of the Redis map: class sizes, 1:1 share, per-namespace and per-type breakdown, `--fingerprint`/`--diff` for churn between builds |
| `idmap-bench.py` | microbenchmark: COPY ingest, index build, point/batched/reverse lookups, union cost |
| `bench-idmap-workload.py` | the real code paths — reconcile, identify, reidentify, merge — over real records, per backend |
