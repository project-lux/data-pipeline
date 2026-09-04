# Moving the identity map and reference queues from Redis to Postgres

Two migrations, done in that order:

* the **identity map** — the `uri -> yuid` / `yuid -> {uris}` union-find behind
  `cfgs.get_idmap()`;
* the **reference queues** — `all_refs` and `done_refs`, the pending-reference
  work queue and distance map that reconcile drives.

This is the runbook, the reasoning, and the measurements, including the ideas
that were tried and discarded. Redis is now down to one map that is genuinely
used (`networkmap`) plus several that are dead or ignorable — see "What is left
in Redis".

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

Those figures are for a 128 GB development box. **Size to the machine**: the
production server is 80 GB / 32 vCPU with Postgres on an AWS io2 volume, where
the right numbers are `shared_buffers = '20GB'` (25% of RAM) and
`effective_cache_size = '56GB'`.

`shared_buffers` and `max_connections` need a full restart, not a reload.

`max_connections` is not an optimisation, it is a correctness requirement:
each worker opens **four** connections — two from `PoolManager`, one shared by
the idmap, one shared by the two reference maps — so 48 workers need 192. With
Redis it was 96, just under the old limit of 100, which is why this never bit
before. The map stores deliberately share connections rather than taking one
each; a connection per map would be six a worker, or 288.

### WAL, for a write-heavy phase on provisioned-IOPS storage

Reconcile commits per record-cache write by design (see the "DO NOT defer
commits here" comment in `run-reconcile.py`), so 24 workers generate a storm of
tiny transactions. Each one needs a WAL flush, and on io2 an fsync is
~0.5–1 ms, so they serialise through `WALWriteLock`. Measured mid-phase before
these settings: `LWLock/WALWriteLock` was 40.9% of active backend time and
`COMMIT` was 51.5% of statements seen running. After, both effectively vanish.

```sql
ALTER SYSTEM SET synchronous_commit = off;      -- COMMIT stops waiting for the flush
ALTER SYSTEM SET max_wal_size = '32GB';         -- the 1GB default is far too small here
ALTER SYSTEM SET min_wal_size = '2GB';
ALTER SYSTEM SET checkpoint_timeout = '30min';
ALTER SYSTEM SET checkpoint_completion_target = 0.9;
ALTER SYSTEM SET commit_delay = 200;            -- group commit, microseconds
ALTER SYSTEM SET commit_siblings = 10;
ALTER SYSTEM SET wal_compression = on;          -- trade CPU (there is plenty) for WAL volume
ALTER SYSTEM SET wal_buffers = '64MB';
```

**What `synchronous_commit = off` costs you.** A crash loses the last ~0.6 s of
commits. Everything reconcile and merge write is reconstructible — record
caches are caches, the reference queues are rebuilt every build, and reconcile
makes no identity writes at all (measured: zero idmap calls). The exception is
`run-identify.py`, which does write identity: a crash there is recoverable but
not resumable, because identity is deterministic from the assertion files, so
the fix is to re-run identify from the start rather than continue. Turn it back
on around that phase if you would rather not carry that.

After a restart the buffer pool is empty, so prewarm rather than paying for it
during the first run:

```sql
CREATE EXTENSION IF NOT EXISTS pg_prewarm;
SELECT pg_prewarm('idmap_pkey'), pg_prewarm('idmap_yuid_idx'), pg_prewarm('idmap');
```

## The reference queues

`all_refs` and `done_refs` are `ReferenceMap`s: a URI maps to a small record of
`dist` (how far the reference is from a record being processed) and `type`.
`all_refs` is the pending-work queue reconcile drains; `done_refs` records what
has been handled and is flushed to `reference_uris.txt` for merge.

```sql
CREATE TABLE all_refs (
    uri    TEXT PRIMARY KEY,
    dist   INTEGER,
    ctype  TEXT
) WITH (fillfactor = 70,
        autovacuum_vacuum_scale_factor = 0.02,
        autovacuum_vacuum_threshold = 5000,
        autovacuum_vacuum_cost_delay = 0,
        autovacuum_analyze_scale_factor = 0.05);
```

The storage parameters are not decoration. This is a queue table — every
reference is inserted, updated by later merges, then deleted when claimed — and
Postgres's defaults (vacuum at 20% dead, throttled) are wrong for that. Tables
created before this change need the settings applied once:

```sql
ALTER TABLE all_refs, done_refs SET (fillfactor = 70, ...);
VACUUM (ANALYZE) all_refs;
```

### The two interesting operations

Both of the things that made the Redis version need Lua collapse into single
statements.

`MERGE_REF_LUA` — `dist` becomes `min(existing, new)`, `type` is set only if
not already set — is one upsert. `least()` is the distance test; `coalesce()`
is the `HSETNX`. An absent type is stored as `''` rather than NULL on purpose,
because Redis `HSETNX`s `ctype or ""`, so a reference first seen without a type
keeps the empty one.

`POP_REF_LUA` — read-and-delete so exactly one worker gets a reference — is the
standard Postgres work queue:

```sql
DELETE FROM all_refs WHERE uri IN (
    SELECT uri FROM all_refs LIMIT %s FOR UPDATE SKIP LOCKED
) RETURNING uri, dist, ctype
```

No SCAN cursor to carry between calls, no window for a concurrent merge to slip
a shorter distance into a row about to vanish, and workers step over each
other's claims rather than queueing. Verified with 12 workers claiming
concurrently: 20,000 claimed, 20,000 distinct, zero duplicates, zero missing.

### Three things that only appear under real concurrency

**Lock ordering.** `ON CONFLICT DO UPDATE` takes row locks in the order the
VALUES list gives them, so two workers whose batches overlap in opposite orders
each hold a row the other wants. Reference walks produce arbitrary order, so
this is the normal case: unsorted, **575 of 600** overlapping merges deadlocked
in a 24-worker test. `merge_refs` now sorts each batch by URI, so every
transaction takes its locks in the same global order and a cycle cannot form —
600 of 600, and 1440 of 1440 on a heavier run. `delete_multi` sorts for the
same reason. Deadlocks are also retried with jittered backoff, because a
concurrent `set()` can still create one; replaying is safe because
`least()`/`coalesce()` make a batch idempotent.

*Redis never had this concept.* Atomicity came from single-threaded execution,
so acquisition order could not exist. Moving to row locks makes it a
correctness property that nothing in the old code had to think about.

**No-op merges.** `resolve_refs` used to re-merge a reference unconditionally
whenever it was already in `all_refs` — the common case, because references
converge on the same few thousand concepts. On Redis that was one `HSET`: no
MVCC, no locks, microseconds. On Postgres each one takes a row lock, writes a
new tuple version, leaves a dead one, and blocks every other worker wanting
that row. Measured mid-phase: **7,882 live rows against 173,981 dead (95.7%)**,
with **78.7%** of active backend time waiting on `Lock/transactionid`.

Fixed on both sides. `resolve_refs` now checks the record it has already read
and skips a merge that cannot change anything; `merge_refs` guards the update
with `WHERE dist IS DISTINCT FROM least(...) OR ctype IS DISTINCT FROM
coalesce(...)` for the races where two workers both decide to write. Safe on a
stale read because both fields move one way only — `dist` is minimised, `ctype`
is set once — so if the read says no change is needed, a fresher value says so
too. Result: dead tuples went to **0.0%** and `Lock/transactionid` disappeared
from the profile entirely.

**Commit latency holding row locks.** In autocommit every merge is its own
transaction and its row locks are held until the WAL sync completes, so on
storage where fsync costs milliseconds the workers queue behind each other's
*syncs*. The reference maps therefore take their own connection pool with
`synchronous_commit = off` — their contents are rebuilt from nothing every
build, so durability is worth nothing. Set `"asyncCommit": false` in the map
config to disable it. The separate pool exists so the setting cannot leak onto
the identity map.

### Queue length, and a trap worth naming

`_claim_size()` scales the claim batch to what is left in the queue, so the
tail spreads across workers rather than landing on whoever asked first. On
Redis that was `DBSIZE`: exact and O(1).

The `reltuples` estimate that works for the idmap is *useless* here — this
table is created and filled inside a single run, so autovacuum never analyses
it and the estimate reads 0 for the whole phase. With `len()` returning 1,
`min(ref_batch, remaining // ref_workers)` makes every worker claim **one
reference at a time**.

So `queue_length(ceiling)` counts no further than `ref_batch * ref_workers`,
above which the answer cannot change the decision — an index-only scan of a few
thousand rows instead of counting a queue of millions on every claim. `len()`
stays exact for reporting. Both backends implement it; on Redis it is
`min(DBSIZE, ceiling)` and free. Claim scaling was verified identical across
backends at 0/1/50/500/2400/5000/20000 queued.

### Migration and switch

```bash
python migrate-refs.py          # usually unnecessary -- see below
```

Both maps are transient: `all_refs` is the pending queue, `done_refs` is
drained into `reference_uris.txt`, and `manage-data.py` clears both between
builds. A fresh reconcile refills them from nothing. It matters in one case: a
build interrupted partway through the reference pass, where whatever is left in
`all_refs` is unprocessed work that would be lost when you switch backends.

Then one line in each of `map_allrefs.json` and `map_donerefs.json`:

```json
"storeClass": "storage.idmap.postgres.ReferenceMap",
```

Table names come from the map's `name`, so `all_refs` and `done_refs`, created
on first use. As with the idmap, the Redis `host`/`port`/`db` in those files are
ignored by the Postgres backend but should stay — `migrate-refs.py` reads them
to know where to copy *from*.

## Diagnosing a slow phase

```bash
python diagnose-refs.py --seconds 30
```

Read-only, safe against a live build. Samples `pg_stat_activity` and reports
backend states, what the active ones are waiting on, the statements actually
running, live/dead ratios per table, and ungranted locks. Reading it:

| what you see | what it means |
|---|---|
| `Lock/transactionid` | row-lock contention — workers writing the same rows |
| `LWLock/WALWriteLock`, `IO/WALSync` | commit storm; see the WAL settings above |
| `IO/DataFileRead` | working set does not fit `shared_buffers` |
| `Client/ClientRead` | Postgres waiting for Python — the bottleneck is not here |
| high dead-tuple ratio | vacuum is losing; check the storage parameters |

Two figures to read carefully. A wait percentage is a share of the **active**
backends only, so 40% of a 10% active population is 4% of the whole. And
`idle in transaction` is expected to be high: each worker holds a server-side
cursor open for the whole phase while streaming its slice of record ids, so
roughly one connection per worker is legitimately in a transaction at all
times. It is worth watching only because it blocks DDL — `clear()`, a table
rewrite — not because it is itself a problem.

## What is left in Redis

| db | map | state |
|---|---|---|
| 0 | idmap | migrated |
| 3 / 4 | all_refs / done_refs | migrated |
| 8 | record_refs | dead: never instantiated, 0 keys |
| 5, 6, 10 | test idmaps | ignorable |
| 9 | redis cache backend | unused: no config references it |
| 7 | redirects | never read by this repo, but holds ~7k old-YUID → new-YUID pairs that look like retired-URI redirects for the public site. Find its owner before flushing. |
| 2 | **networkmap** | the only real port left |

`networkmap` is the HTTP fetch cache: URL → status or redirect target, with a
7-day TTL on errors (`NETWORK_ERROR_TTL`). Four source fetchers read it
ungated — bnf, dnb, getty, oclc — so it is genuinely used. But the *base*
fetcher's read is behind `use_networkmap`, which is `False` and set to `True`
nowhere, while its writes are unconditional. So most of the 6.3M entries are
written by fetchers that never read them. Decide that before porting: it is the
difference between migrating millions of rows and a few hundred thousand.

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

The reference queues hit the same class of problem twice more, in ways the
identity map never could: the `reltuples` estimate that rescues `len()` there
is useless on a table created and filled within one run, and lock *ordering*
became a correctness property that single-threaded Redis had made impossible to
get wrong. Both are written up under "The reference queues".

The general shape: when swapping a store behind an established interface, the
dangerous operations are not the obviously expensive ones. They are the ones
the old backend made free, because nothing in the calling code was ever written
to avoid them.

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

`ReferenceManager.resolve_refs()` no longer re-merges a reference that cannot
change — see "No-op merges" above. `_claim_size()` asks `queue_length(ceiling)`
rather than `len()`, with a fallback to `len()` for backends that do not
provide it.


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
| `migrate-refs.py` | Redis -> Postgres for the reference queues, with verification |
| `diagnose-refs.py` | samples a live build: backend states, wait events, statements, table health, lock waits |
