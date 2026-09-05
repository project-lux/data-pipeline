# Pipeline performance: what was found, what changed, what is left

Written after the first full overnight build on postgres-only (no redis), from
the four-phase timing report the `PhaseTimer` JSON produces. Two changes have
landed off the back of it; the rest of what the report pointed at is recorded
here so it can be picked up **after a re-run confirms the first two**.

Read this top to bottom before touching anything: the order matters, because
the two landed changes move the numbers the remaining items were sized
against.

---

## 1. Baseline: the overnight run (postgres only, 24 workers)

| phase | records | wall | aggregate | worker-hrs | client cpu |
|---|---|---|---|---|---|
| merge | 43,843,923 | 136.7 min | 5,345/s | 54.7 | 41% |
| merge-refs | 4,187,592 | 8.3 min | 8,402/s | 3.3 | 11% |
| reconcile | 43,843,923 | 118.7 min | 6,158/s | 47.4 | 37% |
| reconcile-refs | 10,117,322 | 37.0 min | 4,558/s | 14.7 | 31% |

Per-slice spread was 0–4% in every phase, so the partitioning is not the
problem and there is no straggler to chase. Timing is at least as good as the
redis build it replaced.

Stage breakdown, worker-hours (from the same report):

```
merge            reidentify 13.65 | write_merged 11.33 | write_rewritten 10.80
                 idmap_forward 4.54 | idmap_cluster 4.01 | merge 3.67
                 final_transform 1.50 | (unattributed) 4.48
reconcile        acquire 30.12 | reconcile 8.70 | walk_refs 7.16
reconcile-refs   claim 6.87 | reconcile 4.19 | walk_refs 1.74 | acquire 1.26
merge-refs       claim_member 2.36 | idmap_equivs 0.34
```

---

## 2. The measurement gap — close this on the re-run

**`PhaseTimer` measures client CPU only.** 41% of 24 workers is roughly 10
cores busy in python; the rest of a 32-vCPU box is postgres backends servicing
300M+ statements per phase. "Workers are off-cpu 59% of the time" therefore
does **not** establish that the box is waiting on disk — the likelier reading
is that the workers are waiting on a server competing for the same cores.
Nothing below can be prioritised properly until that is settled.

Collect during the next run:

* `pg_stat_statements` ranked by `total_exec_time` **and** by `calls`. Needs
  `pg_stat_statements.track = all` and a `pg_stat_statements_reset()`
  immediately before the phase starts.
* Sampled `pg_stat_activity` wait events. The previous pass found
  `LWLock/WALWriteLock` at 40.9% of active backend time and `COMMIT` at 51.5%
  of running statements; re-check now that redis is gone and the WAL settings
  in `idmap-migration.md` are in place.
* `pidstat` split between the python workers and the postgres backends, so the
  "off-cpu" share can be attributed.
* `SHOW synchronous_commit`. `docs/idmap-migration.md` prescribes `off` and
  reconcile is the phase it was written for, but **nothing in the code sets it
  for the record-cache pool** — only the reference maps set it for themselves
  (`pipeline/storage/idmap/postgres.py:95`). If it reads `on`, that alone may
  explain reconcile's profile and several items below are moot.
* `n_dead_tup` on `all_refs` and `done_refs` throughout reconcile — see the
  caveat in §3.2.

Also re-run one slice with the `acquire.*` sub-stages, which were added in
`96c85e4` **after** the overnight run and so are absent from the report above.
Reconcile's `acquire` is 63.5% of the phase and currently a single opaque
number; `acquire.cache_hit` / `.fetch` / `.map` / `.post_map`
(`pipeline/process/base/acquirer.py`) split it four ways.

---

## 3. Landed — assess these two first

### 3.1 The upsert sent every document twice

`pipeline/storage/cache/postgres.py:787`

`PooledCache.set()` built `... ON CONFLICT (key) DO UPDATE SET (cols) =
(%s,...)` and passed the parameter list twice (`qvs * 2`). psycopg2's `Json`
adapter re-runs `dumps` on every adaptation — `getquoted()` does no caching,
confirmed in the installed source — so the same `Json` object appearing twice
in the tuple meant the record was serialised twice client-side, crossed the
socket twice, was parsed into jsonb twice server-side, and doubled the WAL
those writes generate. Every record write, in every phase.

Now `SET col = EXCLUDED.col`, one parameter per column. Overwrite semantics are
unchanged: every column the INSERT names is still assigned on conflict, which
`tests/test_upsert_single_payload.py` asserts column-by-column so a later
column addition cannot silently drop out of the update path.

**Expected to move:** merge's `write_merged` (11.33 wh) + `write_rewritten`
(10.80 wh) = 40% of the phase's worker time, and the `post_map` write inside
reconcile's `acquire`.

**Verify:** those two stages in the new report; WAL volume over a fixed record
count (`pg_current_wal_lsn()` delta); and in `pg_stat_statements` the
normalised text now carries one set of placeholders, not two.

### 3.2 Reconcile fetched every row it had just listed

`run-reconcile.py:154`, with `data=` threaded through
`pipeline/process/base/acquirer.py` (`do_fetch` / `acquire` / `acquire_all`).

The loop walked `iter_keys_slice()` and handed each key to `acquire()`, which
SELECTed that same row straight back — one round trip and one large jsonb parse
per record, 43.8M of them. It now streams `iter_records_slice()` and passes the
row it already holds, which is the change `run-merge.py:176` already made.

The partition is unchanged (`iter_records_slice` hashes the same key column
`iter_keys_slice` did). `refetch=True` still ignores a supplied row and goes to
the network; the recordcache pre-check in `acquire()` is untouched, so
incremental builds still short-circuit. `row["source"]` is stamped from
`in_db.config["name"]`, which is literally what `get()` used to set — the
iterators do not set it and the mappers and reconciler read it. Pinned in
`tests/test_acquire_supplied_row.py`.

**⚠ The caveat to watch.** `iter_records_slice` uses a server-side cursor, and
a cursor holds its snapshot for its whole life — for ILS that is ~112 minutes.
A held snapshot pins the **cluster-wide** vacuum horizon, so autovacuum cannot
reclaim dead tuples in *any* table while it is open, `all_refs` and `done_refs`
included. Merge already does this, but merge does not churn the reference
queue; reconcile does, in the same process, at the same time — and that
queue's whole performance story depends on aggressive vacuuming (measured, see
the comment at `pipeline/storage/idmap/postgres.py:720`: a poll costs 1.1 ms
bloated against 0.1 ms after a `VACUUM`).

So on the re-run, watch whether **`reconcile-refs`' `claim` gets worse** even
as `acquire` gets better, and watch `n_dead_tup` on `all_refs` during
reconcile. If it bites, the fix is keyset pagination rather than one long
cursor:

```sql
SELECT * FROM <datacache>
WHERE (hashtext(identifier::text) & 2147483647) % :max = :slice
  AND identifier > :last
ORDER BY identifier LIMIT 10000
```

which still removes 9,999 of every 10,000 round trips but keeps each snapshot
alive for seconds. `identifier` is the primary key on every `DataCache`
(`pipeline/storage/cache/postgres.py:1140`), and the hash is computable from
the index key, so it stays an index scan.

---

## 4. Backlog, in the order worth doing

### 4.1 Collapse `idmap_forward` + `idmap_cluster` into one query — merge, 8.55 wh (16%)

**Where:** `run-merge.py:192` and `run-merge.py:211`.

Two sequential single-row round trips per record for what is one answer: the
record's YUID, then that YUID's member set. 372 µs + 329 µs each, on a
prepared, indexed, unix-socket lookup — which is itself evidence that the
server is saturated rather than that the query is slow.

One query gives both, since membership is derived from the `yuid` column:

```sql
SELECT uri FROM idmap WHERE yuid = (SELECT yuid FROM idmap WHERE uri = $1)
```

The YUID comes back from any returned row (or use a CTE returning both
columns). Add it to `IdMap.PREPARED` (`pipeline/storage/idmap/postgres.py:159`)
as e.g. `cluster_of`, and give it a method — `get_cluster(key)` returning
`(yuid, members)` — rather than open-coding SQL in `run-merge.py`.

Watch out for: the memory cache. `idmap_equivs` costs 13.5 µs against
`idmap_cluster`'s 329 µs precisely because the cluster was already cached by
the earlier lookup, so the new method must populate `memory_cache` under both
keys exactly as `get()` does, or the reidentifier's prefetch hop stops being
free.

**Expected:** ~4 wh. **Verify:** `idmap_forward` and `idmap_cluster` collapse
into one stage at roughly the cost of one of them, and `idmap_equivs` stays in
the tens of microseconds.

### 4.2 Batch the record-cache writes — merge, re-size after §3.1

**Where:** `PooledCache.set()` / `_upsert()`
(`pipeline/storage/cache/postgres.py:751`, `:687`).

One INSERT per record. Commits are already deferred every 500
(`run-merge.py:108`), so the transaction boundary is not the constraint — the
per-statement round trip and parse is. Buffer 25–100 rows and emit them with
`psycopg2.extras.execute_values`.

Do this **after** measuring §3.1, because halving the payload may already have
taken most of what is available here, and batching is the more invasive change
of the two.

Constraints to respect:
* `_upsert()`'s deadlock replay (`pipeline/storage/cache/postgres.py:687`) and
  `PoolManager.deferred_stmts` assume one statement per record. A batched
  statement must be replayable the same way — it is, since these are all
  idempotent upserts, but the bookkeeping needs updating together.
* `checkpoint()` must still land on a record boundary, so the row buffer has to
  be flushed at checkpoint time, not independently of it.
* Ordering: `merge_refs` sorts its batches to keep a consistent lock order
  across workers (`pipeline/storage/idmap/postgres.py:840`). Merge workers are
  key-disjoint by `claim_member()`, so batched cache writes should not need it
  — but if `deadlock detected` appears in a merge log after this change, that
  assumption is what broke, and sorting each batch by key is the cheap
  insurance.
* `start_bulk`/`set_bulk`/`end_bulk` (`:871`) already exist for a
  non-overwrite bulk path. Read them before writing a third write path.

### 4.3 `reidentify` — merge, 13.65 wh (25%), currently one opaque number

**Where:** `pipeline/process/reidentifier.py`.

1127 µs/call covers three different things and no one knows the split:

* the `get_multi` round trip in `prefetch()` (`:115`),
* `_collect_keys()`'s walk of the whole record tree (`:97`),
* `_reidentify()`/`process_entity()`'s **second** walk of the same tree
  (`:294`, `:156`).

Do them in this order:

1. **Instrument.** `timing.stage("reidentify.prefetch")` and
   `reidentify.walk`. Cheap, and it decides whether the next two are worth
   anything.
2. **Raise the idmap memory cache.** `memoryCacheSize` defaults to 200,000
   (`pipeline/storage/idmap/postgres.py:127`) and `get_multi` consults it
   (`:297`). Over 43.8M records the qua'd external keys repeat heavily
   (AAT, ULAN, common concepts), and every hit both saves a round trip and
   shrinks the `ANY()` array. `idmap_equivs` at 13.5 µs is the proof the cache
   works when it hits. Instrument the hit rate before and after; this is the
   cheapest lever in the whole document.
3. **Merge the two tree walks.** `_collect_keys` and `_reidentify` recurse over
   the same structure by the same rules — the comment at `:64` warns that
   `_node_keys` "MUST stay in step with process_entity", which is the smell.
   Collect the keys during the first walk and reuse them, or key the collected
   node→keys mapping by `id(node)`. This is pure client CPU, so its value
   depends on §2's answer about whether the box is CPU-saturated overall.

Also unprepared: `IdMap.get_multi` builds its `uri = ANY(%s)` SQL as an
f-string and takes a fresh cursor each call, so postgres parses and plans it
43.8M times. Preparing it is free and independent of the above.

### 4.4 Reconcile's guaranteed-miss recordcache probe — 43.8M pointless probes

**Where:** `pipeline/process/base/acquirer.py:120-134`.

On a full rebuild (`manage-data.py --clear-all`) the recordcache is empty, so
the pre-check at the top of `acquire()` misses for every single record. On an
incremental build that same check is what makes the phase fast, so it cannot
just be deleted — it needs a flag. `pipeline/process/base/acquirer.py:17` has
`# self.force_rebuild = config.get("force_rebuild", False)` commented out,
which is where this was heading already.

Wire it to a `--rebuild` argument on `run-reconcile.py` and skip the probe when
set. **Verify** via `acquire.cache_hit` (§2) — on a full rebuild it should
drop to zero calls.

### 4.5 `reconcile-refs` claim starvation — 6.87 wh (46.6% of the phase)

**Where:** `pipeline/process/reference_manager.py:197` (`_claim_size`), `:255`
(`pop_ref`), `:44` (`ref_batch = 50`).

`_claim_size()` calls `queue_length(ceiling)` on **every** buffer refill and
returns `min(ref_batch, remaining // ref_workers)`. With 24 workers that
floors to 1 whenever fewer than 24 references are visible — so through the
whole long tail each record costs a count query *plus* a `DELETE ... FOR UPDATE
SKIP LOCKED`, which is how a per-record average of 2437 µs arises from what
should be one claim per 50 records.

* Keep a floor of ~8–16 rather than 1.
* Only re-measure the queue when the previous claim came back short, or every
  N claims — not on every refill.
* Raise `ref_batch` from 50.

Note the tension with `_wait_for_refs()` (`:226`): claims must stay small
enough at the end that one worker cannot inherit the whole remaining
expansion. That is the problem `_claim_size` was written to solve, so the fix
is to make it cheaper, not to remove it.

**Also check, separately:**

```sql
SELECT relname, reloptions FROM pg_class WHERE relname IN ('all_refs','done_refs');
```

`REF_STORAGE` (`pipeline/storage/idmap/postgres.py:735`) is applied only in the
`CREATE`, and `idmap-migration.md` says existing tables need a one-time
`ALTER`. Without those settings the claim cost grows with the corpse pile
exactly as the comment at `:720` predicts — and see §3.2, which can suppress
the vacuuming those settings ask for.

`did_ref` (`:267`) is one `INSERT ... ON CONFLICT` per reference and could be
buffered, but at 0.48 wh it is not worth touching until the claim is fixed.

### 4.6 Is `recordcache2` needed for every source? — merge, 10.8 wh (20%)

**Where:** `run-merge.py:241`, `:363`; `pipeline/process/merger.py:95`.

`write_rewritten` is 20% of merge's worker time, and the only reader of those
`*_rewritten_record_cache` tables in the tree is `post-build-portal.py:74`,
which iterates **YPM's**. If the portal is genuinely YPM-only, skipping the
write for the other five internal sources deletes a fifth of the phase rather
than optimising it — no code cleverness required.

This is a **product question, not a performance question**: ask before
changing it. Note `pipeline/process/merger.py:95` also writes external `recordcache2` rows from
inside the `merge` stage, which is a separate decision from the internal ones.

### 4.7 Instrument merge's unattributed 8.2% — 4.48 wh

`(unattributed)` is merge's fourth-largest bucket and is mostly the
`iter_records_slice` cursor fetches at `run-merge.py:176`, which no
`timer.stage()` wraps. Wrap it. A phase whose fourth-biggest cost has no name
is one nobody can reason about.

### 4.8 `merge-refs` `claim_member` — 2.36 wh (71% of an 8-minute phase)

**Where:** `run-merge.py:328`, `claim_member()` at `run-merge.py:133`.

One `has_item` SELECT per candidate internal member, per cluster. Two options,
in increasing order of goodness:

* batch the probes per source with `identifier = ANY(...)`;
* better, have `write_done_refs()`
  (`pipeline/process/reference_manager.py:90`) — a single process that is
  already resolving each cluster's YUID — also record which internal source,
  if any, owns the cluster. `merge-refs` then reads it from
  `reference_uris.txt` and issues no queries at all. The file format is
  already versioned in spirit (`dist|yuid|uri`, and a stale file is designed to
  fail loudly), so adding a field is tractable.

Small in absolute terms — the whole phase is 8 minutes — so do it last.

### 4.9 Find the actual worker-count peak

24 was never validated. Nothing in these phases is CPU-bound in python,
postgres is on the same box competing for the same cores, and per-slice spread
is 0%. Run one source at 16, 24 and 32 workers and compare wall time. Cheap
experiment, and if §2 shows the server saturated it may be the largest single
number in this document.

---

## 5. Open questions

* **`synchronous_commit` for the record caches.** The reasoning in
  `run-reconcile.py:106` for refusing to defer commits is about *lock
  duration*, and it is correct. But async commit would remove the per-write
  fsync **without** extending lock duration, which is a different trade from
  the one that comment rejects. If §2 finds `synchronous_commit = on`, decide
  deliberately: server-wide (as `idmap-migration.md:150` suggests), or per-pool
  in `PoolManager.make_pool` the way the reference maps do it. The record
  caches are reconstructible from the datacaches, so the durability being
  traded away is worth little — but `run-identify.py` does write identity and
  should keep it.
* **Why are single-row prepared idmap lookups 330–370 µs?** Over a unix socket
  with a prepared statement against a hot index, that should be well under
  100 µs. Either the server is queueing (§2 answers this) or something in the
  connection path is not what the code thinks it is. Worth understanding
  before optimising around it, because the answer changes §4.1's expected win.
* **`TIME_INDEX`** is `False` on every cache except `DataCache`
  (`pipeline/storage/cache/postgres.py:156`, `:1144`). Confirmed correct as
  written — `latest()` is only asked of the data caches — but if a new caller
  starts reading `insert_time` in an order on another cache it will
  seq-scan silently.
