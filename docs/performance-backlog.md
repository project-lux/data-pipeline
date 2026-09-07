# Pipeline performance: what was found, what changed, what is left

Written after the first full overnight build on postgres-only (no redis), from
the four-phase timing report the `PhaseTimer` JSON produces. Two changes have
landed off the back of it; the rest of what the report pointed at is recorded
here so it can be picked up **after a re-run confirms the first two**.

Read this top to bottom before touching anything: the order matters, because
the two landed changes move the numbers the remaining items were sized
against.

---

## 1. Baseline

**The machine: 36 vCPU, 72 GiB** (70,214 MiB visible), Postgres on an AWS io2
volume, 24 worker processes. Measured from a `top` during run 3 — see §2.1.
`docs/idmap-migration.md` used to say 32 vCPU / 80 GB, which was wrong in both
halves; it has been corrected, but prefer `pg-tune.py`, which reads the
machine rather than trusting a document.

### Run 1 (postgres only, no redis)

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

### Run 2 — after §3.1 and §3.2

**Total: 300.7 → 240.9 min wall, 120.1 → 96.1 worker-hours. Both -20%.**

| phase | wall | worker-hrs | client cpu | vs run 1 |
|---|---|---|---|---|
| merge | 116.7 min | 46.6 | 42% | -14.6% |
| merge-refs | 8.5 min | 3.4 | 10% | +2.4% |
| reconcile | 77.9 min | 31.1 | 56% | **-34.4%** |
| reconcile-refs | 37.8 min | 15.0 | 31% | **+2.0%** |

Stage deltas in worker-hours, run 1 → run 2:

```
merge            reidentify      13.65 -> 10.23   -25.1%
                 write_merged    11.33 ->  8.68   -23.4%   <- §3.1
                 write_rewritten 10.80 ->  8.28   -23.3%   <- §3.1
                 idmap_forward    4.54 ->  5.82   +28.2%   <- REGRESSED, see §5
                 idmap_cluster    4.01 ->  3.53   -12.0%
                 final_transform  1.50 ->  1.52    +1.3%   <- cpu control, flat
                 checkpoint       0.41 ->  0.41    +0.0%   <- cpu control, flat
reconcile        acquire         30.12 -> 13.32   -55.8%   <- §3.2
                 reconcile        8.70 ->  7.53   -13.4%
                 walk_refs        7.16 ->  7.44    +3.9%
                 cpu-hours       17.50 -> 17.30    -1.1%   <- same work, less waiting
reconcile-refs   claim            6.87 ->  7.11    +3.5%   <- see §3.2
merge-refs       claim_member     2.36 ->  2.31    -2.1%
```

`final_transform` and `checkpoint` do no postgres work and moved by 1.3% and
0%, which is what makes the rest of the column trustworthy: the box did not
change, so the gains are real reductions in time spent waiting, not drift.
Reconcile's cpu-hours held at 17.3 while its worker-hours fell by a third —
the exact signature of removing a round trip rather than removing work.

---

## 2. Reading the report

### 2.0 Nested-stage accounting — FIXED

See **§3.6**. `timing-report.py` derives nesting from the stage names rather
than from a field in the JSON, so **the run-2 logs already on disk report
correctly if you just run the tool again** — no rebuild needed to get the
corrected numbers.

### 2.1 The client/server gap — ANSWERED: neither CPU nor IO bound

`PhaseTimer` measures client CPU only, so for two runs it could not say
whether "workers are off-cpu 44% of the time" meant waiting on disk, waiting
on a saturated server, or waiting on nothing in particular. A `top` snapshot
during run 3's reconcile settles it.

**The machine is 36 vCPU / 72 GiB** (70,214 MiB visible), not the 32 vCPU /
80 GB that `docs/idmap-migration.md` used to claim.

```
%Cpu(s): 60.1 us,  5.9 sy,  0.0 ni, 32.5 id,  1.5 wa
load average: 24.93
```

| | cores | share |
|---|---|---|
| 23 python workers | 12.7 | 53% |
| ~65 postgres backends | 10.9 | 46% |
| **total** | **23.6 of 36** | **66% — a third of the box idle** |

which is exactly top's `us + sy`. `TIME+` is the more reliable cross-check —
a `top -b -n 1` sample quantises %CPU in 6.25% steps — and agrees: 22:45
python against 10:00 + 5:35 postgres per worker, a **59:41 split**.

**So: not IO-bound (1.5% iowait), not CPU-bound (32.5% idle), and no lock
contention (zero ungranted locks). The workers are round-trip bound.**

The right model is not "the server is saturated". It is that each worker's
requests are strictly serialised — compute, send, wait, compute — so
*anything* that shortens postgres' service time or removes a round trip shows
up in worker wall time at close to 1:1, with no saturation required. That is
why the four changes that removed round trips (§3.2, §3.3, §3.5, §3.9) paid so
much better than §3.1, which halved bytes without removing a trip, and why
§4.9 is now the largest single number in this document.

**The backend groups map exactly onto the connection accounting**, which is
useful independent confirmation of both §3.7 and §3.9:

| group | count | TIME+ | what it is |
|---|---|---|---|
| A | 24 | ~10:00 | the read/write connection |
| B | 24 | ~5:35 | the **reference queues** |
| — | 24 | ~0% | the iterating cursors |
| — | 24 | ~0% | the idmap — reconcile makes no identity calls |
| autovac | 3 | 1:05–1:23 | all three workers busy |

24 x 4 + a few = the 98 backends the `pg_stat_activity` sample counted. The
reference queues being **36% of all postgres CPU** (5:35 against 10:00)
corroborates their 26.6% share of sampled activity, and is what §3.9 goes
after.

**Act on this:** `autovacuum_max_workers` is at the default 3 on a 36-core
box and all three are busy, while `idmap` has never been vacuumed at all
(§3.8). `pg-tune.py` recommends 8 here.

`pg-tune.py` prints the server settings this workload wants, sized to the
machine, including everything in this list that is a setting rather than a
measurement (`pg_stat_statements`, `track_io_timing`, `log_lock_waits`,
`log_autovacuum_min_duration`). It changes nothing -- it prints SQL to review.

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

## 3. Landed

Which run carried what, because the assessments below depend on it:

| run | carried | assessed |
|---|---|---|
| 1 | — | the baseline in §1 |
| 2 | §3.1, §3.2 | yes — see §1's delta table |
| 3 | §3.3, §3.4, §3.5, §3.6 | **stage timings not yet reviewed.** Its `pg_stat_activity` and `top` samples are what resolved §2.1 and §3.2, and what found §3.8 and §3.9 |
| 4 | §3.7, §3.8, §3.9, §3.10, §3.11 | pending |

§3.6 is a reporting fix rather than a build change, so it alters how run 3's
numbers read, not what they are. Run 3's own stage table has not been through
this document yet — do that before acting on anything in §4 that is sized from
run-2 figures.

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

**Result (run 2): confirmed, -23% on both write stages.** `write_merged`
11.33 → 8.68 wh and `write_rewritten` 10.80 → 8.28 wh, per-call 935.8 → 716.9
and 891.5 → 684.0 µs. 5.2 worker-hours off merge.

Less than a halving, which is the expected shape: the duplicated payload was
only part of a write's cost — the round trip, the WAL record and the index
maintenance were never doubled and are still there. That residue is what §4.2
(batching) goes after.

Two knock-on effects worth noting, because they are evidence about the server
rather than about this change:

* `reidentify` fell 25% (13.65 → 10.23 wh) and `idmap_cluster` 12%, neither of
  which was touched. The likeliest explanation is that halving the write
  bytes and jsonb parsing freed server CPU and cut WAL volume, so everything
  else talking to postgres got faster. If so, the box was server-saturated —
  which is §2.1's question, and this is the strongest evidence yet that the
  answer is yes.
* `idmap_forward` went the other way, +28%. See §5.

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

**Result (run 2): confirmed, and the largest win in the build.** Reconcile
went 118.7 → 77.9 min wall and 47.4 → 31.1 worker-hours, both -34%. `acquire`
fell 30.12 → 13.32 wh, -56%, per-call 2473 → 1094 µs. Client cpu-hours held at
17.3 (from 17.5) while worker-hours fell by a third: identical CPU work, far
less blocking, which is precisely what removing a round trip looks like.

**RESOLVED by the pg_stat sample taken during run 3's reconcile: the caveat
does not bite.** `all_refs` sits at **0.2% dead with 181 autovacuums** while 24
backends are idle-in-transaction holding cursor snapshots — autovacuum is
keeping up regardless. The reason is timing: `iter_records_slice`'s cursor
closes when the main loop's generator is exhausted, which is *before* the refs
loop starts, so the long-lived snapshots and the delete-heavy draining of the
queue never overlap. The keyset-pagination fallback below is therefore not
needed for this reason (it would still remove a connection — see §3.7).

The original observation, kept for the record:
`reconcile-refs` got *slightly worse* — 37.0 → 37.8 min, `claim` 6.87 → 7.11
wh, per-call 2437.7 → 2540.1 µs (+4.2%). That is the direction the snapshot
concern predicts but far too small to attribute with confidence, and
`merge-refs` drifted +2.4% in the same run with no plausible connection. It
needs the measurement, not more inference:

```sql
-- during reconcile
SELECT relname, n_dead_tup, last_autovacuum FROM pg_stat_all_tables
WHERE relname IN ('all_refs','done_refs');
SELECT pid, backend_xmin, now()-xact_start AS age FROM pg_stat_activity
WHERE backend_xmin IS NOT NULL ORDER BY age DESC LIMIT 5;
```

If `n_dead_tup` climbs through reconcile without `last_autovacuum` moving, and
the oldest `backend_xmin` belongs to a worker holding an
`iter_records_slice` cursor, the caveat is real. The fix is keyset pagination
rather than one long cursor:

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

### 3.3 The YUID and its class were two round trips (was §4.1)

`IdMap.get_cluster()` in `pipeline/storage/idmap/postgres.py`, used at
`run-merge.py:192`.

`idmap[qrecid]` then `idmap[full_yuid]` was two sequential single-row lookups
for one answer — 5.82 + 3.53 = 9.35 worker-hours in run 2, 20% of merge.
Membership is derived from the `yuid` column rather than stored separately, so
one prepared statement resolves the forward pointer and returns the class it
points at:

```sql
SELECT m.yuid, m.uri FROM idmap m
WHERE m.yuid = (SELECT yuid FROM idmap WHERE uri = $1)
```

The `idmap_forward` stage is gone; `idmap_cluster` now covers both.

Three things it has to preserve, all pinned in
`tests/test_idmap_get_cluster.py`:

* **Both halves stay in the memory cache**, under the same two keys `get()`
  used. Merge reads the class straight back (`idmap_equivs`, 12.7 µs) and so
  does the reidentifier's prefetch hop; caching only the forward pointer would
  move the cost rather than remove it.
* **No rows means the key is unknown, not that the class is empty** — a key is
  always a member of its own class.
* **A failed statement falls back to the two lookups** rather than returning
  no YUID, which merge would turn into a skipped record and a misleading
  "Couldn't find YUID" line.

Under `--resume` this is now unconditional where the cluster fetch used to be
skipped for already-built records. That costs nothing: the resume path already
paid one `metadata()` round trip per record, and this replaces one lookup with
one lookup.

**Verify:** `idmap_forward` absent, `idmap_cluster` at roughly what
`idmap_forward` alone cost, `idmap_equivs` still in the tens of microseconds.
If `idmap_equivs` jumps, the memory caching broke.

### 3.4 One statement per record while deferring (was §4.2, merge half)

`defer_commits(every=500, batch=25)` at `run-merge.py:112`; the mechanism is
`_buffer` / `_emit_batch` / `_read_barrier` in
`pipeline/storage/cache/postgres.py`.

Deferring had already removed the fsync per write, which left one round trip
and one parse per record. Rows now accumulate and go out 25 to a statement.
Memory is unchanged — `deferred_stmts` already held every record until the
commit for replay, so the buffer is the same records, earlier.

Batching applies **only while deferring**, which is what makes it safe:
deferral already requires workers to write disjoint keys. On top of that:

* rows are emitted **in key order**, for the reason `merge_refs` sorts — ON
  CONFLICT locks conflicting rows in the order the VALUES list gives them, so
  two transactions overlapping in opposite orders deadlock;
* a key written twice before the batch lands **folds to its last version**,
  because two rows for one key in one ON CONFLICT statement is an error, not
  an upsert;
* every cache in the process lands its buffer **before the connection
  commits**, so a commit still covers a merged record and its rewritten rows
  together;
* reads, and `delete`/`clear`/`set_metadata`, **land the buffer first**. An
  executed-but-uncommitted statement was visible to a later read on the same
  connection; buffering would have silently lost that.

Pinned in `tests/test_batched_writes.py` (13 tests). `batch` defaults to 0, so
`run-export.py:53`, `run-source-build.py:52` and the loaders are unchanged and
can opt in one argument at a time once merge's numbers are in.

**Verify:** `write_merged` and `write_rewritten` in run 3. Watch the merge logs
for `deadlock detected` — key-disjointness plus sorting should make it
impossible, and one occurrence means an assumption broke rather than that
contention is normal.

### 3.5 The write connection was not in autocommit (was §4.11)

`PoolManager.set_autocommit()` in `pipeline/storage/cache/postgres.py`, applied
in `make_pool()` and driven by `defer_commits()` / `resume_commits()`.

Both connections were created with psycopg2's default `autocommit = False`.
Outside `defer_commits()` every `set()` ended with `conn.commit()`, so
psycopg2 wrapped the work in an explicit transaction: `BEGIN` as its own
command when the connection was idle, `COMMIT` at the end. Two round trips per
record that do no work. Reconcile pays them 43.8M times; a single-row indexed
SELECT there costs 227 µs (`acquire.cache_hit`, 60.5M calls), which puts them
at **~5.5 of the phase's 31.1 worker-hours**. A read plus a write went from
four round trips to two.

**It shortens row locks rather than lengthening them**, which is why this and
not §4.2: in autocommit a lock lives from the statement to its implicit
commit, where before it was held until an explicitly-issued COMMIT arrived a
round trip later. `run-reconcile.py:106`'s reasoning is served, not
contradicted. The precedent is `pipeline/storage/idmap/postgres.py:85`, which
has run its own connection this way all along.

Scope and the things that had to move with it, all pinned in
`tests/test_write_autocommit.py`:

* **Write connection only.** The iterating connection carries server-side
  cursors, which need a transaction to live in. Checked: every
  `_cursor(iter=True)` is on that connection, and the one named cursor on the
  write connection is in `list()`, behind `raise NotImplementedError`.
* **Deferring turns it off**, because a batch needs a transaction the caller
  controls; `resume_commits()` turns it back on, committing first because
  psycopg2 refuses to switch inside a transaction.
* **`end_read()` is a no-op in autocommit** — a read leaves no transaction, so
  there is nothing to end and a ROLLBACK would be another pointless round
  trip. It stays for the deferring case.
* **`_maintenance()` saves and restores `autocommit` directly** rather than
  going through `set_isolation_level`. Whether the legacy getter reports
  autocommit as level 0 is a psycopg2 detail, and getting it wrong would
  silently leave the shared connection in the other mode for the rest of the
  run — either making `defer_commits()` a no-op or reintroducing a COMMIT per
  write.

**One accepted behaviour change:** `_make_table()` issues CREATE TABLE and
CREATE INDEX, which are now separate transactions rather than one. A failure
between them would leave a table without its index instead of neither. It is a
one-time setup path inside a try/except, and `TIME_INDEX` is False for every
cache but `DataCache`, so in practice there is no second statement.

**Verify:** `acquire.post_map` should fall from 685 µs towards ~230, and
`acquire.cache_hit` from 227 µs (it carried the BEGIN). If neither moves, the
premise was wrong — psycopg2 was not issuing BEGIN as its own round trip — and
only the COMMIT saving is real. `pg_stat_statements` should show BEGIN and
COMMIT call counts collapse.

### 3.6 The report double-counted nested stages (was §2.0)

`split_stages()` in `pipeline/process/timing.py`, used by `PhaseTimer.summary()`,
`.report()`, `.finish()` and by `timing-report.py`.

`timing.stage("acquire.map")` runs inside `timer.stage("acquire")`, so summing
every stage and subtracting from the wall clock counted that time twice. Once
the `acquire.*` sub-stages landed, reconcile reported **-16.51 worker-hours
unattributed, -53.1%** — a figure that cannot exist, and one that made every
percentage in the phase wrong.

A dotted name is now a child of its prefix. Only top-level stages are
subtracted; children are reported in a block of their own, and a child whose
parent was never timed is promoted, since it is then the only attribution
there is. Run 2's reconcile now reads **2.54 worker-hours, 8.2%**.

The second half of the fix matters as much. The sub-stages are **not a
breakdown of their parent**: `timing.stage()` attributes to whichever
PhaseTimer is active, so the acquirer's stages accumulate whether it was
called from the top of the loop or from inside `reconcile()` via
`pipeline/process/collector.py:169`. In run 2 the four `acquire.*` stages
totalled 19.03 worker-hours against `acquire`'s 13.32. Printing them indented
under `acquire` would imply a partition that isn't one, so the report says it
outright:

```
  inside acquire -- already counted above, not additional:
    acquire.post_map          9.53               50,107,634      684.9
    acquire.cache_hit         3.82               60,476,605      227.3
    acquire.map               3.64               51,668,639      253.9
    acquire.fetch             2.04               52,123,139      141.0
    ...totalling 19.03 worker-hrs against acquire's 13.32, so 5.71 of it runs
       inside other stages -- not a breakdown of acquire
```

That 5.71 worker-hours is the external-authority acquisition inside
`reconcile`, which is also what makes the reconciler's own logic ~1.82 wh of
its 7.53 — a fact the old report could not have told you.

`summary()` also stamps `"nested": true|false` on each stage for anything
reading the JSON directly, but `timing-report.py` derives nesting from the
names instead, so logs written before this change roll up correctly too.

Pinned in `tests/test_timing_nesting.py`, including the exact run-2 reconcile
shape.

### 3.7 The reference queues took a connection they no longer needed

`_refs_connection()` in `pipeline/storage/idmap/postgres.py`.

`all_refs` and `done_refs` shared a connection of their own, separate from the
identity map's, for exactly one reason: they issue `SET synchronous_commit =
off` and that must not leak onto the idmap. Since the server itself defaults
to `off`, the SET restated the default, the separation protected nothing, and
the connection was one of four per worker — 96 across a 24-way build.

The tag is now claimed only when the session setting would actually differ
from the server's, read once per process with `SHOW synchronous_commit` on the
untagged connection (which never SETs it, so what comes back is the effective
default, including any `ALTER ROLE` or `ALTER DATABASE`). Set `"asyncCommit":
false` in the map config to never want it; point the map at a server with
synchronous commit on and the separate connection returns on its own. An
unreadable setting assumes `on` and keeps the connection — over-provisioning
is safe, under-provisioning is a build that dies partway through.

Sharing is safe because both stores run every statement in autocommit: no
transaction to interleave, each psycopg2 cursor buffers its own result at
execute time, and the pipeline is one thread per process. The one moment that
is briefly untrue is `IdMap._set_once`, which turns autocommit off for a
multi-statement class merge — its docstring now says not to add a
reference-queue call inside it.

Per worker, **4 connections becomes 3**. At 48 workers that is 144 rather than
192, the difference between needing `max_connections` above 100 and not.
`pg-tune.py` reads the server's setting and sizes `max_connections` to match.
Pinned in `tests/test_refs_connection.py`.

**Verify:** each worker log prints `refs: sharing the idmap connection` once at
startup, and `SELECT count(*) FROM pg_stat_activity WHERE datname =
current_database()` mid-phase should be about 3x the worker count plus a few,
not 4x.

**Still open, found while counting:** `NetworkOperationMap` and
`TransitiveMultiMap` — the classes behind the `networkmap` and `redirects`
maps — exist only in `pipeline/storage/idmap/redis.py`; `postgres.py`
implements `IdMap` and `ReferenceMap` and nothing else. `run-reconcile.py:22`
instantiates `networkmap` on every run. Check what `storeClass` those two maps
are set to on the build box: if they are still redis, the postgres-only
migration has a gap the timing runs would not have shown, and if they have
been pointed at `storage.idmap.postgres.IdMap` that is a semantic mismatch.

### 3.8 The identity map was never vacuumed (was §4.12)

`IdMap.optimize()` in `pipeline/storage/idmap/postgres.py`, called at the end
of `run-identify.py` (skip with `--no-vacuum`).

**§3.10 found the cause of the dead tuples this section cleans up.** The
vacuum is still right to keep — incremental builds do move members — but it
should now have almost nothing to do. Read that section before spending more
effort here.

Sampled mid-build: **5.1M dead tuples, 9.1%, autovacuums = 0.** Not "hasn't
run lately" — never, and on the defaults it never would: `autovacuum_vacuum
_scale_factor` is 0.2, so a 50.7M-row table needs ~10.1M dead before it fires.

The litter comes from `assign_bulk()`, which is `INSERT ... ON CONFLICT DO
UPDATE` and leaves a dead tuple per member it moves. Identify does not notice —
it is the writer. Merge does, probing `idmap_pkey` (5.9 GB) and
`idmap_yuid_idx` (3.2 GB) tens of millions of times against a 9% bloated
index, which is the **leading candidate for §5's `idmap_forward` regression**
(372 → 478 µs in run 2 while everything around it got faster).

Both `idmap` and `idmap_yuid` are vacuumed, stats are read before the VACUUM
because it zeroes `n_dead_tup`, a failure on one table does not skip the other,
and a failure reading the stats view does not skip the vacuum. Pinned in
`tests/test_idmap_vacuum.py`.

**Two things this does not do.** It gives no space back — VACUUM marks dead
tuples reusable and leaves the files as large as they grew — and it does not
shrink an index at all. If the map has been accumulating across many builds,
run `REINDEX TABLE CONCURRENTLY idmap;` once before measuring the effect, or
the baseline is still carrying every previous build's bloat. The complementary
storage parameters (§4.12's second half) are still worth adding:

```sql
ALTER TABLE idmap SET (autovacuum_vacuum_scale_factor = 0.02,
                       autovacuum_vacuum_threshold = 10000,
                       autovacuum_vacuum_cost_delay = 0);
```

**Verify:** `idmap_cluster` in merge, and `n_dead_tup` for `idmap` before and
after identify.

### 3.9 Two reference lookups became one (was §4.10)

`ReferenceMap.get_multi_pair()` in `pipeline/storage/idmap/postgres.py`, used
by `ReferenceManager.resolve_refs()`.

`resolve_refs` asked the same question of `all_refs` and `done_refs` for every
record. In the run-3 sample those two SELECTs were **16.3% and 10.3% of
everything postgres was doing**, second only to the record-cache INSERT — and
`done_refs` holds **no rows at all** during the main loop, because `did_ref()`
runs only in the references loop afterwards. Trace `resolve_refs` with
`drefs == {}` and every branch reading it is dead. Ten percent of the server's
work was a round trip to a table that could not answer.

One `UNION ALL` now covers both, which is possible because §3.7 put them on
the same connection. The ratio in the sample is the argument: 16.3% for a
896k-row table against 10.3% for an empty one says most of that cost was the
round trip, not the lookup.

The key array crosses twice, once per branch, which is deliberate and not the
mistake §3.1 fixed: there it was a ~50KB jsonb document parsed twice, 43.6M
times; here it is one record's references — tens of short URIs — against a
round trip worth ~227 µs in this phase. A CTE would send it once at the cost
of giving the planner a semi-join instead of two index scans it already
handles well.

Backends without the paired form fall back to two calls, so redis is
unaffected, and so is any future configuration where `_refs_connection` keeps
the two maps on separate connections — `get_multi_pair` checks and falls back.
Pinned in `tests/test_ref_pair_lookup.py`.

**Not done, because it is not safe:** skipping the `done_refs` read entirely.
Workers do not cross from the main loop to the references loop in lockstep, so
one can be calling `did_ref()` while another is still finishing its main loop,
and that one would then miss a reference needing un-doing at a shorter
distance.

**Verify:** `walk_refs` in reconcile, and the `done_refs` SELECT should vanish
from `pg_stat_activity` as a statement of its own.

### 3.10 identify rewrote 45M rows to the values they already held

`assign_bulk()` in `pipeline/storage/idmap/postgres.py`.

The member upsert was:

```sql
INSERT INTO idmap (uri, yuid) VALUES %s
ON CONFLICT (uri) DO UPDATE SET yuid = EXCLUDED.yuid
```

No `WHERE`. Re-running identify over unchanged data computes the same
clusters, so every member already points at the YUID being written — and this
is not a guess. `touched.tsv` gets a line per member that actually *moved*,
and on a production run it held **exactly one entry against ~45M members**.
Every other row was rewritten to the value it already had: a new tuple
version, a WAL record and a dead tuple each, to change nothing.

**The codebase had already found this failure mode three times and guarded all
three**, including the statement immediately above this one in the same
function:

| statement | guarded before? |
|---|---|
| the `idmap_yuid` token upsert, in `assign_bulk` | yes |
| `token_set` (prepared) | yes — its comment measures 45,513,255 of 45,513,275 rows already correct |
| `merge_refs` | yes — its comment measures 7,882 live against 173,981 dead |
| **the `idmap` member upsert, in `assign_bulk`** | **no** |

It now carries `WHERE idmap.yuid IS DISTINCT FROM EXCLUDED.yuid`, as does
`_add()`, which had the identical statement. No staleness argument is needed
(unlike `merge_refs`, which required a paragraph): this is one atomic
statement, and if the stored value already equals the new one there is nothing
to do.

**What it explains.** `_apply_stream` is the only step of identify that writes
the database, and on the run that prompted this it sat there for over an
hour and three quarters. It also explains §3.8's 5.1M dead tuples — **that
section treated the symptom.** The vacuum is still right to keep, because
incremental builds genuinely do move members, but it should now have almost
nothing to do, and §4.12's storage-parameter recommendation is much less
urgent than it looked.

**Two statements are deliberately left unguarded**, and there are tests
pinning that so nobody "fixes" them:

* `mint()` assigns the column to itself precisely so `RETURNING` fires on
  conflict and the caller learns who won the race. A `WHERE` would return no
  row, making "I minted this" indistinguishable from "someone else did".
* `_import_state()` truncates first, so nothing can conflict. `_set_once()`
  returns early when the value is unchanged, so it never reaches a no-op
  either — both now say so in a comment.

Pinned in `tests/test_idmap_noop_writes.py`.

**Verify:** `n_tup_upd` on `idmap` across an identify run should fall from
~45M to near zero, `n_dead_tup` should stop growing, and `_apply_stream`
should report in minutes rather than hours now that §3.11 makes it report at
all.

### 3.11 identify now says what it is doing

`PhaseTimer` and per-step reporting in `IdentityResolver.resolve_identity()`.

`resolve_identity` printed nothing until it was completely finished, so a run
that had been going for two hours was indistinguishable from a hung one —
which is exactly the situation that turned up §3.10. The step boundaries were
already in the file as commented-out `print()`s.

Every step now announces itself **before** it starts, because the useful line
is the one naming what you are waiting for, not the one confirming what
finished; the nine external sorts announce their input size and `-S` setting
too. Steps report even when they raise, since a step that died is the one you
most want an elapsed time for. It writes `timing-identify-all.json` like every
other phase, so `timing-report.py` rolls identify in and **the phase finally
gets a baseline** — its absence is why "is 2h20m normal?" could not be
answered.

One trap avoided: the sorts are recorded as `<step>.sort:<file>` rather than
`sort.<file>`, so §3.6's nesting rule sees them as children of their step. The
obvious name would have had them summed alongside it and driven identify's
unattributed figure negative in exactly the way reconcile's was.

Pinned in `tests/test_identify_progress.py`.

**Worth knowing:** `LUX_SORT_BUFFER` defaults to `1G`. On a 72 GiB box with
nothing else running, the new per-sort lines will say whether raising it is
worth anything.

---

## 4. Backlog

Item numbers are stable identifiers — they do not change when the order does.
**Priority after run 2**, biggest first, with run-2 sizes:

| | item | size now | why now |
|---|---|---|---|
| — | ~~§4.1 one idmap query~~ | | **landed, see §3.3** |
| — | ~~§4.2 batch the writes (merge)~~ | | **landed, see §3.4** |
| — | ~~§4.11 write connection in autocommit~~ | | **landed, see §3.5** |
| — | ~~§4.10 `walk_refs`~~ | | **landed, see §3.9** |
| — | ~~§4.12 idmap never vacuumed~~ | | **landed, see §3.8** |
| 1 | **§4.9 run 32 workers** | ~25% of wall time | 12 of 36 cores idle, 1.5% iowait, zero lock waits — measured, not inferred |
| 2 | §4.5 reconcile-refs claim | 7.11 wh (47%) | untouched and unimproved; the least-improved phase |
| 3 | §4.4 skip the guaranteed-miss probe | 2.77 wh recoverable | cheapest code change on the list |
| 4 | §4.3 `reidentify` | merge 10.23 wh | fell 25% for free in run 2; re-measure after run 4 before spending on it |
| — | ~~§4.2 batch the writes (reconcile)~~ | | **downgraded**: `synchronous_commit` is `off`, so §4.11 gets the same round trips for less risk |

**Re-measure before starting any of these.** Run 2 showed stages moving 25%
without being touched, because relieving the server lifted everything, and
run 4 carries six more changes of the same kind. The exception is §4.9, which
is sized from a direct measurement of the machine rather than from stage
timings, so it does not need to wait.

§4.6 (`recordcache2`) sits outside this order because it is a product question,
not an engineering one — but it is still worth 8.28 wh.

### 4.1 Collapse `idmap_forward` + `idmap_cluster` into one query — LANDED

See **§3.3**. Awaiting run 3.

### 4.2 Batch the record-cache writes — reconcile — DOWNGRADED, probably don't

The merge half landed as **§3.4**. The reconcile half was held pending
`SHOW synchronous_commit`, which came back **`off`** — so most of the case for
it is gone.

With the fsync per commit already eliminated server-side, batching reconcile's
writes saves round trips and nothing else. It still carries the whole of the
original risk: reconcile is the one phase where workers upsert the *same* rows
(`collector.collect()` stores shared authority records), a multi-row upsert
holds locks on all N rows until it commits, and sorting prevents a deadlock
cycle without preventing 24 workers waiting on each other for N times as long
— against a phase whose current design deliberately keeps each lock alive for
microseconds (`run-reconcile.py:106`).

**§3.5 got the same round trips for less risk, and has landed.** If run 3
shows `acquire.post_map` still large after it, reconsider this with real
numbers.

### 4.3 `reidentify` — merge, 10.23 wh (21.9%), still one opaque number

**Where:** `pipeline/process/reidentifier.py`.

845 µs/call (run 2; 1127 in run 1) covers three different things and no one
knows the split:

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

**Re-measure before spending on this.** It fell 25% in run 2 without being
touched — see §3.1 — so a share of what looked like reidentifier cost was
really server contention caused by the writes. §4.2 may take more of it the
same way. Step 1 (instrument) is still worth doing first and on its own.

### 4.4 Reconcile's guaranteed-miss recordcache probe — 2.77 wh, 43.8M pointless probes

**Where:** `pipeline/process/base/acquirer.py:120-134`.

Run 2 sizes this precisely. `acquire.cache_hit` is 3.82 wh over **60,476,605
calls**, against 52,123,139 `acquire.fetch` calls — so 8.35M lookups (13.8%)
were served from the recordcache and the probe is doing real work overall.

But 43,843,926 of those 60.5M are the main loop's internal records, and on a
full rebuild (`manage-data.py --clear-all`) the recordcache starts empty, so
every one of them is a guaranteed miss: **72.5% of the calls, ~2.77 wh, 8.9% of
the phase.** The other 16.6M are external-authority lookups inside
`collector.collect()`, where the hit rate is what makes the phase work — so
this must be scoped to the main loop on a full rebuild, not applied globally.
On an incremental build the probe is what makes the phase fast and must stay.

It needs a flag. `pipeline/process/base/acquirer.py:17` has
`# self.force_rebuild = config.get("force_rebuild", False)` commented out,
which is where this was heading already.

Wire it to a `--rebuild` argument on `run-reconcile.py` and skip the probe when
set, for the main loop only. **Verify** via `acquire.cache_hit` — its call
count should fall from ~60.5M to ~16.6M, and its hit rate should rise from
13.8% to roughly 50%.

### 4.5 `reconcile-refs` claim starvation — 7.11 wh (47.4%), unimproved in run 2

**Where:** `pipeline/process/reference_manager.py:197` (`_claim_size`), `:255`
(`pop_ref`), `:44` (`ref_batch = 50`).

`_claim_size()` calls `queue_length(ceiling)` on **every** buffer refill and
returns `min(ref_batch, remaining // ref_workers)`. With 24 workers that
floors to 1 whenever fewer than 24 references are visible — so through the
whole long tail each record costs a count query *plus* a `DELETE ... FOR UPDATE
SKIP LOCKED`, which is how a per-record average of 2540 µs (run 2; 2437 µs in
run 1) arises from what should be one claim per 50 records.

Nothing in §3 touched this phase and it did not improve — it is now the least
improved part of the build, and the only stage that got materially worse in
relative terms. Note it also carries `acquire.cache_hit` at 3.08 wh over
40,046,718 calls, which is **4.0 probes per record**; if §4.4 is being done
anyway, look at whether the collector is probing sources it could rule out
first.

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

### 4.6 Is `recordcache2` needed for every source? — merge, 8.28 wh (17.8%)

**Where:** `run-merge.py:241`, `:363`; `pipeline/process/merger.py:95`.

`write_rewritten` is 20% of merge's worker time, and the only reader of those
`*_rewritten_record_cache` tables in the tree is `post-build-portal.py:74`,
which iterates **YPM's**. If the portal is genuinely YPM-only, skipping the
write for the other five internal sources deletes a fifth of the phase rather
than optimising it — no code cleverness required.

This is a **product question, not a performance question**: ask before
changing it. Note `pipeline/process/merger.py:95` also writes external `recordcache2` rows from
inside the `merge` stage, which is a separate decision from the internal ones.

### 4.7 Instrument merge's unattributed 9.5% — 4.44 wh

Unmoved by anything in §3 (4.48 → 4.44 wh), and merge is the one phase where
the figure is trustworthy because it has no nested sub-stages — see §2.0.
`(unattributed)` is one of merge's largest buckets and is mostly the
`iter_records_slice` cursor fetches at `run-merge.py:176`, which no
`timer.stage()` wraps. Wrap it. A phase whose fourth-biggest cost has no name
is one nobody can reason about.

### 4.8 `merge-refs` `claim_member` — 2.31 wh (69% of an 8-minute phase)

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

### 4.9 Run more workers — 12 idle cores, ~25% of wall time

**The largest single number left, and it is a shell-script edit.** §2.1
measured 23.6 of 36 cores busy at 24 workers, with 1.5% iowait and zero lock
waits. Per worker that is **0.98 cores** — 0.53 python, 0.45 postgres — so:

```
24 workers (now):  23.6 cores   66%
32 workers:        31.4 cores   87%
36 workers:        35.3 cores   98%   <- saturated
```

**Try 32.** Roughly 25% off wall time if it scales, and it should: nothing is
saturated, per-slice spread is 0–1%, and the phases are latency-bound rather
than contending for anything.

Do not go straight past 32. The projection assumes per-worker postgres CPU
stays flat, and it will not — more backends means more proc-array and
lock-manager work — so measure 32 before assuming 36 is better than 34.

What to watch, in the order it would break:

* **Lock waits.** Reconcile's workers upsert the same external authority rows
  (`collector.collect()`), and that is the one thing more concurrency makes
  worse. It is at *zero* ungranted locks today, so there is real room, but
  `log_lock_waits` (which `pg-tune.py` turns on) is what will say when it runs
  out.
* **Connections.** 32 x 3 = 96 after §3.7, or 128 without it. Check
  `max_connections` before the run, not during.
* **`shared_buffers` pressure.** More workers means a bigger concurrent
  working set against the same 19.5 GB. `IO/DataFileRead` was 11.8% of active
  backends at 24; if it climbs steeply the box has found its limit.

The change itself is `seq 0 23` in the build scripts. Which of `full-build.sh`,
`reconcile_parallel.sh` and `merge_parallel.sh` is actually driving a given
run is a local question -- change the one you use, and pass the same number as
the second argument to the workers so `max_slice` matches.

### 4.10 `walk_refs` — LANDED

Diagnosed and fixed; see **§3.9**.

### 4.12 The identity map is never vacuumed — LANDED

See **§3.8**.

---

## 5. Open questions

* ~~**`synchronous_commit` for the record caches.**~~ **ANSWERED: it is
  `off`** (checked on the server, 2026-09-06). So the per-write fsync that
  several comments in this codebase reason from does not exist. Two
  consequences, both recorded where they matter: §4.2 loses most of its
  expected value, and §4.11 replaces it. Note that
  `defer_commits()`'s docstring still justifies itself with "Committing per
  write cost an fsync per write" — that is now wrong about *why* deferral
  helps (it saves round trips, not fsyncs) and should be corrected before it
  misleads someone. The comment at `run-reconcile.py:106` is unaffected: it
  reasons about lock duration and deadlocks, which is still exactly right.
* **Why did `idmap_forward` get 28% slower in run 2?** *(Leading candidate
  found: see §4.12 -- the idmap carries 9.1% dead tuples and has never been
  autovacuumed. The rest of this entry is the reasoning that was open before
  that.)* 372 → 478 µs, while
  `idmap_cluster` on the *same connection and cursor* went 329 → 289 µs and the
  phase as a whole got 15% faster. The two differ in three ways worth testing:
  `forward` probes `idmap_pkey` (5.9 GB) and `cluster` probes
  `idmap_yuid_idx` (3.2 GB); `forward`'s keys are ~44M distinct source URIs
  that never repeat, so they never hit the memory cache, while `cluster`'s
  results are cached and reused by `idmap_equivs` (12.7 µs); and run 2 pushed
  17% more queries per second at the server, so a marginal working set would
  show a *lower* buffer hit ratio at the higher arrival rate. Check
  `pg_statio_user_indexes` for `idmap_pkey` (`idx_blks_hit` vs `idx_blks_read`)
  across both runs before theorising further. §4.1 removes the round trip
  either way, but the answer decides whether the underlying problem follows it.
* **Why are single-row prepared idmap lookups 290–480 µs at all?** Over a unix
  socket with a prepared statement against a hot index, that should be well
  under 100 µs. Either the server is queueing (§2.1) or something in the
  connection path is not what the code thinks it is. The run-2 evidence — that
  cutting write bytes made *unrelated* read stages 12–25% faster — points hard
  at queueing.
* **`TIME_INDEX`** is `False` on every cache except `DataCache`
  (`pipeline/storage/cache/postgres.py:156`, `:1144`). Confirmed correct as
  written — `latest()` is only asked of the data caches — but if a new caller
  starts reading `insert_time` in an order on another cache it will
  seq-scan silently.
