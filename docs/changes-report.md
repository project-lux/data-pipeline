# Changes to the legacy codebase: determinism, data loss, and identity

**Branch:** `fabel-pipeline-audit` (off `main`) · 8 commits · 42 files ·
+4,157 / −207 lines
**Validation:** 32 unit/regression tests; full-dataset replay of every
LUX class through both the old and new identity mechanisms
(`docs/build-comparison-report.md`); findings catalog with per-defect
detail in `docs/determinism-audit.md`.

This report describes every change, why it was made, and how it is
tested. It is organized as: the architectural change (identity
resolution), then the targeted fixes by subsystem, then the validation
infrastructure. File references are to the `main`-branch layout.

---

## 1. The architectural change: deterministic identity resolution

**Problem.** YUIDs were minted (`uuid4`) and clusters merged *during*
reconciliation, per record, live against the shared redis idmap, by 24
racing workers. The identity of every entity therefore depended on
arrival order and scheduling: measured on full production data, two
byte-identical rebuilds disagreed on 17% (concepts) to **34%** (places)
of the identity map, silently lost 2–22% of production YUIDs per
rebuild, never registered ~5% of members at all, and honored almost no
differentFrom constraints (900+/1000 injected violations).

**Change.** Identity assignment was separated from reconciliation and
made a pure function of the inputs:

| File | Change |
|---|---|
| `pipeline/process/identity.py` (**new**) | The engine. `AssertionWriter` (per-slice append-only log of `(asserter, a, b)` sameAs assertions, mirroring the qua handling of `manage_identifiers`); `cluster()` — union-find over voted pair-edges, processed most-voted-first with lexicographic tie-breaks, refusing any union that would join a differentFrom pair (checked cluster-wide); `assign()` — prior-YUID reuse by member vote so adding a URI never changes a cluster's YUID, deterministic split-claim resolution, `uuid5(min member)` minting for genuinely new clusters; `apply_assignments()` — pipelined bulk load into the redis idmap (member→yuid strings, yuid→member sets + current update token, stale-set cleanup). |
| `run-reconcile.py` | Both `ref_mgr.manage_identifiers(rec2)` call sites replaced with `assertion_log.write_record(rec2)`. **Reconcile workers no longer write to the idmap at all** — removing both the nondeterminism and the 24-way redis contention (measured: 7–8M sequential round trips per build reduced to ~300 pipelined ones). |
| `run-identify.py` (**new**) | Runs once after all slices finish: aggregates `assertions-*.tsv`, loads differentFrom pairs from the merged `differentDbPath` LMDB, resolves clusters + YUIDs, bulk-loads the idmap, writes refused assertions to `identity_conflicts.jsonl` (a reviewable curation artifact instead of silent wrong merges). |
| `run-all.sh`, `full-build.sh`, `reconcile_parallel.sh` | Orchestration: clear stale assertion logs at reconcile start; run `run-identify.py` after the reconcile wait-loop (build fails loudly if it fails). **Note:** `run-all.sh` is the *live* orchestrator — `full-build.sh` is dead code (`exit 0` at top); both were updated, but the `run-all.sh` edit is the one that matters and was initially missed by everything except the audit. |

`manage_identifiers` remains in `reference_manager.py` but has no
production callers; the comparison harness drives it directly as the
"legacy" baseline. The measured outcome of this change alone, replayed
over all ~44M records: byte-identical identity maps across worker counts
and arrival orders, 100% production-YUID reproduction on rebuild, zero
differentFrom violations, 4–6× faster, in every LUX class.

---

## 2. Reconciliation correctness fixes

### `pipeline/process/base/reconciler.py`
- **Dead multi-match tie-break** (Lmdb + Sqlite variants): the "pick the
  best-supported match" sort used `key=lambda x: len(x)` where `x` is a
  `(uri, [matches])` 2-tuple — `len(x)` is always 2, so the winner was
  dict-insertion order. Now sorts by `(-len(x[1]), x[0])`: most
  supporting matches first, URI tie-break. Records now reconcile to the
  strongest candidate instead of an arbitrary one.
- **Index-open failures were silent**: a worker that failed to open a
  name/id index (sqlite lock, transient FS error) set it to `None`,
  `should_reconcile()` then returned `False`, and that worker emitted
  **zero assertions for its entire slice** with no error. Now retries
  with backoff (3× lmdb, 5× sqlite) then raises with a clear message.

### `pipeline/process/reconciler.py` (`call_reconcilers`)
Three defects in the differentFrom guard, fixed together:
1. it mutated `ids` while iterating it, so removals skipped the next
   element's diff check entirely;
2. which of two known-distinct entities got dropped depended on
   iteration order (now: deterministic — keep the lexicographically
   smaller);
3. **removals never applied to the record's own `equivalent` array** —
   the guard only edited a local list, so "dropped" distinct entities
   stayed in the record's output equivalents and flowed into identity.
   The test proved known-distinct pairs sailed through untouched.
   Dropped ids are now removed from `record["data"]["equivalent"]`.

### `pipeline/sources/authorities/getty/reconciler.py` (`extract_names`)
Two bugs made AAT name matching systematically lossy: names whose
language wasn't in `check_langs` kept their original case, but the name
index is built lowercased — those lookups could never match; and the
per-language loop overwrote an already-matched priority with the
fallback value on every later non-matching language. Now mirrors the
base class: always `clean_names()` (lowercase), break on first language
match, keep the best (lowest) priority per name.

### `pipeline/process/base/index_loader.py`
Name/URI index collisions (two records sharing a normalized name, or
claiming the same equivalent URI) were **last-writer-wins over an
unordered record stream** — the index winner could differ between
builds, so every record that name-reconciled against that key got a
different equivalence assertion per build. Now the smallest recid wins,
making the index a pure function of its inputs.

### `pipeline/sources/authorities/oclc/index_loader.py` (VIAF)
- A stale `viaf_sort_equivs.csv` from a previous build was silently
  reused if present; now regenerated whenever any shard is newer.
- Sort now runs under `LC_ALL=C` (locale-independent ordering).
- An ident appearing in multiple VIAF clusters was last-writer-wins over
  shard layout; now the smallest VIAF id wins deterministically.

---

## 3. Reference-phase fixes (the "lost concepts" mechanisms)

### `run-reconcile.py` (reference loop)
`did_ref(uri, distance)` marked a reference **done before acquiring
it**. A transient fetch failure therefore silently dropped that record
*and every record reachable only through it* for the whole build — the
exact "concepts without internal identifiers disappear between builds"
symptom. Now: mark done only after a successful acquire (failed refs
stay eligible for re-queue when re-encountered); permanently unusable
URIs (unsplittable) are still marked done so they don't loop.

### `pipeline/process/reference_manager.py`
- **`add_ref` distance races**: min-distance updates were
  read-then-write over shared redis, so a slower worker could overwrite
  a shorter distance with a longer one — pushing a reference past
  `max_distance` in some builds and not others (the main surviving
  between-builds loss mechanism after the did_ref fix). New
  `ReferenceMap.merge_ref()` applies min-distance / type-if-unset
  atomically (WATCH/MULTI).
- **Loss-safe re-queue ordering**: re-adding a done ref at a shorter
  distance now adds to `all_refs` *before* deleting from `done_refs`,
  so a crash between the two duplicates work instead of losing the
  reference.
- **`iter_done_refs`**: an empty/blank line yielded a bogus `[""]` item
  (`"".split("|")` is truthy); blank lines are now skipped.

### `merge-metatypes.py`
`glob` order is filesystem-dependent; now sorted, so per-key metatype
list order is stable across builds.

---

## 4. Merge/export-phase fixes

### `pipeline/process/merger.py`
- **Nondeterministic merge order**: `to_merge` comes from a redis
  *set*; sorting only by `merge_order` (a stable sort) preserved
  set-iteration order for ties, so sources with equal `merge_order`
  merged in a different order each run — flipping every first-wins rule
  (primary-name selection, timespan ties, geometry ties). Now
  tie-broken on source name + record identifier (record ids all equal
  the shared YUID at this point, so identifier is the meaningful key).
- **Dimension-merge crash (4 sites)**: `dr.append(dm)` appended to the
  *loop-variable dict* (or raised `NameError` on an empty list) instead
  of `rec["dimension"]` — any Linguistic/Set/Visual/Digital object with
  a non-matching dimension killed the whole merge slice. Fixed, with a
  guard for base records that have no `dimension` list at all.

### `run-merge.py`
- **Reference-phase base-record selection** iterated the unordered
  `equivs` set — a different base record (hence different label/field
  precedence) per run. Now iterates `sorted(equivs)`.
- **Cross-slice YUID claim**: when several *internal* records share a
  YUID, both slices used to build it, racing on an `insert_time`
  check-then-act; whoever wrote first won. Now only the
  lexicographically-smallest internal member still present in its
  recordcache builds the merged record — deterministic, no race.

### `pipeline/process/reidentifier.py`
- **Arbitrary YUID choice**: with >1 YUID in `equiv_map`, `set.pop()`
  picked one at random per process. Now `min(uus)` — deterministic.
- **Merge-phase idmap write removed** (third wave): the fallback
  `self.idmap[qrecid] = uu` was the last identity mutation outside
  `run-identify.py`; 24 merge workers racing it could disagree. Replaced
  with a durable `IDMAP-GAP` log line so identify-phase gaps are fixed
  at the source. **The idmap is now read-only outside run-identify.py.**

### `run-export.py`
The MarkLogic cache persists across builds while YUIDs are stable, so
`if yuid not in ml` served **last build's document** for any entity
whose merged record changed. Now compares `insert_time` and
re-transforms when the merged record is newer.

---

## 5. Storage-layer fixes

### `pipeline/storage/idmap/redis.py`
- **`IdMap.set()` rekey/merge** was a multi-step read-modify-write with
  no atomicity: concurrent rekeys could lose set members, leave dangling
  forward-pointers, or (via a get-after-exists race) silently drop the
  requested mapping. Now a WATCH/MULTI transaction with retry; the
  threaded regression test drives two workers repointing 20 members
  concurrently and asserts nothing is lost.
- **`popitem` (both variants)**: `scan+get+delete` / `randomkey+hgetall
  +delete` let two workers pop the same item, return `(key, None)`, or
  (ReferenceMap) swallow real redis errors as "queue empty" via a bare
  except. Both now claim under WATCH/MULTI; a 4-thread drain test pops
  200 items exactly once. Pop *order* remains arbitrary — harmless now
  that identity is order-independent.
- **`ReferenceMap.merge_ref`** added (see §3).
- **`IdMap.delete_yuid`** added (third wave): guarded deletion of
  token-only YUID sets for the pending-deletes consumer; refuses if real
  members remain. (Regular `delete()` intentionally rejects set keys.)
- **`NetworkOperationMap`**: cached fetch *errors* (000/4xx/5xx) now
  expire after 7 days instead of caching a transient network failure
  forever (a record that failed once stayed missing from every
  subsequent build until someone manually cleared the map). Successes
  and redirects still persist.

### `pipeline/storage/cache/redis.py`
Cache **db-number allocation** was check-then-set on a shared counter:
two workers starting simultaneously could read the same `next`, assign
the same redis db to two different caches, and corrupt both. Now
SETNX/INCR atomic claim (a lost same-name race wastes one slot,
harmlessly).

### `pipeline/storage/cache/postgres.py`
- `iter_keys_type_slice` partitioned rows by position over an
  **unordered** SELECT — with synchronized sequential scans, parallel
  workers see different row orders, so records were double-processed by
  some workers and skipped by others. Now `ORDER BY` the key column.
- `set()` swallowed **every** insert/upsert failure (serialization
  failures, value-too-long, deadlocks — not just duplicate keys),
  losing the write silently. Now: expected duplicate-without-overwrite
  is tolerated and logged; everything else logs and re-raises.
- `get_like` promised "most recent" in its comment but had no
  `ORDER BY`; now `ORDER BY insert_time DESC`.

### `pipeline/storage/idmap/lmdb.py` (TabLmdb)
- Members containing a literal tab silently split into phantom members
  on read (tab is the separator); now rejected at write time.
- Over-long keys (>500 bytes, LMDB's limit) made `__contains__` swallow
  the error and report "absent" — a caller could mint a duplicate
  identity every build. The over-long case now reports loudly (it truly
  cannot exist in the store); *other* LMDB errors now propagate instead
  of masquerading as absence.

### `pipeline/storage/marklogic/rest.py` + `run-load.py`
The batch-update reaper checked `kill.resp` — a field that was **never
assigned** (the thread stores its response in `.result`) — so failed
batch PUTs to MarkLogic always reported success. Now: the thread
catches its own exceptions into `.error`, the reaper reads
`.result`/`.error` and raises on failure, and a new `flush_updates()`
(called at the end of `run-load.py`) joins and verifies the final
in-flight batches, which previously were never checked at all.

### Broken/latent modules repaired
- `pipeline/storage/idmap/memory.py`: had a syntax error (couldn't even
  be imported), an undefined `self.configs`, and a broken `delete`
  using a nonexistent `self.conn`. Now a working in-memory IdMap for
  tests.
- `pipeline/storage/idmap/filesystem.py`: `NameError` on construction
  (undefined global `cfgs`); whole-file persist was truncate-then-write
  (a crash corrupted the entire map) — now atomic temp+rename.
- `pipeline/storage/cache/filesystem.py`: Internal/External/yuid-keyed
  record caches all defaulted to the *same* directory
  (`{name}_record_cache`), so internal and external records for one
  identifier overwrote each other — now distinct directories mirroring
  the postgres tabletypes; `delete()` existed only as a dangling
  `__delitem__` (AttributeError) — implemented; writes are now atomic;
  and `has_item` didn't apply the slash-encoding that `set`/`get` use,
  so any identifier containing `/` was reported absent — fixed (found
  by the new tests).

---

## 6. Delete handling (`pipeline/process/update_manager.py` + `run-process-deletes.py`)

The harvest delete path removed a record and its idmap entry, then hit
three `pass`/FIXME branches — dependents were never rebuilt and
orphaned merged records were never removed, silently. Now:

- every delete appends its required follow-up to
  `{data_dir}/pending_deletes.jsonl` (`rebuild` when other cluster
  members remain, `delete-merged` when the cluster is empty), with the
  full context (yuid, remaining members, timestamps);
- **`run-process-deletes.py`** (third wave) consumes the file: drops the
  stale merged/ML-cache records so the next merge/export cannot serve a
  version containing the deleted member, removes token-only YUID sets
  via `delete_yuid` for emptied clusters, and rotates processed entries
  to `pending_deletes.done.jsonl`. Safe to run any time between harvest
  and merge.

---

## 7. Validation infrastructure (new)

- **`tests/`** (was empty): `test_identity.py` (9 tests — clustering
  semantics, differentFrom enforcement, voting, YUID reuse/split/mint,
  assertion-log round-trip), `test_determinism_fixes.py` (9 tests — one
  per first-wave fix, pure-function with stub stores),
  `test_store_hardening.py` (14 tests — fakeredis-backed atomicity
  incl. threaded rekey/drain races, TTLs, TabLmdb, repaired modules,
  pending-deletes consumer). **32 passing**; `fakeredis` added to
  `requirements_dev.txt`.
- **`experiments/recon_test/`**: the identity engine's proving ground.
  `detrecon.py` + `test_determinism.py` replay all 505k production
  concepts clean and adversarially dirtied (wrong links, splits,
  vanished members, URI variants) with byte-identical-output and
  100%-YUID-reuse assertions. `pull_lux_class.py` streams any LUX class
  from the cluster store into a local LMDB. `legacy_compare.py` +
  `compare_report.py` run the actual `manage_identifiers` code and the
  new engine over identical inputs (cold + rebuild × two arrival orders
  × 1000 injected conflicts) and compute the
  determinism/stability/violation metrics per class — the source of the
  numbers in `docs/build-comparison-report.md`.

## 8. Known-open items (documented, deliberate)

- **Generational-BFS reference discovery**: reference pop order is still
  random; with the loss mechanisms closed this only affects which side
  of the `max_distance` frontier a pathological chain lands on. Assessed
  as best implemented in `rob_refactor`'s task manager, where the
  per-generation barrier has a natural home.
- **`manage_identifiers` retirement**: dormant in production, kept only
  as the comparison baseline and for `test_updates.py`; recommend
  deleting when `rob_refactor` lands.
- Per-class **Person** legacy comparison rerun is in flight (first
  attempt was killed by a machine restart); the deterministic Person
  runs are complete (byte-identical, 100% stability, zero violations).

## Reading order for review

1. `docs/determinism-audit.md` — every defect with file:line and effect
2. this document — what changed, by subsystem
3. `docs/build-comparison-report.md` — measured outcomes on the full dataset
4. `pipeline/process/identity.py` + `tests/` — the engine and its contracts
