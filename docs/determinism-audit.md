# Determinism / data-loss audit

A systematic pass over the codebase for the problem classes behind the
reconciliation nondeterminism: output depending on processing order or
set/dict iteration order, races on shared stores under the 24-process
phases, and silent data loss between builds. Follows the deterministic
identity-resolution work (`pipeline/process/identity.py`,
`run-identify.py`, `experiments/recon_test/`).

Regression tests: `tests/test_determinism_fixes.py` (fixed items) and
`tests/test_identity.py` (identity engine).

## Fixed in this branch

| # | Where | Defect | Effect before fix |
|---|-------|--------|-------------------|
| 1 | `run-all.sh` | The **live** orchestrator never ran `run-identify.py` (only dead `full-build.sh` did) and never cleared stale `assertions-*.tsv` | Builds merged against a never-updated idmap; new entities dropped ("Couldn't find YUID"), stale assertion files folded into identify |
| 2 | `pipeline/process/base/reconciler.py` (Lmdb + Sqlite) | Multi-match tie-break sorted by `len(x)` of a 2-tuple — a no-op — so the "best" match was dict-insertion order | Records reconciled to an arbitrary candidate instead of the most-supported one; result depended on `equivalent` order |
| 3 | `pipeline/process/reconciler.py` `call_reconcilers` | `ids.remove()` while iterating `ids` skipped elements after each removal; drop choice was arbitrary; and removals never applied to the record's own `equivalent` array | Known-distinct pairs survived the diff check (or the wrong member was dropped, varying by order), and even "dropped" entities stayed in the output equivalents |
| 4 | `pipeline/process/reidentifier.py` | `set.pop()` picked an arbitrary YUID when equivalents resolved to more than one | Emitted record id (and an idmap write) varied per process/run |
| 5 | `pipeline/process/merger.py` | Merge order for sources sharing a `merge_order` was redis set-iteration order (stable sort preserved it) | First-wins rules (primary name, timespans, geometry) flipped between runs; now tie-broken on source name + record identifier |
| 6 | `pipeline/process/merger.py` (4 sites) | `dr.append(dm)` appended dimensions to the loop-local dict — `AttributeError` (or `NameError` on an empty list) | Any Linguistic/Set/Visual/Digital object with a non-matching dimension killed the whole merge slice |
| 7 | `run-merge.py` reference phase | Base-record selection iterated the unordered `equivs` set | Different base record chosen per run → different label/field precedence in merged output |
| 8 | `run-reconcile.py` reference phase | `did_ref` marked a reference done **before** acquiring it | A transient fetch failure silently dropped the record and every concept reachable only through it, for the whole build; now marked done only after successful acquire (failed refs stay eligible for re-queue) |
| 9 | `pipeline/storage/cache/postgres.py` `iter_keys_type_slice` | Sliced by row position over an **unordered** SELECT | Parallel workers saw different row orders (synchronized seq scans) → records double-processed and skipped; now `ORDER BY` key |
| 10 | `pipeline/storage/cache/redis.py` | Cache db-number allocation was check-then-set | Two workers could allocate the same redis db for different caches (cross-cache corruption); now SETNX/INCR atomic |
| 11 | `pipeline/process/base/index_loader.py` | Name/URI index collisions were last-writer-wins over an unordered record stream | The index winner for a shared name/URI differed between builds → different equivalence assertions → different clusters; now smallest recid wins (order-independent) |
| 12 | `pipeline/sources/authorities/getty/reconciler.py` `extract_names` | Keys not lowercased in the fallback path (index is lowercased → silent miss) and matched language priorities clobbered to 3 by later iterations | Systematic dropped/incorrectly-ranked AAT name matches |
| 13 | `merge-metatypes.py` | Unordered `glob` | Per-key metatype list order varied by filesystem |
| 14 | `pipeline/process/reference_manager.py` `iter_done_refs` | Empty/blank line yielded a bogus `[""]` item (`"".split("|")` is truthy) | Single-process merge crashed/mishandled on empty ref file |

## Second wave: previously flagged, now fixed

Regression tests: `tests/test_store_hardening.py` (fakeredis-backed for
the redis paths, tmpdir/pure for the rest).

1. **`IdMap.set()` rekey** is now a WATCH/MULTI transaction with retry —
   concurrent rekeys/merges can no longer lose set members or leave
   dangling forward-pointers, and the "key deleted between exists and
   get" race no longer silently drops the requested mapping.
2. **Work-queue pops are atomic**: `IdMap.popitem` and
   `ReferenceMap.popitem` claim their key under WATCH/MULTI, so two
   workers can never process the same item or see `(key, None)`; real
   redis errors propagate instead of masquerading as an empty queue.
   (Pop *order* remains arbitrary — harmless now that identity
   assignment is order-independent.)
3. **`add_ref` distance bookkeeping**: new `ReferenceMap.merge_ref`
   applies min-distance / type-if-unset atomically, and re-queueing a
   done ref adds to `all_refs` *before* deleting from `done_refs` (a
   crash duplicates work instead of losing the reference). This was the
   main surviving between-builds concept-loss mechanism.
4. **Merge phase cross-slice claim** (`run-merge.py`): when several
   internal records share a YUID, only the lexicographically-smallest
   member still present in its recordcache builds the merged record —
   replacing the insert_time TOCTOU race with a deterministic rule.
5. **Export staleness** (`run-export.py`): the MarkLogic cache entry is
   re-transformed when the merged record is newer (insert_time
   comparison) instead of blindly reusing last build's document.
6. **Index-open failures are fatal and loud** (`base/reconciler.py`):
   both Lmdb and Sqlite reconcilers retry (0.5s backoff) then raise,
   instead of silently disabling reconciliation for the whole worker.
7. **VIAF equivalents index** (`oclc/index_loader.py`): the sorted
   equivs file regenerates whenever any shard is newer, sorts under
   `LC_ALL=C`, and ident→VIAF collisions keep the smallest VIAF id
   deterministically.
8. **TabLmdb**: members containing tabs are rejected at write time
   (they silently split into phantom members on read); over-long-key
   `__contains__` reports absent loudly while real LMDB errors now
   propagate. (Single-member str round-trip is kept — callers rely on
   it — and documented.)
9. **Write failures surface**: postgres `set()` re-raises everything
   except the expected duplicate-key-without-overwrite case; MarkLogic
   `update_multiple` now reads the actual thread results (the old check
   read a field that was never set) and a new `flush_updates()` —
   called from `run-load.py` — joins and verifies the final batches,
   which were previously never checked at all.
10. **`get_like`** orders by `insert_time DESC` as its comment promised.
11. **Broken/latent modules repaired**: `idmap/memory.py` (syntax
    error, broken delete) works again for tests; `idmap/filesystem.py`
    constructs (`cfgs` NameError) and persists atomically (temp+rename);
    filesystem record caches get distinct directories (internal/external
    records no longer overwrite each other), a working `delete`, atomic
    writes, and a `has_item` that applies the same slash-encoding as
    `set`/`get` (identifiers containing `/` used to be reported absent).
12. **NetworkOperationMap** error entries (000/4xx/5xx) now expire after
    a week instead of caching a transient fetch failure forever;
    successes and redirects still persist.
13. **`update_manager` deletes are recorded**: each delete appends its
    required follow-up (rebuild the cluster without the member, or
    remove the orphaned merged record) to `pending_deletes.jsonl` in the
    data dir, instead of silently dropping the consequences. Acting on
    that file (rebuilding dependents) remains future work.

## Third wave

- **The idmap is now read-only outside `run-identify.py`**: the
  reidentifier's last fallback write (a merge-phase identity mutation
  racing across 24 workers) is replaced with a durable `IDMAP-GAP` log
  line so identify-phase gaps get fixed at the source.
- **`pending_deletes.jsonl` has a consumer**: `run-process-deletes.py`
  (logic in `UpdateManager.process_pending_deletes`, tested) drops the
  stale merged/ML-cache records for deleted members and removes
  token-only YUID sets via the new guarded `IdMap.delete_yuid`;
  processed entries rotate to `pending_deletes.done.jsonl`.

## Still open

- **Reference pop order** is still random (`randomkey`) — harmless for
  identity but the generational BFS from the experiment design would
  make the *distance frontier* fully deterministic too; `merge_ref`
  removes the loss mechanism but a concept discovered at distance
  `max_distance+1` in one exploration order and `max_distance` in
  another can still differ between builds in pathological graphs.
  Assessed as best done inside `rob_refactor`'s task manager, where the
  per-generation barrier has a natural home.
- The per-process `ref_cache` short-circuit only applies to distance-1
  AAT refs, which cannot be improved upon (1 is the minimum), so it was
  left in place.

## Testing through the pipeline with real data

`experiments/recon_test/` replays all 505k production **concepts**
through the assertion→cluster→assign flow (clean and adversarially
dirtied) and is the model for extending coverage: the merge-phase
equivalents of those tests (`test_merge_output_independent_of_...`) run
on stubs today and could be driven by the same concepts LMDB, and by
people/places/events snapshots once available, to assert byte-identical
merged output across worker counts and orderings end to end.
