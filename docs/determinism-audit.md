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

## Flagged, not yet fixed

Larger or riskier changes, ranked by expected impact. The identity
refactor already removes most idmap *write* traffic from the parallel
phases, which shrinks the blast radius of several of these.

1. **`IdMap.set()` rekey is a non-atomic read-modify-write**
   (`pipeline/storage/idmap/redis.py:289-311`). Concurrent rekeys/merges
   can lose set members and leave dangling forward-pointers. Post-identity
   it is still called from `reidentifier.py` fallback writes. Proper fix:
   Lua script or `WATCH/MULTI`; or remove the remaining runtime writes
   entirely and make the idmap read-only outside `run-identify.py`.
2. **Work-queue pops are non-atomic and random**
   (`IdMap.popitem` scan+get+delete, `ReferenceMap.popitem` via
   `randomkey`). Two workers can pop the same ref (duplicate work) or get
   `(key, None)`; order is random. Identity resolution makes the *ID*
   consequences harmless, but reference **distances** still race (below).
   Fix: atomic Lua pop (SPOP-like) — or adopt the generational BFS from
   the experiment design.
3. **`add_ref` distance bookkeeping races across 24 workers**
   (`reference_manager.py:100-139`): min-distance updates are
   check-then-act over shared redis, and the per-process `ref_cache`
   layer makes it worse. Concepts near the `max_distance` frontier are
   included in some builds and dropped in others. This is the main
   surviving between-builds concept-loss mechanism.
4. **Merge phase cross-slice TOCTOU** (`run-merge.py` `insert_time`
   guard): two slices resolving different records to the same YUID both
   build it; last writer wins. Bounded (each writes a full merge) but
   non-reproducible.
5. **Export reuses stale MarkLogic cache** (`run-export.py:52-61`):
   YUIDs are stable, so changed records short-circuit to last build's
   export unless the ML recordcache is cleared. Needs an orchestration
   decision (clear vs. compare timestamps).
6. **Index-open failures silently disable reconciliation per worker**
   (`base/reconciler.py` Lmdb/Sqlite `except → index = None`, then
   `should_reconcile` returns False). A worker that loses the lock race
   emits zero assertions for its slice, silently. Should be fatal, or at
   minimum a durable log + retry.
7. **VIAF equivalents index**: stale `viaf_sort_equivs.csv` reused if
   present (`oclc/index_loader.py:27`), and ident→VIAF collisions are
   last-writer-wins over per-slice shards.
8. **TabLmdb value ambiguity** (`idmap/lmdb.py`): single-member sets
   round-trip as `str`, members containing tabs split, keys >500 bytes
   raise (and `__contains__` swallows it → invisible keys, potential
   re-mints every build).
9. **Broad exception swallowing that loses writes**: postgres
   `set()` (`cache/postgres.py:438`) swallows all insert failures;
   MarkLogic `update_multiple` (`marklogic/rest.py:213-237`) never reads
   thread results, so failed batch PUTs report success.
10. **`get_like` has no ORDER BY** despite its own comment saying it
    must return the most recent match (`cache/postgres.py:243`).
11. **Broken/latent modules**: `idmap/memory.py` (syntax error),
    `idmap/filesystem.py` (`cfgs` NameError, non-atomic whole-file
    rewrites), filesystem record caches sharing one `tabletype`
    directory, `update_manager.py` delete path is a stub (dangling
    references on delete).
12. **NetworkOperationMap caches fetch errors across builds** (db 2
    persists): a transiently failed external record can stay failed in
    every subsequent build until the cache is cleared.

## Testing through the pipeline with real data

`experiments/recon_test/` replays all 505k production **concepts**
through the assertion→cluster→assign flow (clean and adversarially
dirtied) and is the model for extending coverage: the merge-phase
equivalents of those tests (`test_merge_output_independent_of_...`) run
on stubs today and could be driven by the same concepts LMDB, and by
people/places/events snapshots once available, to assert byte-identical
merged output across worker counts and orderings end to end.
