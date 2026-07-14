# Build comparison: legacy vs deterministic identity resolution

**Scope.** This compares the *identity phase* of the pipeline — the part
all of this work changed — run both ways over the complete production
concepts dataset (505,326 merged concept clusters, 970,858 member URIs,
~1.15M simulated source records). "Legacy" is the actual
`ReferenceManager.manage_identifiers` code driven record-by-record in
arrival order, exactly as `run-reconcile.py` drove it before the
refactor. "New" is `pipeline/process/identity.py`: assertion log →
voted constrained union-find → prior-YUID-reuse assignment → bulk apply.
Both consume **byte-identical input records** and write to the same
redis-shaped idmap (fakeredis, in-process). 1,000 differentFrom conflict
scenarios are injected into both. Each configuration ran twice with
different record arrival orders, cold (empty idmap) and as a rebuild
(production idmap preloaded, new update token).

Harness: `experiments/recon_test/legacy_compare.py`; metrics:
`experiments/recon_test/compare_report.py`. A full production build
(acquire/reconcile/merge/export against postgres + real redis +
MarkLogic) requires the pipeline machine; everything measured here is
the phase those systems feed.

## Results

| Metric | Legacy cold | Legacy rebuild | New cold | New rebuild |
|---|---|---|---|---|
| Identical output across two arrival orders | **no** | **no** | **yes** (byte-identical) | **yes** (byte-identical) |
| Members whose YUID differs between orders | 946,499 (100%) | 160,061 (**16.9%**) | 0 | 0 |
| Production YUIDs kept on rebuild | — | 843,278 / 920,119 registered (91.6%); **86.9%** of all members | — | 970,858 / 970,858 (**100%**) |
| Members missing from the identity map | 50,842 (**5.2%**) | 50,739 (5.2%) | 0 | 0 |
| Production clusters fragmented (excl. injected conflicts) | 3,379 | 3,379 | 0 | 0 |
| differentFrom pairs violated (of 1,000) | 683 | **902** | 0 | 0 |
| Conflict resolution | arbitrary, varies by order | arbitrary | voted, reported, reproducible | voted, reported, reproducible |
| Identity wall time (this hardware, in-process store) | 288 s | 224 s | **53 s** | **52 s** |
| Redis commands | 12.55 M | 9.56 M | 2.45 M | 2.45 M |
| Redis **round trips** | 7.16 M | 8.16 M | **297** | **297** |

### Speed

In-process, the new engine is **~4–5× faster** (52–53s vs 224–289s).
Against a real redis the gap widens dramatically: legacy's 7–8M
*sequential, per-record* round trips at a conservative 100µs each add
**12–14 minutes of pure network wait per build**, serialized inside the
reconcile workers where they also contend with each other. The new
engine performs 297 round trips (pipelined batches) — network cost is
effectively zero, and phase-1 reconcile workers no longer touch the
idmap at all, removing the known contention bottleneck entirely.

The identity resolution itself is a single post-pass: ~35s of
clustering + assignment + bulk load for 1M members. It scales linearly
with edge count and parallelizes by qua-type shard if ever needed (see
"People, places" below).

### Quality

1. **Reproducibility.** Legacy cold builds share zero YUIDs between two
   runs (uuid4). More importantly, legacy **rebuilds** — the everyday
   operation, where stability is the whole point — still disagree with
   each other on **16.9% of members** depending on nothing but arrival
   order. The new engine is byte-identical in every configuration.
2. **Identity stability across builds.** Legacy keeps 91.6% of the
   production YUIDs it re-registers; counting the members it silently
   drops (below), only **86.9%** of the full member set retains its
   production identity per rebuild. Every lost YUID is a broken
   permalink or a re-minted entity downstream. The new engine: 100%.
3. **Silent member loss (newly discovered defect).** The comparison
   surfaced a legacy bug beyond the audit: in `manage_identifiers`,
   when a record resolves to exactly one existing YUID, its **novel**
   equivalents (those not yet in the idmap) are never registered —
   `equiv_map` only carries equivalents that were already known. Chain
   tails and any reconciler output mixing known + new ids fall through.
   On this workload that leaves **5.2% of members (≈51k)** with no
   idmap entry at all, and fragments 3,379 clusters into multiple
   YUIDs. This is the same family as the "concepts lost between
   builds" symptom. The new engine registers every asserted member.
4. **differentFrom enforcement.** Legacy has none at identity time:
   68–90% of injected known-distinct pairs ended up co-clustered
   (rebuild is *worse* because prior state pre-links clusters). The new
   engine violated **zero** in every run, resolves the disputed record
   by vote weight, reproducibly, and writes every refused assertion to
   `identity_conflicts.jsonl` with its asserters — a reviewable
   curation artifact instead of a silent wrong merge.

## Everything fixed (three commits on this branch)

**1. Deterministic identity resolution** (`661cf17`):
`pipeline/process/identity.py` + `run-identify.py` + orchestration;
reconcile slices log assertions instead of live-minting. Validated by
`experiments/recon_test` (clean + adversarially dirtied replay of all
505k concepts) and now by this head-to-head comparison.

**2. First audit wave** (`6f95651`) — 14 defects, each with a
regression test (`tests/test_determinism_fixes.py`): the live
orchestrator never running identify; the no-op multi-match sort in the
base reconciler; the self-mutating differentFrom filter that also never
applied its drops to the record; arbitrary `set.pop()` YUID choice in
the reidentifier; redis-set-order merge ties (first-wins primary names
flipping between runs); the four dimension-merge crash sites; unordered
base-record selection in merge refs; `did_ref`-before-acquire
(fetch-failure = whole subtree silently dropped for the build);
unordered SQL slicing double/skip-processing records; racy redis db
allocation; last-writer-wins name/URI indexes; the AAT case-sensitivity
match bug; unordered metatypes merge; blank-line ref parsing.

**3. Second wave — everything previously flagged** (`93722b7`), tested
in `tests/test_store_hardening.py` (fakeredis, incl. threaded races):
atomic WATCH/MULTI `IdMap.set` rekey and both `popitem`s; atomic
min-distance reference bookkeeping (`merge_ref`) with loss-safe
re-queue ordering; deterministic cross-slice YUID claim in merge;
export staleness detection vs the MarkLogic cache; loud index-open
failures; VIAF index regeneration + deterministic collision winner;
TabLmdb tab-member rejection; postgres/MarkLogic write failures
surfaced (including never-checked final batches, now flushed and
verified); `get_like` ordering; 7-day TTL on cached network errors;
repaired memory/filesystem idmap modules and filesystem caches
(distinct directories, atomic writes, working delete, slash-encoded
`has_item`); delete follow-ups durably recorded to
`pending_deletes.jsonl`.

30 tests pass; the experiment harnesses replay the full production
concepts corpus.

## What remains, and assessment of approaches

1. **Reference discovery is still a random-order shared queue.**
   `merge_ref` closed the *loss* mechanism, but a concept discovered at
   distance `max_distance+1` under one exploration order and
   `max_distance` under another can still flip in/out between builds in
   pathological graphs. *Assessment:* replace the `randomkey` pop loop
   with generational BFS — process all distance-N refs, barrier,
   then distance-N+1 (each generation partitioned by `hash(uri) %
   workers`). Deterministic ref set and distances, simpler code, and it
   removes the last redis polling loop. Moderate effort, best done as
   part of `rob_refactor`'s task manager.
2. **`pending_deletes.jsonl` has no consumer.** Deletes now record
   their required follow-up (rebuild cluster without member / remove
   orphaned merged record) but nothing acts on it. *Assessment:* small
   dedicated phase after identify; the file already contains everything
   needed.
3. **Make the idmap read-only outside `run-identify.py`.** The
   reidentifier still has fallback writes; with the claim fix they are
   near-dead code. Removing them makes the merge phase provably
   side-effect-free on identity. Low effort, high assurance.
4. **Legacy `manage_identifiers` should be retired**, not fixed: the
   novel-equivalent registration bug found here is structural (it would
   need the same cluster-level view the new engine already has). It
   survives only as dead code and in `test_updates.py`.
5. **Port to `rob_refactor`.** Everything here was built on `main`'s
   layout; the rob branch's `ReconcileManager`/`MergeManager` map 1:1
   onto the same call sites (I verified the equivalent line numbers
   earlier). The identity engine module is layout-agnostic.

## How this will handle people, places, events, works

**The engine is type-agnostic by construction**: nodes are qua-typed
URIs (`…##quaPerson`), so the assertion graph partitions naturally by
type — a Person edge can never join a Place cluster. That gives both a
correctness property and a scaling lever (shard the identify pass by
qua type; run shards in parallel or sequentially under one memory cap).

- **People — the hard case, and the one that gains the most.** Person
  reconciliation leans on *name* matching (VIAF/LCNAF/ULAN/Wikidata,
  115M-row VIAF equivalence table) where collisions are endemic
  ("John Smith, 1850–") and differentFrom assertions are concentrated
  (Wikidata differents are mostly people). Three of the fixes land
  directly here: the multi-match tie-break (was returning an arbitrary
  candidate instead of the best-supported one), deterministic name-index
  collision winners, and cluster-level differentFrom with voting — the
  legacy 68–90% violation rate measured above would have been
  person-cluster wrong-merges. Scale: person edges plausibly 10–50M;
  at that size the dict-based aggregator wants the int-interned array
  variant (~16 bytes/edge → a few GB) or per-type sharding; both are
  mechanical changes to `load_assertions`/`cluster` with the same
  determinism argument (fixed global sort order).
- **Places.** Smaller graph, but merge quality was the visible problem:
  `part_of` and `defined_by` geometry selection were first-wins over
  redis set order — fixed by the deterministic merge ordering and the
  cross-slice claim. The `part_of` cycle problem (lux-places-issues) is
  orthogonal to identity and stays with the hierarchy tooling.
- **Events/periods.** Few external authorities, mostly internal records
  with timespan merging (order-sensitive tie-breaks now deterministic).
  Identity is near-trivial; the claim fix matters where several
  internal sources describe the same event.
- **Works/objects.** Dominated by internal records sharing YUIDs across
  units (YUAG/YCBA/YPM) — exactly the cross-slice claim scenario — plus
  the dimension-merge crash that used to kill whole merge slices.

**Validation path:** the concepts corpus validated the engine
end-to-end because we had an exit-of-pipeline snapshot to replay. The
same harness runs unmodified on people/places snapshots (`ingest.py`
in lux-concepts-scc style → LMDB → `legacy_compare.py --db …`), which
would turn the scale estimates above into measurements — worth doing
for Person before the first production build with the new engine.

## Caveats

- Wall times use an in-process store; real-redis gaps are larger and
  in legacy's disfavor (round-trip counts above).
- Source records are simulated from cluster membership (chain + hub
  asserters); production records assert richer, messier equivalent
  sets — which strengthens the case for voting, and is exactly what
  the dirty-input suite stress-tested.
- This compares the identity phase; acquire/reconcile/merge/export are
  shared code between the two approaches (now with the audit fixes) and
  were exercised by the unit suites rather than timed here.
