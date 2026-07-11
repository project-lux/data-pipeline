# Deterministic reconciliation prototype

Tests a redesign of the reconcile/identify phases so that cluster
membership **and** YUID assignment are a pure function of the input
assertions — independent of worker count, scheduling, and processing
order — while keeping the expensive discovery work embarrassingly
parallel.

Motivation: today UUIDs come from `uuid4()` at whichever of the 24
processes first touches a cluster, cluster merges are resolved by
arrival order (`manage_identifiers`), and phase-2 references pop in
`randomkey()` order — so two builds of the same data differ. The idmap
is also a per-record read-modify-write bottleneck.

## Design under test

1. **Emit (parallel, write-only)** — workers do the discovery work and
   emit *assertions* `(asserter, a, b)` to per-worker append-only files.
   No shared mutable state, no idmap contention. `differentFrom` pairs
   are a parallel stream of the same shape.
2. **Cluster (deterministic)** — assertions aggregate into voted pair
   edges (votes = distinct asserters). A constrained union-find
   processes edges most-voted-first (lexicographic tie-break); a union
   is refused when any `differentFrom` pair spans the two clusters —
   the constraint is evaluated against the full cluster regardless of
   which records the links came from. Refused assertions land in
   `conflicts.jsonl` with their asserters and vote context, so
   conflict policy can evolve (better voting, provenance weighting)
   without touching the engine.
   - The "record 3" case (1=A,B / 2=C,D / 3=B,C, B≠C): 3 is still
     assigned to one side, chosen by the weight of the *other*
     assertions about 3, B and C; the B=C link is refused and reported.
3. **Assign (deterministic)** — members vote to reuse the YUID they
   held in the prior idmap, so adding a URI to a cluster never changes
   its YUID; after a cluster split, competing claims are resolved by
   voter count (deterministic tie-breaks); only clusters with no prior
   YUID mint `uuid5(min member)`. The resulting map is what would be
   bulk-loaded into redis for the merge phase.

## Test data

`/Users/wjm55/lux-concepts-scc/concepts.lmdb` — all 505k concept
records as they exit the production pipeline. Each record's
`equivalent` array becomes the incoming members ("as if new"), and the
record's own `id` doubles as the prior-build idmap for rebuild tests.

## Run it

```bash
# unit tests of the conflict/voting/assignment semantics
uv run test_determinism.py --skip-integration

# full run: 5 builds over all 505k clusters
#  - rebuild at 24 workers vs 8 workers, different shuffle seeds
#    -> clusters.tsv byte-identical, 100% production YUID reuse
#  - 1000 injected different_from conflict scenarios -> resolved by votes
#  - perturbation (new URI in every 500th cluster) -> YUIDs unchanged
#  - two cold starts (no prior map) -> byte-identical uuid5 minting
uv run test_determinism.py --db /Users/wjm55/lux-concepts-scc/concepts.lmdb \
    --work /tmp/recon-test
```

## Scale notes (toward the real 100M+ edge run)

- The prototype keeps edges/DSU in plain dicts; at full pipeline scale
  the same algorithm runs on int-interned ids in flat arrays
  (~100M edges ≈ a few GB) or over an external sort of the assertion
  files — the determinism argument only needs a *fixed global edge
  order*, which sorting gives for free.
- Redis remains the serving store: phase 3's output is bulk-loaded
  (pipelined SET) instead of being built via per-record check-then-set
  round trips during reconciliation.
