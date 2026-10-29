# 42. Build roles: internal / external as a build configuration

**Priority:** P2 · **Size:** L (two PRs, 42a and 42b) · **Depends on:** 20, 23, 35, 39 · **Do after:** 33
**Read from:** nothing to port. Neither refactor branch has this; it is new
work on `main`, planned 2026-10-29 with Rob.

## Intent

Today every source record says `"type": "internal"` or `"type": "external"`,
and that one word decides both **what the source is in this build** and
**how its records are stored**. Running a different build over the same
sources (all of the Getty Museum's data as the starting set, with the
authorities as they are) means a second, hand-edited copy of every source
record.

After this item:

* source records describe a source; they no longer have to say which side
  of a build it is on;
* a separate `build` record names a build's internal sources (its starting
  set) and its external ones;
* `lux --build getty reconcile --source all` runs the Getty build with the
  same source records, datacaches and reconcile indexes as the LUX build;
* nothing that is written to a cache or the idmap depends on the role, so
  the same source can be internal in one build and external in another.

```json
{"type": "build", "name": "lux",   "internal": ["ycba", "yuag", "ypm", "ils", "pmc", "ipch"], "external": "*"}
{"type": "build", "name": "getty", "internal": ["getty_museum"], "external": "*", "exclude": ["ycba", "yuag", "ypm", "ils", "pmc", "ipch"]}
```

## Why it matters

* Different collections (Getty Museum, a single Yale unit, a test set) can
  be built without a second configuration tree.
* The authorities (aat, wikidata, lc, viaf ...) are weeks of downloading and
  loading. With the role out of the storage format their datacaches and
  indexes are loaded once and shared by every build.
* It removes role-dependent special cases from the acquirer and merge
  (`if type == "internal": ident = ident.split("#")[0]` and friends), which
  are the kind of branch that silently diverges.

## What `type` controls today

Three different things, which is why it cannot just be moved:

**1. The build's starting set.** `--source all` for reconcile and merge is
every internal source (`cli/reconcile.py:45`, `cli/merge.py:43`,
`phases/reconcile.py:63`, `phases/merge.py:62`); external records are only
reached by following references. `_handler.resolve_sources`, `cli/_targets.py`,
`cli/list.py`, `cli/counts.py`, `cli/testinstall.py` group by it.

**2. Precedence inside the build.** Internal records win `part_of` and
primary names (`process/merger.py:67-80`, `:894-913`); internal namespaces
decide which record builds a cluster (`phases/merge.py:255`, `make_claim_member`),
which records are built at distance 0 (`reference_manager.py:23`, `:606-627`),
the equivalents cap (`reidentifier.py:29`), the reconciler's internal-equivs
filter (`process/reconciler.py:71`), the merge order of externals
(`phases/merge.py:221`), `update_manager.py:13, 155` (`has_internal`), the
URI match order (`config.build_match_index`, internal first) and `viz.py:86`.

**3. The storage format.**

| Where | Internal | External |
|---|---|---|
| record cache key (`acquirer.py:87`, `:131`) | bare identifier | `identifier##quaType` |
| record cache class (`config.py:751-772`) | `InternalRecordCache`: table `<name>_record_cache`, no `yuid` column | `ExternalRecordCache`: `<name>_ext_record_cache`, `yuid` column |
| reconciled record cache | none | `ExternalReconciledRecordCache` (instantiated; nothing outside `cli/` reads it) |
| default harvester | `ASHarvester` | `harvesterClass` or none |
| idmap keys (`idmap/postgres.py:203`, `redis.py:169`, `filesystem.py:119`) | full `http` URI | namespace shortened to `<name>:` |
| key readers that assume the above | `merger.py:113`, `phases/merge.py:180`, `update_manager.py:100` strip or skip the `##qua` part | |

Group 1 and 2 are the build's business. Group 3 is why a source's role
cannot change against existing data; 42a removes it.

Mappers also reach other sources through the groups by name:
`configs.internal["ycba"]` (`yale/ycba/mapper.py:14`),
`internal["ashmol"]` (`ashmol/mapper.py:9`), `internal.get("ils")`
(`yale/library/mapper.py:160`), `external["lcnaf"]` / `["lcsh"]`
(`lc/mapper.py:51, 387`), `external["wikidata"]` (`oclc/mapper.py:83`,
`fast/mapper.py:51, 126`), `external["japansh"]` (`japan/mapper.py:316`),
`external["wikimedia"]` (`lux/final/mapper.py:21`), `external["aat"]`
(`getty/reconciler.py:8`, `base/reconciler.py:67`, `phases/reconcile.py:50`),
`wikidata/base.py:15`. These break as soon as a build moves or excludes the
source they name.

## Design

### 42a. Storage that does not depend on the role (one PR)

1. **Every record cache is keyed `identifier##quaType`.** The acquirer
   stores and looks up the typed key for every source (`acquirer.py:87`,
   `:131` lose their `type == "external"` test). The type is the mapped
   record's `data.type` when storing. When looking up without a `rectype`
   (reconcile's main loop calls `acquire(recid, data=row)`), use the
   supplied row's `data.type` if there is one; otherwise skip the lookup.
   `merger.py:113`, `phases/merge.py:180` and `update_manager.py:100` stop
   stripping or special-casing the suffix. `--recid` paths
   (`phases/merge.py` `fetch_records`, `viz`, `debug-reconcile.py`) accept
   either form and add the type when it is missing.
   *This edits merge and acquirer code.* These are deliberate, listed
   exceptions to ground rule 3; each edit is a key-format change only, and
   no `with timer.stage(...)` block changes its logic.
2. **Cache classes are a property of the source, not the role.** The
   defaults in `config.instantiate` come from a new optional source key,
   `"storage": "internal" | "external"`, which falls back to the record's
   legacy `type`. So every existing table keeps its name and columns, and
   `getty_museum` keeps `getty_museum_ext_record_cache` in a build where it
   is internal. The default harvester follows the same key.
3. **Idmap key shortening is a property of the source.** The prefix map is
   built from every source whose optional `"idmap_prefix"` is true, falling
   back to `type == "external"` (`postgres.py:203`, `redis.py:169`,
   `filesystem.py:119`). The existing idmap is unchanged, needs no
   migration, and a source keeps the same key form whatever role a build
   gives it. `_reusable_key` (the LRU heuristic, `postgres.py:369`) is about
   access patterns, so it uses the build's external prefixes (from 42b;
   until then, the same set).
4. **`cfgs.sources`**: every source record by name, whatever its role. The
   mapper and reconciler lookups listed above use it instead of
   `internal[...]` / `external[...]`. `split_curie` and `instantiate` use it.

After 42a the record caches of the internal sources hold typed keys. A full
rebuild clears and refills them (`lux clear --source all`), so nothing needs
migrating there. A cache kept for an incremental build needs a one-off rekey,
shipped as a function in `lux_pipeline/storage/cache/postgres.py` that a
`lux` option calls (not a script):
`UPDATE <name>_record_cache SET identifier = identifier || '##qua' || (data->'data'->>'type') WHERE identifier NOT LIKE '%##qua%'`,
inside one transaction, after `lux backup`.

### 42b. The build record (one PR)

1. A new record type, `{"type": "build", "name": ..., "internal": [...],
   "external": [...] | "*", "exclude": [...]}`, filed by
   `handle_config_record` like `default` records (and loadable from a
   sub-config store, item 35). `"*"` is every source not named in
   `internal` or `exclude`.
2. Source records may use `"type": "source"`. `internal` / `external` stay
   valid and are the **default role** when no build is selected, so the
   live configs and the `pipeline-configs` repo load unchanged.
3. The build is chosen by, in order: `lux --build NAME` (a global option,
   passed to workers like `--log`), `LUX_BUILD` in the environment or
   `.env`, a `build` key in `system` / `base`. If none is set, or no build
   records exist, roles come from the source records: today's behaviour.
4. `Config` fills `self.internal` / `self.external` from the selected build
   and sets each source's `cfg["type"]` to its role in that build, so every
   reader in group 1 and 2 above works unchanged. Excluded sources are not
   loaded (not in either group, not in `cfgs.sources`, not instantiated,
   not in the match index; their URIs are treated as unknown namespaces).
5. **Per-build stores.** The build record may name the stores that belong to
   one build, each falling back to today's value: `idmap_name`, the
   reference maps (`all_refs`, `done_refs`, `record_refs`, `redirects`, `networkmap`), the
   `merged` and `marklogic` results, `internal_uri`, and a
   `record_cache_prefix` added to every source's record-cache table names
   (`<prefix>_<name>_ext_record_cache`). Datacaches and reconcile indexes
   are always shared.
6. `lux list` / `lux counts` show the selected build and the excluded
   sources; `lux testinstall` checks that every name in every build record
   is a configured source; status file prefixes include the build name when
   one is selected (`<log_dir>/<command>-<build>-<timestamp>`).

## What to keep from `main`

* The groups' names and shape: `cfgs.internal`, `cfgs.external`,
  `cfg["type"]`. Changing their meaning to "role in the selected build"
  is the whole trick; renaming them would touch ~50 files for nothing.
* Config order. Within a role, sources keep configuration order;
  `build_match_index` keeps internal first. A build record's `internal`
  list order is not used, so the same build gives the same order however
  its record is written.
* The legacy scripts (if 33 has not landed) select through `select_sources`
  and so follow the build too; they get no `--build` option and use
  `LUX_BUILD`.

## Known hazards

* **Reconcile's lookup on incremental builds.** The main loop has no type
  until it maps. Using `row["data"]["type"]` works for sources whose
  datacache rows are already Linked Art (the Yale units), but a mapper can
  change the type (`yale/yuag/mapper.py:26` turns a `Group` with
  `born`/`died` into a `Person`); then the lookup misses and the record is
  re-mapped, so it is correct but slower. On a full rebuild the lookup always
  missed anyway (`docs/performance-backlog.md` §4.4). Measure
  `acquire.cache_hit` on an incremental build before and after.
* **`VARCHAR(120)`** on the record cache key. `##qua` plus the longest type
  name (`HumanMadeObject`, `LinguisticObject`) adds 21 characters. Check
  `SELECT max(length(identifier))` on every internal record cache on the
  build machine first; widen the column in the rekey if needed.
* **Same YUIDs or not.** Two builds that share `idmap_name` share YUIDs and
  must not run at the same time (the update token is per idmap). The
  default for a new build record should be its own idmap; sharing is
  something you choose.
* **Excluded sources referenced by name.** After 42a the mapper lookups go
  through `cfgs.sources`; an excluded source is absent there too. Each of
  those call sites must handle the source being missing (log once, carry
  on), or the build record must not exclude it. List the dependencies in
  `testinstall`.
* **`reconcile_filter_internal_equivs`** (`process/reconciler.py:68`, marked
  FIXME) and `reidentifier.MAX_CLUSTER_EQUIVS` change meaning with the
  internal set. That is intended, but say so in the PR.

## Open questions (settle before starting)

1. In a Getty build, are the Yale sources excluded (above) or external
   (reconciled in when a Getty record points at them)? The build record
   allows both; which one should the first `getty` record use?
2. Should a new build get its own idmap by default (recommended above), or
   share LUX's YUIDs so the same entity has the same YUID in both?
3. Is the incremental-build cost in the first hazard acceptable, or should
   the reconcile lookup use an untyped prefix lookup
   (`identifier LIKE 'x##qua%'`) for the internal main loop?

## Files

| Action | Path on `main` | Part | Notes |
|---|---|---|---|
| Edit | `lux_pipeline/process/base/acquirer.py` | a | typed keys for every source |
| Edit | `lux_pipeline/process/merger.py`, `process/phases/merge.py`, `process/update_manager.py` | a | drop the bare-key special cases |
| Edit | `lux_pipeline/config.py` | a, b | `storage`, `idmap_prefix`, `cfgs.sources`; then build records, role assignment, per-build stores |
| Edit | `lux_pipeline/storage/idmap/postgres.py`, `redis.py`, `filesystem.py` | a | prefix map from `idmap_prefix` |
| Edit | `lux_pipeline/storage/cache/postgres.py` | a | the rekey function |
| Edit | the mappers and reconcilers listed above | a | `cfgs.sources[...]` |
| Edit | `lux_pipeline/cli/entry.py`, `_handler.py`, `list.py`, `counts.py`, `testinstall.py` | b | `--build`, display, checks |
| Edit | `lux_pipeline/cli/_handler.py`, `watch.py`, `scripts/wait-for-flags.sh` | b | build name in the default status prefix; `lux watch --latest` and the shell waits still find and parse it |
| Edit | `config_cache/config_cache/*.json` (untracked copies) | b | a `lux` build record; nothing else needs to change |
| Edit | `docs/codebase.md`, `docs/rebuild.md`, `README.md` | b | |
| Create | `tests/test_build_roles.py` | a, b | |

## Steps

**42a**

1. `cfgs.sources` and the mapper / reconciler lookups through it; tests
   with a `StubConfigs` that moves a source between groups.
2. `storage` and `idmap_prefix` with the legacy fallback; a test that the
   live config copies produce exactly today's cache classes, table names
   and prefix maps.
3. Typed keys in the acquirer and the key readers; tests that an internal
   record is stored and found under `id##quaType`, that `merge`'s claim
   check and `Merger` fetch members by it, that `update_manager` deletes it.
4. The rekey function, tested against the fake cursor pattern in
   `tests/test_idmap_subsidiary.py`.
5. On the build machine (Rob): `max(length(identifier))` check, then a
   one-source reconcile + merge (`--source ycba --max-records 100000`) and
   compare the merged records with a run on `main` (`docs/determinism-audit.md`).

**42b**

6. Build records, selection order, role assignment, exclusion; tests for
   each selection route and for "no build record = today".
7. Per-build stores and `record_cache_prefix`.
8. `--build` on the CLI and in the status prefix; `list`, `counts`,
   `testinstall`.
9. A `lux` build record in the config copies; `lux list` and `lux counts`
   identical with and without it.
10. On the build machine (Rob): a `getty` build record and
    `lux --build getty reconcile --source all --max-records 1000`, then merge
    and export.

## Acceptance criteria

* With no build record, `lux list`, `lux counts` and a reconcile + merge of
  one source give the same output as before the item.
* No code under `lux_pipeline/` branches on `type == "internal"` to decide a
  key format, a table name or an idmap prefix (`grep` shows only role uses).
* An internal source's record cache holds only `##qua` keys after a
  reconcile; the existing idmap is read without migration.
* `lux --build getty reconcile` and `merge` run against the same datacaches
  and indexes as `lux`, with `getty_museum` as the only internal source,
  and write to the getty build's own idmap, maps and results.
* Tests pass; config copies updated and parsing; no `print` in library
  code.
