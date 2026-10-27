# 21. `ReconcileManager` and `lux reconcile`

**Priority:** P1 · **Size:** M · **Depends on:** 07, 20 · **Unblocks:** 31
**Read from:** `rob_refactor:lux_pipeline/process/tasks/reconcile_task.py` (180 lines) for the *shape*; `rob_refactor:lux_pipeline/cli/reconcile.py`; `main:run-all.sh` lines 20–75 for the orchestration the driver must reproduce

## Intent

```
lux reconcile --source all --max-workers 24            # records + references in every worker, then the driver finishes the phase
lux reconcile --source ycba --recid obj/123 --no-ui
lux reconcile --source all --no-refs
lux reconcile --source all --new-token                 # mint a new idmap update token first
lux reconcile --my-worker 3 --max-workers 24 --source all --no-ui --no-finish   # one slice, shell-orchestrated
```

Worker side: `_distributed(self, n, cfgs, progress)` = `reconcile_slice(cfgs, …, my_slice=n, max_slice=self.max_workers, recids=self.recids, progress=progress)`.

Driver side, after all workers return (unless `--no-finish`), exactly what
`run-all.sh` does between reconcile and merge:

1. merge the per‑slice `metatypes-<n>.json` into `<data_dir>/metatypes.json` (`merge-metatypes.py` logic → `ReferenceManager.merge_metatypes()`);
2. `IdentityResolver(cfgs, idmap).resolve_identity()` then `idmap.optimize()` (`run-identify.py`);
3. `ReferenceManager(cfgs, idmap).write_done_refs()` (`manage-data.py --write-refs`).

Each of the three is also exposed as its own command in item 22.

## Files

| Action | Path on `main` | Source |
|---|---|---|
| Create | `lux_pipeline/process/tasks/reconcile_task.py` | shape from `rob_refactor`; body calls `phases.reconcile` |
| Create | `lux_pipeline/cli/reconcile.py` | `rob_refactor:lux_pipeline/cli/reconcile.py` |
| Edit | `lux_pipeline/process/reference_manager.py` — add `merge_metatypes()` (port from `rob_refactor:…/reference_manager.py` lines 47–71, reading `metatypes-*.json` from the cwd/`log_dir` where `write_metatypes` wrote them, writing `<data_dir>/metatypes.json`, deleting the slices) | |
| Create | `tests/test_reconcile_task.py` | new |

## What to port

From the refactor only: the manager skeleton (`maybe_add` → `(which, name, recids)`
with `self.recids` read from `args.recid` in `__init__` — the `rob_claude_refactor`
fix; `process()` calling the engine then the driver tail), the CLI handler
(`--recid`, `--no-refs`, `--new-token`; add `--baseline`, `--no-finish`;
no `--profile` until item 41). The old `self.phase` / `super().process()` two‑pass structure
is dropped (see below).

`--new-token`: `main`'s idmap classes do not have `make_update_token()`;
port the token logic from `manage-data.py --new-token` (lines 198–215) into
`lux_pipeline/process/phases/reconcile.py::new_update_token(cfgs, idmap)` and
call it from here and from item 22.

Where `run-merge.py` reads the token from `idmap_update_token.txt`, nothing
changes.

## What to keep from `main`

The whole per‑record loop — it is in `phases.reconcile` now; the manager
never touches records. `idmap.disable_memory_cache()` for `max_slice > -1`
is inside `reconcile_slice` already.

## Working directory

`ReferenceManager.write_metatypes()` writes `metatypes-<n>.json`, and
`write_done_refs()` / `iter_done_refs()` use `reference_uris.txt`, all
relative to the **current working directory** (not `temp_dir`). Workers
spawned by the mp engine inherit the driver's cwd, so this works, but the
driver must not `chdir`. Item 33 may later move these to `temp_dir`; do not
do it here.

## Known bugs in the source — do not copy

* The refactor's `_handle_record` calls `ref_mgr.manage_identifiers(rec2)`:
  in‑flight identity minting, replaced on `main` by assertion logging + identify.
* `_pool_reconcile_refs` opens `temp_log_{n}.txt` in the cwd and writes
  a character per step — debugging litter; drop.
* `process()` imports `cfgs` from the CLI module.
* Two‑phase (`self.phase == 2` runs refs as a second distributed pass).
  `main` processes references **inside each worker** right after its
  records, with a shared claim queue and idle timeout; keep `main`'s model
  (single `_distributed` call does both), so `--no-refs` just passes
  `do_references=False`.

## Steps

1. Add `merge_metatypes()` and `new_update_token()`.
2. Create the manager and CLI handler.
3. Tests: patch `lux_pipeline.process.phases.reconcile.reconcile_slice` with a
   recorder; `run_single()` asserts it is called with `my_slice=0`,
   `max_slice=1`, the selected sources and the `progress` callable; assert the
   driver tail calls the three finishing functions (patched) in order and
   skips them with `no_finish=True`.
4. Live, small: `lux reconcile --source aat --recid 300010001 --no-ui --no-finish`.

## Decisions (2026‑10‑27, before implementation)

* **`--source all` is every internal source**, as `run-reconcile.py
  --all`; the external records are reached through references. The
  handler overrides `resolve_sources` for `all`; `internal`, `external`
  and named sources (external ones included) work as for every command;
  the results group is rejected by `maybe_add`.
* **Selection happens in the worker.** The manager stores names; each
  worker runs the real `select_sources` against its own configuration
  (which orders sources by datacache size) and calls `reconcile_slice`
  with `my_slice=n, max_slice=max_workers`, so every record of every
  selected datacache belongs to exactly one worker and every worker sees
  the same source list. `--recid` with other than one source, and no
  source at all, are rejected in the driver before any worker starts.
* **Explicit record lists are one unsliced worker.** `reconcile_slice`
  iterates a `--recid` list, or the baseline terms, in full wherever it
  is given, so sharing them out would reconcile every record once per
  worker. The manager forces `max_workers` to 1 (logged) and calls the
  phase with `-1, -1`, as the script ran them: the idmap memory cache on,
  no flag file, and the assertions in `assertions-single.tsv` rather
  than over a slice's file from the last full build.
* **`--new-token`.** `phases.reconcile.new_update_token(cfgs, idmap)` is
  `manage-data.py --new-token`'s date stem and letter increment; it writes
  the file **and sets `idmap.update_token`** on the instance it is given,
  which is new and necessary: the driver's idmap was built before the
  token changed and the identity resolution stamps every cluster with it,
  and under the null engine the worker shares the driver's cached
  configuration. Spawned workers read the new file. Called before the
  engine starts.
* **Tail order is `run-all.sh`'s**: identify (`resolve_identity`, then
  `idmap.optimize()` where the idmap has it), then `merge_metatypes`,
  then `write_done_refs` **and** clearing the done‑refs queue, as
  `manage-data.py --write-refs` does (item 22 keeps both). The only real
  constraint is identify before write‑refs, which resolves YUIDs through
  the idmap. The tail runs only when every worker succeeded
  (`WorkerFailed` is raised first).
* **`merge_metatypes`** reads `metatypes-*.json` from the working
  directory, sorted, as `merge-metatypes.py` does; writes
  `<data_dir>/metatypes.json` (`data/files` on the build machine, where
  `run-all.sh` moved it); deletes the slice files; returns the map.
  `merge-metatypes.py` itself is untouched (item 33 retires it).
* **When the tail is skipped:** `--no-finish`, and implied by
  `--my-worker` (one slice of many), `--recid` and `--baseline` (it only
  pre‑populates the global terms), each logged. Identity resolved from a
  fraction of the assertions would rewrite the identity map from a
  fraction of the evidence and delete the YUIDs it no longer saw.
* **Output under the console view:** the tail runs inside
  `manager.process()` after the engine returns, while the view is open;
  Rich redirects stdout during the live display, so the resolver's
  progress lines and its `PhaseTimer` summary land above the worker table.
* **Results:** each worker returns the phase's summaries; the tail logs
  the identify stats in `run-identify.py`'s form and keeps
  `{"identify": stats, "metatypes": keys}` as `finish_stats`. The CLI
  has `--recid`, `--no-refs` (alias `--norefs`), `--new-token`,
  `--baseline`, `--no-finish`.
* **Tests** in `tests/test_reconcile_task.py`: the worker calls, every
  worker's own slice of the same sources, the selection rules, the record
  list and baseline on one unsliced worker, the token before the workers
  and seen by them, the tail in order and every skip, no tail after a
  failed worker, `merge_metatypes` and `new_update_token` for real, the
  command end to end on the null engine.

## Acceptance criteria

* Tests pass; the legacy `run-reconcile.py` is untouched by this PR.
* `lux reconcile --source all --max-workers 2 --no-ui` on a test instance produces `metatypes.json`, an identity resolution and `reference_uris.txt`.
