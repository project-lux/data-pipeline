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

## Acceptance criteria

* Tests pass; the legacy `run-reconcile.py` is untouched by this PR.
* `lux reconcile --source all --max-workers 2 --no-ui` on a test instance produces `metatypes.json`, an identity resolution and `reference_uris.txt`.
