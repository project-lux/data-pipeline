# 24. `MergeManager` and `lux merge`

**Priority:** P1 · **Size:** M · **Depends on:** 07, 23 · **Unblocks:** 31
**Read from:** `rob_refactor:lux_pipeline/process/tasks/merge_task.py` (143 lines) for the shape; `rob_refactor:lux_pipeline/cli/merge.py`; `main:run-all.sh` lines 80–118

## Intent

```
lux merge --source all --max-workers 24
lux merge --source ycba --recid obj/123 --no-ui
lux merge --source all --resume
lux merge --source all --no-refs
lux merge --my-worker 3 --max-workers 24 --source all --no-ui
```

Worker: `_distributed(self, n, cfgs, progress)` = `merge_slice(cfgs, …, my_slice=n, max_slice=self.max_workers, progress=progress)`.
Driver tail (unless `--no-finish`): run `post-build-portal.py`'s logic?
**No** — that script is a separate step with its own flags; leave it to
item 31's orchestration. The merge driver has no tail.

## Files

| Action | Path on `main` | Source |
|---|---|---|
| Create | `lux_pipeline/process/tasks/merge_task.py` | skeleton from `rob_refactor` |
| Create | `lux_pipeline/cli/merge.py` | `rob_refactor:lux_pipeline/cli/merge.py` + `--resume`, `--profile` |
| Create | `tests/test_merge_task.py` | new |

## What to port

Only the skeleton (`__init__`, `maybe_add`, `process`). The refactor's
`_handle_record`, `_pool_merge_records`, `_pool_merge_refs` and
`pref_order` logic are **not** ported — they are the pre‑`claim_member`
algorithm.

The token: `merge_slice` calls `read_update_token(cfgs)` itself; the manager
does not need to.

## Working directory

`ReferenceManager.write_metatypes()` writes `metatypes-<n>.json`, and
`write_done_refs()` / `iter_done_refs()` use `reference_uris.txt`, all
relative to the **current working directory** (not `temp_dir`). Workers
spawned by the mp engine inherit the driver's cwd, so this works, but the
driver must not `chdir`. Item 33 may later move these to `temp_dir`; do not
do it here.

## Steps

1. Create the manager; `_distributed` is ~10 lines.
2. CLI handler with `extra_args = {"no_refs": "no_refs", "resume": "resume", "recid": "recid"}`.
3. Tests: patch `phases.merge.merge_slice` with a recorder; assert
   parameters under `run_single()`; assert `--recid` with two sources
   is rejected.

## Acceptance criteria

* Tests pass; `run-merge.py` untouched by this PR.
