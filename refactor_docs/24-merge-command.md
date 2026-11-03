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
| Create | `lux_pipeline/cli/merge.py` | `rob_refactor:lux_pipeline/cli/merge.py` + `--resume` (no `--profile` until item 41) |
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

## Decisions (2026‑10‑27, before implementation)

* **A. Same shape as `ReconcileManager`.** `--source all` is every
  internal source (`run-merge.py --all`); `internal`, `external` and named
  sources work as for every command; the results group is rejected by
  `maybe_add`. Each worker runs the real `select_sources` and calls
  `merge_slice` with `my_slice=n, max_slice=max_workers`, so every worker
  sees the same source list -- which both loops need: the records loop's
  slice predicate, and the references loop's `claim_src["name"] in
  todo_names` skip. `--recid` with other than one source, and no source,
  are rejected in the driver. `--resume` and `--no-refs` (`--norefs`) pass
  through.
* **B. `--recid` is one unsliced worker and skips the references pass.**
  `merge_slice` processes an explicit list in full wherever it is given
  (one worker, `-1, -1`, as item 21). Unlike reconcile's, merge's
  references pass reads the whole of `reference_uris.txt`, so `run-merge.py
  --recid` rebuilt every referenced cluster in one process after the
  chosen records; `lux merge --recid` does not (logged).
* **C. `--only-refs` (`--onlyrefs`)**: the script's flag; no source, the
  references pass only, sliced over the workers. With `--recid` or
  `--no-refs` it would merge nothing and is rejected.
* **D. Checked in the driver before any worker starts**: the update token
  (`read_update_token`; missing or malformed raises), and
  `reference_uris.txt` in the working directory whenever the references
  pass runs (each worker would otherwise raise `FileNotFoundError` after
  its whole records pass). A missing `<data_dir>/metatypes.json` is a
  warning only: the final mapper carries on without metatypes.
* **E. No in-progress guard** (item 22's `refuse_while_building`): 24
  shell-orchestrated `lux merge --my-worker n` processes would each see the
  others' unfinished status files and refuse. `lux reconcile` has none
  either.
* **F. Unchanged:** the `merge_is_done-<n>.txt` flags are written as the
  phase does (`lux initialize` creates `logs/flags`); workers inherit the
  driver's working directory under mp and under a local Ray cluster
  (checked); no driver tail; `run-merge.py` and `merge_parallel.sh` are not
  touched.

## Acceptance criteria

* Tests pass; `run-merge.py` untouched by this PR.
