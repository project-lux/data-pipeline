# 23. Merge phase as a module

**Priority:** P1 · **Size:** M · **Depends on:** 01 · **Unblocks:** 24, 31
**Read from:** `main:run-merge.py` (≈420 lines) — `main`'s code only

## Intent

Same operation as item 20, for merge. Move `run-merge.py`'s body into
`lux_pipeline/process/phases/merge.py`:

```python
def read_update_token(cfgs) -> str                  # idmap_update_token.txt validation
def select_sources(cfgs, names=(), all_internal=False, only_refs=False) -> list[(name, cfg)]
def merge_slice(cfgs, idmap, to_do, my_slice=-1, max_slice=-1, recids=(),
                do_references=True, resume=False, progress=None, write_flag=True) -> dict
```

`claim_member` / `_claim_member` and their caches, `prefetched` batching,
`CLUSTER-ESCAPE` check, `defer_commits(every=500, batch=25)`, the reference
pass over `iter_done_refs`, the final `flush()`/`put_all` and the flag file
all move **verbatim** inside `merge_slice`.

## Why it matters

See item 20. `run-merge.py` is the most tuned file in the repository
(claim memoisation measured at 52% of the phase on the 88k‑member cluster);
the refactor's `MergeManager` predates all of it. The manager (item 24)
must call this function.

## Files

| Action | Path on `main` |
|---|---|
| Create | `lux_pipeline/process/phases/merge.py` |
| Edit | `run-merge.py` → thin wrapper with today's CLI (`0 24 --all`, `--norefs`, `--resume`, `--recid`, `--onlyrefs`; `--profile` removed from argv and ignored, see item 41) |
| Create | `tests/test_phase_merge_select.py` |

## What to do

1. Move code; parameterise globals (`cfgs`, `idmap`, `RESUME`,
   `DO_REFERENCES`, `MAX_DISTANCE`, `PREF_ORDER`, `reider`, `ref_mgr`,
   `merger`, `merged_cache`, `final`, `internal_namespaces`, `todo_names`,
   `CLAIM_CACHE`). The claim cache becomes a local created per
   `merge_slice` call.
2. `progress` hook exactly as in item 20: pass it to `PhaseTimer`; add
   `total` after each `timer.mark` and `phase="references"` for the second pass.
3. `print` → `logger`.
4. `run-merge.py` unchanged in behaviour for `run-all.sh` / `merge_parallel.sh`.
5. `tests/test_merge_batch.py` already covers `prefetched`; add a test for
   `read_update_token` (good/bad token) and `select_sources`.

## Acceptance criteria

* Diff shows only moved code.
* `python run-merge.py --ycba --recid <id>` behaves as before.
* Existing tests pass.
