# 20. Reconcile phase as a module

**Priority:** P1 · **Size:** M · **Depends on:** 01 · **Unblocks:** 21, 22, 31
**Read from:** `main:run-reconcile.py` (≈330 lines) — this item is about `main`'s code only

## Intent

Move the body of `run-reconcile.py` into
`lux_pipeline/process/phases/reconcile.py` **unchanged in logic**, so the CLI
manager (item 21) and the legacy script can both call it:

```python
def select_sources(cfgs, names=(), all_internal=False, baseline=False, recids=()) -> list[(name, cfg, recids)]
    # the --all / --<src> / --baseline / --recid parsing, and the len_estimate() sort

def reconcile_slice(cfgs, idmap, networkmap, to_do, my_slice=-1, max_slice=-1,
                    do_references=True, progress=None, write_flag=True) -> dict
    # everything from "set up environment" to the flag file; returns timer.summary()
```

`run-reconcile.py` becomes: parse `sys.argv` exactly as today → call the two
functions → exit.

## Why it matters

`main`'s reconcile loop is the product of a year of tuning (row
pass‑through to the acquirer, assertion logging instead of in‑flight
identity, batched reference claims, deferred `did_ref`, phase timing). The
refactor's `ReconcileManager` re‑implements an older loop. The only safe way
to get the manager is to call the real loop. This item does not change what
runs; it changes where the code lives.

## Files

| Action | Path on `main` |
|---|---|
| Create | `lux_pipeline/process/phases/__init__.py` (empty) |
| Create | `lux_pipeline/process/phases/reconcile.py` |
| Edit | `run-reconcile.py` → thin wrapper |
| Create | `tests/test_phase_reconcile_select.py` |

## What to do

1. Cut lines from `# --- set up environment ---` to the end of
   `run-reconcile.py` into `reconcile_slice()`; cut the argument handling
   into `select_sources()`. Replace module globals (`cfgs`, `idmap`,
   `networkmap`, `DO_REFERENCES`, `debug`, `profiling`, `my_slice`,
   `max_slice`) with parameters/locals. **Do not edit any statement inside a
   `with timer.stage(...)` block.**
2. `progress` is a `lux_pipeline.progress.Progress` callable, default
   `null_progress` (item 02). Hand it to `PhaseTimer(..., progress=progress)`,
   which already calls it from `step()`/`skip()`/`mark()`; add
   `progress(total=in_db.len_estimate() // max(max_slice, 1))` after each
   `timer.mark(name)` and `progress(phase="references", total=None)` when the
   references pass starts. Nothing else in the loop changes.
3. `print(...)` in the moved code → `logger.info/warning/error` (item 01
   rule; this module is new so it must not print).
4. The `PoolManager.get_instance().put_all("localsocket")` and flag‑file
   write stay inside `reconcile_slice`, the latter behind `write_flag`.
5. `run-reconcile.py` keeps its exact CLI (`0 24 --all`, `--norefs`,
   `--recid`, `--baseline`) and the `logging.basicConfig` so its
   output is unchanged for `run-all.sh`. `--profile` is dropped: the
   wrapper removes the token and ignores it (item 41 gives profiling one
   home in `PhaseTimer`); the phase function has no `profile` parameter.

## Decisions (2026‑10‑27, before implementation)

* **Logging stream.** The wrapper's `logging.basicConfig` gains
  `stream=sys.stdout`, so the moved messages land where the `print`s did
  (`reconcile_parallel.sh` and `updateExternal_parallel.sh` redirect stdout
  only). `print` → `logger.info`, except the acquire failures (`warning`),
  "Got internal reference" (`error`, still followed by the `raise`) and
  "Not processing" (`debug`, still behind `cfgs.debug_reconciliation`).
* **Selection errors.** `select_sources` raises `ValueError` for `--recid`
  with more than one source (today: message and `exit 0`) and for no source
  at all (today: `IndexError`); the wrapper prints the message and exits 1.
  Unknown `--foo` tokens are still ignored by the wrapper, as today;
  `select_sources` itself rejects an unknown name passed explicitly.
* **`--baseline` quirks preserved:** the aat/wikidata list is not sorted
  by `len_estimate()`, `--recid` is ignored, and the numeric slice arguments
  are ignored (it always runs unsliced).
* **Progress.** The total is a running total across sources (as
  `Loader._add_total`), `len(recids)` for an explicit list, else
  `len_estimate() // max(max_slice, 1)`. The references pass starts with
  `progress(reset=True, description="references")`: `StatusWriter` gains a
  `reset` keyword that zeroes `completed`, clears `total` and sets
  `done=False` (the records timer's `finish()` has just reported
  `done=True`; `total=None` cannot clear a total). Test in
  `tests/test_progress.py`.
* **Return value:** `{"records": <summary>, "references": <summary or None>}`.
* **Tests:** also `tests/test_phase_reconcile_slice.py`, pinning the loop's
  behaviour with stubs (call order per record, the `source` stamp, sliced /
  unsliced / recid iteration, memory‑cache switching, every branch of the
  references loop, `do_references=False`, the flag file, the tidy‑up).

## Acceptance criteria

* `diff <(git show main:run-reconcile.py) lux_pipeline/process/phases/reconcile.py` shows only moved code, parameterisation and print→logger.
* `python run-reconcile.py --aat --recid 300010001` behaves as before.
* `tests/test_phase_reconcile_select.py` covers `select_sources` with a stub config (`--all`, named, `--recid` with two sources rejected, baseline list from `globals_cfg`).
* Existing tests pass.
