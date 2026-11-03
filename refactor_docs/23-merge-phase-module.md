# 23. Merge phase as a module

**Priority:** P1 · **Size:** M · **Depends on:** 01 · **Unblocks:** 24, 31
**Read from:** `main:run-merge.py` (492 lines) — `main`'s code only

## Intent

Same operation as item 20, for merge. Move `run-merge.py`'s body into
`lux_pipeline/process/phases/merge.py`:

```python
def read_update_token(cfgs) -> str                  # idmap_update_token.txt validation
def select_sources(cfgs, names=(), all_internal=False, only_refs=False, recids=()) -> list[(name, cfg)]
def make_claim_member(cfgs, internal_namespaces, chunk_size=CLAIM_CHUNK, cache_size=CLAIM_CACHE_SIZE)
def merge_slice(cfgs, idmap, to_do, my_slice=-1, max_slice=-1, recids=(),
                do_references=True, resume=False, progress=null_progress, write_flag=True) -> dict
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

## Decisions (2026‑10‑27, before implementation)

* **Logging stream.** As item 20: the wrapper's `logging.basicConfig` gains
  `stream=sys.stdout` (`merge_parallel.sh` redirects stdout only). `print`
  → `logger.info` for the start time and the `*** src ***` lines;
  `warning` for every skip (`Couldn't find`, `Couldn't find YUID`,
  `Could not reidentify`, `No YUID for reference`, `FAILED TO BUILD`,
  `Could not find ANY record`, `Final transform returned None`,
  `CLUSTER-ESCAPE` in both loops); `error` for `Final transform raised
  exception` in both loops (still re‑raised in the records loop, still
  swallowed in the references loop, as today).
* **Token.** `read_update_token` raises `ValueError` with the message the
  script printed; `merge_slice` calls it and sets `idmap.update_token`
  itself, as item 24 expects. The wrapper also calls it first so a bad
  token prints and exits 1 before any argument handling.
* **Selection errors.** `select_sources` raises `ValueError` for an
  unknown name, for no source at all without `only_refs` (today: a silent
  references‑only run, including from a mistyped `run-all.sh` argument),
  and for `recids` with other than one source (today: the ids apply to the
  first source and the rest run in full). The wrapper prints the message
  and exits 1; unknown `--foo` tokens are still ignored, as today.
  `run-all.sh`'s `Traceback` grep will not see an exit 1, which is accepted:
  item 31 replaces the script.
* **`claim_member`.** The two call sites are inside `timer.stage`
  blocks, so the signature cannot change: it stays a closure. It is made
  by a module‑level factory, `make_claim_member`, whose bodies are the
  script's with `CLAIM_CHUNK` / `CLAIM_CACHE` / `CLAIM_CACHE_SIZE` as
  parameters, one call per `merge_slice` so the cache is per call.
  `tests/test_claim_member_batching.py` tests the real function instead of
  a hand copy.
* **Dead code.** `MAX_DISTANCE = cfgs.max_distance` is never read and is
  dropped (the distance cap lives in `ReferenceManager`); `distance = 0`
  and `item = 1` sit inside the moved bodies and stay; the `start_time`
  print stays as an info line.
* **Flag file.** Written whenever `write_flag`, including
  `merge_is_done--1.txt` for an unsliced run, as the script does (reconcile
  only writes it sliced). Item 33 may tidy this.
* **Progress.** As item 20: running total per source after `timer.mark`
  (`len(recids)`, else `len_estimate() // max(max_slice, 1)`, 0 for a
  cache without the estimate); `progress(reset=True,
  description="references")` for the second pass, which has no total.
* **Return value:** `{"records": <summary>, "references": <summary or None>}`.
* **Tests:** also `tests/test_phase_merge_slice.py` (both loops with the
  collaborators stubbed and the real `prefetched`: call order, the claim,
  `--resume`, `CLUSTER-ESCAPE`, the equivalents, the base‑record search,
  the commit calls, the flag, progress) and `tests/test_run_merge_script.py`
  (the wrapper's argv).

Two asymmetries inside the loops are preserved, not fixed: the references
loop swallows a `final.transform` exception and stores the untransformed
record, where the records loop re‑raises; and it writes the whole
reidentified record to `recordcache2` where the records loop writes its
`data`. Separate items if either is wrong.

## Acceptance criteria

* `diff <(git show main:run-merge.py) <(sed 's/^    //' lux_pipeline/process/phases/merge.py)` shows only moved code, parameterisation and print→logger; no hunk inside a `timer.stage` block.
* `python run-merge.py --ycba --recid <id>` behaves as before.
* Existing tests pass.
