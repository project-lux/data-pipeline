# 22. `lux identify`, `lux new-token`, `lux write-refs` (and `lux metatypes`)

**Priority:** P1 · **Size:** S · **Depends on:** 05, 20 · **Unblocks:** 31
**Read from:** `main:run-identify.py` (whole), `main:manage-data.py` lines 198–222

## Intent

Three small commands for the steps around reconcile and merge, so shell
orchestration (item 31) can run them separately and so `lux reconcile
--no-finish` can be followed by hand:

```
lux identify [--no-vacuum]     # IdentityResolver.resolve_identity(); idmap.optimize()
lux new-token                  # write a new __YYYYMMDD[a-z]__ token to idmap_update_token.txt
lux write-refs                 # ReferenceManager.write_done_refs()
```

All three are `BaseHandler`s (no manager, no UI). `identify` runs in the
driver process; it is already a streaming unix‑`sort` pipeline and must not
be sliced.

### Is `new-token` still needed with the Postgres identity map? Yes.

The build token (`__YYYYMMDD[a-z]__` in `<data_dir>/idmap_update_token.txt`)
did not go away with Redis; it changed shape. On `main`:

* `storage/idmap/postgres.py` reads the file in `__init__` and **raises** if it
  is missing or malformed (`"update token"`), so no idmap — and therefore no
  phase — starts without one. The yuids table has a `token` column with an
  index (`CREATE TABLE … token TEXT`, `{yuids}_token_idx`).
* `ReferenceManager.manage_identifiers` / `resolve_refs` call
  `idmap.has_update_token(uu)` to decide whether a YUID has already been
  rebuilt in *this* build (`rebuild = not has_update`) and
  `idmap.add_update_token(uu)` after touching it — the token is what makes
  "first time we see this uuid in this build" decidable across 24 workers.
* `IdentityResolver`'s bulk load stamps every cluster it writes with the
  current token (`yuids[iyuid] = self.update_token`), and merge strips the
  token from equivalents (`run-merge.py`: `equivs.remove(idmap.update_token)`).
* `storage/idmap/subsidiary.py` copies the master's token (`copyUpdateToken`).
* Tests pin the behaviour: `test_the_update_token_is_not_a_candidate`,
  `test_the_update_token_is_not_a_member`, `test_idmap_noop_writes`.

So a new token must be minted **before** `lux reconcile` on every rebuild,
exactly as `run-all.sh` reminds the operator today ("Did you clear and update
the token?"). What *can* change: `lux new-token` should refuse to run while a
build is in progress (any `<log_dir>/*-w*.status` without `"done": true`),
and `lux reconcile --new-token` (item 21) should call the same function.
`lux testinstall` (item 28) must check the file exists and is well‑formed,
because the idmap constructor error is otherwise the first thing a new
installation sees.

## Files

| Action | Path on `main` |
|---|---|
| Create | `lux_pipeline/cli/identify.py` |
| Create | `lux_pipeline/cli/new_token.py` |
| Create | `lux_pipeline/cli/write_refs.py` |
| Create | `lux_pipeline/cli/metatypes.py` (decision E below) |
| Create | `lux_pipeline/process/phases/identify.py` |
| Edit | `lux_pipeline/process/phases/reconcile.py` (`next_update_token`, `new_update_token(idmap=None)`, `write_refs`) |
| Edit | `lux_pipeline/process/tasks/reconcile_task.py` (`finish` calls the phase functions) |
| Edit | `lux_pipeline/progress.py` (`unfinished_workers`), `lux_pipeline/cli/_handler.py` (`refuse_while_building`) |
| Edit | `run-identify.py`, `manage-data.py` (`--new-token`, `--write-refs`) → call the same functions |
| Create | `tests/test_cli_small_commands.py` |

## What to port

Nothing from the refactor — these wrap `main` code:

* `identify`: body of `run-identify.py` (stats line via `print`, it is the
  result; vacuum unless `--no-vacuum`).
* `new_token`: `phases.reconcile.new_update_token(cfgs, idmap)` from item 21.
* `write_refs`: `ReferenceManager(cfgs, idmap).write_done_refs()` **then**
  `cfgs.instantiate_map("done_refs")["store"].clear()` — on `main` the
  method does not clear the queue; `manage-data.py --write-refs` does it
  afterwards. Keep both steps. Note that `write_done_refs` writes
  `reference_uris.txt` into the **current working directory** and
  `iter_done_refs` (merge) reads it from there, so every `lux` phase must
  run from the same directory (`run-all.sh` already does).

## Steps

1. Create the three modules (docstrings!).
2. Tests with stubs: `new_token` increments the letter when the token
   already starts with today's date; `identify` passes `--no-vacuum`.

## Decisions (2026‑10‑27, before implementation)

* **A. `new-token` runs without the idmap.** `lux` loads its configuration
  instantiated, which builds the Postgres idmap, which raises when
  `idmap_update_token.txt` is missing or malformed -- so the command that
  writes the file could not start without it. The handler sets
  `needs_configs = False` and loads `get_configs(instantiate=False)`
  itself. `new_update_token(cfgs, idmap=None)` reads the current token from
  `idmap` when given (and sets it there, as item 21 needs), otherwise from
  the file via `phases.merge.read_update_token`; a missing or malformed
  file counts as no token, giving today's first. The rule itself is
  `next_update_token(current, now)`, a pure function, unchanged from
  `manage-data.py`, which now calls `new_update_token` too.
* **B. All four commands refuse while a `lux` build is running.**
  `refuse_while_building` (cli/_handler.py) looks for `<log_dir>/*-w*.status`
  files not marked done (`progress.unfinished_workers`). A failed worker
  still reports done, but a SIGKILLed or OOM-killed one never does, so only
  workers updated within the last hour (`RUNNING_WINDOW`) block; older
  ones are warned about as probably killed. `--force` runs anyway. Legacy
  `run-*.py` / `*_parallel.sh` runs write no status files and cannot be
  seen.
* **C. Second-run guards.** `lux write-refs` with an empty done-refs queue
  and a `reference_uris.txt` already in the working directory refuses:
  it would replace the file merge reads with an empty one. `lux metatypes`
  likewise refuses when there are no `metatypes-*.json` slice files and
  `<data_dir>/metatypes.json` exists. `--force` overrides both. The legacy
  scripts keep their behaviour.
* **D. One function per step.** `phases/identify.py` holds
  `resolve_identity(cfgs, idmap)` (returns the stats with `seconds`),
  `report_lines(stats)` (run-identify.py's three lines) and
  `vacuum_idmap(idmap)`; `phases.reconcile.write_refs(cfgs, idmap,
  ref_mgr=None)` is `write_done_refs()` and clearing the queue. The
  commands, `run-identify.py`, `manage-data.py --write-refs` and
  `ReconcileManager.finish` all call them; no `timer.stage` block is
  touched.
* **E. A fourth command, `lux metatypes`** (`ReferenceManager.
  merge_metatypes`), because `lux reconcile --no-finish` cannot otherwise
  be followed by hand. The order is identify, metatypes, write-refs, as
  `run-all.sh` and `finish` run them; write-refs needs identify first.
* **F. Output.** `identify` prints run-identify.py's lines; `write-refs`
  prints the absolute path it wrote. `new-token` and `metatypes` print
  nothing of their own: the phase functions' log lines (to stdout, as
  `%(message)s`) are the result, as in the legacy scripts. Refusals and
  failures exit 1.

## Acceptance criteria

* `lux identify` == `python run-identify.py`; `lux new-token` == `manage-data.py --new-token`.
* `lux new-token` works with `idmap_update_token.txt` missing or malformed.
* `lux reconcile --no-finish` followed by `lux identify`, `lux metatypes`
  and `lux write-refs` leaves what the finishing steps leave.
