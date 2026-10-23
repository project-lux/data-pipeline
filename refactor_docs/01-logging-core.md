# 01. Logging in core modules

**Priority:** P0 · **Size:** M · **Depends on:** 00 · **Unblocks:** 04, 06, 11, 20, 23, 25, 37
**Read from:** `rob_refactor` (any file under `lux_pipeline/process/` for the pattern)

## Intent

Replace `print(...)` with `logging.getLogger("lux_pipeline")` calls in the
core library modules on `main`, so that output can be captured by the console
UI (item 06), routed to per‑worker log files (item 07), and filtered by level
(`--log DEBUG|INFO|WARNING|ERROR`).

Scope for this item: `lux_pipeline/config.py`, everything under
`lux_pipeline/process/` **except** `lux_pipeline/process/utils/`, and everything under
`lux_pipeline/storage/`. Sources are item 37 (optional). The top‑level
`run-*.py` / `manage-data.py` scripts keep their `print`s.

## Why it matters

The Rich UI replaces the logger's stream handler with a panel handler and
runs `Live`; any `print` from library code writes straight to the terminal
underneath the live display and corrupts it. Workers in the multiprocessing
and Ray engines have no terminal at all, so their `print`s are lost. A single
named logger is the one thing every later item relies on.

## Files

| Action | Path on `main` |
|---|---|
| Edit | `lux_pipeline/config.py` |
| Edit | `lux_pipeline/process/collector.py`, `merger.py`, `reconciler.py`, `reference_manager.py`, `reidentifier.py`, `update_manager.py`, `identity_resolver.py`, `merge_batch.py`, `validator.py` |
| Edit | `lux_pipeline/process/base/acquirer.py`, `fetcher.py`, `harvester.py`, `index_loader.py`, `loader.py`, `mapper.py`, `reconciler.py` |
| Edit | `lux_pipeline/storage/cache/*.py`, `lux_pipeline/storage/idmap/*.py`, `lux_pipeline/storage/marklogic/rest.py` |
| Create | `lux_pipeline/log.py` (5 lines: `logger = logging.getLogger("lux_pipeline")`) — optional convenience |

## What to port

Only the mechanical pattern. At the top of each module:

```python
import logging
logger = logging.getLogger("lux_pipeline")
```

Then map each `print(...)`:

| Old | New |
|---|---|
| diagnostic that only prints when `self.debug` | `logger.debug(...)` (keep the `if self.debug:` guard — formatting the f‑string is the cost) |
| "Failed to …", "Could not …", "Got failure …" | `logger.error(...)` |
| "Skipping …", "Dropping …", soft problems | `logger.warning(...)` |
| progress lines (`"{x} in {t} = …/s"`) | `logger.info(...)` |
| `sys.stdout.write('.'); sys.stdout.flush()` ticks | delete; progress is the manager's job (item 02) |
| `print()` of a whole record for inspection | `logger.debug(...)` |

`traceback`‑style messages that `main` added (e.g. the `[file:line]` suffix in
`acquirer.py`) stay; only the output call changes.

## What to keep from `main` (do not overwrite)

Everything else in these files. This item changes **no logic**. In
particular do not touch `with timing.stage(...)` blocks, `PhaseTimer`
progress lines (they go through `timer.step()`/`report()`, which may keep
printing for now), or the `_say` helper in `identity_resolver.py` (it writes
to stderr deliberately; leave it).

## Known bugs in the source — do not copy

* `rob_refactor` `fetcher.py` dropped the `if self.use_networkmap:` guard
  around `self.networkmap[url] = …`. Keep `main`'s guard.
* `rob_refactor` `acquirer.py` wraps `do_post_map` in a `try/except` that
  `print(rec2)`s and swallows the error. Keep `main`'s behaviour (let it
  raise) — only convert the surrounding prints.

## Steps

1. `grep -rn "print(" lux_pipeline/config.py lux_pipeline/process lux_pipeline/storage | grep -v utils/` to list the sites.
2. Convert file by file per the table. Keep messages' text; drop leading
   spaces that were only there to indent terminal output.
3. In `lux_pipeline/storage/idmap/postgres.py` and `storage/cache/postgres.py`
   there are status/maintenance reports (`bloat_line`, `optimize(report=True)`)
   that *are* the result of a `manage-data.py --bloat` call. Convert those to
   `logger.info` as well; item 08/09 will print them via the CLI.
4. Run `python -m pytest tests/ -q`.
5. Smoke: `python run-reconcile.py --aat --recid 300010001` should still
   work. Because nothing configures the logger yet, output appears via
   Python's last‑resort handler at WARNING+ only. That is acceptable for
   this PR; item 05 installs a stream handler at INFO for the scripts.
   If that loss of INFO output during the gap is a problem, add a two‑line
   `logging.basicConfig(level=logging.INFO, format="%(message)s")` to the top
   of each `run-*.py` and `manage-data.py` in this PR.

## Acceptance criteria

* `grep -rn "print(" lux_pipeline/config.py lux_pipeline/process lux_pipeline/storage | grep -v "process/utils"` returns nothing except `identity_resolver._say` and `timing.py`.
* `python -m pytest tests/ -q` passes.
* No functional diff: the PR touches only `print`/`sys.stdout` lines and adds the two import lines.
