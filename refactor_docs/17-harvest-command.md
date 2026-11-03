# 17. `HarvestManager` and `lux harvest`

**Priority:** P1 · **Size:** M · **Depends on:** 07, 16 · **Unblocks:** 31
**Read from:** `rob_refactor:lux_pipeline/process/tasks/harvest_task.py` (117 lines), `rob_refactor:lux_pipeline/cli/harvest.py`; `main:run-harvest.py`, `harvest_parallel.sh`

## Intent

```
lux harvest --source ycba                        # walk the stream since last harvest
lux harvest --source all --two-phase --max-workers 12   # list in phase 1, fetch records in parallel in phase 2
lux harvest --source ypm --until 2025-01-01T00:00:00
lux harvest --source ycba --pages --max-workers 24      # today's harvest_parallel.sh --pages mode
```

Replaces `run-harvest.py` and `harvest_parallel.sh`.

## Files

| Action | Path on `main` | Source |
|---|---|---|
| Create | `lux_pipeline/process/tasks/harvest_task.py` | `rob_refactor:lux_pipeline/process/tasks/harvest_task.py` |
| Create | `lux_pipeline/cli/harvest.py` | `rob_refactor:lux_pipeline/cli/harvest.py` |
| Create | `tests/test_harvest_task.py` | new |

## What to port

Both files. The two‑phase flow in `process()` (phase 1 with
`max_workers = len(sources)`, then phase 2 re‑using the engine) is the part
worth keeping exactly. Changes:

* Delete every `get_bar_from_layout` call (the source crashes with
  `--no-ui`). Expose `idle_workers()` as in item 15; for two‑phase runs the
  driver sets `view.set_visible(n, True)` again before phase 2 — do this in
  `process()` through the optional `view` argument, which is the only UI
  contact a manager may have.
* `_distributed(self, n, cfgs, progress)` passes `progress` to
  `h.prepare(n, self.max_workers, progress=progress)`; `phase` is reported
  with `progress(phase="list"|"fetch")`.
* Add `--pages` mapping to `protocol.harvest_pages(n, max_workers)` so the
  existing parallel page mode survives.
* `_distributed` sets `self.idmap = self.configs.get_idmap()` — keep; the
  driver's `process_change` needs it (item 16).
* `maybe_add` requires `cfg.get("activitystreams")`; also accept
  `pmhEndpoint`.
* Exceptions: log and return a failure count instead of `raise` (same rule
  as item 15).

## Steps

1. Create both files.
2. Tests: stub harvester recording `prepare`/`process`/`get_record_list`/`harvest_from_list`
   calls; `run_single()` for single‑phase; two‑phase asserts phase 1 ran
   once per source and phase 2 once per worker.
3. Live: `lux harvest --source ycba --no-ui` against the stream.

## Acceptance criteria

* Tests pass; `--no-ui` works; `--pages` reproduces `harvest_parallel.sh`.
