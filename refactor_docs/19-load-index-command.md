# 19. `IndexLoadManager` and `lux load-index`

**Priority:** P1 · **Size:** S · **Depends on:** 07, 18 · **Unblocks:** 31
**Read from:** `rob_refactor:lux_pipeline/process/tasks/index_load_task.py` (21 lines), `rob_refactor:lux_pipeline/cli/load-index.py` (7 lines); `main:manage-data.py` lines 96–110

## Intent

```
lux load-index --source lcnaf,lcsh,aat,ulan,wikidata,viaf
lux load-index --source all          # every source with an indexLoader
```

One worker per source (index builds are single‑writer per LMDB file), so
`max_workers` is capped at `len(sources)`.

## Files

| Action | Path on `main` | Source |
|---|---|---|
| Create | `lux_pipeline/process/tasks/index_load_task.py` | `rob_refactor:…/index_load_task.py` |
| Create | `lux_pipeline/cli/load_index.py` (underscore; item 05 maps the dash) | `rob_refactor:lux_pipeline/cli/load-index.py` |
| Create | `tests/test_index_load_task.py` | new |

## Known bugs in the source — do not copy

* `maybe_add` tests `'loader' in cfg` then uses `cfg['indexLoader']` — test `indexLoader`.
* `_distributed` passes `overwrite=self.overwrite` but `rob_refactor`'s `__init__` never sets it; `rob_claude_refactor` fixed this by reading `args.no_overwrite`/`args.type` in `__init__` — use that version.
* `_distributed` loops over **all** sources in every worker — use
  `self.sources[n::self.max_workers]` like the download manager.
* `print(f"Failed to load")` then `raise` — log with the source name and
  continue (return a failure count).

## Steps

1. Create both files with the fixes.
2. Test with two stub index loaders and `max_workers=2` via `run_single(0)` and `run_single(1)`; `_distributed(self, n, cfgs, progress)` passes `progress` to `prepare()`.

## Acceptance criteria

* Tests pass; `lux load-index --source aat --no-ui` equals `manage-data.py --load-index --aat`.
