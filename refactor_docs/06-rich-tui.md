# 06. Console UI: `BuildView` and `lux watch`

**Priority:** P0 · **Size:** M · **Depends on:** 01, 02 · **Unblocks:** 07, 31
**Read from:** `rob_claude_refactor:lux_pipeline/cli/_rich.py` (139 lines; `LogPanel`, `PanelLogHandler`, `Header`, `LEVEL_COLORS`, `add_worker_log_line`) and `rob_claude_refactor:lux_pipeline/process/tasks/_task_ui_manager.py` (`LogTailer`, 25 lines) — **not** the `rob_refactor` `_rich.py`

## Intent

Add `lux_pipeline/cli/_view.py` with one class that renders a build from files,
and `lux_pipeline/cli/watch.py`, a command that attaches to any running or
finished build:

```python
class BuildView:
    def __init__(self, prefix, workers, log_level="INFO", max_log_lines=500): ...
    def __enter__(self) / __exit__(...)       # starts/stops rich.live.Live; swaps the lux_pipeline logger handler and restores it
    def refresh(self) -> bool                 # read N status files + tail N log files; returns True when every worker reports done
    def set_visible(self, worker, visible)    # hide idle workers (harvest/download with fewer sources than workers)
```

```
lux watch <log_dir>/reconcile-2026-10-23-141500        # attach by prefix
lux watch --latest                                     # newest prefix in log_dir
```

Screen: header (title, clock, prefix), one `rich.progress.Progress` holding
one task per worker (description, bar, completed/total, %, elapsed, ETA)
plus an "all workers" task, and a bounded log panel showing parent‑process
log records and tailed worker lines prefixed `w<n>`.

## Why it matters

Second headline feature, and with the file‑based protocol of item 02 it is
available for **today's shell‑orchestrated builds** before any manager or
engine is ported: the `*_parallel.sh` scripts only need to be given a
`--status-prefix` (item 31). The process layer imports nothing from Rich;
the UI never reaches into a worker.

## Files

| Action | Path on `main` | Source |
|---|---|---|
| Create | `lux_pipeline/cli/_view.py` | `LogPanel`, `PanelLogHandler`, `Header`, `LEVEL_COLORS`, `add_worker_log_line` from `rob_claude_refactor:lux_pipeline/cli/_rich.py`; `LogTailer` from `rob_claude_refactor:…/_task_ui_manager.py` |
| Create | `lux_pipeline/cli/watch.py` | new (`BaseHandler`, works with `cfgs=None` when a prefix is given) |
| Create | `tests/test_build_view.py` | new |

## What to port, and what changes

Keep from `rob_claude_refactor` (these are its fixes for the UI livelock
and layout problems; all of them carry over):

* `LogPanel`: a `deque(maxlen=500)` guarded by a lock, rendered from the
  tail — not a recording `Console` re‑exported on every refresh.
* `PanelLogHandler`: parent‑process records → panel lines with time and
  coloured level.
* `LogTailer.read_new_lines()`: per‑file offsets, consume only up to the
  last newline, `errors="replace"`.
* Worker lines prefixed `w<n>` in cyan.
* Removing the logger's existing handlers while the view is live and the
  log level clamp to a valid name.

Change:

* **One `Progress`, many tasks.** Replace the `Layout` with one row per
  worker (and the `progress_size = max_workers + 3` height fix) by a single
  `rich.progress.Progress` with `add_task` per worker inside
  `Live(Group(Header(), progress, Panel(log_panel)))`. Rich then sizes it.
  `layout._lux_bars`, `get_bar_from_layout` and `add_worker_log_line` as a
  free function disappear; the view owns `worker → TaskID`.
* **Input is files, not calls.** `refresh()` calls `read_status(status_path(prefix, n))`
  for each worker and updates the task (`completed`, `total`, description
  `"w3 ycba"` or `"w3 references"` from `phase`), then tails logs. A worker
  with no status file yet shows as `waiting`.
* `total=None` until a total is reported, so Rich shows an indeterminate bar
  instead of the refactor's `total=10000` placeholder.
* Restore the previous logger handlers in `__exit__` (`runtime.configure_logging`).
* No handler swap when `--no-ui`; `BuildView` is simply not created.
* Terminal too small for `workers` rows: Rich clips; print one warning
  suggesting `--no-ui` or `lux watch` from a taller terminal.

## Known bugs in the source — do not copy

* `rob_refactor`'s `ConsolePanel(Console)` + `LuxHandler(RichHandler)` design.
* `get_layout()` creating bars with `total=10000`.
* `_task_ui_manager` and the task modules calling `get_bar_from_layout` on a
  `None` layout under `--no-ui`.

## Steps

1. Write `_view.py` (≈150 lines) and `watch.py` (≈40 lines: resolve prefix,
   count `-w*.status` files for `workers`, loop `refresh()` every 0.5 s until
   all done or Ctrl‑C).
2. `tests/test_build_view.py`: with `Console(file=io.StringIO(), width=100, force_terminal=True)`
   and a `tmp_path` prefix, write two status files and two log files; after
   `refresh()` assert both tasks exist with the right `completed`/`total`,
   the log panel has lines prefixed `w0`/`w1`, a parent `logger.info` lands
   in the panel, `refresh()` returns `True` only when both say `done`, and
   `__exit__` restores one stream handler.
3. Manual: start `python run-reconcile.py 0 2 --aat` with a `StatusWriter`
   temporarily wired (or wait for item 31) and run `lux watch --latest`.

## Acceptance criteria

* Tests pass; `lux help` unaffected.
* `lux watch <prefix>` renders a finished build's final state from its files alone.
