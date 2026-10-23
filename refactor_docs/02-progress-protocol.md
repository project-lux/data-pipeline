# 02. Progress protocol: `lux_pipeline/progress.py`

**Priority:** P0 · **Size:** S · **Depends on:** 00 · **Unblocks:** 06, 07, 12, 14, 16, 18, 20, 23, 25
**Read from:** `rob_refactor:lux_pipeline/process/base/_managable.py` (45 lines) and `rob_claude_refactor:lux_pipeline/process/tasks/_task_ui_manager.py` (`TaskUiManager.update_progress_bar`, `MpEngine.maybe_update_progress_bar`, `LogTailer`) — for what the contract has to cover, **not** for code to copy

## Intent

Replace the refactor's `Managable` → manager → engine → transport chain
with one small module, `lux_pipeline/progress.py`, that defines:

```python
# The contract every long-running component accepts. Keyword-only, all optional.
Progress = Callable[..., None]          # progress(advance=1 | total=n | completed=n | description=str | phase=str)

def null_progress(**kw) -> None: ...    # default; costs one function call

class StatusWriter:
    """A Progress callable that keeps running totals and, at most `throttle`
    seconds apart, atomically rewrites one small JSON file describing this
    worker. The UI (item 06) tails these files exactly as it tails the logs."""
    def __init__(self, path, worker, throttle=0.5): ...
    def __call__(self, advance=0, total=None, completed=None, description=None, phase=None): ...
    def flush(self): ...                  # write now, ignoring the throttle (call at exit)
    # file content: {"worker": 3, "phase": "merge", "description": "ycba",
    #                "completed": 12345, "total": 98765, "updated": 1761234567.1, "done": false}

def read_status(path) -> dict | None     # tolerant reader (partial writes return None)
def status_path(prefix, worker) -> str   # f"{prefix}-w{worker}.status"
def log_path(prefix, worker) -> str      # f"{prefix}-w{worker}.log"

class Sliced:                            # optional 10-line mixin replacing Managable.prepare()
    my_slice = -1; max_slice = -1; progress = staticmethod(null_progress)
    def prepare(self, my_slice=-1, max_slice=-1, progress=null_progress): ...
```

Components (loaders, harvesters, index loaders, the phase functions of
items 20/23/25, and `PhaseTimer`) take `progress=null_progress` and call it;
they never know whether anything is listening.

## Why it matters

In the refactor, `advance=1` travels four hops with a state dict at each
(`Managable.total`, `manager.bar`, `engine.temp_bar`, then a Manager‑dict
proxy, a Ray actor, or a direct Rich call depending on the engine), and the
component holds a back‑pointer to a manager that holds the Rich layout. That
is why `harvest_task` crashes under `--no-ui`, why components cannot be
unit‑tested without a manager, and why there is no way to watch the 24
interpreters that `reconcile_parallel.sh` starts today. One callable and one
file format fix all three: any process, however it was started, writes a
status file; the UI is a reader.

## Files

| Action | Path on `main` |
|---|---|
| Create | `lux_pipeline/progress.py` |
| Edit | `lux_pipeline/process/timing.py` — `PhaseTimer.__init__(…, progress=null_progress)`; call `progress(advance=n)` in `step()`/`skip()`, `progress(description=label)` in `mark()`, `progress(total=total)` when a total is known, and `progress(completed=…, done=True)` in `finish()`. The JSON summary it already writes is unchanged. |
| Create | `tests/test_progress.py` |

## What to port

Only semantics:

* From `Managable`: the idea that a component receives its slice numbers
  before `process()` and reports `total` once and `advance` many times. Drop
  `divide_by_max_slice` — the caller that knows the whole count divides it
  (`len_estimate() // max_slice`, exactly as `main`'s phase code already does).
* From `TaskUiManager.update_progress_bar`: the keyword set
  (`advance`, `total`, `completed`, `description`). Add `phase` so a worker
  that runs two phases (reconcile records then references) can say so
  without the UI inferring it from the description.
* From `MpEngine.maybe_update_progress_bar`: time‑based throttling (2/s) and
  the `rob_claude_refactor` fix "the send throttle would otherwise drop the
  final bar state" → `flush()` on exit, called by the engine wrapper (item 07)
  in a `finally`.
* From `LogTailer`: tolerant incremental reading. `read_status` must return
  `None` on a half‑written file; the writer uses `os.replace` on a `.tmp` so
  that is rare.

## Design rules (put them in the module docstring)

* `progress()` must be cheap enough to call per record: a dict update and a
  `time.monotonic()` compare; I/O only when the throttle elapses.
* No imports from `rich`, `multiprocessing`, `ray`, or anything under
  `lux_pipeline/cli`. This module sits at the bottom of the import stack with
  `timing.py`.
* Paths are chosen by the caller (the engine or the shell wrapper); the
  convention `<log_dir>/<command>-<timestamp>-w<n>.status|.log` is defined
  here by `status_path`/`log_path` so `lux watch` (item 06) can find them.

## Steps

1. Write the module.
2. Thread `progress` through `PhaseTimer` (keyword, default `null_progress`,
   nothing else changes; existing `run-*.py` keep working).
3. `tests/test_progress.py`: `StatusWriter` with `throttle=0` writes after
   every call and `read_status` round‑trips; with `throttle=10` only
   `flush()` writes; a torn file (`write_text("{\"wor")`) reads as `None`;
   `PhaseTimer(progress=recorder)` reports `advance` on `step()`.

## Acceptance criteria

* `from lux_pipeline.progress import null_progress, StatusWriter, read_status` works with no optional dependency installed.
* Tests pass; `tests/test_timing_nesting.py` still passes.
