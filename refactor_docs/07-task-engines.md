# 07. Task engines and `TaskManager`

**Priority:** P0 · **Size:** M · **Depends on:** 02, 03, 05, 06 · **Unblocks:** 13, 15, 17, 19, 21, 24, 26
**Read from:** `rob_claude_refactor:lux_pipeline/process/tasks/_task_ui_manager.py` (≈330 lines) — for the engine mechanics (`spawn`, `ProcessPoolExecutor`, `ray.wait` loop, `attach_worker_log_handler`, `make_file_handler`); **not** for its progress plumbing

## Intent

Add `lux_pipeline/process/tasks/_task_manager.py` with:

* `TaskManager` — the base every phase manager extends. It selects sources
  (`prepare_single`, `prepare_all`, `maybe_add`), and implements
  `_distributed(n)` per subclass. It has **no** UI state: no `bar`, no
  `log()`, no `layout` parameter.
* Three engines with one job: run `manager._distributed(n)` for
  `n in range(max_workers)` and return the list of results.

| Engine | Mechanism | Notes |
|---|---|---|
| `NullEngine` | calls `_distributed(my_slice)` in this process | `--my-worker i --max-workers N`; today's one‑interpreter‑per‑slice model, tests, debugging |
| `MpEngine` | `ProcessPoolExecutor(max_workers, mp_context=spawn)` | default |
| `RayEngine` | `ray.remote` tasks, `ray.get` | optional extra; multi‑node |

Inside every worker a tiny wrapper (`_run_worker(manager_bytes_or_self, n, prefix, log_level)`) does, in order:

1. `cfgs = lux_pipeline.runtime.get_configs()` (fresh build in a spawned process),
2. `attach_worker_log_handler(prefix, n, log_level)` — the per‑worker
   `FileHandler` from `rob_claude_refactor` (`make_file_handler`: level
   filtered in the worker, `%(asctime)s %(levelname)-8s %(message)s`),
3. `progress = StatusWriter(status_path(prefix, n), n)`,
4. `ret = manager._distributed(n, cfgs, progress)`,
5. `finally: progress.flush(done=True)`.

The driver side is `TaskManager.process(engine, prefix, view=None)`: start
the engine, and while it runs call `view.refresh()` every 0.5 s if a
`BuildView` (item 06) was given; otherwise just wait. Worker exceptions
propagate (`future.result()` / `ray.get`) and `process()` re‑raises after the
others finish.

## Why it matters

First headline feature. With progress and logs both file‑based, each engine
is 20–40 lines and the manager contract is one method. Compared with
`rob_claude_refactor` this removes `ProgressActor`, the `Manager().dict()`
proxies, `temp_bar`, `maybe_update_progress_bar`, `TaskUiManager.log`,
`debug_process`'s special casing, and every `get_bar_from_layout` call in
the task modules.

## Files

| Action | Path on `main` | Source |
|---|---|---|
| Create | `lux_pipeline/process/tasks/__init__.py` (empty) | |
| Create | `lux_pipeline/process/tasks/_task_manager.py` | mechanics from `rob_claude_refactor:lux_pipeline/process/tasks/_task_ui_manager.py` |
| Create | `tests/test_task_engines.py` | new |

## What to port (mechanics worth keeping, all from `rob_claude_refactor`)

* `multiprocessing` start method `spawn`, set once in `entry.py` (item 05),
  and `mp_context=multiprocessing.get_context("spawn")` on the executor so
  it does not depend on the global.
* `attach_worker_log_handler` / `make_file_handler` exactly as written
  (replace all handlers in the worker; `delay=True`; `NullHandler` when no
  prefix).
* Ray: `ray.init(log_to_driver=False)` once in `RayEngine.__init__`; the
  `ray.wait(futures, num_returns=1, timeout=0.5)` polling loop (the
  `timeout` is what lets the driver refresh the view between completions).
* `TaskManager.__init__(configs, max_workers=0, args=None)`: `max_workers`
  falls back to `configs.max_workers`; `my_slice` from `args.my_worker`;
  `log_level` from `args.log`; `recids`, `max_records` and other
  per‑command options are read from `args` here (the `rob_claude_refactor`
  fix that moved `--type`, `--max-records`, `--no-overwrite`, `--recid`
  parsing out of `make_manager` into the managers' `__init__`).
* `self.configs = None` before dispatch so the pickled manager carries no
  DB connections; workers receive `cfgs` as a parameter in step 4 above
  instead of reading a module global.
* `prepare_single(name)` checks `internal`, `external`, **and** `results`
  (export needs `merged`).

## Changes from the source

* `_distributed(self, n, cfgs, progress)` — explicit parameters; no
  `self.configs = cfgs` side effect, no `self.my_slice` mutation.
* No `layout`/`disable_ui` anywhere in `lux_pipeline/process`. `process()` takes
  an optional `view` object with a `refresh()` method and nothing else.
* Ray imported lazily inside `RayEngine`; `--engine ray` without Ray raises
  a one‑line error.
* Unknown engine → `ValueError`.
* Replace `debug_process()` with `run_single(n=0)`: `NullEngine` with
  `prefix=None` (logs to the existing handlers, `null_progress`). Tests and
  `lux interactive` use it.

## Known bugs in the source — do not copy

* `rob_refactor`'s `LoggingActor`/`TaskLogHandler` (logs over IPC; the
  livelock the later branch fixed).
* `TaskUiManager._distributed` importing `cfgs` from `lux_pipeline.cli.entry`.
* `HarvestManager.process` manipulating bars with `layout=None`.
* `MpEngine.process` sleeping 1 s and 5 s after completion.

## Steps

1. Write the module (`TaskManager`, `_run_worker`, three engines, ≈200 lines).
2. Wire `_handler.CommandHandler.process()` (item 05): build `prefix` from
   `log_dir`/`command`/timestamp, create `BuildView(prefix, wks, args.log)`
   unless `--no-ui`, then `manager.process(args.engine, prefix, view)` inside
   `with view:` and print the prefix at the end so `lux watch` can be re‑run.
3. `tests/test_task_engines.py`: a `CountingManager` whose
   `_distributed(n, cfgs, progress)` calls `progress(total=10)`,
   `progress(advance=10)` and writes `n` to a file; `NullEngine` runs only
   `my_slice`; `MpEngine(max_workers=3)` produces three `.status` files all
   `done`, three `.log` files, results `[0,1,2]`; a raising worker makes
   `process()` raise; `RayEngine` under `pytest.importorskip("ray")`.
   Workers need a `Config`: point `LUX_BASEPATH` at the item‑04 test config
   directory via `monkeypatch.setenv` before spawning (spawned children
   inherit the environment), or give `CountingManager` a flag that makes
   `_run_worker` skip `get_configs()`.

## Acceptance criteria

* `python -c "import lux_pipeline.process.tasks._task_manager"` succeeds without Ray.
* Tests pass with and without `ray`.
* `grep -rn "rich\|_view\|layout" lux_pipeline/process/` returns nothing.
