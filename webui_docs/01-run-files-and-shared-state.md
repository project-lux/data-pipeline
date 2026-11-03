# W01. Run files: shared reader, `.run` records, `list_runs`

**Size:** M · **Depends on:** — · **Unblocks:** W06, W09, W10 · **Done:** 2026-11-02
**Read:** `lux_pipeline/progress.py`, `lux_pipeline/cli/_view.py`
(`BuildView._refresh`), `lux_pipeline/cli/watch.py`, `lux_pipeline/cli/_handler.py`
(`CommandHandler.process`), `lux_pipeline/cli/build.py`, `lux_pipeline/cli/entry.py`
(`run`), `tests/test_build_view.py`, `tests/test_cli_build.py`

This document describes the item as built. It was revised before
implementation (2026-11-02, agreed with Rob): the test installation moved
to its own item (W00); the worker count comes from the status files, not
the start of the run; a run's state comes from its record; the command
line comes from `entry.run`; `lux build` records each step; interrupts
are recorded.

## Intent

Make `lux_pipeline/progress.py` the one reader of everything a run leaves
on disk, so the Rich view, `lux watch` and the web pages see a run the same
way; and give a run a record of what it is, `<prefix>.run`, written by the
driver.

```python
from lux_pipeline.progress import (read_build, LogTailer, count_workers, latest_prefix,
                                   list_runs, RunRecord, run_path, read_run, run_state)
state = read_build(prefix, workers)            # every worker's status and the totals
runs = list_runs(log_dir)                      # every run under log_dir, newest first
```

`lux reconcile ...` leaves `<log_dir>/reconcile-<ts>.run` beside its
`-w<n>.status` files; `lux build` leaves `<log_dir>/build-<ts>.run` with
its steps.

## Files

| Action | Path | Change |
|---|---|---|
| Edit | `lux_pipeline/progress.py` | `RUNNING_WINDOW`, `STATUS_RE`, `LogTailer`, `count_workers`, `latest_prefix` (moved); `worker_label`, `read_build`, `run_path`, `RunRecord`, `read_run`, `pid_alive`, `run_state`, `list_runs` (new) |
| Edit | `lux_pipeline/cli/_view.py` | `_refresh` renders from `read_build`; `LogTailer` imported from progress (still importable from `_view`) |
| Edit | `lux_pipeline/cli/watch.py` | `count_workers`, `latest_prefix`, `STATUS_RE` imported from progress (still importable from `watch`) |
| Edit | `lux_pipeline/cli/_handler.py` | `RUNNING_WINDOW`, `_WORKER_RE` from progress; `command_argv`, `exit_code`, `finish_record`; `BaseHandler.run_record` / `run_prefix`; `CommandHandler.start_record`; `process` writes and finishes the record |
| Edit | `lux_pipeline/cli/entry.py` | `args.cli_argv`; re-finishes the record when a command fails after its workers succeeded |
| Edit | `lux_pipeline/cli/build.py` | every worker phase gets `--prefix <P>-<phase>` (P: `--prefix` or `<log_dir>/build-<ts>`); the build's record with `phases`, `current`, `steps`; `LUX_PARENT_RUN` in each step's environment (`step_env`) |
| Create | `tests/webui_fixtures.py` | `write_fake_run`, `dead_pid` (W00 adds the installation) |
| Create | `tests/test_run_files.py` | |
| Edit | `tests/test_build_view.py`, `tests/test_cli_build.py` | view / `read_build` agreement; the build's record |

## Design

### `read_build(prefix, workers, visible=None)`

```python
{"prefix", "workers": {n: status | None}, "labels": {n: label},
 "seen": int, "done": bool, "completed": int, "total": int | None}
```

The console view's rules, moved, not reimplemented: label is
`description`, else `phase`, else `running`, `waiting` without a file;
`done` is `seen > 0` and every worker's status says done, hidden workers
included, so pass the number of workers that will write files; the totals
cover `visible` workers only, and `total` is `None` while any of them has
none. `BuildView._refresh` calls it with the set of its visible tasks and
puts the numbers on screen.

### The record

`RunRecord(prefix, command, argv, *, engine, workers, visible_workers,
sources, parent, extra)` writes `<prefix>.run` atomically (tmp +
`os.replace`) and never raises:

```json
{"prefix": "/.../logs/reconcile-2026-11-02-141500", "command": "reconcile",
 "argv": ["lux", "reconcile", "--source", "all", "--max-workers", "24"],
 "host": "lux-build", "pid": 48213, "engine": "mp", "workers": 24,
 "visible_workers": [0, 1, "..."], "sources": ["ycba", "yuag"], "parent": null,
 "started": 1762111500.1, "finished": null, "exit": null, "error": null}
```

* `start()` stamps `pid` and `started`; `update(**fields)` rewrites;
  `finish(exit, error, workers)` stamps `finished`. A later `finish`
  overwrites an earlier one.
* `exit`: 0, 1 for a failure, 130 for an interrupt (`error` is then
  `"interrupted"`), a `SystemExit`'s own code.
* `workers` is planned at the start (`manager.max_workers`; 1 under the
  null engine, which runs worker 0 only) and replaced at the finish by the
  status files' count (`count_workers`) when there are any: download,
  export and index-load reduce their count inside `process()`, and
  harvest's phase 1 runs fewer than phase 2.
* `visible_workers`: all but those `CommandHandler.process` hides
  (`hide_idle_workers`: worker numbers from `len(manager.sources)`),
  trimmed to the final count.
* `sources`: the manager's source names in order, without repeats
  (download has one entry per file).
* `argv`: `entry.run` keeps the arguments it was given as
  `args.cli_argv`; `command_argv(args)` prefixes `lux`.
* `parent`: `LUX_PARENT_RUN` from the environment, which `lux build` sets
  for its steps.
* Not written for one `--my-worker` slice: the slices of one run share a
  prefix and would overwrite each other's record.

Who writes it:

* `CommandHandler.process` (every worker command), after the manager is
  built and its sources prepared, finishing it on success and on any
  exception, `KeyboardInterrupt` included. Kept as `self.run_record`.
* `entry.run` finishes it again when `hdlr.process` raises: harvest (failed
  sources) and validate (`--strict`) raise after the base `process`
  returned, so the record must say how the command ended, not how the
  workers did.
* `lux build` writes its own (`workers` 0, `extra={"phases", "current",
  "steps"}`). Each step's entry, appended as it starts and completed as it
  ends: `{"phase", "command", "argv": ["lux", ...], "prefix" (None for a
  one-shot step), "started", "finished", "exit"}`. The one-shot steps
  (clear, new-token, sheets, portal) write no record of their own, so this
  is where their outcome is.

### `run_state(record, build)`

| Record | State |
|---|---|
| `exit` 0 / 130 / other | `finished` / `interrupted` / `failed` |
| unfinished, this host, driver alive | `running` |
| unfinished, this host, driver gone | `killed` (SIGKILL, the OOM killer: no `finally` ran) |
| none, or from another host | `finished` if every worker says done; `running` if a status was updated within `RUNNING_WINDOW`; else `unknown` |

### `list_runs(log_dir)`

Every prefix under `log_dir` with a `.run` or any `-w<n>.status` file (a
run started slice by slice from a shell has only status files), newest
first: `{"prefix", "command", "state", "started", "finished", "exit",
"error", "argv", "parent", "workers", "visible_workers", "record",
"progress"}`. `workers` is `count_workers(prefix)` when there are files,
else the record's. `command` is the record's, else the prefix's name up
to its timestamp. `started` is the record's, else the prefix's timestamp,
else the oldest file time.

## Known hazards (kept)

* `progress.py` imports nothing from `rich`, `multiprocessing`, `ray` or
  `lux_pipeline.cli`. `_handler` imports `RUNNING_WINDOW` from it.
* Workers never touch the record; `_run_worker` is unchanged.
* `unfinished_workers` and `refuse_while_building` still read only the
  status files, so a run without a record is still seen; a `.run` file
  does not match the `*-w*.status` globs of `lux watch --latest` and
  `scripts/wait-for-flags.sh`.
* A prefix outside `log_dir` is invisible to `list_runs`, as to
  `lux watch --latest`.
* `run-all2.sh` empties `log_dir` (`rm -f $LOGS/*`) at the start of every
  shell build, so the run list reaches back to the last one. `lux build`
  does not.

## Tests (as written)

* `tests/test_run_files.py`: the moved names still importable where they
  were; `read_build` totals, hidden workers, waiting workers, labels;
  record round trip, worker-count rules, an unwritable record not
  raising; `pid_alive`; every `run_state` row; `list_runs` over a running,
  a killed, a shell-only and a build run, newest first, with the worker
  count from the files; the handler writing and finishing the record
  (success, worker failure, interrupt, `--my-worker`, hidden workers);
  `entry.run` re-finishing a record for a command that failed after its
  workers.
* `tests/test_build_view.py`: `BuildView` and `read_build` agree.
* `tests/test_cli_build.py`: the build's record (phases, steps, exits,
  `current`), a failed step, prefixes without `--prefix`, `LUX_PARENT_RUN`,
  no record from `--dry-run`.

## Acceptance criteria

* `python -m pytest tests/ -q` passes; the pre-existing tests of
  `test_build_view.py` and `test_cli_build.py` pass unchanged.
* `lux watch <prefix>` renders a finished fake run as before.
* `lux build --dry-run` writes nothing; any worker command leaves a
  `<prefix>.run`; `grep -n "rich" lux_pipeline/progress.py` finds nothing.
* On the build machine (Rob): a `lux build` leaves `build-<ts>.run` with
  its steps, and each phase's record names it as `parent`.
