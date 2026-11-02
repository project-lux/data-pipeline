# W01. Run files: shared reader, `.run` records, `list_runs`

**Size:** M · **Depends on:** — · **Unblocks:** W06, W09, W10
**Read:** `lux_pipeline/progress.py` (all), `lux_pipeline/cli/_view.py`
(`LogTailer`, `BuildView._refresh`), `lux_pipeline/cli/watch.py`
(`count_workers`, `latest_prefix`), `lux_pipeline/cli/_handler.py`
(`CommandHandler.process`, `default_prefix`), `lux_pipeline/cli/build.py`
(`process`, `lux`), `tests/test_build_view.py`, `tests/test_progress.py`,
`tests/test_cli_build.py`

## Intent

Make one module, `lux_pipeline/progress.py`, the reader of everything a
run leaves on disk, so the Rich view, `lux watch` and the web pages all
see a run the same way; and give a run a small record of what it is,
`<prefix>.run`, written by the driver.

After this item:

```python
from lux_pipeline.progress import (read_build, LogTailer, count_workers, latest_prefix,
                                   list_runs, RunRecord, run_path, read_run)
state = read_build(prefix, workers)            # one dict with every worker's status and the totals
runs = list_runs(log_dir)                      # every run under log_dir, newest first
```

and `lux reconcile ...` leaves `<log_dir>/reconcile-<ts>.run` beside its
`-w<n>.status` files.

## Why it matters

`BuildView._refresh` does the reading and the Rich updating in one method,
and `LogTailer` sits in the only module allowed to import Rich. A web page
would have to copy both. The status files also do not say which command
ran, with what arguments, or whether the driver is still alive — things a
run list and a cancel button need.

## Files

| Action | Path | Notes |
|---|---|---|
| Edit | `lux_pipeline/progress.py` | add `read_build`, `LogTailer` (moved), `count_workers`, `latest_prefix` (moved), `run_path`, `RunRecord`, `read_run`, `list_runs`, `pid_alive` |
| Edit | `lux_pipeline/cli/_view.py` | import `LogTailer` from progress; `_refresh` calls `read_build` |
| Edit | `lux_pipeline/cli/watch.py` | import `count_workers`, `latest_prefix` from progress (keep the names re-exported so nothing else breaks) |
| Edit | `lux_pipeline/cli/_handler.py` | `CommandHandler.process` writes the run record at start and end |
| Edit | `lux_pipeline/cli/build.py` | writes its own run record with `phases` / `current`; passes `--prefix` to every phase step even when the user gave none (so phases are discoverable), and `parent` through the environment variable `LUX_PARENT_RUN` |
| Edit | `tests/test_progress.py`, `tests/test_build_view.py`, `tests/test_cli_build.py` | extend |
| Create | `tests/webui_fixtures.py` | the database-free installation fixture every later item uses (see Tests) |

## Design

### `read_build(prefix, workers, visible=None) -> dict`

```python
{
  "prefix": prefix,
  "workers": {0: {...status...} | None, 1: ..., },     # read_status(status_path(prefix, n)) for n in range(workers)
  "seen": 2,                                           # how many have a status file
  "done": False,                                       # seen > 0 and every status says done  (BuildView's rule)
  "completed": 24325, "total": 197530 | None,          # over the visible workers: sum of completed; sum of totals only when every visible worker has one
  "labels": {0: "ycba", 1: "references", ...},         # description or phase or "running" (BuildView's label rule); "waiting" for None
}
```

`visible` is a set of worker numbers to include in the totals (default all).
This is exactly what `BuildView._refresh` computes today; move the logic,
do not reimplement it. `BuildView._refresh` becomes: call `read_build`
with `visible = {n for n, tid in self.tasks.items() if task visible}`,
then for each worker update its Rich task (`completed`, `total`,
description `f"w{n} {label}"`, `start_task` on first status), update the
all-workers task from `completed` / `total`, tail the log, set `self.done`.
Behaviour must be identical; `tests/test_build_view.py` must pass
unchanged before you add to it.

### `LogTailer`

Moved verbatim from `cli/_view.py` (it imports nothing from Rich). Leave
`from lux_pipeline.progress import LogTailer` in `_view.py` so the name is
still there.

### `count_workers(prefix)`, `latest_prefix(log_dir)`

Moved verbatim from `cli/watch.py` with their regex; `watch.py` imports
them. `STATUS_RE` moves too; `_handler._WORKER_RE` is the same pattern and
may import it.

### The run record

```python
def run_path(prefix): return f"{prefix}.run"

class RunRecord:
    """What the driver knows about a run and the workers' files do not.
    Written atomically (tmp + os.replace) like StatusWriter; never by a worker."""
    def __init__(self, prefix, command, argv, *, engine=None, workers=0, visible_workers=None,
                 sources=(), parent=None, extra=None): ...
    def start(self): ...            # pid=os.getpid(), started=time.time(), finished=None, exit=None; write
    def finish(self, exit=0, error=None): ...   # finished=time.time(); write
    def update(self, **fields): ... # e.g. current="merge"; write
    def state(self) -> dict

def read_run(prefix) -> dict | None     # tolerant like read_status
def pid_alive(pid) -> bool              # os.kill(pid, 0), False on ProcessLookupError, True on PermissionError
```

Fields, all always present: `prefix, command, argv, pid, engine, workers,
visible_workers, sources, started, finished, exit, error, parent` plus
anything in `extra` (build uses `phases`, `current`; the web runner adds
`web`, `stdout`). `argv` is `["lux", *sys.argv[1:]]` with `sys.argv[0]`
replaced by `lux` whatever it was (`python -m lux_pipeline.cli.entry` runs
too).

### `list_runs(log_dir) -> list[dict]`

Every prefix under `log_dir` that has a `.run` file **or** any `-w<n>.status`
file (a shell-started run has only the latter). For each:

```python
{"prefix", "command", "started", "finished", "exit", "workers", "parent",
 "state": "running" | "finished" | "failed" | "killed" | "unknown",
 "progress": read_build(prefix, workers)}      # workers from the record, else count_workers
```

State: `exit == 0` → finished; `exit` non-zero → failed; `finished` None
and `pid_alive(pid)` → running; `finished` None and pid dead → killed; no
record → `finished` if `read_build(...)["done"]` else `unknown` (if its
newest status `updated` is within `_handler.RUNNING_WINDOW`, call it
running). `command` without a record comes from the prefix's basename up
to the timestamp (`reconcile-2026-11-02-141500` → `reconcile`), `started`
from the oldest status file's mtime. Sorted by `started` descending.
Prefixes are grouped by the `STATUS_RE` prefix group exactly as
`latest_prefix` groups them.

### Writing it from the driver

In `CommandHandler.process` (`_handler.py`), after `manager` is built and
sources prepared and before the engine runs:

```python
visible = list(range(len(manager.sources), wks)) if getattr(manager, "hide_idle_workers", False) else []
record = RunRecord(prefix, args.command, ["lux", *sys.argv[1:]], engine=args.engine, workers=wks,
                   visible_workers=[n for n in range(wks) if n not in visible],
                   sources=[name for _, name in manager.sources] if manager.sources and isinstance(manager.sources[0], tuple) else list(manager.sources),
                   parent=os.environ.get("LUX_PARENT_RUN") or None)
record.start()
try:
    ... existing run ...
except BaseException as e:
    record.finish(exit=1, error=f"{type(e).__name__}: {e}"); raise
else:
    record.finish(exit=0)
```

`manager.sources` is `[(which, name)]` for most managers; check the shape
rather than assume (some managers keep names only). Writing the record
must never fail the command: wrap the writes as `StatusWriter._write`
does. The "status/log prefix" line is still printed.

In `build.py`: `process` creates `RunRecord(prefix, "build", ..., extra={"phases": [p for p, _ in steps], "current": None})`
with `prefix = args.prefix or default_prefix(cfgs, "build")`, calls
`start()`, `update(current=phase)` as each phase begins, and `finish`.
`steps` must give every phase a prefix: today `self.lux` only adds
`--prefix` when `args.prefix` is set; change it to use the build's own
prefix as the base (`f"{prefix}-{phase}"`) always. One-shot steps
(`workers=False`: clear, new-token, sheets) are `BaseHandler`s and write
no record; that is fine. `run_step` sets `LUX_PARENT_RUN=<build prefix>`
in the child's environment (`subprocess.run(argv, env={**os.environ, ...})`).

## Known hazards

* `progress.py` imports nothing from `rich`, `multiprocessing`, `ray` or
  `lux_pipeline.cli` (its docstring's rule). `_handler.RUNNING_WINDOW` is in
  `cli`; copy the constant into `progress.py` as `RUNNING_WINDOW = 3600` and
  have `_handler` import it from there.
* `lux watch --latest` and `scripts/wait-for-flags.sh` glob `*-w*.status`;
  a `.run` file does not match that glob, so neither notices it. (The
  shell-script tests were removed in PR #341; there is nothing to update
  there.)
* Status files are written by workers in **other processes**; the record by
  the driver. Do not let a worker (`_run_worker`) touch the record.
* A `--my-worker i` run (null engine, shell-orchestrated) is one slice of a
  larger run: do **not** write a `.run` for it, or 24 shell slices will
  fight over one file. `if args.my_worker > -1: record = None`.
* `default_prefix` uses `log_dir` or `"."`; a prefix outside `log_dir` is
  invisible to `list_runs`, as it is to `lux watch --latest` today.

## Steps

1. Move `LogTailer`, `count_workers`, `latest_prefix`, `STATUS_RE` into
   `progress.py`; re-import them in `_view.py` and `watch.py`. Run the
   tests: green, unchanged.
2. Add `read_build`; rewrite `BuildView._refresh` on top of it. Tests green.
3. Add `run_path`, `RunRecord`, `read_run`, `pid_alive`, `list_runs`.
4. Write the record from `CommandHandler.process` and `build.py`.
5. Create `tests/webui_fixtures.py` (below) and the tests.
6. Update the `lux_pipeline/progress.py` row of the table in `CLAUDE.md`
   and the `BuildView` doc line in `docs/codebase.md` if it describes
   `_refresh`.

## Tests

`tests/webui_fixtures.py` — not a test file; imported by tests. It builds a
**database-free installation** in a `tmp_path`:

```python
def make_installation(tmp_path) -> Path:
    """Copy docs/sample_config into <tmp>/config_cache, then rewrite:
    base.json  -> base_dir=<tmp>, data_dir=data/files, log_dir=data/logs, exports_dir=data/exports,
                  indexes_dir=data/indexes, dumps_dir=data/dumps, temp_dir=data/tmp, tests_dir=data/tests, max_workers=2
    caches.json -> no host/port/user; every record gets the filesystem classes:
                  sources: datacacheClass=storage.cache.filesystem.DataCache,
                           recordcacheClass=storage.cache.filesystem.InternalRecordCache | ExternalRecordCache,
                           recordcacheReconciledClass=storage.cache.filesystem.ReconciledRecordCache,
                           recordcache2Class=storage.cache.filesystem.RecordCache
                  merged/marklogic: recordcacheClass=storage.cache.filesystem.MergedRecordCache
                  maps: storeClass=storage.idmap.filesystem.IdMap (idmap) and ...filesystem.FsMap for the rest;
                  drop map_idmap_sub.json, ml_*.json
    Create the directories. Write <tmp>/.env with LUX_BASEPATH=<tmp>.
    Keep only a handful of sources (ycba, yuag, aat, ulan, wikidata, lcnaf) so Config builds fast."""

def make_configs(tmp_path, instantiate=False) -> Config     # Config(basepath=str(tmp_path)), optionally instantiate_all()
def seed_records(cfgs) -> dict                              # a few records in ycba's datacache and recordcache, aat's, one merged record; returns the keys/yuids
def write_fake_run(log_dir, command, workers, *, done=False, record=True, ts="2026-11-02-141500") -> str   # status + log files (+ .run); returns prefix
```

Check first that `Config(basepath=...)` over the rewritten sample builds
with `instantiate=True` and no network. What the filesystem stores need
(read `storage/idmap/filesystem.py` and `storage/cache/filesystem.py`):

* `FsMap.__init__` reads `config["base_dir"]` and `config["all_configs"]`
  (`instantiate_map` sets the latter); give every map record
  `"base_dir": "<tmp>/data/files"` (or put it in a `{"type": "default",
  "name": "map"}` record, which `merge_configs` applies to every map).
* The filesystem `IdMap` reads `<data_dir>/idmap_update_token.txt` and
  raises without a well-formed token: write `__20260101__` there.
* `cache_globals` looks every `globals.json` term up in the idmap;
  `FsMap.get` returns `None` for a missing key, so the globals come out
  `None` and nothing raises. Seed the terms only if a test needs them.
* `FsCache.__init__` creates `<base_dir>/<name>_<tabletype>`; the cache
  records need `base_dir` too (the `caches` record is merged under every
  cache config, so set it there).
* Sources with `fetch` build a `Fetcher` and look up `networkmap`; keep
  `map_networkmap.json` with the `FsMap` class. The internal sources'
  default `ASHarvester` is built at instantiate time from the record's
  `activitystreams` key; the sample records have it.

Record in the fixture's docstring what you had to adjust; the later items
rely on this fixture building a *real* `Config`.

`tests/test_progress.py` additions:

* `read_build` on two status files (one done, one not) gives `seen=2`,
  `done=False`, totals as BuildView computed; with a hidden worker the
  totals exclude it; with no files `seen=0, done=False`.
* `RunRecord.start()` then `read_run()`; `finish(exit=1, error=...)`;
  the write is atomic (no `.run.*.tmp` left).
* `list_runs` over a fake log dir: a run with a record and a live pid
  (`os.getpid()`) is `running`; a dead pid (use a pid from `subprocess`
  that has exited, or 2**22) is `killed`; a shell-only run (status files,
  no record) is `finished` when all done; sorted newest first.

`tests/test_build_view.py`: unchanged tests pass; add one that `BuildView`
and `read_build` agree on `completed` / `total` for the same files.

`tests/test_cli_build.py`: the build writes `<prefix>.run` with `phases`
and the last `current`; every worker step's argv has `--prefix
<prefix>-<phase>`; the child env has `LUX_PARENT_RUN`.

`tests/test_cli_entry.py` or a new `tests/test_run_record.py`: a
`CommandHandler` whose `make_manager` returns a trivial `TaskManager`
(`needs_configs = False`, null engine) leaves a `.run` with `exit 0`; one
whose worker raises leaves `exit 1` and an `error`; `--my-worker 0` leaves
none.

## Acceptance criteria

* `python -m pytest tests/ -q` passes; `tests/test_build_view.py`'s
  pre-existing tests pass without edits.
* `lux watch` behaves exactly as before (manual check on a fake run from
  the fixture: `lux watch <prefix>` from the test's tmp dir).
* After `lux counts` nothing new is written (it is a `BaseHandler`); after
  any `CommandHandler` command a `<prefix>.run` exists with the fields in
  §Design; `lux build --dry-run` writes nothing.
* `grep -n "import rich\|from rich" lux_pipeline/progress.py` is empty.
