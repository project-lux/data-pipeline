# CLAUDE.md

Guidance for Claude Code sessions in this repository. Read this first; then
`refactor_docs/README.md` and `refactor_docs/00-INDEX.md` for the current
work.

## What this repository is

The LUX data pipeline: harvests records from Yale units and external
authorities, reconciles and merges them into one Linked Art record per entity,
and exports to MarkLogic / QLever. The Python package is `lux_pipeline/`
(namespace package, no `__init__.py` files; one directory per source under
`lux_pipeline/sources/`, plus `yale/` and `lux/` groups). Legacy entry points
are the root `run-*.py`, `manage-data.py` and `*_parallel.sh` scripts; the new
entry point is the `lux` console script (`lux help`).

## The current work: porting `rob_refactor` onto `main`

`refactor_docs/` is a self-contained, ordered plan (41 items) for bringing the
useful parts of the `rob_refactor` / `rob_claude_refactor` branches onto
`main` as one PR per item. Items 00–07 (layout, logging, progress protocol,
runtime, config, packaging + `lux` entry, console view + `lux watch`, task
engines) are merged as of 2026-10-24. Find the next item by reading
`refactor_docs/00-INDEX.md` top to bottom for the first unticked box whose
dependencies are ticked.

Per item, the workflow that has worked:

1. `git checkout main && git pull --ff-only`, then branch `NN-<slug>` using the
   slug from the index entry's document name (e.g. `08-counts-command`).
2. Follow the item's `NN-*.md` exactly: Files, What to port, What to keep from
   `main`, Known bugs (do not copy them), Steps, Acceptance criteria. Read
   source with `git show rob_claude_refactor:<path>` (prefer it over
   `rob_refactor`).
3. Write the tests the document asks for; run `python -m pytest tests/ -q`.
4. Tick the item in `refactor_docs/00-INDEX.md` in the same commit.
5. Commit with a message titled `NN: <item name>`, body explaining what was
   ported, kept and deliberately not copied, ending with the Co-Authored-By
   line. Push the branch and open a PR against `main` titled `NN: <item name>`
   whose body covers what changed, deviations from the document, verification
   (test counts, acceptance commands run), and the config-copies note.
6. Each item is its own PR straight against `main`; no integration branch.

Ground rules from `refactor_docs/README.md` that matter most: one item per
PR; no behaviour change to reconcile/identify/merge/export logic (never edit
a `with timer.stage(...)` block); legacy scripts keep working until item 33;
new config keys are optional with fallbacks; library code logs via
`logging.getLogger("lux_pipeline")` and never `print`s; components take
`progress=null_progress` and nothing under `lux_pipeline/process` or
`lux_pipeline/storage` imports `rich` or knows which engine runs it.

## Opening PRs

The `gh` CLI is not installed. Push with git, then create the PR with the
GitHub REST API: write the body to a scratch file, build the JSON with
Python, and POST to
`https://api.github.com/repos/project-lux/data-pipeline/pulls` using the
token from `printf "protocol=https\nhost=github.com\n" | git credential fill`
as a Bearer token. PATCH the same URL plus `/<number>` to update a body.

## Build-machine config copies

`config_cache/config_cache/` (untracked, never committed) is a copy of the
build machine's live configuration files. Any PR that adds, renames or
retires a config key or `*Class` path must apply the same edit there, check
the files still parse as JSON and every `*Class` value resolves through
`lux_pipeline.config.importObject` without a legacy warning, and say so in
the PR description. Leave the local instance under
`/Users/rs2668/Development/lux/instance` alone; Rob rebuilds it from the
copies once the port is complete. The copies load as a full `Config` on this
laptop (the validator reads its schemas on first use, not at construction).

## Environment and testing

- Python 3.14 virtualenv at `/Users/rs2668/Development/ENV`; the repo is
  installed editable (`pip install -e .`, package `lux-pipeline`), so `lux`
  is on the path. Ray 2.55 is installed.
- `python -m pytest tests/ -q` must pass. Expect one skip:
  `tests/test_identity_streaming.py::test_stream_partition_matches_reference`
  is a known failure on `main` (the partition run writes no
  `assertions-0.tsv`), marked skip until it is fixed separately. The Ray
  engine test takes ~5 s; `LUX_TEST_RAY=0` skips it. Ray workers cannot
  import test modules by name, so a test manager for the Ray engine must be
  defined inside the test function.
- No Postgres or Redis runs locally. Tests stub them (see
  `tests/test_identity.py`, `tests/test_merge_batch.py`,
  `tests/test_config_records.py`). Anything that needs the live instance
  (`manage-data.py --counts`, `run-reconcile.py --aat --recid ...`, a full
  build) is run by Rob on the build server; say so in the PR instead of
  claiming it was verified.
- The local `.env` points `LUX_BASEPATH` at the local instance, whose configs
  still use pre-item-00 class paths, so `lux help` and
  `lux_pipeline.runtime.get_configs()` print ~40 legacy-path warnings from
  the `importObject` shim. That is expected until the instance is rebuilt.

## Where the new pieces live

| Module | Role |
|---|---|
| `lux_pipeline/progress.py` | `null_progress`, `StatusWriter`, `read_status`, `unfinished_workers`, `status_path`/`log_path`, `Sliced` |
| `lux_pipeline/runtime.py` | `get_configs()` (cached per process), `reset_configs()`, `configure_logging()` |
| `lux_pipeline/log.py` | the shared `logger` |
| `lux_pipeline/config.py` | `importObject` with legacy-path shim; `Config` reading `system`/`base` and `default` records; `max_workers`, `temp_dir`, `indexes_dir` |
| `lux_pipeline/cli/entry.py`, `_handler.py`, `help.py` | `lux` dispatcher, `BaseHandler` / `CommandHandler` (`needs_configs`, `resolve_sources`, `make_manager`, `make_view`) |
| `lux_pipeline/cli/_view.py`, `watch.py` | `BuildView` (the only Rich user) and `lux watch` |
| `lux_pipeline/process/tasks/_task_manager.py` | `TaskManager`, `_run_worker`, null / mp / ray engines, `WorkerFailed` |
| `lux_pipeline/process/timing.py` | `PhaseTimer` (takes `progress=`; its prints are intentionally kept). Item 41: `profile=` / `LUX_PROFILE=1` (set by `--profile` on reconcile, merge, export) runs cProfile and writes `profile-<phase>-<slice>.prof` beside the timing JSON; `timing-report.py --profile` aggregates them |
| `lux_pipeline/process/phases/reconcile.py` | `select_sources`, `reconcile_slice`: `run-reconcile.py`'s body; the loops are moved verbatim, never edited; `next_update_token` / `new_update_token(cfgs, idmap=None)` (`manage-data.py --new-token`; reads the file when no idmap); `write_refs` (`--write-refs`) |
| `lux_pipeline/process/phases/identify.py` | `resolve_identity`, `report_lines`, `vacuum_idmap`: `run-identify.py`'s body, also `ReconcileManager.finish`'s first step |
| `lux_pipeline/cli/identify.py`, `new_token.py`, `metatypes.py`, `write_refs.py` | the steps between reconcile and merge as `BaseHandler`s; all refuse while a build runs (`_handler.refuse_while_building`, `--force`); `new-token` has `needs_configs = False` and never builds the idmap |
| `lux_pipeline/process/tasks/reconcile_task.py`, `cli/reconcile.py` | `ReconcileManager` and `lux reconcile`: workers call `reconcile_slice`; the driver tail (identify, `ReferenceManager.merge_metatypes`, write-refs) is skipped for `--no-finish`, `--my-worker`, `--recid`, `--baseline` |
| `lux_pipeline/process/phases/merge.py` | `read_update_token`, `select_sources`, `make_claim_member`, `merge_slice`: `run-merge.py`'s body; the loops are moved verbatim, never edited |
| `lux_pipeline/process/tasks/merge_task.py`, `cli/merge.py` | `MergeManager` and `lux merge`: workers call `merge_slice`; `--recid` is one unsliced worker with no references pass; `--only-refs`; the driver checks the token and `reference_uris.txt` (cwd) before any worker starts; no tail |
| `lux_pipeline/process/phases/export.py` | `export_slice` (`run-export.py`'s body, loop verbatim), `export_sources_slice` (`run-source-build.py`), `export_nt_slice` (`manage-data.py --nt`); the wrappers pass the old `/data-export/output/lux/nt` path |
| `lux_pipeline/process/tasks/export_task.py`, `cli/export.py` | `ExportManager` and `lux export`: `--type` (`marklogic`, `merged`, `nt`, `sources`, `external`) dispatches to the phase functions (`external` is `export_external`, the datacache records a recordcache used, for `lux load --type export`; one worker per source); JSONL to `exports_dir`, N-Triples to the optional `nt_dir` (falls back to `exports_dir`), `--out-dir` overrides both; no tail |
| `lux_pipeline/storage/backup.py`, `cli/_targets.py` | item 39: `backup(store, kind, name, dest_dir)` / `restore(store, src_dir, entry)` and the manifest; native `pg-copy` (Postgres caches, idmap, reference maps; restore drops and rebuilds indexes in one transaction) and `lmdb-copy`, else portable `jsonl` / `idmap-jsonl`; `backup_refused()` on the subsidiary and Redis idmaps. `_targets` is the `--source`/`--map`/`--index` resolution `counts`, `clear` and backup share |
| `lux_pipeline/cli/backup.py`, `restore.py` | `lux backup` / `lux restore` (`BaseHandler`s, sequential): `<dest>/<label>/` + `manifest.json` written last; `--all`, `--portable`, `--verify`, free-space check; restore resolves and checks everything first, needs `--force` for the idmap and non-empty stores; both refuse while building. Optional `backup_dir` (falls back to `<data_dir>/backups`) |
| `run-all2.sh`, `*_parallel2.sh`, `scripts/wait-for-flags.sh` | item 31: the shell orchestration on `lux`, beside the unchanged `run-all.sh` / `*_parallel.sh` until both are compared on the build machine (then the old ones go). `ENGINE=null` (default): one `lux … --my-worker i --prefix P` per slice; `ENGINE=mp`: one `lux` per phase. `wait_for_phase` waits for `"done": true` in every `P-w*.status` and fails on a `Traceback` in a `P-w*.log` or a `lux <cmd>:` line in the stdout files. `load_parallel.sh` still runs `run-load.py` |
| `lux_pipeline/cli/build.py` | `lux build`: `run-all.sh` as one command. Phases `clear` (`--clear` only), `google`, `reconcile` (with its finishing steps), `merge`, `portal`, `export`, each a `lux` subprocess (`python -m lux_pipeline.cli.entry`), never a root script; `--from`/`--to`/`--skip`/`--dry-run`; global options forwarded, `--prefix P` becomes `P-<phase>`; the phases' own clean-up is theirs (`clear_stale` in the reconcile / merge / export managers: stale `metatypes-*.json`, `<log_dir>/flags/<phase>_is_done-*`, the type's `export_*_<n>.jsonl*`, only on a run over every slice); appends to `<log_dir>/pipeline_process_status.txt`; `needs_configs = False`, so the driver never opens the idmap |
| `lux_pipeline/process/sheets.py`, `cli/google.py` | `lux google`: `google-sames-diffs.py` + the two `load-csv-map2.py` loads. `SheetImport` is the process (tabs Same As / Different From / Fixes; `fetch` reads every tab, then writes `<data_dir>/sameAs|differentFrom/google.csv` and `xpath_fixes.json`; `load` adds the CSVs to the merged `indexLoader`); a format subclass only implements `read_rows(name, last_col)` (and `open`/`close`/`describe`): `GoogleSheetImport` (default; `.[google]` extra), `XlsxSheetImport` (`.[xlsx]`, openpyxl), `CsvSheetImport` (a directory of `<tab name>.csv`). `sheet_import(cfgs, path)` picks one; `lux google --file` selects a file. Optional libraries are imported in `open` |
| `lux_pipeline/process/phases/portal.py`, `cli/portal.py` | `lux portal`: `post-build-portal.py`. YPM `recordcache2` keys are distance 0; references walked to distance 5; `change = "ypm"` set in the merged recordcache metadata; spawn process pools whose workers use `runtime.get_configs()` |
| `lux_pipeline/cli/viz.py` | item 36: `lux viz --recid <from>,<to>` replays one identity's records with debug on (Reconciler, Collector, source reconcilers) and draws `Reconciler.debug_graph` with netext (`.[ui-extras]`, imported inside `process`); a second Rich user besides `_view.py` |
| `lux_pipeline/storage/cache/abstract.py` | item 38: `AbstractCache`, the cache API contract: required methods raise `NotImplementedError`, the rest (`get_multi`, `has_multi`, `get_fresh`, `len_estimate`, type iterators, bulk, commit hooks, dict methods) have defaults; `PooledCache` and `FsCache` inherit it. Backend-only capabilities (`backup`/`restore`, Postgres maintenance, `get_like`) stay out because callers probe for them with `getattr`. `FsCache` keeps times as file mtimes and `refresh_time`/`valid`/`change` in `<key>.json.meta` |

Worker status and log files follow `<log_dir>/<command>-<timestamp>-w<n>.status|.log`.
