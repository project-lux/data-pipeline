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

## The web UI (`webui_docs/`, done)

`webui_docs/` is a second self-contained plan, in the same form: a web
application (`lux serve`) for configuring, inspecting and running the
pipeline. Read `webui_docs/README.md`, then `DESIGN.md`, then
`00-INDEX.md`; items are `W01`–`W13`, branches `wNN-<slug>`, PR titles
`WNN: <item name>`. W01–W05 are refactors of existing modules (shared run
reader and `.run` records, command introspection, data functions, config
store, record service) with no web code; W06 onwards is the server. All
thirteen were merged by 2026-11-02; `docs/web.md` is the operator's guide.
A new page is a new area (`web/api/<area>.py` + `web/routers/<area>.py`,
one line in `app.AREAS`); anything it changes goes through
`web/actions.record` and is covered by `tests/test_web_security.py`.

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
  `tests/test_config_records.py`), or, for a real `Config` with every
  component instantiated, build on `tests/webui_fixtures.py`:
  `make_configs(tmp_path, instantiate=True)` writes a small installation
  whose caches and maps are all filesystem stores, `seed_records` puts
  records in it, `use_installation` points `runtime.get_configs()` at it
  (web UI item W00). Never build tests on `docs/sample_config` (out of
  date) or `config_cache/` (untracked). Anything that needs the live instance
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
| `lux_pipeline/progress.py` | `null_progress`, `StatusWriter`, `read_status`, `unfinished_workers`, `status_path`/`log_path`, `Sliced`; the one reader of a run's files (web UI item W01): `read_build` (every worker's status and the totals; `BuildView` renders from it), `LogTailer`, `count_workers`, `latest_prefix`, `RUNNING_WINDOW`; the run record `<prefix>.run` (`RunRecord`, `read_run`, `run_state`, `list_runs`), written by the driver, never a worker |
| `lux_pipeline/runtime.py` | `get_configs()` (cached per process; `LUX_BASEPATH` from the environment, then `./.env`, then `~/.lux/.env`, no upward search: `dotenv_paths`, `find_dotenvs`; the global `--env NAME|FILE|DIR` (`use_env`: `~/.lux/NAME.env`, a file, or `DIR/.env`) overrides all of them and sets `LUX_ENV_FILE`), `reset_configs()`, `configure_logging()` |
| `lux_pipeline/log.py` | the shared `logger` |
| `lux_pipeline/config.py` | `importObject` with legacy-path shim; `Config` reading `system`/`base` and `default` records; `max_workers`, `temp_dir`, `indexes_dir`; optional `nt_dir`, `distribution_dir` (both fall back to `exports_dir`), `backup_dir`; `record_origins` (each record's file and sub-config store) and `base_record_key`; a source's or map's `name` (and a map's `tableName`) that Postgres cannot use unquoted stops loading (`store_name_problem`: `[a-z_][a-z0-9_]*`, at most 40) |
| `lux_pipeline/config_store.py`, `config_schema.py`, `cli/config.py` | web UI item W04: configuration records read, checked and written in their own files. `write_record` edits in place (only changed top-level keys, in the file's own indent: the files are 4-space, 2-space, tab and hand-grouped, and kept in git); `write_text` writes an editor's text verbatim; `validate_record` / `check_record` (types from the catalogue, every `*Class` importable, required keys, source name = key, unique map names; a pre-rename class path or a key written twice is a warning); `config_schema` lists every key in use with its reader. `lux config list|show|check|set|unset|edit|diff|new|delete`, configuration read not instantiated; writes refuse while a build runs (`--force`) |
| `lux_pipeline/cli/entry.py`, `_handler.py`, `help.py` | `lux` dispatcher, `BaseHandler` / `CommandHandler` (`needs_configs`, `resolve_sources`, `make_manager`, `make_view`) |
| `lux_pipeline/cli/_view.py`, `watch.py` | `BuildView` (the only Rich user) and `lux watch` |
| `lux_pipeline/cli/counts.py`, `testinstall.py`, `list.py` | web UI item W03: each prints from data a page can show too: `counts.counts_data` (a section per kind, None when not asked for), `testinstall.CommandHandler.run_checks(args, out, stop_on_fail, cfgs)` (`[{label, state, detail, required}]`; without stopping, a check whose prerequisite failed says so instead of running), `list.source_rows` / `map_rows` / `index_rows` |
| `lux_pipeline/records.py`, `cli/show.py` | web UI item W05: `resolve` any reference (URI, `##qua` key, curie, YUID, or identifier + source; works on an unbuilt configuration) and `journey` it through datacache, recordcache, reconciled, identity (`get_cluster` where the map has it), rewritten, merged and marklogic, trying every key form the pipeline writes; `mapped=True` maps the stored raw record without fetching or storing; `instantiate_source` builds a source once (`Config.instantiate` rebuilds every call); `sample` (closes its iteration and rolls back the cache's `iterating_conn`, which `end_read` does not touch: otherwise a Postgres connection stays idle in transaction holding the read lock), `metadata`, `validate`. `lux show REF [--cache STAGE] [--map] [--validate] [--raw]`, `lux show --sample SOURCE`. `cli/viz.graph_data` is `lux viz`'s graph as data |
| `lux_pipeline/web/`, `cli/serve.py` | web UI item W06: `lux serve` (`.[web]` extra: FastAPI, uvicorn, Jinja2; imported only by `web/` and inside `serve.process`). `web/app.py` `create_app(settings)`; `AREAS` lists each area as `web/api/<area>.py` (data functions + JSON under `/api/<area>`) and `web/routers/<area>.py` (pages via `app.page()`, `NAV`); `redirect_with` / `flash` for a message after a POST (kept in the server). Areas so far: dashboard, counts / timing / maintenance (W11: shaped from `cli/counts`, `process/timing_report`, `storage/pg_tune` and each cache's `bloat()`; counts and bloat stream rows over SSE on request via `static/streamtable.js`; other directories only inside the installation, `security.under`), records (W08: `/records/show?q=<any reference>[&source=]` from `records.journey`; mapping via `AppState.build_all`, never `instantiate_all`), runs (W09: `web/api/runs.py` run_data / log_data / run_events from `progress.describe_run`, `LogTailer`; a URL names a run by its prefix relative to `log_dir`; `/log` returns file offsets the `/events?offsets=` stream continues from; test streams with `tests/web_client.LiveServer`, a real uvicorn server, since `TestClient` holds a stream until it ends), config (W07: Fields form saved with `config_store.write_record`, JSON tab with `write_text`; actions at `/config/_new`, `/config/_reload`), health, commands (W10: `web/runner.py` `Runner` starts `python -m lux_pipeline.cli.entry <cmd> ... --no-ui --prefix P` in its own session, output in `P.out`, a provisional `P.run` at once; `LUX_RUN_EXTRA` (JSON, `_handler.run_extra`) puts `web`/`stdout` into the driver's record, `lux build` drops it for its phases; a reaper thread finishes records the process left unfinished (cancel: exit -15, read as interrupted); `cancel` signals the process group, web runs only; `adopt` after a restart; the build guard per `refuse_while_building`; forms from `introspect.command_spec` minus `introspect.HIDDEN`, read by `api/commands.form_values` for preview, start and prefill; "run again" via `introspect.values_from_argv`; `/commands/build` with phase boxes and `lux build --dry-run` steps), graph (W12: `/graph?a=&b=`, `lux viz`'s `records.identity_graph` after `AppState.build_all`, one replay at a time under `api/graph.REPLAY_LOCK` with the source reconcilers' debug settings put back, seeded spring layout to SVG, 120 s timeout). `web/state.py` `AppState`: its own `Config` (not `runtime`'s; never instantiated whole), `source()` via `records.instantiate_source`, `idmap()` with the memory cache off, `reset()`, the last health run. CSRF and the token are app-wide dependencies (`web/security.py`; W13: `/login` sets the token cookie, `OPEN_PATHS` / `OPEN_PREFIXES` need none, `SECURITY_HEADERS` on every response (CSP `default-src 'self'`, not on `/api/docs`), `under` for paths from URLs). Every change or start is a line in `<log_dir>/web_actions.log` (`web/actions.record`, shown at `/actions`); slow reports run one at a time (`AppState.claim` / `only_one`: counts, bloat). Templates in `web/templates`, one stylesheet and ES modules in `web/static`, no inline scripts or styles (bars are `<progress>`). Tests: `tests/web_client.make_client` over the W00 installation. `docs/web.md` |
| `lux_pipeline/cli/introspect.py` | web UI item W02: every command and its options as data from the real parsers (`command_spec`, `all_command_specs`; `lux help [cmd] --json`), `argv_from_values` (form values to a command line: coerced, positionals first, choices / required checked, `my_worker` refused), `GROUPS`, `NOT_RUNNABLE`, `confirm_reason` / `confirm_needed` (the commands that delete or rewrite what a build depends on). Not a command, like `entry.py` |
| `lux_pipeline/process/tasks/_task_manager.py` | `TaskManager`, `_run_worker`, null / mp / ray engines, `WorkerFailed` |
| `lux_pipeline/process/timing.py` | `PhaseTimer` (takes `progress=`; its prints are intentionally kept; its JSON carries `started_at` / `updated_at` clock times, and `host_cpu` start / now readings from `process/host_cpu.py`: postgres's cpu and the machine's busy / iowait split, from `/proc` on Linux, psutil on macOS). Item 41: `profile=` / `LUX_PROFILE=1` (set by `--profile` on reconcile, merge, export) runs cProfile and writes `profile-<phase>-<slice>.prof` beside the timing JSON; `lux timing-report --profile` aggregates them: the numbers in `lux_pipeline/process/timing_report.py` (`*_data` helpers plus the plain text the root `timing-report.py` and `--no-ui` print), the Rich view in `cli/_timing_view.py` |
| `lux_pipeline/process/phases/reconcile.py` | `select_sources`, `reconcile_slice`: `run-reconcile.py`'s body; the loops are moved verbatim, never edited; `next_update_token` / `new_update_token(cfgs, idmap=None)` (`manage-data.py --new-token`; reads the file when no idmap); `write_refs` (`--write-refs`). The files the phases hand each other live under `temp_dir`: `reference_manager.metatypes_path` / `metatypes_pattern` (`metatypes-<n>.json`) and `references_path` (`reference_uris.txt`), never the working directory |
| `lux_pipeline/process/phases/identify.py` | `resolve_identity`, `report_lines`, `vacuum_idmap`: `run-identify.py`'s body, also `ReconcileManager.finish`'s first step |
| `lux_pipeline/cli/identify.py`, `new_token.py`, `metatypes.py`, `write_refs.py` | the steps between reconcile and merge as `BaseHandler`s; all refuse while a build runs (`_handler.refuse_while_building`, `--force`); `new-token` has `needs_configs = False` and never builds the idmap |
| `lux_pipeline/process/tasks/reconcile_task.py`, `cli/reconcile.py` | `ReconcileManager` and `lux reconcile`: workers call `reconcile_slice`; the driver tail (identify, `ReferenceManager.merge_metatypes`, write-refs) is skipped for `--no-finish`, `--my-worker`, `--recid`, `--baseline` |
| `lux_pipeline/process/phases/merge.py` | `read_update_token`, `select_sources`, `make_claim_member`, `merge_slice`: `run-merge.py`'s body; the loops are moved verbatim, never edited |
| `lux_pipeline/process/tasks/merge_task.py`, `cli/merge.py` | `MergeManager` and `lux merge`: workers call `merge_slice`; `--recid` is one unsliced worker with no references pass; `--only-refs`; the driver checks the token and `<temp_dir>/reference_uris.txt` before any worker starts; no tail |
| `lux_pipeline/process/phases/export.py` | `export_slice` (`run-export.py`'s body, loop verbatim), `export_sources_slice` (`run-source-build.py`), `export_nt_slice` (`manage-data.py --nt`); the wrappers pass the old `/data-export/output/lux/nt` path |
| `lux_pipeline/process/tasks/export_task.py`, `cli/export.py` | `ExportManager` and `lux export`: `--type` (`marklogic`, `merged`, `nt`, `sources`, `external`) dispatches to the phase functions (`external` is `export_external`, the datacache records a recordcache used, for `lux load --type export`; one worker per source); `--cache datacache[,...]` instead exports whole caches of any `--source` (`export_cache_slice`, each source's cache sliced across the workers, `export_<source>_<cache>_<n>.jsonl.gz`, the same tabjson lines; replaced export-source.py / exportExternal.py); `--format lmdb` builds LMDB distribution stores from merged / external / `--cache` in `distribution_dir` (workers write sorted runs, the driver tail merges them; `--my-worker` slices then `--merge-only`; `--lmdb-keys uuid|text`; replaced make_dataset_lmdb.py / make_external_lmdb.py); JSONL to `exports_dir`, N-Triples to the optional `nt_dir` (falls back to `exports_dir`), `--out-dir` overrides both; no tail |
| `lux_pipeline/storage/backup.py`, `cli/_targets.py` | item 39: `backup(store, kind, name, dest_dir)` / `restore(store, src_dir, entry)` and the manifest; native `pg-copy` (Postgres caches, idmap, reference maps; restore drops and rebuilds indexes in one transaction) and `lmdb-copy`, else portable `jsonl` / `idmap-jsonl`; `backup_refused()` on the subsidiary and Redis idmaps. `_targets` is the `--source`/`--map`/`--index` resolution `counts`, `clear` and backup share |
| `lux_pipeline/cli/backup.py`, `restore.py` | `lux backup` / `lux restore` (`BaseHandler`s, sequential): `<dest>/<label>/` + `manifest.json` written last; `--all`, `--portable`, `--verify`, free-space check; restore resolves and checks everything first, needs `--force` for the idmap and non-empty stores; both refuse while building. Optional `backup_dir` (falls back to `<data_dir>/backups`) |
| `run-all2.sh`, `*_parallel2.sh`, `scripts/wait-for-flags.sh` | item 31: the shell orchestration on `lux`, beside the unchanged `run-all.sh` / `*_parallel.sh` until both are compared on the build machine (then the old ones go). `ENGINE=null` (default): one `lux … --my-worker i --prefix P` per slice; `ENGINE=mp`: one `lux` per phase. `wait_for_phase` waits for `"done": true` in every `P-w*.status` and fails on a `Traceback` in a `P-w*.log` or a `lux <cmd>:` line in the stdout files. `load_parallel.sh` still runs `run-load.py` |
| `lux_pipeline/cli/build.py` | `lux build`: `run-all.sh` as one command. Phases `clear` (`--clear` only), `sheets`, `reconcile` (with its finishing steps), `merge`, `portal`, `export`, each a `lux` subprocess (`python -m lux_pipeline.cli.entry`), never a root script; `--from`/`--to`/`--skip`/`--dry-run`; global options forwarded, `--prefix P` becomes `P-<phase>`; the phases' own clean-up is theirs (`clear_stale` in the reconcile / merge / export managers: stale `metatypes-*.json`, `<log_dir>/flags/<phase>_is_done-*`, the type's `export_*_<n>.jsonl*`, only on a run over every slice); appends to `<log_dir>/pipeline_process_status.txt`; `needs_configs = False`, so the driver never opens the idmap |
| `lux_pipeline/process/sheets.py`, `cli/sheets.py` | `lux sheets`: `google-sames-diffs.py` + the two `load-csv-map2.py` loads. `SheetImport` is the process (tabs Same As / Different From / Fixes; `fetch` reads every tab, then writes `<data_dir>/sameAs|differentFrom/google.csv` and `xpath_fixes.json`; `load` adds the CSVs to the merged `indexLoader`); a format subclass only implements `read_rows(name, last_col)` (and `open`/`close`/`describe`): `GoogleSheetImport` (default; `.[google]` extra), `XlsxSheetImport` (`.[xlsx]`, openpyxl), `CsvSheetImport` (a directory of `<tab name>.csv`). `sheet_import(cfgs, path)` picks one; `lux sheets --file` selects a file. Optional libraries are imported in `open` |
| `lux_pipeline/process/phases/portal.py`, `cli/portal.py` | `lux portal`: `post-build-portal.py`. YPM `recordcache2` keys are distance 0; references walked to distance 5; `change = "ypm"` set in the merged recordcache metadata; spawn process pools whose workers use `runtime.get_configs()` |
| `lux_pipeline/storage/pg_tune.py`, `cli/pg_tune.py` | `lux pg-tune` (formerly pg-tune.py): `analyse` (machine + the server `Config.caches` points at), `report` (the SQL as lines), `pending_changes` (what `lux testinstall`'s last step warns about; `--skip-pg-tune`); `--workers` defaults to `max_workers`, `--idmap-table` to the idmap's `tableName`; needs no configuration |
| `lux_pipeline/storage/distribution.py` | LMDB distribution stores: `data` (key -> zlib JSON), `index` (dupsort, class -> blocks of keys in key order: 30 UUIDs, or NUL-joined text keys up to 500 bytes), `meta` (`b"store"` -> JSON description); keys 16-byte UUIDs or `<source>:<identifier>` text; `RunWriter` (sorted runs + `w<n>.done`), `build_store` (in-order merge, `append=True` checked, built in `<path>.tmp`), `DistributionStore` (reader) |
| `lux_pipeline/cli/pg_maintain.py` | `lux pg-maintain` (mirrors `pg-tune`): `--bloat`, `--drop-time-indexes`, `--idmap-indexes [--keep-old-index]`, `--rewrite` (named sources and `--cache` only), `--vacuum`/`--optimize`, run in that order over manage-data.py's tables (sources' datacache/recordcache/recordcache2, results' recordcache/recordcache2) narrowed by `--source`/`--cache`, plus `--map` for vacuum/bloat; non-Postgres stores skipped; all but `--bloat` refuse while building |
| `lux_pipeline/process/tasks/validate_task.py`, `cli/validate.py` | `ValidateManager` and `lux validate` (replaces `manage-data.py --validate`): each worker validates its slice of each `--source`'s `--cache` (default recordcache; default source internal) with `cfgs.validator`, one JSON line per error in `<prefix>-errors/validate-w<n>.jsonl`; anyOf/oneOf errors reported by `best_match`'s sub-error (rule `anyOf>...`); summary groups errors by class, rule and path with list positions as `*`; `--ignore`, `--max-records`, `--strict` (exit 1 on invalid); missing schemas stop it before any worker |
| `lux_pipeline/process/tasks/refetch_task.py`, `cli/refetch.py` | `RefetchManager` and `lux refetch` (replaces `manage-data.py --reload` and updateExternal.py, both of which had become no-ops: they set `acquirer.force_rebuild`, unread since the acquirer took `refetch=`): `acquirer.do_fetch(id, store=True, refetch=True)` for the identifiers each source's recordcache used (`##qua` stripped; default) or, with `--all-records`, every datacache key; the worker enables the source's fetcher; `--source` required (`all` = external sources); counts fetched / empty / redirected / failed |
| `lux_pipeline/process/tasks/differents_task.py`, `cli/build_differents.py` | `DifferentsManager` and `lux build-differents` (replaces make-wd-differents.py): sources with a datacache, a `namespace` and a component (mapper / reconciler / loader) with `process_equivs` (wikidata); each worker writes its slice's diffs as sorted unique `<ns>a,<ns>b` URI rows to `<temp_dir>/differents/<source>-w<n>.csv`, the driver merges them into `<data_dir>/differentFrom/<source>.csv` (`--out-dir`; `.tmp` + rename); `--my-worker` writes the part only, `--merge-only` merges; `--load` adds it to merged's `indexLoader` `diffs` (refuses while building, `--force`). `WdLoader` no longer writes the unused bare-Q-id `wd_diffs_*.csv` |
| `lux_pipeline/cli/viz.py` | item 36: `lux viz --recid <from>,<to>` replays one identity's records with debug on (Reconciler, Collector, source reconcilers) and draws `Reconciler.debug_graph` with netext (`.[ui-extras]`, imported inside `process`); a second Rich user besides `_view.py` |
| `lux_pipeline/storage/cache/abstract.py` | item 38: `AbstractCache`, the cache API contract: required methods raise `NotImplementedError`, the rest (`get_multi`, `has_multi`, `get_fresh`, `len_estimate`, type iterators, bulk, commit hooks, dict methods) have defaults; `PooledCache` and `FsCache` inherit it. Backend-only capabilities (`backup`/`restore`, Postgres maintenance, `get_like`) stay out because callers probe for them with `getattr`. `FsCache` keeps times as file mtimes and `refresh_time`/`valid`/`change` in `<key>.json.meta` |

Worker status and log files follow `<log_dir>/<command>-<timestamp>-w<n>.status|.log`.
Beside them the driver writes `<prefix>.run`: the command line, host and
pid, planned and final worker count, sources, `parent` (a `lux build`'s
prefix, from `LUX_PARENT_RUN`), start, finish and exit status (130 for an
interrupt). `CommandHandler.process` writes it for every worker command
(not for one `--my-worker` slice) and `entry.run` re-finishes it when a
command fails after its workers; `lux build` writes its own with
`phases`, `current` and `steps`, and gives every worker phase the prefix
`<build prefix>-<phase>`. A run the web UI starts also has `<prefix>.out`
(its output) and `"web": true` / `"stdout"` in its record (passed in
`LUX_RUN_EXTRA`); a signal's negative exit reads as interrupted. The web
UI's own log of what it changed is `<log_dir>/web_actions.log`.
