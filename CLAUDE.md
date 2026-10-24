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

`refactor_docs/` is a self-contained, ordered plan (40 items) for bringing the
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
copies once the port is complete. Loading the copies as a full `Config` on
this laptop needs `Validator.__init__` stubbed (their `base_dir` is on the
build machine).

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
| `lux_pipeline/progress.py` | `null_progress`, `StatusWriter`, `read_status`, `status_path`/`log_path`, `Sliced` |
| `lux_pipeline/runtime.py` | `get_configs()` (cached per process), `reset_configs()`, `configure_logging()` |
| `lux_pipeline/log.py` | the shared `logger` |
| `lux_pipeline/config.py` | `importObject` with legacy-path shim; `Config` reading `system`/`base` and `default` records; `max_workers`, `temp_dir`, `indexes_dir` |
| `lux_pipeline/cli/entry.py`, `_handler.py`, `help.py` | `lux` dispatcher, `BaseHandler` / `CommandHandler` (`needs_configs`, `resolve_sources`, `make_manager`, `make_view`) |
| `lux_pipeline/cli/_view.py`, `watch.py` | `BuildView` (the only Rich user) and `lux watch` |
| `lux_pipeline/process/tasks/_task_manager.py` | `TaskManager`, `_run_worker`, null / mp / ray engines, `WorkerFailed` |
| `lux_pipeline/process/timing.py` | `PhaseTimer` (takes `progress=`; its prints are intentionally kept) |

Worker status and log files follow `<log_dir>/<command>-<timestamp>-w<n>.status|.log`.
