# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

LUX data transformation pipeline (Yale). It harvests/loads cultural-heritage records from ~20 external sources (Wikidata, LC, Getty, VIAF, etc.) and internal Yale units, maps them to Linked Art JSON-LD, reconciles records describing the same entity, merges them, and exports to a discovery layer (MarkLogic / QLever).

## Setup and commands

```bash
pip install -e .                 # installs the `lux` console script
pip install -r requirements.txt
```

Requires Postgres and Redis running locally (see install.md). Runtime configuration is bootstrapped from the `LUX_BASEPATH` env var (read from a `.env` file via python-dotenv), which points at a directory containing a `config_cache` of JSON config records (`system.json` is the base; see `docs/sample_config/`).

```bash
lux initialize       # create a new installation (only command that works without LUX_BASEPATH)
lux testinstall      # diagnose installation issues
lux download --source all --type records --max_workers 10
lux load --source all --type export --max_workers 10
lux harvest / reconcile / merge / export / counts / clear / viz / interactive
```

Global flags on every command: `--source <name,name|all|internal|external>`, `--max_workers N`, `--engine ray|mp|null` (default ray), `--no-ui` (disable the rich terminal UI — useful for non-interactive runs), `--log DEBUG`, `--debug` (raise exceptions instead of swallowing them — use this when developing).

There is no lint config or CI test suite. `pytest` and `coverage` are in requirements_dev.txt; `tests/test-loader.py` is a standalone script exercising the loader against fixtures in `tests/loader_tests/` (note: it still imports the old `pipeline.*` package name and predates the `lux_pipeline` rename).

## Architecture

Everything lives in `lux_pipeline/`, split into four layers:

- **`config.py`** — `Config` loads JSON config records from a config cache, splits sources into `cfgs.internal` (Yale units) and `cfgs.external` (Wikidata, LC, …), and instantiates every component via `importObject("module.Class")` dotted paths from config (auto-prefixed with `lux_pipeline.`). Components are wired by configuration, not imports — to find what class actually runs, look at the source's JSON config (e.g. `fetcherClass`, `mapperClass`, `datacacheClass`).

- **`process/`** — source-agnostic pipeline logic.
  - `process/base/` — base classes for each capability: `fetcher`, `harvester` (IIIF Change Discovery / OAI-PMH), `loader` (dump files → cache), `mapper` (source format → Linked Art), `reconciler`, `index_loader`, `acquirer` (wraps fetcher+mapper: get a record from cache or network), `exporter`, `downloader`. All inherit `Managable` (`_managable.py`), which provides the progress-bar/task-manager integration.
  - `process/tasks/` — one task manager per pipeline phase (`load_task`, `harvest_task`, `merge_task`, …). `_task_ui_manager.py` distributes work across processes (ray or multiprocessing, selected by `--engine`). Each worker writes its own log file (`{log_dir}/{command}-{timestamp}-w{n}.log`, filtered at the `--log` level) — those files are the log of record; the parent tails them for the rich UI, and only throttled progress-bar state crosses IPC.
  - Top-level modules: `collector` (recursively follow links to gather referenced records), `reconciler` (iterates name/uri reconciliation across all configured sources until no new equivalents are found, writing to the record's `equivalent` property), `merger`, `reidentifier` (rewrite external URIs to internal ids using the idmap), `reference_manager`, `update_manager`, `validator`.

- **`sources/<name>/`** — per-source subclasses of the base classes (`fetcher.py`, `loader.py`, `mapper.py`, `reconciler.py`, `index_loader.py`). This is where all source-specific format knowledge lives. README.md has the implementation-status matrix per source.

- **`storage/`** — `cache/` (postgres, filesystem, redis, github backends behind a dict-like interface; a source typically has a *datacache* of raw records and a *recordcache* of mapped Linked Art), `idmap/` (redis/filesystem/memory key-value store aligning internal ids ↔ external URIs — losing the redis idmap is catastrophic, treat it carefully), `index/` (LMDB inverted indexes used by reconcilers for exact name/uri lookups), `marklogic/` (MarkLogic REST interaction and query translation).

**CLI dispatch:** `lux <command>` dynamically imports `lux_pipeline/cli/<command>.py` and calls its `CommandHandler.process()` (see `cli/_handler.py` for the shared source-resolution / worker / UI scaffolding). A dotted command name (`lux my.module`) imports an external extension command. Note `cli/entry.py` instantiates the global `Config` at import time; other modules import `cfgs` from it. Multiprocessing uses `spawn`, not fork.

**Pipeline data flow:** download/harvest/load → datacache (raw) → mapper → recordcache (Linked Art) → reconcile (builds `equivalent` links via indexes) → idmap → merge → reidentify → export.

## Repo conventions

- `old/` is the pre-refactor script collection — reference only, do not extend. `docs/rebuild.md` still describes that old `./scripts/manage.py` workflow; the `lux` CLI is the current interface. `experiments/` and `contrib/` are one-off scripts.
- This branch (`rob_claude_refactor`) is part of an ongoing refactor from the old `pipeline` package to `lux_pipeline`; stale references to the old name may remain in docs/tests.
