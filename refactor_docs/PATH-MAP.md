# Path map: `rob_refactor` → `main`

`rob_refactor` renamed the package `pipeline` → `lux_pipeline` and removed the
category level under `sources/`. **Both are adopted on `main` by item 00**, so
after it every `rob_refactor:lux_pipeline/...` path is also the path on
`main`, with the two exceptions below. All `main` paths in these documents
are post‑item‑00; `git show main:pipeline/...` references name files as they
are on today's `main` (before item 00).

## Sources

| `rob_refactor` (`lux_pipeline/sources/…`) | `main` after item 00 | `main` today | Note |
|---|---|---|---|
| `viaf/` | `oclc/` | `pipeline/sources/authorities/oclc/` | **directory name differs**: `main` keeps `oclc`; class names (`ViafLoader`, `ViafFetcher`, …) are the same |
| `wikipedia/` | *(none)* | — | new source on `rob_refactor`; not ported (see SUPERSEDED) |
| `bne/`, `bnf/`, `dnb/`, `japan/` | same | `libraries/<name>/` | `main` has no `bne/loader.py`, `bnf/loader.py` (item 29) |
| `fast/`, `gbif/`, `getty/`, `homosaurus/`, `lc/`, `nomisma/` | same | `authorities/<name>/` | |
| `geonames/`, `orcid/`, `ror/`, `wikidata/`, `wikimedia/`, `wof/` | same | `general/<name>/` | `main` has no `ror/downloader.py` (item 30); `orcid/loader.py` on `main` is a rewrite, keep it |
| `snac/` | same | `archives/snac/` | |
| `lux/`, `pleiades/`, `yale/` | same | same | grouping directories kept |

`main`‑only sources (no `rob_refactor` counterpart; they just move up one
level in item 00b): `ethan/` (from `archives/`), `artic/`, `ashmol/`, `mds/`
(from `museums/`), `yale/arches/`, `lux/internal/`, `lux/qlever/mapper2.py`.

## Everything else

| `rob_refactor` | `main` | Status |
|---|---|---|
| `lux_pipeline/config.py` | `lux_pipeline/config.py` | item 04 |
| `lux_pipeline/cli/*` | `lux_pipeline/cli/*` (new directory) | items 05–10, 13, 15, 17, 19, 21, 22, 24, 26–28 |
| `lux_pipeline/process/base/_managable.py` | `lux_pipeline/progress.py` (new; `Sliced` mixin + `Progress` callable) | item 02 |
| `lux_pipeline/process/base/downloader.py` | `lux_pipeline/process/base/downloader.py` (new) | item 14 |
| `lux_pipeline/process/base/loader.py` | `lux_pipeline/process/base/loader.py` | item 12 |
| `lux_pipeline/process/base/harvester.py` | `lux_pipeline/process/base/harvester.py` | item 16 |
| `lux_pipeline/process/base/index_loader.py` | `lux_pipeline/process/base/index_loader.py` | item 18 |
| `lux_pipeline/process/base/mapper.py` | `lux_pipeline/process/base/mapper.py` | item 11 (hooks only) |
| `lux_pipeline/process/base/exporter.py` | *(none)* | not ported |
| `lux_pipeline/process/tasks/_task_ui_manager.py` | `lux_pipeline/process/tasks/_task_manager.py` (new) | item 07 |
| `lux_pipeline/process/tasks/*_task.py` | `lux_pipeline/process/tasks/*_task.py` (new directory) | items 13, 15, 17, 19, 21, 24, 26 |
| `lux_pipeline/cli/_rich.py` | `lux_pipeline/cli/_view.py` + `lux_pipeline/cli/watch.py` (new) | item 06 |
| `lux_pipeline/process/utils/date_utils.py` | `lux_pipeline/process/utils/mapper_utils.py` | not ported (rename only; `main` newer) |
| `lux_pipeline/storage/cache/abstract.py` | *(none)* | item 38 (optional) |
| `lux_pipeline/storage/cache/github.py` | *(none)* | not ported |
| `lux_pipeline/storage/index/lmdb.py` | `lux_pipeline/storage/idmap/lmdb.py` | not ported (`main` uses raw `lmdb`) |
| `old/*` | repo root `run-*.py`, `*.sh`, `manage-data.py` | item 33 moves `main`'s to `scripts/legacy/` |
| `setup.py` | `pyproject.toml` (new) | item 05 |
| `install.md` | `docs/install.md` | item 32 |
| `tests/test-loader.py`, `tests/loader_tests/*` | `tests/test_loader_dsl.py`, `tests/loader_tests/*` | item 12 |

## Config records

| `rob_refactor` expects | `main` has | Handling (item 04) |
|---|---|---|
| `system.json` (`"type": "system"`) | `base.json` (`"type": "base"`) | read `system` if present, else `base` |
| `{"type":"default","name":"cache"\|"map"\|"ml"}` | `caches.json` (`"type":"caches"`) | `defaults["cache"]` falls back to `caches` |
| `{"type":"config", …}` sub‑config stores | — | item 35 |
| `indexes_dir`, `temp_dir`, `max_workers` in base | `indexes_dir` exists; `temp_dir` exists; no `max_workers` | defaults added |
| per‑source `downloaderClass`, `input_files`, `dumps_dir`, `dumpFileType` | — | consumed by items 12, 14; all optional |
| per‑source `indexes: {name: {indexClass, path, type}}` | `reconcileDbPath`, `inverseEquivDbPath` | item 34; `main`'s keys stay primary |
