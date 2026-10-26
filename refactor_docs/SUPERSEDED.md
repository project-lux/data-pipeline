# Superseded: do not port these

Everything below exists on `rob_refactor` and must **not** be ported to
`main`: either `main` has moved further in a direction the refactor does not
know about, or the refactor's version never worked. Nothing in this file is
scheduled by `00-INDEX.md`; if an index item tells you to port something
listed here, the index wins and this file is out of date — say so in the PR.
Diff sizes are lines added/removed going from `main` to `rob_refactor`, so a
large "removed" number means `main` has that much more code.

## Structural

| What | Why not |
|---|---|
| `old/` directory holding the legacy scripts | `main`'s scripts are still the production path. Item 33 moves them to `scripts/legacy/` only after the CLI covers every phase. |
| `.DS_Store` committed under `lux_pipeline/` | Noise. |
| `tests/test-loader.py` | Stale: imports `lux_pipeline.config`, calls `prepare_load()`/`load()` which the DSL loader does not have. Item 12 rewrites it as a pytest module; keep only the fixture files under `tests/loader_tests/`. |

## Storage layer (`main` is far ahead)

| File on `rob_refactor` | Diff vs `main` | Why not |
|---|---|---|
| `storage/cache/postgres.py` | +89/−921 | `main` added `PoolManager` pending/deferred commits (`defer_commits`, `checkpoint`, `resume_commits`, `flush`), `get_multi`, `has_multi`, `get_fresh`, raw reads, type‑sliced iterators, maintenance (`optimize`, `bloat`, `rewrite`, `drop_time_index`), deadlock retry. Every phase on `main` depends on these. |
| `storage/idmap/redis.py` | +153/−590 | `main` grew `get_cluster`, memory‑cache controls, token handling, and then moved production to `storage/idmap/postgres.py` (see `docs/idmap-migration.md`). The live config uses the postgres store. |
| `storage/idmap/memory.py`, `filesystem.py` | small | `main` versions match the newer interface. |
| `storage/index/lmdb.py` (`LmdbIndex` over `lmdbm`) | new file | `main` deliberately dropped `lmdbm` for raw `lmdb` (`storage/idmap/lmdb.py`: one read txn per instance, cursor `__contains__`, `clear()` as `MDB_DROP`, chunked sorted `update()`). The refactor's wrapper re‑introduces per‑call transactions. Its `open("r"/"w")`/`close()` context‑manager API is not needed. |
| `storage/cache/github.py` | new file | Marked "toy status" in its own header (1000‑files‑per‑dir limit, rate limiter). Only used by the sub‑config store (item 35), which uses `FsCache` instead. |
| `storage/cache/filesystem.py`, `redis.py`, `marklogic/rest.py` | `main` newer | keep `main`. |
| `requirements.txt`: `lmdbm` instead of `lmdb` | | `main` needs `lmdb`. Item 05 adds `rich` and the `ray` extra; nothing else changes. |

## Process layer (`main` is ahead; the refactor only swapped `print` for `logger`)

| File | Diff vs `main` | What `main` has that the refactor lacks |
|---|---|---|
| `process/reference_manager.py` | +168/−357 | batched reference claiming (`_claim_size`, `_wait_for_refs`), `resolve_refs`/`collect_ref`, sorted metatypes, `workers` arg. The refactor's `merge_metatypes()` and `get_len_refs()` helpers are re‑created in item 21/22, not copied. |
| `process/reidentifier.py` | +30/−219 | `prefetch`, `_cap_equivs`, key collection, equivalence capping for oversized clusters. |
| `process/merger.py` | +149/−253 | `_fetch_members`, `_data_size` guards, pre‑merge fixes for massive clusters. |
| `process/merge_batch.py`, `identity_resolver.py`, `timing.py`, `storage/uricache.py` | `main`‑only | The refactor predates all of them. The reconcile/merge managers (items 21, 24) must call these, which is why items 20/23/25 extract `main`'s loop bodies first. |
| `process/reconciler.py` | +39/−40 | `main` has the `all_seen` termination fix and deterministic differentFrom handling in `call_reconcilers`. Take only the logging. |
| `process/collector.py` | +21/−19 | logging only; item 01 covers it. |
| `process/update_manager.py` | +29/−113 | `main` has `_record_pending_delete` / `process_pending_deletes` and `harvest_pages`. Item 16 keeps them. |
| `process/base/acquirer.py` | +33/−60 | `main` has `data=` row pass‑through, combined cache‑hit query, `timing.stage` instrumentation, traceback location in the mapper error. Take only the logging. |
| `process/base/fetcher.py` | +9/−9 | the refactor *removed* the `use_networkmap` guard (writes to a possibly‑None map). Take only the logging. |
| `process/base/reconciler.py` | +59/−102 | `main` has loud index‑open failures with retry, single‑lookup `get()`, deterministic multi‑match ordering, and skips a half with no index. The refactor's `config["indexes"]` lookup is item 34. Take only the logging. |
| `process/base/mapper.py` | +35/−30 | `main` fixed the century regex (`group(2)` with diagnostics); the refactor has the older `group(1)`. Item 11 takes only the two hook methods and logging. |
| `process/utils/date_utils.py` (renamed from `mapper_utils.py`) | +57/−143 | `main`'s `mapper_utils.py` has the dateparser language list (14 ms → 0.37 ms per call) and the stricter BC regex. Do not rename the module. |
| `process/base/exporter.py` | new | Unused sketch (`Exporter`, `CacheExporter`); nothing calls it. |
| `process/validator.py`, `storage/marklogic/*` | identical | nothing to do. |

## Sources (`main` is newer for every mapper)

Every `sources/*/mapper.py` on `main` is at least as new as the refactor's,
and most are much newer (`lc/mapper.py` +76/−217, `lux/final/mapper.py`
+52/−304, `lux/qlever/mapper.py` +433/−667 plus the new `mapper2.py`,
`wof/mapper.py` +78/−110, `yale/library/mapper.py` +40/−78). The large
"vs base" numbers for `dnb`, `nomisma`, `bnf`, `yuag`, `japan`, `bne`,
`snac` mappers are a `black` reformat on the refactor, not logic. Do not
port any mapper.

Specific files:

| File | Why not |
|---|---|
| `sources/viaf/*` as a directory name | `main`'s directory is `oclc/` (after item 00b); contents are the same classes. Item 30 adds the downloader there. Do not rename the directory. |
| `sources/wikidata/loader.py`, `oclc/loader.py`, `orcid/loader.py`, `geonames/loader.py`, `lc/loader.py`, `yale/*/loader.py`, `ror/loader.py` on `rob_refactor` | Still old‑style `load()` loaders that read `self.in_path`, which the refactor's base `Loader` no longer sets — they are broken there. `main`'s versions work. Item 12 keeps `load()` support so they keep working; item 29 moves only the simple ones onto the DSL. |
| `sources/wikipedia/*` | New source with no mapper logic of note (`extract_identifier` returns `title`) and no config on the live instance. Add later as its own feature if wanted. |
| `sources/lux/final/index_loader.py`, `lux/final/reconciler.py` | Depend on the `indexes` config block (item 34). `main`'s versions use the path‑based config and the raw lmdb store. |
| `sources/lc/index_loader.py`, `wikidata/index_loader.py` | `main`'s inherit `LmdbIndexLoader` and are newer (`wd_equivs` sort). Item 18 keeps them working. |

## UI plumbing replaced by the file‑based design (items 02, 06, 07)

| What | Why not |
|---|---|
| `process/base/_managable.py` as a base class with a `manager` back‑pointer, `divide_by_max_slice`, `update_progress_bar(total, increment_total, desc)` | Components get a `progress=` callable instead (item 02). Four‑hop progress chain, untestable without a manager. |
| `TaskUiManager.bar`, `.log()`, `.update_progress_bar()`, `debug_process()` | Progress goes to a `StatusWriter`; logging goes to the logger. `run_single()` replaces `debug_process()`. |
| `MpEngine` Manager‑dict proxies, `temp_bar`, `maybe_update_progress_bar`; `RayEngine.ProgressActor`; `LoggingActor`/`TaskLogHandler` (`rob_refactor`) | One transport (status files) for every engine and for shell‑started interpreters. |
| `_rich.get_layout` `Layout` rows, `layout._lux_bars`, `get_bar_from_layout`, bars with `total=10000`, `ConsolePanel`/`LuxHandler` | One `rich.progress.Progress` with a task per worker inside `BuildView`; the UI reads files. `LogPanel`, `PanelLogHandler`, `LogTailer` **are** kept. |
| `process(layout, engine=…, disable_ui=…)` signature and every `if layout is not None` in the task modules | Managers take an optional `view`; task modules never see the UI. |

## CLI pieces not worth porting as‑is

| File | Why not |
|---|---|
| `cli/initialize.py` GitHub clone of `project-lux/pipeline-configs` | `main` ships `docs/sample_config`; the live config is a local directory. Item 28 initialises from `docs/sample_config` and drops the `PyGithub` dependency. |
| `cli/export.py` / `tasks/export_task.py` as written | The task has undefined names (`json`, `zipfile`, `self.results`, `ttl`) and a bad import (`from config import importObject`); it never ran. Item 26 implements the manager against `main`'s `run-export.py` logic. |
| `tasks/reconcile_task.py` per‑record logic | Calls `ref_mgr.manage_identifiers()` (in‑flight identity) which `main` replaced with assertion logging + `run-identify.py`. Item 21 wraps `main`'s loop. |
| `tasks/merge_task.py` per‑record logic | Predates `claim_member`, `prefetched`, `--resume`, `CLUSTER-ESCAPE` checks. Item 24 wraps `main`'s loop. |
| `entry.py` default `--engine ray` | Ray is heavy and optional on `main`; default becomes `mp`. |
