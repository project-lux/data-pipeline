# `rob_claude_refactor` fixes — where each one lands

`rob_claude_refactor` is `rob_refactor` plus 11 commits (2026‑06‑24):
*fixes for log livelocking*, *readline and config list*, *Make it prettier*,
*fix loading, add lmdb distribution option*, *improve counts, list; fix
cache.get()*, *Many fixes*, *Fix ROR style loaders*, *dynamic task bar
height*, *index loading*, two *update*s. Every file it changed is listed
here with the document that carries the fix, so none is lost in the port.
"Design superseded" means the problem it fixed no longer exists under the
file‑based progress design of items 02/06/07.

| File on `rob_claude_refactor` | What the fix is | Carried by |
|---|---|---|
| `process/tasks/_task_ui_manager.py` | logs via per‑worker files + `LogTailer` instead of IPC (`LoggingActor`/`TaskLogHandler` removed); `make_file_handler` filters at `--log` level in the worker; final bar state flushed after the throttle; `ProgressActor.get_progress_bar(-1)` one round trip | 06 (`LogTailer`, handler), 07 (`attach_worker_log_handler`, worker wrapper), 02 (`flush()` ≙ "send throttle drops final state"). Progress actor / Manager dict: design superseded by `StatusWriter` |
| `cli/_rich.py` | `LogPanel` (bounded deque, locked) + `PanelLogHandler` replace recording `Console`/`RichHandler`; `add_worker_log_line` with `w<n>` prefix; progress panel height `max_workers + 3`; level clamp | 06. Height fix: design superseded (single `Progress`, Rich sizes it) |
| `cli/_handler.py` | always build the sub‑parser (`rest` may be empty with defaults); per‑worker log file prefix `<log_dir>/<command>-<timestamp>`; `manager.log_level = args.log` | 05, 07 |
| `cli/entry.py` | `--max-records` global option | 05 |
| `cli/interactive.py` | `gnureadline`, history in `<base_dir>/.python_history`, `atexit` write, `rlcompleter` import order | 27 |
| `cli/list.py` (new) | the `lux list` command | 10 |
| `cli/load.py`, `cli/load-index.py`, `cli/reconcile.py` | option parsing moved into the managers' `__init__` (`args`), `recid` passed through `extra_args`, `--type`/`--no-overwrite` on `load-index` | 07 (rule), 13, 19, 21 |
| `process/tasks/load_task.py` | reads `args.type` (now incl. `lmdb`) and `args.max_records`; passes `max_records` to `prepare()`; drops `ldr.local_debug` | 13 |
| `process/tasks/index_load_task.py` | `__init__` sets `overwrite`/`load_type` from `args` (was undefined attribute) | 19 |
| `process/tasks/reconcile_task.py` | `--recid` list reaches `maybe_add`; `temp_log_{n}.txt` debugging litter removed; `logger.critical` on worker exception | 21 (the per‑record loop itself is `main`'s, see 20) |
| `config.py` | `max_workers` default `cpu_count() - 3` (was `2/3`); formatting | 04 |
| `process/base/loader.py` | `lmdb` container + `LmdbPointer` + `z` compression (reads the `make_*_lmdb.py` distribution stores); `make_json` accepts an already‑parsed dict; `post_process_json(data, identifier)` signature; `should_store_record` `None` guards; `max_records`; `should_process_item(child)` hook; `prepare(…, max_records)`; `lmdb_count_entries` for totals; `logger.debug` instead of `print` in `process_step` | 12 (all), 13 (`max_records`) |
| `process/base/index_loader.py` | `indexes` from config; `prepare()` refuses `max_slice > 1` (LMDB single writer); `self.disable_ui` | 18 (single‑writer rule), 34 (`indexes` block) |
| `process/base/acquirer.py`, `process/collector.py`, `process/reconciler.py`, `process/base/reconciler.py`, `sources/{viaf,wikidata,lux/final}/reconciler.py` | `self.debug`/`debug_graph` removed; always‑on `logger.debug` with greppable `RECONCILE: <a> EQUIV/… <b>` / `REMOVE/…` lines; `Reconciler.reconcile` raises `NotImplementedError`; dead `uniquePerRecord` branch and the VIAF/WD "truth" checks deleted; WD `ext_hash` prefixes trimmed | **Review, do not auto‑port.** Logic files are `main`'s (SUPERSEDED). The structured `RECONCILE:` debug lines are a good idea: item 36 offers them as the alternative to `debug_graph` for `lux viz`; item 37 may adopt the message format. The WD `ext_hash`/truth changes are data decisions for the owner of `wikidata/reconciler.py` on `main` |
| `sources/lc/index_loader.py` | `prefLabel` may be a plain string, not only `{"@value"}` | **Apply to `main:pipeline/sources/authorities/lc/index_loader.py`** if it still only handles dicts (check `extract_names`); 3 lines, any PR |
| `sources/lc/loader.py` | DSL conversion: `prepare()` reads `externallinks.nt.zip` from `dumps_dir/lc/`, `make_identifier` from `Pointer`, `extract_identifier` from `@id`/`@context.about`, `post_process_json(data, ident)` drops `-781` and undifferentiated records | 29 (listed as a candidate; `main`'s `lc/loader.py` has newer sync logic — compare before converting) |
| `sources/lc/mapper.py`, `sources/wikidata/fetcher.py` (`post_process` idempotence guard), `sources/wof/*` | mapper/fetcher edits | SUPERSEDED (`main` newer). The `post_process` "already processed, skip" guard: **check `main:pipeline/sources/general/wikidata/fetcher.py`**; if absent, add the 2‑line guard in any PR |
| `sources/ror/loader.py`, `sources/ror/downloader.py` | loader reduced to `should_process_item` (skip the `.csv` in the v2 zip); downloader `get_urls(type=…)` + Zenodo comment | 29, 30 |
| `sources/yale/{ycba,ypm,yuag,pmc,ipch}/loader.py` | reduced to DSL subclasses (`make_identifier` strips the S3/namespace prefix; `pass` for ipch) | 29 — moves these from "leave" to "convert from `rob_claude_refactor`" once their configs gain `input_files` |
| `sources/bnf/loader.py` | `make_identifier` from `Pointer`; `extract_identifier` guard for empty data | 29 |
| `sources/wikidata/index_loader.py` | `WdFileIndexLoader` body deleted (`pass`) | **Do not port** — `main`'s version (sorted `wd_equivs_*` load) is the working one |
| `storage/cache/abstract.py`, `storage/cache/postgres.py` (`PooledCache(AbstractCache)`, `_validate_key_type`, `_prepare_write_params`, `_build_insert_parts`) | cache interface cleanup; "fix cache.get()" | 38 (optional). `main`'s `postgres.py` is the production class and is not replaced |
| `storage/cache/lmdb.py` (new stub) | read‑only LMDB distribution cache | **Do not port** (stub; `env` undefined, iterators unimplemented). The read side is covered by the loader's `lmdb` container (12) |
| `storage/index/lmdb.py` | `LmdbIndex.clear()` via `remove_lmdbm` + re‑`initialize` | 09 uses `main`'s `remove_lmdb`; the wrapper itself is SUPERSEDED |
| `sources/lux/final/reconciler.py` | commented‑out `idx.open()` | 34 (only relevant with the `indexes` block) |
| `pyrightconfig.json` (new) | relaxed Pyright settings | copy as‑is in item 05 if the team uses Pyright; harmless otherwise |
