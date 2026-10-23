# 12. Loader micro‑DSL: `lux_pipeline/process/base/loader.py`

**Priority:** P1 · **Size:** L · **Depends on:** 02, 04, 11 · **Unblocks:** 13, 29
**Read from:** `rob_claude_refactor:lux_pipeline/process/base/loader.py` (≈690 lines; has the `lmdb` container and `max_records`) · fixtures: `rob_refactor:tests/loader_tests/**` · stale test for the case list: `rob_refactor:tests/test-loader.py`

## Intent

Replace the body of `main`'s `Loader` with the declarative loader while
keeping the old `load()` entry points working for the sources that still
override them. A source config describes its dump files as a list of
`{path|url, type}` entries, where `type` is a small path language:

```
(container[.compression]/)*format[.compression]

containers: dir dirs pair zip tar lines dict array arraylines xmls lmdb
formats:    json raw other
compression: gz bz2 z
```

Examples that the fixtures cover: `json`, `json.gz`, `zip/json`,
`tar.gz/json`, `tar.gz/lines/json`, `lines.gz/json`, `dir/zip/lines/json`,
`array/raw`, `dict/raw`, `tar.gz/lines.gz/json`, `other` (XML), and
`lmdb/json.z` (an LMDB export, from `rob_claude_refactor`).

Loading walks the spec: each container step yields children (paths, archive
members, lines, JSON items) to the next step; the final format step builds
`{"identifier", "data"}` and stores it. Slicing (`seen % max_slice ==
my_slice`) happens at the format step, so N workers can read the same file.

## Why it matters

Third headline feature. Today every source has a 40–150 line `load()` with
its own gzip/zip handling, identifier regex, progress prints and slice loop,
and `manage-data.py --load` has one `if` per source. With the DSL a new
dump‑file source is a config entry plus (sometimes) a 5‑line
`extract_identifier`.

## Files

| Action | Path on `main` | Source |
|---|---|---|
| Edit (rewrite) | `lux_pipeline/process/base/loader.py` | `rob_claude_refactor:lux_pipeline/process/base/loader.py` |
| Create | `tests/loader_tests/**` (fixtures) | `git checkout rob_refactor -- tests/loader_tests` |
| Create | `tests/test_loader_dsl.py` | new; case list from `rob_refactor:tests/test-loader.py` |

## What to port

The entire `rob_claude_refactor` module (`Pointer`, `TarPointer`,
`ZipPointer`, `LmdbPointer`, the refactor's `Loader(Managable)` — rebased onto
`Sliced` as shown below — with `guess_fmt`,
`process_fmt`, the `iterate_*` generators, `make_*`, `prepare`, `process`),
**plus** a compatibility layer:

```python
from lux_pipeline.progress import Sliced, null_progress

class Loader(Sliced):
    # --- legacy API kept from main (verbatim) -------------------------------
    in_url, in_path          # set in __init__ from remoteDumpFile / dumpFilePath as today
    get_identifier_raw(line); get_identifier_json(js); filter_line(line)
    load(self, *a, **kw)     # main's default gzipped-JSONL loader (unchanged)
    load_export(); load_export2()
    # --- new DSL API --------------------------------------------------------
    prepare(my_slice=-1, max_slice=-1, load_type="records", max_records=-1, progress=null_progress)
    process(overwrite=True)
    should_process_item(child) -> bool   # hook (rob_claude_refactor): e.g. skip the .csv in the ROR zip
    uses_dsl -> bool         # True when prepare() found input_files or a dumpFileType
```

Progress: `self.progress(total=n)` once the count is known (the DSL's
`increment_total` accumulation becomes `self.progress(total=running_total)`),
`self.progress(advance=1)` in `store_record`, `self.progress(description=path)`
per file. All `self.manager.log(level, …)` calls become `logger.<level>(…)`;
there is no manager. The `disable_ui` parameter disappears (nothing in the
loader depends on it).

Rules for the compatibility layer:

* `__init__` sets **both** sets of attributes. `self.out_cache` is
  `config["datacache"]` (as on `main`), not `config.get("datacache", {})`.
* `post_process_json(self, data, identifier=None)`: `main` sources override
  `post_process_json(js)` with one argument and wikidata overrides with two.
  Call it via a small shim that inspects the signature once
  (`inspect.signature`) and caches whether to pass the identifier.
* `prepare()` without `input_files` and without `dumpFileType` leaves
  `my_files` empty and sets `uses_dsl = False`; the load manager (item 13)
  then calls `load(my_slice, max_slice)` if the loader accepts slices, else
  `load()`.
* `dumps_dir`: `config["all_configs"].dumps_dir`, joined with the optional
  per‑source `config["dumps_dir"]`; a bare filename in `input_files[*].path`
  is resolved against it; `dumpFilePath` is already absolute (config
  `path_types`).

## What to keep from `main`

* `load()`, `load_export()`, `load_export2()` bodies exactly, including
  `defer_commits(every=1000)` / `checkpoint()` / `flush()` in `load_export2`.
* Add the same deferral to `process()`: call `self.out_cache.defer_commits(every=1000)`
  before the loop and `checkpoint()` after each `store_record`, with
  `resume_commits()` in a `finally`. (The refactor commits once at the end,
  which is wrong for a 50M‑record load.) Guard each with `hasattr` so a
  non‑Postgres cache works.

## Known bugs in the source — do not copy

| Where | Bug | Fix |
|---|---|---|
| `iterate_directories` | recursive call result is discarded | `yield from self.iterate_directories(full, comp, remaining)` |
| `iterate_tar` | `th.namelist()` does not exist on `TarFile` | `th.getnames()`; or skip the count for tars (reading the whole index of a 100 GB tar is slow) |
| `post_store_record` | `self.temp_file_handle[f]` (singular) | `self.temp_file_handles[f]` |
| `open_temp_files` | `self.configs_temp_dir` | `self.configs.temp_dir` |
| `guess_fmt` | `.bz2` sets `compression = "bz"` but the accepted token is `"bz2"` | use `"bz2"` |
| `file_opener` | `open(path)` opens text mode; `json.load` then gets `str` for `.json` but `lines` yields `bytes` for gz — inconsistent | open binary (`"rb"`) everywhere and decode in `iterate_lines`/`make_other` |
| `process_step` | `max_records` check returns from the *recursion*, not the whole load | raise a private `_StopLoad` exception caught in `process()` |
| `prepare` | `return False` after logging a missing `load_type` leaves `is_prepared=True` from `super().prepare()` | set `self.my_files = []` and return normally |
| `lmdb_count_entries` / `iterate_lmdb` | open the env twice; `lock=False` | open once in `prepare`, reuse |
| `should_store_record` | checks `data['data'] is None` before `data is None` guard order is fine but `identifier in self.out_cache` is a DB round trip per record when `overwrite=False` | keep, document the cost |

## Steps

1. `git checkout rob_refactor -- tests/loader_tests` (fixture files only).
2. Write the new `loader.py` as described; keep `main`'s three legacy
   methods at the bottom under a `# legacy` comment.
3. `tests/test_loader_dsl.py`: for every case in the stale
   `test-loader.py` list (17 single‑record cases + the 20‑line multi file),
   build `Loader({"name":…, "all_configs": StubConfigs(dumps_dir=…), "datacache": {}, "input_files": {"records": [{"path":…, "type":…}]}})`,
   call `prepare(0, 1, progress=recorder)` then `process()` and assert
   `len(out_cache)`. Add: `process_fmt` rejects a bad container/compression;
   slicing with `max_slice=5, my_slice=0` on the 20‑line file stores 4
   records including `"5"`; `max_records=3` stops at 3; `uses_dsl` is False
   for a config with only `dumpFilePath`.
   `StubConfigs` needs `dumps_dir`, `temp_dir`; no DB. Assert `recorder` saw
   `total` and the right number of `advance` calls.
4. Run the existing loader‑based tests on `main` (`tests/test_cache_raw_and_fresh.py` etc. do not touch the loader; `grep -l "Loader" tests/`).
5. Smoke on the live instance with a source that still uses `load()`:
   `python manage-data.py --load --ror` must behave exactly as before.

## Acceptance criteria

* All DSL fixture cases pass; legacy `load()` paths untouched.
* `python -m pytest tests/ -q` passes.
* No source file under `lux_pipeline/sources` is modified in this PR.
