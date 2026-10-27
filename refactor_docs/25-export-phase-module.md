# 25. Export phase as a module

**Priority:** P1 · **Size:** M · **Depends on:** 01 · **Unblocks:** 26, 31
**Read from:** `main:run-export.py` (185 lines), `main:run-source-build.py` (80 lines), `main:manage-data.py --nt` section (lines 255–302)

## Intent

Move `run-export.py` into `lux_pipeline/process/phases/export.py`:

```python
def export_slice(cfgs, my_slice, max_slice, *, no_ml=False, gz=False, chunk_size=CHUNK, gzip_level=GZIP_LEVEL,
                 out_dir=None, progress=null_progress, write_flag=True) -> dict
```

keeping the chunked `get_multi` freshness check, `defer_commits(every=500)`
/ `checkpoint()`, raw reads, and `PhaseTimer`. Also expose the two other
exporters that exist as scripts so item 26 can offer them as `--type`s:

```python
def export_sources_slice(cfgs, my_slice, max_slice, sources=SOURCES, out_dir=None, nt_dir=None,
                         gzip_level=1, progress=null_progress) -> dict   # run-source-build.py body: JSONL + NT via QleverMapper(mapper2)
def export_nt_slice(cfgs, my_slice, max_slice, out_dir=None, gzip_level=1, progress=null_progress) -> dict   # manage-data.py --nt body
```

## Files

| Action | Path on `main` |
|---|---|
| Create | `lux_pipeline/process/phases/export.py` |
| Edit | `run-export.py` → thin wrapper (`0 24`, `--gz`, `--no-ml`, env `LUX_EXPORT_CHUNK`/`LUX_EXPORT_GZIP`; `--profile` removed from argv and ignored, see item 41) |
| Edit | `run-source-build.py` → thin wrapper |
| Edit | `manage-data.py` → the `--nt` branch calls `export_nt_slice` |
| Create | `tests/test_phase_export.py`, `tests/test_run_export_script.py`, `tests/test_run_source_build_script.py` |

## What to do

1. Move code, parameterise, `print` → `logger`, add the `progress` hook as
   in item 20 (`PhaseTimer(progress=…)` covers `timer.step(len(chunk))`;
   add `progress(total=merged.len_estimate() // max_slice)` at the start).
2. Hard‑coded paths in `run-source-build.py` (`/data-export/output/lux/...`)
   become `out_dir` defaulting to `cfgs.exports_dir`; keep the file names.
3. The export flag file: `run-export.py` already writes
   `<log_dir>/flags/export_is_done-<n>.txt` at the end (lines 180–183);
   it moves into `export_slice` behind `write_flag`.
4. Test `export_slice` with stub caches: `iter_records_slice(raw=True)`
   yields two rows; `get_multi` returns one fresh row; assert the fresh row
   is written as‑is and the stale one is transformed (stub mapper) and
   stored back.

## Decisions (2026‑10‑27, before implementation)

* **`export_slice` verbatim.** The `--profile` block is gone (item 41);
  `CHUNK` / `GZIP_LEVEL` / `NO_ML` / `GZ` become `chunk_size` / `gzip_level`
  / `no_ml` / `gz` (`chunk_size`, not the sketch's `chunk`, because the loop
  variable is `chunk`); `print` → `logger`; `progress` into `PhaseTimer`
  and `progress(total=len_estimate() // max(max_slice, 1))`, 0 for a cache
  without the estimate. Phase name `export`, so `timing-export-<n>.json`
  keeps its name. The flag file is written whenever `write_flag`, for an
  unsliced run too (`export_is_done-0.txt`), as the script did. `os.mkdir`
  of the exports directory becomes `makedirs(exist_ok=True)`.
* **One line inside a stage block.** The mapper‑error message
  `print(f"{yuid} errored in marklogic mapper: {e}")` sits inside
  `timer.stage("transform")`. It becomes `logger.warning`: a log call, no
  change to control flow, and the only line touched inside a stage.
* **Output directories.** Today the NT exporters write to
  `/data-export/output/lux/nt/` and the sources JSONL to
  `/data-export/output/lux/latest/`, which is `exports_dir`. The phase
  functions default to `cfgs.exports_dir` (`export_sources_slice` takes
  `out_dir` for the JSONL and `nt_dir` for the NT, defaulting to `out_dir`);
  the legacy wrappers pass today's hard‑coded paths explicitly so nothing
  moves. No new config key here; item 26 adds one if `lux export --type nt`
  needs it.
* **`manage-data.py --nt`** calls `export_nt_slice` with the old path, so
  there is one copy of the loop. Its `logging.basicConfig` gains
  `stream=sys.stdout`, as the other wrappers.
* **Known bugs in `run-source-build.py`, not copied.** `start` was never
  defined in any commit, so the script raised `NameError` on its 100,000th
  record; and an unsliced run called `iter_records_slice(-1, -1)`, which
  raises before reading anything. The module iterates `iter_records()` when
  unsliced, as the `--nt` branch did, and `timer.step()` replaces the
  `x % 100000` progress print.
* **`PhaseTimer` for the two new functions** (`export-sources`,
  `export-nt`): progress, a `mark` per source, and
  `timing-export-sources-<n>.json` / `timing-export-nt-<n>.json`. Both get
  the standard tidy‑up (`resume_commits` after each source's `flush` for
  the sources recordcache, `put_all("localsocket")`), which neither script
  did. No flag file for them, as today.
* **Everything else in the two scripts verbatim:** memory‑cache switching by
  slice, `defer_commits(every=1000)` / `checkpoint` / `flush` per source,
  the qua'd identifier stored back to the source recordcache, JSONL written
  (with `ujson`'s default escaped slashes, unlike `export_slice`) before the
  NT attempt, qlever failures logged and skipped, gzip level 1 as a keyword,
  file names unchanged (`sources_<n>.jsonl.gz`, `sources_<n>.nt.gz`,
  `lux_<n>.nt.gz`, with `-1` when unsliced).
* **Return values:** `{"records": <summary>, "written": n, "path": fn}`,
  `paths` (JSONL, NT) for the sources pair. `written` leaves out the
  records a mapper could not transform; the summary's `records` counts
  what the timer stepped.
* **Wrapper logging** to stdout, as items 20 and 23 (`export_parallel.sh`
  and `nt_parallel.sh` redirect stdout only). `run-export.py` reads its
  slice arguments only when both are numeric, as the other wrappers, so
  `--no-ml --gz` alone no longer raises.
* **Tests:** `tests/test_phase_export.py` (every freshness branch, the
  chunked lookups and checkpoints, `--no-ml`, `--gz`, the mapper error, the
  file names, the flag, progress, the return value; the two other functions'
  record paths, sliced and unsliced iteration, the rejected record, the
  qlever failure, the commit calls); `tests/test_run_export_script.py` and
  `tests/test_run_source_build_script.py` (argv and environment).
  `manage-data.py` cannot be loaded as a module without running everything,
  so its edit is checked by reading.

## Acceptance criteria

* `diff <(git show main:run-export.py) <(sed 's/^    //' lux_pipeline/process/phases/export.py)` shows only moved code, parameterisation and print→logger; the one hunk inside a `timer.stage` block is the mapper‑error `print` → `logger.warning`.
* `python run-export.py 0 1 --no-ml` produces the same file as before.
* Tests pass.
