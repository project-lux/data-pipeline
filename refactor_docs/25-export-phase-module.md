# 25. Export phase as a module

**Priority:** P1 · **Size:** M · **Depends on:** 01 · **Unblocks:** 26, 31
**Read from:** `main:run-export.py` (≈180 lines), `main:run-source-build.py`, `main:manage-data.py --nt` section (lines 254–304)

## Intent

Move `run-export.py` into `lux_pipeline/process/phases/export.py`:

```python
def export_slice(cfgs, my_slice, max_slice, *, no_ml=False, gz=False, chunk=1000, gzip_level=9,
                 out_dir=None, progress=None, write_flag=True) -> dict
```

keeping the chunked `get_multi` freshness check, `defer_commits(every=500)`
/ `checkpoint()`, raw reads, and `PhaseTimer`. Also expose the two other
exporters that exist as scripts so item 26 can offer them as `--type`s:

```python
def export_sources_slice(cfgs, my_slice, max_slice, sources=("aat","ulan","lcsh","tgn"), out_dir=None, progress=None)   # run-source-build.py body: JSONL + NT via QleverMapper(mapper2)
def export_nt_slice(cfgs, my_slice, max_slice, out_dir=None, progress=None)   # manage-data.py --nt body
```

## Files

| Action | Path on `main` |
|---|---|
| Create | `lux_pipeline/process/phases/export.py` |
| Edit | `run-export.py` → thin wrapper (`0 24`, `--gz`, `--no-ml`, env `LUX_EXPORT_CHUNK`/`LUX_EXPORT_GZIP`; `--profile` removed from argv and ignored, see item 41) |
| Edit | `run-source-build.py` → thin wrapper |
| Create | `tests/test_phase_export.py` |

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

## Acceptance criteria

* `python run-export.py 0 1 --no-ml` produces the same file as before.
* Tests pass.
