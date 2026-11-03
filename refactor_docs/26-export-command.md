# 26. `ExportManager` and `lux export`

**Priority:** P1 · **Size:** M · **Depends on:** 07, 11, 25 · **Unblocks:** 31
**Read from:** `rob_refactor:lux_pipeline/process/tasks/export_task.py` (175 lines) **for ideas only — it never ran**; `rob_refactor:lux_pipeline/cli/export.py`

## Intent

```
lux export --max-workers 24                      # == run-export.py 0..23 24  (marklogic documents, JSONL)
lux export --max-workers 24 --gz
lux export --type merged                         # == --no-ml
lux export --type sources --max-workers 24       # == run-source-build.py (JSONL + NT for aat/ulan/lcsh/tgn)
lux export --type nt --max-workers 24            # == manage-data.py --nt
lux export --type external-zip --source aat      # zip of the datacache records that the recordcache uses (the refactor's idea; feeds `lux load --type export`)
```

Default source is `merged` (results group); item 07 made `prepare_single`
accept results.

## Files

| Action | Path on `main` | Source |
|---|---|---|
| Create | `lux_pipeline/process/tasks/export_task.py` | new, calling `phases.export` |
| Create | `lux_pipeline/cli/export.py` | `rob_refactor:lux_pipeline/cli/export.py` reshaped |
| Create | `tests/test_export_task.py` | new |

## What to port

From the refactor: the `--type` dispatch idea, `export_external_used`
(zip of used external records — rewrite it against `main`'s cache API:
`recordcache.iter_keys()`, `split_qua`, `datacache[ident]`, `zipfile` with
`ZIP_BZIP2`; this is the counterpart of `main`'s `Loader.load_export`),
and `mapper.make_export_filename` (item 11).

## Known bugs in the source — do not copy

* `from config import importObject` (wrong module), `json`/`zipfile` used
  without import, `self.results` (should be `self.configs.results`), `ttl`
  undefined in `export_external_used`, `prepare_single` looks up results
  in `self.configs.external`. Treat the file as a sketch.
* It transforms with `mapper.transform(rec, type)` per record with a
  presence check only; `main`'s freshness check (`insert_time >=`) and
  `get_multi` chunking are required.

## From item 25

The three phase functions are `export_slice(cfgs, my_slice, max_slice, *,
no_ml, gz, chunk_size, gzip_level, out_dir, progress, write_flag)`,
`export_sources_slice(cfgs, my_slice, max_slice, sources, out_dir, nt_dir,
gzip_level, progress)` and `export_nt_slice(cfgs, my_slice, max_slice,
out_dir, gzip_level, progress)`; each returns `{"records": <PhaseTimer
summary>, "written": n, "path"|"paths": ...}`. Their output directories
default to `cfgs.exports_dir`, but on the build machine the N‑Triples have
always gone to `/data-export/output/lux/nt/`, a sibling of `exports_dir`
(`.../latest`), and the legacy wrappers still pass that path. `--type nt`
and `--type sources` need a home for the NT files: an optional base key
(`nt_dir`, say, falling back to `exports_dir`) added to the config copies
and the pipeline‑configs repo, or `--out-dir` on the command. Decide here.

## Steps

1. Manager: `_distributed(self, n, cfgs, progress)` dispatches on
   `self.export_type` to the three phase functions or `export_external_used`,
   passing `progress` through.
2. CLI: `--type marklogic|merged|sources|nt|external-zip`, `--gz`,
   `--cache`, `--chunk`.
3. Tests: dispatch per type with patched phase functions; `external-zip`
   against in‑memory caches writes a zip with one member per identifier.

## Acceptance criteria

* `lux export --max-workers 2 --no-ui` writes `export_full_0.jsonl`, `export_full_1.jsonl` identical in content to `run-export.py 0 2` / `1 2`.
