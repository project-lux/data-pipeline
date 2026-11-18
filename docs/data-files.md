# Files the pipeline reads outside Postgres

An audit (2026-11-18) of every file a build reads that is not in Postgres. It
leaves out the source dumps themselves, which `lux download` / `lux load`
manage, and the configuration. Paths are relative to the configured
directories: `data_dir` (`<base>/files` in the live config), `indexes_dir`
(`<base>/indexes`) and `temp_dir`. Code references are to `lux_pipeline/`.

Each file has one of three origins:

* **Generated**: a `lux` command or a step of the build writes it.
* **Fetched**: a `lux` command downloads it, ready-made, from elsewhere.
* **Pre-built outside**: nothing in the pipeline makes it. It exists only
  because someone made it by hand, with an experiment script or an LLM run,
  or copied it in. It cannot be rebuilt from the pipeline, and a backup is
  the only copy.

## 1. Files for mappers

Read when a mapper is constructed. Every one of these except
`metatypes.json` is skipped silently if missing, so a fresh install maps
differently with no warning.

| File (in `data_dir`) | Read at | Origin |
|---|---|---|
| `type_overrides.json` | `process/base/mapper.py:128` (every mapper) | **Pre-built outside**: no writer anywhere |
| `xpath_fixes.json` | `process/base/mapper.py:144` (every mapper) | **Generated** by `lux sheets` (`process/sheets.py:168`) from the "Fixes" tab of the Google Sheet. The sheet itself is curated by hand. |
| `<source>_cycle_breaks.json` for aat, bne, bnf, dnb, geonames, lcnaf, lcsh, tgn, wikidata, wof (the source's `cycleBreakPath`, which `path_types` resolves under `data_dir`) | `process/base/mapper.py:121` | **Pre-built outside**: no writer |
| `metatypes.json` | `sources/lux/final/mapper.py:32` (merge); checked by `process/tasks/merge_task.py:119` | **Generated** by `lux metatypes` and reconcile's final step (`process/reference_manager.py:143`) |
| `periods_by_name.json` | `sources/lc/mapper.py:373` (`LcshMapper`) | **Pre-built outside** |
| `parenthetical_places.json` | `sources/lc/mapper.py:561` (`LcnafMapper`), `sources/yale/library/mapper.py:92` | **Pre-built outside** |
| `stupid_table.json` | `sources/yale/library/mapper.py:51` | **Pre-built outside** |
| `ils-exhs.csv`, `ils-objs.csv` | `sources/yale/library/mapper.py:60-61` | **Pre-built outside** |
| `lib_okay_parts.json` | `sources/yale/library/mapper.py:77` | **Pre-built outside** |
| `yul-wiki-recon.csv` | `sources/yale/library/mapper.py:85` | **Pre-built outside** |
| `lib_enhance_places.json` | `sources/yale/library/mapper.py:101` | **Pre-built outside**. Probably from an LLM run (`experiments/gemini-places.py` writes a `results.jsonl`), but nothing turns that output into this file. |
| `macs.nt` | `sources/dnb/mapper.py:26` | **Pre-built outside**: downloaded by hand from DNB (`checkDataUpdates.py` only checks the URL) |
| `unidentified_people_ulan.csv` | `sources/getty/fetcher.py:33` | **Pre-built outside** |
| `indexes_dir/yuid_personname.lmdb` | `sources/lux/final/mapper.py:39` (merge) | **Pre-built outside**. `LlmNameIndexLoader` (`sources/lux/final/index_loader.py:16`) builds it from a JSONL of LLM-parsed names, but no config or command uses that class. The LMDB is keyed by YUID. |

## 2. Indexes for reconciliation

LMDB stores in `indexes_dir`, read through `TabLmdb`.

| Index | Read at | Origin |
|---|---|---|
| `label_to_aat.lmdb`, `label_to_ulan.lmdb` | `process/base/reconciler.py:245` (opened on first use) | **Generated** by `lux load-index` with `LmdbIndexLoader`, from the source's datacache (`process/base/index_loader.py`) |
| `label_to_lcsh.lmdb` / `id_to_lcsh.lmdb`, `label_to_lcnaf.lmdb` / `id_to_lcnaf.lmdb` | same | **Generated** by `lux load-index` (`sources/lc/index_loader.py`) from the datacache |
| `id_to_wikidata.lmdb` | same | **Generated**. `lux load` for wikidata writes `temp_dir/wd_equivs_<n>.csv` (`sources/wikidata/loader.py:68`), and `WdFileIndexLoader` turns them into the index (`sources/wikidata/index_loader.py:24`). |
| `id_to_viaf.lmdb` | same | **Generated** the same way: `temp_dir/viaf_equivs_<n>.csv` (`sources/oclc/loader.py:29`), then `ViafIndexLoader` |
| `global_equivs.lmdb` (merged, `reconcile_id`) | `sources/lux/final/reconciler.py` (`GlobalReconciler`) | **Generated** by `GlobalIndexLoader.load`, from the CSVs in `data_dir/sameAs/`. `google.csv` there comes from `lux sheets`. Any other CSVs in that directory are **pre-built outside** and loaded only by the root `load-csv-map2.py --all`. |
| `global_diffs.lmdb` (merged, `differents`) | `sources/lux/final/reconciler.py:21`; identify (`process/identity_resolver.py:295`) | **Generated** from `data_dir/differentFrom/`. `google.csv` comes from `lux sheets` and `wikidata.csv` from `lux build-differents --load`. Other CSVs are **pre-built outside**, as with sameAs. |
| `yul_headings.lmdb` (ils `headingsPath`) | `sources/yale/library/mapper.py:155`, via `YulIndexLoader.load_index` | **Pre-built outside**. `experiments/create_precoordinated_headings.py` writes `precoordinated_headings.json` from the ils datacache, then `YulIndexLoader.update(file)` loads it. No command or build step runs either part. |

## 3. Other files

| File | Read at | Origin |
|---|---|---|
| `data_dir/idmap_update_token.txt` | `process/phases/reconcile.py:135`, `process/phases/merge.py:43`, every idmap (`storage/idmap/postgres.py:260` and others) | **Generated** by `lux new-token` and `lux initialize` |
| `artifacts_dir/getty_replacement_uris.json` (before artifacts, `data_dir/`): the replaced AAT, ULAN and TGN ids | `process/reference_manager.py` `load_redirects`, called by the reference manager (reconcile) and the Getty fetcher (`sources/getty/fetcher.py`) | **Generated** by `lux artifact --type getty_replacement_uris` (`artifacts/getty.py`, docs/artifacts.md), into `artifacts_dir`; read from `data_dir` until the artifact is made. Called `replacements.json` (reference manager) and `getty_replacements.json` (fetcher) before 2026-11; an old name is still read, with a warning to rename it. |
| `data_dir/schema/*.json` | `process/validator.py:48` | **Fetched** by `lux initialize` (`cli/initialize.py:369`) from the Linked Art site |
| `data_dir/pending_deletes.jsonl` | `process/update_manager.py:20` | **Generated** by the harvesters (`process/base/harvester.py`) |
| `data_dir/google_sheet_id.txt`, `token.json`, `credentials.json` | `process/sheets.py:218, 236` | **Pre-built outside**: the sheet's id and the Google API credentials |
| `data_dir/lcsh/external_links.nt` (`externalLinksPath`) | `sources/lc/loader.py:15` (load time) | **Pre-built outside**: downloaded by hand from id.loc.gov. The live config does not fetch it. |
| `temp_dir`: `assertions-<n>.tsv`, `metatypes-<n>.json`, `reference_uris.txt`, `wd_equivs_<n>.csv`, `viaf_equivs_<n>.csv`, `<source>_<kind>_<n>.tsv` | identify, metatypes, merge, `lux load-index` | **Generated**: files the phases hand to each other |

## Problems found

1. **`YulIndexLoader.update` only stores one heading.** In
   `sources/yale/library/index_loader.py:59` the `all_dicts = ...` and store
   lines sit outside the `for key ...` loop, so loading a headings file
   writes only the last key.
2. **`yuid_personname.lmdb` is keyed by YUID but built from a one-off LLM
   run.** If YUIDs change (an idmap rebuild, expiry), the parsed names
   silently stop matching. Its builder is connected to nothing, and it
   writes `bad-js-lines.jsonl` into the current directory.
3. **`getty_<prefix>_uris.json` is written but never read.** The Getty
   harvester writes it (`sources/getty/harvester.py:62`), and nothing reads it.
4. **viaf's `reconcileDumpPath` (`viaf/viaf-links.txt.gz`) is configured but
   unused.** `ViafIndexLoader` reads the temp CSVs instead.
5. **The disaster-recovery gap.** About 17 files are pre-built outside the
   pipeline, mostly mapper data. None can be rebuilt, and a fresh install
   runs without them, with no warning.

Fixed since the audit: the Getty replaced-ids file was in two copies
(`replacements.json`, `getty_replacements.json`); it is now one file,
`getty_replacement_uris.json`, read by both. It is now the first artifact
(docs/artifacts.md): `lux artifact --type getty_replacement_uris` makes it,
with a manifest entry saying when and from what, and `lux testinstall`
reports it when stale.

Two cheap improvements would close most of that gap:

* warn when an expected data file is missing, as `getty_replacement_uris.json`
  already does;
* have `lux testinstall` list which of these files are present.
