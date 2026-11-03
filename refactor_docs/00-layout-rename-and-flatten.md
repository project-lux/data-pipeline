# 00. Layout: rename the package to `lux_pipeline` and flatten `sources/`

**Priority:** P0 · **Size:** L (mechanical) · **Depends on:** — · **Unblocks:** everything
**Read from:** `git ls-tree -r --name-only rob_refactor lux_pipeline/sources` for the target shape. Nothing else: both steps are `git mv` plus search‑and‑replace on `main`.

Two PRs, in this order, each containing nothing but its move and the
import/config edits that move forces:

| PR | What | Result |
|---|---|---|
| **00a** | `pipeline/` → `lux_pipeline/` | package name says what it is |
| **00b** | `sources/<category>/<name>/` → `sources/<name>/` | one directory per source; newcomers no longer have to guess whether a new source is an "authority", "library" or "general" |

After 00b the tree matches `rob_refactor`'s: every `rob_refactor:lux_pipeline/...`
reference in these documents is then also the path on `main`, with two
exceptions noted in `PATH-MAP.md` (`main` keeps the directory name `oclc`
where `rob_refactor` has `viaf`; `rob_refactor`'s `wikipedia/` does not
exist on `main`).

## Why

`pipeline` is too generic a top‑level name, and the category level under
`sources/` is the thing newcomers most often get wrong when adding a source.
Both were done on `rob_refactor` and are the largest single cause of the
branch divergence, so they go first, alone, and never again.

## What stays

* `sources/yale/<unit>/` and `sources/lux/<kind>/` keep their grouping
  directory, exactly as on `rob_refactor` (they are not external sources).
* `sources/pleiades/` is already flat.
* Every file name and class name. No symbol is renamed.
* Directory name `oclc` (do not rename it back to `viaf`).

## PR 00a — package rename

| Action | Paths |
|---|---|
| `git mv` | `pipeline/` → `lux_pipeline/` |
| Edit imports | ~90 in‑package `from pipeline.` / `import pipeline.`; ~80 files outside the package (`run-*.py`, `manage-data.py`, root `*.py`, `experiments/**/*.py`, `tests/*.py`) |
| Edit | `lux_pipeline/config.py` `importObject`: auto‑prefix `lux_pipeline.`; if `objectType.startswith("pipeline.")` rewrite to `lux_pipeline.` and log once at WARNING |
| Edit | `docs/sample_config/base.json`: `"validatorClass": "lux_pipeline.process.validator.Validator"` |
| Edit | path mentions in `README.md`, `docs/*.md` |

Steps:

1. `git mv pipeline lux_pipeline`
2. `git grep -l -E "^(from|import) pipeline(\.|$)" -- '*.py' | xargs sed -i '' -E 's/^(from|import) pipeline(\.|$)/\1 lux_pipeline\2/'` (GNU sed: drop `''`). Check `git grep -n '"pipeline\.'` for string imports.
3. `importObject` edits; new `tests/test_import_object.py` asserting that
   `"process.validator.Validator"`, `"lux_pipeline.process.validator.Validator"`
   and `"pipeline.process.validator.Validator"` resolve to the same class.
4. `python -m pytest tests/ -q`; `python manage-data.py --counts` on the live instance (its `base.json` still says `pipeline.…` and must work).

## PR 00b — flatten `sources/`

| Action | Paths |
|---|---|
| `git mv` | `lux_pipeline/sources/archives/{ethan,snac}` → `lux_pipeline/sources/{ethan,snac}`; `authorities/{fast,gbif,getty,homosaurus,lc,nomisma,oclc}`, `general/{geonames,orcid,ror,wikidata,wikimedia,wof}`, `libraries/{bne,bnf,dnb,japan}`, `museums/{artic,ashmol,mds}` likewise; then `rmdir` the five empty category directories |
| Edit imports | `from lux_pipeline.sources.<category>.<name>` → `from lux_pipeline.sources.<name>` (in‑package cross imports such as `oclc/mapper.py` → `wikidata.base`; `tests/test_determinism_fixes.py`, `tests/test_mapper_error_handling.py`) |
| Edit | `lux_pipeline/config.py` `importObject`: legacy mapping — if the dotted path matches `sources.(archives|authorities|general|libraries|museums).<rest>` (with or without the package prefix), drop the category segment and log once at WARNING |
| Edit | `docs/sample_config/*.json`: every `*Class` value `sources.<category>.<name>.…` → `sources.<name>.…` (≈45 values; `sed -E 's/"sources\.(archives|authorities|general|libraries|museums)\./"sources./'`) |
| Edit | `docs/codebase.md`, `README.md` ("divided by organization that publishes the data" → one directory per source) |

Steps:

1. The moves (one `git mv` per source directory; 22 of them).
2. Import rewrite with one `sed` over `'*.py'`; review the diff.
3. `importObject` legacy mapping + tests (same test file as 00a: `"sources.authorities.lc.mapper.LcnafMapper"` and `"sources.lc.mapper.LcnafMapper"` resolve to the same class).
4. Sample configs.
5. `python -m pytest tests/ -q`; `python manage-data.py --counts` on the live instance **with its configs unchanged** (they still say `sources.authorities.…`; the mapping must carry them). Then update the live instance's configs at leisure — the warning tells the operator which ones.

## Rules

* Nothing else in either PR: no formatter, no symbol rename, no logic.
* `git diff -M --stat` must show renames plus import/config‑string edits only.
* Do not touch `refactor_docs/` paths — they already use the final layout.

## Acceptance criteria

* `ls lux_pipeline/sources` shows one directory per source plus `yale`, `lux`, `pleiades`; no `archives|authorities|general|libraries|museums`.
* `git grep -n "sources\.\(archives\|authorities\|general\|libraries\|museums\)" -- '*.py' '*.json'` returns only the compatibility code in `importObject` and its test.
* Live instance loads with unchanged configs; tests pass.
