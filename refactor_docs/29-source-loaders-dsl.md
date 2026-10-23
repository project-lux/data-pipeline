# 29. Source loaders moved onto the DSL

**Priority:** P2 · **Size:** M · **Depends on:** 12, 13
**Read from:** `rob_claude_refactor:lux_pipeline/sources/{bnf,ror,lc}/loader.py`, `rob_claude_refactor:lux_pipeline/sources/yale/{ycba,ypm,yuag,pmc,ipch}/loader.py`, `rob_refactor:lux_pipeline/sources/{bne,dnb}/loader.py`, `rob_refactor:lux_pipeline/sources/pleiades/*`; `main:pipeline/sources/libraries/dnb/loader.py`, `main:pipeline/sources/pleiades/mapper.py`

## Intent

Convert the sources whose dumps are plain containers of JSON/XML to the
DSL, deleting their hand‑written `load()`; add loaders for sources that
have a mapper but no loader on `main`. Leave the complex ones alone.

| Source (`main` path) | Action | `input_files` type |
|---|---|---|
| `bnf/loader.py` | **create** from `rob_claude_refactor:…/bnf/loader.py` (`make_identifier` from `Pointer`, `extract_identifier` from `rdf:about` with the empty‑data guard) | `tar.gz/xmls/other` |
| `bne/loader.py` | **create** from `rob_refactor:…/bne/loader.py` | `lines.gz/json` (check the dump) |
| `dnb/loader.py` | **replace** `load()`/`load_sachbegriff()` with the refactor's `DnbLoader` (`extract_identifier` on `@id`, `iterate_sachbegriff`) and register `"sachbegriff"` as an extra container step | main file `arraylines.gz/json`; subjects `sachbegriff/json` |
| `pleiades/` | `main` has a mapper + downloader but no loader; the base `Loader` with `input_files` suffices (`dict`‑or‑`array` of places; check the dump) | `array/raw` + `other` for the `.ttl` types file |
| `ror/loader.py` | **replace** with `rob_claude_refactor:…/ror/loader.py`: only `should_process_item` (skip the `.csv` member of the v2 zip); identifier comes from the base `extract_identifier` (`id` → last path segment) | `zip/array/raw` |
| `yale/{ycba,yuag,ypm,ipch,pmc}/loader.py` | **convert** from `rob_claude_refactor` (each is ≤ 10 lines: `make_identifier` strips the S3/namespace prefix; ipch is `pass`), **after** diffing against `main`'s current versions — `main` touched these in 2026; carry over any post‑processing that `rob_claude_refactor` dropped | ycba `tar.gz/json`; others `lines/json` (check the dumps) |
| `lc/loader.py` | candidate: `rob_claude_refactor:…/lc/loader.py` is a full DSL conversion (`prepare()` reads `externallinks.nt.zip` from `dumps_dir/lc/`, `post_process_json(data, ident)` drops `-781` and undifferentiated records). `main`'s loader has newer online‑sync logic; convert only if that logic is folded in | `lines.gz/json` |
| `wikidata/`, `oclc/`, `orcid/`, `geonames/`, `yale/library/` | **leave** (special slicing, equivs side files, S3, text formats) | — |

Each converted source needs its config to gain `input_files` (or
`dumpFileType`) — a `docs/sample_config/<src>.json` edit in the same PR and a
note for the live instance.

## Known bugs in the source — do not copy

* `BneLoader.extract_identifier` calls `self.make_identifer` (typo) and
  returns `None` when `id` is present (should return the super result).
* `BnfLoader.extract_identifier` relies on the first line's last token being
  `rdf:about="..."` — fine for the BnF dump but log at DEBUG, not ERROR, for
  non‑matching `rdf:Description` children (there are many).

## Steps

1. One source per commit inside the PR; run `lux load --source <src> --no-ui --max-records 1000` for each.
2. Update `docs/sample_config/<src>.json`.
3. Add a fixture‑based test per converted source using a 3‑record sample dump under `tests/loader_tests/sources/<src>/`.

## Acceptance criteria

* Converted sources load the same identifiers as before (compare `lux counts`).
* No `load()` remains in the converted loaders.
