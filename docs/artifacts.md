# Artifacts: generated files with a record of when and how

Status: plan (2026-11-18). Nothing here is implemented yet.

## Why

A build reads a number of files that are neither source data nor
configuration: `type_overrides.json`, `getty_replacement_uris.json`, the
CSVs and `xpath_fixes.json` from the Google Sheet, Wikidata's
`differentFrom` pairs. Today each is made by hand, by a one-off script or by
a `lux` command that leaves no trace of when it ran. Nothing says how old a
file is, what made it, or when it should be made again. `type_overrides.json`
is the example: a 2022 snapshot of Wikidata's language items with no
generator anywhere, about 390 of its Wikidata items no longer carrying an
ISO 639 code, and 89 of its 1,478 AAT ids outside AAT's languages hierarchy
(74 culture or people concepts, 12 replaced ids, 3 others, and one
malformed id).

An **artifact** is such a file (or a few files made together) with:

* a configuration record saying how to make it, kept in pipeline-configs
  beside the sources;
* a generator class that makes it from that record;
* an entry in a manifest saying when it was made, by which code and which
  configuration, and what it should checksum to;
* an age after which it is stale.

`lux artifact --type <name>` makes it again and updates the manifest.

Files with no generator are not artifacts. Each one either gets a generator
or, if it is curated by hand, moves into the codebase beside the code that
reads it (see "The other files" below).

## Layout

* `artifacts_dir`: a new optional base key, falling back to
  `<data_dir>/artifacts` as `backup_dir` falls back to `<data_dir>/backups`.
  Relative paths resolve against `base_dir` like the other directories.
* `<artifacts_dir>/manifest.json`: one entry per artifact made at least
  once, kept by `ArtifactCache` (see "The cache and the manifest").
* `<artifacts_dir>/<file>` for each file an artifact lists; subdirectories
  allowed (`sameAs/google.csv`).

## Configuration records

Artifacts are configuration records of a new `type`, `"artifact"`, filed by
`Config.handle_config_record` into `cfgs.artifacts` (name to record). In
pipeline-configs they are a sub-config store like sources and maps:
`configs/subs/artifacts_cache/<name>.json`, registered by one `config`
record, `configs/system/config_cache/artifacts.json`, the same shape as
`maps.json`.

```json
{
  "name": "getty_replacement_uris",
  "type": "artifact",
  "generatorClass": "lux_pipeline.artifacts.getty.ReplacementUris",
  "files": ["getty_replacement_uris.json"],
  "maxAgeDays": 90,
  "maxShrink": 0.5,
  "endpoint": "https://vocab.getty.edu/sparql.json",
  "query": "SELECT ?from ?to WHERE { ?from <http://purl.org/dc/terms/isReplacedBy> ?to }"
}
```

Keys every artifact has:

| Key | Meaning |
|---|---|
| `name` | required; the record's key, as for sources |
| `type` | `"artifact"` |
| `generatorClass` | required; resolved with `importObject` like every `*Class` |
| `files` | required; the files the generator writes, relative to `artifacts_dir`, unique across artifacts, no `..` |
| `maxAgeDays` | optional; older than this is stale. Absent or `null`: never stale by age, only when its record changes |
| `maxShrink` | optional, default `0.5`: refuse to replace a file whose row count drops by more than this fraction (a SPARQL endpoint that answers with nothing) unless `--force` |

Everything else in the record is the generator's own: an endpoint, a query,
a sheet id, which properties to keep. Each generator class lists its keys
(`required_keys`, `optional_keys`) and `lux config check` checks them, as it
checks the common keys above. The catalogue in `config_schema.py` gains an
`ARTIFACT` table for the common keys.

Records are read, never instantiated by `instantiate_all`: a generator is
built only when `lux artifact` runs it. Its `__init__` opens nothing (the
rule for every component).

## Generators

Artifacts have their own part of the package, `lux_pipeline/artifacts/`,
beside `lux_pipeline/sources/` (a namespace package like the rest: no
`__init__.py`):

| Module | Holds |
|---|---|
| `artifacts/base.py` | `ArtifactGenerator`, the base class |
| `artifacts/cache.py` | `ArtifactCache`: the files, `manifest.json`, staleness |
| `artifacts/getty.py` | `ReplacementUris` (`getty_replacement_uris`) |
| `artifacts/languages.py` | `LanguageTypes` (`type_overrides`) |
| `artifacts/sheets.py` | the Google Sheet generator, over `process/sheets.py`'s `SheetImport` |
| `artifacts/differents.py` | `wikidata_differents`, over `lux build-differents` |

An artifact does not need a configured source. A generator reads public
services (SPARQL endpoints, downloads) or files, never a source's
datacache, so an installation without Wikidata as a source still gets
`type_overrides` for languages. The exception is an artifact that is by
nature derived from a source's data (`wikidata_differents` is the
`differentFrom` pairs in the wikidata datacache); its generator says which
source it needs (`needs_sources`), `lux config check` warns when the record
exists without that source, and `lux artifact` refuses to run it.

The base class:

```python
class ArtifactGenerator:
    required_keys = ()
    optional_keys = ()
    needs_sources = ()

    def __init__(self, config, cfgs, progress=null_progress): ...

    def generate(self, out_dir):
        """Write every file in config["files"] under out_dir (a temporary
        directory with the same relative layout). Return
        {relative path: row count}."""
```

Generators log through `lux_pipeline` and never print.

## The cache and the manifest

`ArtifactCache` (`artifacts/cache.py`) is `artifacts_dir`, and the only
thing that writes in it. Every write updates `manifest.json` as part of the
write, so the manifest cannot drift from the files: there is no separate
"now record it" step for a generator or a command to forget. Readers go
through it too.

| Method | Does |
|---|---|
| `path(name, file=None)` | the artifact's file (`file` for one with several); `None` when missing |
| `entry(name)` / `entries()` | manifest entries |
| `stale(name)` | `None`, or why it is stale (see "Stale") |
| `set(name, files, config, generator=None, seconds=None, force=False)` | publish `files` (`{relative path: (temporary path, rows)}`) as artifact `name` |
| `make(name, force=False)` | build the record's generator, run it into a temporary directory, `set` the result |
| `check(name=None)` | recompute sizes and md5s against the manifest |

It is not an `FsCache`: an artifact is a file of any format (CSV, N-Triples,
JSON), not a JSON record, and has several files under one name. It borrows
`FsCache`'s ways: temporary file then `os.replace`, and `manifest.json`
written normalized (2-space, sorted, `/` unescaped) so it diffs well.

`set`:

1. checks each listed file exists, and its row count against the manifest's
   (`maxShrink`; `force` passes);
2. renames each into place (temporary files are made in
   `<artifacts_dir>/.tmp-<name>-<pid>/`, on the same filesystem, so atomic
   per file);
3. updates the artifact's entry in `manifest.json`: under an `fcntl` lock on
   `manifest.json.lock`, read the file, replace the entry, write a temporary
   file and rename it, so two artifacts made at once cannot lose each
   other's entries;
4. removes the temporary directory, on failure too. A failed run leaves the
   previous files and entry as they were.

`make` is what `lux artifact` calls. A command that is a generator in its
own right (`lux build-differents`'s merge, `lux sheets`) calls `set` with
what it wrote, and gets the same checks and manifest entry.

`Config.artifacts_cache` is built on first use, not at construction, like
the other lazily opened stores.

`manifest.json`:

```json
{
  "getty_replacement_uris": {
    "generator": "lux_pipeline.artifacts.getty.ReplacementUris",
    "generated_at": "2026-11-18T14:02:11+00:00",
    "seconds": 4.2,
    "files": [
      {"path": "getty_replacement_uris.json", "size": 8226003,
       "md5": "…", "rows": 40606}
    ],
    "code": {"commit": "12cb4b5…", "dirty": false, "version": "0.1.0"},
    "config": {"commit": "d4e202d…", "dirty": false,
               "path": "/…/configs/subs/artifacts_cache/getty_replacement_uris.json",
               "record_sha256": "…"}
  }
}
```

* `generated_at` is in the entry, not taken from the files' mtimes, so
  copying or restoring the directory does not make an artifact look new.
* `code.commit` / `dirty`: `git rev-parse HEAD` and `git status --porcelain`
  in the checkout `lux_pipeline` was imported from; `null` when it is not a
  checkout. `version` is the installed `lux-pipeline` version.
* `config.commit` / `dirty`: the same for the record's file
  (`Config.record_origins`), `dirty` for that one file
  (`git status --porcelain -- <file>`); `null` outside a checkout, as the
  `config_cache` copies are.
* `config.record_sha256`: SHA-256 of the record as read
  (`json.dumps(record, sort_keys=True, separators=(",", ":"))`). It catches
  a local edit the commit does not, and can be compared after the config
  repo has moved on.
* `rows`: the generator's count (lines of a CSV, keys of a JSON object,
  bindings of a SPARQL result), for the shrink check and for `--list`.

## Stale

An artifact is stale when any of:

* it has no manifest entry, or a listed file is missing;
* `generated_at` is more than `maxAgeDays` ago;
* its record's SHA-256 differs from `config.record_sha256` (the query, the
  sheet, the filters changed);
* `generatorClass` differs from `generator`.

A new code commit alone does not make an artifact stale (it changes on
nearly every merge); the manifest records it so a file can be traced to the
code that made it. A checksum that no longer matches is reported by
`--check` as damaged, not stale.

## Reading an artifact

`Config.artifact_path(name, file=None)` is `artifacts_cache.path`, with:

* a warning, once per process, when the artifact is stale (several mappers in
  each of many workers read the same files);
* when the configuration has no record for `name`, the old location in
  `data_dir` (`type_overrides.json`, `getty_replacement_uris.json`, …), with
  a warning, so nothing breaks before pipeline-configs is updated;
* `None` when the file exists in neither place; readers keep their current
  behaviour for a missing file (skip, or warn as `load_redirects` does).

The readers changed:

| File | Reader |
|---|---|
| `type_overrides.json` | `process/base/mapper.py` (`Mapper.__init__`) |
| `getty_replacement_uris.json` | `process/reference_manager.load_redirects`, used by the reference manager and the Getty fetcher; the `OLD_REDIRECTS_FILES` names stay as the last fallback |
| `xpath_fixes.json` | `process/base/mapper.py` |
| `sameAs/google.csv`, `differentFrom/google.csv`, `differentFrom/wikidata.csv` | `GlobalIndexLoader` (`sources/lux/final/index_loader.py`) and `SheetImport.load`: read the artifacts' files as well as the CSVs still in `data_dir/sameAs|differentFrom` |

## `lux artifact`

A `BaseHandler` (sequential, one-shot; gets a run record from
`entry.start_record` like the other one-shot commands):

| Option | Does |
|---|---|
| (none), `--list` | every configured artifact: files, rows, age, max age, stale and why |
| `--type NAME[,NAME…]` | make these again |
| `--stale` | make every stale one again |
| `--all` | make every one again |
| `--check` | recompute sizes and md5s against the manifest; exit 1 on a mismatch |
| `--force` | past the shrink check and the build guard |
| `--json` | `--list` / `--check` as JSON (for the web UI) |

Making an artifact refuses while a build runs (`refuse_while_building`):
mappers read these files as they start, so workers would see different
versions. Listing and checking never refuse.

Also:

* `lux testinstall`: a check listing missing and stale artifacts (a warning,
  not a failure).
* `lux list --artifacts`: `list.artifact_rows`, as `--maps` uses `map_rows`.
* `lux config check`: the record checks above; `config_store` gets
  `ConfigError`s for an unknown generator key and an overlapping `files`
  entry.
* The web UI's config page shows the records with no change (it lists every
  store). An artifacts page (list, check, "make again" through the runner) is
  a later, separate PR.
* `lux backup`: unchanged for now. Artifacts can be made again, and the
  manifest says how.

## The first artifacts

One PR each, in this order. Each adds its record to pipeline-configs (and the
`config_cache` copies) in a companion PR.

1. **Framework and `getty_replacement_uris`.** `artifacts_dir`, the record
   type, catalogue and checks, `artifacts/base.py`, `ArtifactCache`,
   `artifact_path`,
   `lux artifact`, the testinstall check, and the Getty generator: the SPARQL
   query from `load_redirects`' docstring (keys `endpoint`, `query`), the
   SPARQL JSON result written as it is read today. `maxAgeDays: 90`.
2. **`type_overrides`.** `artifacts/languages.py`, `LanguageTypes`, from two
   SPARQL endpoints and no source:
   * Wikidata (`wikidataEndpoint`, default
     `https://query.wikidata.org/sparql`): items with any of
     `codeProperties` (default `P218`, `P219`, `P220`, the ISO 639 codes;
     8,330 items in 2026-11), with their `P1014` (AAT), `P244` (LC) and
     `P4801` (LC vocabularies);
   * LC ids kept only when they are subject headings (`sh…`), LC vocabulary
     ids only under `languages/`;
   * Getty (`gettyEndpoint`, default `https://vocab.getty.edu/sparql.json`):
     every concept under `aatRoot` (default `300389739`, "languages and
     writing systems": 2,327 concepts, the "by specific type" hierarchy and
     the alphabetical list `300411913` that `AatMapper.guess_type` tests).
     An AAT id from Wikidata is kept only when it is in that set. Tried
     against today's file, this drops 74 of its 75 AAT ids outside the
     languages range (all but Ga'anda, a language in the alphabetical list),
     the malformed `aat/6000320089674`, 12 ids Getty has replaced and 3
     non-language concepts. A replaced id whose replacement is in the set
     could be kept as the replacement; decided when writing it;
   * every kept URI to `"Language"`, the file's shape today.

   `maxAgeDays: 180`. The applying code is unchanged here (see "Not in
   these PRs").
3. **`google_sheets`.** `SheetImport.fetch` becomes the generator: files
   `sameAs/google.csv`, `differentFrom/google.csv`, `xpath_fixes.json`; the
   sheet id moves from `data_dir/google_sheet_id.txt` into the record
   (`sheet`), the file still read when the record has none. Credentials stay
   in `data_dir` (secrets are not configuration). `lux sheets` keeps its
   options: fetch through the artifact, then load. How `lux sheets --file`
   (a local xlsx or CSV directory instead of the sheet the record names)
   relates to the manifest is decided in this PR.
4. **`wikidata_differents`.** `lux build-differents` writes
   `differentFrom/wikidata.csv` through `ArtifactCache.set` when its
   merge finishes; the generator (`needs_sources = ("wikidata",)`) runs the
   command's manager. The 137 MB
   `differentFrom/wikidata_differents.csv` in today's `data_dir` is the
   older output under another name.

## The other files

The audit of `data_dir` ([data-files.md](data-files.md), 2026-11-18) found these made outside the pipeline.
Each is to get a generator or move into the codebase; nothing is an artifact
without a generator.

| File | Looks like | Proposed |
|---|---|---|
| `parenthetical_places.json` (9 KB) | label to TGN URI, curated | codebase, beside `sources/lc/mapper.py` |
| `stupid_table.json` (22 KB) | YUL node to replacement URI and class, curated | codebase, beside `sources/yale/library/mapper.py` |
| `lib_okay_parts.json` (57 KB) | YUL node to count | unknown origin: Rob |
| `periods_by_name.json` (700 KB) | period label to YUL node | generator from the ils datacache (`needs_sources`), if that is where it came from: Rob |
| `ils-exhs.csv`, `ils-objs.csv` | ILS id to YCBA activity URIs | generator from the ycba / ils datacaches (`needs_sources`), if derivable: Rob |
| `yul-wiki-recon.csv` (1.4 MB) | YUL node to Wikidata entity | a reconciliation run's output: Rob |
| `unidentified_people_ulan.csv` (1.3 MB) | ULAN ids of unidentified people | generator over Getty's SPARQL endpoint (by label or hierarchy) |
| `lib_enhance_places.json` (14 MB), `sameAs/matches-llm.csv` (15 MB) | LLM runs | generator wrapping the run (`experiments/gemini-places.py`), `maxAgeDays: null`; or kept as data with a recorded origin: Rob |
| `macs.nt` (DNB), `lcsh/external_links.nt` | downloaded by hand | generator that downloads the URL (or the source's own `lux download`) |
| `differentFrom/aat_different_from.csv`, `tgn_…`, `ulan_…`, `quick_differents.csv` | different-from pairs | Getty ones: generator, if they come from Getty data; `quick_differents.csv` (3 pairs): codebase or the sheet |
| `<source>_cycle_breaks.json` (`cycleBreakPath` on bnf, lcsh, wikidata) | | not in the download: the mappers skip them silently. Generator from cycle detection over the source's datacache (`needs_sources`), or drop the key |
| `all_aats.csv`, `all_ulans.csv`, `works-wiki-recon-old.csv`, `place-cycle-fixes.jsonl`, `results.jsonl.copy`, `populate-timestamps.txt` | | read by no pipeline code (`place-cycle-fixes.jsonl` by the untracked `merge_place_equivs.py`, `populate-timestamps.txt` by the root `populate-timestamps.py`): delete, or archive outside `data_dir` |

Codebase files go in a `data/` directory beside their reader, added to
`[tool.setuptools.package-data]` in `pyproject.toml`, and read relative to
the module.

Not artifacts at all: `idmap_update_token.txt`, `pending_deletes.jsonl`,
`metatypes.json` (each build's own state), `schema/*.json` (fetched by
`lux initialize`), credentials.

## Not in these PRs

* Where `type_overrides` is applied. `Mapper.post_reconcile` changes a
  record's type in memory during reconcile, so its assertions are written
  under `##quaLanguage`, but the recordcache keeps `Type` and merge looks the
  record up as `##quaType` (`process/merge_batch.prefetched`). Read from the
  code, not yet seen in a build's logs (`Couldn't find YUID for internal
  record …##quaType`). Fixing it, by applying the override at mapping time, is
  a change to reconcile behaviour and its own PR.
* The web UI artifacts page.
* `lux build` running `lux artifact --stale` as a first step.
