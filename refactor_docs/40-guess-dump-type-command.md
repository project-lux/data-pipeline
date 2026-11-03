# 40. `lux guess-dump-type`

**Priority:** P1 · **Size:** M · **Depends on:** 05, 12 · **Unblocks:** 29 (drafts its `input_files` blocks)
**Read from:** nothing to port. Neither branch has this. The precedents on
`main` after item 12 are `Loader.guess_fmt()` (a spec from the file *name*,
used for an `input_files` entry without `type`) and `Loader.process_fmt()`
(the spec grammar); the per‑source `load()` methods on `main` are where the
knowledge of each dump's shape currently lives.

## Intent

Look at the files in a source's dumps directory and print the `input_files`
block that describes them, so a config author copies it in instead of
working the path language out by hand:

```
lux guess-dump-type --source ror            # scan dumps_dir/<ror's dir>, print the block
lux guess-dump-type --source all            # every source that has a dumps directory
lux guess-dump-type --path /data/input/dnb  # a directory or file, no config needed
lux guess-dump-type --source ror --explain  # the evidence behind each guess
lux guess-dump-type --source ror --type export   # file these entries under "export"
```

Output is the JSON a config takes, nothing else on stdout:

```json
"input_files": {
  "records": [
    {"path": "v1.63-2025-05-20-ror-data.zip", "type": "zip/array/raw"}
  ]
}
```

Files the sniffer cannot place are listed on stderr with the reason (SQLite
header, tab‑separated text, N‑Triples, empty file), and the exit status is
1 when any file in the directory was unplaceable, so a `--source all` sweep
says exactly which sources still need a hand‑written loader.

The command **writes nothing**. Guessing happens when the config is written
and the result is pinned in the config; a build loads what the config says.
A stray file appearing in a dumps directory can then never change what a
build loads, and `lux list` keeps showing the truth.

## Why it matters

* Item 29 moves the source loaders onto the DSL, and most of its work is
  writing the `input_files` entries for eighteen sources, each of which
  means opening the dump and checking whether the JSON is one record, an
  array, one object per line, or an array with one object per line that
  must not be parsed whole. That is mechanical and this item makes it so.
* `Loader.guess_fmt()` guesses from the file name only. `records.json` may
  be a record, a dict of records, an array or lines; the name cannot tell.
  Content sniffing gives the fallback for an entry without `type` the same
  accuracy as the command, and logs what it assumed.
* New sources arrive as a directory of files. Today the first question is
  "what is in it"; the answer should come from the tool.

## Files

| Action | Path on `main` | Notes |
|---|---|---|
| Create | `lux_pipeline/process/utils/sniff.py` | pure functions, no config, no Rich: `sniff_compression`, `sniff_container`, `sniff_spec`, `discover_input_files` |
| Edit | `lux_pipeline/process/base/loader.py` | `guess_fmt()` delegates to `sniff_spec()` and logs the guess at INFO |
| Create | `lux_pipeline/cli/guess_dump_type.py` | `BaseHandler`; `--source`, `--path`, `--type`, `--explain` |
| Create | `tests/test_sniff.py`, `tests/test_cli_guess_dump_type.py` | |
| Edit | `refactor_docs/29-source-loaders-dsl.md` | step 0: draft each block with the command |

## Design

### What is sniffed

Never the whole file. The sniffer reads at most 64 KB of raw bytes, inflates
at most 1 MB when the file is compressed, and opens an archive only far
enough to see its first members. The one bounded exception is the
single‑record‑or‑dict decision below.

| Evidence | Conclusion |
|---|---|
| bytes `1f 8b` | compression `gz` |
| bytes `BZh` | compression `bz2` |
| bytes `78 01` / `78 9c` / `78 da` | compression `z` (zlib; only meaningful inside an `lmdb` container) |
| bytes `PK\x03\x04` | container `zip` |
| `ustar` at offset 257 (after inflating if compressed) | container `tar` |
| a directory containing `data.mdb` | container `lmdb` |
| a directory whose files are all in sub‑directories | container `dirs` |
| any other directory | container `dir` |
| bytes `SQLite format 3\0` | unplaceable (SQLite) |
| first line has tabs or commas and no `{`/`[`/`<` | unplaceable (delimited text) |
| first line ends in ` .` and starts with `<` or `_:` | unplaceable (N‑Triples) |

For the content itself, after any compression and container have been
stripped, the first non‑whitespace character decides:

| First char | Then | Spec |
|---|---|---|
| `[` | a newline before the first `{` | `arraylines/json` |
| `[` | otherwise, and the file is under 1 GB | `array/raw` |
| `[` | otherwise, 1 GB or more | `arraylines/json` with a warning that it was chosen for size |
| `{` | the first line is a complete JSON object and the next non‑blank line starts with `{` | `lines/json` |
| `{` | one object spanning the file, under 64 MB, every top‑level value a dict | `dict/raw` |
| `{` | one object spanning the file, otherwise | `json` |
| `{` | one object, 64 MB or more, not lines | `json` with a note to verify |
| `<` | more content follows the first root element's close | `xmls/other` |
| `<` | otherwise | `other` |

The format step follows the container: `json` after `lines`, `raw` after
`array` and `dict` (their children are already parsed), `other` after
`xmls` and for a single XML file. These are the conventions the item 12
fixtures use (`array/raw`, `dict/raw`, `tar.gz/xmls/other`).

### Archives

Members are filtered by name first: directories, `__MACOSX/`, dot‑files and
members with an unrecognised extension are listed as *ignored* and not
opened. The first remaining member is sniffed as above, giving e.g.
`zip/lines/json` or `tar.gz/lines.gz/json`. The next few members (up to
five) are checked the same way; if they disagree with the first, the
majority wins and the disagreement is reported. A zip whose only members
are ignored is unplaceable.

The ignored list is what the ROR loader's `should_process_item` encodes by
hand (the `.csv` beside the `.json` in the v2 zip). Printing it tells the
author the loader needs that override, which the base `Loader` applies by
default for members whose extension is not recognised once item 29 lands;
until then the entry is correct and the override still exists.

### Directories

`--source` resolves the directory the way the loader resolves a bare
filename: `dumps_dir`, then the source's own `dumps_dir` sub‑directory if it
has one; failing that, the directory of `dumpFilePath`. Every regular file in
it gets one entry, sorted by name, with the path written relative to that
directory so the block is portable between machines. Files that are
themselves directories of records (a `dir` of JSON, an LMDB) get one entry
with the directory container. Sub‑directories are not recursed into unless
the directory as a whole is `dirs`.

A source with neither key and no directory on disk prints nothing and
exits 0 with a message on stderr; `--source all` skips it.

### `--explain`

Adds, on stderr, one line per file: the magic bytes seen, the first 60
characters of content after inflation, the member examined for an archive,
and the top‑level keys of the first record (`id`, `@id`, `uri`, `about`),
which is the hint a loader author needs for `extract_identifier`. Nothing
about identifiers goes into the JSON: the entry describes the file, the
loader class describes the record.

### The loader fallback

`Loader.guess_fmt(path)` keeps its signature and callers but gets its answer
from `sniff_spec(path)` instead of the extension table, so an entry without
`type` is guessed from content. The guess is logged at INFO
(`"<source>: guessed <spec> for <path>"`) so a build log shows every
assumption. An unplaceable path still raises `ValueError` telling the author
to set `type`.

## Steps

1. `sniff.py` with the two tables above as data, not nested `if`s, so a new
   signature is one row. Fixture tests first: every file under
   `tests/loader_tests/` must sniff to a spec the item 12 cases accept
   (`tar/records.tgz` sniffs to `tar.gz/json`; `tar.gz/lines/json` also
   loads it and is not wrong, so the test accepts either where the fixture
   has one record). Then built fixtures in `tmp_path` for the shapes the
   checked‑in set lacks: `arraylines`, a dict of records versus a single
   record, concatenated XML, an LMDB directory, a zip with a CSV to ignore,
   a mixed‑member archive, a SQLite header, a tab‑separated file.
2. `guess_fmt()` delegation and the INFO line; the existing loader tests
   stay green.
3. The CLI handler. `--path` needs no configs (`needs_configs` false when it
   is given); `--source` uses the handler's source resolution from item 05.
   Output JSON with `json.dumps(indent=2)`; everything else to stderr.
4. Round‑trip test: feed the printed block to a `Loader` on a stub config and
   assert the fixture loads. That is the acceptance in one test.
5. Add step 0 to item 29: run the command for each source and start from its
   output.

**Live config copies.** `config_cache/config_cache/` is an untracked copy of the
build machine's configuration files (see README ground rule 5). This item
adds no config key and writes no file, so no copy changes. Running
`lux guess-dump-type --source all` against the build machine's `/data/input`
is Rob's check; its output is the starting point for item 29's blocks.

## Acceptance criteria

* Every fixture under `tests/loader_tests/` sniffs to a spec that loads it.
* The printed block for a fixture directory, pasted into a config, loads it
  through `Loader` unchanged (the round‑trip test).
* `lux guess-dump-type --source ror` on the build machine prints
  `zip/array/raw` for the ROR zip and lists its `.csv` member as ignored;
  `--source wof` reports the SQLite file as unplaceable and exits 1.
* The command never writes to disk; `lux list` output is unchanged by running it.
