# Describing a source's dump files: the loader DSL

A source whose records come as dump files (not fetched one record at a time
from an API) lists those files in its configuration, under `input_files`.
`lux download` fetches them, `lux load` reads them into the source's
datacache, and `lux guess-dump-type` works out how to describe them. This
page is the reference for writing that block when you set up a new source.

The code is `lux_pipeline/process/base/loader.py` (the loader and the
grammar), `lux_pipeline/process/utils/sniff.py` (the guesser) and
`lux_pipeline/process/tasks/load_task.py` (how `lux load` runs a loader).


## Setting up a new source, start to finish

1. Give the source a directory for its dumps and list where they come from:

   ```json
   "dumps_dir": "example",
   "input_files": {
     "records": [
       {"url": "https://example.org/downloads/example-records.jsonl.gz"}
     ]
   }
   ```

2. Download the files:

   ```
   lux download --source example
   ```

   An entry with a `url` is saved to its `path`, or to the last segment of
   the URL when it has none, under `<dumps_dir>/example/`.

3. Ask what the files are:

   ```
   lux guess-dump-type --source example --explain
   ```

   It prints the `"input_files"` fragment with a `type` on every entry.
   Check the guess (see [Reading what guess-dump-type prints](#lux-guess-dump-type)),
   then paste the types into the config, keeping your `url`s:

   ```json
   "input_files": {
     "records": [
       {"url": "https://example.org/downloads/example-records.jsonl.gz",
        "path": "example-records.jsonl.gz",
        "type": "lines.gz/json"}
     ]
   }
   ```

4. Try a few records, then check them:

   ```
   lux load --source example --max-workers 1 --max-records 100 --clear
   lux show --sample example
   ```

   Look at the identifiers the records were stored under. If they are not
   what the mapper and the reconcilers expect, add an
   [`identifier` rule](#identifiers).

5. Load the lot:

   ```
   lux load --source example --clear
   ```

Always pin `type` in the config. An entry without one is guessed at load
time (the guess is logged), but a build should not depend on a guess.


## The `input_files` block

```json
"input_files": {
  "<load type>": [ <entry>, <entry>, ... ],
  ...
}
```

### Load types

The keys are load types. `lux load --type` picks one, and `lux download --type`
picks which ones to download (`all` by default).

| Key | Loaded by | What it is |
|---|---|---|
| `records` | `lux load` (the default) | The source's own dump, as the publisher releases it |
| `export` | `lux load --type export` | A LUX export of the datacache, e.g. the published `https://yale-lux-data.s3.amazonaws.com/sources/<name>.zip` (`zip.bz2/json`), or `lux export --type external` output (`lines.gz/tabjson`) |
| `lmdb` | `lux load --type lmdb` | An LMDB distribution store from `lux export --format lmdb` (`lmdb/json.z`) |
| anything else | not loaded | Downloaded with `lux download --type <key>` for a source's own code to read (DNB's `mappings`) |

A source can have several entries for one load type: they are loaded in the
order listed, into the same datacache. When the same identifier is in two
files and overwriting is on, the later file wins (DNB depends on this).

### Entry keys

| Key | Meaning |
|---|---|
| `url` | Where `lux download` fetches the file from. Leave it out (or empty) for a file that is already on disk. |
| `path` | The file or directory the loader reads. A relative path resolves under the base config's `dumps_dir` plus the source's own `dumps_dir`, the same place `lux download` saved it. With no `path`, the last segment of `url` is used. |
| `type` | How to read it: a spec in the [type language](#the-type-language) below. |
| `slice` | `records` (default) or `files`: how the work is shared between workers. See [Slicing](#slicing). |
| `identifier` | Where each record's identifier comes from. See [Identifiers](#identifiers). |

An entry needs a `url` or a `path`; one with neither is skipped with a warning.

### Other source keys the loader reads

| Key | Meaning |
|---|---|
| `dumps_dir` | The source's sub-directory of the base config's `dumps_dir`. Also where `lux guess-dump-type --source` looks. |
| `namespace` | The source's URI namespace; the `namespace` identifier step strips it. |
| `totalRecords` | How many records the dump holds, for the progress bar. Without it the containers count as they go, where that is cheap (not for tar or large compressed files). |
| `loaderClass` | The loader to use; defaults to `process.base.loader.Loader`, which is all most sources need. See [When the DSL is not enough](#when-the-dsl-is-not-enough). |
| `dumpFilePath`, `dumpFileType`, `remoteDumpFile` | The older one-file form: used for `records` when there is no `input_files` entry for it. `dumpFileType` takes the same type language. Prefer `input_files` for a new source. |


## The type language

A `type` is the path from the file on disk down to one record:

```
(container[.compression]/)*format[.compression]
```

Each `/` is one step. Every step but the last is a **container**: it opens
what it is given and passes each child (a file, an archive member, a line,
a JSON item) to the next step. The last step is the **format**: it turns one
child into a record, works out its identifier and stores it. Compression
goes on the step that reads the compressed bytes.

### Containers

| Container | Reads | Each child is |
|---|---|---|
| `dir` | A directory | Each file directly in it, sorted by name (sub-directories are not read) |
| `dirs` | A directory tree | Each file anywhere under it, walked in sorted order |
| `pair` | A directory tree of two-character sub-directories | The same as `dirs` |
| `zip` | A zip archive | Each member (directories left out) |
| `tar` | A tar archive | Each regular file member, in archive order |
| `lines` | A text file | Each non-blank line |
| `arraylines` | A JSON array written one record per line (`[` / `,` / `]` around them) | Each line that holds an object, brackets and commas stripped. Reads a file of any size. |
| `array` | A JSON file whose top level is an array | Each item, already parsed. Parses the whole file in memory. |
| `dict` | A JSON file whose top level is an object | Each value, already parsed. Parses the whole file in memory. |
| `graph` | A JSON-LD document | Each item of its top-level `@graph` array, already parsed |
| `xmls` | An XML file | Each child element of the root, as XML text. Parses the whole file in memory. |
| `lmdb` | An LMDB distribution store (a directory holding `data.mdb`) | Each value whose key starts `<source name>:`; the rest of the key is the name |

Every container leaves out what operating systems put in directories and
archives: `__MACOSX/`, `._*`, `.DS_Store`, `Thumbs.db`, `desktop.ini`.

### Formats

| Format | Takes | Makes |
|---|---|---|
| `json` | A file, member or line holding JSON | The parsed JSON (a UTF-8 byte-order mark is dropped). Inside a container, only names ending `.json`, `.jsonl`, `.jsonc`, `.jsonld` or with no extension are read; the others are skipped and counted in a warning at the end, so the `.csv` or `README.txt` next to the records in a zip does no harm. A file an entry names itself is read whatever its name. |
| `raw` | An item already parsed by `array`, `dict` or `graph` | The item as it is |
| `tabjson` | A line `<identifier><TAB><JSON>`, as `lux export --type external` writes | The JSON after the tab; a record that is only `{"data": ...}` is unwrapped. The prefix is the name. |
| `other` | Anything else (XML, text) | `{"data": "<the text>"}`, for a loader whose `post_process_other` or mapper reads it |

### Compression

| Suffix | Meaning |
|---|---|
| `.gz` | gzip |
| `.bz2` | bzip2 |
| `.z` | zlib (as the LMDB distribution stores write values) |

On `zip`, the suffix names the archive's compression method (`zip.bz2` is how
the LUX export zips are written). On `tar` it is the compression of the whole
archive (`tar.gz` for a `.tgz`). On `lines`, `arraylines`, `array`, `dict`,
`graph` and `xmls` it is the file's compression. On a format it is the
compression of each record, for example a tar of `.json.gz` members, or
compressed LMDB values.

### Examples

| Type | What it reads | Used by |
|---|---|---|
| `lines.gz/json` | Gzipped JSON Lines | DNB entityfacts, wikipedia |
| `lines/json` | Plain JSON Lines | IPCH |
| `tar.gz/json` | A gzipped tar of one JSON file per record | YCBA, YPM, PMC |
| `zip/json` | A zip of one JSON file per record | YUAG |
| `zip.bz2/json` | A LUX datacache export | Every `export` entry |
| `zip/arraylines/json` | A zip of JSON arrays written one record per line | BNE |
| `zip/array/raw` | A zip of one big JSON array | ROR, BNE materia |
| `graph.gz/raw` | A gzipped JSON-LD `@graph` | Pleiades places |
| `dir/lines.gz/json` | A directory of gzipped JSON Lines files | Yale library (with `"slice": "files"`) |
| `lines.gz/tabjson` | `lux export --type external` output | Re-loading an external export |
| `lmdb/json.z` | An LMDB distribution store | `lux load --type lmdb` |
| `tar.gz/lines.bz2/json` | A gzipped tar whose members are bzip2'd JSON Lines files | |
| `xmls.gz/other` | A gzipped XML file of sibling record elements | |

Some sources add a container of their own (DNB's `sachbegriff`, BnF's
`entities`, Pleiades' `ttl`); those names only work with that source's
`loaderClass`.


## Identifiers

Each record is stored under an identifier: its datacache key, and the part
of its URI the mapper adds to the source's `namespace`. An entry says where
the identifier comes from with its `identifier` key. Set it alongside `type`:
a `type` that reads the records correctly can still key them wrongly, and
nothing complains until reconciliation fails to find them. Run a
`--max-records` load and look at the keys (`lux show --sample`) before a
full load.

### Syntax

```
"identifier": "name"
"identifier": "record"
"identifier": {
  "from":   "name" | "record",          // default "record"
  "fields": ["<field>", ...],           // from "record" only; default ["id"]; a string is one field
  "form":   ["<step>", ...]             // default ["last", "extension"]; a string is one step
}
```

| Value | Means |
|---|---|
| no `identifier` key, or `"name"` | The default rule below |
| `"record"` | `{"from": "record", "fields": ["id"], "form": ["last", "extension"]}` |
| `{"from": "name", ...}` | The file or member name (for `tabjson`, the line's prefix), shaped by `form` |
| `{"from": "record", ...}` | The first of the record's top-level `fields` that is present and not empty, shaped by `form` |

The `form` steps, applied in the order given:

| Step | Does |
|---|---|
| `namespace` | Strips the source's `namespace` from the front. If a value is not under it, the record is skipped, logged and counted in a warning at the end. Only for record values and `tabjson` prefixes, and only on a source that has a `namespace`. |
| `last` | Keeps the last `/` segment |
| `extension` | Strips one trailing `.gz` or `.bz2`, then one of `.json`, `.jsonld`, `.xml`, `.rdf`, `.ttl`, `.nt`, `.txt`. Any other dots stay (`1.2.3` keeps them). |

What counts as a name:

* a file named by the entry, or found by `dir` / `dirs` / `pair`: its own
  file name, without its directory;
* an archive member (`zip`, `tar`): its full path inside the archive, so
  `records/1.2.3.json` and not `1.2.3.json`. Keep `last` in the `form` to
  drop the directories;
* a `tabjson` line: the text before the tab, often a URI;
* an `lmdb` value: its key after `<source>:`;
* a line from `lines` or `arraylines`, or an item from `array`, `dict` or
  `graph`: none. These are always identified from the record.

### The default rule

With no `identifier` key (or `"name"`):

* a record with a name gets the name's last segment, **cut at the first
  `.`**: `records/1.2.3.json` gives `1`;
* a record without one gets its `id` field, treated the same way:
  `https://example.org/data/1` gives `1`;
* a `tabjson` line gets its prefix exactly as written.

The default is wrong for identifiers with dots (they are cut short) or with
slashes (only the last segment is kept). Use a rule for those.

### Examples

For a source whose `namespace` is `http://example.org/data/` (each row was
checked against the loader):

| Rule | Input | Identifier |
|---|---|---|
| (none) | member `records/1.2.3.json` | `1` |
| `{"from": "name"}` | member `records/1.2.3.json` | `1.2.3` |
| `{"from": "name", "form": ["extension"]}` | member `records/1.2.3.json` | `records/1.2.3` (no `last`) |
| (none) | line `{"id": "https://example.org/data/1", ...}` | `1` |
| `"record"` | `{"id": "http://example.org/data/class/1.2.3.json"}` | `1.2.3` |
| `{"from": "record", "form": ["namespace", "extension"]}` | `{"id": "http://example.org/data/class/1.2.3.json"}` | `class/1.2.3` |
| `{"fields": ["@id", "uri"], "form": ["namespace"]}` | `{"@id": "http://example.org/data/p/9"}` | `p/9` |
| `"record"` | `{"@id": "http://example.org/data/p/9"}` (no `id`) | fails: `Could not get an identifier` |
| (none) | tabjson line `http://example.org/data/class/1<TAB>{...}` | `http://example.org/data/class/1` |
| `{"from": "name", "form": ["namespace"]}` | the same line | `class/1` |
| `"record"` | the same line | from the record's `id`; the prefix is ignored |

A complete entry:

```json
"input_files": {
  "records": [
    {"url": "https://example.org/downloads/records.tar.gz",
     "path": "records.tar.gz",
     "type": "tar.gz/json",
     "identifier": {"from": "record", "fields": ["id", "@id"],
                    "form": ["namespace", "extension"]}}
  ]
}
```

### What is refused

A rule is checked when the loader is prepared, before any record is read.
These stop the load with a message naming the source:

* an `identifier` that is not `"name"`, `"record"` or an object;
* keys other than `from`, `fields` and `form`;
* `from` other than `name` or `record`;
* `fields` with `"from": "name"`, or `fields` that are not non-empty field names;
* a `form` step other than `namespace`, `last` and `extension`;
* `namespace` with `"from": "name"`, except for `tabjson`;
* `namespace` on a source with no `namespace`;
* `"from": "name"` with the `raw` format (raw items have no name).

At load time, a record for which no identifier can be found (none of its
`fields` is set) fails its file: the error is logged, the source's other
files are still loaded, and the worker fails at the end.

### With `--file` and loader classes

`lux load --file` keeps the configured rule when the source has exactly one
entry for that load type; with several entries the default rule applies to
the replacement file. A `loaderClass` that overrides `extract_identifier`
decides record identifiers itself (DNB, BnF, Pleiades, IPCH, Wikipedia): a
rule on its entries only chooses between the record and the name.


## Slicing

`lux load` runs `--max-workers` workers (default `max_workers` in the base
config). Each record goes to exactly one of them.

* `"slice": "records"` (the default): every worker reads every file and
  keeps every Nth record. That suits one big file, but every worker
  decompresses all of it.
* `"slice": "files"`: each worker takes every Nth file of the type's first
  directory step (`dir`, `dirs`, `pair`) and every record in it, so each file
  is read once. Use it for a dump of many large compressed files, as the
  Yale library does with `{"path": "yul", "type": "dir/lines.gz/json", "slice": "files"}`.
  For an entry with no directory step, the entry itself is the unit: with
  four such entries and four workers, each worker loads one file. Files are
  counted in order across the entries.

Both ways depend on every worker seeing the children in the same order, which
is why directory listings are sorted.


## `lux guess-dump-type`

`lux guess-dump-type` prints the `input_files` block that describes a
source's dump files. It reads only the start of each file, never changes the
config, and needs no configuration at all with `--path`.

```
lux guess-dump-type --source ror               # scan ror's dumps directory
lux guess-dump-type --source all               # every source with a dumps directory
lux guess-dump-type --path /data/input/dnb     # a directory or file, no config needed
lux guess-dump-type --source ror --explain     # the evidence behind each guess
lux guess-dump-type --source ror --type export # file the entries under "export"
```

With `--source` it scans the source's `dumps_dir`. Without one, it scans the
directory `dumpFilePath` is in, or only `dumpFilePath` itself when that is
directly in the shared dumps directory. Every file in the directory is one
entry, and so is every sub-directory (`dir`, `dirs` or `lmdb`); paths are
relative to the directory, as the loader resolves them.

Only the JSON goes to stdout, so it can be piped or pasted. Everything else
goes to stderr. For example:

```
$ lux guess-dump-type --path dumps --explain
array.json: arraylines/json  magic=5b 0a 7b 22 69 64 22 3a  head=[\n{"id":"a1","n":1},...  keys=['id']
parts: dir/lines.gz/tabjson  member=export_ex.jsonl.gz  head=https://example.org/data/1	{"data":...  keys=identifier<TAB>JSON lines
records.jsonl.gz: lines.gz/json  magic=1f 8b 08 00 ...  head={"id":"https://example.org/data/1",...  keys=['id']
recs.zip: ignored member recs/README.csv
recs.zip: zip/json  magic=50 4b 03 04 ...  member=recs/x.json  head={"id":"x"}\n  keys=['id']
triples.nt: cannot place: N-Triples
"input_files": {
  "records": [
    {"path": "array.json", "type": "arraylines/json"},
    {"path": "parts", "type": "dir/lines.gz/tabjson"},
    {"path": "records.jsonl.gz", "type": "lines.gz/json"},
    {"path": "recs.zip", "type": "zip/json"}
  ]
}
```

(The real output indents the JSON one key per line.)

Reading it:

* **`keys=`** lists the identifier-like fields (`id`, `@id`, `uri`, `about`) of
  the first record, or its first few keys when it has none of those. If
  there is no `id`, you need an [`identifier` rule](#identifiers) with
  `fields`, or a loader that overrides `extract_identifier`.
* **`head=`** is the start of the content after decompression; check that it
  looks like a record.
* **`member=`** is the archive member or file the guess for a container was
  based on. Up to five members are checked; when they disagree, a note says
  so and the majority wins.
* **`ignored member`** lines are archive members the guesser did not open
  because their names are not JSON or XML. With a `json` format the loader
  skips them too. With `other` or `raw` it does not, and the source needs a
  loader with a `should_process_item` override (as ROR has).
* **`note:`** lines are things to check: one JSON object of 64 MB or more
  (taken as one record), an array of 1 GB or more read as `array/raw` (parsed
  whole in memory, since it is not written one record per line), a `dir`
  with sub-directories it will not read, or a JSON Lines file of one line.
* **`cannot place`** means the loader cannot read the file as it is:
  N-Triples, CSV or TSV, a SQLite database, several XML documents in one
  file, an archive inside an archive, or JSON that does not parse. Such a
  file needs a loader of its own, or converting first. The command exits 1
  when any file cannot be placed.

What it can tell from the content: gzip, bzip2 and zlib compression; zip and
tar archives (compressed or not); LMDB stores and their value compression;
JSON Lines, `identifier<TAB>JSON` lines, JSON arrays (`arraylines` when
written one record per line, `array` otherwise), a single JSON record versus
an object of records (`dict/raw`), and XML (`xmls/other` when the root has
repeated children of one kind, `lines/other` for one document per line,
otherwise `other`).

What it cannot tell: identifiers (it shows you the keys, you choose the
rule), `slice`, whether a `.jsonld` file is a JSON-LD `@graph` (use `graph`),
or a source's own containers. Review those yourself.


## `lux load`

```
lux load --source example                         # load the records
lux load --source example --clear                 # empty the datacache first
lux load --source example --no-overwrite          # keep records already loaded
lux load --source all --type export               # every source's LUX export
lux load --source example --type lmdb             # from an LMDB distribution store
lux load --source example --max-workers 1 --max-records 100
lux load --source example --file /tmp/extra.jsonl.gz --file-type lines.gz/json
```

| Option | Meaning |
|---|---|
| `--type records\|export\|lmdb` | Which `input_files` load type to read (default `records`) |
| `--clear` | Empty each source's datacache once, before any worker starts |
| `--no-overwrite` | Skip records whose identifier is already in the datacache. One database lookup per record, so slower. |
| `--file PATH` | Read this file or directory instead of the configured one, for one run (exactly one `--source`) |
| `--file-type TYPE` | The `--file`'s type, when it differs from the configured one |
| `--max-records N` | Stop each worker after N records; with `--max-workers 1`, a quick sample |
| `--max-workers N`, `--engine`, `--prefix`, `--background`, `--no-ui` | As for every `lux` worker command (`lux help load`) |

`--file` without `--file-type` takes the type the file's content shows (as
`lux guess-dump-type` would), and failing that the configured entry's type.
When the source has exactly one entry for the load type, that entry's
`identifier` rule still applies, so a replacement file is keyed the same way
as the configured one, and so does `"slice": "files"` if the replacement is
read through a directory step. This is how to load a
one-off delivery, or a `lux export --type external` file, into a source whose
configured dump has another shape.

`--type export` with no `export` entry falls back to the older export loader:
`<dumps_dir>/harvested/export_<name>.jsonl.gz`, read by worker 0 alone.

At the end of each worker, the log says how many files the `json` step skipped
for their names and how many records fell outside the namespace. If a file
failed to load, the error is logged, the other files are still loaded, and the
worker then fails and names the files that failed.


## When the DSL is not enough

Set `loaderClass` to a subclass of `lux_pipeline.process.base.loader.Loader`
and override only what differs. Each hook works with any type:

| Hook | Use it to |
|---|---|
| `post_process_json(self, data)` (or `(self, data, identifier)`) | Reshape a parsed record before it is stored; return `None` to drop it (Wikipedia, LC, Ashmolean) |
| `post_process_other(self, text)` | The same for an `other` record |
| `extract_identifier(self, data)` | Work out an identifier no rule can express (DNB, BnF, Pleiades) |
| `should_process_item(self, child)` | Skip container children, e.g. the files beside the records in an archive (ROR, the Yale library) |
| `json_extensions` | Widen or narrow the names the `json` step reads in a container |
| `extra_containers` + `step_functions` | Add a container step of your own |

A new container is a generator that is given the parent (`path`), its
compression and the remaining steps, and yields children for the next step.
For example, DNB's:

```python
class DnbLoader(Loader):
    extra_containers = ("sachbegriff",)

    def _init_dsl(self):
        super()._init_dsl()
        self.step_functions["sachbegriff"] = self.iterate_sachbegriff

    def iterate_sachbegriff(self, path, comp, remaining):
        for member in self.iterate_array(path, comp, remaining):
            ...
            yield {"list": [item]}
```

used as `"type": "sachbegriff.gz/json"`. Register steps in `_init_dsl`, not
`__init__`, and do not open files in `__init__`: every source's loader is
built whenever the configuration is instantiated.

A loader that overrides `load()` and whose config has no `input_files` or
`dumpFileType` takes the legacy path: `lux load` calls its `load()` and the
DSL is not used. Do not write new loaders that way.


## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `Cannot process container type 'x'` / `format type` / `compression type` | A misspelled step, or a source's own container used without its `loaderClass` |
| `Could not get an identifier` | The record has no `id`: add an `identifier` rule with `fields`, or override `extract_identifier` |
| Every record has the same short identifier, or identifiers lose their dots or slashes | The default rule cut at the first `.` or kept only the last segment: add an `identifier` rule |
| `skipped N file(s) whose names are not JSON` | Expected for a README or CSV next to the records; if it is the records themselves, they need a name with a JSON extension, or `json_extensions` widened |
| `N record(s) skipped: their identifier is not under the namespace` | The `namespace` step met values outside the source's `namespace`: check the namespace, or drop the step |
| `... is not JSON: ...; it starts b'...'` | The type is wrong for the file: check it with `lux guess-dump-type --path <file> --explain` |
| `Cannot guess the type of ...; set 'type' in input_files` | An entry without `type` whose content the guesser cannot place |
| A worker runs out of memory | `array`, `dict`, `graph` or `xmls` parse the whole file; if the JSON array is written one record per line, use `arraylines` |
| Loading takes N times as long as reading the files once | A dump of many compressed files sliced by record: use `"slice": "files"` |
