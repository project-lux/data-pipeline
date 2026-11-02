# W04. Config store, key catalogue and `lux config`

**Size:** M · **Depends on:** — · **Unblocks:** W07
**Read:** `lux_pipeline/config.py` (`Config.__init__` from the bootstrap
`FsCache` down to the sub-config loop, `handle_config_record`,
`process_base`, `instantiate`, `instantiate_map`, `instantiate_ml`,
`instantiate_config`, `load_subconfig_store`, `resolve_indexes`,
`importObject`), `lux_pipeline/storage/cache/filesystem.py` (`FsCache._path`,
`set`, `iter_keys`), `lux_pipeline/cli/initialize.py` (`write_json`,
`record_file`), `lux_pipeline/cli/_services.py`, `refactor_docs/04`, `34`,
`35`, `42` (the `build` record type that is coming), `tests/test_config_records.py`,
`tests/test_subconfig_stores.py`

## Intent

Three things, one PR:

1. `Config` remembers where every record came from
   (`record_origins[key] = {"store": None | <sub-config key>, "path": "/abs/file.json"}`).
2. `lux_pipeline/config_store.py` reads, validates, diffs, writes, creates
   and deletes configuration records **in the files**, atomically, in the
   format the files use (4-space indent, trailing newline, `ensure_ascii=False`).
3. `lux_pipeline/config_schema.py` is a catalogue of the keys each record
   type is known to carry, with type and help, for forms and `lux config check`.

Plus a small command, `lux config`, so all of it is usable and testable
from the shell before the web exists:

```
lux config list                              # key, type, file for every record
lux config show aat                          # the file's JSON
lux config check [aat]                       # validate_record problems for one or all
lux config set aat merge_order 12            # one key (JSON value; strings may be bare)
lux config unset aat cycleBreakPath
lux config edit aat                          # $EDITOR on a temp copy, validate, write
lux config diff aat < new.json               # what a write would change
lux config new wd2 --type external --store sources < rec.json
lux config delete wd2 --force
```

## Files

| Action | Path | Notes |
|---|---|---|
| Edit | `lux_pipeline/config.py` | `record_origins`; `handle_config_record(k, rec, origin=None)`; `load_subconfig_store` passes the origin; the main loop passes `{"store": None, "path": configcache._path(k)}` |
| Create | `lux_pipeline/config_store.py` | |
| Create | `lux_pipeline/config_schema.py` | |
| Create | `lux_pipeline/cli/config.py` | `lux config` (`BaseHandler`; `needs_configs = True`; refuses `set/unset/edit/new/delete` while a build runs, `--force`) |
| Create | `tests/test_config_store.py`, `tests/test_config_schema.py`, `tests/test_cli_config.py` | |
| Edit | `CLAUDE.md` table, `docs/codebase.md` | one row / one paragraph each |

## Design

### Origins in `Config`

`handle_config_record(self, k, rec, origin=None)`: when `origin` is given,
`self.record_origins[k] = origin`. The main-directory loop passes
`{"store": None, "path": os.path.abspath(configcache._path(k))}` (the
bootstrap cache is an `FsCache`; a `Config` built with a `configcache=`
argument that is not an `FsCache` passes `path=None`).
`load_subconfig_store(key)` passes `{"store": key, "path": cache._path(k)}`
when the store's cache has `_path`. The `base`/`system` record is read
before the loop: record its origin too (`self.record_origins["base" or "system"]`).
Also keep `self.base_record_key` (`"system"` or `"base"`, whichever was
found) — `write_record` needs to know which file the base settings live in.

### `config_store.py`

```python
class ConfigError(ValueError): ...

def record_path(cfgs, key) -> str                     # from record_origins; ConfigError if unknown or path None
def read_record(cfgs, key) -> dict                    # json.load of the file (NOT cfgs.internal[key]: that dict has live objects in it)
def all_records(cfgs) -> list[dict]                   # [{"key", "type", "name", "store", "path"}] in Config's load order
def validate_record(cfgs, key, data, *, new=False) -> list[str]
def diff_record(cfgs, key, data) -> str               # difflib.unified_diff(file text, dumps(data)) with the path as both names
def write_record(cfgs, key, data) -> str              # validates (raises ConfigError listing problems), writes tmp + os.replace, returns path
def new_record(cfgs, key, data, store=None) -> str    # path = main dir or sub-config store's cache._path(key); ConfigError if it exists
def delete_record(cfgs, key) -> str                   # os.remove; returns path
def dumps(data) -> str                                # json.dumps(indent=4, ensure_ascii=False) + "\n"  (initialize.write_json's format)
def set_key(data, dotted, value) / unset_key(data, dotted)   # "indexes.reconcile_label.path"; value parsed as JSON, falling back to the bare string
```

`validate_record` checks, each a message in the list:

1. `data` is a dict with a `type` that `handle_config_record` accepts
   (`internal external results caches default globals base system marklogic map config build`;
   keep the list in `config_schema.RECORD_TYPES` so `build` is ready for item 42).
2. for `internal/external/results/map/marklogic/config`: `name` present
   and equal to `key` (that is how every lookup works), unless the type's
   convention differs (`caches` has `name: "_caches"`; `default` has
   `name` = kind).
3. every key ending in `Class` whose value is a string resolves through
   `importObject` (catch `Exception`, report `"<key>: <error>"`); a value
   that is `None` is fine (it switches the component off). A legacy path
   (`_modernize_object_type` changes it) is reported as a warning-level
   message prefixed `warning:`; `write_record` writes anyway when only
   warnings remain, and returns them for the caller to show.
4. `indexes`, if present: `Config.resolve_indexes` on a **deep copy** of
   the record with a throwaway `Config`-like object (it needs
   `indexes_dir`; pass `cfgs`) raises nothing.
5. for `base`/`system`: every key in `path_types` names a `*_dir` attribute
   that exists in the record or has a default; `max_workers` is a positive
   int if present.
6. `new=True`: `key` is not in `record_origins` and the target file does
   not exist.
7. anything the schema (below) marks `type: int|bool|list|dict` that is
   present with the wrong JSON type: a message.

It never rejects an unknown key: sources carry keys only their mapper
reads.

### `config_schema.py`

```python
RECORD_TYPES = (...)
KEYS = {
  "base": {                                     # also used for "system"
     "base_dir": Key("path", "Root of the installation; relative *_dir keys resolve under it", attr="base_dir"),
     "data_dir": Key("path", "...", attr="data_dir"), "log_dir": ..., "exports_dir": ..., "nt_dir": ..., "distribution_dir": ..., "backup_dir": ..., "temp_dir": ..., "indexes_dir": ..., "dumps_dir": ..., "tests_dir": ...,
     "max_workers": Key("int", "Workers a parallel phase starts by default (cores - 3 when unset)"),
     "idmap_name": Key("str", ...), "internal_uri": Key("str", ...), "validatorClass": Key("class", ...),
     "allow_network": Key("bool", ...), "debug_reconciliation": Key("bool", ...), "max_distance": Key("int", ...), "reconcile_min_equivs": Key("int", ...),
     "ok_record_types": Key("dict", ...), "parent_record_types": Key("dict", ...), "reconcile_record_types": Key("list", ...),
     "do_not_reidentify": Key("list", ...), "other_external_matches": Key("list", ...), "external_uri_rewrites": Key("dict", ...),
     "path_types": Key("dict", ...), "activitystreams_cache_source": Key("str", ...), "validate_allow_underscore_properties": Key("bool", ...),
     "web": Key("dict", "lux serve defaults: host, port (item W06)"),
  },
  "caches": {"host", "port", "user", "password", "dbname", "connect", "overwrite", "change_types", "refresh_delta", "cursor_size", "export_path"},   # with types/help
  "source": {   # internal / external / results share it; the schema says which roles use a key
     "name", "type", "namespace", "matches", "fetch", "activitystreams", "wikidata_property", "merge_order",
     "harvesterClass", "mapperClass", "loaderClass", "downloaderClass", "fetcherClass", "acquirerClass", "reconcilerClass", "indexLoaderClass", "testerClass",
     "datacacheClass", "recordcacheClass", "recordcacheReconciledClass", "recordcache2Class",
     "reconcileDbPath", "inverseEquivDbPath", "differentDbPath", "indexes", "cycleBreakPath", "mapSizeExponent",
     "dumpFilePath", "dumpFileType", "remoteDumpFile", "input_files", "totalRecords", "networkmap_name",
     "validate_on_acquire", "raise_on_validation_error", "ignore_sources_for_records", "debug",
     "idmap_name", "trash_non_yale_images",
  },
  "map": {"name", "type", "storeClass", "tableName", "host", "port", "db", "prefix_map_out", "masterMap", "base_dir"},
  "marklogic": {"name", "type", "storeClass", "host", "port", "user", "password", "database", ...},   # read storage/marklogic/rest.py for the real keys
  "globals": {"idmap_name", "*": Key("str", "label -> AAT id (3...), Wikidata Q-id, or a URI")},
  "config": {"name", "type", "base_dir", "datacacheClass", "tabletype"},
  "default": {"name", "type", "*": "the kind's keys"},
  "build": {"name", "type", "internal", "external", "exclude"},     # refactor_docs/42; marked "planned"
}
class Key: type, help, attr=None, roles=("internal","external","results"), planned=False
def keys_for(record_type) -> dict[str, Key]
def known(record_type, key) -> Key | None
def template(record_type, name) -> dict      # a minimal new record: name, type, and the keys with sensible defaults
```

Find each key's reader with `grep -n '"<key>"\|\.<key>\b' lux_pipeline/` and
write the help from what the code does, one sentence. Keys whose only
reader is a source's own mapper are **not** in the catalogue; the form
shows them in the JSON tab. The catalogue says which `*Class` key defaults
to which class (from `config.instantiate`), so the form can show the
default when the key is absent.

### `lux config`

A `BaseHandler` with a `subcommand` positional and `key`, `path`, `value`
positionals (`nargs="?"`), `--type`, `--store`, `--force`. `edit` uses
`$EDITOR` / `$VISUAL` / `vi` on a temp file and writes only if the content
changed and validates. Mutating subcommands call
`refuse_while_building(cfgs, "config", args.force)` first. Output: `show`
and `diff` print text; `list` a table (Rich when a TTY, as `lux list`);
`check` one line per problem and exit 1 when a record has a non-warning
problem.

## Known hazards

* **Do not write the live dict.** `cfgs.internal["aat"]` holds the
  resolved absolute paths, the instantiated components and `all_configs`.
  `read_record` reads the file.
* **Sub-config stores** read records in key order and a later record
  replaces an earlier one with the same key. `record_origins` keeps the
  winner; `new_record` into a store whose key is already in the main
  directory must refuse (it would be shadowed or shadow).
* `Config.__init__` reads `base` **before** the loop; `handle_config_record`
  is not called for it (`process_base` is). Record its origin separately.
* `FsCache._path` maps `/` in keys to `___`; configuration keys have no
  slashes, but use `_path` anyway rather than joining by hand.
* Reloading: after a write the caller must `runtime.reset_configs()` and
  build a new `Config`. `lux config` exits, so it never needs to; the web
  (W07) does. Say so in `write_record`'s docstring.
* Do not touch `config_cache/config_cache/` in this item unless you add a
  key; you add none. `lux config check` run over those copies is a good
  smoke test (`LUX_BASEPATH=$PWD/config_cache lux config check`) and
  should report only the legacy-path warnings `CLAUDE.md` mentions.

## Steps

1. `record_origins` and `base_record_key`; tests in `test_config_records.py`
   (a `Config` over a tmp dir with two records and one sub-config store:
   origins point at the right files).
2. `config_store.py` with `dumps`, `read_record`, `all_records`,
   `diff_record`, `write_record`, `new_record`, `delete_record`, `set_key`.
3. `config_schema.py`; then `validate_record` using it.
4. `lux config`; docs rows.

## Tests

`tests/test_config_store.py` over `tests/webui_fixtures.make_installation`
(W01) or, if W01 is not merged, a tmp dir with `docs/sample_config` copied
and `base.json` rewritten the same way (keep the helper local, then switch
to the fixture later):

* `read_record("aat")` equals `json.load` of the file; is not the same
  object as `cfgs.external["aat"]`.
* `write_record` round trip: file text equals `dumps(data)`; indent 4;
  trailing newline; no `.tmp` left; a `Config` rebuilt from the directory
  sees the change.
* `validate_record`: wrong `name`; unknown `type`; `mapperClass` that does
  not import; legacy `pipeline.sources.x.Y` path → a `warning:` message
  only; `indexes` with a bad entry; `max_workers: "24"` on base → type
  message.
* `new_record` into the main directory and into a sub-config store (make
  one in the fixture: `subs/sources_cache/` with a `config` record
  pointing at it); refusing an existing key.
* `delete_record` removes the file and a rebuilt `Config` no longer has it.
* `diff_record` output starts with `--- <path>` and shows the changed line.

`tests/test_config_schema.py`: every `RECORD_TYPES` entry has keys; every
`*Class` key in `KEYS["source"]` names a default class that imports;
`template("external", "x")` validates with `new=True`.

`tests/test_cli_config.py`: `show`, `set` (file changed, rest of the file
identical), `set` with invalid value refused with exit 1, `check` on the
installation reports nothing, `delete` without `--force` asks / refuses,
mutating subcommands refuse while a fake run (W01 fixture or a hand-written
status file updated now) is unfinished.

## Acceptance criteria

* Tests pass; `Config` builds identically (no output change in
  `lux list`, `lux counts`).
* `LUX_BASEPATH=$PWD/config_cache lux config check` on the build-machine
  copies reports only legacy-path warnings (or nothing).
* `lux config set aat merge_order 11 && lux config show aat` shows the
  change and `git diff` of that file (if it is a checkout) touches one line.
