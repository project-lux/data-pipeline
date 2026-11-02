# W00. A database-free test installation

**Size:** S · **Depends on:** — · **Unblocks:** W04, W05, W06 (and every page item's tests)
**Read:** `lux_pipeline/config.py` (`Config.__init__`, `process_base`,
`instantiate`, `instantiate_map`, `get_idmap`, `cache_globals`),
`lux_pipeline/storage/cache/filesystem.py`, `lux_pipeline/storage/idmap/filesystem.py`,
`tests/webui_fixtures.py` (W01's `write_fake_run`)

Split out of W01 on 2026-11-02, with Rob: W01's tests need only fake run
files, while W04, W05 and the web items need a real `Config`.

## Intent

Add to `tests/webui_fixtures.py` a function that writes a small, complete
installation into a temporary directory, so a test can build a real
`Config` and call `instantiate_all()` with no Postgres, no Redis and no
network:

```python
from webui_fixtures import make_installation, make_configs, seed_records
root = make_installation(tmp_path)            # config_cache/*.json, data dirs, token file, .env
cfgs = make_configs(tmp_path, instantiate=True)
keys = seed_records(cfgs)                      # a few records in the caches and the idmap
```

## Do not copy configuration from anywhere

* **Not `docs/sample_config`.** It is out of date: its `base.json` has no
  `idmap_name`, `wikidata.json` names `RawWdIndexLoader` and `merged.json`
  `CsvReconciler`, neither of which exists. Do not fix it as part of this
  item (Rob: it is not maintained).
* **Not `config_cache/config_cache/`.** Those are the build machine's live
  files, untracked and absent from any other checkout.

Write the records in `tests/webui_fixtures.py` itself, using the live
copies only as a guide to which keys a record needs.

## Design

A prototype of this built and passed `instantiate_all()` in 0.4 s
(2026-11-02). Records, as Python dicts written to
`<tmp>/config_cache/<key>.json`:

| Key | What it needs |
|---|---|
| `base` | `type: base`, `base_dir: <tmp>`, `max_workers: 2`, **`idmap_name: "idmap"`**, the dirs relative to base_dir (`data_dir: data/files`, `log_dir: data/logs`, `exports_dir`, `indexes_dir`, `dumps_dir`, `temp_dir`, `tests_dir`), `internal_uri`, `allow_network: false`, `debug_reconciliation: false`, `max_distance: 3`, `validate_allow_underscore_properties: true`, `ok_record_types` (at least HumanMadeObject, Type, Person, Group, Place, LinguisticObject, Set), `parent_record_types: {}`, `reconcile_record_types`, `path_types` (`reconcileDbPath: indexes_dir`, `dumpFilePath: dumps_dir`), `other_external_matches: []`, `external_uri_rewrites: {}`, `do_not_reidentify: []` |
| `caches` | `name: _caches`, `type: caches`, **`base_dir: <tmp>/data/files`** (merged under every cache config; the filesystem caches need it), no host / user / dbname |
| `globals` | `type: globals` and nothing else (`cache_globals` then has nothing to look up) |
| `ycba` (internal) | `namespace`, `matches`, **`fetch`** (the acquirer needs a fetcher), `merge_order`, the filesystem classes `storage.cache.filesystem.DataCache`, `InternalRecordCache`, `RecordCache` (recordcache2) |
| `aat` (external) | as ycba with `ExternalRecordCache`, `ReconciledRecordCache` (`recordcacheReconciledClass`), `mapperClass: sources.getty.mapper.AatMapper` |
| `merged` (results) | `MergedRecordCache`, `DataCache`, `RecordCache`, `acquirerClass: null`; no `mapperClass` (the real `Cleaner` needs a `wikimedia` source) and no reconciler |
| `map_idmap` | `type: map`, `name: idmap`, `storeClass: storage.idmap.filesystem.IdMap`, `base_dir: <tmp>/data/files` |
| `map_networkmap`, `map_all_refs`, `map_done_refs`, `map_redirects` | `storeClass: storage.idmap.filesystem.FsMap`, `base_dir` as above |

Also: create every directory; write `<tmp>/data/files/idmap_update_token.txt`
containing `__20260101__` (the filesystem `IdMap` raises without a
well-formed token); write `<tmp>/.env` with `LUX_BASEPATH=<tmp>`.

```python
def make_installation(tmp_path, sources=("ycba", "aat")) -> Path
def make_configs(tmp_path, instantiate=False) -> Config   # Config(basepath=...); with instantiate, get_idmap + cache_globals + instantiate_all
def seed_records(cfgs) -> dict
    # ycba: one datacache row and its recordcache row (bare key, HumanMadeObject)
    # aat: one datacache row and its recordcache row under "300033618##quaType"
    # idmap: both URIs (typed keys) -> one YUID each; merged: one record per YUID
    # returns {"ycba": {"identifier", "uri", "yuid"}, "aat": {...}}
```

Callers that need `runtime.get_configs()` to find the installation set
`LUX_BASEPATH` with `monkeypatch.setenv` and call `runtime.reset_configs()`
before and after.

## Tests (`tests/test_webui_fixtures.py`)

* `make_configs(tmp, instantiate=True)` builds; `internal`, `external`,
  `results` are `ycba`, `aat`, `merged`; every cache is a filesystem cache
  under `<tmp>/data/files`; nothing was written outside `tmp_path`.
* `seed_records` round trip: each seeded row reads back from its cache,
  `cfgs.get_idmap()` resolves each URI to its YUID, the merged record is
  found under the YUID.
* `cfgs.split_uri("http://vocab.getty.edu/aat/300033618")` resolves to
  `aat`.
* Building twice in one test session (two `tmp_path`s) does not share
  state (`runtime.reset_configs()`).

## Acceptance criteria

* Tests pass; no file under `docs/sample_config` or `config_cache/` read
  or changed; the fixture builds in well under a second.
