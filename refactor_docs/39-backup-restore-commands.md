# 39. `lux backup` and `lux restore`

**Priority:** P2 · **Size:** L · **Depends on:** 02, 05, 08, 09 · **Unblocks:** 33 (retires `backup_tables.sh`)
**Read from:** nothing to port. Neither branch has this. The precedents on
`main` are `backup_tables.sh` (a hand‑maintained list of five `pg_dump`
lines), `IdMap.dump_members()` and the test‑only `_export_state()` /
`_import_state()` pairs in `lux_pipeline/storage/idmap/{postgres,redis,memory}.py`
and `lux_pipeline/storage/cache/redis.py`, and `migrate-idmap.py` (a
COPY‑based bulk load of the Postgres idmap with the indexes built after).

## Intent

Back up and restore any store the configuration names, whatever backend it
is on, through one command and one on‑disk layout:

```
lux backup --map idmap                        # before `lux clear --map idmap --force`
lux backup --source aat --cache datacache     # what backup_tables.sh does today
lux backup --index all                        # the lmdb reconcile indexes
lux backup --all                              # every cache, map and index (not MarkLogic)
lux backup --all --portable                   # backend-independent format, for moving stores
lux backup --map all_refs,done_refs --verify  # count and sample after writing

lux restore <dir> --list                      # show the manifest, restore nothing
lux restore <dir> --map idmap --force         # the idmap always needs --force, as in clear
lux restore <dir> --source aat --cache datacache
```

`lux clear` is a `TRUNCATE` or a file delete: nothing on `main` can undo it.
The pipeline has four backends (Postgres, Redis, LMDB, the filesystem) and a
backup that only shells out to `pg_dump` covers one of them, needs OS‑level
credentials and a matching server version to restore, cannot report
progress, and leaves the other three with nothing. The store classes know
their own fastest exact copy; the command should ask them.

## Why it matters

* The idmap is the one store that cannot be rebuilt from the sources: a
  cleared idmap is new YUIDs for every entity. Item 09 made clearing it a
  one‑liner.
* Data caches are weeks of harvesting. `backup_tables.sh` lists five of
  them by table name and goes stale every time a source is added.
* The reconcile indexes take hours to load and are cheap to copy.
* Moving a store between backends (Redis → Postgres was `migrate-idmap.py`,
  a one‑off) becomes `backup --portable` then `restore`.

## Files

| Action | Path on `main` | Notes |
|---|---|---|
| Create | `lux_pipeline/storage/backup.py` | the protocol, the portable format, the manifest |
| Edit | `lux_pipeline/storage/cache/postgres.py` | `backup()` / `restore()` on `PooledCache` via COPY |
| Edit | `lux_pipeline/storage/idmap/postgres.py` | `backup()` / `restore()` on `IdMap` (two tables) and `ReferenceMap` |
| Edit | `lux_pipeline/storage/idmap/lmdb.py` | `backup()` via `Environment.copy`, `restore()` |
| Edit | `lux_pipeline/storage/idmap/redis.py`, `memory.py`, `filesystem.py`, `lux_pipeline/storage/cache/redis.py`, `filesystem.py` | `iter_items()` where missing; the portable default does the rest |
| Edit | `lux_pipeline/storage/idmap/subsidiary.py` | `backup()` raises, pointing at the master |
| Create | `lux_pipeline/cli/_targets.py` | the source / map / index resolution that `counts` and `clear` each carry today |
| Edit | `lux_pipeline/cli/counts.py`, `clear.py` | use `_targets`; no behaviour change, their tests stay green |
| Create | `lux_pipeline/cli/backup.py`, `lux_pipeline/cli/restore.py` | the two commands |
| Edit | `config_cache/config_cache/base*.json` (untracked copies) | optional `backup_dir` |
| Create | `tests/test_storage_backup.py`, `tests/test_cli_backup.py` | |

## Design

### 1. The store protocol (`lux_pipeline/storage/backup.py`)

Every store class gets two methods and one attribute. A class that does not
override them gets the portable implementation from the module.

```python
backup_format = "jsonl"          # or "pg-copy", "lmdb-copy": what backup() writes

def backup(self, dest_dir, progress=null_progress) -> dict:
    """Write this store under dest_dir; return its manifest entry."""

def restore(self, src_dir, entry, progress=null_progress) -> int:
    """Load the files entry names from src_dir into this (emptied) store;
    return the row count."""
```

Nothing here imports `rich` or knows about engines (ground rule 11);
`progress` is called with the keyword arguments item 02 defines (`advance=`, `total=`, `completed=`, `description=`), one call per chunk.

**Native implementations**, chosen because each is the backend's own exact
copy and runs through the connection the store already holds:

| Backend | Classes | `backup()` | `restore()` |
|---|---|---|---|
| Postgres | `cache/postgres.PooledCache`, `idmap/postgres.IdMap`, `idmap/postgres.ReferenceMap` | `COPY <table> TO STDOUT` through `cursor.copy_expert`, text format, gzip‑compressed, one file per table; the manifest records the table and column list. `IdMap` copies both `<table>` and `<table>_yuid` inside one `REPEATABLE READ` transaction so the pair is consistent. | `TRUNCATE` then `COPY <table> (<columns>) FROM STDIN` in **one transaction**, so a failed restore leaves the previous content. `IdMap` then clears `memory_cache` and calls `ensure_covering_indexes()`. For a large restore into an empty idmap follow `migrate-idmap.py`: drop the indexes, COPY, rebuild (several times faster at 100M rows). |
| LMDB | `idmap/lmdb.Lmdb` and subclasses (the reconcile indexes) | `self.env.copy(path, compact=True)`: LMDB's hot backup, consistent under concurrent readers. Open the index with `TabLmdb.open(path, "r")` as the reconciler does. | `remove_lmdb(path)` then copy the backup file in; the next `open` finds it. |
| Redis | `idmap/redis.*`, `cache/redis.RedisDataCache` | portable (below). Per‑key `DUMP`/`RESTORE` was considered and rejected: the payload is tied to the Redis version and the set‑valued classes (`MultiMap`, `TransitiveMultiMap`, `NetworkOperationMap`) each need their own logical value anyway. | portable |
| Filesystem | `cache/filesystem.FsCache`, `idmap/filesystem.FsMap` | portable. They are already files, but a tar would couple the backup to the directory layout. | portable |
| Memory | `idmap/memory.IdMap` | portable | portable |
| Subsidiary idmap | `idmap/subsidiary.IdMap` | **raises**: `supports_member_dump = False`, its table is a partial view of the master. The message names the master map. | raises |
| MarkLogic | `marklogic_stores` | out of scope; MarkLogic has its own backup | — |

**The portable format** is what `--portable` forces for every store and what
the backends without a native copy use: gzipped JSON Lines, one object per
key, `{"k": <key>, "v": <value>}`, where the value is exactly what the class
hands back for that key (a `str` for an idmap entry, a `list` for a
multi‑map, the record dict with its metadata for a cache). It is produced
from `iter_items(chunk=...)`, which `idmap/postgres.ReferenceMap` and
`idmap/redis.ReferenceMap` already have and `lmdb.Lmdb` has as `items()`;
add it to the classes that only have `iter_keys()` (read‑only, no behaviour
change). Restore goes through `__setitem__` / `set()` in chunks with a
`commit()` per chunk, which is what every loader on `main` does.

A portable backup restores into any class of the same kind (an idmap into
an idmap, a cache into a cache); a native one only into the same class.

### 2. The manifest

`<backup_dir>/<label>/manifest.json`, written last so a half‑finished backup
has no manifest and `restore` refuses it:

```json
{"version": 1, "created": "...", "host": "...", "git": "<sha>", "label": "...",
 "entries": [
   {"name": "idmap", "kind": "map", "store_class": "storage.idmap.postgres.IdMap",
    "format": "pg-copy", "files": ["idmap.copy.gz", "idmap_yuid.copy.gz"],
    "rows": 97000000, "bytes": 0, "seconds": 0.0,
    "extra": {"table": "idmap", "columns": ["uri", "yuid"]}},
   {"name": "aat", "kind": "cache", "cache": "datacache", ...},
   {"name": "aat", "kind": "index", "index": "name", "format": "lmdb-copy", ...}
 ]}
```

`kind` is `cache`, `map` or `index`; `name` is the source or map name, the
same words the CLI options take. Everything the storage layer returns from
`backup()` goes under `extra`.

### 3. The commands

Both are `BaseHandler`s (one process, sequential over stores, a line per
store on stdout as the command's result; `progress` prints a counter when
stdout is a tty). Running each store as a task under the item 07 engines so
`lux watch` can follow a long backup is a later, optional step; the storage
protocol does not change for it.

`lux backup`:

* Targets: `--source`/`--cache`, `--map`, `--index`, `--all`; the resolution
  is the one `counts` and `clear` have, moved into `cli/_targets.py` and
  shared by all three. `--all` is every cache of every source plus every map
  store plus every index path.
* `--dest DIR` (default: `backup_dir` from `base.json`, falling back to
  `<data_dir>/backups`), `--label NAME` (default: a timestamp).
* `--portable` forces the JSON Lines format for every store.
* `--verify` after writing: for every entry, the manifest's `rows` against
  `len()` (exact stores) or within 1% of `len_estimate()` (Postgres caches),
  and a sample re‑read as `migrate-idmap.py --verify-pct` does for the idmap.
* Never writes to any store. A store that refuses (subsidiary) is reported
  and skipped; the exit status is non‑zero if any entry failed.

`lux restore <dir>`:

* `--list` prints the manifest and stops.
* The same target options select entries; with none given, every entry.
* Refuses an entry whose target store is not empty unless `--force`, and
  refuses `idmap` without `--force` whether or not stdin is a tty (the
  rule from item 09). Refuses a native entry whose `store_class` differs
  from the configured one, and says to use a portable backup.
* Resolves everything before touching anything, as `clear` does.

### 4. Configuration

One optional key, `backup_dir` in `base.json`, read by `Config.process_base`
like the other `*_dir` keys; relative paths are under `base_dir`. Apply the
same edit to the build‑machine copies in `config_cache/config_cache/` and
say so in the PR (ground rule 5). No new credentials: every backend is
reached through the store's own connection.

## What to keep from `main`

* `IdMap.dump_members()` stays as it is; identify depends on its exact
  text‑COPY shape. `backup()` may call the same `COPY` but writes its own
  files.
* `_export_state()` / `_import_state()` stay as the test round trip.
* `migrate-idmap.py` stays until item 33; its load shape (bare tables,
  COPY, indexes after) is what a large idmap restore should copy.
* `backup_tables.sh` stays until item 33, which retires it with a note
  pointing at `lux backup --source <s> --cache datacache`.

## Known hazards

* **Consistency.** A COPY is a snapshot of one table; the idmap pair must
  be copied in one transaction. Redis and filesystem iteration is not a
  snapshot at all. Document that a backup is taken when no phase is
  running; the command does not try to lock anything.
* **Transactions on restore.** `PooledCache` and the maps commit through
  their own cursor helpers; the restore must run `TRUNCATE` and `COPY FROM`
  on the same connection in one transaction, not through `clear()` plus a
  separate load, or a failed COPY leaves an empty store.
* **Disk.** `Environment.copy(compact=True)` needs free space equal to the
  live data; a Postgres COPY of a data cache is roughly the table's size
  before gzip. Check free space under `--dest` against the manifest
  estimate before writing and refuse early.
* **Memory.** Never collect a store into a dict (`_export_state` does;
  it is for tests). Everything streams.
* **The update token.** Restoring the idmap does not touch
  `idmap_update_token.txt`, for the reason given in item 09.
* **Config copies.** The build machine's `map_idmap.json` still carries
  Redis keys (`host`, `port`, `db`, `snapshotPath`) from before the
  Postgres migration; `snapshotPath` is not a backup setting and nothing
  reads it. Leave it alone; it is item 35's problem.

## Decisions (2026-10-28, with Rob)

* **Two PRs.** 39a: `cli/_targets.py` and the storage layer (`storage/backup.py`,
  the native copies, the portable formats) with their tests. 39b: `lux backup`,
  `lux restore` and `backup_dir`. The index box is ticked by 39b.
* **The portable idmap is two files**, `<stem>.members.jsonl.gz`
  (`{"k": uri, "v": yuid}`) and `<stem>.yuids.jsonl.gz`
  (`{"yuid", "token", "minted"}`), format `idmap-jsonl`: a key -> value dump
  would lose the `<table>_yuid` registry (update tokens, minted YUIDs with no
  members). Supported by the Postgres and memory idmaps; the Redis idmap
  refuses and points at `migrate-idmap.py` (no live instance uses it).
* **Every Postgres restore** -- native or the portable idmap -- drops the
  table's indexes and primary-key / unique constraints (read from the
  catalog), TRUNCATEs, COPYs and recreates them exactly, all in one
  transaction: `migrate-idmap.py`'s load speed with a rollback to the old
  contents on any failure. `ensure_covering_indexes()` is therefore not
  called: the indexes that were there are put back.
* **`--verify` checks the files**: each file's rows (lines, or the copied
  LMDB environment's entries) against the manifest, whose counts come from
  the copy itself, and a sample of portable keys looked up in the store. It
  never counts a live table (a full scan on a read-bound machine).
* Backup and restore refuse while a build runs (`refuse_while_building`,
  `--force`); `backup_dir` in the build machine's copies is
  `/data/output/backup`, where `backup_tables.sh` writes.

## Steps

1. `cli/_targets.py`: move the resolution helpers out of `counts.py` and
   `clear.py`; both test files unchanged and green.
2. `storage/backup.py`: the protocol, the portable writer/reader, the
   manifest; `iter_items()` on the classes that lack it.
3. Native `backup()` / `restore()` on the Postgres classes with a fake
   cursor test that records the `COPY` statements and feeds rows back
   (the fake pattern in `tests/test_idmap_subsidiary.py`); on `Lmdb` with
   real files in `tmp_path`.
4. `lux backup` and `lux restore` with a `StubConfigs` as in
   `tests/test_cli_counts.py`; refusal cases; `--list`; `--verify`.
5. `backup_dir` in `Config` and in the config copies.
6. Round trip on the build machine: `lux backup --map all_refs,done_refs`
   then `lux clear --refs` then `lux restore`; `lux counts --map
   all_refs,done_refs` before and after agree. Then the idmap into a
   scratch `tableName`, as `migrate-idmap.py --table` does.

## Acceptance criteria

* Every store class in `lux_pipeline/storage` either backs up and restores
  through the protocol or raises with a reason; the table above is
  complete and matches the code.
* `backup --verify` passes on the live instance for the maps and one
  Postgres data cache; the idmap round‑trips into a scratch table.
* A portable backup of a Redis map restores into the Postgres class of
  the same kind.
* `restore` of the idmap without `--force` is refused with nothing
  written, tty or not.
* Tests pass; no `print` in `lux_pipeline/storage`; nothing there imports
  `rich`.
