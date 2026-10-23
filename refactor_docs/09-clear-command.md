# 09. `lux clear`

**Priority:** P1 · **Size:** S · **Depends on:** 05 · **Unblocks:** 33
**Read from:** `rob_refactor:lux_pipeline/cli/clear.py` (102 lines); `main:manage-data.py` lines 401–420 (`--clear`) and 524–552 (`--clear-external|results|internal|refs|all`, `--clear-idmap`)

## Intent

Add `lux_pipeline/cli/clear.py` covering every clearing operation `manage-data.py`
has, with the refactor's safer interface:

```
lux clear --source ycba                       # recordcache, reconciledRecordcache, recordcache2 (NOT datacache)
lux clear --source ycba --cache datacache     # explicitly
lux clear --source all                        # == manage-data --clear-all minus idmap/refs
lux clear --map all_refs                      # refs queues; --map idmap requires --force
lux clear --index lcnaf                       # delete + recreate the lmdb index files
```

## Files

| Action | Path on `main` | Source |
|---|---|---|
| Create | `lux_pipeline/cli/clear.py` | `rob_refactor:lux_pipeline/cli/clear.py` |
| Create | `tests/test_cli_clear.py` | new |

## What to port

The handler as written, with:

* `--index`: the source prints "FIXME: can't clear". Implement it: for
  `reconcileDbPath` / `inverseEquivDbPath`, call
  `lux_pipeline.storage.idmap.lmdb.remove_lmdb(path)` (exists on `main`) and let
  the next index load recreate it. Item 34 may generalise.
* `--map idmap` without `--force` refuses (keep). With `--force` also delete
  `idmap_update_token.txt`? **No** — leave the token alone; document it.
* Add `--refs` as an alias for `--map all_refs,done_refs` to mirror
  `manage-data --clear-refs`; the refactor only allows one map per call —
  allow a comma list.
* Confirmation: when more than one source or `all` is given and stdin is a
  TTY, ask `y/N` unless `--force`. Shell scripts pass `--force`.

## Known bugs in the source — do not copy

* `clear.py` falls through after `print("Could not find cache to clear")`
  and then uses `cfg` from the previous loop iteration. Add `continue`.

## Steps

1. Create the file with a docstring.
2. Tests with stub caches that record `clear()` calls: default cache set
   excludes `datacache`; `--map idmap` without `--force` clears nothing;
   comma lists work.

## Acceptance criteria

* Every `--clear*` path of `manage-data.py` has an equivalent (table it in the module docstring).
* Tests pass.
