# 08. `lux counts`

**Priority:** P1 · **Size:** S · **Depends on:** 05 · **Unblocks:** 33
**Read from:** `rob_refactor:lux_pipeline/cli/counts.py` (115 lines); `main:manage-data.py` lines 484–522 (`--counts`)

## Intent

Add `lux_pipeline/cli/counts.py`: print record counts for caches, maps and
indexes. This is the first real command and the simplest way to validate the
dispatcher: it needs a config, no manager, no UI.

```
lux counts                         # datacache/recordcache/reconciledRecordcache/recordcache2 for every source
lux counts --source aat,ulan --cache datacache
lux counts --map all               # idmap, networkmap, all_refs, done_refs …
lux counts --index all             # reconcile name/id indexes per source
```

## Files

| Action | Path on `main` | Source |
|---|---|---|
| Create | `lux_pipeline/cli/counts.py` | `rob_refactor:lux_pipeline/cli/counts.py` |
| Create | `tests/test_cli_counts.py` | new |

## What to port

The whole handler. It is a `BaseHandler` (no manager). Changes:

* Replace the `"indexes"` config walk (`cfg["indexes"].values()`) with
  `main`'s path‑based keys: for each source with `reconcileDbPath` or
  `inverseEquivDbPath`, open the file read‑only with
  `lux_pipeline.storage.idmap.lmdb.TabLmdb.open(path, "r", readahead=False, writemap=True)`
  and report `len()`. Item 34 may later switch this to named indexes.
* `len_estimate()` then exact `len()` under 100k — keep.
* Output goes through `print` (it is the command's result), formatted as in
  the source. Optionally render with a `rich.table.Table` when stdout is a
  TTY.

## Known bugs in the source — do not copy

* Totals: the map and index totals are added onto the cache total variable
  (`total += n`) so "Total Map Records Seen" includes caches. Use separate
  totals.

## Steps

1. Create the file; module docstring `"""Count records in caches, maps and indexes."""` (item 05's `help` reads it).
2. `tests/test_cli_counts.py`: a `StubConfigs` with `internal`/`external`/`results`
   dicts whose cache objects implement `len_estimate()`/`__len__`; run
   `CommandHandler(cfgs).process(args, rest)` with `capsys` and assert the
   lines. Follow the stub pattern in `tests/test_identity.py`.
3. Compare against `python manage-data.py --counts` on the live instance.

## Acceptance criteria

* `lux counts` matches `manage-data.py --counts` for every cache.
* Tests pass.
