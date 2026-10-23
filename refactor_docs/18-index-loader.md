# 18. Index loader as a managed task

**Priority:** P1 · **Size:** M · **Depends on:** 02 · **Unblocks:** 19, 34
**Read from:** `rob_refactor:lux_pipeline/process/base/index_loader.py` (154 lines); `main:pipeline/process/base/index_loader.py` (≈170 lines)

## Intent

Make `IndexLoader` a `Sliced` component with a `progress` callable (item 02)
so it can run under a task manager, while keeping `main`'s storage (`TabLmdb` from
`lux_pipeline/storage/idmap/lmdb.py`, opened by path) and `main`'s
determinism rule (smallest `recid` wins a collision).

## Why it matters

Index building is the one phase with no parallel story and no progress
output except a print every 100k records. The refactor gives it the same
shape as the loader; `main` gives it the right storage. Take both halves.

## Files

| Action | Path on `main` | Source |
|---|---|---|
| Edit | `lux_pipeline/process/base/index_loader.py` | shape from `rob_refactor`, body from `main` |
| Create | `tests/test_index_loader.py` | new |

## What to port

From the refactor: `class IndexLoader(Sliced)`, the `index_records(names, uris)`
extraction method, `process(overwrite=True)` driving
`get_storage()` → clear → `index_records` → `update`, progress calls, and
`export()`/`write_csv()` as methods on the base.

## What to keep from `main`

* `LmdbIndexLoader.get_storage()` opening `TabLmdb` at `reconcileDbPath` /
  `inverseEquivDbPath` with `mapSizeExponent`. `SqliteIndexLoader` too.
* The collision rule: `prev is None or str(recid) < str(prev[0])` for both
  names and uris. The refactor is last‑writer‑wins (non‑deterministic).
* The `len(nm.encode("utf-8")) < 499` byte‑length check (the refactor uses
  character length, which lets a 400‑char CJK name exceed LMDB's 511‑byte key limit).
* `prepare()` refuses `max_slice > 1` with a clear `ValueError` (LMDB has one
  writer; `rob_claude_refactor` fix).
* `load()` as a thin alias: `def load(self): self.prepare(); return self.process()`
  so `manage-data.py --load-index` and the subclasses
  (`WdFileIndexLoader.load`, `LCIndexLoader`) keep working.
* `index.update(all_names)` without the refactor's `with index.open("w"):`
  (that API belongs to the superseded `LmdbIndex` wrapper).

## Known bugs in the source — do not copy

* `index_records(names=True, uris=True)`: inside the loop `names = self.extract_names(...)`
  rebinds the boolean parameter, so after the first record `if names:` is
  truthy only if the dict is non‑empty, and `if uris is not None` is always
  true. Rename the locals (`want_names`, `want_uris`).
* `process()` calls `self.get_storage()` **and** `self.get_index(...)`; the
  second pair overwrites the first with `None` unless the `indexes` config
  block exists. Use only `get_storage()`.
* Index names mismatch across the refactor: the loader uses
  `reconcile_labels`/`reconcile_ids`, the reconciler `reconcile_label`/`reconcile_id`.
  Irrelevant here (path‑based), relevant for item 34.

## Steps

1. Rewrite the base class; keep the two storage subclasses as they are.
2. Check the subclasses still import: `lc/index_loader.py`,
   `wikidata/index_loader.py`, `yale/library/index_loader.py`,
   `lux/final/index_loader.py`, `oclc/index_loader.py`.
3. `tests/test_index_loader.py`: in‑memory `in_cache` (list of records),
   stub reconciler with `extract_names`, stub acquirer returning the record;
   `get_storage()` returning two dicts; assert the smallest‑recid rule, the
   byte‑length drop, that `prepare(0, 2)` raises, and that a recording
   `progress` saw `total` and advances.
4. `python manage-data.py --load-index --aat` on the live instance (small).

## Acceptance criteria

* Tests pass; `manage-data.py --load-index` unchanged in behaviour.
