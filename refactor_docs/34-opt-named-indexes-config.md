# 34. Optional: named `indexes` config block

**Priority:** P3 · **Size:** M · **Depends on:** 18, 19
**Read from:** `rob_refactor:lux_pipeline/config.py` (`instantiate()` — the `"indexes"` block), `rob_refactor:lux_pipeline/process/base/reconciler.py` (`LmdbReconciler.__init__`), `rob_refactor:lux_pipeline/sources/lux/final/{index_loader,reconciler}.py`, `rob_refactor:lux_pipeline/cli/{counts,clear}.py`

## Intent

Generalise the two hard‑wired index paths per source
(`reconcileDbPath`, `inverseEquivDbPath`, plus the global
`differents`/`equivalents` on `merged`) into a named block:

```json
"indexes": {
  "reconcile_label": {"indexClass": "storage.idmap.lmdb.TabLmdb", "path": "lcnaf_labels.lmdb", "mapExponent": 30},
  "reconcile_id":    {"indexClass": "storage.idmap.lmdb.TabLmdb", "path": "lcnaf_ids.lmdb"}
}
```

`Config.instantiate()` resolves `path` against `indexes_dir` and puts an
`"index"` object in each entry; `IndexLoader.get_storage()`,
`LmdbReconciler.__init__`, `lux counts --index`, `lux clear --index` read
the block, with the two legacy keys still accepted (mapped to
`reconcile_label` / `reconcile_id`).

## Why optional

Nothing in the live configuration uses it, the raw‑`lmdb` store on `main`
opens by path already, and the refactor itself was inconsistent about the
names (`reconcile_labels` vs `reconcile_label`). Do it when a source needs
a third index.

## Steps

1. Config: port the block; use `main`'s `TabLmdb`/`StringLmdb`/`JsonLmdb`
   classes from `storage/idmap/lmdb.py` as `indexClass` (no new wrapper).
   Open lazily: store the class and kwargs, create the object on first use,
   because 24 workers must not each open every index at startup.
2. Reconciler/IndexLoader: look in `indexes` first, fall back to the legacy
   keys. Keep `main`'s loud‑failure retry.
3. `counts`/`clear`: enumerate `indexes`.
4. Fix the name mismatch: canonical names are `reconcile_label`,
   `reconcile_id`, `equivalents`, `differents`.
