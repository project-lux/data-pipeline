# 34. Named `indexes` config block

**Priority:** P1 · **Size:** M · **Depends on:** 18, 19 · **Unblocks:** the `pipeline-configs` repository
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

## Why required

The `pipeline-configs` repository uses the block as the *only* index
configuration for aat, ulan, lcnaf, viaf, wikidata and merged (lcsh has
both forms). On `main` the reconcilers, index loaders, `lux counts` and
`lux clear` read `reconcileDbPath` / `inverseEquivDbPath` only, and the
identity resolver reads `differentDbPath` from `merged` — with the block
alone those sources reconcile with no index and identify clusters without
diff constraints, silently. Promoted from P3 on 2026‑10‑26 for that reason.

The repository's names are `reconcile_labels`, `reconcile_ids`,
`reconcile_id` and `differents` with `indexClass:
storage.index.lmdb.LmdbIndex`; step 4 fixes the names and step 1 maps the
class to `main`'s store. Note also that the repository's `system.json`
`path_types` has no entries for the two legacy keys, so the block's own
`indexes_dir` resolution is what makes its paths absolute.

## Steps

1. Config: port the block; use `main`'s `TabLmdb`/`StringLmdb`/`JsonLmdb`
   classes from `storage/idmap/lmdb.py` as `indexClass` (no new wrapper).
   Open lazily: store the class and kwargs, create the object on first use,
   because 24 workers must not each open every index at startup.
2. Reconciler/IndexLoader: look in `indexes` first, fall back to the legacy
   keys. Keep `main`'s loud‑failure retry.
3. `counts`/`clear`: enumerate `indexes`.
4. Fix the name mismatch: canonical names are `reconcile_label`,
   `reconcile_id`, `equivalents`, `differents`; accept the plural
   `reconcile_labels` / `reconcile_ids` the `pipeline-configs` repository
   uses today, with a warning, or fix the repository in the same change.
   `differentDbPath` on `merged` maps to `differents`.

**Live config copies.** `config_cache/config_cache/` is an untracked copy of the
build machine's configuration files (see README ground rule 5). In the same PR,
apply this item's config changes there: an `indexes` block for each source config whose path‑based index settings you migrate (or none, if the live instance keeps the path‑based form). Check the edited files still
parse as JSON and that every `*Class` value resolves through `importObject`
without a legacy warning, and list the edited files in the PR description.
Leave the local instance alone.
