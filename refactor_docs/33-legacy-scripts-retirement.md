# 33. Retire the legacy scripts

**Priority:** P2 · **Size:** S · **Depends on:** 31, 32
**Read from:** `rob_refactor` has `old/` as a precedent; do not copy it

## Intent

Once every phase runs through `lux` and `run-all.sh` no longer calls them,
move the per‑phase scripts out of the repository root so there is one way to
run the pipeline.

| Action | Paths |
|---|---|
| `git mv` to `scripts/legacy/` | `run-reconcile.py`, `run-merge.py`, `run-export.py`, `run-identify.py`, `run-harvest.py`, `run-load.py`, `run-source-build.py`, `manage-data.py`, `*_parallel.sh` (the old versions, if item 31 kept copies), `merge-metatypes.py` |
| Leave at root | `run-all.sh`, `google-sames-diffs.py`, `load-csv-map2.py`, `post-build-portal.py`, `pg-tune.py`, `timing-report.py`, the `make_*_lmdb.py` distribution builders, one‑off `*.py` utilities |
| Delete | `load_parallel.sh` if `run-load.py` (MarkLogic load) is confirmed unused ("STOP. You probably want to use MLCP instead") |

The thin wrappers created in items 20/23/25 still work from `scripts/legacy/`
because they import `lux_pipeline.process.phases.*`; add
`sys.path.insert(0, repo_root)` at their top as the tests do.

`manage-data.py` has functions not yet in `lux` (`--validate`, `--reload`,
`--clean-idmap`, `--vacuum/--optimize`, `--bloat`, `--idmap-indexes`,
`--drop-time-indexes`, `--rewrite`, `--debug-record`, `--places/--concepts/--periods`).
Before moving it, list them in `docs/rebuild.md` under "Maintenance
(legacy)" with the `scripts/legacy/manage-data.py` path; porting them is a
follow‑on (`lux maintain …`), not part of this plan.

## Acceptance criteria

* `ls *.py` at the root shows no `run-*.py` and no `manage-data.py`.
* `python scripts/legacy/run-reconcile.py --aat --recid 300010001` still works.
* CI/tests pass.
