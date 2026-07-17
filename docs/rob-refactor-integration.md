# Integrating the reconciliation + audit work into rob_refactor

**Branch:** `fabel-recon-rob-refactor` (off `rob_refactor`) · applies the
delta of `fabel-recon-test` + `fabel-pipeline-audit` (deterministic
identity resolution + every determinism/data-loss fix) onto the
restructured pipeline. All 32 ported tests pass.

The three reports in this directory (`determinism-audit.md`,
`changes-report.md`, `build-comparison-report.md`) describe the original
work against the `main` layout; this file maps it onto `rob_refactor`.

## How it was applied

`rob_refactor` renamed `pipeline/` → `lux_pipeline/`, restructured
`sources/` (e.g. `sources/authorities/oclc` → `sources/viaf`), retired
the root `run-*.py` scripts to `old/`, and replaced them with the `lux`
CLI + task managers. The recon+audit delta was applied with git rename
detection, then everything git couldn't map was ported by hand.

## The identity architecture in rob_refactor terms

| main layout | rob_refactor home |
|---|---|
| `pipeline/process/identity.py` | `lux_pipeline/process/identity.py` (unchanged engine; `AssertionWriter` now writes to `configs.temp_dir` and takes a `phase` tag; new `resolve_identity_from_config()` helper) |
| `run-reconcile.py`: workers log assertions instead of writing the idmap | `lux_pipeline/process/tasks/reconcile_task.py`: `_handle_record` calls `assertion_log.write_record()` instead of `ref_mgr.manage_identifiers()`; each worker slice writes `assertions-p{phase}-{slice}.tsv` |
| `run-identify.py` after the reconcile wait-loop | runs automatically inside `ReconcileManager.process()` after phase 2 (fails the build loudly), and standalone as `lux identify` for re-runs |
| `run-reconcile.py`: `did_ref` only after successful acquire | same fix in `_pool_reconcile_refs` (unusable URIs still marked done) |
| `run-process-deletes.py` | `lux process-deletes` (`lux_pipeline/cli/process-deletes.py`) driving `UpdateManager.process_pending_deletes()` |
| stale assertion-log clearing in `run-all.sh` | `ReconcileManager.process()` clears `temp_dir/assertions-*.tsv` before phase 1 |

`manage_identifiers` remains in `reference_manager.py` with no production
callers (comparison baseline only), as before.

## Fixes that moved to a different home

- `pipeline/storage/idmap/lmdb.py` (TabLmdb hardening: tab-in-member
  rejection, loud over-long-key `__contains__`) → the classes now live in
  `lux_pipeline/storage/index/lmdb.py`; fixes applied there.
- `pipeline/sources/authorities/oclc/index_loader.py` (VIAF regenerate /
  LC_ALL=C / smallest-id-wins) → `lux_pipeline/sources/viaf/index_loader.py`.
- `run-merge.py` cross-slice YUID claim + sorted base-record selection →
  `lux_pipeline/process/tasks/merge_task.py`.
- `run-export.py` stale export-cache check (insert_time comparison) →
  `lux_pipeline/process/tasks/export_task.py`.
- `run-load.py`'s `flush_updates()` call: `rob_refactor` has no ML
  bulk-load driver yet; the hardened reaper and `flush_updates()` sit in
  `lux_pipeline/storage/marklogic/rest.py` for whichever task grows that
  role.

## Fixes that were already covered or no longer apply

- **LmdbReconciler silent-None index**: `rob_refactor` gets reconcile
  indexes from the config index registry and `LmdbIndex.open()` raises on
  failure, so only the Sqlite variant needed the retry-then-raise port.
- **`iter_keys_type_slice` missing ORDER BY**: `rob_refactor` removed the
  type-slice methods; its own `iter_*_slice` methods already order by key.
- **`full-build.sh`**: dead code on `main`, deleted by `rob_refactor`;
  stays deleted. `old/run-*.py` received the fixes via rename detection
  but are archival (they import the removed `pipeline` package).

## Open items

- The reference queue is still random-order pop; the audit assessed the
  generational-BFS follow-up as best implemented in the task manager
  (per-generation barrier). Unchanged here.
- `tests/test-loader.py` (pre-existing in rob_refactor) still imports the
  old `pipeline` package and cannot run; the three ported test files use
  `lux_pipeline` paths.
- `experiments/recon_test/` was ported (imports updated) so the
  legacy-vs-deterministic comparison harness can run against this layout.
