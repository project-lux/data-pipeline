# 31. Shell orchestration on the null engine

**Priority:** P2 · **Size:** M · **Depends on:** 06, 13, 17, 21, 22, 24, 26 · **Unblocks:** 32, 33
**Read from:** `main:run-all.sh`, `main:*_parallel.sh`

## Intent

Rewrite the shell wrappers so they launch `lux` with the **null engine**
(one interpreter per slice — exactly today's process model, so operational
behaviour such as `nohup`, per‑slice log files and flag files is unchanged)
and add a `run-all.sh` that can alternatively run each phase as a single
`lux` command on the `mp` engine.

```bash
# today
nohup python ./run-reconcile.py $count 24 --all > ../data/logs/reconcile_$count.txt 2>&1 &
# after
PREFIX=../data/logs/reconcile-$(date +%Y-%m-%d-%H%M%S)
nohup lux reconcile --source all --my-worker $count --max-workers 24 --no-ui --no-finish --prefix $PREFIX > ../data/logs/reconcile_$count.txt 2>&1 &
# ... and from any terminal:
lux watch $PREFIX
```

Every slice gets the same `--prefix`, so each writes `<PREFIX>-w<n>.status`
and `<PREFIX>-w<n>.log` and `lux watch` (item 06) shows the whole build.
The per‑slice stdout redirect stays for anything not routed to the logger.

## Files

| Action | Path on `main` |
|---|---|
| Edit | `run-all.sh` — add `ENGINE=${ENGINE:-null}`; when `null`, the existing loops with `lux … --my-worker`; when `mp`, one `lux reconcile --source all --max-workers 24 --no-ui` per phase (which already includes identify/metatypes/write‑refs in its tail) |
| Edit | `reconcile_parallel.sh`, `merge_parallel.sh`, `export_parallel.sh`, `import_parallel.sh`, `harvest_parallel.sh`, `nt_parallel.sh`, `load_parallel.sh`, `updateExternal_parallel.sh` → `lux` equivalents |
| Create | `scripts/wait-for-flags.sh` — the "count `*_is_done-*.txt`, grep Traceback" loop that `run-all.sh` repeats three times, as a function |

## Mapping table (put it in `run-all.sh`'s header comment)

| Old | New |
|---|---|
| `manage-data.py --clear-all --new-token` | `lux clear --source all --force && lux clear --refs --force && lux new-token` |
| `google-sames-diffs.py`, `load-csv-map2.py --same/--different` | unchanged (not part of the refactor) |
| `run-reconcile.py i 24 --all` | `lux reconcile --source all --my-worker i --max-workers 24 --no-ui --no-finish` |
| `run-identify.py` | `lux identify` |
| `merge-metatypes.py; mv metatypes.json ../data/files/` | `lux merge-metatypes` → **add** as a 10‑line command in this item (calls `ReferenceManager.merge_metatypes()`, item 21) |
| `manage-data.py --write-refs` | `lux write-refs` |
| `run-merge.py i 24 --all` | `lux merge --source all --my-worker i --max-workers 24 --no-ui` |
| `post-build-portal.py --no-tqdm` | unchanged |
| `run-export.py i 24` | `lux export --my-worker i --max-workers 24 --no-ui` |
| `manage-data.py i 24 --load --wikidata` | `lux load --source wikidata --my-worker i --max-workers 24 --no-ui` |
| `run-harvest.py i 50 --pages --ycba` | `lux harvest --source ycba --pages --my-worker i --max-workers 50 --no-ui` |
| `manage-data.py i 24 --nt` | `lux export --type nt --my-worker i --max-workers 24 --no-ui` |

## Steps

1. Add `lux merge-metatypes`.
1b. In `wait-for-flags.sh`, prefer the status files (`"done": true` in every
    `<PREFIX>-w*.status`) over counting flag files when a prefix is known; keep
    the flag‑file path as a fallback.
2. Rewrite the scripts; keep the old lines commented for one release.
3. Run `ENGINE=null ./run-all.sh --all` and `ENGINE=mp ./run-all.sh --all` on a test instance; compare `lux counts` and a sample of merged records.

## Decisions (2026-10-28, with Rob)

* **New scripts beside the old ones, not rewrites.** `run-all2.sh` and
  `reconcile_parallel2.sh`, `merge_parallel2.sh`, `export_parallel2.sh`,
  `nt_parallel2.sh`, `import_parallel2.sh`, `harvest_parallel2.sh`,
  `updateExternal_parallel2.sh` are the `lux` versions; `run-all.sh` and
  the `*_parallel.sh` scripts are unchanged, so builds with both can be
  compared on the build machine. Once they have been, the old ones are
  removed (item 33) and the new ones can take their names.
* **Failures are found in the logs.** A worker that raises logs the
  exception, with its `Traceback`, to `<PREFIX>-w<n>.log`, and then
  reports `"done": true` all the same (`_run_worker`'s `finally`), so the
  status files only say when a slice stopped. `wait-for-flags.sh` waits for
  `"done": true` in every status file and fails on a `Traceback` in any
  worker log, or on a `lux <command>:` / `lux:` line in the slices' stdout
  files (a `lux` that failed before its worker started writes no status
  or log). No change to the Python. A slice killed outright (`kill -9`)
  leaves no trace and is waited for forever, as before.
* Step 1 was already done: `lux metatypes` (item 22).
* `run-all2.sh` keeps `--all|--<source>` (the `--` is optional) and its
  paths, now variables overridable from the environment (`LOGS`,
  `STATUS_FILE`, `EXPORTS`, `WORKERS`, `WAIT_INTERVAL`). On an error it
  exits 1 (was 0); the steps between the phases are checked too; the
  fixed `sleep`s are gone (on the null engine it `wait`s for the slices to
  exit before the next step). `post-build-portal.py` stays unchecked, as
  before.
* `load_parallel.sh` (`run-load.py`, the MarkLogic load) is left alone:
  `lux` has no equivalent; item 33 decides.
* The wrappers start their slices and return, printing the `lux watch`
  and `scripts/wait-for-flags.sh` commands for the run. Their stdout files
  now also get stderr; `harvest_parallel.sh`'s are `harvest_<n>_<source>.txt`.
* Tests: `tests/test_shell_scripts.py` runs the scripts against a stub
  `lux`. Step 3 (both engines on a test instance, compare counts) is Rob's,
  on the build server.

## Acceptance criteria

* Both engines complete a small build with identical counts.
* `run-all.sh` no longer invokes any `run-*.py` or `manage-data.py`.
