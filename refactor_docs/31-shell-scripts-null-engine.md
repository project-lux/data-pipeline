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

## Acceptance criteria

* Both engines complete a small build with identical counts.
* `run-all.sh` no longer invokes any `run-*.py` or `manage-data.py`.
