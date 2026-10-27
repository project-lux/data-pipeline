# 41. Optional: function‑level profiling inside `PhaseTimer`

**Priority:** P3 · **Size:** S · **Depends on:** 20, 23, 25
**Read from:** `main:run-reconcile.py`, `run-merge.py`, `run-export.py` as they
were before items 20/23/25 (`git show <merge-base>:run-reconcile.py`), the
`--profile` blocks

## Intent

Give the three phase scripts' old `--profile` one home, in the timing code
that already exists for the same question:

```
LUX_PROFILE=1 lux reconcile --source all --max-workers 24      # or --profile on the command
python timing-report.py --profile                                # top functions per phase, per stage
```

`PhaseTimer` answers *which stage* is slow and whether the phase is cpu
bound or waiting; `cProfile` answers *which Python function inside a stage*.
Both views have paid for themselves (the `DateDataParser` 14 ms/call finding
in `mapper_utils.py` came out of a reconcile profile), and they belong
together: one switch, one output directory, one report.

## Why optional, and why last

Debugging and optimisation only; nothing functional depends on it. Only the
owner has ever used `--profile`. Items 20, 23 and 25 drop it from the phase
functions and the legacy wrappers ignore the flag, so there is no profiler
until this item lands.

## What the old flag did, and what not to copy

Each script wrapped only the *records* pass in `cProfile.Profile()`, printed
`pstats` sorted by cumulative time to stdout, and then ended the run with a
bare `raise ValueError()` so that none of the tidy‑up ran (reconcile:
no references pass, no `assertion_log.close()`, no metatypes, no flag). Keep
the idea, not the mechanics.

## If done

* `PhaseTimer(..., profile=False)`, also switched on by `LUX_PROFILE=1` so
  the legacy wrappers need no flag: `enable()` in `__init__`, `disable()` in
  `finish()`, and the stats written as a binary `profile-<phase>-<slice>.prof`
  next to `timing-<phase>-<slice>.json` in `out_dir` (loadable with `pstats`
  or snakeviz). Nothing to stdout, no exception, the phase runs to the end.
  A second timer in the same process (reconcile's references pass) gets its
  own file.
* `timing-report.py --profile`: load the `.prof` files beside the timing
  JSON, aggregate them across slices with `pstats.Stats.add`, and print the
  top N functions by cumulative and by internal time, and where a stage name
  can be matched, per stage.
* `--profile` on `lux reconcile`, `lux merge` and `lux export` (items 21, 24,
  26) sets the environment variable for the workers; the phase functions do
  not grow a parameter.
* Tests: a `PhaseTimer(profile=True)` run writes a `.prof` the report can
  read; the report aggregates two slices.
