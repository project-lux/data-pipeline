# 27. `lux interactive`

**Priority:** P2 · **Size:** S · **Depends on:** 05
**Read from:** `rob_claude_refactor:lux_pipeline/cli/interactive.py` (≈65 lines)

## Intent

A Python REPL with the configuration already built: `cfgs`, `idmap`, and a
`setup_for("load")`‑style helper that returns a one‑worker manager for a
command so you can call `mgr.run_single()` by hand (item 07). Rich `pretty` and
`inspect` are installed, history persists in `<base_dir>/.python_history`,
tab completion works.

## Files

| Action | Path on `main` | Source |
|---|---|---|
| Create | `lux_pipeline/cli/interactive.py` | `rob_claude_refactor:lux_pipeline/cli/interactive.py` |

## What to port

The whole file. Changes:

* `import gnureadline as readline` → try `gnureadline`, fall back to the
  stdlib `readline`, then to `None` (the refactor dropped the stdlib
  fallback; macOS stdlib readline is libedit and the source already handles
  the `bind ^I rl_complete` case).
* `setup_for(task)` imports `lux_pipeline.cli.<task>`.
* Remove the `??` intercept in `LUXREPL.runsource` (placeholder for a
  feature that does not exist) or keep it as a no‑op; do not advertise it.
* Also expose `ref_mgr = ReferenceManager(cfgs, idmap)` lazily (a function,
  since it opens `replacements.json`).

## Acceptance criteria

* `echo 'print(len(cfgs.external))' | lux interactive` prints a number and exits.
