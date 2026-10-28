# 36. Optional: `lux viz`

**Priority:** P3 · **Size:** S · **Depends on:** 05
**Read from:** `rob_refactor:lux_pipeline/cli/viz.py` (199 lines)

## Intent

Render the reconciliation debug graph between two records in the terminal
(`netext` over `networkx`): `lux viz --recid <from>,<to>`. It runs
`Reconciler.reconcile()` with `debug_reconciliation` on and draws
`debug_graph`.

## Notes

* Depends on `netext` and `networkx` (`pip install -e .[ui-extras]`); import
  them inside `process()` and fail with a one‑line message otherwise.
* `main`'s `Reconciler` keeps `debug_graph` and merges each source
  reconciler's graph into it (`lux_pipeline/process/reconciler.py` lines
  168–176) but **not** the `Collector`'s. Two ways to feed `viz`:
  1. Port `rob_refactor`'s 6‑line merge of `self.collector.debug_graph`
     into `reconcile_uris()`, guarded by `self.debug`
     (`rob_refactor:lux_pipeline/process/reconciler.py`, after
     `self.collector.collect(record)`).
  2. **Preferred, from `rob_claude_refactor`:** drop `debug_graph` and emit
     greppable `logger.debug` lines at every decision —
     `RECONCILE: <src> EQUIV/rec <uri>`, `… EQUIV/index <uri>`, `… NAME <id> 'name'`,
     `… REMOVE/diff <uri>`, `… REMOVE/internal <uri>`, `… REMOVE/collect <uri>`
     (see `rob_claude_refactor:lux_pipeline/process/reconciler.py`,
     `process/collector.py`, `process/base/reconciler.py`). `viz` then builds
     its graph by running one record with `--log DEBUG` and parsing those
     lines, and the same lines are in every worker's log file for free.
     Apply only the logging lines to `main`'s files; the logic changes in
     those commits are not ported (see `CLAUDE-REFACTOR-FIXES.md`).
* Pure debugging aid; no tests beyond import.

## Decisions (2026-10-28, with Rob)

* **Option 1, the debug graph.** `main`'s Collector already fills its
  `debug_graph`; the only missing piece was merging it into the
  Reconciler's after `self.collector.collect(record)` in
  `reconcile_uris()`, under `if self.debug` (a no-op with
  `debug_reconciliation` off, as in a build). No log lines added.
* `debug-reconcile.py` (the same graph drawn with matplotlib) is left for
  item 33.
* `viz` switches debug on in the Reconciler, its Collector and every source
  reconciler for its own run (the source reconcilers read
  `debug_reconciliation` when they are built, so `rob_refactor`'s
  `reconciler.debug = True` alone recorded no index edges), and stops the
  collector storing what it fetches.
* Not copied from `rob_refactor:cli/viz.py`: the second URI typed with the
  first URI's mapper; `chr(ord(x) + 1)` node labels (punctuation after ~40
  nodes; now `<source>:<n>`); unreachable `raise` before `sys.exit`, bare
  excepts; `_rich.get_layout` / `Live`. Written for netext 0.5's
  `ConsoleGraph(nodes, edges)` API (the installed version; the networkx
  graph argument was an older API). The key and the shortest path (dead
  code behind `if False` in `rob_refactor`) are printed.
* Tests (`tests/test_cli_viz.py`) cover the URI typing, the replay order,
  labels, edges, the path, the command's refusals and the missing-extras
  message, and the reconciler merge with debug on and off.
