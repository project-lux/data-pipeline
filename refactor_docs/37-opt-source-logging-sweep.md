# 37. Optional: logging sweep over `lux_pipeline/sources`

**Priority:** P3 · **Size:** L (many files, trivial each) · **Depends on:** 01
**Read from:** item 01's table

## Intent

Apply item 01's `print` → `logger` conversion to every file under
`lux_pipeline/sources/`. Mappers print validation complaints and odd data;
under the UI those lines corrupt the display and under the mp/ray engines
they vanish.

## How

* One PR per `sources/<category>/` directory, so each is reviewable.
* `grep -rn "print(" lux_pipeline/sources/<category>` → convert per the table.
  Mapper diagnostics about data quality are `logger.debug` unless the
  message starts with "Failed"/"Error" (→ `warning`).
* `sys.stdout.write('.')` progress ticks → delete.
* Do **not** touch logic, do not reformat (no `black` — the refactor's
  reformatting is what makes its mapper diffs unreadable).

## Acceptance criteria

* `grep -rn "print(" lux_pipeline/sources` returns nothing except deliberate CLI‑style output in `artic/tester.py`.
* Tests pass.

## Decisions (as done)

* One PR per `sources/<dir>/` (16 of them; `wikimedia/` only has
  commented-out prints). The `lux/` PR carries the index tick and these notes.
* Levels, from item 01's table plus the rule above:
  * mappers (and `wikidata/base.py`, `wikidata/fetcher.py`'s datatype
    complaint): data-quality notes are `debug`; "Failed", "Could not",
    "Error", "FAILED" and "Warning: …" are `warning`.
  * a print that only gives context for an exception raised on the next
    line (`print(rec); raise`, `print(l); raise NotImplementedError`) is
    `error`, so the context lands next to the traceback.
  * loaders, fetchers, index loaders, reconcilers: progress lines `info`;
    "Failed …", "Got failure …", "Error …", printed exceptions `error`;
    skips and soft problems `warning`; "Fetching …" and other per-record
    chatter `debug`.
  * the VIAF / Wikidata reconcilers: lines under `if self.debug:` are
    `debug`; "record suggests X, reconciler suggests Y" and "in record
    but NOT found" `warning` (they ask to be logged for a manual check);
    "in record and in reconciler; not overriding" `info`.
* `print(k, v)` in `lc/fetcher.py` became `logger.debug(f"{k} {v}")`.
* `sys.stdout.write('x')` in `getty/harvester.py` and the
  `sys.stdout.flush()` after progress lines in `mds/loader.py` and
  `yale/library/loader.py` are deleted; their `import sys` stays.
* Commented-out `# print(...)` lines are left as they are.
* `artic/tester.py` no longer exists; nothing is exempt.
* Two tests read these messages from stdout and now use `caplog`
  (`test_reconciler_lazy_indexes`, `test_qlever_mapper_ntriples`). The
  QLever padded-date line is `debug`, so that test sets the level.
