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
