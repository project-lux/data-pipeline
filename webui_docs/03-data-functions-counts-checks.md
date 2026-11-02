# W03. Data functions for counts, testinstall and list

**Size:** S · **Depends on:** — · **Unblocks:** W06, W11
**Read:** `lux_pipeline/cli/counts.py`, `lux_pipeline/cli/testinstall.py`,
`lux_pipeline/cli/list.py`, `lux_pipeline/process/timing_report.py`
(the `*_data` / `*_lines` pattern to copy), `tests/test_cli_counts.py`,
`tests/test_cli_list.py`, `tests/test_cli_small_commands.py`

## Intent

Three commands print tables straight from their `process` methods. Split
each into a function that returns the rows and a `process` that prints
them, as `timing_report.directory_data` / `directory_report` already do,
so the web pages (Dashboard, Health, Counts, Configuration's source list)
render the same rows. Output of the three commands must not change by a
character.

## Files

| Action | Path | Change |
|---|---|---|
| Edit | `lux_pipeline/cli/counts.py` | `counts_data(cfgs, sources, caches, maps, indexes) -> dict`; `process` prints from it |
| Edit | `lux_pipeline/cli/testinstall.py` | `Check` keeps `results: list[dict]`; `CommandHandler.checks(args) -> list[(label, fn, required)]`; `run_checks(args, out=None, stop_on_fail=True) -> list[dict]`; `process` prints from them |
| Edit | `lux_pipeline/cli/list.py` | `source_rows(cfgs, which) -> list[dict]`, `map_rows(cfgs)`, `index_rows(cfgs, which)` (the method exists; make it a function the method calls) |
| Edit | `tests/test_cli_counts.py`, `tests/test_cli_small_commands.py` (or a new `tests/test_cli_testinstall_data.py`), `tests/test_cli_list.py` | extend |

## Design

### counts

```python
def counts_data(cfgs, sources, caches, maps, indexes, open_index=None) -> dict
# sources: [(group, cfg)]  caches: [cache type]  maps: [name]  indexes: [(group, source, label, path)]
# {"caches": [{"group", "source", "cache", "count", "exact"}], "caches_total": int,
#  "maps":   [{"map", "count"}], "maps_total": int,
#  "indexes":[{"group", "source", "index", "count" | None}], "indexes_total": int}
```

Move the bodies of `count_caches`, `count_maps`, `count_indexes` into it
(keep the methods as one-line wrappers so nothing that calls them breaks);
`open_index` defaults to the current `TabLmdb.open` so a test can pass a
stub. `process` resolves the targets as today and prints from the dict
with the same `_fmt` and lines.

### testinstall

```python
class Check:
    def __init__(self, out=None, record=True): ...; self.results = []
    def run(self, label, fn, required=True) -> bool:
        # as today, and appends {"label", "state": "OK"|"WARN"|"FAIL", "detail", "required"} to self.results
```

`CommandHandler.checks(args)` returns the `steps` list that `process`
builds today. `run_checks(self, args, out=None, stop_on_fail=True)` runs
them through a `Check(out)` and returns `check.results`; `process` calls it
with `out=sys.stdout` and exits 1 on a FAIL as today. With
`stop_on_fail=False` (the web's health page wants every row) later checks
still run, each guarded: `check_config` failing means `self.cfgs` is None,
so every later check must say `FAIL  <label>: no configuration` rather
than raise `AttributeError` — add a guard at the top of `run_checks`'s loop:
if `self.cfgs is None and label needs it`, record `{"state": "FAIL",
"detail": "configuration did not build"}` without calling `fn`. The
`out=None` case writes nothing (`Check` gets `io.StringIO()`).

A caller that already has a `Config` (the server) must be able to say so:
`run_checks(args, out, stop_on_fail, cfgs=None)`; when given, `check_config`
uses it instead of `runtime.get_configs(...)` (read what `check_config`
does today and keep its detail text).

### list

```python
def source_rows(cfgs, which="all") -> list[dict]
# [{"group", "source", "namespace", "namespace_display", "merge_order", "classes": [{"label", "class", "short"}], "found": bool}]
def map_rows(cfgs) -> list[dict]      # [{"map", "class", "store"}]  (_store_details)
def index_rows(cfgs, which) -> list[tuple]   # the method's body
```

`_row_values`, `_build_table` and `list_sources` keep working from these.
Unknown names in `which` give `found: False` rows (the red "(not found)"
cell) instead of raising — that is what `list_sources` prints today.

## Known hazards

* `counts.process` has an "only maps or indexes asked" rule in
  `resolve_sources`; leave it there, the data function takes resolved
  targets only.
* `check_workers` spawns processes and `check_pg_tune` connects to
  Postgres; `run_checks` must honour `--skip-workers` / `--skip-pg-tune`
  exactly as `process` does (the web passes both).
* Do not change any printed string; the tests compare output.

## Steps

1. counts: data function, wrappers, tests green.
2. testinstall: `results`, `checks`, `run_checks`; `process` on top; tests.
3. list: rows functions; tests.

## Tests

* `counts_data` with the stub configs of `tests/test_cli_counts.py`:
  rows and totals; an index whose `open_index` raises gives `count None`
  and does not add to the total; `process` output unchanged (existing
  tests).
* `run_checks(stop_on_fail=False)` with `check_dotenv` patched to raise:
  every later result is FAIL with "configuration did not build" and none
  raised; with `stop_on_fail=True` the list stops at the first FAIL;
  `process` still exits 1 and prints the same lines.
* `source_rows` for `"aat,nosuch"`: two rows, second `found False`;
  classes list matches `_CLASS_KEYS` order; `lux list` output unchanged.

## Acceptance criteria

* Tests pass; `lux counts`, `lux list`, `lux testinstall` output unchanged
  (diff before / after on the sample installation of `tests/webui_fixtures.py`
  if W01 is in, else on stub configs).
