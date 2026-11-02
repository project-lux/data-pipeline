# W11. Timing, counts and maintenance pages

**Size:** M · **Depends on:** W03, W06
**Read:** `lux_pipeline/process/timing_report.py` (`directory_data`,
`compare_data`, `summarise`, `overall_summary`, `slice_rates`,
`nested_overrun`, `PHASE_ORDER`, `fmt_*`), `lux_pipeline/cli/_timing_view.py`
(the layout to mirror: a panel per phase, stage bars, nested stages,
slices, profile tables), `lux_pipeline/cli/counts.py` (`counts_data`, W03),
`lux_pipeline/storage/pg_tune.py` (`analyse`, `report`, `pending_changes`),
`lux_pipeline/cli/pg_maintain.py` (`--bloat` path only), `lux_pipeline/cli/list.py`

## Intent

The read-only reports as pages, each rendered from the function the CLI
already prints from:

| Method, path | Source function | Page |
|---|---|---|
| `GET /timing?phase=&profile=N` | `timing_report.directory_data(log_dir, phase, profile)` | a card per phase in `PHASE_ORDER`: records, wall, worker-hours, cpu % with a bar coloured as `_cpu_color`; a stage table with a share bar per stage; nested stages under their parent; the slice table with slow slices red; the all-phases summary (timeline as a horizontal bar chart in SVG, gaps, top stages); profile tables when `.prof` files exist |
| `GET /timing/compare?before=&after=` | `compare_data` | two directories side by side, changes coloured better / worse as the Rich view does |
| `GET /counts?source=&cache=&map=&index=` | `counts_data` after the same target resolution as `lux counts` | a table per kind; `~` for estimates; totals; a *Refresh* button; takes seconds on big caches: run in a thread and stream rows over SSE like Health |
| `GET /maintenance` | `pg_tune.analyse(...)`, `pg_tune.report(...)`, `pg_tune.pending_changes(...)`; `pg_maintain`'s bloat rows | the machine and server summary, the recommended settings with the ones that differ marked, the SQL as a copyable block; bloat per table (Postgres caches only; "not a Postgres store" otherwise); buttons that go to the launcher prefilled (`/commands/pg-maintain?vacuum=1`) rather than running anything here |
| `GET /api/timing`, `/api/timing/compare`, `/api/counts`, `/api/maintenance` | JSON |

## Design

`api/timing.py: timing_data(state, phase=None, profile=None, directory=None)`
returns `directory_data`'s result plus `fmt`-ed strings for every number
the Rich view formats (`fmt_duration`, `fmt_time`), so the template does
no arithmetic. The timeline: each phase a bar from `started_at` to
`updated_at` on a shared axis from the earliest start to the latest end;
SVG built in the template from precomputed percentages. Keep the Rich
view's colours: cpu ≥ 80 green, ≥ 50 yellow, else red; slow slices red.
A phase with `complete False` shows a *running* badge and links to its run
if `list_runs` has one with that command.

`api/counts.py: counts_rows(state, source, cache, map, index)` reuses
`counts.CommandHandler(cfgs)`'s `resolve_*` methods with a `Namespace`
(they are the target rules) then `counts_data`. Sources are instantiated
through `state.source(name)` for each named source first (counting needs
the cache objects). The SSE variant yields one row at a time.

`api/maintenance.py: maintenance_data(state, workers=None)`: `pg_tune.analyse`
needs the server the cache defaults point at; wrap in try and show the
connection error as the page's message when Postgres is unreachable (the
fixture has none, so the test asserts the message). Bloat: for each
Postgres cache of each source, `cache.bloat()` where the method exists
(`getattr`), as `pg-maintain --bloat` does; read `pg_maintain.py` for the
row shape and reuse its function if it has one (make one if it does not,
in `pg_maintain.py`, so the CLI and the page share it — a W03-style split,
small enough to do here).

## Known hazards

* Timing files may be from a run in progress; `directory_data` already
  handles `complete False` and stale files (`stale_note`); show both notes.
* `counts` on the build machine takes a while (exact `len()` under 100k,
  estimates above); never run it on page load — the page loads empty with
  a *Count* button, or with the last result cached in `state`.
* `pg_tune.analyse` reads `/proc` on Linux and psutil elsewhere; it is
  already written to run anywhere.
* Nothing here mutates; every button that would goes to the launcher (W10).

## Steps

1. Timing data + page (+ compare).
2. Counts data + page + SSE.
3. Maintenance data + page.
4. Links: run page → timing (W09 left the link), dashboard → counts.
5. `docs/web.md` section.

## Tests (`tests/test_web_timing.py`, `tests/test_web_counts.py`, `tests/test_web_maintenance.py`)

* With two `timing-merge-*.json` and one `timing-reconcile-0.json` written
  to the fixture's `log_dir` (copy the shape from `tests/test_cli_timing_report.py`):
  `/api/timing` lists phases in `PHASE_ORDER`, numbers equal
  `directory_data`'s; `/timing` contains the stage names and the formatted
  wall time; `?phase=merge` filters.
* `/api/counts?source=aat&cache=recordcache` on the seeded fixture gives
  the seeded count exactly (`exact true`); the SSE variant streams the
  same rows.
* `/api/maintenance` on the fixture returns the unreachable-Postgres
  message and no exception; with `pg_tune.analyse` patched to return a
  sample analysis, the page shows the pending changes marked.

## Acceptance criteria

* On the build machine (Rob): `/timing` matches `lux timing-report`;
  `/counts` matches `lux counts`; `/maintenance` matches `lux pg-tune`
  and `lux pg-maintain --bloat`.
* Tests pass.
