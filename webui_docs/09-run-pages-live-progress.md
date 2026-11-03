# W09. Run pages and live progress

**Size:** L · **Depends on:** W01, W06 · **Unblocks:** W10 · **Done:** 2026-11-02
**Read:** `lux_pipeline/cli/_view.py` (every rendering rule: the header,
the Progress columns, `w<n> waiting`, `total=None` indeterminate,
`start_task` on first status, `set_visible`, the all-workers row, the
log panel's `w<n>` cyan prefix and `LEVEL_COLORS`, `MIN_LOG_ROWS`),
`lux_pipeline/progress.py` (W01: `read_build`, `LogTailer`, `list_runs`,
`read_run`), `lux_pipeline/cli/watch.py`, `lux_pipeline/cli/build.py`
(`PHASES`), `tests/test_build_view.py`

## Intent

The console view in the browser, attached to any run from its files, plus
a list of runs.

| Method, path | What |
|---|---|
| `GET /runs` | the run table (`list_runs`), filter by command / state, newest first |
| `GET /runs/{prefix:path}` | the run page: header, worker bars, all-workers bar, log panel; a phase strip when the run is a build or has a `parent` |
| `GET /api/runs` | `list_runs` as JSON |
| `GET /api/runs/{prefix}` | `{"run": read_run or synthesised, "build": read_build(...), "children": [runs with parent == prefix]}` |
| `GET /api/runs/{prefix}/events` | SSE: a `status` event every `settings.refresh` seconds with the `read_build` dict and run state, `log` events with new lines, a `done` event and close when finished |
| `GET /api/runs/{prefix}/log?worker=3&level=WARNING&offset=...` | a page of log lines (for the initial fill and "load earlier") |
| `GET /api/runs/{prefix}/stdout` | the driver's stdout file if the web runner captured one (W10) |

## Design

### Prefix handling

A prefix is an absolute path under `log_dir`. In URLs it is relative to
`log_dir` (`/runs/reconcile-2026-11-02-141500`); `api/runs.py:
full_prefix(state, rel)` joins and **refuses** anything that resolves
outside `log_dir` (`os.path.commonpath`), 404 otherwise. The web shows
only runs under `log_dir`, as `lux watch --latest` does.

### `read_build` → the page

`run_data(state, prefix) -> dict`:

```python
{"run": {...read_run(prefix) or synthesised as list_runs does...},
 "state": "running" | "finished" | "failed" | "killed" | "unknown",
 "workers": [ {"n", "label", "completed", "total", "percent" | None, "done", "visible", "started", "updated", "elapsed", "eta"} ...],
 "all": {"completed", "total", "percent", "elapsed", "eta"},
 "phases": [...] | None, "current": ..., "children": [{"phase", "prefix", "state"}] }
```

Rules copied from `BuildView`, one for one:

* a worker with no status file: label `waiting`, no bar fill, not started;
* `total None`: indeterminate bar (`.bar.indeterminate`), `M/N` shows
  `completed/—`;
* label: `description` or `phase` or `running`;
* `visible`: from `run["visible_workers"]` when the record has it, else
  all; hidden workers are not rendered and not in the totals;
* elapsed: from the first time a status was seen (the server keeps
  `first_seen[prefix][n]` in memory while it serves events; for a finished
  run use `started` from the run record or the status file's ctime), ETA
  = elapsed × (total − completed) / completed when both known — the same
  estimate Rich's `TimeRemainingColumn` makes;
* `all`: sum of completed over visible; total only when every visible
  worker has one;
* done when every worker's status says `done` (and `seen > 0`).

`progress.js` renders the worker rows from a `status` event: one row per
worker, `w<n>` label, description, a `.bar` with `--pct`, `M/N`, `%`,
elapsed, ETA — the columns of `BuildView.progress` in that order. A done
worker's bar is full and dimmed; a failed run's header turns red.

### The log panel

`logpanel.js`: a bounded buffer (500 lines, as `LogPanel`), each line
`{"worker": n | None, "time", "level", "text"}` parsed server-side from
the worker file's `%(asctime)s %(levelname)-8s %(message)s` format
(`make_file_handler`); lines that do not parse (a traceback continuation)
inherit the previous line's level and worker. Rendering: time, level
coloured by `LEVEL_COLORS` → CSS variables, `w<n>` in the worker colour,
text in monospace. Controls: level select (hide below), worker select,
*follow* toggle (auto-scroll), *first ERROR* jump, *load earlier* (calls
`/log?offset=`). Server side the SSE generator holds one `LogTailer` per
subscription and sends only new lines; the initial fill comes from
`/log` (last 200 lines of each worker's file, interleaved by time).

### SSE generator

```python
async def run_events(state, prefix):
    tailer = LogTailer(prefix, workers)
    while True:
        data = await anyio.to_thread.run_sync(run_data, state, prefix)
        yield sse("status", data)
        lines = await anyio.to_thread.run_sync(tailer.read_new_lines)
        if lines: yield sse("log", [parse_line(n, l) for n, l in lines])
        if data["state"] in ("finished", "failed", "killed"): yield sse("done", data); return
        await anyio.sleep(state.settings.refresh)
```

`sse(event, obj)` formats `event: ...\ndata: <json>\n\n`. Response
`media_type="text/event-stream"` with `Cache-Control: no-cache`. The
client reconnects on a dropped connection (`sse.js`); the server re-sends
the full status every tick, so nothing is lost.

### Build runs

A run whose record has `phases` (a `lux build`, W01) shows a strip:
one chip per phase, state from the child run with `parent == prefix`
and `command == phase` (or the current / pending ones from `current`),
each a link. A phase run with `parent` shows a breadcrumb back to the
build. Children are found by `list_runs(log_dir)` filtered on `parent`;
cache that list per tick (it reads every `.run` in `log_dir`).

### Finished runs

Same page, no SSE: the final `read_build`, the whole log paged, the run
record (argv, started, finished, duration, exit, error) in a `kv`. The
timing JSON for the phase, if present in `log_dir`, is linked
(`/timing?phase=<command>`, W11).

## Known hazards

* The page must render a run **started from a shell** with no `.run`
  file: `run_data` synthesises as `list_runs` does; `argv` shows "(not
  recorded)".
* A `--my-worker` shell-orchestrated run has 24 slices writing one prefix's
  status files and no record (W01): it must still render — it is the
  `*_parallel2.sh` case.
* Never block the event loop: `read_build` reads up to `workers` files
  and `LogTailer` reads files; both in `to_thread`. Keep `refresh` ≥ 0.25 s.
* Log files can be gigabytes at DEBUG. `/log?offset` reads from the end
  (seek, read a block, split) — never the whole file.
* HTML-escape every log line and label; worker logs contain record data.
* Many open SSE connections each tail the files: fine for one operator;
  cap at 8 concurrent per server and return 429 above (W13 may tune).
* `BuildView` is **not** changed here; it already uses `read_build` (W01).
* States are `run_state`'s (W01): `running`, `finished`, `failed`,
  `interrupted`, `killed`, `unknown`. Take a run's worker count from
  `list_runs` / `count_workers`, never the record's planned count alone:
  some managers run fewer workers than they were given.
* A build's phase strip comes from its record's `steps` (W01), which
  include the one-shot steps (clear, new-token, sheets, portal) that write
  no record of their own; link a step to its run through its `prefix`.

## Steps

1. `api/runs.py`: `full_prefix`, `run_data`, `parse_line`, `/log` paging,
   `list` JSON; tests.
2. Run list page.
3. Run page static render (finished run) with `progress.js` rendering the
   initial data.
4. SSE generator; live updates; `logpanel.js` controls.
5. Build strip and breadcrumb.
6. `docs/web.md` section; `CLAUDE.md` row for `lux watch` mentions the web.

## As built

`web/api/runs.py`, `web/routers/runs.py`, `templates/runs/{index,run}.html`,
`static/runs.js`; `progress.describe_run` (one run as `list_runs`
describes it; `list_runs` now calls it) and `progress.run_exists`.
Differences from the design above:

* **Elapsed time** is counted from the run's start (its record, else its
  prefix's timestamp), not from when the server first saw a worker: it is
  stateless, and a page opened late or after a restart shows the real
  figure (`lux watch` attached after a run shows 0:00:00). A worker that
  is done stops at its last status update; the estimate of what remains
  is elapsed x remaining / done, only while the run is running.
* **The log's initial fill and the stream join exactly:** `/log` returns
  each worker's file offset (`offsets`), the page passes them to
  `/events?offsets=0:1234,1:5678`, and the stream reads on from there, so
  no line is lost or shown twice. `starts` is where "earlier lines" asks
  back from (one worker at a time). `tail` reads a file from the end a
  block at a time and never a line still being written.
* Log lines interleave across workers by time; a continuation (a
  traceback) takes its line's level and stays under it.
* **The phase strip** comes from the build's record's `steps` (W01):
  finished / failed / running / pending (killed or "not run" once the
  build is over), each linked to its run; a phase run links back to its
  build ("part of ...").
* `/runs/_latest` goes to the newest run (the dashboard's "watch the
  latest"); the dashboard and the header badge link to run pages.
* Server-sent event streams are capped at 8 at once (429).
* **Testing streams needs a real server.** Starlette's `TestClient` runs a
  response to its end before handing over any of it, so a stream that
  only ends when the test changes files deadlocks.
  `tests/web_client.LiveServer` runs the app on uvicorn on a free port in
  a thread and reads the stream with urllib. `lux serve` (and the test
  server) start uvicorn with `ws="none"`: nothing uses websockets.

Run against the local instance: a page opened on a running `lux validate
--source aat --max-records 3000 --max-workers 2` followed it to the end
over the event stream (14 status events, 82 log lines, then `done`:
finished, exit 0, 6,000 of 11,190) and agreed with `lux watch` on the
same run (3,000 of 5,595 per worker, 6,000 of 11,190 in all).

## Tests (`tests/test_web_runs.py`)

Using `write_fake_run` and the `.run` helpers:

* `/api/runs` lists runs newest first with states; `/runs` renders them.
* `/api/runs/<rel>` for a two-worker run: `workers[1]["label"] == "waiting"`
  when w1 has no file; `percent None` when `total` null; hidden worker
  excluded from `all`; `done` only when both say done.
* `../etc` as a prefix is 404; an absolute path is 404.
* SSE: with `TestClient.stream("GET", ".../events")`, the first event is
  `status`; after appending a line to `-w0.log` the next is `log` with the
  parsed `level` and `worker`; after writing done statuses a `done` event
  arrives and the stream ends. (Write the files between reads from the
  test thread; `refresh` small via `Settings(refresh=0.05)`.)
* `parse_line`: a normal line, a traceback continuation, a line with
  unicode replacement characters.
* `/log?offset` on a 10,000-line file returns the last 200 and an offset
  that pages backwards without overlap or gaps.
* A build run with two child phase runs: `phases` and `children` with the
  right states; the child page has the breadcrumb.
* Equivalence: for the same files, `run_data(...)["all"]` equals the Rich
  `BuildView.progress` all-task's `completed` / `total` after `refresh()`
  (import `BuildView` with a `Console(file=StringIO())`).

## Acceptance criteria

* `lux validate --source aat --max-records 2000 --no-ui` on the laptop
  (or any multi-worker command on the build machine) shows live bars and
  logs at `/runs/<prefix>` that match `lux watch <prefix>` side by side.
* A finished run from last week renders from its files alone.
* Tests pass.
