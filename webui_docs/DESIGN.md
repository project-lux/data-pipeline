# Design: the LUX pipeline web UI

Written 2026-11-02 against `main` at `7f858d8` (every `refactor_docs` item
merged except 33 and 42; PR #355 moved the files the phases hand each
other, `metatypes-*.json` and `reference_uris.txt`, under `temp_dir`, so
no command depends on its working directory). Read `CLAUDE.md` for the
shape of the repository the design builds on; this document assumes it.

## 1. What it is

`lux serve` starts a local web application (default `http://127.0.0.1:8321`,
decided 2026-11-02) that gives an operator, in a
browser, everything the `lux` command line gives them at a prompt:

| Area | What you can do | Today's equivalent |
|---|---|---|
| **Dashboard** | See the installation at a glance: base paths, source counts, what is running, the last build's outcome, health | `lux testinstall`, `lux list`, `<log_dir>/pipeline_process_status.txt` |
| **Configuration** | Browse every configuration record by type; edit a record as a form (known keys) or raw JSON; validate; save back to the file it came from; add / delete records; reload | editing `config_cache/*.json` by hand, `lux list`, `lux testinstall` |
| **Records** | Look up anything (a URI, a YUID, a `source:identifier` curie, a bare identifier); see the record in every cache it passed through; its identity cluster; validate it; sample a cache | `lux interactive`, `manage-data.py --debug-record`, `psql` |
| **Runs** | List every run (from its status files), attach to a running one and watch progress and logs live, read a finished one | `lux watch`, `tail -f` |
| **Commands** | Run any `lux` command from a form generated from its options, see the exact command line first, confirm destructive ones, cancel; run `lux build` as a phase strip | typing `lux ...` |
| **Timing & maintenance** | Timing report, profiles, counts, Postgres tuning advice and bloat | `lux timing-report`, `lux counts`, `lux pg-tune`, `lux pg-maintain --bloat` |
| **Identity graph** | How reconciliation connected two URIs, drawn | `lux viz` |

It is a single-operator tool for the build machine (or a laptop with the
local instance), not a public service. It assumes one trusted user at a
time, like the shell it replaces, but it must never be *less* safe than
the shell: the same guards (`refuse_while_building`, `--force`,
confirmations) apply, and a bind to a non-loopback address needs a token.

## 2. Architecture in one picture

```
 browser ──HTTP/SSE──► lux serve (FastAPI + Jinja2, one process)
                          │
                          ├── reads  <log_dir>/<prefix>-w<n>.status|.log, <prefix>.run, timing-*.json
                          ├── reads/writes <basepath>/config_cache/*.json and sub-config stores
                          ├── reads  caches / idmap through Config (lazily instantiated)
                          │
                          └── starts  python -m lux_pipeline.cli.entry <cmd> ... --no-ui --prefix P
                                         │
                                         └── workers write the status / log files above
```

Three rules make this hold together, and they are the same rules the
console UI already lives by (`refactor_docs/02`, `06`, `07`):

1. **Files are the interface between a run and anything watching it.**
   Workers write `<prefix>-w<n>.status` (JSON, `StatusWriter`) and
   `<prefix>-w<n>.log`. `BuildView` renders them; so will the web page.
   Nothing crosses a process boundary but files.
2. **A command is a process.** The web server starts commands the way
   `lux build` starts phases and never imports a manager into its own
   process. The server can therefore be restarted while a build runs and
   re-attach to it from the files.
3. **One function, two renderers.** Whatever the console prints as a
   table, the web shows as a page from the same `*_data()` function.
   Where that function does not exist yet (counts, testinstall), an
   enabling item adds it; `timing_report.directory_data` and
   `pg_tune.analyse` are the precedents.

### Why no front-end framework

The UI is a few dozen pages of tables, forms, one live progress screen and
one graph. Server-rendered templates cover all of it; the live parts are a
progress list fed by Server-Sent Events and a log panel, which is a hundred
lines of JavaScript. A React/Vue build would add a Node toolchain to a
Python repository, a second package manager, a second test runner, and a
class of bugs (state duplicated client-side) that the file-based protocol
exists to avoid. htmx would be a reasonable middle ground but means
vendoring a third-party script; the plan avoids that too so the package
stays pure Python plus its own static files. Revisit only if a page turns
out to need rich client-side state; none in this plan does.

### Technology

| Piece | Choice | Why |
|---|---|---|
| Server | FastAPI on uvicorn | Typed query/form parameters, automatic `/api` docs, `TestClient` for tests, SSE via `StreamingResponse`; widely documented, which matters for the implementer |
| Templates | Jinja2 (`fastapi.templating.Jinja2Templates`) | Server-rendered pages, autoescaping on |
| Live data | Server-Sent Events, polled from files every 0.5 s server-side | Same cadence as `BuildView`; one-way, trivially reconnectable, no websockets |
| Styling | One stylesheet `static/lux.css`, CSS variables, `prefers-color-scheme` | Light and dark for free; no build step |
| JavaScript | ES modules under `static/`: `progress.js`, `logpanel.js`, `jsontree.js`, `confirm.js`, `editor.js` | Small, hand-written, each one page's live behaviour |
| Graph layout | `networkx.spring_layout` server-side, rendered as inline SVG by a template | No client graph library; `networkx` is already an optional extra |
| Packaging | `web = ["fastapi", "uvicorn", "jinja2", "python-multipart"]` extra; templates and static files as package data | Optional for everyone who only wants `lux` |

### Where it lives

```
lux_pipeline/web/                     the application (item W06 creates it)
    __init__.py
    app.py            create_app(settings) -> FastAPI; mounts static, registers routers
    settings.py       Settings: host, port, token, basepath, log_dir, config reload policy
    state.py          AppState: the server's Config handle (see §5), the Runner, caches of cheap data
    security.py       CSRF token, bearer/cookie token for non-local binds, confirm phrase check
    runner.py         start / list / cancel commands as subprocesses; writes .run records
    routers/          one module per area: dashboard, config, records, runs, commands, timing, graph
    api/              the JSON routes the pages and tests use (same functions as routers/)
    templates/        base.html + one directory per area
    static/           lux.css, *.js, favicon
lux_pipeline/cli/serve.py             `lux serve` (BaseHandler, needs_configs = False)
```

Routers call plain functions in the existing package (`counts_data`,
`timing_report.directory_data`, `config_store.write_record`,
`records.journey`, ...). Nothing of substance is implemented inside the
web package: it is routes, templates and glue, so it can be tested through
the API and the functions can be tested without it.

## 3. The screens

Navigation (left rail on wide screens, top bar on narrow): **Dashboard ·
Configuration · Records · Runs · Commands · Timing · Health**. Every page
has the installation's `LUX_BASEPATH` in the header, a clock, and a
"running" badge that links to the active run when there is one.

### Dashboard (`/`)
Cards: *Installation* (base_dir, data_dir, log_dir, exports_dir, max_workers,
Postgres target from `_services.postgres_settings`), *Sources* (internal /
external / results counts, link to Configuration), *Running now* (from
`unfinished_workers` grouped by prefix, each with an all-workers bar),
*Last build* (last lines of `pipeline_process_status.txt`), *Health*
(the latest `testinstall` results, with a "re-check" button that runs
them in a thread and streams lines). Quick actions: *Start build*, *Watch
latest*, *Counts*.

### Configuration (`/config`)
Left: record list grouped by type (base/system, caches/defaults, internal,
external, results, map, marklogic, globals, config, build) with the file
each came from. Right: the record. Two tabs: **Fields** — known keys from
`config_schema.py` as typed inputs with help text, `*Class` values checked
with `importObject` live, path keys shown resolved; **JSON** — a textarea
with the raw record, validated on blur. *Save* shows a unified diff of the
file before writing it. *New record* from a template per type. *Delete*
with typed confirmation. *Reload configuration* re-reads everything.
Refused while a build runs (banner explains; `--force` equivalent is a
checkbox for an operator who knows).

### Records (`/records`)
A search box that accepts anything: a full URI, a `##qua` key, a YUID, a
`source:identifier` curie, or a bare identifier with a source chosen. The
result page is the record's **journey**: tabs for *datacache* (raw), *mapped*
(the acquirer's transform, not stored), *recordcache* (reconciled), *identity*
(YUID and cluster members, each a link), *merged*, *marklogic*, each
showing the row's metadata (insert / record / refresh time, valid, change)
and the JSON as a collapsible tree with a raw view and a download. *Validate*
runs `cfgs.validator.validate`. The *Browse* side panel samples a cache
(first N keys, by type, or since a time) for a source and cache type.

### Runs (`/runs`)
A table of runs found under `log_dir` (newest first): command, started,
workers, state (running / finished / failed / killed), duration, link.
The run page is the console view in HTML:

```
┌ LUX Data Pipeline        <log_dir>/reconcile-2026-11-02-141500        14:21:07 ┐
│ w0  ycba        ████████████░░░░░░░░  12,345/98,765  12%  0:03:12  0:22:40      │
│ w1  ycba        ███████████░░░░░░░░░  11,980/98,765  12%  0:03:12  0:23:01      │
│ ...                                                                              │
│ all 24 workers  ████████████░░░░░░░░ 296,280/2,370,360 12% 0:03:12  0:22:55     │
├ Log  [level ▾] [worker ▾] [follow ✓]                                             ┤
│ 14:21:06 INFO     w3  acquired 12,000 records                                    │
│ 14:21:07 WARNING  w7  ...                                                        │
└──────────────────────────────────────────────────────────────────────────────────┘
```

Rows, columns, colours (`LEVEL_COLORS`, cyan `w<n>`), "waiting" for a
worker with no status yet, an indeterminate bar until a total is known,
hidden idle workers, the "all workers" row: all exactly as `BuildView`
(`lux_pipeline/cli/_view.py`). Plus what a terminal cannot do: filter the
log by level and worker, pause following, jump to the first ERROR, and
for a `lux build` run a phase strip (clear · sheets · reconcile · merge ·
portal · export) with each phase a link to its own run.

### Commands (`/commands`)
A catalogue of every `lux` command (from `help.list_commands`), grouped:
*Build phases* (download, harvest, load, load-index, reconcile, merge,
export, portal, sheets, build), *Steps* (identify, metatypes, new-token,
write-refs, build-differents, refetch, validate), *Stores* (counts, clear,
backup, restore, pg-maintain), *Information* (list, timing-report, pg-tune,
testinstall, guess-dump-type, viz). Each command's page is a form generated
from its argparse options (item W02): global options (`--source`,
`--max-workers`, `--engine`, `--max-records`, `--log`) then its own, with
help text, choices as selects, flags as checkboxes. The exact `argv` is
shown as you fill it in. *Run* starts it and goes to its run page.
Destructive commands (clear, restore, new-token, identify, write-refs,
metatypes, pg-maintain without `--bloat`, build with `--clear`) require
typing the command name to confirm. A second build-type run while one is
running is refused, as `refuse_while_building` refuses at the shell.

### Timing (`/timing`), Health (`/health`), Counts (`/counts`), Graph (`/graph`)
Timing: `timing_report.directory_data` as the Rich view lays it out (a
panel per phase, stage bars, slices, the all-phases summary), with
`--profile` tables when `.prof` files exist. Health: `testinstall`'s checks
as rows, re-runnable. Counts: `lux counts` as a table, re-runnable
(estimates marked `~`). Graph: two URIs in, `lux viz`'s replay out as an
SVG with a key and the shortest path.

## 4. The data flows, precisely

### 4.1 Watching a run

Today `BuildView._refresh` reads the status files and updates Rich tasks in
one method, and `LogTailer` lives in the Rich module. W01 moves the reading
into `lux_pipeline/progress.py` (no Rich there, by its design rules):

```python
def read_build(prefix, workers, visible=None) -> dict
# {"workers": {n: status-or-None}, "labels": {n: label}, "seen": int, "done": bool,
#  "completed": int, "total": int|None}        # the all-workers numbers, visible workers only
class LogTailer: ...                            # moved verbatim from cli/_view.py
def count_workers(prefix) / latest_prefix(log_dir)   # moved from cli/watch.py
def list_runs(log_dir) / run_state(record, build)    # new
```

(Built in W01, 2026-11-02; `01-run-files-and-shared-state.md` is the
reference for the details.)

`BuildView.refresh` becomes "call `read_build`, update the tasks"; the web
SSE generator calls the same `read_build` and the same `LogTailer`, so the
two views cannot disagree about what a run is doing.

### 4.2 The run record

The status files say what workers are doing; they do not say what command
was run, with which options, by which process, or whether the driver
finished. `lux watch` gets by without; the web UI needs it to list runs,
show the command line, know whether a process can be cancelled, and group
a build's phases. W01 adds `<prefix>.run`, a JSON file the **driver**
writes (never a worker):

```json
{"prefix": "/.../logs/reconcile-2026-11-02-141500", "command": "reconcile",
 "argv": ["lux", "reconcile", "--source", "all", "--max-workers", "24"],
 "host": "lux-build", "pid": 48213, "engine": "mp", "workers": 24,
 "visible_workers": [0, 1, "..."], "sources": ["ycba", "yuag", "..."],
 "started": 1762111500.1, "finished": null, "exit": null, "error": null,
 "parent": null}
```

written at the start of `CommandHandler.process` (after the manager is
built) and rewritten at the end with `finished`, `exit` (0; 1 with
`error` the exception text; 130 for an interrupt) and the worker count
the status files show. `entry.run` re-finishes it when a command fails
after its workers succeeded. `lux build` writes one for itself with
`"phases"`, `"current"` and `"steps"` (each step's command, prefix,
times and exit status, the one-shot steps included), gives every worker
phase a prefix `P-<phase>`, and each phase records the build as its
`parent`. The web runner adds `"web": true` and the stdout file path. A
run's state comes from its record: its exit status once finished,
*running* while its driver lives, *killed* once it is gone (`run_state`). `unfinished_workers` and `refuse_while_building` are
unchanged: they still read the status files, so a run started at a shell
without `.run` is still seen.

### 4.3 Starting a command

`web/runner.py`:

```python
class Runner:
    def start(self, command, argv_tail, *, prefix=None) -> RunHandle
    def cancel(self, prefix, grace=10) -> None          # SIGTERM the process group, SIGKILL after grace
    def running(self) -> list[RunHandle]                # from .run files with a live pid
```

`start` builds `[sys.executable, "-m", "lux_pipeline.cli.entry", command,
*argv_tail, "--no-ui", "--prefix", prefix]` (the `lux build` recipe),
sets `start_new_session=True` so cancel can signal the group, redirects
stdout/stderr to `<prefix>.out`, passes the environment through
(`LUX_BASEPATH`, `LUX_ENV_FILE`, `LUX_PROFILE`), and returns at once. It
does not wait for the process; the run page reads the files. A thread
reaps the process to record `exit` in `.run` if the driver died before
writing it. Before starting anything that `refuse_while_building` would
refuse at the shell, the runner calls `refuse_while_building` itself so
the web cannot do what the shell will not; the form's "force" checkbox is
`--force` where the command has it.

### 4.4 Reading and writing configuration

`Config` reads records through an `FsCache` over `<basepath>/config_cache`
and, for sub-config stores (item 35), through each store's own cache. It
does not remember which store a record came from. W04 adds
`Config.record_origins: {key: {"store": None | store key, "path": str}}`
filled in by `handle_config_record` (one new optional parameter, default
`None`, so every existing caller is unchanged), and a module
`lux_pipeline/config_store.py`:

```python
def read_record(cfgs, key) -> dict                     # the raw JSON from the file, not the live cfg dict
def validate_record(cfgs, key, data) -> list[str]      # problems; [] means writable
def write_record(cfgs, key, data) -> str               # path written; atomic, indent=4, trailing newline
def delete_record(cfgs, key) -> str
def new_record(cfgs, key, data, store=None) -> str     # into the main directory or a named sub-config store
def diff_record(cfgs, key, data) -> str                # unified diff of the file as it would be written
```

The live `cfg` dicts are **not** what is edited: `instantiate` adds
`datacache`, `mapper`, `all_configs` and resolved paths to them, and
`resolve_indexes` rewrites `indexes`. The editor works on the file's JSON
and reloads the configuration after a write (`runtime.reset_configs()`,
then a fresh `Config(instantiate=False)`). `validate_record` checks: valid
JSON object; `type` present and known to `handle_config_record`; every
`*Class` value resolves through `importObject` (and warns on a legacy
path); `indexes` parses through `resolve_indexes` on a copy; `name`
matches the key for source / map / marklogic records; the key is not one
that would collide across stores. Writes are refused while a build runs
(spawned workers re-read the configuration), with the same force escape.

`lux_pipeline/config_schema.py` is a hand-written catalogue of the keys
each record type is known to have — name, type (str / int / bool / list /
dict / path / class / enum), which base attribute or reader uses it, help
text — so the form can show fields, and unknown keys fall through to the
JSON tab. It describes; it never rejects: a key the catalogue does not
know is legal (sources carry source-specific keys).

### 4.5 Looking at records

`lux_pipeline/records.py` (W05) wraps what `lux interactive`, `lux viz` and
`manage-data.py --debug-record` do by hand:

```python
def resolve(cfgs, text, source=None) -> Ref     # Ref(kind: "yuid"|"uri"|"curie"|"identifier", source cfg or None, identifier, uri, typed_key)
def journey(cfgs, ref) -> dict                  # {"datacache": row|None, "mapped": rec|None, "recordcache": row|None,
                                                #  "identity": {"yuid", "members": [...]}|None, "merged": row|None, "marklogic": row|None}
def sample(cache, n=50, typ=None, since=None) -> list[str]
def validate(cfgs, rec) -> list[str]
def identity_graph(cfgs, uri_a, uri_b) -> dict  # viz.replay + node_labels + graph_edges + shortest_path, as JSON
```

Keys: internal record caches hold bare identifiers, external ones
`identifier##quaType` (until `refactor_docs/42` changes that); `merged` /
`marklogic` are keyed by YUID. `journey` tries the typed and bare forms
and says which it found. Every cache read goes through the
`AbstractCache` contract (`get`, `metadata`, `iter_keys_type`,
`iter_keys_since`), so the filesystem caches of the test installation and
the Postgres caches of the build machine behave the same. `sample` takes
`itertools.islice` of the iterator and calls `end_read()` after, so a
Postgres cursor is not left open.

## 5. The server's relationship with `Config`

A `Config` built with `instantiate=True` opens the idmap and every
source's caches. The server must not do that at start-up (24 workers'
worth of connections for a page nobody asked for; `lux build` deliberately
sets `needs_configs = False` so the driver never holds the idmap open
while identify rewrites it). The rule:

* `AppState.configs(instantiate=False)` is built on demand and cached; it
  is what Configuration, Dashboard, Commands and Runs pages use. They
  never touch a store.
* Records, Counts and Health pages ask `AppState.source(name)`, which
  calls `cfgs.instantiate(name, group)` for that source only (idempotent:
  `instantiate` returns the components already built), and
  `AppState.idmap()`, which calls `cfgs.get_idmap()` and then
  `disable_memory_cache()` so a long-lived server never answers from a
  stale in-process cache after identify rewrote the map.
* `AppState.reset()` drops everything (`runtime.reset_configs()`, close
  what has `shutdown()`), and is called after every configuration write
  and when a run finishes (the `Runner` reaper thread fires it).
* The server loads the configuration through `runtime.get_basepath()`
  and honours `--env`, so `lux --env staging serve` serves another
  installation.

## 6. Changes to existing code (the enabling items)

Every one of these is a refactor that leaves `lux` behaving as it does
today, has its own tests, and is a PR on its own before any web code.

| # | Module | Change | Why the web needs it |
|---|---|---|---|
| W00 | `tests/webui_fixtures.py` | a database-free installation written in test code | a real `Config` for every later test |
| W01 | `lux_pipeline/progress.py` | `read_build`, `LogTailer`, `count_workers`, `latest_prefix`, `list_runs`, `run_state`, `RunRecord` (`<prefix>.run`); `cli/_view.py` and `cli/watch.py` import them from here | one reader for both views; a list of runs; the command line and liveness of a run |
| W01 | `cli/_handler.py` `CommandHandler.process`, `cli/entry.py`, `cli/build.py` | write and finish the run record; entry re-finishes it after a late failure; build records its steps and passes `parent` | run list, build phase strip, cancel |
| W02 | `cli/introspect.py` (new) | `command_spec(name)`: a command's options from its `ArgumentParser` as data; `all_command_specs()`; `argv_from_values(spec, values)`; classification (phase / step / store / info; destructive) | forms generated from the real options, never a hand-kept copy that drifts |
| W03 | `cli/counts.py`, `cli/testinstall.py`, `cli/list.py` | `counts_data(cfgs, sources, caches, maps, indexes)` and `Check` collecting `results` (`[(label, state, detail)]`) with printing done from them; `list.source_rows(cfgs, which)` | tables as data |
| W04 | `config.py` `handle_config_record(k, rec, origin=None)` + `record_origins`; `config_store.py`, `config_schema.py` (new); `cli/config.py` (new, `lux config show|set|check`) | the editor writes to the right file; a CLI face so it is testable and useful without the web |
| W05 | `records.py` (new); `cli/show.py` (new, `lux show <ref>`) | the journey, sampling, resolution; `viz.replay` reused | record pages |
| W06 | `pyproject.toml` `web` extra and package data; `cli/serve.py` (new); optional `web` block in `base`/`system` (`host`, `port`) | the server |

Nothing under `lux_pipeline/process`, `storage` or `sources` changes in
any item. `BuildView`'s rendering is untouched beyond calling `read_build`.

## 7. What is deliberately out of scope

* **Git.** The editor writes the files; on the build machine the config
  directory is a checkout of `pipeline-configs`, and committing it stays
  in the shell (decided 2026-11-02). The UI shows the file path it wrote.
* **Multi-user, roles, audit log.** One trusted operator. W13 adds a token
  and a `web_actions.log` of mutations (who is "the browser", what argv,
  what file), which is enough for a build machine.
* **Editing records in caches.** Records are read-only in the UI. The
  pipeline recomputes them; hand edits would be lost on the next phase.
* **Scheduling.** Cron stays cron. A *Start build* button is enough.
* **A general query interface** over Postgres. Lookups and samples only;
  `psql` is the right tool for the rest.
* **Replacing `lux watch` or the Rich view.** They stay; W01 makes them
  share a reader with the web, nothing more.

## 8. Order of work and effort

W00 is the test installation the later items' tests build on. W01–W05
are the enabling refactors (each S or M, each independent of the others
except as the index says). W06 is the skeleton every page needs. W07–W12
are the pages, independent of each other once W06 is in, so they can go
in parallel. W13 is hardening and docs, last. Rough total: five small,
four medium and five large items; the large ones (W06, W07, W08, W09,
W10) are mostly templates and routes.

Build-machine verification (anything needing the live Postgres, the real
idmap or a running build) is Rob's; a PR says what it could not verify
locally, as `CLAUDE.md` asks.
