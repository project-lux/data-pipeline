# W10. Command launcher and the build page

**Size:** L · **Depends on:** W02, W09
**Read:** `02-command-introspection.md`; `lux_pipeline/cli/introspect.py`
(as merged); `lux_pipeline/cli/build.py` (`CommandHandler.lux`, `steps`,
`run_step`, `PHASES`; the `subprocess.run` recipe); `lux_pipeline/cli/_handler.py`
(`refuse_while_building`, `default_prefix`); `lux_pipeline/progress.py`
(`RunRecord`, `run_path`, `pid_alive`); `tests/test_cli_build.py` (how
steps are stubbed)

## Intent

Run any `lux` command from the browser, with a form built from its real
options, the exact command line shown first, confirmation for anything
destructive, and a cancel button; and a build page that drives `lux build`.

| Method, path | What |
|---|---|
| `GET /commands` | the catalogue by group (`introspect.GROUPS`), each with its summary |
| `GET /commands/{name}` | the form; `?` + previous values to prefill (a "run again" link from a run page) |
| `POST /commands/{name}/preview` | values → `argv` (JSON) — the page calls it as you type |
| `POST /commands/{name}` | start: validates, `confirm` phrase if `confirm_needed`, `Runner.start`, redirect to `/runs/<prefix>` |
| `POST /runs/{prefix}/cancel` | `Runner.cancel` (typed confirmation "cancel") |
| `GET /commands/build` | the build page: a phase form (`--from`, `--to`, `--skip` as checkboxes per phase, `--clear`, `--source`, `--max-workers`, `--engine`, `--profile`), the dry-run step list (`lux build --dry-run` run as a subprocess, output shown), *Start build* |
| `GET /api/commands`, `/api/commands/{name}` | specs |

## Design

### `web/runner.py`

```python
class RunHandle: prefix, command, argv, pid, started, stdout_path
class Runner:
    def __init__(self, state): ...
    def argv_for(self, command, argv_tail, prefix) -> list[str]
        # [sys.executable, "-m", "lux_pipeline.cli.entry", command, *argv_tail, "--no-ui", "--prefix", prefix]
        # if "--no-ui" already present do not repeat; never pass --my-worker
    def start(self, command, argv_tail, *, force=False) -> RunHandle
    def cancel(self, prefix, grace=10.0) -> bool
    def reap(self)                                  # thread: waits on started processes, writes exit into .run if the driver did not, calls state.reset() when a run ends
```

`start`:

1. `prefix = default_prefix(cfgs, command)` (the same name a shell run
   would get; `cfgs = state.configs()`, so `log_dir` is right). If the
   command is `build`, the record will be written by `lux build` itself;
   for a `BaseHandler` command (no record from the driver) the runner
   writes a minimal `RunRecord(..., extra={"web": True, "stdout": ...})`
   with `workers=0` so the run list still shows it; for a `CommandHandler`
   command the driver writes the record and the runner only adds
   `web`/`stdout` after it appears (reaper updates it).
2. If `confirm_needed` or the command is a phase: `refuse_while_building(cfgs, command, force)`
   — the same guard the shell applies, applied before the process exists.
   Raises `RuntimeError` → 409 (W06 maps it) with the message, and the
   form shows a "force" checkbox when the command has `--force` (most
   steps do; a phase does not, and then the answer is "wait").
3. `stdout_path = f"{prefix}.out"`; `Popen(argv, stdout=open(stdout_path, "ab"), stderr=STDOUT, start_new_session=True, env={**os.environ, "LUX_PARENT_RUN": ""}, cwd=cfgs.base_dir)`.
   The working directory does not matter to the phases any more (the
   `metatypes-*.json` and `reference_uris.txt` files live under
   `temp_dir`, 2026-11-02); `base_dir` is used so any stray relative
   path lands inside the installation.
4. Register the handle; start the reaper thread if not running.

`cancel`: `os.killpg(os.getpgid(pid), SIGTERM)`; wait `grace`; `SIGKILL`
the group if still alive; `RunRecord.finish(exit=-15, error="cancelled
from the web")` if the driver did not write its own. Only runs the server
started (in `self.handles`) or whose `.run` has `web: true` can be
cancelled from the web; a shell-started run shows "started elsewhere;
cancel it there" (killing a process the operator started at a prompt
from a browser button is a surprise; the pid is shown so they can).

### Forms (`templates/commands/form.html`, `static/cmdform.js`)

From `command_spec`: a fieldset *Global options* (`source`, `max_workers`,
`max_records`, `engine`, `log`; `no_ui`, `prefix`, `my_worker`, `debug`,
`help`, `env` are **not** shown — the runner sets or forbids them) and a
fieldset *Options* for the command's own. Input by `kind`: flag →
checkbox; int/float → number; choices → select with an empty first
option; path/str → text; `nargs` 2 → two inputs; positional → text at the
top. Help text under each. A read-only *Command line* box updates on
every change from `/preview` (debounced) and shows exactly what
`argv_for` will run (`lux` instead of `python -m …`). `source` gets a
datalist of `all`, `internal`, `external`, `results` and every source
name.

Confirmation: when `confirm_needed(spec, values)`, the *Run* button opens
the typed-confirmation modal (`confirm.js`) asking for the command name;
the phrase is posted as `confirm` and checked server-side (never trust
the modal). The page says *why* in one sentence:
`introspect.confirm_reason(spec, values)` (W02), which also knows the
exceptions (`build` only with `--clear`, `pg-maintain` with `--bloat`
alone). Do not keep a second table of reasons in the web package.

### The build page

`lux build --dry-run <options>` is run as a subprocess (`Runner.dry_run(argv_tail)`,
synchronous, 20 s timeout) and its lines shown as the step list; *Start
build* then runs `lux build <options>` through `Runner.start`. The run
page (W09) shows the phase strip. A checkbox *clear first* adds
`--clear` and the confirmation.

### Catalogue page

Groups in `introspect.GROUPS` order, a card per command with its summary
and a *destructive* marker, plus a "Running now" line linking to active
runs. Commands that make no sense from a browser (`interactive`, `help`,
`watch`, `serve`, `initialize`) are listed under *Information* as
descriptions only, without a Run button: `spec["runnable"]` is False for
them (`introspect.NOT_RUNNABLE`, W02). Build forms from
`introspect.command_spec` and command lines with
`introspect.argv_from_values`, which coerces form strings, puts
positionals first, checks choices and required options, and refuses
`my_worker`.

## Known hazards

* `lux build` phases reload the configuration per process; the server's
  `state.reset()` after the run (reaper) is what makes the web see a
  configuration a build changed (none do today, but `sheets` writes
  `data_dir` files that later pages read).
* `Popen` with a file object for stdout: open it in the server, pass it,
  then close the server's copy after `Popen` returns.
* The reaper must survive the server having been restarted: on start-up,
  `Runner.adopt()` scans `list_runs` for `web: true` records with a live
  pid and watches them (it cannot `wait()` on a non-child; poll
  `pid_alive` every second).
* `refuse_while_building` reads `unfinished_workers`; a run the web just
  started has no status files for a second or two, so two quick clicks
  could start two builds. The runner also refuses when it has a live
  handle for a phase command (`self.handles`).
* Never pass `--debug` from the web (`entry.run` re-raises and the process
  would die with a traceback in `.out`, which is fine, but `--debug` also
  switches the log level; let `--log` do that).
* A form value containing spaces or quotes is one argv element; never
  build a shell string, never `shell=True`.

## Steps

1. `runner.py` with tests on a fake `lux` (patch `argv_for` to run
   `python -c` scripts that write status files / sleep / exit non-zero).
2. Catalogue and form pages; `/preview`.
3. Start, redirect, confirmation, cancel.
4. Build page with dry-run.
5. `docs/web.md` section.

From W11: the maintenance page (`templates/maintenance/index.html`) shows
`lux pg-maintain` commands to copy (vacuum, rewrite one table, idmap
indexes) and each bloat row's rewrite suggestion; turn them into links to
the launcher (`/commands/pg-maintain?...` prefilled) here.

Testing note (from W09): to read a run's event stream while the test
starts or changes things, use `tests/web_client.LiveServer` (a real uvicorn
server in a thread); the `TestClient` holds a streaming response until it
ends.

## Tests (`tests/test_web_commands.py`, `tests/test_web_runner.py`)

* `/api/commands` has every command with a group; `/commands/reconcile`
  renders inputs for `recid`, `no_refs` and the globals, and none for
  `my_worker`, `no_ui`, `prefix`.
* `/preview` for the W02 example returns its argv; a value for an unknown
  dest is 400.
* `POST /commands/clear` without `confirm` is 400 with the reason text;
  with the wrong phrase 400; with `confirm=clear` the runner's `start` is
  called (patched) with `--source ... --force` and the response redirects
  to the run page.
* `Runner.argv_for` adds `--no-ui` and `--prefix` once and never
  `--my-worker`; `start` on a fake script creates `<prefix>.out` and, for a
  `BaseHandler`-type command, a `.run` with `web: true`; the reaper writes
  `exit` when the script exits 3; `cancel` on a sleeping script ends it
  and the record says cancelled.
* While a fake running run exists, `POST /commands/reconcile` is 409 and
  the message names the running prefix; `POST /commands/identify` with
  `force=1` passes `--force`.
* `Runner.adopt` picks up a `.run` with `web: true` and `pid` =
  `os.getpid()` as running.
* `/commands/build` shows the dry-run lines (patch `dry_run` to return
  three lines) and posting starts `build` with `--from merge --skip sheets`.

## Acceptance criteria

* On the laptop: start `lux validate --source aat --max-records 500`
  from the form, watch it on the run page, cancel another one mid-way;
  `.out`, `.run`, status and log files are all under `log_dir`.
* On the build machine (Rob): `lux build --dry-run` from the build page
  shows the same steps as the shell; a real build started from the page
  shows the phase strip and completes; `lux watch` and the web agree.
* Tests pass.

## As built

`web/runner.py`, `web/api/commands.py`, `web/routers/commands.py`,
`templates/commands/{index,form,build,_fields}.html`, `static/cmdform.js`;
the Commands area in the navigation, after the dashboard. Differences
from the design above:

* **The record is there from the first moment.** `start` writes a
  provisional `<prefix>.run` (`web: true`, `stdout`, the child's pid,
  `workers: 0`) for every command, so the run page exists when the
  browser is redirected to it. A worker command's driver (and `lux build`)
  replaces it with its own; the web fields survive because the runner
  passes them in `LUX_RUN_EXTRA` (JSON), which
  `CommandHandler.start_record` and `lux build` add to their records
  (`_handler.run_extra`; anyone starting a command can use it). Merging
  into the file after the driver writes it would be lost at the driver's
  next write. `lux build` drops `LUX_RUN_EXTRA` from its phases'
  environment, so only the build reads as web-started: a phase's page
  says "cancel the build".
* **The reaper** polls every half second (`proc.poll()` for a child,
  `pid_alive` for an adopted run). A run that ends without having
  finished its record (a `BaseHandler` command, one that failed before
  its driver wrote one, a cancelled one) is finished with the exit code
  and the last line of `.out` as the error. A SIGTERM'd Python never
  writes its own record, so a cancel is finished as `exit: -15`,
  `error: "cancelled from the web"`. `progress.exit_state` reads a
  negative exit (ended by a signal) as **interrupted**, like 130. An
  adopted run (not this server's child) has no exit code to record: when
  its process goes, the record is left for `run_state` to call killed,
  unless it was cancelled from here.
* **The guard** (`Runner.check`) is the shell's `refuse_while_building`
  for a phase (`GROUPS["phases"]`) and for a run that needs confirming
  (`guarded`: the route passes whether `confirm_reason` gave a reason, so
  `pg-maintain --bloat` is not held up); `force` passes only when the
  command has `--force` and it was ticked. Two phases started from here
  never run at once.
* **Confirmation** is a field under the command line, shown when the
  preview says the run needs it (no modal, no `confirm.js`); the server
  checks the phrase.
* **Forms**: a field per option of `command_spec` not in
  `introspect.HIDDEN` (`no_ui`, `prefix`, `my_worker`, `debug`, `help`,
  `env`). An optional-value option (`timing-report --profile`) has a
  "given without a value" box (`<dest>__bare`); a repeated option
  (`validate --ignore`) a textarea, a line each. `api/commands.form_values`
  reads a submitted form or a query string the same way, for the preview,
  the start and a prefilled form. A form sent back with an error keeps
  what was typed.
* **Run again**: `introspect.values_from_argv(spec, argv)` reads a run's
  recorded command line back through the real parsers (the options a form
  never shows and defaults left out); `api/commands.run_again_url` makes
  the query string (for a build, the phase boxes from `--skip`). A test
  checks every option of every runnable command round-trips: form values
  → command line → link → form values → the same command line.
* **The build page** has phase boxes (`phase`, with `phases_shown` to tell
  "none ticked" from "not this form"; unticked phases become `--skip`),
  *clear first*, `--from` / `--to`, and the other options as fields.
  *Show the steps* posts to `/api/commands/build/dry-run` from the page,
  or, without scripts, reloads it with `?preview_steps=1`; the lines are
  `lux build --dry-run`'s `[phase]` lines with the interpreter shown as
  `lux`.
* `POST /api/commands/<name>/preview` takes a form (the page's; the error
  is in the answer) or JSON `{"values": {...}}` (an option the command
  lacks, or a value it cannot take, is a 400) and returns the argv too.
* Acceptance on the laptop (2026-11-02): `lux validate --source aat
  --max-records 500` started from the form (15 workers, mp) finished;
  its `.run` (with `web`, `stdout`), `.out`, status, log and errors files
  are all in `log_dir`; a second one with `--max-records 200000` was
  cancelled from its page at about 3,000 records and read as interrupted,
  no worker left. The build page's steps for `--source ycba`, reconcile
  and merge ticked, match `lux build --dry-run` in the shell.

