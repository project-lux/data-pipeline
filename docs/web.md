# The web UI (`lux serve`)

A browser view of an installation: its configuration, its records, its
runs, and the `lux` commands, for one operator on the build machine (or a
laptop with a local instance). It reads what the console reads -- the
workers' status and log files, the configuration files -- through the
same functions the `lux` commands print from, and it runs commands as
`lux` processes of their own, exactly as a shell would. Nothing it shows
is kept anywhere but in the installation's own files. (The plan it was
built from is in `webui_docs/`.)

## A first session

1. Install the extra and start the server from the repository, with the
   installation's `.env` in place (as for any `lux` command):

   ```
   pip install -e '.[web]'
   lux serve
   ```

   A browser opens at `http://127.0.0.1:8321/`: the **Dashboard**, with
   the installation's directories, its sources, and what is running.
2. **Find a record.** *Records*: type `aat:300033618` (or any URI, a
   `...##quaType` key, a YUID, or a source's own identifier with the
   source picked). The tabs show it in each cache, its identity and the
   merged record. *Graph with this* beside another member of the identity
   draws how reconciliation joined them.
3. **Start a build.** *Commands* → *the build page*: tick the phases,
   pick the sources and workers, *Show the steps* to see the `lux`
   commands it will run, then *Start the build* (with *clear first*,
   type `build` to confirm).
4. **Watch it.** You land on the build's run page: its phases as they go,
   each linking to its own run with a row per worker, the bars, and the
   log as it is written. Leave and come back from *Runs* or the header's
   *running* badge; `lux watch --latest` in a terminal shows the same.
   *Cancel the run* (type `cancel`) stops it and its workers.

## Install and start

```
pip install -e '.[web]'        # fastapi, uvicorn, jinja2, python-multipart
lux serve                      # http://127.0.0.1:8321/, opens a browser
lux serve --port 9000 --no-open
lux --env staging serve        # another installation's .env
```

The defaults can go in the base (or system) record:

```json
"web": {"host": "127.0.0.1", "port": 8321}
```

Options win over it. The configuration is read when a page first needs
it, so a broken installation still serves a page saying what is wrong
(and `/healthz` answers regardless, for a monitor).

## Reaching it from another machine

Prefer an SSH tunnel to an open bind: the server stays on loopback, with
no token needed.

```
ssh -L 8321:127.0.0.1:8321 buildhost      # then http://127.0.0.1:8321/ on the laptop
```

A bind other than `127.0.0.1` / `localhost` / `::1` is refused without a
token:

```
lux serve --host 0.0.0.0 --token "$(openssl rand -hex 16)"
LUX_WEB_TOKEN=... lux serve --host 0.0.0.0           # or LUX_WEB_TOKEN in ./.env or ~/.lux/.env
```

With a token, every request needs it except `/healthz`, the sign-in page
and its stylesheet. A browser is sent to `/login`, which asks for the
token once and keeps it as a cookie (HTTP only, same site); a link
`http://host:8321/login?token=<token>` does the same. Other clients send
`Authorization: Bearer <token>`. The server logs one line at start-up
saying which way it is protected. The token travels in clear over plain
HTTP: on a network you do not trust, use the tunnel.

## The pages

* **Dashboard** (`/`): the installation's directories, worker count and
  Postgres target; the sources by group; what is running now, with its
  progress; the recent runs under `log_dir` (as `lux watch --latest` finds
  them, with their `<prefix>.run` records); the last lines of
  `pipeline_process_status.txt`; the last health check. The header's
  *running* badge shows from any page while a run is going.
* **Commands** (`/commands`): every `lux` command by group, each a form
  built from its own options (a tick box for a flag, a list for a choice,
  a number, a line per value for a repeated option), the sources offered
  as you type. Under the form is the exact command line it will run,
  updated as you fill it in. A run that deletes or rewrites what a build
  depends on (`clear`, `identify`, `build --clear`, `pg-maintain` other
  than `--bloat`, ...) says what it will do and starts only once you type
  the command's name. A build phase, or such a run, is refused while a
  build is running, as in the shell; a command with `--force` can be
  forced. A run started here is a process of its own (as `lux build` runs
  its phases), its output in `<prefix>.out` beside its status and log
  files in `log_dir`; you go straight to its run page. The server can be
  restarted while it runs. `config`, `serve`, `watch`, `help`,
  `interactive` and `initialize` are described, not run. *Build*
  (`/commands/build`): `lux build` with a tick box per phase, `--from` /
  `--to`, *clear first*, the sources, workers, engine and profiling; *Show
  the steps* lists what `lux build --dry-run` prints for those choices.
* **Runs** (`/runs`): every run under `log_dir`, newest first, with its
  state (running, finished, failed, interrupted, killed when its process
  died without finishing, unknown for an old shell run), filtered by
  command or state. A run's page is `lux watch` in the browser: a row per
  worker (what it is doing, its bar, done/total, %, elapsed, remaining),
  the all-workers row, and the log, all following the run live and
  stopping when it ends. The log holds the workers' lines and the
  driver's own (`<prefix>.log`, shown as `driver`): what the command did
  before, between and after its workers, so a lone `lux reconcile`'s
  identify, metatypes and write-refs are followed after its workers have
  finished, and a one-shot step (sheets, portal, and a build's identify
  phase: identify, metatypes, write-refs) has a log of its own. The log can
  be filtered by level and worker, stop following, jump to the first
  error, and read a worker's earlier lines. A `lux build` shows its
  phases, each linked to its own run, says which step it is on, and its
  log follows that step (lines tagged with the phase), passing to the
  next step as the build does; each phase links back to its build. A run
  started from a shell slice by slice shows too. `/runs/_latest` opens the newest. *Run again* opens the
  command's form filled in as the run was. A run started from the web can
  be cancelled from its page by typing `cancel`: its process and its
  workers are sent SIGTERM (SIGKILL ten seconds later if still there), and
  the run reads as interrupted. A run started in a shell cannot be: stop
  it there (its pid is shown; one started with `lux --background` stops
  with `kill -- -<pid>`, which reaches its workers too); a phase of a web-started build is stopped
  by cancelling the build.
* **Records** (`/records`): type anything you can name a record by (a
  URI, a `…##quaType` key, a curie like `aat:300033618`, a YUID, or a
  source's own identifier with the source chosen) and see where it is: a
  tab for each stage (datacache, recordcache, reconciled, mapped,
  identity, rewritten, merged, MarkLogic), marked found or missing, each
  with its key, times and size, the record as a collapsible tree (every
  `id` a link to its own lookup) or raw, a download, and validation
  against the schemas. The identity lists the members as links. *Map it
  now* runs the source's mapper on the stored raw record (nothing is
  fetched or stored). *Browse a cache* lists keys of any source's cache,
  by type or since a time. Nothing on these pages changes anything.
* **Graph** (`/graph?a=<uri>&b=<uri>`): `lux viz` in the browser. Give
  two URIs (or `##qua` keys) of one identity and see how reconciliation
  connected them. The identity's records are reconciled again in the
  server with debug recording on, and nothing is stored. With
  `allow_network` on, records not cached may be fetched. The drawing marks
  each kind of connection: a solid line for an equivalent, dotted for an
  identifier index hit, dashed for a name index hit. Nodes are coloured by
  source and the shortest path is thicker. A node links to its record.
  Below it are the path and a key from each label to its URI. The record
  page's identity tab has *Graph with this* beside each other member. The
  first graph builds every source's reconciler, so it is slower. A replay
  that takes over two minutes is abandoned with a pointer to `lux viz`.
  Label numbers follow the order the replay meets the URIs, so they can
  differ from one run to the next, in the shell as here.
* **Counts** (`/counts`): `lux counts`, run when you ask (it takes a
  while on the build machine): the caches of the sources you name (a
  group, names, or all), the maps, the indexes; rows arrive as each is
  counted, with totals. Estimates are `~`, exact counts `=`. The last count
  is shown when you come back.
* **Timing** (`/timing`): `lux timing-report` laid out as cards, one per
  phase in build order: records, wall time, worker and cpu hours, the
  share of worker time in each stage, the nested stages, slow slices,
  profiles (`?profile=20` after a `--profile` run); then all the phases
  together, with a timeline, where the time went, and stragglers.
  `/timing/compare` sets two runs' timing directories (inside the
  installation) side by side, changes coloured better or worse.
* **Maintenance** (`/maintenance`): `lux pg-tune`'s view of the machine
  and Postgres, the settings to change for the build's workers and the SQL
  to do it; and, on request, `lux pg-maintain --bloat` for each cache
  table: rows, size, and the space VACUUM will not give back, with the
  command that would. Nothing on the page changes a table: its
  `lux pg-maintain` commands are links to the command's form.
* **Configuration** (`/config`): every record, grouped by type, with the
  file (and sub-config store) it came from, and the sources at a glance.
  A record's page has two ways to edit it. *Fields*: the keys the
  catalogue knows, with their help, where a path resolves to and whether
  it exists, and whether a class imports. *JSON*: the file's text, checked
  as you type, with the diff of what saving would change. Either way the
  record is saved as normalized JSON (2-space indent; a hand-formatted
  file is reformatted the first time it is saved), so a commit in the
  configuration's git checkout shows just what changed. Both write
  exactly what `lux config` writes, refuse an
  invalid record (keeping what you typed), and refuse while a build runs
  unless you tick "write anyway". New records start from a template; a
  record is deleted by typing its key. *Reload from the files* re-reads
  the configuration after editing it elsewhere. Committing the
  configuration checkout stays in the shell.
* **Actions** (`/actions`): everything this UI changed or started, newest
  first: configuration writes, new and deleted records, reloads, commands
  started (with their command lines) and cancelled, health checks, counts
  and bloat reports run, each with the time and the address it came from.
  The file is `<log_dir>/web_actions.log`, one JSON object per line.
* **Health** (`/health`): `lux testinstall`'s checks, run on request in
  the background, each row arriving as its check finishes. Unlike the
  command, every check runs even after one fails; one that needs what
  failed says so. Spawning worker processes and comparing Postgres's
  settings with `lux pg-tune` are off unless ticked.

Everything a page shows is also JSON under `/api/` (`/api/dashboard`,
`/api/dashboard/running`, `/api/runs`, `/api/runs/<run>` (and its `log`,
`events`, `stdout`, `timing?worker=<n>|driver`), `/api/records/journey?q=`, `/api/records/sample?source=`, `/api/graph?a=&b=`,
`/api/counts`, `/api/timing`, `/api/maintenance/tune`,
`/api/maintenance/bloat`,
`/api/config`, `/api/config/<key>`, `/api/commands`, `/api/commands/<name>`
(a command's options), `POST /api/commands/<name>/preview` (a form, or
JSON `{"values": {option: value}}`, to its command line),
`/api/health`, `/api/actions`); `/api/docs` lists the routes.

## How it runs commands

A form's values are turned into a command line by the command's own
parser, and the command runs as a process of its own:

```
python -m lux_pipeline.cli.entry <command> <options> --no-ui --prefix <log_dir>/<command>-<timestamp>
```

in a new session (so cancelling it reaches its workers), with
`LUX_BASEPATH` passed on and its standard output and errors in
`<prefix>.out`. Its record, `<prefix>.run`, is written the moment it
starts (`"web": true`, the pid, the `.out` file) and the command's driver
then keeps it as a shell run would. When the process ends without
finishing its record (it failed early, or it was cancelled), the server
finishes it with the exit code and the last line of output.

* **Cancel** sends SIGTERM to the run's process group, then SIGKILL ten
  seconds later if anything is left; the run reads as *interrupted*. Only
  a run started from the web can be cancelled from it. A run started in a
  shell is stopped there (its page shows the pid), and a phase of a
  web-started build by cancelling the build.
* **A server restart** does not stop anything: the runs are not the
  server's threads. On start the server finds the web-started runs still
  going and watches them again.
* **Run again** on a run's page opens the command's form filled in as it
  ran.

## The files it reads

All under the configuration's `log_dir`, as the shell's `lux` writes them:

| File | What |
|---|---|
| `<prefix>-w<n>.status` | each worker's progress (JSON, rewritten as it goes) |
| `<prefix>-w<n>.log` | each worker's log |
| `<prefix>-w<n>.timing`, `<prefix>.timing` | `PhaseTimer`'s reports (rates, cpu, each stage's share), kept out of the logs; `/api/runs/<run>/timing` |
| `<prefix>-w<n>.distance` | reconcile only: one `dist<TAB>uri##qua<Class>` line per reference dropped for being beyond `max_distance`, for grepping; no file when none was |
| `<prefix>.run` | the run: command line, host, pid, workers, sources, parent build, start, finish, exit |
| `<prefix>.out` | a web-started run's own output |
| `timing-*.json`, `profile-*.prof` | `PhaseTimer`'s timings and profiles (the Timing page) |
| `pipeline_process_status.txt` | `lux build`'s one line per build (the Dashboard) |
| `web_actions.log` | the action log |

The configuration pages read and write the configuration records' own
files (and sub-config stores); the record pages read the caches and maps
the configuration names.

## Safety

* **The token** (above), when the server has one.
* **CSRF**: every request that changes something carries the page's CSRF
  token (a new one each server start), or is refused (403) before
  anything else is looked at.
* **Confirmations** are typed and checked by the server: a record's key
  to delete it, a command's name for a run that deletes or rewrites what
  a build depends on (`clear`, `identify`, `new-token`, `restore`,
  `build --clear`, `pg-maintain` other than `--bloat`, ...), `cancel` to
  cancel a run.
* **While a build runs**, configuration writes are refused (tick "write
  anyway" to override), as are build phases and confirmed commands from
  the Commands pages (a command with `--force` can be forced). Two phases
  started from the web never run at once. One counts run, one bloat
  report and one set of health checks at a time.
* **Paths** in URLs stay inside their roots: a run is named relative to
  `log_dir`, timing directories must be inside the installation's
  directories, and a record key is a name (letters, digits, `_ . -`),
  never a path.
* **Commands** run with their arguments as a list, never through a
  shell. `--my-worker`, `--debug`, `--prefix` and `--env` never come
  from a form.
* **Headers**: `Content-Security-Policy: default-src 'self'` (no inline
  scripts or styles anywhere; data reaches the scripts in
  `<script type="application/json">` blocks), `frame-ancestors 'none'`,
  `X-Content-Type-Options: nosniff`, `Referrer-Policy: same-origin`.
  `/api/docs` (FastAPI's page, whose scripts come from a CDN) is served
  without the content security policy.
* **The action log** records every change and start (`/actions`).

## Limits

* One operator: there are no user accounts, and anyone with the token can
  do everything the operator can.
* Records are shown, never edited: caches and maps change only through
  `lux` commands.
* The configuration is edited in place; committing and pushing the
  configuration checkout stays in the shell.
* It sees runs on this machine: a run recorded by another host shows from
  its files, but cannot be cancelled here.
