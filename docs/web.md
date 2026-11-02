# The web UI (`lux serve`)

A browser view of an installation: its configuration, its records, its
runs, and the `lux` commands, for one operator on the build machine (or a
laptop with a local instance). It reads what the console reads -- the
workers' status and log files, the configuration files -- through the
same functions the `lux` commands print from. The plan is in
`webui_docs/`; this page grows with it (item W13 completes it).

Built so far: the server, the **Dashboard**, **Runs**, **Records**,
**Counts**, **Timing**, **Configuration**, **Maintenance** and **Health**
pages.

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

A bind other than `127.0.0.1` / `localhost` / `::1` needs a token:

```
lux serve --host 0.0.0.0 --token "$(openssl rand -hex 16)"      # or LUX_WEB_TOKEN in .env
```

Open any page once as `http://host:8321/?token=<token>`; the browser
keeps it as a cookie. Other clients send `Authorization: Bearer <token>`.
Prefer an SSH tunnel (`ssh -L 8321:127.0.0.1:8321 buildhost`) to an open
bind where you can.

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
  stopping when it ends. The log can be filtered by level and worker,
  stop following, jump to the first error, and read a worker's earlier
  lines. A `lux build` shows its phases, each linked to its own run, and
  each phase links back to its build. A run started from a shell slice by
  slice shows too. `/runs/_latest` opens the newest. *Run again* opens the
  command's form filled in as the run was. A run started from the web can
  be cancelled from its page by typing `cancel`: its process and its
  workers are sent SIGTERM (SIGKILL ten seconds later if still there), and
  the run reads as interrupted. A run started in a shell cannot be: stop
  it there (its pid is shown); a phase of a web-started build is stopped
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
  it exists, and whether a class imports; saving changes only the keys
  you changed, in the file's own layout. *JSON*: the file's text, checked
  as you type, with the diff of what saving would change; saved exactly
  as you leave it. Both write exactly what `lux config` writes, refuse an
  invalid record (keeping what you typed), and refuse while a build runs
  unless you tick "write anyway". New records start from a template; a
  record is deleted by typing its key. *Reload from the files* re-reads
  the configuration after editing it elsewhere. Committing the
  configuration checkout stays in the shell.
* **Health** (`/health`): `lux testinstall`'s checks, run on request in
  the background, each row arriving as its check finishes. Unlike the
  command, every check runs even after one fails; one that needs what
  failed says so. Spawning worker processes and comparing Postgres's
  settings with `lux pg-tune` are off unless ticked.

Everything a page shows is also JSON under `/api/` (`/api/dashboard`,
`/api/dashboard/running`, `/api/runs`, `/api/runs/<run>` (and its `log`,
`events`), `/api/records/journey?q=`, `/api/records/sample?source=`,
`/api/counts`, `/api/timing`, `/api/maintenance/tune`,
`/api/maintenance/bloat`,
`/api/config`, `/api/config/<key>`, `/api/commands`, `/api/commands/<name>`
(a command's options), `POST /api/commands/<name>/preview` (a form, or
JSON `{"values": {option: value}}`, to its command line),
`/api/health`); `/api/docs` lists the routes.

## Safety

Every request that changes something (a POST) carries the page's CSRF
token, or is refused. The server never runs a pipeline phase in its own
process: a command runs as `python -m lux_pipeline.cli.entry <command>
... --no-ui --prefix <log_dir>/<command>-<timestamp>`, its arguments a
list (never a shell string). `--my-worker`, `--debug`, `--prefix` and
`--env` never come from a form. Whatever a form sends, the server checks
it against the command's own parser and the confirmation itself.
