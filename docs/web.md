# The web UI (`lux serve`)

A browser view of an installation: its configuration, its records, its
runs, and the `lux` commands, for one operator on the build machine (or a
laptop with a local instance). It reads what the console reads -- the
workers' status and log files, the configuration files -- through the
same functions the `lux` commands print from. The plan is in
`webui_docs/`; this page grows with it (item W13 completes it).

Built so far: the server, the **Dashboard** and the **Health** page.

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
* **Health** (`/health`): `lux testinstall`'s checks, run on request in
  the background, each row arriving as its check finishes. Unlike the
  command, every check runs even after one fails; one that needs what
  failed says so. Spawning worker processes and comparing Postgres's
  settings with `lux pg-tune` are off unless ticked.

Everything a page shows is also JSON under `/api/` (`/api/dashboard`,
`/api/dashboard/running`, `/api/health`); `/api/docs` lists the routes.

## Safety

Every request that changes something (a POST) carries the page's CSRF
token, or is refused. The server never runs a pipeline phase in its own
process.
