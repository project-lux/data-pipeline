# W06. Server skeleton, `lux serve`, dashboard, health

**Size:** L · **Depends on:** W00, W01, W03 · **Unblocks:** W07–W12
**Read:** `DESIGN.md` §2, §3 (Dashboard), §5; `lux_pipeline/cli/_handler.py`
(`BaseHandler`, `refuse_while_building`); `lux_pipeline/cli/entry.py`
(`NO_CONFIG_COMMANDS`, how `needs_configs` is honoured); `lux_pipeline/runtime.py`;
`lux_pipeline/cli/_services.py` (`postgres_settings`, `describe_postgres`);
`lux_pipeline/cli/_view.py` (`LEVEL_COLORS`, the header: title · prefix ·
clock — the web header mirrors it); `pyproject.toml`; `tests/test_cli_entry.py`
(how a command is tested without an installation)

## Intent

The application every page item plugs into, and the two pages that need
nothing but what W01 and W03 already provide:

```
pip install -e .[web]
lux serve                          # http://127.0.0.1:8321, opens the browser
lux serve --port 9000 --no-open
lux --env staging serve            # another installation
lux serve --host 0.0.0.0 --token $(openssl rand -hex 16)   # non-local needs a token (W13 enforces)
```

Delivers: the package layout, settings, the `AppState` that owns the
server's `Config` (DESIGN §5), the base template and stylesheet, the
navigation, CSRF for POSTs, the JSON API convention, the Dashboard, the
Health page, and the test harness.

## Files

| Action | Path | Notes |
|---|---|---|
| Edit | `pyproject.toml` | `web = ["fastapi", "uvicorn", "jinja2", "python-multipart"]`; `[tool.setuptools.package-data] lux_pipeline = ["web/templates/**/*.html", "web/static/*"]` |
| Create | `lux_pipeline/web/__init__.py` | empty docstring module |
| Create | `lux_pipeline/web/settings.py` | `Settings` dataclass: `host="127.0.0.1"`, `port=8321`, `token=None`, `open_browser=True`, `debug=False`, `basepath` (from `runtime.get_basepath()`), `refresh=0.5`; `from_config(cfgs)` reads the optional `web` block of `base`/`system` for `host`/`port` |
| Create | `lux_pipeline/web/state.py` | `AppState` (below) |
| Create | `lux_pipeline/web/security.py` | CSRF token (random per server start, in a cookie and a hidden field; checked on every POST), `require_token` dependency (W13 wires it; create the function now, always-pass when `settings.token is None` and the bind is loopback) |
| Create | `lux_pipeline/web/app.py` | `create_app(settings) -> FastAPI`; `templates()`; error handlers; `/healthz` |
| Create | `lux_pipeline/web/routers/__init__.py`, `dashboard.py`, `health.py` | pages |
| Create | `lux_pipeline/web/api/__init__.py`, `dashboard.py`, `health.py` | JSON |
| Create | `lux_pipeline/web/templates/base.html`, `dashboard/index.html`, `health/index.html`, `_macros.html`, `error.html` | |
| Create | `lux_pipeline/web/static/lux.css`, `static/app.js`, `static/sse.js`, `static/favicon.svg` | |
| Create | `lux_pipeline/cli/serve.py` | `lux serve` |
| Create | `tests/test_web_app.py`, `tests/test_web_dashboard.py`, `tests/test_cli_serve.py`, `tests/web_client.py` (helper) | |
| Edit | `tests/test_cli_entry.py` | the dependency-agreement test: the `web` extra is optional, so nothing to add to `requirements.txt`; make sure the test still passes |
| Edit | `config_cache/config_cache/base.json` | nothing required (the `web` block is optional). Say so in the PR |
| Edit | `CLAUDE.md` table, `README.md` (one paragraph), `docs/web.md` (create, skeleton; W13 completes) | |

## Design

### `AppState` (`state.py`)

```python
class AppState:
    def __init__(self, settings): self.settings = settings; self._cfgs = None; self._idmap = None; self.lock = threading.RLock(); self.started = time.time()
    def configs(self) -> Config          # runtime.get_configs(instantiate=False), cached here; never instantiate_all
    def source(self, name) -> dict       # group, cfg = _targets.lookup(cfgs, name); cfgs.instantiate(name, group); returns cfg (idempotent)
    def idmap(self)                      # cfgs.get_idmap(); disable_memory_cache() if it has it; cached
    def reset(self)                      # shutdown() on every instantiated cache/map that has it (swallow errors), runtime.reset_configs(), clear caches
    def log_dir(self) -> str | None
    def running(self) -> list[dict]      # progress.list_runs(log_dir) filtered to state == "running"
```

`configs()` may raise (no `.env`, bad config): the app catches that in a
request and renders `error.html` with the message and the `lux initialize`
/ `lux testinstall` hint, status 503, so a broken installation still
serves a page that says what is wrong. Every access to `_cfgs` goes
through `self.lock`; FastAPI runs sync routes in a thread pool, and
`Config` is not thread-safe to build twice.

### `create_app(settings)` (`app.py`)

* `app.state.lux = AppState(settings)`.
* `app.mount("/static", StaticFiles(directory=<package>/web/static))`.
* `Jinja2Templates(directory=<package>/web/templates)` with globals:
  `settings`, `nav` (the list of areas with `href`, `label`, `active`),
  `version` (`importlib.metadata.version("lux-pipeline")`), `fmt_time`,
  `fmt_duration` (reuse `timing_report.fmt_time` / `fmt_duration`),
  `fmt_n` (`f"{n:,}"`), `csrf_token`.
* Routers: every area module has `router = APIRouter()` for pages and
  `api = APIRouter(prefix="/api/<area>")` for JSON; `create_app` includes
  what exists (`dashboard`, `health` now; later items add theirs with one
  line each). Keep the include list in one place: `ROUTERS = [...]`.
* Middleware: CSRF check for POST/PUT/DELETE (form field `_csrf` or header
  `X-CSRF-Token` equals the cookie); `require_token` when configured.
* Exception handlers: `ValueError` → 400 page/JSON; `RuntimeError` from
  `refuse_while_building` → 409 with the message; anything else → 500 with
  the message when `settings.debug`, generic otherwise, logged.
* Content negotiation: a route under `/api` returns JSON; a page route
  renders a template. Both call the same function from the area's `api`
  module (`dashboard_data(state)`), so tests assert on JSON and smoke-test
  the HTML.
* `/healthz` → `{"ok": true, "basepath": ..., "uptime": ...}` with no
  configuration touched (so a monitor can hit it while the config is broken).

### Base template and stylesheet

`base.html`: header bar (`LUX Data Pipeline` · basepath · clock updated by
`app.js` every second · a `running` badge from `/api/dashboard/running`
polled every 5 s by `app.js`), left navigation, `{% block content %}`,
footer with the version. `lux.css`: CSS variables for colours (map
`LEVEL_COLORS` to `--level-debug|info|warning|error|critical`; `--worker`
cyan; a bar colour and track colour), `prefers-color-scheme: dark`
override, a `.card`, `.table` (sticky header, zebra, numeric right-aligned
`.num`), `.badge`, `.bar` (a div with a `--pct` custom property and an
indeterminate animation class), `.log` (monospace, preformatted, bounded
height, scroll), `.form` (label above input, help below, fieldset per
group), `.kv` (definition grid). Keep it under ~400 lines; no CSS
framework. `_macros.html`: `kv(dict)`, `table(rows, cols)`, `bar(completed,
total)`, `badge(state)`, `json_block(obj)` (pretty JSON in `<pre>`).

### Dashboard

`api/dashboard.py`:

```python
def dashboard_data(state) -> dict
# {"installation": {"basepath", "base_dir", "data_dir", "log_dir", "exports_dir", "nt_dir", "indexes_dir", "max_workers",
#                   "postgres": describe_postgres(postgres_settings(cfgs.defaults["cache"])), "config_dir": cfgs.config_dir,
#                   "env_file": os.environ.get("LUX_ENV_FILE")},
#  "sources": {"internal": [names], "external": [names], "results": [names]},
#  "running": [run dicts from state.running()],
#  "recent": list_runs(log_dir)[:10],
#  "last_build": the last 5 lines of <log_dir>/pipeline_process_status.txt, newest last, or [],
#  "health": the cached last health result (state.health_cache) or None}
```

Page: the cards of DESIGN §3, the running card showing one `bar(...)` per
run from `run["progress"]`, quick-action buttons that link to
`/commands/build`, `/runs/<latest prefix>`, `/counts` (the targets exist
after W09–W11; until then the links 404 — acceptable, note it in the PR).

### Health

`api/health.py`: `health_data(state, *, workers=False, pg_tune=False) -> list[dict]`
calls `testinstall.CommandHandler(None).run_checks(args, out=None, stop_on_fail=False, cfgs=state.configs_or_none())`
with an `argparse.Namespace(skip_workers=not workers, workers=0, skip_pg_tune=not pg_tune)`,
caches the result with a timestamp in `state.health_cache`. Page: a row
per check with a coloured badge (OK green, WARN yellow, FAIL red) and the
detail; a *Re-check* form (POST `/health/run`, checkboxes for the worker
spawn and pg-tune checks) that runs the checks in a thread and streams the
rows as they finish over SSE (`/api/health/events`): this is the first
use of `sse.js` and sets the pattern W09 follows. `sse.js` exports
`subscribe(url, handlers)` wrapping `EventSource` with reconnect and a
`stop()`.

### `lux serve` (`cli/serve.py`)

`BaseHandler`, `needs_configs = False` (the app loads lazily, so a broken
config still serves the error page). Options `--host`, `--port`, `--token`,
`--no-open`, `--reload` (uvicorn reload for development, debug only).
`process`: import `uvicorn` and `create_app` inside (ImportError → a
message naming `pip install -e .[web]`); build `Settings` from args over
`Settings.from_config(cfgs)` where the config loads (ignore failure);
refuse `--host` not in (`127.0.0.1`, `localhost`, `::1`) without `--token`
(W13 hardens, but refuse now); open the browser with `webbrowser.open` in
a thread after 1 s unless `--no-open`; `uvicorn.run(app, host, port,
log_level="warning" unless --debug)`. Print the URL.

### Test harness (`tests/web_client.py`)

```python
def make_client(tmp_path, *, seed=True) -> (TestClient, AppState)
# make_installation(tmp_path) (W00); monkeypatch LUX_BASEPATH; Settings(open_browser=False); create_app; TestClient(app)
```

`pytest.importorskip("fastapi")` at the top of every web test module, so
the suite passes where the extra is not installed.

## Known hazards

* `multiprocessing.set_start_method("spawn")` is set by `entry.run` before
  the handler; uvicorn with `--reload` starts a child that re-imports —
  only for development; document it as such.
* Jinja2 autoescape must be on (it is, for `.html`); JSON embedded in a
  `<script>` must go through a `tojson` filter, never `|safe` on a string.
* Sync route functions run in a thread pool; `AppState.lock` around
  anything that touches `_cfgs`. SSE generators must be `async` or use
  `anyio.to_thread` for file reads; keep the per-tick work tiny (W09).
* `StaticFiles` needs the directory to exist in an installed package: the
  `package-data` entry. Check with `pip install .` into a scratch venv
  once (or `python -m build` and inspect the wheel) and say so in the PR.
* Do not import `fastapi` at module level anywhere under `lux_pipeline/cli`
  except inside `serve.process`; `help.list_commands` parses source, it
  does not import, so `serve.py` can import at top — but `entry.resolve_command("serve")`
  imports the module, so keep the imports inside `process`.
* Port 8321 is the agreed default; set it in `Settings` and the help
  text in one place.

## Steps

1. `pyproject.toml`; `pip install -e .[web]`.
2. `settings.py`, `state.py`, `security.py`, `app.py` with `/healthz` and
   the error page; `serve.py`; `tests/web_client.py`; first test: the app
   serves `/healthz` and `/` renders with the fixture installation.
3. `base.html`, `lux.css`, `app.js`, `_macros.html`.
4. Dashboard data + page + tests.
5. Health data + page + SSE + tests.
6. `docs/web.md` skeleton (how to install and start; the rest comes in W13);
   `CLAUDE.md` row; README paragraph.

## Tests

* `GET /healthz` 200 without a configuration (unset `LUX_BASEPATH`, cwd
  without `.env`): `ok true`.
* With no configuration, `GET /` is 503 and the page names
  `lux initialize`.
* With the fixture installation: `GET /api/dashboard` has the installation
  keys, the three source lists, `recent` from `write_fake_run`; `GET /`
  contains the base_dir and a source name; a running fake run (status
  file updated now, `.run` with `os.getpid()`) shows in `running` and the
  page has the `running` badge.
* `POST /health/run` without `_csrf` is 403; with it 200 and
  `GET /api/health` returns rows with `label`, `state`; the `.env` and
  `LUX_BASEPATH` checks are OK on the fixture; `postgres reachable` is FAIL
  (no server) and nothing raised.
* `lux serve --help` lists the options with no `fastapi` import at module
  import (test imports `lux_pipeline.cli.serve` with `fastapi` blocked in
  `sys.modules` and asserts the import succeeds; `process` then raises the
  install hint).
* `Settings.from_config` reads `{"web": {"port": 9000}}`; `--port` wins.

## Acceptance criteria

* `lux serve --no-open` on the laptop (local instance) serves `/`, `/health`
  and `/api/dashboard`; the running badge appears while a worker command
  runs (`lux validate --source aat --max-records 2000 --no-ui`; `counts`
  is a `BaseHandler` and writes no status files, so it does not count).
* `python -m pytest tests/ -q` passes with and without the `web` extra
  installed (skips without).
* Every other `lux` command works with `fastapi` uninstalled.
