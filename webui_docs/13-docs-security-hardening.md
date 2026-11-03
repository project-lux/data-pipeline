# W13. Security, action log, docs

**Size:** M · **Depends on:** W07–W12
**Read:** `lux_pipeline/web/security.py`, `app.py`, `runner.py` (as merged);
`docs/install.md`, `docs/rebuild.md`, `README.md`, `CLAUDE.md`

## Intent

Make the server safe to leave running on the build machine and write the
documentation an operator needs.

## Security

1. **Bind and token.** `lux serve` already refuses a non-loopback `--host`
   without `--token`. Enforce it in the app: when `settings.token` is set,
   every request needs `Authorization: Bearer <token>` or the session
   cookie set by `GET /login?token=...` (a form page too); `/healthz` is
   exempt. The token may come from `LUX_WEB_TOKEN` in the environment or
   `.env` (`runtime` loads `.env`; read `os.environ` after
   `runtime.get_basepath()`). Log one line at start-up saying how the
   server is protected.
2. **CSRF** (W06) on every mutating route; verify by test that no POST
   route is registered without the middleware (walk `app.routes`).
3. **Path containment.** Every path that comes from a URL (run prefixes,
   timing directories for `/timing/compare`, config record keys) is
   checked against its allowed root (`log_dir`, `config_dir` and the
   sub-config stores' directories). Add a `security.under(root, path)`
   helper and a test that `../` and absolute paths are rejected everywhere
   it applies.
4. **Confirmation phrases** are checked server-side (W07 delete, W10
   destructive commands and cancel); test each.
5. **Rate / concurrency limits.** SSE subscriptions capped (W09); the
   runner refuses a second phase run (W10); the health and counts
   re-check buttons refuse while one is already running (a `state.busy`
   set).
6. **Headers.** `Content-Security-Policy: default-src 'self'` (inline
   scripts are not used: every `<script>` is a module file, data passes
   through `<script type="application/json">` blocks read by the module),
   `X-Content-Type-Options: nosniff`, `Referrer-Policy: same-origin`.
   Test that no template contains an inline event handler (`onclick=`)
   or inline `<script>` without `type="application/json"`.
7. **Action log.** `web/actions.py: record(action, **fields)` appends one
   JSON line to `<log_dir>/web_actions.log` for every mutation: config
   write / new / delete / reload, command start / cancel,
   health / counts run, with the remote address, time, and the argv or
   path. Shown at `/actions` (last 200 lines).

## Docs

* `docs/web.md` (completing the sections W06–W12 started): install
  (`pip install -e .[web]`), start (`lux serve`, options, the `web` block
  in `base.json`, `LUX_WEB_TOKEN`), one section per area with a
  screenshot-free description of the page and the `lux` command it
  mirrors, the file protocol it reads (`<prefix>-w<n>.status|.log`,
  `<prefix>.run`, `<prefix>.out`, `timing-*.json`, `web_actions.log`),
  how it runs commands (subprocess recipe, where the files go, how to
  cancel, what happens on a server restart), safety (token, CSRF,
  confirmations, what it refuses while a build runs), and limits (one
  operator, no cache edits, no git push).
* `docs/install.md`: the optional extra and the `web` block.
* `docs/rebuild.md`: "or from the web: Commands → build" beside the shell
  recipe, and `lux watch` ↔ `/runs`.
* `README.md`: a paragraph and the URL.
* `CLAUDE.md`: table rows for `web/`, `serve.py`, `config_store.py`,
  `config_schema.py`, `records.py`, `introspect.py`, and the `.run` file
  beside the status/log convention line.
* `webui_docs/00-INDEX.md`: tick W13; add a "done" note at the top with
  the date and anything left open.

## Tests

* Token: with `Settings(token="x")`, `/` is 401 without and 200 with the
  header; `/healthz` 200 without; the login page sets the cookie.
* Every POST route rejects a missing CSRF token (walk `app.routes`,
  POST each with an empty body, expect 403, never 404/405/500 before the
  check — order the middleware first).
* `security.under`: cases.
* Headers present on `/`.
* The template grep (no inline scripts / handlers).
* `web_actions.log` gets a line for a config write and a command start.

## Acceptance criteria

* `lux serve --host 0.0.0.0 --token t` on the build machine (Rob):
  reachable from a laptop with the token, 401 without.
* `docs/web.md` lets someone who has never seen the UI install, start,
  find a record, start a build and watch it.
* All tests pass; `python -m pytest tests/ -q` runtime did not grow by
  more than a few seconds (the web tests use the filesystem fixture and
  no sleeps beyond the SSE test).

## As built

`web/actions.py` (with `web/api/actions.py`, `web/routers/actions.py`,
`templates/actions/index.html`: the Actions area, last in the
navigation), `templates/login.html`, and changes to `web/security.py`,
`web/app.py`, `web/state.py`, `cli/serve.py` and the counts, maintenance,
health, config and commands routes. Differences from the design above:

* **Already there from earlier items**: the token check (W06, as an
  app-wide dependency), the CSRF check (W06), `security.under` (W11, for
  timing directories), the run-stream cap (W09, `STREAM_LIMIT`), the
  health checks' one-at-a-time (W06), the runner's one-phase rule (W10),
  and the confirmation phrases (W07, W10). W13 adds tests across them.
* **Sign-in**: `GET /login` is a page with a token field (a GET form, so
  it needs no CSRF token before there is a session), and
  `/login?token=...&next=/path` sets the cookie (HTTP only, same site
  strict) and goes on to `next` (a path on this server only). A browser
  asking for any page without the token gets the sign-in page with a 401,
  `next` set to where it was going; `/api/...` gets JSON. `?token=` on any
  page still sets the cookie, as W06 had it. `/healthz`, `/login`,
  `/favicon.ico` and `/static/` need no token (`OPEN_PATHS`,
  `OPEN_PREFIXES`). `serve` reads `./.env` and `~/.lux/.env` before its
  settings, so `LUX_WEB_TOKEN` may be in either.
* **Redirects stay on this server.** W06's `redirect_with` (used with the
  configuration reload form's `back` field) refused `//host` but not
  `/\host`, which browsers also read as another host; it now goes
  through `security.safe_next`, as `/login`'s `next` does.
* **Start-up line**: `create_app` logs at INFO whether a token protects
  the server or it answers on loopback only.
* **Headers**, from the middleware on every response, errors included:
  `Content-Security-Policy: default-src 'self'; img-src 'self' data:;
  frame-ancestors 'none'; base-uri 'self'; form-action 'self'`,
  `X-Content-Type-Options: nosniff`, `Referrer-Policy: same-origin`,
  `X-Frame-Options: DENY`. `/api/docs` (FastAPI's Swagger page loads its
  scripts from a CDN and starts them inline) gets everything but the
  CSP.
* **Path containment**: no new checks were needed. Run names already go
  through `api/runs.full_prefix` (relative to `log_dir`, 404 outside or
  for an absolute path), timing directories through `under`, and record
  keys through `config_store.KEY_RE` (a name, never a path; a new record
  goes in its store's directory). File cache keys have their slashes
  escaped by `FsCache`. The tests try `../`, encoded `..%2F`, and
  absolute paths at each.
* **One at a time**: `AppState.claim(name)` / `release` / `only_one`
  (`state.busy`) for `counts` and `bloat`. A second request is a 409 (a
  page shows the message), or for a stream a `failed` event; the claim
  is taken inside the stream's generator, so a client that goes away
  before the stream starts cannot leave it held.
* **The action log** records `config.write` / `config.new` /
  `config.delete` / `config.reload`, `command.start` (command, argv,
  prefix, pid, forced) / `command.cancel`, `health.run`, `counts.run`,
  `bloat.run`, each with `time` and `remote`; also logged at INFO. A
  write that fails logs a warning and never fails the request.
  `read_actions` reads the file's tail backwards (the last 200 by
  default, `/api/actions?limit=` up to 5000).
* **Tests**: `tests/test_web_security.py` (31): the token and sign-in
  (and `next` refusing other hosts), the `.env` token, every mutating
  route has both dependencies and answers 403 with no token, a wrong one
  as a field or a header, the headers (on pages, API, static files, 404s
  and `/healthz`), no inline script / style / handler / `javascript:` in
  any template, no `innerHTML` / `eval` / `document.write` in any module,
  `under` (with a symbolic link), run names, timing directories and
  record keys, the server-side confirmations, counts / bloat / health one
  at a time, and the action log (lines, page, API, tail of a long file).
  The suite's time grew by about a second.
* Acceptance on the laptop (loopback with a token, so nothing was
  exposed): 401 without the token on pages and the API, 200 with the
  bearer header, `/healthz` 200, `/login` with a wrong token 401, with
  the right one a 303 to `next` and the cookie working, the headers on
  every response, and the start-up line in the server's log.

