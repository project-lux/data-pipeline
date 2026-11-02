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
