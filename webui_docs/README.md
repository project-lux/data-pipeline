# A web UI for the LUX data pipeline — the plan

This directory is the complete, self-contained plan for adding a web user
interface to the pipeline: one that shows the installation (configuration,
sources, caches, records, identities, runs, timing), lets the operator edit
the configuration and have it written back to the files it came from, runs
any `lux` command (one phase, a maintenance step, or the whole build) and
shows its progress live with the same shape as the console view.

It is written for an agent with limited context, in the same form as
`refactor_docs/`: an ordered index, one document per PR, each naming the
files to create or edit, what to reuse, what not to do, the steps, the
tests, and the acceptance criteria. You should not need to explore beyond
what a document points you at, but `CLAUDE.md` at the repository root and
`refactor_docs/README.md` explain the conventions this plan inherits.

## Read this first

* **`DESIGN.md`** — the design: what the UI is, how it is built, how it
  talks to the pipeline, the screens, and the changes to existing code it
  needs. Read it once, all of it, before any item.
* **`00-INDEX.md`** — the ordered list of items. Work top to bottom; never
  start an item whose dependencies are not merged.
* **`NN-*.md`** — one document per item / PR.

## Ground rules for every PR

They are `refactor_docs/README.md`'s rules, which still apply, plus the
ones the web UI adds.

1. **One index item per PR**, titled `WNN: <item name>` (the `W` keeps the
   numbers apart from `refactor_docs/`). Tick the box in `00-INDEX.md` in
   the same PR. Branch from `main` as `wNN-<slug>`.
2. **The process layer stays ignorant of the web.** Nothing under
   `lux_pipeline/process`, `lux_pipeline/storage` or `lux_pipeline/sources`
   imports anything from `lux_pipeline/web`, Rich, FastAPI or Jinja2.
   Components keep taking `progress=null_progress`. Workers keep writing
   status and log files; the web UI reads those files, as `lux watch` does.
3. **The web server never runs a phase in its own process.** Commands run
   as `python -m lux_pipeline.cli.entry <command> ... --no-ui --prefix P`
   subprocesses, exactly as `lux build` runs its phases
   (`lux_pipeline/cli/build.py`, `CommandHandler.lux`). The server must
   survive a worker crash, a `SIGKILL`ed build, and its own restart.
4. **No behaviour change to phase logic**, ever. If an item needs data the
   phases do not provide, it gets it from the files they write, or adds an
   optional argument with a default that leaves every existing caller
   unchanged.
5. **Enabling changes first, as their own PRs** (items W01–W05). They are
   small refactors of existing modules that make the same data available
   to the console and the web: a `*_data` function the printing code
   calls, a reader shared by `BuildView` and the web page. Each one must
   leave `lux` behaving exactly as before and is tested on its own.
6. **Everything the web shows has a JSON endpoint under `/api/`** and the
   page is rendered from the same function. Tests hit the API with
   FastAPI's `TestClient`; page tests check the HTML contains what the API
   returned. No test needs Postgres, Redis or a browser.
7. **No front-end build step and no vendored third-party JavaScript.**
   Pages are Jinja2 templates served by the Python package; live regions
   use small hand-written JavaScript modules under `lux_pipeline/web/static`
   and Server-Sent Events. One stylesheet, with CSS variables, light and
   dark. If an item is tempted to add a framework, stop and re-read
   `DESIGN.md` § "Why no front-end framework".
8. **Optional dependency.** FastAPI, uvicorn and Jinja2 are the `web`
   extra (`pip install -e .[web]`), imported inside `lux serve` and the
   `lux_pipeline/web` package only. `lux help` and every other command
   must work without them (`tests/test_cli_entry.py` checks that
   `requirements.txt` and `pyproject.toml` agree; the extra goes in
   `pyproject.toml` only).
9. **Mutations are POSTs with a CSRF token and, for anything destructive,
   a typed confirmation**; reads are GETs. The server binds to
   `127.0.0.1` unless told otherwise, and a non-local bind requires a
   token. Item W13 hardens this; every earlier item follows the pattern
   W06 sets.
10. **Logging:** the server logs through `logging.getLogger("lux_pipeline")`
    like everything else. Request logging is uvicorn's and stays at
    WARNING unless `--debug`.
11. **Config copies.** An item that adds a configuration key (W01 adds
    none; W04's `config_schema.py` describes keys, it adds none; W06 adds
    the optional `web` block to `base`/`system`) applies the same edit to
    `config_cache/config_cache/` and says so in the PR, per `CLAUDE.md`.
12. **Tests:** `python -m pytest tests/ -q` must pass. New tests go in
    `tests/test_web_*.py` and `tests/test_<module>.py`. The database-free
    installation fixture of W01 (`tests/webui_fixtures.py`) is what every
    later test builds on.

## How to use an item document

Each `NN-*.md` has the same sections: Intent, Why it matters, Files, What
to reuse, Design (the functions, routes and templates with their
signatures), Known hazards (do not trip over them), Steps, Tests,
Acceptance criteria. Follow Steps in order. The acceptance criteria are
the PR's definition of done.
