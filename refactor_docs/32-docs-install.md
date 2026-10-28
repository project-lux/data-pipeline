# 32. Docs: install, README, rebuild

**Priority:** P2 · **Size:** S · **Depends on:** 31
**Read from:** `rob_refactor:install.md`, `rob_refactor:README.md`; `main:docs/rebuild.md`, `main:README.md`, `main:docs/install-notes.md`

## Intent

Document the new way of running the pipeline.

| Action | Path on `main` |
|---|---|
| Create | `docs/install.md` — from `rob_refactor:install.md`: system packages (Postgres, Redis if still used, libmagic optional), `pip install -e .[dev]`, `lux initialize`, `lux testinstall`, `lux download`, `lux load`. Remove `git checkout rob_refactor` and the `pip install lux-pipeline` placeholder. |
| Edit | `docs/rebuild.md` — replace each `./scripts/manage.py …` / `run-*.py` instruction with the `lux` equivalent from item 31's table; add a section "Choosing an engine" (`--engine mp` default, `null` for nohup‑per‑slice, `ray` with `pip install -e .[ray]`). |
| Edit | `README.md` — add "Running" (five `lux` commands), keep the architecture diagram and component list; add `cli/` and `process/tasks/` to the component list; update the "Fetching external source dump files" section to `lux download`. |
| Edit | `docs/codebase.md` — add short sections for `runtime.py`, `cli/`, `process/tasks/`, `process/phases/`, `process/base/_managable.py`, `loader.py` DSL grammar (copy the grammar block from the loader module docstring). |

## Decisions (2026-10-28, with Rob)

* `docs/install-notes.md` is kept, with a note at the top that
  `docs/install.md` supersedes it.
* `docs/rebuild.md` ends at the export: the MarkLogic headings (pre-load,
  load) are removed. "Move the previous dataset" stays, before the export
  (which empties `exports_dir`).
* The refresh section (`./scripts/refresh.py`, which is not in the
  repository and has no `lux` equivalent) is dropped.
* The rebuild and install docs name `run-all2.sh` (item 31's side-by-side
  script) and say it takes `run-all.sh`'s name once the old one goes.
* `process/base/_managable.py` does not exist on `main`; there is no section
  for it. `docs/codebase.md`'s code-layout list is corrected to the real
  package layout.
* `docs/install.md` puts the installation in `~/lux/data` beside the
  repository, so `run-all2.sh`'s default `../data/logs` is the configured
  `log_dir` with pipeline-configs, and tells the reader to copy the `.env`
  `lux initialize` writes into the repository (the build scripts run
  there; `lux` searches for `.env` upwards from the current directory).

## Acceptance criteria

* A new developer can go from clone to `lux counts` using only `docs/install.md`.
* No reference to `manage-data.py` or `run-*.py` remains in `docs/rebuild.md`.
