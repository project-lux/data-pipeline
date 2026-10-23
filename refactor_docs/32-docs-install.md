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

## Acceptance criteria

* A new developer can go from clone to `lux counts` using only `docs/install.md`.
* No reference to `manage-data.py` or `run-*.py` remains in `docs/rebuild.md`.
