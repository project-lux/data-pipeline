# Porting `rob_refactor` onto `main` — the plan

This directory is the complete, self-contained plan for bringing the useful
parts of the `rob_refactor` branch (and its direct continuation
`rob_claude_refactor`) onto `main` as a series of small, independent PRs.

It is written for an agent with limited context. Every change document tells
you exactly which files to read on which branch, what to create or edit on
`main`, what to leave alone, which known bugs not to copy, and how to prove
the change works. You should not need to explore either branch beyond what a
document points you at.

## Read this first

* **`00-INDEX.md`** — the ordered list of changes. Work top to bottom. Each
  entry names its dependencies; never start an item whose dependencies are
  not merged.
* **`PATH-MAP.md`** — where a `rob_refactor` file lives on `main` (the two
  branches use different package names and source layouts).
* **`SUPERSEDED.md`** — the parts of `rob_refactor` that must **not** be
  ported, with the reason for each. Check it before copying anything not
  named in a change document.
* **`CLAUDE-REFACTOR-FIXES.md`** — every fix in the later `rob_claude_refactor`
  branch and which item carries it.
* **`NN-*.md`** — one document per change / PR.

## The two branches in one paragraph

Both branches fork from commit `b7f341c` (2025‑02‑26). Since then `main`
received 404 commits (to 2026‑10‑23) and `rob_refactor` 141 (to 2026‑03‑30);
`rob_claude_refactor` is `rob_refactor` plus 11 later fix commits
(2026‑06‑24) and is strictly newer for the files it touches. `main` kept the
`pipeline/` package and the old `sources/<category>/<name>/` layout, and spent its
commits on the data path: a Postgres identity map, deterministic identity
resolution (`run-identify.py`), batched merge, phase timing, a much larger
Postgres cache, a raw‑`lmdb` index store, and 38 test files. `rob_refactor`
renamed the package to `lux_pipeline/`, flattened `sources/` (both adopted
here as item 00), and spent its
commits on how the pipeline is *driven*: a `lux` CLI, a Rich console UI,
pluggable parallel engines (multiprocessing, Ray, or the current
one‑interpreter‑per‑slice model), a declarative dump‑file loader, a downloader
class, and a reworked `Config`. The two sets of work barely overlap, which is
why the port is tractable.

## Verdict by feature

| Feature the refactor was for | Verdict | Where |
|---|---|---|
| Pluggable parallel engines (ray / multiprocessing / separate interpreters) | **Port the mechanics** (from `rob_claude_refactor`); engines no longer carry UI state | 03, 07, 31 |
| Rich console UI during a build | **Port the panels** (from `rob_claude_refactor`), redesign the plumbing: workers write status + log files, the UI (`BuildView`, `lux watch`) reads them | 02, 06 |
| `lux` CLI that invokes functions and the UI | **Port** | 05, 08–10, 13, 15, 17, 19, 21, 22, 24, 26–28 |
| Dump‑file extraction micro‑DSL (`tar.gz/lines/json`) | **Port** (from `rob_claude_refactor`), keep `load()` for legacy loaders | 12, 29 |
| Built‑in downloader class | **Port** (also repairs a broken import already on `main`) | 14, 15, 30 |
| Configuration management / division | **Port the compatible parts**, keep `main`'s URI cache | 04, 34, 35 |
| `Managable` base (progress/slice protocol for loaders etc.) | **Replace** with a `progress=` callable + `StatusWriter`; `Sliced` mixin keeps the slice part | 02 |
| Logging instead of `print` | **Port** (core first, sources later) | 01, 37 |
| Harvester → protocol/driver split | **Port**, keep `main`'s delete handling | 16, 17 |
| Index loader as a managed task | **Port the shape**, keep `main`'s lmdb + determinism | 18, 19 |
| Reconcile / merge / export managers | **Port the shape only**; the per‑record logic must be `main`'s | 20–26 |
| Package rename `pipeline` → `lux_pipeline` and flattening `sources/<category>/<name>` → `sources/<name>` | **Do, first, as two mechanical PRs** (owner's decision: `pipeline` is too generic, and the category level confuses newcomers) | 00 |
| Storage layer changes (postgres cache, redis idmap, lmdbm index, GithubCache) | **Do not port** | SUPERSEDED |
| Process‑layer logic changes (merger, reidentifier, reference manager, reconciler, acquirer, fetcher, date utils) | **Do not port** | SUPERSEDED |
| Source mappers | **Do not port** (`main` is newer for every one) | SUPERSEDED |

## Ground rules for every PR

1. **One index item per PR.** Title the PR with the item number and name.
   Mark the item done in `00-INDEX.md` in the same PR.
2. **Layout changes first, alone.** Item 00 renames the package to
   `lux_pipeline` and flattens `sources/` in two PRs that change nothing
   else. After them the layout is fixed: one directory per source under
   `lux_pipeline/sources/` (plus `yale/` and `lux/` groups), scripts at the
   repo root until item 33. Never rename or move an existing file as part of
   another item.
3. **No behaviour change to phase logic.** Reconcile, identify, merge and
   export on `main` are heavily tuned and tested. Items 20, 23 and 25 move
   that code into importable functions *unchanged*; the CLI then calls those
   functions. If you find yourself editing a `with timer.stage(...)` block,
   stop.
4. **Legacy entry points keep working** until item 33 retires them:
   `run-*.py`, `manage-data.py` and `*_parallel.sh` must run after every PR.
5. **New config keys are optional** and have fallbacks. The live instance
   config (`base.json`, `caches.json`, path‑based index settings) must load
   unchanged after every PR.
   **Keep the build machine's config copies current.** `config_cache/config_cache/`
   in this checkout (untracked, never committed) is a copy of the build
   machine's live configuration files. Any PR that adds, renames or retires a
   config key or a `*Class` path must apply the same edit to every affected
   file there, check the files still parse as JSON and that every `*Class`
   value resolves through `importObject` without a legacy warning, and say so
   in the PR description. Do not touch the local instance under
   `instance/pipeline/config`; it is rebuilt from these copies once all items
   are in.
6. **Read source code with `git show <branch>:<path>`**, e.g.
   `git show rob_claude_refactor:lux_pipeline/process/tasks/_task_ui_manager.py`.
   Each document says which branch to read; when both have the file, prefer
   `rob_claude_refactor` unless told otherwise. References of the form
   `main:pipeline/...` name files as they are on `main` **before item 00**;
   once the rename has landed, read the same file at `lux_pipeline/...`.
   "Path on `main`" columns always give the post‑rename path.
7. **Do not copy the bugs listed in each document.** They are real; several
   files on `rob_refactor` never ran.
8. **Tests:** `python -m pytest tests/ -q` must pass. Add the tests each
   document asks for. Tests that need Postgres/Redis are not expected; use
   the stub pattern in `tests/test_identity.py` and `tests/test_merge_batch.py`.
9. **Dependencies:** `rich` is required; `ray` is an optional extra
   (`pip install -e .[ray]`); `gnureadline`, `netext`, `networkx` are optional.
10. **Logging:** everything logs to `logging.getLogger("lux_pipeline")`. Never
    add a `print()` to library code (`lux_pipeline/`); `print` is fine in
    `scripts` and in CLI command output that is the command's result.
11. **Progress:** components and phase functions take `progress=null_progress`
    (item 02) and call it; nothing under `lux_pipeline/process` or
    `lux_pipeline/storage` imports `rich`, holds a reference to a UI object, or
    knows which engine it runs under.

## How to use a change document

Each `NN-*.md` has the same sections: Intent, Why it matters, Files (a table
of create/edit/read), What to port, What to keep from `main`, Known bugs in
the source, Steps, Acceptance criteria. Follow Steps in order. The acceptance
criteria are the PR's definition of done.
