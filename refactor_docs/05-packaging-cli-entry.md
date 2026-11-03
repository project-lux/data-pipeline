# 05. Packaging, the `lux` entry point, and the handler base

**Priority:** P0 · **Size:** M · **Depends on:** 03, 04 · **Unblocks:** every `lux <command>` item
**Read from:** `rob_claude_refactor:lux_pipeline/cli/entry.py`, `rob_claude_refactor:lux_pipeline/cli/_handler.py`, `rob_refactor:setup.py`

## Intent

Make the repository installable (`pip install -e .`) with a console script
`lux`, and add the command dispatcher and the base classes every command
inherits from. After this item `lux help` works and `lux <anything>` reports
"unknown command"; real commands arrive in later items.

## Why it matters

The CLI is the user‑facing half of the refactor. Everything else (UI,
engines, managers) hangs off `CommandHandler.process()`. Getting the
dispatcher right first means each later command is a 15‑line file.

## Files

| Action | Path on `main` | Source |
|---|---|---|
| Create | `pyproject.toml` | `rob_refactor:setup.py` for the dependency list |
| Create | `lux_pipeline/cli/__init__.py` (empty) | |
| Create | `lux_pipeline/cli/entry.py` | `rob_claude_refactor:lux_pipeline/cli/entry.py` |
| Create | `lux_pipeline/cli/_handler.py` | `rob_claude_refactor:lux_pipeline/cli/_handler.py` |
| Create | `lux_pipeline/cli/help.py` | new (lists commands by scanning `lux_pipeline/cli/*.py`) |
| Edit | `requirements.txt` — add `rich`; leave `lmdb`, `py-yaml`; **do not** add `lmdbm`, `PyGithub` | |
| Create | `tests/test_cli_entry.py` | new |

## What to port

### `pyproject.toml`

```toml
[project]
name = "lux-pipeline"
version = "0.1.0"
requires-python = ">=3.10"
dependencies = [ ...everything in requirements.txt..., "rich" ]
[project.optional-dependencies]
ray = ["ray"]
dev = ["pytest", "coverage", "fakeredis"]
ui-extras = ["gnureadline", "netext", "networkx"]
[project.scripts]
lux = "lux_pipeline.cli.entry:main"
[tool.setuptools.packages.find]
include = ["lux_pipeline*"]
```

### `entry.py`

Port the structure of `main()`; replace the module‑level bootstrap with
`lux_pipeline.runtime`:

* Global options parsed with `parse_known_args`: `command`, `--log`,
  `--debug`, `--source`, `--max-records`, `--max-workers`, `--my-worker`,
  `--engine {mp,ray,null}`, `--no-ui`, `--prefix` (status/log file prefix;
  default `<log_dir>/<command>-<timestamp>`; shell orchestration passes the
  same prefix to every `--my-worker` process so `lux watch` sees one build). Keep both spellings
  (`--max_workers`/`--max-workers`) as the refactor does.
* `--my-worker N` forces `--engine null` and requires `--max-workers > N`
  (same check and message as the refactor).
* **Default engine is `mp`**, not `ray`.
* Trap `--log` before anything else and call `runtime.configure_logging(level)`.
* Command resolution: `lux_pipeline.cli.<command with '-' → '_'>`; if the name
  contains a dot, import it as an external module (keep the refactor's
  extension mechanism, it is how a deployment adds private commands) and
  require a `CommandHandler` attribute.
* Config: `cfgs = runtime.get_configs()` inside a `try`; on failure, only
  `initialize`, `testinstall` and `help` may continue (with `cfgs=None`);
  otherwise print the error and exit 1 (not 0 — the refactor exits 0 on
  every failure, which breaks shell orchestration).
* `multiprocessing.set_start_method("spawn", force=False)` guarded by
  `try/except RuntimeError` (it raises if already set, e.g. under pytest).
* `hdlr.process(args, rest)` wrapped so that `--debug` re‑raises and the
  normal path exits non‑zero on exception.

### `_handler.py`

Port `BaseHandler` and `CommandHandler` from `rob_claude_refactor` (its
`process()` always builds the sub‑parser, which is the fixed version). Add:

* `BaseHandler.add_args(ap)` and `process(args, rest)` as in the source.
* `CommandHandler.resolve_sources(args)` extracted from the inline code:
  `"all"` → internal + external; `"internal"`/`"external"`/`"results"` →
  that group; else comma‑split. Unknown names raise `ValueError` with the
  name.
* `CommandHandler.process()` sequence: parse extra args → `wks` → sources →
  `manager = self.make_manager(wks, args)` (managers read their own options
  from `args` in `__init__`, the `rob_claude_refactor` pattern) →
  `manager.prepare_single(s)` for each source → `prefix = args.prefix or
  default` → if not `--no-ui`: `view = BuildView(prefix, wks, args.log)`
  (item 06), hide workers beyond `len(manager.sources)` when the manager
  says so → `with view: manager.process(args.engine, prefix, view)` → print
  the prefix so the build can be re‑attached with `lux watch`. Until items
  06/07 land, `process()` only resolves sources and prints them.
* Remove the stray `print(xargs)`.

### `help.py`

New, small: list every module in `lux_pipeline/cli/` that is not `_`‑prefixed,
print its name and the first line of its docstring. Works with `cfgs=None`.

## Known bugs in the source — do not copy

* `entry.py` exits with status 0 on every error path.
* `entry.py` builds `cfgs` at import time and swallows all exceptions.
* `multiprocessing.set_start_method("spawn")` unguarded.
* `cli/load-index.py` has a hyphen in the module name; use
  `load_index.py` and map the dash in `entry.py`.

## Steps

1. Add `pyproject.toml`; `pip install -e .`; `which lux`.
2. Write `entry.py`, `_handler.py`, `help.py`.
3. `tests/test_cli_entry.py`: import `lux_pipeline.cli.entry`, call `main()` via
   `monkeypatch.setattr(sys, "argv", ["lux", "help"])` with
   `runtime.get_configs` patched to raise; assert exit code 0 and the help
   text. Test the `--my-worker` validation and the dash→underscore mapping
   with a fake command module placed on `sys.modules`.
4. Add a docstring to each later command module; `help.py` depends on it.

## Acceptance criteria

* `lux help` lists `help`; `lux nosuch` exits non‑zero with a clear message.
* `lux help --log DEBUG` sets the logger level (verify with a debug line in `help.py`).
* `python -m pytest tests/ -q` passes.
* `requirements.txt` + `pyproject.toml` agree.
