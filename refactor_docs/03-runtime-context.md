# 03. Runtime context: `lux_pipeline/runtime.py`

**Priority:** P0 · **Size:** S · **Depends on:** 00 · **Unblocks:** 05, 07
**Read from:** `rob_refactor:lux_pipeline/cli/entry.py` lines 1–45 (the module‑level bootstrap), and the top of any `main` `run-*.py`

## Intent

Create one place that turns `LUX_BASEPATH` (from `.env` or the environment)
into a ready `Config` — idmap instantiated, globals cached, all sources
instantiated — and caches it per process. Both the CLI entry point (item 05)
and worker processes started by the engines (item 07) call it.

## Why it matters

`rob_refactor` builds the config at import time in `cli/entry.py` and then
has the task managers `from lux_pipeline.cli.entry import cfgs` to get it
back inside workers. That makes the process layer import the CLI, and it
means importing the CLI module has side effects (connects to Postgres). Every
`run-*.py` on `main` repeats the same eight bootstrap lines. One function
removes both problems.

## Files

| Action | Path on `main` |
|---|---|
| Create | `lux_pipeline/runtime.py` |
| Create | `tests/test_runtime.py` |

## What to write

```python
"""Process-wide bootstrap for the pipeline configuration."""
import logging, os, sys
from dotenv import find_dotenv, load_dotenv
from lux_pipeline.config import Config

_cfgs = None

def get_basepath():
    fn = find_dotenv(usecwd=True)
    if fn:
        load_dotenv(fn)
    return os.getenv("LUX_BASEPATH", "")

def get_configs(instantiate=True, basepath=None):
    """Return the process's Config, building it on first call.
    instantiate=True also calls get_idmap(), cache_globals(), instantiate_all()
    exactly as run-*.py do today. Raises if LUX_BASEPATH is unset."""
    global _cfgs
    if _cfgs is None:
        bp = basepath or get_basepath()
        if not bp:
            raise RuntimeError("LUX_BASEPATH is not set (no .env found)")
        cfgs = Config(basepath=bp)
        if instantiate:
            cfgs.get_idmap(); cfgs.cache_globals(); cfgs.instantiate_all()
        _cfgs = cfgs
    return _cfgs

def reset_configs():        # for tests and for forked/spawned workers that want a fresh build
    global _cfgs; _cfgs = None

def configure_logging(level="INFO", stream=sys.stdout):
    """Attach one StreamHandler to the 'lux_pipeline' logger at `level`,
    replacing any existing handlers. Returns the handler."""
```

Notes:

* `get_configs()` must be safe to call in a spawned worker: the module global
  starts as `None` in the new process, so the first call rebuilds the config
  there. That is the mechanism item 07 relies on.
* Keep the `try/except` leniency out of here. `rob_refactor`'s entry swallows
  every exception and leaves `cfgs = None`; that hides misconfiguration.
  Let it raise; item 05 catches it only for `initialize`/`testinstall`.
* `configure_logging` is the only place that touches handlers for the
  non‑UI case. Item 06 replaces the handler when the UI is on.

## Steps

1. Write the module.
2. `tests/test_runtime.py`: monkeypatch `lux_pipeline.runtime.Config` with a stub
   class recording the basepath; assert caching, `reset_configs`, and the
   error when `LUX_BASEPATH` is empty. Assert `configure_logging` leaves
   exactly one handler at the requested level.
3. Optionally replace the bootstrap block in **one** script
   (`run-identify.py` is the smallest) with
   `cfgs = get_configs(instantiate=False); idmap = cfgs.get_idmap(); cfgs.cache_globals()`
   to prove it; leave the others for items 20/23/25.

## Acceptance criteria

* `python -c "from lux_pipeline.runtime import get_configs; print(get_configs().data_dir)"` works with the live `.env`.
* Tests pass.
