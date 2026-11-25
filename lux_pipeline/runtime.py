"""Process-wide bootstrap for the pipeline configuration.

One place that turns ``LUX_BASEPATH`` (from the environment, ``./.env`` or
``~/.lux/.env``) into a ready `Config` and caches it for the rest of the process.
The CLI entry point and the worker processes the task engines start both call
`get_configs`; nothing imports the CLI to get at the configuration, and
importing this module has no side effects.

A spawned worker starts with the module global at ``None``, so its first
`get_configs` call rebuilds the configuration in that process. That is the
mechanism the engines rely on.

Misconfiguration is allowed to raise. Swallowing exceptions here and leaving
the configuration as ``None`` only moves the failure somewhere less obvious.
"""

import logging
import os
import sys
from pathlib import Path

from dotenv import dotenv_values

from lux_pipeline.config import Config

logger = logging.getLogger("lux_pipeline")

_cfgs = None


def dotenv_paths():
    """Where ``lux`` looks for a ``.env``, in order of precedence: the
    current directory, then ``~/.lux/.env``."""
    return [Path.cwd() / ".env", Path.home() / ".lux" / ".env"]


def find_dotenvs():
    """The `dotenv_paths` that exist, as strings, in order."""
    return [str(p) for p in dotenv_paths() if p.is_file()]


# A .env sets only these. A .env is read from the current directory, and
# every variable it set reached every worker and child process: PYTHONPATH
# or LD_PRELOAD in a .env another user left in a directory the operator
# ran `lux` from ran their code as the operator
ENV_PREFIX = "LUX_"
_ignored_warned = set()


def dotenv_problem(path):
    """Why the ``.env`` at `path` must not be read, or None: one another
    user owns or can write could set ``LUX_BASEPATH``, and with it every
    class the configuration names."""
    st = os.stat(path)
    if hasattr(os, "getuid") and st.st_uid != os.getuid():
        return f"{path} belongs to another user"
    if st.st_mode & 0o022:
        return f"{path} can be written by other users (chmod go-w {path})"
    return None


def read_dotenv(path):
    """The ``LUX_`` variables of the ``.env`` at `path` (`ENV_PREFIX`); any
    other is ignored, with a warning once per file."""
    values = dotenv_values(path)
    ignored = sorted(k for k in values if not k.startswith(ENV_PREFIX))
    if ignored and str(path) not in _ignored_warned:
        _ignored_warned.add(str(path))
        logger.warning(f"{path}: only {ENV_PREFIX} variables are read from a .env; ignored "
                       f"{', '.join(ignored)}")
    return {k: v for k, v in values.items() if k.startswith(ENV_PREFIX) and v is not None}


def resolve_env(value):
    """The ``.env`` file ``--env value`` names. A bare name (no ``/``) is
    ``~/.lux/<name>.env`` and nothing else, so a file or directory of that
    name in the current directory is never read in its place; a path (a
    ``/`` in it, as ``./prod`` or ``~/lux/data``) is that file, or that
    directory's ``.env``. Raises ``FileNotFoundError`` saying what was
    tried."""
    if "/" not in value and os.sep not in value and value not in (".", ".."):
        named = Path.home() / ".lux" / f"{value}.env"
        if named.is_file():
            return named
        raise FileNotFoundError(f"no .env for --env {value} (tried {named}; for a file or "
                                f"directory here, write ./{value})")
    p = Path(value).expanduser()
    if p.is_file():
        return p
    if p.is_dir() and (p / ".env").is_file():
        return p / ".env"
    tried = p / ".env" if p.is_dir() else p
    raise FileNotFoundError(f"no .env for --env {value} (tried {tried})")


def use_env(value):
    """``--env``: load the file `resolve_env` finds over the environment
    (it wins over the shell's variables and over ``./.env`` and
    ``~/.lux/.env``, which `get_basepath` then only fills in around it), and
    record it as ``LUX_ENV_FILE``. Spawned workers and subprocesses inherit
    the result. Only its ``LUX_`` variables are read (`read_dotenv`).
    Raises ``ValueError`` when the file sets no ``LUX_BASEPATH``, or
    belongs to or can be written by another user (`dotenv_problem`).
    Returns the file."""
    fn = resolve_env(value)
    problem = dotenv_problem(fn)
    if problem:
        raise ValueError(f"not reading --env {value}: {problem}")
    values = read_dotenv(fn)
    if not values.get("LUX_BASEPATH"):
        raise ValueError(f"{fn} does not set LUX_BASEPATH")
    os.environ.update(values)
    os.environ["LUX_ENV_FILE"] = str(fn)
    reset_configs()
    return fn


def get_basepath():
    """``LUX_BASEPATH``, after loading ``./.env`` and ``~/.lux/.env``. A
    variable already in the environment wins, then the current directory's
    file, then the one in ``~/.lux``. Empty string when unset. Only ``LUX_``
    variables are read (`read_dotenv`), and a file another user owns or
    can write is skipped with a warning (`dotenv_problem`)."""
    for fn in find_dotenvs():
        problem = dotenv_problem(fn)
        if problem:
            if fn not in _ignored_warned:
                _ignored_warned.add(fn)
                logger.warning(f"not reading {problem}")
            continue
        # never overrides: the first to set a variable keeps it
        for k, v in read_dotenv(fn).items():
            os.environ.setdefault(k, v)
    return os.getenv("LUX_BASEPATH", "")


def get_configs(instantiate=True, basepath=None):
    """The process's `Config`, built on first call and cached after.

    ``instantiate=True`` also calls ``get_idmap()``, ``cache_globals()`` and
    ``instantiate_all()``, exactly as the ``run-*.py`` scripts do. Pass
    ``instantiate=False`` for a script that only needs some of those and
    wants to do them itself. Both arguments matter only on the call that
    builds the configuration.

    Raises ``RuntimeError`` when ``LUX_BASEPATH`` is unset and no
    ``basepath`` is given.
    """
    global _cfgs
    if _cfgs is None:
        bp = basepath or get_basepath()
        if not bp:
            raise RuntimeError("LUX_BASEPATH is not set (no ./.env or ~/.lux/.env found)")
        cfgs = Config(basepath=bp)
        if instantiate:
            cfgs.get_idmap()
            cfgs.cache_globals()
            cfgs.instantiate_all()
        _cfgs = cfgs
    return _cfgs


def reset_configs():
    """Forget the cached configuration, so the next `get_configs` rebuilds it.

    For tests, and for forked workers that want their own connections rather
    than the parent's."""
    global _cfgs
    _cfgs = None


def configure_logging(level="INFO", stream=None):
    """Attach one ``StreamHandler`` to the ``lux_pipeline`` logger at `level`,
    replacing any handlers already there. Returns the handler.

    `stream` defaults to whatever ``sys.stdout`` is *when called*, so a
    redirected stdout (a test capturing output, a shell wrapper) is honoured.
    This is the only place that touches handlers for the non-UI case; the
    console UI installs its own handler instead when it is on.
    """
    if stream is None:
        stream = sys.stdout
    if isinstance(level, str):
        lvl = getattr(logging, level.upper(), None)
        if not isinstance(lvl, int):
            raise ValueError(f"unknown log level: {level}")
    else:
        lvl = level
    logger = logging.getLogger("lux_pipeline")
    for h in list(logger.handlers):
        logger.removeHandler(h)
    handler = logging.StreamHandler(stream=stream)
    handler.setLevel(lvl)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    logger.setLevel(lvl)
    return handler
