"""Process-wide bootstrap for the pipeline configuration.

One place that turns ``LUX_BASEPATH`` (from a ``.env`` file or the
environment) into a ready `Config` and caches it for the rest of the process.
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

from dotenv import find_dotenv, load_dotenv

from lux_pipeline.config import Config

_cfgs = None


def get_basepath():
    """``LUX_BASEPATH``, after loading the nearest ``.env`` (searched from the
    current directory upwards). Empty string when unset."""
    fn = find_dotenv(usecwd=True)
    if fn:
        load_dotenv(fn)
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
            raise RuntimeError("LUX_BASEPATH is not set (no .env found)")
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
