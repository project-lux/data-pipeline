"""What the server keeps between requests: its own configuration, built
on demand, and the results of the last health check.

The configuration is built with ``instantiate=False`` and is the server's
own, not `lux_pipeline.runtime`'s cached one: a health check (`lux
testinstall`'s checks) resets and rebuilds that, and must not take the
server's away. A page that reads a store builds only that source
(`source`, through `records.instantiate_source`), and the idmap is used
with its in-process memory cache off, so a long-running server never
answers from a map that identify has since rewritten.
"""

import logging
import threading
import time

from lux_pipeline import runtime
from lux_pipeline.config import Config
from lux_pipeline.progress import list_runs

logger = logging.getLogger("lux_pipeline")


class ConfigUnavailable(RuntimeError):
    """The installation's configuration cannot be built."""


class AppState:
    def __init__(self, settings):
        self.settings = settings
        self.started = time.time()
        self.lock = threading.RLock()
        self._cfgs = None
        self._idmap = None
        self.health = {"running": False, "results": [], "started": None, "finished": None,
                       "options": {}}
        # the last lookups on the records page, newest first
        self.recent_lookups = []
        # the last `lux counts` the counts page ran
        self.last_counts = None

    # --- configuration ----------------------------------------------------------------

    def configs(self):
        """The installation's `Config`, read (not instantiated) on first use.
        ConfigUnavailable saying why when it cannot be."""
        with self.lock:
            if self._cfgs is None:
                bp = runtime.get_basepath()
                if not bp:
                    raise ConfigUnavailable("LUX_BASEPATH is not set (no ./.env or ~/.lux/.env, "
                                            "and none in the environment)")
                try:
                    self._cfgs = Config(basepath=bp)
                except Exception as e:
                    raise ConfigUnavailable(f"the configuration at {bp} does not build: "
                                            f"{type(e).__name__}: {e}") from e
            return self._cfgs

    def configs_or_none(self):
        try:
            return self.configs()
        except ConfigUnavailable:
            return None

    def source(self, name):
        """Source `name` with its components built (once)."""
        from lux_pipeline.records import instantiate_source
        with self.lock:
            return instantiate_source(self.configs(), name)

    def build_all(self):
        """Every source built (once each, through `source`) and the globals
        looked up: what mapping a record needs, since mappers reach other
        sources. Never `Config.instantiate_all`, which builds everything
        again on every call."""
        cfgs = self.configs()
        with self.lock:
            for group in ("internal", "external", "results"):
                for name in list(getattr(cfgs, group)):
                    self.source(name)
            if not getattr(self, "_globals_cached", False):
                cfgs.cache_globals(self.idmap())
                self._globals_cached = True
        return cfgs

    def idmap(self):
        """The identity map, its memory cache off."""
        with self.lock:
            if self._idmap is None:
                idmap = self.configs().get_idmap()
                if hasattr(idmap, "disable_memory_cache"):
                    idmap.disable_memory_cache()
                self._idmap = idmap
            return self._idmap

    def reset(self):
        """Forget the configuration (after it was edited, or a run ended):
        close what was opened, and build it again on next use."""
        with self.lock:
            cfgs, self._cfgs, self._idmap = self._cfgs, None, None
            self._globals_cached = False
        if cfgs is None:
            return
        for group in ("internal", "external", "results"):
            for cfg in getattr(cfgs, group, {}).values():
                for key in ("datacache", "recordcache", "reconciledRecordcache", "recordcache2"):
                    shutdown = getattr(cfg.get(key), "shutdown", None)
                    if shutdown is not None:
                        try:
                            shutdown()
                        except Exception as e:
                            logger.debug(f"closing {cfg.get('name')} {key}: {e}")

    # --- runs -------------------------------------------------------------------------

    def log_dir(self):
        cfgs = self.configs_or_none()
        return getattr(cfgs, "log_dir", None) if cfgs is not None else None

    def runs(self):
        return list_runs(self.log_dir())

    def running(self):
        return [r for r in self.runs() if r["state"] == "running"]
