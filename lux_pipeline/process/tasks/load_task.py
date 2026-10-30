"""`LoadManager`: load each selected source's dump into its datacache,
one slice per worker. The manager behind `lux load`; replaces
`manage-data.py --load` and `import_parallel.sh`.

Which loader entry point a worker calls depends on the loader (decided
per source in `_distributed`):

* ``--type export``: ``load_export2()`` on worker 0 (main's export loader
  is not sliced) unless the config's ``input_files`` describes the export
  file, in which case the DSL loads it like anything else;
* a loader whose config describes its files (``uses_dsl``): ``process()``,
  every worker taking its slice;
* a legacy ``load(my_slice, max_slice)``: called with this worker's slice;
* a legacy ``load()``: worker 0 only, the other workers idle.

``--clear`` empties each source's datacache in the driver before any worker
starts, which is what `manage-data.py --load` did for the simple sources.

``--file`` (and ``--file-type``) replace the one selected source's
configured file for the run; `Loader.override_file` says how each kind of
loader takes it.
"""

import inspect
import logging

from lux_pipeline.process.base.loader import Loader
from ._task_manager import TaskManager

logger = logging.getLogger("lux_pipeline")

LOAD_TYPES = ("records", "export", "lmdb")


def accepts_slices(fn):
    """True when ``fn`` (a bound ``load``) takes a slice and a slice count
    positionally, like ``load(self, slicen=None, maxSlice=None)``."""
    try:
        params = inspect.signature(fn).parameters.values()
    except (TypeError, ValueError):
        return False
    positional = [p for p in params
                  if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)]
    return len(positional) >= 2


class LoadManager(TaskManager):
    """Options, read from `args` in ``__init__``: ``type`` (one of
    `LOAD_TYPES`), ``overwrite`` (``--no-overwrite`` clears it),
    ``clear``, ``file`` / ``file_type`` (a file to load instead of the
    configured one; one source only) and the common ``max_records``."""

    def __init__(self, configs, max_workers=0, args=None):
        super().__init__(configs, max_workers, args)
        self.load_type = "records"
        self.overwrite = True
        self.clear = False
        self.file = None
        self.file_type = None
        if args is not None:
            lt = getattr(args, "type", None)
            if lt is not None:
                if lt not in LOAD_TYPES:
                    raise ValueError(f"Unknown load type {lt!r}; "
                                     f"expected one of {', '.join(LOAD_TYPES)}")
                self.load_type = lt
            self.overwrite = bool(getattr(args, "overwrite", True))
            self.clear = bool(getattr(args, "clear", False))
            self.file = getattr(args, "file", None) or None
            self.file_type = getattr(args, "file_type", None) or None
            if self.file_type and not self.file:
                raise ValueError("--file-type needs --file")

    # ----------------------------------------------------------- sources

    def maybe_add(self, which, cfg):
        ldr = cfg.get("loader")
        if isinstance(ldr, Loader) and (which, cfg["name"]) not in self.sources:
            self.sources.append((which, cfg["name"]))

    # ----------------------------------------------------------- driver

    def clear_datacaches(self):
        """Empty the selected sources' datacaches. Driver only: called from
        `process` before the workers start, never from a worker."""
        cleared = []
        for which, src in self.sources:
            cache = getattr(self.configs, which)[src].get("datacache")
            if cache is None or not hasattr(cache, "clear"):
                logger.warning(f"{src} has no datacache to clear")
                continue
            logger.info(f"Clearing {src} datacache")
            cache.clear()
            cleared.append(src)
        return cleared

    def process(self, engine="mp", prefix=None, view=None):
        if self.file and len(self.sources) != 1:
            names = ", ".join(name for _, name in self.sources)
            raise ValueError(f"--file replaces one source's file; select exactly one --source "
                             f"(got {len(self.sources)}: {names})")
        if self.clear:
            self.clear_datacaches()
        return super().process(engine, prefix, view)

    # ----------------------------------------------------------- workers

    def _load_one(self, n, ldr, progress):
        """Run loader `ldr` for worker `n`; the name of the entry point
        used, or ``None`` when this worker had nothing to do."""
        ldr.prepare(n, self.max_workers, self.load_type, self.max_records,
                    progress=progress, path=self.file, path_type=self.file_type)
        if self.load_type == "export" and not (ldr.uses_dsl and ldr.my_files):
            if n != 0:
                return None
            ldr.load_export2()
            return "load_export2"
        if ldr.uses_dsl:
            ldr.process(overwrite=self.overwrite)
            return "process"
        if accepts_slices(ldr.load):
            ldr.load(n, self.max_workers)
            return "load(slice)"
        if n != 0:
            return None
        ldr.load()
        return "load"

    def _distributed(self, n, cfgs, progress):
        """Load every selected source for slice `n`. A source that fails is
        logged and the rest are still loaded; the failures are raised
        together at the end so the worker is seen to fail. Returns the
        names of the sources this worker loaded."""
        done = []
        failed = []
        for which, src in self.sources:
            ldr = getattr(cfgs, which)[src]["loader"]
            progress(phase="load", description=src)
            try:
                how = self._load_one(n, ldr, progress)
            except Exception as e:
                logger.critical(f"Failed to load {src} in slice {n}: "
                                f"{type(e).__name__}: {e}", exc_info=True)
                failed.append((src, e))
                continue
            if how is None:
                logger.debug(f"{src}: nothing for worker {n} to do")
            else:
                logger.info(f"Loaded {src} in slice {n} via {how}")
                done.append(src)
        if failed:
            names = ", ".join(src for src, _ in failed)
            raise RuntimeError(f"load failed for {names} in slice {n}") from failed[0][1]
        return done
