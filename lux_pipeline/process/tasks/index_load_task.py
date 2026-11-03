"""`IndexLoadManager`: build each selected source's reconciliation indexes,
one worker per source. The manager behind `lux load-index`; replaces
`manage-data.py --load-index`.

An index build has a single writer per LMDB file, so a source is never
split across workers: worker `n` takes every ``max_workers``-th source,
and ``max_workers`` is capped at the number of sources. For each source
the worker calls ``prepare(0, 1, progress=...)`` then ``process(overwrite)``
on the index loader (item 18); a loader that overrides the legacy
``load()`` with no arguments (viaf, wikidata) is run through that
instead. A source that fails is logged and counted, the worker carries
on, and ``{"sources", "names", "uris", "failed"}`` is its result.
"""

import inspect
import logging

from lux_pipeline.process.base.index_loader import IndexLoader
from ._task_manager import TaskManager

logger = logging.getLogger("lux_pipeline")


def overrides_load(ldr):
    """True when the loader's class replaces the base ``load()``."""
    return getattr(type(ldr), "load", None) is not IndexLoader.load


def takes_no_arguments(fn):
    try:
        params = inspect.signature(fn).parameters.values()
    except (TypeError, ValueError):
        return False
    return all(p.default is not p.empty or p.kind in (p.VAR_POSITIONAL, p.VAR_KEYWORD)
               for p in params)


class IndexLoadManager(TaskManager):
    """Option, read from `args` in ``__init__``: ``overwrite``
    (``--no-overwrite`` clears it; existing index entries are then merged
    over rather than cleared first)."""

    hide_idle_workers = True

    def __init__(self, configs, max_workers=0, args=None):
        super().__init__(configs, max_workers, args)
        self.overwrite = True
        self.results = None
        if args is not None:
            self.overwrite = bool(getattr(args, "overwrite", True))

    # ----------------------------------------------------------- sources

    def maybe_add(self, which, cfg):
        ldr = cfg.get("indexLoader")
        if isinstance(ldr, IndexLoader) and (which, cfg["name"]) not in self.sources:
            self.sources.append((which, cfg["name"]))

    def my_sources(self, n):
        return self.sources[n::self.max_workers]

    def idle_workers(self):
        return range(len(self.sources), self.max_workers)

    # ----------------------------------------------------------- driver

    def process(self, engine="mp", prefix=None, view=None):
        if not self.sources:
            raise RuntimeError("nothing to index: none of the selected sources has an indexLoader")
        if self.max_workers > len(self.sources):
            logger.info(f"{len(self.sources)} source(s) to index: using "
                        f"{len(self.sources)} of {self.max_workers} workers")
            self.max_workers = len(self.sources)
        self.results = super().process(engine, prefix, view)
        return self.results

    def summary(self):
        out = {"sources": 0, "names": 0, "uris": 0, "failed": 0}
        for r in self.results or []:
            if isinstance(r, dict):
                for k in out:
                    out[k] += r.get(k, 0)
        return out

    # ----------------------------------------------------------- workers

    def _build_one(self, ldr, progress):
        """Build one source's indexes; the loader's result dict, or None."""
        # the loader is the only writer of its files whatever worker runs it
        ldr.prepare(0, 1, progress=progress)
        if overrides_load(ldr):
            if not takes_no_arguments(ldr.load):
                raise TypeError(f"{type(ldr).__name__}.load() needs arguments; "
                                "it is not a managed index build")
            return ldr.load()
        return ldr.process(overwrite=self.overwrite)

    def _distributed(self, n, cfgs, progress):
        counts = {"sources": 0, "names": 0, "uris": 0, "failed": 0}
        mine = self.my_sources(n)
        if not mine:
            logger.debug(f"worker {n}: nothing to index")
            return counts
        for which, src in mine:
            ldr = getattr(cfgs, which)[src]["indexLoader"]
            progress(phase="index", description=src)
            try:
                result = self._build_one(ldr, progress)
            except Exception as e:
                logger.error(f"{src}: index build failed in worker {n}: "
                             f"{type(e).__name__}: {e}", exc_info=True)
                counts["failed"] += 1
                continue
            counts["sources"] += 1
            if isinstance(result, dict):
                counts["names"] += result.get("names", 0) or 0
                counts["uris"] += result.get("uris", 0) or 0
            logger.info(f"{src}: indexes built in worker {n}")
        return counts
