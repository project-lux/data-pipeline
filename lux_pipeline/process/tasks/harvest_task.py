"""`HarvestManager`: walk each selected source's change feed and apply the
changes, spread over the workers. The manager behind `lux harvest`;
replaces `run-harvest.py` and `harvest_parallel.sh`.

Three ways of running, chosen from the options:

* **single** (default): worker `n` takes every ``max_workers``-th source
  and runs its driver's ``process()``, walking the feed and applying
  every change. More workers than sources leaves the rest idle.
* **two-phase** (``--two-phase``, or one half with ``--phase 1|2``):
  phase 1 runs ``get_record_list()`` once per source, one worker each
  (``max_workers`` is lowered to the number of sources for that phase);
  phase 2 runs ``harvest_from_list(n, max_workers)`` in every worker for
  every source, each taking its share of the listed records.
* **pages** (``--pages``): every worker runs ``harvest_pages(n,
  max_workers)`` for every source, fetching every ``max_workers``-th feed
  page into the page cache. Today's ``harvest_parallel.sh --pages``.

``--endpoint URL`` replaces the one selected source's configured feed for
the run (its ActivityStreams collection, or its OAI-PMH endpoint), in
every mode.

A source that fails is logged and counted, the worker carries on with
its next source, and ``{"sources", "records", "failed"}`` is the
worker's result so the command can exit non-zero at the end.
"""

import logging

from ._task_manager import TaskManager

logger = logging.getLogger("lux_pipeline")

MODES = ("single", "two-phase", "pages")


class HarvestManager(TaskManager):
    """Options, read from `args` in ``__init__``: ``two_phase``, ``phase``
    (1 or 2; implies two-phase), ``pages``, ``until`` (ISO 8601: walk the
    feed back to this time), ``overwrite`` (``--no-overwrite`` keeps
    cached records) and ``endpoint`` (the feed URL to walk instead of the
    configured one; needs exactly one source)."""

    def __init__(self, configs, max_workers=0, args=None):
        super().__init__(configs, max_workers, args)
        self.until = None
        self.overwrite = True
        self.endpoint = None
        self.phases = ()        # () single; (1,), (2,) or (1, 2) two-phase
        self.pages = False
        self.phase = 0          # the phase a worker is running
        self.results = None
        if args is not None:
            self.until = getattr(args, "until", None) or None
            self.overwrite = bool(getattr(args, "overwrite", True))
            self.endpoint = getattr(args, "endpoint", None) or None
            self.pages = bool(getattr(args, "pages", False))
            phase = getattr(args, "phase", None)
            if phase is not None:
                if phase not in (1, 2):
                    raise ValueError(f"phase must be 1 or 2, not {phase!r}")
                self.phases = (phase,)
                self.phase = phase      # so run_single() / --my-worker run that phase
            elif getattr(args, "two_phase", False):
                self.phases = (1, 2)
            if self.pages and self.phases:
                raise ValueError("--pages cannot be combined with --two-phase / --phase")

    @property
    def mode(self):
        if self.pages:
            return "pages"
        return "two-phase" if self.phases else "single"

    @property
    def hide_idle_workers(self):
        """Single mode and phase 1 give each worker whole sources, so
        workers beyond the number of sources have nothing to do; in pages
        mode and phase 2 every worker takes a share of every source."""
        return self.mode == "single" or self.phases == (1,) or self.phases == (1, 2)

    # ----------------------------------------------------------- sources

    def maybe_add(self, which, cfg):
        """A source with a harvester and a feed: ``activitystreams`` or
        ``pmhEndpoint``, or ``--endpoint`` giving one."""
        if cfg.get("harvester") is None:
            return
        if not (self.endpoint or cfg.get("activitystreams") or cfg.get("pmhEndpoint")):
            return
        if (which, cfg["name"]) not in self.sources:
            self.sources.append((which, cfg["name"]))

    def idle_workers(self):
        """Workers with nothing to do in single mode / phase 1."""
        return range(len(self.sources), self.max_workers)

    # ----------------------------------------------------------- driver

    def process(self, engine="mp", prefix=None, view=None):
        """Run the selected mode; for two-phase, phase 1 then phase 2 under
        the same engine, prefix and view. Keeps every worker's counts in
        ``results`` (both phases' results for two-phase)."""
        if not self.sources:
            raise RuntimeError("nothing to harvest: none of the selected sources has a "
                               "harvester with an activitystreams or pmhEndpoint feed")
        if self.endpoint and len(self.sources) != 1:
            names = ", ".join(name for _, name in self.sources)
            raise ValueError(f"--endpoint replaces one source's feed; select exactly one "
                             f"--source (got {len(self.sources)}: {names})")
        if not self.phases:
            self.phase = 0
            self.results = super().process(engine, prefix, view)
            return self.results

        results = []
        if 1 in self.phases:
            self.phase = 1
            wanted = self.max_workers
            self.max_workers = min(wanted, len(self.sources))
            if view is not None:
                for n in range(self.max_workers, wanted):
                    view.set_visible(n, False)
            logger.info(f"Phase 1: listing records from {len(self.sources)} source(s) "
                        f"with {self.max_workers} worker(s)")
            try:
                results.extend(super().process(engine, prefix, view))
            finally:
                self.max_workers = wanted
            logger.info("Phase 1 done")
        if 2 in self.phases:
            self.phase = 2
            if view is not None:
                for n in range(len(self.sources), self.max_workers):
                    view.set_visible(n, True)
            logger.info(f"Phase 2: fetching listed records with {self.max_workers} worker(s)")
            results.extend(super().process(engine, prefix, view))
        self.results = results
        return results

    def summary(self):
        """``{"sources", "records", "failed"}`` summed over the workers
        that reported."""
        out = {"sources": 0, "records": 0, "failed": 0}
        for r in self.results or []:
            if isinstance(r, dict):
                for k in out:
                    out[k] += r.get(k, 0)
        return out

    # ----------------------------------------------------------- workers

    def my_sources(self, n):
        """The sources worker `n` acts on: its share in single mode and
        phase 1, every source in pages mode and phase 2."""
        if self.phase == 2 or self.pages:
            return list(self.sources)
        return self.sources[n::self.max_workers]

    def _run_one(self, h, n, idmap, progress):
        """Run one driver for worker `n`; the number of records it
        reported."""
        if self.endpoint:
            h.set_endpoint(self.endpoint)
        h.prepare(n, self.max_workers, progress=progress, until=self.until, idmap=idmap)
        if not self.overwrite:
            h.protocol.overwrite = False
        if self.phase == 1:
            records, deleted = h.get_record_list()
            return len(records)
        if self.phase == 2:
            return h.harvest_from_list(n, self.max_workers)
        if self.pages:
            return h.harvest_pages(n, self.max_workers)
        return h.process()

    def _distributed(self, n, cfgs, progress):
        mine = self.my_sources(n)
        counts = {"sources": 0, "records": 0, "failed": 0}
        if not mine:
            logger.debug(f"worker {n}: nothing to harvest")
            return counts
        # one idmap per worker, shared by its drivers; only process() applies
        # changes through the UpdateManager, the other entry points do not
        # need it (a driver fetches its own lazily if it ever does)
        idmap = cfgs.get_idmap() if self.mode == "single" else None
        label = {1: "list", 2: "fetch"}.get(self.phase, "pages" if self.pages else "harvest")
        for which, src in mine:
            h = getattr(cfgs, which)[src]["harvester"]
            progress(phase=label, description=src)
            try:
                count = self._run_one(h, n, idmap, progress)
            except Exception as e:
                logger.error(f"{src}: harvest ({label}) failed in worker {n}: "
                             f"{type(e).__name__}: {e}", exc_info=True)
                counts["failed"] += 1
                continue
            counts["sources"] += 1
            counts["records"] += count if isinstance(count, int) else 0
            logger.info(f"{src}: {label} done in worker {n}")
        return counts
