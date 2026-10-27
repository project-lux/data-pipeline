"""`ReconcileManager`: reconcile each selected source's records, one slice
per worker, then the references they lead to, and finish the phase in the
driver. The manager behind `lux reconcile`; replaces `run-reconcile.py`
and `reconcile_parallel.sh`, and the three steps `run-all.sh` runs after
them.

Each worker calls `reconcile_slice` (lux_pipeline.process.phases.reconcile)
with its worker number and the worker count, so every record of every
selected datacache is processed by exactly one worker -- the cache's slice
predicate hashes the key, and every worker sees the same source list --
and the references are claimed from the shared queue. The manager never
touches a record. A run over chosen records (``--recid``) or the baseline
terms (``--baseline``) is one worker, unsliced, as the script ran it: the
phase processes an explicit list in full wherever it is given.

The driver tail, unless skipped, is what `run-all.sh` does between
reconcile and merge, in its order:

1. `IdentityResolver.resolve_identity()`, then `idmap.optimize()`
   (`run-identify.py`);
2. `ReferenceManager.merge_metatypes()` (`merge-metatypes.py` and the
   ``mv`` into the data directory);
3. `ReferenceManager.write_done_refs()` and clearing the done-refs queue
   (`manage-data.py --write-refs`).

The same three steps are the commands `lux identify`, `lux metatypes` and
`lux write-refs` (item 22), for a run that skipped them.

It runs only when every worker succeeded, and not at all after a run that
cannot have reconciled everything: ``--no-finish``, ``--my-worker`` (one
slice of many), ``--recid`` or ``--baseline``. Identity resolved from a
fraction of the assertions would rewrite the identity map from a fraction
of the evidence, and delete the YUIDs it no longer saw.
"""

import logging

from lux_pipeline.process.phases.identify import report_lines, resolve_identity, vacuum_idmap
from lux_pipeline.process.phases.reconcile import (new_update_token, reconcile_slice, select_sources,
                                                   write_refs)
from lux_pipeline.process.reference_manager import ReferenceManager
from ._task_manager import TaskManager

logger = logging.getLogger("lux_pipeline")


class ReconcileManager(TaskManager):
    """Options, read from `args` in ``__init__``: ``recid`` (comma-separated
    identifiers, from one source), ``no_refs``, ``new_token``, ``baseline``
    and ``no_finish``."""

    def __init__(self, configs, max_workers=0, args=None):
        super().__init__(configs, max_workers, args)
        self.recids = []
        self.no_refs = False
        self.new_token = False
        self.baseline = False
        self.no_finish = False
        self.results = None
        self.finish_stats = None
        if args is not None:
            recid = getattr(args, "recid", None)
            if recid:
                self.recids = [r.strip() for r in recid.split(",") if r.strip()]
            self.no_refs = bool(getattr(args, "no_refs", False))
            self.new_token = bool(getattr(args, "new_token", False))
            self.baseline = bool(getattr(args, "baseline", False))
            self.no_finish = bool(getattr(args, "no_finish", False))
        if self.unsliced and self.max_workers != 1:
            logger.info(f"one worker for {'--baseline' if self.baseline else '--recid'}, "
                        f"not {self.max_workers}: an explicit record list is not sliced")
            self.max_workers = 1

    @property
    def unsliced(self):
        """True for a run over an explicit record list, which one worker
        does whole: `reconcile_slice` iterates the list wherever it is
        given, so sharing it out would reconcile every record once per
        worker."""
        return bool(self.recids) or self.baseline

    @property
    def hide_idle_workers(self):
        return self.unsliced

    # ----------------------------------------------------------- sources

    def maybe_add(self, which, cfg):
        """An internal or external source; the results group has no
        datacache to reconcile."""
        if which not in ("internal", "external"):
            raise ValueError(f"reconcile applies to internal and external sources, "
                             f"not {cfg['name']}")
        if (which, cfg["name"]) not in self.sources:
            self.sources.append((which, cfg["name"]))

    @property
    def names(self):
        return [name for _, name in self.sources]

    def check(self):
        """What `select_sources` would reject, raised in the driver before
        any worker starts."""
        if self.baseline:
            return
        if not self.sources:
            raise ValueError("No source selected")
        if self.recids and len(self.sources) != 1:
            raise ValueError("Can only build individual records from a single source")

    # ----------------------------------------------------------- workers

    def slice_for(self, n):
        """``(my_slice, max_slice)`` for worker `n`: its share of every
        datacache, or ``(-1, -1)`` -- unsliced, as the script ran them --
        for a record list or the baseline."""
        if self.unsliced:
            return -1, -1
        return n, self.max_workers

    def _distributed(self, n, cfgs, progress):
        """Worker `n`'s slice of every selected source, then its share of
        the references. Returns the phase's ``{"records", "references"}``
        summaries."""
        idmap = cfgs.get_idmap()
        networkmap = cfgs.instantiate_map("networkmap")["store"]
        to_do = select_sources(cfgs, names=self.names, baseline=self.baseline, recids=self.recids)
        my_slice, max_slice = self.slice_for(n)
        return reconcile_slice(cfgs, idmap, networkmap, to_do, my_slice=my_slice, max_slice=max_slice,
                               do_references=not self.no_refs, progress=progress)

    # ----------------------------------------------------------- driver

    def why_not_finish(self):
        """Why the finishing steps are skipped after this run, or ``None``
        when they should run."""
        if self.no_finish:
            return "--no-finish"
        if self.my_slice > -1:
            return f"--my-worker {self.my_slice} is one slice of {self.max_workers}"
        if self.recids:
            return "--recid reconciles chosen records only"
        if self.baseline:
            return "--baseline only pre-populates the global terms"
        return None

    def process(self, engine="mp", prefix=None, view=None):
        """Check the selection, mint the token if asked, run every worker,
        then finish the phase unless this run cannot have reconciled
        everything. Returns the workers' results."""
        self.check()
        if self.new_token:
            new_update_token(self.configs, self.configs.get_idmap())
        self.results = super().process(engine, prefix, view)
        skip = self.why_not_finish()
        if skip:
            logger.info(f"Not finishing the phase ({skip}): identify, metatypes and "
                        f"write-refs are left to run separately")
        else:
            self.finish()
        return self.results

    def finish(self):
        """The driver tail, in `run-all.sh`'s order: resolve identity and
        vacuum the map, merge the metatypes, write the referenced URIs and
        clear the queue. Returns ``{"identify": <resolver stats>,
        "metatypes": <keys>}``, also kept as ``finish_stats``."""
        cfgs = self.configs
        idmap = cfgs.get_idmap()

        logger.info("Resolving the identity map from the assertions")
        stats = resolve_identity(cfgs, idmap)
        for line in report_lines(stats):
            logger.info(line)
        # The map has just been rewritten member by member and nothing else
        # vacuums it; merge is the phase that pays for the litter
        if hasattr(idmap, "optimize"):
            logger.info("Vacuuming the identity map")
        vacuum_idmap(idmap)

        ref_mgr = ReferenceManager(cfgs, idmap)
        logger.info("Merging metatypes")
        metatypes = ref_mgr.merge_metatypes()
        logger.info("Writing the referenced URIs to reference_uris.txt")
        write_refs(cfgs, idmap, ref_mgr)
        logger.info("Done, ready to merge")

        self.finish_stats = {"identify": stats, "metatypes": len(metatypes)}
        return self.finish_stats
