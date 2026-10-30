"""`MergeManager`: merge each selected source's records, one slice per
worker, then rebuild the clusters the references pass owns. The manager
behind `lux merge`; replaces `run-merge.py` and `merge_parallel.sh`.

Each worker calls `merge_slice` (lux_pipeline.process.phases.merge) with
its worker number and the worker count, and runs the real `select_sources`
against its own configuration, so every worker sees the same source list.
Both of the phase's loops depend on that: the records loop takes the
records whose key hashes to its slice, and the references loop takes every
``max_workers``-th line of ``reference_uris.txt`` and leaves a cluster to
the records loop when a selected source owns it (`claim_member`). So every
record, and every referenced cluster, is built by exactly one worker. The
manager never touches a record.

A run over chosen records (``--recid``) is one worker, unsliced, as the
script ran it -- the phase processes an explicit list in full wherever it
is given -- and skips the references pass, which would otherwise rebuild
every referenced cluster in the file in that one process.

Before any worker starts, the driver checks what every worker would
otherwise find missing only on its own: the update token, and
``reference_uris.txt`` in the working directory when the references pass
runs (``lux write-refs`` writes it; workers inherit the driver's working
directory). A missing ``<data_dir>/metatypes.json`` is only warned about:
the final mapper carries on without metatypes.

There is no driver tail: `post-build-portal.py` is a separate step.
"""

import logging
import os

from lux_pipeline.process.phases.merge import merge_slice, read_update_token, select_sources
from ._task_manager import TaskManager, flags_pattern, remove_stale

logger = logging.getLogger("lux_pipeline")

REFERENCES_FILE = "reference_uris.txt"


class MergeManager(TaskManager):
    """Options, read from `args` in ``__init__``: ``recid`` (comma-separated
    identifiers, from one source), ``no_refs``, ``only_refs`` and
    ``resume``."""

    def __init__(self, configs, max_workers=0, args=None):
        super().__init__(configs, max_workers, args)
        self.recids = []
        self.no_refs = False
        self.only_refs = False
        self.resume = False
        self.results = None
        if args is not None:
            recid = getattr(args, "recid", None)
            if recid:
                self.recids = [r.strip() for r in recid.split(",") if r.strip()]
            self.no_refs = bool(getattr(args, "no_refs", False))
            self.only_refs = bool(getattr(args, "only_refs", False))
            self.resume = bool(getattr(args, "resume", False))
        if self.recids and self.max_workers != 1:
            logger.info(f"one worker for --recid, not {self.max_workers}: an explicit "
                        f"record list is not sliced")
            self.max_workers = 1

    @property
    def unsliced(self):
        """True for a run over an explicit record list, which one worker
        does whole: `merge_slice` iterates the list wherever it is given,
        so sharing it out would merge every record once per worker."""
        return bool(self.recids)

    @property
    def hide_idle_workers(self):
        return self.unsliced

    @property
    def do_references(self):
        """Whether the references pass runs: not for ``--no-refs``, and not
        for ``--recid``, where it would be the whole file in one process."""
        return not (self.no_refs or self.recids)

    # ----------------------------------------------------------- sources

    def maybe_add(self, which, cfg):
        """An internal or external source; the results group has no
        recordcache to merge."""
        if which not in ("internal", "external"):
            raise ValueError(f"merge applies to internal and external sources, "
                             f"not {cfg['name']}")
        if (which, cfg["name"]) not in self.sources:
            self.sources.append((which, cfg["name"]))

    @property
    def names(self):
        return [name for _, name in self.sources]

    def check(self):
        """What the workers would find wrong, raised in the driver before
        any starts: the selection `select_sources` would reject, an
        unreadable update token, and no ``reference_uris.txt`` for the
        references pass. A missing ``metatypes.json`` is a warning."""
        if self.only_refs:
            if self.recids:
                raise ValueError("--recid and --only-refs together merge nothing")
            if self.no_refs:
                raise ValueError("--no-refs and --only-refs together merge nothing")
        else:
            if not self.sources:
                raise ValueError("No source selected")
            if self.recids and len(self.sources) != 1:
                raise ValueError("Can only build individual records from a single source")
        cfgs = self.configs
        try:
            read_update_token(cfgs)
        except OSError as e:
            raise ValueError(f"No update token ({e}); 'lux new-token' writes one") from e
        if self.do_references and not os.path.exists(REFERENCES_FILE):
            raise ValueError(f"No {REFERENCES_FILE} in {os.getcwd()} for the references pass: "
                             f"run 'lux write-refs' here first (or 'lux reconcile' without "
                             f"--no-finish), or pass --no-refs")
        metatypes = os.path.join(cfgs.data_dir, "metatypes.json")
        if not os.path.exists(metatypes):
            logger.warning(f"No {metatypes}: the merged records will have no metatypes "
                           f"('lux metatypes' writes it after reconcile)")
        if self.recids and not self.no_refs:
            logger.info("Not running the references pass for --recid")

    # ----------------------------------------------------------- workers

    def slice_for(self, n):
        """``(my_slice, max_slice)`` for worker `n`: its share of every
        recordcache and of ``reference_uris.txt``, or ``(-1, -1)`` --
        unsliced, as the script ran it -- for a record list."""
        if self.unsliced:
            return -1, -1
        return n, self.max_workers

    def _distributed(self, n, cfgs, progress):
        """Worker `n`'s slice of every selected source, then its share of
        the references. Returns the phase's ``{"records", "references"}``
        summaries."""
        idmap = cfgs.get_idmap()
        to_do = select_sources(cfgs, names=self.names, only_refs=self.only_refs,
                               recids=self.recids)
        my_slice, max_slice = self.slice_for(n)
        return merge_slice(cfgs, idmap, to_do, my_slice=my_slice, max_slice=max_slice,
                           recids=self.recids, do_references=self.do_references,
                           resume=self.resume, progress=progress)

    # ----------------------------------------------------------- driver

    def clear_stale(self):
        """Before a run over every slice: the phase's done flags
        (run-all.sh's ``rm``). A single slice or ``--recid`` run leaves
        them."""
        if self.my_slice > -1 or self.recids:
            return 0
        return remove_stale(flags_pattern(self.configs, "merge"))

    def process(self, engine="mp", prefix=None, view=None):
        """Check the run, clear what an earlier run left, then run every
        worker. Returns the workers' results."""
        self.check()
        self.clear_stale()
        self.results = super().process(engine, prefix, view)
        return self.results
