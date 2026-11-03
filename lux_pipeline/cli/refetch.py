"""Fetch records again from their source, and store the fresh data.

    lux refetch --source wikidata --max-workers 24      # the records the build used
    lux refetch --source tgn,ulan --all-records         # everything in their datacaches
    lux refetch --source all --max-workers 24           # every external source with a fetcher
    lux refetch --source aat --max-records 100          # try it: 100 per worker

Replaces ``manage-data.py --reload`` and updateExternal.py, which had
stopped re-fetching anything already cached (they set a ``force_rebuild``
that nothing reads any more). By default the identifiers are the ones each
source's recordcache holds -- the records a build actually used -- and
``--all-records`` takes every key of the datacache instead. Each is
fetched from the network (``refetch=True``) and stored in the datacache;
nothing is mapped, so run reconcile and merge afterwards for the new data
to reach the records. The workers turn on the fetchers of the sources they
re-fetch whatever ``allow_network`` says. ``--source`` is required:
re-fetching a source is a lot of requests to someone else's server.

Prints, per source, how many came back, how many were empty (gone or not
found), how many redirected to another identifier (stored under that one),
and how many failed (logged in each worker's log).
"""

import logging

from lux_pipeline.process.tasks.refetch_task import RefetchManager

from ._handler import CommandHandler as CH

logger = logging.getLogger("lux_pipeline")


class CommandHandler(CH):
    def __init__(self, cfgs):
        super().__init__(cfgs)
        self.default_source = None

    def add_args(self, ap):
        ap.add_argument("--all-records", dest="all_records", action="store_true",
                        help="Refetch every record in the datacache, not just the ones the "
                             "recordcache used")
        self.extra_args = {"all_records": "all_records"}

    def resolve_sources(self, args):
        """``--source`` is required; ``all`` is every external source (the
        ones that fetch from someone else's server)."""
        which = args.source
        if not which:
            raise ValueError("lux refetch needs --source: a name, a list, external, or all")
        if which == "all":
            return list(self.configs.external.keys())
        return super().resolve_sources(args)

    def make_manager(self, wks, args):
        return RefetchManager(self.configs, wks, args)

    def process(self, args, rest):
        manager = super().process(args, rest)
        if manager is None:
            return None
        s = manager.summary()
        width = max([len(k) for k in s] + [6])
        # the summary is the command's result
        print(f"{'source':<{width}} {'fetched':>12} {'empty':>10} {'redirected':>11} {'failed':>8}")
        for name, c in s.items():
            print(f"{name:<{width}} {c['fetched']:>12,} {c['empty']:>10,} {c['redirected']:>11,} "
                  f"{c['failed']:>8,}")
        return manager
