"""Suggest Postgres settings for this pipeline, sized to this machine and build.

    lux pg-tune                    # inspect the machine and the server, print what to change
    lux pg-tune --workers 32       # size for 32 workers (default: max_workers)
    lux pg-tune --measure-io       # add an fsync / sequential-write probe
    lux pg-tune --all              # print the settings already correct too
    lux pg-tune --no-db            # the machine only, without connecting

Changes nothing: prints ALTER SYSTEM statements (and the REINDEX, prewarm
and table storage statements the pipeline wants) for a superuser to review
and run. Reads the machine's cpus, memory and storage, and the server the
configuration's cache settings point at (its version, current settings,
the identity map's size), so the recommendations are for the database the
pipeline actually uses. Formerly pg-tune.py; ``lux testinstall`` ends with
the same check, summarised. See `lux_pipeline.storage.pg_tune` for why each
setting is what it is.

The configuration is read, not instantiated, and only for the database
settings, ``max_workers`` and the idmap's ``tableName``; without one, the
machine alone is sized.
"""

import sys

from lux_pipeline import runtime
from lux_pipeline.storage import pg_tune

from ._handler import BaseHandler

DEFAULT_WORKERS = 24


def pipeline_settings(cfgs):
    """``(cache settings, max_workers, idmap table)`` from a configuration."""
    idmap = (getattr(cfgs, "map_stores", None) or {}).get("idmap") or {}
    return ((getattr(cfgs, "caches", None) or {}),
            getattr(cfgs, "max_workers", None) or DEFAULT_WORKERS,
            idmap.get("tableName") or "idmap")


class CommandHandler(BaseHandler):
    # never opens the idmap or builds a source; works without a configuration
    needs_configs = False

    def add_args(self, ap):
        ap.add_argument("--workers", type=int, default=None,
                        help="Worker processes a build will use (default: max_workers, else "
                             f"{DEFAULT_WORKERS}); size BEFORE a run with more")
        ap.add_argument("--measure-io", dest="measure_io", action="store_true",
                        help="Probe fsync latency and sequential write throughput")
        ap.add_argument("--io-path", dest="io_path", default=None,
                        help="Where to probe (default: the data directory, else the cwd)")
        ap.add_argument("--all", action="store_true",
                        help="Print the settings already correct too")
        ap.add_argument("--no-db", dest="no_db", action="store_true",
                        help="Do not connect; recommend from the machine alone")
        ap.add_argument("--idmap-table", dest="idmap_table", default=None,
                        help="The identity map's table, whose size sizes shared_buffers "
                             "(default: the idmap's tableName, else idmap)")

    def process(self, args, rest):
        super().process(args, rest)
        db, workers, table, unreadable = None, DEFAULT_WORKERS, "idmap", None
        try:
            cfgs = self.configs or runtime.get_configs(instantiate=False)
            db, workers, table = pipeline_settings(cfgs)
        except Exception as e:
            # a machine-only run is still useful
            unreadable = f"could not read the pipeline configuration ({e})"
        args.workers = args.workers or workers
        args.idmap_table = args.idmap_table or table
        m, srv, why = pg_tune.analyse(db, args.no_db, args.idmap_table, unreadable)
        lines, status = pg_tune.report(m, srv, why, args)
        # the report is the command's result: SQL to copy
        for line in lines:
            print(line)
        if status:
            sys.exit(status)
        return lines
