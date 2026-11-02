"""Maintain the Postgres tables: vacuum, report bloat, rewrite, index DDL.

    lux pg-maintain --bloat                          # report what each cache table holds (read-only)
    lux pg-maintain --vacuum                         # VACUUM (ANALYZE, FREEZE) every cache table
    lux pg-maintain --vacuum --source ycba --cache datacache
    lux pg-maintain --vacuum --map idmap             # the identity map too (identify does this itself)
    lux pg-maintain --rewrite --source orcid --cache datacache   # CLUSTER one table: exclusive lock
    lux pg-maintain --drop-time-indexes              # one-off DDL: drop unused insert_time indexes
    lux pg-maintain --idmap-indexes [--keep-old-index]           # one-off DDL: covering (yuid, uri) index

The Postgres counterpart of ``lux pg-tune``: that one says how the server
should be set, this one tidies the tables. Replaces ``manage-data.py``'s
maintenance flags:

    --vacuum / --optimize       lux pg-maintain --vacuum
    --bloat                     lux pg-maintain --bloat
    --rewrite <src>_<cache>     lux pg-maintain --rewrite --source <src> --cache <cache>
    --drop-time-indexes         lux pg-maintain --drop-time-indexes
    --idmap-indexes             lux pg-maintain --idmap-indexes (--keep-old-index)

Without ``--source`` / ``--cache``, the tables are every source's
``datacache``, ``recordcache`` and ``recordcache2`` and the results'
``recordcache`` and ``recordcache2``, as manage-data.py walked them;
``--source`` and ``--cache`` narrow that (``--source`` takes a name, a list,
``internal``, ``external``, ``results`` or ``all``). ``--map`` adds maps to
``--vacuum`` and ``--bloat``. A store that is not Postgres (LMDB, files) is
skipped.

Given several actions, they run in this order: ``--bloat`` (before any
VACUUM, which zeroes the dead-tuple counts it reports), then the index DDL,
then ``--rewrite``, then ``--vacuum`` (whose FREEZE is what lets the new
covering index answer from the index alone). ``--rewrite`` holds an ACCESS
EXCLUSIVE lock on each table for as long as it runs and needs as much free
disk as the table, so it only takes sources named one by one, never a
group. Everything but ``--bloat`` refuses while a ``lux`` build is running
(``--force`` overrides).
"""

import logging

from lux_pipeline.cli import _targets as targets
from lux_pipeline.cli._handler import BaseHandler, refuse_while_building

logger = logging.getLogger("lux_pipeline")

# the tables manage-data.py's maintenance flags walked
SOURCE_CACHES = ("datacache", "recordcache", "recordcache2")
RESULT_CACHES = ("recordcache", "recordcache2")
ACTIONS = ("bloat", "drop_time_indexes", "idmap_indexes", "rewrite", "vacuum")


class CommandHandler(BaseHandler):
    def add_args(self, ap):
        ap.add_argument("--vacuum", "--optimize", dest="vacuum", action="store_true",
                        help="VACUUM (ANALYZE, FREEZE) the tables")
        ap.add_argument("--bloat", action="store_true",
                        help="Report each table's size and the space VACUUM cannot return")
        ap.add_argument("--rewrite", action="store_true",
                        help="CLUSTER the named tables (exclusive lock; needs --source names "
                             "and as much free disk as each table)")
        ap.add_argument("--drop-time-indexes", dest="drop_time_indexes", action="store_true",
                        help="Drop the insert_time index from caches that never use it")
        ap.add_argument("--idmap-indexes", dest="idmap_indexes", action="store_true",
                        help="Build the idmap's covering (yuid, uri) index CONCURRENTLY and "
                             "drop the (yuid) one it replaces")
        ap.add_argument("--keep-old-index", dest="keep_old_index", action="store_true",
                        help="With --idmap-indexes: keep the old (yuid) index")
        ap.add_argument("--cache", type=str,
                        help="Cache types separated by commas (default: datacache, recordcache, "
                             "recordcache2)")
        ap.add_argument("--map", type=str,
                        help="Maps to --vacuum or report --bloat on, separated by commas, "
                             "or 'all'")
        ap.add_argument("--force", action="store_true",
                        help="Run even though a build looks to be running")

    # --- targets -------------------------------------------------------------

    def tables(self, args):
        """``[(source name, cache type, cache)]``: the cache tables selected,
        in configuration order, whatever their backend."""
        which = getattr(args, "source", None) or "all"
        named = targets.split(args.cache) if getattr(args, "cache", None) else None
        if which == "all":
            selected = targets.all_sources(self.configs)
        else:
            selected = targets.sources(self.configs, which)
        out = []
        for group, cfg in selected:
            types = named or (RESULT_CACHES if group == "results" else SOURCE_CACHES)
            for t in types:
                cache = cfg.get(t)
                if cache is not None:
                    out.append((cfg["name"], t, cache))
        return out

    def maps(self, args):
        """``[(name, store)]`` for ``--map``."""
        which = getattr(args, "map", None)
        if not which:
            return []
        return [(m, targets.map_store(self.configs, m)) for m in targets.maps(self.configs, which)]

    # --- the actions -----------------------------------------------------------

    @staticmethod
    def _has(obj, method, label, skipped):
        if callable(getattr(obj, method, None)):
            return True
        skipped.append(label)
        return False

    def report_bloat(self, tables, maps, skipped):
        for name, t, cache in tables:
            if self._has(cache, "bloat_line", f"{name} {t}", skipped):
                # the report is the command's result
                print(cache.bloat_line())
        for m, store in maps:
            if self._has(store, "index_sizes", f"map {m}", skipped):
                sizes = store.index_sizes()
                print(f"map {m}: " + ", ".join(f"{k} {_fmt(v)}" for k, v in sorted(sizes.items())))

    def drop_time_indexes(self, tables, skipped):
        dropped = 0
        for name, t, cache in tables:
            if self._has(cache, "drop_time_index", f"{name} {t}", skipped):
                dropped += bool(cache.drop_time_index())
        print(f"dropped {dropped} insert_time index(es)")

    def idmap_indexes(self, keep_old):
        idmap = self.configs.get_idmap()
        if not callable(getattr(idmap, "ensure_covering_indexes", None)):
            raise ValueError(f"the idmap ({type(idmap).__name__}) is not the Postgres one; "
                             f"--idmap-indexes has nothing to build")
        idmap.ensure_covering_indexes(drop_old=not keep_old)

    def rewrite(self, tables, skipped):
        for name, t, cache in tables:
            if self._has(cache, "rewrite", f"{name} {t}", skipped):
                print(f"Rewriting {name} {t}")
                cache.rewrite()

    def vacuum(self, tables, maps, skipped):
        for name, t, cache in tables:
            if self._has(cache, "optimize", f"{name} {t}", skipped):
                print(f"Vacuuming {name} {t}")
                cache.optimize()
        for m, store in maps:
            if self._has(store, "optimize", f"map {m}", skipped):
                print(f"Vacuuming map {m}")
                store.optimize()

    # --- the command -------------------------------------------------------

    def process(self, args, rest):
        super().process(args, rest)
        chosen = [a for a in ACTIONS if getattr(args, a, False)]
        if not chosen:
            raise ValueError("Nothing to do: give --vacuum, --bloat, --rewrite, "
                             "--drop-time-indexes or --idmap-indexes")
        if getattr(args, "keep_old_index", False) and not args.idmap_indexes:
            raise ValueError("--keep-old-index goes with --idmap-indexes")
        if args.rewrite:
            which = getattr(args, "source", None)
            if not which or which in ("all", *targets.GROUPS):
                raise ValueError("--rewrite locks each table and needs its size again in free "
                                 "disk: name the sources (--source a,b) and caches (--cache)")
            if not getattr(args, "cache", None):
                raise ValueError("--rewrite needs --cache: the cache types to rewrite")
        if any(a != "bloat" for a in chosen):
            refuse_while_building(self.configs, "pg-maintain", getattr(args, "force", False))

        # resolve everything before touching anything, so a typo does nothing
        tables = self.tables(args)
        maps = self.maps(args)
        skipped = []
        if args.bloat:
            self.report_bloat(tables, maps, skipped)
        if args.drop_time_indexes:
            self.drop_time_indexes(tables, skipped)
        if args.idmap_indexes:
            self.idmap_indexes(args.keep_old_index)
        if args.rewrite:
            self.rewrite(tables, skipped)
        if args.vacuum:
            self.vacuum(tables, maps, skipped)
        skipped = sorted(set(skipped))
        if skipped:
            print(f"skipped, not Postgres tables: {', '.join(skipped)}")
        return {"actions": chosen, "tables": [(n, t) for n, t, _ in tables],
                "maps": [m for m, _ in maps], "skipped": skipped}


def _fmt(n):
    for unit in ("B", "kB", "MB", "GB", "TB"):
        if abs(n) < 1024 or unit == "TB":
            return f"{n:.0f}{unit}" if unit == "B" else f"{n:.1f}{unit}"
        n /= 1024
