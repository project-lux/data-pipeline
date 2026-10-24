"""Count records in caches, maps and indexes."""

import logging

from lux_pipeline.cli._handler import BaseHandler

logger = logging.getLogger("lux_pipeline")

GROUPS = ("internal", "external", "results")
DEFAULT_CACHES = ("datacache", "recordcache", "reconciledRecordcache", "recordcache2")
# the reconcile indexes are configured as paths on the source (item 34 may
# give them names); key -> label used in the output
INDEX_PATHS = {"reconcileDbPath": "name", "inverseEquivDbPath": "id"}
# below this a catalog estimate is replaced by an exact count
EXACT_BELOW = 100_000


def _fmt(n, exact):
    return f"{'=' if exact else '~'}{n:,}"


def _count(cache):
    """``(count, exact)`` for a cache: the store's estimate when it has one
    and is large, otherwise an exact ``len()`` -- the rule from
    ``manage-data.py --counts``."""
    est = getattr(cache, "len_estimate", None)
    if est is None:
        return len(cache), True
    n = est()
    if n < EXACT_BELOW:
        return len(cache), True
    return n, False


def _split(value):
    return [s.strip() for s in value.split(",") if s.strip()]


class CommandHandler(BaseHandler):
    def add_args(self, ap):
        ap.add_argument("--cache", type=str,
                        help="Cache types separated by commas, or 'all' "
                             f"(default: {','.join(DEFAULT_CACHES)})")
        ap.add_argument("--map", type=str,
                        help="Map names separated by commas, or 'all'")
        ap.add_argument("--index", type=str,
                        help="Sources whose reconcile indexes to count, "
                             "separated by commas, or 'all'")

    # --- lookups -----------------------------------------------------------

    def _lookup(self, name):
        """``(group, config)`` for a source name, or ``ValueError``."""
        for group in GROUPS:
            cfg = getattr(self.configs, group)
            if name in cfg:
                return group, cfg[name]
        raise ValueError(f"Unknown source: {name}")

    def _all_sources(self):
        return [(g, c) for g in GROUPS
                for c in getattr(self.configs, g).values()]

    def resolve_sources(self, args):
        """``[(group, config)]`` for the caches to count.

        Without ``--source`` every source in every group is counted, unless
        only maps or indexes were asked for. ``all`` and the group names
        work as for every other command.
        """
        which = getattr(args, "source", None)
        if not which:
            others = getattr(args, "map", None) or getattr(args, "index", None)
            return [] if others else self._all_sources()
        if which == "all":
            return self._all_sources()
        if which in GROUPS:
            return [(which, c) for c in getattr(self.configs, which).values()]
        return [self._lookup(s) for s in _split(which)]

    def resolve_caches(self, args):
        which = getattr(args, "cache", None)
        if not which or which == "all":
            return list(DEFAULT_CACHES)
        return _split(which)

    def resolve_maps(self, args):
        which = getattr(args, "map", None)
        if not which:
            return []
        stores = self.configs.map_stores
        if which == "all":
            return list(stores.keys())
        names = _split(which)
        for n in names:
            if n not in stores:
                raise ValueError(f"Unknown map: {n}")
        return names

    def resolve_indexes(self, args):
        """``[(source name, label, path)]`` for every configured index of the
        requested sources."""
        which = getattr(args, "index", None)
        if not which:
            return []
        if which == "all":
            srcs = self._all_sources()
        else:
            srcs = [self._lookup(s) for s in _split(which)]
        out = []
        for _, cfg in srcs:
            for key, label in INDEX_PATHS.items():
                path = cfg.get(key)
                if path:
                    out.append((cfg["name"], label, path))
        return out

    # --- counting ----------------------------------------------------------

    def count_caches(self, sources, caches):
        """``([(source, cache type, count, exact)], total)``"""
        rows = []
        total = 0
        for _, cfg in sources:
            for c in caches:
                cache = cfg.get(c)
                if cache is None:
                    continue
                n, exact = _count(cache)
                rows.append((cfg["name"], c, n, exact))
                total += n
        return rows, total

    def count_maps(self, maps):
        """``([(map name, count)], total)``. Counts are reported as estimates:
        the postgres idmap answers ``len()`` from the catalog."""
        rows = []
        total = 0
        for m in maps:
            cfg = self.configs.map_stores[m]
            store = cfg.get("store")
            if store is None:
                store = self.configs.instantiate_map(m)["store"]
            n = len(store)
            rows.append((m, n))
            total += n
        return rows, total

    def open_index(self, path):
        from lux_pipeline.storage.idmap.lmdb import TabLmdb
        return TabLmdb.open(path, "r", readahead=False, writemap=True)

    def count_indexes(self, indexes):
        """``([(source, label, count or None)], total)``; ``None`` when the
        index file could not be opened (not built yet, or wrong path)."""
        rows = []
        total = 0
        for source, label, path in indexes:
            try:
                idx = self.open_index(path)
            except Exception as e:
                logger.warning(f"Could not open {label} index for {source} at {path}: {e}")
                rows.append((source, label, None))
                continue
            try:
                n = len(idx)
            finally:
                close = getattr(idx, "close", None)
                if close:
                    close()
            rows.append((source, label, n))
            total += n
        return rows, total

    # --- the command -------------------------------------------------------

    def process(self, args, rest):
        super().process(args, rest)
        sources = self.resolve_sources(args)
        caches = self.resolve_caches(args)
        maps = self.resolve_maps(args)
        indexes = self.resolve_indexes(args)

        # print is the command's result; the totals are kept apart so the
        # map and index totals do not include the caches
        if sources:
            rows, total = self.count_caches(sources, caches)
            for source, c, n, exact in rows:
                print(f"{source} {c}: {_fmt(n, exact)}")
            print(f"Total cache records: ~{total:,}")

        if getattr(args, "map", None):
            rows, total = self.count_maps(maps)
            for m, n in rows:
                print(f"{m}: ~{n:,}")
            print(f"Total map records: ~{total:,}")

        if getattr(args, "index", None):
            rows, total = self.count_indexes(indexes)
            for source, label, n in rows:
                shown = "missing" if n is None else _fmt(n, True)
                print(f"{source} {label} index: {shown}")
            print(f"Total index records: ={total:,}")
