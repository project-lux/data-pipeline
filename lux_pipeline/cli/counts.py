"""Count records in caches, maps and indexes."""

import logging
import sys

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
        ap.add_argument("--table", action="store_true",
                        help="Render as tables (rich) instead of plain lines")

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
        for group, cfg in srcs:
            for key, label in INDEX_PATHS.items():
                path = cfg.get(key)
                if path:
                    out.append((group, cfg["name"], label, path))
        return out

    # --- counting ----------------------------------------------------------

    def count_caches(self, sources, caches):
        """``([(group, source, cache type, count, exact)], total)``"""
        rows = []
        total = 0
        for group, cfg in sources:
            for c in caches:
                cache = cfg.get(c)
                if cache is None:
                    continue
                n, exact = _count(cache)
                rows.append((group, cfg["name"], c, n, exact))
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
        """``([(group, source, label, count or None)], total)``; ``None``
        when the index file could not be opened (not built yet, or wrong
        path)."""
        rows = []
        total = 0
        for group, source, label, path in indexes:
            try:
                idx = self.open_index(path)
            except Exception as e:
                logger.warning(f"Could not open {label} index for {source} at {path}: {e}")
                rows.append((group, source, label, None))
                continue
            try:
                n = len(idx)
            finally:
                close = getattr(idx, "close", None)
                if close:
                    close()
            rows.append((group, source, label, n))
            total += n
        return rows, total

    # --- the command -------------------------------------------------------

    def process(self, args, rest):
        super().process(args, rest)
        sources = self.resolve_sources(args)
        caches = self.resolve_caches(args)
        maps = self.resolve_maps(args)
        indexes = self.resolve_indexes(args)

        # each section is (rows, total); None when it was not asked for.
        # The totals are kept apart so the map and index totals do not
        # include the caches
        cache_sec = self.count_caches(sources, caches) if sources else None
        map_sec = self.count_maps(maps) if getattr(args, "map", None) else None
        index_sec = self.count_indexes(indexes) if getattr(args, "index", None) else None

        # print is the command's result
        if getattr(args, "table", False):
            render_tables(cache_sec, map_sec, index_sec)
        else:
            render_plain(cache_sec, map_sec, index_sec)


# --- output ------------------------------------------------------------------


def render_plain(cache_sec, map_sec, index_sec):
    """One line per count, as ``manage-data.py --counts`` prints them."""
    if cache_sec:
        rows, total = cache_sec
        for _, source, c, n, exact in rows:
            print(f"{source} {c}: {_fmt(n, exact)}")
        print(f"Total cache records: ~{total:,}")
    if map_sec:
        rows, total = map_sec
        for m, n in rows:
            print(f"{m}: ~{n:,}")
        print(f"Total map records: ~{total:,}")
    if index_sec:
        rows, total = index_sec
        for _, source, label, n in rows:
            shown = "missing" if n is None else _fmt(n, True)
            print(f"{source} {label} index: {shown}")
        print(f"Total index records: ={total:,}")


_TYPE_STYLE = {"internal": "bold blue", "external": "bold magenta", "results": "yellow"}


def _source_table(title, col3, rows, total, widths, exact_total=False):
    """A Source | Type | <col3> | Count table with a total footer.

    rows: ``(group, source, col3 value, count or None, exact)``
    """
    from rich import box
    from rich.table import Table
    from rich.text import Text

    w_source, w_type, w_col3, w_count = widths
    table = Table(title=title, box=box.ROUNDED, title_justify="left",
                  header_style="bold", expand=False, padding=(0, 1))
    table.add_column("Source", style="bold cyan", no_wrap=True, min_width=w_source)
    table.add_column("Type", no_wrap=True, min_width=w_type)
    table.add_column(col3, style="green", no_wrap=True, min_width=max(w_col3, len(col3)))
    table.add_column("Count", justify="right", style="yellow", no_wrap=True, min_width=w_count)
    for group, source, value, n, exact in rows:
        count = Text("missing", style="red") if n is None else _fmt(n, exact)
        table.add_row(source, Text(group, style=_TYPE_STYLE.get(group, "")), value, count)
    table.add_section()
    table.add_row(Text("Total", style="bold"), "", "",
                  Text(_fmt(total, exact_total), style="bold yellow"))
    return table


def _maps_table(rows, total):
    from rich import box
    from rich.table import Table
    from rich.text import Text

    table = Table(title="Map counts", box=box.ROUNDED, title_justify="left",
                  header_style="bold", expand=False, padding=(0, 1))
    table.add_column("Map", style="bold cyan", no_wrap=True)
    table.add_column("Count", justify="right", style="yellow", no_wrap=True)
    for name, n in rows:
        table.add_row(name, f"~{n:,}")
    table.add_section()
    table.add_row(Text("Total", style="bold"), Text(f"~{total:,}", style="bold yellow"))
    return table


def render_tables(cache_sec, map_sec, index_sec):
    """The same counts as rich tables; the cache and index tables share
    column widths so they line up, and a grand total follows when more
    than one section is shown."""
    from rich.console import Console
    from rich.rule import Rule

    console = Console(file=sys.stdout)
    index_rows = [(g, s, label, n, True)
                  for g, s, label, n in index_sec[0]] if index_sec else []
    cache_rows = cache_sec[0] if cache_sec else []

    widths = [len("Source"), len("Type"), 0, len("Count")]
    for _, source, value, n, exact in [*cache_rows, *index_rows]:
        widths[0] = max(widths[0], len(source))
        widths[2] = max(widths[2], len(value))
        widths[3] = max(widths[3], len("missing" if n is None else _fmt(n, exact)))
    widths[1] = max([widths[1], *(len(r[0]) for r in [*cache_rows, *index_rows])])

    shown = []
    grand = 0
    if cache_sec:
        shown.append(_source_table("Cache counts", "Cache", cache_rows, cache_sec[1], widths))
        grand += cache_sec[1]
    if map_sec:
        shown.append(_maps_table(*map_sec))
        grand += map_sec[1]
    if index_sec:
        shown.append(_source_table("Index counts", "Index", index_rows, index_sec[1], widths,
                                   exact_total=True))
        grand += index_sec[1]

    for i, table in enumerate(shown):
        if i:
            console.print()
        console.print(table)
    if len(shown) > 1:
        console.print()
        console.print(Rule(f"[bold]Grand total: ~{grand:,}[/bold]"))
