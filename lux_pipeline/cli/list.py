"""List the configured sources, maps and indexes.

    lux list                     # internal, external and results sources
    lux list --source external
    lux list --source aat,lcnaf
    lux list --maps              # the map stores (idmap, all_refs, ...)
    lux list --indexes           # the reconcile index files per source
"""

import sys

from rich import box
from rich.console import Console
from rich.table import Table
from rich.text import Text

from lux_pipeline.cli._handler import BaseHandler
from lux_pipeline.config import INDEX_LABELS, source_indexes

GROUPS = ("internal", "external", "results")

# Classes that are interesting enough to display (in priority order)
_CLASS_KEYS = [
    ("mapperClass", "Mapper"),
    ("harvesterClass", "Harvester"),
    ("downloaderClass", "Downloader"),
    ("loaderClass", "Loader"),
    ("fetcherClass", "Fetcher"),
    ("acquirerClass", "Acquirer"),
    ("reconcilerClass", "Reconciler"),
    ("indexLoaderClass", "IndexLoader"),
]

# a source's indexes come from its named `indexes` block or the legacy path
# keys (lux_pipeline.config.source_indexes); INDEX_LABELS gives the words
# `lux counts` uses

GROUP_TITLES = {
    "internal": "Internal sources  [dim](Yale units)[/dim]",
    "external": "External sources  [dim](authorities & linked-data)[/dim]",
    "results": "Result sources",
}


def _short_class(value) -> str:
    """Just the class name from a dotted module path; a class object (a
    config that was built in code) by its name."""
    if not isinstance(value, str):
        return getattr(value, "__name__", str(value))
    return value.rsplit(".", 1)[-1]


def _namespace_display(ns: str) -> str:
    """Trim long namespaces to a readable domain + path stub."""
    if not ns:
        return ""
    ns = ns.removeprefix("https://").removeprefix("http://").rstrip("/")
    if len(ns) > 42:
        ns = ns[:39] + "…"
    return ns


# --- the listing as data -------------------------------------------------------------


def source_row(group, name, cfg):
    """One source as data: ``{"group", "source", "found", "namespace",
    "namespace_display", "merge_order", "order", "classes": [{"key",
    "label", "class", "short"}]}``. ``order`` is the text the table shows
    (``merge_order`` as written, empty when absent). A class key set to
    null switches that component off (merged.json and marklogic.json do it
    for acquirerClass), so it is not listed. `cfg` None: an unknown name
    (``found`` False)."""
    if cfg is None:
        return {"group": None, "source": name, "found": False, "namespace": "",
                "namespace_display": "", "merge_order": None, "order": "", "classes": []}
    classes = [{"key": key, "label": label,
                "class": cfg[key] if isinstance(cfg[key], str) else _short_class(cfg[key]),
                "short": _short_class(cfg[key])}
               for key, label in _CLASS_KEYS if cfg.get(key)]
    namespace = cfg.get("namespace", "")
    return {"group": group, "source": name, "found": True, "namespace": namespace,
            "namespace_display": _namespace_display(namespace),
            "merge_order": cfg.get("merge_order"), "order": str(cfg.get("merge_order", "")),
            "classes": classes}


def source_rows(cfgs, which=None):
    """The sources `lux list` shows for ``--source`` `which` (None or
    ``all``: every group; a group name; or names separated by commas), as
    `source_row` dicts. Named sources come in group order and, within a
    group, in the order named; unknown names follow, ``found`` False."""
    if not which or which == "all":
        return [source_row(g, n, c) for g in GROUPS for n, c in getattr(cfgs, g).items()]
    if which in GROUPS:
        return [source_row(which, n, c) for n, c in getattr(cfgs, which).items()]
    binned = {g: [] for g in GROUPS}
    unknown = []
    for name in which.split(","):
        for g in GROUPS:
            if name in getattr(cfgs, g):
                binned[g].append(name)
                break
        else:
            unknown.append(name)
    rows = [source_row(g, n, getattr(cfgs, g)[n]) for g in GROUPS for n in binned[g]]
    return rows + [source_row(None, n, None) for n in unknown]


def map_rows(cfgs):
    """``[{"map", "class", "store"}]`` for every map store; ``store`` says
    where it lives (`_store_details`)."""
    return [{"map": name, "class": cfg.get("storeClass", ""), "store": _store_details(cfg)}
            for name, cfg in cfgs.map_stores.items()]


def index_rows(cfgs, which):
    """``(group, source, label, path)`` for every configured index of the
    sources `which` selects (``all``, a group, or names)."""
    if which in GROUPS:
        groups, names = (which,), None
    elif not which or which == "all":
        groups, names = GROUPS, None
    else:
        groups, names = GROUPS, set(which.split(","))
    rows = []
    for group in groups:
        for name, cfg in getattr(cfgs, group).items():
            if names is not None and name not in names:
                continue
            for idx_name, path in source_indexes(cfg).items():
                rows.append((group, name, INDEX_LABELS.get(idx_name, idx_name), path))
    return rows


# --- the tables ------------------------------------------------------------------------


def _row_values(row) -> tuple[str, str, str, str]:
    """Plain-text (source, namespace, order, classes) of a `source_row`,
    for width measurement and the table."""
    classes = "\n".join(f"{c['label']}: {c['short']}" for c in row["classes"])
    return row["source"], row["namespace_display"], row["order"], classes


def _measure_col_widths(rows) -> tuple[int, int, int, int]:
    """Return the minimum column widths needed so every table can share the
    same layout.  Headers provide the floor for each column."""
    w_source = len("Source")
    w_ns = len("Namespace")
    w_order = len("Order")
    w_class = len("Classes")

    for row in rows:
        src, ns, order, classes = _row_values(row)
        w_source = max(w_source, len(src))
        w_ns = min(max(w_ns, len(ns)), 44)  # respect the 44-char cap
        w_order = max(w_order, len(order))
        for line in classes.splitlines():
            w_class = max(w_class, len(line))

    return w_source, w_ns, w_order, w_class


def _build_table(
    title: str,
    rows: list,
    col_widths: tuple[int, int, int, int] | None = None,
) -> Table:
    w_source, w_ns, w_order, w_class = col_widths or (0, 44, 0, 0)
    table = Table(
        title=title,
        box=box.ROUNDED,
        show_lines=False,
        title_justify="left",
        header_style="bold",
        expand=False,
        padding=(0, 1),
    )
    table.add_column("Source", style="bold cyan", no_wrap=True, min_width=w_source)
    table.add_column("Namespace", style="dim", max_width=44, min_width=w_ns)
    table.add_column(
        "Order", justify="right", style="yellow", no_wrap=True, min_width=w_order
    )
    table.add_column("Classes", style="green", min_width=w_class)

    for row in rows:
        src, namespace, merge_order, _ = _row_values(row)
        marked = "\n".join(f"[dim]{c['label']}:[/dim] {c['short']}" for c in row["classes"])
        table.add_row(src, namespace, merge_order, marked)

    return table


def _store_details(cfg: dict) -> str:
    """Where a map store lives, chosen by its class: the Postgres classes
    default the table to the map's name, and a config migrated from Redis
    may still carry host/port keys that nothing reads."""
    clss = cfg.get("storeClass", "")
    if "postgres" in clss:
        return f"table {cfg.get('tableName') or cfg.get('name', '')}"
    if "redis" in clss and cfg.get("host"):
        where = f"{cfg['host']}:{cfg['port']}" if cfg.get("port") else cfg["host"]
        return f"{where}/{cfg['db']}" if "db" in cfg else where
    return ""


def _maps_table(rows: list) -> Table:
    """rows: `map_rows`"""
    table = Table(title="Maps", box=box.ROUNDED, show_lines=False, title_justify="left",
                  header_style="bold", expand=False, padding=(0, 1))
    table.add_column("Map", style="bold cyan", no_wrap=True)
    table.add_column("Class", style="green", no_wrap=True)
    table.add_column("Store", style="dim", no_wrap=True)
    for row in rows:
        table.add_row(row["map"], row["class"], row["store"])
    return table


def _indexes_table(rows: list[tuple[str, str, str, str]]) -> Table:
    """rows: (group, source, label, path)"""
    table = Table(title="Indexes", box=box.ROUNDED, show_lines=False, title_justify="left",
                  header_style="bold", expand=False, padding=(0, 1))
    table.add_column("Source", style="bold cyan", no_wrap=True)
    table.add_column("Type", no_wrap=True)
    table.add_column("Index", style="green", no_wrap=True)
    table.add_column("Path", style="dim")
    for group, source, label, path in rows:
        table.add_row(source, group, label, path)
    return table


class CommandHandler(BaseHandler):
    def add_args(self, ap):
        ap.add_argument("--maps", action="store_true", help="List the map stores")
        ap.add_argument("--indexes", action="store_true",
                        help="List the reconcile index files per source")

    def index_rows(self, which):
        """``(group, source, label, path)`` for every configured index of the
        sources `which` selects (``all``, a group, or names)."""
        return index_rows(self.configs, which)

    def process(self, args, rest):
        super().process(args, rest)
        cfgs = self.configs
        console = Console(file=sys.stdout)
        only_extras = (args.maps or args.indexes) and not args.source

        if not only_extras:
            self.list_sources(args, console)
        if args.maps:
            console.print(_maps_table(map_rows(cfgs)))
            console.print()
        if args.indexes:
            console.print(_indexes_table(self.index_rows(args.source)))
            console.print()

    def list_sources(self, args, console):
        rows = source_rows(self.configs, args.source)
        for row in rows:
            if not row["found"]:
                console.print(f"[bold red]Unknown source:[/bold red] {row['source']}")
        found = [r for r in rows if r["found"]]
        # every table shares the widths measured across all of them
        col_widths = _measure_col_widths(found)
        for group in GROUPS:
            shown = [r for r in found if r["group"] == group]
            if not shown:
                continue
            console.print(_build_table(GROUP_TITLES[group], shown, col_widths))
            console.print()
