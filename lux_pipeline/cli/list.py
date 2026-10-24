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

# the reconcile indexes are configured as paths on the source (item 34 may
# give them names); key -> label, the words `lux counts` uses
INDEX_PATHS = {"reconcileDbPath": "name", "inverseEquivDbPath": "id"}

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


def _row_values(s: str, cfg: dict | None) -> tuple[str, str, str, str]:
    """Return plain-text (source, namespace, order, classes) for width measurement."""
    if cfg is None:
        return s, "(not found)", "", ""
    namespace = _namespace_display(cfg.get("namespace", ""))
    merge_order = str(cfg.get("merge_order", ""))
    # a key set to null in the config switches that component off
    # (merged.json and marklogic.json do it for acquirerClass): not a class
    class_lines = [
        f"{label}: {_short_class(cfg[key])}" for key, label in _CLASS_KEYS if cfg.get(key)
    ]
    return s, namespace, merge_order, "\n".join(class_lines)


def _measure_col_widths(
    groups_data: list[tuple[list, dict]],
) -> tuple[int, int, int, int]:
    """Return the minimum column widths needed so every table can share the
    same layout.  Headers provide the floor for each column."""
    w_source = len("Source")
    w_ns = len("Namespace")
    w_order = len("Order")
    w_class = len("Classes")

    for sources, cfg_group in groups_data:
        for s in sources:
            cfg = cfg_group.get(s)
            src, ns, order, classes = _row_values(s, cfg)
            w_source = max(w_source, len(src))
            w_ns = min(max(w_ns, len(ns)), 44)  # respect the 44-char cap
            w_order = max(w_order, len(order))
            for line in classes.splitlines():
                w_class = max(w_class, len(line))

    return w_source, w_ns, w_order, w_class


def _build_table(
    title: str,
    sources: list,
    cfgs_group: dict,
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

    for s in sources:
        cfg = cfgs_group.get(s)
        src, namespace, merge_order, classes_text = _row_values(s, cfg)
        if cfg is None:
            table.add_row(src, Text("(not found)", style="red"), "", "")
            continue
        marked_lines = []
        for line in classes_text.splitlines():
            label, _, cls = line.partition(": ")
            marked_lines.append(f"[dim]{label}:[/dim] {cls}")
        table.add_row(src, namespace, merge_order, "\n".join(marked_lines))

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


def _maps_table(map_stores: dict) -> Table:
    table = Table(title="Maps", box=box.ROUNDED, show_lines=False, title_justify="left",
                  header_style="bold", expand=False, padding=(0, 1))
    table.add_column("Map", style="bold cyan", no_wrap=True)
    table.add_column("Class", style="green", no_wrap=True)
    table.add_column("Store", style="dim", no_wrap=True)
    for name, cfg in map_stores.items():
        table.add_row(name, cfg.get("storeClass", ""), _store_details(cfg))
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
        if which in GROUPS:
            groups, names = (which,), None
        elif not which or which == "all":
            groups, names = GROUPS, None
        else:
            groups, names = GROUPS, set(which.split(","))
        rows = []
        for group in groups:
            for name, cfg in getattr(self.configs, group).items():
                if names is not None and name not in names:
                    continue
                for key, label in INDEX_PATHS.items():
                    if cfg.get(key):
                        rows.append((group, name, label, cfg[key]))
        return rows

    def process(self, args, rest):
        super().process(args, rest)
        cfgs = self.configs
        console = Console(file=sys.stdout)
        only_extras = (args.maps or args.indexes) and not args.source

        if not only_extras:
            self.list_sources(args, console)
        if args.maps:
            console.print(_maps_table(cfgs.map_stores))
            console.print()
        if args.indexes:
            console.print(_indexes_table(self.index_rows(args.source)))
            console.print()

    def list_sources(self, args, console):
        cfgs = self.configs

        # Determine which group(s) to show
        if not args.source or args.source == "all":
            groups = list(GROUPS)
        elif args.source in GROUPS:
            groups = [args.source]
        else:
            # Named sources — look them up and bin by group
            named = args.source.split(",")
            binned: dict[str, list] = {g: [] for g in GROUPS}
            for s in named:
                for g in GROUPS:
                    if s in getattr(cfgs, g):
                        binned[g].append(s)
                        break
                else:
                    console.print(f"[bold red]Unknown source:[/bold red] {s}")
            groups = [g for g in GROUPS if binned[g]]
            col_widths = _measure_col_widths(
                [(binned[g], getattr(cfgs, g)) for g in groups]
            )
            for group in groups:
                table = _build_table(
                    GROUP_TITLES[group],
                    binned[group],
                    getattr(cfgs, group),
                    col_widths,
                )
                console.print(table)
                console.print()
            return

        # Gather all groups that have content, then measure widths across all
        # of them so every table renders with identical column widths.
        active = [
            (group, list(getattr(cfgs, group).keys()), getattr(cfgs, group))
            for group in groups
            if getattr(cfgs, group)
        ]
        col_widths = _measure_col_widths(
            [(srcs, cfg_grp) for _, srcs, cfg_grp in active]
        )

        for group, sources, cfg_group in active:
            table = _build_table(GROUP_TITLES[group], sources, cfg_group, col_widths)
            console.print(table)
            console.print()
