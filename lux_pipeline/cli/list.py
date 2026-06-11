from rich import box
from rich.console import Console
from rich.table import Table
from rich.text import Text

from ._handler import BaseHandler as BH

# Classes that are interesting enough to display (in priority order)
_CLASS_KEYS = [
    ("mapperClass", "Mapper"),
    ("harvesterClass", "Harvester"),
    ("loaderClass", "Loader"),
    ("fetcherClass", "Fetcher"),
    ("reconcilerClass", "Reconciler"),
]


# Strip the common "lux_pipeline." / "sources." / "process." prefix so the
# cell stays readable.
def _short_class(dotted: str) -> str:
    parts = dotted.rsplit(".", 1)
    return parts[-1] if parts else dotted


def _namespace_display(ns: str) -> str:
    """Trim long namespaces to a readable domain + path stub."""
    if not ns:
        return ""
    # Drop scheme
    ns = ns.removeprefix("https://").removeprefix("http://")
    # Trim trailing slash
    ns = ns.rstrip("/")
    # Keep at most 40 chars
    if len(ns) > 42:
        ns = ns[:39] + "…"
    return ns


def _row_values(s: str, cfg: dict | None) -> tuple[str, str, str, str]:
    """Return the four plain-text cell values for a single source row."""
    if cfg is None:
        return s, "(not found)", "", ""
    namespace = _namespace_display(cfg.get("namespace", ""))
    merge_order = str(cfg.get("merge_order", ""))
    class_lines = []
    for key, label in _CLASS_KEYS:
        if key in cfg:
            class_lines.append(f"{label}: {_short_class(cfg[key])}")
    classes_text = "\n".join(class_lines)
    return s, namespace, merge_order, classes_text


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
            # classes cell may be multi-line — measure longest line
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
        # Re-apply markup for the classes cell
        marked_lines = []
        for line in classes_text.splitlines():
            label, _, cls = line.partition(": ")
            marked_lines.append(f"[dim]{label}:[/dim] {cls}")
        table.add_row(src, namespace, merge_order, "\n".join(marked_lines))

    return table


class CommandHandler(BH):
    def process(self, args, rest):
        super().process(args, rest)
        cfgs = self.configs
        console = Console()

        # Determine which group(s) to show
        if not args.source or args.source == "all":
            groups = ["internal", "external", "results"]
        elif args.source in ["internal", "external", "results"]:
            groups = [args.source]
        else:
            # Named sources — look them up and bin by group
            named = args.source.split(",")
            binned: dict[str, list] = {"internal": [], "external": [], "results": []}
            for s in named:
                if s in cfgs.internal:
                    binned["internal"].append(s)
                elif s in cfgs.external:
                    binned["external"].append(s)
                elif s in cfgs.results:
                    binned["results"].append(s)
                else:
                    console.print(f"[bold red]Unknown source:[/bold red] {s}")
            groups = [g for g in ["internal", "external", "results"] if binned[g]]
            for group in groups:
                cfg_group = getattr(cfgs, group)
                table = _build_table(
                    f"[bold]{group.capitalize()} sources[/bold]",
                    binned[group],
                    cfg_group,
                )
                console.print(table)
            return

        GROUP_TITLES = {
            "internal": "Internal sources  [dim](Yale units)[/dim]",
            "external": "External sources  [dim](authorities & linked-data)[/dim]",
            "results": "Result sources",
        }

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
