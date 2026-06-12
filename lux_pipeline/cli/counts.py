from rich import box
from rich.console import Console
from rich.rule import Rule
from rich.table import Table
from rich.text import Text

from ._handler import BaseHandler as BH

_TYPE_STYLE = {
    "internal": "bold blue",
    "external": "bold magenta",
    "results": "yellow",
}


def _fmt(n: int, exact: bool) -> str:
    """Format a count with thousand-separators and an exact/estimate prefix."""
    return f"{'=' if exact else '~'}{n:,}"


def _measure_source_table_widths(
    sections: list[tuple[list[tuple[str, str, str, int, bool | None]]]],
) -> tuple[int, int, int, int]:
    """Return shared min-widths for (Source, Type, col2, Count) across multiple
    same-structure sections so their tables line up.
    Headers provide the floor for each column."""
    w_source = len("Source")
    w_type = len("Type")
    w_col2 = 0  # floor set per-table from the column header
    w_count = len("Count")
    for (rows,) in sections:
        for source, src_type, col2, n, exact in rows:
            w_source = max(w_source, len(source))
            w_type = max(w_type, len(src_type))
            w_col2 = max(w_col2, len(col2))
            w_count = max(w_count, len(_fmt(n, bool(exact))))
    return w_source, w_type, w_col2, w_count


def _source_table(
    title: str,
    col2_header: str,
    rows: list[tuple[str, str, str, int, bool | None]],
    section_total: int,
    col_widths: tuple[int, int, int, int],
) -> Table:
    """Build a Source | Type | <col2> | Count table with a total footer row.

    rows: (source, src_type, col2_value, count, exact_or_None)
    """
    w_source, w_type, w_col2, w_count = col_widths
    w_col2 = max(w_col2, len(col2_header))  # ensure the header always fits
    table = Table(
        title=title,
        box=box.ROUNDED,
        title_justify="left",
        header_style="bold",
        show_lines=False,
        expand=False,
        padding=(0, 1),
    )
    table.add_column("Source", style="bold cyan", no_wrap=True, min_width=w_source)
    table.add_column("Type", no_wrap=True, min_width=w_type)
    table.add_column(col2_header, style="green", no_wrap=True, min_width=w_col2)
    table.add_column(
        "Count", justify="right", style="yellow", no_wrap=True, min_width=w_count
    )

    for source, src_type, col2, n, exact in rows:
        type_cell = Text(src_type, style=_TYPE_STYLE.get(src_type, ""))
        table.add_row(source, type_cell, col2, _fmt(n, bool(exact)))

    table.add_section()
    table.add_row(
        Text("Total", style="bold"),
        "",
        "",
        Text(f"~{section_total:,}", style="bold yellow"),
    )
    return table


def _maps_table(
    rows: list[tuple[str, int]],
    section_total: int,
) -> Table:
    """Build the maps Name | Count table."""
    w_name = max(len("Map"), max((len(name) for name, _ in rows), default=0))
    w_count = max(len("Count"), max((len(f"={n:,}") for _, n in rows), default=0))

    table = Table(
        title="Map counts",
        box=box.ROUNDED,
        title_justify="left",
        header_style="bold",
        show_lines=False,
        expand=False,
        padding=(0, 1),
    )
    table.add_column("Map", style="bold cyan", no_wrap=True, min_width=w_name)
    table.add_column(
        "Count", justify="right", style="yellow", no_wrap=True, min_width=w_count
    )

    for name, n in rows:
        table.add_row(name, f"={n:,}")

    table.add_section()
    table.add_row(
        Text("Total", style="bold"),
        Text(f"~{section_total:,}", style="bold yellow"),
    )
    return table


class CommandHandler(BH):
    def add_args(self, ap):
        ap.add_argument(
            "--cache", type=str, help="Types of cache separated by commas, or 'all'"
        )
        ap.add_argument(
            "--map", type=str, help="Types of map separated by commas, or 'all'"
        )
        ap.add_argument(
            "--index", type=str, help="Index sources separated by commas, or 'all'"
        )

    def process(self, args, rest):
        super().process(args, rest)
        cfgs = self.configs
        console = Console()

        # --- resolve maps ---
        if not hasattr(args, "map") or not args.map:
            maps = []
        elif args.map == "all":
            maps = list(cfgs.map_stores.keys())
        else:
            maps = args.map.split(",")

        # --- resolve indexes ---
        indexes = []
        if not hasattr(args, "index") or not args.index:
            pass
        elif args.index == "all":
            for group, cfg_list in [
                ("internal", cfgs.internal),
                ("external", cfgs.external),
                ("results", cfgs.results),
            ]:
                for cfg in cfg_list.values():
                    if "indexes" in cfg:
                        for idx in cfg["indexes"].values():
                            idx["source"] = cfg["name"]
                            idx["src_type"] = group
                            indexes.append(idx)
        else:
            for s in args.index.split(","):
                if s in cfgs.internal:
                    cfg = cfgs.internal[s]
                    src_type = "internal"
                elif s in cfgs.external:
                    cfg = cfgs.external[s]
                    src_type = "external"
                elif s in cfgs.results:
                    cfg = cfgs.results[s]
                    src_type = "results"
                else:
                    raise ValueError(f"Unknown index source: {s}")
                if "indexes" in cfg:
                    for idx in cfg["indexes"].values():
                        idx["source"] = cfg["name"]
                        idx["src_type"] = src_type
                        indexes.append(idx)

        # --- resolve sources ---
        if not args.source and (maps or indexes):
            sources = []
        elif not args.source or args.source == "all":
            sources = [
                *cfgs.internal.keys(),
                *cfgs.external.keys(),
                *cfgs.results.keys(),
            ]
        elif args.source in ["internal", "external", "results"]:
            sources = list(getattr(cfgs, args.source).keys())
        else:
            sources = args.source.split(",")

        # --- resolve cache types ---
        if not hasattr(args, "cache") or not args.cache:
            caches = [
                "datacache",
                "recordcache",
                "reconciledRecordcache",
                "recordcache2",
            ]
        else:
            caches = args.cache.split(",")

        # ------------------------------------------------------------------
        # Collect cache rows
        # ------------------------------------------------------------------
        cache_rows: list[tuple[str, str, str, int, bool]] = []
        cache_total = 0

        for s in sources:
            if s in cfgs.internal:
                cfg = cfgs.internal[s]
                src_type = "internal"
            elif s in cfgs.external:
                cfg = cfgs.external[s]
                src_type = "external"
            elif s in cfgs.results:
                cfg = cfgs.results[s]
                src_type = "results"
            else:
                console.print(f"[bold red]Unknown source:[/bold red] {s}")
                continue

            for c in caches:
                cache = cfg.get(c, None)
                if cache is not None:
                    est = cache.len_estimate()
                    if est < 100_000:
                        est = len(cache)
                        exact = True
                    else:
                        exact = False
                    cache_rows.append((s, src_type, c, est, exact))
                    cache_total += est

        # ------------------------------------------------------------------
        # Collect map rows
        # ------------------------------------------------------------------
        map_rows: list[tuple[str, int]] = []
        map_total = 0

        for m in maps:
            if m in cfgs.map_stores:
                cfg = cfgs.map_stores[m]
                store = cfg.get("store", None)
                if store is None:
                    store = cfgs.instantiate_map(m)["store"]
                n = len(store)
                map_rows.append((m, n))
                map_total += n

        # ------------------------------------------------------------------
        # Collect index rows
        # ------------------------------------------------------------------
        index_rows: list[tuple[str, str, str, int, None]] = []
        index_total = 0

        for idx in indexes:
            idx["index"].open()
            n = len(idx["index"])
            index_rows.append(
                (idx["source"], idx.get("src_type", ""), idx["name"], n, None)
            )
            index_total += n

        # ------------------------------------------------------------------
        # Render — share column widths between the two tricolumn sections
        # ------------------------------------------------------------------
        sections_shown = 0
        grand_total = cache_total + map_total + index_total

        # Measure shared widths for cache + index tables (same shape)
        tri_sections = []
        if cache_rows:
            tri_sections.append((cache_rows,))
        if index_rows:
            tri_sections.append((index_rows,))
        col_widths = (
            _measure_source_table_widths(tri_sections) if tri_sections else (6, 8, 5, 5)
        )

        if cache_rows:
            console.print(
                _source_table(
                    "Cache counts", "Cache", cache_rows, cache_total, col_widths
                )
            )
            sections_shown += 1

        if map_rows:
            if sections_shown:
                console.print()
            console.print(_maps_table(map_rows, map_total))
            sections_shown += 1

        if index_rows:
            if sections_shown:
                console.print()
            console.print(
                _source_table(
                    "Index counts", "Index", index_rows, index_total, col_widths
                )
            )
            sections_shown += 1

        if sections_shown > 1:
            console.print()
            console.print(Rule(f"[bold]Grand total: ~{grand_total:,}[/bold]"))
