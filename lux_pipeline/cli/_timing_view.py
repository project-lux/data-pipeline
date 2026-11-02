"""Rich rendering of `lux timing-report`.

The numbers come from `lux_pipeline.process.timing_report` (`directory_data`,
`compare_data`, `slice_rates`, `nested_overrun`), which also renders them as
plain text for ``--no-ui`` and the root ``timing-report.py``; this only
lays them out: a panel per phase with its summary, a stage table with a bar
for each stage's share of worker time, the nested stages, the slices, and
the profile tables and stage trees.
"""

import os
import sys

from rich import box
from rich.bar import Bar
from rich.console import Console, Group
from rich.panel import Panel
from rich.rule import Rule
from rich.table import Table
from rich.text import Text
from rich.tree import Tree

from lux_pipeline.process.timing_report import (
    METRIC_FORMATS, NO_PROFILES, PROFILE_TITLES, nested_overrun, slice_rates)

BAR_WIDTH = 20


def make_console(file=None):
    return Console(file=file or sys.stdout, highlight=False)


# --- pieces -----------------------------------------------------------------


def _bar(percent, color):
    """A `BAR_WIDTH` bar filled to `percent` (clamped to 0..100)."""
    return Bar(size=100, begin=0, end=max(0.0, min(100.0, percent)), width=BAR_WIDTH,
               color=color, bgcolor="grey23")


def _cpu_color(percent):
    return "green" if percent >= 80 else "yellow" if percent >= 50 else "red"


def func_text(func):
    """A pstats key as the function's name, then its file and line, dimmed."""
    filename, line, name = func
    if filename == "~":
        return Text(name, style="bold")     # a builtin: "<method 'execute' ...>"
    parts = filename.split(os.sep)
    short = os.sep.join(parts[-2:]) if len(parts) > 1 else filename
    return Text.assemble((name, "bold"), (f"  {short}:{line}", "dim"))


def _table(title=None, **kw):
    return Table(title=title, title_justify="left", title_style="bold", box=box.SIMPLE_HEAD,
                 header_style="bold", pad_edge=False, show_edge=False, **kw)


def _per_call(secs, calls):
    return f"{secs / calls * 1e6:,.1f}" if calls else "-"


# --- one phase --------------------------------------------------------------


def summary_grid(s):
    cpu = s["cpu_percent"]
    cpu_cell = Table.grid(padding=(0, 1))
    cpu_cell.add_row(Text(f"{cpu:.0f}%", style=f"bold {_cpu_color(cpu)}"),
                     _bar(cpu, _cpu_color(cpu)))
    grid = Table.grid(padding=(0, 2))
    for _ in range(6):
        grid.add_column()
    label = "dim"
    records = Text(f"{s['records']:,}", style="bold")
    if s["skipped"]:
        records.append(f"  ({s['skipped']:,} skipped)", style="yellow")
    grid.add_row(Text("records", label), records,
                 Text("wall", label), Text(f"{s['wall'] / 60:,.1f} min", style="bold"),
                 Text("rate", label), Text(f"{s['rate']:,.0f}/s", style="bold"))
    grid.add_row(Text("worker-hours", label), f"{s['worker_seconds'] / 3600:,.1f}",
                 Text("cpu-hours", label), f"{s['cpu_seconds'] / 3600:,.1f}",
                 Text("cpu", label), cpu_cell)
    return grid


def stage_table(s):
    ws = s["worker_seconds"]
    t = _table("Stages", show_footer=False)
    t.add_column("stage", style="cyan", no_wrap=True)
    t.add_column("worker-hrs", justify="right")
    t.add_column("% worker", justify="right")
    t.add_column("", no_wrap=True)
    t.add_column("calls", justify="right")
    t.add_column("µs/call", justify="right")
    for name, (calls, secs) in sorted(s["top_stages"].items(), key=lambda kv: -kv[1][1]):
        pct = secs / ws * 100 if ws else 0
        t.add_row(name, f"{secs / 3600:,.2f}", f"{pct:.1f}%", _bar(pct, "cyan"),
                  f"{calls:,}", _per_call(secs, calls))
    un = s["unattributed"]
    pct = un / ws * 100 if ws else 0
    t.add_section()
    t.add_row(Text("(unattributed)", style="italic dim"), Text(f"{un / 3600:,.2f}", "dim"),
              Text(f"{pct:.1f}%", "dim"), _bar(pct, "grey50"), "", "")
    return t


def nested_tables(s):
    """A heading and a table per parent (these ran inside it and are not
    additional), preceded by a blank line."""
    out = []
    for parent, group in s["nested_stages"].items():
        heading = Text.assemble((f"inside {parent}", "bold"),
                                ("  already counted above, not additional", "dim italic"))
        t = _table()
        t.add_column("stage", style="cyan dim", no_wrap=True)
        t.add_column("worker-hrs", justify="right")
        t.add_column("calls", justify="right")
        t.add_column("µs/call", justify="right")
        for name, (calls, secs) in sorted(group.items(), key=lambda kv: -kv[1][1]):
            t.add_row(name, f"{secs / 3600:,.2f}", f"{calls:,}", _per_call(secs, calls))
        out += [Text(""), heading, t]
        over = nested_overrun(s, parent)
        if over:
            ptotal = s["stages"][parent][1]
            out.append(Text(f"...totalling {(ptotal + over) / 3600:.2f} worker-hrs against "
                            f"{parent}'s {ptotal / 3600:.2f}, so {over / 3600:.2f} of it runs "
                            f"inside other stages -- not a breakdown of {parent}",
                            style="yellow"))
    return out


def slices_text(s):
    sr = slice_rates(s)
    if sr is None:
        return None
    t = Text.assemble(("slices  ", "bold"),
                      f"{sr['lowest']:,.0f} to {sr['highest']:,.0f}/s",
                      ("  median ", "dim"), f"{sr['median']:,.0f}",
                      ("  spread ", "dim"), f"{sr['spread']:.0f}%")
    if sr["slow"]:
        t.append(f"\nslow (>20% under median): {', '.join(map(str, sr['slow']))}", style="red")
    elif sr["even"]:
        t.append("\nevenly spread -- no straggler, so the work is partitioned well",
                 style="green")
    if sr["fast"]:
        t.append(f"\nfast (>20% over median): {', '.join(map(str, sr['fast']))}",
                 style="yellow")
    return t


def phase_panel(phase, s):
    parts = [summary_grid(s)]
    idle = 100 - s["cpu_percent"]
    if idle > 20:
        parts.append(Text(f"workers are off-cpu {idle:.0f}% of the time -- the stages "
                          f"say what they are waiting in", style="yellow"))
    if s["stages"]:
        parts += [Text(""), stage_table(s), *nested_tables(s)]
    st = slices_text(s)
    if st is not None:
        parts += [Text(""), st]
    marks = s["runs"][0].get("marks") or []
    if marks:
        parts.append(Text.assemble((f"source boundaries (slice {s['runs'][0]['slice']})  ", "dim"),
                                   ", ".join(f"{m[0]} at {m[1] / 60:.0f}m" for m in marks)))
    finished = s["complete"] == s["slices"]
    status = Text(f"{s['complete']}/{s['slices']} finished",
                  style="green" if finished else "yellow")
    if not finished:
        status.append(" · running or stopped", style="yellow")
    return Panel(Group(*parts), title=Text(phase, style="bold cyan"), title_align="left",
                 subtitle=status, subtitle_align="right", border_style="cyan",
                 padding=(0, 1))


# --- profiles ---------------------------------------------------------------


def profile_panel(phase, p):
    parts = []
    for key, title in PROFILE_TITLES.items():
        t = _table(f"Top {p['n']} by {title}")
        t.add_column("calls", justify="right")
        t.add_column("own s", justify="right", style="magenta" if key == "tottime" else "")
        t.add_column("cum s", justify="right", style="magenta" if key == "cumtime" else "")
        t.add_column("µs/call", justify="right")
        t.add_column("function", overflow="fold")
        for f, nc, tt, ct in p["top"][key]:
            per = (ct if key == "cumtime" else tt) / nc * 1e6 if nc else 0
            t.add_row(f"{nc:,}", f"{tt:,.1f}", f"{ct:,.1f}", f"{per:,.1f}", func_text(f))
        if parts:
            parts.append(Text(""))
        parts.append(t)
    if p["matched"]:
        tree = Tree(Text("Stages matched to a function of the same name (the largest one)",
                         style="bold"), guide_style="dim")
        for st, f, ct, callees in p["matched"]:
            branch = tree.add(Text.assemble((st, "cyan"), ": ", func_text(f),
                                            (f"  {ct:,.1f}s cumulative", "magenta")))
            for g, share, cct in callees:
                branch.add(Text.assemble((f"{share:5.1f}%", "bold"), f"  {cct:,.1f}s  ",
                                         func_text(g)))
        parts += [Text(""), tree]
    sub = (f"{p['slices']} slice{'s' if p['slices'] != 1 else ''} · {p['calls']:,} calls · "
           f"{p['cpu_seconds'] / 3600:.2f} profiled cpu-hours")
    return Panel(Group(*parts), title=Text(f"{phase} profile", style="bold magenta"),
                 title_align="left", subtitle=Text(sub, style="dim"), subtitle_align="right",
                 border_style="magenta", padding=(0, 1))


# --- whole reports ----------------------------------------------------------


def render_directory(console, directory, phases, no_profiles):
    console.print(Rule(Text.assemble(("timing report  ", "bold"), (directory, "dim")),
                       align="left", style="dim"))
    if no_profiles:
        console.print(Text(NO_PROFILES, style="yellow"))
    for ph, s, prof in phases:
        console.print(phase_panel(ph, s))
        if prof is not None:
            console.print(profile_panel(ph, prof))


# for these a lower number is better; for records/s a higher one
LOWER_IS_BETTER = {"wall minutes"}
HIGHER_IS_BETTER = {"records/s"}


def _change(chg, lower_better=None):
    """A signed percentage, green when better, red when worse, dim when
    under 2% or when neither direction is better."""
    text = f"{chg:+.1f}%"
    if lower_better is None or abs(chg) < 2:
        return Text(text, style="dim")
    better = chg < 0 if lower_better else chg > 0
    return Text(text, style="green" if better else "red")


def render_compare(console, a_dir, b_dir, phases):
    console.print(Rule(Text.assemble(("timing comparison  ", "bold"), (a_dir, "dim"),
                                     ("  →  ", "bold"), (b_dir, "dim")),
                       align="left", style="dim"))
    if not phases:
        console.print(Text(f"no timing json in either {a_dir} or {b_dir}", style="yellow"))
    for c in phases:
        if c["only_in"]:
            console.print(Text.assemble((c["phase"], "bold cyan"),
                                        (f": only in {c['only_in']}", "dim")))
            continue
        m = _table()
        m.add_column("", style="dim")
        m.add_column("before", justify="right")
        m.add_column("after", justify="right", style="bold")
        m.add_column("change", justify="right")
        for label, ka, kb, chg in c["metrics"]:
            fmt = METRIC_FORMATS[label]
            lower = True if label in LOWER_IS_BETTER else False if label in HIGHER_IS_BETTER else None
            m.add_row(label, fmt.format(ka), fmt.format(kb), _change(chg, lower))
        st = _table("Stages")
        st.add_column("stage", style="cyan", no_wrap=True)
        st.add_column("before hrs", justify="right")
        st.add_column("after hrs", justify="right", style="bold")
        st.add_column("change", justify="right")
        for name, ha, hb, chg in c["stages"]:
            st.add_row(name, f"{ha:.2f}", f"{hb:.2f}",
                       Text("new", style="magenta") if chg is None else _change(chg, True))
        console.print(Panel(Group(m, Text(""), st), title=Text(c["phase"], style="bold cyan"),
                            title_align="left", border_style="cyan", padding=(0, 1)))
