import logging
import threading
from collections import deque
from datetime import datetime
from rich import progress
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.text import Text
from rich.table import Table

logger = logging.getLogger("lux_pipeline")

# If we need images (e.g. debugging graphs), consider
# https://github.com/lnqs/textual-image

LEVEL_COLORS = {
    "DEBUG": "white",
    "INFO": "green",
    "WARNING": "yellow",
    "ERROR": "bright_red",
    "CRITICAL": "bright_magenta",
}


class LogPanel:
    """Display-only view of the most recent log lines. Bounded, so render cost
    and memory stay constant however much is logged over the run; the log of
    record is the per-worker files, not this panel."""

    def __init__(self, max_lines=500):
        self.lines = deque(maxlen=max_lines)
        # appended from the main thread, rendered from Live's refresh thread
        self._lock = threading.Lock()

    def add_line(self, line):
        if isinstance(line, str):
            line = Text(line)
        with self._lock:
            self.lines.append(line)

    def __rich_console__(self, console, options):
        with self._lock:
            lines = list(self.lines)
        height = options.height or options.max_height or len(lines)
        for line in lines[-height:]:
            yield line


class PanelLogHandler(logging.Handler):
    """Renders parent-process log records into the LogPanel."""

    def __init__(self, panel):
        super().__init__()
        self.panel = panel

    def emit(self, record):
        line = Text()
        line.append(datetime.fromtimestamp(record.created).strftime("%H:%M:%S") + " ")
        line.append(record.levelname.ljust(8), style=LEVEL_COLORS.get(record.levelname, "white"))
        line.append(" ")
        line.append(record.getMessage())
        self.panel.add_line(line)


class Header:
    def __rich__(self) -> Panel:
        grid = Table.grid(expand=True)
        grid.add_column(justify="center", ratio=1)
        grid.add_column(justify="right")
        grid.add_row("[b]LUX Data Pipeline", datetime.now().ctime())
        hdr = Panel(grid)
        return hdr


def get_layout(cfgs, max_workers, log_level):
    layout = Layout()
    layout.split_column(
        Layout(name="title", size=3), Layout(name="progress", minimum_size=18), Layout(name="log", minimum_size=8)
    )

    hdr = Header()
    layout["title"].update(hdr)

    l2 = Layout()
    layout["progress"].update(Panel(l2, title="Progress"))
    l2.split_column(*[Layout(name=f"process {x}", size=1) for x in range(max_workers)])

    bars = []
    for x in range(max_workers):
        pbar = progress.Progress(
            "[progress.description]{task.description}",
            progress.BarColumn(),
            "[progress.completed]{task.completed}",
            "/",
            "[progress.total]{task.total}",
            "[progress.percentage]{task.percentage:>3.0f}%",
            progress.TimeRemainingColumn(),
            progress.TimeElapsedColumn(),
            refresh_per_second=4,
        )
        task_id = pbar.add_task(f"[red]task {x}", total=10000)
        l2[f"process {x}"].update(pbar)
        bars.append([pbar, task_id])

    cp = LogPanel()
    layout["log"].update(Panel(cp, title="Log Messages"))
    # remove stream handler
    while logger.handlers:
        logger.removeHandler(logger.handlers[0])

    handler = PanelLogHandler(cp)
    log_level = (log_level or "INFO").upper()
    if log_level not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
        log_level = "INFO"
    handler.setLevel(getattr(logging, log_level))
    logger.addHandler(handler)

    layout._lux_bars = bars
    layout._lux_log_panel = cp
    return layout


# FIXME: This is a nasty hack. Figure someway to manage this better
def get_bar_from_layout(layout, which):
    return layout._lux_bars[which]


def add_worker_log_line(layout, worker, line):
    # line is already formatted (time/level) by the worker's FileHandler
    text = Text()
    text.append(f"w{worker} ", style="cyan")
    text.append(line)
    layout._lux_log_panel.add_line(text)
