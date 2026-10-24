"""Console view of a build, rendered from the workers' status and log files.

`BuildView` is the only place the pipeline touches Rich. It reads the
``<prefix>-w<n>.status`` files that `lux_pipeline.progress.StatusWriter`
writes and tails the ``<prefix>-w<n>.log`` files the workers log to; it never
holds a reference to a worker, and no worker knows it exists. That is what
lets `lux watch` attach to any build -- one started by a `lux` engine, by a
``*_parallel.sh`` loop, or by hand -- while it runs or after it finished.

Screen: a header (title, prefix, clock); one `rich.progress.Progress` with a
task per worker plus an "all workers" task; and a bounded panel of log lines
holding this process's own log records and the tailed worker lines, prefixed
``w<n>``.
"""

import logging
import threading
import time
from collections import deque
from datetime import datetime

from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn, MofNCompleteColumn, Progress, TextColumn, TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table
from rich.text import Text

from lux_pipeline.progress import log_path, read_status, status_path

logger = logging.getLogger("lux_pipeline")

LEVEL_COLORS = {
    "DEBUG": "white",
    "INFO": "green",
    "WARNING": "yellow",
    "ERROR": "bright_red",
    "CRITICAL": "bright_magenta",
}
LEVEL_NAMES = tuple(LEVEL_COLORS)

# header (3) + progress rows + log panel worth showing (8)
MIN_LOG_ROWS = 8


class LogPanel:
    """Display-only view of the most recent log lines. Bounded, so render
    cost and memory stay constant however much is logged over the run; the
    log of record is the per-worker files, not this panel."""

    def __init__(self, max_lines=500):
        self.lines = deque(maxlen=max_lines)
        # appended from the main thread, rendered from Live's refresh thread
        self._lock = threading.Lock()

    def add_line(self, line):
        if isinstance(line, str):
            line = Text(line)
        with self._lock:
            self.lines.append(line)

    def snapshot(self):
        """The current lines as plain strings (for tests and summaries)."""
        with self._lock:
            return [l.plain for l in self.lines]

    def __rich_console__(self, console, options):
        with self._lock:
            lines = list(self.lines)
        height = options.height or options.max_height or len(lines)
        for line in lines[-height:]:
            yield line


class PanelLogHandler(logging.Handler):
    """Renders this process's log records into the LogPanel."""

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
    def __init__(self, prefix):
        self.prefix = prefix

    def __rich__(self):
        grid = Table.grid(expand=True)
        grid.add_column(justify="left", ratio=1)
        grid.add_column(justify="center", ratio=1)
        grid.add_column(justify="right")
        grid.add_row("[b]LUX Data Pipeline", self.prefix or "", datetime.now().strftime("%H:%M:%S"))
        return Panel(grid)


class LogTailer:
    """Incrementally reads complete new lines from each worker's log file."""

    def __init__(self, prefix, workers):
        self.prefix = prefix
        self.workers = workers
        self.offsets = {}

    def read_new_lines(self):
        """``[(worker, line)]`` for every complete line written since the
        last call. A worker may be mid-write, so only data up to the last
        newline is consumed."""
        if not self.prefix:
            return []
        lines = []
        for n in range(self.workers):
            try:
                with open(log_path(self.prefix, n), "rb") as fh:
                    fh.seek(self.offsets.get(n, 0))
                    data = fh.read()
            except OSError:
                continue
            nl = data.rfind(b"\n")
            if nl == -1:
                continue
            self.offsets[n] = self.offsets.get(n, 0) + nl + 1
            for line in data[:nl].split(b"\n"):
                lines.append((n, line.decode("utf-8", errors="replace")))
        return lines


def clamp_level(name):
    name = (name or "INFO").upper()
    return name if name in LEVEL_NAMES else "INFO"


class BuildView:
    """Live console display of one build, read from its files.

    ``with view:`` starts the display, routes the ``lux_pipeline`` logger
    into the log panel (restoring the previous handlers on exit) and, unless
    ``auto_refresh`` is off, polls the files every ``interval`` seconds. An
    engine or `lux watch` may also call `refresh` itself.
    """

    def __init__(self, prefix, workers, log_level="INFO", max_log_lines=500,
                 console=None, interval=0.5, auto_refresh=True):
        self.prefix = prefix
        self.workers = workers
        self.console = console or Console()
        self.interval = interval
        self.auto_refresh = auto_refresh
        self.log_panel = LogPanel(max_log_lines)
        self.header = Header(prefix)
        self.progress = Progress(
            TextColumn("{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TextColumn("{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=self.console,
        )
        # worker -> TaskID; tasks start when their first status arrives so
        # the elapsed column measures the worker, not the wait for it
        self.tasks = {}
        for n in range(workers):
            self.tasks[n] = self.progress.add_task(f"w{n} waiting", total=None, start=False)
        self.all_task = self.progress.add_task(f"all {workers} workers", total=None)
        self.tailer = LogTailer(prefix, workers)
        self.handler = PanelLogHandler(self.log_panel)
        self.level = getattr(logging, clamp_level(log_level))
        self.handler.setLevel(self.level)
        self.done = False
        self.status = {}
        self.live = None
        self._saved = None
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None

    # ----------------------------------------------------------- rendering

    def renderable(self):
        return Group(self.header, self.progress, Panel(self.log_panel, title="Log"))

    def _task(self, task_id):
        for t in self.progress.tasks:
            if t.id == task_id:
                return t
        return None

    def add_worker_line(self, worker, line):
        # already formatted (time/level) by the worker's file handler
        text = Text()
        text.append(f"w{worker} ", style="cyan")
        text.append(line)
        self.log_panel.add_line(text)

    def set_visible(self, worker, visible):
        """Hide a worker that has nothing to do (fewer sources than workers)."""
        self.progress.update(self.tasks[worker], visible=visible)

    # ----------------------------------------------------------- the files

    def refresh(self):
        """Read every worker's status file and tail its log. Returns True
        when every worker has reported ``done``."""
        with self._lock:
            return self._refresh()

    def _refresh(self):
        all_done = True
        seen = 0
        for n, tid in self.tasks.items():
            st = read_status(status_path(self.prefix, n))
            if st is None:
                all_done = False
                continue
            seen += 1
            self.status[n] = st
            label = st.get("description") or st.get("phase") or "running"
            self.progress.update(tid, completed=st.get("completed") or 0,
                                 total=st.get("total"), description=f"w{n} {label}")
            task = self._task(tid)
            if task is not None and not task.started:
                self.progress.start_task(tid)
            if not st.get("done"):
                all_done = False
        workers = [self._task(tid) for tid in self.tasks.values()]
        workers = [t for t in workers if t is not None and t.visible]
        completed = sum(t.completed for t in workers)
        totals = [t.total for t in workers]
        total = sum(totals) if workers and all(t is not None for t in totals) else None
        self.progress.update(self.all_task, completed=completed, total=total)
        for n, line in self.tailer.read_new_lines():
            self.add_worker_line(n, line)
        self.done = seen > 0 and all_done
        return self.done

    # ----------------------------------------------------------- lifecycle

    def _check_height(self):
        need = 3 + len(self.tasks) + 1 + MIN_LOG_ROWS
        height = self.console.size.height
        if height and height < need:
            self.console.print(
                f"[yellow]terminal is {height} rows; {need} would show every worker. "
                f"Rich will clip -- use --no-ui, or 'lux watch {self.prefix}' from a taller terminal.[/]"
            )

    def _poll(self):
        while not self._stop.wait(self.interval):
            try:
                if self.refresh():
                    return
            except Exception as e:  # the view must never kill the build
                logger.debug(f"view refresh failed: {e}")

    def __enter__(self):
        self._saved = (list(logger.handlers), logger.level)
        for h in list(logger.handlers):
            logger.removeHandler(h)
        logger.addHandler(self.handler)
        if logger.level == logging.NOTSET or logger.level > self.level:
            logger.setLevel(self.level)
        self._check_height()
        self.live = Live(self.renderable(), console=self.console, refresh_per_second=4, screen=False)
        self.live.__enter__()
        if self.auto_refresh:
            self._stop.clear()
            self._thread = threading.Thread(target=self._poll, name="lux-view", daemon=True)
            self._thread.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._thread is not None:
            self._stop.set()
            self._thread.join(timeout=self.interval * 4)
            self._thread = None
        try:
            self.refresh()        # the final state, whatever the throttle dropped
        except Exception:
            pass
        if self.live is not None:
            self.live.__exit__(exc_type, exc, tb)
            self.live = None
        logger.removeHandler(self.handler)
        if self._saved is not None:
            handlers, level = self._saved
            for h in handlers:
                if h not in logger.handlers:
                    logger.addHandler(h)
            logger.setLevel(level)
            self._saved = None
        return False
