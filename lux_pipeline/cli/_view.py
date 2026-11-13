"""Console view of a build, rendered from the workers' status and log files.

`BuildView` is the only place the pipeline touches Rich. It reads the
``w<n>.status`` files in the run's directory that
`lux_pipeline.progress.StatusWriter` writes and tails the ``w<n>.log``
files the workers log to, and the driver's own ``driver.log``; it never holds a reference to a worker,
and no worker knows it exists. That is what lets `lux watch` attach to any
build -- one started by a `lux` engine, by a ``*_parallel.sh`` loop, or by
hand -- while it runs or after it finished.

Screen: a header (title, prefix, clock); one `rich.progress.Progress` with a
task per worker plus an "all workers" task; and a bounded panel of log lines
holding this process's own log records and the tailed lines, prefixed
``w<n>`` for a worker and ``driver`` for the driver.

A run is over when its record (``run.json``) says so; a run with no
record -- started slice by slice from a shell -- is over when every
worker says done. The difference is the driver's tail: reconcile's
identify, metatypes and write-refs run after the last worker finishes,
and the view stays on them.
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

# LogTailer lives with the other readers of a run's files; imported here
# too, where it used to be defined
from lux_pipeline.progress import DRIVER, LogTailer, count_workers, read_build, read_run, run_state

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
    def __init__(self, prefix, title=None):
        self.prefix = prefix
        self.title = title

    def __rich__(self):
        grid = Table.grid(expand=True)
        grid.add_column(justify="left", ratio=1)
        grid.add_column(justify="center", ratio=1)
        grid.add_column(justify="right")
        middle = self.prefix or ""
        if self.title:
            middle = f"{self.title}\n{middle}"
        grid.add_row("[b]LUX Data Pipeline", middle, datetime.now().strftime("%H:%M:%S"))
        return Panel(grid)


def clamp_level(name):
    name = (name or "INFO").upper()
    return name if name in LEVEL_NAMES else "INFO"


class BuildView:
    """Live console display of one build, read from its files.

    ``with view:`` starts the display, routes the ``lux_pipeline`` logger
    into the log panel (restoring the previous handlers on exit; a file
    handler -- the driver's ``driver.log`` -- stays, since the file is the
    log of record and the panel is only a display) and, unless
    ``auto_refresh`` is off, polls the files every ``interval`` seconds. An
    engine or `lux watch` may also call `refresh` itself.

    `workers` is how many worker rows to start with; a status file for a
    worker beyond them adds a row, so a run can be watched from before its
    workers have written anything (0), and a one-shot run has no rows.
    """

    def __init__(self, prefix, workers, log_level="INFO", max_log_lines=500,
                 console=None, interval=0.5, auto_refresh=True, title=None):
        self.prefix = prefix
        self.workers = workers
        self.console = console or Console()
        self.interval = interval
        self.auto_refresh = auto_refresh
        self.log_panel = LogPanel(max_log_lines)
        self.header = Header(prefix, title)
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
        # the run's state as of the last refresh (lux_pipeline.progress.run_state)
        self.state = "running"
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
        if worker == DRIVER:
            text.append("driver ", style="magenta")
        else:
            text.append(f"w{worker} ", style="cyan")
        text.append(line)
        self.log_panel.add_line(text)

    def set_visible(self, worker, visible):
        """Hide a worker that has nothing to do (fewer sources than workers)."""
        self.progress.update(self.tasks[worker], visible=visible)

    def _grow(self, workers):
        """Rows for workers that have appeared since the view was built."""
        for n in range(self.workers, workers):
            self.tasks[n] = self.progress.add_task(f"w{n} waiting", total=None, start=False)
        self.workers = workers
        self.tailer.workers = workers
        self.progress.update(self.all_task, description=f"all {workers} workers")

    # ----------------------------------------------------------- the files

    def refresh(self):
        """Read every worker's status file and tail the logs. Returns True
        when the run is over: its record finished, or, with no record,
        every worker reported ``done``."""
        with self._lock:
            return self._refresh()

    def _refresh(self):
        # the files are read by lux_pipeline.progress.read_build, which every
        # view of a run shares; this only puts the numbers on the screen
        found = count_workers(self.prefix)
        if found > self.workers:
            self._grow(found)
        visible = set()
        for n, tid in self.tasks.items():
            task = self._task(tid)
            if task is not None and task.visible:
                visible.add(n)
        build = read_build(self.prefix, self.workers, visible)
        for n, tid in self.tasks.items():
            st = build["workers"][n]
            if st is None:
                continue
            self.status[n] = st
            self.progress.update(tid, completed=st.get("completed") or 0,
                                 total=st.get("total"), description=f"w{n} {build['labels'][n]}")
            task = self._task(tid)
            if task is not None and not task.started:
                self.progress.start_task(tid)
        self.progress.update(self.all_task, completed=build["completed"], total=build["total"])
        for n, line in self.tailer.read_new_lines():
            self.add_worker_line(n, line)
        record = read_run(self.prefix)
        if record is None:
            self.state = "finished" if build["done"] else "running"
            self.done = build["done"]
        else:
            self.state = run_state(record, build)
            self.done = self.state != "running"
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
            # the driver's log file stays: it is the log of record, and
            # what another watcher of this run tails
            if not isinstance(h, logging.FileHandler):
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
