"""Attach to a running or finished build and show its progress and logs."""

import glob
import os
import re
import time

from lux_pipeline.cli._handler import BaseHandler
from lux_pipeline.cli._view import BuildView

STATUS_RE = re.compile(r"^(?P<prefix>.*)-w(?P<n>\d+)\.status$")


def count_workers(prefix):
    """How many ``<prefix>-w<n>.status`` files there are: the highest n + 1."""
    highest = -1
    for fn in glob.glob(glob.escape(prefix) + "-w*.status"):
        m = STATUS_RE.match(fn)
        if m and m.group("prefix") == prefix:
            highest = max(highest, int(m.group("n")))
    return highest + 1


def latest_prefix(log_dir):
    """The prefix of the most recently updated build under `log_dir`, or None."""
    newest, best = None, -1.0
    for fn in glob.glob(os.path.join(glob.escape(log_dir), "*-w*.status")):
        m = STATUS_RE.match(fn)
        if not m:
            continue
        mtime = os.path.getmtime(fn)
        if mtime > best:
            newest, best = m.group("prefix"), mtime
    return newest


class CommandHandler(BaseHandler):
    # watching needs the files, not the pipeline configuration
    needs_configs = False

    def add_args(self, ap):
        ap.add_argument("prefix", nargs="?", help="Status/log file prefix of the build to watch")
        ap.add_argument("--latest", action="store_true", help="Watch the newest build in log_dir")
        ap.add_argument("--interval", type=float, default=0.5, help="Seconds between file reads")
        ap.add_argument("--workers", type=int, default=0, help="Worker count (default: from the status files)")

    def resolve_prefix(self, args):
        prefix = getattr(args, "prefix", None)
        if prefix:
            return prefix
        if not args.latest:
            raise ValueError("give a prefix (lux watch <log_dir>/<command>-<timestamp>) or --latest")
        cfgs = self.configs
        if cfgs is None:
            from lux_pipeline import runtime
            cfgs = runtime.get_configs(instantiate=False)
        log_dir = getattr(cfgs, "log_dir", None)
        if not log_dir:
            raise ValueError("--latest needs log_dir in the base configuration")
        prefix = latest_prefix(log_dir)
        if prefix is None:
            raise ValueError(f"no *-w*.status files under {log_dir}")
        return prefix

    def process(self, args, rest):
        # the global --prefix doubles as the positional
        global_prefix = getattr(args, "prefix", None)
        args.prefix = None
        super().process(args, rest)
        args.prefix = args.prefix or global_prefix
        prefix = self.resolve_prefix(args)
        workers = args.workers or count_workers(prefix)
        if workers < 1:
            raise ValueError(f"no status files found for {prefix} (expected {prefix}-w0.status ...)")
        view = BuildView(prefix, workers, args.log, interval=args.interval, auto_refresh=False)
        with view:
            try:
                while not view.refresh():
                    time.sleep(args.interval)
            except KeyboardInterrupt:
                pass
        state = "finished" if view.done else "still running"
        print(f"{prefix}: {workers} worker(s), {state}")
