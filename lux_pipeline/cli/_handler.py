"""Base classes for `lux` commands.

A command is a module in `lux_pipeline.cli` with a `CommandHandler` class.
`entry.main` builds it with the process's `Config` (or ``None`` for the few
commands that run before an installation exists) and calls
``process(args, rest)``.

`BaseHandler` is for commands that do one thing in this process (`help`,
`counts`, `clear`, ...). `CommandHandler` is for the build phases: it
resolves the sources, builds a manager, and runs it under an engine with
the console UI watching. Items 06 and 07 supply the view and the engines;
until a command overrides `make_manager`, `process` only resolves the
sources and reports them.
"""

import logging
import os
import re
import time
from argparse import ArgumentParser
from datetime import datetime

from lux_pipeline.progress import unfinished_workers

logger = logging.getLogger("lux_pipeline")

# A worker that has not reported done but wrote its status this recently is
# taken to be running; an older one was killed without reporting
RUNNING_WINDOW = 3600
_WORKER_RE = re.compile(r"^(?P<prefix>.*)-w(?P<n>\d+)\.status$")


def default_prefix(cfgs, command):
    """``<log_dir>/<command>-<timestamp>``: where this run's per-worker
    status and log files go, and what `lux watch` needs to find them."""
    log_dir = getattr(cfgs, "log_dir", None) or "."
    started = datetime.now().strftime("%Y-%m-%d-%H%M%S")
    return os.path.join(log_dir, f"{command or 'task'}-{started}")


def refuse_while_building(cfgs, command, force=False, window=RUNNING_WINDOW, now=None):
    """Raise ``RuntimeError`` when a ``lux`` build looks to be running:
    some worker under ``<log_dir>`` has not reported done and updated its
    status within `window` seconds. ``force`` turns the refusal into a
    warning. Older unfinished workers are warned about (probably killed)
    and do not block. Returns the running builds' prefixes.

    For the steps that must not overlap a phase: identify rewrites the map
    from whatever assertions exist, write-refs clears the queue the workers
    fill, and a new token mid-build splits one build across two. Only
    ``lux`` runs write status files: a legacy ``run-*.py`` /
    ``*_parallel.sh`` run cannot be seen, nor one given a ``--prefix``
    outside the log directory.
    """
    now = time.time() if now is None else now
    running, stale = {}, {}
    for fn, st in unfinished_workers(getattr(cfgs, "log_dir", None)):
        m = _WORKER_RE.match(fn)
        prefix = m.group("prefix") if m else fn
        age = now - (st.get("updated") or 0)
        group = running if age <= window else stale
        group.setdefault(prefix, []).append(age)

    def describe(builds):
        return "; ".join(f"{p}: {len(ages)} worker(s) not done, last update "
                         f"{int(min(ages) // 60)} min ago" for p, ages in builds.items())

    if stale:
        logger.warning(f"Unfinished workers with no update for over {window // 60} min, "
                       f"probably killed: {describe(stale)}")
    if running:
        msg = f"a build is running ({describe(running)})"
        if not force:
            raise RuntimeError(f"{msg}; not running {command} during it. "
                               f"Wait for it to finish, or pass --force")
        logger.warning(f"{msg}; running {command} anyway (--force)")
    return list(running)


class BaseHandler:
    # False for a command that does not need the pipeline configuration at
    # all (entry then passes cfgs=None without trying to load one)
    needs_configs = True

    def __init__(self, cfgs):
        self.configs = cfgs
        self.default_source = "all"

    def add_args(self, ap):
        """Add this command's own options to `ap` (an ArgumentParser)."""
        return None

    def resolve_sources(self, args):
        """The source names this run covers, from ``--source``.

        ``all`` is every internal and external source; ``internal``,
        ``external`` and ``results`` are that group; anything else is a
        comma-separated list of names. An unknown name raises ``ValueError``
        naming it.
        """
        cfgs = self.configs
        which = args.source or self.default_source
        if which == "all":
            return [*cfgs.internal.keys(), *cfgs.external.keys()]
        if which in ("internal", "external", "results"):
            return list(getattr(cfgs, which).keys())
        names = [s.strip() for s in which.split(",") if s.strip()]
        known = set(cfgs.internal) | set(cfgs.external) | set(cfgs.results)
        for n in names:
            if n not in known:
                raise ValueError(f"Unknown source: {n}")
        return names

    def process(self, args, rest):
        # Always parse: a command may have options with defaults even when
        # nothing was passed on the command line
        ap = ArgumentParser(prog=f"lux {getattr(args, 'command', '')}".strip())
        self.add_args(ap)
        ap.parse_args(rest, namespace=args)


class CommandHandler(BaseHandler):
    def __init__(self, cfgs):
        super().__init__(cfgs)
        # {manager keyword: args attribute}; managers read their own options
        # from `args` in __init__, this is only for the record
        self.extra_args = {}

    def make_manager(self, workers, args):
        """Build the manager that runs this command's phase over `workers`
        workers. Commands override this; item 07 supplies the base."""
        raise NotImplementedError(f"{type(self).__name__} has no manager yet")

    def make_view(self, prefix, workers, args):
        """The console view for a build, or ``None`` for plain output."""
        from lux_pipeline.cli._view import BuildView
        return BuildView(prefix, workers, getattr(args, "log", "INFO"))

    def process(self, args, rest):
        super().process(args, rest)
        wks = args.max_workers if args.max_workers > 0 else self.configs.max_workers
        sources = self.resolve_sources(args)
        prefix = args.prefix or default_prefix(self.configs, args.command)

        try:
            manager = self.make_manager(wks, args)
        except NotImplementedError:
            # a command whose manager has not been ported yet: report what would run
            print(f"{args.command}: {len(sources)} source(s) over {wks} worker(s), "
                  f"engine {args.engine}: {', '.join(sources)}")
            return None

        for s in sources:
            manager.prepare_single(s)

        view = None if args.no_ui else self.make_view(prefix, wks, args)
        if view is None:
            manager.process(args.engine, prefix, None)
        else:
            if getattr(manager, "hide_idle_workers", False):
                for n in range(len(manager.sources), wks):
                    view.set_visible(n, False)
            with view:
                manager.process(args.engine, prefix, view)
        # so a build can be re-attached with `lux watch`
        print(f"status/log prefix: {prefix}")
        return manager
