"""Attach to a running or finished build and show its progress and logs.

    lux watch <log_dir>/reconcile-2026-11-03-101500    # one run: its workers and its driver
    lux watch <log_dir>/build-2026-11-03-101500        # a lux build: step by step, to the end
    lux watch --latest                                 # the newest run; its build, if it has one

A run is its files under ``log_dir``: the workers' ``-w<n>.status`` and
``-w<n>.log``, the driver's ``<prefix>.log`` and its record
``<prefix>.run``. The view shows a row per worker and tails every log,
the driver's included, so a phase's tail -- reconcile's identify,
metatypes and write-refs after its workers have finished -- is seen as it
runs, and the view ends when the run's record says it is over (or, for a
run with no record, when every worker says done).

A ``lux build`` has no workers of its own: watching it follows its steps
in turn, each as its own run, moving on as the build's record marks one
finished and the next started, until the build ends.
"""

import time

from lux_pipeline.cli._handler import BaseHandler
from lux_pipeline.cli._view import BuildView
# the readers of a run's files live in lux_pipeline.progress; imported here
# too, where they used to be defined
from lux_pipeline.progress import (STATUS_RE, count_workers, current_step, latest_prefix,  # noqa: F401
                                   list_runs, read_run, run_exists, run_state, read_build)


class CommandHandler(BaseHandler):
    # watching needs the files, not the pipeline configuration
    needs_configs = False
    # --prefix is the run to watch, not a run of its own
    uses_prefix = False

    def add_args(self, ap):
        ap.add_argument("prefix", nargs="?", help="Status/log file prefix of the run to watch")
        ap.add_argument("--latest", action="store_true", help="Watch the newest run in log_dir")
        ap.add_argument("--interval", type=float, default=0.5, help="Seconds between file reads")
        ap.add_argument("--workers", type=int, default=0, help="Worker count (default: from the status files)")

    def log_dir(self):
        cfgs = self.configs
        if cfgs is None:
            from lux_pipeline import runtime
            cfgs = runtime.get_configs(instantiate=False)
        log_dir = getattr(cfgs, "log_dir", None)
        if not log_dir:
            raise ValueError("--latest needs log_dir in the base configuration")
        return log_dir

    def resolve_prefix(self, args):
        prefix = getattr(args, "prefix", None)
        if prefix:
            return prefix
        if not args.latest:
            raise ValueError("give a prefix (lux watch <log_dir>/<command>-<timestamp>) or --latest")
        log_dir = self.log_dir()
        # the newest run by its record or files; a phase of a build that
        # is still going hands over to the build, which passes from phase
        # to phase
        runs = list_runs(log_dir)
        if runs:
            newest = runs[0]
            parent = newest.get("parent")
            if parent and run_exists(parent):
                rec = read_run(parent)
                if rec is not None and rec.get("exit") is None:
                    return parent
            return newest["prefix"]
        prefix = latest_prefix(log_dir)
        if prefix is None:
            raise ValueError(f"no *.run or *-w*.status files under {log_dir}")
        return prefix

    # --- one run -----------------------------------------------------------

    def watch_run(self, prefix, args, until=None, title=None):
        """Show `prefix` until its run is over (`BuildView.refresh`), or
        `until()` says so. Returns the view."""
        workers = args.workers or count_workers(prefix)
        view = BuildView(prefix, workers, args.log, interval=args.interval, auto_refresh=False,
                         title=title)
        with view:
            try:
                while not view.refresh():
                    if until is not None and until():
                        break
                    time.sleep(args.interval)
            except KeyboardInterrupt:
                raise
        return view

    # --- a build -----------------------------------------------------------

    def watch_build(self, prefix, args):
        """Follow a ``lux build``: each step as it starts, as its own run,
        until the build's record is finished. Returns the steps followed."""
        followed = []
        seen = 0
        while True:
            record = read_run(prefix) or {}
            steps = record.get("steps") or []
            if len(steps) > seen:
                i = seen
                seen += 1
                step = steps[i]
                sp = step.get("prefix")
                label = f"{step.get('phase')}: {' '.join(step.get('argv') or [])}"
                print(f"=== {label} ===", flush=True)
                followed.append(step)
                if not sp:
                    continue

                def step_over(i=i):
                    rec = read_run(prefix) or {}
                    mine = (rec.get("steps") or [])
                    return i >= len(mine) or mine[i].get("finished") is not None \
                        or rec.get("exit") is not None
                # wait for the step's own files; a step may fail before it
                # writes any, which step_over catches
                while not run_exists(sp) and not step_over():
                    time.sleep(args.interval)
                if run_exists(sp):
                    view = self.watch_run(sp, args, until=step_over, title=label)
                    print(f"{sp}: {view.workers} worker(s), {view.state}", flush=True)
                continue
            if record.get("exit") is not None:
                return followed
            if not record:
                raise ValueError(f"no record at {prefix}.run")
            build = read_build(prefix, 0)
            if run_state(record, build) != "running":
                return followed
            time.sleep(args.interval)

    # --- the command -------------------------------------------------------

    def process(self, args, rest):
        # the global --prefix doubles as the positional
        global_prefix = getattr(args, "prefix", None)
        args.prefix = None
        super().process(args, rest)
        args.prefix = args.prefix or global_prefix
        prefix = self.resolve_prefix(args)
        record = read_run(prefix)
        if record is not None and record.get("phases") is not None:
            try:
                steps = self.watch_build(prefix, args)
            except KeyboardInterrupt:
                print(f"{prefix}: still running")
                return None
            record = read_run(prefix) or {}
            state = run_state(record, read_build(prefix, 0))
            print(f"{prefix}: {len(steps)} step(s), {state}")
            return steps
        if record is None and (args.workers or count_workers(prefix)) < 1:
            raise ValueError(f"no run found at {prefix} (expected {prefix}.run or "
                             f"{prefix}-w0.status ...)")
        try:
            view = self.watch_run(prefix, args)
        except KeyboardInterrupt:
            print(f"{prefix}: still running")
            return None
        print(f"{prefix}: {view.workers} worker(s), {view.state}")
        return view
