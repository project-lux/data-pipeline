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
import subprocess
import sys
import time
from argparse import ArgumentParser
from datetime import datetime

# RUNNING_WINDOW: a worker that has not reported done but wrote its status
# this recently is taken to be running; an older one was killed without
# reporting. Defined with the other readers of a run's files.
from lux_pipeline.log import send_timing_to, timing_logger
from lux_pipeline.progress import (RUNNING_WINDOW, RunRecord, count_workers, driver_log_path,
                                   driver_timing_path, ensure_run_dir, read_run, run_exists,
                                   status_file_run, stdout_path, unfinished_workers)

logger = logging.getLogger("lux_pipeline")

# set by `lux build` in each phase's environment: the build's own prefix
PARENT_RUN_ENV = "LUX_PARENT_RUN"
# set by whoever starts a command to add fields to its run record, as JSON
# (the web UI's runner: {"web": true, "stdout": "<prefix>/stdout.log"})
RUN_EXTRA_ENV = "LUX_RUN_EXTRA"


def run_extra():
    """The fields `RUN_EXTRA_ENV` asks for, or ``{}`` (also for a value that
    is not a JSON object: a record must never fail a command)."""
    import json
    try:
        extra = json.loads(os.environ.get(RUN_EXTRA_ENV) or "{}")
    except ValueError:
        return {}
    return extra if isinstance(extra, dict) else {}


def default_prefix(cfgs, command):
    """``<log_dir>/<command>-<timestamp>``: the run's directory, where its
    record, the driver's log and every worker's status and log files go,
    and what `lux watch` needs to find them."""
    log_dir = getattr(cfgs, "log_dir", None) or "."
    started = datetime.now().strftime("%Y-%m-%d-%H%M%S")
    return os.path.join(log_dir, f"{command or 'task'}-{started}")


def free_prefix(cfgs, command, taken=()):
    """`default_prefix`, with ``-2``, ``-3``... added while a run (or the
    output of one) is already there, or the name is in `taken`: two runs
    started in the same second never share a directory."""
    prefix = default_prefix(cfgs, command)
    base, n = prefix, 1
    while os.path.exists(prefix) or run_exists(prefix) or os.path.exists(out_path(prefix)) \
            or prefix in taken:
        n += 1
        prefix = f"{base}-{n}"
    return prefix


def out_path(prefix):
    """``<prefix>/stdout.log``: the output of a run started detached (`lux
    --background`, the web UI); `progress.stdout_path`."""
    return stdout_path(prefix)


def start_detached(argv, out, env=None, cwd=None):
    """Start `argv` as a process that outlives this one, as ``nohup ... &``
    would: in a session of its own (no controlling terminal, so closing
    the terminal sends it no SIGHUP, and its pid is its process group, so
    ``kill -- -<pid>`` stops its workers too), stdin from ``/dev/null``,
    stdout and stderr appended to `out` (its directory made: a run's
    output is the first of its files). Returns the `subprocess.Popen`."""
    with open(ensure_run_dir(out), "ab") as fh:
        return subprocess.Popen(argv, stdout=fh, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                                start_new_session=True, env=env, cwd=cwd)


def last_lines(path, n=1, limit=4096):
    """The last `n` non-empty lines of a file (from its last `limit`
    bytes), for a failed run's error; ``[]`` when there are none."""
    try:
        with open(path, "rb") as fh:
            fh.seek(0, os.SEEK_END)
            size = fh.tell()
            fh.seek(max(0, size - limit))
            lines = [l.strip() for l in fh.read().decode("utf-8", "replace").splitlines() if l.strip()]
    except OSError:
        return []
    return lines[-n:]


def last_line(path, limit=4096):
    """The last non-empty line of a file, or None."""
    lines = last_lines(path, 1, limit)
    return lines[-1] if lines else None


def finish_file(prefix, exit, error=None):
    """Finish the run's record (``run.json``) as it is on disk now (the
    driver may have replaced the starter's), unless it is finished
    already: for whoever started a run detached and saw it end. True when
    it wrote."""
    data = read_run(prefix)
    if data is not None and data.get("exit") is not None:
        return False
    rec = RunRecord(prefix, (data or {}).get("command") or "?", (data or {}).get("argv") or [])
    if data:
        rec.data = dict(data)
    # the starter's record, never replaced by the driver's (`_background`)
    rec.data.pop("provisional", None)
    rec.finish(exit=exit, error=error, workers=count_workers(prefix))
    return True


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
        got = status_file_run(fn)
        prefix = got[0] if got else fn
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


def command_argv(args):
    """The command line as typed, ``["lux", ...]``: what `entry.run` was
    given (it keeps it as ``args.cli_argv``), else this process's
    arguments."""
    argv = getattr(args, "cli_argv", None)
    if argv is None:
        argv = sys.argv[1:]
    return ["lux", *argv]


def exit_code(exc):
    """The exit status a run ending in `exc` reports: 130 for an
    interrupt, a ``SystemExit``'s own code, 1 for anything else."""
    if isinstance(exc, KeyboardInterrupt):
        return 130
    if isinstance(exc, SystemExit):
        code = exc.code
        return code if isinstance(code, int) else (0 if code is None else 1)
    return 1


def finish_record(record, exc=None, prefix=None):
    """Finish a run record after its command ends, with `exc` the exception
    it ended in (None: success). The worker count is re-read from the
    status files under `prefix`, since a manager may run fewer workers than
    it was given. Never raises."""
    if record is None:
        return
    try:
        workers = count_workers(prefix) if prefix else 0
        if exc is None:
            record.finish(exit=0, workers=workers)
        else:
            error = "interrupted" if isinstance(exc, KeyboardInterrupt) else \
                f"{type(exc).__name__}: {exc}"
            record.finish(exit=exit_code(exc), error=error, workers=workers)
    except Exception as e:  # the record must never be what fails a command
        logger.debug(f"could not finish the run record: {e}")


def driver_log_handler(prefix, log_level="INFO"):
    """A file handler on the driver's log, ``<prefix>/driver.log``, in the
    workers' line format (time, level, message) so every reader of a
    run's logs parses it the same way. Opened on the first line."""
    lvl = (log_level or "INFO").upper()
    if not hasattr(logging, lvl):
        lvl = "INFO"
    handler = logging.FileHandler(ensure_run_dir(driver_log_path(prefix)), delay=True)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(message)s",
                                           datefmt="%H:%M:%S"))
    handler.setLevel(getattr(logging, lvl))
    # so the console view leaves it in place when it takes the handlers
    # over, and so this can find it again
    handler.lux_driver_log = prefix
    return handler


def ensure_driver_log(prefix, log_level="INFO"):
    """Send this process's ``lux_pipeline`` log to ``<prefix>/driver.log`` as
    well as wherever it goes already: the driver's side of a run, which
    `lux watch` and the web UI tail beside the workers' files; and
    PhaseTimer's reports (identify's, in the driver) to ``driver.timing``
    only. Idempotent per prefix. Returns the handler, or ``None`` without
    a prefix."""
    if not prefix:
        return None
    timing = driver_timing_path(prefix)
    if not any(getattr(h, "lux_timing_file", None) == timing for h in timing_logger.handlers):
        send_timing_to(timing)
    lg = logging.getLogger("lux_pipeline")
    for h in lg.handlers:
        if getattr(h, "lux_driver_log", None) == prefix:
            return h
    handler = driver_log_handler(prefix, log_level)
    lg.addHandler(handler)
    if lg.level == logging.NOTSET or lg.level > handler.level:
        lg.setLevel(handler.level)
    return handler


def add_profile_arg(ap):
    """``--profile`` for the phase commands; see `enable_profiling`."""
    ap.add_argument("--profile", action="store_true",
                    help="Profile every worker with cProfile (as LUX_PROFILE=1): "
                         "profile-<phase>-<slice>.prof beside the timing JSON in log_dir; "
                         "read with lux timing-report --profile")


def enable_profiling(args):
    """``--profile`` sets ``LUX_PROFILE=1`` in this process, which the
    workers it starts inherit, and every PhaseTimer reads it. The phase
    functions take no parameter for it."""
    if getattr(args, "profile", False):
        os.environ["LUX_PROFILE"] = "1"


class BaseHandler:
    # False for a command that does not need the pipeline configuration at
    # all (entry then passes cfgs=None without trying to load one)
    needs_configs = True
    # Given --prefix, a command logs to <prefix>/driver.log and (unless it writes
    # its own record: the worker commands, lux build) entry writes
    # <prefix>/run.json for it, so a one-shot step of a build -- sheets,
    # identify by hand, portal -- is a run `lux watch` and the web UI can
    # follow like any other. False for the commands that take the option
    # to mean something else (watch) or are not runs (serve, help).
    uses_prefix = True
    own_record = False

    def __init__(self, cfgs):
        self.configs = cfgs
        self.default_source = "all"
        # the RunRecord of this run, if the command writes one (the worker
        # commands and `lux build`); entry.run finishes it again if the
        # command fails after its own finish
        self.run_record = None
        self.run_prefix = None

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
    # start_record writes the run's record itself: it knows the manager
    own_record = True

    def __init__(self, cfgs):
        super().__init__(cfgs)
        # {manager keyword: args attribute}; managers read their own options
        # from `args` in __init__, this is only for the record
        self.extra_args = {}

    def make_manager(self, workers, args):
        """Build the manager that runs this command's phase over `workers`
        workers. Commands override this; item 07 supplies the base."""
        raise NotImplementedError(f"{type(self).__name__} has no manager yet")

    def start_record(self, prefix, args, manager, workers):
        """Write ``<prefix>/run.json`` for this run and return its `RunRecord`;
        ``None`` for one ``--my-worker`` slice of a larger run, which is one
        of many processes sharing the prefix (each slice would overwrite
        the others' record). Never raises."""
        if getattr(args, "my_worker", -1) > -1:
            return None
        try:
            hidden = set()
            if getattr(manager, "hide_idle_workers", False):
                hidden = set(range(len(manager.sources), workers))
            # the null engine runs worker 0 only; otherwise the manager's
            # count, which list_runs replaces by the status files' once
            # there are any (some managers run fewer than they were given)
            planned = 1 if args.engine == "null" else max(1, int(manager.max_workers))
            names = []
            for s in manager.sources:
                name = s[1] if isinstance(s, (tuple, list)) else s
                if name not in names:
                    names.append(name)
            record = RunRecord(prefix, args.command, command_argv(args), engine=args.engine,
                               workers=planned,
                               visible_workers=[n for n in range(planned) if n not in hidden],
                               sources=names, parent=os.environ.get(PARENT_RUN_ENV) or None,
                               extra=run_extra())
            return record.start()
        except Exception as e:
            logger.debug(f"could not write the run record for {prefix}: {e}")
            return None

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

        # <prefix>/run.json: the command, its options and its outcome, beside the
        # workers' status and log files (lux_pipeline.progress.list_runs);
        # <prefix>/driver.log: what this process logs before, between and after
        # the workers (a defaulted prefix: entry attached one given)
        self.run_prefix = prefix
        ensure_driver_log(prefix, getattr(args, "log", "INFO"))
        self.run_record = self.start_record(prefix, args, manager, wks)
        try:
            view = None if args.no_ui else self.make_view(prefix, wks, args)
            if view is None:
                manager.process(args.engine, prefix, None)
            else:
                if getattr(manager, "hide_idle_workers", False):
                    for n in range(len(manager.sources), wks):
                        view.set_visible(n, False)
                with view:
                    manager.process(args.engine, prefix, view)
        except BaseException as e:
            finish_record(self.run_record, e, prefix)
            raise
        finish_record(self.run_record, None, prefix)
        # so a build can be re-attached with `lux watch`
        print(f"status/log prefix: {prefix}")
        return manager
