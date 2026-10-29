"""Task managers and the engines that run them.

A *manager* knows which sources a command covers and how one worker does its
share: it implements ``_distributed(n, cfgs, progress)``. An *engine* has one
job: run ``_distributed`` for ``n in range(max_workers)`` and return the list
of results. Three engines:

=============  ===========================================  ==================
``null``       ``_distributed(my_slice)`` in this process   ``--my-worker i``,
                                                            tests, debugging
``mp``         ``ProcessPoolExecutor`` with spawned workers  the default
``ray``        ``ray.remote`` tasks                          optional extra,
                                                            multi-node
=============  ===========================================  ==================

Inside every worker `_run_worker` attaches the per-worker log file, builds
the process's `Config` afresh (a spawned process starts empty), hands the
manager a `StatusWriter` as its ``progress`` callable, runs
``_distributed``, and flushes the final status on the way out. Logs and
progress are files; nothing crosses the process boundary but the manager
(pickled without its configuration) and the result. The console view reads
the same files, which is why nothing under ``lux_pipeline/process`` knows it
exists: ``process()`` takes an optional object with a ``refresh()`` method
and calls it while waiting.
"""

import io
import logging
import multiprocessing
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, wait as wait_futures

from lux_pipeline import runtime
from lux_pipeline.progress import StatusWriter, log_path, null_progress, status_path

logger = logging.getLogger("lux_pipeline")

ENGINES = ("mp", "ray", "null")
REFRESH_INTERVAL = 0.5


# --------------------------------------------------------------- worker side


def make_file_handler(prefix, n, log_level="INFO"):
    """The worker's log file. Filtered at the user's level here, in the
    worker: the hot paths log per-record DEBUG messages that would otherwise
    be formatted only to be discarded."""
    lvl = (log_level or "INFO").upper()
    if not hasattr(logging, lvl):
        lvl = "INFO"
    handler = logging.FileHandler(log_path(prefix, n), delay=True)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(message)s", datefmt="%H:%M:%S"))
    handler.setLevel(getattr(logging, lvl))
    return handler


def attach_worker_log_handler(prefix, n, log_level="INFO"):
    """Make the worker's file its only log destination. The file is the log
    of record; the parent tails it for the live display, so a slow or
    stalled UI cannot block a worker. No prefix: a NullHandler."""
    lg = logging.getLogger("lux_pipeline")
    while lg.handlers:
        lg.removeHandler(lg.handlers[0])
    if prefix:
        handler = make_file_handler(prefix, n, log_level)
        lg.addHandler(handler)
        lg.setLevel(handler.level)
    else:
        lg.addHandler(logging.NullHandler())
    return lg


class LogStream(io.TextIOBase):
    """A stdout/stderr for a worker process: every complete line becomes a
    log record at `level`. Legacy code still ``print``s its progress
    (the source loaders, `PhaseTimer`); a spawned worker inherits the
    driver's terminal, so those lines would land in the middle of the
    console view and shift it. Through here they reach the worker's log
    file with a time and level like everything else, and the view tails
    them from there."""

    def __init__(self, level=logging.INFO):
        super().__init__()
        self.level = level
        self.buf = ""
        # a handler that fails while emitting reports on sys.stderr, which
        # may be this stream: drop rather than recurse
        self._busy = False

    def writable(self):
        return True

    def write(self, s):
        if self._busy:
            return len(s)
        self.buf += s
        while "\n" in self.buf:
            line, self.buf = self.buf.split("\n", 1)
            self._emit(line)
        return len(s)

    def flush(self):
        if self.buf:
            line, self.buf = self.buf, ""
            self._emit(line)

    def _emit(self, line):
        if not line.strip():
            return
        self._busy = True
        try:
            logger.log(self.level, line.rstrip())
        finally:
            self._busy = False


def _run_worker(manager, n, prefix, log_level="INFO", capture_output=False):
    """Everything one worker does, in order. Module-level so the spawn
    context can pickle it. `capture_output` (set by the engines that run
    workers in other processes) sends the worker's stdout and stderr to
    its log via `LogStream`; the null engine leaves them alone, since in
    the driver's process the console view already handles them."""
    status = None
    progress = null_progress
    saved = None
    if prefix:
        attach_worker_log_handler(prefix, n, log_level)
        status = StatusWriter(status_path(prefix, n), n)
        progress = status
        if capture_output:
            saved = (sys.stdout, sys.stderr)
            sys.stdout = LogStream(logging.INFO)
            sys.stderr = LogStream(logging.WARNING)
    try:
        cfgs = runtime.get_configs() if manager.needs_configs else None
        return manager._distributed(n, cfgs, progress)
    except BaseException as e:
        logger.error(f"worker {n} failed: {type(e).__name__}: {e}", exc_info=True)
        raise
    finally:
        if saved is not None:
            for stream in (sys.stdout, sys.stderr):
                try:
                    stream.flush()
                except Exception:
                    pass
            sys.stdout, sys.stderr = saved
        if status is not None:
            status(done=True)
            status.flush()


# --------------------------------------------------------------- the manager


class TaskManager:
    """Base for a command's manager. Subclasses implement `_distributed`
    and usually `maybe_add`; they read their own options from `args` in
    ``__init__``. Holds no UI state."""

    # False for a manager whose workers do not need the pipeline
    # configuration (tests); workers then get cfgs=None
    needs_configs = True
    # True when workers beyond len(sources) have nothing to do and the view
    # should hide them (harvest, download)
    hide_idle_workers = False

    def __init__(self, configs, max_workers=0, args=None):
        self.configs = configs
        if max_workers > 0:
            self.max_workers = max_workers
        else:
            self.max_workers = getattr(configs, "max_workers", 1) if configs is not None else 1
        self.sources = []
        self.my_slice = -1
        self.log_level = "INFO"
        self.max_records = -1
        self.args = args
        if args is not None:
            if getattr(args, "my_worker", -1) > -1:
                self.my_slice = args.my_worker
            if getattr(args, "log", None):
                self.log_level = args.log
            if getattr(args, "max_records", None) is not None:
                self.max_records = args.max_records

    # ----------------------------------------------------------- sources

    def maybe_add(self, which, cfg):
        """Add a source this command can act on. Subclasses test the config
        (has a loader, has a harvester, ...) before calling super()."""
        self.sources.append((which, cfg["name"]))

    def prepare_single(self, name):
        cfgs = self.configs
        for which in ("internal", "external", "results"):
            group = getattr(cfgs, which, {})
            if name in group:
                self.maybe_add(which, group[name])
                return
        raise ValueError(f"Unknown source: {name}")

    def prepare_all(self):
        for which in ("internal", "external"):
            for cfg in getattr(self.configs, which, {}).values():
                self.maybe_add(which, cfg)

    # ----------------------------------------------------------- running

    def _distributed(self, n, cfgs, progress):
        """Worker `n`'s share of the work. `cfgs` is this process's Config,
        `progress` a callable per lux_pipeline.progress."""
        raise NotImplementedError(f"{type(self).__name__}._distributed")

    def run_single(self, n=0):
        """Run worker `n` in this process with the configuration already
        loaded, logging to the existing handlers and reporting nothing.
        For tests and `lux interactive`."""
        return self._distributed(n, self.configs, null_progress)

    def process(self, engine="mp", prefix=None, view=None):
        """Run every worker under `engine`. `prefix` is where the workers'
        status and log files go (None: no files). `view`, if given, has a
        ``refresh()`` method that is called while waiting. Returns the list
        of worker results, indexed by worker; raises `WorkerFailed` after
        every worker has finished if any raised."""
        eng = make_engine(engine, self)
        cfgs = self.configs
        # pickled to the workers; carries no DB connections. Workers build
        # their own through lux_pipeline.runtime
        self.configs = None
        try:
            return eng.run(prefix, view)
        finally:
            self.configs = cfgs


class WorkerFailed(RuntimeError):
    """One or more workers raised. `failures` is ``[(worker, exception)]``;
    the first exception is the cause."""

    def __init__(self, failures):
        self.failures = failures
        workers = ", ".join(str(n) for n, _ in failures)
        n0, e0 = failures[0]
        super().__init__(f"worker(s) {workers} failed; first: worker {n0}: {type(e0).__name__}: {e0}")
        self.__cause__ = e0


# --------------------------------------------------------------- engines


class Engine:
    def __init__(self, manager):
        self.manager = manager
        self.max_workers = manager.max_workers

    def run(self, prefix, view):
        raise NotImplementedError

    @staticmethod
    def _refresh(view):
        if view is not None:
            view.refresh()

    @staticmethod
    def _finish(results, errors, view):
        if view is not None:
            view.refresh()
        if errors:
            for n, e in errors:
                logger.error(f"worker {n} failed: {type(e).__name__}: {e}")
            raise WorkerFailed(errors)
        return results


class NullEngine(Engine):
    """One worker, this process: ``--my-worker i --max-workers N`` is
    today's one-interpreter-per-slice model run by a shell loop."""

    def run(self, prefix, view):
        n = self.manager.my_slice if self.manager.my_slice > -1 else 0
        errors = []
        result = None
        try:
            result = _run_worker(self.manager, n, prefix, self.manager.log_level)
        except Exception as e:
            errors.append((n, e))
        return self._finish([result], errors, view)


class MpEngine(Engine):
    """Spawned processes via ProcessPoolExecutor; the default."""

    def run(self, prefix, view):
        ctx = multiprocessing.get_context("spawn")
        results = [None] * self.max_workers
        errors = []
        if prefix:
            logger.info(f"worker logs: {prefix}-w*.log")
        with ProcessPoolExecutor(max_workers=self.max_workers, mp_context=ctx) as ex:
            futures = {ex.submit(_run_worker, self.manager, n, prefix, self.manager.log_level, True): n
                       for n in range(self.max_workers)}
            pending = set(futures)
            while pending:
                done, pending = wait_futures(pending, timeout=REFRESH_INTERVAL)
                self._refresh(view)
            for f, n in futures.items():
                try:
                    results[n] = f.result()
                except Exception as e:
                    errors.append((n, e))
        errors.sort(key=lambda x: x[0])
        return self._finish(results, errors, view)


class RayEngine(Engine):
    """Ray remote tasks; the optional extra for multi-node runs."""

    def __init__(self, manager):
        super().__init__(manager)
        try:
            import ray
        except ImportError:
            raise RuntimeError("--engine ray needs the ray package: pip install -e '.[ray]'") from None
        self.ray = ray
        if not ray.is_initialized():
            ray.init(log_to_driver=False)

    def run(self, prefix, view):
        ray = self.ray
        remote = ray.remote(_run_worker)
        if os.getenv("LUX_PROFILE"):
            # a local ray's workers inherit it; a cluster's don't
            remote = remote.options(runtime_env={"env_vars": {"LUX_PROFILE": os.environ["LUX_PROFILE"]}})
        if prefix:
            logger.info(f"worker logs: {prefix}-w*.log")
        futures = {remote.remote(self.manager, n, prefix, self.manager.log_level, True): n
                   for n in range(self.max_workers)}
        pending = list(futures)
        results = [None] * self.max_workers
        errors = []
        while pending:
            # the timeout is what lets the driver refresh the view between completions
            ready, pending = ray.wait(pending, num_returns=1, timeout=REFRESH_INTERVAL)
            self._refresh(view)
            for ref in ready:
                n = futures[ref]
                try:
                    results[n] = ray.get(ref)
                except Exception as e:
                    errors.append((n, e))
        errors.sort(key=lambda x: x[0])
        return self._finish(results, errors, view)


_ENGINES = {"null": NullEngine, "mp": MpEngine, "ray": RayEngine}


def make_engine(name, manager):
    try:
        cls = _ENGINES[name]
    except KeyError:
        raise ValueError(f"Unknown engine '{name}'; expected one of {', '.join(ENGINES)}") from None
    return cls(manager)
