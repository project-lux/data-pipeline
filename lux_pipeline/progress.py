"""Progress reporting for long-running components.

A component that does a lot of work (a loader, harvester, index loader, one
of the phase functions, `PhaseTimer`) accepts ``progress=null_progress`` and
calls it as it goes::

    progress(total=n)              # once, when the count is known
    progress(advance=1)            # per unit of work
    progress(description="ycba")   # what it is working on now
    progress(phase="references")   # a worker that runs two phases says so
    progress(completed=n)          # set the count outright
    progress(done=True)            # on the way out
    progress(reset=True)           # a second pass starts: from zero, no total, not done

It never knows whether anything is listening. The default listener is
`null_progress`, which costs one function call. The other listener this
module provides is `StatusWriter`, which keeps running totals and rewrites
one small JSON file describing the worker; the console UI (``lux watch``)
reads those files exactly as it tails the workers' log files. Any process,
however it was started -- a ``lux`` engine, a ``*_parallel.sh`` loop, a
developer at a prompt -- can therefore be watched.

Design rules:

* ``progress()`` must be cheap enough to call per record: a dict update and a
  ``time.monotonic()`` compare. I/O happens only when the throttle elapses or
  on ``flush()``.
* No imports from ``rich``, ``multiprocessing``, ``ray`` or anything under
  ``lux_pipeline/cli``. This module sits at the bottom of the import stack
  alongside ``timing.py``.
* Paths are chosen by the caller (the engine or the shell wrapper). The
  convention ``<log_dir>/<command>-<timestamp>-w<n>.status|.log`` is defined
  here by `status_path` / `log_path` so the UI can find the files.

This module is also the one reader of everything a run leaves on disk, so
the console view (``BuildView``, ``lux watch``) and any other view see a
run the same way: `read_build` (every worker's status and the totals),
`LogTailer` (new log lines), `count_workers` / `latest_prefix` /
`list_runs` (which runs there are), and the run record, ``<prefix>.run``:
what the driver knows and the workers' files do not -- the command, its
arguments, the driver's process, whether and how it finished
(`RunRecord`, `read_run`, `run_state`). Workers never write the record.
"""

import glob
import json
import logging
import os
import re
import socket
import time
from datetime import datetime
from typing import Callable

__all__ = [
    "Progress",
    "null_progress",
    "StatusWriter",
    "read_status",
    "status_path",
    "log_path",
    "unfinished_workers",
    "Sliced",
    "RUNNING_WINDOW",
    "STATUS_RE",
    "LogTailer",
    "count_workers",
    "latest_prefix",
    "worker_label",
    "read_build",
    "run_path",
    "RunRecord",
    "read_run",
    "pid_alive",
    "run_state",
    "list_runs",
]

logger = logging.getLogger("lux_pipeline")

# A worker that has not reported done but wrote its status this recently is
# taken to be running; an older one was killed without reporting
RUNNING_WINDOW = 3600
# <prefix>-w<n>.status
STATUS_RE = re.compile(r"^(?P<prefix>.*)-w(?P<n>\d+)\.status$")
# the timestamp default_prefix (cli/_handler) puts at the end of a prefix
_STAMP_RE = re.compile(r"^(?P<command>.*)-(?P<stamp>\d{4}-\d{2}-\d{2}-\d{6})$")

# The contract every long-running component accepts: any callable taking
# these keyword arguments, all optional --
#   progress(advance=1 | total=n | completed=n | description=str | phase=str | done=bool | reset=bool)
Progress = Callable[..., None]


def null_progress(**kw):
    """The default listener: accepts everything, does nothing."""
    return None


def status_path(prefix, worker):
    """Where worker `worker` of the run identified by `prefix` writes status."""
    return f"{prefix}-w{worker}.status"


def log_path(prefix, worker):
    """Where worker `worker` of the run identified by `prefix` writes its log."""
    return f"{prefix}-w{worker}.log"


class StatusWriter:
    """A Progress callable that keeps running totals and, at most `throttle`
    seconds apart, atomically rewrites one small JSON file describing this
    worker.

    File content::

        {"worker": 3, "phase": "merge", "description": "ycba",
         "completed": 12345, "total": 98765, "updated": 1761234567.1,
         "done": false}

    `total` is ``None`` until the component reports one. The write goes to a
    ``.tmp`` name and is renamed into place, so a reader never sees half a
    file. Call `flush` on the way out: the throttle would otherwise drop the
    final state.
    """

    def __init__(self, path, worker, throttle=0.5):
        self.path = path
        self.worker = worker
        self.throttle = throttle
        self.phase = None
        self.description = None
        self.completed = 0
        self.total = None
        self.done = False
        self._last_write = float("-inf")

    def __call__(self, advance=0, total=None, completed=None, description=None,
                 phase=None, done=None, reset=False):
        if reset:
            # a second pass in the same worker (reconcile's references): the
            # first pass's finish() reported done and a total that no longer
            # applies, and total=None cannot clear one
            self.completed = 0
            self.total = None
            self.done = False
        if advance:
            self.completed += advance
        if completed is not None:
            self.completed = completed
        if total is not None:
            self.total = total
        if description is not None:
            self.description = description
        if phase is not None:
            self.phase = phase
        if done is not None:
            self.done = done
        now = time.monotonic()
        if now - self._last_write >= self.throttle:
            self._write(now)

    def flush(self):
        """Write now, ignoring the throttle."""
        self._write(time.monotonic())

    def state(self):
        return {
            "worker": self.worker,
            "phase": self.phase,
            "description": self.description,
            "completed": self.completed,
            "total": self.total,
            "updated": time.time(),
            "done": self.done,
        }

    def _write(self, now):
        self._last_write = now
        tmp = f"{self.path}.{os.getpid()}.tmp"
        try:
            with open(tmp, "w") as fh:
                json.dump(self.state(), fh)
            os.replace(tmp, self.path)
        except OSError:
            # status must never be the thing that kills a worker
            try:
                os.unlink(tmp)
            except OSError:
                pass


def read_status(path):
    """The dict a `StatusWriter` last wrote to `path`, or ``None``.

    Tolerant: a missing file, a torn write, or anything that is not a JSON
    object all read as ``None``. The writer renames into place so a torn
    file is rare, but a reader polling at the wrong moment must not crash.
    """
    try:
        with open(path) as fh:
            data = json.load(fh)
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def unfinished_workers(log_dir):
    """``[(path, status), ...]`` for every ``<log_dir>/*-w<n>.status`` whose
    worker has not reported done, newest first; ``[]`` when `log_dir` is
    unset or missing.

    A worker that fails still reports done on the way out (`_run_worker`),
    so these are workers still running -- or killed outright (SIGKILL, the
    OOM killer), which no ``finally`` survives. ``status["updated"]`` tells
    the two apart. A file that cannot be read is skipped.
    """
    if not log_dir or not os.path.isdir(log_dir):
        return []
    out = []
    for fn in glob.glob(os.path.join(glob.escape(log_dir), "*-w*.status")):
        st = read_status(fn)
        if st is not None and not st.get("done"):
            out.append((fn, st))
    out.sort(key=lambda x: x[1].get("updated") or 0, reverse=True)
    return out


# --------------------------------------------------------------- reading a run


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


def worker_label(status):
    """What a worker is doing, as the console view labels it:
    ``description``, else ``phase``, else ``running``; ``waiting`` before
    its first status."""
    if status is None:
        return "waiting"
    return status.get("description") or status.get("phase") or "running"


def read_build(prefix, workers, visible=None):
    """Every worker's status, and the all-workers numbers, for the run at
    `prefix` with `workers` workers. `visible` is the set of worker numbers
    the totals cover (default: all); hidden workers are idle ones
    (`TaskManager.hide_idle_workers`)::

        {"prefix": prefix,
         "workers": {n: <status dict> | None},
         "labels": {n: "ycba" | "references" | "running" | "waiting"},
         "seen": <workers with a status file>,
         "done": <seen > 0 and every worker's status says done>,
         "completed": <sum over visible workers>,
         "total": <sum over visible workers, or None while any has none>}

    The rules are the console view's (`cli._view.BuildView`, which renders
    from this): a worker with no status file yet is not done and has no
    total, and ``done`` asks every worker, hidden or not, so pass the
    number of workers that will write files (`count_workers` once any
    have)."""
    statuses = {}
    seen = 0
    all_done = True
    for n in range(workers):
        st = read_status(status_path(prefix, n))
        statuses[n] = st
        if st is None:
            all_done = False
            continue
        seen += 1
        if not st.get("done"):
            all_done = False
    shown = [n for n in range(workers) if visible is None or n in visible]
    completed = sum((statuses[n] or {}).get("completed") or 0 for n in shown)
    totals = [(statuses[n] or {}).get("total") for n in shown]
    total = sum(totals) if shown and all(t is not None for t in totals) else None
    return {
        "prefix": prefix,
        "workers": statuses,
        "labels": {n: worker_label(statuses[n]) for n in range(workers)},
        "seen": seen,
        "done": seen > 0 and all_done,
        "completed": completed,
        "total": total,
    }


# --------------------------------------------------------------- the run record


def run_path(prefix):
    """Where the driver of the run at `prefix` writes its record."""
    return f"{prefix}.run"


def _write_json_atomic(path, data):
    tmp = f"{path}.{os.getpid()}.tmp"
    try:
        with open(tmp, "w") as fh:
            json.dump(data, fh)
        os.replace(tmp, path)
    except (OSError, TypeError, ValueError) as e:
        # a record must never be the thing that fails a command
        logger.debug(f"could not write {path}: {e}")
        try:
            os.unlink(tmp)
        except OSError:
            pass


class RunRecord:
    """What the driver of a run knows and its workers' files do not,
    written to ``<prefix>.run`` (atomically, like `StatusWriter`)::

        {"prefix", "command", "argv", "host", "pid", "engine", "workers",
         "visible_workers", "sources", "parent", "started", "finished",
         "exit", "error", ...extra}

    `start` stamps the process and the time; `update` changes fields
    (``lux build`` keeps its ``current`` phase and ``steps`` here);
    `finish` records the exit status (0, 1 for a failure, 130 for an
    interrupt) and, if given, the worker count the files show. A later
    `finish` overwrites an earlier one: the entry point re-finishes a run
    whose command failed after its workers succeeded. Never written by a
    worker, and not for one ``--my-worker`` slice of a larger run."""

    def __init__(self, prefix, command, argv, *, engine=None, workers=0, visible_workers=None,
                 sources=(), parent=None, extra=None):
        self.path = run_path(prefix)
        self.data = {
            "prefix": prefix,
            "command": command,
            "argv": list(argv),
            "host": socket.gethostname(),
            "pid": None,
            "engine": engine,
            "workers": workers,
            "visible_workers": list(visible_workers) if visible_workers is not None else None,
            "sources": list(sources),
            "parent": parent,
            "started": None,
            "finished": None,
            "exit": None,
            "error": None,
        }
        if extra:
            self.data.update(extra)

    def state(self):
        return dict(self.data)

    def start(self):
        self.data.update(pid=os.getpid(), started=time.time(), finished=None, exit=None, error=None)
        _write_json_atomic(self.path, self.data)
        return self

    def update(self, **fields):
        self.data.update(fields)
        _write_json_atomic(self.path, self.data)

    def finish(self, exit=0, error=None, workers=None):
        self.data.update(finished=time.time(), exit=exit, error=error)
        if workers:
            self.data["workers"] = workers
            vis = self.data.get("visible_workers")
            if vis is not None:
                self.data["visible_workers"] = [n for n in vis if n < workers]
        _write_json_atomic(self.path, self.data)


def read_run(prefix):
    """The record at ``<prefix>.run``, or ``None``; as tolerant as
    `read_status`."""
    return read_status(run_path(prefix))


def pid_alive(pid):
    """Whether process `pid` exists on this machine."""
    if not pid:
        return False
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True        # someone else's process, but alive
    except (OSError, ValueError, OverflowError):
        return False
    return True


def exit_state(code):
    return "finished" if code == 0 else "interrupted" if code == 130 else "failed"


def run_state(record, build, now=None, window=RUNNING_WINDOW):
    """``running``, ``finished``, ``failed``, ``interrupted``, ``killed`` or
    ``unknown`` for a run, from its record (`read_run`) and its files
    (`read_build`).

    With a record from this machine: its exit status once finished; while
    not, ``running`` if the driver's process is alive and ``killed`` if not
    (SIGKILL, the OOM killer, a closed terminal: no ``finally`` ran).
    Without one -- one ``--my-worker`` slice per process, as the shell
    scripts run them -- or from another machine: ``finished`` when every
    worker says done, ``running`` when some status was updated within
    `window` seconds, otherwise ``unknown``."""
    now = time.time() if now is None else now
    if record is not None:
        if record.get("exit") is not None:
            return exit_state(record["exit"])
        if record.get("host") in (None, socket.gethostname()):
            return "running" if pid_alive(record.get("pid")) else "killed"
    if build["done"]:
        return "finished"
    updated = [st.get("updated") or 0 for st in build["workers"].values() if st]
    if updated and now - max(updated) <= window:
        return "running"
    return "unknown"


def _started(prefix, record):
    """When a run started: its record's start, else the timestamp in the
    prefix, else the oldest of its files' modification times."""
    if record and record.get("started"):
        return record["started"]
    m = _STAMP_RE.match(os.path.basename(prefix))
    if m:
        try:
            return datetime.strptime(m.group("stamp"), "%Y-%m-%d-%H%M%S").timestamp()
        except ValueError:
            pass
    times = []
    for fn in glob.glob(glob.escape(prefix) + "-w*.*"):
        try:
            times.append(os.path.getmtime(fn))
        except OSError:
            pass
    return min(times) if times else None


def list_runs(log_dir, now=None):
    """Every run under `log_dir`, newest first: each prefix with a
    ``.run`` record or any ``-w<n>.status`` file (a run started from a
    shell slice by slice has only those)::

        {"prefix", "command", "state", "started", "finished", "exit",
         "error", "argv", "parent", "workers", "visible_workers",
         "record": <the record or None>, "progress": <read_build>}

    The worker count is the status files' (`count_workers`) once there are
    any, else the record's: a manager may run fewer workers than it was
    given (download, export, index-load, harvest phase 1) and a null
    engine runs one. ``[]`` when `log_dir` is unset or missing."""
    if not log_dir or not os.path.isdir(log_dir):
        return []
    base = glob.escape(log_dir)
    prefixes = set()
    for fn in glob.glob(os.path.join(base, "*.run")):
        prefixes.add(fn[:-len(".run")])
    for fn in glob.glob(os.path.join(base, "*-w*.status")):
        m = STATUS_RE.match(fn)
        if m:
            prefixes.add(m.group("prefix"))
    out = []
    for prefix in prefixes:
        record = read_run(prefix)
        workers = count_workers(prefix) or int((record or {}).get("workers") or 0)
        visible = (record or {}).get("visible_workers")
        build = read_build(prefix, workers, set(visible) if visible is not None else None)
        name = os.path.basename(prefix)
        m = _STAMP_RE.match(name)
        command = (record or {}).get("command") or (m.group("command") if m else name)
        r = record or {}
        out.append({
            "prefix": prefix,
            "command": command,
            "state": run_state(record, build, now),
            "started": _started(prefix, record),
            "finished": r.get("finished"),
            "exit": r.get("exit"),
            "error": r.get("error"),
            "argv": r.get("argv"),
            "parent": r.get("parent"),
            "workers": workers,
            "visible_workers": visible,
            "record": record,
            "progress": build,
        })
    out.sort(key=lambda x: (x["started"] or 0, x["prefix"]), reverse=True)
    return out


class Sliced:
    """Mixin for a component that works on one slice of a larger whole.

    `prepare` is called before `process`; the component then uses
    ``self.my_slice`` / ``self.max_slice`` to pick its share of the work and
    ``self.progress(...)`` to report on it. The caller that knows the whole
    count divides it (``len_estimate() // max_slice``); nothing here does.
    """

    my_slice = -1
    max_slice = -1
    progress = staticmethod(null_progress)

    def prepare(self, my_slice=-1, max_slice=-1, progress=null_progress):
        self.my_slice = my_slice
        self.max_slice = max_slice
        self.progress = progress
        return self
