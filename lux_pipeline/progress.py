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
"""

import json
import os
import time
from typing import Callable

__all__ = [
    "Progress",
    "null_progress",
    "StatusWriter",
    "read_status",
    "status_path",
    "log_path",
    "Sliced",
]

# The contract every long-running component accepts: any callable taking
# these keyword arguments, all optional --
#   progress(advance=1 | total=n | completed=n | description=str | phase=str | done=bool)
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
                 phase=None, done=None):
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
