"""Helpers the web UI's tests share (webui_docs/). Not a test module.

`write_fake_run` leaves what a run leaves under ``log_dir``: one status
and one log file per worker and, unless asked not to, the driver's
``<prefix>.run`` record. Item W00 adds a database-free installation here.
"""

import json
import os
import socket
import time
from pathlib import Path

from lux_pipeline.progress import StatusWriter, log_path, run_path, status_path


def write_fake_run(log_dir, command, workers, *, done=False, record=True,
                   ts="2026-11-02-141500", totals=100, completed=40, pid=None,
                   exit=None, started=None, updated=None, extra=None, visible=None):
    """Write a run's files under `log_dir` and return its prefix.

    `done` is one flag for every worker or a sequence with one per worker;
    `totals` likewise (``None`` for a worker that has not reported one).
    The record gets ``pid`` (default: this process, so the run reads as
    running until `exit` is given), ``exit`` and ``started``. `updated`
    back-dates every status file's ``updated`` time. `record=False` leaves
    only the workers' files, as a shell-started slice does."""
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    prefix = os.path.join(str(log_dir), f"{command}-{ts}")
    dones = list(done) if isinstance(done, (list, tuple)) else [done] * workers
    tots = list(totals) if isinstance(totals, (list, tuple)) else [totals] * workers
    for n in range(workers):
        w = StatusWriter(status_path(prefix, n), n, throttle=0)
        w(phase=command, description=f"source{n}", completed=completed,
          **({"total": tots[n]} if tots[n] is not None else {}), done=dones[n])
        if updated is not None:
            st = json.loads(Path(status_path(prefix, n)).read_text())
            st["updated"] = updated
            Path(status_path(prefix, n)).write_text(json.dumps(st))
        Path(log_path(prefix, n)).write_text(f"14:15:0{n} INFO     worker {n} started\n")
    if record:
        rec = {"prefix": prefix, "command": command, "argv": ["lux", command],
               "host": socket.gethostname(), "pid": os.getpid() if pid is None else pid,
               "engine": "mp", "workers": workers,
               "visible_workers": list(range(workers)) if visible is None else list(visible),
               "sources": [], "parent": None,
               "started": started if started is not None else time.time(),
               "finished": None if exit is None else time.time(), "exit": exit,
               "error": None if exit in (None, 0) else "failed"}
        rec.update(extra or {})
        Path(run_path(prefix)).write_text(json.dumps(rec))
    return prefix


def dead_pid():
    """The pid of a process that has exited (and been reaped)."""
    import subprocess
    import sys
    p = subprocess.Popen([sys.executable, "-c", "pass"])
    p.wait()
    return p.pid
