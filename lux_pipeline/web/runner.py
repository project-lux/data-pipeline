"""Starting, watching and cancelling `lux` commands from the web UI.

A command is a process of its own, as `lux build` runs its phases:
``python -m lux_pipeline.cli.entry <command> ... --no-ui --prefix <P>``,
in a new session (so cancelling it stops its workers too), with its
output in ``<P>.out``. The server never runs a phase itself, and does not
wait for one: the run page reads the run's files (W09). So the server can
be restarted while a build runs (`adopt` picks the runs up again).

Every run the runner starts has a record (``<P>.run``) from its first
moment: the runner writes one with the child's pid, the command's driver
(a worker command, `lux build`) replaces it with its own, and, through
``LUX_RUN_EXTRA``, keeps ``web: true`` and the ``stdout`` file in it. When
the process ends without having finished its record (a command with no
driver record, one that failed before writing it, or one cancelled), the
runner finishes it with the exit code and the last line of output.

The same guard as the shell's: a phase, or a command that rewrites what a
build depends on, is refused while a build is running
(`refuse_while_building`), unless forced (only a command with ``--force``
can be); and two phases the web started are never run at once.
"""

import json
import logging
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field

from lux_pipeline import runtime
from lux_pipeline.cli._handler import (PARENT_RUN_ENV, RUN_EXTRA_ENV, finish_file,  # noqa: F401
                                       free_prefix, last_line, out_path, refuse_while_building,
                                       start_detached)
from lux_pipeline.cli.introspect import GROUPS, command_spec
from lux_pipeline.progress import RunRecord, list_runs, pid_alive, read_run

logger = logging.getLogger("lux_pipeline")

PHASES = frozenset(GROUPS["phases"])
FORBIDDEN = ("--my-worker", "--my_worker", "--debug", "--prefix", "--env", "--background", "-b")
CANCEL_ERROR = "cancelled from the web"
DRY_RUN_TIMEOUT = 20
REAP_EVERY = 0.5


@dataclass
class RunHandle:
    prefix: str
    command: str
    argv: list
    pid: int
    started: float
    stdout: str
    proc: object = field(default=None, repr=False)   # None for a run adopted after a restart


class Runner:
    def __init__(self, state, python=None):
        self.state = state
        self.python = python or sys.executable
        self.handles = {}
        self.cancelled = set()
        self.lock = threading.Lock()
        self._reaper = None

    # --- command lines ---------------------------------------------------------------

    def base_argv(self):
        return [self.python, "-m", "lux_pipeline.cli.entry"]

    def argv_for(self, command, tail, prefix):
        """The process's command line. `tail` is what a form produced
        (`introspect.argv_from_values` without the command); the runner adds
        ``--no-ui`` and the prefix, and refuses what it must set itself."""
        for a in tail:
            if a.split("=", 1)[0] in FORBIDDEN:
                raise ValueError(f"{a} is set by the runner, not the form")
        out = [*self.base_argv(), command, *tail]
        if "--no-ui" not in tail:
            out.append("--no-ui")
        return out + ["--prefix", prefix]

    def env(self, stdout):
        env = dict(os.environ)
        env.pop(PARENT_RUN_ENV, None)
        env[RUN_EXTRA_ENV] = json.dumps({"web": True, "stdout": stdout})
        bp = runtime.get_basepath()
        if bp:
            env["LUX_BASEPATH"] = bp
        env["PYTHONUNBUFFERED"] = "1"
        return env

    def cwd(self, cfgs):
        d = getattr(cfgs, "base_dir", None)
        return d if d and os.path.isdir(d) else None

    def new_prefix(self, cfgs, command):
        return free_prefix(cfgs, command, taken=self.handles)

    # --- starting --------------------------------------------------------------------

    def check(self, command, spec, force, guarded=None):
        """Refuse what the shell would refuse now (RuntimeError): a phase,
        or a run that rewrites what a build depends on (`guarded`; default:
        any run of a command that can), while a build runs."""
        if not spec["runnable"]:
            raise ValueError(f"lux {command} is not run from the web UI")
        if guarded is None:
            guarded = spec["confirms"] is not None
        if command in PHASES or guarded:
            has_force = any(o["dest"] == "force" for o in spec["options"])
            refuse_while_building(self.state.configs(), command, force and has_force)
        if command in PHASES:
            with self.lock:
                for h in self.handles.values():
                    if h.command in PHASES and (h.proc is None or h.proc.poll() is None):
                        raise RuntimeError(f"lux {h.command} started from here is still running "
                                           f"({h.prefix}); wait for it, or cancel it")

    def start(self, command, tail, *, force=False, guarded=None):
        """Start ``lux <command> <tail>``; returns its `RunHandle` at once.
        `guarded`: this run rewrites what a build depends on (`check`)."""
        spec = command_spec(command)
        self.check(command, spec, force, guarded)
        cfgs = self.state.configs()
        with self.lock:
            prefix = self.new_prefix(cfgs, command)
            stdout = out_path(prefix)
            argv = self.argv_for(command, list(tail), prefix)
            shown = ["lux", command, *tail]
            rec = RunRecord(prefix, command, shown, workers=0,
                            extra={"web": True, "stdout": stdout})
            rec.start()
            proc = start_detached(argv, stdout, env=self.env(stdout), cwd=self.cwd(cfgs))
            rec.update(pid=proc.pid)
            handle = RunHandle(prefix, command, shown, proc.pid, time.time(), stdout, proc)
            self.handles[prefix] = handle
        logger.info(f"started from the web: {' '.join(shown)} ({prefix}, pid {proc.pid})")
        self._ensure_reaper()
        return handle

    def dry_run(self, tail, timeout=DRY_RUN_TIMEOUT):
        """``lux build <tail> --dry-run``: the steps it would run, as lines;
        RuntimeError with its output when it fails."""
        cfgs = self.state.configs()
        for a in tail:
            if a.split("=", 1)[0] in FORBIDDEN:
                raise ValueError(f"{a} is set by the runner, not the form")
        argv = [*self.base_argv(), "build", *tail, "--dry-run"]
        env = dict(os.environ)
        env.pop(RUN_EXTRA_ENV, None)
        bp = runtime.get_basepath()
        if bp:
            env["LUX_BASEPATH"] = bp
        try:
            done = subprocess.run(argv, capture_output=True, text=True, timeout=timeout, env=env,
                                  cwd=self.cwd(cfgs), stdin=subprocess.DEVNULL)
        except subprocess.TimeoutExpired:
            raise RuntimeError(f"lux build --dry-run took over {timeout} s") from None
        lines = [l for l in done.stdout.splitlines() if l.startswith("[")]
        if done.returncode != 0:
            raise RuntimeError((done.stderr or done.stdout).strip().splitlines()[-1]
                               if (done.stderr or done.stdout).strip() else f"exit {done.returncode}")
        return [l.replace(f"{self.python} -m lux_pipeline.cli.entry", "lux") for l in lines]

    # --- cancelling ------------------------------------------------------------------

    def cancellable(self, record):
        """Why a run cannot be cancelled from here, or None if it can."""
        if not record:
            return "it has no record"
        if not record.get("web"):
            parent = read_run(record["parent"]) if record.get("parent") else None
            if parent and parent.get("web"):
                return "it is a phase of a build: cancel the build"
            return f"it was started elsewhere: stop it there (pid {record.get('pid')})"
        if record.get("host") not in (None, socket.gethostname()):
            return f"it runs on {record.get('host')}"
        if record.get("exit") is not None or not pid_alive(record.get("pid")):
            return "it is not running"
        return None

    def cancel(self, prefix, grace=10.0):
        """Stop a run the web started: SIGTERM to its process group (its
        workers too), SIGKILL after `grace` seconds if it is still there.
        RuntimeError saying why when it cannot be."""
        record = read_run(prefix)
        why = self.cancellable(record)
        if why:
            raise RuntimeError(f"not cancelled: {why}")
        pid = int(record["pid"])
        try:
            pgid = os.getpgid(pid)
        except ProcessLookupError:
            return False
        self.cancelled.add(prefix)
        os.killpg(pgid, signal.SIGTERM)
        logger.info(f"cancelled from the web: {prefix} (pid {pid})")

        def kill():
            if pid_alive(pid):
                try:
                    os.killpg(pgid, signal.SIGKILL)
                except ProcessLookupError:
                    pass

        t = threading.Timer(grace, kill)
        t.daemon = True
        t.start()
        with self.lock:
            if prefix not in self.handles:          # started before a restart: watch it end
                self.handles[prefix] = RunHandle(prefix, record.get("command") or "?",
                                                 record.get("argv") or [], pid, time.time(),
                                                 record.get("stdout") or "")
        self._ensure_reaper()
        return True

    # --- watching --------------------------------------------------------------------

    def adopt(self):
        """After a restart: watch the runs the web started that are still
        running (their processes are no longer this server's children)."""
        try:
            log_dir = getattr(self.state.configs(), "log_dir", None)
        except Exception:
            return 0
        n = 0
        for r in list_runs(log_dir):
            rec = r["record"] or {}
            if rec.get("web") and r["state"] == "running" and r["prefix"] not in self.handles:
                self.handles[r["prefix"]] = RunHandle(r["prefix"], r["command"], rec.get("argv") or [],
                                                      int(rec.get("pid") or 0), rec.get("started") or 0,
                                                      rec.get("stdout") or "")
                n += 1
        if n:
            self._ensure_reaper()
        return n

    def _ensure_reaper(self):
        with self.lock:
            if self._reaper is None or not self._reaper.is_alive():
                self._reaper = threading.Thread(target=self._reap_loop, name="lux-runner", daemon=True)
                self._reaper.start()

    def _reap_loop(self):
        while True:
            with self.lock:
                items = list(self.handles.items())
            if not items:
                return
            for prefix, h in items:
                self.reap_one(prefix, h)
            time.sleep(REAP_EVERY)

    def reap_one(self, prefix, h):
        """Finish `h`'s record if it has ended; True when it has."""
        if h.proc is not None:
            code = h.proc.poll()
            if code is None:
                return False
        elif pid_alive(h.pid):
            return False
        else:
            code = None
        if prefix in self.cancelled:
            finish_file(prefix, code if code is not None else -signal.SIGTERM, CANCEL_ERROR)
        elif code is not None:
            finish_file(prefix, code, None if code == 0 else (last_line(h.stdout) or f"exit {code}"))
        with self.lock:
            self.handles.pop(prefix, None)
        try:
            self.state.reset()
        except Exception as e:
            logger.debug(f"reset after {prefix}: {e}")
        return True
