"""``lux --background``: start the command as a process of its own and return.

What ``nohup lux ... &`` did, done by `lux` itself, and the way the web UI
starts a run (`web.runner`): the same command line, less the flag, plus
``--no-ui`` and ``--prefix P``, run as ``python -m lux_pipeline.cli.entry``
in a session of its own (`_handler.start_detached`), its output appended
to ``P/stdout.log``. The run is then what any other run is, the directory
``P`` with ``run.json``, ``driver.log`` and the workers' files, which
`lux watch P` and the web UI's run page
follow, and ``kill -- -<pid>`` stops it with its workers.

The starter writes a provisional ``P/run.json`` with the child's pid at once,
so the run is listed (and `lux watch --latest` finds it) while the child
is still loading its configuration; the child's own record replaces it.
It then waits a little for the child to get that far, so that a mistake
on the command line is reported here with the child's exit status rather
than left in ``P/stdout.log``. One ``--my-worker`` slice writes no record (the
shell scripts give every slice the same prefix): its output is
``P/w<n>.out`` and the starter only waits a moment.
"""

import json
import os
import sys
import time

from lux_pipeline.cli._handler import (PARENT_RUN_ENV, RUN_EXTRA_ENV, finish_file, free_prefix,
                                       last_lines, out_path, run_extra, start_detached)
from lux_pipeline.progress import RunRecord, read_run, worker_out_path

FLAG = "--background"
# set in the child: a started command never starts itself again (entry
# reads it as BACKGROUND_CHILD_ENV)
CHILD_ENV = "LUX_BACKGROUND_CHILD"
SHORT_FLAG = "-b"
# seconds to wait for the child to write its own record (or end)
WAIT = 15.0
# one --my-worker slice has no record to wait for
SLICE_WAIT = 1.0
# once the child's record is there, a little longer for an option error
GRACE = 0.5
POLL = 0.1


def is_flag(token):
    """Whether `token` is how the global parser would have read
    ``--background``: the flag, its short form, or an abbreviation."""
    return token == SHORT_FLAG or (len(token) > 2 and FLAG.startswith(token))


def child_args(cli_argv, args, prefix):
    """The child's arguments: as typed, less the flag, with ``--no-ui``
    and the prefix added when they were not given."""
    out = [a for a in cli_argv if not is_flag(a)]
    if not args.no_ui:
        out.append("--no-ui")
    if not args.prefix:
        out += ["--prefix", prefix]
    return out


def child_env(out):
    env = dict(os.environ)
    env[RUN_EXTRA_ENV] = json.dumps({**run_extra(), "background": True, "stdout": out})
    env["PYTHONUNBUFFERED"] = "1"
    env[CHILD_ENV] = "1"
    return env


def wait_for(proc, prefix, has_record, wait):
    """The child's exit status if it ends while we wait, else None. With a
    record, stop waiting soon after the child has written its own."""
    deadline = time.monotonic() + wait
    seen = False
    while time.monotonic() < deadline:
        code = proc.poll()
        if code is not None:
            return code
        if has_record and not seen:
            rec = read_run(prefix)
            if rec is not None and not rec.get("provisional"):
                seen = True
                deadline = min(deadline, time.monotonic() + GRACE)
        time.sleep(POLL)
    return proc.poll()


def start(args, cfgs, out=None, err=None, python=None, wait=None):
    """Start ``lux <args.command> ...`` detached; the exit status for this
    process: 0 once the child is running (or ended well at once), the
    child's own status when it failed at once."""
    out = out or sys.stdout
    err = err or sys.stderr
    one_slice = args.my_worker > -1
    prefix = args.prefix or free_prefix(cfgs, args.command)
    stdout = worker_out_path(prefix, args.my_worker) if one_slice else out_path(prefix)
    tail = child_args(args.cli_argv, args, prefix)
    argv = [python or sys.executable, "-m", "lux_pipeline.cli.entry", *tail]
    shown = ["lux", *(a for a in args.cli_argv if not is_flag(a))]

    proc = start_detached(argv, stdout, env=child_env(stdout))
    if not one_slice:
        # after the start, so it carries the child's pid: the child takes
        # far longer to import than this takes to write
        RunRecord(prefix, args.command, shown, workers=0,
                  parent=os.environ.get(PARENT_RUN_ENV) or None,
                  extra={"background": True, "stdout": stdout, "provisional": True}
                  ).update(pid=proc.pid, started=time.time())

    if wait is None:
        wait = SLICE_WAIT if one_slice else WAIT
    code = wait_for(proc, prefix, not one_slice, wait)
    if code is None:
        print(f"lux {args.command}: running in the background, pid {proc.pid}", file=out)
        print(f"  output: {stdout}", file=out)
        print(f"  watch:  lux watch {prefix}", file=out)
        print(f"  stop:   kill -- -{proc.pid}", file=out)
        return 0
    if not one_slice:
        finish_file(prefix, code, None if code == 0 else (last_lines(stdout)[-1:] or [f"exit {code}"])[0])
    if code == 0:
        print(f"lux {args.command}: finished at once; its output is in {stdout}", file=out)
        return 0
    print(f"lux {args.command}: ended at once with exit status {code}; from {stdout}:", file=err)
    for line in last_lines(stdout, 10):
        print(f"  {line}", file=err)
    return code
