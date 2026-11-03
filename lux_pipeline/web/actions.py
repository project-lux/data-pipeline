"""The action log: one JSON line in ``<log_dir>/web_actions.log`` for
everything the web UI changes or starts (a configuration write, new
record, delete or reload; a command started or cancelled; a health check,
count or bloat report run), with the time, the remote address and what
was done. Shown at ``/actions``. Recording never fails a request."""

import json
import logging
import os
import threading
import time

logger = logging.getLogger("lux_pipeline")

ACTIONS_FILE = "web_actions.log"
SHOWN = 200
_lock = threading.Lock()


def actions_path(state):
    d = state.log_dir()
    return os.path.join(d, ACTIONS_FILE) if d else None


def record(request, action, **fields):
    """Append `action` (``config.write``, ``command.start``, ...) and
    `fields` to the log; returns the entry."""
    client = getattr(request, "client", None)
    entry = {"time": time.time(), "action": action,
             "remote": getattr(client, "host", None), **fields}
    logger.info(f"web: {action} " + " ".join(f"{k}={v}" for k, v in fields.items()))
    try:
        path = actions_path(request.app.state.lux)
        if path is None:
            return entry
        line = json.dumps(entry, default=str)
        with _lock, open(path, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except Exception as e:
        logger.warning(f"could not write the web action log: {type(e).__name__}: {e}")
    return entry


def read_actions(state, limit=SHOWN, block=65536):
    """The last `limit` entries, newest first; lines that do not parse are
    skipped."""
    path = actions_path(state)
    if not path or not os.path.exists(path):
        return []
    with open(path, "rb") as fh:
        fh.seek(0, os.SEEK_END)
        size = fh.tell()
        data, pos = b"", size
        while pos > 0 and data.count(b"\n") <= limit:
            step = min(block, pos)
            pos -= step
            fh.seek(pos)
            data = fh.read(step) + data
    lines = data.decode("utf-8", "replace").splitlines()
    if pos > 0:
        lines = lines[1:]                 # the first may be cut
    out = []
    for line in reversed(lines):
        try:
            out.append(json.loads(line))
        except ValueError:
            continue
        if len(out) >= limit:
            break
    return out
