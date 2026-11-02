"""Runs, from their files: the list, one run as the console view shows it,
its log, and a live stream of both.

Everything comes from what a run leaves under ``log_dir`` -- the workers'
``-w<n>.status`` and ``-w<n>.log``, the driver's ``.run`` record -- read
by `lux_pipeline.progress` (W01), the reader `lux watch` uses, so the two
can never disagree. A run started from a shell (status files only) shows
too. A run is named in a URL by its prefix relative to ``log_dir``, and a
name that would lead outside it is a 404.

Per worker, the rules are the console view's (`cli/_view.BuildView`): no
status file yet is ``waiting``; no total is an indeterminate bar; the
label is the description, else the phase, else ``running``; hidden
(idle) workers are left out of the totals. Elapsed time is counted from
the run's start; the estimate of what remains is elapsed x remaining /
done, as Rich's column estimates it.
"""

import asyncio
import json
import os
import re
import threading
import time

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse

from lux_pipeline.progress import LogTailer, describe_run, list_runs, log_path, run_exists

router = APIRouter(prefix="/api/runs", tags=["runs"])

# <time> <LEVEL padded to 8> <message>: the worker log format
# (process/tasks/_task_manager.make_file_handler)
LOG_RE = re.compile(r"^(\d\d:\d\d:\d\d) (DEBUG|INFO|WARNING|ERROR|CRITICAL) +(.*)$")
LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
FINISHED = ("finished", "failed", "interrupted", "killed", "unknown")
LOG_LINES = 200
STREAM_LIMIT = 8
_streams = {"count": 0, "lock": threading.Lock()}


# --- naming runs ---------------------------------------------------------------------


def log_dir(state):
    d = state.log_dir()
    if not d:
        raise HTTPException(status_code=404, detail="no log_dir in the configuration")
    return os.path.abspath(d)


def rel_prefix(state, prefix):
    """`prefix` relative to ``log_dir`` (what a URL names it by), or None
    when it is not under it."""
    if not prefix:
        return None
    base = log_dir(state)
    full = os.path.abspath(prefix)
    if os.path.commonpath([base, full]) != base or full == base:
        return None
    return os.path.relpath(full, base)


def full_prefix(state, rel):
    """The prefix a URL names, refusing anything outside ``log_dir`` or a
    run with nothing on disk (404)."""
    base = log_dir(state)
    if not rel or os.path.isabs(rel) or "\\" in rel or "\x00" in rel:
        raise HTTPException(status_code=404, detail=f"no run {rel!r}")
    full = os.path.normpath(os.path.join(base, rel))
    if os.path.commonpath([base, full]) != base or full == base or not run_exists(full):
        raise HTTPException(status_code=404, detail=f"no run {rel!r} under {base}")
    return full


# --- one run --------------------------------------------------------------------------


def _eta(elapsed, completed, total, active):
    if not active or not total or not completed or completed >= total or elapsed is None:
        return None
    return elapsed * (total - completed) / completed


def worker_rows(run, now):
    """The console view's rows, as data."""
    p = run["progress"]
    started = run["started"]
    visible = run["visible_workers"]
    running = run["state"] == "running"
    rows = []
    for n in range(run["workers"]):
        st = p["workers"].get(n)
        completed = (st or {}).get("completed") or 0
        total = (st or {}).get("total")
        done = bool((st or {}).get("done"))
        updated = (st or {}).get("updated")
        end = now if running and not done else (updated or now)
        elapsed = max(0.0, end - started) if started and st else None
        rows.append({
            "n": n, "label": p["labels"][n], "completed": completed, "total": total,
            "percent": min(100.0, 100.0 * completed / total) if total else None,
            "done": done, "waiting": st is None, "phase": (st or {}).get("phase"),
            "visible": visible is None or n in visible, "updated": updated,
            "elapsed": elapsed, "eta": _eta(elapsed, completed, total, running and not done),
        })
    return rows


def all_row(run, rows, now):
    p = run["progress"]
    started = run["started"]
    running = run["state"] == "running"
    if run["finished"]:
        end = run["finished"]
    elif running:
        end = now
    else:
        end = max([r["updated"] for r in rows if r["updated"]] or [started or now])
    elapsed = max(0.0, end - started) if started else None
    total = p["total"]
    return {"completed": p["completed"], "total": total,
            "percent": min(100.0, 100.0 * p["completed"] / total) if total else None,
            "workers": len([r for r in rows if r["visible"]]), "elapsed": elapsed,
            "eta": _eta(elapsed, p["completed"], total, running)}


def link(state, prefix):
    """``{"prefix", "rel"}`` for a run to link to; ``rel`` None when it
    cannot be (outside log_dir, or nothing on disk)."""
    if not prefix:
        return None
    rel = rel_prefix(state, prefix)
    return {"prefix": prefix, "rel": rel if rel and run_exists(prefix) else None}


def phase_strip(state, run):
    """For a `lux build`: each phase's state, from its record's steps
    (W01): ``[{"phase", "state", "steps", "run": link}]``."""
    record = run["record"] or {}
    phases = record.get("phases")
    if not phases:
        return None
    steps = record.get("steps") or []
    building = run["state"] == "running"
    out = []
    for phase in phases:
        mine = [s for s in steps if s.get("phase") == phase]
        if not mine:
            st = "pending" if building else "not run"
        elif any(s.get("exit") not in (None, 0) for s in mine):
            st = "failed"
        elif all(s.get("finished") for s in mine):
            st = "finished"
        else:
            st = "running" if building else "killed"
        prefix = next((s["prefix"] for s in mine if s.get("prefix")), None)
        out.append({"phase": phase, "state": st, "steps": len(mine),
                    "commands": [" ".join(s.get("argv") or []) for s in mine],
                    "run": link(state, prefix)})
    return out


def run_data(state, prefix, now=None, children=True):
    """One run, everything its page shows."""
    now = time.time() if now is None else now
    run = describe_run(prefix, now)
    rows = worker_rows(run, now)
    out = {
        "rel": rel_prefix(state, prefix), "prefix": prefix, "command": run["command"],
        "state": run["state"], "started": run["started"], "finished": run["finished"],
        "exit": run["exit"], "error": run["error"], "argv": run["argv"], "workers": run["workers"],
        "engine": (run["record"] or {}).get("engine"), "sources": (run["record"] or {}).get("sources"),
        "rows": rows, "all": all_row(run, rows, now),
        "parent": link(state, run["parent"]), "phases": phase_strip(state, run),
        "stdout": os.path.exists(f"{prefix}.out"), "now": now,
    }
    if children:
        out["children"] = [{"command": r["command"], "state": r["state"],
                            "run": link(state, r["prefix"])}
                           for r in list_runs(log_dir(state), now) if r["parent"] == prefix]
    return out


def run_summary(state, run):
    """A run in the list."""
    p = run["progress"]
    return {"rel": rel_prefix(state, run["prefix"]), "prefix": run["prefix"],
            "command": run["command"], "state": run["state"], "started": run["started"],
            "finished": run["finished"], "exit": run["exit"], "workers": run["workers"],
            "completed": p["completed"], "total": p["total"],
            "parent": rel_prefix(state, run["parent"]) if run["parent"] else None,
            "duration": (run["finished"] or time.time()) - run["started"]
            if run["started"] and (run["finished"] or run["state"] == "running") else None}


def runs_data(state, command=None, run_state=None, limit=500):
    runs = list_runs(log_dir(state))
    commands = sorted({r["command"] for r in runs})
    if command:
        runs = [r for r in runs if r["command"] == command]
    if run_state:
        runs = [r for r in runs if r["state"] == run_state]
    return {"runs": [run_summary(state, r) for r in runs[:limit]], "commands": commands,
            "log_dir": log_dir(state)}


# --- logs ---------------------------------------------------------------------------------


def parse_line(worker, line, last=None):
    """A worker's log line as ``{"worker", "time", "level", "text"}``. A
    line that does not start with a time and level (a traceback, a long
    message's next line) carries on the one before it: its level, no time."""
    m = LOG_RE.match(line)
    if m:
        return {"worker": worker, "time": m.group(1), "level": m.group(2), "text": m.group(3)}
    return {"worker": worker, "time": None, "level": (last or {}).get("level") or "INFO",
            "text": line, "cont": True}


def tail(path, limit=LOG_LINES, before=None, block=65536):
    """The last `limit` complete lines of `path` that end at or before byte
    `before` (default: the last newline): ``(lines, start, end)``, where
    `start` is where the first line begins (pass it as `before` for the
    lines before those) and `end` where the lines read end. Reads from the
    end, a block at a time, never the whole of a large file."""
    try:
        size = os.path.getsize(path)
    except OSError:
        return [], 0, 0
    with open(path, "rb") as fh:
        if before is None:
            fh.seek(max(0, size - block))
            data = fh.read()
            nl = data.rfind(b"\n")
            end = size - len(data) + nl + 1 if nl != -1 else 0
        else:
            end = max(0, min(int(before), size))
        pos, buf = end, b""
        while pos > 0 and buf.count(b"\n") <= limit:
            step = min(block, pos)
            pos -= step
            fh.seek(pos)
            buf = fh.read(step) + buf
    lines = buf.split(b"\n")
    if buf.endswith(b"\n"):
        lines = lines[:-1]
    if pos > 0:
        lines = lines[1:]          # the first may be the end of an earlier line
    lines = lines[-limit:] if limit else []
    start = end - sum(len(l) + 1 for l in lines)
    return [l.decode("utf-8", errors="replace") for l in lines], start, end


def log_data(state, prefix, worker=None, before=None, limit=LOG_LINES, level=None):
    """The last lines of the run's logs, oldest first, with where each
    worker's file was read up to (``offsets``: what the event stream goes
    on from) and from (``starts``: what "earlier" asks before). One worker,
    or every worker interleaved by time. `level`: that level and above."""
    workers = describe_run(prefix)["workers"]
    which = [int(worker)] if worker not in (None, "") else list(range(workers))
    lines, offsets, starts = [], {}, {}
    floor = LEVELS.index(level) if level in LEVELS else 0
    for n in which:
        raw, start, end = tail(log_path(prefix, n), limit, before if len(which) == 1 else None)
        offsets[n], starts[n] = end, start
        last = None
        for seq, text in enumerate(raw):
            parsed = parse_line(n, text, last)
            last = parsed
            parsed["seq"] = seq
            lines.append(parsed)
    if len(which) > 1:
        # interleave the workers by time; a continuation line sorts with the
        # line it continues (its worker's last time), so it stays after it
        current, keyed = {}, []
        for line in lines:
            if line["time"]:
                current[line["worker"]] = line["time"]
            keyed.append(((current.get(line["worker"], ""), line["worker"], line["seq"]), line))
        lines = [l for _, l in sorted(keyed, key=lambda x: x[0])][-limit:]
    lines = [l for l in lines if LEVELS.index(l["level"]) >= floor]
    for l in lines:
        l.pop("seq", None)
    return {"lines": lines, "offsets": offsets, "starts": starts, "workers": workers}


def stdout_data(prefix, limit=LOG_LINES):
    """The end of the driver's own output, ``<prefix>.out`` (a run the web
    UI started keeps it, W10)."""
    lines, start, end = tail(f"{prefix}.out", limit)
    return {"lines": lines, "start": start, "end": end}


# --- the live stream -----------------------------------------------------------------------


def sse(event, data):
    return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"


def parse_offsets(text):
    """``"0:1234,1:5678"`` -> ``{0: 1234, 1: 5678}``."""
    out = {}
    for part in (text or "").split(","):
        if ":" in part:
            n, off = part.split(":", 1)
            if n.strip().isdigit() and off.strip().isdigit():
                out[int(n)] = int(off)
    return out


async def run_events(state, prefix, offsets=None, tick=None, request=None):
    """``status`` (the run's data) every tick, ``log`` with the lines
    written since `offsets` (each worker's file read from there; a file not
    named, from its start), and ``done`` once the run is no longer running."""
    tick = state.settings.refresh if tick is None else tick
    tailer = LogTailer(prefix, 0)
    tailer.offsets = dict(offsets or {})
    last = {}
    while True:
        if request is not None and await request.is_disconnected():
            return
        data = await asyncio.to_thread(run_data, state, prefix, None, False)
        yield sse("status", data)
        tailer.workers = data["workers"]
        raw = await asyncio.to_thread(tailer.read_new_lines)
        if raw:
            batch = []
            for n, text in raw:
                parsed = parse_line(n, text, last.get(n))
                last[n] = parsed
                batch.append(parsed)
            yield sse("log", batch)
        if data["state"] in FINISHED:
            yield sse("done", data)
            return
        await asyncio.sleep(tick)


# --- routes -------------------------------------------------------------------------------------


@router.get("")
def get_runs(request: Request, command: str = "", state: str = ""):
    return runs_data(request.app.state.lux, command or None, state or None)


@router.get("/{rel:path}/events")
async def get_events(request: Request, rel: str, offsets: str = ""):
    state = request.app.state.lux
    prefix = full_prefix(state, rel)
    with _streams["lock"]:
        if _streams["count"] >= STREAM_LIMIT:
            raise HTTPException(status_code=429, detail="too many live pages open; close one")
        _streams["count"] += 1

    async def stream():
        try:
            async for chunk in run_events(state, prefix, parse_offsets(offsets), request=request):
                yield chunk
        finally:
            with _streams["lock"]:
                _streams["count"] -= 1

    return StreamingResponse(stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache"})


@router.get("/{rel:path}/log")
def get_log(request: Request, rel: str, worker: str = "", before: int | None = None,
            limit: int = LOG_LINES, level: str = ""):
    state = request.app.state.lux
    return log_data(state, full_prefix(state, rel), worker or None, before,
                    max(1, min(limit, 5000)), level or None)


@router.get("/{rel:path}/stdout")
def get_stdout(request: Request, rel: str):
    state = request.app.state.lux
    return stdout_data(full_prefix(state, rel))


@router.get("/{rel:path}")
def get_run(request: Request, rel: str):
    state = request.app.state.lux
    return run_data(state, full_prefix(state, rel))
