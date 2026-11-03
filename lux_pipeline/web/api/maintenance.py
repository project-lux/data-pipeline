"""Postgres, for the operator: `lux pg-tune`'s view of the machine and the
server (what to change for the build, and the SQL), and `lux pg-maintain
--bloat`'s report of what each cache table holds. Both read only.

The actions that change tables (vacuum, rewrite, index changes) are `lux
pg-maintain` runs, so the page shows the command for each (W10's launcher
will run them); nothing here writes. The bloat report runs on request,
streamed table by table, since it reads every table's statistics.
"""

import asyncio
import json
import time
from argparse import Namespace

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from lux_pipeline.cli import pg_maintain
from lux_pipeline.cli.pg_tune import pipeline_settings
from lux_pipeline.storage import pg_tune
from lux_pipeline.web.actions import record

router = APIRouter(prefix="/api/maintenance", tags=["maintenance"])
GB = 1024 ** 3


def tune_data(state, workers=None):
    """``{"machine", "server", "why", "workers", "pending", "report"}``:
    what `lux pg-tune` prints, as data (and its text, for the SQL)."""
    cfgs = state.configs()
    db, default_workers, table = pipeline_settings(cfgs)
    workers = int(workers or default_workers)
    m, srv, why = pg_tune.analyse(db, False, table)
    args = Namespace(workers=workers, measure_io=False, io_path=None, all=False,
                     idmap_table=table, no_db=False)
    lines, _status = pg_tune.report(m, srv, why, args)
    pending = pg_tune.pending_changes(m, srv, workers)
    c, r = m["cpus"], m["ram"]
    return {
        "workers": workers, "why": why if srv is None else None,
        "machine": {"cpus": c["logical"], "physical": c["physical"], "cpus_limited_by": c["limited_by"],
                    "ram_gb": round(r["total"] / GB, 1) if r["total"] else None,
                    "ram_limited_by": r["limited_by"], "storage": m["storage"]["kind"],
                    "storage_why": m["storage"]["why"]},
        "server": None if srv is None else {"version": srv.get("version"),
                                            "data_dir": srv.get("data_dir")},
        "pending": None if pending is None else [{"name": n, "current": cur, "recommended": want}
                                                for n, cur, want in pending],
        "report": lines,
    }


def bloat_targets(state, source="", cache=""):
    cfgs = state.configs()
    h = pg_maintain.CommandHandler(cfgs)
    which = source or "all"
    from lux_pipeline.cli import _targets
    for _, cfg in (_targets.all_sources(cfgs) if which == "all" else _targets.sources(cfgs, which)):
        state.source(cfg["name"])
    return h.tables(Namespace(source=source or None, cache=cache or None))


def fmt_bytes(n):
    """As `lux pg-maintain --bloat` writes sizes (the cache's own
    bloat_line: decimal MB, GB from 1e9)."""
    from lux_pipeline.storage.cache.postgres import _fmt_bytes
    return _fmt_bytes(n)


def iter_bloat(state, source="", cache=""):
    """A row per cache table, in configuration order; ``skipped`` for one
    that is not in Postgres."""
    for name, t, store in bloat_targets(state, source, cache):
        if not callable(getattr(store, "bloat", None)):
            yield {"skipped": f"{name} {t}"}
            continue
        try:
            b = store.bloat()
        finally:
            end = getattr(store, "end_read", None)
            if end:
                end()
        free = b["free_percent"]
        dead_share = b["dead"] / max(b["live"] + b["dead"], 1) * 100 if b["dead"] else 0
        rewrite = (free or 0) > 30 or (b["dead"] and b["dead"] > b["live"] * 0.3)
        notes = []
        if not b["live"] and b["total"]:
            notes.append("no statistics: ANALYZE has never run on it")
        if rewrite:
            notes.append(f"lux pg-maintain --rewrite --source {name} --cache {t}")
        yield {"source": name, "cache": t, "table": b["name"], "live": b["live"], "dead": b["dead"],
               "total": b["total"], "rewrite": bool(rewrite),
               "cells": [name, t, f"{b['live']:,}", fmt_bytes(b["total"]), fmt_bytes(b["heap"]),
                         fmt_bytes(b["toast"]), fmt_bytes(b["indexes"]),
                         f"{free:.0f}%" if free is not None else (f"{dead_share:.0f}% dead" if b["dead"] else "-"),
                         "; ".join(notes)]}


def sse(event, data):
    return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"


def bloat_rows(state, source="", cache=""):
    """Every row of the report at once; one report at a time."""
    with state.only_one("bloat"):
        return list(iter_bloat(state, source, cache))


async def bloat_events(state, source="", cache=""):
    started = time.time()
    try:
        state.claim("bloat")
    except RuntimeError as e:
        yield sse("failed", {"error": str(e)})
        return
    try:
        it = await asyncio.to_thread(iter_bloat, state, source, cache)
        done = object()
        skipped, n = [], 0
        while True:
            try:
                row = await asyncio.to_thread(next, it, done)
            except Exception as e:
                yield sse("failed", {"error": f"{type(e).__name__}: {e}"})
                return
            if row is done:
                break
            if "skipped" in row:
                skipped.append(row["skipped"])
                continue
            n += 1
            yield sse("row", dict(row, section="bloat"))
    finally:
        state.release("bloat")
    yield sse("done", {"tables": n, "skipped": skipped, "seconds": time.time() - started})


@router.get("/tune")
def get_tune(request: Request, workers: int | None = None):
    return tune_data(request.app.state.lux, workers)


@router.get("/bloat")
def get_bloat(request: Request, source: str = "", cache: str = ""):
    record(request, "bloat.run", source=source, cache=cache)
    rows = bloat_rows(request.app.state.lux, source, cache)
    return {"rows": [r for r in rows if "skipped" not in r],
            "skipped": [r["skipped"] for r in rows if "skipped" in r]}


@router.get("/bloat/events")
async def get_bloat_events(request: Request, source: str = "", cache: str = ""):
    state = request.app.state.lux
    await asyncio.to_thread(bloat_targets, state, source, cache)
    record(request, "bloat.run", source=source, cache=cache)
    return StreamingResponse(bloat_events(state, source, cache), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache"})
