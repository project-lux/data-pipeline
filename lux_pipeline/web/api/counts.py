"""What is in the caches, maps and indexes: `lux counts`, row by row.

The targets are resolved exactly as `lux counts` resolves them (its
handler's ``resolve_*``), and each row counted with the same functions
(`cli/counts.py`): a Postgres cache's estimate above 100,000 rows, an
exact count below. Counting a large installation takes a while, so it
runs only when asked: streamed as server-sent events (a ``row`` per cache,
map or index as it is counted, then ``done`` with the totals), or all at
once for a page without scripts.
"""

import asyncio
import json
import time
from argparse import Namespace

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from lux_pipeline.cli import counts
from lux_pipeline.web.actions import record

router = APIRouter(prefix="/api/counts", tags=["counts"])
SECTIONS = ("caches", "maps", "indexes")


def targets(state, source="", cache="", map="", index=""):
    """``(sources, caches, maps, indexes)`` as `lux counts` resolves them;
    each source built first (counting needs its caches)."""
    cfgs = state.configs()
    h = counts.CommandHandler(cfgs)
    args = Namespace(source=source or None, cache=cache or None, map=map or None,
                     index=index or None)
    sources = h.resolve_sources(args)
    for _, cfg in sources:
        state.source(cfg["name"])
    return sources, h.resolve_caches(args), (h.resolve_maps(args) if map else None), \
        (h.resolve_indexes(args) if index else None)


def iter_rows(state, source="", cache="", map="", index=""):
    """``{"section", "cells", "count", ...}`` for every cache, map and index
    counted, one at a time, in `lux counts`'s order."""
    sources, caches, maps, indexes = targets(state, source, cache, map, index)
    cfgs = state.configs()
    for group, cfg in sources:
        rows, _ = counts.count_caches([(group, cfg)], caches)
        for g, name, c, n, exact in rows:
            yield {"section": "caches", "cells": [g, name, c, counts._fmt(n, exact)], "count": n}
    for m in maps or []:
        rows, _ = counts.count_maps(cfgs, [m])
        for name, n in rows:
            yield {"section": "maps", "cells": [name, f"~{n:,}"], "count": n}
    for idx in indexes or []:
        errors = {}
        rows, _ = counts.count_indexes([idx], counts.open_index_file, errors)
        for g, name, label, n in rows:
            text = "missing" if n is None else counts._fmt(n, True)
            yield {"section": "indexes", "cells": [g, name, label, text], "count": n or 0,
                   "error": errors.get((name, label))}


def totals(rows):
    out = {}
    for s in SECTIONS:
        mine = [r for r in rows if r["section"] == s]
        if mine:
            total = sum(r["count"] or 0 for r in mine)
            out[s] = f"={total:,}" if s == "indexes" else f"~{total:,}"
    return out


def counts_data(state, source="", cache="", map="", index=""):
    """Everything at once, as the page without scripts and the API show it;
    kept as the last count."""
    started = time.time()
    with state.only_one("counts"):
        rows = list(iter_rows(state, source, cache, map, index))
    result = {"rows": rows, "totals": totals(rows), "counted_at": time.time(),
              "seconds": time.time() - started,
              "asked": {"source": source, "cache": cache, "map": map, "index": index}}
    state.last_counts = result
    return result


def sse(event, data):
    return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"


async def count_events(state, source="", cache="", map="", index=""):
    """The rows as they are counted; one count at a time (a second one is
    told so in a ``failed`` event)."""
    started = time.time()
    try:
        state.claim("counts")
    except RuntimeError as e:
        yield sse("failed", {"error": str(e)})
        return
    try:
        it = await asyncio.to_thread(iter_rows, state, source, cache, map, index)
        rows = []
        done = object()
        while True:
            try:
                row = await asyncio.to_thread(next, it, done)
            except Exception as e:
                yield sse("failed", {"error": f"{type(e).__name__}: {e}"})
                return
            if row is done:
                break
            rows.append(row)
            yield sse("row", row)
    finally:
        state.release("counts")
    result = {"rows": rows, "totals": totals(rows), "counted_at": time.time(),
              "seconds": time.time() - started,
              "asked": {"source": source, "cache": cache, "map": map, "index": index}}
    state.last_counts = result
    yield sse("done", {"totals": result["totals"], "seconds": result["seconds"]})


@router.get("")
def get_counts(request: Request, source: str = "", cache: str = "", map: str = "", index: str = ""):
    record(request, "counts.run", source=source, cache=cache, map=map, index=index)
    return counts_data(request.app.state.lux, source, cache, map, index)


@router.get("/events")
async def get_events(request: Request, source: str = "", cache: str = "", map: str = "",
                     index: str = ""):
    state = request.app.state.lux
    # resolve now, so a bad name is a 400 before the stream starts
    await asyncio.to_thread(targets, state, source, cache, map, index)
    record(request, "counts.run", source=source, cache=cache, map=map, index=index)
    return StreamingResponse(count_events(state, source, cache, map, index),
                             media_type="text/event-stream", headers={"Cache-Control": "no-cache"})
