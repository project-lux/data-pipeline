"""`lux testinstall`'s checks, run in a thread and streamed as they finish.

The checks build their own configuration through `lux_pipeline.runtime`
(not the server's), so a check run never disturbs what the pages use.
Starting a run while one is going is refused (409). The worker-spawn and
Postgres-tuning checks are off unless asked for: the first starts
processes, the second needs the server to answer.
"""

import asyncio
import json
import threading
import time
from argparse import Namespace

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

router = APIRouter(prefix="/api/health", tags=["health"])
TICK = 0.2


def health_data(state):
    with state.lock:
        h = dict(state.health)
        h["results"] = list(h["results"])
    return h


def run_health(state, workers=False, pg_tune=False):
    """Run the checks now, in this thread; the results land in
    ``state.health`` one by one. Returns them."""
    from lux_pipeline.cli import testinstall

    def on_result(result):
        with state.lock:
            state.health["results"].append(result)

    args = Namespace(skip_workers=not workers, skip_pg_tune=not pg_tune, workers=0)
    try:
        testinstall.CommandHandler(None).run_checks(args, out=None, stop_on_fail=False,
                                                    on_result=on_result)
    except Exception as e:
        on_result({"label": "checks", "state": "FAIL", "detail": f"{type(e).__name__}: {e}",
                   "required": True})
    finally:
        with state.lock:
            state.health["running"] = False
            state.health["finished"] = time.time()
    return health_data(state)["results"]


def start_health(state, workers=False, pg_tune=False, wait=False):
    """Start a run of the checks in a thread (or in this one, `wait`).
    RuntimeError if one is already running."""
    with state.lock:
        if state.health["running"]:
            raise RuntimeError("the checks are already running")
        state.health.update(running=True, results=[], started=time.time(), finished=None,
                            options={"workers": bool(workers), "pg_tune": bool(pg_tune)})
    if wait:
        return run_health(state, workers, pg_tune)
    threading.Thread(target=run_health, args=(state, workers, pg_tune), name="lux-health",
                     daemon=True).start()
    return None


def sse(event, data):
    return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"


async def health_events(state, tick=TICK):
    """Server-sent events: a ``result`` for every check as it finishes
    (those already finished first), then ``done`` with the whole run."""
    sent = 0
    while True:
        h = health_data(state)
        for r in h["results"][sent:]:
            yield sse("result", r)
        sent = len(h["results"])
        if not h["running"]:
            yield sse("done", h)
            return
        await asyncio.sleep(tick)


@router.get("")
def get_health(request: Request):
    return health_data(request.app.state.lux)


@router.post("/run")
def post_run(request: Request, workers: bool = False, pg_tune: bool = False):
    state = request.app.state.lux
    start_health(state, workers, pg_tune)
    return health_data(state)


@router.get("/events")
def get_events(request: Request):
    return StreamingResponse(health_events(request.app.state.lux), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache"})
