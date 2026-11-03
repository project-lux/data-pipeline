"""The action log page: what the web UI changed or started, newest first."""

from fastapi import APIRouter, Request

from lux_pipeline.web.actions import SHOWN, actions_path, read_actions
from lux_pipeline.web.app import page

router = APIRouter(include_in_schema=False)
NAV = ("Actions", "/actions")


@router.get("/actions")
def actions(request: Request):
    state = request.app.state.lux
    rows = []
    for e in read_actions(state):
        details = {k: v for k, v in e.items() if k not in ("time", "action", "remote")}
        rows.append({"time": e.get("time"), "action": e.get("action"), "remote": e.get("remote"),
                     "details": details})
    return page(request, "actions/index.html", rows=rows, path=actions_path(state), shown=SHOWN)
