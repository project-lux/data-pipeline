"""The runs: a list of every run under log_dir, and one run as the console
view shows it, live while it runs."""

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

from lux_pipeline.progress import list_runs
from lux_pipeline.web.api.runs import (FINISHED, LEVELS, full_prefix, log_data, log_dir, rel_prefix,
                                       run_data, runs_data)
from lux_pipeline.web.app import page

router = APIRouter(include_in_schema=False)
NAV = ("Runs", "/runs")


@router.get("/runs")
def runs(request: Request, command: str = "", state: str = ""):
    return page(request, "runs/index.html", data=runs_data(request.app.state.lux, command or None,
                                                           state or None),
                command=command, state=state,
                states=("running", "finished", "failed", "interrupted", "killed", "unknown"))


@router.get("/runs/_latest")
def latest(request: Request):
    state = request.app.state.lux
    found = list_runs(log_dir(state))
    rel = rel_prefix(state, found[0]["prefix"]) if found else None
    return RedirectResponse(f"/runs/{rel}" if rel else "/runs", status_code=303)


@router.get("/runs/{rel:path}")
def run(request: Request, rel: str):
    state = request.app.state.lux
    prefix = full_prefix(state, rel)
    data = run_data(state, prefix)
    log = log_data(state, prefix)
    live = data["state"] not in FINISHED
    script = {"rel": data["rel"], "live": live, "offsets": log["offsets"],
              "starts": log["starts"], "workers": data["workers"]}
    return page(request, "runs/run.html", r=data, log=log, live=live, levels=LEVELS, script=script)
