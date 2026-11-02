"""The dashboard's data: the installation, its sources, what is running,
the recent runs and the last build's outcome."""

import os

from fastapi import APIRouter, Request

from lux_pipeline.cli._services import describe_postgres, postgres_settings

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])

DIRS = ("base_dir", "data_dir", "log_dir", "exports_dir", "nt_dir", "indexes_dir", "dumps_dir",
        "temp_dir")
STATUS_FILE = "pipeline_process_status.txt"


def installation(state):
    cfgs = state.configs()
    out = {k: getattr(cfgs, k, None) for k in DIRS}
    out.update(basepath=cfgs.config_dir, max_workers=getattr(cfgs, "max_workers", None),
               base_record=cfgs.base_record_key, env_file=os.environ.get("LUX_ENV_FILE"),
               postgres=describe_postgres(postgres_settings(cfgs.defaults.get("cache") or {})))
    return out


def last_build(state, lines=5):
    """The last lines `lux build` (and run-all.sh) appended to
    ``<log_dir>/pipeline_process_status.txt``, newest last."""
    log_dir = state.log_dir()
    if not log_dir:
        return []
    try:
        with open(os.path.join(log_dir, STATUS_FILE), encoding="utf-8", errors="replace") as fh:
            return [l.rstrip("\n") for l in fh.readlines()[-lines:] if l.strip()]
    except OSError:
        return []


def run_summary(run):
    """A run as the dashboard shows it: no per-worker detail."""
    p = run["progress"]
    return {"prefix": run["prefix"], "command": run["command"], "state": run["state"],
            "started": run["started"], "finished": run["finished"], "exit": run["exit"],
            "workers": run["workers"], "parent": run["parent"],
            "completed": p["completed"], "total": p["total"],
            "labels": [p["labels"][n] for n in sorted(p["labels"])]}


def dashboard_data(state):
    cfgs = state.configs()
    runs = state.runs()
    return {
        "installation": installation(state),
        "sources": {g: list(getattr(cfgs, g)) for g in ("internal", "external", "results")},
        "running": [run_summary(r) for r in runs if r["state"] == "running"],
        "recent": [run_summary(r) for r in runs[:10]],
        "last_build": last_build(state),
        "health": state.health if state.health["finished"] else None,
    }


def running_data(state):
    """What the header's badge needs, cheap enough to poll."""
    return [run_summary(r) for r in state.running()]


@router.get("")
def get_dashboard(request: Request):
    return dashboard_data(request.app.state.lux)


@router.get("/running")
def get_running(request: Request):
    return running_data(request.app.state.lux)
