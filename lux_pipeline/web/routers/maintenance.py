"""The Postgres page: lux pg-tune's advice and lux pg-maintain --bloat's
report, both read only."""

from fastapi import APIRouter, Request

from lux_pipeline.cli.pg_maintain import RESULT_CACHES, SOURCE_CACHES
from lux_pipeline.web.api.maintenance import iter_bloat, tune_data
from lux_pipeline.web.app import page

router = APIRouter(include_in_schema=False)
NAV = ("Maintenance", "/maintenance")


@router.get("/maintenance")
def maintenance(request: Request, workers: int | None = None, bloat: bool = False,
                source: str = "", cache: str = ""):
    state = request.app.state.lux
    try:
        tune, tune_error = tune_data(state, workers), None
    except Exception as e:                     # the machine or the server could not be read
        tune, tune_error = None, f"{type(e).__name__}: {e}"
    rows = skipped = None
    error = None
    if bloat:
        try:
            got = list(iter_bloat(state, source, cache))
            rows = [r for r in got if "skipped" not in r]
            skipped = [r["skipped"] for r in got if "skipped" in r]
        except Exception as e:
            error = f"{type(e).__name__}: {e}"
    cfgs = state.configs()
    return page(request, "maintenance/index.html", tune=tune, tune_error=tune_error, rows=rows,
                skipped=skipped, error=error, asked={"source": source, "cache": cache},
                caches=sorted(set(SOURCE_CACHES) | set(RESULT_CACHES)),
                groups={g: list(getattr(cfgs, g)) for g in ("internal", "external", "results")})
