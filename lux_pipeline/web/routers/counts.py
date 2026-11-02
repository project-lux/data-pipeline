"""The counts page: lux counts, run when asked."""

from fastapi import APIRouter, Request

from lux_pipeline.cli.counts import DEFAULT_CACHES
from lux_pipeline.web.api.counts import counts_data
from lux_pipeline.web.app import page

router = APIRouter(include_in_schema=False)
NAV = ("Counts", "/counts")


@router.get("/counts")
def counts(request: Request, source: str = "", cache: str = "", map: str = "", index: str = "",
           go: bool = False):
    state = request.app.state.lux
    asked = {"source": source, "cache": cache, "map": map, "index": index}
    error = None
    result = state.last_counts
    if go:
        try:
            result = counts_data(state, **asked)
        except ValueError as e:
            error = str(e)
    cfgs = state.configs()
    return page(request, "counts/index.html", status_code=400 if error else 200, result=result,
                asked=asked, error=error, caches=DEFAULT_CACHES, maps=list(cfgs.map_stores),
                groups={g: list(getattr(cfgs, g)) for g in ("internal", "external", "results")})
