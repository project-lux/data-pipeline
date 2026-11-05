"""The timing report page, and two runs compared."""

from fastapi import APIRouter, Request

from lux_pipeline.process.utils.timing_report import DEFAULT_TOP, PHASE_ORDER
from lux_pipeline.web.api.timing import compare_view, timing_data
from lux_pipeline.web.app import page

router = APIRouter(include_in_schema=False)
NAV = ("Timing", "/timing")


@router.get("/timing")
def timing(request: Request, phase: str = "", profile: int | None = None, dir: str = ""):
    data = timing_data(request.app.state.lux, phase, profile, dir or None)
    return page(request, "timing/index.html", data=data, phase=phase, profile=profile,
                dir=dir, phases=PHASE_ORDER, default_top=DEFAULT_TOP)


@router.get("/timing/compare")
def compare(request: Request, before: str = "", after: str = "", phase: str = ""):
    data = compare_view(request.app.state.lux, before, after, phase) if before and after else None
    return page(request, "timing/compare.html", data=data, before=before, after=after, phase=phase)
