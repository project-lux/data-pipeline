"""The health page: lux testinstall's checks, re-run on request."""

from fastapi import APIRouter, Form, Request
from fastapi.responses import RedirectResponse

from lux_pipeline.web.api.health import health_data, start_health
from lux_pipeline.web.app import page

router = APIRouter(include_in_schema=False)
NAV = ("Health", "/health")


@router.get("/health")
def health(request: Request):
    return page(request, "health/index.html", health=health_data(request.app.state.lux))


@router.post("/health/run")
def run(request: Request, workers: str = Form(""), pg_tune: str = Form("")):
    start_health(request.app.state.lux, workers=bool(workers), pg_tune=bool(pg_tune))
    return RedirectResponse("/health", status_code=303)
