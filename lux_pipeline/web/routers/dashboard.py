"""The dashboard page."""

from fastapi import APIRouter, Request

from lux_pipeline.web.api.dashboard import dashboard_data
from lux_pipeline.web.app import page

router = APIRouter(include_in_schema=False)
NAV = ("Dashboard", "/")


@router.get("/")
def dashboard(request: Request):
    return page(request, "dashboard/index.html", data=dashboard_data(request.app.state.lux))
