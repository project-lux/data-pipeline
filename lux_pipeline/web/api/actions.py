"""The action log as data (`web/actions.py`)."""

from fastapi import APIRouter, Request

from lux_pipeline.web.actions import SHOWN, read_actions

router = APIRouter(prefix="/api/actions", tags=["actions"])


@router.get("")
def get_actions(request: Request, limit: int = SHOWN):
    return read_actions(request.app.state.lux, max(1, min(limit, 5000)))
