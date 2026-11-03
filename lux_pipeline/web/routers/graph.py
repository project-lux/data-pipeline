"""The identity graph page: two URIs of one identity, how reconciliation
connected them."""

from fastapi import APIRouter, Request

from lux_pipeline.web.api.graph import KINDS, GraphTimeout, graph_data
from lux_pipeline.web.app import page

router = APIRouter(include_in_schema=False)
NAV = ("Graph", "/graph")


@router.get("/graph")
async def graph(request: Request, a: str = "", b: str = ""):
    a, b = a.strip(), b.strip()
    state = request.app.state.lux
    cfgs = state.configs_or_none()
    context = {"a": a, "b": b, "kinds": KINDS, "g": None, "error": None,
               "network": bool(getattr(cfgs, "allow_network", False))}
    status = 200
    if a and b:
        try:
            context["g"] = await graph_data(state, a, b)
        except ValueError as e:
            context["error"], status = str(e), 400
        except GraphTimeout as e:
            context["error"], status = str(e), 504
        except RuntimeError as e:
            context["error"], status = str(e), 409
    return page(request, "graph/index.html", status_code=status, **context)
