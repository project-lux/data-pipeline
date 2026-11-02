"""The records pages: look anything up and see where it is in the
pipeline; sample a source's cache.

A record is named by a query string, ``/records/show?q=<anything>`` (a
URI, a typed key, a curie, a YUID) with ``&source=<name>`` for a source's
bare identifier: references are URIs, with slashes, ``#`` and ``:`` in
them, which a query carries unchanged where a path would need careful
encoding.
"""

import json
from urllib.parse import urlencode

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from lux_pipeline.web.api.records import (STAGES, browse_options, journey_data, sample_data,
                                          stage_row, validate_data)
from lux_pipeline.web.app import page

router = APIRouter(include_in_schema=False)
NAV = ("Records", "/records")


def index_page(request, status_code=200, *, q="", source="", error=None, sample=None,
               browse=None):
    state = request.app.state.lux
    return page(request, "records/index.html", status_code=status_code, q=q, source=source,
                error=error, sample=sample, browse=browse or {}, options=browse_options(state),
                recent=list(state.recent_lookups))


@router.get("/records")
def index(request: Request):
    return index_page(request)


@router.get("/records/show")
def show(request: Request, q: str = "", source: str = "", mapped: bool = False, tab: str = "",
         validate: str = ""):
    if not q.strip():
        return index_page(request)
    state = request.app.state.lux
    try:
        j = journey_data(state, q, source or None, mapped)
    except ValueError as e:                      # what was typed is not a reference
        return index_page(request, 400, q=q, source=source, error=str(e))
    check = None
    if validate in STAGES:
        try:
            check = validate_data(state, q, source or None, validate)
        except FileNotFoundError as e:
            check = {"stage": validate, "valid": None, "errors": [],
                     "missing": f"{e} (lux initialize fetches them)"}
        except ValueError as e:
            check = {"stage": validate, "valid": None, "errors": [], "missing": str(e)}
    first = next((s for s in STAGES if j["stages"][s]["found"]), "identity")
    params = {"q": q, **({"source": source} if source else {})}
    return page(request, "records/journey.html", j=j, q=q, source=source,
                tab=tab if tab in STAGES else (validate or first), check=check,
                base=urlencode(params), stages=STAGES)


@router.get("/records/raw")
def raw(request: Request, q: str, source: str = "", stage: str = "recordcache", download: bool = False):
    data = stage_row(request.app.state.lux, q, source or None, stage)
    headers = {}
    if download:
        name = "".join(c if c.isalnum() or c in "-_." else "_" for c in f"{q}-{stage}")[:120]
        headers["Content-Disposition"] = f'attachment; filename="{name}.json"'
    return JSONResponse(data, headers=headers)


@router.get("/records/browse")
def browse(request: Request, source: str = "", cache: str = "recordcache", type: str = "",
           since: str = "", n: int = 50):
    form = {"source": source, "cache": cache, "type": type, "since": since, "n": n}
    if not source:
        return index_page(request, browse=form)
    try:
        sample = sample_data(request.app.state.lux, source, cache, type, since, n)
    except ValueError as e:
        return index_page(request, 400, error=str(e), browse=form)
    return index_page(request, sample=sample, browse=form)
