"""The configuration pages: every record; one record's fields and text,
checked and saved; new records; deleting; reloading.

The actions live under ``/config/_new`` and ``/config/_reload``: a record
key cannot start with ``_``, so they can never hide a record.
"""

from fastapi import APIRouter, Request

from lux_pipeline import config_schema as schema
from lux_pipeline import config_store as store
from lux_pipeline.cli._handler import refuse_while_building
from lux_pipeline.config_store import ConfigError
from lux_pipeline.web.api.config import (config_index, data_from_fields, parse_text,
                                         record_data)
from lux_pipeline.web.app import page, redirect_with

router = APIRouter(include_in_schema=False)
NAV = ("Configuration", "/config")


def written(path, warnings):
    note = f" ({len(warnings)} warning{'s' if len(warnings) != 1 else ''}: {'; '.join(warnings)})" \
        if warnings else ""
    return f"Wrote {path}; the configuration was reloaded{note}"


def record_page(request, key, status_code=200, *, text=None, tab="fields", error=None,
                problems=None, form=None):
    """The record page, keeping what was typed when a save is refused."""
    d = record_data(request.app.state.lux, key, text=text)
    if problems is not None:
        d["problems"] = problems
    d["errors"] = store.errors_only(d["problems"])
    d["warnings"] = [p[len("warning: "):] for p in store.warnings_only(d["problems"])]
    return page(request, "config/record.html", status_code=status_code, r=d, tab=tab,
                error=error, submitted=dict(form or {}))


@router.get("/config")
def index(request: Request):
    return page(request, "config/index.html", data=config_index(request.app.state.lux))


@router.post("/config/_reload")
async def reload(request: Request):
    form = await request.form()
    state = request.app.state.lux
    state.reset()
    n = len(state.configs().record_origins)
    return redirect_with(request, form.get("back") or "/config",
                         f"Reloaded the configuration: {n} records")


def new_page(request, status_code=200, *, record_type=None, key="", store_key="", text=None,
             error=None, problems=None):
    cfgs = request.app.state.lux.configs()
    if record_type and text is None:
        try:
            text = store.dumps(schema.template(record_type, key or "newone"))
        except ValueError as e:
            error, record_type = str(e), None
    return page(request, "config/new.html", status_code=status_code, record_type=record_type,
                types=sorted(schema.TEMPLATES), key=key, store_key=store_key, text=text,
                stores=list(cfgs.subconfig_stores), error=error, problems=problems or [])


@router.get("/config/_new")
def new(request: Request, type: str = "", key: str = "", store_key: str = ""):
    return new_page(request, record_type=type or None, key=key, store_key=store_key)


@router.post("/config/_new")
async def create(request: Request):
    form = await request.form()
    state = request.app.state.lux
    cfgs = state.configs()
    key = (form.get("key") or "").strip()
    store_key = form.get("store") or ""
    text = form.get("text") or ""
    typ = form.get("type") or None
    again = dict(record_type=typ, key=key, store_key=store_key, text=text)
    try:
        refuse_while_building(cfgs, "config", bool(form.get("force")))
    except RuntimeError as e:
        return new_page(request, 409, error=str(e), **again)
    data, error = parse_text(text)
    if error:
        return new_page(request, 400, error=f"Not JSON: {error}", **again)
    try:
        path, warnings = store.new_record(cfgs, key, data, store=store_key or None)
    except ConfigError as e:
        return new_page(request, 400, error="Not written", problems=e.problems or [str(e)], **again)
    state.reset()
    return redirect_with(request, f"/config/{key}", written(path, warnings))


@router.get("/config/{key}")
def record(request: Request, key: str, tab: str = "fields"):
    return record_page(request, key, tab=tab if tab in ("fields", "text") else "fields")


@router.post("/config/{key}")
async def save(request: Request, key: str):
    form = await request.form()
    state = request.app.state.lux
    cfgs = state.configs()
    mode = form.get("mode") if form.get("mode") in ("fields", "text") else "fields"
    text = form.get("text") if mode == "text" else None
    keep = dict(text=text, tab=mode, form=form)
    try:
        refuse_while_building(cfgs, "config", bool(form.get("force")))
    except RuntimeError as e:
        return record_page(request, key, 409, error=str(e), **keep)
    try:
        if mode == "text":
            if text == store.read_text(cfgs, key):
                return redirect_with(request, f"/config/{key}?tab=text", "No change", "info")
            _, error = parse_text(text)
            if error:
                return record_page(request, key, 400, error=f"Not JSON: {error}", **keep)
            path, warnings = store.write_text(cfgs, key, text)
        else:
            data, problems = data_from_fields(cfgs, key, form)
            if problems:
                return record_page(request, key, 400, error="Not saved", problems=problems, **keep)
            if not store.diff_record(cfgs, key, data):
                return redirect_with(request, f"/config/{key}", "No change", "info")
            path, warnings = store.write_record(cfgs, key, data)
    except ConfigError as e:
        return record_page(request, key, 400, error="Not saved", problems=e.problems or [str(e)],
                           **keep)
    state.reset()
    return redirect_with(request, f"/config/{key}" + ("?tab=text" if mode == "text" else ""),
                         written(path, warnings))


@router.post("/config/{key}/delete")
async def delete(request: Request, key: str):
    form = await request.form()
    state = request.app.state.lux
    cfgs = state.configs()
    if (form.get("confirm") or "").strip() != key:
        return record_page(request, key, 400, error=f"Not deleted: type {key} to confirm")
    try:
        refuse_while_building(cfgs, "config", bool(form.get("force")))
    except RuntimeError as e:
        return record_page(request, key, 409, error=str(e))
    path = store.delete_record(cfgs, key)
    state.reset()
    return redirect_with(request, "/config", f"Deleted {key} ({path}); the configuration was reloaded")
