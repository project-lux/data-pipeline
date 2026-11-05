"""The configuration as the pages show it: every record, by type and file;
one record with a form field per key the catalogue knows; and checking,
diffing and writing, through the `ConfigCache` each record is in
(`lux_pipeline.config_store.find`), which writes exactly what `lux config`
writes.

Two ways to save, as `lux config` has: the fields (`write`) and the
record's text (`write_text`). Either way the record is
stored as its store writes JSON: normalized (2-space indent) unless the
store says otherwise. After a write the server's configuration is rebuilt
(`AppState.reset`). Every write is refused while a build runs, unless
forced, as `lux config` refuses it.
"""

import difflib
import json
import os

from fastapi import APIRouter, HTTPException, Request

from lux_pipeline import config_schema as schema
from lux_pipeline import config_store as store
from lux_pipeline.cli._handler import refuse_while_building
from lux_pipeline.cli.list import source_rows

router = APIRouter(prefix="/api/config", tags=["config"])

GROUPS = (("base", "Base", ("base", "system")), ("caches", "Cache settings", ("caches",)),
          ("default", "Defaults", ("default",)), ("globals", "Globals", ("globals",)),
          ("internal", "Internal sources", ("internal",)),
          ("external", "External sources", ("external",)), ("results", "Results", ("results",)),
          ("map", "Maps", ("map",)), ("marklogic", "MarkLogic stores", ("marklogic",)),
          ("config", "Sub-config stores", ("config",)))
TEXT_TYPES = ("str", "path", "url", "class")


# --- the list -------------------------------------------------------------------------


def _short(value):
    return value.rsplit(".", 1)[-1] if isinstance(value, str) else ""


def summary(data):
    """One line about a record, for the list."""
    typ = data.get("type")
    if typ in schema.SOURCE_TYPES:
        return data.get("namespace", "")
    if typ == "map":
        return _short(data.get("storeClass"))
    if typ in ("base", "system", "config"):
        return data.get("base_dir", "")
    if typ == "caches":
        return f"{data.get('host') or 'local socket'}, database {data.get('dbname') or '(user)'}"
    if typ == "marklogic":
        return data.get("rest", "")
    if typ == "default":
        return f"defaults for {data.get('name')}"
    if typ == "globals":
        return f"{len(data) - 1} terms"
    return ""


def building(cfgs):
    """Why a write would be refused now (a build is running), or None."""
    try:
        refuse_while_building(cfgs, "config")
    except RuntimeError as e:
        return str(e)
    return None


def config_index(state):
    cfgs = state.configs()
    rows = store.records(cfgs)
    by_type = {}
    for row in rows:
        try:
            data = store.find(cfgs, row["key"]).read(row["key"]) if not row["error"] else {}
        except store.ConfigError:
            data = {}
        row["summary"] = summary(data)
        by_type.setdefault(row["type"], []).append(row)
    groups = []
    for gid, label, types in GROUPS:
        records = [r for t in types for r in by_type.pop(t, [])]
        if records:
            groups.append({"type": gid, "label": label, "records": records})
    leftover = [r for rs in by_type.values() for r in rs]
    if leftover:
        groups.append({"type": "other", "label": "Unreadable or unknown", "records": leftover})
    return {"groups": groups, "count": len(rows), "sources": source_rows(cfgs, "all"),
            "building": building(cfgs), "config_dir": cfgs.config_dir,
            "stores": list(cfgs.subconfig_stores)}


# --- one record -------------------------------------------------------------------------


def input_text(spec, value, present):
    """A value as its form input shows it."""
    if not present:
        return ""
    if spec.type in ("list", "dict", "any") or (value is not None and spec.type == "int"
                                               and not isinstance(value, int)):
        return json.dumps(value, ensure_ascii=False, indent=2)
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def resolve_path(cfgs, record_type, key, value):
    """Where a path key points, as the pipeline resolves it, and whether it
    is there: ``{"path", "exists"}``, or None when it cannot be told."""
    if not isinstance(value, str) or not value:
        return None
    if record_type in ("base", "system") and hasattr(cfgs, key):
        path = getattr(cfgs, key)
    elif os.path.isabs(value):
        path = value
    else:
        base = (getattr(cfgs, "path_types", {}) or {}).get(key)
        if not base or not getattr(cfgs, base, None):
            return None
        root = getattr(cfgs, base)
        if not os.path.isabs(root):
            root = os.path.join(cfgs.base_dir, root)
        path = os.path.join(root, value)
    return {"path": path, "exists": os.path.exists(path)}


def class_state(value):
    """``"ok"``, ``"off"`` (null), the current path for a pre-rename one,
    or the import error."""
    if value is None:
        return {"state": "off", "detail": "switched off (null)"}
    if not isinstance(value, str) or not value:
        return {"state": "error", "detail": "empty"}
    _, error, modern = store.resolve_class(value)
    if error:
        return {"state": "error", "detail": error}
    if modern:
        return {"state": "legacy", "detail": f"old path: write {modern}"}
    return {"state": "ok", "detail": ""}


def fields(cfgs, data):
    """A field per key the catalogue knows that the record has, and per
    class key with a default (shown as that default when absent)."""
    typ = data.get("type")
    keys = schema.keys_for(data)
    defaults = schema.class_defaults(typ) if typ in schema.KEYS else {}
    out = []
    for key, spec in keys.items():
        if key in ("*", "type"):
            continue
        present = key in data
        if not present and key not in defaults and not spec.required:
            continue
        value = data.get(key)
        f = {"key": key, "type": spec.type, "help": spec.help, "required": spec.required,
             "nullable": spec.nullable, "unused": spec.unused, "planned": spec.planned,
             "present": present, "value": value, "input": input_text(spec, value, present),
             "default": defaults.get(key), "resolved": None, "class_state": None}
        if spec.type == "path" and present:
            f["resolved"] = resolve_path(cfgs, typ, key, value)
        if spec.type == "class" and present:
            f["class_state"] = class_state(value)
        out.append(f)
    return out


def record_data(state, key, text=None):
    """One record for its page: the file's text (or `text`, what was typed),
    its data, the fields, the keys the catalogue does not know, and its
    problems."""
    cfgs = state.configs()
    origin = cfgs.record_origins.get(key)
    if origin is None:
        raise HTTPException(status_code=404, detail=str(store.unknown(cfgs, key)))
    file_text = store.find(cfgs, key).text(key)
    raw = json.loads(file_text)
    known = schema.keys_for(raw)
    return {
        "key": key, "type": raw.get("type"), "name": raw.get("name"), "path": origin["path"],
        "store": origin["store"], "raw": raw, "file_text": file_text,
        "text": file_text if text is None else text,
        "fields": fields(cfgs, raw),
        "unknown": {k: v for k, v in raw.items() if k not in known and "*" not in known},
        "defaults": schema.class_defaults(raw.get("type")) if raw.get("type") in schema.KEYS else {},
        "problems": store.find(cfgs, key).check(key),
        "building": building(cfgs),
    }


def schema_data(record_type):
    """The catalogue for a record type, as JSON."""
    if record_type not in schema.RECORD_TYPES and record_type != "default":
        raise ValueError(f"no record type {record_type!r}")
    keys = schema.keys_for({"type": record_type, "name": "cache"})
    return {k: {"type": s.type, "help": s.help, "required": s.required, "nullable": s.nullable,
                "unused": s.unused, "planned": s.planned, "default": s.default_for(record_type)}
            for k, s in keys.items()}


# --- turning what was submitted into a record ---------------------------------------------


def parse_text(text):
    """``(data, error)``: the text as JSON, or where it is not."""
    try:
        return json.loads(text), None
    except json.JSONDecodeError as e:
        return None, f"line {e.lineno}, column {e.colno}: {e.msg}"


def data_from_fields(cfgs, key, form):
    """The record with the fields form's values applied: ``field:<key>``
    holds a value, ``unset:<key>`` removes the key. Keys the form has no
    field for keep their value. ``(data, problems)``."""
    raw = store.find(cfgs, key).read(key)
    data = dict(raw)
    problems = []
    keys = schema.keys_for(raw)
    for k, spec in keys.items():
        name = f"field:{k}"
        if name not in form:
            continue
        if form.get(f"unset:{k}"):
            data.pop(k, None)
            continue
        text = form[name]
        if spec.type == "bool":
            if text in ("true", "false"):
                data[k] = text == "true"
            else:
                data.pop(k, None)
        elif spec.type == "int":
            if text.strip() == "":
                data.pop(k, None)
            else:
                try:
                    data[k] = int(text)
                except ValueError:
                    problems.append(f"{k}: {text!r} is not a whole number")
        elif spec.type in ("list", "dict", "any"):
            if text.strip() == "":
                data.pop(k, None)
            else:
                value, error = parse_text(text)
                if error:
                    problems.append(f"{k}: not JSON ({error})")
                else:
                    data[k] = value
        else:  # str, path, url, class
            if spec.type == "class" and text.strip() == "null":
                data[k] = None
            elif text == "" and k not in raw:
                continue
            else:
                data[k] = text
    return data, problems


# --- JSON routes ---------------------------------------------------------------------------


@router.get("")
def get_index(request: Request):
    return config_index(request.app.state.lux)


@router.get("/schema/{record_type}")
def get_schema(record_type: str):
    return schema_data(record_type)


@router.get("/{key}")
def get_record(request: Request, key: str):
    d = record_data(request.app.state.lux, key)
    d.pop("file_text")
    return d


async def _submitted(request):
    """``{"text": ...}`` or ``{"data": ...}`` from a JSON body."""
    body = await request.json()
    if not isinstance(body, dict) or not ({"text", "data"} & set(body)):
        raise ValueError('send {"text": "..."} or {"data": {...}}')
    return body


@router.post("/{key}/validate")
async def post_validate(request: Request, key: str):
    """Problems with what was typed: ``{"json_error", "problems",
    "errors", "warnings"}``."""
    cfgs = request.app.state.lux.configs()
    body = await _submitted(request)
    if "text" in body:
        data, error = parse_text(body["text"])
        if error:
            return {"json_error": error, "problems": [], "errors": 1, "warnings": 0}
    else:
        data = body["data"]
    problems = store.find(cfgs, key).validate(key, data)
    return {"json_error": None, "problems": problems,
            "errors": len(store.errors_only(problems)), "warnings": len(store.warnings_only(problems))}


@router.post("/{key}/diff")
async def post_diff(request: Request, key: str):
    """What saving would change, as a unified diff of the stored text: the
    record is written as its store writes JSON, from the text or the data.
    Text that is not JSON is diffed as it is."""
    cfgs = request.app.state.lux.configs()
    body = await _submitted(request)
    data = body.get("data")
    if "text" in body:
        data, error = parse_text(body["text"])
        if error:
            path = store.find(cfgs, key).path(key)
            old = store.find(cfgs, key).text(key)
            new = body["text"] if body["text"].endswith("\n") else body["text"] + "\n"
            return {"diff": "".join(difflib.unified_diff(old.splitlines(True), new.splitlines(True),
                                                         fromfile=path, tofile=path))}
    return {"diff": store.find(cfgs, key).diff(key, data)}
