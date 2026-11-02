"""Records, as the pages show them: anything typed resolved to a reference
and followed through every cache and the identity map
(`lux_pipeline.records`, W05); a cache's keys sampled; a record checked
against the schemas.

Only what a lookup reads is built: the reference's source, and the
results for the merged and MarkLogic records (`AppState.source`, once
each). Mapping a record now (``mapped``) builds every source and the
globals once (`AppState.build_all`), since mappers reach other sources.
The idmap is the server's, its memory cache off.
"""

import datetime
import json

from fastapi import APIRouter, HTTPException, Request

from lux_pipeline import records
from lux_pipeline.cli.show import SAMPLE_CACHES

router = APIRouter(prefix="/api/records", tags=["records"])

STAGES = ("datacache", "recordcache", "reconciled", "mapped", "identity", "rewritten", "merged",
          "marklogic")
# the stages a YUID has: no source of its own
YUID_STAGES = ("identity", "merged", "marklogic")
RECENT = 20


def jsonable(obj):
    """`obj` as plain JSON (times as ISO strings), so a template or a
    response can carry it."""
    return json.loads(json.dumps(obj, default=lambda o: o.isoformat(sep=" ")
                                 if hasattr(o, "isoformat") else str(o)))


def resolve_data(state, q, source=None):
    return records.resolve(state.configs(), q, source=source or None).as_dict()


def _build_for(state, ref, mapped, notes):
    if mapped:
        try:
            state.build_all()
        except Exception as e:
            notes.append(f"could not build every source to map with: {type(e).__name__}: {e}")
        return
    names = [ref.source] if ref.source else []
    cfgs = state.configs()
    names += [n for n in ("merged", "marklogic") if n in cfgs.results]
    for name in names:
        try:
            state.source(name)
        except Exception as e:
            notes.append(f"could not open {name}'s stores: {type(e).__name__}: {e}")


def journey_data(state, q, source=None, mapped=False):
    """The journey of whatever `q` names, as JSON, with for each stage
    found its key, row, metadata and size."""
    cfgs = state.configs()
    ref = records.resolve(cfgs, q, source=source or None)
    pre = []
    _build_for(state, ref, mapped, pre)
    try:
        idmap = state.idmap()
    except Exception as e:
        idmap = None
        pre.append(f"no identity map: {type(e).__name__}: {e}")
    j = records.journey(cfgs, ref, mapped=mapped, idmap=idmap)
    j["notes"] = pre + j["notes"]
    stages = {}
    for stage in STAGES:
        entry = j.get(stage)
        applies = stage in YUID_STAGES or ref.kind != "yuid"
        if stage == "reconciled" and ref.group != "external":
            applies = False
        if stage in ("merged", "marklogic") and stage not in cfgs.results:
            applies = False
        if stage == "mapped" and not mapped:
            applies = False
        info = {"applies": applies, "found": entry is not None}
        if entry is not None and stage == "identity":
            info.update(entry)
        elif entry is not None and stage == "mapped":
            info["records"] = entry["records"]
            info["size"] = sum(len(json.dumps(r.get("data"), default=str)) for r in entry["records"])
        elif entry is not None:
            row = entry["row"]
            info["key"] = entry["key"]
            info["data"] = row.get("data")
            info["meta"] = {k: row.get(k) for k in ("insert_time", "record_time", "refresh_time",
                                                     "valid", "change") if row.get(k) is not None}
            info["size"] = len(json.dumps(row.get("data"), default=str))
        stages[stage] = info
    remember(state, q, source, ref)
    return jsonable({"ref": j["ref"], "notes": j["notes"], "stages": stages, "mapped": mapped})


def remember(state, q, source, ref):
    entry = {"q": q, "source": source or None, "kind": ref.kind, "label": ref.uri or ref.uuid}
    lst = [e for e in state.recent_lookups if (e["q"], e["source"]) != (q, source or None)]
    state.recent_lookups[:] = [entry] + lst[:RECENT - 1]


def stage_row(state, q, source, stage):
    """The record one stage holds, as its data (for a download)."""
    d = journey_data(state, q, source, mapped=stage == "mapped")
    info = d["stages"].get(stage)
    if not info or not info["found"]:
        raise HTTPException(status_code=404, detail=f"{q}: nothing in {stage}")
    if stage == "identity":
        return {"yuid": info["yuid"], "members": info["members"]}
    if stage == "mapped":
        return [r.get("data") for r in info["records"]]
    return info["data"]


def validate_data(state, q, source, stage):
    """``{"valid", "errors"}`` for the record a stage holds.
    FileNotFoundError when the schemas are missing (409 on the page)."""
    if stage in ("identity",):
        raise ValueError("the identity is not a record to validate")
    data = stage_row(state, q, source, stage)
    rows = data if stage == "mapped" else [data]
    errors = [e for row in rows for e in records.validate(state.configs(), {"data": row})]
    return {"stage": stage, "valid": not errors, "errors": errors}


def browse_options(state):
    cfgs = state.configs()
    return {"groups": {g: list(getattr(cfgs, g)) for g in ("internal", "external", "results")},
            "caches": list(SAMPLE_CACHES), "types": [*cfgs.ok_record_types, "Concept"]}


def sample_data(state, source, cache="recordcache", typ=None, since=None, n=50):
    """Up to `n` keys of one of `source`'s caches, each with the reference
    that looks it up."""
    if cache not in SAMPLE_CACHES:
        raise ValueError(f"a source's caches are {', '.join(SAMPLE_CACHES)}, not {cache!r}")
    cfg = state.source(source)
    store = cfg.get(SAMPLE_CACHES[cache])
    if store is None:
        raise ValueError(f"{source} has no {cache}")
    when = None
    if since:
        try:
            when = datetime.datetime.fromisoformat(since)
        except ValueError:
            raise ValueError(f"since: {since!r} is not a date and time") from None
    keys = records.sample(store, max(1, min(int(n), 1000)), typ=typ or None, since=when)
    results = cfg.get("type") == "results"
    return {"source": source, "cache": cache, "type": typ or None, "since": since or None,
            "results": [{"key": k, "q": k, "source": None if results else source} for k in keys],
            "cost": "reading every record to filter by type: on a large cache this takes a while"
            if typ else None}


# --- routes ------------------------------------------------------------------------------


@router.get("/resolve")
def get_resolve(request: Request, q: str, source: str = ""):
    return resolve_data(request.app.state.lux, q, source)


@router.get("/journey")
def get_journey(request: Request, q: str, source: str = "", mapped: bool = False):
    return journey_data(request.app.state.lux, q, source, mapped)


@router.get("/sample")
def get_sample(request: Request, source: str, cache: str = "recordcache", type: str = "",
               since: str = "", n: int = 50):
    return sample_data(request.app.state.lux, source, cache, type, since, n)


@router.get("/validate")
def get_validate(request: Request, q: str, source: str = "", stage: str = "recordcache"):
    try:
        return validate_data(request.app.state.lux, q, source, stage)
    except FileNotFoundError as e:
        raise RuntimeError(f"{e} (lux initialize fetches the schemas)") from None
