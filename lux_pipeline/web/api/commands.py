"""The `lux` commands as forms: the catalogue, a command's options as
fields (from its real parser, `cli/introspect.py`, W02), the command line a
form makes, and starting it (`web/runner.py`).

A submitted form is read the same way for the live preview and for the
real start (`form_values`), and turned into a command line by
`introspect.argv_from_values`, which checks it as the command's parser
would. Options a form never shows (`introspect.HIDDEN`: the prefix, the
console view, one worker of many, ...) are the runner's.
"""

import shlex
from urllib.parse import urlencode

from fastapi import APIRouter, Request

from lux_pipeline.cli.build import PHASES as BUILD_PHASES
from lux_pipeline.cli.introspect import (GROUPS, HIDDEN, all_command_specs, argv_from_values,
                                         command_spec, confirm_reason, option_name,
                                         values_from_argv)

router = APIRouter(prefix="/api/commands", tags=["commands"])
GROUP_LABELS = {"phases": "Build phases", "steps": "Steps between and after the phases",
                "stores": "Caches, maps and indexes", "info": "Information"}


def shown(spec):
    """The options a form shows, global ones first, each with the
    ``name`` a command line spells it with."""
    return [{**o, "name": option_name(o)} for o in spec["options"] if o["dest"] not in HIDDEN]


def catalogue():
    specs = {s["name"]: s for s in all_command_specs()}
    out = []
    for gid, names in GROUPS.items():
        cmds = [{"name": n, "summary": specs[n]["summary"], "runnable": specs[n]["runnable"],
                 "confirms": specs[n]["confirms"], "runs_workers": specs[n]["runs_workers"]}
                for n in names if n in specs]
        if cmds:
            out.append({"group": gid, "label": GROUP_LABELS[gid], "commands": cmds})
    return out


def _get(form, key):
    v = form.get(key)
    return v.strip() if isinstance(v, str) else v


def form_values(spec, form):
    """``{dest: value}`` from a submitted form (or query string), for the
    options the form shows: a ticked box is its flag's value, a two-valued
    option two fields, a repeated one a line each, an optional value given
    bare a ``<dest>__bare`` box; empty fields are left out."""
    out = {}
    for o in shown(spec):
        d = o["dest"]
        if o["kind"] == "flag":
            if _get(form, d):
                out[d] = o["flag_value"]
        elif isinstance(o["nargs"], int):
            items = [v.strip() for v in form.getlist(d) if isinstance(v, str) and v.strip()] \
                if hasattr(form, "getlist") else []
            if items:
                out[d] = items
        elif o["repeat"]:
            texts = form.getlist(d) if hasattr(form, "getlist") else [form.get(d) or ""]
            items = [line.strip() for t in texts if isinstance(t, str)
                     for line in t.splitlines() if line.strip()]
            if items:
                out[d] = items
        else:
            value = _get(form, d)
            if value:
                out[d] = value
            elif o["nargs"] == "?" and o["const"] is not None and _get(form, f"{d}__bare"):
                out[d] = True
    return out


def build_values(form):
    """`lux build`'s values from its page: the phases ticked become
    ``--skip`` for those that are not (clear is ``--clear``)."""
    spec = command_spec("build")
    values = form_values(spec, form)
    if hasattr(form, "getlist") and "phases_shown" in form:
        ticked = set(form.getlist("phase"))
        skip = [p for p in BUILD_PHASES if p != "clear" and p not in ticked]
        if skip:
            values["skip"] = ",".join(skip)
        else:
            values.pop("skip", None)
    values.pop("dry_run", None)
    return values


def command_line(spec, values):
    """``(tail, shown text, confirmation reason or None)``: the arguments
    after the command, the whole command line as it will run (``lux``, the
    runner's ``--no-ui --prefix``), and why it needs confirming.
    ValueError for values the command's parser would refuse."""
    argv = argv_from_values(spec, values)
    tail = argv[1:]
    text = "lux " + shlex.join(argv) + " --no-ui --prefix <log_dir>/" + spec["name"] + "-<timestamp>"
    return tail, text, confirm_reason(spec, values)


def preview(spec, values):
    """What the form would run, for the page as it is typed in."""
    try:
        _, text, reason = command_line(spec, values)
        return {"command_line": text, "confirm": reason, "error": None}
    except ValueError as e:
        return {"command_line": None, "confirm": None, "error": str(e)}


def run_again_url(command, argv):
    """A link to the form, filled in as the run was."""
    try:
        spec = command_spec(command)
        if not spec["runnable"]:
            return None
        values = values_from_argv(spec, argv)
    except (ValueError, SystemExit):
        return None
    query = []
    for k, v in values.items():
        if isinstance(v, list):
            query += [(k, x) for x in v]
        elif isinstance(v, bool):
            query.append((k, "1"))          # a flag ticked (store_false: False)
        else:
            query.append((k, str(v)))
    if command == "build":
        skip = set((values.get("skip") or "").split(","))
        query = [(k, v) for k, v in query if k != "skip"]
        query += [("phases_shown", "1")] + [("phase", p) for p in BUILD_PHASES
                                             if p != "clear" and p not in skip]
    return f"/commands/{command}" + (f"?{urlencode(query)}" if query else "")


@router.get("")
def get_catalogue():
    return catalogue()


@router.get("/{name}")
def get_spec(name: str):
    return command_spec(name)


@router.post("/{name}/preview")
async def post_preview(request: Request, name: str):
    """The command line for a form (what the page sends as it is typed
    in: the error, if any, in the answer) or for JSON ``{"values": {dest:
    value}}`` (a value the command cannot take is a 400)."""
    spec = command_spec(name)
    if "json" in request.headers.get("content-type", ""):
        body = await request.json()
        values = body.get("values", body) if isinstance(body, dict) else None
        if not isinstance(values, dict):
            raise ValueError('send {"values": {option: value}}')
        tail, text, reason = command_line(spec, values)
        return {"argv": [name, *tail], "command_line": text, "confirm": reason, "error": None}
    form = await request.form()
    values = build_values(form) if name == "build" else form_values(spec, form)
    return preview(spec, values)


@router.post("/build/dry-run")
async def post_dry_run(request: Request):
    form = await request.form()
    values = build_values(form)
    tail, _, _ = command_line(command_spec("build"), values)
    return {"steps": request.app.state.runner.dry_run(tail)}
