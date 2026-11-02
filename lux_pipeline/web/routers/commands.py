"""The command pages: the catalogue, a form per command, the build page;
starting a command and cancelling a run."""

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

from lux_pipeline.cli.build import PHASES as BUILD_PHASES
from lux_pipeline.cli.introspect import command_spec
from lux_pipeline.web.api.commands import (build_values, catalogue, command_line, form_values,
                                           preview, shown)
from lux_pipeline.web.api.runs import full_prefix, rel_prefix
from lux_pipeline.web.app import page, redirect_with

router = APIRouter(include_in_schema=False)
NAV = ("Commands", "/commands")


def sources(state):
    """What a source field offers: the groups, then every source's name."""
    cfgs = state.configs_or_none()
    names = [n for g in ("internal", "external", "results") for n in getattr(cfgs, g, {})] \
        if cfgs is not None else []
    return ["all", "internal", "external", "results", *names]


def running(state):
    """The runs going now, each linked to its page."""
    return [{"command": r["command"], "rel": rel_prefix(state, r["prefix"])}
            for r in state.running()]


def form_page(request, name, values, status_code=200, error=None):
    spec = command_spec(name)
    state = request.app.state.lux
    p = preview(spec, values)
    options = shown(spec)
    return page(request, "commands/form.html", status_code=status_code, spec=spec,
                globals_=[o for o in options if o["global"]],
                own=[o for o in options if not o["global"]], values=values, preview=p,
                error=error, sources=sources(state), running=running(state))


def build_page(request, values, status_code=200, error=None, steps=None, steps_error=None):
    spec = command_spec("build")
    state = request.app.state.lux
    skipped = set((values.get("skip") or "").split(","))
    return page(request, "commands/build.html", status_code=status_code, spec=spec, values=values,
                options=shown(spec),
                preview=preview(spec, values), error=error, steps=steps, steps_error=steps_error,
                phases=[p for p in BUILD_PHASES if p != "clear"], skipped=skipped,
                sources=sources(state), running=running(state))


def start(request, name, values, force):
    """Start the command and go to its run page; the form again, saying
    why, when it cannot."""
    state = request.app.state.lux
    spec = command_spec(name)
    again = build_page if name == "build" else (lambda req, v, **kw: form_page(req, name, v, **kw))
    try:
        tail, _, reason = command_line(spec, values)
    except ValueError as e:
        return again(request, values, status_code=400, error=str(e))
    if reason and (request.state.form.get("confirm") or "").strip() != name:
        return again(request, values, status_code=400,
                     error=f"This run {reason}: type {name} to confirm.")
    try:
        handle = request.app.state.runner.start(name, tail, force=force,
                                                 guarded=reason is not None)
    except ValueError as e:
        return again(request, values, status_code=400, error=str(e))
    except RuntimeError as e:
        return again(request, values, status_code=409, error=str(e))
    rel = rel_prefix(state, handle.prefix)
    return RedirectResponse(f"/runs/{rel}" if rel else "/runs", status_code=303)


@router.get("/commands")
def index(request: Request):
    return page(request, "commands/index.html", groups=catalogue(),
                running=running(request.app.state.lux))


@router.get("/commands/build")
def build(request: Request, preview_steps: bool = False):
    values = build_values(request.query_params) if request.query_params else {}
    steps = steps_error = None
    if preview_steps:
        try:
            tail, _, _ = command_line(command_spec("build"), values)
            steps = request.app.state.runner.dry_run(tail)
        except (RuntimeError, ValueError) as e:
            steps_error = str(e)
    return build_page(request, values, steps=steps, steps_error=steps_error)


@router.post("/commands/build")
async def start_build(request: Request):
    form = await request.form()
    request.state.form = form
    values = build_values(form)
    return start(request, "build", values, force=bool(values.get("force")))


@router.get("/commands/{name}")
def command(request: Request, name: str):
    spec = command_spec(name)
    values = form_values(spec, request.query_params) if spec["runnable"] else {}
    return form_page(request, name, values)


@router.post("/commands/{name}")
async def start_command(request: Request, name: str):
    form = await request.form()
    request.state.form = form
    values = form_values(command_spec(name), form)
    return start(request, name, values, force=bool(values.get("force")))


@router.post("/runs/{rel:path}/cancel")
async def cancel(request: Request, rel: str):
    form = await request.form()
    state = request.app.state.lux
    prefix = full_prefix(state, rel)
    if (form.get("confirm") or "").strip() != "cancel":
        return redirect_with(request, f"/runs/{rel}", "Not cancelled: type cancel to confirm", "fail")
    try:
        request.app.state.runner.cancel(prefix)
    except RuntimeError as e:
        return redirect_with(request, f"/runs/{rel}", str(e), "fail")
    return redirect_with(request, f"/runs/{rel}", "Cancelling: the run's processes were sent SIGTERM")

