"""`create_app`: the FastAPI application `lux serve` runs.

Each area of the UI is two modules with the same name: ``web.api.<area>``
(the data functions and their JSON routes under ``/api/<area>``) and
``web.routers.<area>`` (the pages, rendered from the same functions).
`AREAS` lists them, in navigation order; an area module that defines
``NAV = (label, href)`` gets a navigation entry. Later items add their
area here, one line each.
"""

import importlib
import logging
import secrets
import time
from collections import OrderedDict
from importlib import metadata
from pathlib import Path

from fastapi import Depends, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.exceptions import HTTPException as StarletteHTTPException

from lux_pipeline import runtime
from lux_pipeline.process.timing_report import fmt_duration, fmt_time

from .security import TOKEN_COOKIE, new_csrf_token, require_csrf, require_token
from .settings import Settings
from .state import AppState, ConfigUnavailable

logger = logging.getLogger("lux_pipeline")

HERE = Path(__file__).parent
# the areas, in navigation order (web.api.<name> and web.routers.<name>)
AREAS = ("dashboard", "runs", "records", "counts", "timing", "config", "maintenance", "health")
# messages kept for the page a POST redirects to (?flash=<id>)
FLASH_LIMIT = 200

INSTALL_HINT = ("Use 'lux initialize <dir>' to create an installation, or 'lux testinstall' "
                "to see what is wrong with this one.")


def _version():
    try:
        return metadata.version("lux-pipeline")
    except metadata.PackageNotFoundError:
        return "dev"


def fmt_n(n):
    return "-" if n is None else f"{n:,}"


def wants_json(request):
    """A request for data rather than a page: under /api, or asking for JSON."""
    if request.url.path.startswith("/api/"):
        return True
    accept = request.headers.get("accept", "")
    return "application/json" in accept and "text/html" not in accept


def flash(request, message, kind="ok"):
    """Keep `message` for the next page; returns the id to redirect with
    (``?flash=<id>``). Kept in the server, so a link cannot make a page say
    something the server did not."""
    flashes = request.app.state.flashes
    fid = secrets.token_urlsafe(8)
    flashes[fid] = {"message": message, "kind": kind}
    while len(flashes) > FLASH_LIMIT:
        flashes.popitem(last=False)
    return fid


def redirect_with(request, url, message, kind="ok"):
    """303 to `url` (a path on this server) showing `message` there."""
    from fastapi.responses import RedirectResponse
    if not url.startswith("/") or url.startswith("//"):
        url = "/"
    sep = "&" if "?" in url else "?"
    return RedirectResponse(f"{url}{sep}flash={flash(request, message, kind)}", status_code=303)


def page(request, template, status_code=200, **context):
    """Render `template` with what every page has: the navigation (the
    current area marked), the installation, the CSRF token, and a message
    a redirect left (`redirect_with`)."""
    app = request.app
    state = app.state.lux
    path = request.url.path
    nav = [{"label": label, "href": href,
            "active": path == href or (href != "/" and path.startswith(href))}
           for label, href in app.state.nav]
    cfgs = state.configs_or_none() if context.pop("with_configs", True) else None
    fid = request.query_params.get("flash")
    context.setdefault("flash", app.state.flashes.pop(fid, None) if fid else None)
    context.update(request=request, nav=nav, settings=state.settings, csrf_token=app.state.csrf,
                   version=app.state.version, basepath=runtime.get_basepath() or None,
                   config_ok=cfgs is not None)
    return app.state.templates.TemplateResponse(request, template, context,
                                                status_code=status_code)


def error_response(request, status, message, hint=None):
    if wants_json(request):
        return JSONResponse({"error": message, "status": status, "hint": hint}, status_code=status)
    return page(request, "error.html", status_code=status, status=status, message=message,
                hint=hint, with_configs=False)


def make_templates():
    templates = Jinja2Templates(directory=str(HERE / "templates"))
    env = templates.env
    env.globals.update(fmt_time=lambda t: fmt_time(t) if t else "-",
                       fmt_duration=lambda s: fmt_duration(s) if s is not None else "-",
                       fmt_n=fmt_n)
    return templates


def create_app(settings=None):
    """The application, with `settings` (default: `Settings()`)."""
    settings = settings or Settings()
    app = FastAPI(title="LUX data pipeline", version=_version(),
                  dependencies=[Depends(require_token), Depends(require_csrf)],
                  docs_url="/api/docs", openapi_url="/api/openapi.json", redoc_url=None)
    app.state.lux = AppState(settings)
    app.state.csrf = new_csrf_token()
    app.state.version = _version()
    app.state.templates = make_templates()
    app.state.nav = []
    app.state.flashes = OrderedDict()
    app.mount("/static", StaticFiles(directory=str(HERE / "static")), name="static")

    for area in AREAS:
        api = importlib.import_module(f"lux_pipeline.web.api.{area}")
        pages = importlib.import_module(f"lux_pipeline.web.routers.{area}")
        app.include_router(api.router)
        app.include_router(pages.router)
        if getattr(pages, "NAV", None):
            app.state.nav.append(pages.NAV)

    @app.get("/healthz", include_in_schema=False)
    def healthz():
        """Answers without reading the configuration, so a monitor can tell
        the server is up even when the installation is broken."""
        return {"ok": True, "basepath": runtime.get_basepath() or None,
                "uptime": round(time.time() - app.state.lux.started, 1)}

    @app.middleware("http")
    async def remember_token(request: Request, call_next):
        """``?token=...`` once: the browser keeps it as a cookie."""
        response = await call_next(request)
        token = settings.token
        if token and request.query_params.get("token") == token:
            response.set_cookie(TOKEN_COOKIE, token, httponly=True, samesite="strict")
        return response

    @app.exception_handler(ConfigUnavailable)
    async def no_config(request, exc):
        return error_response(request, 503, str(exc), INSTALL_HINT)

    @app.exception_handler(ValueError)
    async def bad_value(request, exc):
        return error_response(request, 400, str(exc))

    @app.exception_handler(RuntimeError)
    async def refused(request, exc):
        # refuse_while_building, a check already running, ...
        return error_response(request, 409, str(exc))

    @app.exception_handler(StarletteHTTPException)
    async def http_error(request, exc):
        return error_response(request, exc.status_code, str(exc.detail))

    @app.exception_handler(RequestValidationError)
    async def invalid(request, exc):
        return error_response(request, 422, "; ".join(
            f"{'.'.join(str(p) for p in e.get('loc', ()))}: {e.get('msg')}" for e in exc.errors()))

    @app.exception_handler(Exception)
    async def failed(request, exc):
        logger.error(f"{request.method} {request.url.path} failed: {type(exc).__name__}: {exc}",
                     exc_info=settings.debug)
        message = f"{type(exc).__name__}: {exc}" if settings.debug else \
            "something went wrong; the server's log says what"
        return error_response(request, 500, message)

    return app


def create_app_from_env():
    """For ``uvicorn --factory`` (``lux serve --reload``): the settings come
    from the environment `lux serve` sets."""
    import json
    import os
    values = json.loads(os.environ.get("LUX_WEB_SETTINGS", "{}"))
    return create_app(Settings().updated(**values))
