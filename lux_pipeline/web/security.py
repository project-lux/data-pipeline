"""What every request passes through: the token, when the server has one,
and the CSRF check on anything that changes something. With no token, a
``Host`` header that names anything but this machine is refused
(`host_allowed`, applied by a middleware in `create_app`).

Both are application-wide FastAPI dependencies (`create_app`), so no
route can be added without them. The CSRF token is one random value per
server start: pages carry it in a ``<meta name="csrf-token">`` and in a
hidden ``_csrf`` field of every form; `app.js` sends it as
``X-CSRF-Token`` with fetches. A POST, PUT, PATCH or DELETE without it is
refused (403).
"""

import hmac
import secrets

from fastapi import HTTPException, Request

SAFE_METHODS = ("GET", "HEAD", "OPTIONS")
TOKEN_COOKIE = "lux_token"
# paths that answer without the token: a monitor's check, the sign-in
# page, and the stylesheet, scripts and icon (nothing secret) it needs
OPEN_PATHS = ("/healthz", "/login", "/favicon.ico")
OPEN_PREFIXES = ("/static/",)
# every response: scripts, styles and everything else only from this
# server, never in a frame
SECURITY_HEADERS = {
    "Content-Security-Policy": "default-src 'self'; img-src 'self' data:; frame-ancestors 'none'; "
                               "base-uri 'self'; form-action 'self'",
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "same-origin",
    "X-Frame-Options": "DENY",
}
# Fetch Metadata: what every current browser says about where a request
# comes from (``Sec-Fetch-Site``). This server's own pages, or the operator
# typing the address or opening a bookmark; anything else (another site,
# another port on this machine) is elsewhere. Clients that are not
# browsers (curl, scripts) send no header and are not anyone's page.
FROM_HERE = ("same-origin", "none")


def host_name(host_header):
    """The host name of a ``Host`` header: lower case, no port, no IPv6
    brackets; "" for none."""
    host = (host_header or "").strip().lower()
    if host.startswith("["):
        return host[1:host.find("]")] if "]" in host else ""
    return host.rsplit(":", 1)[0] if host.count(":") == 1 else host


def host_allowed(settings, host_header):
    """Whether a request naming `host_header` may be answered.

    With no token the server is loopback only (`Settings.check`) and
    answers anyone who can reach it, so a web page could rebind its own
    domain to 127.0.0.1 (DNS rebinding) and read and drive this server
    from the browser of whoever opened it. Such a request names the
    page's domain in ``Host``, so only loopback names and the configured
    host are answered. With a token the browser never sends the cookie to
    another name, so any ``Host`` will do (the server can be reached by
    the machine's DNS name)."""
    from .settings import LOOPBACK
    if settings.token:
        return True
    name = host_name(host_header)
    return name in LOOPBACK or name == (settings.host or "").strip("[]").lower()


def from_elsewhere(request):
    """Whether a browser says this request comes from another site's page
    (`FROM_HERE`)."""
    site = request.headers.get("sec-fetch-site")
    return site is not None and site not in FROM_HERE


def embedded_elsewhere(request):
    """Whether another site's page is loading or fetching this server (an
    ``<img>``, a script, ``fetch``, a form posted to it) rather than the
    operator following a link to one of its pages. Refused for every path
    (`create_app`): a link in is a navigation, nothing else need be."""
    if not from_elsewhere(request):
        return False
    navigation = (request.method in ("GET", "HEAD")
                  and request.headers.get("sec-fetch-mode") == "navigate"
                  and request.headers.get("sec-fetch-dest", "document") == "document")
    return not navigation


class FromElsewhere(Exception):
    """A request that starts work, from a link on another site's page."""


def from_this_server(request: Request):
    """For a GET that starts work (counts, a bloat report, a graph replay,
    mapping a record, a scan by type, a build's dry run): refuse it when it
    comes from a link on another site, which could otherwise start it just
    by being opened. The page says so, with a link to run it from here."""
    if from_elsewhere(request):
        raise FromElsewhere()


def is_open(path):
    return path in OPEN_PATHS or path.startswith(OPEN_PREFIXES)


def token_ok(request, given):
    return _same(given, request.app.state.lux.settings.token)


def safe_next(url):
    """A path on this server to go to after signing in, or ``/``."""
    if not url or not url.startswith("/") or url.startswith("//") or "\\" in url:
        return "/"
    return url


def new_csrf_token():
    return secrets.token_urlsafe(24)


def _same(a, b):
    return bool(a) and bool(b) and hmac.compare_digest(str(a), str(b))


async def require_token(request: Request):
    """401 unless the request carries the server's token, as
    ``Authorization: Bearer <token>`` or the ``lux_token`` cookie (set by
    ``/login``, or by opening any page once with ``?token=<token>``).
    Nothing to check when the server has no token (loopback only:
    `Settings.check`), nor for `OPEN_PATHS`."""
    token = request.app.state.lux.settings.token
    if not token or is_open(request.url.path):
        return
    auth = request.headers.get("authorization", "")
    given = auth[7:] if auth.lower().startswith("bearer ") else None
    given = given or request.cookies.get(TOKEN_COOKIE) or request.query_params.get("token")
    if not _same(given, token):
        raise HTTPException(status_code=401, detail="this server needs its token")


async def require_csrf(request: Request):
    """403 for a request that changes something without the page's CSRF
    token (the ``_csrf`` form field or the ``X-CSRF-Token`` header)."""
    if request.method in SAFE_METHODS:
        return
    expected = request.app.state.csrf
    given = request.headers.get("x-csrf-token")
    if not given and "form" in request.headers.get("content-type", ""):
        form = await request.form()
        given = form.get("_csrf")
    if not _same(given, expected):
        raise HTTPException(status_code=403, detail="missing or wrong CSRF token: reload the page")


def under(root, path):
    """Whether `path` is `root` or inside it, after resolving ``..`` and
    symbolic links: for a directory or file named by a request."""
    import os
    if not root or not path:
        return False
    root = os.path.realpath(root)
    path = os.path.realpath(path)
    return path == root or os.path.commonpath([root, path]) == root
