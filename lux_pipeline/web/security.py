"""What every request passes through: the token, when the server has one,
and the CSRF check on anything that changes something.

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
# paths that answer without the token (a monitor's check)
OPEN_PATHS = ("/healthz",)


def new_csrf_token():
    return secrets.token_urlsafe(24)


def _same(a, b):
    return bool(a) and bool(b) and hmac.compare_digest(str(a), str(b))


async def require_token(request: Request):
    """401 unless the request carries the server's token, as
    ``Authorization: Bearer <token>`` or the ``lux_token`` cookie (set by
    opening any page once with ``?token=<token>``). Nothing to check when
    the server has no token (loopback only: `Settings.check`)."""
    token = request.app.state.lux.settings.token
    if not token or request.url.path in OPEN_PATHS:
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
