"""Serve the web UI: configuration, records, runs and commands in a browser.

    lux serve                                   # http://127.0.0.1:8321/, opens the browser
    lux serve --port 9000 --no-open
    lux --env staging serve                     # another installation
    lux serve --host 0.0.0.0 --token SECRET     # reachable from other machines: needs a token

Needs the web extra: ``pip install -e .[web]``. The defaults may be set in
the base (or system) record's optional ``web`` block, ``{"host": ...,
"port": ...}``; the options win. A bind other machines can reach is
refused without ``--token`` (or ``LUX_WEB_TOKEN``); a browser then opens
any page once with ``?token=SECRET`` and keeps it as a cookie, and other
clients send ``Authorization: Bearer SECRET``.

The configuration is read when a page first needs it, not at start-up,
so a broken installation still serves a page saying what is wrong.
``--reload`` restarts the server when the code changes (development).
"""

import json
import logging
import os
import threading
import webbrowser

from lux_pipeline import runtime

from ._handler import BaseHandler

logger = logging.getLogger("lux_pipeline")

INSTALL_HINT = "lux serve needs the web extra: pip install -e '.[web]'"


class CommandHandler(BaseHandler):
    # the server reads the configuration when a page asks, not before
    needs_configs = False

    def add_args(self, ap):
        ap.add_argument("--host", help="Address to listen on (default 127.0.0.1)")
        ap.add_argument("--port", type=int, help="Port to listen on (default 8321)")
        ap.add_argument("--token", help="Required of every request; needed for a host other "
                                        "than 127.0.0.1 (default: LUX_WEB_TOKEN)")
        ap.add_argument("--no-open", dest="no_open", action="store_true",
                        help="Do not open a browser")
        ap.add_argument("--reload", action="store_true",
                        help="Restart when the code changes (development)")

    def settings(self, args):
        """Defaults, the configuration's ``web`` block (when the
        configuration reads), then the options."""
        from lux_pipeline.web.settings import Settings
        # ./.env and ~/.lux/.env first, so LUX_WEB_TOKEN may be set there
        runtime.get_basepath()
        cfgs = self.configs
        if cfgs is None:
            try:
                cfgs = runtime.get_configs(instantiate=False)
            except Exception as e:
                logger.debug(f"no configuration for the web block: {e}")
                cfgs = None
        settings = Settings.from_config(cfgs, host=args.host, port=args.port, token=args.token,
                                        open_browser=not args.no_open,
                                        debug=bool(getattr(args, "debug", False)))
        settings.check()
        return settings

    def open_browser(self, url):
        timer = threading.Timer(1.0, webbrowser.open, args=(url,))
        timer.daemon = True
        timer.start()

    def run(self, settings, reload=False):
        import uvicorn
        log_level = "info" if settings.debug else "warning"
        if reload:
            os.environ["LUX_WEB_SETTINGS"] = json.dumps(
                {"host": settings.host, "port": settings.port, "token": settings.token,
                 "debug": settings.debug, "open_browser": False})
            uvicorn.run("lux_pipeline.web.app:create_app_from_env", factory=True, reload=True,
                        host=settings.host, port=settings.port, log_level=log_level, ws="none")
            return
        from lux_pipeline.web.app import create_app
        # no websockets: the live pages use server-sent events
        uvicorn.run(create_app(settings), host=settings.host, port=settings.port,
                    log_level=log_level, ws="none")

    def process(self, args, rest):
        super().process(args, rest)
        try:
            import fastapi  # noqa: F401
            import jinja2  # noqa: F401
            import uvicorn  # noqa: F401
        except ImportError as e:
            raise RuntimeError(f"{INSTALL_HINT} ({e.name} is missing)") from None
        settings = self.settings(args)
        url = settings.url
        print(f"LUX web UI at {url}" + (" (token required)" if settings.token else ""), flush=True)
        if settings.open_browser:
            self.open_browser(url + (f"?token={settings.token}" if settings.token else ""))
        self.run(settings, reload=args.reload)
        return settings
