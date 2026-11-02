"""How the server runs: where it listens, how it is protected, how often
live pages refresh."""

import os
from dataclasses import dataclass, field, fields, replace

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8321
LOOPBACK = ("127.0.0.1", "localhost", "::1")


@dataclass(frozen=True)
class Settings:
    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    # required for a bind that is not loopback; LUX_WEB_TOKEN by default
    token: str | None = field(default_factory=lambda: os.environ.get("LUX_WEB_TOKEN") or None)
    open_browser: bool = True
    debug: bool = False
    # seconds between reads of a run's files on a live page
    refresh: float = 0.5

    @property
    def loopback(self):
        return self.host in LOOPBACK

    @property
    def url(self):
        host = f"[{self.host}]" if ":" in self.host else self.host
        return f"http://{host}:{self.port}/"

    def updated(self, **values):
        """A copy with the given values, ignoring None (an option not given)."""
        known = {f.name for f in fields(self)}
        return replace(self, **{k: v for k, v in values.items() if k in known and v is not None})

    @classmethod
    def from_config(cls, cfgs, **overrides):
        """Defaults, then the optional ``web`` block of the base / system
        record (``{"host": ..., "port": ...}``), then `overrides`."""
        s = cls()
        block = getattr(cfgs, "web", None) if cfgs is not None else None
        if isinstance(block, dict):
            s = s.updated(host=block.get("host"), port=block.get("port"))
        return s.updated(**overrides)

    def check(self):
        """ValueError for a setting the server must not start with: a bind
        others can reach, with no token to keep them out."""
        if not self.loopback and not self.token:
            raise ValueError(f"listening on {self.host} lets other machines in: give --token "
                             f"(or LUX_WEB_TOKEN), or keep the default {DEFAULT_HOST}")
        if not (0 < int(self.port) < 65536):
            raise ValueError(f"port {self.port} is not a port")
