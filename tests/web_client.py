"""The web UI's test harness (webui_docs/06). Not a test module.

    client, state = make_client(tmp_path, monkeypatch)            # the W00 installation, seeded
    client, state = make_client(tmp_path, monkeypatch, root=None) # no installation at all

The application is built over the database-free installation (W00), with
``LUX_BASEPATH`` pointing at it; Postgres is never reached (the probe
`lux testinstall` uses is stubbed to answer). `csrf(client)` is the token
every POST needs.
"""

from pathlib import Path

from webui_fixtures import make_configs, make_installation, seed_records, use_installation


def make_client(tmp_path, monkeypatch, *, seed=True, root="inst", settings=None,
                postgres="PostgreSQL (stub)"):
    from fastapi.testclient import TestClient

    from lux_pipeline.cli import _services
    from lux_pipeline.web.app import create_app
    from lux_pipeline.web.settings import Settings

    monkeypatch.setenv("HOME", str(Path(tmp_path) / "home"))
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("LUX_WEB_TOKEN", raising=False)
    if root is None:
        monkeypatch.delenv("LUX_BASEPATH", raising=False)
        monkeypatch.delenv("LUX_ENV_FILE", raising=False)
    else:
        inst = make_installation(Path(tmp_path) / root)
        if seed:
            seed_records(make_configs(inst, instantiate=True))
        use_installation(monkeypatch, inst)

    def probe(settings_):
        if isinstance(postgres, Exception):
            raise postgres
        return postgres

    monkeypatch.setattr(_services, "probe_postgres", probe)
    app = create_app(settings or Settings(open_browser=False, token=None))
    return TestClient(app), app.state.lux


def csrf(client):
    return client.app.state.csrf


class LiveServer:
    """The application behind a real uvicorn server on a free loopback port,
    in a thread: for server-sent event streams that must be read while the
    test changes files (the TestClient runs a response to its end before
    handing over any of it).

        with LiveServer(client.app) as server:
            for name, data in server.events("/api/runs/x/events"): ...
    """

    def __init__(self, app):
        self.app = app
        self.server = None
        self.thread = None
        self.url = None

    def __enter__(self):
        import socket
        import threading
        import time

        import uvicorn

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", 0))
        self.url = f"http://127.0.0.1:{sock.getsockname()[1]}"
        self.server = uvicorn.Server(uvicorn.Config(self.app, log_level="warning", lifespan="off",
                                                    ws="none"))
        self.thread = threading.Thread(target=self.server.run, kwargs={"sockets": [sock]},
                                       daemon=True)
        self.thread.start()
        deadline = time.time() + 10
        while not self.server.started and time.time() < deadline:
            time.sleep(0.01)
        if not self.server.started:
            raise RuntimeError("the test server did not start")
        return self

    def __exit__(self, *exc):
        self.server.should_exit = True
        self.thread.join(timeout=10)

    def events(self, path, timeout=20):
        """``(event, data)`` pairs as the server sends them; stops when the
        stream ends or after `timeout` seconds without a line."""
        import json
        import urllib.request
        req = urllib.request.Request(self.url + path, headers={"Accept": "text/event-stream"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            name = None
            for raw in resp:
                line = raw.decode().rstrip("\n")
                if line.startswith("event: "):
                    name = line[7:]
                elif line.startswith("data: "):
                    yield name, json.loads(line[6:])
