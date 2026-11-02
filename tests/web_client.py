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
