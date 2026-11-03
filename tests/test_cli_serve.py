"""Web UI item W06: `lux serve`: it imports nothing of the web extra until
it runs, refuses an open bind without a token, and starts uvicorn with
the settings it resolved."""

import builtins
import sys
from argparse import Namespace
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.cli import entry, serve


def test_importing_and_help_need_no_web_extra(monkeypatch, capsys):
    real_import = builtins.__import__

    def blocked(name, *a, **kw):
        if name.split(".")[0] in ("fastapi", "uvicorn", "jinja2", "starlette"):
            raise ImportError(f"No module named {name!r}", name=name)
        return real_import(name, *a, **kw)

    monkeypatch.setattr(builtins, "__import__", blocked)
    for mod in [m for m in sys.modules if m.startswith("lux_pipeline.cli.serve")]:
        monkeypatch.delitem(sys.modules, mod)
    import importlib
    mod = importlib.import_module("lux_pipeline.cli.serve")
    assert entry.run(["help", "serve"]) == 0
    assert "--no-open" in capsys.readouterr().out
    with pytest.raises(RuntimeError, match=r"pip install -e '\.\[web\]'"):
        mod.CommandHandler(None).process(Namespace(command="serve"), [])


def parse(argv):
    args, rest = entry.build_parser().parse_known_args(["serve", *argv])
    return args, rest


@pytest.fixture
def started(monkeypatch):
    pytest.importorskip("fastapi")
    calls = []
    monkeypatch.setattr(serve.CommandHandler, "run",
                        lambda self, settings, reload=False: calls.append((settings, reload)))
    monkeypatch.setattr(serve.CommandHandler, "open_browser", lambda self, url: calls.append(url))
    monkeypatch.delenv("LUX_WEB_TOKEN", raising=False)
    monkeypatch.setattr(serve.runtime, "get_configs", lambda **kw: (_ for _ in ()).throw(RuntimeError("no config")))
    return calls


def test_defaults_and_options(started, capsys):
    h = serve.CommandHandler(None)
    s = h.process(*parse(["--port", "9001", "--no-open"]))
    assert (s.host, s.port, s.open_browser, s.token) == ("127.0.0.1", 9001, False, None)
    assert started == [(s, False)]
    assert capsys.readouterr().out.strip() == "LUX web UI at http://127.0.0.1:9001/"


def test_the_browser_gets_the_token(started):
    serve.CommandHandler(None).process(*parse(["--token", "t0k", "--host", "0.0.0.0"]))
    assert started[0] == "http://0.0.0.0:8321/?token=t0k"


def test_an_open_bind_needs_a_token(started):
    with pytest.raises(ValueError, match="give --token"):
        serve.CommandHandler(None).process(*parse(["--host", "0.0.0.0"]))
    assert started == []


def test_the_configurations_web_block_is_the_default(started, monkeypatch):
    class Cfgs:
        web = {"port": 9300}

    monkeypatch.setattr(serve.runtime, "get_configs", lambda **kw: Cfgs())
    s = serve.CommandHandler(None).process(*parse(["--no-open"]))
    assert s.port == 9300
    s = serve.CommandHandler(None).process(*parse(["--no-open", "--port", "9400"]))
    assert s.port == 9400
