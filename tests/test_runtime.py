"""lux_pipeline.runtime: one bootstrap for the configuration, cached per
process, and one place that sets up logging for the non-UI case."""

import io
import logging
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline import runtime


class StubConfig:
    """Records how it was built and which of the expensive steps ran."""

    instances = []

    def __init__(self, basepath=None):
        self.basepath = basepath
        self.steps = []
        StubConfig.instances.append(self)

    def get_idmap(self):
        self.steps.append("idmap")
        return "idmap"

    def cache_globals(self):
        self.steps.append("globals")

    def instantiate_all(self):
        self.steps.append("all")


@pytest.fixture(autouse=True)
def _isolated(monkeypatch):
    StubConfig.instances = []
    monkeypatch.setattr(runtime, "Config", StubConfig)
    # no .env on the search path, and nothing in the environment
    monkeypatch.setattr(runtime, "find_dotenv", lambda usecwd=True: "")
    monkeypatch.delenv("LUX_BASEPATH", raising=False)
    runtime.reset_configs()
    yield
    runtime.reset_configs()


# --- get_basepath ------------------------------------------------------------


def test_basepath_comes_from_the_environment(monkeypatch):
    monkeypatch.setenv("LUX_BASEPATH", "/inst")
    assert runtime.get_basepath() == "/inst"


def test_basepath_loads_the_nearest_dotenv(monkeypatch, tmp_path):
    env = tmp_path / ".env"
    env.write_text("LUX_BASEPATH=/from-dotenv\n")
    monkeypatch.setattr(runtime, "find_dotenv", lambda usecwd=True: str(env))
    assert runtime.get_basepath() == "/from-dotenv"


def test_basepath_is_empty_when_unset():
    assert runtime.get_basepath() == ""


# --- get_configs -------------------------------------------------------------


def test_get_configs_builds_once_and_caches(monkeypatch):
    monkeypatch.setenv("LUX_BASEPATH", "/inst")
    a = runtime.get_configs()
    b = runtime.get_configs()
    assert a is b
    assert len(StubConfig.instances) == 1
    assert a.basepath == "/inst"
    assert a.steps == ["idmap", "globals", "all"]


def test_instantiate_false_skips_the_expensive_steps(monkeypatch):
    monkeypatch.setenv("LUX_BASEPATH", "/inst")
    c = runtime.get_configs(instantiate=False)
    assert c.steps == []


def test_explicit_basepath_wins_over_the_environment(monkeypatch):
    monkeypatch.setenv("LUX_BASEPATH", "/env")
    assert runtime.get_configs(basepath="/given").basepath == "/given"


def test_unset_basepath_raises():
    with pytest.raises(RuntimeError, match="LUX_BASEPATH"):
        runtime.get_configs()
    assert StubConfig.instances == []


def test_a_failed_build_is_not_cached(monkeypatch):
    class Boom(StubConfig):
        def __init__(self, basepath=None):
            raise OSError("no such config dir")

    monkeypatch.setattr(runtime, "Config", Boom)
    monkeypatch.setenv("LUX_BASEPATH", "/inst")
    with pytest.raises(OSError):
        runtime.get_configs()
    monkeypatch.setattr(runtime, "Config", StubConfig)
    assert runtime.get_configs().basepath == "/inst"


def test_reset_configs_forces_a_rebuild(monkeypatch):
    monkeypatch.setenv("LUX_BASEPATH", "/inst")
    a = runtime.get_configs()
    runtime.reset_configs()
    b = runtime.get_configs()
    assert a is not b
    assert len(StubConfig.instances) == 2


# --- configure_logging -------------------------------------------------------


@pytest.fixture
def lux_logger():
    logger = logging.getLogger("lux_pipeline")
    saved = (list(logger.handlers), logger.level)
    yield logger
    for h in list(logger.handlers):
        logger.removeHandler(h)
    for h in saved[0]:
        logger.addHandler(h)
    logger.setLevel(saved[1])


def test_configure_logging_leaves_exactly_one_handler_at_the_level(lux_logger):
    lux_logger.addHandler(logging.NullHandler())
    lux_logger.addHandler(logging.NullHandler())
    h = runtime.configure_logging("WARNING")
    assert lux_logger.handlers == [h]
    assert h.level == logging.WARNING
    h2 = runtime.configure_logging("debug")
    assert lux_logger.handlers == [h2]
    assert h2.level == logging.DEBUG


def test_configure_logging_writes_messages_to_the_stream(lux_logger):
    out = io.StringIO()
    runtime.configure_logging("INFO", stream=out)
    lux_logger.info("hello")
    lux_logger.debug("hidden")
    assert out.getvalue() == "hello\n"


def test_configure_logging_rejects_unknown_levels(lux_logger):
    with pytest.raises(ValueError):
        runtime.configure_logging("LOUD")
