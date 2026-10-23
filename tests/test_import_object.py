"""importObject must resolve object paths written for the pre-rename layout.

Live configs may still say ``pipeline.process.validator.Validator`` (the
package was renamed to ``lux_pipeline``); those paths must keep working and
log a warning once so the operator knows which config to update.
"""

import logging
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline import config as config_mod
from lux_pipeline.config import importObject
from lux_pipeline.process.validator import Validator


@pytest.fixture(autouse=True)
def _reset_warned():
    config_mod._warned_legacy_paths.clear()
    yield
    config_mod._warned_legacy_paths.clear()


@pytest.mark.parametrize(
    "path",
    [
        "lux_pipeline.process.validator.Validator",
        "process.validator.Validator",
        "pipeline.process.validator.Validator",
    ],
)
def test_validator_paths_resolve_to_the_same_class(path):
    assert importObject(path) is Validator


def test_legacy_package_prefix_warns_once(caplog):
    with caplog.at_level(logging.WARNING, logger="lux_pipeline"):
        importObject("pipeline.process.validator.Validator")
        importObject("pipeline.process.validator.Validator")
    warnings = [r for r in caplog.records if "pipeline.process.validator.Validator" in r.getMessage()]
    assert len(warnings) == 1
    assert "lux_pipeline.process.validator.Validator" in warnings[0].getMessage()


def test_current_paths_do_not_warn(caplog):
    with caplog.at_level(logging.WARNING, logger="lux_pipeline"):
        importObject("lux_pipeline.process.validator.Validator")
        importObject("process.validator.Validator")
    assert not caplog.records


def test_empty_path_is_none():
    assert importObject("") is None
    assert importObject(None) is None


def test_missing_module_raises():
    with pytest.raises(ModuleNotFoundError):
        importObject("no.such.module.Thing")


def test_missing_class_raises():
    with pytest.raises(AttributeError):
        importObject("lux_pipeline.process.validator.NoSuchClass")


def test_malformed_path_raises():
    with pytest.raises(ValueError):
        importObject("Validator")


# --- flattened sources/ (the category level was removed) -------------------


@pytest.mark.parametrize(
    "path",
    [
        "lux_pipeline.sources.lc.mapper.LcnafMapper",
        "sources.lc.mapper.LcnafMapper",
        "sources.authorities.lc.mapper.LcnafMapper",
        "lux_pipeline.sources.authorities.lc.mapper.LcnafMapper",
        "pipeline.sources.authorities.lc.mapper.LcnafMapper",
    ],
)
def test_source_paths_with_or_without_category_resolve_to_the_same_class(path):
    from lux_pipeline.sources.lc.mapper import LcnafMapper

    assert importObject(path) is LcnafMapper


def test_legacy_category_warns_once_with_the_flat_path(caplog):
    with caplog.at_level(logging.WARNING, logger="lux_pipeline"):
        importObject("sources.authorities.lc.mapper.LcnafMapper")
        importObject("sources.authorities.lc.mapper.LcnafMapper")
    assert len(caplog.records) == 1
    assert "'sources.lc.mapper.LcnafMapper'" in caplog.records[0].getMessage()


def test_grouped_source_dirs_are_not_treated_as_categories(caplog):
    from lux_pipeline.sources.yale.ycba.mapper import YcbaMapper

    with caplog.at_level(logging.WARNING, logger="lux_pipeline"):
        assert importObject("sources.yale.ycba.mapper.YcbaMapper") is YcbaMapper
    assert not caplog.records
