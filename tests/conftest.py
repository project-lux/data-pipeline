"""Fixtures every test gets."""

import pytest

from lux_pipeline.log import restore_timing, timing_state


@pytest.fixture(autouse=True)
def _restore_timing_logger():
    """A test that starts a worker or a driver log with a prefix sends
    PhaseTimer's reports to a file under its tmp_path; the next test gets
    the logger as it was."""
    saved = timing_state()
    yield
    restore_timing(saved)
