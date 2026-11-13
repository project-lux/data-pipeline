"""PhaseTimer's reports go to the ``lux_pipeline.timing`` logger, which a
run sends to ``<prefix>/w<n>.timing`` / ``driver.timing``: not into the
log a person follows in lux watch or the web UI, which gets one line per
phase finished."""

import io
import logging
from pathlib import Path
from types import SimpleNamespace

import pytest

from lux_pipeline import runtime
from lux_pipeline.cli._handler import ensure_driver_log
from lux_pipeline.log import send_timing_to, timing_logger, timing_run_dir
from lux_pipeline.process.tasks._task_manager import TaskManager, _run_worker
from lux_pipeline.process.utils.timing import PhaseTimer, timing_out_dir
from lux_pipeline.progress import (LogTailer, driver_log_path, driver_timing_path, log_path,
                                   timing_log_path)

LOGGER = logging.getLogger("lux_pipeline")


@pytest.fixture(autouse=True)
def _restore_logger():
    saved = (list(LOGGER.handlers), LOGGER.level)
    yield
    for h in list(LOGGER.handlers):
        LOGGER.removeHandler(h)
        if isinstance(h, logging.FileHandler):
            h.close()
    for h in saved[0]:
        LOGGER.addHandler(h)
    LOGGER.setLevel(saved[1])


def flush_all():
    for h in LOGGER.handlers + timing_logger.handlers:
        h.flush()


def run_a_phase(phase="reconcile", n=3):
    t = PhaseTimer(phase, slice_n=n, max_slice=24, out_dir=None, report_every=1e9)
    for _ in range(5):
        with t.stage("acquire"):
            pass
        t.step()
    t.report()
    t.finish()
    return t


class TimedManager(TaskManager):
    """A worker whose phase is timed, as reconcile's, merge's and export's are."""

    needs_configs = False

    def _distributed(self, n, cfgs, progress):
        LOGGER.info(f" *** source {n} ***")
        run_a_phase("reconcile", n)
        return n


def test_without_a_file_the_reports_go_with_the_rest():
    out = io.StringIO()
    runtime.configure_logging("INFO", stream=out)
    send_timing_to(None)
    run_a_phase()
    text = out.getvalue()
    assert "[reconcile[3/24]]   acquire" in text
    assert "TIMING reconcile slice=3 records=5" in text
    assert "reconcile[3/24] finished: 5 records in" in text


def test_a_worker_writes_its_reports_beside_its_log(tmp_path):
    prefix = str(tmp_path / "reconcile-1")
    console = io.StringIO()
    runtime.configure_logging("INFO", stream=console)
    assert _run_worker(TimedManager(None, 1), 0, prefix) == 0
    log = Path(log_path(prefix, 0)).read_text()
    timing = Path(timing_log_path(prefix, 0)).read_text()
    # the log: what happened, and one line when the phase is done
    assert "*** source 0 ***" in log
    assert "reconcile[0/24] finished: 5 records in" in log
    assert "acquire" not in log and "TIMING" not in log and "cpu" not in log
    # the timing file: every report, with a time and no level
    assert "[reconcile[0/24]]   acquire" in timing
    assert "TIMING reconcile slice=0 records=5" in timing
    assert "INFO" not in timing
    # the null engine put the driver's routing back: no file, propagating
    assert timing_logger.propagate and not timing_logger.handlers
    assert "acquire" not in console.getvalue()


def test_the_tailer_never_sees_the_reports(tmp_path):
    prefix = str(tmp_path / "merge-1")
    runtime.configure_logging("INFO", stream=io.StringIO())
    _run_worker(TimedManager(None, 1), 0, prefix)
    lines = [line for _, line in LogTailer(prefix, 1).read_new_lines()]
    assert any("finished: 5 records" in line for line in lines)
    assert not any("acquire" in line or "TIMING" in line for line in lines)


def test_a_worker_at_warning_still_writes_its_reports(tmp_path):
    prefix = str(tmp_path / "export-1")
    runtime.configure_logging("INFO", stream=io.StringIO())
    _run_worker(TimedManager(None, 1), 0, prefix, log_level="WARNING")
    assert "TIMING reconcile" in Path(timing_log_path(prefix, 0)).read_text()
    assert not Path(log_path(prefix, 0)).exists()            # nothing at WARNING


def test_the_driver_writes_its_reports_beside_its_log(tmp_path):
    prefix = str(tmp_path / "identify-1")
    console = io.StringIO()
    runtime.configure_logging("INFO", stream=console)
    h = ensure_driver_log(prefix, "INFO")
    timing_handler = timing_logger.handlers[0]
    assert ensure_driver_log(prefix, "INFO") is h
    assert timing_logger.handlers == [timing_handler]       # once per prefix
    run_a_phase("identify", -1)
    flush_all()
    log = Path(driver_log_path(prefix)).read_text()
    timing = Path(driver_timing_path(prefix)).read_text()
    assert "identify finished: 5 records" in log and "TIMING" not in log
    assert "TIMING identify slice=-1" in timing
    assert "TIMING" not in console.getvalue()


def test_a_null_engine_worker_hands_the_reports_back_to_the_driver(tmp_path):
    prefix = str(tmp_path / "reconcile-2")
    runtime.configure_logging("INFO", stream=io.StringIO())
    ensure_driver_log(prefix, "INFO")
    _run_worker(TimedManager(None, 1), 0, prefix)
    run_a_phase("identify", -1)
    flush_all()
    assert "TIMING reconcile slice=0" in Path(timing_log_path(prefix, 0)).read_text()
    driver = Path(driver_timing_path(prefix)).read_text()
    assert "TIMING identify" in driver and "TIMING reconcile" not in driver


def test_a_stream_given_to_the_timer_gets_the_reports():
    out = io.StringIO()
    logs = io.StringIO()
    runtime.configure_logging("INFO", stream=logs)
    t = PhaseTimer("merge", out_dir=None, report_every=1e9, stream=out)
    t.count = 2
    t.finish()
    assert "TIMING merge" in out.getvalue() and "TIMING" not in logs.getvalue()
    assert "merge finished: 2 records" in logs.getvalue()


# --- the timing JSON and profiles: in the run's directory ---------------------------------


class JsonTimedManager(TaskManager):
    """A worker timed as the phases are: its JSON goes to `timing_out_dir`."""

    needs_configs = False

    def _distributed(self, n, cfgs, progress):
        t = PhaseTimer("merge", slice_n=n, max_slice=2, report_every=1e9, profile=True,
                       out_dir=timing_out_dir(self.cfgs_stub))
        for _ in range(3):
            with t.stage("acquire"):
                pass
            t.step()
        t.finish()
        return n


def test_a_workers_timing_json_and_profile_are_in_its_run(tmp_path):
    logs = tmp_path / "logs"
    prefix = str(logs / "merge-1")
    runtime.configure_logging("INFO", stream=io.StringIO())
    m = JsonTimedManager(None, 2)
    m.cfgs_stub = SimpleNamespace(log_dir=str(logs), data_dir=str(tmp_path))
    assert _run_worker(m, 1, prefix) == 1                        # its result
    assert Path(prefix, "timing-merge-1.json").exists()
    assert Path(prefix, "profile-merge-1.prof").exists()
    assert sorted(p.name for p in logs.iterdir()) == ["merge-1"]      # nothing flat


def test_the_drivers_timing_json_is_in_its_run(tmp_path):
    cfgs = SimpleNamespace(log_dir=str(tmp_path / "logs"), data_dir=str(tmp_path))
    send_timing_to(None)
    assert timing_run_dir() is None
    assert timing_out_dir(cfgs) == cfgs.log_dir                      # no run: log_dir
    assert timing_out_dir(SimpleNamespace(data_dir="d")) == "d"
    prefix = str(tmp_path / "logs" / "identify-1")
    runtime.configure_logging("INFO", stream=io.StringIO())
    ensure_driver_log(prefix, "INFO")
    assert timing_run_dir() == prefix and timing_out_dir(cfgs) == prefix
    send_timing_to(None)
