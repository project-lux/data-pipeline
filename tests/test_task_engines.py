"""TaskManager and the null / mp / ray engines: each worker gets its own
status and log files, results come back by worker, failures propagate."""

import io
import logging
import os
import sys
import time
from argparse import Namespace
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.process.tasks._task_manager import (
    MpEngine, NullEngine, TaskManager, WorkerFailed, attach_worker_log_handler,
    make_engine, _run_worker,
)
from lux_pipeline.progress import log_path, null_progress, read_status, status_path

logger = logging.getLogger("lux_pipeline")


class CountingManager(TaskManager):
    """Each worker reports a total of 10, advances 10, logs a line, writes
    its number to a file, and returns it. Module-level so spawned workers
    can unpickle it; needs no Config."""

    needs_configs = False

    def __init__(self, configs, max_workers=0, args=None, out_dir=None, fail=()):
        super().__init__(configs, max_workers, args)
        self.out_dir = out_dir
        self.fail = tuple(fail)

    def _distributed(self, n, cfgs, progress):
        progress(phase="count", description=f"worker {n}", total=10)
        if n in self.fail:
            raise ValueError(f"worker {n} was told to fail")
        for _ in range(10):
            progress(advance=1)
        logger.info(f"counted to ten in worker {n}")
        logger.debug("not at INFO")
        if self.out_dir:
            Path(self.out_dir, str(n)).write_text(str(n))
        return n


class FakeView:
    def __init__(self):
        self.refreshes = 0

    def refresh(self):
        self.refreshes += 1
        return False


class StubConfigs:
    max_workers = 4
    internal = {"ycba": {"name": "ycba"}}
    external = {"aat": {"name": "aat"}}
    results = {"merged": {"name": "merged"}}


@pytest.fixture(autouse=True)
def _restore_logger():
    saved = (list(logger.handlers), logger.level)
    yield
    for h in list(logger.handlers):
        logger.removeHandler(h)
    for h in saved[0]:
        logger.addHandler(h)
    logger.setLevel(saved[1])


# --- TaskManager -------------------------------------------------------------


def test_manager_reads_its_options_from_args():
    args = Namespace(my_worker=2, log="DEBUG", max_records=50)
    m = TaskManager(StubConfigs(), args=args)
    assert (m.max_workers, m.my_slice, m.log_level, m.max_records) == (4, 2, "DEBUG", 50)
    m2 = TaskManager(StubConfigs(), max_workers=9)
    assert (m2.max_workers, m2.my_slice, m2.log_level) == (9, -1, "INFO")


def test_prepare_single_covers_internal_external_and_results():
    m = TaskManager(StubConfigs())
    for name in ("ycba", "aat", "merged"):
        m.prepare_single(name)
    assert m.sources == [("internal", "ycba"), ("external", "aat"), ("results", "merged")]
    with pytest.raises(ValueError, match="Unknown source: nope"):
        m.prepare_single("nope")


def test_prepare_all_covers_internal_and_external_only():
    m = TaskManager(StubConfigs())
    m.prepare_all()
    assert m.sources == [("internal", "ycba"), ("external", "aat")]


def test_run_single_uses_the_loaded_config_and_no_files(tmp_path):
    m = CountingManager(StubConfigs(), max_workers=3, out_dir=str(tmp_path))
    assert m.run_single(2) == 2
    assert (tmp_path / "2").read_text() == "2"
    assert list(tmp_path.glob("*.status")) == []


def test_process_restores_the_config_after_running(tmp_path):
    cfgs = StubConfigs()
    m = CountingManager(cfgs, max_workers=1, out_dir=str(tmp_path))
    m.process("null", None, None)
    assert m.configs is cfgs


def test_unknown_engine_raises():
    with pytest.raises(ValueError, match="Unknown engine 'turbo'"):
        make_engine("turbo", TaskManager(StubConfigs()))


def test_base_distributed_is_abstract():
    with pytest.raises(NotImplementedError):
        TaskManager(StubConfigs()).run_single()


# --- the worker wrapper ------------------------------------------------------


def test_run_worker_writes_status_and_log_and_flushes_done(tmp_path):
    prefix = str(tmp_path / "count-1")
    m = CountingManager(None, max_workers=1)
    assert _run_worker(m, 0, prefix, "INFO") == 0
    st = read_status(status_path(prefix, 0))
    assert (st["completed"], st["total"], st["done"]) == (10, 10, True)
    assert st["description"] == "worker 0"
    assert st["phase"] == "count"
    log = Path(log_path(prefix, 0)).read_text()
    assert "INFO     counted to ten in worker 0" in log
    assert "not at INFO" not in log


def test_run_worker_marks_done_and_logs_on_failure(tmp_path):
    prefix = str(tmp_path / "count-2")
    m = CountingManager(None, max_workers=1, fail=(0,))
    with pytest.raises(ValueError):
        _run_worker(m, 0, prefix, "INFO")
    assert read_status(status_path(prefix, 0))["done"] is True
    assert "worker 0 failed: ValueError" in Path(log_path(prefix, 0)).read_text()


def test_run_worker_without_a_prefix_keeps_the_handlers(tmp_path):
    stream = io.StringIO()
    h = logging.StreamHandler(stream)
    logger.handlers[:] = [h]
    logger.setLevel(logging.INFO)
    m = CountingManager(None, max_workers=1, out_dir=str(tmp_path))
    assert _run_worker(m, 0, None) == 0
    assert logger.handlers == [h]
    assert "counted to ten in worker 0" in stream.getvalue()


def test_attach_worker_log_handler_replaces_everything_and_clamps(tmp_path):
    logger.addHandler(logging.NullHandler())
    logger.addHandler(logging.NullHandler())
    lg = attach_worker_log_handler(str(tmp_path / "p"), 3, "LOUD")
    assert len(lg.handlers) == 1
    assert lg.handlers[0].level == logging.INFO
    assert lg.handlers[0].baseFilename == str(tmp_path / "p-w3.log")
    assert lg.level == logging.INFO
    # delay=True: nothing on disk until something is logged
    assert not (tmp_path / "p-w3.log").exists()
    lg2 = attach_worker_log_handler(None, 0)
    assert isinstance(lg2.handlers[0], logging.NullHandler)


# --- null engine -------------------------------------------------------------


def test_null_engine_runs_only_my_slice(tmp_path):
    prefix = str(tmp_path / "null-1")
    args = Namespace(my_worker=1, log="INFO", max_records=-1)
    m = CountingManager(None, max_workers=3, args=args, out_dir=str(tmp_path))
    assert m.process("null", prefix, None) == [1]
    assert sorted(p.name for p in tmp_path.glob("*.status")) == ["null-1-w1.status"]
    assert (tmp_path / "1").exists() and not (tmp_path / "0").exists()


def test_null_engine_defaults_to_worker_zero(tmp_path):
    m = CountingManager(None, max_workers=2, out_dir=str(tmp_path))
    assert m.process("null", str(tmp_path / "p"), None) == [0]


def test_null_engine_failure_raises_worker_failed(tmp_path):
    m = CountingManager(None, max_workers=1, fail=(0,))
    with pytest.raises(WorkerFailed) as ex:
        m.process("null", str(tmp_path / "p"), None)
    assert ex.value.failures[0][0] == 0
    assert isinstance(ex.value.__cause__, ValueError)


# --- mp engine ---------------------------------------------------------------


def test_mp_engine_runs_every_worker_in_its_own_process(tmp_path):
    prefix = str(tmp_path / "count-3")
    m = CountingManager(None, max_workers=3, out_dir=str(tmp_path))
    view = FakeView()
    assert m.process("mp", prefix, view) == [0, 1, 2]
    for n in range(3):
        st = read_status(status_path(prefix, n))
        assert (st["completed"], st["total"], st["done"]) == (10, 10, True)
        assert f"counted to ten in worker {n}" in Path(log_path(prefix, n)).read_text()
        assert (tmp_path / str(n)).read_text() == str(n)
    assert view.refreshes >= 1


def test_mp_engine_raises_after_every_worker_finished(tmp_path):
    prefix = str(tmp_path / "count-4")
    m = CountingManager(None, max_workers=3, out_dir=str(tmp_path), fail=(1,))
    with pytest.raises(WorkerFailed) as ex:
        m.process("mp", prefix, None)
    assert [n for n, _ in ex.value.failures] == [1]
    assert "worker 1 was told to fail" in str(ex.value)
    # the others still ran to completion
    assert (tmp_path / "0").exists() and (tmp_path / "2").exists()
    assert read_status(status_path(prefix, 1))["done"] is True


def test_mp_engine_without_a_prefix_writes_no_files(tmp_path):
    m = CountingManager(None, max_workers=2, out_dir=str(tmp_path))
    assert m.process("mp", None, None) == [0, 1]
    assert list(tmp_path.glob("*.status")) == []


# --- ray engine --------------------------------------------------------------


@pytest.mark.skipif(os.environ.get("LUX_TEST_RAY", "1") == "0", reason="LUX_TEST_RAY=0")
def test_ray_engine_runs_every_worker(tmp_path):
    ray = pytest.importorskip("ray")
    prefix = str(tmp_path / "ray-1")

    # Defined here, not at module level: Ray workers cannot import this test
    # module by name, and cloudpickle ships a locally defined class by value
    class RayCounting(TaskManager):
        needs_configs = False

        def __init__(self, out_dir):
            super().__init__(None, max_workers=2)
            self.out_dir = out_dir

        def _distributed(self, n, cfgs, progress):
            progress(total=10)
            progress(advance=10)
            logging.getLogger("lux_pipeline").info(f"ray worker {n}")
            Path(self.out_dir, str(n)).write_text(str(n))
            return n

    m = RayCounting(str(tmp_path))
    try:
        assert m.process("ray", prefix, FakeView()) == [0, 1]
    finally:
        ray.shutdown()
    for n in range(2):
        assert read_status(status_path(prefix, n))["done"] is True
        assert (tmp_path / str(n)).read_text() == str(n)


def test_ray_engine_without_ray_is_a_one_line_error(monkeypatch):
    import builtins
    real_import = builtins.__import__

    def no_ray(name, *a, **kw):
        if name == "ray":
            raise ImportError("No module named 'ray'")
        return real_import(name, *a, **kw)
    monkeypatch.setattr(builtins, "__import__", no_ray)
    with pytest.raises(RuntimeError, match="pip install"):
        make_engine("ray", TaskManager(StubConfigs()))
