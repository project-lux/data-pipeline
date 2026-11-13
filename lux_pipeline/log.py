"""The one logger every module in the package shares.

Use ``from lux_pipeline.log import logger`` or
``logging.getLogger("lux_pipeline")``; they are the same object.

`timing_logger` (``lux_pipeline.timing``) carries `PhaseTimer`'s reports:
rates, cpu, the share of each stage. A run sends it to its own file
(`send_timing_to`) so the run's log stays readable; otherwise it
propagates to ``lux_pipeline`` like any child logger.
"""

import logging
import os

logger = logging.getLogger("lux_pipeline")
timing_logger = logging.getLogger("lux_pipeline.timing")


def timing_state():
    """What `send_timing_to` changes, for `restore_timing`."""
    return list(timing_logger.handlers), timing_logger.propagate, timing_logger.level


def restore_timing(state):
    """Put `timing_logger` back as `timing_state` found it, closing the
    file handlers it has now that it did not have then."""
    handlers, propagate, level = state
    for h in list(timing_logger.handlers):
        if h not in handlers:
            timing_logger.removeHandler(h)
            h.close()
    for h in handlers:
        if h not in timing_logger.handlers:
            timing_logger.addHandler(h)
    timing_logger.propagate = propagate
    timing_logger.setLevel(level)


def timing_run_dir():
    """The run directory `timing_logger` writes to: the directory of the
    file `send_timing_to` gave it (``<prefix>/w<n>.timing`` in a worker,
    ``<prefix>/driver.timing`` in the driver), or None outside a run.
    PhaseTimer's JSON and profiles go there too (`timing.timing_out_dir`)."""
    for h in timing_logger.handlers:
        path = getattr(h, "lux_timing_file", None)
        if path:
            return os.path.dirname(path) or None
    return None


def send_timing_to(path):
    """Write `timing_logger`'s records to `path` only, whatever the run's
    log level (the reports are INFO, and a run at ``--log WARNING`` still
    wants them); ``None``: no file, propagate to ``lux_pipeline`` again.
    Replaces the file it wrote to before. Returns the handler or None."""
    for h in list(timing_logger.handlers):
        timing_logger.removeHandler(h)
        h.close()
    if not path:
        timing_logger.propagate = True
        timing_logger.setLevel(logging.NOTSET)
        return None
    d = os.path.dirname(path)
    if d:
        # a run's directory, made by whichever of its files comes first;
        # one that cannot be made fails on the first report, as before
        try:
            os.makedirs(d, exist_ok=True)
        except OSError:
            pass
    handler = logging.FileHandler(path, delay=True)
    handler.setFormatter(logging.Formatter("%(asctime)s %(message)s", datefmt="%H:%M:%S"))
    handler.lux_timing_file = path
    timing_logger.addHandler(handler)
    timing_logger.propagate = False
    timing_logger.setLevel(logging.INFO)
    return handler
