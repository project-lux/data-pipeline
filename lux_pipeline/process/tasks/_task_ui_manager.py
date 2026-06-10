import multiprocessing
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from lux_pipeline.cli.entry import cfgs
from lux_pipeline.cli._rich import get_bar_from_layout, add_worker_log_line
import logging
import ray
import traceback

logger = logging.getLogger("lux_pipeline")


@ray.remote
class ProgressActor:
    """Holds per-worker progress bar state for the driver to poll. Log messages
    do not pass through here -- workers write them to their own files and the
    driver tails those."""

    def __init__(self):
        self.bars = {}

    def update_progress_bar(self, which, completed=-1, total=-1, description=None):
        if which not in self.bars:
            self.bars[which] = {"completed": -1, "total": -1, "description": ""}
        if completed != -1:
            self.bars[which]["completed"] = completed
        if total != -1:
            self.bars[which]["total"] = total
        if description is not None:
            self.bars[which]["description"] = description

    def get_progress_bar(self, which=-1):
        if which == -1:
            return self.bars
        try:
            return self.bars[which]
        except Exception:
            self.bars[which] = {"completed": -1, "total": -1, "description": ""}
            return self.bars[which]


def make_file_handler(manager, my_slice):
    # Filter at the user's --log level here in the worker: the lux_pipeline
    # logger itself is set to DEBUG, and the hot paths log per-record DEBUG
    # messages that would otherwise be written only to be discarded
    lvl = (manager.log_level or "INFO").upper()
    if not hasattr(logging, lvl):
        lvl = "INFO"
    handler = logging.FileHandler(f"{manager.log_file_prefix}-w{my_slice}.log", delay=True)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(message)s", datefmt="%H:%M:%S"))
    handler.setLevel(getattr(logging, lvl))
    return handler


def attach_worker_log_handler(manager, my_slice):
    # The worker's file is the log of record; log messages never cross the
    # process boundary. The parent tails the files for the live display, so a
    # slow or stalled UI cannot block the workers.
    logger = logging.getLogger("lux_pipeline")
    while logger.handlers:
        logger.removeHandler(logger.handlers[0])
    if manager.log_file_prefix:
        logger.addHandler(make_file_handler(manager, my_slice))
    else:
        logger.addHandler(logging.NullHandler())


class LogTailer:
    """Incrementally reads complete new lines from each worker's log file,
    to feed the live display."""

    def __init__(self, prefix, max_workers):
        self.prefix = prefix
        self.max_workers = max_workers
        self.offsets = {}

    def read_new_lines(self):
        if not self.prefix:
            return []
        lines = []
        for n in range(self.max_workers):
            try:
                with open(f"{self.prefix}-w{n}.log", "rb") as fh:
                    fh.seek(self.offsets.get(n, 0))
                    data = fh.read()
            except OSError:
                continue
            # only consume up to the last newline; the worker may be mid-write
            nl = data.rfind(b"\n")
            if nl == -1:
                continue
            self.offsets[n] = self.offsets.get(n, 0) + nl + 1
            for line in data[:nl].split(b"\n"):
                lines.append((n, line.decode("utf-8", errors="replace")))
        return lines


class ProcessingEngine:
    def __init__(self, manager):
        self.manager = manager
        self.max_workers = manager.max_workers
        self.last_bar_time = 0
        self.temp_bar = {"total": -1, "completed": -1, "description": ""}
        self.tailer = LogTailer(manager.log_file_prefix, self.max_workers)

    def refresh_logs(self, layout):
        if layout is None:
            return
        self.tailer.max_workers = self.max_workers
        for n, line in self.tailer.read_new_lines():
            add_worker_log_line(layout, n, line)


class NullEngine(ProcessingEngine):
    # rely on os/shell level parallelization
    # Here there's only one process, so we'll route straight to _distributed
    # but if max_workers and my_worker are set, then only do that slice
    # can still run with the UI to watch the log and a single bar

    def _distributed(self, i, log_details):
        return self.manager._distributed(i)

    def maybe_update_progress_bar(self, completed, total, description):
        if time.time() > self.last_bar_time + 0.5:
            self.last_bar_time = time.time()
            if self.layout is not None:
                bar = get_bar_from_layout(self.layout, self.manager.my_slice)
                bar[0].update(bar[1], completed=completed, total=total, description=description)

    def process(self, layout):
        i = self.manager.my_slice
        self.layout = layout
        if self.manager.log_file_prefix:
            # single process: keep the local/UI handlers and also write the file
            logging.getLogger("lux_pipeline").addHandler(make_file_handler(self.manager, max(i, 0)))
        return self._distributed(i, None)


class MpEngine(ProcessingEngine):
    # Use multiprocessing to distribute

    def _distributed(self, i, bar):
        self.bar = bar
        self.my_slice = i
        self.temp_bar = {"total": -1, "completed": -1, "description": ""}

        attach_worker_log_handler(self.manager, i)

        ret = self.manager._distributed(i)
        # the send throttle would otherwise drop the final bar state
        self.bar.update(self.temp_bar)
        return ret

    def maybe_update_progress_bar(self, completed, total, description):
        # The shared dict is a Manager proxy -- every assignment is a round
        # trip to the Manager server process -- so send at most 2/second
        if completed > -1:
            self.temp_bar["completed"] = completed
        if total > -1:
            self.temp_bar["total"] = total
        if description:
            self.temp_bar["description"] = description
        if time.time() > self.last_bar_time + 0.5:
            self.last_bar_time = time.time()
            self.bar.update(self.temp_bar)

    def refresh(self, layout, bars):
        self.refresh_logs(layout)
        if layout is not None:
            for k, v in bars.items():
                bar = get_bar_from_layout(layout, k)
                bar[0].update(bar[1], **dict(v))

    def process(self, layout):
        with multiprocessing.Manager() as mgr:
            # shared progress state per worker; logs go via files, not IPC
            bars = {}
            for b in range(self.max_workers):
                bars[b] = mgr.dict({"total": -1, "completed": -1, "description": ""})

            if self.manager.log_file_prefix:
                logger.info(f"Worker logs: {self.manager.log_file_prefix}-w*.log")
            logger.info("Starting...")
            futures = []
            with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                for n in range(self.max_workers):
                    futures.append(executor.submit(self._distributed, n, bars[n]))
                if not self.manager.disable_ui:
                    while (sum([future.done() for future in futures])) < len(futures):
                        self.refresh(layout, bars)
                        time.sleep(0.5)
                for future in as_completed(futures):
                    future.result()
                if not self.manager.disable_ui:
                    time.sleep(1)
                    self.refresh(layout, bars)


class RayEngine(ProcessingEngine):
    # Use ray to distribute

    def __init__(self, manager):
        super().__init__(manager)
        self.actor = None
        # Only instantiate the environment once
        # so call init here, not in process
        # init spits out a print() message
        # but without log_to_driver, can't capture it?
        ray.init(log_to_driver=False)

    # Entry point for distributed tasks
    @ray.remote
    def _distributed(self, i, actor):
        self.actor = actor
        self.my_slice = i
        self.temp_bar = {"total": -1, "completed": -1, "description": ""}

        attach_worker_log_handler(self.manager, i)

        ret = self.manager._distributed(i)
        # the send throttle would otherwise drop the final bar state
        self.actor.update_progress_bar.remote(self.my_slice, **self.temp_bar)
        return ret

    def maybe_update_progress_bar(self, completed, total, description):
        if completed > -1:
            self.temp_bar["completed"] = completed
        if total > -1:
            self.temp_bar["total"] = total
        if description:
            self.temp_bar["description"] = description
        if time.time() > self.last_bar_time + 0.5:
            self.last_bar_time = time.time()
            if self.actor is not None:
                self.actor.update_progress_bar.remote(self.my_slice, **self.temp_bar)

    def refresh(self, layout, actor):
        self.refresh_logs(layout)
        # one round trip for every worker's bar state
        bars = ray.get(actor.get_progress_bar.remote(-1))
        for n, state in bars.items():
            if state["completed"] > -1:
                if layout is not None:
                    bar = get_bar_from_layout(layout, n)
                    bar[0].update(bar[1], **state)
                else:
                    print(f"[{n}]: {state['completed']}/{state['total']}")

    def process(self, layout):
        actor = ProgressActor.remote()
        if self.manager.log_file_prefix:
            logger.info(f"Worker logs: {self.manager.log_file_prefix}-w*.log")
        logger.info("Sending tasks")
        futures = [self._distributed.remote(self, i, actor) for i in range(self.max_workers)]
        while futures:
            ready_refs, futures = ray.wait(futures, num_returns=1, timeout=0.5)
            self.refresh(layout, actor)
            if ready_refs:
                ray.get(ready_refs[0])
        time.sleep(1)
        self.refresh(layout, actor)
        logger.info("Done")


class TaskUiManager:
    def __init__(self, configs, max_workers: int = 0, args=None):
        self.configs = configs
        self.disable_ui = False
        if max_workers > 0:
            self.max_workers = max_workers
        else:
            self.max_workers = configs.max_workers
        self.sources = []
        self.my_slice = -1
        self.log_level = "INFO"
        self.log_file_prefix = None
        if args is not None:
            if hasattr(args, "my_worker") and args.my_worker > -1:
                self.my_slice = args.my_worker
            if getattr(args, "log", None):
                self.log_level = args.log

        self.engine = None
        self.local_debug = False
        self.command_line_args = args

        self.bar = {"total": -1, "completed": -1, "description": None}

    def _distributed(self, n):
        self.configs = cfgs
        self.my_slice = n

    def update_progress_bar(self, advance=-1, total=-1, description=None, completed=-1):
        if total > -1:
            self.bar["total"] = total
        if completed > -1:
            self.bar["completed"] = completed
        if description is not None:
            self.bar["description"] = description
        if advance > -1:
            self.bar["completed"] = self.bar["completed"] + advance
        if self.engine is not None:
            self.engine.maybe_update_progress_bar(self.bar["completed"], self.bar["total"], self.bar["description"])

    def log(self, level, message):
        if isinstance(message, Exception):
            message = "".join(traceback.format_exception(type(message), message, message.__traceback__))
        logger.log(level, message)

    def maybe_add(self, which, cfg):
        # SubClasses should re-define this to actually test
        self.sources.append((which, cfg["name"]))

    def prepare_single(self, name) -> bool:
        if name in self.configs.internal:
            self.maybe_add("internal", self.configs.internal[name])
        elif name in self.configs.external:
            self.maybe_add("external", self.configs.external[name])
        else:
            raise ValueError(f"Unknown source: {name}")

    def prepare_all(self) -> bool:
        for cfg in self.configs.external.values():
            self.maybe_add("external", cfg)
        for cfg in self.configs.internal.values():
            self.maybe_add("internal", cfg)

    def process(self, layout, engine="mp", disable_ui=False, **args) -> bool:
        # local_configs = self.configs
        # This is necessary to set to None so it can be pickled to send to
        # remote tasks
        self.configs = None
        self.disable_ui = disable_ui
        for a, v in args.items():
            if hasattr(self, a):
                setattr(self, a, v)

        # Here call out to engine
        logger.info(f"Creating {engine} processing engine")
        if engine == "ray":
            engine = RayEngine(self)
        elif engine == "mp":
            engine = MpEngine(self)
        elif engine == "null":
            engine = NullEngine(self)
        self.engine = engine
        engine.process(layout)

    def debug_process(self, **args):
        self.local_debug = True
        return self.process(None, engine="null", disable_ui=True, **args)
