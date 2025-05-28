import os
import multiprocessing
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from lux_pipeline.cli.entry import cfgs
from lux_pipeline.cli._rich import get_bar_from_layout
import logging
import ray

logger = logging.getLogger("lux_pipeline")
import traceback


@ray.remote
class LoggingActor:
    def __init__(self):
        self.logs = {}
        self.bars = {}

    def add_to_log(self, which, level, txt):
        try:
            self.logs[which].append((level, txt))
        except:
            self.logs[which] = [(level, txt)]

    def get_log(self, which):
        if which == -1:
            return self.logs
        try:
            l = self.logs[which]
            self.logs[which] = []
            return l
        except:
            self.logs[which] = []
            return []

    def update_progress_bar(self, which, completed=-1, total=-1, description=None):
        if not which in self.bars:
            self.bars[which] = {"completed": -1, "total": -1, "description": ""}
        if completed != -1:
            self.bars[which]["completed"] = completed
        if total != -1:
            self.bars[which]["total"] = total
        if description is not None:
            self.bars[which]["description"] = description

    def get_progress_bar(self, which):
        if which == -1:
            return self.bars
        try:
            return self.bars[which]
        except:
            self.bars[which] = {"completed": -1, "total": -1, "description": ""}
            return self.bars[which]


class TaskLogHandler(logging.Handler):
    def __init__(self, manager):
        super().__init__()
        self.manager = manager

    def emit(self, record):
        self.manager.log(record.levelno, record.getMessage())


class ProcessingEngine:
    def __init__(self, manager):
        self.manager = manager
        self.max_workers = manager.max_workers
        self.last_bar_time = 0
        self.last_log_time = 0


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

    def maybe_update_log(self, level, message):
        # route directly to logger
        logger = logging.getLogger("lux_pipeline")
        logger.log(level, message)

    def process(self, layout):
        i = self.manager.my_slice
        self.layout = layout
        return self._distributed(i, None)


class MpEngine(ProcessingEngine):
    # Use multiprocessing to distribute

    def _distributed(self, i, log_details):
        # deal with log_actor here
        self.bar = log_details[0]
        self.messages = log_details[1]
        self.my_slice = i
        self.temp_bar = {"total": -1, "completed": -1, "description": ""}
        self.temp_log = []

        logger = logging.getLogger("lux_pipeline")
        if logger.handlers:
            logger.removeHandler(logger.handlers[0])
        logger.addHandler(TaskLogHandler(self.manager))

        return self.manager._distributed(i)

    def maybe_update_progress_bar(self, completed, total, description):
        # Shared memory, so no need to batch and send
        if completed > -1:
            self.temp_bar["completed"] = completed
        if total > -1:
            self.temp_bar["total"] = total
        if description:
            self.temp_bar["description"] = description
        if time.time() > self.last_bar_time + 0.5:
            self.last_bar_time = time.time()
            self.bar["completed"] = self.temp_bar["completed"]
            self.bar["total"] = self.temp_bar["total"]
            self.bar["description"] = self.temp_bar["description"]

    def maybe_update_log(self, level, message):
        self.temp_log.append((level, message))
        if time.time() > self.last_log_time + 0.5:
            self.messages[:] = []
            for l in self.temp_log:
                self.messages.append(l)
            self.temp_log = []

    def process(self, layout):
        cfgs = self.manager.configs

        with multiprocessing.Manager() as mgr:
            bars = {}  # keep track of tasks across Processes
            messages = {}
            for b in range(self.max_workers):
                bars[b] = mgr.dict({"total": -1, "completed": -1, "description": ""})
            for b in range(self.max_workers):
                messages[b] = mgr.list([])

            if hasattr(cfgs, "log_file"):
                log_fh = open(cfgs.log_file, "a")
            else:
                fn = "lux_log_command.txt"  # FIXME: replace command with CLI command
                if hasattr(cfgs, "log_dir"):
                    fn = os.path.join(cfgs.log_dir, fn)
                log_fh = open(fn, "a")

            logger.info("Starting...")
            log_fh.write("----- Starting -----\n")
            futures = []
            with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                for n in range(self.max_workers):
                    futures.append(executor.submit(self._distributed, n, [bars[n], messages[n]]))
                if not self.manager.disable_ui:
                    while (n_finished := sum([future.done() for future in futures])) < len(futures):
                        for k, v in bars.items():
                            if v:
                                bar = get_bar_from_layout(layout, k)
                                bar[0].update(bar[1], **v)
                        for k, v in messages.items():
                            while v:
                                lvl, msg = v.pop(0)
                                logger.log(lvl, msg)
                                log_fh.write(msg + "\n")
                            log_fh.flush()
                        time.sleep(0.5)
                for future in as_completed(futures):
                    future.result()
                if not self.manager.disable_ui:
                    time.sleep(1)
                    for k, v in bars.items():
                        if v:
                            bar = get_bar_from_layout(layout, k)
                            bar[0].update(bar[1], **v)
                        for k, v in messages.items():
                            while v:
                                lvl, msg = v.pop(0)
                                logger.log(lvl, msg)
                                log_fh.write(msg + "\n")
                            log_fh.flush()
                    time.sleep(5)
                    log_fh.close()


class RayEngine(ProcessingEngine):
    # Use ray to distribute

    def __init__(self, manager):
        super().__init__(manager)
        # Only instantiate the environment once
        # so call init here, not in process
        # init spits out a print() message
        # but without log_to_driver, can't capture it?
        ray.init(log_to_driver=False)

    # Entry point for distributed tasks
    @ray.remote
    def _distributed(self, i, log_actor):
        # deal with log_actor here
        self.actor = log_actor
        self.my_slice = i

        logger = logging.getLogger("lux_pipeline")
        if logger.handlers:
            logger.removeHandler(logger.handlers[0])
        logger.addHandler(TaskLogHandler(self.manager))

        return self.manager._distributed(i)

    def maybe_update_progress_bar(self, completed, total, description):
        if time.time() > self.last_bar_time + 0.5:
            self.last_bar_time = time.time()
            if self.actor is not None:
                self.actor.update_progress_bar.remote(self.my_slice, completed, total, description)

    def maybe_update_log(self, level, message):
        # Use same technique as MP engine to batch logs?
        # if time.time() > self.last_log_time + 0.5:
        #    self.last_log_time = time.time()
        if self.actor is not None:
            self.actor.add_to_log.remote(self.my_slice, level, message)

    def process(self, layout):
        log_actor = LoggingActor.remote()
        logger.info("Sending tasks")
        futures = [self._distributed.remote(self, i, log_actor) for i in range(self.max_workers)]
        while futures:
            ready_refs, futures = ray.wait(futures, num_returns=1, timeout=0.5)
            time.sleep(0.5)
            for n in range(self.max_workers):
                resp = ray.get(log_actor.get_log.remote(n))
                for lvl, msg in resp:
                    # send to log to render
                    logger.log(lvl, msg)
                resp = ray.get(log_actor.get_progress_bar.remote(n))
                if resp["completed"] > -1:
                    # process log entry r from process n
                    if layout is not None:
                        bar = get_bar_from_layout(layout, n)
                        bar[0].update(bar[1], **resp)
                    else:
                        print(f"[{n}]: {resp['completed']}/{resp['total']}")
            if ready_refs:
                # Completed tasks
                res = ray.get(ready_refs[0])
                if type(res) == int:
                    # process response from task
                    pass
                # logger.log(logging.DEBUG, f"Got {res} from task")

        time.sleep(1)
        for n in range(self.max_workers):
            resp = ray.get(log_actor.get_log.remote(n))
            for lvl, msg in resp:
                # send to log to render
                logger.log(lvl, msg)
            resp = ray.get(log_actor.get_progress_bar.remote(n))
            if resp["completed"] > -1:
                # process log entry r from process n
                if layout is not None:
                    bar = get_bar_from_layout(layout, n)
                    bar[0].update(bar[1], **resp)
                else:
                    print(f"[{n}]: {resp['completed']}/{resp['total']}")
        time.sleep(1)
        logger.info("Done")


class TaskUiManager:
    def __init__(self, configs, max_workers: int = 0):
        self.configs = configs
        self.disable_ui = False
        if max_workers > 0:
            self.max_workers = max_workers
        else:
            self.max_workers = configs.max_workers
        self.sources = []
        self.my_slice = -1
        self.engine = None

        self.bar = {"total": -1, "completed": -1, "description": None}
        self.messages = []
        self.actor = None

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
        if self.engine is not None:
            self.engine.maybe_update_log(level, message)

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
