import os
import time
from lux_pipeline.cli.entry import cfgs
from lux_pipeline.cli._rich import get_bar_from_layout
import logging
import ray
import traceback

def init_logger():
    return logging.getLogger("lux_pipeline")


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
            self.bars[which]['completed'] = completed
        if total != -1:
            self.bars[which]['total'] = total
        if description is not None:
            self.bars[which]['description'] = description

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


class TaskUiManager:

    def __init__(self, configs, max_workers: int = 0):
        self.configs = configs
        self.verbose = False
        self.disable_ui = False
        self.actor = None
        if max_workers > 0:
            self.max_workers = max_workers
        else:
            self.max_workers = configs.max_workers
        self.bars = {}
        self.sources = []
        self.last_log_time = 0

    def _distributed(self, n, actor):
        self.configs = cfgs
        self.my_slice = n
        self.actor = actor
        self.last_log_time = time.time()
        self.bar = {'total': -1, 'completed': -1, 'description': None}


    def update_progress_bar(self, advance=None, total=None, description=None, completed=None):
        if total is not None:
            self.bar['total'] = total
        if completed is not None:
            self.bar['completed'] = completed
        if description is not None:
            self.bar['description'] = description
        if advance is not None and completed is None:
            if self.bar['completed'] == -1:
                self.bar['completed'] = advance
            else:
                self.bar['completed'] = self.bar['completed'] + advance

        if time.time() > self.last_log_time + 0.5:
            self.last_log_time = time.time()
            # send to IPC actor
            if self.actor is not None:
                self.actor.update_progress_bar.remote(self.my_slice, self.bar['completed'], self.bar['total'], self.bar['description'])


    def log(self, level, message):
        if isinstance(message, Exception):
            message = "".join(traceback.format_exception(type(message), message, message.__traceback__))
        if self.actor is not None:
            # Don't batch log messages?
            self.actor.add_to_log.remote(self.my_slice, level, message)

    def maybe_add(self, which, cfg):
        # SubClasses should re-define this to actually test
        self.sources.append((which, cfg['name']))

    def prepare_single(self, name) -> bool:
        if name in self.configs.internal:
            self.maybe_add('internal', self.configs.internal[name])
        elif name in self.configs.external:
            self.maybe_add('external', self.configs.external[name])
        else:
            raise ValueError(f"Unknown source: {name}")

    def prepare_all(self) -> bool:
        for cfg in self.configs.external.values():
            self.maybe_add('external', cfg)
        for cfg in self.configs.internal.values():
            self.maybe_add('internal', cfg)

    def process(self, layout, disable_ui=False, **args) -> bool:
        # This is necessary to set to None so it can be pickled to send to
        # remote tasks
        self.configs = None
        self.disable_ui = disable_ui
        for (a,v) in args.items():
            if hasattr(self, a):
                setattr(self, a, v)

        logger = init_logger()
        log_actor = LoggingActor.remote()

        if self.max_workers == 1:
            # Just do it in our main process
            pass
        else:
            logger.info("Sending tasks")

            futures = [self._distributed.remote(self, i, log_actor) for i in range(self.max_workers)]
            while futures:
                ready_refs, futures = ray.wait(futures, num_returns=1, timeout=0.5)
                for n in range(self.max_workers):
                    resp = ray.get(log_actor.get_log.remote(n))
                    for lvl, msg in resp:
                        # send to log to render
                        logger.log(lvl, msg)
                    resp = ray.get(log_actor.get_progress_bar.remote(n))
                    if resp['completed'] > -1:
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

            logger.info("Done")

