import os
import time
from lux_pipeline.cli.entry import cfgs
from lux_pipeline.cli._rich import get_bar_from_layout
import logging
import ray
import traceback

def init_logger():
    return logging.getLogger("lux_pipeline")

class DriverLogHandler(logging.Handler):
    def __init__(self, manager):
        super().__init__()
        self.manager = manager

    def emit(self, record):
        #self.manager.log(record.levelno, record.getMessage())
        print(f" ||| {record.getMessage()}")

class TaskUiManager:
    """
    """
    def __init__(self, configs, max_workers: int = 0):
        self.configs = configs
        self.verbose = False
        self.disable_ui = False
        if max_workers > 0:
            self.max_workers = max_workers
        else:
            self.max_workers = configs.max_workers
        self.bars = {}
        self.messages = []
        self.sources = []

    def _distributed(self,n):
        self.configs = cfgs
        self.my_slice = n
        self.last_log_time = time.time()
        self.logger = init_logger()
        while self.logger.handlers:
            self.logger.handlers.remove(self.logger.handlers[0])
        self.bars = {'total': -1, 'completed': 0, 'description': '', 'n': self.my_slice}

    def update_progress_bar(self, advance=None, total=None, description=None, completed=None):
        curr = self.bars
        if total is not None:
            curr['total'] = total
        if completed is not None:
            curr['completed'] = completed
        if description is not None:
            curr['description'] = description
        if advance is not None:
            curr['completed'] = curr['completed'] + advance

        if time.time() > self.last_log_time + 1:
            self.last_log_time = time.time()
            self.logger.info(curr)

    def log(self, level, message):
        if isinstance(message, Exception):
            message = "".join(traceback.format_exception(type(message), message, message.__traceback__))
        self.logger.log(level, message)

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

    def process(self, layout, disable_ui=False, verbose=None, **args) -> bool:
        # This is necessary to set to None so it can be pickled to send to
        # remote tasks
        self.configs = None
        self.disable_ui = disable_ui
        self.verbose = verbose
        for (a,v) in args.items():
            if hasattr(self, a):
                setattr(self, a, v)

        logger = init_logger()
        while logger.handlers:
            logger.removeHandler(logger.handlers[0])
        logger.addHandler(DriverLogHandler(self))

        if self.max_workers == 1:
            # Just do it in our main process
            pass
        else:
            futures = []
            print("Sending tasks")
            for n in range(self.max_workers):
                futures.append(self._distributed.remote(self, n))
            while futures:
                ready_refs, futures = ray.wait(futures, num_returns=1, timeout=None)
                # ready_refs has one result in it
                res = ray.get(ready_refs)
                for ref in res:
                    if type(ref) == int:
                        print("got int")
                    else:
                        txt = ray.get(ref)
                        print(f"Got txt from remote: {txt}")
                        n = txt['n']
                        del txt['n']
                        logger.info(txt)
                        bar = get_bar_from_layout(layout, n)
                        bar[0].update(bar[1], **txt)

            print("Done")

