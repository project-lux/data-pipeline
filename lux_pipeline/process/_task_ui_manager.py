import os
import multiprocessing
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from lux_pipeline.cli.entry import cfgs
from lux_pipeline.cli._rich import get_bar_from_layout
import logging
logger = logging.getLogger("lux_pipeline")
import traceback

class TaskLogHandler(logging.Handler):
    def __init__(self, manager):
        super().__init__()
        self.manager = manager

    def emit(self, record):
        self.manager.log(record.levelno, record.getMessage())

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

    def _distributed(self, bars, messages, n):
        self.bars = bars
        self.messages = messages
        self.configs = cfgs
        self.my_slice = n
        logger = logging.getLogger("lux_pipeline")
        if logger.handlers:
            logger.removeHandler(logger.handlers[0])
        logger.addHandler(TaskLogHandler(self))
        # And do any other initial, non-task specific set up

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

    def log(self, level, message):
        if isinstance(message, Exception):
            message = "".join(traceback.format_exception(type(message), message, message.__traceback__))
        self.messages.append((level, message))

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


    def process_single(self, layout, disable_ui=False, verbose=None, **args):


    def process(self, layout, disable_ui=False, verbose=None, **args) -> bool:
        # local_configs = self.configs
        # This is necessary to set to None so it can be pickled to send to
        # remote tasks
        self.configs = None
        self.disable_ui = disable_ui
        self.verbose = verbose
        for (a,v) in args.items():
            if hasattr(self, a):
                setattr(self, a, v)

        if self.max_workers == 1:
            # Don't make a single thread
            # instead just attach the logger directly

        with multiprocessing.Manager() as manager:

            bars = {} # keep track of tasks across Processes
            messages = {}
            for b in range(self.max_workers):
                bars[b] = manager.dict({'total': 1000000, 'completed': 0})
            for b in range(self.max_workers):
                messages[b] = manager.list([])

            # FIXME: This should be a StreamHandler on the logger
            if hasattr(cfgs, 'log_file'):
                log_fh = open(cfgs.log_file, 'a')
            else:
                fn = "lux_log_command.txt"  # FIXME: replace command with CLI command
                if hasattr(cfgs, 'log_dir'):
                    fn = os.path.join(cfgs.log_dir, fn)
                log_fh = open(fn, 'a')


            logger.info("Starting...")
            log_fh.write("----- Starting -----\n")
            futures = []
            with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                for n in range(self.max_workers):
                    futures.append(executor.submit(self._distributed, bars[n], messages[n], n))
                if not self.disable_ui:
                    while (n_finished := sum([future.done() for future in futures])) < len(futures):
                        for (k,v) in bars.items():
                            if v:
                                bar = get_bar_from_layout(layout, k)
                                bar[0].update(bar[1], **v)
                        for (k,v) in messages.items():
                            while v:
                                lvl, msg = v.pop(0)
                                logger.log(lvl, msg)
                                log_fh.write(msg + "\n")
                            log_fh.flush()
                        time.sleep(0.25)
                for future in as_completed(futures):
                    future.result()
                if not self.disable_ui:
                    time.sleep(1)
                    for (k,v) in bars.items():
                        if v:
                            bar = get_bar_from_layout(layout, k)
                            bar[0].update(bar[1], **v)
                        for (k,v) in messages.items():
                            while v:
                                lvl, msg = v.pop(0)
                                logger.log(lvl, msg)
                                log_fh.write(msg + "\n")
                            log_fh.flush()
                    time.sleep(5)
                    log_fh.close()
