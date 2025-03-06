
import multiprocessing
import time
from concurrent.futures import ProcessPoolExecutor
from pipeline.cli.entry import cfgs
from pipeline.cli._rich import get_bar_from_layout


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
        self.sources = []

    def _distributed(self, bars, n):
        self.bars = bars
        self.configs = cfgs
        # And do any other initial, non-task specific set up

    def update_progress_bar(self, which, advance=None, total=None, description=None, completed=None):
        curr = self.bars[which]
        new = curr.copy()
        if total is not None:
            new['total'] = total
        if completed is not None:
            new['completed'] = completed
        if description is not None:
            new['description'] = description
        if advance is not None:
            new['completed'] = new['completed'] + advance
        self.bars[which] = new


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
        self.configs = None
        self.disable_ui = disable_ui
        self.verbose = verbose
        for (a,v) in args.items():
            if hasattr(self, a):
                setattr(self, a, v)

        with multiprocessing.Manager() as manager:
            if not self.disable_ui:
                bars = manager.dict() # keep track of tasks across Processes
                for b in range(self.max_workers):
                    bars[b] = {'total': 1000000, 'completed': 0}
            else:
                bars = {}

            futures = []

            # hide unnecessary bars
            if len(self.sources) < self.max_workers:
                for n in range(len(self.sources), self.max_workers):
                    bar = get_bar_from_layout(layout, n)
                    bar[0].update(bar[1], visible=False)

            with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                for n in range(self.max_workers):
                    futures.append(executor.submit(self._distributed, bars, n))
                if not self.disable_ui:
                    while (n_finished := sum([future.done() for future in futures])) < len(futures):
                        for (k,v) in bars.items():
                            if v:
                                bar = get_bar_from_layout(layout, k)
                                bar[0].update(bar[1], **v)
                        time.sleep(0.25)
                for future in futures:
                    future.result()
                if not self.disable_ui:
                    time.sleep(1)
                    for (k,v) in bars.items():
                        if v:
                            bar = get_bar_from_layout(layout, k)
                            bar[0].update(bar[1], **v)

                    time.sleep(2)
