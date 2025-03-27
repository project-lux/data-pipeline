
from ._task_ui_manager import TaskUiManager
from lux_pipeline.process.base.loader import Loader
import logging

class LoadManager(TaskUiManager):
    """
    """
    def __init__(self, configs, max_workers: int = 0):
        super().__init__(configs, max_workers)
        self.overwrite = True
        self.load_type = "records"

    def _distributed(self, bars, messages, n):
        super()._distributed(bars, messages, n)
        for (which, src) in self.sources:
            ldr = getattr(self.configs, which)[src]['loader']
            try:
                ldr.prepare_load(self, n, self.max_workers, self.load_type)
                ldr.load(disable_ui=self.disable_ui, verbose=self.verbose, overwrite=self.overwrite)
            except Exception as e:
                self.log(logging.ERROR, f"[red]Failed to load")
                self.log(logging.ERROR, e)

    def maybe_add(self, which, cfg):
        if 'loader' in cfg and isinstance(cfg['loader'], Loader):
            self.sources.append((which, cfg['name']))
