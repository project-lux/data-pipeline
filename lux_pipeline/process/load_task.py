
from ._task_ui_manager import TaskUiManager
from lux_pipeline.process.base.loader import Loader
import logging
import ray
import time

class LoadManager(TaskUiManager):
    """
    """
    def __init__(self, configs, max_workers: int = 0):
        super().__init__(configs, max_workers)
        self.overwrite = True
        self.load_type = "records"

    def _distributed(self, n):
        super()._distributed(n)
        for (which, src) in self.sources:
            ldr = getattr(self.configs, which)[src]['loader']
            try:
                ldr.prepare(self, n, self.max_workers, self.load_type)
                ldr.process(disable_ui=self.disable_ui, overwrite=self.overwrite)
            except Exception as e:
                self.log(logging.CRITICAL, f"Failed to load {which} in {self.my_slice}")
                self.log(logging.CRITICAL, e)
                time.sleep(1)
                return 0
        time.sleep(1)
        return 1

    def maybe_add(self, which, cfg):
        if 'loader' in cfg and isinstance(cfg['loader'], Loader):
            self.sources.append((which, cfg['name']))
