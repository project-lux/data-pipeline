
from pipeline.process._task_ui_manager import TaskUiManager
from pipeline.process.base.loader import Loader

class LoadManager(TaskUiManager):
    """
    """
    def __init__(self, configs, max_workers: int = 0):
        super().__init__(configs, max_workers)
        self.overwrite = True
        self.load_type = "records"

    def _distributed(self, bars, n):
        super()._distributed(bars, n)
        for (which, src) in self.sources:
            ldr = getattr(self.configs, which)[src]['loader']
            ldr.prepare_load(self, n, self.max_workers, self.load_type)
            ldr.load(disable_ui=self.disable_ui, verbose=self.verbose, overwrite=self.overwrite)

    def maybe_add(self, which, cfg):
        if 'loader' in cfg and isinstance(cfg['loader'], Loader):
            self.sources.append((which, cfg['name']))

