
from ._task_ui_manager_ray import TaskUiManager
from lux_pipeline.process.base.loader import Loader
import logging
import ray

class LoadManager(TaskUiManager):
    """
    """
    def __init__(self, configs, max_workers: int = 0):
        super().__init__(configs, max_workers)
        self.overwrite = True
        self.load_type = "records"

    @ray.remote
    def _distributed(self, n, actor):
        super()._distributed(n, actor)
        for (which, src) in self.sources:
            ldr = getattr(self.configs, which)[src]['loader']
            try:
                ldr.prepare_load(self, n, self.max_workers, self.load_type)
                ldr.load(disable_ui=self.disable_ui, overwrite=self.overwrite)
            except Exception as e:
                print(f"Failed to load")
                raise
        return 1


    def maybe_add(self, which, cfg):
        if 'loader' in cfg and isinstance(cfg['loader'], Loader):
            self.sources.append((which, cfg['name']))
