from ._task_ui_manager import TaskUiManager
from lux_pipeline.process.base.index_loader import IndexLoader

class IndexLoadManager(TaskUiManager):

    def __init__(self, configs, max_workers: int = 0, args=None):
        super().__init__(configs, max_workers, args)
        if args is not None:
            self.overwrite = not args.no_overwrite
            self.load_type = args.type
        else:
            self.overwrite = False
            self.load_type = "records"


    def _distributed(self, n):
        super()._distributed(n)
        for (which, src) in self.sources:
            ldr = getattr(self.configs, which)[src]['indexLoader']
            try:
                ldr.prepare(self, n, self.max_workers)
                ldr.process(disable_ui=self.disable_ui, overwrite=self.overwrite)
            except Exception as e:
                print(f"Failed to load")
                raise
        return 1


    def maybe_add(self, which, cfg):
        if 'loader' in cfg and isinstance(cfg['indexLoader'], IndexLoader):
            self.sources.append((which, cfg['name']))
