import logging
from ._task_ui_manager import TaskUiManager
from lux_pipeline.cli._rich import get_bar_from_layout
import logging

class DownloadManager(TaskUiManager):
    """
    DownloadManager is responsible for downloading files from the internet.
    Each source configuration file contains a Downloader class. The Downloader class is responsible for preparing a list of urls that need to be downloaded. To see an example, see the BaseDownloader class.
    """
    def __init__(self, configs, max_workers: int = 0):
        self.download_type = "all"
        super().__init__(configs, max_workers)

    def _distributed(self, n):
        super()._distributed(n)
        # this process should fetch every % n file

        for (which, src, url) in self.sources[n::self.max_workers]:
            try:
                ldr = getattr(self.configs, which)[src]['downloader']
                ldr.prepare_download(self, n, self.max_workers)
                ldr.download(url, disable_ui=self.disable_ui)
            except Exception as e:
                self.log(logging.ERROR, "Caught Exception:")
                self.log(logging.ERROR, e)
                raise
        return 1

    def maybe_add(self, which, cfg):
        downloader = cfg.get('downloader', None)
        if downloader is not None:
            urls = downloader.get_urls(self.download_type)
            if urls:
                for u in urls:
                    self.sources.append((which, cfg['name'], u))
        return False

    def process(self, layout, engine="ray", disable_ui=False, **args) -> bool:
        # hide unnecessary bars
        if len(self.sources) < self.max_workers:
            for n in range(len(self.sources), self.max_workers):
                bar = get_bar_from_layout(layout, n)
                bar[0].update(bar[1], visible=False)
        super().process(layout, engine=engine, disable_ui=disable_ui, **args)
