
from ._task_ui_manager import TaskUiManager
from pipeline.cli._rich import get_bar_from_layout

class DownloadManager(TaskUiManager):
    """
    DownloadManager is responsible for downloading files from the internet.
    Each source configuration file contains a Downloader class. The Downloader class is responsible for preparing a list of urls that need to be downloaded. To see an example, see the BaseDownloader class.
    """

    def _distributed(self, bars, messages, n):
        super()._distributed(bars, messages, n)
        # this process should fetch ever %n file

        for (which, src, url) in self.sources[n::self.max_workers]:
            try:
                ldr = getattr(self.configs, which)[src]['downloader']
                ldr.prepare_download(self, n, self.max_workers)
                ldr.download(url, disable_ui=self.disable_ui, verbose=self.verbose)
            except Exception as e:
                self.log(logging.ERROR, "[red]Caught Exception:")
                self.log(logging.ERROR, e)

    def maybe_add(self, which, cfg):
        downloader = cfg.get('downloader', None)
        if downloader is not None:
            urls = downloader.get_urls()
            if urls:
                for u in urls:
                    self.sources.append((which, cfg['name'], u))
        return False

    def process(self, layout, disable_ui=False, verbose=None, **args) -> bool:
        # hide unnecessary bars
        if len(self.sources) < self.max_workers:
            for n in range(len(self.sources), self.max_workers):
                bar = get_bar_from_layout(layout, n)
                bar[0].update(bar[1], visible=False)
        super().process(layout, disable_ui=disable_ui, verbose=verbose, **args)
