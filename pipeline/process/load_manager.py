import ujson as json
from tqdm.auto import tqdm
from concurrent.futures import ProcessPoolExecutor

class LoadManager:
    """
    DownloadManager is responsible for loading files from the disk into a cache.
    Each source configuration file contains a Loader class. The Downloader class is responsible for preparing a list of urls that need to be downloaded. To see an example, see the BaseDownloader class.
    """
    def __init__(self,
                 configs,
                 max_workers: int = 0
                 ):
        self.configs = configs
        self.verbose = False
        if max_workers > 0:
            self.max_workers = max_workers
        else:
            self.max_workers = configs.max_workers
        self.progress_bars = {}
        self.status_thread = None
        self.should_stop = False

    def _update_progress_bars(self):
        """Update all progress bars"""
        for url, download in self.downloads.items():
            if url not in self.progress_bars and download.total_size > 0:
                self.progress_bars[url] = tqdm(
                    total=download.total_size,
                    initial=download.downloaded_size,
                    unit='iB',
                    unit_scale=True,
                    desc=f"{download.filename} ({download.status})",
                    position=len(self.progress_bars),
                    leave=True
                )
            elif url in self.progress_bars:
                self.progress_bars[url].n = download.downloaded_size
                self.progress_bars[url].set_description(
                    f"{download.filename} ({download.status})"
                )
                self.progress_bars[url].refresh()

    def _load_file(self, url: str) -> bool:


    def prepare_load(self, cfg) -> bool:
        loader = cfg.get('loader', None)
        okay = False
        if loader is not None:
            pass
        return okay

    def prepare_single(self, name) -> bool:
        if name in self.configs.internal:
            self.prepare_load(self.configs.internal[name])
        elif name in self.configs.external:
            self.prepare_load(self.configs.external[name])
        else:
            raise ValueError(f"Unknown source: {name}")

    def prepare_all(self) -> bool:
        for cfg in self.configs.external.values():
            self.preparse_load(cfg)
        for cfg in self.configs.internal.values():
            self.prepare_load(cfg)

    def load_all(self, verbose=None) -> bool:

        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [
                executor.submit(self._load_file, f)
                for f in self.files.keys()
            ]
            results = [f.result() for f in futures]
        return all(results)
