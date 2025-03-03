import ujson as json
from tqdm.auto import tqdm
from concurrent.futures import ProcessPoolExecutor

from pipeline.cli.entry import cfgs

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
        self.sources = []

    def _load(self, n):
        self.configs = cfgs
        for (which, src) in self.sources:
            ldr = getattr(self.configs, which)[src]['loader']
            ldr.prepare_load(n, self.max_workers)
            ldr.load()

    def maybe_add(self, which, cfg):
        if 'loader' in cfg:
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

    def load_all(self, verbose=None) -> bool:
        self.configs = None
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [
                executor.submit(self._load, n)
                for n in range(self.max_workers)
            ]
            results = [f.result() for f in futures]
        return all(results)
