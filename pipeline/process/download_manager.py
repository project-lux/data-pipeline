from typing import List
from dataclasses import dataclass
from pathlib import Path
import json
import threading
import requests
from tqdm.auto import tqdm
from concurrent.futures import ThreadPoolExecutor
from importlib import import_module
import time

@dataclass
class DownloadInfo:
    """Information about a file download.

    Attributes:
        url (str): The URL to download from
        filename (str): Local path where file will be saved
        total_size (int): Total size of file in bytes, 0 if unknown
        downloaded_size (int): Number of bytes downloaded so far
        status (str): Current status of download - one of:
            "pending": Download not yet started
            "downloading": Download in progress 
            "completed": Download finished successfully
            "error": Download failed
    """
    url: str
    filename: str
    total_size: int = 0
    downloaded_size: int = 0
    status: str = "pending"  # pending, downloading, completed, error

class DownloadManager:
    """
    DownloadManager is responsible for downloading files from the internet.
    Each source configuration file contains a Downloader class. The Downloader class is responsible for preparing a list of urls that need to be downloaded. To see an example, see the BaseDownloader class.
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
        self.downloads = {}

    def _start_status_display(self):
        """Start thread to update progress bars"""
        def update_progress():
            while not self.should_stop:
                self._update_progress_bars()
                time.sleep(0.1)
        self.status_thread = threading.Thread(target=update_progress)
        self.status_thread.start()
    
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
    
    def _download_file(self, url: str) -> bool:
        download = self.downloads[url]
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()

            # Create parent directories if they don't exist
            Path(download.filename).parent.mkdir(parents=True, exist_ok=True)

            download.total_size = int(response.headers.get('content-length', 0))
            download.status = "downloading"
            
            with open(download.filename, 'wb') as f:
                for data in response.iter_content(chunk_size=1024):
                    size = f.write(data)
                    download.downloaded_size += size
            
            download.status = "completed"
            return True
            
        except Exception as e:
            download.status = f"error: {str(e)}"
            return False

    def prepare_download(self, cfg) -> bool:
        downloader = cfg.get('downloader', None)
        okay = False
        if downloader is not None:
            urls = downloader.get_urls()
            for pair in urls:
                url = pair["url"]
                path = pair["path"]
                okay = True
                self.downloads[url] = DownloadInfo(
                    url=url,
                    filename=str(path)
                )
        return okay

    def prepare_single(self, name) -> bool:
        if name in self.configs.internal:
            self.prepare_download(self.configs.internal[name])
        elif name in self.configs.external:
            self.prepare_download(self.configs.external[name])
        else:
            raise ValueError(f"Unknown source: {name}")

    def prepare_all(self) -> bool:
        for cfg in self.configs.external.values():
            self.preparse_download(cfg)
        for cfg in self.configs.internal.values():
            self.prepare_download(cfg)

    def download_all(self) -> bool:
        self.start_status_display()
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [
                executor.submit(self._download_file, url)
                for url in self.downloads.keys()
            ]
            results = [f.result() for f in futures]
        self.should_stop = True
        self.status_thread.join()
        return all(results)

