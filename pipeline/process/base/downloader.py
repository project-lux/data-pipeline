from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from typing import List
import requests
from tqdm.auto import tqdm
from dataclasses import dataclass
from queue import Queue
import time
import json
from importlib import import_module

from lux_pipeline.sources.utils import get_available_sources

class BaseDownloader:
    """
    The purpose of the downloader is to provide urls to the DownloadManager. These will then be downloaded and placed into the required paths.
    """
    def __init__(self, config):
        self.config = config
        self.input_files = config["input_files"]
        self.urls = self.get_urls()

    def get_urls(self):
        """
        Download files from specified source(s). Returns a list of urls and paths as a list of dictionaries.
        """

        # {records: [{url, path}, ...], other: [{url, path}, ...], ...}
        # Need all of them

        # FIXME: allow a directory in dumpFilePath and then auto-assign a filename when downloading

        urls = []
        if 'dumpFilePath' in self.config and 'remoteDumpFile' in self.config:
            urls.append({"url": config['remoteDumpFile'], 'path': config['dumpFilePath']})

        for records in self.input_files.values():
            for record in records:
                url = record.get("url", None)
                if not url:
                    raise ValueError(f"URL not found for input file: {record}")
                if record.get("path", None):
                    urls.append({"url": url, "path": record.get("path", None)})
                else:
                    raise ValueError(f"Path not found for input file: {record}")

        return urls
