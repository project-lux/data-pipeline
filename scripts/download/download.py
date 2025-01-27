#!/usr/bin/env python3
import argparse
import subprocess
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from typing import List
import requests
from tqdm.auto import tqdm
import threading
from dataclasses import dataclass
from queue import Queue
import time
import json
from importlib import import_module

from pipeline.sources.utils import get_available_sources

base_download_config = Path(__file__).parent.parent.parent / 'configs' / 'base_download.json'

with open(base_download_config, 'r') as f:
    base_download_config = json.load(f)

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
    """Manages concurrent downloads of multiple files with progress tracking.

    The DownloadManager class handles downloading multiple files simultaneously while
    displaying progress bars for each download. It tracks the status and progress of
    each download and provides a clean interface for initiating and monitoring downloads.

    Attributes:
        downloads (dict): Maps URLs to DownloadInfo objects tracking download state
        progress_bars (dict): Maps URLs to tqdm progress bars
        status_thread (Thread): Thread that updates progress displays
        should_stop (bool): Flag to stop the status display thread

    Example:
        urls = ["http://example.com/file1.gz", "http://example.com/file2.gz"]
        paths = {
            "http://example.com/file1.gz": "downloads/file1.gz",
            "http://example.com/file2.gz": "downloads/file2.gz"
        }
        manager = DownloadManager(urls, paths)
        manager.start_status_display()
        # Downloads will show progress bars
    """
    def __init__(self, urls: List[str], download_paths: dict):
        self.downloads = {
            url: DownloadInfo(
                url=url, 
                filename=str(download_paths[url])
            ) for url in urls
        }
        self.progress_bars = {}
        self.status_thread = None
        self.should_stop = False
        
    def start_status_display(self):
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
    
    def download_file(self, url: str) -> bool:
        download = self.downloads[url]
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
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

    def download_all(self, max_workers: int = 20) -> bool:
        self.start_status_display()
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(self.download_file, url)
                for url in self.downloads.keys()
            ]
            results = [f.result() for f in futures]
        
        self.should_stop = True
        self.status_thread.join()
        return all(results)

def ensure_download_dirs(base_config: dict, source_config: dict) -> Path:
    """
    Create and return the download directory for a source.

    Args:
        base_config (dict): Base configuration containing dump_file_dir setting
        source_config (dict): Source-specific configuration containing dump_file_subdir setting

    Returns:
        Path: Path object representing the download directory for the source

    This function creates the necessary directory structure for downloading source files:
    - Uses base_config['dump_file_dir'] as the root directory (defaults to 'data')
    - If source_config specifies a dump_file_subdir, creates that subdirectory
    - Creates any missing parent directories
    - Returns the full path to use for downloads
    """
    """Create and return the download directory for a source"""
    # Get base download directory
    base_dir = Path(base_config.get('dump_file_dir', 'data'))
    
    # Get source subdirectory
    subdir = source_config.get('dump_file_subdir')
    if not subdir:
        return base_dir
    
    # Create full path
    full_path = base_dir / subdir
    full_path.mkdir(parents=True, exist_ok=True)
    
    return full_path

def main():
    """Main entry point for the download script.

    This script downloads data files from various sources specified via command line arguments.
    It supports downloading from multiple sources at once using comma-separated values or 'all'
    to download from all available sources.

    For each source:
    1. Checks for and loads source-specific config file from configs/{source}.json
    2. Creates download directory based on base and source configs
    3. Gets download URLs either from:
       - Associated Python script in sources/{source}.py
       - remote_dump_files list in source config
    4. Downloads files from all collected URLs

    Command line arguments:
        --source: Source(s) to download from. Can be:
                 - Single source name (e.g. "viaf")
                 - Comma-separated list (e.g. "viaf,dnb") 
                 - "all" to download from all available sources

    Returns:
        None. Exits with status 1 on error.
    """
    available_sources = get_available_sources()
    
    parser = argparse.ArgumentParser(description='Download data files from various sources')
    parser.add_argument(
        '--source', 
        required=True,
        help='Source(s) to download from. Use comma-separated values or "all"'
    )
    
    args = parser.parse_args()
    
    # Handle source selection
    if args.source.lower() == 'all':
        sources_to_process = available_sources
    else:
        sources_to_process = args.source.split(',')
        # Validate sources
        invalid_sources = [s for s in sources_to_process if s not in available_sources]
        if invalid_sources:
            print(f"Error: Invalid source(s): {', '.join(invalid_sources)}")
            print(f"Available sources: {', '.join(available_sources)}")
            sys.exit(1)
    
    all_urls = []
    download_paths = {}  # Map of URLs to their download paths
    
    # Collect URLs from all specified sources
    for source in sources_to_process:
        try:
            # Load the source's config file
            config_path = Path(__file__).parent.parent.parent / 'configs' / f'{source}.json'
            source_config = {}
            if config_path.exists():
                with open(config_path, 'r') as f:
                    source_config = json.load(f)
            
            # Create download directory
            download_dir = ensure_download_dirs(base_download_config, source_config)
            
            urls = []
            
            # Try to get URLs from associated script first
            if not source_config.get("remote_dump_files"):
                download_function_path = "pipeline." + source_config.get("downloaderFunction")
                print(f"Download function path: {download_function_path}")
                try:
                    # Split into module path and function name
                    module_path, function_name = download_function_path.rsplit('.', 1)
                    
                    # Import module and get function in one clean step
                    download_function = getattr(import_module(module_path), function_name)
                    
                    if (script_urls := download_function()):
                        urls.extend(script_urls)
                except Exception as e:
                    print(f"Warning: Failed to execute download function for {source}: {e}")
            
            # If config exists, add URLs from it
            if source_config:
                # Add URLs from remote_dump_files
                if 'remote_dump_files' in source_config:
                    urls.extend(source_config['remote_dump_files'])
                
                # Add URL from remoteDumpFile if it exists and isn't already included
                # We're going to remove this from the JSON
                # if 'remoteDumpFile' in source_config and source_config['remoteDumpFile'] not in urls:
                #     urls.append(source_config['remoteDumpFile'])

            # Map URLs to their download paths
            for url in urls:
                filename = url.split('/')[-1]
                download_paths[url] = download_dir / filename
            
            if urls:
                all_urls.extend(urls)
            else:
                print(f"Warning: No URLs found in config or script for {source}")
        except Exception as e:
            print(f"Error: Failed to fetch URLs for '{source}': {e}")
            sys.exit(1)
    if not all_urls:
        print("No URLs to download from any source")
        sys.exit(1)
    
    # Modify DownloadManager to use the correct paths
    manager = DownloadManager(all_urls, download_paths)
    if not manager.download_all():
        sys.exit(1)

if __name__ == '__main__':
    main()