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

@dataclass
class DownloadInfo:
    url: str
    filename: str
    total_size: int = 0
    downloaded_size: int = 0
    status: str = "pending"  # pending, downloading, completed, error

class DownloadManager:
    def __init__(self, urls: List[str]):
        self.downloads = {url: DownloadInfo(url=url, filename=url.split('/')[-1]) 
                         for url in urls}
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

def get_available_sources() -> List[str]:
    """Get all available source modules from the sources directory"""
    sources_dir = Path(__file__).parent / 'sources'
    return [
        f.stem for f in sources_dir.glob('*.py')
        if f.stem != '__init__'
    ]

def main():
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
    # Collect URLs from all specified sources
    for source in sources_to_process:
        try:
            module_path = f'scripts.download.sources.{source}'
            source_module = __import__(module_path, fromlist=['main'])
            urls = source_module.main()
            if urls:
                all_urls.extend(urls)
            else:
                print(f"Warning: No URLs returned from {source}")
        except Exception as e:
            print(f"Error: Failed to fetch URLs for '{source}': {e}")
            sys.exit(1)
    
    if not all_urls:
        print("No URLs to download from any source")
        sys.exit(1)
    
    # Use download manager for all collected URLs
    manager = DownloadManager(all_urls)
    if not manager.download_all():
        sys.exit(1)

if __name__ == '__main__':
    main()