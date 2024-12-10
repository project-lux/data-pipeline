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


base_download_config = Path(__file__).parent.parent.parent / 'configs' / 'base_download.json'

with open(base_download_config, 'r') as f:
    base_download_config = json.load(f)

@dataclass
class DownloadInfo:
    url: str
    filename: str
    total_size: int = 0
    downloaded_size: int = 0
    status: str = "pending"  # pending, downloading, completed, error

class DownloadManager:
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

def get_available_sources() -> List[str]:
    """Get all available sources from the configs directory"""
    configs_dir = Path(__file__).parent.parent.parent / 'configs'
    return [
        f.stem for f in configs_dir.glob('*.json')
        if f.stem != 'base_download'  # exclude the base config
    ]

def ensure_download_dirs(base_config: dict, source_config: dict) -> Path:
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

def get_urls_from_script(source: str, download_dir: Path) -> List[str]:
    """Execute a script to get URLs for a source"""
    # First check in the sources directory
    sources_script_path = Path(__file__).parent / 'sources' / f"{source}.py"
    # Then check in the current directory (legacy support)
    current_script_path = Path(__file__).parent / f"{source}.py"
    
    script_path = sources_script_path if sources_script_path.exists() else current_script_path
    if not script_path.exists():
        return []
    
    try:
        result = subprocess.run([sys.executable, str(script_path)], 
                              capture_output=True, 
                              text=True, 
                              check=True)
        try:
            # Try to parse as JSON first
            urls = json.loads(result.stdout.strip())
            if isinstance(urls, list):
                return [url for url in urls if url]
        except json.JSONDecodeError:
            # Fall back to newline-separated format
            urls = result.stdout.strip().split('\n')
            return [url for url in urls if url]  # Filter out empty lines
    except subprocess.CalledProcessError as e:
        print(f"Warning: Failed to run {script_path.name}: {e}")
        return []

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
            script_urls = get_urls_from_script(source, download_dir)
            if script_urls:
                urls.extend(script_urls)
            
            # If config exists, add URLs from it
            if source_config:
                # Add URLs from remote_dump_files
                if 'remote_dump_files' in source_config:
                    urls.extend(source_config['remote_dump_files'])
                
                # Add URL from remoteDumpFile if it exists and isn't already included
                if 'remoteDumpFile' in source_config and source_config['remoteDumpFile'] not in urls:
                    urls.append(source_config['remoteDumpFile'])

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