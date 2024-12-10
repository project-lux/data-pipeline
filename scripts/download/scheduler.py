import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))


from pathlib import Path
from typing import List, Optional
import json
import argparse
from pipeline.sources.checker import verify_files
from pipeline.sources.utils import get_available_sources, get_urls_from_script
from typing import List

def check_source_updates(source_name: str, download_dir: Path) -> List[str]:
    """Check if source files need to be updated by comparing local and remote files.
    
    Args:
        source_name: Name of the source to check
        download_dir: Base directory for downloads
        
    Returns:
        List of URLs that need to be downloaded/updated
    """
    # Load source config
    config_path = Path('configs') / f'{source_name}.json'
    if not config_path.exists():
        print(f"No config found for source: {source_name}")
        return []
        
    with open(config_path) as f:
        config = json.load(f)
    
    # Get source directory
    source_dir = download_dir
    if subdir := config.get('dump_file_subdir'):
        source_dir = download_dir / subdir
        
    urls_to_update = []
    # Special handling for VIAF - use script to get URLs
    if source_name.lower() == 'viaf':
        remote_urls = get_urls_from_script(source_name, source_dir)
    else:
        remote_urls = config.get('remote_dump_files', [])
    
    for url in remote_urls:
        filename = url.split('/')[-1]
        local_path = source_dir / filename
        
        # Add special handling for VIAF
        if source_name.lower() == 'viaf':
            expected_filename = 'viaf-clusters-marc21.xml.gz'
            if expected_filename in filename:
                print(f"Warning: VIAF filename '{filename}' doesn't match expected '{expected_filename}'")
                continue
        
        if not local_path.exists() or verify_files(str(local_path), url) is False:
            urls_to_update.append(url)
            
    return urls_to_update

if __name__ == "__main__":
    available_sources = get_available_sources()
    
    parser = argparse.ArgumentParser(description='Check source files for updates')
    parser.add_argument(
        '--source', 
        required=True,
        help=f'Source(s) to check. Use comma-separated values or "all". Available: {", ".join(available_sources)}'
    )
    parser.add_argument('--download-dir', type=Path, default=Path('data'),
                       help='Base directory for downloads')
    
    args = parser.parse_args()
    
    # Handle source selection
    if args.source.lower() == 'all':
        sources_to_check = available_sources
    else:
        sources_to_check = [s.strip() for s in args.source.split(',')]
        invalid_sources = [s for s in sources_to_check if s not in available_sources]
        if invalid_sources:
            print(f"Error: Invalid source(s): {', '.join(invalid_sources)}")
            print(f"Available sources: {', '.join(available_sources)}")
            sys.exit(1)
    
    all_urls = []
    for source in sources_to_check:
        urls = check_source_updates(source, args.download_dir)
        if urls:
            print(f"\nUpdates needed for {source} ({len(urls)} files):")
            for url in urls:
                print(f"  {url}")
            all_urls.extend(urls)
        else:
            print(f"\n{source}: All files are up to date")
    
    if all_urls:
        print(f"\nTotal updates needed: {len(all_urls)} files")
    else:
        print("\nAll files are up to date")