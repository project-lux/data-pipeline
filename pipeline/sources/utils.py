import requests
from pathlib import Path
from typing import List
import json
import subprocess
import sys


def get_download_url(source_name: str, base_url: str, data_key: str) -> str:
    """
    Fetches the download URL for a given source by querying its download page.

    Args:
        source_name (str): Name of the data source (e.g., 'ror', 'viaf').
        base_url (str): The API endpoint to fetch data from.
        data_key (str): The key path to extract the download link.

    Returns:
        str: The download URL, or exits with an error message.
    """
    response = fetch_webpage(base_url)

    try:
        page_data = json.loads(response)
        if not page_data:
            sys.exit(f"Error: Received empty JSON response from {source_name}")
    except json.JSONDecodeError:
        sys.exit(f"Error: Failed to parse JSON response for {source_name}")

    keys = data_key.split('.')
    download_url = page_data

    try:
        for key in keys:
            # Check if the key includes an index (e.g., hits[0])
            if '[' in key and ']' in key:
                key_name, index = key.split('[')
                index = int(index.rstrip(']'))
                download_url = download_url.get(key_name, [])[index]
            else:
                download_url = download_url.get(key, {})

        return download_url if isinstance(download_url, str) else None

    except (IndexError, KeyError, TypeError) as e:
        print(f"Error accessing JSON path '{data_key}': {e}")
        return None
    except Exception as e:
        sys.exit(f"Unexpected error fetching {source_name} data: {e}")


def fetch_webpage(url: str) -> str:
    """Fetch the webpage content from the given URL.
    
    Args:
        url (str): The URL of the webpage to fetch.
        
    Returns:
        str: The text content of the webpage if successful, None if the request fails.
        
    Raises:
        requests.RequestException: If there is an error making the HTTP request.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"Error fetching the URL '{url}': {e}")
        return None
    
def get_available_sources() -> List[str]:
    """
    Get all available sources from the configs directory.

    Returns:
        List[str]: A list of source names (config filenames with .json extension),
                  excluding the base_download config.

    This function scans the configs directory for JSON files and returns their names
    as available sources. Each JSON file represents a data source configuration.
    The base_download.json file is excluded as it contains common configuration.
    """
    """Get all available sources from the configs directory"""
    configs_dir = Path(__file__).parent.parent.parent / 'configs'
    return [
        f.stem for f in configs_dir.glob('*.json')
        if f.stem not in {'base_download', 'schema', 'validator'}  # exclude the base config, schema and validator files
    ]

def get_urls_from_script(source: str, download_dir: Path) -> List[str]:
    """Execute a script to get URLs for a source.

    Args:
        source (str): Name of the source to get URLs for
        download_dir (Path): Directory where downloads should be saved

    Returns:
        List[str]: List of URLs to download from the source. Empty list if script fails or doesn't exist.

    This function looks for and executes a Python script for the given source in the root scripts/sources
    directory. The script is expected to output either a JSON array of URLs or newline-separated URLs 
    to stdout.

    The script path is checked in:
    /scripts/sources/{source}.py
    """
    # Get the root project directory by going up from pipeline/sources
    script_path = Path(__file__).parent.parent.parent / 'scripts' / 'download' / 'sources' / f"{source}.py"

    if not script_path.exists():
        print(f"Script not found: {script_path}")
        return []
    
    try:
        result = subprocess.run([sys.executable, str(script_path)], 
                              capture_output=True, 
                              text=True, 
                              check=True)
        output = result.stdout.strip()
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