import sys
import os
import json
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from pipeline.sources.utils import fetch_webpage, get_download_url


def main():
    """
    Main function to fetch and return the download URL for ROR data.

    This script retrieves the ROR data download link from the Zenodo API, constructs the full
    download URL, and returns it in JSON format.

    Steps:
    1. Define the base API endpoint.
    2. Use `get_download_url` to extract the download link from the JSON response.
    3. Print and return the download URL as a JSON-encoded list.

    Raises:
        SystemExit: If fetching the URL fails or returns invalid data.
    
    Returns:
        list: A list containing the full download URL.

    Example usage:
        $ python ror.py
        Output: ["https://zenodo.org/api/records/14429114/files/v1.58-2024-12-11-ror-data.zip/content"]
    """
    base_url = "https://zenodo.org/api/records/14429114/versions?size=5&sort=version&allversions=true"
    data_key = "hits.hits[0].files[0].links.self"

    try:
        download_url = get_download_url("ror", base_url, data_key)
        if not download_url:
            raise ValueError("Error: No download URL found in the response.")
        urls = [download_url]
        print(json.dumps(urls))
        return urls
    except ValueError as ve:
        sys.exit(f"Error: {ve}")

if __name__ == "__main__":
    main()