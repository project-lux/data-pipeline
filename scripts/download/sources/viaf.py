import sys
import os
import json
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from pipeline.sources.utils import fetch_webpage, get_download_url


def main():
    """
    Main function to fetch and return the download URL for VIAF data.

    This script retrieves the VIAF data download link from the API, constructs the full
    download URL, and returns it in JSON format.

    Steps:
    1. Define the base API endpoint and stub URL.
    2. Use `get_download_url` to extract the download link from the JSON response.
    3. Construct the full download URL.
    4. Print and return the download URL as a JSON-encoded list.

    Raises:
        SystemExit: If fetching the URL fails or returns invalid data.
    
    Returns:
        list: A list containing the full download URL.

    Example usage:
        $ python viaf.py
        Output: ["https://downloads.viaf.org/viaf/data/monthly/viaf-YYYYMMDD-clusters-rdf.xml.gz"]
    """
    base_url = "https://viaf.org/api/download/files"
    stub = "https://downloads.viaf.org/viaf/data/monthly/"
    data_key = "queryResult.Monthly.clusterRdfXml"

    try:
        monthly = get_download_url("viaf", base_url, data_key) 
        if not monthly:
            raise ValueError("Error: No download URL found in the response.")
           
        to_download = stub + monthly
        urls = [to_download]
        print(json.dumps(urls))
        return urls
        
    except ValueError as ve:
        sys.exit(f"Error: {ve}")

if __name__ == "__main__":
    main()