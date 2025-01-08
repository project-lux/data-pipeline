import sys
import os
import json
import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from pipeline.sources.general.orcid.parser import get_yearly_url, get_download_link
from pipeline.sources.utils import fetch_webpage

"""
The main function coordinates the process of locating and extracting the download 
link for the ORCID Public Data File for the current year.

Steps:
    1. Determine the current year dynamically.
    2. Fetch and parse the base webpage to find the yearly URL for the data file.
    3. Fetch and parse the yearly webpage to extract the direct download link.
    4. Output the download link as a JSON array.

Error Handling:
    - Prints errors to `stderr` and exits with status code 1 if any step fails 
      (e.g., webpage fetch, URL parsing, or download link extraction).

Returns:
    - JSON array containing the download link if successful.
"""

def main():
    # current_year = datetime.datetime.now().year
    current_year = 2024

    base_url = "https://info.orcid.org/documentation/integration-guide/working-with-bulk-data/"
    html_content = fetch_webpage(base_url)
    if not html_content:
        print("Failed to fetch the base webpage content.", file=sys.stderr)
        sys.exit(1)

    yearly_url = get_yearly_url(html_content, current_year)
    if not yearly_url:
        print(f"No valid yearly URL found for {current_year}.", file=sys.stderr)
        sys.exit(1)

    yearly_html = fetch_webpage(yearly_url)
    if not yearly_html:
        print(f"Failed to fetch the yearly webpage at {yearly_url}.", file=sys.stderr)
        sys.exit(1)

    download_link = get_download_link(yearly_html, current_year)
    if not download_link:
        print(f"No valid download link found for {current_year}.", file=sys.stderr)
        sys.exit(1)

    urls = [download_link]
    print(json.dumps(urls))
    return urls


if __name__ == "__main__":
    main()