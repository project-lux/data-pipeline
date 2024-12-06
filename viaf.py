"""
This script is designed to fetch the VIAF data page, extract the first `resource` URL
that matches the specific pattern:
`https://viaf.org/viaf/data/viaf-NNNNNNNN-clusters.xml.gz`, where `NNNNNNNN` represents
a variable numeric identifier, and print the URL.

The script consists of three main functions:

1. **fetch_webpage(url):**
   - Fetches the HTML content of the specified URL using the `requests` library.
   - Includes error handling to manage network-related issues.

2. **parse_file_url(html_content):**
   - Parses the fetched HTML content using `BeautifulSoup`.
   - Searches within the `<article class="data-files">` section for `<dl>` elements with
     a `resource` attribute.
   - Validates that the `resource` attribute matches the required URL pattern using
     regular expressions.
   - Returns the first matching URL or `None` if no match is found.

3. **main():**
   - Combines the above two functions to fetch and parse the webpage.
   - Prints the first valid `resource` URL matching the pattern or an appropriate
     error message if no valid URL is found.

"""

import re
import requests
from bs4 import BeautifulSoup


def fetch_webpage(url: str) -> str:
    """Fetch the webpage content from the given URL."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"Error fetching the URL '{url}': {e}")
        return None


def parse_file_url(html_content: str) -> str:
    """Parse the first `resource` URL matching the specific pattern."""
    soup = BeautifulSoup(html_content, 'html.parser')
    files_available_section = soup.find('article', class_='data-files')
    if files_available_section:
        # Regex to match the desired pattern
        pattern = r"https://viaf\.org/viaf/data/viaf-\d+-clusters\.xml\.gz"
        # Find all <dl> elements with a 'resource' attribute
        dl_elements = files_available_section.find_all('dl', resource=True)
        for dl in dl_elements:
            resource_url = dl.get('resource')
            if resource_url and re.match(pattern, resource_url):
                return resource_url
    return None


def main():
    """Main function to fetch and parse file URL from VIAF data page."""
    url = "https://viaf.org/viaf/data/"
    html_content = fetch_webpage(url)
    if html_content:
        file_url = parse_file_url(html_content)
        if file_url:
        	#This is the one we want
            print(f"First file URL: {file_url}")
        else:
            print("No valid file URL found.")
    else:
        print("Failed to fetch webpage content.")


if __name__ == "__main__":
    main()






