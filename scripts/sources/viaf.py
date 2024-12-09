"""
This script is designed to fetch the VIAF data page, extract the first `resource` URL
that matches the specific pattern:
`https://viaf.org/viaf/data/viaf-NNNNNNNN-clusters.xml.gz`, where `NNNNNNNN` represents
a variable numeric identifier, and print the URL.

The script consists of two main functions:

1. **parse_viaf(html_content):**
   - Parses the fetched HTML content using `BeautifulSoup`.
   - Searches within the `<article class="data-files">` section for `<dl>` elements with
     a `resource` attribute.
   - Validates that the `resource` attribute matches the required URL pattern using
     regular expressions.
   - Returns the first matching URL or `None` if no match is found.

2. **main():**
   - Uses the imported fetch_webpage function to fetch the webpage.
   - Then calls the parse_viaf function  to parse the webpage.
   - Prints the first valid `resource` URL matching the pattern or an appropriate
     error message if no valid URL is found.
   - Returns the valid url.

## Example usage:
url=$(python scripts/download/viaf.py)
curl -O "$url"
"""

import re
from bs4 import BeautifulSoup
from utils import fetch_webpage
import sys

def parse_viaf(html_content: str) -> str:
    """Parse the first `resource` URL matching the specific pattern.
    
    Args:
        html_content (str): The HTML content of the VIAF data page to parse.
        
    Returns:
        str: The first resource URL matching the pattern 'https://viaf.org/viaf/data/viaf-NNNNNNNN-clusters.xml.gz',
             or None if no matching URL is found.
    """
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
        file_url = parse_viaf(html_content)
        if file_url:
            print(file_url)
            return file_url
        else:
            # Use stderr for error messages
            print("No valid file URL found.", file=sys.stderr)
            sys.exit(1)
    else:
        print("Failed to fetch webpage content.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()


