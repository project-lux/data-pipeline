import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from pipeline.sources.viaf.parser import parse_viaf
from pipeline.sources.utils import fetch_webpage


def main():
    """
    This script is combines the utility function `fetch_webpage` with the VIAF `parser` 
    to return the download URL to the download.sh script.

    **main():**
    - Uses the imported fetch_webpage function to fetch the webpage.
    - Calls the imported parse_viaf function to parse the webpage.
    - Prints the first valid `resource` URL matching the pattern or an appropriate
        error message if no valid URL is found.
    - Returns the valid url.

    ## Example usage:
    url=$(python scripts/download/viaf.py)
    curl -O "$url"
    """
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


