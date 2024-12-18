import sys
import os
import json
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from pipeline.sources.general.ror.parser import parse_ror
from pipeline.sources.utils import fetch_webpage


def main():
    url = "https://zenodo.org/records/14429114"
    html_content = fetch_webpage(url)
    if html_content:
        file_url = parse_ror(html_content)
        if file_url:
            urls = [file_url]  # Wrap single URL in a list
            print(json.dumps(urls))
            return urls
        else:
            # Use stderr for error messages
            print("No valid file URL found.", file=sys.stderr)
            sys.exit(1)
    else:
        print("Failed to fetch webpage content.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()