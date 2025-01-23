import sys
import os
import json
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from pipeline.sources.utils import fetch_webpage


def main():
    """
    Fetches the latest VIAF download URL. The resulting URL is formatted 
    and returned for use in shell scripts.

    **Function Flow:**
    1. Fetches the VIAF download page content.
    2. Parses the JSON response to extract the download link.
    3. Returns the formatted download URL as a JSON string.

    **Example usage in a shell script:**
        url=$(python scripts/download/viaf.py)
        curl -O "$url"

    **Returns:**
    - A list containing the VIAF RDF XML download URL.

    **Error Handling:**
    - Exits with an error message if fetching or parsing fails.
    """ 

    url = "https://viaf.org/api/download/files"

    try:
        api_response = fetch_webpage(url)
        page = json.loads(api_response)
        stub = "https://downloads.viaf.org/viaf/data/monthly/"

        #Extract the monthly RDF file key safely
        monthly = page.get('queryResult', {}).get('Monthly', {}).get('clusterRdfXml')
        if not monthly:
            raise ValueError("Monthly RDF file key not found in response.")
        
        to_download = stub + monthly
        urls = [to_download]
        print(json.dumps(urls))
        return urls

    except json.JSONDecodeError:
        sys.exit("Error: Failed to parse JSON response from VIAF API.")
    except ValueError as e:
        sys.exit(f"Error: {e}")
    except Exception as e:
        sys.exit(f"Unexpected error: {e}")


if __name__ == "__main__":
    main()