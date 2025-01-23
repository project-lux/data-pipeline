import sys
import os
import json
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from pipeline.sources.utils import fetch_webpage


def main():
    url = "https://zenodo.org/api/records/14429114/versions?size=5&sort=version&allversions=true"
    try:
        api_response = fetch_webpage(url)
        page = json.loads(api_response)
        data_list = page.get('hits', {}).get('hits', [])
        for h in data_list:
            files = h.get('files',[])
            for f in files:
                links = f.get('links',{})
                download_link = links['self']
                break
            break

        urls = [download_link]
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