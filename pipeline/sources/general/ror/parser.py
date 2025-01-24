from bs4 import BeautifulSoup
import json
from pipeline.sources.utils import fetch_webpage, get_download_url



def parse_ror(html_content: str) -> str:
    """Parses the provided HTML content, extracts JSON data from a specific 
        <div> element with a "data-record" attribute, and searches for the 
        latest version URL of a .zip file in the "files" entries.
    
    Args:
        html_content (str): The HTML content of a webpage as a string.
        
    Returns:
        str: The URL of the latest version of the .zip file if found, 
            otherwise returns None.
    """

    soup = BeautifulSoup(html_content, 'html.parser')
    data_div = soup.find("div", {"data-record": True})
    if data_div:
        data_record_json = json.loads(data_div["data-record"])
        files_entries = data_record_json.get("files", {}).get("entries", {})
        latest_version_url = None
        
        for file_info in files_entries.values():
            content_url = file_info.get("links", {}).get("content")
            if content_url and content_url.endswith(".zip/content"):
                latest_version_url = content_url
                break

        if latest_version_url:
            return latest_version_url
    return None

def get_ror_urls():
    base_url = "https://zenodo.org/api/records/14429114/versions?size=5&sort=version&allversions=true"
    data_key = "hits.hits[0].files[0].links.self"

    download_url = get_download_url("ror", base_url, data_key)
    if not download_url:
        raise ValueError("Error: No download URL found in the response.")
    urls = [download_url]
    return urls

