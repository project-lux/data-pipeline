
from pipeline.sources.utils import fetch_webpage, get_download_url

def get_viaf_urls():
    base_url = "https://viaf.org/api/download/files"
    stub = "https://downloads.viaf.org/viaf/data/monthly/"
    data_key = "queryResult.Monthly.clusterRdfXml"

    monthly = get_download_url("viaf", base_url, data_key) 
    if not monthly:
        raise ValueError("Error: No download URL found in the response.")
        
    to_download = stub + monthly
    urls = [to_download]
    return urls

