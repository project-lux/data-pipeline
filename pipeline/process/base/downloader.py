import os
import requests
import ujson as json


class BaseDownloader:
    """
    The purpose of the downloader is to provide urls to the DownloadManager. These will then be downloaded and placed into the required paths.
    """
    def __init__(self, config):
        self.config = config
        self.dumps_dir = config['all_configs'].dumps_dir
        if 'dumps_dir' in config:
            self.dumps_dir = os.path.join(self.dumps_dir, config['dumps_dir'])
        # self.urls = self.get_urls()

    def fetch_webpage(self, url: str) -> str:
        """Fetch the webpage content from the given URL.
        Args:
            url (str): The URL of the webpage to fetch.

        Returns:
            str: The text content of the webpage if successful, None if the request fails.

        Raises:
            requests.RequestException: If there is an error making the HTTP request.
        """
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            print(f"Error fetching the URL '{url}': {e}")
            return None

    def get_value_from_json(self, base_url: str, data_key: str) -> str:
        """
        Fetches the download URL for a given source by querying its download page.

        Args:
            base_url (str): The API endpoint to fetch data from.
            data_key (str): The key path to extract the download link.

        Returns:
            str: The download URL, or exits with an error message.
        """

        response = self.fetch_webpage(base_url)
        try:
            page_data = json.loads(response)
            if not page_data:
                raise ValueError(f"Error: Received empty JSON response from {source_name}")
        except json.JSONDecodeError:
            raise ValueError(f"Error: Failed to parse JSON response for {source_name}")

        if '/' in data_key and not '.' in data_key:
            data_key = data_key.replace('/', '.')
        keys = data_key.split('.')
        download_url = page_data

        try:
            for key in keys:
                # Check if the key includes an index (e.g., hits[0])
                if '[' in key and ']' in key:
                    key_name, index = key.split('[')
                    index = int(index.rstrip(']'))
                    download_url = download_url.get(key_name, [])[index]
                else:
                    download_url = download_url.get(key, {})
            return download_url if isinstance(download_url, str) else None

        except (IndexError, KeyError, TypeError) as e:
            print(f"Error accessing JSON path '{data_key}': {e}")
            return None
        except Exception as e:
            raise ValueError(f"Unexpected error fetching {source_name} data: {e}")

    def get_value_from_html(self, base_url:str, xpath: str) -> str:
        # FIXME: Implement a basic "splash page" reader from HTML
        pass

    def get_value_from_xml(self, base_url:str, xpath: str) -> str:
        # FIXME: Implement a basic "splash page" reader from XML
        pass


    def get_urls(self):
        """
        Download files from specified source(s). Returns a list of urls and paths as a list of dictionaries.
        """
        # {records: [{url, path}, ...], other: [{url, path}, ...], ...}

        urls = []
        if 'dumpFilePath' in self.config and 'remoteDumpFile' in self.config:
            urls.append({"url": self.config['remoteDumpFile'], 'path': self.config['dumpFilePath']})

        for records in self.config.get('input_files', {}).values():
            for record in records:
                url = record.get("url", None)
                if not url:
                    raise ValueError(f"URL not found for input file: {record}")
                if (p := record.get("path", None)):
                    if not '/' in p:
                        # just the filename
                        p = os.path.join(self.dumps_dir, p)
                    urls.append({"url": url, "path": p})
                elif self.dumps_dir:
                    # find the filename and dump it in the path
                    np = os.path.join(self.dumps_dir, url.rsplit('/', 1)[-1])
                    urls.append({"url": url, "path": np})
                else:
                    raise ValueError(f"No download path for input file: {record}")
        return urls
