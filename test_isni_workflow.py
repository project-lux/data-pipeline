#!/usr/bin/env python3

import os
import time
import requests
import ujson as json
from pathlib import Path
import logging
import tempfile
import shutil
from unittest.mock import Mock

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger("lux_pipeline")

# Mock the Managable base class
class MockManagable:
    def __init__(self, config):
        self.config = config

# Mock BaseDownloader with the actual interface
class BaseDownloader(MockManagable):
    """
    The purpose of the downloader is to provide urls to the DownloadManager. These will then be downloaded and placed into the required paths.
    """

    def __init__(self, config):
        super().__init__(config)
        self.dumps_dir = config["all_configs"].dumps_dir
        if "dumps_dir" in config:
            self.dumps_dir = os.path.join(self.dumps_dir, config["dumps_dir"])

    def fetch_webpage(self, url: str) -> str:
        """Fetch the webpage content from the given URL."""
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logger.error(f"Error fetching the URL '{url}': {e}")
            return None

    def get_value_from_json(self, base_url: str, data_key: str) -> str:
        """Fetches the download URL for a given source by querying its download page."""
        response = self.fetch_webpage(base_url)
        try:
            page_data = json.loads(response)
            if not page_data:
                raise ValueError(f"Error: Received empty JSON response from {base_url}")
        except json.JSONDecodeError:
            raise ValueError(f"Error: Failed to parse JSON response for {base_url}")

        if "/" in data_key and not "." in data_key:
            data_key = data_key.replace("/", ".")
        keys = data_key.split(".")
        download_url = page_data

        try:
            for key in keys:
                # Check if the key includes an index (e.g., hits[0])
                if "[" in key and "]" in key:
                    key_name, index = key.split("[")
                    index = int(index.rstrip("]"))
                    download_url = download_url.get(key_name, [])[index]
                else:
                    download_url = download_url.get(key, {})
            return download_url if isinstance(download_url, str) else None

        except (IndexError, KeyError, TypeError) as e:
            logger.error(f"Error accessing JSON path '{data_key}': {e}")
            return None
        except Exception as e:
            raise ValueError(f"Unexpected error fetching {base_url} data: {e}")

    def get_urls(self, type="all"):
        """Download files from specified source(s). Returns a list of urls and paths as a list of dictionaries."""
        urls = []
        if "dumpFilePath" in self.config and "remoteDumpFile" in self.config:
            urls.append({"url": self.config["remoteDumpFile"], "path": self.config["dumpFilePath"]})

        for rt, records in self.config.get("input_files", {}).items():
            if type in ["all", rt]:
                for record in records:
                    url = record.get("url", None)
                    if not url:
                        raise ValueError(f"URL not found for input file: {record}")
                    if p := record.get("path", None):
                        if not "/" in p:
                            # just the filename
                            p = os.path.join(self.dumps_dir, p)
                        urls.append({"url": url, "path": p})
                    elif self.dumps_dir:
                        # find the filename and dump it in the path
                        np = os.path.join(self.dumps_dir, url.rsplit("/", 1)[-1])
                        urls.append({"url": url, "path": np})
                    else:
                        raise ValueError(f"No download path for input file: {record}")
        return urls

    def process(self, download, disable_ui=False):
        url = download["url"]
        path = download["path"]

        response = self.manager.session.get(url, stream=True, verify=False)
        response.raise_for_status()
        # Create parent directories if they don't exist
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        total_size = int(response.headers.get("content-length", 0))
        filename = os.path.split(path)[-1]
        self.manager.update_progress_bar(total=total_size, description=f"{self.config['name']}/{filename}")
        with open(path, "wb") as f:
            for data in response.iter_content(chunk_size=1024):
                size = f.write(data)
                self.manager.update_progress_bar(advance=size)
        time.sleep(1)
        self.manager.update_progress_bar(completed=total_size)
        self.manager.log(logging.INFO, f"Finished downloading {filename}")
        time.sleep(2)
        return 1

# ISNI Downloader
class ISNIDownloader(BaseDownloader):
    """
    Person data URL: https://isni.org/isni/data-person/data.jsonld
    Organization data URL: https://isni.org/isni/data-organization/data.jsonld
    """
    
    def get_urls(self, type="all"):
        """Override to use the records from input_files configuration"""
        urls = []
        for rt, records in self.config.get("input_files", {}).items():
            if type in ["all", rt]:
                for record in records:
                    url = record.get("url", None)
                    if not url:
                        raise ValueError(f"URL not found for input file: {record}")
                    
                    # Use specified path or generate one in dumps_dir
                    if p := record.get("path", None):
                        if not "/" in p:
                            # just the filename
                            p = os.path.join(self.dumps_dir, p)
                        path = p
                    else:
                        # Generate filename based on content type
                        if "person" in url.lower():
                            filename = "isni-persons.jsonld"
                        elif "organization" in url.lower():
                            filename = "isni-organizations.jsonld"
                        else:
                            filename = url.rsplit("/", 1)[-1]
                        path = os.path.join(self.dumps_dir, filename)
                    
                    urls.append({"url": url, "path": path})
        return urls

    def process(self, download, disable_ui=False):
        """Override process to add post-processing for JSON-LD restructuring"""
        url = download["url"]
        path = download["path"]
        temp_path = path + ".tmp"

        # Download to temporary file first
        response = self.manager.session.get(url, stream=True, verify=False)
        response.raise_for_status()
        
        # Create parent directories if they don't exist
        Path(temp_path).parent.mkdir(parents=True, exist_ok=True)
        
        total_size = int(response.headers.get("content-length", 0))
        filename = os.path.split(path)[-1]
        self.manager.update_progress_bar(total=total_size, description=f"{self.config['name']}/{filename}")
        
        # Download to temporary file
        with open(temp_path, "wb") as f:
            for data in response.iter_content(chunk_size=1024):
                size = f.write(data)
                self.manager.update_progress_bar(advance=size)
        
        time.sleep(1)
        self.manager.update_progress_bar(completed=total_size)
        self.manager.log(logging.INFO, f"Downloaded {filename}, starting post-processing...")
        
        try:
            # Read the downloaded data
            with open(temp_path, 'r', encoding='utf-8') as f:
                data = f.read()
            
            # Post-process the data
            processed_data = self.restructure_jsonld(data)
            
            # Write the processed data to final location
            with open(path, 'w', encoding='utf-8') as f:
                f.write(processed_data)
            
            # Remove temp file
            os.remove(temp_path)
            
            self.manager.log(logging.INFO, f"Finished processing {filename}")
            
        except Exception as e:
            self.manager.log(logging.ERROR, f"Error post-processing {filename}: {e}")
            # Fall back to using the temp file as-is
            if os.path.exists(temp_path):
                os.rename(temp_path, path)
            raise
        
        time.sleep(2)
        return 1
    
    def restructure_jsonld(self, data):
        """Restructure ISNI JSON-LD from array of objects to proper JSON-LD format"""
        try:
            # Clean up control characters that break JSON parsing
            # Remove record separator (0x1e) and other control characters
            clean_data = data.replace('\x1e', '').replace('\x1f', '').replace('\x1d', '')
            
            # Parse the JSON data
            json_data = json.loads(clean_data)
            
            if not isinstance(json_data, list) or not json_data:
                logger.warning("ISNI data is not in expected array format")
                return data
            
            # Extract context from first record (they should all be the same)
            first_record = json_data[0]
            if "@context" not in first_record:
                logger.warning("No @context found in first record")
                return data
            
            context = first_record["@context"]
            
            # Collect all entities from all @graph arrays
            all_entities = []
            for record in json_data:
                if "@graph" in record and isinstance(record["@graph"], list):
                    all_entities.extend(record["@graph"])
            
            # Create the restructured JSON-LD document
            restructured = {
                "@id": "ISNI JSON-LD",
                "@context": context,
                "@graph": all_entities
            }
            
            logger.info(f"Restructured {len(json_data)} records into {len(all_entities)} entities")
            
            # Convert back to JSON string with nice formatting
            return json.dumps(restructured, indent=2, ensure_ascii=False)
            
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON data: {e}")
            return data
        except Exception as e:
            logger.error(f"Error restructuring ISNI data: {e}")
            return data


# Test with local file instead of downloading
def test_with_local_file():
    """Test the ISNI downloader workflow with your local ISNI file"""
    
    input_file = "/Users/wjm55/Downloads/ISNI_organizations_20241025T200000Z.jsonld"
    
    if not os.path.exists(input_file):
        print(f"File {input_file} not found. Cannot run test.")
        return False
    
    # Create temporary directory for output
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"Using temporary directory: {temp_dir}")
        
        # Mock configuration
        class MockAllConfigs:
            def __init__(self):
                self.dumps_dir = temp_dir
        
        config = {
            "name": "isni",
            "all_configs": MockAllConfigs(),
            "input_files": {
                "records": [
                    {
                        "url": f"file://{input_file}",  # Use local file
                        "path": "isni-organizations-processed.jsonld"
                    }
                ]
            }
        }
        
        # Mock manager
        class MockManager:
            def __init__(self):
                self.session = requests.Session()
            
            def update_progress_bar(self, **kwargs):
                if 'description' in kwargs:
                    print(f"Progress: {kwargs['description']}")
                elif 'advance' in kwargs:
                    print(".", end="", flush=True)
                elif 'completed' in kwargs:
                    print(" [DONE]")
            
            def log(self, level, message):
                print(f"LOG: {message}")
        
        # Create downloader
        downloader = ISNIDownloader(config)
        downloader.manager = MockManager()
        
        print("Testing ISNI downloader workflow...")
        
        try:
            # Test get_urls
            urls = downloader.get_urls()
            print(f"URLs to process: {urls}")
            
            # Since we're using a local file, we'll simulate the process differently
            for download in urls:
                print(f"\nProcessing: {download}")
                
                # Read the local file directly
                with open(input_file, 'r', encoding='utf-8') as f:
                    data = f.read()
                
                print(f"Read {len(data)} characters from input file")
                
                # Test the restructuring
                processed_data = downloader.restructure_jsonld(data)
                
                # Write to output
                output_path = download['path']
                Path(output_path).parent.mkdir(parents=True, exist_ok=True)
                
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write(processed_data)
                
                print(f"Wrote processed data to: {output_path}")
                
                # Verify the output is valid JSON
                try:
                    with open(output_path, 'r', encoding='utf-8') as f:
                        result = json.loads(f.read())
                    
                    print(f"✓ Output is valid JSON-LD")
                    print(f"✓ Contains {len(result.get('@graph', []))} entities")
                    print(f"✓ Has @context: {'@context' in result}")
                    print(f"✓ Output file size: {os.path.getsize(output_path)} bytes")
                    
                    # Copy to a permanent location for inspection
                    permanent_path = "/tmp/isni_workflow_test_output.jsonld"
                    shutil.copy2(output_path, permanent_path)
                    print(f"✓ Copied output to {permanent_path} for inspection")
                    
                    return True
                    
                except Exception as e:
                    print(f"✗ Error validating output: {e}")
                    return False
                    
        except Exception as e:
            print(f"✗ Error in workflow: {e}")
            import traceback
            traceback.print_exc()
            return False

if __name__ == "__main__":
    success = test_with_local_file()
    if success:
        print("\n🎉 ISNI downloader workflow test PASSED!")
    else:
        print("\n❌ ISNI downloader workflow test FAILED!") 