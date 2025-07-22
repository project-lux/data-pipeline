import os
import time
import ujson as json
from pathlib import Path
import logging
from pipeline.process.base.downloader import BaseDownloader

logger = logging.getLogger("lux_pipeline")

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