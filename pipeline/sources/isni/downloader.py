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
            # Post-process the data using streaming approach
            self.restructure_jsonld_streaming(temp_path, path)
            
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
    
    def restructure_jsonld_streaming(self, input_path, output_path):
        """Restructure ISNI JSON-LD using streaming to handle large files"""
        logger.info("Starting streaming JSON-LD restructuring...")
        
        # First pass: extract context and count records
        context = None
        record_count = 0
        
        with open(input_path, 'r', encoding='utf-8') as f:
            # Clean control characters and parse incrementally
            content = f.read(100000)  # Read first 100KB to get context
            clean_start = content.replace('\x1e', '').replace('\x1f', '').replace('\x1d', '')
            
            # Find first complete JSON object to extract context
            brace_count = 0
            in_string = False
            escape_next = False
            start_pos = clean_start.find('[')
            
            if start_pos == -1:
                raise ValueError("Invalid JSON-LD structure: no opening bracket found")
            
            pos = start_pos + 1
            while pos < len(clean_start):
                char = clean_start[pos]
                
                if escape_next:
                    escape_next = False
                    pos += 1
                    continue
                
                if char == '\\' and in_string:
                    escape_next = True
                    pos += 1
                    continue
                
                if char == '"':
                    in_string = not in_string
                    pos += 1
                    continue
                
                if not in_string:
                    if char == '{':
                        brace_count += 1
                        if brace_count == 1:
                            record_start = pos
                    elif char == '}':
                        brace_count -= 1
                        if brace_count == 0:
                            # Found complete first record
                            record_json = clean_start[record_start:pos+1]
                            try:
                                first_record = json.loads(record_json)
                                context = first_record.get("@context")
                                if context:
                                    logger.info("Extracted context from first record")
                                    break
                            except:
                                pass
                pos += 1
        
        if not context:
            raise ValueError("Could not extract @context from JSON-LD file")
        
        # Second pass: stream process all records and extract entities
        logger.info("Starting entity extraction...")
        
        with open(input_path, 'r', encoding='utf-8') as input_file:
            with open(output_path, 'w', encoding='utf-8') as output_file:
                # Write JSON-LD header
                output_file.write('{\n')
                output_file.write('  "@id": "ISNI JSON-LD",\n')
                output_file.write(f'  "@context": {json.dumps(context, ensure_ascii=False)},\n')
                output_file.write('  "@graph": [\n')
                
                # Process file in chunks
                buffer = ""
                entity_count = 0
                first_entity = True
                chunk_size = 1024 * 1024  # 1MB chunks
                
                while True:
                    chunk = input_file.read(chunk_size)
                    if not chunk:
                        break
                    
                    # Clean control characters
                    clean_chunk = chunk.replace('\x1e', '').replace('\x1f', '').replace('\x1d', '')
                    buffer += clean_chunk
                    
                    # Extract complete JSON objects from buffer
                    while True:
                        # Find start of next record
                        start_idx = buffer.find('{"@id": "ISNI JSON-LD"')
                        if start_idx == -1:
                            break
                        
                        # Find end of this record by counting braces
                        brace_count = 0
                        in_string = False
                        escape_next = False
                        end_idx = -1
                        
                        for i in range(start_idx, len(buffer)):
                            char = buffer[i]
                            
                            if escape_next:
                                escape_next = False
                                continue
                            
                            if char == '\\' and in_string:
                                escape_next = True
                                continue
                            
                            if char == '"':
                                in_string = not in_string
                                continue
                            
                            if not in_string:
                                if char == '{':
                                    brace_count += 1
                                elif char == '}':
                                    brace_count -= 1
                                    if brace_count == 0:
                                        end_idx = i + 1
                                        break
                        
                        if end_idx == -1:
                            # Incomplete record, need more data
                            break
                        
                        # Extract and parse record
                        record_text = buffer[start_idx:end_idx]
                        buffer = buffer[end_idx:].lstrip(' \t\n\r,')
                        
                        try:
                            record = json.loads(record_text)
                            if "@graph" in record and isinstance(record["@graph"], list):
                                # Write entities from this record
                                for entity in record["@graph"]:
                                    if not first_entity:
                                        output_file.write(',\n')
                                    else:
                                        first_entity = False
                                    
                                    output_file.write('    ')
                                    json.dump(entity, output_file, ensure_ascii=False, separators=(',', ':'))
                                    entity_count += 1
                                    
                                    if entity_count % 10000 == 0:
                                        logger.info(f"Processed {entity_count} entities...")
                        except json.JSONDecodeError:
                            continue
                
                # Close JSON-LD structure
                output_file.write('\n  ]\n}')
        
        logger.info(f"Streaming restructuring complete: processed {entity_count} entities")
    
    def restructure_jsonld(self, data):
        """Legacy method for smaller files - kept for compatibility"""
        try:
            # For smaller files, use the original approach
            if len(data) < 100 * 1024 * 1024:  # Less than 100MB
                return self._restructure_small_file(data)
            else:
                # For larger files, this shouldn't be called
                logger.warning("Large file detected, should use streaming approach")
                return data
        except Exception as e:
            logger.error(f"Error in legacy restructuring: {e}")
            return data
    
    def _restructure_small_file(self, data):
        """Original restructuring for small files"""
        try:
            # Clean up control characters
            clean_data = data.replace('\x1e', '').replace('\x1f', '').replace('\x1d', '')
            
            # Parse the JSON data
            json_data = json.loads(clean_data)
            
            if not isinstance(json_data, list) or not json_data:
                logger.warning("ISNI data is not in expected array format")
                return data
            
            # Extract context from first record
            first_record = json_data[0]
            if "@context" not in first_record:
                logger.warning("No @context found in first record")
                return data
            
            context = first_record["@context"]
            
            # Collect all entities
            all_entities = []
            for record in json_data:
                if "@graph" in record and isinstance(record["@graph"], list):
                    all_entities.extend(record["@graph"])
            
            # Create restructured document
            restructured = {
                "@id": "ISNI JSON-LD",
                "@context": context,
                "@graph": all_entities
            }
            
            logger.info(f"Restructured {len(json_data)} records into {len(all_entities)} entities")
            
            return json.dumps(restructured, indent=2, ensure_ascii=False)
            
        except Exception as e:
            logger.error(f"Error restructuring small file: {e}")
            return data