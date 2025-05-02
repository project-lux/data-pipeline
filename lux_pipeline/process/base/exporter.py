import zipfile
import json
from lux_pipeline.storage.cache.postgres import DataCache
from ._managable import Managable


class Exporter:
    """
    The Exporter class is the base class for all exporters. The purpose of the Exporter is to take a list of records or cache and commit them to a dump file and upload data to HuggingFace.
    """
    def __init__(self, config):
        self.config = config

    def write_to_zip(self, zip_filename, data_generator):
        source = self.config["name"]
        with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for record in data_generator:
                # Use the identifier from the record as the filename
                filename = f"{source}/{record['identifier']}.json"
                # Convert the record data to a string
                data_str = json.dumps(record['data'], indent=2)
                # Write to the ZIP archive
                zipf.writestr(filename, data_str)


class CacheExporter(Exporter):
    def __init__(self, config):
        super().__init__(config)
        self.cache_source = DataCache(config)
        
    def iterate_cache(self):
        """
        Generator: Iterates across the cache and yields records.
        """
        data = self.cache_source.iter_records()
        for record in data:
            yield record
