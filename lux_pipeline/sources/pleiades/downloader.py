import os
from lux_pipeline.process.base.downloader import BaseDownloader

class PleiadesDownloader(BaseDownloader):
    """
    Types url: https://atlantides.org/downloads/pleiades/rdf/place-types.ttl
    Places url: https://atlantides.org/downloads/pleiades/json/pleiades-places-latest.json.gz
    """
    def get_urls(self):
        place_types_url = self.config['input_files']["records"][0]['url']
        places_url = self.config['input_files']["records"][1]['url']
        dumps_dir = self.config['dumps_dir']
        place_types_path = os.path.join(dumps_dir, place_types_url.rsplit('/')[-1])
        places_path = os.path.join(dumps_dir, places_url.rsplit('/')[-1])
        return [{"url": place_types_url, "path": place_types_path}, {"url": places_url, "path": places_path}]