
import os
from lux_pipeline.process.base.downloader import BaseDownloader

class ViafDownloader(BaseDownloader):

    def get_urls(self):
        url1 = self.config['downloadMdUrl']
        xpath = self.config['downloadXPath']
        base = self.config['downloadUrlBase']
        url2 = self.get_value_from_json(url1, xpath)
        p = os.path.join(self.dumps_dir, url2.rsplit('/')[-1])
        return [{"url": base + url2, "path": p}]

