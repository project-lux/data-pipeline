import os
from pipeline.process.base.downloader import BaseDownloader

class RorDownloader(BaseDownloader):
    def get_urls(self):
        url1 = self.config['downloadMdUrl']
        xpath = self.config['downloadXPath']
        url2 = self.get_value_from_json(url1, xpath)
        p = os.path.join(self.dumps_dir, url2.rsplit('/')[-2])
        return [{"url": url2, "path": p}]
