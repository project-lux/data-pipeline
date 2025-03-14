import os
from lux_pipeline.process.base.downloader import BaseDownloader

class RorDownloader(BaseDownloader):
    def get_urls(self):
        url1 = self.config['downloadMdUrl']
        xpath = self.config['downloadXPath']
        url2 = self.get_value_from_json(url1, xpath)
        if 'dumpFilePath' in self.config:
            dfn = self.config['dumpFilePath']
        else:
            dfn = url2.rsplit('/')[-2]
        p = os.path.join(self.dumps_dir, dfn)
        return [{"url": url2, "path": p}]
