import os
from lux_pipeline.process.base.downloader import BaseDownloader


# "downloadMdUrl": "https://zenodo.org/api/records/14429114/versions?size=5&sort=version&allversions=true",
# "downloadXPath": "hits/hits[0]/files[0]/links/self",
# --> "https://zenodo.org/api/records/20512981/files/v2.8-2026-06-02-ror-data.zip/content"

class RorDownloader(BaseDownloader):
    def get_urls(self, type="records"):
        url1 = self.config['downloadMdUrl']
        xpath = self.config['downloadXPath']
        url2 = self.get_value_from_json(url1, xpath)
        if 'dumpFilePath' in self.config:
            dfn = self.config['dumpFilePath']
        else:
            dfn = url2.rsplit('/')[-2]
        p = os.path.join(self.dumps_dir, dfn)
        return [{"url": url2, "path": p}]
