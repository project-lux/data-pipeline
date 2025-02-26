from pipeline.process.base.downloader import BaseDownloader

class RorDownloader(BaseDownloader):
    def get_urls(self):

        url = self.config['downloadMdUrl']
        xpath = self.config['downloadXPath']
        url = self.get_value_from_json(url, xpath)
        return [url]

