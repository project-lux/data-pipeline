"""The library (ILS) dump is not downloaded over HTTP: it is synced from
S3 into ``dumpFilePath`` outside the pipeline. This downloader exists so a
configuration can name it and ``lux download`` has nothing to do for the
source rather than failing."""

from lux_pipeline.process.base.downloader import BaseDownloader


class S3Downloader(BaseDownloader):
    def get_urls(self, type="all"):
        return []
