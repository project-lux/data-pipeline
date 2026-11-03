"""VIAF's monthly cluster dump. The download API's JSON names the current
monthly file; the URL is the configured base plus that name.

Configuration keys (see ``docs/sample_config/viaf.json``)::

    "downloaderClass": "sources.oclc.downloader.ViafDownloader",
    "downloadMdUrl": "https://viaf.org/api/download/files",
    "downloadXPath": "queryResult/Monthly/clusterRdfXml",
    "downloadUrlBase": "https://downloads.viaf.org/viaf/data/monthly/"

The file is saved under its own (dated) name in ``dumps_dir``;
``dumpFilePath`` (``viaf/viaf-clusters.xml.gz``) is what the loader reads,
so point it at the downloaded file, or link it, after a download.
"""

import logging

from lux_pipeline.process.base.downloader import BaseDownloader

logger = logging.getLogger("lux_pipeline")


class ViafDownloader(BaseDownloader):
    def get_urls(self, type="all"):
        urls = super().get_urls(type)
        if type not in ("all", "records"):
            return urls
        md_url = self.config.get("downloadMdUrl")
        xpath = self.config.get("downloadXPath")
        if not (md_url and xpath):
            return urls
        name = self.get_value_from_json(md_url, xpath)
        if not name:
            logger.error(f"{self.name}: could not discover the current monthly dump from {md_url}")
            return urls
        base = self.config.get("downloadUrlBase") or ""
        url = name if name.startswith("http") else base + name
        path = self.resolve_path(name.rstrip("/").rsplit("/", 1)[-1])
        return [{"url": url, "path": path}, *urls]
