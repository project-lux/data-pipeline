"""ROR publishes each data release on Zenodo. The record's JSON lists the
versions newest first; the download URL is read from it with the
configured JSON path and fetched into ``dumps_dir``.

Configuration keys (see ``docs/sample_config/ror.json``)::

    "downloaderClass": "sources.ror.downloader.RorDownloader",
    "downloadMdUrl": "https://zenodo.org/api/records/14429114/versions?size=5&sort=version&allversions=true",
    "downloadXPath": "hits/hits[0]/files[0]/links/self"

which resolves to a URL such as
``https://zenodo.org/api/records/20512981/files/v2.8-2026-06-02-ror-data.zip/content``.
The file is saved as ``dumpFilePath`` when that names a ``.zip``, else
under the Zenodo file name (the path segment before ``/content``) in
``dumps_dir``; a ``dumpFilePath`` naming the extracted JSON, as the live
configuration does until the loader reads the zip (item 29), is left
alone. Any ``input_files`` entries (the published export) come from the
base class as usual.
"""

import logging

from lux_pipeline.process.base.downloader import BaseDownloader

logger = logging.getLogger("lux_pipeline")


def zenodo_filename(url):
    """The file name in a Zenodo download URL: the segment before a
    trailing ``/content``, else the last segment."""
    parts = url.rstrip("/").rsplit("/", 2)
    if parts[-1] == "content" and len(parts) > 1:
        return parts[-2]
    return parts[-1]


class RorDownloader(BaseDownloader):
    def get_urls(self, type="all"):
        urls = super().get_urls(type)
        if type not in ("all", "records"):
            return urls
        md_url = self.config.get("downloadMdUrl")
        xpath = self.config.get("downloadXPath")
        if not (md_url and xpath):
            return urls
        url = self.get_value_from_json(md_url, xpath)
        if not url:
            logger.error(f"{self.name}: could not discover the current data release from {md_url}")
            return urls
        dfp = self.config.get("dumpFilePath") or ""
        if dfp.lower().endswith(".zip"):
            path = self.resolve_path(dfp)
        else:
            path = self.resolve_path(zenodo_filename(url))
        return [{"url": url, "path": path}, *urls]
