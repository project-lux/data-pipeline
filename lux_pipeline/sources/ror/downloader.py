"""ROR publishes each data release on Zenodo under one concept record,
whose API URL redirects to the current version
(https://ror.readme.io/docs/zenodo). The file's download URL, name and
MD5 are read from that record.

Configuration keys (see ``docs/sample_config/ror.json``)::

    "downloaderClass": "sources.ror.downloader.RorDownloader",
    "downloadMdUrl": "https://zenodo.org/api/records/6347574",
    "downloadXPath": "files[0]/links/self"

which today resolves to a URL such as
``https://zenodo.org/api/records/23500708/files/v2.15-2026-10-20-ror-data.zip/content``.
The sibling ``key`` and ``checksum`` of the same ``files`` entry give the
file name and the MD5 the base class verifies. The file is saved as
``dumpFilePath`` when that names a ``.zip``, else under its own name in
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


def sibling_path(xpath, field):
    """The JSON path of `field` in the same ``files[]`` entry as `xpath`:
    ``files[0]/links/self`` -> ``files[0]/checksum``."""
    sep = "/" if "/" in xpath else "."
    parts = xpath.split(sep)
    while parts and parts[-1] in ("self", "links"):
        parts.pop()
    return sep.join([*parts, field])


class RorDownloader(BaseDownloader):
    def get_urls(self, type="all"):
        urls = super().get_urls(type)
        if type not in ("all", "records"):
            return urls
        md_url = self.config.get("downloadMdUrl")
        xpath = self.config.get("downloadXPath")
        if not (md_url and xpath):
            return urls
        doc = self.fetch_json(md_url)
        url = self.json_value(doc, xpath, md_url) if doc else None
        if not url:
            logger.error(f"{self.name}: could not discover the current data release from {md_url}")
            return urls
        # the same files[] entry carries the name and the checksum
        name = self.json_value(doc, sibling_path(xpath, "key"), md_url) or zenodo_filename(url)
        checksum = self.json_value(doc, sibling_path(xpath, "checksum"), md_url)
        dfp = self.config.get("dumpFilePath") or ""
        path = self.resolve_path(dfp) if dfp.lower().endswith(".zip") else self.resolve_path(name)
        entry = {"url": url, "path": path}
        if checksum:
            entry["checksum"] = checksum
        return [entry, *urls]
