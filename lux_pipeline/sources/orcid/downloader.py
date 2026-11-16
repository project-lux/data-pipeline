"""ORCID publishes its Public Data File once a year as a figshare article:
eleven ``ORCID_<year>_<month>_activities_<n>.tar.gz`` files and one
``ORCID_<year>_<month>_summaries.tar.gz`` (57GB in 2026). Only the
summaries are loaded, so only that file is downloaded. Its URL, name and
MD5 are read from the figshare API.

Configuration keys (pipeline-configs' ``orcid.json``)::

    "downloaderClass": "sources.orcid.downloader.OrcidDownloader",
    "downloadMdUrl": "https://api.figshare.com/v2/articles/34697766",
    "dumpFilePath": "orcid/orcid.tgz",
    "shards": 24

``downloadMdUrl`` names the year's article, either as its API URL or as
the page (``https://orcid.figshare.com/articles/dataset/<title>/<id>``),
and has to be updated when ORCID publishes the next year's file. The
file is saved as ``dumpFilePath`` (else under its own name in
``dumps_dir``).

Once the download is complete, the archive is split into ``shards``
(default 24) gzipped JSON Lines files beside it,
``<dump>_000.jsonl.gz`` and on, holding the members `OrcidLoader.should_load`
keeps as ``<orcid id><TAB>{"xml": ...}`` lines; ``lux load --source
orcid`` reads those rather than walking the archive in every worker (see
`OrcidLoader.split`). The old shards are deleted before a new download
is split, so a split that fails leaves the loader reading the archive
itself, not last year's records. When the archive is kept
(``--no-overwrite``) and its shards are all there, nothing is split again.
"""

import logging
import re

from lux_pipeline.process.base.downloader import BaseDownloader
from lux_pipeline.sources.orcid.loader import OrcidLoader

logger = logging.getLogger("lux_pipeline")

FIGSHARE_API = "https://api.figshare.com/v2/articles/{}"
SUMMARIES = re.compile(r"_summaries\.tar\.gz$")
DEFAULT_SHARDS = 24


def figshare_api_url(url):
    """The API URL of a figshare article, from its API URL or its page
    URL (the article id is the page's last numeric segment)."""
    if "api.figshare.com" in url:
        return url
    ids = re.findall(r"/(\d+)(?=/|$)", url.split("?", 1)[0].rstrip("/"))
    if not ids:
        raise ValueError(f"no figshare article id in {url}")
    return FIGSHARE_API.format(ids[-1])


def summaries_file(article):
    """The article's ``files`` entry for the summaries archive, or None."""
    for f in (article or {}).get("files") or []:
        if SUMMARIES.search(f.get("name") or ""):
            return f
    return None


class OrcidDownloader(BaseDownloader):

    def shard_count(self):
        n = int(self.config.get("shards") or DEFAULT_SHARDS)
        if n < 1:
            raise ValueError(f"{self.name}: shards must be at least 1, not {n}")
        return n

    def get_urls(self, type="all"):
        urls = super().get_urls(type)
        md_url = self.config.get("downloadMdUrl")
        if type not in ("all", "records") or not md_url:
            return urls
        api = figshare_api_url(md_url)
        f = summaries_file(self.fetch_json(api))
        if f is None or not f.get("download_url"):
            logger.error(f"{self.name}: no *_summaries.tar.gz file in the figshare article {api}")
            return urls
        dfp = self.config.get("dumpFilePath")
        path = self.resolve_path(dfp or f["name"])
        entry = {"url": f["download_url"], "path": path, "shards": self.shard_count()}
        md5 = f.get("computed_md5") or f.get("supplied_md5")
        if md5:
            entry["checksum"] = f"md5:{md5}"
        # the figshare file replaces a remoteDumpFile saved to the same path
        return [entry, *(u for u in urls if u["path"] != path)]

    def splitter(self, path):
        """The source's OrcidLoader, reading `path`."""
        ldr = self.config.get("loader")
        if not isinstance(ldr, OrcidLoader):
            ldr = OrcidLoader(self.config)
        ldr.in_path = path
        return ldr

    def process(self, download, overwrite=True):
        count = download.get("shards")
        ldr = self.splitter(download["path"]) if count else None
        if ldr is not None and (overwrite or not ldr.shards_ready(count)):
            # stale as soon as a new archive starts to arrive
            ldr.remove_shards()
        written = super().process(download, overwrite)
        if ldr is None:
            return written
        if not written and ldr.shards_ready(count):
            logger.info(f"{self.name}: the {count} files split from {download['path']} are there")
            return written
        ldr.progress = self.progress
        ldr.split(None, count)
        return written
