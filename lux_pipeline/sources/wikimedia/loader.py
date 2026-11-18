"""Wikimedia Commons file pages from a MediaWiki XML dump
(``commonswiki-<date>-pages-articles*.xml.bz2``, or one of its
``-p<first>p<last>`` parts), filtered to the images LUX may show.

The ``mediawiki`` container streams the dump's ``<page>`` elements
(lxml ``iterparse``, each page freed once read) and passes on the pages
in namespace 6 (``File:``) that are not redirects, as
``{"pageid", "title", "revision", "timestamp", "wikitext"}``.
`post_process_json` then keeps a page only when its title has an image
extension (``image_extensions``, default `wikitext.IMAGE_EXTENSIONS`; the
mapper reads the same key, so nothing is loaded that it would not map) and
its templates grant one of the licences in ``allowed_licenses`` (the
same key `WmMapper` reads; default ``pd``, ``cc0``, ``cc-by-4.0``,
``cc-by-sa-4.0``), adding ``license`` (the least restrictive) and
``licenses`` (all of them): see `wikitext.page_licenses`.

The key is the file name as the image lookup in `lux.final.mapper`
writes it: the title without ``File:``, spaces as underscores,
percent-encoded (``Izvori_vru%C4%87e_vode_0114.jpg``). That is the key the
API records the fetcher stores are under, and `WmMapper` maps both
shapes.

Configuration::

    "loaderClass": "sources.wikimedia.loader.WmLoader",
    "input_files": {"records": [
        {"path": "commonswiki", "type": "dir/mediawiki.bz2/raw", "slice": "files",
         "include": ["*-pages-articles-multistream[0-9]*.xml-p*.bz2"]}
    ]}

for a directory of dump parts, one part per worker at a time (``include``
leaves out the checksum and index files a dump directory also holds, which
are not XML); or
``"type": "mediawiki.bz2/raw"`` for a single file, where every worker
reads the whole file and keeps its share of the pages. ``lux load
--source wikimedia --file <part>.xml.bz2`` recognises a MediaWiki export
(`sniffed_type`) and reads it that way.
"""

import bz2
import gzip
import logging
import os
from collections import Counter
from urllib.parse import quote

from lxml import etree

from lux_pipeline.process.base.loader import Loader
from lux_pipeline.sources.wikimedia import wikitext

logger = logging.getLogger("lux_pipeline")

FILE_NAMESPACE = "6"


def file_key(title):
    """The datacache key of a ``File:`` title."""
    if title.startswith("File:"):
        title = title[5:]
    return quote(title.strip().replace(" ", "_"))


class WmLoader(Loader):
    extra_containers = ("mediawiki",)

    def __init__(self, config):
        super().__init__(config)
        self.licenses = tuple(config.get("allowed_licenses") or wikitext.LICENSES)
        self.image_extensions = wikitext.image_extensions(config)
        self.pages = Counter()

    def _init_dsl(self):
        super()._init_dsl()
        self.step_functions["mediawiki"] = self.iterate_mediawiki

    def sniffed_type(self, path):
        """A MediaWiki export (plain, .bz2 or .gz) is ``mediawiki[.<comp>]/raw``;
        the generic sniffer would call it ``other``, one record."""
        if os.path.isfile(path):
            for comp, opener in (("bz2", bz2.open), ("gz", gzip.open), (None, open)):
                try:
                    with opener(path, "rb") as fh:
                        head = fh.read(4096)
                except (OSError, EOFError, ValueError):
                    continue
                if b"<mediawiki" in head:
                    return f"mediawiki.{comp}/raw" if comp else "mediawiki/raw"
        return super().sniffed_type(path)

    def iterate_mediawiki(self, path, comp, remaining):
        """The File pages of a MediaWiki XML export, as dicts."""
        before = Counter(self.pages)
        with self.file_opener(path, comp) as fh:
            for _, page in etree.iterparse(fh, events=("end",), tag="{*}page"):
                rec = self.file_page(page)
                # free what has been read: the dump is far larger than memory
                page.clear()
                while page.getprevious() is not None:
                    del page.getparent()[0]
                if rec is not None:
                    yield rec
        seen = self.pages - before
        logger.info(f"{self.name}: {path}: {seen['file']:,} file pages "
                    f"({seen['redirect']:,} redirects and {seen['other']:,} other pages skipped)")

    def file_page(self, page):
        """A namespace 6 page that is not a redirect, else None."""
        if page.findtext("{*}ns") != FILE_NAMESPACE:
            self.pages["other"] += 1
            return None
        if page.find("{*}redirect") is not None:
            self.pages["redirect"] += 1
            return None
        self.pages["file"] += 1
        rev = page.find("{*}revision")
        pageid = page.findtext("{*}id")
        return {
            "pageid": int(pageid) if pageid and pageid.isdigit() else pageid,
            "title": page.findtext("{*}title") or "",
            "revision": rev.findtext("{*}id") if rev is not None else None,
            "timestamp": rev.findtext("{*}timestamp") if rev is not None else None,
            "wikitext": (rev.findtext("{*}text") if rev is not None else None) or "",
        }

    def post_process_json(self, data):
        """Keep an image page with an allowed licence; None otherwise."""
        if not isinstance(data, dict) or "wikitext" not in data:
            return data
        if not wikitext.is_image(data.get("title", ""), self.image_extensions):
            self.pages["not an image"] += 1
            return None
        found = wikitext.page_licenses(data["wikitext"], self.licenses)
        if not found:
            self.pages["no allowed licence"] += 1
            return None
        self.pages["kept"] += 1
        data["license"] = found[0]
        data["licenses"] = found
        return data

    def extract_identifier(self, data):
        title = data.get("title") if isinstance(data, dict) else None
        return file_key(title) if title else None

    def process(self, *args, **kw):
        try:
            return super().process(*args, **kw)
        finally:
            if self.pages["file"]:
                logger.info(f"{self.name}: of this worker's file pages, kept {self.pages['kept']:,}; "
                            f"{self.pages['not an image']:,} not images, "
                            f"{self.pages['no allowed licence']:,} without an allowed licence")
