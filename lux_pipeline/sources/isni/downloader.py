"""ISNI publishes its people and organisations as two JSON-LD dumps:

* https://isni.org/isni/data-person/data.jsonld (about 20 GB)
* https://isni.org/isni/data-organization/data.jsonld

Each is a sequence of documents, one per ISNI record, written as a JSON
array or separated by ASCII record separators (``\\x1e``), with ``\\x1d`` /
``\\x1f`` control characters scattered through them::

    [{"@id": "ISNI JSON-LD", "@context": {...}, "@graph": [{"@id":
      "https://isni.org/isni/0000000121032683", "@type": "schema:Person",
      "schema:alternateName": [...], "owl:sameAs": [...], ...}]}, ...]

The context is the same in every document and the nodes are written in
its compact form (``schema:``, ``owl:``, ``madsrdf:``), which the mapper
reads. So the downloader saves the dump beside its target, then rewrites
it as one ``<ISNI><TAB><node JSON>`` line per node whose ``@id`` is an
ISNI, gzipped: what the loader reads as ``lines.gz/tabjson``, one record
at a time, without a source-specific loader. The original is removed
when the rewrite succeeds; nodes without an ISNI ``@id`` are counted and
logged, not kept.

Configuration (the ``path`` names the rewritten file; without one, the
URL's directory names it: ``data-person`` -> ``isni-person.jsonl.gz``)::

    "name": "isni",
    "type": "external",
    "namespace": "http://isni.org/isni/",
    "matches": ["isni.org/isni/"],
    "downloaderClass": "sources.isni.downloader.IsniDownloader",
    "mapperClass": "sources.isni.mapper.IsniMapper",
    "dumps_dir": "isni",
    "input_files": {"records": [
        {"url": "https://isni.org/isni/data-person/data.jsonld",
         "path": "isni-person.jsonl.gz", "type": "lines.gz/tabjson"},
        {"url": "https://isni.org/isni/data-organization/data.jsonld",
         "path": "isni-organization.jsonl.gz", "type": "lines.gz/tabjson"}]}

The namespace is the ``http`` form the other sources' equivalents and
``external_uri_rewrites`` use.
"""

import codecs
import gzip
import json
import logging
import os
import re

from lux_pipeline.process.base.downloader import BaseDownloader

logger = logging.getLogger("lux_pipeline")

# 15 digits and a check character (a digit or X)
ISNI_RE = re.compile(r"isni\.org/isni/([0-9]{15}[0-9X])$", re.IGNORECASE)
# separators the dump puts between and inside its documents
CONTROL = str.maketrans("", "", "\x1d\x1e\x1f")
BETWEEN = " \t\r\n,[]"
READ_BYTES = 8 * 1024 * 1024
# no single document is anywhere near this: an unparsable one, not a long one
MAX_DOCUMENT = 256 * 1024 * 1024
GZIP_LEVEL = 1


def isni_number(uri):
    """The 16-character ISNI at the end of an ISNI URI, upper case; None
    for anything else."""
    if not isinstance(uri, str):
        return None
    m = ISNI_RE.search(uri.strip().rstrip("/"))
    return m.group(1).upper() if m else None


def iter_documents(fh, read_bytes=READ_BYTES, progress=None):
    """Every top-level JSON object in the binary file `fh`, whether written
    as an array, one per line, or between record separators; the control
    characters are dropped first. `progress(n)` is called with the bytes
    read. Raises ``ValueError`` (with the byte offset) for a document that
    cannot be parsed."""
    decoder = json.JSONDecoder()
    utf8 = codecs.getincrementaldecoder("utf-8")()
    buf, pos, offset, eof = "", 0, 0, False

    def more():
        nonlocal eof, offset
        data = fh.read(read_bytes)
        if progress is not None and data:
            progress(len(data))
        offset += len(data)
        if not data:
            eof = True
        return utf8.decode(data, final=eof).translate(CONTROL)

    while True:
        while pos < len(buf) and buf[pos] in BETWEEN:
            pos += 1
        if pos >= len(buf):
            if eof:
                return
            buf, pos = more(), 0
            continue
        try:
            doc, end = decoder.raw_decode(buf, pos)
        except ValueError as e:
            if eof or len(buf) - pos > MAX_DOCUMENT:
                raise ValueError(f"unparsable ISNI document before byte {offset:,}: {e}") from None
            buf, pos = buf[pos:] + more(), 0
            continue
        pos = end
        if isinstance(doc, dict):
            yield doc


def flatten(src, dest, level=GZIP_LEVEL, progress=None):
    """Rewrite the dump `src` as ``<ISNI><TAB><node>`` lines in the gzip
    file `dest` (written to ``<dest>.part``, then renamed). Returns
    ``{"documents", "records", "skipped"}``: skipped nodes have no ISNI
    ``@id``."""
    stats = {"documents": 0, "records": 0, "skipped": 0}
    part = dest + ".part"
    try:
        with open(src, "rb") as fh, gzip.open(part, "wt", compresslevel=level,
                                              encoding="utf-8") as out:
            for doc in iter_documents(fh, progress=progress):
                stats["documents"] += 1
                nodes = doc.get("@graph")
                if not isinstance(nodes, list):
                    nodes = [doc] if isinstance(doc.get("@type"), (str, list)) else []
                for node in nodes:
                    num = isni_number(node.get("@id")) if isinstance(node, dict) else None
                    if num is None:
                        stats["skipped"] += 1
                        continue
                    out.write(num)
                    out.write("\t")
                    out.write(json.dumps(node, ensure_ascii=False, separators=(",", ":")))
                    out.write("\n")
                    stats["records"] += 1
                if not stats["documents"] % 1000000:
                    logger.info(f"{stats['documents']:,} ISNI documents, "
                                f"{stats['records']:,} records")
    except BaseException:
        try:
            os.remove(part)
        except FileNotFoundError:
            pass
        raise
    os.replace(part, dest)
    return stats


class IsniDownloader(BaseDownloader):
    def get_urls(self, type="all"):
        """The base class's list, except that an entry with no ``path`` is
        named from its URL's directory: both dumps end in ``data.jsonld``."""
        urls = []
        for rtype, entries in (self.config.get("input_files") or {}).items():
            if type not in ("all", rtype):
                continue
            for entry in entries or []:
                url = entry.get("url")
                if not url:
                    continue
                path = entry.get("path")
                if not path:
                    parts = url.rstrip("/").split("/")
                    stem = parts[-2] if len(parts) > 1 else parts[-1]
                    path = f"isni-{stem.removeprefix('data-')}.jsonl.gz"
                urls.append({"url": url, "path": self.resolve_path(path)})
        return urls

    def process(self, download, overwrite=True):
        """Download the dump to ``<path>.jsonld``, rewrite it as `path`,
        and remove the original. Returns 1, or 0 when `path` exists and
        ``overwrite`` is false."""
        path = download["path"]
        label = f"{self.name}/{os.path.basename(path)}"
        if not overwrite and os.path.exists(path):
            logger.info(f"{label}: exists, not overwriting")
            self.progress(description=f"{label} (kept)")
            return 0
        raw = path + ".jsonld"
        super().process({**download, "path": raw}, overwrite=True)

        size = os.path.getsize(raw)
        self.progress(total=size, completed=0, description=f"{label} (rewriting)")
        logger.info(f"{label}: rewriting {size:,} bytes of JSON-LD as one record per line")
        stats = flatten(raw, path, progress=lambda n: self.progress(advance=n))
        os.remove(raw)
        logger.info(f"{label}: {stats['records']:,} records from {stats['documents']:,} "
                    f"documents; {stats['skipped']:,} nodes without an ISNI @id left out")
        return 1
