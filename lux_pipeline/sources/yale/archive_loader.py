"""Shared base for the Yale units whose dump is a tar or zip of one JSON
file per record (ycba, ypm, yuag, pmc).

What main's hand-written loaders all did, kept here so the DSL (item 12)
reproduces their datacache keys and tolerance exactly:

* only members whose name contains ``/`` and ends with ``json`` are
  records (``should_process_item``);
* a member under `MIN_RECORD_BYTES` is a deleted record and is skipped;
* a member that is not valid JSON is logged and skipped rather than
  aborting the archive (``make_json``), after one attempt at
  ``repair_json`` for a subclass that knows a recurring defect;
* the key comes from ``make_identifier(pointer)``, which each unit
  defines its own way.
"""

import logging

import ujson as json

from lux_pipeline.process.base.loader import Loader, Pointer, TarPointer, ZipPointer

logger = logging.getLogger("lux_pipeline")

# an archive member shorter than this is an emptied (deleted) record
MIN_RECORD_BYTES = 30


class ArchiveJsonLoader(Loader):
    def member_size(self, child):
        if isinstance(child, TarPointer):
            return child.info.size
        if isinstance(child, ZipPointer):
            try:
                return child.parent.getinfo(child.info).file_size
            except KeyError:
                return None
        return None

    def archive_name(self, child):
        """The file name of the archive a member came from, without
        extensions (``agent.tar.gz`` -> ``agent``)."""
        parent = child.parent
        path = getattr(parent, "name", None) or getattr(parent, "filename", None) or ""
        return str(path).rstrip("/").rsplit("/", 1)[-1].split(".")[0]

    def should_process_item(self, child):
        if not isinstance(child, Pointer):
            return True
        name = child.get_name()
        if not (name.endswith("json") and "/" in name):
            return False
        size = self.member_size(child)
        return size is None or size >= MIN_RECORD_BYTES

    def repair_json(self, raw):
        """A second chance for a member that failed to parse; return the
        bytes to retry with, or the same bytes to give up."""
        return raw

    def make_json(self, path, comp, parent):
        if not isinstance(path, Pointer):
            return super().make_json(path, comp, parent)
        with self.file_opener(path, comp) as fh:
            raw = fh.read()
        if len(raw) < MIN_RECORD_BYTES:
            return None
        try:
            data = json.loads(raw)
        except Exception as e:
            fixed = self.repair_json(raw)
            if fixed == raw:
                logger.warning(f"{self.name}: broken record {path.get_name()}: {e}")
                return None
            try:
                data = json.loads(fixed)
            except Exception as e2:
                logger.warning(f"{self.name}: broken record {path.get_name()}: {e2}")
                return None
        ident = self.make_identifier(path)
        data = self._post_process_json(data, ident)
        if data is None:
            return None
        if not ident:
            ident = self.extract_identifier(data)
            if not ident:
                raise ValueError(f"Could not get an identifier in {self.name} for {path.get_name()}")
        return {"identifier": ident, "data": data}
