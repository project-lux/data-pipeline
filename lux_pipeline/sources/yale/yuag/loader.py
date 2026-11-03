"""YUAG: one zip of JSON members (``dumpFileType: zip/json``). The key is
the member name as it is, as the hand-written loader stored them."""

from lux_pipeline.process.base.loader import Pointer
from lux_pipeline.sources.yale.archive_loader import ArchiveJsonLoader


class YuagLoader(ArchiveJsonLoader):
    def make_identifier(self, value):
        if isinstance(value, Pointer):
            return value.get_name()
        return super().make_identifier(value)
