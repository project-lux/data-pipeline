"""YCBA: one tar.gz of ``<type>/<uu>/<uuid>.json`` members
(``dumpFileType: tar.gz/json``). The key is the last three components of
the member name, extension included, as the hand-written loader stored
them."""

from lux_pipeline.process.base.loader import Pointer
from lux_pipeline.sources.yale.archive_loader import ArchiveJsonLoader


class YcbaLoader(ArchiveJsonLoader):
    def make_identifier(self, value):
        if isinstance(value, Pointer):
            return "/".join(value.get_name().split("/")[-3:])
        return super().make_identifier(value)
