"""Paul Mellon Centre: one tar.gz of JSON members (``dumpFileType:
tar.gz/json``). The key is the member name without its ``.json``, as
the hand-written loader stored them."""

from lux_pipeline.process.base.loader import Pointer
from lux_pipeline.sources.yale.archive_loader import ArchiveJsonLoader


class PmcLoader(ArchiveJsonLoader):
    def make_identifier(self, value):
        if isinstance(value, Pointer):
            name = value.get_name()
            return name[:-5] if name.endswith(".json") else name
        return super().make_identifier(value)
