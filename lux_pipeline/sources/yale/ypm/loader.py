"""YPM: one tar.gz per record type (agent, object, place, taxon), listed
as ``input_files`` records entries typed ``tar.gz/json``. The key is
``<archive name>/<member name>`` (``agent/agent/123.json``), as the
hand-written loader stored them. The Peabody export sometimes writes a
bare ``"value": .``; that is repaired as before."""

from lux_pipeline.process.base.loader import Pointer
from lux_pipeline.sources.yale.archive_loader import ArchiveJsonLoader


class YpmLoader(ArchiveJsonLoader):
    def make_identifier(self, value):
        if isinstance(value, Pointer):
            return f"{self.archive_name(value)}/{value.get_name()}"
        return super().make_identifier(value)

    def repair_json(self, raw):
        return raw.replace(b'"value": .', b'"value": 0.')
