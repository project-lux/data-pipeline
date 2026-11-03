"""Wikipedia article texts are loaded for lookup only; the mapper
produces no records. Ported from the refactor branch as it was."""

from lux_pipeline.process.base.mapper import Mapper


class WpMapper(Mapper):
    def transform(self, record, rectype, reference=False):
        # Just kill them for now
        return None
