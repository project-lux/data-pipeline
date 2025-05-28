from lux_pipeline.process.base.mapper import Mapper as Mpr


class WpMapper(Mpr):
    def transform(self, record, rectype, reference=False):
        # Just kill them for now
        return None
