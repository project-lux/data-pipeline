from pipeline.process.base.mapper import Mapper
from pipeline.process.utils.mapper_utils import validate_timespans
import ujson as json
from shapely.geometry import shape


class PmcMapper(Mapper):
    def __init__(self, config):
        Mapper.__init__(self, config)

    def transform(self, rec, rectype, reference=False):
        data = rec["data"]

        self.fix_links(rec)

        return rec
