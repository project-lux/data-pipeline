from pipeline.process.base.mapper import Mapper
from pipeline.process.utils.mapper_utils import validate_timespans
import ujson as json

class NullMapper(Mapper):

    def transform(self, rec, rectype=None, reference=False):
        rec = Mapper.transform(self, rec, rectype)
        return rec

