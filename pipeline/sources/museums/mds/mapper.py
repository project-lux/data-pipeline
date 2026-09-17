from pipeline.process.base.mapper import Mapper
from pipeline.process.utils.mapper_utils import validate_timespans
import ujson as json


class EuropeanaMapper(Mapper):

    def transform(self, record, rectype=None, reference=False):
        rec = record['data']
        if rec['type'] == 'Period' and 'part_of' in rec and type(rec['part_of']) != list:
            rec['part_of'] = [rec['part_of']]
        return record


class NullMapper(Mapper):

    def transform(self, rec, rectype=None, reference=False):
        return rec

