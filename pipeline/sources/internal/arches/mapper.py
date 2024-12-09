from pipeline.process.base.mapper import Mapper

class ArchesMapper(Mapper):

    def transform(self, rec, rectype=None, reference=False):
        rec = Mapper.transform(self, rec, rectype)
        data = rec['data']
        # modify in place

        # As needed?

        return rec

