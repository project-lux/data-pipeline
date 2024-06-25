from pipeline.process.base.mapper import Mapper

class IpchMapper(Mapper):

    def __init__(self, config):
        Mapper.__init__(self, config)

    def should_merge_into(self, base, to_merge):
        return True

    def should_merge_from(self, base, to_merge):
        return True


    def transform(self, rec, rectype, reference=False):
        data = rec['data']

        # Do any messing around here

        return rec