from pipeline.process.base.mapper import Mapper
from pipeline.process.utils.mapper_utils import validate_timespans
import json

class AshmolMapper(Mapper):

    def __init__(self, config):
        Mapper.__init__(self, config)
        self.datacache = self.configs.internal['ashmol']['datacache']

    def transform(self, rec, rectype):
        data = rec['data']
        self.fix_links(rec)

        # Add CollectionItem to HMOs

        if data['type'] == "HumanMadeObject":
            if 'classified_as' in data:
                classes = data['classified_as']
            else:
                classes = []
            classes.append({"id": "http://vocab.getty.edu/aat/300404024", "type":"Type", "_label": "Collection Item"})
            data['classified_as'] = classes

        return rec