from pipeline.process.base.mapper import Mapper
from pipeline.process.utils.mapper_utils import validate_timespans
import ujson as json
from shapely.geometry import shape


class PmcMapper(Mapper):
    def __init__(self, config):
        Mapper.__init__(self, config)

    def transform(self, rec, rectype, reference=False):
        data = rec["data"]

        if data["type"] == "Group" and "part_of" in data:
            data["member_of"] = data["part_of"]
            del data["part_of"]

        if data["type"] == "Set" and "classified_as" in data:
            cxns = [x.get("id", None) for x in data["classified_as"]]
            if "http://vocab.getty.edu/aat/300375748" in cxns and "http://vocab.getty.edu/aat/300025976" in cxns:
                # only archive for now
                for x in data["classified_as"][:]:
                    if x.get("id", None) == "http://vocab.getty.edu/aat/300025976":
                        data["classified_as"].remove(x)

        self.fix_links(rec)

        return rec
