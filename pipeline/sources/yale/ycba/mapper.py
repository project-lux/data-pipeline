from pipeline.process.base.mapper import Mapper
from pipeline.process.utils.mapper_utils import validate_timespans
import ujson as json
from shapely.geometry import shape


class YcbaMapper(Mapper):
    def __init__(self, config):
        Mapper.__init__(self, config)
        self.datacache = self.configs.internal["ycba"]["datacache"]

    def should_merge_into(self, base, to_merge):
        # Prevent merging Wikidata records into YCBA object/work records
        if base["data"]["type"] in ["HumanMadeObject", "LinguisticObject", "Set", "VisualItem"]:
            if to_merge["source"] == "wikidata":
                # print(f"returning false for wikidata to ycba")
                return False
        return True

    def should_merge_from(self, base, to_merge):
        # Prevent merging Wikidata records into YCBA object/work records
        if to_merge["data"]["type"] in ["HumanMadeObject", "LinguisticObject", "Set", "VisualItem"]:
            if base["source"] == "wikidata":
                # print(f"returning false for ycba into wikidata?")
                return False
        return True

    def transform(self, rec, rectype, reference=False):
        data = rec["data"]

        # 2022-08-19
        # straight up delete all ycba_term entries before they make a mess

        for p in ["represents", "about", "classified_as"]:
            if p in data:
                to_kill = []
                for item in data[p]:
                    if "id" in item and (
                        item["id"].startswith("http://collection.britishart.yale.edu/")
                        or item["id"].startswith("https://en.wikipedia.org/wiki/")
                    ):
                        to_kill.append(item)
                for k in to_kill:
                    data[p].remove(k)

        if "dimension" in data:
            for d in data["dimension"]:
                if "classified_as" in d:
                    to_kill = []
                    for c in d["classified_as"]:
                        if c["id"].startswith("http://collection.britishart.yale.edu/"):
                            to_kill.append(c)
                    for k in to_kill:
                        d["classified_as"].remove(k)

        # 2022-07-29: Fix double aats
        # Example: https://ycba-lux.s3.amazonaws.com/v3/visual/63/63e129b1-1415-469a-8997-a6d897904c38.json
        if data["type"] == "VisualItem":
            if "represents" in data:
                for i in data["represents"]:
                    if "id" in i and i["id"].startswith("http://vocab.getty.edu/aat/http://vocab.getty.edu/aat/"):
                        i["id"] = i["id"].replace(
                            "http://vocab.getty.edu/aat/http://vocab.getty.edu/aat/", "http://vocab.getty.edu/aat/"
                        )

        validate_timespans(data)

        # 2022-01-14 - Add Collection Item classification
        if "identified_by" in data:
            item = False
            ids = [x for x in data["identified_by"] if x["type"] == "Identifier"]
            for i in ids:
                if "classified_as" in i:
                    for c in i["classified_as"]:
                        if c["id"] == "http://vocab.getty.edu/aat/300312355":
                            # we're an item
                            item = True
                            break
            if item:
                if "classified_as" in data:
                    classes = data["classified_as"]
                else:
                    classes = []
                classes.append(
                    {"id": "http://vocab.getty.edu/aat/300404024", "type": "Type", "_label": "Collection Item"}
                )
                data["classified_as"] = classes

        # Rewrite any Place's coordinates into WKT
        if data["type"] == "Place" and "defined_by" in data:
            geo = data["defined_by"]
            geojs = json.loads(geo)
            geojs = geojs["features"][0]["geometry"]
            shp = shape(geojs)
            data["defined_by"] = shp.wkt

        self.fix_links(rec)

        return rec
