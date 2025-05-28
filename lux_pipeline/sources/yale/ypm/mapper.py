from lux_pipeline.process.base.mapper import Mapper
from lux_pipeline.process.utils.date_utils import validate_timespans


class YpmMapper(Mapper):
    def transform(self, rec, rectype=None, reference=False):
        rec = Mapper.transform(self, rec, rectype)
        data = rec["data"]

        if "@context" in data:
            data["@context"] = "https://linked.art/ns/v1/linked-art.json"

        if "_last_mod_local" in data:
            del data["_last_mod_local"]

        if data["type"] == "Type" and "classified_as" in data:
            for cxn in data["classified_as"]:
                if "id" in cxn and not cxn["id"]:
                    if cxn["_label"] == "Species":
                        cxn["id"] = "https://www.wikidata.org/entity/Q7432"
                        break

        if data["type"] == "Place" and "identified_by" in data:
            # check for 'no locality data' as primary name and something else as alternate
            # then make alternate into primary and delete NLD
            p = None
            alt = None
            for n in data["identified_by"]:
                if n["type"] == "Name" and "classified_as" in n:
                    if n["content"].lower() == "[no locality data]":
                        for c in n["classified_as"]:
                            if c["id"] == "http://vocab.getty.edu/aat/300404670":
                                p = n
                                break
                    elif n["content"].lower() != "[no locality data]":
                        alt = n
                        break
            if p is not None and alt is not None:
                data["identified_by"].remove(alt)
                p["content"] = alt["content"]

        bad_ypm_cxn = "https://lux-front-dev.collections.yale.edu/data/concept/c6fc19d0-44e1-4464-82d0-d08ac1022555"
        # -->  http://vocab.getty.edu/aat/300215302
        if "representation" in data:
            for rep in data["representation"]:
                if "digitally_shown_by" in rep:
                    for do in rep["digitally_shown_by"]:
                        if "classified_as" in do:
                            for cxn in do["classified_as"]:
                                if "id" in cxn and cxn["id"] == bad_ypm_cxn:
                                    cxn["id"] = "http://vocab.getty.edu/aat/300215302"

        for p in ["born", "died"]:
            if p in data:
                if "took_place_at" in data[p]:
                    to_kill = []
                    for i in data[p]["took_place_at"]:
                        if not "id" in i:
                            to_kill.append(i)
                    for k in to_kill:
                        data[p]["took_place_at"].remove(k)
        if "used_for" in data:
            for uf in data["used_for"]:
                for p in ["took_place_at", "carried_out_by"]:
                    if p in uf:
                        to_kill = []
                        for i in uf[p]:
                            if not "id" in i:
                                to_kill.append(i)
                        for k in to_kill:
                            uf[p].remove(k)

        if "equivalent" in data:
            for eq in data["equivalent"]:
                eq["type"] = data["type"]

        if "identified_by" in data:
            to_kill = []
            for i in data["identified_by"]:
                if not "content" in i:
                    to_kill.append(i)
            for k in to_kill:
                data["identified_by"].remove(k)

        # toss out defined_by garbage
        # YPM recs only have valid data beginning with POLYGON
        if "defined_by" in data:
            defined = data["defined_by"].strip()
            if not defined.startswith(("POLYGON", "POINT")):
                del data["defined_by"]

        validate_timespans(data)

        # 2022-01-14: Add item classified_as to make finding items easier
        # Rely on things in collection having accession numbers
        item = False
        if "identified_by" in data and data["type"] == "HumanMadeObject":
            ids = [x for x in data["identified_by"] if x["type"] == "Identifier"]
            for i in ids:
                if "classified_as" in i:
                    for c in i["classified_as"]:
                        if c["id"] in [
                            "http://vocab.getty.edu/aat/300404620",
                            "http://vocab.getty.edu/aat/300404621",
                            "http://vocab.getty.edu/aat/300312355",
                        ]:
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

        return rec
