from pipeline.process.base.mapper import Mapper
from cromulent import model, vocab


class RorMapper(Mapper):
    def __init__(self, config):
        Mapper.__init__(self, config)

    def guess_type(self, data):
        # uhh, France?
        return model.Group

    def transform(self, record, rectype, reference=False):
        # NOTE this is v2 of the API
        # api.ror.org/v2/organizations/{ident}
        rec = record["data"]

        if "name" in rec and not "names" in rec:
            rec["names"] = [rec["name"]]
            del rec["name"]
        if "labels" in rec:
            if not "names" in rec:
                rec["names"] = []
            for l in rec["labels"]:
                n = {"value": l["label"]}
                if "iso639" in l:
                    n["lang"] = l["iso639"]
                rec["names"].append(n)

        if not rectype:
            rectype = "Group"
        if rectype and rectype != "Group":
            return None
        elif not "names" in rec or not rec["names"]:
            return None

        top = model.Group(ident=rec["id"])
        for n in rec["names"]:
            if type(n) == str:
                n = {"value": n, "types": ["ror_display"]}
            if "ror_display" in n["types"]:
                nm = vocab.PrimaryName(content=n["value"])
            elif not "alias" in n["types"]:
                nm = model.Name(content=n["value"])
            else:
                continue
            if "lang" in n:
                nm.language = self.process_langs[n["lang"]]
            top.identified_by = nm

        if "established" in rec and rec["established"]:
            fmn = model.Formation()
            ts = model.TimeSpan()
            b = rec["established"]  # year as int
            ts.begin_of_the_begin = f"{b}-01-01T00:00:00"
            ts.end_of_the_end = f"{b}-12-31T23:59:59"
            ts.identified_by = vocab.DisplayName(content=f"{b}")
            fmn.timespan = ts
            top.formed_by = fmn

        # types = classified_as, but need mapping for values:
        # Education, Healthcare, Company, Archive, Nonprofit, Government
        # Facility, Other

        if "locations" in rec and rec["locations"]:
            for addr in rec["addresses"]:
                if "geonames_id" in addr:
                    gn = addr["geonames_id"]
                    top.residence = model.Place(ident=f"https://sws.geonames.org/{gn}")

        if "links" in rec:
            # home page
            for l in rec["links"]:
                if type(l) == str:
                    l = {"type": "website", "value": l}
                if l["type"] == "website":
                    lo = model.LinguisticObject(label="Website Text")
                    do = vocab.WebPage(label="Website")
                    ap = model.DigitalObject(ident=l["value"])
                    do.access_point = ap
                    lo.digitally_carried_by = do
                    top.subject_of = lo

        known_typs = {"ISNI": "isni", "Wikidata": "wd"}
        if "external_ids" in rec and rec["external_ids"]:
            for ext in rec["external_ids"]:
                typ = ext["type"]
                if typ in known_typs:
                    for a in ext["all"]:
                        top.equivalent = model.Group(ident=f"{known_typs[typ]}{a}")

        # Relationships
        if "relationships" in rec and rec["relationships"]:
            for rel in rec["relationships"]:
                if "type" in rel and rel["type"].lower() == "parent":
                    top.member_of = model.Group(ident=rel["id"], label=rel["label"])

        data = model.factory.toJSON(top)
        recid = record["identifier"]
        return {"identifier": recid, "data": data, "source": "ror"}
