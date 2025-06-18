from pipeline.process.base.mapper import Mapper
from bs4 import BeautifulSoup

"""
Create a mapper that produces completely artificial triples.
If there isn't a search, then there isn't a triple.
"""


def simple_xpath(rec, xpath):
    props = xpath.split("/")
    for p in props:
        n = -1
        if p.endswith("]"):
            # Allow [n]
            sq = p.find("[")
            if sq > -1:
                try:
                    n = int(p[sq + 1 : -1])
                except ValueError:
                    n = -1
                p = p[:sq]
        rec = rec.get(p, {})
        if type(rec) is list and n > -1:
            rec = rec[n]
        if not rec:
            return None
    return rec


class QleverMapper(Mapper):
    def __init__(self, config):
        Mapper.__init__(self, config)
        self.configs = config["all_configs"]
        self.idmap = self.configs.get_idmap()
        self.globals = self.configs.globals

        self.primaryName = self.globals["primaryName"]
        self.sortName = self.globals["sortName"]
        self.gender = self.globals["gender"]
        self.nationality = self.globals["nationality"]
        self.occupation = self.globals["occupation"]
        self.sortIdentifier = self.idmap["https://vocab.getty.edu/aat/300456575##quaType"]
        self.height = self.idmap["http://vocab.getty.edu/aat/300055644##quaType"]
        self.width = self.idmap["http://vocab.getty.edu/aat/300055647##quaType"]
        self.depth = self.idmap["http://vocab.getty.edu/aat/300072633##quaType"]
        self.weight = self.idmap["http://vocab.getty.edu/aat/300056240##quaType"]

        self.triple_pattern = "<{subject}> <{predicate}> <{object}> ."
        self.literal_pattern = '<{subject}> <{predicate}> "{value}"{datatype} .'
        self.number_type = "^^<http://www.w3.org/2001/XMLSchema#decimal>"
        self.date_type = "^^<http://www.w3.org/2001/XMLSchema#dateTime>"

        self.datans = "https://lux.collections.yale.edu/data/"
        self.luxns = "https://lux.collections.yale.edu/ns/"
        self.rdfns = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        self.rdfsns = "http://www.w3.org/2000/01/rdf-schema#"
        self.lans = "https://linked.art/ns/terms/"
        self.crmns = "http://www.cidoc-crm.org/cidoc-crm/"

    def sanitize_uri(self, uri):
        if not uri.startswith(self.datans):
            # sanitize external links
            uri = uri.replace(" ", "%20")
            uri = uri.replace("\n", "")
            uri = uri.replace("\t", "")
            uri = uri.replace("\r", "")
            uri = uri.replace('"', "")
            uri = uri.replace("{", "%7B")
            uri = uri.replace("}", "%7D")
        return uri

    def do_bs_html(self, content):
        content = content.strip()
        if content.startswith("<"):
            soup = BeautifulSoup(content, features="lxml")
            clncont = soup.get_text()
            return clncont

    def get_prefix(self, which):
        if type(which) is dict and "type" in which:
            which = which["type"]
        if which in ["VisualItem", "LinguisticObject"]:
            pfx = "work"
        elif which in ["HumanMadeObject", "DigitalObject"]:
            pfx = "item"
        elif which in ["Person", "Group"]:
            pfx = "agent"
        elif which == "Place":
            pfx = "place"
        elif which in ["Type", "Language", "Material", "Currency", "MeasurementUnit"]:
            # Set here is Collection / Holdings. UI decision to put in with concepts
            pfx = "concept"
        elif which in ["Activity", "Event", "Period"]:
            pfx = "event"
        elif which == "Set":
            pfx = "set"
        else:
            print(f"Failed to find a prefix for {which}")
            pfx = "other"
        return pfx

    def transform(self, record, rectype=None, reference=False):
        data = record["data"]
        me = data["id"]
        rectype = data["type"]
        pfx = self.get_prefix(rectype)
        triples = []
        recordText = []
        t = {"subject": me, "predicate": f"{self.rdfns}type", "object": f"{self.luxns}{pfx.title()}"}
        lt = {"subject": me, "predicate": "", "value": "", "datatype": ""}
        triples.append(self.triple_pattern.format(**t))
        if pfx not in ["set", "place"]:
            t["object"] = f"{self.luxns}{rectype}"
            triples.append(self.triple_pattern.format(**t))

        # names
        lt["datatype"] = ""
        for idb in data["identified_by"]:
            if "content" not in idb or not idb["content"]:
                continue
            recordText.append(idb["content"])
            lt["value"] = idb["content"]
            if idb["type"] == "Name":
                # primaryName
                cxns = [x.get("id", None) for x in idb.get("classified_as", [])]
                if self.primaryName in cxns:
                    lt["predicate"] = f"{self.luxns}{pfx}PrimaryName"
                    triples.append(self.literal_pattern.format(**lt))
                elif self.sortName in cxns:
                    lt["predicate"] = f"{self.luxns}{pfx}SortName"
                    triples.append(self.literal_pattern.format(**lt))
                    continue
                lt["predicate"] = f"{self.luxns}{pfx}Name"
                triples.append(self.literal_pattern.format(**lt))
            else:
                cxns = [x.get("id", None) for x in idb.get("classified_as", [])]
                if self.sortIdentifier in cxns:
                    lt["predicate"] = f"{self.luxns}{pfx}SortIdentifier"
                    triples.append(self.literal_pattern.format(**lt))
                else:
                    lt["predicate"] = f"{self.luxns}{pfx}Identifier"
                    triples.append(self.literal_pattern.format(**lt))

        # statements
        for rtb in data.get("referred_to_by", []):
            # just add to recordText for now
            ct = rtb.get("content", "")
            if ct:
                ct = self.do_bs_html(ct)
                if ct:
                    recordText.append(ct)

        # equivalents
        t["predicate"] = f"{self.lans}equivalent"
        for eq in data.get("equivalent", []):
            if eqid := eq.get("id", None):
                t["object"] = eqid
                triples.append(self.triple_pattern.format(**t))

        # digital image
        # true iff representation/digitally_shown_by/access_point/id
        rep = simple_xpath(data, "representation[0]/digitally_shown_by[0]/access_point[0]/id")
        if rep:
            lt["predicate"] = f"{self.luxns}{pfx}HasDigitalImage"
            lt["value"] = 1
            lt["datatype"] = self.number_type
            triples.append(self.literal_pattern.format(**lt))

        # all classifications
        # agentClassification, workClassification (etc)
        t["predicate"] = f"{self.luxns}{pfx}Classification"
        for cls in data.get("classified_as", []):
            t["object"] = cls["id"]
            triples.append(self.triple_pattern.format(**t))

        # beginning/ending
        drels = {}
        if pfx in ["work", "concept", "set"] or rectype in ["DigitalObject"]:
            drels["created_by"] = "Beginning"
            drels["used_for"] = "Publication"
        elif rectype == "HumanMadeObject":
            drels["produced_by"] = "Beginning"
            drels["encountered_by"] = "Encounter"
            drels["used_for"] = "Publication"
            # possible: destroyed_by, modified_by, removed_by
        elif rectype == "Person":
            drels["born"] = "Beginning"
            drels["died"] = "Ending"
            drels["carried_out"] = "Activity"
            drels["participated_in"] = "Activity"
        elif rectype == "Group":
            drels["formed_by"] = "Beginning"
            drels["dissolved_by"] = "Ending"
            drels["carried_out"] = "Activity"
            drels["participated_in"] = "Activity"

        # Process embedded activities down to single artificial relationships
        for dprop, dtyp in drels.items():
            vals = data.get(dprop, None)
            if not vals:
                continue
            if type(vals) is not list:
                vals = [vals]
            for val in vals:
                pcls = f"{pfx.title()}{dtyp}"
                check = [val]
                check.extend(val.get("part", []))
                for bit in check:
                    whos = bit.get("carried_out_by", [])
                    t["predicate"] = f"{self.luxns}agentOf{pcls}"
                    for who in whos:
                        if "id" in who:
                            t["object"] = who["id"]
                            triples.append(self.triple_pattern.format(**t))
                    wheres = bit.get("took_place_at", [])
                    t["predicate"] = f"{self.luxns}placeOf{pcls}"
                    for where in wheres:
                        if "id" in where:
                            t["object"] = where["id"]
                            triples.append(self.triple_pattern.format(**t))
                    types = bit.get("classified_as", [])
                    types.extend(bit.get("technique", []))
                    t["predicate"] = f"{self.luxns}typeOf{pcls}"
                    for typ in types:
                        if "id" in typ:
                            t["object"] = typ["id"]
                            triples.append(self.triple_pattern.format(**t))
                    causes = bit.get("caused_by", [])
                    t["predicate"] = f"{self.luxns}causeOf{pcls}"
                    for cause in causes:
                        if "id" in cause:
                            t["object"] = cause["id"]
                            triples.append(self.triple_pattern.format(**t))
                    infs = bit.get("influenced_by", [])
                    infs.extend(bit.get("used_specific_object", []))
                    for inf in infs:
                        if "type" in inf:
                            infpfx = self.get_prefix(inf["type"])
                            t["predicate"] = f"{self.luxns}{infpfx}InfluenceOf{pcls}"
                        else:
                            continue
                        if "id" in inf:
                            t["object"] = inf["id"]
                            triples.append(self.triple_pattern.format(**t))
                    # timespan
                    timespan = bit.get("timespan", {})
                    if timespan:
                        # start, end dates
                        lt["datatype"] = self.date_type
                        startval = timespan.get("begin_of_the_begin", "")
                        if startval:
                            lt["predicate"] = f"{self.luxns}startOf{pcls}"
                            lt["value"] = startval
                            triples.append(self.literal_pattern.format(**lt))
                        endval = timespan.get("end_of_the_end", "")
                        if endval:
                            lt["predicate"] = f"{self.luxns}endOf{pcls}"
                            lt["value"] = endval
                            triples.append(self.literal_pattern.format(**lt))

        # memberOf
        for member in data.get("memberOf", []):
            if "id" in member:
                t["object"] = member["id"]
                t["predicate"] = f"{self.luxns}{pfx}MemberOf{member['type']}"
                triples.append(self.triple_pattern.format(**t))

        # partOf
        parents = data.get("part_of", [])
        for parent in parents:
            if "id" in parent:
                t["predicate"] = f"{self.luxns}{pfx}PartOf"
                t["object"] = parent["id"]
                triples.append(self.triple_pattern.format(**t))  #

        # Class Specific relationships
        if pfx in ["work", "set"]:
            # Language
            langs = data.get("language", [])
            t["predicate"] = f"{self.luxns}{pfx}Language"
            for lang in langs:
                if "id" in lang:
                    t["object"] = lang["id"]
                    triples.append(self.triple_pattern.format(**t))

            # Subjects
            abouts = data.get("about", [])
            abouts.extend(data.get("depicts", []))
            for about in abouts:
                if "id" in about:
                    t["object"] = about["id"]
                    abpfx = self.get_prefix(about)
                    t["predicate"] = f"{self.luxns}{pfx}About{abpfx}"
                    triples.append(self.triple_pattern.format(**t))

        elif pfx == "item":
            # carries/shows
            carries = data.get("carries", [])
            carries.extend(data.get("shows", []))
            for c in carries:
                if "id" in c:
                    t["object"] = c["id"]
                    t["predicate"] = f"{self.luxns}carries"
                    triples.append(self.triple_pattern.format(**t))

            # materials
            mats = data.get("made_of", [])
            t["predicate"] = f"{self.luxns}material"
            for mat in mats:
                if "id" in mat:
                    t["object"] = mat["id"]
                    triples.append(self.triple_pattern.format(**t))

            # dimensions
            dims = data.get("dimension", [])
            lt["datatype"] = self.number_type
            for d in dims:
                if "value" in d:
                    lt["value"] = d["value"]
                    cxns = [x["id"] for x in d.get("classified_as", []) if "id" in x]
                    if self.height in cxns:
                        lt["predicate"] = f"{self.luxns}height"
                    elif self.width in cxns:
                        lt["predicate"] = f"{self.luxns}width"
                    elif self.depth in cxns:
                        lt["predicate"] = f"{self.luxns}depth"
                    elif self.weight in cxns:
                        lt["predicate"] = f"{self.luxns}weight"
                    triples.append(self.literal_pattern.format(**lt))
                    lt["predicate"] = f"{self.luxns}dimension"
                    triples.append(self.literal_pattern.format(**lt))

        elif pfx == "agent":
            # nationality, occupation, gender
            cxns = data.get("classified_as", [])
            for cxn in cxns:
                if "id" in cxn:
                    metas = cxn.get("classified_as", [])
                    if self.nationality in metas:
                        t["predicate"] = f"{self.luxns}nationality"
                    elif self.occupation in metas:
                        t["predicate"] = f"{self.luxns}occupation"
                    elif cxn["id"] == self.gender:
                        t["predicate"] = f"{self.luxns}gender"
                    else:
                        continue
                    t["object"] = cxn["id"]
                    triples.append(self.triple_pattern.format(**t))

        elif pfx == "concept":
            broaders = data.get("broader", [])
            for b in broaders:
                if "id" in b:
                    t["predicate"] = f"{self.luxns}broader"
                    t["object"] = b["id"]
                    triples.append(self.triple_pattern.format(**t))

        elif pfx == "place":
            pass
        elif pfx == "event":
            whos = data.get("carried_out_by", [])
            t["predicate"] = f"{self.luxns}agentOfEvent"
            for who in whos:
                if "id" in who:
                    t["object"] = who["id"]
                    triples.append(self.triple_pattern.format(**t))
            wheres = data.get("took_place_at", [])
            t["predicate"] = f"{self.luxns}placeOfEvent"
            for where in wheres:
                if "id" in where:
                    t["object"] = where["id"]
                    triples.append(self.triple_pattern.format(**t))
            causes = data.get("caused_by", [])
            t["predicate"] = f"{self.luxns}causeOfEvent"
            for cause in causes:
                if "id" in cause:
                    t["object"] = cause["id"]
                    triples.append(self.triple_pattern.format(**t))
            infs = data.get("influenced_by", [])
            infs.extend(data.get("used_specific_object", []))
            for inf in infs:
                if "type" in inf:
                    infpfx = self.get_prefix(inf["type"])
                    t["predicate"] = f"{self.luxns}eventUsed{infpfx.title()}"
                else:
                    continue
                if "id" in inf:
                    t["object"] = inf["id"]
                    triples.append(self.triple_pattern.format(**t))
            # timespan
            timespan = data.get("timespan", {})
            if timespan:
                # start, end dates
                lt["datatype"] = self.date_type
                startval = timespan.get("begin_of_the_begin", "")
                if startval:
                    lt["predicate"] = f"{self.luxns}startOfEvent"
                    lt["value"] = startval
                    triples.append(self.literal_pattern.format(**lt))
                endval = timespan.get("end_of_the_end", "")
                if endval:
                    lt["predicate"] = f"{self.luxns}endOfEvent"
                    lt["value"] = endval
                    triples.append(self.literal_pattern.format(**lt))

        elif pfx == "set":
            curates = data.get("used_for", [])
            t["predicate"] = f"{self.luxns}setCuratedBy"
            for c in curates:
                if "carried_out_by" in c and "id" in c["carried_out_by"]:
                    t["object"] = c["carried_out_by"]["id"]
                    triples.append(self.triple_pattern.format(**t))

        else:
            raise ValueError(f"Unsupported prefix: {pfx}")

        # add in recordText
        rtxt = " ".join([x for x in recordText if x])
        lt["predicate"] = f"{self.luxns}recordText"
        lt["value"] = rtxt
        lt["datatype"] = ""
        triples.append(self.literal_pattern.format(**lt))

        return triples
