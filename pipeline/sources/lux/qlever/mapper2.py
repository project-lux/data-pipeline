from pipeline.process.base.mapper import Mapper
from bs4 import BeautifulSoup

"""
Create a mapper that produces completely artificial triples.
If there isn't a search, then there isn't a triple.
"""


examples = """
PREFIX lux: <https://lux.collections.yale.edu/ns/>
SELECT DISTINCT ?what WHERE {
   ?what lux:recordText ?ftxt .
   ?t ql:contains-word "guide* routing" .
   ?t ql:contains-entity ?ftxt .
   FILTER (contains(?ftxt, "guidebook routing")) # as a phrase
  } LIMIT 100


PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
PREFIX lux: <https://lux.collections.yale.edu/ns/>
PREFIX la: <https://linked.art/ns/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
SELECT DISTINCT ?what ?ql_matchingword_t_nann WHERE {
  ?what a crm:E21_Person ;
      crm:P2_has_type ?type ;
      crm:P98i_was_born ?birth .
  ?birth crm:P4_has_time-span ?ts .
  ?ts crm:P82a_begin_of_the_begin ?tsb .
  FILTER (?tsb < "1900-01-01T00:00:00Z"^^xsd:dateTime)
  ?type crm:P1_is_identified_by ?name .
  ?name crm:P190_has_symbolic_content ?txt .
  ?t ql:contains-word "Nann*" ; ql:contains-entity ?txt .
  } LIMIT 1000

PREFIX lux: <https://lux.collections.yale.edu/ns/>
SELECT DISTINCT ?what ?score WHERE {
	?what a lux:Item ; lux:recordText ?ft ; lux:itemPrimaryName ?name .
    ?t ql:contains-word "dort" ; ql:contains-entity ?ft .
	OPTIONAL { ?t2 ql:contains-word "dort" ; ql:contains-entity ?name . }
	BIND(?ql_score_word_t_dort + (?ql_score_word_t2_dort*4) AS ?score)
} ORDER BY DESC(?score) LIMIT 25

PREFIX textSearch: <https://qlever.cs.uni-freiburg.de/textSearch/>
PREFIX lux: <https://lux.collections.yale.edu/ns/>
SELECT DISTINCT ?what ?score WHERE {
	?what a lux:Work ; lux:recordText ?ft ; lux:workPrimaryName ?name ; ?foo ?who .
	?who lux:agentName ?name2 .

    ?t ql:contains-word "froissart" ; ql:contains-entity ?ft .
	?t2 ql:contains-word "froissart" ; ql:contains-entity ?name .
	?t3 ql:contains-word "robinson" ; ql:contains-entity ?name2 .

	BIND(?ql_score_word_t_froissart + (?ql_score_word_t2_froissart*4) + (?ql_score_word_t3_robinson*2) AS ?score)
} ORDER BY DESC(?score)


### This should work replacing aoWB with agentAny
PREFIX lux: <https://lux.collections.yale.edu/ns/>
SELECT ?work ?score  WHERE {
{
  {
    ?work a lux:Work ; lux:agentOfWorkBeginning/lux:agentPrimaryName ?nm .
    ?t ql:contains-word "robinson" ; ql:contains-entity ?nm .
    BIND (?ql_score_word_t_robinson*2 as ?score1)
  } OPTIONAL {
    ?work a lux:Work ; lux:recordText ?ft .
    ?t2 ql:contains-word "robinson" ; ql:contains-entity ?ft .
    BIND (?ql_score_word_t2_robinson as ?score2)
  }
  BIND (?score1 + ?score2 as ?score)
} UNION {
  {
    ?work a lux:Work ; lux:recordText ?ft .
    ?t2 ql:contains-word "robinson" ; ql:contains-entity ?ft .
    BIND (?ql_score_word_t2_robinson as ?score2)
  } OPTIONAL {
    ?work a lux:Work ; lux:agentOfWorkBeginning/lux:agentPrimaryName ?nm .
    ?t ql:contains-word "robinson" ; ql:contains-entity ?nm .
    BIND (?ql_score_word_t_robinson*2 as ?score1)
  }
  BIND (?score1 + ?score2 as ?score)
}
} ORDER BY DESC(?score)



SELECT ?p (COUNT(?p) AS ?ct) WHERE {
?s ?p ?o .
} GROUP BY (?p) ORDER BY DESC (?ct)

"""


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

    def sanitize_string(self, string):
        if not string:
            return ""
        string = string.replace("\r", " ")
        string = string.replace("\n", " ")
        string = string.replace("\t", " ")
        string = string.replace('"', "")
        string = string.replace("\\", "")
        return string

    def do_bs_html(self, content):
        content = content.strip()
        if content.startswith("<"):
            soup = BeautifulSoup(content, features="lxml")
            clncont = soup.get_text()
            return clncont
        return content

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
        anyt = {"subject": me, "predicate": f"{self.luxns}{pfx}Any", "object": ""}
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
            val = self.sanitize_string(idb["content"])
            recordText.append(val)
            lt["value"] = val
            if idb["type"] == "Name":
                # primaryName
                cxns = [x.get("id", None) for x in idb.get("classified_as", [])]
                if self.primaryName in cxns:
                    lt["predicate"] = f"{self.luxns}{pfx}PrimaryName"
                    triples.append(self.literal_pattern.format(**lt))
                    lt["predicate"] = f"{self.luxns}primaryName"
                    triples.append(self.literal_pattern.format(**lt))
                elif self.sortName in cxns:
                    lt["predicate"] = f"{self.luxns}{pfx}SortName"
                    triples.append(self.literal_pattern.format(**lt))
                    continue
                lt["predicate"] = f"{self.luxns}{pfx}Name"
                triples.append(self.literal_pattern.format(**lt))
                lt["predicate"] = f"{self.luxns}name"
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
                ct = self.sanitize_string(ct)
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
        rep = data.get("representation", None)
        if rep:
            rep = rep[0].get("digitally_shown_by", None)
            if rep:
                rep = rep[0].get("access_point", None)
                if rep:
                    rep = rep[0].get("id", None)
                    if rep:
                        lt["predicate"] = f"{self.luxns}{pfx}HasDigitalImage"
                        lt["value"] = 1
                        lt["datatype"] = self.number_type
                        triples.append(self.literal_pattern.format(**lt))

        # all classifications
        # agentClassification, workClassification (etc)
        t["predicate"] = f"{self.luxns}{pfx}Classification"
        for cls in data.get("classified_as", []):
            if "id" in cls:
                t["object"] = cls["id"]
                triples.append(self.triple_pattern.format(**t))
                anyt["object"] = cls["id"]
                triples.append(self.triple_pattern.format(**anyt))

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
                            anyt["object"] = who["id"]
                            triples.append(self.triple_pattern.format(**anyt))

                    wheres = bit.get("took_place_at", [])
                    t["predicate"] = f"{self.luxns}placeOf{pcls}"
                    for where in wheres:
                        if "id" in where:
                            t["object"] = where["id"]
                            triples.append(self.triple_pattern.format(**t))
                            anyt["object"] = where["id"]
                            triples.append(self.triple_pattern.format(**anyt))

                    types = bit.get("classified_as", [])
                    types.extend(bit.get("technique", []))
                    t["predicate"] = f"{self.luxns}typeOf{pcls}"
                    for typ in types:
                        if "id" in typ:
                            t["object"] = typ["id"]
                            triples.append(self.triple_pattern.format(**t))
                            anyt["object"] = typ["id"]
                            triples.append(self.triple_pattern.format(**anyt))
                    causes = bit.get("caused_by", [])
                    t["predicate"] = f"{self.luxns}causeOf{pcls}"
                    for cause in causes:
                        if "id" in cause:
                            t["object"] = cause["id"]
                            triples.append(self.triple_pattern.format(**t))
                            anyt["object"] = cause["id"]
                            triples.append(self.triple_pattern.format(**anyt))
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
                            anyt["object"] = inf["id"]
                            triples.append(self.triple_pattern.format(**anyt))
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
        for member in data.get("member_of", []):
            if "id" in member:
                t["object"] = member["id"]
                t["predicate"] = f"{self.luxns}{pfx}MemberOf{member['type']}"
                triples.append(self.triple_pattern.format(**t))
                anyt["object"] = member["id"]
                triples.append(self.triple_pattern.format(**anyt))

        # partOf
        parents = data.get("part_of", [])
        for parent in parents:
            if "id" in parent:
                t["predicate"] = f"{self.luxns}{pfx}PartOf"
                t["object"] = parent["id"]
                triples.append(self.triple_pattern.format(**t))
                anyt["object"] = parent["id"]
                triples.append(self.triple_pattern.format(**anyt))

        # Class Specific relationships
        if pfx in ["work", "set"]:
            # Language
            langs = data.get("language", [])
            t["predicate"] = f"{self.luxns}{pfx}Language"
            for lang in langs:
                if "id" in lang:
                    t["object"] = lang["id"]
                    triples.append(self.triple_pattern.format(**t))
                    anyt["object"] = lang["id"]
                    triples.append(self.triple_pattern.format(**anyt))

            # Subjects
            abouts = data.get("about", [])
            abouts.extend(data.get("depicts", []))
            for about in abouts:
                if "id" in about:
                    t["object"] = about["id"]
                    abpfx = self.get_prefix(about)
                    t["predicate"] = f"{self.luxns}{pfx}About{abpfx.title()}"
                    triples.append(self.triple_pattern.format(**t))
                    anyt["object"] = about["id"]
                    triples.append(self.triple_pattern.format(**anyt))

        elif pfx == "item":
            # carries/shows
            carries = data.get("carries", [])
            carries.extend(data.get("shows", []))
            for c in carries:
                if "id" in c:
                    t["object"] = c["id"]
                    t["predicate"] = f"{self.luxns}carries"
                    triples.append(self.triple_pattern.format(**t))
                    anyt["object"] = c["id"]
                    triples.append(self.triple_pattern.format(**anyt))

            # materials
            mats = data.get("made_of", [])
            t["predicate"] = f"{self.luxns}material"
            for mat in mats:
                if "id" in mat:
                    t["object"] = mat["id"]
                    triples.append(self.triple_pattern.format(**t))
                    anyt["object"] = mat["id"]
                    triples.append(self.triple_pattern.format(**anyt))

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
                    metas = [x["id"] for x in cxn.get("classified_as", []) if "id" in x]
                    if self.nationality in metas:
                        t["predicate"] = f"{self.luxns}nationality"
                    elif self.occupation in metas:
                        t["predicate"] = f"{self.luxns}occupation"
                    elif self.gender in metas:
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
                    anyt["object"] = b["id"]
                    triples.append(self.triple_pattern.format(**anyt))

        elif pfx == "place":
            pass
        elif pfx == "event":
            whos = data.get("carried_out_by", [])
            t["predicate"] = f"{self.luxns}agentOfEvent"
            for who in whos:
                if "id" in who:
                    t["object"] = who["id"]
                    triples.append(self.triple_pattern.format(**t))
                    anyt["object"] = who["id"]
                    triples.append(self.triple_pattern.format(**anyt))
            wheres = data.get("took_place_at", [])
            t["predicate"] = f"{self.luxns}placeOfEvent"
            for where in wheres:
                if "id" in where:
                    t["object"] = where["id"]
                    triples.append(self.triple_pattern.format(**t))
                    anyt["object"] = where["id"]
                    triples.append(self.triple_pattern.format(**anyt))
            causes = data.get("caused_by", [])
            t["predicate"] = f"{self.luxns}causeOfEvent"
            for cause in causes:
                if "id" in cause:
                    t["object"] = cause["id"]
                    triples.append(self.triple_pattern.format(**t))
                    anyt["object"] = cause["id"]
                    triples.append(self.triple_pattern.format(**anyt))
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
                    anyt["object"] = inf["id"]
                    triples.append(self.triple_pattern.format(**anyt))
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
                    anyt["object"] = c["carried_out_by"]["id"]
                    triples.append(self.triple_pattern.format(**anyt))

        else:
            raise ValueError(f"Unsupported prefix: {pfx}")

        # add in recordText
        rtxt = " ".join([x for x in recordText if x])
        lt["predicate"] = f"{self.luxns}recordText"
        lt["value"] = rtxt.replace("\n", " ").replace("\r", " ").replace('"', "")
        lt["datatype"] = ""
        triples.append(self.literal_pattern.format(**lt))

        return triples
