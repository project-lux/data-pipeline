from rdflib import URIRef, Literal
from pipeline.process.base.mapper import Mapper
from bs4 import BeautifulSoup

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





SELECT ?p (COUNT(?p) AS ?ct) WHERE {
?s ?p ?o .
} GROUP BY (?p) ORDER BY DESC (?ct)


"""


class QleverMapper(Mapper):
    def __init__(self, config):
        Mapper.__init__(self, config)
        self.configs = config["all_configs"]
        self.luxns = "https://lux.collections.yale.edu/ns/"
        self.crmns = "http://www.cidoc-crm.org/cidoc-crm/"
        self.lans = "https://linked.art/ns/terms/"
        self.skosns = "http://www.w3.org/2004/02/skos/core#"
        self.rdfns = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        self.scins = "http://www.ics.forth.gr/isl/CRMsci/"
        self.digns = "http://www.ics.forth.gr/isl/CRMdig/"
        self.dcns = "http://purl.org/dc/elements/1.1/"
        self.dctns = "http://purl.org/dc/terms/"
        self.datans = "https://lux.collections.yale.edu/data/"

        self.triple_pattern = "<{subject}> <{predicate}> <{object}> ."
        self.literal_pattern = "<{subject}> <{predicate}> {value}{datatype} ."
        self.number_type = "^^<http://www.w3.org/2001/XMLSchema#decimal>"
        self.date_type = "^^<http://www.w3.org/2001/XMLSchema#dateTime>"

        self.globals = self.configs.globals
        self.ignore_props_for_refs = [
            "identified_by",
            "referred_to_by",
            "equivalent",
            "subject_of",
            "attributed_by",
            "contact_point",
            "defined_by",
            "shown_by",
            "carried_by",
            "digitally_carried_by",
            "digitally_shown_by",
            "approximated_by",
            "dimension",
            "access_point",
            "digitally_carries",
            "subject_to",
            "assigned_by",
            "exemplary_member_of",
        ]

        self.type_map = {
            "HumanMadeObject": f"{self.crmns}E22_Human-Made_Object",
            "DigitalObject": f"{self.digns}D1_Digital_Object",
            "VisualItem": f"{self.crmns}E36_Visual_Item",
            "LinguisticObject": f"{self.crmns}E33_Linguistic_Object",
            "Set": f"{self.lans}Set",
            "Person": f"{self.crmns}E21_Person",
            "Group": f"{self.crmns}E74_Group",
            "Place": f"{self.crmns}E53_Place",
            "Type": f"{self.crmns}E55_Type",
            "Material": f"{self.crmns}E57_Material",
            "Language": f"{self.crmns}E56_Language",
            "Unit": f"{self.crmns}E58_Measurement_Unit",
            "MeasurementUnit": f"{self.crmns}E58_Measurement_Unit",
            "Currency": f"{self.crmns}E98_Currency",
            "Event": f"{self.crmns}E5_Event",
            "Activity": f"{self.crmns}E7_Activity",
            "Period": f"{self.crmns}E4_Period",
            "Production": f"{self.crmns}E12_Production",
            "AttributeAssignment": f"{self.crmns}E13_Attribute_Assignment",
            "Right": f"{self.crmns}E30_Right",
            "Identifier": f"{self.crmns}E42_Identifier",
            "TimeSpan": f"{self.crmns}E52_Time-Span",
            "Dimension": f"{self.crmns}E54_Dimension",
            "Creation": f"{self.crmns}E65_Creation",
            "Formation": f"{self.crmns}E66_Formation",
            "Birth": f"{self.crmns}E67_Birth",
            "Dissolution": f"{self.crmns}E68_Dissolution",
            "Death": f"{self.crmns}E69_Death",
            "InformationObject": f"{self.crmns}E73_Information_Object",
            "Name": f"{self.crmns}E33_E41_Linguistic_Appellation",
            "DigitalService": f"{self.lans}DigitalService",
            "Encounter": f"{self.scins}S19_Encounter",
        }

        self.prop_map = {
            "identified_by": f"{self.crmns}P1_is_identified_by",
            "classified_as": f"{self.crmns}P2_has_type",
            "timespan": f"{self.crmns}P4_has_time-span",
            "took_place_at": f"{self.crmns}P7_took_place_at",
            "carried_out_by": f"{self.crmns}P14_carried_out_by",
            "carried_out": f"{self.crmns}P14i_performed",
            "influenced_by": f"{self.crmns}P15_was_influenced_by",
            "used_specific_object": f"{self.crmns}P16_used_specific_object",
            "used_for": f"{self.crmns}P16i_was_used_for",
            "type": f"{self.rdfns}type",
            "referred_to_by": f"{self.crmns}P67i_is_referred_to_by",
            "equivalent": f"{self.lans}equivalent",
            "representation": f"{self.crmns}P138i_has_representation",
            "subject_of": f"{self.crmns}P129i_is_subject_of",
            "attributed_by": f"{self.crmns}P140i_was_attributed_by",
            "broader": f"{self.skosns}broader",
            "created_by": f"{self.crmns}P94i_was_created_by",
            "technique": f"{self.crmns}P32_used_general_technique",
            "motivated_by": f"{self.crmns}P17_was_motivated_by",
            "about": f"{self.crmns}P129_is_about",
            "assigned": f"{self.crmns}P141_assigned",
            "language": f"{self.crmns}P72_has_language",
            "unit": f"{self.crmns}P91_has_unit",
            "made_of": f"{self.crmns}P45_consists_of",
            "dimension": f"{self.crmns}P43_has_dimension",
            "format": f"{self.dcns}format",
            "conforms_to": f"{self.dctns}conformsTo",
            "digitally_carries": f"{self.lans}digitally_carries",
            "digitally_shows": f"{self.lans}digitally_shows",
            "digitally_available_via": f"{self.lans}digitally_available_via",
            "access_point": f"{self.lans}access_point",
            "caused_by": f"{self.scins}O13i_is_triggered_by",
            "participant": f"{self.crmns}P11_had_participant",
            "participated_in": f"{self.crmns}P11i_participated_in",
            "contact_point": f"{self.crmns}P76_has_contact_point",
            "residence": f"{self.crmns}P74_has_current_or_former_residence",
            "formed_by": f"{self.crmns}P95i_was_formed_by",
            "dissolved_by": f"{self.crmns}P99i_was_dissolved_by",
            "born": f"{self.crmns}P98i_was_born",
            "died": f"{self.crmns}P100i_died_in",
            "current_owner": f"{self.crmns}P52_has_current_owner",
            "current_custodian": f"{self.crmns}P50_has_current_keeper",
            "current_location": f"{self.crmns}P55_has_current_location",
            "carries": f"{self.crmns}P128_carries",
            "shows": f"{self.crmns}P65_shows_visual_item",
            "produced_by": f"{self.crmns}P108i_was_produced_by",
            "destroyed_by": f"{self.crmns}P13i_was_destroyed_by",
            "encountered_by": f"{self.scins}O19i_was_object_encountered_at",
            "defined_by": f"{self.crmns}P168_place_is_defined_by",
            "members_exemplified_by": f"{self.lans}members_exemplified_by",
            "content": f"{self.crmns}P190_has_symbolic_content",
            "represents": f"{self.crmns}P138_represents",
            "represents_instance_of_type": f"{self.crmns}P199_represents_instance_of_type",
            "digitally_shown_by": f"{self.lans}digitally_shown_by",
            "digitally_carried_by": f"{self.lans}digitally_carried_by",
            "assigned_by": f"{self.crmns}P141i_was_assigned_by",
            "begin_of_the_begin": f"{self.crmns}P82a_begin_of_the_begin",
            "begin_of_the_end": f"{self.crmns}crm:P81b_begin_of_the_end",
            "end_of_the_end": f"{self.crmns}P82b_end_of_the_end",
            "end_of_the_begin": f"{self.crmns}P81a_end_of_the_begin",
            "assigned_property": f"{self.crmns}P177_assigned_property_of_type",
            "value": f"{self.crmns}P90_has_value",
            "subject_to": f"{self.crmns}P104_is_subject_to",
            "occurs_during": f"{self.crmns}P10_during",
            "used_object_of_type": f"{self.crmns}P125_used_object_of_type",
        }

    def walk_for_triples(self, node, conf, ignore=False):
        if not "id" in node:
            me = f"{conf['base']}_{conf['bid']}"
            conf["bid"] += 1
        else:
            me = node["id"]
            if me != conf["base"] and me.startswith(self.datans):
                # Triples will come from its own record, including metatypes
                if not ignore:
                    conf["refs"][me] = 1
                return me
            else:
                if not me.startswith(self.datans):
                    # sanitize external links
                    me = me.replace(" ", "%20")
                    me = me.replace("\n", "")
                    me = me.replace("\t", "")
                    me = me.replace("\r", "")
                    me = me.replace('"', "")
                    me = me.replace("{", "%7B")
                    me = me.replace("}", "%7D")

        try:
            uri_me = URIRef(me).n3()
        except Exception as e:
            print(f"Failed to build URI from {me}")
            print(e)
            return None

        for k, v in node.items():
            if k in ["id", "_label", "@context"]:
                continue
            pred = self.prop_map.get(k, None)
            if pred is None:
                if k in ["part", "part_of"]:
                    if "type" in node:
                        mytype = node["type"]
                        if mytype in [
                            "LinguisticObject",
                            "VisualItem",
                            "DigitalObject",
                            "Name",
                            "Identifier",
                            "InformationObject",
                        ]:
                            if k == "part":
                                pred = f"{self.crmns}P106_is_composed_of"
                            else:
                                pred = f"{self.crmns}P106i_forms_part_of"
                        elif mytype in [
                            "Production",
                            "Creation",
                            "Formation",
                            "Dissolution",
                            "Event",
                            "Activity",
                            "Period",
                            "AttributeAssignment",
                        ]:
                            if k == "part":
                                pred = f"{self.crmns}P9_consists_of"
                            else:
                                pred = f"{self.crmns}P9i_forms_part_of"
                        elif mytype == "HumanMadeObject":
                            if k == "part":
                                pred = f"{self.crmns}P46_is_composed_of"
                            else:
                                pred = f"{self.crmns}P46i_forms_part_of"
                        elif mytype == "Place":
                            if k == "part":
                                pred = f"{self.crmns}P89i_contains"
                            else:
                                pred = f"{self.crmns}P89_falls_within"
                        elif mytype in ["Type", "Currency", "MeasurementUnit", "Material", "Language"]:
                            if k == "part":
                                pred = f"{self.skosns}broader"
                            else:
                                pred = f"{self.skosns}narrower"
                        else:
                            print(f"Saw {mytype} as class for node with {k} property")
                            pred = f"{self.luxns}{k}"
                    else:
                        print(f"Saw {node} with {k} but no type?")
                        pred = f"{self.luxns}{k}"
                elif k == "member_of":
                    if v and type(v) == list and type(v[0]) == dict and "type" in v[0]:
                        objtype = v[0]["type"]
                        if objtype == "Set":
                            pred = f"{self.lans}member_of"
                        elif objtype == "Group":
                            pred = f"{self.crmns}P107i_is_current_or_former_member_of"
                        else:
                            print(f"Saw {objtype} as class of object of 'member_of'")
                            pred = f"{self.lans}member_of"
                    elif "type" in node and node["type"] in ["Person", "Group"]:
                        pred = f"{self.crmns}P107i_is_current_or_former_member_of"
                    else:
                        print(f"Saw {v} as value for member_of")
                        pred = f"{self.lans}member_of"
                else:
                    print(f"Failed to process property: {k} in {me}")
                    continue

            t = {"subject": me, "predicate": pred}
            ignore_now = ignore
            if not ignore:
                ignore = k in self.ignore_props_for_refs

            if not type(v) in [list, dict]:
                # process a value
                if k in ["content", "format", "defined_by"]:
                    value = v.replace("\t", "\\t")
                    value = value.replace("\n", "\\n")
                    value = value.replace("\r", "\\r")
                    t["datatype"] = ""
                    try:
                        nvalue = Literal(value).n3()
                        t["value"] = nvalue
                    except Exception as e:
                        print(f"Failed to process literal {v} in {me}")
                        print(e)
                        continue
                    if k == "content":
                        conf["recordText"].append(value)
                elif k == "value":
                    # t['datatype'] = self.number_type
                    t["datatype"] = ""
                    t["value"] = str(v)
                elif k in ["begin_of_the_begin", "end_of_the_end", "begin_of_the_end", "end_of_the_begin"]:
                    t["datatype"] = self.date_type
                    t["value"] = f'"{v}"'
                elif k == "type":
                    t["object"] = self.type_map[v]
                    conf["triples"].append(self.triple_pattern.format(**t))
                    continue
                elif k == "access_point":
                    # magic @vocab props
                    t["object"] = v
                    conf["triples"].append(self.triple_pattern.format(**t))
                    continue
                elif k == "assigned_property":
                    po = self.prop_map.get(v, None)
                    if pos is None:
                        print(f"Could not calculate expansion of {v} in {k}")
                    else:
                        t["object"] = pos
                        conf["triples"].append(self.triple_pattern.format(**t))
                    continue
                else:
                    print(f"Unhandled literal value type: {k} / {pred}")
                    continue
                conf["triples"].append(self.literal_pattern.format(**t))
            elif type(v) == list:
                for vi in v:
                    if type(vi) == dict:
                        obj = self.walk_for_triples(vi, conf, ignore)
                        if obj is not None:
                            t["object"] = obj
                            conf["triples"].append(self.triple_pattern.format(**t))
                    else:
                        print(f"found non dict in a list :( {node}")
            elif type(v) == dict:
                obj = self.walk_for_triples(v, conf, ignore)
                if obj is not None:
                    t["object"] = obj
                    conf["triples"].append(self.triple_pattern.format(**t))
            ignore = ignore_now

        return me

    def do_bs_html(self, part):
        content = part.get("content", "")
        content = content.strip()
        if content.startswith("<"):
            soup = BeautifulSoup(content, features="lxml")
            clncont = soup.get_text()
            if clncont == "":
                pass
            else:
                part["content"] = clncont

    def get_prefix(self, which):
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
        # QLever needs NT / TTL format
        # Returns a list of triples

        data = record["data"]
        me = data["id"]

        # strip html from content (at least for now)
        for part in data.get("referred_to_by", []):
            self.do_bs_html(part)
        for rep in data.get("representation", []):
            for dsb in rep.get("digitally_shown_by", []):
                for ref in dsb.get("referred_to_by", []):
                    self.do_bs_html(ref)

        # Where do we generally live
        cxns = [x["id"] for x in data.get("classified_as", []) if "id" in x]
        if (self.globals["archives"] in cxns) and data["type"] == "Set":
            pfx = "work"
        else:
            pfx = self.get_prefix(data["type"])

        triples = []
        conf = {"triples": triples, "base": me, "bid": 0, "recordText": [], "refs": {}}
        self.walk_for_triples(data, conf)

        if cxns:
            cxPred = f"{self.luxns}{pfx}ClassifiedAs"
            for c in cxns:
                t = {"subject": me, "predicate": cxPred, "object": c}
                triples.append(self.triple_pattern.format(**t))

        if conf["recordText"]:
            value = " ".join(conf["recordText"])
            try:
                nvalue = Literal(value).n3()
                t = {"subject": me, "predicate": f"{self.luxns}recordText", "value": nvalue, "datatype": ""}
                triples.append(self.literal_pattern.format(**t))
            except Exception as e:
                print(f"Failed to build full record text for {me}: {e}")

        shortcuts = {
            "produced_by": "Production",
            "created_by": "Creation",
            "born": "Beginning",
            "died": "Ending",
            "formed_by": "Beginning",
            "dissolved_by": "Ending",
            "used_for": "Publication",
            "encountered_by": "Encounter",
            "carried_out": "Activity",
        }

        for prop, predClass in shortcuts.items():
            if prop in data:
                node = data[prop]
                apred = f"agentOf{predClass}"
                ppred = f"placeOf{predClass}"
                tpred = f"techniqueOf{predClass}"
                cpred = f"causeOf{predClass}"
                ipred = f"influenced{predClass}"
                agents = []
                places = []
                techs = []
                causes = []
                influences = []

                if type(node) == dict:
                    node = [node]

                for n in node:
                    if "carried_out_by" in n:
                        agents.extend([x["id"] for x in n["carried_out_by"] if "id" in x])
                    if "took_place_at" in n:
                        places.extend([x["id"] for x in n["took_place_at"] if "id" in x])
                    if "technique" in n:
                        techs.extend([x["id"] for x in n["technique"] if "id" in x])
                    if "caused_by" in n:
                        causes.extend([x["id"] for x in n["caused_by"] if "id" in x])
                    if "influenced_by" in n:
                        influences.extend([x["id"] for x in n["influenced_by"] if "id" in x])
                    if "part" in n:
                        for p in n["part"]:
                            if "carried_out_by" in p:
                                agents.extend([x["id"] for x in p["carried_out_by"] if "id" in x])
                            if "took_place_at" in p:
                                places.extend([x["id"] for x in p["took_place_at"] if "id" in x])
                            if "technique" in p:
                                techs.extend([x["id"] for x in p["technique"] if "id" in x])
                            if "influenced_by" in p:
                                influences.extend([x["id"] for x in p["influenced_by"] if "id" in x])
                            if "attributed_by" in p:
                                for aa in p["attributed_by"]:
                                    if "assigned" in aa:
                                        for p2 in aa["assigned"]:
                                            if "carried_out_by" in p2:
                                                agents.extend([x["id"] for x in p2["carried_out_by"] if "id" in x])
                                            if "took_place_at" in p2:
                                                places.extend([x["id"] for x in p2["took_place_at"] if "id" in x])
                                            if "technique" in p2:
                                                techs.extend([x["id"] for x in p2["technique"] if "id" in x])

                    if "attributed_by" in n:
                        for p in n["attributed_by"]:
                            if "assigned" in p:
                                for p2 in p["assigned"]:
                                    if "carried_out_by" in p2:
                                        agents.extend([x["id"] for x in p2["carried_out_by"] if "id" in x])
                                    if "took_place_at" in p2:
                                        places.extend([x["id"] for x in p2["took_place_at"] if "id" in x])
                                    if "technique" in p2:
                                        techs.extend([x["id"] for x in p2["technique"] if "id" in x])

                for a in agents:
                    t = {"subject": me, "predicate": f"{self.luxns}{apred}", "object": a}
                    triples.append(self.triple_pattern.format(**t))
                for p in places:
                    t = {"subject": me, "predicate": f"{self.luxns}{ppred}", "object": p}
                    triples.append(self.triple_pattern.format(**t))
                for tt in techs:
                    t = {"subject": me, "predicate": f"{self.luxns}{tpred}", "object": tt}
                    triples.append(self.triple_pattern.format(**t))
                for c in causes:
                    t = {"subject": me, "predicate": f"{self.luxns}{cpred}", "object": c}
                    triples.append(self.triple_pattern.format(**t))
                for i in influences:
                    t = {"subject": me, "predicate": f"{self.luxns}{ipred}", "object": i}
                    triples.append(self.triple_pattern.format(**t))

        # Pre-construct other useful triples

        # Make primary name its own triple
        if "identified_by" in data:
            for nm in data["identified_by"]:
                nmcxns = [y["id"] for y in nm.get("classified_as", []) if "id" in y]
                if (
                    "type" in nm
                    and nm["type"] == "Name"
                    and self.globals["primaryName"] in nmcxns
                    and "content" in nm
                ):
                    t = {"subject": me, "predicate": f"{self.luxns}primaryName", "value": "", "datatype": ""}
                    v = nm["content"]
                    value = v.replace("\t", "\\t")
                    value = value.replace("\n", "\\n")
                    value = value.replace("\r", "\\r")
                    try:
                        nvalue = Literal(value).n3()
                        t["value"] = nvalue
                    except Exception as e:
                        print(f"Failed to process literal {v} in {data['identifier']}")
                        print(e)
                        continue
                    triples.append(self.literal_pattern.format(**t))

        # {pfx}Any
        for ref in conf["refs"].keys():
            t = {"subject": me, "predicate": f"{self.luxns}{pfx}Any", "object": ref}
            triples.append(self.triple_pattern.format(**t))

        if pfx == "agent":
            natls = [
                x["id"]
                for x in data.get("classified_as", [])
                if self.globals["nationality"] in [y["id"] for y in x.get("classified_as", [])] and "id" in x
            ]
            for f in natls:
                t = {"subject": me, "predicate": f"{self.luxns}agentNationality", "object": f}
                triples.append(self.triple_pattern.format(**t))

            occs = [
                x["id"]
                for x in data.get("classified_as", [])
                if self.globals["occupation"] in [y["id"] for y in x.get("classified_as", [])] and "id" in x
            ]
            for f in occs:
                t = {"subject": me, "predicate": f"{self.luxns}agentOccupation", "object": f}
                triples.append(self.triple_pattern.format(**t))
        elif data["type"] in ["LinguisticObject", "Set", "VisualItem"]:
            if "about" in data:
                for a in data["about"]:
                    if "id" in a:
                        t = {"subject": me, "predicate": f"{self.luxns}about_or_depicts", "object": a["id"]}
                        triples.append(self.triple_pattern.format(**t))
                        # add target specific triples
                        if "type" in a:
                            typ = self.get_prefix(a["type"])
                            t = {"subject": me, "predicate": f"{self.luxns}about_or_depicts_{typ}", "object": a["id"]}
                            triples.append(self.triple_pattern.format(**t))
        elif pfx == "event":
            # Add triples for carried_out_by, took_place_at of record, vs production etc
            if "carried_out_by" in data:
                ags = [x["id"] for x in data.get("carried_out_by", []) if "id" in x]
                aPred = f"{self.luxns}{pfx}CarriedOutBy"
                for a in ags:
                    t = {"subject": me, "predicate": aPred, "object": a}
                    triples.append(self.triple_pattern.format(**t))
            if "took_place_at" in data:
                places = [x["id"] for x in data.get("took_place_at", []) if "id" in x]
                pPred = f"{self.luxns}{pfx}TookPlaceAt"
                for p in places:
                    t = {"subject": me, "predicate": pPred, "object": p}
                    triples.append(self.triple_pattern.format(**t))

        elif pfx == "concept":
            if "created_by" in data and "influenced_by" in data["created_by"]:
                for inf in data["created_by"]["influenced_by"]:
                    if "id" in inf and "type" in inf:
                        typ = self.get_prefix(inf["type"])
                        t = {"subject": me, "predicate": f"{self.luxns}influenced_by_{typ}", "object": inf["id"]}
                        triples.append(self.triple_pattern.format(**t))

        if data["type"] == "Person":
            # Do person indexes here
            genders = [
                x["id"]
                for x in data.get("classified_as", [])
                if self.globals["gender"] in [y["id"] for y in x.get("classified_as", [])] and "id" in x
            ]
            for f in genders:
                t = {"subject": me, "predicate": f"{self.luxns}agentGender", "object": f}
                triples.append(self.triple_pattern.format(**t))

            if "born" in data and "timespan" in data["born"]:
                bd = data["born"]["timespan"].get("begin_of_the_begin", "")
                if bd:
                    t = {
                        "subject": me,
                        "predicate": f"{self.luxns}agentBeginDate",
                        "value": f'"{bd}"',
                        "datatype": self.date_type,
                    }
                    triples.append(self.literal_pattern.format(**t))
            if "died" in data and "timespan" in data["died"]:
                dd = data["died"]["timespan"].get("end_of_the_end", "")
                if dd:
                    t = {
                        "subject": me,
                        "predicate": f"{self.luxns}agentEndDate",
                        "value": f'"{dd}"',
                        "datatype": self.date_type,
                    }
                    triples.append(self.literal_pattern.format(**t))

        elif data["type"] == "Group":
            if "formed_by" in data and "timespan" in data["formed_by"]:
                bd = data["formed_by"]["timespan"].get("begin_of_the_begin", "")
                if bd:
                    t = {
                        "subject": me,
                        "predicate": f"{self.luxns}agentBeginDate",
                        "value": f'"{bd}"',
                        "datatype": self.date_type,
                    }
                    triples.append(self.literal_pattern.format(**t))
            if "dissolved_by" in data and "timespan" in data["dissolved_by"]:
                dd = data["dissolved_by"]["timespan"].get("end_of_the_end", "")
                if dd:
                    t = {
                        "subject": me,
                        "predicate": f"{self.luxns}agentEndDate",
                        "value": f'"{dd}"',
                        "datatype": self.date_type,
                    }
                    triples.append(self.literal_pattern.format(**t))

        elif data["type"] == "Set":
            if "used_for" in data:
                for uf in data["used_for"]:
                    if "classified_as" in uf and "carried_out_by" in uf:
                        for c in uf["classified_as"]:
                            if "id" in c and c["id"] == self.globals["curation"]:
                                for who in uf["carried_out_by"]:
                                    if "id" in who:
                                        t = {
                                            "subject": me,
                                            "predicate": f"{self.luxns}agentOfCuration",
                                            "object": who["id"],
                                        }
                                        triples.append(self.triple_pattern.format(**t))

        elif data["type"] == "HumanMadeObject":
            if "carries" in data:
                for c in data["carries"]:
                    if "id" in c:
                        t = {"subject": me, "predicate": f"{self.luxns}carries_or_shows", "object": c["id"]}
                        triples.append(self.triple_pattern.format(**t))
            if "shows" in data:
                for s in data["shows"]:
                    if "id" in s:
                        t = {"subject": me, "predicate": f"{self.luxns}carries_or_shows", "object": s["id"]}
                        triples.append(self.triple_pattern.format(**t))

        elif data["type"] == "DigitalObject":
            if "digitally_carries" in data:
                for c in data["digitally_carries"]:
                    if "id" in c:
                        t = {"subject": me, "predicate": f"{self.luxns}carries_or_shows", "object": c["id"]}
                        triples.append(self.triple_pattern.format(**t))
            if "digitally_shows" in data:
                for s in data["digitally_shows"]:
                    if "id" in s:
                        t = {"subject": me, "predicate": f"{self.luxns}carries_or_shows", "object": s["id"]}
                        triples.append(self.triple_pattern.format(**t))

        elif data["type"] == "VisualItem":
            if "represents" in data:
                for r in data["represents"]:
                    if "id" in r:
                        t = {"subject": me, "predicate": f"{self.luxns}about_or_depicts", "object": r["id"]}
                        triples.append(self.triple_pattern.format(**t))

                        if "type" in r:
                            typ = self.get_prefix(r["type"])
                            t = {"subject": me, "predicate": f"{self.luxns}about_or_depicts_{typ}", "object": r["id"]}
                            triples.append(self.triple_pattern.format(**t))

        return triples
