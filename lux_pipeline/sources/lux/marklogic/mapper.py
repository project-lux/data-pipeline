import os
import sys
import uuid
import copy
import datetime
from ctypes import c_int32
from lux_pipeline.process.base.mapper import Mapper
import ujson as json
import numpy as np
from bs4 import BeautifulSoup
import re


class MlMapper(Mapper):
    def __init__(self, config):
        Mapper.__init__(self, config)
        self.start_str = datetime.datetime.now().isoformat()
        self.configs = config["all_configs"]
        self.globals = self.configs.globals
        self.ignore_props = [
            "identified_by",
            "referred_to_by",
            "equivalent",
            "access_point",
            "dimension",
            "subject_of",
            "attributed_by",
            "used_specific_object",
            "contact_point",
            "defined_by",
            "shown_by",
            "carried_by",
            "digitally_carried_by",
            "digitally_shown_by",
            "approximated_by",
            "language",
            "technique",
            "occurs_during",
            "digitally_carries",
            "subject_to",
            "assigned_by",
            "exemplary_member_of",
        ]
        self.external_types = {}

        self.ref_ctr_excludes = set([x[-36:] for x in list(self.configs.globals.values()) if x])
        non_global_externals = [
            'http://vocab.getty.edu/aat/300264578##quaType',
            'http://vocab.getty.edu/aat/300435704##quaType',
            'http://id.loc.gov/authorities/names/n80008747##quaGroup',
            'http://vocab.getty.edu/aat/300133046##quaType',
            'http://vocab.getty.edu/aat/300025883##quaType',
            'http://vocab.getty.edu/aat/300435452##quaType',
            'http://vocab.getty.edu/aat/300435430##quaType',
            'http://vocab.getty.edu/aat/300202362##quaType',
            'http://vocab.getty.edu/aat/300311706##quaType',
            'http://vocab.getty.edu/aat/300404621##quaType',
            'http://vocab.getty.edu/aat/300055665##quaType',
            'http://vocab.getty.edu/aat/300027200##quaType',
            'http://vocab.getty.edu/aat/300404333##quaType',
            'http://id.loc.gov/authorities/names/n78015294##quaGroup',
            'http://vocab.getty.edu/aat/300404264##quaType',
            'http://vocab.getty.edu/aat/300417443##quaType',
            'http://vocab.getty.edu/aat/300026497##quaType',
            'http://vocab.getty.edu/aat/300215302##quaType'
        ]
        idmap = self.configs.get_idmap()
        # They might not exist yet if idmap is empty
        non_global_excludes = [idmap[x][-36:] for x in non_global_externals if x in idmap]
        self.ref_ctr_excludes.update(set(non_global_excludes))

    def _walk_node_ref(self, node, refs, all_refs, top=False, ignore=False):
        if (
            not top
            and "id" in node
            and node["id"] is not None
            and node["id"].startswith(self.configs.internal_uri)
        ):
            # record node['id']
            if not node["id"] in all_refs:
                all_refs.append(node["id"])
            if not ignore and not node["id"] in refs:
                refs.append(node["id"])
        elif not top and "id" in node and node["id"] is None:
            print(f"ID is None?? {node}")

        if "type" in node and node["type"] == "TimeSpan":
            # Add integer seconds for properties
            for p in [
                "begin_of_the_begin",
                "begin_of_the_end",
                "end_of_the_begin",
                "end_of_the_end",
            ]:
                if p in node:
                    val = node[p]
                    try:
                        secs = np.datetime64(val).astype("<M8[s]").astype(np.int64)
                        node[f"_seconds_since_epoch_{p}"] = int(secs)
                    except:
                        print(f"Could not get seconds for {val}")

        if (
            not top
            and "id" in node
            and node["id"] is not None
            and node["id"].startswith(self.configs.internal_uri)
            and "classified_as" in node
        ):
            # some other record has one or more classifications in this record
            # these will likely disappear if not captured
            t = node["id"]
            if not t in self.external_types:
                self.external_types[t] = []
            cxns = [x["id"] for x in node.get("classified_as", []) if "id" in x]
            for c in cxns:
                if not c in self.external_types[t]:
                    self.external_types[t].append(c)

        for k, v in node.items():
            if not type(v) in [list, dict]:
                continue
            ignore_now = ignore
            if not ignore:
                ignore = k in self.ignore_props
            if type(v) == list:
                for vi in v:
                    if type(vi) == dict:
                        self._walk_node_ref(vi, refs, all_refs, False, ignore)
                    else:
                        print(f"found non dict in a list :( {node}")
            elif type(v) == dict:
                self._walk_node_ref(v, refs, all_refs, False, ignore)
            ignore = ignore_now

    def find_named_refs(self, data):
        refs = []
        all_refs = []
        self._walk_node_ref(data, refs, all_refs, True, False)
        return refs, all_refs

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
                part["_content_html"] = content

    def get_pfx(self, typ, archive=False):
        if typ in ["VisualItem", "LinguisticObject"]:
            # facets["uiType"] = "CollectionWork"
            prefix = "work"
        elif typ in ["HumanMadeObject", "DigitalObject"]:
            # facets["uiType"] = "CollectionItem"
            prefix = "item"
        elif archive and typ == "Set":
            # facets["uiType"] = "CollectionWork"
            prefix = "work"
        elif typ in ["Person", "Group"]:
            # facets["uiType"] = "Agent"
            prefix = "agent"
        elif typ == "Place":
            # facets["uiType"] = "Place"
            prefix = "place"
        elif typ in [
            "Type",
            "Language",
            "Material",
            "Currency",
            "MeasurementUnit",
            "Set",
        ]:
            # Set here is Collection / Holdings. UI decision to put in with concepts
            # facets["uiType"] = "Concept"
            prefix = "concept"
        elif typ in ["Activity", "Event", "Period"]:
            # facets["uiType"] = "Temporal"
            prefix = "event"
        else:
            # Things that don't fall into the above categories
            # Probably due to bugs
            print(f"Failed to find a prefix for {typ}")
            prefix = "other"
            # facets["uiType"] = "Other"

        return prefix

    def transform(self, record, rectype=None, reference=False):
        ## XXX FIXME to config
        luxns = "https://lux.collections.yale.edu/ns/"
        crmns = "http://www.cidoc-crm.org/cidoc-crm/"
        lans = "https://linked.art/ns/terms/"
        skosns = "http://www.w3.org/2004/02/skos/core#"
        rdfns = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        type_map = {
            "HumanMadeObject": [f"{crmns}E22_Human-Made_Object", f"{luxns}Item"],
            "DigitalObject": [f"{crmns}D1_Digital_Object", f"{luxns}Item"],
            "VisualItem": [f"{crmns}E36_Visual_Item", f"{luxns}Work"],
            "LinguisticObject": [f"{crmns}E33_Linguistic_Object", f"{luxns}Work"],
            "Set": [f"{lans}Set"],
            "Person": [f"{crmns}E21_Person", f"{luxns}Agent"],
            "Group": [f"{crmns}E74_Group", f"{luxns}Agent"],
            "Place": [f"{crmns}E53_Place"],
            "Type": [f"{crmns}E55_Type", f"{luxns}Concept"],
            "Material": [f"{crmns}E57_Material", f"{luxns}Concept"],
            "Language": [f"{crmns}E56_Language", f"{luxns}Concept"],
            "Unit": [f"{crmns}E58_Measurement_Unit", f"{luxns}Concept"],
            "MeasurementUnit": [f"{crmns}E58_Measurement_Unit", f"{luxns}Concept"],
            "Event": [f"{crmns}E5_Event", f"{luxns}Event"],
            "Activity": [f"{crmns}E7_Activity", f"{luxns}Event"],
            "Period": [f"{crmns}E4_Period", f"{luxns}Event"],
        }

        data = record["data"]
        me = data["id"]

        cxns = [x["id"] for x in data.get("classified_as", []) if "id" in x]
        pfx = self.get_pfx(data["type"], archive=self.globals["archives"] in cxns)

        # strip html from content, move to _content_html
        for part in data.get("referred_to_by", []):
            self.do_bs_html(part)
        for rep in data.get("representation", []):
            for dsb in rep.get("digitally_shown_by", []):
                for ref in dsb.get("referred_to_by", []):
                    self.do_bs_html(ref)

        ml = {"json": data, "triples": [], "admin": {"conversion-date": self.start_str}}

        if "change" in record and record["change"]:
            sources = record["change"].split("|")
            okay = ["ipch", "pmc", "ils", "yuag", "ycba", "ypm"]
            for s in sources[:]:
                if not s in okay:
                    sources.remove(s)
            ml["admin"]["sources"] = sources

        facets = {}
        # Add in triples for ML

        facets["dataType"] = data["type"]

        if data["type"] in type_map:
            for a in type_map[data["type"]]:
                t = {"subject": me, "predicate": f"{rdfns}type", "object": a}
                ml["triples"].append({"triple": t})
        else:
            print(f"Unknown type in {me}: {data['type']}")

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

        facets["isCollectionItem"] = 0
        # MFHDs are never collection items
        isMFHD = False
        if "identified_by" in data:
            for i in data["identified_by"]:
                if (
                    i["type"] == "Identifier"
                    and "content" in i
                    and i["content"].startswith("ils:yul:mfhd:")
                ):
                    isMFHD = True
                    break

        if isMFHD == False and "subject_of" in data:
            for s in data["subject_of"]:
                if "digitally_carried_by" in s:
                    for do in s["digitally_carried_by"]:
                        if "access_point" in do:
                            ap = do["access_point"][0]["id"]
                            if (
                                ap.startswith("https://collections.britishart.")
                                or ap.startswith("https://artgallery.yale")
                                or ap.startswith("https://collections.peabody")
                                or ap.startswith("https://archives.yale.edu")
                                or ap.startswith("https://search.library.yale")
                            ):
                                facets["isCollectionItem"] = 1
                                break

        for prop, predClass in shortcuts.items():
            if prop in data:
                node = data[prop]
                apred = f"agentOf{predClass}"
                ppred = f"placeOf{predClass}"
                tpred = f"techniqueOf{predClass}"
                cpred = f"causeOf{predClass}"
                agents = []
                places = []
                techs = []
                causes = []

                if type(node) == dict:
                    node = [node]
                for n in node:
                    if "carried_out_by" in n:
                        agents.extend(
                            [x["id"] for x in n["carried_out_by"] if "id" in x]
                        )
                    if "took_place_at" in n:
                        places.extend(
                            [x["id"] for x in n["took_place_at"] if "id" in x]
                        )
                    if "technique" in n:
                        techs.extend([x["id"] for x in n["technique"] if "id" in x])
                    if "caused_by" in n:
                        causes.extend([x["id"] for x in n["caused_by"] if "id" in x])
                    if "influenced_by" in n:
                        for inf in n["influenced_by"]:
                            if "id" in inf:
                                if "type" in inf:
                                    infpfx = self.get_pfx(inf["type"])
                                else:
                                    infpfx = "other"
                                t = {
                                    "subject": me,
                                    "predicate": f"{luxns}{infpfx}Influenced{predClass}",
                                    "object": inf["id"],
                                }
                                ml["triples"].append({"triple": t})
                    if "part" in n:
                        for p in n["part"]:
                            if "carried_out_by" in p:
                                agents.extend(
                                    [x["id"] for x in p["carried_out_by"] if "id" in x]
                                )
                            if "took_place_at" in p:
                                places.extend(
                                    [x["id"] for x in p["took_place_at"] if "id" in x]
                                )
                            if "technique" in p:
                                techs.extend(
                                    [x["id"] for x in p["technique"] if "id" in x]
                                )
                            if "caused_by" in p:
                                causes.extend(
                                    [x["id"] for x in p["caused_by"] if "id" in x]
                                )
                            if "influenced_by" in p:
                                for inf in p["influenced_by"]:
                                    if "id" in inf:
                                        if "type" in inf:
                                            infpfx = self.get_pfx(inf["type"])
                                        else:
                                            infpfx = "other"
                                        t = {
                                            "subject": me,
                                            "predicate": f"{luxns}{infpfx}Influenced{predClass}",
                                            "object": inf["id"],
                                        }
                                        ml["triples"].append({"triple": t})
                            if "attributed_by" in p:
                                for aa in p["attributed_by"]:
                                    if "assigned" in aa:
                                        for p2 in aa["assigned"]:
                                            if "carried_out_by" in p2:
                                                agents.extend(
                                                    [
                                                        x["id"]
                                                        for x in p2["carried_out_by"]
                                                        if "id" in x
                                                    ]
                                                )
                                            if "took_place_at" in p2:
                                                places.extend(
                                                    [
                                                        x["id"]
                                                        for x in p2["took_place_at"]
                                                        if "id" in x
                                                    ]
                                                )
                                            if "technique" in p2:
                                                techs.extend(
                                                    [
                                                        x["id"]
                                                        for x in p2["technique"]
                                                        if "id" in x
                                                    ]
                                                )

                    if "attributed_by" in n:
                        for p in n["attributed_by"]:
                            if "assigned" in p:
                                for p2 in p["assigned"]:
                                    if "carried_out_by" in p2:
                                        agents.extend(
                                            [
                                                x["id"]
                                                for x in p2["carried_out_by"]
                                                if "id" in x
                                            ]
                                        )
                                    if "took_place_at" in p2:
                                        places.extend(
                                            [
                                                x["id"]
                                                for x in p2["took_place_at"]
                                                if "id" in x
                                            ]
                                        )
                                    if "technique" in p2:
                                        techs.extend(
                                            [
                                                x["id"]
                                                for x in p2["technique"]
                                                if "id" in x
                                            ]
                                        )

                for a in agents:
                    t = {"subject": me, "predicate": f"{luxns}{apred}", "object": a}
                    ml["triples"].append({"triple": t})
                for p in places:
                    t = {"subject": me, "predicate": f"{luxns}{ppred}", "object": p}
                    ml["triples"].append({"triple": t})
                for tt in techs:
                    t = {"subject": me, "predicate": f"{luxns}{tpred}", "object": tt}
                    ml["triples"].append({"triple": t})
                for c in causes:
                    t = {"subject": me, "predicate": f"{luxns}{cpred}", "object": c}
                    ml["triples"].append({"triple": t})

        # extracted data for indexes/facets

        if cxns:
            # facets["classifiedAsId"] = cxns
            cxPred = f"{luxns}{pfx}ClassifiedAs"
            for c in cxns:
                t = {"subject": me, "predicate": f"{crmns}P2_has_type", "object": c}
                ml["triples"].append({"triple": t})
                t = {"subject": me, "predicate": cxPred, "object": c}
                ml["triples"].append({"triple": t})
                if pfx in ["agent", "place", "concept", "event"]:
                    t = {
                        "subject": me,
                        "predicate": f"{luxns}referenceClassifiedAs",
                        "object": c,
                    }
                    ml["triples"].append({"triple": t})
                if data["type"] == "Set":
                    t = {
                        "subject": me,
                        "predicate": f"{luxns}setClassifiedAs",
                        "object": c,
                    }
                    ml["triples"].append({"triple": t})

        if "member_of" in data:
            if pfx == "agent":
                pred = f"{crmns}P107i_is_current_or_former_member_of"
            else:
                pred = f"{lans}member_of"
            for m in data["member_of"]:
                if "id" in m:
                    t = {"subject": me, "predicate": pred, "object": m["id"]}
                    ml["triples"].append({"triple": t})

        if "equivalent" in data:
            for eq in data["equivalent"]:
                if "id" in eq:
                    t = {
                        "subject": me,
                        "predicate": f"{lans}equivalent",
                        "object": eq["id"],
                    }
                    ml["triples"].append({"triple": t})

        if pfx == "agent":
            facets["nationalityId"] = [
                x["id"]
                for x in data.get("classified_as", [])
                if self.globals["nationality"]
                in [y["id"] for y in x.get("classified_as", [])]
                and "id" in x
            ]
            for f in facets["nationalityId"]:
                t = {
                    "subject": me,
                    "predicate": f"{luxns}agentNationality",
                    "object": f,
                }
                ml["triples"].append({"triple": t})

            facets["occupationId"] = [
                x["id"]
                for x in data.get("classified_as", [])
                if self.globals["occupation"]
                in [y["id"] for y in x.get("classified_as", [])]
                and "id" in x
            ]
            for f in facets["occupationId"]:
                t = {"subject": me, "predicate": f"{luxns}agentOccupation", "object": f}
                ml["triples"].append({"triple": t})

            # facets["agentActivePlaceId"] = []
            if "carried_out" in data:
                for co in data["carried_out"]:
                    cxns = [x["id"] for x in co.get("classified_as", []) if "id" in x]
                    if self.globals["active"] in cxns:
                        for cx in cxns:
                            if cx != self.globals["active"]:
                                t = {
                                    "subject": me,
                                    "predicate": f"{luxns}typeOfProfessionalActivity",
                                    "object": cx,
                                }
                                ml["triples"].append({"triple": t})

        if data["type"] == "Person":
            # facets["personId"] = data["id"]
            # Do person indexes here
            facets["genderId"] = [
                x["id"]
                for x in data.get("classified_as", [])
                if self.globals["gender"]
                in [y["id"] for y in x.get("classified_as", [])]
                and "id" in x
            ]
            for f in facets["genderId"]:
                t = {"subject": me, "predicate": f"{luxns}agentGender", "object": f}
                ml["triples"].append({"triple": t})

            # if "born" in data and "timespan" in data["born"]:
            #     facets["agentBeginDate"] = data["born"]["timespan"].get("begin_of_the_begin", "")
            # if "died" in data and "timespan" in data["died"]:
            #     facets["agentEndDate"] = data["died"]["timespan"].get("end_of_the_end", "")

        # elif data["type"] == "Group":
        #     facets["groupId"] = data["id"]
        #     if "formed_by" in data and "timespan" in data["formed_by"]:
        #         facets["agentBeginDate"] = data["formed_by"]["timespan"].get("begin_of_the_begin", "")
        #     if "dissolved_by" in data and "timespan" in data["dissolved_by"]:
        #         facets["agentEndDate"] = data["dissolved_by"]["timespan"].get("end_of_the_end", "")

        # elif data["type"] in ["HumanMadeObject", "DigitalObject", "LinguisticObject"]:
        #     # Type of work / supertype
        #     facets["superType"] = [
        #         x["id"]
        #         for x in data.get("classified_as", [])
        #         if self.globals["typeOfWork"] in [y["id"] for y in x.get("classified_as", [])] and "id" in x
        #     ]
        elif data["type"] == "Set":
            if "used_for" in data:
                for uf in data["used_for"]:
                    if "id" in uf:
                        t = {
                            "subject": me,
                            "predicate": f"{crmns}P16i_was_used_for",
                            "object": uf["id"],
                        }
                        ml["triples"].append({"triple": t})
                    elif "classified_as" in uf and "carried_out_by" in uf:
                        for c in uf["classified_as"]:
                            if "id" in c and c["id"] == self.globals["curation"]:
                                for who in uf["carried_out_by"]:
                                    if "id" in who:
                                        t = {
                                            "subject": me,
                                            "predicate": f"{luxns}agentOfCuration",
                                            "object": who["id"],
                                        }
                                        ml["triples"].append({"triple": t})

        if data["type"] == "HumanMadeObject":
            if "carries" in data:
                for c in data["carries"]:
                    if "id" in c:
                        t = {
                            "subject": me,
                            "predicate": f"{luxns}carries_or_shows",
                            "object": c["id"],
                        }
                        ml["triples"].append({"triple": t})
                        t = {
                            "subject": me,
                            "predicate": f"{crmns}P128_carries",
                            "object": c["id"],
                        }
                        ml["triples"].append({"triple": t})

            if "shows" in data:
                for s in data["shows"]:
                    if "id" in s:
                        t = {
                            "subject": me,
                            "predicate": f"{luxns}carries_or_shows",
                            "object": s["id"],
                        }
                        ml["triples"].append({"triple": t})
                        t = {
                            "subject": me,
                            "predicate": f"{crmns}P65_shows_visual_item",
                            "object": s["id"],
                        }
                        ml["triples"].append({"triple": t})
            if "made_of" in data:
                for m in data["made_of"]:
                    if "id" in m:
                        t = {
                            "subject": me,
                            "predicate": f"{crmns}P45_consists_of",
                            "object": m["id"],
                        }
                        ml["triples"].append({"triple": t})

        elif data["type"] == "DigitalObject":
            if "digitally_carries" in data:
                for c in data["digitally_carries"]:
                    if "id" in c:
                        t = {
                            "subject": me,
                            "predicate": f"{luxns}carries_or_shows",
                            "object": c["id"],
                        }
                        ml["triples"].append({"triple": t})
                        t = {
                            "subject": me,
                            "predicate": f"{lans}digitally_carries",
                            "object": c["id"],
                        }
                        ml["triples"].append({"triple": t})

            if "digitally_shows" in data:
                for s in data["digitally_shows"]:
                    if "id" in s:
                        t = {
                            "subject": me,
                            "predicate": f"{luxns}carries_or_shows",
                            "object": s["id"],
                        }
                        ml["triples"].append({"triple": t})
                        t = {
                            "subject": me,
                            "predicate": f"{lans}digitally_shows",
                            "object": s["id"],
                        }
                        ml["triples"].append({"triple": t})

        elif data["type"] in ["LinguisticObject", "Set"]:
            if "about" in data:
                for a in data["about"]:
                    if "id" in a:
                        t = {
                            "subject": me,
                            "predicate": f"{luxns}about_or_depicts",
                            "object": a["id"],
                        }
                        ml["triples"].append({"triple": t})
                        t = {
                            "subject": me,
                            "predicate": f"{crmns}P129_is_about",
                            "object": a["id"],
                        }
                        ml["triples"].append({"triple": t})
                        # add target specific triples
                        if "type" in a:
                            apfx = self.get_pfx(a["type"])
                            t = {
                                "subject": me,
                                "predicate": f"{luxns}about_or_depicts_{apfx}",
                                "object": a["id"],
                            }
                            ml["triples"].append({"triple": t})
                            t = {
                                "subject": me,
                                "predicate": f"{luxns}about_{apfx}",
                                "object": a["id"],
                            }
                            ml["triples"].append({"triple": t})

            if "language" in data:
                # Triple for language of record, vs language of embedded content
                langs = [x["id"] for x in data.get("language", []) if "id" in x]
                lPred = f"{luxns}{pfx}Language"
                for l in langs:
                    t = {
                        "subject": me,
                        "predicate": f"{crmns}P72_has_language",
                        "object": l,
                    }
                    ml["triples"].append({"triple": t})
                    t = {"subject": me, "predicate": lPred, "object": l}
                    ml["triples"].append({"triple": t})

            whole = []
            if "part_of" in data:
                parts = [x["id"] for x in data["part_of"] if "id" in x]
                whole.extend(parts)
            for w in whole:
                t = {
                    "subject": me,
                    "predicate": f"{crmns}P106i_forms_part_of",
                    "object": w,
                }
                ml["triples"].append({"triple": t})

        elif data["type"] == "VisualItem":
            if "about" in data:
                for a in data["about"]:
                    if "id" in a:
                        t = {
                            "subject": me,
                            "predicate": f"{luxns}about_or_depicts",
                            "object": a["id"],
                        }
                        ml["triples"].append({"triple": t})
                        t = {
                            "subject": me,
                            "predicate": f"{crmns}P129_is_about",
                            "object": a["id"],
                        }
                        ml["triples"].append({"triple": t})
                        if "type" in a:
                            apfx = self.get_pfx(a["type"])
                            t = {
                                "subject": me,
                                "predicate": f"{luxns}about_or_depicts_{apfx}",
                                "object": a["id"],
                            }
                            ml["triples"].append({"triple": t})
                            t = {
                                "subject": me,
                                "predicate": f"{luxns}about_{apfx}",
                                "object": a["id"],
                            }
                            ml["triples"].append({"triple": t})

            if "represents" in data:
                for r in data["represents"]:
                    if "id" in r:
                        t = {
                            "subject": me,
                            "predicate": f"{luxns}about_or_depicts",
                            "object": r["id"],
                        }
                        ml["triples"].append({"triple": t})
                        t = {
                            "subject": me,
                            "predicate": f"{crmns}P138_represents",
                            "object": r["id"],
                        }
                        ml["triples"].append({"triple": t})
                        if "type" in r:
                            rpfx = self.get_pfx(r["type"])
                            t = {
                                "subject": me,
                                "predicate": f"{luxns}about_or_depicts_{rpfx}",
                                "object": r["id"],
                            }
                            ml["triples"].append({"triple": t})
                            t = {
                                "subject": me,
                                "predicate": f"{luxns}depicts_{rpfx}",
                                "object": r["id"],
                            }
                            ml["triples"].append({"triple": t})

        elif pfx == "event":
            # Add triples for carried_out_by, took_place_at of record, vs production etc
            if "carried_out_by" in data:
                ags = [x["id"] for x in data.get("carried_out_by", []) if "id" in x]
                aPred = f"{luxns}{pfx}CarriedOutBy"
                for a in ags:
                    t = {"subject": me, "predicate": aPred, "object": a}
                    ml["triples"].append({"triple": t})
            if "took_place_at" in data:
                places = [x["id"] for x in data.get("took_place_at", []) if "id" in x]
                pPred = f"{luxns}{pfx}TookPlaceAt"
                for p in places:
                    t = {"subject": me, "predicate": pPred, "object": p}
                    ml["triples"].append({"triple": t})
            if "used_specific_object" in data:
                for uso in data["used_specific_object"]:
                    if "id" in uso:
                        t = {
                            "subject": me,
                            "predicate": f"{crmns}P16_used_specific_object",
                            "object": uso["id"],
                        }
                        ml["triples"].append({"triple": t})

        elif pfx == "concept":
            # broader
            if "broader" in data:
                for b in data["broader"]:
                    if "id" in b:
                        t = {
                            "subject": me,
                            "predicate": f"{skosns}broader",
                            "object": b["id"],
                        }
                        ml["triples"].append({"triple": t})

        elif data["type"] == "Place":
            if "part_of" in data:
                for p in data["part_of"]:
                    if "id" in p:
                        t = {
                            "subject": me,
                            "predicate": f"{crmns}P89_falls_within",
                            "object": p["id"],
                        }
                        ml["triples"].append({"triple": t})

        # Add in inter-record shortcut triples

        (reffed, all_reffed) = self.find_named_refs(data)

        for r in reffed:
            t = {"subject": me, "predicate": f"{luxns}{pfx}Any", "object": r}
            ml["triples"].append({"triple": t})
            t2 = {"subject": me, "predicate": f"{luxns}any", "object": r}
            ml["triples"].append({"triple": t2})
            if data["type"] == "Set":
                t3 = {"subject": me, "predicate": f"{luxns}setAny", "object": r}
                ml["triples"].append({"triple": t3})
            elif data["type"] in [
                "Group",
                "Person",
                "Place",
                "Activity",
                "Period",
                "Type",
                "Language",
                "MeasurementUnit",
            ]:
                t4 = {"subject": me, "predicate": f"{luxns}referenceAny", "object": r}
                ml["triples"].append({"triple": t4})

        for r in all_reffed:
            if r[-36:] in self.ref_ctr_excludes:
                # exclude globals and top 20
                continue
            elif r in reffed:
                # exclude lux:any
                continue
            else:
                t = {"subject": me, "predicate": f"{luxns}refCtr", "object": r}
                ml['triples'].append({'triple': t})

        # Add boolean flags for hasImage and isOnline

        facets["hasDigitalImage"] = 0
        # true iff representation/digitally_shown_by/access_point/id
        if "representation" in data:
            for r in data["representation"]:
                if "digitally_shown_by" in r:
                    for do in r["digitally_shown_by"]:
                        if "access_point" in do:
                            for ap in do["access_point"]:
                                if "id" in ap:
                                    facets["hasDigitalImage"] = 1
                                    break
        facets["isOnline"] = 0
        if pfx in ["item", "work"]:
            facets["isOnline"] = facets["hasDigitalImage"]
            if data["type"] == "DigitalObject" and "access_point" in data:
                facets["isOnline"] = 1
            if "subject_of" in data:
                for so in data["subject_of"]:
                    if "digitally_carried_by" in so:
                        for dcb in so["digitally_carried_by"]:
                            if "access_point" in dcb:
                                for apo in dcb["access_point"]:
                                    if "id" in apo:
                                        ap = apo.get("id", "")
                                        if (
                                            ap
                                            and not ap.startswith(
                                                "https://search.library.yale.edu/"
                                            )
                                            and not ap.startswith(
                                                "https://collections.britishart.yale.edu/"
                                            )
                                            and not ap.startswith(
                                                "https://artgallery.yale.edu/"
                                            )
                                            and not ap.startswith(
                                                "https://collections.peabody.yale.edu/"
                                            )
                                            and not ap.startswith(
                                                "https://archives.yale.edu/"
                                            )
                                        ):
                                            facets["isOnline"] = 1

        # Add PD flag for works
        if pfx == "work":
            facets["isPublicDomain"] = 0
            if "subject_to" in data:
                for r in data["subject_to"]:
                    if "classified_as" in r:
                        for c in r["classified_as"]:
                            if (
                                "id" in c
                                and "creativecommons.org/publicdomain" in c["id"]
                            ):
                                facets["isPublicDomain"] = 1

        ml["indexedProperties"] = facets

        if data["type"] == "Place" and "defined_by" in data:
            # separate points from polygons
            coords = data["defined_by"]
            if coords.lower().startswith("point"):
                ml["indexedProperties"]["defined_by_point"] = coords
            elif coords.lower().startswith("polygon"):
                ml["indexedProperties"]["defined_by_polygon"] = coords
            else:
                print(f"Unknown place coordinate value: {coords}")

        return ml
