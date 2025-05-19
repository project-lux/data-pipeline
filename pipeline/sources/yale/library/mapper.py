from pipeline.process.base.mapper import Mapper
from pipeline.process.utils.mapper_utils import validate_timespans
from pipeline.process.utils.mapper_utils import make_datetime
from pipeline.sources.yale.library.index_loader import YulIndexLoader
import os
import ujson as json
import csv
import re

multi_props = [
    "part_of",
    "identified_by",
    "classified_as",
    "equivalent",
    "member_of",
    "subject_of",
    "referred_to_by",
    "influenced_by",
    "about",
    "carries",
    "shows",
    "attributed_by",
    "carried_out_by",
    "took_place_at",
]

single_props = [
    "timespan",
    "produced_by",
    "created_by",
    "content",
    "begin_of_the_begin",
    "end_of_the_end",
    "value",
]


class YulMapper(Mapper):
    def __init__(self, config):
        Mapper.__init__(self, config)
        lccns = {}
        cfgs = config["all_configs"]
        data_dir = cfgs.data_dir
        fn = os.path.join(data_dir, "stupid_table.json")
        self.object_work_mismatch = {}
        if os.path.exists(fn):
            with open(fn) as fh:
                self.object_work_mismatch = json.load(fh)
        # stubid --> [real_id, type]

        ycbaexhs = {}
        ycbaobjs = {}
        fn1 = os.path.join(data_dir, "ils-exhs.csv")
        fn2 = os.path.join(data_dir, "ils-objs.csv")
        if os.path.exists(fn1):
            with open(fn1, "r") as file:
                reader = csv.reader(file)
                exhslist = list(reader)
                for e in exhslist:
                    ycbaexhs[e[0]] = e[1:]
        if os.path.exists(fn2):
            with open(fn2, "r") as file:
                reader = csv.reader(file)
                objslist = list(reader)
                for o in objslist:
                    ycbaobjs[o[0]] = o[1:]
        self.ycbaobjs = ycbaobjs
        self.ycbaexhs = ycbaexhs

        fn = os.path.join(data_dir, "lib_okay_parts.json")
        if os.path.exists(fn):
            with open(fn) as fh:
                okay = json.load(fh)
        self.okay_place_parts = okay

        self.wiki_recon = {}
        fn = os.path.join(data_dir, "yul-wiki-recon.csv")
        if os.path.exists(fn):
            with open(fn, "r") as fh:
                reader = csv.reader(fh)
                for row in reader:
                    self.wiki_recon[row[0]] = row[1]

        fn = os.path.join(data_dir, "parenthetical_places.json")
        if os.path.exists(fn):
            with open(fn) as fh:
                data = fh.read()
            self.parenthetical_places = json.loads(data)
        else:
            self.parenthetical_places = {}
        self.parens_re = re.compile("^(.+) \((.+)\)$")

        fn = os.path.join(data_dir, "lib_enhance_places.json")
        if os.path.exists(fn):
            with open(fn) as fh:
                data = fh.read()
            self.gemini_place_data = json.loads(data)
        else:
            self.gemini_place_data = {}

    def walk_multi(self, node, top=False):
        for k, v in node.items():
            if k in multi_props and not type(v) == list:
                node[k] = [v]
                v = [v]
            if k in single_props and type(v) == list:
                node[k] = v[0]
            if not top and "id" in node and node["id"] in self.object_work_mismatch:
                node["type"] = self.object_work_mismatch[node["id"]][1]
                node["id"] = self.object_work_mismatch[node["id"]][0]

            if type(v) == list:
                for vi in v:
                    if type(vi) == dict:
                        self.walk_multi(vi)
                    else:
                        print(f"found non dict in a list :( {node}")
            elif type(v) == dict:
                self.walk_multi(v)

    def transform(self, rec, rectype, reference=False):
        #headings_index = YulIndexLoader().load_index()

        data = rec["data"]

        if data["id"] in self.object_work_mismatch:
            return None

        # add abouts for ycba exhs & objs
        abouts = []
        if data["type"] == "LinguisticObject":
        #     current_block = data.get("about", [])
        #     for a in current_block:
        #         if a.get("id","") in headings_index:
        #             del a
        #             for h in headings_index[a["id"]]:
        #                 abouts.append({"id": h, "type": ""})
        #         else:
        #             abouts.append(a)

            # get yul ID #
            for ident in data["identified_by"]:
                if ident["content"].startswith("ils:yul:"):
                    ilsnum = ident["content"].split(":")[-1]
                    break
            try:
                objslist = self.ycbaobjs[ilsnum]
                for objs in objslist:
                    if objs != "":
                        objsblock = {"id": objs, "type": "HumanMadeObject"}
                        abouts.append(objsblock)
            except:
                pass
            try:
                exhslist = self.ycbaexhs[ilsnum]
                for exhs in exhslist:
                    if exhs != "":
                        exhsblock = {"id": exhs, "type": "Activity"}
                        abouts.append(exhsblock)
            except:
                pass
            
            del data["about"]
            if abouts:
                data["about"] = abouts

        if data["id"] in self.wiki_recon:
            equivs = data.get("equivalent", [])
            equivs.append(
                {
                    "id": self.wiki_recon[data["id"]],
                    "type": data["type"],
                    "_label": data.get("_label", "wikidata equivalent"),
                }
            )
            data["equivalent"] = equivs

        # modify in place
        ### 2021-12-01
        self.walk_multi(data, top=True)
        validate_timespans(data)

        # 2022-08-1 -- Digitally_shown_by is still wrong. id needs to move to access_point
        # Move representation and digitally_shown_by to subject_of
        # EG https://linked-art.library.yale.edu/node/07e71913-f2be-479c-a388-65025173b4fb
        #    https://linked-art.library.yale.edu/node/14a3551f-6e93-4501-9b18-1b107f0274aa

        for key in ["representation", "digitally_shown_by", "digitally_carried_by"]:
            if key in data:
                del_reps = []
                aps = []
                for r in data[key]:
                    if "id" in r and not "linked-art" in r["id"]:
                        # Access point
                        ap = r["id"]
                        if "identified_by" in r:
                            names = r["identified_by"]
                        else:
                            names = []
                        aps.append((ap, names))
                        del_reps.append(r)
                    elif "type" in r and r["type"] == "VisualItem" and "digitally_shown_by" in r:
                        kill = False
                        for dsb in r["digitally_shown_by"]:
                            if "id" in dsb:
                                ap = dsb["id"]
                                if "identified_by" in dsb:
                                    names = dsb["identified_by"]
                                else:
                                    names = []
                                aps.append((ap, names))
                                kill = True
                        if kill:
                            del_reps.append(r)

                for d in del_reps:
                    try:
                        data[key].remove(d)
                    except:
                        print(f"Failed to remove {d} from {data[key]}")
                if not data[key]:
                    del data[key]
                if aps:
                    # Move to subject_of
                    if not "subject_of" in data:
                        data["subject_of"] = []
                    for ap, names in aps:
                        a = {
                            "type": "DigitalObject",
                            "access_point": [{"id": ap, "type": "DigitalObject"}],
                        }
                        if names:
                            a["identified_by"] = names
                        data["subject_of"].append(
                            {
                                "type": "LinguisticObject",
                                "_label": "Representation/Reference",
                                "digitally_carried_by": [a],
                            }
                        )

        # Trash all part_ofs for SBX testing June 5 2024
        # if data['type'] == 'Place' and 'part_of' in data:
        #    del data['part_of']

        if data["type"] == "Place":
            primary = "http://vocab.getty.edu/aat/300404670"
            name = ""
            for n in data["identified_by"]:
                if "classified_as" in n:
                    cxns = [x["id"] for x in n["classified_as"]]
                    if primary in cxns:
                        name = n["content"]
                        break
            name = name.strip()
            if name and (m := self.parens_re.match(name)):
                (nm, par) = m.groups()
                par = par.strip()
                if ":" in par:
                    test = par.split(":", 1)
                    # Now test both to find which is right
                    # Could be: 'Abbey : Paris, France'
                    # or: (Norfolk, England : Parish)
                    # or: (Sweden : Kommun)
                    # (etc)
                else:
                    test = [par]
                parent = None

                for t in test:
                    if "," in t and not t in self.parenthetical_places:
                        (a, b) = t.split(",", 1)
                        if b.strip() in self.parenthetical_places:
                            # (Potsdam, Germany)
                            parent = b.strip()
                            break
                        elif a.strip() in self.parenthetical_places:
                            parent = a.strip()
                            break
                    elif t.strip() in self.parenthetical_places:
                        parent = t.strip()
                        break

                if parent is None and " and " in par and not par in self.parenthetical_places:
                    (a, b) = par.split(" and ", 1)
                    # just pick the first
                    if a.strip() in self.parenthetical_places:
                        parent = a.strip()
                    elif b.strip() in self.parenthetical_places:
                        parent = b.strip()

                if parent:
                    uri = self.parenthetical_places[parent]
                    rec["data"]["part_of"] = [{"id": uri, "type": "Place", "_label": parent}]

            uu = data["id"].split("/")[-1]
            if uu in self.gemini_place_data:
                info = self.gemini_place_data[uu]
                del self.gemini_place_data[uu]
                if "wd" in info:
                    wd = "http://www.wikidata.org/entity/" + info["wd"]
                    try:
                        data["equivalent"].append({"id": wd, "type": "Place", "_label": data.get("_label", name)})
                    except:
                        data["equivalent"] = [{"id": wd, "type": "Place", "_label": data.get("_label", name)}]

                    # Too often wp but no wd means invented wp URI :(
                    if "wp" in info:
                        page = {"id": "http://vocab.getty.edu/aat/300264578", "type": "Type", "_label": "Web Page"}
                        wp = {
                            "type": "LinguisticObject",
                            "digitally_carried_by": [
                                {
                                    "type": "DigitalObject",
                                    "classified_as": [page],
                                    "access_point": [{"id": info["wp"], "type": "DigitalObject"}],
                                }
                            ],
                        }
                        try:
                            data["subject_of"].append(wp)
                        except:
                            data["subject_of"] = [wp]

                if "desc" in info:
                    dt = {
                        "id": "http://vocab.getty.edu/aat/300435416",
                        "type": "Type",
                        "classified_as": [{"id": "http://vocab.getty.edu/aat/300418049", "type": "Type"}],
                    }
                    lang = {
                        "id": "http://vocab.getty.edu/aat/300388277",
                        "type": "Language",
                        "_label": "English",

                    }
                    desc = {
                        "type": "LinguisticObject",
                        "content": info["desc"] + " (AI generated)",
                        "classified_as": [dt],
                        "language": [lang],
                    }
                    try:
                        data["referred_to_by"].append(desc)
                    except:
                        data["referred_to_by"] = [desc]

        # Swap MarcGT to AAT equivalents
        if "classified_as" in data:
            for cxns in data["classified_as"]:
                if cxns["id"] == "http://id.loc.gov/vocabulary/marcgt/rea":
                    cxns["id"] = "http://vocab.getty.edu/aat/300265419"
                elif cxns["id"] == "http://id.loc.gov/vocabulary/marcgt/pic":
                    cxns["id"] = "http://vocab.getty.edu/aat/300264388"

        # Swap sort title AAT for sort value
        for ident in data.get("identified_by",[]):
            if "classified_as" in ident:
                for cxn in ident['classified_as']:
                    if cxn['id'] == "https://vocab.getty.edu/aat/300451544":
                        cxn['id'] = "http://vocab.getty.edu/aat/300456575"


        # Add collection item flag
        # FIXME: This doesn't work for archives

        item = False
        if data["type"] in ["HumanMadeObject", "DigitalObject"]:
            item = True
        elif "identified_by" in data:
            ids = [x for x in data["identified_by"] if x["type"] == "Identifier"]
            for i in ids:
                if "classified_as" in i:
                    for c in i["classified_as"]:
                        if (
                            "id" in c
                            and c["id"] == "http://vocab.getty.edu/aat/300435704"
                            and "content" in i
                            and i["content"].startswith("ils:yul:")
                            and not i["content"].startswith("ils:yul:mfhd:")
                        ):
                            # we're an item
                            item = True
                            break
                        elif not "id" in c or not "content" in i:
                            # FIXME: Fix this based on _label?
                            pass
        if item:
            if "classified_as" in data:
                classes = data["classified_as"]
            else:
                classes = []
            classes.append(
                {
                    "id": "http://vocab.getty.edu/aat/300404024",
                    "type": "Type",
                    "_label": "Collection Item",
                }
            )
            data["classified_as"] = classes

        if "defined_by" in data and data["defined_by"] == "":
            del data["defined_by"]

        if "identified_by" in data:
            for ident in data["identified_by"]:
                if "attributed_by" in ident:
                    ident["assigned_by"] = ident["attributed_by"]
                    del ident["attributed_by"]
                if "classified_as" in ident:
                    for c in ident["classified_as"]:
                        cxnid = c.get("id", "")
                        if cxnid and cxnid.startswith("https://vocab.getty.edu"):
                            c["id"] = cxnid.replace("https://", "http://")
                            
        if data['type'] == "Period":
            self.process_period_record(data)

        if data['type'] == "Set":
            for c in data.get("classified_as",[]):
                if c.get("id") == "http://vocab.getty.edu/aat/300311990":
                    c['id'] = "http://vocab.getty.edu/aat/300456764"



        return rec
