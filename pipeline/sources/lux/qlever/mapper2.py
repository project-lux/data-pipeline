import unicodedata
from string import punctuation, whitespace

import lxml.etree
import lxml.html
from shapely.wkt import loads

from pipeline.process.base.mapper import Mapper

"""
Create a mapper that produces completely artificial triples.
If there isn't a search, then there isn't a triple.
"""

### TO DO
# Consider: make different text fields for case sensitive/insensitive, diacritics/not diacritics


# A dict lookup rather than the chain of `in [list]` tests it replaces:
# get_prefix is called once per record plus once per subject and per
# influence, and the chain walked up to six list scans to answer "concept".
PREFIX_BY_TYPE = {
    "VisualItem": "work",
    "LinguisticObject": "work",
    "HumanMadeObject": "item",
    "DigitalObject": "item",
    "Person": "agent",
    "Group": "agent",
    "Place": "place",
    # Set here is Collection / Holdings. UI decision to put in with concepts
    "Type": "concept",
    "Language": "concept",
    "Material": "concept",
    "Currency": "concept",
    "MeasurementUnit": "concept",
    "Activity": "event",
    "Event": "event",
    "Period": "event",
    "Set": "set",
}

# One pass instead of six: str.translate walks the string once, where the
# chained .replace() calls each scanned it and allocated a new string. Value
# None deletes the character. Order didn't matter in the chained form either
# -- no replacement produces a character a later one would match -- so this
# is exactly equivalent.
SANITIZE_TABLE = str.maketrans({"\r": " ", "\n": " ", "\t": " ", "-": " ", '"': None, "\\": None})


class QleverMapper(Mapper):
    def __init__(self, config):
        Mapper.__init__(self, config)
        self.configs = config["all_configs"]
        self.idmap = self.configs.get_idmap()
        self.globals = self.configs.globals

        self.remove_diacritics = False
        self.min_word_chars = 0
        # self.padding_char = "Þ"
        self.padding_char = b"\xc3\xbe".decode("utf-8")

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

        # Triples are emitted as f-strings inline in transform():
        #   <subject> <predicate> <object> .
        #   <subject> <predicate> "value"datatype .
        # They were str.format(**dict), which re-parses the pattern and builds
        # a kwargs dict on every call -- and this is the innermost loop of the
        # whole export, tens of triples for each of tens of millions of
        # records. The f-string form is several times faster for identical
        # output (tests/test_qlever_mapper.py pins it).
        self.number_type = "^^<http://www.w3.org/2001/XMLSchema#decimal>"
        self.date_type = "^^<http://www.w3.org/2001/XMLSchema#dateTime>"
        self.wkt_type = "^^<http://www.opengis.net/ont/geosparql#wktLiteral>"

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
        string = string.lower().translate(SANITIZE_TABLE)
        # remove diacritics
        if self.remove_diacritics:
            nfkd_form = unicodedata.normalize("NFD", string)
            string = "".join([c for c in nfkd_form if not unicodedata.category(c) == "Mn"])

        if self.min_word_chars > 1:
            string = " ".join(
                [
                    word.strip(whitespace + punctuation).ljust(self.min_word_chars, self.padding_char)
                    for word in string.split()
                ]
            )
        return string

    def do_bs_html(self, content):
        # Same change as the marklogic mapper: BeautifulSoup(features="lxml")
        # parsed with lxml and then built a second, complete bs4 tree on top
        # of it, all of which was discarded for the text. Going straight to
        # lxml is several times faster, and this runs over every statement of
        # every record in the export.
        content = content.strip()
        if content.startswith("<"):
            try:
                tree = lxml.html.fromstring(content)
            except lxml.etree.ParserError:
                # starts with '<' but isn't parseable as markup; leave as-is
                return content
            # bs4's get_text() leaves script/style contents out. text_content()
            # would splice css and javascript into the record text, so drop
            # those subtrees first -- with_tail keeps the text after them.
            lxml.etree.strip_elements(tree, "script", "style", with_tail=False)
            return tree.text_content()
        return content

    def get_prefix(self, which):
        if type(which) is dict and "type" in which:
            which = which["type"]
        try:
            return PREFIX_BY_TYPE[which]
        except (KeyError, TypeError):
            # TypeError: `which` is still an unhashable dict, i.e. a node with
            # no type at all
            print(f"Failed to find a prefix for {which}")
            return "other"

    def transform(self, record, rectype=None, reference=False):
        data = record["data"]
        me = data["id"]
        rectype = data["type"]
        pfx = self.get_prefix(rectype)
        triples = []
        recordText = []

        # Bind the namespaces and the title-cased prefix to locals. Every
        # predicate below interpolates luxns, and there are tens of them per
        # record over tens of millions of records -- a local is a LOAD_FAST
        # where self.luxns is a LOAD_ATTR plus a dict lookup each time.
        luxns = self.luxns
        pfx_title = pfx.title()

        anyt = {"subject": me, "predicate": f"{luxns}{pfx}Any", "object": ""}
        lt = {"subject": me, "predicate": "", "value": "", "datatype": ""}

        # Otherwise can't distinguish between event category and event vs period vs activity
        t = {"subject": me, "predicate": f"{self.rdfns}type", "object": f"{luxns}{pfx_title}"}
        triples.append(f'<{me}> <{t["predicate"]}> <{t["object"]}> .')
        t["object"] = f"{self.lans}{rectype}"
        triples.append(f'<{me}> <{t["predicate"]}> <{t["object"]}> .')

        # meta-metadata -- sources for the record
        if "change" in record and record["change"]:
            sources = record["change"].split("|")
            okay = ["ipch", "pmc", "ils", "yuag", "ycba", "ypm"]
            t["predicate"] = f"{luxns}source"
            for s in sources[:]:
                if s in okay:
                    # add triple
                    t["object"] = f"{luxns}{s.upper()}"
                    triples.append(f'<{me}> <{t["predicate"]}> <{t["object"]}> .')

        # names
        lt["datatype"] = ""
        # .get: this was the one unguarded access in the mapper, and
        # manage-data.py --nt had no error handling around transform, so a
        # single record without identified_by ended the whole slice's export
        for idb in data.get("identified_by", []):
            if "content" not in idb or not idb["content"]:
                continue
            val = self.sanitize_string(idb["content"])
            recordText.append(val)
            lt["value"] = val
            if idb["type"] == "Name":
                # primaryName
                # `x.get("id", None)` put None in the list for classifications
                # without an id. self.sortIdentifier and friends are idmap
                # lookups that are themselves None when the aat term is
                # missing, and `None in cxns` then matched anything untyped
                # and mislabelled the field. Only real ids belong here.
                cxns = [x["id"] for x in idb.get("classified_as", []) if "id" in x]
                if self.primaryName in cxns:
                    lt["predicate"] = f"{luxns}{pfx}PrimaryName"
                    triples.append(f'<{me}> <{lt["predicate"]}> "{lt["value"]}"{lt["datatype"]} .')
                    lt["predicate"] = f"{luxns}primaryName"
                    triples.append(f'<{me}> <{lt["predicate"]}> "{lt["value"]}"{lt["datatype"]} .')
                if self.sortName in cxns:
                    lt["predicate"] = f"{luxns}{pfx}SortName"
                    triples.append(f'<{me}> <{lt["predicate"]}> "{lt["value"]}"{lt["datatype"]} .')
                lt["predicate"] = f"{luxns}{pfx}Name"
                triples.append(f'<{me}> <{lt["predicate"]}> "{lt["value"]}"{lt["datatype"]} .')
                lt["predicate"] = f"{luxns}name"
                triples.append(f'<{me}> <{lt["predicate"]}> "{lt["value"]}"{lt["datatype"]} .')
            else:
                cxns = [x["id"] for x in idb.get("classified_as", []) if "id" in x]
                if self.sortIdentifier in cxns:
                    lt["predicate"] = f"{luxns}sortIdentifier"
                    triples.append(f'<{me}> <{lt["predicate"]}> "{lt["value"]}"{lt["datatype"]} .')
                else:
                    lt["predicate"] = f"{luxns}{pfx}Identifier"
                    triples.append(f'<{me}> <{lt["predicate"]}> "{lt["value"]}"{lt["datatype"]} .')

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
                triples.append(f'<{me}> <{t["predicate"]}> <{t["object"]}> .')

        # digital image
        # true iff representation/digitally_shown_by/access_point/id
        rep = data.get("representation", None)
        hasDigitalImage = 0
        if rep:
            rep = rep[0].get("digitally_shown_by", None)
            if rep:
                rep = rep[0].get("access_point", None)
                if rep:
                    rep = rep[0].get("id", None)
                    if rep:
                        lt["predicate"] = f"{luxns}{pfx}HasDigitalImage"
                        lt["value"] = 1
                        hasDigitalImage = 1
                        lt["datatype"] = self.number_type
                        triples.append(f'<{me}> <{lt["predicate"]}> "{lt["value"]}"{lt["datatype"]} .')

        # all classifications
        # agentClassification, workClassification (etc)
        t["predicate"] = f"{luxns}{pfx}Classification"
        for cls in data.get("classified_as", []):
            if "id" in cls:
                t["object"] = cls["id"]
                triples.append(f'<{me}> <{t["predicate"]}> <{t["object"]}> .')
                anyt["object"] = cls["id"]
                triples.append(f'<{me}> <{anyt["predicate"]}> <{anyt["object"]}> .')

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
            # depends only on the prefix and the relation, not on val
            pcls = f"{pfx_title}{dtyp}"
            for val in vals:
                if type(val) is not dict:
                    print(f"*** string not dict: {dprop} in {me} ***")
                    continue
                check = [val]
                check.extend(val.get("part", []))
                for bit in check:
                    whos = bit.get("carried_out_by", [])
                    t["predicate"] = f"{luxns}agentOf{pcls}"
                    for who in whos:
                        if "id" in who:
                            t["object"] = who["id"]
                            triples.append(f'<{me}> <{t["predicate"]}> <{t["object"]}> .')
                            anyt["object"] = who["id"]
                            triples.append(f'<{me}> <{anyt["predicate"]}> <{anyt["object"]}> .')

                    wheres = bit.get("took_place_at", [])
                    t["predicate"] = f"{luxns}placeOf{pcls}"
                    for where in wheres:
                        if "id" in where:
                            t["object"] = where["id"]
                            triples.append(f'<{me}> <{t["predicate"]}> <{t["object"]}> .')
                            anyt["object"] = where["id"]
                            triples.append(f'<{me}> <{anyt["predicate"]}> <{anyt["object"]}> .')

                    # concatenate, don't extend: bit.get() returns the
                    # record's own list and extending it appended the
                    # technique entries into its classified_as
                    types = bit.get("classified_as", []) + bit.get("technique", [])
                    t["predicate"] = f"{luxns}typeOf{pcls}"
                    for typ in types:
                        if "id" in typ:
                            t["object"] = typ["id"]
                            triples.append(f'<{me}> <{t["predicate"]}> <{t["object"]}> .')
                            anyt["object"] = typ["id"]
                            triples.append(f'<{me}> <{anyt["predicate"]}> <{anyt["object"]}> .')
                    causes = bit.get("caused_by", [])
                    t["predicate"] = f"{luxns}causeOf{pcls}"
                    for cause in causes:
                        if "id" in cause:
                            t["object"] = cause["id"]
                            triples.append(f'<{me}> <{t["predicate"]}> <{t["object"]}> .')
                            anyt["object"] = cause["id"]
                            triples.append(f'<{me}> <{anyt["predicate"]}> <{anyt["object"]}> .')
                    infs = bit.get("influenced_by", []) + bit.get("used_specific_object", [])
                    for inf in infs:
                        if "type" in inf:
                            infpfx = self.get_prefix(inf["type"])
                            t["predicate"] = f"{luxns}{infpfx}InfluenceOf{pcls}"
                        else:
                            continue
                        if "id" in inf:
                            t["object"] = inf["id"]
                            triples.append(f'<{me}> <{t["predicate"]}> <{t["object"]}> .')
                            anyt["object"] = inf["id"]
                            triples.append(f'<{me}> <{anyt["predicate"]}> <{anyt["object"]}> .')
                    # timespan
                    timespan = bit.get("timespan", {})
                    if timespan:
                        # start, end dates
                        lt["datatype"] = self.date_type
                        startval = timespan.get("begin_of_the_begin", "")
                        if startval:
                            lt["predicate"] = f"{luxns}startOf{pcls}"
                            lt["value"] = startval
                            triples.append(f'<{me}> <{lt["predicate"]}> "{lt["value"]}"{lt["datatype"]} .')
                        endval = timespan.get("end_of_the_end", "")
                        if endval:
                            lt["predicate"] = f"{luxns}endOf{pcls}"
                            lt["value"] = endval
                            triples.append(f'<{me}> <{lt["predicate"]}> "{lt["value"]}"{lt["datatype"]} .')

        # memberOf
        for member in data.get("member_of", []):
            if "id" in member:
                t["object"] = member["id"]
                t["predicate"] = f"{luxns}{pfx}MemberOf{member['type']}"
                triples.append(f'<{me}> <{t["predicate"]}> <{t["object"]}> .')
                anyt["object"] = member["id"]
                triples.append(f'<{me}> <{anyt["predicate"]}> <{anyt["object"]}> .')

        # partOf
        parents = data.get("part_of", [])
        for parent in parents:
            if "id" in parent:
                t["predicate"] = f"{luxns}{pfx}PartOf"
                t["object"] = parent["id"]
                triples.append(f'<{me}> <{t["predicate"]}> <{t["object"]}> .')
                anyt["object"] = parent["id"]
                triples.append(f'<{me}> <{anyt["predicate"]}> <{anyt["object"]}> .')

        # isOnline for items, works, sets
        if pfx in ["item", "work", "set"]:
            isOnline = hasDigitalImage
            if not isOnline:
                if data["type"] == "DigitalObject" and "access_point" in data:
                    isOnline = 1
                elif "subject_of" in data:
                    for so in data["subject_of"]:
                        if "digitally_carried_by" in so:
                            for dcb in so["digitally_carried_by"]:
                                if "access_point" in dcb:
                                    for apo in dcb["access_point"]:
                                        if "id" in apo:
                                            ap = apo.get("id", "")
                                            if (
                                                ap
                                                and not ap.startswith("https://search.library.yale.edu/")
                                                and not ap.startswith("https://collections.britishart.yale.edu/")
                                                and not ap.startswith("https://artgallery.yale.edu/")
                                                and not ap.startswith("https://collections.peabody.yale.edu/")
                                                and not ap.startswith("https://archives.yale.edu/")
                                            ):
                                                isOnline = 1
            lt["predicate"] = f"{luxns}{pfx}IsOnline"
            lt["value"] = isOnline
            lt["datatype"] = self.number_type
            triples.append(f'<{me}> <{lt["predicate"]}> "{lt["value"]}"{lt["datatype"]} .')

        # Class Specific relationships
        if pfx in ["work", "set"]:
            # Language
            langs = data.get("language", [])
            t["predicate"] = f"{luxns}{pfx}Language"
            for lang in langs:
                if "id" in lang:
                    t["object"] = lang["id"]
                    triples.append(f'<{me}> <{t["predicate"]}> <{t["object"]}> .')
                    anyt["object"] = lang["id"]
                    triples.append(f'<{me}> <{anyt["predicate"]}> <{anyt["object"]}> .')

            # Subjects
            abouts = data.get("about", []) + data.get("represents", [])
            for about in abouts:
                if "id" in about:
                    t["object"] = about["id"]
                    abpfx = self.get_prefix(about)
                    t["predicate"] = f"{luxns}{pfx}About{abpfx.title()}"
                    triples.append(f'<{me}> <{t["predicate"]}> <{t["object"]}> .')
                    anyt["object"] = about["id"]
                    triples.append(f'<{me}> <{anyt["predicate"]}> <{anyt["object"]}> .')

            # Public Domain
            #
            isPublicDomain = 0
            if "subject_to" in data:
                for r in data["subject_to"]:
                    if "classified_as" in r:
                        for c in r["classified_as"]:
                            if "id" in c and "creativecommons.org/publicdomain" in c["id"]:
                                isPublicDomain = 1
                                break

            lt["datatype"] = self.number_type
            lt["value"] = isPublicDomain
            lt["predicate"] = f"{luxns}{pfx}IsPublicDomain"
            triples.append(f'<{me}> <{lt["predicate"]}> "{lt["value"]}"{lt["datatype"]} .')

            # Set specific predicates
            if pfx == "set":
                curates = data.get("used_for", [])
                t["predicate"] = f"{luxns}setCuratedBy"
                for c in curates:
                    for cby in c.get("carried_out_by", []):
                        if "id" in cby:
                            t["object"] = cby["id"]
                            triples.append(f'<{me}> <{t["predicate"]}> <{t["object"]}> .')
                            anyt["object"] = cby["id"]
                            triples.append(f'<{me}> <{anyt["predicate"]}> <{anyt["object"]}> .')

        elif pfx == "item":
            # carries/shows
            carries = (data.get("carries", []) + data.get("shows", [])
                       + data.get("digitally_carries", [])
                       + data.get("digitally_shows", []))
            for c in carries:
                if "id" in c:
                    t["object"] = c["id"]
                    t["predicate"] = f"{luxns}carries"
                    triples.append(f'<{me}> <{t["predicate"]}> <{t["object"]}> .')
                    anyt["object"] = c["id"]
                    triples.append(f'<{me}> <{anyt["predicate"]}> <{anyt["object"]}> .')

            # materials
            mats = data.get("made_of", [])
            t["predicate"] = f"{luxns}material"
            for mat in mats:
                if "id" in mat:
                    t["object"] = mat["id"]
                    triples.append(f'<{me}> <{t["predicate"]}> <{t["object"]}> .')
                    anyt["object"] = mat["id"]
                    triples.append(f'<{me}> <{anyt["predicate"]}> <{anyt["object"]}> .')

            # dimensions
            dims = data.get("dimension", [])
            lt["datatype"] = self.number_type
            for d in dims:
                if "value" in d:
                    lt["value"] = d["value"]
                    cxns = [x["id"] for x in d.get("classified_as", []) if "id" in x]
                    if self.height in cxns:
                        lt["predicate"] = f"{luxns}height"
                    elif self.width in cxns:
                        lt["predicate"] = f"{luxns}width"
                    elif self.depth in cxns:
                        lt["predicate"] = f"{luxns}depth"
                    elif self.weight in cxns:
                        lt["predicate"] = f"{luxns}weight"
                    else:
                        continue
                    triples.append(f'<{me}> <{lt["predicate"]}> "{lt["value"]}"{lt["datatype"]} .')
                    lt["predicate"] = f"{luxns}dimension"
                    triples.append(f'<{me}> <{lt["predicate"]}> "{lt["value"]}"{lt["datatype"]} .')

        elif pfx == "agent":
            # nationality, occupation, gender
            cxns = data.get("classified_as", [])
            for cxn in cxns:
                if "id" in cxn:
                    metas = [x["id"] for x in cxn.get("classified_as", []) if "id" in x]
                    if self.nationality in metas:
                        t["predicate"] = f"{luxns}nationality"
                    elif self.occupation in metas:
                        t["predicate"] = f"{luxns}occupation"
                    elif self.gender in metas:
                        t["predicate"] = f"{luxns}gender"
                    else:
                        continue
                    t["object"] = cxn["id"]
                    triples.append(f'<{me}> <{t["predicate"]}> <{t["object"]}> .')

        elif pfx == "concept":
            broaders = data.get("broader", [])
            for b in broaders:
                if "id" in b:
                    t["predicate"] = f"{luxns}broader"
                    t["object"] = b["id"]
                    triples.append(f'<{me}> <{t["predicate"]}> <{t["object"]}> .')
                    anyt["object"] = b["id"]
                    triples.append(f'<{me}> <{anyt["predicate"]}> <{anyt["object"]}> .')

        elif pfx == "place":
            wkt = data.get("defined_by", "")
            if wkt:
                # Clean the WKT string of points that can't exist
                # parse the string using shapely
                okay = True
                try:
                    geom = loads(wkt)
                    if geom.is_empty:
                        okay = False
                except Exception as e:
                    okay = False
                if okay:
                    # step through each point in the geometry
                    # and test if within the bounds of lat/long
                    if geom.geom_type == "Point":
                        if not (-90 <= geom.y <= 90 and -180 <= geom.x <= 180):
                            okay = False
                    else:
                        for point in geom.exterior.coords:
                            if not (-90 <= point[1] <= 90 and -180 <= point[0] <= 180):
                                okay = False
                                break
                    if okay:
                        lt["predicate"] = f"{luxns}placeWKT"
                        lt["value"] = wkt
                        lt["datatype"] = self.wkt_type
                        triples.append(f'<{me}> <{lt["predicate"]}> "{lt["value"]}"{lt["datatype"]} .')

        elif pfx == "event":
            whos = data.get("carried_out_by", [])
            t["predicate"] = f"{luxns}agentOfEvent"
            for who in whos:
                if "id" in who:
                    t["object"] = who["id"]
                    triples.append(f'<{me}> <{t["predicate"]}> <{t["object"]}> .')
                    anyt["object"] = who["id"]
                    triples.append(f'<{me}> <{anyt["predicate"]}> <{anyt["object"]}> .')
            wheres = data.get("took_place_at", [])
            t["predicate"] = f"{luxns}placeOfEvent"
            for where in wheres:
                if "id" in where:
                    t["object"] = where["id"]
                    triples.append(f'<{me}> <{t["predicate"]}> <{t["object"]}> .')
                    anyt["object"] = where["id"]
                    triples.append(f'<{me}> <{anyt["predicate"]}> <{anyt["object"]}> .')
            causes = data.get("caused_by", [])
            t["predicate"] = f"{luxns}causeOfEvent"
            for cause in causes:
                if "id" in cause:
                    t["object"] = cause["id"]
                    triples.append(f'<{me}> <{t["predicate"]}> <{t["object"]}> .')
                    anyt["object"] = cause["id"]
                    triples.append(f'<{me}> <{anyt["predicate"]}> <{anyt["object"]}> .')
            infs = data.get("influenced_by", []) + data.get("used_specific_object", [])
            for inf in infs:
                if "type" in inf:
                    infpfx = self.get_prefix(inf["type"])
                    t["predicate"] = f"{luxns}eventUsed{infpfx.title()}"
                else:
                    continue
                if "id" in inf:
                    t["object"] = inf["id"]
                    triples.append(f'<{me}> <{t["predicate"]}> <{t["object"]}> .')
                    anyt["object"] = inf["id"]
                    triples.append(f'<{me}> <{anyt["predicate"]}> <{anyt["object"]}> .')
            # timespan
            timespan = data.get("timespan", {})
            if timespan:
                # start, end dates
                lt["datatype"] = self.date_type
                startval = timespan.get("begin_of_the_begin", "")
                if startval:
                    lt["predicate"] = f"{luxns}startOfEvent"
                    lt["value"] = startval
                    triples.append(f'<{me}> <{lt["predicate"]}> "{lt["value"]}"{lt["datatype"]} .')
                endval = timespan.get("end_of_the_end", "")
                if endval:
                    lt["predicate"] = f"{luxns}endOfEvent"
                    lt["value"] = endval
                    triples.append(f'<{me}> <{lt["predicate"]}> "{lt["value"]}"{lt["datatype"]} .')
        else:
            raise ValueError(f"Unsupported prefix: {pfx}")

        # add in recordText, with prefix

        # Every element was sanitized as it went in, and joining with a space
        # can't introduce anything the sanitizer strips -- so re-sanitizing
        # here was a second full scan of the largest string in the record for
        # no change to it.
        rtxt = " ".join([x for x in recordText if x])
        lt["predicate"] = f"{luxns}{pfx}RecordText"
        lt["value"] = rtxt
        lt["datatype"] = ""
        triples.append(f'<{me}> <{lt["predicate"]}> "{lt["value"]}"{lt["datatype"]} .')

        # Experiment: Try keeping recordTexts in separate triples

        return triples
