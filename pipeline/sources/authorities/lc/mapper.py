from pipeline.process.base.mapper import Mapper
from pipeline.process.utils.mapper_utils import make_datetime, test_birth_death
from cromulent import model, vocab
import ujson as json
import sys
import re
import os


class LcMapper(Mapper):
    def __init__(self, config):
        Mapper.__init__(self, config)
        self.acquirer = None
        self.config = config
        self.type_map = {
            "madsrdf:Geographic": "Place",
            "madsrdf:Language": "Language",
            "madsrdf:ConferenceName": "Activity",
            "madsrdf:FamilyName": "Group",
            "madsrdf:CorporateName": "Group",
            "madsrdf:PersonalName": "Person",
            "madsrdf:Temporal": "Period",
            "madsrdf:Organization": "Group",
            "foaf:Person": "Person",
            "http://id.loc.gov/ontologies/bibframe/Person": "Person",
            "http://xmlns.com/foaf/0.1/Person": "Person",
            "http://xmlns.com/foaf/0.1/Organization": "Group",
            "http://id.loc.gov/ontologies/bibframe/Organization": "Group",
            "madsrdf:Title": "LinguisticObject",
        }

        self.ignore_types = ["madsrdf:DeprecatedAuthority", "madsrdf:NameTitle"]

    def build_recs_and_reconcile(self, txt, rectype=""):
        """
        Builds a record and reconciles it based on the provided type.

        Args:
            txt (str): The textual content to reconcile.
            rectype (str): The type of the record (e.g., Place, Concept, Person).

        Returns:
            URI or None: The reconciled record's URI, or None if reconciliation fails.
        """
        # Fetch the appropriate reconcilers
        nafreconciler = self.config["all_configs"].external["lcnaf"]["reconciler"]
        shreconciler = self.config["all_configs"].external["lcsh"]["reconciler"]

        # Base record structure
        rec = {
            "type": "",
            "identified_by": [
                {
                    "type": "Name",
                    "content": txt,
                    "classified_as": [{"id": "http://vocab.getty.edu/aat/300404670"}],
                }
            ],
            "source": "",
        }

        # Map rectype to reconciler and record type
        reconcilers = {
            "Place": nafreconciler,
            "Concept": shreconciler,
            "Group": nafreconciler,
            "Person": nafreconciler,
            "Type": shreconciler,
            "Activity": nafreconciler,
            "Material": shreconciler,
            "Language": shreconciler,
        }

        if rectype in reconcilers:
            reconciler = reconcilers[rectype]
            rec["type"] = rectype
            if rectype in ["Place", "Group", "Person", "Activity"]:
                rec["source"] = "lcnaf"
            else:
                rec["source"] = "lcsh"
            reconrec = reconciler.reconcile(rec, reconcileType="name")
        else:
            # Handle invalid rectype
            print(f"Warning: Unrecognized rectype '{rectype}'")
            reconrec = None

        return reconrec

    def fix_identifier(self, identifier):
        if identifier == "@@LMI-SPECIAL-TERM@@":
            return None
        elif identifier.endswith("-781"):
            return identifier[:-4]
        return identifier

    def fix_links(self, record):
        if record:
            Mapper.fix_links(self, record)
            # and strip BNF as frequently trash
            new = []
            for eq in record["data"].get("equivalent", []):
                if not "bnf.fr" in eq["id"]:
                    new.append(eq)
            if new:
                record["data"]["equivalent"] = new
        return record

    def transform(self, record, rectype=None, reference=False):
        if not self.acquirer:
            self.acquirer = self.config["acquirer"]

        rec = record["data"]
        if "@id" in rec and rec["@id"].startswith("/authorities/"):
            ident = rec["@id"].rsplit("/", 1)[1]
            topid = f"{self.namespace}{ident}"
        elif "@context" in rec and type(rec["@context"]) == dict:
            topid = rec["@context"]["about"]
        else:
            identifier = record["identifier"]
            topid = f"{self.namespace}{identifier}"

        if len(rec.keys()) == 1 and "value" in rec:
            rec["@graph"] = rec["value"]
            del rec["value"]

        nodes = {}
        for n in rec["@graph"]:
            try:
                nodes[n["@id"]] = n
            except:
                pass
        try:
            new = self.reconstitute(nodes[topid], nodes)
        except:
            return None

        if "@type" in new:
            for bad_type in self.ignore_types:
                if bad_type in new["@type"]:
                    # Trash it
                    return None
            if rectype != "LinguisticObject" and "madsrdf:Title" in new["@type"]:
                # Also trash it
                return None
        return new

    def guess_type(self, new):
        ident = new["@id"]
        typs = new["@type"]
        for bad_type in self.ignore_types:
            if bad_type in typs:
                return None

        if type(typs) != list:
            typs = [typs]
        topcls = None
        for t in new["@type"]:
            if t in self.type_map:
                topcls = self.type_map[t]
                break
        if not topcls and "madsrdf:identifiesRWO" in new:
            # process RWO classes
            rwo = new["madsrdf:identifiesRWO"]
            if type(rwo) == list:
                rwo = rwo[0]
            for t in rwo["@type"]:
                if t in self.type_map:
                    topcls = self.type_map[t]
                    break
        if not topcls:
            topcls = "Type"
        return topcls

    def handle_note(self, notes, top):
        if type(notes) == str:
            notes = {"@value": notes}
        if type(notes) == dict:
            notes = [notes]
        if notes and type(notes) == list and type(notes[0]) == str:
            notes = [{"@value": x} for x in notes]
        for n in notes:
            top.referred_to_by = vocab.Note(content=n["@value"])

    def map_label(self, new, top):
        ### Names of the Entity
        prefs = new.get("madsrdf:authoritativeLabel", [])
        if type(prefs) == str:
            prefs = {"@value": prefs}
        if type(prefs) == dict:
            prefs = [prefs]
        if not prefs:
            print(f"Uhoh, no preflabels in {new['@id']}!")
            print(json.dumps(new, indent=2))
            return {}
        top._label = prefs[0]["@value"]
        for p in prefs:
            nm = vocab.PrimaryName(content=p["@value"])
            if "@language" in p and p["@language"] and p["@language"] in self.process_langs:
                nm.language = self.process_langs[p["@language"]]
            top.identified_by = nm

    def map_common(self, new, top):
        self.map_label(new, top)

        ### And other stuff
        # skos:altLabel
        alts = new.get("madsrdf:hasVariant", [])
        if type(alts) == str:
            alts = {"@value": alts}
        if type(alts) == dict:
            alts = [alts]

        for a in alts:
            vl = a.get("madsrdf:variantLabel", {})
            if vl:
                if type(vl) == str:
                    nm = vocab.AlternateName(content=vl)
                elif type(vl) == list:
                    nm = vocab.AlternateName(content=vl[0]["@value"])
                else:
                    nm = vocab.AlternateName(content=vl["@value"])
                if type(a) == dict and "@language" in a and a["@language"] and a["@language"] in self.process_langs:
                    nm.language = self.process_langs[a["@language"]]
                top.identified_by = nm
            else:
                print(f"Can't find the alternate label in: {a}")

        ###  Descriptions
        # skos:note

        noteFields = ["note", "definitionNote", "scopeNote"]
        for nf in noteFields:
            notes = new.get(f"madsrdf:{nf}", [])
            if notes:
                self.handle_note(notes, top)

        ### Equivalent Resources

        # Some records have externals in identifiesRWO
        idby = new.get("madsrdf:identifiesRWO", [])
        if type(idby) != list:
            idby = [idby]
        ex = new.get("madsrdf:hasExactExternalAuthority", [])
        if type(ex) != list:
            ex = [ex]

        if top.__class__ != model.Group:
            later = new.get("madsrdf:hasLaterEstablishedForm", [])
            if later:
                if type(later) != list:
                    later = [later]
                ex.extend(later)

            earliers = new.get("madsrdf:hasEarlierEstablishedForm", [])
            earlier_ids = []
            if earliers:
                if not isinstance(earliers, list):
                    earliers = [earliers]
                for e in earliers:
                    txt = e.get("madsrdf:variantLabel")
                    txt = txt.get("@value") if isinstance(txt, dict) else txt
                    eid = e.get("@id")

                    reid = None
                    if eid and eid.startswith("_:"):
                        if txt:
                            reid = self.build_recs_and_reconcile(txt, type(top).__name__)
                    else:
                        reid = eid
                    if reid:
                        earlier_ids.append(reid)
                ex.extend(earlier_ids)

        # skos:closeMatch -- Only as a last resort
        close = new.get("madsrdf:hasCloseExternalAuthority", [])
        if type(close) != list:
            close = [close]

        eqs = []
        sawviaf = False
        sawwd = False
        for i in idby:
            if type(i) == str:
                i = {"@id": i}
            # drop dbpedia, bbc, musicbrainz
            uri = i["@id"]
            if "dbpedia.org" in uri or "bbc.co.uk" in uri or "musicbrainz.org" in uri:
                continue
            elif "/tgn/" in uri and "-place" in uri:
                uri = uri.replace("-place", "")
            elif "viaf.org/viaf" in uri:
                sawviaf = True
            elif "loc.gov/rwo" in uri:
                # handled separately
                continue
            eqs.append(uri)
        for e in ex:
            if type(e) == str:
                e = {"@id": e}
            uri = e["@id"]
            if uri in eqs:
                continue
            elif "viaf.org/viaf/" in uri and sawviaf == True:
                continue
            eqs.append(uri)
        # Only add wd, or if few than 4
        if sawwd == False or len(eqs) < 4:
            for c in close:
                if type(c) == str:
                    c = {"@id": c}
                uri = c["@id"]
                if "bnf.fr/" in uri:
                    continue
                elif uri in eqs:
                    continue
                elif sawwd == False:
                    if "wikidata" in uri:
                        eqs.append(uri)
                        if len(eqs) >= 4:
                            break
                        else:
                            continue
                eqs.append(uri)

        doneids = []
        topcls = top.__class__
        for eid in eqs:
            if eid in doneids:
                continue
            doneids.append(eid)
            equiv = topcls(ident=eid)
            top.equivalent = equiv

    def reconstitute(self, js, nodes):
        # recursively build tree from @graph node list
        # only replace into tree once to ensure non-infinite-recursion
        # ignore number, string, etc values
        del nodes[js["@id"]]
        for k, v in js.items():
            if type(v) == dict and "@id" in v and v["@id"] in nodes:
                js[k] = self.reconstitute(nodes[v["@id"]], nodes)
            elif type(v) == list or (type(v) is dict and "@list" in v):
                new = []
                if type(v) is dict:
                    v = v["@list"]
                for vi in v:
                    if type(vi) == dict and "@id" in vi and vi["@id"] in nodes:
                        new.append(self.reconstitute(nodes[vi["@id"]], nodes))
                    else:
                        new.append(vi)
                js[k] = new
        return js


class LcshMapper(LcMapper):
    def __init__(self, config):
        LcMapper.__init__(self, config)
        fn = os.path.join(config["all_configs"].data_dir, "periods_by_name.json")
        if os.path.exists(fn):
            with open(fn) as fh:
                self.period_names = json.load(fh)
        else:
            self.period_names = {}
        self.lcnaf_mapper = None

    def transform(self, record, rectype=None, reference=False):
        rec = record["data"]
        if not rec["@graph"] or rec["@graph"] == {}:
            return None

        if self.lcnaf_mapper is None:
            self.lcnaf_mapper = self.configs.external["lcnaf"]["mapper"]

        new = LcMapper.transform(self, record, rectype)
        if not new:
            return None
        if not rectype:
            rectype = self.guess_type(new)
        if not rectype:
            return None
        topcls = getattr(model, rectype)
        top = topcls(ident=new["@id"])

        if reference:
            self.map_label(new, top)
            del top.identified_by

        else:
            self.map_common(new, top)
            if topcls in [
                model.Type,
                model.Language,
                model.Material,
                model.Currency,
                model.MeasurementUnit,
            ]:
                cxn = new.get("madsrdf:classification", "")
                if cxn and type(cxn) == str:
                    top.equivalent = topcls(ident=f"https://id.loc.gov/authorities/classification/{cxn}")

                # broader == madsrdf:hasBroaderAuthority
                brdr = new.get("madsrdf:hasBroaderAuthority", [])
                if type(brdr) != list:
                    brdr = [brdr]
                for b in brdr:
                    bident = b["@id"]
                    if bident.startswith("_"):
                        continue
                    try:
                        blbl = b["madsrdf:authoritativeLabel"]["@value"]
                    except:
                        blbl = ""
                    # concept broader concept
                    top.broader = topcls(ident=bident, label=blbl)
                # Eventually add in handling for member_of Group, broader Place, part_of Events

                comps = new.get("madsrdf:componentList", {})
                if comps:
                    print(comps)
                    cre = model.Creation()
                    top.created_by = cre
                    if type(comps) is dict and "@list" in comps:
                        comps = comps["@list"]
                    if type(comps) is list:
                        for c in comps:
                            # add c to influenced_by in the Creation after mapping it to the right class
                            if type(c) is dict and "@id" in c:
                                uri = c["@id"]

                                if uri[0] == "_":
                                    # blank node, try to reconcile based on name
                                    lbl = c.get("madsrdf:authoritativeLabel", {"@value": ""})
                                    if type(lbl) is list:
                                        lbl = lbl[0]
                                    if type(lbl) is dict:
                                        lbl = lbl["@value"]
                                    # first check Periods in the mapper
                                    uri = self.period_names.get(lbl.lower(), "")
                                    if uri:
                                        uri = uri.replace(
                                            "yul:", "https://linkedhttps://linked-art.library.yale.edu/node/"
                                        )
                                        ref = model.Period(uri, label=lbl)
                                    else:
                                        # Now do reconciliation
                                        print(c)
                                        ref = {"_label": lbl}
                                    cre.influenced_by = ref
                                else:
                                    uri = uri.replace("rwo/agents", "authorities/names")
                                    # Need to know what class this is
                                    if "@type" in c and c["@type"] in self.type_map:
                                        clsnm = self.type_map[c["@type"]]
                                        cls = getattr(model, clsnm)
                                        ref = cls(ident=uri)
                                    else:
                                        ident = uri.rsplit("/", 1)[-1]
                                        if "subjects" in uri:
                                            ref = self.get_reference(ident)
                                        else:
                                            ref = self.lcnaf_mapper.get_reference(ident)
                                    cre.influenced_by = ref
                            else:
                                print(f"Unknown form of component: {c}")

        js = model.factory.toJSON(top)
        return {"identifier": record["identifier"], "data": js, "source": self.name}


class LcnafMapper(LcMapper):
    def __init__(self, config):
        LcMapper.__init__(self, config)
        self.lc_male_uris = [
            "http://id.loc.gov/authorities/demographicTerms/dg2015060359",
            "http://id.loc.gov/authorities/demographicTerms/dg2015060003",
            "http://id.loc.gov/authorities/subjects/sh85080137",
            "http://id.loc.gov/authorities/subjects/sh85083510",
        ]
        self.lc_female_uris = [
            "http://id.loc.gov/authorities/demographicTerms/dg2015060004",
            "http://id.loc.gov/authorities/subjects/sh85047734",
            "http://id.loc.gov/authorities/demographicTerms/dg2015060358",
            "http://id.loc.gov/authorities/subjects/sh85147274",
            "http://id.loc.gov/authorities/subjects/sh2002006249",
        ]

        self.lc_transgender_uris = [
            "http://id.loc.gov/authorities/demographicTerms/dg2015060006",
            "http://id.loc.gov/authorities/subjects/sh2007003708",
        ]

        self.lc_transgender_woman_uris = ["http://id.loc.gov/authorities/subjects/sh2018002623"]

        # or string "transgender man"
        self.lc_transgender_male_uris = ["http://id.loc.gov/authorities/subjects/sh2018002395"]

        self.config = config
        self.parens_re = re.compile("^(.+) \((.+)\)$")
        cfgs = config["all_configs"]
        fn = os.path.join(cfgs.data_dir, "parenthetical_places.json")
        if os.path.exists(fn):
            with open(fn) as fh:
                data = fh.read()
            self.parenthetical_places = json.loads(data)
        else:
            self.parenthetical_places = {}

    def transform(self, record, rectype=None, reference=False):
        try:
            rec = record["data"]
        except:
            if type(record) == list:
                # BAD CALLER!
                rec = record
        if not rec["@graph"] or rec["@graph"] == {}:
            return None

        new = LcMapper.transform(self, record, rectype, reference)

        if new and not rectype:
            rectype = self.guess_type(new)
        if not new or not rectype:
            return None

        recid = new["@id"]
        topcls = getattr(model, rectype)
        top = topcls(ident=recid)

        if reference:
            self.map_label(new, top)
            del top.identified_by
        else:
            self.map_common(new, top)

            if topcls == model.Place:
                # Test if () in name and add a broader if we know it
                # https://id.loc.gov/authorities/names/n96039009.html
                name = top._label.strip()
                if name and (m := self.parens_re.match(name)):
                    (nm, parent) = m.groups()
                    if parent.strip() in self.parenthetical_places:
                        uri = self.parenthetical_places[parent.strip()]
                        top.part_of = model.Place(ident=uri, label=parent)

            # Now fill out the details from RWO
            # if we have one
            if "madsrdf:identifiesRWO" in new:
                rwo = new["madsrdf:identifiesRWO"]
                if type(rwo) == list:
                    for r in rwo:
                        self.process_rwo(r, top)
                elif type(rwo) == dict:
                    self.process_rwo(rwo, top)

                if top.type == "Person":
                    okay = test_birth_death(top)
                    if not okay:
                        try:
                            top.born = None
                            top.died = None
                        except:
                            # This shouldn't ever happen, but not going to die on the hill
                            pass

        js = model.factory.toJSON(top)
        return {"identifier": record["identifier"], "data": js, "source": self.name}

    def process_rwo(self, rwo, top):
        if "madsrdf:gender" in rwo:
            g = rwo["madsrdf:gender"]
            if type(g) == list:
                g = g[0]

            if "rdfs:label" in g:
                txt = g["rdfs:label"]
                if type(txt) == list:
                    txt = txt[0]
                if type(txt) == dict:
                    txt = txt.get("@value", "")
            elif "@id" in g:
                gdr = g["@id"]
                if gdr in self.lc_female_uris:
                    txt = "female"
                elif gdr in self.lc_male_uris:
                    txt = "male"
                else:
                    # print(f"*** [{recid}] Unknown gender id: {g}")
                    txt = ""
            else:
                # print(f"*** [{recid}] Unknown gender format: {g}")
                txt = ""

            # FIXME: Actually make use of the transgender URIs
            # AAT terms

            txt = txt.strip().lower()
            if txt.startswith("("):
                # "(something) value"
                txt = txt.split(" ")[-1]
            if txt in ["male", "males", "men", "man"]:
                gender = vocab.instances["male"]
            elif txt in ["female", "females", "woman", "women"]:
                gender = vocab.instances["female"]
            else:
                # print(f"*** [{recid}] Unknown gender: {txt}")
                gender = None
            if gender:
                top.classified_as = gender

        if "madsrdf:birthDate" in rwo:
            txt = ""
            bd = rwo["madsrdf:birthDate"]
            if type(bd) == list:
                bd = bd[0]
            if type(bd) == dict:
                if "rdfs:label" in bd:
                    txt = bd["rdfs:label"]
                    if type(txt) == list:
                        txt = txt[0]
                    if type(txt) == dict:
                        txt = txt["@value"]
                elif "@value" in bd:
                    txt = bd["@value"]
            elif type(bd) == str:
                txt = bd
            txt = txt.replace("(edtf) ", "").strip()
            try:
                (bdate, edate) = make_datetime(txt)
            except:
                # No date found
                # print(f"*** [{recid}] Unparsable birthDate: {txt}")
                bdate = None
            if bdate:
                birth = model.Birth()
                ts = model.TimeSpan()
                ts.begin_of_the_begin = bdate
                ts.end_of_the_end = edate
                ts.identified_by = vocab.DisplayName(content=txt)
                birth.timespan = ts
                top.born = birth

        if "madsrdf:birthPlace" in rwo:
            bp = rwo["madsrdf:birthPlace"]
            if type(bp) == list:
                bp = bp[0]
            if type(bp) == dict:
                bpid = bp.get("@id", "")
            if type(bp) == str:
                bp = {"rdfs:label": bp}
            if "madsrdf:authoritativeLabel" in bp:
                lbl = bp["madsrdf:authoritativeLabel"]
            elif "rdfs:label" in bp:
                lbl = bp["rdfs:label"]
            else:
                lbl = ""
            if type(lbl) == list:
                lbl = lbl[0]
            if type(lbl) == dict:
                lbl = lbl.get("@value", "")

            txt = lbl.strip()
            if txt and "(" in txt:
                txt = re.sub(r"^\(.*?\)\s*", "", txt)
            if not txt and bpid.startswith("_:"):
                bpid = None
            elif txt and (not bpid or bpid.startswith("_:")):
                bpid = self.build_recs_and_reconcile(txt, "Place")
            if bpid:
                # bpid is full uri
                if "/rwo/" in bpid:
                    ident = bpid.rsplit("/", 1)[-1]
                    where = self.get_reference(ident)
                else:
                    try:
                        src, ident = self.config["all_configs"].split_uri(bpid)
                        where = src["mapper"].get_reference(ident)
                    except:
                        print(f"Failed to split URI: {bpid}")
                        where = None
                if where and where.__class__ == model.Place:
                    if not hasattr(top, "born"):
                        birth = model.Birth()
                        top.born = birth
                    birth.took_place_at = where

        if "madsrdf:deathDate" in rwo:
            txt = ""
            dd = rwo["madsrdf:deathDate"]
            if type(dd) == list:
                dd = dd[0]
            if type(dd) == dict:
                if "rdfs:label" in dd:
                    txt = dd["rdfs:label"]
                    if type(txt) == list:
                        txt = txt[0]
                    if type(txt) == dict:
                        txt = txt["@value"]
                elif "@value" in dd:
                    txt = dd["@value"]
            elif type(dd) == str:
                txt = dd
            txt = txt.replace("(edtf) ", "").strip()

            try:
                (bdate, edate) = make_datetime(txt)
            except:
                # print(f"*** [{recid}] Unparsable deathDate: {txt}")
                bdate = None
            if bdate:
                death = model.Death()
                ts = model.TimeSpan()
                ts.begin_of_the_begin = bdate
                ts.end_of_the_end = edate
                ts.identified_by = vocab.DisplayName(content=txt)
                death.timespan = ts
                top.died = death

        if "madsrdf:deathPlace" in rwo:
            bp = rwo["madsrdf:deathPlace"]
            if type(bp) == list:
                bp = bp[0]
            if type(bp) == dict:
                dpid = bp.get("@id", "")
            if type(bp) == str:
                bp = {"rdfs:label": bp}
            if "madsrdf:authoritativeLabel" in bp:
                lbl = bp["madsrdf:authoritativeLabel"]
            elif "rdfs:label" in bp:
                lbl = bp["rdfs:label"]
            if type(lbl) == list:
                lbl = lbl[0]
            if type(lbl) == dict:
                lbl = lbl.get("@value", "")
            txt = lbl.strip()
            if txt and "(" in txt:
                txt = re.sub(r"^\(.*?\)\s*", "", txt)

            if not txt and dpid.startswith("_:"):
                # nothing to do
                dpid = None
            elif txt and (not dpid or dpid.startswith("_:")):
                dpid = self.build_recs_and_reconcile(txt, "Place")
            if dpid:
                # dpid is full uri
                if "/rwo/" in dpid:
                    ident = dpid.rsplit("/", 1)[-1]
                    where = self.get_reference(ident)
                else:
                    try:
                        src, ident = self.config["all_configs"].split_uri(dpid)
                        where = src["mapper"].get_reference(ident)
                    except:
                        print(f"In record: {top.id}")
                        print(f"Failed to split deathPlace URI: {dpid}")
                        print(f"txt is: {txt}")
                        where = None
                if where and where.__class__ == model.Place:
                    if not hasattr(top, "died"):
                        death = model.Death()
                        top.died = death
                    death.took_place_at = where

        if "madsrdf:fieldOfActivity" in rwo:
            # fieldOfActivity --> professional activity
            # activityStartDate
            # activityEndDate

            foa = rwo["madsrdf:fieldOfActivity"]
            if type(foa) != list:
                foa = [foa]
            for f in foa:
                fid = f["@id"]
                al = ""
                if "madsrdf:authoritativeLabel" in f:
                    al = f["madsrdf:authoritativeLabel"]
                elif "rdfs:label" in f:
                    al = f["rdfs:label"]
                if type(al) == list:
                    al = al[0]
                if type(al) == dict:
                    al = al.get("@value", "")
                if fid.startswith("_:"):
                    if al.startswith("("):
                        al = re.sub(r"^\(.*?\)\s*", "", al)
                    fid = self.build_recs_and_reconcile(al, "Concept")
                if fid:
                    if "authorities/names" in fid:
                        continue
                    act = vocab.Active()
                    act.classified_as = model.Type(ident=fid, label=al)
                    top.carried_out = act
        if "madsrdf:activityStartDate" in rwo:
            print(f"LCNAF start activity: {rwo['madsrdf:activityStartDate']}")
        if "madsrdf:activityEndDate" in rwo:
            print(f"LCNAF end activity: {rwo['madsrdf:activityEndDate']}")

        if "madsrdf:occupation" in rwo:
            # occupation --> classified_as
            occ = rwo["madsrdf:occupation"]
            if type(occ) != list:
                occ = [occ]
            for o in occ:
                oid = o["@id"]
                al = ""
                if "madsrdf:authoritativeLabel" in o:
                    al = o["madsrdf:authoritativeLabel"]
                elif "rdfs:label" in o:
                    al = o["rdfs:label"]
                if type(al) == list:
                    al = al[0]
                if type(al) == dict:
                    al = al.get("@value", "")
                if oid.startswith("_:"):
                    if al.startswith("("):
                        al = re.sub(r"^\(.*?\)\s*", "", al)
                    oid = self.build_recs_and_reconcile(al, "Concept")

                # Can this be /rwo/ ?
                if oid and "names" in oid:
                    # actually member_of
                    top.member_of = model.Group(ident=oid, label=al)
                elif oid:
                    cxn = model.Type(ident=oid, label=al)
                    cxn.classified_as = model.Type(
                        ident="http://vocab.getty.edu/aat/300435108",
                        label="Occupation",
                    )
                    top.classified_as = cxn

        if "madsrdf:hasAffiliation" in rwo:
            # affiliation to organization = member_of Group
            topcls = top.__class__
            orgs = []
            affs = rwo["madsrdf:hasAffiliation"]
            if type(affs) != list:
                affs = [affs]
            for aff in affs:
                if "madsrdf:organization" in aff:
                    # affiliation as member_of
                    org = aff["madsrdf:organization"]
                    if type(org) == list:
                        orgs.extend(org)
                    else:
                        orgs.append(org)
                elif "madsrdf:hasAffiliationAddress" in aff:
                    # affiliation as ... self?
                    affa = aff["madsrdf:hasAffiliationAddress"]
                    if type(affa) != list:
                        affa = [affa]
                    for add in affa:
                        # streetAddress, city, state, postcode
                        bits = []
                        for x in ["streetAddress", "city", "state", "postcode"]:
                            if f"madsrdf:{x}" in add:
                                bit = add[f"madsrdf:{x}"]
                                if type(bit) == list:
                                    bit = bit[0]
                                if type(bit) == dict:
                                    bit = bit["@value"]
                                bits.append(bit)
                        if bits:
                            # print(f"contact bits: {bits}")
                            cp = vocab.StreetAddress(content=", ".join(bits))
                            if topcls in [model.Group, model.Person]:
                                top.contact_point = cp

            for o in orgs:
                lbl = ""
                if "madsrdf:authoritativeLabel" in o:
                    lbl = o["madsrdf:authoritativeLabel"]
                elif "rdfs:label" in o:
                    lbl = o["rdfs:label"]
                if type(lbl) == list:
                    lbl = lbl[0]
                if type(lbl) == dict:
                    lbl = lbl["@value"]
                if lbl.startswith("("):
                    lbl = re.sub(r"^\(.*?\)\s*", "", lbl)
                gid = o.get("@id", "")

                if (gid == "" or gid.startswith("_")) and lbl:
                    gid = self.build_recs_and_reconcile(lbl, "Group")
                if gid:
                    fetchid = gid.rsplit("/", 1)[-1]
                    frec = self.get_reference(fetchid)
                    if frec.__class__ == model.Group:
                        if topcls in [model.Group, model.Person]:
                            top.member_of = frec
                        elif topcls in [model.Activity]:
                            top.carried_out_by = frec

        if "madsrdf:hasCorporateParentAuthority" in rwo:
            print(f"got parent: {rwo['madsrdf:hasCorporateParentAuthority']}")
            pass

        if "madsrdf:establishDate" in rwo:
            txt = ""
            dd = rwo["madsrdf:establishDate"]
            if type(dd) == list:
                dd = dd[0]
            if type(dd) == dict:
                if "rdfs:label" in dd:
                    txt = dd["rdfs:label"]
                    if type(txt) == list:
                        txt = txt[0]
                    if type(txt) == dict:
                        txt = txt["@value"]
                elif "@value" in dd:
                    txt = dd["@value"]
            elif type(dd) == str:
                txt = dd
            txt = txt.replace("(edtf) ", "").strip()
            try:
                (bdate, edate) = make_datetime(txt)
            except:
                # print(f"*** [{recid}] Unparsable establishDate: {txt}")
                bdate = None
            if bdate:
                frm = model.Formation()
                ts = model.TimeSpan()
                ts.begin_of_the_begin = bdate
                ts.end_of_the_end = edate
                ts.identified_by = vocab.DisplayName(content=txt)
                frm.timespan = ts
                top.formed_by = frm
        if "madsrdf:terminateDate" in rwo:
            txt = ""
            dd = rwo["madsrdf:terminateDate"]
            if type(dd) == list:
                dd = dd[0]
            if type(dd) == dict:
                if "rdfs:label" in dd:
                    txt = dd["rdfs:label"]
                    if type(txt) == list:
                        txt = txt[0]
                    if type(txt) == dict:
                        txt = txt["@value"]
                elif "@value" in dd:
                    txt = dd["@value"]
            elif type(dd) == str:
                txt = dd
            txt = txt.replace("(edtf) ", "").strip()
            try:
                (bdate, edate) = make_datetime(txt)
            except:
                # print(f"*** [{recid}] Unparsable terminateDate: {txt}")
                bdate = None
            if bdate:
                dss = model.Dissolution()
                ts = model.TimeSpan()
                ts.begin_of_the_begin = bdate
                ts.end_of_the_end = edate
                ts.identified_by = vocab.DisplayName(content=txt)
                dss.timespan = ts
                top.dissolved_by = dss

        # if 'madsrdf:associatedLanguage' in rwo:
        # if 'madsrdf:associatedLocale' in rwo:
        # Could use AttributeAssignment pattern
