from pipeline.process.base.mapper import Mapper
from cromulent import model, vocab
from pipeline.process.utils.mapper_utils import make_datetime, test_birth_death
from lxml import etree
import re


class ViafMapper(Mapper):
    def __init__(self, config):
        Mapper.__init__(self, config)

        self.datere = re.compile("^[0-9-]+$")

        self.ignore_sameas = [
            "http://data.bnf.fr/#foaf:Person",
            "http://data.bnf.fr/#foaf:Organization",
            "http://data.bnf.fr/#spatialThing",
            "http://data.bnf.fr/#owl:Thing",
        ]

        self.nss = {
            "viaf": "http://viaf.org/viaf/terms#",
            "foaf": "http://xmlns.com/foaf/0.1/",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        }

        self.nameTypeMap = {"Personal": model.Person, "Corporate": model.Group, "Geographic": model.Place}

        self.viaf_prefixes = {
            "ISNI": "http://isni.org/isni/",
            "WKP": "http://www.wikidata.org/entity/",
            "NDL": "http://id.ndl.go.jp/auth/entity/",
            "DNB": "https://d-nb.info/gnd/",
            "LC": "http://id.loc.gov/authorities/names/",
            "LCSH": "http://id.loc.gov/authorities/subjects/",
            "JPG": "http://vocab.getty.edu/ulan/",
            "ULAN": "http://vocab.getty.edu/ulan/",
            "FAST": "http://id.worldcat.org/fast/",
        }

        self.viaf_nationalities = {
            "de": "300111192",
            "fr": "300111188",
            "us": "300107956",
            "kr": "300018668",
            "pl": "300111204",
            "gb": "300111159",
            "it": "300111198",
            "at": "300111153",
            "ch": "300111221",
            "ca": "300107962",
            "jp": "300018519",
            "ru": "300111276",
            "nl": "300111175",
            "pt": "300111207",
            "es": "300111215",
            "se": "300111218",
            "no": "300111201",
            "cn": "300018322",
            "lt": "300379451",
            "in": "300018863",
            "be": "300111156",
            "br": "300107967",
            "au": "300021861",
            "cz": "300111166",
            "hu": "300111195",
            "dk": "300111172",
            "sk": "300386533",
            "ua": "300380343",
            "ar": "300107965",
            "ee": "300387725",
            "tr": "300193868",
            "mx": "300107963",
            "gr": "300264816",
            "il": "300195487",
            "ro": "300111210",
            "fi": "300111181",
        }

        self.wikidata_config = config["all_configs"].external["wikidata"]

    def guess_type(self, rec, nameType=None):
        if nameType is None:
            nss = self.nss
            top = self.parse_xml(rec["xml"])
            if top is None:
                return None
            nameType = top.xpath("./viaf:nameType/text()", namespaces=nss)[0]

        topCls = self.nameTypeMap.get(nameType, None)
        return topCls

    def fix_identifier(self, identifier):
        if identifier.startswith("sourceID/"):
            return None
        else:
            return identifier

    def parse_xml(self, xml):
        nss = self.nss
        if "<?xml" in xml:
            xml = bytes(xml, "utf-8")
        try:
            dom = etree.XML(xml)
        except:
            raise
            return None
        try:
            top = dom.xpath("/viaf:VIAFCluster", namespaces=nss)[0]
        except:
            # Likely a redirection record
            # print(f"No cluster: {xml}")
            return None
        return top

    def transform(self, record, rectype=None, reference=False):
        nss = self.nss
        rec = record["data"]
        xml = rec["xml"]

        top = self.parse_xml(rec["xml"])
        if top is None:
            return top

        nameType = top.xpath("./viaf:nameType/text()", namespaces=nss)[0]
        topCls = self.guess_type(rec, nameType)
        if not topCls:
            if nameType != "UniformTitleWork":
                print(f"VIAF mapper saw nameType {nameType} but cannot process")
            return None

        ident = record["identifier"]
        rec = topCls(ident=f"http://viaf.org/viaf/{ident}")
        names = top.xpath("./viaf:mainHeadings/viaf:data", namespaces=nss)

        primary = False
        # According to VIAF all of these are primary ... but no language data
        # So pick one most likely to be english and useful
        for n in names:
            val = n.xpath("./viaf:text/text()", namespaces=nss)
            srcs = n.xpath("./viaf:sources/viaf:s/text()", namespaces=nss)
            if val and val[0]:
                val = val[0]
                if not primary and ("JPG" in srcs or "LC" in srcs or "LCSH" in srcs or "ULAN" in srcs):
                    primary = val
                    rec.identified_by = vocab.PrimaryName(content=primary)
                    rec._label = primary
                elif "WKP" in srcs and len(names) > 1:
                    # skip it
                    continue
                else:
                    rec.identified_by = model.Name(content=val)
        if not hasattr(rec, "identified_by"):
            # No names, skip
            print("No names")
            return None
        if not primary:
            primary = rec.identified_by[0]
            primary.classified_as = vocab.instances["primary"]
            rec._label = primary.content

        # DNB in VIAF is broken, need to use @nsid

        nsequivs = top.xpath("./viaf:sources/viaf:source/@nsid", namespaces=nss)
        done = {}
        for eq in nsequivs:
            if eq.startswith("http://d-nb.info/gnd/") and not eq in done:
                done[eq] = True
            elif eq.startswith("http://catalogue.bnf.fr/") and not eq in done:
                done[eq] = True
                eq = eq.replace("http://catalogue", "https://data")
                done[eq] = True
            else:
                continue
            rec.equivalent = topCls(ident=eq, label=rec._label)

        equivs = top.xpath("./viaf:sources/viaf:source/text()", namespaces=nss)
        wdm = self.wikidata_config["mapper"]
        for eq in equivs:
            (which, val) = eq.split("|")
            if which == "LC" and val[0] == "s":
                which = "LCSH"
            elif which in ["DNB", "BNF"]:
                # processed via @nsid above for now
                continue
            elif which == "FAST":
                val = val.replace("fst", "")
            if which in self.viaf_prefixes:
                val = val.replace(" ", "")  # eg sometimes LC is "n  123456" and should be n123456

                # Only include Wikidata references from VIAF if they guess to the right class
                if which == "WKP":
                    wdeq = wdm.get_reference(val)
                    if wdeq is not None and wdeq.type == rec.type:
                        rec.equivalent = wdeq
                else:
                    rec.equivalent = topCls(ident=f"{self.viaf_prefixes[which]}{val}", label=rec._label)

        if nameType in ["Personal", "Corporate"]:
            birthdate = top.xpath("./viaf:birthDate/text()", namespaces=nss)
            deathdate = top.xpath("./viaf:deathDate/text()", namespaces=nss)
            dateType = top.xpath("./viaf:dateType/text()", namespaces=nss)

            if dateType and dateType[0]:
                # lived, flourished
                dateType = dateType[0]

            if dateType == "lived":
                # born and died

                if birthdate and birthdate[0]:
                    bd = birthdate[0].strip()
                    if bd != "0":
                        try:
                            b, e = make_datetime(bd)
                        except:
                            b = None
                        if b and e:
                            birth = model.Birth()
                            ts = model.TimeSpan()
                            ts.begin_of_the_begin = b
                            ts.end_of_the_end = e
                            birth.timespan = ts
                            rec.born = birth
                            birth.identified_by = vocab.DisplayName(content=bd)
                if deathdate and deathdate[0]:
                    dd = deathdate[0].strip()
                    if dd not in ["0", "2050", "9800"]:
                        try:
                            b, e = make_datetime(dd)
                        except:
                            b = None
                        if b and e:
                            death = model.Death()
                            ts = model.TimeSpan()
                            ts.begin_of_the_begin = b
                            ts.end_of_the_end = e
                            death.timespan = ts
                            rec.died = death
                            death.identified_by = vocab.DisplayName(content=dd)
            elif dateType == "flourished":
                if birthdate and birthdate[0] and deathdate and deathdate[0]:
                    bd = birthdate[0].strip()
                    dd = deathdate[0].strip()
                    if bd != "0" and dd not in ["0", "2050", "9800"]:
                        try:
                            b, be = make_datetime(bd)
                        except:
                            b = None
                        try:
                            e, ee = make_datetime(dd)
                        except:
                            e = None
                        if b and e:
                            active = vocab.Active()
                            ts = model.TimeSpan()
                            ts.begin_of_the_begin = b
                            ts.end_of_the_end = e
                            active.timespan = ts
                            rec.carried_out = active
                            ts.identified_by = vocab.DisplayName(content=f"{bd} to {dd}")

            if nameType == "Personal":
                gender = top.xpath("./viaf:fixed/viaf:gender/text()", namespaces=nss)
                if gender and gender[0]:
                    # a = female, b = male, u = unknown, x = unknown
                    g = gender[0].strip()
                    if g == "a":
                        rec.classified_as = vocab.instances["female"]
                    elif g == "b":
                        rec.classified_as = vocab.instances["male"]
                    elif not g in ["u", "x"]:
                        print(f"Unknown gender: {g} in {what}")

                nat_list = top.xpath("./viaf:nationalityOfEntity/viaf:data/viaf:text/text()", namespaces=nss)
                for nat in nat_list:
                    nat = nat.lower()
                    if len(nat) == 2 and nat in self.viaf_nationalities:
                        # add it to the record
                        ident = self.viaf_nationalities[nat]
                        aaturi = f"http://vocab.getty.edu/aat/{ident}"
                        nationalityType = vocab.Nationality(ident=aaturi, label=nat)
                        rec.classified_as = nationalityType

        if rec.type == "Person":
            okay = test_birth_death(rec)
            if not okay:
                try:
                    rec.born = None
                    rec.died = None
                except:
                    # This shouldn't ever happen, but not going to die on the hill
                    pass

        new = model.factory.toJSON(rec)
        return {"identifier": record["identifier"], "data": new, "source": "viaf"}

class FastMapper(Mapper):
    def __init__(self, config):
        super().__init__(config)

        self.nss = {
            'mx': 'http://www.loc.gov/MARC21/slim'
            }

        self.nameTypeMap = {
            "148": model.Period,
            "100": model.Person, 
            "150": model.Type,
            "155": model.Type,
            "151": model.Place,
            "110": model.Group,
            "111": model.Activity,
            "147": model.Activity
            }

    def build_recs_and_reconcile(self, txt, rectype=""):
        # reconrec returns URI

        rec = {
            "type": "",
            "identified_by": [
                {
                    "type": "Name",
                    "content": txt,
                    "classified_as": [{"id": "http://vocab.getty.edu/aat/300404670"}],
                }
            ],
        }
        if rectype == "place":
            rec["type"] = "Place"
            reconrec = self.config["all_configs"].external['lcnaf']['reconciler'].reconcile(rec, reconcileType="name")
        elif rectype == "concept":
            rec["type"] = "Type"
            reconrec = self.config["all_configs"].external["lcsh"]["reconciler"].reconcile(rec, reconcileType="name")
        elif rectype == "group":
            rec["type"] = "Group"
            reconrec = self.config["all_configs"].external['lcnaf']['reconciler'].reconcile(rec, reconcileType="name")

        return reconrec

    def guess_type(self, root):
        #148 (chronological): period
        #100 (personal): person
        #150 (topical), 155 (genre/form): concept
        #151 (geographic): place
        #110 (corporate): group
        #111 (meeting) meetings, conferences (events)
        #147 (event): events
        #130 (title): return None
        nss = self.nss
        tags = ["148","100","150","151","155","110","111","147","130"]
        for tag in tags:
            if root.find(f".//mx:datafield[@tag='{tag}']", self.nss) is not None:
                return self.nameTypeMap.get(tag, None)

        return None 

    def transform(self, record, rectype=None, reference=False):
        rec = record["data"]
        root = etree.fromstring(rec['xml'])

        if root is None:
            return root

        topCls = self.guess_type(root)
        if not topCls:
            return None

        identifier = record["identifier"]
        rec = topCls(ident=f"http://id.worldcat.org/fast/{identifier}")

        #person records

        birth_date = None
        death_date = None
        birth_year = None 
        death_year = None 
        df046 = root.xpath(".//mx:datafield[@tag='046']", namespaces=self.nss)
        if df046:
            for d in df046:
                subfield_f = d.find("mx:subfield[@code='f']", self.nss)
                if subfield_f is not None:
                    birth_year = self.to_plain_string(subfield_f.text)
                    try:
                        birth_date = make_datetime(birth_year)
                    except Exception as e:
                        print(f"Failed to parse birth date: {birth_year}, Error: {e}")
                
                subfield_g = d.find("mx:subfield[@code='g']", self.nss)
                if subfield_g is not None:
                    death_year = self.to_plain_string(subfield_g.text)
                    try:
                        death_date = make_datetime(death_year)
                    except Exception as e:
                        print(f"Failed to parse death date: {death_year}, Error: {e}")

        df100 = root.xpath(".//mx:datafield[@tag='100']", namespaces=self.nss)
        primary = False
        if df100:
            name_subfields = {}
            for df in df100:
                for s in df.findall("mx:subfield", self.nss):
                    code = s.attrib.get("code")
                    if code:
                        text = self.to_plain_string(s.text)
                        name_subfields.setdefault(code, []).append(text)

            if "a" in name_subfields:
                primary = True
                rec.identified_by = vocab.PrimaryName(content=name_subfields['a'][0])

            if not birth_date and "d" in name_subfields:
                dates = name_subfields['d'][0]
                if "-" in dates:
                    b,e = dates.split("-")
                    try:
                        bb, eb = make_datetime(b)
                    except:
                        bb = None
                    try:
                        be, ee = make_datetime(e)
                    except:
                        be = None
                    if bb and be:
                        birth_date = bb 
                        death_date = be
                elif len(dates) == 4:
                    #birth or death, who knows??
                    pass

        if birth_date:
            birth = model.Birth()
            ts = model.TimeSpan()
            ts.begin_of_the_begin = birth_date
            birth.timespan = ts
            bcont = birth_year if birth_year else b
            birth.identified_by = vocab.DisplayName(content=bcont)
            rec.born = birth

        if death_date:
            death = model.Death()
            ts = model.TimeSpan()
            ts.begin_of_the_begin = death_date
            death.timespan = ts
            dcont = death_year if death_year else e
            death.identified_by = vocab.DisplayName(content=dcont)
            rec.died = death


        if not test_birth_death(rec):
            rec.born = None
            rec.died = None

        # Extract alternate names and identifiers from 700 fields
        df700 = root.xpath(".//mx:datafield[@tag='700']", namespaces=self.nss)
        alternate_names = []
        equivalents = []
        if df700:
            for df in df700:
                subfield_a = df.find("mx:subfield[@code='a']", namespaces=self.nss)
                subfield_0 = df.find("mx:subfield[@code='0']", namespaces=self.nss)
                subfield_1 = df.find("mx:subfield[@code='1']", namespaces=self.nss)

                if subfield_a is not None:
                    alt_name = self.to_plain_string(subfield_a.text)
                    alternate_names.append(vocab.AlternateName(content=alt_name))

                if subfield_0 is not None:
                    uri_0 = self.to_plain_string(subfield_0.text)
                    print(f"Found 700$0: {uri_0}")
                    if "wikipedia.org" in uri_0:
                        wikidata_qid = self.get_wikidata_qid(uri_0)
                        if wikidata_qid:
                            equivalents.append(model.Person(ident=wikidata_qid))
                    elif uri_0.startswith("(DLC)"):
                        #Should be LCCN
                        uri_0 = "".join(uri_0.split(")")[1].split())
                        lcnafuri = self.config["all_configs"].external['lcnaf']['namespace'] + uri_0
                        equivalents.append(model.Person(ident=lcnafuri))


                if subfield_1 is not None:
                    #VIAF equiv
                    uri_1 = self.to_plain_string(subfield_1.text)
                    equivalents.append(model.Person(ident=uri_1))

        if not primary and alternate_names:
            rec.identified_by = vocab.PrimaryName(content=alternate_names[0].content)
            alternate_names = alternate_names[1:]
            rec.identified_by.extend(alternate_names)
        if not primary and not alternate_names:
            #no names
            return None

        if equivalents:
            if not hasattr(rec, "equivalent"):
                setattr(rec, "equivalent", [])
            rec.equivalent.extend(equivalents)

        df370 = root.xpath(".//mx:datafield[@tag='370']", namespaces=self.nss)
        if df370:
            place_subfields = {}
            for d in df370:
                for subfield in d.findall('mx:subfield', namespaces=self.nss):                   
                    code = subfield.attrib.get("code")  
                    if code:
                        text = self.to_plain_string(subfield.text)
                        place_subfields.setdefault(code, []).append(text)

            if "a" in place_subfields:
                for a in place_subfields['a']:
                    bpid = self.build_recs_and_reconcile(a, "place")
                    if bpid:
                        try:
                            src, ident = self.config["all_configs"].split_uri(bpid)
                            where = src["mapper"].get_reference(ident)
                        except:
                            print(f"Failed to split URI: {bpid}")
                            where = None
                        if where and where.__class__ == model.Place:
                            if not hasattr(rec, "born"):
                                birth = model.Birth()
                                rec.born = birth
                            birth.took_place_at = where
            elif "b" in place_subfields:
                for b in place_subfields['b']:
                    rpid = self.build_recs_and_reconcile(b,"place")
                    if rpid:
                        try:
                            src, ident = self.config["all_configs"].split_uri(rpid)
                            where = src["mapper"].get_reference(ident)
                        except:
                            print(f"Failed to split URI: {rpid}")
                            where = None
                        if where and where.__class__ == model.Place:
                            rec.residence = where
            elif "e" in place_subfields:
                for e in place_subfields['e']:
                    plpid = self.build_recs_and_reconcile(e, "place")
                    if plpid:
                        try:
                            src, ident = self.config["all_configs"].split_uri(plpid)
                            where = src["mapper"].get_reference(ident)
                        except:
                            print(f"Failed to split URI: {plpid}")
                            where = None
                        if where and where.__class__ == model.Place:
                            if not hasattr(rec,"carried_out"):
                                active = vocab.Active()
                                rec.carried_out = active
                            active.took_place_at = where

        # Extract occupations from 374 fields
        df374 = root.xpath(".//mx:datafield[@tag='374']", namespaces=self.nss)
        occupations = []
        if df374:
            for df in df374:
                subfield_a = df.find("mx:subfield[@code='a']", self.nss)
                subfield_0 = df.find("mx:subfield[@code='0']", self.nss)
                if subfield_0 is not None:  # Use URI if available
                    occupation_uri = self.to_plain_string(subfield_0.text)
                    occupations.append(model.Type(ident=occupation_uri))
                elif subfield_a is not None:
                    occupation = self.to_plain_string(subfield_a.text)
                    try:
                        occupation_uri = self.build_recs_and_reconcile(occupation, "type")
                        if occupation_uri:
                            src, ident = self.config['all_configs'].split_uri(occupation_uri)
                            what = src['mapper'].get_reference(ident)
                            if what and what.__class__ == model.Type:
                                occupations.append(model.Type(ident=occupation))
                    except:
                        continue
        
        if occupations:
            if not hasattr(rec, "classified_as"):
                setattr(rec, "classified_as", [])
            rec.classified_as.extend(occupations)

        data = model.factory.toJSON(rec)
        return {'identifier': identifier, 'data': data, 'source': 'fast'}
        #just hand off to class mappers because each record has specific class tags?

