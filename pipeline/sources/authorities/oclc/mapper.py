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

                if which == "WKP":
                    wdeq = wdm.get_reference(val)
                    if wdeq.type == rec.type:
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
