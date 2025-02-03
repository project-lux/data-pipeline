from pipeline.process.base.mapper import Mapper
from cromulent import model, vocab
from pipeline.process.utils.mapper_utils import make_datetime, test_birth_death
from lxml import etree
from collections import Counter

class FastMapper(Mapper):
    """

    this mapper may not need the to_plain_string function,
    as the loader decodes everything to string before storage. however, if we fetch
    across the network, it will still need the conversion.

    """
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

        if not rectype:
            rectype = self.guess_type(root)
            if not rectype:
                return None

        identifier = record["identifier"]
        rec = rectype(ident=f"http://id.worldcat.org/fast/{identifier}")

        field_counter = Counter()
        if isinstance(rectype, model.Group):
            for datafield in root.findall(".//mx:datafield", namespaces=nss):
                tag = datafield.get("tag")
                field_counter[tag] += 1
            print("\n📊 MARC Fields Found in FAST Records:")
            for tag, count in field_counter.most_common(50):  # Show top 50 fields
                print(f"Field {tag}: {count} occurrences")

        #person records

        birth_date = None
        death_date = None
        birth_year = None 
        death_year = None 

        #get birth and death from 046
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

        #get primary name and birth and death from 100
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

        # Extract alternate names and birth and death from 400
        df700 = root.xpath(".//mx:datafield[@tag='700']", namespaces=self.nss)
        df400 = root.xpath(".//mx:datafield[@tag='400']", namespaces=self.nss)
        alternate_names = set()
        equivalents = []

        if df400:
            for df in df400:
                subfield_a = df.find("mx:subfield[@code='a']", self.nss)
                subfield_q = df.find("mx:subfield[@code='q']", self.nss)  # Fuller form
                subfield_d = df.find("mx:subfield[@code='d']", self.nss)  # Dates

                if subfield_a is not None:
                    alt_name = self.to_plain_string(subfield_a.text)
                    alternate_names.add(alt_name)
                if subfield_q is not None:
                    alt_name = self.to_plain_string(subfield_q.text)
                    alternate_names.add(alt_name)

                if subfield_d is not None:
                    dates = self.to_plain_string(subfield_d)
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

        #extract alternate names and equivalents from 700 field
        if df700:
            for df in df700:
                subfield_a = df.find("mx:subfield[@code='a']", namespaces=self.nss)
                subfield_0 = df.find("mx:subfield[@code='0']", namespaces=self.nss)
                subfield_1 = df.find("mx:subfield[@code='1']", namespaces=self.nss)

                if subfield_a is not None:
                    alt_name = self.to_plain_string(subfield_a.text)
                    alternate_names.add(alt_name)

                if subfield_0 is not None:
                    uri_0 = self.to_plain_string(subfield_0.text)
                    if "wikipedia.org" in uri_0:
                        wikidata_qid = self.config['all_configs'].external['wikidata']['reconciler'].get_wikidata_qid(uri_0)
                        if wikidata_qid:
                            wikiuri = self.config['all_configs'].external['wikidata']['namespace'] + wikidata_qid
                            equivalents.append(model.Person(ident=wikiuri))
                    elif uri_0.startswith("(DLC)"):
                        #Should be LCCN
                        uri_0 = "".join(uri_0.split(")")[1].split())
                        lcnafuri = self.config["all_configs"].external['lcnaf']['namespace'] + uri_0
                        equivalents.append(model.Person(ident=lcnafuri))

                if subfield_1 is not None:
                    #VIAF equiv
                    uri_1 = self.to_plain_string(subfield_1.text)
                    equivalents.append(model.Person(ident=uri_1))

        #set equivalents
        if equivalents:
            if not hasattr(rec, "equivalent"):
                rec.equivalent = []
                rec.equivalent.extend(equivalents)

        alternate_names = sorted(list(alternate_names))

        #set alternate names
        if not hasattr(rec,"identified_by"):
            rec.identified_by = []
            rec.identified_by.extend(vocab.AlternateName(content=name) for name in alternate_names)

        # Assign a primary name if none exists
        if not primary and alternate_names:
            primary_name = alternate_names.pop(0)  # Take the first alternate name
            rec.identified_by.insert(0, vocab.PrimaryName(content=primary_name))

        # If no names remain, return None
        if not rec.identified_by:
            return None

        #set birth and death
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

        #test birth and death
        if not test_birth_death(rec):
            rec.born = None
            rec.died = None

        #extract related places from 370
        df370 = root.xpath(".//mx:datafield[@tag='370']", namespaces=self.nss)
        if df370:
            place_subfields = {}
            for d in df370:
                for subfield in d.findall('mx:subfield', namespaces=self.nss):                   
                    code = subfield.attrib.get("code")  
                    if code:
                        text = self.to_plain_string(subfield.text)
                        place_subfields.setdefault(code, []).append(text)
            #birth place
            if "a" in place_subfields:
                for a in place_subfields['a']:
                    bpid = self.build_recs_and_reconcile(a, "place")
                    if bpid:
                        if not hasattr(rec, "born"):
                            birth = model.Birth()
                            rec.born = birth
                        birth.took_place_at = model.Place(ident=bpid)
                        
            #death place
            elif "b" in place_subfields:
                for b in place_subfields['b']:
                    dpid = self.build_recs_and_reconcile(b,"place")
                    if dpid:
                        if not hasattr(rec, "died"):
                            death = model.Death()
                            rec.died = death
                        death.took_place_at = model.Place(ident=dpid)
                        
            #residence
            elif "e" in place_subfields:
                for e in place_subfields['e']:
                    resid = self.build_recs_and_reconcile(e, "place")
                    if resid:
                        rec.residence = model.Place(ident=resid)

        # Extract occupations from 374 fields
        df374 = root.xpath(".//mx:datafield[@tag='374']", namespaces=self.nss)
        classifications = []
        if df374:
            for df in df374:
                subfield_a = df.find("mx:subfield[@code='a']", namespaces=self.nss)
                subfield_0 = df.find("mx:subfield[@code='0']", namespaces=self.nss)
                if subfield_0 is not None:  # Use URI if available
                    occupation_uri = self.to_plain_string(subfield_0.text)
                    classifications.append(model.Type(ident=occupation_uri))
                elif subfield_a is not None: #try to reconcile name
                    occupation = self.to_plain_string(subfield_a.text)
                    try:
                        occupation_uri = self.build_recs_and_reconcile(occupation, "concept")
                        if occupation_uri:
                            classifications.append(model.Type(ident=occupation_uri,label=occupation))
                    except:
                        continue
        

        #extract gender from 375
        df375 = root.xpath(".//mx:datafield[@tag='375']", namespaces=self.nss)
        if df375:
            for df in df375:
                subfield_a = df.find("mx:subfield[@code='a']",namespaces=self.nss)
                subfield_0 = df.find("mx:subfield[@code='0']",namespaces=self.nss)
                if subfield_0 is not None:
                    gender_uri = self.to_plain_string(subfield_0.text)
                    if "wikidata" in gender_uri or gender_uri == "http://id.loc.gov/authorities/subjects/sh2007005819":
                        classifications.append(model.Type(ident=gender_uri))
                elif subfield_a is not None:
                    gender = self.to_plain_string(subfield_a.text)
                    if gender == "male":
                        classifications.append(vocab.instances['male'])
                    elif gender == "female":
                        classifications.append(vocab.instances['female'])

        #set classified_as
        if classifications:
            if not hasattr(rec, "classified_as"):
                setattr(rec, "classified_as", [])
            rec.classified_as.extend(classifications)

        #extract affiliations from 373
        df373 = root.xpath(".//mx:datafield[@tag='373']", namespaces=self.nss)
        affiliations = []
        if df373:
            for df in df373:
                subfield_a = df.find("mx:subfield[@code='a']",namespaces=self.nss)
                subfield_0 = df.find("mx:subfield[@code='0']",namespaces=self.nss)
                if subfield_0 is not None: #Use URI if available
                    affiliation_uri = self.to_plain_string(subfield_0.text)
                    affiliations.append(model.Group(ident=affiliation_uri))
                elif subfield_a is not None: #try to reconcile name
                    affiliation = self.to_plain_string(subfield_a.text)
                    try:
                        affiliation_uri = self.build_recs_and_reconcile(affiliation, "group")
                        if affiliation_uri:
                            affiliations.append(model.Group(ident=affiliation_uri,label=affiliation))
                    except:
                        continue

        #set member_of
        if affiliations:
            if not hasattr(rec,"member_of"):
                setattr(rec,"member_of",[])
            rec.member_of.extend(affiliations)

        #extract notes from 500
        df500 = root.xpath(".//mx:datafield[@tag='500']", namespaces=self.nss)
        biographies = []
        if df500:
            for df in df500:
                subfield_a = df.find("mx:subfield[@code='a']", namespaces=self.nss)
                if subfield_a is not None:
                    bio = self.to_plain_string(subfield_a.text)
                    biographies.append(model.LinguisticObject(content=bio))

        #set referred_to_by Notes
        if biographies:
            if not hasattr(rec,"referred_to_by"):
                setattr(rec,"referred_to_by",[])
            rec.referred_to_by.extend(biographies)

        #Group marc fields

        #primary name
        df110 = root.xpath(".//mx:datafield[@tag='110']", namespaces=self.nss)
        primary = False
        if df110:
            for df in df110:
                subfield_a = df.find("mx:subfield[@code='a']",namespaces=self.nss)
                subfield_b = df.find("mx:subfield[@code='b']",namespaces=self.nss)

                org_name = self.to_plain_string(subfield_a.text) if subfield_a is not None else ""
                sub_unit = self.to_plain_string(subfield_b.text) if subfield_b is not None else ""
                primary_name = f"{org_name}, {sub_unit}" if sub_unit else org_name

                if primary_name:
                    primary = True
                    rec.identified_by = vocab.PrimaryName(content=primary_name)

        #broader
        df510 = root.xpath(".//mx:datafield[@tag='510']", namespaces=self.nss)
        if df510:
            for df in df510:
                subfield_a = df.find("mx:subfield[@code='a']",namespaces=self.nss)
                subfield_0 = df.find("mx:subfield[@code='0']",namespaces=self.nss)
                subfield_w = df.find("mx:subfield[@code='w']",namespaces=self.nss)


                # Check if it's a broader organization (subfield `w` should contain "b")
                relationship_type = self.to_plain_string(subfield_w.text).lower() if subfield_w is not None else None
                if relationship_type == "b":
                    broader_uri = self.to_plain_string(subfield_0.text) if subfield_0 is not None else None

                    if broader_uri:
                        rec.member_of = model.Group(ident=broader_uri)
                    else:
                        # Try reconciling the name if no direct URI is available
                        broader_name = self.to_plain_string(subfield_a.text) if subfield_a is not None else None
                        if broader_name:
                            try:
                                reconciled_uri = self.build_recs_and_reconcile(broader_name, "group")
                                if reconciled_uri:
                                    rec.member_of = model.Group(ident=reconciled_uri, label=broader_name)
                            except:
                                continue



        data = model.factory.toJSON(rec)
        #return {'identifier': identifier, 'data': data, 'source': 'fast'}
        #just hand off to class mappers because each record has specific class tags?

