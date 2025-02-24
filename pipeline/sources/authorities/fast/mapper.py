from pipeline.process.base.mapper import Mapper
from cromulent import model, vocab
from pipeline.process.utils.mapper_utils import make_datetime, test_birth_death
from lxml import etree
import re

class FastMapper(Mapper):
    """
    
    this mapper may not need the to_plain_string function,
    as the loader decodes everything to string before storage. 
    however, if data is retrieved another way, it will need the conversion.

    """
    def __init__(self, config):
        super().__init__(config)

        self.nss = {'mx': 'http://www.loc.gov/MARC21/slim'}

        self.nameTypeMap = {
            "148": model.Period, "100": model.Person, "150": model.Type,
            "155": model.Type, "151": model.Place, "110": model.Group,
            "111": model.Activity, "147": model.Activity
        }


    def build_recs_and_reconcile(self, txt, rectype=""):
        """Creates a record and reconciles it to an external source. Returns a URI."""

        if not txt:
            return None 

        rec = {
            "type": rectype.capitalize(),
            "identified_by": [{
                    "type": "Name", "content": txt,
                    "classified_as": [{"id": "http://vocab.getty.edu/aat/300404670"}],
                }],
        }

        source_map = {
            "place":"lcnaf", "concept":"lcsh", "group":"lcnaf"
        }

        source = source_map.get(rectype, None)
        if source:
            reconciler = self.config['all_configs'].external[source]['reconciler']
            return reconciler.reconcile(rec, reconcileType="name")

        return None

    def guess_type(self, root):
        """Guesses entity type based on MARC data fields"""
        for tag in self.nameTypeMap.keys():
            if root.find(f".//mx:datafield[@tag='{tag}']", self.nss) is not None:
                return self.nameTypeMap[tag]

        return None
    
    def extract_numeric_id(self, marc_id):
        """Extracts the numeric portion of an FST identifier"""
        match = re.search(r"fst0*(\d+)",marc_id)
        return match.group(1) if match else None

    def extract_datafields(self, root, tag, subfields):
        """Extracts and returns data from specific MARC subfields"""
        data = {}
        fields = root.xpath(f".//mx:datafield[@tag='{tag}']", namespaces=self.nss)
        for field in fields:
            for subfield in subfields:
                sf = field.find(f"mx:subfield[@code='{subfield}']", self.nss)
                if sf is not None and sf.text:
                    data.setdefault(subfield, []).append(self.to_plain_string(sf.text))
        return data

    def process_agent(self, root, rec):
        """Processes common fields for People and Groups"""

        # Extract affiliations (373)
        membership = [] 
        affiliations = self.extract_datafields(root, '373', ['a', '0'])
        for uri in affiliations.get('0', ['']):
            membership.append(model.Group(ident=uri))
        for name in affiliations.get('a', ['']):
            uri = self.build_recs_and_reconcile(name, "group")
            if uri:
                membership.append(model.Group(ident=uri, label=name))
        
        # Set member_of
        if membership:
            rec.member_of = getattr(rec, "member_of", []) + membership

        # Extract occupations (374)
        classifications = []
        occupations = self.extract_datafields(root, '374',['a','0'])
        for uri in occupations.get('0', ['']):
            classifications.append(model.Type(ident=uri))
        for name in occupations.get('a',['']):
            uri = self.build_recs_and_reconcile(name, "type")
            if uri:
                classifications.append(model.Type(ident=uri, label=name))

        # Set classification
        if classifications:
            rec.classified_as = getattr(rec, "classified_as", []) + classifications


        # Extract residence (370 or 551)
        # Prefer 370
        locations = self.extract_datafields(root, '370', ['c','e'])
        if locations.get('c') or locations.get('e'):
            for assoc_place, residence in zip(locations.get('c',['']), locations.get('e',[''])):
                if residence:
                    rpid = self.build_recs_and_reconcile(residence, 'place')
                    if rpid:
                        rec.residence = model.Place(ident=rpid, label=residence)
                elif assoc_place:
                    rpid = self.build_recs_and_reconcile(assoc_place, 'place')
                    if rpid:
                        rec.residence = model.Place(ident=rpid, label=assoc_place)
        else:
        # Try 551
            locations = self.extract_datafields(root, '551',['a'])
            for place in locations.get('a',['']):
                rpid = self.build_recs_and_reconcile(place, "place")
                if rpid:
                    rec.residence = model.Place(ident=rpid, label=place)

        # Extract and set professional activity (372)
        activities = self.extract_datafields(root, '372',['a','s','t'])
        for field_of_activity, work_start, work_end in zip(
            activities.get('a', ['']), activities.get('s', ['']), activities.get('t', [''])
        ):
            if field_of_activity or work_start or work_end:
                activity = vocab.Active()
            
            if field_of_activity:
                fpid = self.build_recs_and_reconcile(field_of_activity, "concept")
                if fpid:
                    activity.classified_as = model.Type(ident=fpid, label=field_of_activity)
                    rec.carried_out = activity
            
            if work_start or work_end:
                ts = model.TimeSpan()
                bstart = bend = None
                dstart = dend = None

                if work_start:
                    try:
                        bstart, bend = make_datetime(work_start)
                    except:
                        pass
                if work_end:
                    try:
                        dstart, dend = make_datetime(work_end)
                    except:
                        pass

                if bstart:
                    ts.begin_of_the_begin = bstart
                    ts.end_of_the_begin = bend
                if dstart:
                    ts.begin_of_the_end = dstart
                    ts.end_of_the_end = dend

                if bstart or dstart:
                    activity.timespan = ts

    def process_person(self, root, rec):
        ##FIXME: test names and don't add alts if same as primary
        
        """Processes Person-only fields"""
        df100_data = self.extract_datafields(root, '100', ['a', 'd'])
        df046_data = self.extract_datafields(root, '046', ['f', 'g'])
        df400_data = self.extract_datafields(root, '400', ['a', 'q', 'd'])
        df700_data = self.extract_datafields(root, '700', ['a', '0', '1'])
        df378_data = self.extract_datafields(root, '378', ['a', 'q'])
        df450_data = self.extract_datafields(root, '450', ['a'])
        df410_data = self.extract_datafields(root, '410', ['a'])

        # Extract primary name
        primary_name = df100_data.get('a',[None])[0]

        # Extract potential alternate names (400, 700)
        alternate_names = set(
            df400_data.get('a', [None]) + 
            df400_data.get('q', [None]) + 
            df700_data.get('a', [None]) + 
            df378_data.get('a', [None]) + 
            df378_data.get('q', [None]) +
            df450_data.get('a', [None]) +
            df410_data.get('a', [None])
        )

        if primary_name:
            alternate_names.discard(primary_name)  # Remove primary name if it exists in alternates

        # Assign primary name (100) if available
        if primary_name:
            rec.identified_by = [vocab.PrimaryName(content=primary_name)]
        else:
            rec.identified_by = []

        # Add alternate names
        rec.identified_by.extend(vocab.AlternateName(content=name) for name in alternate_names)

        # Assign a primary name from alternates if none exists
        if not any(isinstance(name, vocab.PrimaryName) for name in rec.identified_by) and alternate_names:
            primary_name = alternate_names.pop(0) 
            rec.identified_by.insert(0, vocab.PrimaryName(content=primary_name))

        # If no names remain, return None
        if not rec.identified_by:
            return None

        # Extract birth and death dates (046)
        birth_date = make_datetime(df046_data.get('f', [''])[0]) if 'f' in df046_data else None
        death_date = make_datetime(df046_data.get('g', [''])[0]) if 'g' in df046_data else None

        # Extract birth and death dates (100, 400) if not set
        
        for data in [df100_data, df400_data]:
            if 'd' in data:
                dates = data['d'][0]
                if "-" in dates:
                    b, e = dates.split("-")
                    birth_tmp = make_datetime(b) if b else None
                    death_tmp = make_datetime(e) if e else None

                    if not birth_date and birth_tmp:
                        birth_date = birth_tmp
                    if not death_date and death_tmp:
                        death_date = death_tmp
                    
                    if birth_date and death_date:
                        break

        # Set birth and death timespans
        if birth_date:
            birth = model.Birth(timespan=model.TimeSpan(begin_of_the_begin=birth_date))
            rec.born = birth
        
        if death_date:
            death = model.Death(timespan=model.TimeSpan(begin_of_the_begin=death_date))
            rec.died = death
        
        if not test_birth_death(rec):
            rec.born = None
            rec.died = None

        # Extract birth and death places (370)
        df370_data = self.extract_datafields(root, '370', ['a', 'b'])

        # Set birth and death places
        birth_place = df370_data.get('a', [None])[0]
        death_place = df370_data.get('b', [None])[0]
        if birth_place:
            bpid = self.build_recs_and_reconcile(birth_place, "place")
            if bpid:
                if not hasattr(rec, "born"):
                    rec.born = model.Birth()
                rec.born.took_place_at = model.Place(ident=bpid, label=birth_place)
        
        if death_place:
            dpid = self.build_recs_and_reconcile(death_place, "place")
            if dpid:
                if not hasattr(rec, "died"):
                    rec.died = model.Death()
                rec.died.took_place_at = model.Place(ident=dpid, label=death_place)

        # Extract equivalents (700)
        uri_0 = df700_data.get('0', [None])

        equivalents = []
        for uri in uri_0:
            # Wikidata and LCNAF
            if "wikipedia.org" in uri:
                wikidata_qid = self.config['all_configs'].external['wikidata']['reconciler'].get_wikidata_qid(uri)
                if wikidata_qid:
                    wikiuri = self.config['all_configs'].external['wikidata']['namespace'] + wikidata_qid
                    equivalents.append(model.Person(ident=wikiuri))
            elif uri.startswith("(DLC)"):
                #Should be LCCN
                lcnaf_uri = self.config["all_configs"].external['lcnaf']['namespace'] + "".join(uri.split(")")[1].split())
                equivalents.append(model.Person(ident=lcnaf_uri))
        # VIAF
        equivalents.extend(model.Person(ident=uri) for uri in df700_data.get('1', [None]) if uri)

        # Set equivalents
        if equivalents:
            if not hasattr(rec, "equivalent"):
                rec.equivalent = []
                rec.equivalent.extend(equivalents)
                        
        # Extract gender (375)
        df375_data = self.extract_datafields(root, '375', ['a', '0'])
        genders = []
        for uri in df375_data.get('0',['']):
            if "wikidata" in uri or uri == "http://id.loc.gov/authorities/subjects/sh2007005819":
                genders.append(vocab.Gender(ident=uri))
        for gender in df375_data.get('a', ['']):
            if gender.lower() in ("male", "males"):
                genders.append(vocab.instances['male'])
            elif gender.lower() in ("female", "females"):
                genders.append(vocab.instances['female'])

        # Set gender
        if genders:
            rec.classified_as = getattr(rec, "classified_as", []) + genders

        
        # Extract biographical note (500)
        df500_data = self.extract_datafields(root, '500', ['a'])
        biographies = [model.LinguisticObject(content=bio) for bio in df500_data.get('a', [None]) if bio]

        # Set biographical note
        if biographies:
            rec.referred_to_by = getattr(rec, "referred_to_by", []) + biographies
        
        # Send to process_agent for common fields
        self.process_agent(root, rec) 

    def process_group(self, root, rec):
        """Processes organizational details, including names and classifications."""

        df110_data = self.extract_datafields(root, '110', ['a', 'b'])
        df410_data = self.extract_datafields(root, '410', ['a', 'b'])
        df710_data = self.extract_datafields(root, '710', ['a', 'b'])
        df411_data = self.extract_datafields(root, '411', ['a'])


        primary = False
        alternate_names = []

        # Extract primary (110)
        if df110_data.get('a'):
            org_name = df110_data.get('a', [None])[0]
            sub_unit = df110_data.get('b', [None])[0]
            primary_name = f"{org_name}, {sub_unit}" if sub_unit else org_name
            rec.identified_by = [vocab.PrimaryName(content=primary_name)]
            primary = True
        else:
            rec.identified_by = []

        # Extract alternates (410, 710, 411, 378)
        # Set one as primary if 110 did not exist

        for data in [df410_data, df710_data, df411_data]:
            for org_name, sub_unit in zip(data.get('a', ['']), data.get('b', [''])):
                name = f"{org_name}, {sub_unit}" if sub_unit else org_name
                if not primary:
                    rec.identified_by = [vocab.PrimaryName(content=name)]
                    primary = True
                else:
                    alternate_names.append(vocab.AlternateName(content=name))
        
        rec.identified_by.extend(alternate_names)

        # If no names remain, return None
        if not rec.identified_by:
            return None

        # Extract and set classification (368)
        df368_data = self.extract_datafields(root, '368', ['a'])
        for classification in df368_data.get('a',['']):
            try:
                reconciled_uri = self.build_recs_and_reconcile(classification, "concept")
                if reconciled_uri:
                    rec.classified_as = getattr(rec, "classified_as", []) + [model.Type(ident=reconciled_uri, label=classification)]
            except:
                continue

        self.process_agent(root, rec)


    def transform(self, record, rectype=None, reference=False):
        """Transforms a MARC record into Linked.art JSON."""
        try:
            root = etree.fromstring(record["data"]['xml'])
        except Exception as e:
            print(f"Error parsing XML: {e}")
            return None  # Return None if XML parsing fails

        identifier = record["identifier"]

        if not rectype:
            crmcls = self.guess_type(root)
            if not crmcls:
                return None
            rectype = crmcls.__name__
        else:
            crmcls = getattr(model, rectype, None)


        rec = crmcls(ident=f"http://id.worldcat.org/fast/{identifier}")

        if not reference:
            typ = rectype.lower()
            process_fn = getattr(self, f"process_{typ}", None)
            if process_fn:
                process_fn(root, rec)
        else:
            pass
            # Add functionality for get_reference

        return {'identifier': identifier, 'data': model.factory.toJSON(rec), 'source': 'fast'}
