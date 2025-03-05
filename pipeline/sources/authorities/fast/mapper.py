from pipeline.process.base.mapper import Mapper
from cromulent import model, vocab
from pipeline.process.utils.mapper_utils import make_datetime, test_birth_death, get_wikidata_qid
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
            "place":"lcnaf", "type":"lcsh", "group":"lcnaf"
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

    def fast_id_to_uri(self, fast_id):
        numeric_part = fast_id.replace("(OCoLC)fst", "").lstrip("0")
        return f"http://id.worldcat.org/fast/{numeric_part}"

    def extract_datafields(self, root, tag, subfields):
        """Extracts and returns data from specific MARC subfields"""
        data = {}
        fields = root.xpath(f".//mx:datafield[@tag='{tag}']", namespaces=self.nss)
        for field in fields:
            for subfield in subfields:
                sf_list = field.findall(f"mx:subfield[@code='{subfield}']", self.nss)
                for sf in sf_list:
                    if sf.text is not None:
                        data.setdefault(subfield, []).append(self.to_plain_string(sf.text.rstrip(',')))
        return data

    def process_agent(self, root, rec):
        """Processes common fields for People and Groups"""

        # Extract affiliations (373 or 510)
        membership = set() 
        affiliations = self.extract_datafields(root, '373', ['a', '0'])
        if '0' in affiliations and any(affiliations['0']):
            for uri in affiliations['0']:
                if uri and uri.startswith("http://id.loc.gov/"):
                    membership.add(uri)
        else: #Only try to process name if uri doesn't exist, to avoid adding duplicate group membership
            for name in affiliations.get('a',[]):
                if name:
                    uri = self.build_recs_and_reconcile(name, "group")
                    if uri:
                        membership.add(uri)

        related = self.extract_datafields(root, '510',['a','0'])
        if '0' in related and any(related['0']):
            for uri in related['0']:
                if uri and uri.startswith("(OCoLC)fst"):
                    uri = self.fast_id_to_uri(uri)
                    membership.add(uri)
        else:
            for name in related.get("a",[]):
                if name:
                    uri = self.build_recs_and_reconcile(name, "group")
                    if uri:
                        membership.add(uri)
        
        # Set member_of
        if membership:
            for mem in membership:
                rec.member_of = model.Group(ident=mem)

        # Extract occupations (374)
        classifications = set()
        occupations = self.extract_datafields(root, '374',['a','0'])
        for uri in occupations.get('0',[]):
            if uri:
                classifications.add(uri)
        for occ_name in occupations.get('a',[]):
            if occ_name:
                uri = self.build_recs_and_reconcile(occ_name.lower(), "type")
                if uri:
                    classifications.add(uri)

        # Extract classification (368)
        df368_data = self.extract_datafields(root, '368', ['a'])
        for cxn_name in df368_data.get('a',[]):
            if cxn_name:
                uri = self.build_recs_and_reconcile(cxn_name.lower(), "type")
                if uri:
                    classifications.add(uri)


        # Set classification
        if classifications:
            for cxn in classifications:
                rec.classified_as = model.Type(ident=cxn)

        # Extract residence (370 or 551)
        # Prefer 370
        locations = self.extract_datafields(root, '370', ['c','e'])
        assoc_places = locations.get('c',[])

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
            for place in locations.get('a',[]):
                if place:
                    rpid = self.build_recs_and_reconcile(place, "place")
                    if rpid:
                        rec.residence = model.Place(ident=rpid, label=place)

        # Extract and set professional activity (372)
        activities = self.extract_datafields(root, '372',['a','s','t'])
        carried_out_activities = []
        fields_of_activity = activities.get('a',[])
        work_starts = activities.get('s',[])
        work_ends = activities.get('t',[])

        for i, field_of_activity in enumerate(fields_of_activity):
            work_start = work_starts[i] if i < len(work_starts) else None
            work_end = work_ends[i] if i < len(work_ends) else None
            if field_of_activity:
                fpid = self.build_recs_and_reconcile(field_of_activity.lower(), "type")
                if fpid:
                    # Only create an activity if we have an URI for the type
                    activity = vocab.Active()
                    activity.classified_as = [model.Type(ident=fpid, label=field_of_activity)]

                    # If there is a timespan (s or t), attach it
                    if work_start or work_end:
                        ts = model.TimeSpan()
                        try:
                            bstart, bend = make_datetime(work_start)
                        except:
                            bstart, bend = None, None

                        try:
                            dstart, dend = make_datetime(work_end)
                        except:
                            dstart, dend = None, None

                        if bstart:
                            ts.begin_of_the_begin = bstart
                            ts.end_of_the_begin = bend
                        if dstart:
                            ts.begin_of_the_end = dstart
                            ts.end_of_the_end = dend

                        activity.timespan = ts  # Attach timespan to the specific activity
                    carried_out_activities.append(activity)

        if carried_out_activities:
            rec.carried_out = carried_out_activities

        # Extract and set biographical note (500)
        df500_data = self.extract_datafields(root, '500', ['a','i'])
        for sub_i, sub_a in zip(df500_data.get('i',['']), df500_data.get('a',[''])):
            note = " ".join(filter(None, [sub_i, sub_a]))
            if note:
                rec.referred_to_by = model.LinguisticObject(content=note)

        # Extract equivalents (700)
        df700_data = self.extract_datafields(root, '700', ['a', '0', '1'])
        uri_0 = df700_data.get('0', [])

        equivalents = []
        for uri in uri_0:
            if uri:
                # Wikidata and LCNAF
                if "wikipedia.org" in uri:
                    wikidata_qid = get_wikidata_qid(uri)
                    if wikidata_qid:
                        wikiuri = self.config['all_configs'].external['wikidata']['namespace'] + wikidata_qid
                        equivalents.append(wikiuri)
                elif uri.startswith("(DLC)"):
                    #Should be LCCN
                    lcnaf_uri = self.config["all_configs"].external['lcnaf']['namespace'] + "".join(uri.split(")")[1].split())
                    equivalents.append(lcnaf_uri)
        # VIAF
        equivalents.extend(uri for uri in df700_data.get('1', []) if uri)

        # Set equivalents
        if equivalents and rec.__class__ == model.Person:
            for e in equivalents:
                rec.equivalent = model.Person(ident=e)
        elif equivalents and rec.__class__ == model.Group:
            for e in equivalents:
                rec.equivalent = model.Group(ident=e)

        # Extract birth/formation and death/dissolution dates (046)
        df046_data = self.extract_datafields(root, '046', ['f', 'g'])
        df100_data = self.extract_datafields(root, '100', ['a', 'd'])
        df400_data = self.extract_datafields(root, '400', ['a', 'q', 'd'])
        try:
            begin_dates = make_datetime(df046_data.get('f', [''])[0])
        except:
            begin_dates = None
        try:
            end_dates = make_datetime(df046_data.get('g', [''])[0])
        except: 
            end_dates = None

        for potential_dates in [df100_data.get('d',[]), df400_data.get('d',[])]:
                for date in potential_dates:
                    if date and "-" in date:
                        try:
                            b, e = date.split("-")
                        except: 
                            b = e = None
                        try:
                            begin_tmps = make_datetime(b)
                        except:
                            begin_tmps = None
                        try:
                            end_tmps = make_datetime(e)
                        except:
                            end_tmps = None

                        if not begin_dates and begin_tmps:
                            begin_dates = begin_tmps
                        if not end_dates and end_tmps:
                            end_dates = end_tmps

                        if begin_dates and end_dates:
                            break

        # Set begin and end timespans
        if begin_dates:
            ts = model.TimeSpan()
            ts.begin_of_the_begin = begin_dates[0]
            ts.end_of_the_end = begin_dates[1]
            if rec.__class__ == model.Person:
                begin = model.Birth()
                begin.timespan = ts
                rec.born = begin
            elif rec.__class__ == model.Group:
                begin = model.Formation()
                begin.timespan = ts
                rec.formed = begin
        
        if end_dates:
            ts = model.TimeSpan()
            ts.begin_of_the_begin = end_dates[0]
            ts.end_of_the_end = end_dates[1]
            if rec.__class__ == model.Person:
                end = model.Death()
                end.timespan = ts
                rec.died = end
            elif rec.__class__ == model.Group:
                end = model.Dissolution()
                end.timespan = ts
                rec.dissolved = end

        if not test_birth_death(rec):
            rec.born = None
            rec.died = None


    def process_person(self, root, rec):
        # Send to process_agent for common fields
        self.process_agent(root, rec)  

        """Process Person-only fields"""
        df100_data = self.extract_datafields(root, '100', ['a', 'd'])
        df400_data = self.extract_datafields(root, '400', ['a', 'q', 'd'])
        df700_data = self.extract_datafields(root, '700', ['a', '0', '1'])
        df378_data = self.extract_datafields(root, '378', ['a', 'q'])
        df450_data = self.extract_datafields(root, '450', ['a'])
        df410_data = self.extract_datafields(root, '410', ['a'])

        names = set()
        primary = False
        # Extract primary name
        for nm in df100_data.get('a',[]):
            if primary and nm:
                names.add(nm)
            elif nm:
                rec.identified_by = vocab.PrimaryName(content=nm)
                primary = True

        # Extract potential alternate names (400, 700)
        names.update(
            df400_data.get('a', []) + 
            df400_data.get('q', []) + 
            df700_data.get('a', []) + 
            df378_data.get('a', []) + 
            df378_data.get('q', []) +
            df450_data.get('a', []) +
            df410_data.get('a', [])
        )

        # Assign a primary name from alternates if none exists
        if not primary and names:
            rec.identified_by = vocab.PrimaryName(cont=names.pop(0))
            primary = True 

        for nm in names:
            rec.identified_by = vocab.AlternateName(content=nm)

        # If no names remain, return None
        if not rec.identified_by:
            return None
        
        # Extract birth and death places (370)
        df370_data = self.extract_datafields(root, '370', ['a', 'b'])

        # Set birth and death places
        birth_place = next(iter(df370_data.get('a', [])), None)
        death_place = next(iter(df370_data.get('b', [])), None)
        if birth_place:
            bpid = self.build_recs_and_reconcile(birth_place, "place")
            if bpid:
                if not hasattr(rec, "born") or rec.born is None:
                    rec.born = model.Birth()
                rec.born.took_place_at = model.Place(ident=bpid, label=birth_place)
        
        if death_place:
            dpid = self.build_recs_and_reconcile(death_place, "place")
            if dpid:
                if not hasattr(rec, "died") or rec.died is None:
                    rec.died = model.Death()
                rec.died.took_place_at = model.Place(ident=dpid, label=death_place)
                        
        # Extract gender (375)
        gender = False
        df375_data = self.extract_datafields(root, '375', ['a', '0'])
        for uri in df375_data.get('0',['']):
            if "wikidata" in uri or uri == "http://id.loc.gov/authorities/subjects/sh2007005819":
                gender = [model.Type(ident=uri)]
                break
        for gen in df375_data.get('a', ['']):
            if gen.lower() in ("male", "males"):
                gender = [vocab.instances['male']]
                break
            elif gen.lower() in ("female", "females"):
                gender = [vocab.instances['female']]
                break

        # Set gender
        if gender:
            rec.classified_as = getattr(rec, "classified_as", []) + gender

    def process_group(self, root, rec):
        # Send to process_agent for common fields
        self.process_agent(root, rec)

        """Processes organizational details, including names and classifications."""
        df110_data = self.extract_datafields(root, '110', ['a', 'b'])
        df410_data = self.extract_datafields(root, '410', ['a', 'b'])
        df710_data = self.extract_datafields(root, '710', ['a', 'b'])
        df411_data = self.extract_datafields(root, '411', ['a'])

        primary = False
        alternate_names = set()

        # Extract primary (110), if there are multiple 110 values use the first as primary, add the rest to alternates.

        for org_name, sub_unit in zip(df110_data.get('a',['']), df110_data.get('b',[''])):
            name = ", ".join(filter(None, [org_name, sub_unit]))
            if primary and name:
                alternate_names.add(name)
            elif name:
                rec.identified_by = vocab.PrimaryName(content=name)
                primary = True

        # Extract alternates (410, 710, 411)
        # Set one as primary if 110 did not exist

        for data in [df410_data, df710_data, df411_data]:
            for org_name, sub_unit in zip(data.get('a', ['']), data.get('b', [''])):
                name = ", ".join(filter(None, [org_name, sub_unit]))
                if primary and name:
                    alternate_names.add(name)
                elif name:
                    rec.identified_by = vocab.PrimaryName(content=name)
                    primary = True

        for alt in alternate_names:
            rec.identified_by = vocab.AlternateName(content=alt)

        # If no names remain, return None
        if not rec.identified_by:
            return None

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
            # Add functionality for get_reference?

        return {'identifier': identifier, 'data': model.factory.toJSON(rec), 'source': 'fast'}
