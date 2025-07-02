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
            "148": model.Period, "448": model.Period, "100": model.Person, "150": model.Type,
            "155": model.Type, "151": model.Place, "110": model.Group,
            "411": model.Activity, "147": model.Activity
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
            "place": "lcnaf", "type":"lcsh", "group":"lcnaf","person":"lcnaf","activity":"lcsh",
            "period":"lcsh"
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
    
    def dms_to_wkt(self, dms_string):
        def dms_to_dd(degrees, minutes, seconds, direction):
            dd = float(degrees) + float(minutes)/60 + float(seconds)/3600
            return -dd if direction in ['S', 'W'] else dd

        def parse_dms(dms_str):
            match = re.match(r"(\d+)°(\d+)[ʹ'](\d+)[ʺ\"]?([NSEW])", dms_str)
            if not match:
                raise ValueError(f"Cannot parse DMS string: {dms_str}")
            return dms_to_dd(*match.groups())

        parts = dms_string.split()
        if len(parts) != 2:
            raise ValueError("Expected string format like '52°22ʹ51ʺN 004°38ʹ13ʺE'")
        
        lat_dd = parse_dms(parts[0])
        lon_dd = parse_dms(parts[1])
        return f"POINT({lon_dd} {lat_dd})"
    
    def assign_names(self, rec, names, primary=False):
    # Assigns PrimaryName to the first name (if not already assigned), others as AlternateName.
        for i, name in enumerate(names):
            if not name:
                continue
            if not hasattr(rec, "identified_by"):
                rec.identified_by = []
            if not primary:
                rec.identified_by.append(vocab.PrimaryName(content=name))
                primary = True
            else:
                rec.identified_by.append(vocab.AlternateName(content=name))


    def combine_subfields(self, *fields):
        return ", ".join(filter(None, fields))

    def process_equivalents(self, rec, uris, cls):
        """Processes equivalent URIs for a given class, avoiding duplicates."""
        seen = set()
        for uri in uris:
            if not uri:
                continue

            if "wikipedia.org" in uri:
                wikidata_qid = get_wikidata_qid(uri)
                if not wikidata_qid:
                    continue
                uri = self.config['all_configs'].external['wikidata']['namespace'] + wikidata_qid

            elif uri.startswith("(DLC)"):
                clean_uri = uri.replace("(DLC)", "").replace(" ", "").strip()
                uri = "http://id.loc.gov/authorities/subjects/" + clean_uri

            elif uri.startswith("(OCoLC)fst"):
                uri = self.fast_id_to_uri(uri)

            if uri not in seen:
                seen.add(uri)
                rec.equivalent = cls(ident=uri)
    
    def process_classifications(self, rec, uris):
        for uri in uris:
            if uri:
                rec.classified_as = model.Type(ident=uri)
    
    def build_timespan(self, start=None, end=None):
        """Builds a TimeSpan from start and/or end date strings using make_datetime."""
        ts = model.TimeSpan()

        try:
            bstart, bend = make_datetime(start) if start else (None, None)
        except:
            bstart, bend = None, None

        try:
            dstart, dend = make_datetime(end) if end else (None, None)
        except:
            dstart, dend = None, None

        if not any([bstart, bend, dstart, dend]):
            return None  # Don't return empty timespans

        if bstart:
            ts.begin_of_the_begin = bstart
            ts.end_of_the_begin = bend
        if dstart:
            ts.begin_of_the_end = dstart
            ts.end_of_the_end = dend

        return ts


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
                    uri = self.build_recs_and_reconcile(name.lower(), "group")
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
                    uri = self.build_recs_and_reconcile(name.lower(), "group")
                    if uri:
                        membership.add(uri)
        
        # Set member_of
        if membership:
            for mem in membership:
                rec.member_of = model.Group(ident=mem)

        # Classifications
        df374_data = self.extract_datafields(root, '374',['a','0'])
        df368_data = self.extract_datafields(root, '368', ['a'])
        cxns = set()
        cxns.update(df374_data.get('a',[]),
                df374_data.get('0',[]),
                df368_data.get('a',[]))
        self.process_classifications(rec, cxns)

        # Extract residence (370 or 551)
        # Prefer 370
        locations = self.extract_datafields(root, '370', ['c','e'])
        if locations.get('c') or locations.get('e'):
            for assoc_place, residence in zip(locations.get('c',['']), locations.get('e',[''])):
                if residence:
                    rpid = self.build_recs_and_reconcile(residence.lower(), 'place')
                    if rpid:
                        rec.residence = model.Place(ident=rpid, label=residence)
                elif assoc_place:
                    rpid = self.build_recs_and_reconcile(assoc_place.lower(), 'place')
                    if rpid:
                        rec.residence = model.Place(ident=rpid, label=assoc_place)
        else:
        # Try 551
            locations = self.extract_datafields(root, '551',['a'])
            for place in locations.get('a',[]):
                if place:
                    rpid = self.build_recs_and_reconcile(place.lower(), "place")
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

                    if work_start or work_end:
                        ts = self.build_timespan(work_start, work_end)
                        if ts:
                            activity.timespan = ts
                    carried_out_activities.append(activity)

        if carried_out_activities:
            rec.carried_out = carried_out_activities

        # Extract and set biographical note (500)
        df500_data = self.extract_datafields(root, '500', ['a','i'])
        for sub_i, sub_a in zip(df500_data.get('i',['']), df500_data.get('a',[''])):
            note = " ".join(filter(None, [sub_i, sub_a]))
            if note:
                rec.referred_to_by = model.LinguisticObject(content=note)

        # Equivalents
        df700_data = self.extract_datafields(root, '700', ['0','1'])
        df710_data = self.extract_datafields(root, '710', ['0','1'])

        uris = (
            df700_data.get('0', []) + df710_data.get('0', []) +
            df700_data.get('1', []) + df710_data.get('1', [])
        )
        if isinstance(rec, model.Person):
            self.process_equivalents(rec, uris, model.Person)
        elif isinstance(rec, model.Group):
            self.process_equivalents(rec, uris, model.Group)

        # Extract dates (046, 100|d, 400|d)
        df046_data = self.extract_datafields(root, '046', ['f', 'g'])
        df100_data = self.extract_datafields(root, '100', ['d'])
        df400_data = self.extract_datafields(root, '400', ['d'])

        # Try 046 first
        begin_ts = self.build_timespan(df046_data.get('f', [''])[0])
        end_ts = self.build_timespan(df046_data.get('g', [''])[0])

        # Fallback: try parsing textual date ranges in 100|d and 400|d
        if not begin_ts or not end_ts:
            for field_data in [df100_data.get('d', []), df400_data.get('d', [])]:
                for date_str in field_data:
                    if date_str and "-" in date_str:
                        start, end = date_str.split("-", 1)
                        if not begin_ts:
                            begin_ts = self.build_timespan(start.strip())
                        if not end_ts:
                            end_ts = self.build_timespan(end.strip())
                        if begin_ts and end_ts:
                            break
                if begin_ts and end_ts:
                    break

        # Assign TimeSpans
        if begin_ts:
            if isinstance(rec, model.Person):
                rec.born = model.Birth(timespan=begin_ts)
            elif isinstance(rec, model.Group):
                rec.formed = model.Formation(timespan=begin_ts)

        if end_ts:
            if isinstance(rec, model.Person):
                rec.died = model.Death(timespan=end_ts)
            elif isinstance(rec, model.Group):
                rec.dissolved = model.Dissolution(timespan=end_ts)

        # Sanity check
        if not test_birth_death(rec):
            rec.born = None
            rec.died = None


    def process_person(self, root, rec):
        # Send to process_agent for common fields
        self.process_agent(root, rec)  

        # Person only fields

        # Names
        df100_data = self.extract_datafields(root, '100', ['a'])
        df400_data = self.extract_datafields(root, '400', ['a', 'q'])
        df700_data = self.extract_datafields(root, '700', ['a'])
        df378_data = self.extract_datafields(root, '378', ['a', 'q'])
        df450_data = self.extract_datafields(root, '450', ['a'])
        df410_data = self.extract_datafields(root, '410', ['a'])

        names = set()
        names.update(
            df100_data.get('a', []) +
            df400_data.get('a', []) + 
            df400_data.get('q', []) + 
            df700_data.get('a', []) + 
            df378_data.get('a', []) + 
            df378_data.get('q', []) +
            df450_data.get('a', []) +
            df410_data.get('a', [])
        )
        primary = self.assign_names(rec, names, primary=False)
        if not primary:
            return None
        
        # Extract birth and death places (370)
        df370_data = self.extract_datafields(root, '370', ['a', 'b'])

        # Set birth and death places
        birth_place = next(iter(df370_data.get('a', [])), None)
        death_place = next(iter(df370_data.get('b', [])), None)
        if birth_place:
            bpid = self.build_recs_and_reconcile(birth_place.lower(), "place")
            if bpid:
                if not hasattr(rec, "born") or rec.born is None:
                    rec.born = model.Birth()
                rec.born.took_place_at = model.Place(ident=bpid, label=birth_place)
        
        if death_place:
            dpid = self.build_recs_and_reconcile(death_place.lower(), "place")
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

        # Process Group only fields

        # Names
        df110_data = self.extract_datafields(root, '110', ['a', 'b'])
        df410_data = self.extract_datafields(root, '410', ['a', 'b'])
        df710_data = self.extract_datafields(root, '710', ['a', 'b', '0'])
        df411_data = self.extract_datafields(root, '411', ['a'])

        names = set()

        for org_name, sub_unit in zip(df110_data.get('a',['']), df110_data.get('b',[''])):
            name = self.combine_subfields(org_name, sub_unit)
            if name:
                names.add(name)

        for data in [df410_data, df710_data, df411_data]:
            a_vals = data.get('a', [])
            b_vals = data.get('b', [])
            for org_name, sub_unit in zip(a_vals, b_vals):
                name = self.combine_subfields(org_name, sub_unit)
                if name:
                    names.add(name)
        primary = self.assign_names(rec, names, primary=False)
        if not primary:
            return None
        
    def process_type(self, root, rec):
        #Equivalents
        equivs = []
        df750_data = self.extract_datafields(root, '750', ['1','0'])
        df710_data = self.extract_datafields(root, '710', ['0'])
        df751_data = self.extract_datafields(root, '751', ['0'])
        df755_data = self.extract_datafields(root, '755', ['0'])

        uris = (
            df750_data.get("0", []) +
            df710_data.get("0", []) +
            df751_data.get("0", []) +
            df750_data.get("1", [])
        )

        self.process_equivalents(rec, uris, model.Type)
        
        #GenreForm has separate namespace
        for uri in df755_data.get("0",[]):
            if uri and uri.startswith("(DLC)"):
                clean_uri = uri.replace("(DLC)", "").replace(" ", "").strip()
                rec.equivalent = model.Type(ident="http://id.loc.gov/authorities/genreForms/" + clean_uri)

        #Names
        names = set()
        for a, x in zip(df150_data.get('1',['']), df150_data.get('a',[''])):
            name = self.combine_subfields(a, x)
            if name:
                names.add(name)

        for field_data in [df450_data, df155_data, df455_data]:
            for nm in field_data.get('a', []):
                if nm:
                    names.add(nm)

        primary = self.assign_names(rec, names, primary=False)
        if not primary:
            return None
    
        # Broader (550, 555)
        for tag, subfields in [('550', ('g', '0')), ('555', ('a', '0'))]:
            data = self.extract_datafields(root, tag, list(subfields))
            for brdr, brid in zip(data.get(subfields[0], []), data.get(subfields[1], [])):
                if brdr and brid and brid.startswith("(OCoLC)fst"):
                    brid = self.fast_id_to_uri(brid)
                    rec.broader = model.Concept(ident=brid, label=brdr)

        # Extract and set scope note (680)
        df680_data = self.extract_datafields(root, '680', ['i'])
        for note in df680_data.get('i', []):
            if note:
                rec.referred_to_by = model.LinguisticObject(content=note)

    def process_place(self, root, rec):
        # Equivalents
        df751_data = self.extract_datafields(root, '751', ['0','a'])
        df370_data = self.extract_datafields(root, '370', ['c','e','f','0'])
        uris = set()
        uris.update(df751_data.get('0', []))
        uris.update(uri for uri in df370_data.get('0', []) if uri.startswith("http://id.loc.gov/authorities/"))
        for place_names in [df370_data.get('c', []), df370_data.get('e', []), df370_data.get('f', [])]:
            for place_name in place_names:
                if place_name:
                    uri = self.build_recs_and_reconcile(place_name.lower(), "place")
                    if uri:
                        uris.update(uri)
        self.process_equivalents(rec, uris, model.Place)

        # Names
        df151_data = self.extract_datafields(root, '151', ['a','z'])
        df410_data = self.extract_datafields(root, '410', ['a'])
        names = set()
        for a in df151_data.get('a',[]):
            for z in df151_data.get('z',[]):
                name = self.combine_subfields(a, z)
                if name:
                    names.add(name)
        for df in [df751_data, df410_data]:
            for a in df.get('a', []):
                if a:
                    names.add(a)
        primary = self.assign_names(rec, names, primary=False)
        if not primary:
            return None

        # WKT    
        df670_data = self.extract_datafields(root, '670', ['b'])
        for point in df670_data.get('b', []):
            if ";" in point:
                point = point.split(";")[1]
            wkt = self.dms_to_wkt(point)
            if wkt:
                rec.defined_by = wkt

        # Classifications
        df550_data = self.extract_datafields(root, '550', ['a','0'])
        df368_data = self.extract_datafields(root, '368', ['a'])

        cxns = set()
        for name, raw_uri in zip(df550_data.get('a', []), df550_data.get('0', [])):
            if raw_uri and raw_uri.startswith("(OCoLC)fst"):
                cxns.add(self.fast_id_to_uri(raw_uri))
            elif raw_uri:
                cxns.add(raw_uri)
            elif name:
                uri = self.build_recs_and_reconcile(name.lower(), "type")
                if uri:
                    cxns.add(uri)
        for name in df368_data.get('a', []):
            if name:
                uri = self.build_recs_and_reconcile(name.lower(), "type")
                if uri:
                    cxns.add(uri)
        self.process_classifications(rec, cxns)


    def process_period(self, root, rec):
        # Names
        names = set()
        df448_data = self.extract_datafields(root, '448', ['a'])
        for nm in df448_data.get('a', []):
            if nm:
                names.add(nm)
        primary = self.assign_names(rec, names, primary=False)
        if primary == False:
            return None
        
        # Timespans
        df148_data = self.extract_datafields(root, '148', ['a'])

        for val in df148_data.get('a', []):
            val = val.strip()
            if "-" in val:
                start, end = val.split("-", 1)
                ts = self.build_timespan(start.strip(), end.strip())
            else:
                ts = self.build_timespan(val)
            if ts:
                rec.timespan = ts
                break


    def process_activity(self, root, rec):
        # Names
        names = set()
        df147_data = self.extract_datafields(root, '147', ['a','d'])
        for a, d in zip(df147_data.get('a', []), df147_data.get('d', [])):
            name = self.combine_subfields(a, d)
            if name:
                names.add(name)

        df111_data = self.extract_datafields(root, '111', ['a','n','d'])
        for a,n,d in zip(df111_data.get('a', []), df111_data.get('n', []), df111_data.get('d', [])):
            name = self.combine_subfields(a, n, d)
            if name:
                names.add(name)

        df410_data = self.extract_datafields(root, '410', ['a','b'])
        a_vals = df410_data.get('a', [])
        b_vals = df410_data.get('b', [])
        for a in a_vals:
            name = self.combine_subfields(a, *b_vals)
            if name:
                names.add(name)
        
        df411_data = self.extract_datafields(root, '411', ['a'])
        for a in df411_data.get('a', []):
            if a:
                names.add(a)

        df447_data = self.extract_datafields(root, '447', ['a','d'])
        for a, d in zip(df447_data.get('a', []), df447_data.get('d', [])):
            name = self.combine_subfields(a, d)
            if name:
                names.add(name)

        primary = self.assign_names(rec, names, primary=False)
        if not primary:
            return None

        # Took place at
        df551_data = self.extract_datafields(root, '551', ['a', '0'])
        df370_data = self.extract_datafields(root, '370', ['c','e','f'])
        uris = df551_data.get('0', [])
        places = df551_data.get('a', [])
        places_for_reconcile = df370_data.get('c', []) + df370_data.get('e', []) + df370_data.get('f', [])
        for place in places_for_reconcile:
            if place:
                rpid = self.build_recs_and_reconcile(place.lower(), "place")
                if rpid:
                    rec.took_place_at = model.Place(ident=rpid, label=place)

        for uri, place in zip(uris, places):
            if uri and uri.startswith("(OCoLC)fst"):
                uri = self.fast_id_to_uri(uri)
                rec.took_place_at = model.Place(ident=uri)
            elif place:
                rpid = self.build_recs_and_reconcile(place.lower(), "place")
                if rpid:
                    rec.took_place_at = model.Place(ident=rpid, label=place)
        
        # Timespan from 046 |s and |t
        df046_data = self.extract_datafields(root, '046', ['s', 't'])
        timespan_set = False

        for start, end in zip(df046_data.get('s', []), df046_data.get('t', [])):
            ts = self.build_timespan(start, end)
            if ts:
                rec.timespan = ts
                timespan_set = True
                break

        # Fallback: Timespan from 748 |a
        if not timespan_set:
            df748_data = self.extract_datafields(root, '748', ['a'])
            for date in df748_data.get('a', []):
                if not date:
                    continue
                if "-" in date:
                    start, end = date.split("-", 1)
                    ts = self.build_timespan(start.strip(), end.strip())
                else:
                    ts = self.build_timespan(date.strip())
                if ts:
                    rec.timespan = ts
                    break


        # Part of
        df547_data = self.extract_datafields(root, '547', ['a','c','d','0'])
        part = df547_data.get('a', [])
        subunit = df547_data.get('c', [])
        date = df547_data.get('d', [])
        uri = df547_data.get('0', [])
        if part or subunit or date:
            name = ", ".join(filter(None, [part, subunit, date]))
        if uri and uri.startswith("(OCoLC)fst"):
            uri = self.fast_id_to_uri(uri)
            rec.part_of = model.Activity(ident=uri, label=name)

        # Classifications
        df550_data = self.extract_datafields(root, '550', ['a','0'])
        df368_data = self.extract_datafields(root, '368', ['a'])
        cxns = set()
        for name, raw_uri in zip(df550_data.get('a', []), df550_data.get('0', [])):
            if raw_uri and raw_uri.startswith("(OCoLC)fst"):
                cxns.add(self.fast_id_to_uri(raw_uri))
            elif raw_uri:
                cxns.add(raw_uri)
            elif name:
                uri = self.build_recs_and_reconcile(name.lower(), "activity")
                if uri:
                    cxns.add(uri)
        for name in df368_data.get('a', []):
            if name:
                uri = self.build_recs_and_reconcile(name.lower(), "activity")
                if uri:
                    cxns.add(uri)
        self.process_classifications(rec, cxns)

        # Equivalents
        uris = set()
        df711_data = self.extract_datafields(root, '711', ['0','1'])
        df751_data = self.extract_datafields(root, '751', ['0'])
        df750_data = self.extract_datafields(root, '750', ['1'])
        uris.update(df751_data.get('0', []))
        uris.update(df711_data.get('0', []))
        uris.update(df711_data.get('1', []))
        uris.update(df750_data.get('1', []))

        self.process_equivalents(rec, uris, model.Activity)

    def transform(self, record, rectype=None):
        """Transforms a MARC record into Linked.art JSON."""
        try:
            root = etree.fromstring(record["data"]['xml'])
        except Exception as e:
            print(f"Error parsing XML: {e}")
            return None  # Return None if XML parsing fails

        identifier = record["identifier"]

        if not rectype:
            topcls = self.guess_type(root)
            if topcls:
                rectype = topcls.__name__
            else:
                return None
        else:
            topcls = getattr(model, rectype, None)

        rec = topcls(ident=f"http://id.worldcat.org/fast/{identifier}")

        typ = rectype.lower()
        process_fn = getattr(self, f"process_{typ}", None)
        if process_fn:
            process_fn(root, rec)

        return {'identifier': identifier, 'data': model.factory.toJSON(rec), 'source': 'fast'}