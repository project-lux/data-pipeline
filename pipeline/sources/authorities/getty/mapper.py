from cromulent import model, vocab
from pipeline.process.base.mapper import Mapper
from pipeline.process.utils.mapper_utils import make_datetime, test_birth_death
import datetime

class GettyMapper(Mapper):
    # Core functions

    def __init__(self, config):
        Mapper.__init__(self, config)
        self.name = config['name']
        self.config = config
        # None = drop / no mapping
        self.name_classifications = {
            'http://vocab.getty.edu/term/type/Descriptor': vocab.PrimaryName,
            'http://vocab.getty.edu/aat/300404670': vocab.PrimaryName,
            'http://vocab.getty.edu/term/type/AlternateDescriptor': vocab.AlternateName,
            'http://vocab.getty.edu/aat/300404650': model.Name, # "names"
            'http://vocab.getty.edu/aat/300449151': vocab.AlternateName # historical
        }
        self.ignore_name_classifications = [
            'http://vocab.getty.edu/term/type/UsedForTerm'
        ]

        self.statements = {}
        for (name, thing) in vocab.ext_classes.items():
            if 'metatype' in thing and thing['metatype'] == 'brief text':
                self.statements[thing['id']] = getattr(vocab, name) 

        self.statements['300080102'] = vocab.BiographyStatement
        self.ignore_statements = ['300418049']

        self.ignore_values = [
            "http://vocab.getty.edu/aat/300379012", # undetermined
            "http://vocab.getty.edu/aat/300400512", # unavailable
        ]

        self.gender_flag = "http://vocab.getty.edu/aat/300055147"
        self.nationality_flag = "http://vocab.getty.edu/aat/300379842"
        self.occupation_flag = "http://vocab.getty.edu/aat/300263369"
        self.active_flag = "http://vocab.getty.edu/aat/300393177"

        self.aat_language_ids = [x.id for x in self.process_langs.values()]

    def process_getty_name(self, js):
        if not 'content' in js:
            return None

        nmcls = model.Name
        ignore = False
        if 'classified_as' in js:
            for c in js['classified_as']:
                if c['id'] in self.name_classifications:
                    nmcls = self.name_classifications[c['id']]
                elif c['id'] in self.ignore_name_classifications:
                    ignore = True
                elif c['id'].startswith('http://vocab.getty.edu/term/POS/'):
                    pass
                else:
                    print(f"Unknown name type: {c['id']}")

        nm = nmcls(content=js['content'])
        if 'language' in js:
            lang = js['language'][0]['_label']
            if type(lang) == list:
                # gah
                for l in lang:
                    if len(l) < 4: # en or eng
                        lang = l
                        break
            if len(lang) > 2:
                ll = lang[:2]
            else:
                ll = lang
            try:
                if ll in self.process_langs:
                    nm.language = self.process_langs[ll]
                else:
                    # Actually just drop it on the floor if it's a lang we don't map
                    return None
            except:
                print(ll)
                return None
        return nm


    def process_getty_statement(self, js):
        if not 'content' in js:
            return None
        if type(js['content']) == list:
            # just take the first
            js['content'] = js['content'][0]

        st = None
        if 'classified_as' in js:
            for cx in js['classified_as']:
                if type(cx) == str:
                    cxid = cx
                else:
                    cxid = cx['id']
                if cxid.find('/aat/') > -1:
                    code = cxid.rsplit('/',1)[1]
                    if code in self.statements:
                        st = self.statements[code](content=js['content'])
                        break
                    elif code in self.ignore_statements:
                        continue
                    else:
                        print(f"Unknown statement type: {code}")
        if st == None:
            print(f"Classified: {js.get('classified_as', None)}")
            st = vocab.Note(content=js['content'])
        if 'language' in js:
            lang = js['language'][0]['_label']
            if type(lang) == list:
                for l in lang:
                    if len(l) < 4:
                        lang = l
            if len(lang) > 2:
                ll = lang[:2]
            else:
                ll = lang
            try:
                if ll in self.process_langs:
                    st.language = self.process_langs[ll]            
            except:
                print(f"language on statement: {ll}")
        return st

    def fix_getty_timestamp(self, value, which=None):
        if len(value) < 19:
            # Cannot be complete, throw it to date parser
            try:
                b,e = make_datetime(value)
            except:
                print(f"Got unfixable datestamp: {value}")
                return ""
            if which and which.startswith('end_'):
                value = e
            else:
                value = b
        value = value.replace('24:00:00', '23:59:59')
        if which and which.startswith('end_'):
            # ensure 23:59:59
            value = value.replace('00:00:00', '23:59:59')
        if value.startswith('1200-01-01T'):
            return ""
        if value[0] != '-' and value[:4].isnumeric() and int(value[:4]) > 2025:
            return ""
        return value

    def get_classn(self, what, meta=None):
        # Given the metatype, retrieve the classification on what
        result = []
        cxns = what.get('classified_as', [])
        for c in cxns:
            if 'classified_as' in c:
                metas = [x['id'] for x in c['classified_as']]
                if meta and meta in metas:
                    result.append(c)
        return result

    def do_common(self, rec, top):
        if 'identified_by' in rec:
            for i in rec['identified_by']:
                if i['type'] == 'Name':
                    nm = self.process_getty_name(i)
                    if nm:
                        top.identified_by = nm
                        if not hasattr(top, '_label') and \
                            isinstance(nm, vocab.PrimaryName) and \
                            hasattr(nm, 'language') and \
                            nm.language[0].id == self.process_langs['en']:
                            top._label = nm.content

                elif i['type'] == 'Identifier':
                    # FIXME: Add this?
                    # system number assigned by Getty
                    pass
                elif i['type'] == "crm:E47_Spatial_Coordinates": 
                    # This is the only place where they are now
                    if isinstance(top, model.Place):
                        coords = i.get('value', i.get('content', '')).strip()
                        # [long, lat]
                        if coords:
                            if coords[0] == '[':
                                coords = coords[1:]
                            if coords[-1] == ']':
                                coords = coords[:-1]
                            lng, lat = coords.split(',')
                        top.defined_by = f"POINT ( {lng} {lat} )"

            if hasattr(top, 'identified_by'):
                if not hasattr(top, '_label'):
                    # No primary english, just pick the first
                    top._label = top.identified_by[0].content
            else:
                # Didn't find any names?
                print(f"Found no names for {rec['id']}")
                return None
        else:
            # We're pretty broken
            print(f"Found no names in {rec['id']}")
            return None

        mstmts = rec.get('referred_to_by', [])
        mstmts.extend(rec.get('subject_of', []))
        for st in mstmts:
            newst = self.process_getty_statement(st)
            if newst:
                top.referred_to_by = newst
        return top

    def do_common_event(self, data, event):
        # copy knowledge from data to event

        if 'timespan' in data:
            tsd = data['timespan']
            ts = model.TimeSpan()
            for p in ['begin_of_the_begin', 'begin_of_the_end', 'end_of_the_begin', 'end_of_the_end']:
                if p in tsd:
                    val = self.fix_getty_timestamp(tsd[p], p)
                    if val:
                        setattr(ts, p, val)
            event.timespan = ts

        if 'took_place_at' in data:
            tpa = data['took_place_at']
            if type(tpa) != list:
                tpa = [tpa]
            for place in tpa:
                if type(place) == str:
                    place = {'id': place}
                pid = place.get('id', '')
                if not pid:
                    continue
                pid = pid.replace('-place', '')
                plbl = place.get('_label', '')
                p = model.Place(ident=pid, label=plbl)
                event.took_place_at = p

class AatMapper(GettyMapper):


    def should_merge_from(self, base, to_merge):
        # Don't merge from here into non-Type records
        if base['data']['type'] not in ['Type', 'Language', 'MeasurementUnit', 'Currency', 'Material']:
            return False 
        return True

    def guess_type(self, data):
        if data['id'] in self.aat_language_ids:
            return model.Language
        elif data['id'] in self.aat_material_ids:
            return model.Material
        elif data['id'] in self.aat_unit_ids:
            return model.MeasurementUnit
        elif type(data['type']) == list:
            if 'Language' in data['type']:
                return model.Language
            elif 'Material' in data['type']:
                return model.Material
            else:
                return model.Type
        elif data['type'] == 'Type':
            # test for more specific via properties
            if 'part_of' in data:
                pof = data['part_of']
            elif 'broader' in data:
                pof = data['broader']
            else:
                pof = []

            if pof:
                if type(pof) != list:
                    pof = [pof]
                for p in pof:
                    if 'id' in p:
                        if p['id'] == "http://vocab.getty.edu/aat/300411913":
                            return model.Language
                        elif p['id'] == 'http://vocab.getty.edu/aat/300411993':
                            return model.Currency

            lbl = data['_label']
            if type(lbl) == list:
                lbl = lbl[0]

            if lbl.endswith('language)'):
                return model.Language
            elif lbl.endswith('material)'):
                return model.Material
            elif lbl.endswith(' of money)'):
                return model.Currency
            elif lbl.endswith('currency)'):
                return model.Currency

        elif hasattr(model, data['type']):
            return getattr(model, data['type'])

        return model.Type

    def transform(self, record, rectype, reference=False):
        # strip cache metadata
        rec = record['data']

        # Why isn't this just record['identifier'] ?
        myid = rec['id'].rsplit('/',1)[1]
        myid = f"http://vocab.getty.edu/{self.name}/{myid}"

        # rectype
        if rectype is None or rectype == "Type":
            topcls = self.guess_type(rec)
        else:
            topcls = getattr(model, rectype)
        top = topcls(ident=myid)
        res = self.do_common(rec, top)
        # Could be just broken
        if not res:
            return None

        # broader only for real types
        if rectype in ['Type', 'Material', 'Language', 'Currency', 'MeasurementUnit']:
            brdrs = rec.get('part_of', [])
            if type(brdrs) != list:
                brdrs = [brdrs]
            brdrs2 = rec.get('broader', [])
            if type(brdrs2) != list:
                brdrs2 = [brdrs2]       
            brdrs.extend(brdrs2)        
            for br in brdrs:
                if type(br) == str:
                    br = {'id': br, '_label': ""}
                lbl = br.get("_label", "")
                if type(lbl) == dict:
                    lbl = lbl['@value']
                top.broader = topcls(ident=br['id'], label=lbl)

        data = model.factory.toJSON(top)
        return {'identifier': record['identifier'], 'data': data, 'source':'aat'}

class UlanMapper(GettyMapper):

    def __init__(self, config):
        GettyMapper.__init__(self, config)
        self.now_year = str(datetime.datetime.now().year)
        self.accept_values = [
            "http://vocab.getty.edu/aat/300386154"
        ]

    def fix_identifier(self, identifier):
        # do ULAN specific fixes
        if '-agent' in identifier:
            identifier = identifier.replace('-agent', '')
        return identifier

    def guess_type(self, data):
        typ = data.get('type', None)
        if not typ or not typ in ['Person', 'Group']:
            print(f"Data from {data.get('id', '???')} has no useful type")
            return None
        return getattr(model, data['type'], None)

    def transform(self, record, rectype, reference=False):

        rec = record['data']
        try:
            myid = rec['id'].rsplit('/',1)[1]
        except:
            print(rec)
            raise
        # get the numeric and reapply to the namespace :(
        myid = f"http://vocab.getty.edu/{self.name}/{myid}"
        if rectype is None:
            topcls = self.guess_type(rec)
        else:            
            topcls = getattr(model, rectype)
        if not topcls:
            # Broken data, no type
            return None
        top = topcls(ident=myid)

        self.do_common(rec, top)

        if 'classified_as' in rec:
            # gender, profession, nationality
            for cx in rec['classified_as']:
                cxid = cx.get('id', '')
                lbl = cx.get('_label', '')
                if not cxid:
                    continue
                elif cxid in self.ignore_values:
                    continue
                elif cxid in self.accept_values:
                    top.classified_as = model.Type(ident=cxid, label=lbl)
                else:
                    cxlbl = cx.get('_label', '')
                    for cx2 in cx.get('classified_as', []):
                        cx2id = cx2.get('id', '')
                        if not cx2id:
                            continue
                        if cx2id == self.nationality_flag:
                            top.classified_as = vocab.Nationality(ident=cxid, label=cxlbl)
                            break
                        elif cx2id == self.gender_flag:
                            top.classified_as = vocab.Gender(ident=cxid, label=cxlbl)
                            break
                        elif cx2id == self.occupation_flag:
                            top.classified_as = vocab.Occupation(ident=cxid, label=cxlbl)
                        else:
                            print(f"Unknown meta-class: {cx2id}")

        if topcls in [model.Person, model.Group]:
            if 'born' in rec:
                born = rec['born']
            elif 'formed_by' in rec:
                born = rec['formed_by']
            else:
                born = None
            if born:
                if top.type == "Person": 
                    b = model.Birth()
                    top.born = b
                    tp = 'born'
                else:
                    b = model.Formation()
                    top.formed_by = b
                    tp = 'formed_by'
                self.do_common_event(born, b)

            if 'died' in rec:
                died = rec['died']
            elif 'dissolved_by' in rec:
                died = rec['dissolved_by']
            else:
                died = None
            if died:
                if top.type == "Person": 
                    d = model.Death()
                    top.died = d
                    tp2 = 'died'                
                else:
                    d = model.Dissolution()
                    top.dissolved_by = d
                    tp2 = 'dissolved_by'
                self.do_common_event(died, d)

            if top.type == "Person":
                okay = test_birth_death(top)
                if not okay:
                    try:
                        top.born = None
                        top.died = None
                    except:
                        # This shouldn't ever happen, but not going to die on the hill
                        pass

            if 'carried_out' in rec:
                co = rec['carried_out']
                if type(co) != list:
                    co = [co]
                for act in co:
                    cxns = act.get('classified_as', [])
                    cxnids = [x['id'] for x in cxns if 'id' in x]
                    if self.active_flag in cxnids:
                        active = vocab.Active()
                        if len(cxns) > 1:
                            # FIXME: copy in other types
                            print("Found more cxns on active")
                        self.do_common_event(act, active)
                        top.carried_out = active
                    else:
                        print(f"Found non active dates carried_out in ulan:{myid}")

        data = model.factory.toJSON(top)
        return {'identifier': record['identifier'], 'data': data, 'source':'ulan'}

class TgnMapper(GettyMapper):

    def __init__(self, config):
        GettyMapper.__init__(self, config)
        self.accept_values = [
            'http://vocab.getty.edu/aat/300000771', "http://vocab.getty.edu/aat/300387506",
            'http://vocab.getty.edu/aat/300008372', 'http://vocab.getty.edu/aat/300000774', 
            'http://vocab.getty.edu/aat/300000776', 'http://vocab.getty.edu/aat/300008389',
            'http://vocab.getty.edu/aat/300000745', 'http://vocab.getty.edu/aat/300008694',
            'http://vocab.getty.edu/aat/300008375', 'http://vocab.getty.edu/aat/300008057',
            'http://vocab.getty.edu/aat/300008791', 'http://vocab.getty.edu/aat/300387218',
            'http://vocab.getty.edu/aat/300387356'
        ]

    def guess_type(self, data):
        return model.Place

    def transform(self, record, rectype, reference=False):        
        rec = record['data']
        myid = rec['id'].rsplit('/',1)[1]
        # get the numeric and reapply to the namespace :(
        myid = f"http://vocab.getty.edu/{self.name}/{myid}"

        if rectype is None:
            topcls = self.guess_type(rec)
        else:
            topcls = getattr(model, rectype)
        top = topcls(ident=myid)

        self.do_common(rec, top)
        if 'classified_as' in rec:
            for cx in rec['classified_as']:
                cxid = cx.get('id', '')
                lbl = cx.get('_label', '')
                if not cxid:
                    continue
                elif cxid in self.ignore_values:
                    continue
                elif cxid in self.accept_values:
                    top.classified_as = model.Type(ident=cxid, label=lbl)

        if topcls == model.Place:
            # broader
            brdrs = rec.get('part_of', [])
            if type(brdrs) != list:
                brdrs = [brdrs]
            brdrs2 = rec.get('broader', [])
            if type(brdrs2) != list:
                brdrs2 = [brdrs2]       
            brdrs.extend(brdrs2)
            print(brdrs)       
            for br in brdrs:
                if type(br) == str:
                    br = {'id': br, '_label': ""}
                lbl = br.get("_label", "")
                if type(lbl) == dict:
                    lbl = lbl['@value']
                brid = br['id'].rsplit('/',1)[1]
                where = self.name['mapper'].get_reference(brid)
                if where and hasattr(where,'classified_as'):
                    cxns = where['classified_as']
                    for c in cxns:
                        if c['id'] and c['id'] == "http://vocab.getty.edu/aat/300387356":
                            continue
                top.part_of = model.Place(ident=br['id'], label=lbl)

        data = model.factory.toJSON(top)
        return {'identifier': record['identifier'], 'data': data, 'source':'tgn'}





