from pipeline.process.base.mapper import Mapper
from pipeline.process.utils.mapper_utils import make_datetime, test_birth_death
from cromulent import model, vocab
import os

### Documentation
# https://d-nb.info/standards/elementset/gnd 

class DnbMapper(Mapper):
    
    def __init__(self, config):
        Mapper.__init__(self, config)
        self.male = "https://d-nb.info/standards/vocab/gnd/gender#male"
        self.female = "https://d-nb.info/standards/vocab/gnd/gender#female"
        self.process_macs_nt_file(config)

    #process entity graph dump file
    def process_macs_nt_file(self, config):
        cfgs = config['all_configs']
        data_dir = cfgs.data_dir
        fn = os.path.join(data_dir, 'macs.nt')
        close = {}
        if os.path.exists(fn):
            fh = open(fn)
        else:
            self.closeMatches = {}
            return None
        lines = fh.readlines()
        fh.close()

        for l in lines:
            l = l.strip()
            # <https://d-nb.info/gnd/4129090-2> <http://www.w3.org/2004/02/skos/core#closeMatch> <http://id.loc.gov/authorities/subjects/sh85000691> .
            if l.startswith('<https://d-nb.info/gnd/') and 'closeMatch' in l:
                l = l.replace(' .', '')
                (a,b,c) = l.split(' ')
                gnd = a.rsplit('/',1)[1][:-1]
                tgt = c[1:-1]
                try:
                    close[gnd].append(tgt)
                except:
                    close[gnd] = [tgt]
        self.closeMatches = close

#person specific attributes from entity graph data
    def handle_person(self, rec, top):

        birth = None
        dob = ""
        if 'dateOfBirth' in rec:
            dob = rec['dateOfBirth']
            if type(dob) == list:
                dob = dob[0]
            try:
                b,e = make_datetime(dob)
            except:
                b = None
            if b:
                birth = model.Birth()
                ts = model.TimeSpan()
                ts.begin_of_the_begin = b
                ts.end_of_the_end = e
                birth.timespan = ts
                top.born = birth

        if 'placeOfBirth' in rec:
            pob = rec['placeOfBirth']
            if type(pob) == list:
                pob = pob[0]
            if not birth:
                birth = model.Birth()
                top.born = birth

            pid = pob.get('@id', '')
            lbl = pob.get('preferredName', '')
            birth.took_place_at = model.Place(ident=pid, label=lbl)                

        death = None
        if 'dateOfDeath' in rec:
            dod = rec['dateOfDeath']
            if type(dod) == list:
                dod = dod[0]
            if dob and len(dod) == 2:
                century = dob[0:2]
                dod = century + dod
            try:
                b,e = make_datetime(dod)
            except:
                b = None
            if b:
                death = model.Death()
                ts = model.TimeSpan()
                ts.begin_of_the_begin = b
                ts.end_of_the_end = e
                death.timespan = ts
                top.died = death

        if 'placeOfDeath' in rec:
            pod = rec['placeOfDeath']
            if type(pod) == list:
                pod = pod[0]
            if not death:
                death = model.Death()
                top.died = death
            pid = pod.get('@id', '')
            lbl = pod.get('preferredName', '')
            death.took_place_at = model.Place(ident=pid, label=lbl) 

        active = None
        if 'periodOfActivity' in rec:
            doa = rec['periodOfActivity']
            if '-' in doa:
                # can split into range. Otherwise, what to do with '1800'?
                try:
                    bs,es = doa.split('-')
                except:
                    # multiple -s??
                    print(doa)
                try:
                    b,be = make_datetime(bs)
                except:
                    b = None
                try:
                    e,ee = make_datetime(es)
                except:
                    e = None
                if b and e:
                    active = vocab.Active()
                    ts = model.TimeSpan()
                    ts.begin_of_the_begin = b
                    ts.end_of_the_end = e
                    active.timespan = ts
                    top.carried_out = active

        if 'placeOfActivity' in rec:
            poa = rec['placeOfActivity']
            if not type(poa) == list:
                poa = [poa]
            if not active:
                active = vocab.Active()
                top.carried_out = active
            for p in poa:
                pid = p.get('@id', '')
                lbl = p.get('preferredName', '')
                active.took_place_at = model.Place(ident=pid, label=lbl)

        if 'gender' in rec:
            gdr = rec['gender']
            if type(gdr) != list:
                gdr = [gdr]
            for g in gdr:
                if g['@id'] == self.male:
                    top.classified_as = vocab.instances['male']
                elif g['@id'] == self.female:
                    top.classified_as = vocab.instances['female']
                else:
                    print(f"unknown dnb gender: {g}")
                    lbl = g.get('preferredName', '')                    
                    top.classified_as = vocab.Gender(ident=g['@id'], label=lbl)

        if 'professionOrOccupation' in rec:
            occ = rec['professionOrOccupation']
            if type(occ) != list:
                occ = occ
            for o in occ:
                oid = o.get("@id", "")
                lbl = o.get('preferredName', '')
                top.classified_as = vocab.Occupation(ident=oid, label=lbl)

        if 'affiliation' in rec:
            aff = rec['affiliation']
            if type(aff) != list:
                aff = [aff]
            for a in aff:
                aid = a.get('@id', '')
                lbl = a.get('preferredName', '')
                top.member_of = model.Group(ident=aid, label=lbl)

        okay = test_birth_death(top)
        if not okay:
            try:
                top.born = None
                top.died = None
            except:
                # This shouldn't ever happen, but not going to die on the hill
                pass

#group specific attributes from entity graph data
    def handle_group(self, rec, top):
        ##FIXME: handle multiple dissolution and establishment dates?
        
        typ = rec.get("@type", "") 
        if typ in ['organization', 'organisation']:
            top.classified_as = vocab.Organization._classification[0]
        elif typ == 'family':
            top.classified_as = vocab.Family._classification[0]

        doe = None
        if 'dateOfEstablishment' in rec:
            doe = rec['dateOfEstablishment']
            if type(doe) == list:
                doe = doe[0]
            try:
                b,e = make_datetime(doe)
            except:
                b = None
            if b:
                form = model.Formation()
                ts = model.TimeSpan()
                ts.begin_of_the_begin = b
                ts.end_of_the_end = e
                form.timespan = ts
                top.formed_by = form

        if 'dateOfTermination' in rec:
            dot = rec['dateOfTermination']
            if type(dot) == list:
                dot = dot[0]
            if doe and len(dot) == 2:
                century = doe[0:2]
                dot = century + dot
            try:
                b,e = make_datetime(dot)
            except:
                b = None
            if b:
                diss = model.Dissolution()
                ts = model.TimeSpan()
                ts.begin_of_the_begin = b
                ts.end_of_the_end = e
                diss.timespan = ts
                top.dissolved_by = diss

        #if 'placeOfBusiness' in rec:
        #    pob = rec['placeOfBusiness']
        #    # residence
        #    for p in pob:
        #        if '@id' in p:
        #            pl = model.Place(ident=p['@id'], label=p.get('preferredName', "Place Entity"))
        #            top.residence = pl

        if 'isA' in rec:
            # classified_as ?
            isa = rec['isA']
            ### FIXME: no idea what these are. Ignore


    def handle_type(self, rec, top):
        pass

#place specific attributes from entity graph
    def handle_place(self, rec, top):
        
        if 'location' in rec:
            ft = rec['location']
            # GeoJSON Feature with geometry?
            if 'geometry' in ft:
                gt = ft['geometry']['type']
                coords = ft['geometry']['coordinates']
                if len(coords) == 2:
                    lng = coords[0]
                    lat = coords[1]
                if gt == "Point":
                    top.defined_by = f"POINT ( {lng} {lat} )"
                else:
                    print(f"Got geometry: {gt} in {rec['@id']}")
            else:
                print(f"No geometry in {rec['@id']}")

        if 'associatedCountry' in rec:
            # probably part_of this?
            # but is a vocab lookup...
            # "https://d-nb.info/standards/vocab/gnd/geographic-area-code#XA-GB"
            pass

#guess for entity graph data
    def guess_type(self, data):
        typ = data.get("@type", "")        
        if typ == 'person':
            topcls = model.Person
        elif typ in ['organization', 'organisation']:
            topcls = vocab.Organization
            typ = 'group'
        elif typ == 'family':
            topcls = vocab.Family
            typ = 'group'
        elif typ == "place":
            topcls = model.Place
        else:
            print(f"Unknown @type in dnb {data['@id']}: {typ}")
            return None
        return topcls

#  rec:
#  {'@id': 'https://d-nb.info/gnd/4133150-3', 
#   '@type': ['https://d-nb.info/standards/elementset/gnd#SubjectHeadingSensoStricto'], 
#   'http://www.w3.org/2002/07/owl#sameAs': [{'@id': 'http://www.wikidata.org/entity/Q330369'}], 
#   'http://www.w3.org/2007/05/powder-s#describedby': [{'@id': 'https://d-nb.info/gnd/4133150-3/about'}], 
#   'https://d-nb.info/standards/elementset/gnd#broaderTermGeneral': [{'@id': 'https://d-nb.info/gnd/4114333-4'}, {'@id': 'https://d-nb.info/gnd/4073883-8'}], 
#   'https://d-nb.info/standards/elementset/gnd#contributingPerson': [{'@id': 'https://d-nb.info/gnd/118584251'}], 'https://d-nb.info/standards/elementset/gnd#dateOfProduction': [{'@value': '1884-1903'}], 'https://d-nb.info/standards/elementset/gnd#geographicAreaCode': [{'@id': 'https://d-nb.info/standards/vocab/gnd/geographic-area-code#XA-GB'}], 'https://d-nb.info/standards/elementset/gnd#gndIdentifier': [{'@value': '4133150-3'}], 'https://d-nb.info/standards/elementset/gnd#gndSubjectCategory': [{'@id': 'https://d-nb.info/standards/vocab/gnd/gnd-sc#13.1b'}, {'@id': 'https://d-nb.info/standards/vocab/gnd/gnd-sc#13.1a'}], 'https://d-nb.info/standards/elementset/gnd#oldAuthorityNumber': [{'@value': '(DE-588c)4133150-3'}], 'https://d-nb.info/standards/elementset/gnd#preferredNameForTheSubjectHeading': [{'@value': 'Arts and crafts movement'}], 'https://d-nb.info/standards/elementset/gnd#relatedDdcWithDegreeOfDeterminacy1': [{'@id': 'http://dewey.info/class/745.094109034/'}], 'https://d-nb.info/standards/elementset/gnd#relatedPlaceOrGeographicName': [{'@id': 'https://d-nb.info/gnd/4022153-2'}], 'https://d-nb.info/standards/elementset/gnd#variantNameForTheSubjectHeading': [{'@value': 'Großbritannien / Arts and crafts movement'}]}, 

#guess for sachbegriff dump file
    def guess_lds_type(self, rec):
        types = rec.get('@type', [])
        for t in types:
            if t.endswith("SubjectHeadingSensoStricto"):
                return model.Type
            elif t.endswith('Language'):
                return model.Language
            elif t.endswith('SubjectHeading'):
                return model.Type
            elif t.endswith('EthnographicName'):
                return model.Group
            elif t.endswith('NomenclatureInBiologyOrChemistry'):
                return model.Type
            else: 
                topcls = None

#handler for sachbegriff
    def handle_lds(self, record, rectype=""):

        rec = record['data']['list']
        for r in rec:
            i = r.get('@id', "")
            if i and not i.endswith('/about'):
                # found the right dictionary
                rec = r
                break

        guess = self.guess_lds_type(rec)
        if not guess and not rectype:
            return None 
        elif not rectype:
            topcls = guess  
        else:
            topcls = getattr(model, rectype)

        top = topcls(ident=rec['@id'])

        pname = "https://d-nb.info/standards/elementset/gnd#preferredNameForTheSubjectHeading"
        aname = "https://d-nb.info/standards/elementset/gnd#variantNameForTheSubjectHeading" 
        owlsame = "http://www.w3.org/2002/07/owl#sameAs" 
        desc = "https://d-nb.info/standards/elementset/gnd#definition"
        homepage = "http://www.w3.org/2007/05/powder-s#describedby"

        broaders = [
            "https://d-nb.info/standards/elementset/gnd#broaderTermGeneral",
            "https://d-nb.info/standards/elementset/gnd#broaderTermInstantial",
            "https://d-nb.info/standards/elementset/gnd#broaderTermGeneric",
            "https://d-nb.info/standards/elementset/gnd#broaderTermPartitive"
        ]

        relatedPlaces = [
            "https://d-nb.info/standards/elementset/gnd#relatedPlaceOrGeographicName",
            "https://d-nb.info/standards/elementset/gnd#contributingPlaceOrGeographicName",
            "https://d-nb.info/standards/elementset/gnd#place"
        ]

        for val in rec.get(homepage,[]):
            lo = model.LinguisticObject(label="Website Text")
            do = vocab.WebPage(label="Home Page")            
            do.access_point = model.DigitalObject(ident=val['@id'])
            matchnumber = val['@id'].split('/')
            recnumber = rec['@id'].split('/')[-1]
            if recnumber in matchnumber:
                continue
            lo.digitally_carried_by = do
            top.subject_of = lo

        for val in rec.get(pname, []):
             primary = vocab.PrimaryName(content=val['@value'])
             primary.language = self.process_langs['de']
             top.identified_by = primary
        for val in rec.get(aname, []):
            altname = vocab.AlternateName(content=val['@value'])
            altname.language = self.process_langs['de']
            top.identified_by = altname

        for val in rec.get(desc, []):
            description = vocab.Description(content=val['@value'])
            description.language = self.process_langs['de']
            top.referred_to_by = description

        for val in rec.get(owlsame, []):
            top.equivalent = topcls(ident=val['@id'])

        if topcls in [model.Type, model.Language]:
            for b in broaders:
                for val in rec.get(b, []):
                    top.broader = topcls(ident=val['@id'])

        for rel in relatedPlaces:
            for val in rec.get(rel, []):
                if '@id' in val:
                    aa = model.AttributeAssignment()
                    aa.assigned = model.Place(ident=val['@id'])
                    top.attributed_by = aa

        if record['identifier'] in self.closeMatches:
            for cm in self.closeMatches[record['identifier']]:
                top.equivalent = topcls(ident=cm)

        data = model.factory.toJSON(top)
        return {'identifier': record['identifier'], 'data': data, 'source': 'dnb'}

        #  "https://d-nb.info/standards/elementset/gnd#contributingPerson"
        #  e.g. william morris contributed to arts and crafts movement ?
        #  "https://d-nb.info/standards/elementset/gnd#dateOfProduction"
        #  e.g arts and crafts dop is "1884-1903"

        # "https://d-nb.info/standards/elementset/gnd#geographicAreaCode"
        # is the same as 
        # "https://d-nb.info/standards/elementset/gnd#relatedPlaceOrGeographicName"
        # ?
        #KD: it is. area code is vocab look up, but related place is auth record. 

    #gets called from config.py
    def fix_identifier(self, identifier):
        if identifier.endswith('/about'):
            return identifier.replace('/about', '')
        return identifier

    #transform for all: entity graph and sachbegriff
    def transform(self, record, rectype="", reference=False):

        rec = record['data']
        if 'list' in rec:
            # lds.jsonld record
            return self.handle_lds(record, rectype)

        if rectype == "" or rectype is None:
            topcls = self.guess_type(rec)
        else:
            topcls = getattr(model, rectype)
        top = topcls(ident=rec['@id'])

        # names
        if 'preferredName' in rec:
            pn = rec['preferredName']
            if type(pn) == str:
                top._label = pn
                top.identified_by = vocab.PrimaryName(content=pn)
        if 'variantName' in rec:
            vn = rec['variantName']
            if type(vn) != list:
                vn = [vn]
            for v in vn:
                top.identified_by = vocab.AlternateName(content=v)
        if 'pseudonym' in rec:
            pss = rec['pseudonym']
            if type(pss) != list:
                pss = [pss]
            for p in pss:
                top.identified_by = vocab.Pseudonym(content=p['preferredName'])

        if 'biographicalOrHistoricalInformation' in rec:
            bhi = rec['biographicalOrHistoricalInformation']
            top.referred_to_by = vocab.Description(content=bhi)
        if 'homepage' in rec:
            hp = rec['homepage']
            if type(hp) != list:
                hp = [hp]
            for h in hp:
                matchnumber = h.split('/')
                recnumber = rec['@id'].split('/')[-1]
                if recnumber in matchnumber:
                    continue
                # make homepage ref
                lo = model.LinguisticObject(label="Website Text")
                do = vocab.WebPage(label="Home Page")            
                try:
                    do.access_point = model.DigitalObject(ident=h)
                except:
                    print(f"Failed to build a DigitalObject with URI: {h}")
                    continue
                lo.digitally_carried_by = do
                top.subject_of = lo

        if 'depiction' in rec:
            dep = rec['depiction']
            if type(dep) != list:
                dep = [dep]
            for d in dep:
                # Image is actually in @id
                jpg = d['@id']
                jpg = jpg.replace(" ", "_")
                do = vocab.DigitalImage(label=f"Digital Image of {pn}")
                vi = model.VisualItem(label=f"Appearance of {pn}")
                do.access_point = model.DigitalObject(ident=jpg)
                vi.digitally_shown_by = do
                top.representation = vi

        if 'sameAs' in rec:
            sa = rec['sameAs']
            try:
                lbl = top._label
            except:
                lbl = ""
            if type(sa) != list:
                sa = [sa]
            for s in sa:
                try:
                    suri = s['@id']
                except:
                    print(f"Non dict / unidentified sameAs in DNB {rec['@id']}: {s}")
                    continue
                top.equivalent = topcls(ident=suri, label=lbl)

        # Per class specific stuff
        fn = getattr(self, f"handle_{top.type.lower()}", None)
        if fn:
            fn(rec, top)

        unknowns = []
        for u in unknowns:
            if u in rec:
                print(f"Found {u}: {rec[u]} in {rec['@id']}")

        #boilerplate/factory/record builder
        data = model.factory.toJSON(top)
        return {'identifier': record['identifier'], 'data': data, 'source': 'dnb'}
