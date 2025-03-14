from lux_pipeline.process.base.mapper import Mapper
from lux_pipeline.process.utils.mapper_utils import make_datetime
from cromulent import model, vocab
from lxml import etree

### Documentation
    ##some properties not handled:
        # foaf: names - ignore because getting from preflabel
        # sameAs - ignore because just redirects back
        # relatedMatch - ignore as not exact enough (apples -> apple trees)
        # related - not close enough
        # seeAlso - catalog for the same thing in BNF
        # don't process BNF foaf:page, which are BNF URIs


class BnfXmlMapper(Mapper):

    def __init__(self, config):
        Mapper.__init__(self, config)
        self.nss = {
            "bio":"http://vocab.org/bio/0.1/",
            "bnf-onto":"http://data.bnf.fr/ontology/bnf-onto/",
            "dcterms":"http://purl.org/dc/terms/",
            "geo":"http://www.w3.org/2003/01/geo/wgs84_pos#",
            "foaf":"http://xmlns.com/foaf/0.1/",
            "owl":"http://www.w3.org/2002/07/owl#",
            "rdau":"http://rdaregistry.info/Elements/u/#",
            "rdf":"http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "rdfs":"http://www.w3.org/2000/01/rdf-schema#",
            "skos":"http://www.w3.org/2004/02/skos/core#",
            "rdagroup2elements":"http://rdvocab.info/ElementsGr2/"
        }
        self.lang = self.process_langs.get("fr","")

    def parse_xml(self, xml):
        nss = self.nss
        if '<?xml' in xml:
            xml = bytes(xml, 'utf-8')
        try:
            dom = etree.XML(xml)
        except:
            raise
            return None
        try:
            top = dom.xpath('/rdf:RDF', namespaces=nss)[0]
        except:
            # Likely a redirection record
            # print(f"No cluster: {xml}")
            return None
        return top

    def guess_type(self, rec, ident):
        nss = self.nss

        try:
            classes = rec.xpath(f"//rdf:Description[@rdf:about='https://data.bnf.fr/ark:/12148/{ident}#about']",namespaces=nss)[0]
        except:
            classes = None

        try: 
            class_typ = self.to_plain_string(classes.xpath('./rdf:type/@rdf:resource', namespaces=nss)[0])
        except:
            class_typ = None

        if class_typ == 'http://xmlns.com/foaf/0.1/Organization':
            topcls = model.Group
            rectype = "Group"
        elif class_typ == 'http://xmlns.com/foaf/0.1/Person':
            topcls = model.Person
            rectype = "Person"
        elif class_typ == "http://www.w3.org/2003/01/geo/wgs84_pos#SpatialThing":
            topcls = model.Place
            rectype = "Place"
        else:
            topcls = model.Type 
            rectype = "Type"


        return (topcls, rectype)

    def make_timespan(self, date, top, event=""):
        try:
            b,e = make_datetime(date)
        except:
            b = None
        if b:
            if event == "Birth":
                birth = model.Birth()
                ts = model.TimeSpan()
                ts.begin_of_the_begin = b
                ts.end_of_the_end = e
                ts.identified_by = vocab.DisplayName(content=date)
                birth.timespan = ts
                top.born = birth

            elif event == "Death":
                death = model.Death()
                ts = model.TimeSpan()
                ts.begin_of_the_begin = b
                ts.end_of_the_end = e
                ts.identified_by = vocab.DisplayName(content=date)
                death.timespan = ts
                top.died = death

            elif event == "Formation":
                formation = model.Formation()
                ts = model.TimeSpan()
                ts.begin_of_the_begin = b
                ts.end_of_the_end = e
                ts.identified_by = vocab.DisplayName(content=date)
                formation.timespan = ts
                top.formed_by = formation

            elif event == "Dissolution":
                dissolution = model.Dissolution()
                ts = model.TimeSpan()
                ts.begin_of_the_begin = b
                ts.end_of_the_end = e
                ts.identified_by = vocab.DisplayName(content=date)
                dissolution.timespan = ts
                top.dissolved_by = dissolution

    def handle_person(self, rec, ident, top):
        #FIXME: add more property backups or nah?

        nss = self.nss 
        lang = self.lang
        me = rec.xpath(f"./rdf:Description[@rdf:about='https://data.bnf.fr/ark:/12148/{ident}#about']",namespaces=nss)[0]
        #if top doesn't have name, set it
        if not hasattr(top, 'identified_by'):
            name = self.to_plain_string(me.xpath('./foaf:name/text()', namespaces=nss)[0])
            top.identified_by = vocab.PrimaryName(content=name)
        if not hasattr(top, 'referred_to_by'):
            notes = me.xpath('./rdagroup2elements:biographicalInformation/text()',namespaces=nss)
            if len(notes) >= 1:
                notes = self.to_plain_string(notes[0])
            top.referred_to_by = vocab.Description(content=notes)

        gender = me.xpath('//foaf:gender/text()',namespaces=nss)
        if len(gender) >= 1:
            gender = self.to_plain_string(gender[0])
        if gender:
            if gender.lower() == "male":
                top.classified_as = vocab.instances['male']
            elif gender.lower() == "female":
                top.classified_as = vocab.instances['female']
 
        dob = me.xpath('./bio:birth/text()',namespaces=nss)
        if len(dob) >= 1:
            dob = self.to_plain_string(dob[0])
        dod = me.xpath('./bio:death/text()',namespaces=nss)
        if len(dod) >= 1:
            dod = self.to_plain_string(dod[0])

        if dob:
            self.make_timespan(dob, top, event="Birth")

        if dod:
            self.make_timespan(dod, top, event="Death")


    def handle_place(self, rec, ident, top):
        nss = self.nss
        #if top doesn't have name, set it
        me = rec.xpath(f"./rdf:Description[@rdf:about='https://data.bnf.fr/ark:/12148/{ident}#about']",namespaces=nss)[0]
        if not hasattr(top, 'identified_by'):
            name = self.to_plain_string(me.xpath('./rdfs:label/text()', namespaces=nss)[0])
            if name:
                top.identified_by = vocab.PrimaryName(content=name)

        lat = me.xpath('./geo:lat/text()',namespaces=nss)
        if len(lat) >= 1:
            lat = self.to_plain_string(lat[0])
        lon = me.xpath('./geo:long/text()',namespaces=nss)
        if len(lon) >= 1:
            lon = self.to_plain_string(lon[0])
        if lat and lon:
            top.defined_by = f"POINT ( {lon} {lat} )"


    def handle_group(self, rec, ident, top):
        #FIXME: add property backups?
        nss = self.nss 
        lang = self.lang
        me = rec.xpath(f"./rdf:Description[@rdf:about='https://data.bnf.fr/ark:/12148/{ident}#about']",namespaces=nss)[0]
        #if top doesn't have name, set it
        if not hasattr(top, 'identified_by'):
            name = self.to_plain_string(me.xpath('./foaf:name/text()', namespaces=nss)[0])
            top.identified_by = vocab.PrimaryName(content=name)

        if not hasattr(top, 'referred_to_by'):
            notes = me.xpath('./rdagroup2elements:corporateHistory/text()',namespaces=nss)
            if len(notes) >= 1:
                notes = self.to_plain_string(notes[0])
            reference = vocab.Description(content=notes)
            reference.language = lang
            top.referred_to_by = reference

        dof = me.xpath('./bnf-onto:firstYear[@rdf:datatype="http://www.w3.org/2001/XMLSchema#integer"]/text()', namespaces=nss)
        if len(dof) >= 1:
            dof = self.to_plain_string(dof[0])
        dod = me.xpath('./bnf-onto:lastYear[@rdf:datatype="http://www.w3.org/2001/XMLSchema#integer"]/text()', namespaces=nss)
        if len(dod) >= 1:
            dod = self.to_plain_string(dod[0])
        if dof:
            self.make_timespan(dof, top, event="Formation")
        if dod:
            self.make_timespan(dod, top, event="Dissolution")


    def handle_common(self, rec, top, common, topcls):
        lang = self.lang
        nss = self.nss

        if len(common) != 1:
            # uhoh
            return None
        else:
            common = common[0]

        try:
            pref = self.to_plain_string(common.xpath('./skos:prefLabel/text()', namespaces=nss)[0])
        except:
            pref = None

        close = common.xpath('./skos:closeMatch/@rdf:resource', namespaces=nss)
        alts = common.xpath('./skos:altLabel/text()', namespaces=nss)
        broaders = common.xpath('./skos:broader/@rdf:resource', namespaces=nss)
        #editorialNote = common.xpath('./skos:editorialNote/text()', namespaces=nss)
        scope = common.xpath('./skos:scopeNote/text()',namespaces=nss)
        note = common.xpath('./skos:note/text()',namespaces=nss)
        notes = note + scope
        exact = common.xpath('./skos:exactMatch/@rdf:resource',namespaces=nss)
        equivs = exact + close

        if pref:
            pn = vocab.PrimaryName(content=pref)
            pn.language = lang
            top.identified_by = pn

        if alts:
            for a in alts:
                a = self.to_plain_string(a)
                an = vocab.AlternateName(content=a)
                an.language = lang
                top.identified_by = an
        if broaders:
            if topcls == model.Place:
                for b in broaders:
                    b = self.to_plain_string(b)
                    top.part_of = topcls(ident=b)
            else:
                for b in broaders:
                    b = self.to_plain_string(b)
                    top.broader = topcls(ident=b)
        if equivs:
            for e in equivs:
                e = self.to_plain_string(e)
                top.equivalent = topcls(ident=e)

        if notes:
            for note in notes:
                note = self.to_plain_string(note)
                desc = vocab.Description(content=note)
                desc.language = lang
                top.referred_to_by = desc

    def transform(self, record, rectype=None, reference=False):
        nss = self.nss
        try:
            data = record['data']
            ident = record['identifier']
            xml = data['xml']
            rec = self.parse_xml(xml)
            if rec is None:
                return rec
        except Exception as e:
            return None 

        guess, rectype = self.guess_type(rec, ident)
        if not guess and not rectype:
            return None
        if guess:
            topcls = guess
        elif rectype:
            topcls = getattr(model, rectype)

        top = topcls(ident=f"https://data.bnf.fr/ark:/12148/{ident}")

        # Process XML
        common = rec.xpath(f"./rdf:Description[@rdf:about='https://data.bnf.fr/ark:/12148/{ident}']", namespaces=nss)
        self.handle_common(rec, top, common, topcls)


        fn = getattr(self, f"handle_{rectype.lower()}", None)
        if fn:
            fn(rec, ident, top)

        data = model.factory.toJSON(top)
        return {'identifier': record['identifier'], 'data': data, 'source': 'bnf'}

class BnfMapper(Mapper):
    def reconstitute(self, js, nodes):
        # recursively build tree from @graph node list
        # only replace into tree once to ensure non-infinite-recursion
        # ignore number, string, etc values
        del nodes[js['@id']]
        for (k,v) in js.items():
            if type(v) == dict and '@id' in v and v['@id'] in nodes:
                js[k] = self.reconstitute(nodes[v['@id']], nodes)
            elif type(v) == list:
                new = []
                for vi in v:
                    if type(vi) == dict and '@id' in vi and vi['@id'] in nodes:
                        new.append(self.reconstitute(nodes[vi['@id']], nodes))
                    else:
                        new.append(vi)
                js[k] = new
        return js


    def guess_type(self, data):
        typ = new.get('@type', "")
        focus = new.get('foaf:focus',{})
        foc_typ = focus.get('@type',"")
        if typ == "skos:Concept":
            return model.Type
        if foc_typ == "foaf:Person":
            return model.Person 
        if foc_typ == "foaf:Organization":
            return model.Group
        else: 
            topcls = None
        return None
        
        ##guess not currently being called in this code

    def transform(self, record, rectype="", reference=False):

        rec = record['data']
        topid = f"{self.namespace}{record['identifier']}"
        nodes = {}
        if not '@graph' in rec:
            new = rec   
        else:
            for n in rec['@graph']:
                try:
                    nodes[n['@id']] = n
                except:
                    pass
            try:
                new = self.reconstitute(nodes[topid], nodes)    
            except:
                return None
    
        topcls = getattr(model, rectype)
        top = topcls(ident=new['@id'])
        
        self.handle_common(new, top, topcls)

        # Per class specific stuff
        fn = getattr(self, f"handle_{rectype.lower()}", None)
        if fn:
            fn(new, top, topcls)
        
        data = model.factory.toJSON(top)
        return {'identifier': record['identifier'], 'data': data, 'source': 'bnf'}


    def handle_common(self, new, top, topcls):
        pref = new.get('skos:prefLabel', {})
        if type(pref) == str:
            pref = {"@value": pref}
        preflbl = pref.get('@value', "")
        language = pref.get('@language',"")
        prefl = vocab.PrimaryName(content=preflbl)
        lang = self.process_langs.get(language, None)
        if lang:
            prefl.language = lang
        top.identified_by = prefl
       
        alt = new.get('skos:altLabel',[])
        if not type(alt) == list:
            alt = [alt]
        for a in alt:
            if type(a) == str:
                altLbl = a
                language = ""
            else:
                altLbl = a.get('@value',"")
                language = a.get('@language',"")
            altl = vocab.AlternateName(content=altLbl)
            lang = self.process_langs.get(language, None)
            if lang:
                altl.language = lang
            top.identified_by = altl 

        broader = new.get('skos:broader',[])
        if type(broader) != list:
            broader = [broader]
        for bt in broader:
            uri = bt.get('@id',"")
            if uri:
                top.broader = topcls(ident=uri)
        if not broader:
            domaine = new.get('bnf-onto:domaine',[])
            if type(domaine) != list:
                domaine = [domaine]
            for do in domaine:
                uri = do.get('@id','')
                if uri:
                    top.broader = topcls(ident=uri)
        
        eq_dupes = {}
        exact = new.get('skos:exactMatch', [])
        if type(exact) != list:
            exact = [exact]
        for e in exact:
            uri = e.get('@id',"")
            if uri in eq_dupes:
                pass 
            else:
                top.equivalent = topcls(ident=uri)
                eq_dupes[uri] = 1
        
        equivs = new.get('skos:closeMatch', [])
        if type(equivs) != list:
            equivs = [equivs]
        for eq in equivs:
            uri = eq['@id']
            if uri in eq_dupes:
                pass
            else:
                top.equivalent = topcls(ident=uri)
                eq_dupes[uri] = 1

        scnote = new.get('skos:scopeNote',[])
        if type(scnote) != list:
            scnote = [scnote]
        for s in scnote:
            scnote_val = s['@value']
            scnote_lang = s['@language']
            if scnote_val:
                scnote_val = vocab.Description(content=scnote_val)
            if scnote_lang:
                lang = self.process_langs.get(scnote_lang, None)
                scnote_val.language = lang
            top.referred_to_by = scnote_val

        note = new.get('skos:note',[])
        if not type(note) == list:
            note = [note]
        for n in note:
            note_val = n['@value']
            note_lang = n['@language']
            if note_val:
                note_val = vocab.Description(content=note_val)
            if note_lang:
                lang = self.process_langs.get(note_lang, None)
                note_val.language = lang
            top.referred_to_by = note_val
    

    def handle_person(self, new, top, topcls): 
        focus = new.get('foaf:focus',{})

        dob = focus.get('bio:birth',"")
        if dob:
            ends = make_datetime(dob)
            if ends:
                ts = model.TimeSpan()
                ts.begin_of_the_begin = ends[0]
                ts.end_of_the_end = ends[1]
                ts.identified_by = vocab.DisplayName(content=dob)
                birth = model.Birth()
                birth.timespan = ts
                top.born = birth

        dod = focus.get('bio:death', "")
        if dod:
            ends = make_datetime(dod)
            if ends:
                ts = model.TimeSpan()
                ts.begin_of_the_begin = ends[0]
                ts.end_of_the_end = ends[1]
                ts.identified_by = vocab.DisplayName(content=dod)
                death = model.Death()
                death.timespan = ts
                top.died = death

        depiction = focus.get('foaf:depiction',[])
        if not type(depiction) == list:
            depiction = [depiction]
        if len(depiction) < 4:
            # Some people have thousands of images from objects associated with them
            # rather than of them. Bug in BNF data.
            for d in depiction:
                did = d['@id']
                if did:
                    image = model.VisualItem()
                    top.representation = image
                    dig_img = vocab.DigitalImage()
                    dig_img.access_point = model.DigitalObject(ident=did)
                    image.digitally_shown_by = dig_img 

        fields = focus.get('rdaa:P50100', [])
        if type(fields) != list:
            fields = [fields]
        for field in fields:
            if type(field) == str:
                top.referred_to_by = vocab.BiographyStatement(content=field)
            else:
                activity = field.get('@value',"")
                if activity:
                    top.referred_to_by = vocab.BiographyStatement(content=activity)

        gender = focus.get('foaf:gender',"")
        if gender:
            if gender == "male":
                top.classified_as = vocab.instances['male']
            elif gender == "female":
                top.classified_as = vocab.instances['female']
            elif gender != "male" or "female":
                top.classified_as = vocab.Gender(ident=gender)    

        eq_dupes = {}
        if hasattr(top, 'equivalent'):
            for eq in top.equivalent:
                eq_dupes[eq.id] = 1
        sameAs = focus.get('owl:sameAs',[])
        if type(sameAs) != list:
            sameAs = [sameAs]
        for same in sameAs:
            uri = same['@id']
            if 'bnf' in uri:
                pass
            elif uri in eq_dupes:
                pass
            else:
                eq_dupes[uri] = 1
                top.equivalent = topcls(ident=uri)

    def handle_group(self, new, top, topcls):
        focus = new.get('foaf:focus',{})

        dob = focus.get('bnf-onto:firstYear')
        if type(dob) == int:
            dob = str(dob)
            ends = make_datetime(dob)
            if ends:
                ts = model.TimeSpan()
                ts.begin_of_the_begin = ends[0]
                ts.end_of_the_end = ends[1]
                ts.identified_by = vocab.DisplayName(content=dob)
                birth = model.Formation()
                birth.timespan = ts
                top.formed_by = birth

        dod = focus.get('bnf-onto:lastYear')
        if type (dod) == int:
            dod = str(dod)
            ends = make_datetime(dod)
            if ends:
                ts = model.TimeSpan()
                ts.begin_of_the_begin = ends[0]
                ts.end_of_the_end = ends[1]
                ts.identified_by = vocab.DisplayName(content=dod)
                death = model.Dissolution()
                death.timespan = ts
                top.dissolved_by = death

        fields = focus.get('rdaa:P500022', [])
        if type(fields) != list:
            fields = [fields]
        for field in fields:
            if type(field) == str:
                top.referred_to_by = vocab.BiographyStatement(content=field)
            else:
                activity = field.get('@value',"")
                if activity:
                    top.referred_to_by = vocab.BiographyStatement(content=activity)

        eq_dupes = {}
        if hasattr(top, 'equivalent'):
            for eq in top.equivalent:
                eq_dupes[eq.id] = 1
        sameAs = focus.get('owl:sameAs',[])
        if type(sameAs) != list:
            sameAs = [sameAs]
        for same in sameAs:
            uri = same['@id']
            if 'bnf' in uri:
                pass
            elif uri in eq_dupes:
                pass
            else:
                eq_dupes[uri] = 1
                top.equivalent = topcls(ident=uri)
