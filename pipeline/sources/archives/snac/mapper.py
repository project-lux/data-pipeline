from pipeline.process.base.mapper import Mapper
from pipeline.process.utils.mapper_utils import make_datetime
from cromulent import model, vocab
import lxml.etree as ET
import re 

# keys:
# dataType= Constellation
# ark= arkid with http
# entityType['term'] == "person"
# maintenanceStatus: skip
# maintenanceAgency: skip
# maintenanceEvents: skip
# sources: related resources
#e.g.
#{'dataType': 'Source', 'type': {'id': '28296', 'term': 'simple', 'type': 'source_type'}, 
#'uri': 'http://www.worldcat.org/oclc/220209297', 'id': '69648192', 'version': '10180040'}
# conventionDeclarations: ??
# languagesUsed: across rec, not specific to each entry?
# nameEntries: primary and alternates
# occupations: occupation/roles. string from their vocab list but no uris given anywhere/ids don't resolve to anything
# biogHists: long text bio
# relations: a lot of related people/groups, only thing poss useful is maybeSameAs
# sameAsRelations
# resourceRelations: probably not useful right now
# places: carried_out professional locations. repetitive and messy. 
    #theoretically these have geonames which should be requestable EXCEPT none of the ids from Trumbull's rec returned anything valid
# subjects: same prob as occupations.
# nationalities: same prob as occupations.
# id: constellation id
# version: skip
# dates: birth and death dates

class SNACMapper(Mapper):
    def __init__(self, config):
        super().__init__(config)        
        self.lang = self.process_langs.get("en","")
    
    def do_setup(self, rec, rectype=""):
        if rectype == "":
            rectyp = rec['data']['entityType']['term']
            if rectyp in ['corporateBody','family']:
                topcls = model.Group
            else:
                topcls = model.Person

        top = topcls(ident=rec['data']['ark'])
        return(top, topcls)

    def transform(self, record, rectype=""):
        (top, topcls) = self.do_setup(record, rectype)    
        self.handle_common(record['data'], top)
        
        data = model.factory.toJSON(top)
        return {'data': data, 'identifier': record['identifier'], 'source': 'snac'}

    def make_timespan(self, date, top, event=""):
        try:
            b,e = make_datetime(date)
        except:
            b = None
        if b:
            self.create_event(event, b, e, date, top)

    def create_event(self, event, b, e, date, top):
        event_dict = {
            "Birth": model.Birth(),
            "Death": model.Death(),
            "Formation": model.Formation(),
            "Dissolution": model.Dissolution(),
            "Activity": vocab.Active()
        }

        if event in event_dict:
            event_obj = event_dict[event]
            ts = model.TimeSpan()
            ts.begin_of_the_begin = b
            ts.end_of_the_end = e
            ts.identified_by = vocab.DisplayName(content=date)
            event_obj.timespan = ts
            setattr(top, event.lower(), event_obj)

    def handle_common(self, rec, top):
        lang = self.lang
        names = rec.get("nameEntries",[])
        nmslist = []
        if names:
            for n in names:
                cont = n.get("original","")
                prefScr = n.get("preferenceScore","")
                if cont and prefScr:
                    nmslist.append({cont:prefScr})
                elif cont:
                    nmslist.append(cont)
        primary = False
        for name in nmslist:
            if type(name) == dict:
                for k, v in name.items():
                    if v == "99" and not primary:
                        pn = vocab.PrimaryName(content=k)
                        pn.language = lang
                        top.identified_by = pn
                        primary = True
                    else:
                        an = vocab.AlternateName(content=k)
                        an.language = lang
                        top.identified_by = an
            elif isinstance(name, str) and not primary:
                pn = vocab.PrimaryName(content=name)
                pn.language = lang
                top.identified_by = pn
                primary = True
            else:
                an = vocab.AlternateName(content=name)
                an.language = lang
                top.identified_by = an

        biog = rec.get("biogHists",[])
        if biog:
            for b in biog:
                text = b.get("text","")
                if text and text.startswith("<biogHist>"):
                    root = ET.fromstring(text)
                    text = ''.join(root.itertext())
                    text = re.sub(r'\s+', ' ', text.strip())
                bstat = vocab.BiographyStatement(content=text)
                lngblk = b.get("language",{})
                if lngblk:
                    term = lngblk.get("term","")
                    if len(term) == 3:
                        term = self.lang_three_to_two.get(term, None)
                    lang = self.process_langs.get(term, None)
                    bstat.language = lang
                top.referred_to_by = bstat

        rel = rec.get('relations',[])
        for r in rel:
            if r['type']['term'] == "mayBeSameAs":
                if top.__class__ == model.Person:
                    top.equivalent = model.Person(ident=r['targetArkID'])
                elif top.__class__ == model.Group:
                    top.equivalent = model.Group(ident=r['targetArkID'])

        same = rec.get('sameAsRelations',[])
        for s in same:
            if top.__class__ == model.Person:
                top.equivalent = model.Person(ident=s['uri'])
            elif top.__class__ == model.Group:
                top.equivalent = model.Group(ident=s['uri'])

        dates = rec.get("dates",[])
        if dates:
            for d in dates:
                fromType = d.get("fromType",{})
                toType = d.get("toType",{})
                if fromType:
                    fromTerm = fromType.get("term","")
                if toType:
                    toTerm = toType.get("term","")
                if fromTerm:
                    if fromTerm == "Birth":
                        dob = d.get("fromDate", "")
                        self.make_timespan(dob, top, event="Birth")
                    elif fromTerm == "Establishment":
                        formedDate = d.get("fromDate", "")
                        self.make_timespan(formedDate, top, event="Formation")
                    elif fromTerm == "Active":
                        activeStart = d.get("fromDate", "")
                    else:
                        activeStart = None
                if toTerm:
                    if toTerm == "Death":
                        dod = d.get("toDate", "")
                        self.make_timespan(dod, top, event="Death")
                    elif toTerm == "Disestablishment":
                        dissolvedDate = d.get("toDate", "")
                        self.make_timespan(dissolvedDate, top, event="Dissolution")
                    elif toTerm == "Active": 
                        activeEnd = d.get("toDate", "")
                    else:
                        activeEnd = None   
                
                if activeStart and activeEnd:
                    aDates = f"{activeStart} - {activeEnd}"
                    self.make_timespan(aDates, top, event="Activity")
                elif activeStart:
                    self.make_timespan(activeStart, top, event="Activity")
                elif activeEnd:
                    self.make_timespan(activeEnd, top, event="Activity")