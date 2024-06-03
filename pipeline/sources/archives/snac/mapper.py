from pipeline.process.base.mapper import Mapper
from pipeline.process.utils.mapper_utils import make_datetime
from cromulent import model, vocab

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
# occupations: occupation/roles. string from their vocab list but no uris given anywhere.
# biogHists: long text bio
# relations: a lot of related people/groups, only thing poss useful is:
rel = rec['relations']
for r in rel:
    if r['type']['term'] == "mayBeSameAs":
        ARKsameAs = r['targetArkID']
# sameAsRelations
same = rec['sameAsRelations']
for s in same:
    uri = s['uri']
#https://viaf.org/viaf/56862633
#https://www.worldcat.org/identities/lccn-n50011728
#https://id.loc.gov/authorities/n50011728
#https://www.wikidata.org/entity/Q369263

# resourceRelations: probably not useful right now
# places: carried_out professional locations. repetitive and messy. only use ones with uris?
# subjects: skip
# nationalities: string only
# id: constellation id
# version: skip
# dates: birth and death dates

class SNACMapper(Mapper):
    def __init__(self, config):
        Mapper.__init__(self, config)
        self.lang = self.process_langs.get("en","")
    

    def do_setup(self, rec, rectype=""):
        if rectype == "":
            rectyp = rec['data']['entityType']['term']
            if rectyp in ['corporateBody','family']:
                topcls = model.Group
            else:
                topcls = model.Person

        top = topcls(ident=rec['ark'])
        return(top, topcls)

    def transform(self, record, rectype=""):
        (top, topcls) = self.do_setup(record, rectype)    
        self.handle_common(record, top)

        if not rectype:
            rectype = topcls.__name__
        #handle classes
        fn = getattr(self, f"handle_{rectype.lower()}")
        if fn:
            fn(record, top, topcls)
        
        data = model.factory.toJSON(top)
        return {'data': data, 'identifier': record['identifier'], 'source': 'snac'}

    def handle_common(self, rec, top):
        #names
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


        desc = rec.get('description','')
        if desc:
            top.referred_to_by = vocab.Description(content=desc)
        short_desc = rec.get('short_description','')
        if short_desc:
            top.referred_to_by = vocab.Description(content=short_desc)

    def handle_timespan(self, event, date1, date2=None, date3=None):
        pass

    