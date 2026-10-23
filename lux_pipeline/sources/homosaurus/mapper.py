
from lux_pipeline.process.base.mapper import Mapper
from cromulent import model, vocab

class HomosaurusMapper(Mapper):

    def __init__(self, config):
        Mapper.__init__(self, config)

    def transform(self, record, rectype=None, reference=False):
        # All should be Type
        data = record['data']

        uri = f"{self.namespace}{record['identifier']}"
        top = model.Type(ident=uri)

        preflbl = data.get('skos:prefLabel','')
        if preflbl and preflbl != '':
            top.identified_by = vocab.PrimaryName(content=preflbl)
        altlbls = data.get('skos:altLabel',[])
        if type(altlbls) != list:
            altlbls = [altlbls]
        if altlbls and altlbls != []:
            for alt in altlbls:
                top.identified_by = vocab.AlternateName(content=alt)

        sames = []
        same = data.get('skos:exactMatch',[])
        if type(same) != list:
            same = [same]
        if same and same != []:
            for s in same:
                for k,v in s.items():
                    if k == '@id':
                        sames.append(v)
        close = data.get('skos:closeMatch',[])
        if type(close) != list:
            close = [close]
        if close and close != []:
            for c in close:
                for k,v in c.items():
                    if k == '@id':
                        if v not in sames:
                            sames.append(v)
        for s in sames:
            top.equivalent = model.Type(ident=s)


        note = data.get('rdfs:comment','')
        if note and note != '':
            top.referred_to_by = vocab.Note(content=note)

        broader = data.get('skos:broader',[])
        if type(broader) != list:
            broader = [broader]
        if broader and broader != []:
            for b in broader:
                for k,v in b.items():
                    if k == '@id':
                        top.broader = model.Type(ident=v)
     
        data = model.factory.toJSON(top)
        return {'data': data, 'identifier': record['identifier'], 'source': 'homosaurus'}
