
from pipeline.process.base.mapper import Mapper
from cromulent import model, vocab

class GbifMapper(Mapper):

    def __init__(self, config):
        Mapper.__init__(self, config)
        self.rank_types = {
            "kingdom": "http://www.wikidata.org/entity/Q36732",
            "phylum": "http://www.wikidata.org/entity/Q38348",
            "subphylum": "http://www.wikidata.org/entity/Q1153785",
            "superclass": "http://www.wikidata.org/entity/Q3504061",
            "class": "http://www.wikidata.org/entity/Q37517",
            "subclass": "http://www.wikidata.org/entity/Q5867051",
            "superorder": "http://www.wikidata.org/entity/Q5868144",
            "order": "http://www.wikidata.org/entity/Q36602",
            "family": "http://www.wikidata.org/entity/Q35409",
            "genus": "http://www.wikidata.org/entity/Q34740",
            "species": "http://www.wikidata.org/entity/Q7432",
            "subspecies": "http://www.wikidata.org/entity/Q68947"
        }


    def transform(self, record, rectype=None,reference=False):
        # All should be Type
        data = record['data']

        uri = f"{self.namespace}{data['key']}"
        top = model.Type(ident=uri)
        rank = data['rank'].lower()

        names = []

        if 'canonicalName' in data and data['canonicalName']:
            names.append(data['canonicalName'])
        if 'vernacularName' in data and data['vernacularName']:
            names.append(data['vernacularName'])
        if not names and 'scientificName' in data and data['scientificName']:
            names.append(data['scientificName'])
        if not names and rank and rank in data and data[rank]:
            names.append(data[rank])

        top._label = names[0]
        top.identified_by = vocab.PrimaryName(content=names[0])
        for n in names[1:]:
            top.identified_by = vocab.AlternateName(content=n)

        if 'parentKey' in data and data['parentKey']:
            top.broader = model.Type(ident=f"{self.namespace}{data['parentKey']}")

        if rank in self.rank_types:
            top.classified_as = model.Type(ident=self.rank_types[rank])

        #check for descriptions
        fetcher = self.configs.external['gbif']['fetcher']
        try:
            rec = fetcher.fetch(f"{data['key']}/descriptions")
        except:
            rec = None 
        if rec is not None:
            descs = rec['data']['results']
            for d in descs:
                desc = d['description']
                #toss out useless descriptions
                if desc.startswith("Figs"):
                    continue
                lo = model.LinguisticObject(content=desc)
                top.referred_to_by = lo
                if 'source' in d:
                    source = d['source']
                    aa = model.AttributeAssignment()
                    aa.referred_to_by = model.LinguisticObject(content=source)
                    lo.assigned_by = aa
                dlang = d.get('language', '')
                if len(dlang) == 3:
                    dlang = self.lang_three_to_two.get(dlang, None)
                lang = self.process_langs.get(dlang, None)
                if lang is not None:
                    lo.language = lang 

        #check for alt identifiers
        try:
            rec = fetcher.fetch(f"{data['key']}/speciesProfiles")
        except:
            rec = None 
        if rec is not None:
            altids = rec['data']['results']
            for a in altids:
                altid = a['sourceTaxonKey']
                altname = vocab.AlternateName(content=altid)
                top.identified_by = altname
                if 'source' in a:
                    source = a['source']
                    aa = model.AttributeAssignment()
                    aa.referred_to_by = model.LinguisticObject(content=source)
                    altname.assigned_by = aa 


        data = model.factory.toJSON(top)
        return {'data': data, 'identifier': record['identifier'], 'source': 'gbif'}
