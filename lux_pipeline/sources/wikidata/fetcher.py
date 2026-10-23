
from lux_pipeline.process.base.fetcher import Fetcher

class WdFetcher(Fetcher):

    def validate_identifier(self, identifier):
        if not identifier.startswith('Q') or not len(identifier) >= 2 or not identifier[1:].isdigit():
            return False
        return True

    def make_fetch_uri(self, identifier):
        if identifier.startswith('q'):
            identifier = "Q" + identifier[1:]
        return Fetcher.make_fetch_uri(self, identifier)

    def post_process(self, js, identifier):
        new = {}
        if 'entities' in js:
            js = js['entities']
            js = js[list(js.keys())[0]]

        new['id'] = js['id']

        lbls = {}
        for (k,v) in js['labels'].items():
            lbls[k] = v['value']
        descs = {}
        for (k,v) in js['descriptions'].items():
            descs[k] = v['value']   
        altLbls = {}
        for (k,v) in js['aliases'].items():
            altLbls[k] = [z['value'] for z in v]
        new['prefLabel'] = lbls
        new['altLabel'] = altLbls
        new['description'] = descs

        # Include enwiki sitelink
        if 'sitelinks' in js and 'enwiki' in js['sitelinks']:
            new['sitelinks'] = {'enwiki': js['sitelinks']['enwiki']}

        for (prop, vals) in js['claims'].items():
            newvals = []
            for val in vals:
                if val['mainsnak']['snaktype'] in ["somevalue", 'novalue']:
                    continue
                if val['rank'] == 'deprecated':
                    continue
                if 'qualifiers' in val and 'P2241' in val['qualifiers']:
                    # deprecated reason, but not deprecated rank
                    continue
                if prop in ['P131','P17','P2283','P276']:
                    # Don't include historical part_ofs or Activity took_place_at
                    if 'qualifiers' in val and 'P582' in val['qualifiers']:
                        continue

                dv = val['mainsnak']['datavalue']
                dvt = dv['type']
                if dvt == "wikibase-entityid":
                    dvv = dv['value']['id']
                elif dvt in ["external-id", 'string']:
                    dvv = dv['value']
                elif dvt == "time":
                    dvv = {'time': dv['value']['time'], 'precision': dv['value']['precision']}
                elif dvt in ["commonsMedia", 'url']:
                    dvv = dv['value']['value']
                elif dvt == "monolingualtext":
                    t = dv['value']['text']
                    lang = dv['value']['language']
                    dvv = {lang:t}
                elif dvt == "quantity":
                    t = dv['value']['amount']
                    unit = dv['value']['unit']
                    dvv = [t, unit]
                elif dvt == "globecoordinate":              
                    dvv = {'lat':dv['value']['latitude'], 'long':dv['value']['longitude'], 'alt':dv['value']['altitude']}
                else:
                    print(f"{js['id']} has {prop} with unknown datatype {dvt}: {dv}")
                    continue
                newvals.append(dvv)
            new[prop] = newvals

        return new
