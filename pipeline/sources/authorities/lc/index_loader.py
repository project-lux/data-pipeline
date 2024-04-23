
from pipeline.process.base.index_loader import LmdbIndexLoader

class LCIndexLoader(LmdbIndexLoader):

    def acquire_record(self, rec):
        rec = record['data']
        if '@id' in rec and rec['@id'].startswith('/authorities/'):
            ident = rec['@id'].rsplit('/', 1)[1]
            topid = f"{self.namespace}{ident}"
        elif '@context' in rec and type(rec['@context']) == dict:
            topid = rec['@context']['about']
        else:
            identifier = record['identifier']
            topid = f"{self.namespace}{identifier}"
        if len(rec.keys()) == 1 and 'value' in rec:
            rec['@graph'] = rec['value']
            del rec['value']

        nodes = {}
        for node in rec['@graph']:
            try:
                nodes[node['@id']] = node
            except:
                pass
        try:
            new = self.mapper.reconstitute(nodes[topid], nodes)    
        except:
            return None          
        return new

    def extract_names(self, rec):
        vals = []
        prefs = rec.get('madsrdf:authoritativeLabel', [])
        if type(prefs) == str:
            prefs = {'@value': prefs}
        if type(prefs) == dict:
            prefs = [prefs]
        for p in prefs:
            if type(p) != dict:
                print(f"Got weird prefLabel {p} in {recid}")
                continue
            val = p['@value']
            vals.append(val)
        return vals       

    def extract_uris(self, rec):
        # Some records have externals in identifiesRWO
        idby = rec.get('madsrdf:identifiesRWO', [])
        if type(idby) != list:
            idby = [idby]
        ex = rec.get('madsrdf:hasExactExternalAuthority', [])
        if type(ex) != list:
            ex = [ex]
        # skos:closeMatch -- Only as a last resort
        close = rec.get('madsrdf:hasCloseExternalAuthority', [])
        if type(close) != list:
            close = [close]

        eqs = []
        sawviaf = False
        sawwd = False
        for i in idby:
            if type(i) == str:
                i = {"@id": i}
            # drop dbpedia, bbc, musicbrainz
            uri = i['@id']
            if 'dbpedia.org' in uri or 'bbc.co.uk' in uri or 'musicbrainz.org' in uri:
                continue
            elif '/tgn/' in uri and '-place' in uri:
                uri = uri.replace('-place', '')
            elif 'viaf.org/viaf' in uri:
                sawviaf = True
            elif 'loc.gov/rwo' in uri:
                # handled separately
                continue
            eqs.append(uri)
        for e in ex:
            if type(e) == str:
                e = {"@id": e}            
            uri = e['@id']
            if uri in eqs:
                continue
            elif 'viaf.org/viaf/' in uri and sawviaf == True:
                continue
            eqs.append(uri)
        # Only add wd, or if few than 4
        if sawwd == False or len(eqs) < 4:
            for c in close:
                if type(c) == str:
                    c = {"@id": c}
                uri = c['@id']
                if 'bnf.fr/' in uri:
                    continue
                elif uri in eqs:
                    continue
                elif sawwd == False:
                    if 'wikidata' in uri:
                        eqs.append(uri)
                        if len(eqs) >= 4:
                            break
                        else:
                            continue
                eqs.append(uri)          
        return eqs
        