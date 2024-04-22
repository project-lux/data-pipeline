
import sys
import time
from sqlitedict import SqliteDict

class LCIndexLoader(object):
    def __init__(self, config):
        self.in_cache = config['datacache']
        self.out_path = config['reconcileDbPath']
        self.config = config
        self.namespace = config['namespace']

    def load(self):
        in_cache = self.config['datacache']
        rec_cache = self.config['recordcache']
        out_path = self.config['reconcileDbPath']
        inverse_path = self.config['inverseEquivDbPath']
        if out_path:
            index = SqliteDict(out_path, autocommit=False)
        else:
            index = None
        if inverse_path:
            eqindex = SqliteDict(inverse_path, autocommit=False)
        else:
            eqindex = None
        mapper = self.config['mapper']
        ttl = self.config.get('totalRecords', -1)
        n = 0

        # Clear all current entries
        if index is not None:
            index.clear()
        if eqindex is not None:
            eqindex.clear()

        start = time.time()
        for record in in_cache.iter_records():
            recid = record['identifier']

            # Can't just use mapper, as mapper needs access to the index for reconciling
            # names of places etc.

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
                new = mapper.reconstitute(nodes[topid], nodes)    
            except:
                continue          

            typ = mapper.guess_type(new)

            if index is not None:
                prefs = new.get('madsrdf:authoritativeLabel', [])
                if type(prefs) == str:
                    prefs = {'@value': prefs}
                if type(prefs) == dict:
                    prefs = [prefs]
                if not prefs:
                    continue
                for p in prefs:
                    if type(p) != dict:
                        print(f"Got weird prefLabel {p} in {recid}")
                        continue
                    val = p['@value']
                    index[val.lower()] = [recid, typ]

            if eqindex is not None:

                # Some records have externals in identifiesRWO
                idby = new.get('madsrdf:identifiesRWO', [])
                if type(idby) != list:
                    idby = [idby]
                ex = new.get('madsrdf:hasExactExternalAuthority', [])
                if type(ex) != list:
                    ex = [ex]
                # skos:closeMatch -- Only as a last resort
                close = new.get('madsrdf:hasCloseExternalAuthority', [])
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

                doneids = []
                for eid in eqs:
                    if eid in doneids:
                        continue
                    doneids.append(eid)  
                    eqindex[eid] = [recid, typ]

            n += 1
            if not n % 10000:
                if index is not None:
                    index.commit()
                if eqindex is not None:
                    eqindex.commit()
                durn = time.time()-start
                print(f"{n} of {ttl} in {int(durn)} = {n/durn}/sec -> {ttl/(n/durn)} secs")
                sys.stdout.flush()
        if index is not None:
            index.commit()
        if eqindex is not None:
            eqindex.commit()
        