
from pipeline.process.base.fetcher import Fetcher

prefx = {
    "skos":"http://www.w3.org/2004/02/skos/core#",
    "madsrdf":"http://www.loc.gov/mads/rdf/v1#",
    "owl":"http://www.w3.org/2002/07/owl#",
    "identifiers":"http://id.loc.gov/vocabulary/identifiers/",
    "bflc":"http://id.loc.gov/ontologies/bflc/",
    "ri":"http://id.loc.gov/ontologies/RecordInfo#",
    "cs":"http://purl.org/vocab/changeset/schema#",
    "skosxl": "http://www.w3.org/2008/05/skos-xl#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#", 
    "bf": "http://id.loc.gov/ontologies/bibframe/",
    "foaf": "http://xmlns.com/foaf/0.1/"
}

# Rewrite to match the (more usable) dump file format
# This would be reversible if needed, so not mapping
def prefixify(js):
    njs = {}
    for (k,v) in js.items():
        if k == '@type':
            ntyps = []
            for t in v:
                found = False
                for (pfx,ns) in prefx.items():
                    if t.startswith(ns):
                        ntyps.append(t.replace(ns, f"{pfx}:"))
                        found = True
                        break
                if not found:
                    ntyps.append(t)
            njs['@type'] = ntyps            
        elif k == '@id':
            njs[k] = v
        else:
            found = False
            for (pfx,ns) in prefx.items():
                if k.startswith(ns):
                    njs[k.replace(ns, f"{pfx}:")] = v
                    found = True
                    break
            if not found:
                print(k, v)
    return njs

class LcshFetcher(Fetcher):


    def validate_identifier(self, identifier):
        if identifier.startswith('tmp'):
            return False
        return True

    def post_process(self, js, identifier):
        # Rewrite to look like the dump files
        # reframing to a dict happens in mapper
        if type(js) == list:
            new = []
            for entry in js:
                new.append(prefixify(entry))
        else:
            new = [prefixify(js)]
        return {'@context': {'about':f'http://id.loc.gov/authorities/subjects/{identifier}'}, '@graph':new}

class LcnafFetcher(Fetcher):

    def post_process(self, js, identifier):
        if type(js) == list:
            new = []
            for entry in js:
                new.append(prefixify(entry))
        else:
            new = [prefixify(js)]
        return {'@context': {'about':f'http://id.loc.gov/authorities/names/{identifier}'}, '@graph':new}


class LcdgtFetcher(Fetcher):

    def post_process(self, js, identifier):
        if type(js) == list:
            new = []
            for entry in js:
                new.append(prefixify(entry))
        else:
            new = [prefixify(js)]
        return {'@context': {'about':f'http://id.loc.gov/authorities/demographicTerms/{identifier}'}, '@graph':new}

