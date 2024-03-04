
# Base fetcher does the right thing so far

from pipeline.process.base.fetcher import Fetcher
from SPARQLWrapper import SPARQLWrapper, JSON


class JapanFetcher(Fetcher):
    def validate_identifier(self, identifier):
        return True

class JapanShFetcher(Fetcher):
    def validate_identifier(self, identifier):
        return True


class JapanSparqlFetcher(object):

#query = """
# PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
# SELECT ?topic WHERE {
#   ?topic skos:exactMatch <http://viaf.org/viaf/sourceID/NDL%7C00270331#skos:Concept> .
# }
#"""

    def __init__(self, configs):
        self.configs = configs
        self.prefixes = """
        prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        prefix skos: <http://www.w3.org/2004/02/skos/core#>
        prefix xsd: <http://www.w3.org/2001/XMLSchema#>
        prefix ndl: <http://ndl.go.jp/dcndl/terms/>
        prefix foaf: <http://xmlns.com/foaf/0.1/>
        prefix dc: <http://purl.org/dc/terms/>
        prefix owl: <http://www.w3.org/2002/07/owl#>
        prefix skosxl: <http://www.w3.org/2008/05/skos-xl#>
        prefix frbrent: <http://RDVocab.info/uri/schema/FRBRentitiesRDA/>
        prefix rda: <http://RDVocab.info/ElementsGr2/>
        """

    def search(self, query, auth="ndlna"):
        sparql = SPARQLWrapper(f"https://id.ndl.go.jp/auth/{auth}/")
        sparql.setReturnFormat(JSON)

        query = self.prefixes + "\n" + query
        sparql.setQuery(query)

        try:
            ret = sparql.queryAndConvert()
            return ret['results']['bindings']
        except Exception as e:
            print(f"Japan fetcher: {e}")
            return None

    def fetch_records(self, query, auth="ndlna"):
        results = self.search(query, auth)

        if auth == "ndlna":
            fetcher = self.configs['japan']['fetcher']
        else:
            fetcher = self.configs['japansh']['fetcher']

        records = []
        x = 0
        for result in results:
            value = list(result.values())[0]
            uri = value['value']
            rest, identifier = uri.rsplit('/', 1)
            record = fetcher.fetch(identifier)
            records.append(record)
            x += 1
            if x > 25:
                break
        return records

