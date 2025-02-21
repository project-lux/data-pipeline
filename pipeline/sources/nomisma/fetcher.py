
from pipeline.process.base.fetcher import Fetcher 

#works
class NomismaFetcher(Fetcher):
    def validate_identifier(self, identifier):
        return True