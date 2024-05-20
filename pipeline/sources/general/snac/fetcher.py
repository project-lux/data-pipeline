
from pipeline.process.base.fetcher import Fetcher 

class SNACFetcher(Fetcher):
    def validate_identifier(self, identifier):
        return True