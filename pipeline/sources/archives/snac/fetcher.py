from pipeline.process.base.fetcher import Fetcher

class SnacFetcher(Fetcher):

    def validate_identifier(self, identifier):
        if not identifier.startswith('Q') or not len(identifier) >= 2 or not identifier[1:].isdigit():
            return False
        return True

    def fetch(self, identifier):
        pass
