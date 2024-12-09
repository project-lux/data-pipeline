
from pipeline.process.base.fetcher import Fetcher

class WmFetcher(Fetcher):

    def __init__(self, config):
        Fetcher.__init__(self, config)
        # Just allow it always as otherwise new images won't show
        self.enabled = True

    def make_fetch_uri(self, identifier):
        if '#' in identifier:
            identifier = identifier.split('#', 1)[0]
        return self.fetch_uri.format(identifier=identifier)
