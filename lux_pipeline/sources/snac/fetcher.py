from lux_pipeline.process.base.fetcher import Fetcher
#we might want to increase the timeout time for SNAC as bigger records take longer to load, e.g. Yale w6r8240t


class SNACFetcher(Fetcher):
    def validate_identifier(self, identifier):
        return True
