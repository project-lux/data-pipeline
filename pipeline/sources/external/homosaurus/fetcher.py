
from pipeline.process.base.fetcher import Fetcher 

#LGBTQ+ adoption: https://homosaurus.org/v3/homoit0000809.jsonld
#
class HomosaurusFetcher(Fetcher):
    def validate_identifier(self, identifier):
        return True