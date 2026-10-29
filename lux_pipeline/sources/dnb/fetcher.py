
import logging
import requests
from lux_pipeline.process.base.fetcher import Fetcher

logger = logging.getLogger("lux_pipeline")

class DnbFetcher(Fetcher):
    def validate_identifier(self, identifier):
        # Can't rule out 4????-? as it is also used for subjects
        # which don't exist in the culturegraph hub 
        return True


    def fetch(self, identifier):
        if not self.enabled:
            return None
        result = Fetcher.fetch(self, identifier)
        if not result:
            # try the d-nb.info version
            newurl = f"https://d-nb.info/gnd/{identifier}/about/lds.jsonld"

            if newurl in self.networkmap:
                return None

            # FIXME: This should also be more robust
            try:
                logger.debug(f"Fetching {newurl}")
                resp = requests.get(newurl)
            except:
                # FIXME: Log network failure
                self.networkmap[newurl] = 0
                return None
            if resp.status_code == 200:
                data = resp.json()
                if type(data) == list:
                    data = {'list': data}
                return {'data': data, 'identifier': identifier, 'source': self.name}
            else:
                self.networkmap[newurl] = resp.status_code
                return None                
        else:
            return result