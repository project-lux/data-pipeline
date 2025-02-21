from pipeline.process.base.fetcher import Fetcher

# Store the raw RDF/XML as a value inside the JSON blob
# Then mapper can sort it out
class GnFetcher(Fetcher):
    
    def post_process(self, data, identifier):
        if 'Please throttle your requests' in str(data):
            self.allow_network = False
            return None
        return data