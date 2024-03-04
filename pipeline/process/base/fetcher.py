import requests

class Fetcher(object):

    def __init__(self, config):
        self.name = config['name']
        self.fetch_uri = config['fetch']
        self.headers = config.get('fetch_headers', {})
        self.allow_redirects = True
        self.networkmap = {}
        self.timeout = 5
        self.refetch = False
        self.enabled = config['all_configs'].allow_network

    def post_process(self, data, identifier):
        return data

    def validate_identifier(self, identifier):
        return True

    def make_fetch_uri(self, identifier):
        if '#' in identifier:
            identifier = identifier.split('#', 1)[0]
        identifier = identifier.strip().replace(" ", '')
        if self.validate_identifier(identifier):
            return self.fetch_uri.format(identifier=identifier)
        else:
            return None

    def fetch(self, identifier):
        # fetch 
        if not self.enabled:
            return None

        url = self.make_fetch_uri(identifier)
        if not url:
            # print(f"Invalid identifier for {self.name}: {identifier}")
            return None

        if url in self.networkmap:
            resp = self.networkmap[url]
            if resp in ['0', '000'] or (len(resp) == 3 and resp.isnumeric() and int(resp) > 399):
                return None
            elif len(resp) > 3:
                # a previous redirect
                # XXX FIXME: Don't refollow? Do refollow? configurable?
                # Some redirects come from other data sources rather than the network
                # so can't just refollow everything. Default to respecting networkmap
                url = self.make_fetch_uri(resp)

        try:
            # print(f"Fetching {url}")
            resp = requests.get(url, headers=self.headers, 
                allow_redirects=self.allow_redirects, timeout=self.timeout)
        except:
            # Failed to open network, resolve DNS, or similar
            # FIXME: log
            print(f"Failed to get response from {url}")
            self.networkmap[url] = 0
            return None
        if resp.status_code == 200:
            # Got a response
            ct = resp.headers.get('content-type', '')
            if 'json' in ct:
                # good to store
                try:
                    data = resp.json()
                except:
                    data = {"value": resp.text, "ct": ct, "error": "json parse failed"}
            else:
                # Might still be json
                content = resp.text    
                data = {"value": resp.text, "ct": ct}
            data = self.post_process(data, identifier)
        else:
            # URL returned fail status
            # FIXME: log
            print(f"Got failure {resp.status_code} from {url}")
            self.networkmap[url] = resp.status_code
            return None

        # return a real record structure
        return {'data': data, 'source': self.name, 'identifier': identifier}
