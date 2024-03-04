
import requests
from pipeline.process.base.fetcher import Fetcher


class BnfXmlFetcher(Fetcher):

    def fetch(self, identifier):


        if not self.enabled:
            return None

        url = self.make_fetch_uri(identifier)

        try:
            resp = requests.get(url, headers=self.headers, timeout=self.timeout)
        except:
            return None
        if resp.history:
            base = resp.history[-1].url
            if not base.endswith('/'):
                # something went wrong
                return None
            url = base + "rdf.xml"
        else:
            return None

        try:
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
            expected_ct = "application/rdf+xml"
            data = {'xml': resp.text}            

        else:
            # URL returned fail status
            print(f"Got failure {resp.status_code} from {url}")
            self.networkmap[url] = resp.status_code
            return None

        # return a real record structure
        return {'data': data, 'source': self.name, 'identifier': identifier}



class BnfFetcher(Fetcher):

    def fetch(self, identifier):

        if not self.enabled:
            return None

        url = self.make_fetch_uri(identifier)
        url = url.replace("/12148/12148/", "/12148/")

        if url in self.networkmap:
            return None

        if not url:
            return None
        try:
            resp = requests.get(url)
        except:
            return None
        if resp.history:
            base = resp.history[-1].url
            if not base.endswith('/'):
                # something went wrong
                return None
            url = base + "rdf.jsonld"
        else:
            return None

        try:
            print(f"Fetching {url}")
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

        if '@context' in data:
            del data['@context']

        # return a real record structure
        return {'data': data, 'source': self.name, 'identifier': identifier}
