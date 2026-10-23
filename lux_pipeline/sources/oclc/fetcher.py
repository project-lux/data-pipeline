from lux_pipeline.process.base.fetcher import Fetcher
import requests

class ViafFetcher(Fetcher):

    def fetch(self, identifier):
        # fetch 
        if not self.enabled:
            return None

        url = self.make_fetch_uri(identifier)
        try:
            print(f"Fetching {url}")
            resp = self.session.get(url, allow_redirects=False)
        except:
            # Failed to open network, resolve DNS, or similar
            # FIXME: log
            print(f"Failed to get response from {url}")
            self.networkmap[url] = 0
            return None
        if resp.status_code == 200:
            # Got a response
            ct = resp.headers.get('content-type', '')
            if 'xml' in ct:
                value = resp.text
                if "directto" in value and "abandoned_viaf_record" in value:
                    # redirect :(
                    start = value.index('directto>')
                    end = value[start+9:].index('<')
                    newid = value[start+9:start+9+end]
                    return self.fetch(newid)
                # else good to store
            else:
                # Might still be xml???
                content = resp.text    
                if resp.text.startswith('<?xml version="1.0"'):
                    value = resp.text
                else:
                    # Probably an error?
                    return None          
            data = {"xml": value}

        elif resp.status_code > 299 and resp.status_code < 400:
            # Got a redirected record :/
            newurl = resp.next.url
            # print(f"Got redirect to {newurl}")
            # Need to communicate the new identifier
            # turn redirect into identifier...
            newid = newurl.rsplit('/', 2)[-2]
            if newid == identifier:
                # uhoh, infinite loop somewhere...
                self.networkmap[url] = 0
                return None
            new = self.fetch(newid)
            if new:
                data = new['data']
                identifier = newid
                self.networkmap[url] = newid
            else:
                self.networkmap[url] = 0
                return None

        else:
            # URL returned fail status
            # FIXME: log
            print(f"Got failure {resp.status_code} from {url}")
            self.networkmap[url] = resp.status_code
            return None
        return {'data': data, 'identifier': identifier, 'source': self.name}

