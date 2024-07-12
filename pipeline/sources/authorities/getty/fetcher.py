from pipeline.process.base.fetcher import Fetcher
import requests
import ujson as json
import os

### 2024-04 RS: Not sure this is necessary now that Getty has made linked art the default serialization
### for the vocabs


class GettyFetcher(Fetcher):
    def __init__(self, config):
        Fetcher.__init__(self, config)
        fn = os.path.join(config["all_configs"].data_dir, "getty_replacements.json")
        if os.path.exists(fn):
            fh = open(fn)
            data = fh.read()
            fh.close()
        else:
            pass
        try:
            js = json.loads(data)
        except:
            js = {"results": {"bindings": []}}
        self.redirects = {}
        res = js["results"]["bindings"]
        for r in res:
            f = r["from"]["value"].replace("http://vocab.getty.edu/", "")
            t = r["to"]["value"].replace("http://vocab.getty.edu/", "")
            self.redirects[f] = t

    def fetch(self, identifier):
        if not self.enabled:
            return None

        # Should have dealt with -agent and -place upstream!
        if not identifier.isnumeric():
            print(f"Getty fetcher got bad identifier: {identifier}")
            return None

        f = f"{self.name}/{identifier}"
        if f in self.redirects:
            identifier = self.redirects[f].split("/")[1]

        result = Fetcher.fetch(self, identifier)

        if not result:
            # Try and fetch original from vocab.getty.edu
            newurl = f"http://vocab.getty.edu/{self.name}/{identifier}.jsonld"
            # FIXME: This should be more robust

            if newurl in self.networkmap:
                return None

            try:
                print(f"Fetching {newurl}")
                resp = self.session.get(newurl)
            except Exception as e:
                # FIXME: Log network failure
                self.networkmap[newurl] = 0
                print(e)
                return None
            if resp.status_code == 200:
                data = json.loads(resp.text)
                if type(data) == list:
                    try:
                        newid = data[0]["http://purl.org/dc/terms/isReplacedBy"][0]["@id"]
                        newid = newid.replace(f"http://vocab.getty.edu/{self.name}/", "")
                        print(f"Got new id for {identifier}: {newid}")
                    except:
                        print("got nuttin")
                        return None
                    res = Fetcher.fetch(self, newid)
                    old = self.make_fetch_uri(identifier)
                    self.networkmap[old] = newid
                    return res
            else:
                self.networkmap[newurl] = resp.status_code
                return None
            return {"data": result, "identifier": identifier, "source": self.name}
        else:
            if did := result["data"].get("id", None):
                if "data.getty.edu" in did:
                    result["data"]["id"] = did.replace("https://data.getty.edu/vocab/", "http://vocab.getty.edu/")
            return result
