

# NOTE WELL - this doesn't actually load from a dump file
# it uses wikidata to inverse all the referenced subjects and looks them up online.

# XXX FIXME - this is a terrible hack


class JapanDereferencer(object):

    def __init__(self, config):
        self.config = config
        self.configs = config['all_configs']

    def load(self, slice, max_slice):
        wd_rc = self.configs['wikidata']['recordcache']
        wd_dc = self.configs['wikidata']['datacache']
        jsh_dc = self.configs['japansh']['datacache']
        jna_dc = self.configs['japan']['datacache']

        jna_f = self.configs['japan']['fetcher']
        jsh_f = self.configs['japansh']['fetcher']

        for key in wd_rc.iter_keys():
            dk = self.configs.split_qua(key)[0]
            rec = wd_dc[dk]
            if 'P349' in rec['data']:
                print(dk)
                # Try and fetch from na first
                for ident in rec['data']['P349']:
                    uri = f"http://id.ndl.go.jp/auth/ndlna/{ident}.json"
                    resp = requests.get(uri, allow_redirects=False)
                    if resp.status_code == 200:
                        # we have it
                        pass
                    else:
                        # Try sh
                        
                        
