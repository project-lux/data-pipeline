import requests
import ujson as json
from requests_toolbelt.multipart import decoder
import re
import threading
import time
from requests.packages.urllib3.fields import RequestField
from requests.packages.urllib3.filepost import encode_multipart_formdata

from requests.auth import HTTPDigestAuth, HTTPBasicAuth
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class RequestThread(threading.Thread):

    def run(self):
        params = {'database': self.store.database}
        fields = [self.store.metadata_field]
        bx = 0
        for rec in self.batch:
            jstr = json.dumps(rec, separators=(',',':'))
            target = rec['json']['id']
            rf = RequestField(name=f"data{bx}", data=jstr, filename=target)
            rf.make_multipart(content_disposition='attachment', content_type="application/json")
            fields.append(rf)  
            bx += 1

        post_body, content_type = encode_multipart_formdata(fields)
        post_ct = ''.join(('multipart/mixed',) + content_type.partition(';')[1:])
        headers = self.store.headers.copy()
        headers['Content-Type'] = post_ct
        start = time.time()
        resp = requests.post(self.store.rest, params=params, auth=self.store.auth, \
            headers=headers, data=post_body, timeout=self.store.timeout_short, verify=self.store.verify_ssl)
        if resp.status_code != 200:
            print(resp.text)
        print(f"POST finished in {time.time() - start}: {resp.status_code}")
        self.result = resp

class MarkLogicStore(object):

    def __init__(self, config):

        self.database = config.get('database', 'lux-content')
        self.search = config['search']
        self.sparql = config['sparql']
        self.rest = config['rest']
        self.xquery = config['xquery']
        self.verify_ssl = config.get('verify_ssl', True)

        auth = config['auth']
        if auth['type'] == "HTTPDigestAuth":
            self.auth = HTTPDigestAuth(auth['user'], auth['password'])
        elif auth['type'] == "HTTPBasicAuth":
            self.auth = HTTPBasicAuth(auth['user'], auth['password'])            
        else:
            raise ValueError(f"Unknown auth type {auth['type']} for ML store {config['name']}")

        self.apps = config.get('apps', '')
        self.headers = {"Accept": "application/json"}
        self.timeout_short = 30
        self.timeout_long = 3600
        self.permissions = {'lux-endpoint-consumer': 'read', 'lux-writer': 'update'}

        self.threads = [] 
        self.max_threads = 3

        md = """
<rapi:metadata xmlns:rapi="http://marklogic.com/rest-api"
    xmlns:prop="http://marklogic.com/xdmp/property">
    <rapi:permissions>
        <rapi:permission>
            <rapi:role-name>lux-endpoint-consumer</rapi:role-name>
            <rapi:capability>read</rapi:capability>
        </rapi:permission>
        <rapi:permission>
            <rapi:role-name>lux-writer</rapi:role-name>
            <rapi:capability>update</rapi:capability>
        </rapi:permission>
    </rapi:permissions>
</rapi:metadata>"""
        md_type = "application/xml"
        mdrf = RequestField(name=f"defaultMetadata", data=md)
        mdrf.make_multipart(content_disposition='attachment; category=metadata',content_type=md_type)
        self.metadata_field = mdrf

        self.prefixes = """
        prefix lux: <https://lux.collections.yale.edu/ns/>
        prefix crm: <http://www.cidoc-crm.org/cidoc-crm/>
        prefix la: <https://linked.art/ns/terms/>
        prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        prefix skos: <http://www.w3.org/2004/02/skos/core#>
        prefix sci: <http://www.ics.forth.gr/isl/CRMsci/>
        prefix dig: <http://www.ics.forth.gr/isl/CRMdig/>
        prefix xsd: <http://www.w3.org/2001/XMLSchema#>
        """

        self.error_re = re.compile("<message>(.+)</message>")


    def endpoint(self, which, params):
        if not which.endswith('.mjs'):
            which = f"{which}.mjs"
        url = self.apps + which
        if 'q' in params and type(params['q']) != str:
            params['q'] = json.dumps(params['q']) 
        resp = requests.post(url, auth=self.auth, headers=self.headers, verify=self.verify_ssl, data=params)
        try:
            js = json.loads(resp.test)
            return js
        except:
            raise ValueError(self.error_re.search(resp.text).group())

    def search_sparql(self, qry):
        if not qry.startswith('prefix') and not qry.startswith('PREFIX'):
            qry = self.prefixes + qry

        resp = requests.get(self.sparql, params={'query':qry, 'database':self.database}, 
                auth=self.auth, headers=self.headers, timeout=self.timeout_long, verify=self.verify_ssl)
        try:
            js = json.loads(resp.text)
        except:
            raise ValueError(self.error_re.search(resp.text).group())

        if 'results' in js:
            binds = js['results']['bindings']     
            return binds
        else:
            return js

    def search_sparql_ids(self, qry):
        # assume only one var returned, and it's a URI
        res = self.search_sparql(qry)
        if not res:
            return []
        ids = []
        k = list(res[0].keys())[0]
        for r in res:
            ids.append(r[k]['value'])
        return ids

    def search_cts(self, qry):
        params = {'q': qry, 'database': self.database, 'pageLength':25, 'view':'results'}
        resp = requests.get(self.search, params=params, auth=self.auth, headers=self.headers, \
            timeout=self.timeout_short, verify=self.verify_ssl)
        js = json.text(resp.text)
        ids = [x['uri'] for x in js['results']]
        return ids

    def do_eval(self, qry, which="javascript"):
        # which in 'javascript' , 'xquery'
        params = {'database':self.database}
        resp = requests.post(self.xquery, params=params, auth=self.auth, data={which:qry}, \
            timeout=3600, verify=self.verify_ssl)
        if 'multipart/mixed' in resp.headers['Content-Type']:
            parsed = decoder.MultipartDecoder.from_response(resp)
            resp_js = {'vals':[]}
            for p in parsed.parts:
                try:
                    resp_js['vals'].append(json.loads(p.text))
                except:
                    resp_js['vals'].append(p.text)
            return resp_js
        elif 'application/json' in resp.headers['Content-Type']:
            return json.loads(resp.text)
        else:
            return resp.text


    def get_record(self, identifier):
        if not identifier.startswith("https://lux.collections.yale.edu/data/"):
            identifier = f"https://lux.collections.yale.edu/data/{identifier}"
        params = {'database': self.database, 'uri': identifier}
        resp = requests.get(self.rest, params=params, auth=self.auth, headers=self.headers, \
            timeout=self.timeout_short, verify=self.verify_ssl)
        try:
            js = json.loads(resp.text)
        except:
            print(resp.text)
            return None
        if 'errorResponse' in js:
            # If unauthorized, then alert to fix it
            if js['errorResponse']['statusCode'] in [401, 403]:
                print(f"ERROR: Could not authenticate to MarkLogic: {js['errorResponse']['message']}")
            else:
                print(f"ERROR: Could not access MarkLogic: {js['errorResponse']['message']}")                
            return None
        else:
            return js

    def update_record(self, identifier, record):
        if not identifier.startswith("https://lux.collections.yale.edu/data/"):
            identifier = f"https://lux.collections.yale.edu/data/{identifier}"
        params = {'database': self.database, 'uri': identifier}
        for (k,v) in self.permissions.items():
            params[f"perm:{k}"] = v
        data = json.dumps(record)
        headers = self.headers.copy()
        headers['Content-Type'] = "application/json"
        resp = requests.put(self.rest, params=params, auth=self.auth, headers=headers, \
            data=data, timeout=self.timeout_short, verify=self.verify_ssl)
        return resp

    def delete_record(self, identifier):
        if not identifier.startswith("https://lux.collections.yale.edu/data/"):
            identifier = f"https://lux.collections.yale.edu/data/{identifier}"
        params = {'database': self.database, 'uri': identifier}
        resp = requests.delete(self.rest, params=params, auth=self.auth, headers=self.headers, \
            timeout=self.timeout_short, verify=self.verify_ssl)
        return resp

    def update_multiple(self, batch):
        if len(self.threads) == self.max_threads:
            kill = None
            cont = True
            while cont:
                for t in self.threads:
                    if not t.is_alive():
                        # Finished, move on
                        kill = t
                        cont = False
                if cont:
                    time.sleep(0.2)
                    # print('.')
            if kill:
                self.threads.remove(kill)
            if kill.resp and kill.resp.status_code != 200:
                print(f"Previous batch got {kill.resp.status_code}")

        t = RequestThread()
        self.threads.append(t)
        t.store = self
        t.batch = batch
        t.resp = None
        t.start()
        return 1
        
    def __getitem__(self, key):
        return self.get_record(key)

    def __setitem__(self, key, value):
        return self.update_record(key, value)

    def __delitem__(self, key):
        return self.delete_record(key)

