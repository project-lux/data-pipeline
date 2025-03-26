
import os
import ujson as json
import gzip
import time
from lux_pipeline.process.base.loader import Loader, OldLoader


class DnbLoader(Loader):
    # Main file is arraylines.gz/json
    # Sachbegriff is not a normal structure: sachbegriff.gz/json

    def extract_identifier(self, record):
        uri = record['@id']
        return uri.rsplit('/', 1)[1]

    def iterate_sachbegriff(self, path, comp, parent):
        # sbg is array but items can also be arrays OR dicts
        for member in self.iterate_array(path, comp, parent):
            if type(member) == dict:
                yield member
            elif type(member) == list:
                for item in member:
                    yield item



class OldDnbLoader:

    def __init__(self, config):
        Loader.__init__(self, config)
        d = config['all_configs'].data_dir
        fn = config.get('externalLinksPath', 'authorities-sachbegriff_lds.jsonld')
        if os.path.exists(fn):
            self.subject_path = os.path.join(d, fn)
        else:
            self.subject_path = ""

    def get_identifier_raw(self, line):
        ididx = line.find('"@id":"https://d-nb.info/gnd/')
        endidx =line[ididx+29:ididx+60].find('"')
        what = line[ididx+29:ididx+29+endidx]
        return what

    def get_identifier_json(self, js):
        # Should never actually get called, but for completeness...
        uri = js['@id']
        return uri.rsplit('/', 1)[1]

    def process_sachbegriff(self, rec):
        if '@id' in rec and '/d-nb.info/gnd/' in rec['@id'] and not rec['@id'].endswith('about'):
            # store as a list because that's what we get from the web
            ident = rec['@id'].rsplit('/', 1)[1]
            self.out_cache[ident] = {'list': [rec]}

    def load_sachbegriff(self):
        if os.path.exists(self.subject_path):
            with open(self.subject_path) as fh:
            # has *weird* json format
                data = fh.read()
            js = json.loads(data)
            del data
        else:
            js = []

        for outer in js:
            if type(outer) == list:
                for inner in outer:
                    if type(inner) == dict:
                        self.process_sachbegriff(inner)
                    else:
                        print(f"Got {inner} in inner")
            elif type(outer) == dict:
                self.process_sachbegriff(outer)
            else:
                print(f"Got {outer} in outer")


    def load(self):

        # load the subject headings
        self.load_sachbegriff()

        # And now load the entityfacts

        with gzip.open(self.in_path) as fh:
            start = time.time()
            x = 0 
            done_x = 0
            l = 1
            while l:
                l = fh.readline()
                if not l:
                    break
                l = l.decode("utf-8")
                if l[0] in ['[', ',']:
                    l = l[1:]
                elif l[-1] == ']':
                    l = l[:-1]            
                l = l.strip()

                # Find id and check if already exists before processing JSON
                what = self.get_identifier_raw(l)

                if what in self.out_cache:
                    done_x += 1
                    if not done_x % 10000:
                        print(f"Skipping past {done_x} {time.time() - start}")
                    continue
                x += 1
                try:
                    js = json.loads(l)
                except:
                    print(f"Failed to parse json from record {x}: {l}")
                    raise
                    continue
                self.out_cache[what] = js
                if not x % 10000:
                    t = time.time() - start
                    xps = x/t
                    ttls = self.total / xps
                    print(f"{x} in {t} = {xps}/s --> {ttls} total ({ttls/3600} hrs)")
        self.out_cache.commit()
