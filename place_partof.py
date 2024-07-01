import os
import re
import sys
import json
import time
import csv
import requests
from dotenv import load_dotenv
from pipeline.config import Config

load_dotenv()
basepath = os.getenv('LUX_BASEPATH', "")
cfgs = Config(basepath=basepath)
idmap = cfgs.instantiate_map('idmap')['store']
cfgs.cache_globals()
cfgs.instantiate_all()

merged = cfgs.results['merged']['recordcache']

def make_list():

    primary = "https://lux.collections.yale.edu/data/concept/f7ef5bb4-e7fb-443d-9c6b-371a23e717ec"
    parens = re.compile("^(.+) \((.+)\)$")

    hiers = {}
    x = 0
    ttl = 603685
    start = time.time()
    has_broader = 0
    for rec in merged.iter_records_type('Place'):
        x += 1
        data = rec['data']
        if not 'part_of' in data:
            name = ""
            for n in data['identified_by']:
                if 'classified_as' in n:
                    cxns = [x['id'] for x in n['classified_as']]
                    if primary in cxns:
                        name = n['content']
                        break  
            name = name.strip()
            if (name and (m := parens.match(name))):
                (nm, parent) = m.groups()
                parent = parent.lower().strip()
                if parent in hiers:
                    hiers[parent].append(nm)
                else:
                    hiers[parent] = [nm]
        else:
            has_broader += 1
        if not x % 10000:
            durn = time.time() - start
            print(f"{x}/{ttl} in {durn} = {x/durn}/sec; found parents: {len(hiers)}")


with open('place_hiers.tsv') as fh:
    rdr = csv.DictReader(fh, delimiter='\t')
    for row in rdr:
        par = row['Expanded'] if row['Expanded'] else row['Parent']
        typ = row['Type']
        if par and typ:
            if typ == "states":
                q = f"https://lux-front-sbx.collections.yale.edu/api/search/place?q=%7B%22AND%22%3A%5B%7B%22_lang%22%3A%22en%22%2C%22name%22%3A%22{par}%22%2C%22_options%22%3A%5B%22unstemmed%22%2C%22unwildcarded%22%5D%2C%22_complete%22%3Atrue%7D%2C%7B%22classification%22%3A%7B%22OR%22%3A%5B%7B%22name%22%3A%22state%22%7D%2C%7B%22name%22%3A%22province%22%7D%5D%7D%7D%5D%7D"
            else:
                q = f"https://lux-front-sbx.collections.yale.edu/api/search/place?q=%7B%22AND%22%3A%5B%7B%22name%22%3A%22{par}%22%2C%22_options%22%3A%5B%22unstemmed%22%2C%22unwildcarded%22%5D%2C%22_complete%22%3Atrue%7D%2C%7B%22classification%22%3A%7B%22name%22%3A%22{typ}%22%7D%7D%5D%7D"
            res = requests.get(q)
            data = res.json()
            its = data['orderedItems']
            if len(its) == 1:
                # seems likely

                # get the record
                rest, yuid = its[0]['id'].rsplit('/',1)
                print(f"{par} [{typ}]:\t{yuid}")
                rec = merged[yuid]
                if rec:
                    eqs = rec['data'].get('equivalent', [])
                    # filter for TGN, LCNAF
                    opts = []
                    tgn = []
                    for e in eqs:
                        if 'vocab.getty.edu/tgn' in e['id']:
                            opts.append(e)
                    if len(opts) > 1:
                        print(opts)

            else:
                print(f" *** {par} [{typ}]: {len(its)}")

