import os
import re
import sys
import json
import time
import datetime
import gzip
from dotenv import load_dotenv
from pipeline.config import Config

load_dotenv()
basepath = os.getenv('LUX_BASEPATH', "")
cfgs = Config(basepath=basepath)
idmap = cfgs.instantiate_map('idmap')['store']
cfgs.cache_globals()
cfgs.instantiate_all()

merged = cfgs.results['merged']['recordcache']
primary = "https://lux.collections.yale.edu/data/concept/f7ef5bb4-e7fb-443d-9c6b-371a23e717ec"

parens = re.compile("^(.+) \((.+)\)$")

hiers = {}

for rec in merged.iter_records_type('Place'):
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
        if name and m := parens.match(name):
            (nm, parent) = m.groups()
            if parent in hiers:
                hiers[parent].append(nm)
            else:
                hiers[parent] = [nm]
