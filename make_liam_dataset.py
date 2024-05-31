import os
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

# Find people in merged who have wikipedia links

merged = cfgs.results['merged']['recordcache']

candidates = []

for rec in merged.iter_records_type('Person'):
    data = rec['data']
    if 'subject_of' in data:
        for lo in data['subject_of']:
            if 'digitally_carried_by' in lo:
                for do in lo['digitally_carried_by']:
                    if 'access_point' in do:
                        for ap in do['access_point']:
                            if 'id' in ap and 'wikipedia.org' in ap['id']:
                                candidates.append(data)
                                print(f"Added: {data.get('_label', 'unknown person')}")

# Find objects with wikidata, then look in WD to see if there's wikipedia


