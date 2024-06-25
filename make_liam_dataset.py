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

merged = cfgs.results['merged']['recordcache']
wd = cfgs.external['wikidata']['datacache']

# Find people in merged who have wikipedia links
candidates = []
try:
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
                                    print(f"Added: {data.get('_label', 'unknown person')} to {len(candidates)}")
                                    if len(candidates) > 12000:
                                        raise ValueError()
except ValueError as e:
    pass
candidates.sort(key=lambda x: len(x.keys()), reverse=True)


# Find objects with wikidata, then look in WD to see if there's wikipedia
ocandidates = []
try:
    for rec in merged.iter_records_type('HumanMadeObject'):
        data = rec['data']
        if 'equivalent' in data:
            for eq in data['equivalent']:
                if 'id' in eq and 'wikidata.org' in eq['id']:
                    q = eq['id'].rsplit('/', 1)[-1]
                    wdrec = wd[q]
                    if not wdrec:
                        print(f"invalid wd ref {q}")
                    else:
                        if 'sitelinks' in wdrec['data']:
                            print(f"Added: {data.get('_label', 'unknown object')} to {len(ocandidates)}")
                            ocandidates.append(data)
                            if len(ocandidates) > 1000:
                                raise ValueError()
except ValueError as e:
    pass




