import os
import sys
from dotenv import load_dotenv
from pipeline.config import Config

load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.cache_globals()
cfgs.instantiate_all()

# Exclude globals
gls = set([x[-36:] for x in list(cfgs.globals.values()) if x])

# Walk through ML store and create 16 LMDBs for Object:Subject
ml = cfgs.results['marklogic']['recordcache']

for rec in ml.iter_records():
    trips = rec['data']['triples']
    # Exclude lux:any
    anys = set([x['triple']['object'][-36:] for x in trips if x['triple']['predicate'].endswith('/any')])
    refs = set([x['triple']['object'][-36:] for x in trips if x['triple']['predicate'].endswith('allRefCtr')])
    res = refs - gls
    res = res - anys
    print(f"{len(refs) --> {len(res)}}")
