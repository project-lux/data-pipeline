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

non_global_externals = [
    'http://vocab.getty.edu/aat/300264578##quaType',
    'http://vocab.getty.edu/aat/300435704##quaType',
    'http://id.loc.gov/authorities/names/n80008747##quaGroup',
    'http://vocab.getty.edu/aat/300133046##quaType',
    'http://vocab.getty.edu/aat/300025883##quaType',
    'http://vocab.getty.edu/aat/300435452##quaType',
    'http://vocab.getty.edu/aat/300435430##quaType',
    'http://vocab.getty.edu/aat/300202362##quaType',
    'http://vocab.getty.edu/aat/300311706##quaType',
    'http://vocab.getty.edu/aat/300404621##quaType',
    'http://vocab.getty.edu/aat/300055665##quaType',
    'http://vocab.getty.edu/aat/300027200##quaType',
    'http://vocab.getty.edu/aat/300404333##quaType',
    'http://id.loc.gov/authorities/names/n78015294##quaGroup',
    'http://vocab.getty.edu/aat/300404264##quaType',
    'http://vocab.getty.edu/aat/300417443##quaType',
    'http://vocab.getty.edu/aat/300026497##quaType',
    'http://vocab.getty.edu/aat/300215302##quaType'
]
non_global_excludes = [idmap[x][:-36:] for x in non_global_externals]
all_excludes.update(set(non_global_excludes))

# Walk through ML store and create 16 LMDBs for Object:Subject
ml = cfgs.results['marklogic']['recordcache']

refcts = []
rescts = []

ref_hash = {}
c = 0
for rec in ml.iter_records():
    c += 1
    trips = rec['data']['triples']
    # Exclude lux:any
    anys = set([x['triple']['object'][-36:] for x in trips if x['triple']['predicate'].endswith('/any')])
    refs = set([x['triple']['object'][-36:] for x in trips if x['triple']['predicate'].endswith('/allRefCtr')])
    res = refs - gls
    res = res - anys
    refcts.append(len(refs))
    rescts.append(len(res))

    for x in res:
        try:
            ref_hash[x] += 1
        except:
            ref_hash[x] = 1


    if c >= 1000000:
        break
