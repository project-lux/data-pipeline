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

non_global_excludes = [
    '2eca07bd-be42-4ef5-9ec5-87c1bbfe639d', # web pages
    '89630361-18a3-4c4b-bcd7-16894d95defd', # record identifiers
    'd07b9b86-0a1e-4026-aa4c-8ecba8bbd9c9', # YUL group
    '03f4eb19-0611-4f31-8e09-fc111c52f898', # access statement type
    'd97f2e4a-1549-4c4b-9a27-bcf41a2b4972', # visitors (?) statement type
    '91fd5ce0-b58a-4571-9fde-839aed0c876e', # physical description
    '53922f57-dab5-43c5-a527-fc20a63fe128', # dimensions description
    'f3e231d7-6b87-4af2-9bb8-e7b0b1f6ec09', # imprint stmt type
    '35961a03-7a62-4494-bd50-f1630477035f', # call numbers
    'a1922667-5c32-4181-8897-3e71ecea0ce8', # repository numbers
    'b0ac44c2-83ba-4eae-9a01-fcfa547dd5b0', # numerals (?)
    'acc79f08-f8f7-4f7c-8179-1c8f03aa9020', # notes stmt type
    'fcd4a80f-b3e2-4c56-9153-3e3a4f4dc19d', # transcription stmt type
    '18b96dab-a9f1-4b30-83d9-599289e490ba', # OCLC group
    'c365fba7-32a1-48eb-91b6-235d90bdca9c', # attribution stmt type
    '047d3267-e00d-4f90-a08f-fda2041aa1b7', # ISBN
    '372190e4-3284-4d1d-8236-d771cb7f2dbf', # books
]


for ng in non_global_excludes:
    exts = idmap[f"https://lux.collections.yale.edu/data/concept/{ng}"]
    if not exts:
        exts = idmap[f"https://lux.collections.yale.edu/data/group/{ng}"]
    fe = False
    for e in exts:
        if e.startswith('http://vocab.getty.edu/aat/'):
            fe = True
            print(f"{ng} --> {e}")
            break
    if not fe:
        print(exts)



gls.update(set(non_global_excludes))

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
