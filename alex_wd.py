import os
from dotenv import load_dotenv
from pipeline.config import Config

load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.cache_globals()
cfgs.instantiate_all()

wd = cfgs.external['wikidata']['datacache']

oafh = open("oa_wd.tsv", "w")
x = 0
for d in wd.iter_records():
    x += 1
    if x % 1000 == 0:
        print(x)
    oas = d['data'].get("P10283", [])
    if oas:
        id = d['identifier']
        for oa in oas:
            oafh.write(f"{oa}\t{id}\n")
    if not x % 1000000:
        print(f"Processed {x} records")
        oafh.flush()
oafh.close()