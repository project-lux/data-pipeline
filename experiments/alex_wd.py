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

all_oas = []

x = 0
for d in wd.iter_records():
    x += 1
    oas = d['data'].get("P10283", [])
    if oas:
        id = d['identifier']
        for oa in oas:
            all_oas.append((oa, id))
    if not x % 1000000:
        print(f"Processed {x} records")

all_oas.sort(key=lambda x: x[0])
print(f"Total OAs: {len(all_oas)}")

with open("oa_wd.tsv", "w") as oafh:
    for oa, id in all_oas:
        oafh.write(f"{oa}\t{id}\n")
