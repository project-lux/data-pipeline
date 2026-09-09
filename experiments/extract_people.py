import json
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

if len(sys.argv) > 2 and sys.argv[1].isnumeric() and sys.argv[2].isnumeric():
    my_slice = int(sys.argv[1])
    max_slice = int(sys.argv[2])
else:
    my_slice = -1
    max_slice = -1

merged_cache = cfgs.results["merged"]["recordcache"]

outh = open(f"dump_person_{my_slice}.jsonl", "w")

total = 5275500 / max_slice
x = 0
for recid in merged_cache.iter_keys_type_slice("Person", my_slice, max_slice):
    rec = merged_cache[recid]
    outh.write(json.dumps(rec["data"]) + "\n")
    x += 1
    if not x % 50000:
        print(f"{x}/{total}")

outh.close()
