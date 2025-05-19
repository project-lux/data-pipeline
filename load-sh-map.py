import os
import sys
import csv
import json
import time
from dotenv import load_dotenv
from pipeline.config import Config

load_dotenv()
basepath = os.getenv('LUX_BASEPATH', "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.cache_globals()
cfgs.instantiate_all()

loader = cfgs.internal['ils']['indexLoader']


# Dedicated LMDB index for heading mappings
heading_index = loader.load_index()
csvfn = sys.argv[-1]

if "--clear" in sys.argv:
    loader.clear()

if "--update" in sys.argv:
    loader.update(csvfn)

if not os.path.exists(csvfn):
    print(f"That file ({csvfn}) does not exist")
    sys.exit(0)

with open(csvfn, newline='') as fh:
    rdr = csv.reader(fh)
    for row in rdr:
        if not row or len(row) < 2:
            continue
        key = row[0].strip()
        values = [v.strip() for v in row[1:] if v.strip()]
        loader.set(heading_index, key, values)

print(f"Loaded heading map from {csvfn}")

