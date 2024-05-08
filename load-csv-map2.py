import os
import sys
import csv
import json
import time
from dotenv import load_dotenv
from pipeline.config import Config

# Load from CSV to LMDB via index_loader in merged config

# Usage load-csv-map.py --same|--different <path/to/file.csv>
# or: load-csv-map.py --all [--clear]

load_dotenv()
basepath = os.getenv('LUX_BASEPATH', "")
cfgs = Config(basepath=basepath)
idmap = cfgs.instantiate_map('idmap')['store']
cfgs.cache_globals()
cfgs.instantiate_all()

# Load CSV file to given Index
loader = cfgs.results['merged']['indexLoader']

if '--all' in sys.argv:
    # process based on directories
    same_dir = os.path.join(cfgs.data_dir, "sameAs")
    diff_dir = os.path.join(cfgs.data_dir, "differentFrom")

    if '--clear' in sys.argv:
        loader.clear('equivs')
        loader.clear('diffs')

    for csvfn in os.listdir(same_dir):
        if csvfn.endswith('.csv'):
            print(f" -- Adding {csvfn} to sameAs map")
            fn = os.path.join(same_dir, csvfn)
            loader.load(fn, "equivs")

    for csvfn in os.listdir(diff_dir):
        if csvfn.endswith('.csv'):
            print(f" -- Adding {csvfn} to diff map")
            fn = os.path.join(diff_dir, csvfn)
            loader.load(fn, "diffs")

else:
    if '--same' in sys.argv:
        which = "equivs"
    elif '--different' in sys.argv:
        which = "diffs"
    else:
        print("You must give --all, or either --same or --different when loading maps")
        sys.exit(0)

    csvfn = sys.argv[-1]
    if not os.path.exists(csvfn):
        print(f"That file ({csvfn}) does not exist")
        sys.exit(0)
    loader.load(csvfn, which)

print("Done")
