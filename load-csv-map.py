import os
import sys
import csv
import json
import time
from dotenv import load_dotenv
from pipeline.config import Config


# Usage load-csv-map.py --same|--different [--pipe] <path/to/file.csv>
# or: load-csv-map.py --all

load_dotenv()
basepath = os.getenv('LUX_BASEPATH', "")
cfgs = Config(basepath=basepath)
idmap = cfgs.instantiate_map('idmap')['store']
cfgs.cache_globals()
cfgs.instantiate_all()

def process_file(csvfn, delim, targetmap):
    with open(csvfn) as csvfh:
        rdr = csv.reader(csvfh, delimiter=delim)
        x = 0
        start = time.time()
        for row in rdr:
            x += 1
            if not x % 1000:
                secs = time.time() - start
                print(f"{x} in {secs} = {x/secs}/sec")
            if not row[0].startswith('http') or not row[1].startswith('http'):
                continue
            (a,b) = row[:2]

            if a.startswith('https://lux.collections.yale.edu/data/'):
                # replace with externals
                origs = idmap[a]
                all_a = []
                if origs:
                    # make them all equivalent
                    for o in origs:
                        if o != b and not o.startswith('__') and not o.startswith('https://lux.collections'):
                            if "##qua" in o:
                                o = o.split("##qua")[0]
                            all_a.append(o)
            else:                
                a = cfgs.canonicalize(a)
                all_a = [a]

            if b.startswith('https://lux.collections.yale.edu/data/'):
                # replace with an external
                origs = idmap[b]
                all_b = []
                if origs:
                    # make them all equivalent
                    for o in origs:
                        if o != a and not o.startswith('__') and not o.startswith('https://lux.collections'):
                            if "##qua" in o:
                                o = o.split("##qua")[0]
                            all_b.append(o)
            else:                
                b = cfgs.canonicalize(b)
                all_b = [b]

            # Already merged b into a
            # So ensure a == b later
            if not all_b:
                all_b = all_a

            for a in all_a:
                for b in all_b:
                    if not a or not b:
                        print(f"got NONE for {a} / {b} from row {row}")
                        continue
                    targetmap[a] = b
                    # map will do b=a itself

# Load CSV file to given Map

if '--all' in sys.argv:
    # process based on directories
    same_dir = os.path.join(cfgs.data_dir, "sameAs")
    same_map = cfgs.instantiate_map('equivalents')['store']
    diff_dir = os.path.join(cfgs.data_dir, "differentFrom")
    diff_map = cfgs.instantiate_map('distinct')['store']

    # clear & reload
    same_map.clear()
    for csvfn in os.listdir(same_dir):
        if csvfn.endswith('.csv'):
            print(f" -- Adding {csvfn} to sameAs map")
            process_file(os.path.join(same_dir, csvfn), ',', same_map)
    diff_map.clear()
    for csvfn in os.listdir(diff_dir):
        if csvfn.endswith('.csv'):
            print(f" -- Adding {csvfn} to diff map")
            process_file(os.path.join(diff_dir, csvfn), ',', diff_map)

else:
    if '--same' in sys.argv:
        target = cfgs.instantiate_map('equivalents')
    elif '--different' in sys.argv:
        target = cfgs.instantiate_map('distinct')
    else:
        print("You must give --all, or either --same or --different when loading maps")
        sys.exit(0)

    targetmap = target['store']
    csvfn = sys.argv[-1]
    if not os.path.exists(csvfn):
        print(f"That file ({csvfn}) does not exist")
        sys.exit(0)

    if '--pipe' in sys.argv:
        delim = "|"
    else:
        delim = ","
    process_file(csvfn, delim, targetmap)

print("Done")
