import os
import sys
import ujson as json
import gzip
import time
from dotenv import load_dotenv
from pipeline.config import Config

load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.cache_globals()
cfgs.instantiate_all()

to_do = []
for src, cfg in cfgs.external.items():
    if f"--{src}" in sys.argv:
        to_do.append((src, cfg))
for src, cfg in cfgs.internal.items():
    if f"--{src}" in sys.argv:
        to_do.append((src, cfg))

if len(sys.argv) > 2 and sys.argv[1].isnumeric() and sys.argv[2].isnumeric():
    my_slice = int(sys.argv[1])
    max_slice = int(sys.argv[2])
else:
    my_slice = -1
    max_slice = -1

if not to_do:
    print("No source given to export")
    sys.exit()

for src, cfg in to_do:
    # iterate through slice of recordcache
    # and export the raw data from datacache

    dc = cfg["datacache"]
    ttl = dc.len_estimate()

    print(f"Exporting ~{ttl} records from {src}")

    if my_slice == -1:
        outfn = f"/data-export/output/external/export_{src}.jsonl.gz"
        itr = dc.iter_records(raw=True)
    else:
        itr = dc.iter_records_slice(my_slice, max_slice, raw=True)
        outfn = f"/data-export/output/external/export_{src}_{my_slice}.jsonl.gz"

    start = time.time()
    x = 0
    with gzip.open(outfn, "wt", 1) as fh:
        for rec in itr:
            x += 1
            ident = rec['identifier']
            rec = rec['data'] # this is still a string due to raw=True
            outs = f"{ident}\t{rec}\n"
            fh.write(outs)
            if not x % 25000:
                print(f"  {x} in {time.time() - start}")
                sys.stdout.flush()

    fh.close()
    end = time.time()
    print(end - start)
