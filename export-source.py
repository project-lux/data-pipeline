import os
import sys
import ujson as json
import zipfile
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

    outfn = f"/data-export/output/external/export_{src}_{my_slice}.zip"

    start = time.time()
    x = 0
    with zipfile.ZipFile(outfn, "w", compression=zipfile.ZIP_BZIP2) as fh:
        for rec in dc.iter_records_slice(my_slice, max_slice, raw=True):
            x += 1
            rec = rec['data']
            outs = json.dumps(rec, separators=(",", ":"))
            outb = outs.encode("utf-8")
            fh.write(outb)
            if not x % 25000:
                print(f"  {x} in {time.time() - start}")
                sys.stdout.flush()

    fh.close()
    end = time.time()
    print(end - start)
