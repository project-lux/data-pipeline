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

ALL_DATA = "--all-data" in sys.argv

to_do = []
if "--all" in sys.argv:
    to_do = list(cfgs.external.items())
else:
    for src, cfg in cfgs.external.items():
        if f"--{src}" in sys.argv:
            to_do.append((src, cfg))


if len(sys.argv) > 2 and sys.argv[1].isnumeric() and sys.argv[2].isnumeric():
    my_slice = int(sys.argv[1])
    max_slice = int(sys.argv[2])
else:
    my_slice = -1
    max_slice = -1

if not to_do:
    print("No source to update given")
    sys.exit()

for src, cfg in to_do:
    # iterate through slice of recordcache
    # and export the raw data from datacache

    dc = cfg["datacache"]
    rc = cfg["recordcache"]
    store = dc if ALL_DATA else rc
    ttl = store.len_estimate()

    print(f"Exporting {ttl} records from {src}")

    outfn = f"/data-export/output/external/export_{src}_{my_slice}.zip"
    done = {}

    start = time.time()
    x = 0
    with zipfile.ZipFile(outfn, "w", compression=zipfile.ZIP_BZIP2) as fh:
        for ident in store.iter_keys():
            ident = cfgs.split_qua(ident)[0]
            if ident in done:
                continue
            x += 1
            done[ident] = 1
            rec = dc[ident]
            outjs = {"data": rec["data"]}
            with fh.open(ident, "w") as ffh:
                outs = json.dumps(outjs, separators=(",", ":"))
                outb = outs.encode("utf-8")
                ffh.write(outb)
            if not x % 25000:
                print(f"  {x} in {time.time() - start}")

    fh.close()
    end = time.time()
    print(end - start)
