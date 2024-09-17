import os
import sys
import json
import time
from dotenv import load_dotenv
from pipeline.config import Config

load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
cfgs.allow_network = True
idmap = cfgs.get_idmap()
cfgs.cache_globals()
cfgs.instantiate_all()
cfgs.allow_network = True  # just in case

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
    # and retrieve new raw data for datacache
    acq = cfg["acquirer"]
    acq.force_rebuild = True
    rc = cfg["recordcache"]
    dc = cfg["datacache"]

    for ident in rc.iter_keys_slice(my_slice, max_slice):
        ident = cfgs.split_qua(ident)[0]
        # This plus force_rebuild avoids current caches and stores data
        # but doesn't map or post_map
        rec = acq.acquire(ident, dataonly=True, store=True)
