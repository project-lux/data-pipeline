import os
import sys
import json
import time
from dotenv import load_dotenv
from pipeline.config import Config
from pipeline.process.update_manager import UpdateManager

load_dotenv()
basepath = os.getenv('LUX_BASEPATH', "")
cfgs = Config(basepath=basepath)
idmap = cfgs.instantiate_map('idmap')['store']
cfgs.cache_globals()
cfgs.instantiate_all()

to_do = []

if '--all' in sys.argv:
    to_do = list(cfgs.internal.items()) 
else:
    for src, cfg in cfgs.internal.items():
        if f"--{src}" in sys.argv:
            to_do.append((src, cfg))
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
    print("No source to havest given")
    sys.exit()

mgr = UpdateManager(cfgs, idmap)
last_update = None
harvest_from = None
# last_update = "2020-01-01T00:00:00"
# harvest_from = "2024-01-01T00:00:00"

for src, cfg in to_do:

    if max_slice > -1:
        # call harvest_from_list
        mgr.harvest_from_list(cfg, my_slice, max_slice)
    else:
        if last_update:
            cfg['harvester'].last_harvest = last_update
        if harvest_from:
            cfg['harvester'].harvest_from = harvest_from

        cfg['harvester'].page_cache = cfgs.external['activitystreams']['datacache']
        print(f"Harvesting {src} records")
        mgr.harvest_single(src)
