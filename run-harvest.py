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

if not to_do:
    print("No source to havest given")
    sys.exit()

mgr = UpdateManager(cfgs, idmap)
#last_update = None
#harvest_from = None
last_update = "2023-01-01T00:00:00"
harvest_from = "2024-01-01T00:00:00"

for src, cfg in to_do:
    if last_update:
        cfg['harvester'].last_harvest = last_update
    if harvest_from:
        cfg['harvester'].harvest_from = harvest_from
    print(f"Harvesting {src} records")
    mgr.harvest_single(src)
